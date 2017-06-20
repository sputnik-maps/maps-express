#include "tile_handler.h"

#include <fstream>
#include <cctype>

#include <folly/io/async/EventBaseManager.h>

#include <proxygen/httpserver/ResponseBuilder.h>

#include "util.h"


// TODO: Needs refactoring

// TODO: Implement ttl calculaton for cacher

using std::experimental::optional;
using std::experimental::nullopt;

using HTTPMessage = proxygen::HTTPMessage;
using HTTPMethod = proxygen::HTTPMethod;

static std::string MakeCacherKey(const TileId& id, const std::string& info_str) {
    std::string key;
    key.append(std::to_string(id.x));
    key.append("/");
    key.append(std::to_string(id.y));
    key.append("/");
    key.append(std::to_string(id.z));
    key.append("/");
    key.append(info_str);
    return key;
}

static std::string MakeRequestInfoStr(const std::set<std::string> tags, util::ExtensionType ext,
                                      const std::string& data_version, std::set<std::string>* layers,
                                      uint metatile_width = 1, uint metatile_height= 1) {
    std::string info_str;
    for (const std::string& tag : tags) {
        info_str.append(tag);
        info_str.append("/");
    }
    info_str.append(".");
    info_str.append(util::ext2str(ext));
    info_str.append("/");
    info_str.append(data_version);
    info_str.append("/");
    info_str.append(std::to_string(metatile_width));
    info_str.append("/");
    info_str.append(std::to_string(metatile_height));
    info_str.append("/");
    if (layers) {
        info_str.append("l:");
        for (const std::string& layer_name : *layers) {
            info_str.append(layer_name);
            info_str.append("/");
        }
    }
    return info_str;
}

static std::chrono::seconds TTLPolicyToSeconds(CachedTile::TTLPolicy policy) {
    switch (policy) {
    case CachedTile::TTLPolicy::regular:
        return std::chrono::seconds(86400);
    case CachedTile::TTLPolicy::extended:
        return std::chrono::seconds(259200);
    case CachedTile::TTLPolicy::error:
        return std::chrono::seconds(20);
    }
    return std::chrono::seconds(0);
}


TileHandler::TileHandler(RenderManager& render_manager,
                         DataManager& data_manager,
                         std::shared_ptr<const endpoints_map_t> endpoints,
                         TileCacher* cacher) :
        rm_(render_manager),
        dm_(data_manager),
        endpoints_(std::move(endpoints)),
        cacher_(cacher) {}

static inline bool is_version(const std::string& segment) noexcept {
    const auto segment_size = segment.size();
    if (segment_size < 2 || segment_size > 6 || segment[0] != 'v') {
        return false;
    }
    for (uint i = 1; i < segment_size; ++i) {
        if (!std::isdigit(segment[i])) {
            return false;
        }
    }
    return true;
}

void TileHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod() != HTTPMethod::GET) {
        SendError(405);
        return;
    }

    std::vector<std::string> split_path;
    util::split(headers->getPath(), split_path);
    const size_t num_segments = split_path.size();
    if (num_segments < 3) {
        SendError(400);
        return;
    }

    endpoints_map_t::const_iterator endpoint_itr = endpoints_->end();
    if (num_segments > 3) {
        uint first_tag_pos = 0;
        if (is_version(split_path[0])) {
            data_version_ = std::move(split_path[0]);
            if (num_segments > 4 &&
                    (endpoint_itr = endpoints_->find(split_path[1])) != endpoints_->end()) {
                first_tag_pos = 2;
            } else {
                first_tag_pos = 1;
            }
        } else {
            if ((endpoint_itr = endpoints_->find(split_path[0])) != endpoints_->end()) {
                first_tag_pos = 1;
            }
        }
        for (uint ti = first_tag_pos; ti < num_segments - 3; ++ti) {
            tags_.insert(std::move(split_path[ti]));
        }
    }

    if (endpoint_itr == endpoints_->end()) {
        endpoint_itr = endpoints_->find("");
        if (endpoint_itr == endpoints_->end()) {
            SendError(404);
            return;
        }
    }

    std::string ext;
    uint x, y, z;
    const std::string& last_segment = split_path.back();
    size_t ext_pos = last_segment.find('.');
    if (ext_pos != std::string::npos) {
        if (ext_pos + 1 > last_segment.size()) {
            SendError(400);
            return;
        }
        ext = last_segment.substr(ext_pos + 1);
        y = std::atoi(last_segment.data());
    } else {
        y = std::atoi(last_segment.data());
    }
    x = std::atoi(split_path[num_segments-2].data());
    z = std::atoi(split_path[num_segments-3].data());

    tile_id_ = TileId(x, y, z);

    ext_ = util::str2ext(ext);
    if (ext_ == ExtensionType::none) {
        SendError(404);
        return;
    }

    for (const auto& endpoint_params : endpoint_itr->second) {
        if (endpoint_params->minzoom <= tile_id_.z && endpoint_params->maxzoom >= tile_id_.z) {
            endpoint_params_ = endpoint_params;
            break;
        }
    }
    if (!endpoint_params_) {
        SendError(404);
        return;
    }

    if (!CheckParams()) {
        SendError(400);
    }

    if (!endpoint_params_->provider_name.empty()) {
        data_provider_ = dm_.GetProvider(endpoint_params_->provider_name);
    }

    if (endpoint_params_->allow_layers_query) {
        const std::string& requested_layers_param = headers->getQueryParam("layers");
        if (!requested_layers_param.empty()) {
            layers_ = util::ParseArray(requested_layers_param);
        }
    }

    if (cacher_ && endpoint_params_->type != EndpointType::static_files) {
        metatile_id_ = GetMetatileId();
        if (!metatile_id_) {
            SendError(500);
            return;
        }
        request_info_ = MakeRequestInfoStr(tags_, ext_, data_version_, layers_.get(),
                                           metatile_id_->width(), metatile_id_->height());
        // We do not cache static files
        LoadFromCacheOrGenerate();
    } else {
        GenerateTile();
    }
}


void TileHandler::GenerateTile() noexcept {
    if (!endpoint_params_->provider_name.empty()) {
        // need to load tile
        LoadTile();
    } else if (endpoint_params_->type == EndpointType::render) {
        ProcessRender();
    } else if (endpoint_params_->type == EndpointType::mvt) {
        ProcessMvt();
    } else {
        SendError(500);
    }
}

void TileHandler::LoadFromCacheOrGenerate() noexcept {
    assert(cacher_);
    assert(!request_info_.empty());
    std::string key = MakeCacherKey(tile_id_, request_info_);
    auto cacher_task = std::make_shared<TileCacher::GetTask>([this, key]
                                                             (std::shared_ptr<const CachedTile> tile) {
        CancelTaskTimeout();
        if (!tile) {
            // Tile not found in cache
            std::vector<std::string> locked_cache_keys;
            if (endpoint_params_->type == EndpointType::render) {
                std::vector<TileId> ids_to_lock = metatile_id_->TileIds();
                for (const TileId& id : ids_to_lock) {
                    locked_cache_keys.push_back(MakeCacherKey(id, request_info_));
                }
            } else {
                locked_cache_keys.push_back(key);
            }
            if (!cacher_->LockUntilSet(locked_cache_keys)) {
                LoadFromCacheOrError(key);
                return;
            }
            save_to_cache_ = true;
            locked_cache_keys_ = std::move(locked_cache_keys);
            GenerateTile();
            return;
        }
        cacher_->Touch(key, TTLPolicyToSeconds(tile->policy));
        OnProcessingSuccess(std::string(tile->data));
    }, [this]{
        CancelTaskTimeout();
        GenerateTile();
    }, true);
    cacher_->Get(key, cacher_task);
    // TODO: Maybe handle timeout overdue
    ScheduleTaskTimeout(std::move(cacher_task), std::chrono::seconds(20));
}

void TileHandler::LoadFromCacheOrError(const std::string& key) noexcept {
    auto cacher_task = std::make_shared<TileCacher::GetTask>([this](std::shared_ptr<const CachedTile> tile){
        if (!tile) {
            SendError(500);
            return;
        }
        OnProcessingSuccess(std::string(tile->data));
    }, [this]{
        GenerateTile();
    }, true);
    cacher_->Get(key, cacher_task);
    // TODO: Maybe handle timeout overdue
    ScheduleTaskTimeout(std::move(cacher_task), std::chrono::seconds(20));
}


bool TileHandler::CheckParams() noexcept {
    if (!tile_id_.Valid()) return false;
    if (ext_ == ExtensionType::png && endpoint_params_->type == EndpointType::mvt) return false;
    if (ext_ == ExtensionType::mvt && endpoint_params_->type != EndpointType::mvt) return false;
    if (ext_ == ExtensionType::json && (endpoint_params_->type != EndpointType::render ||
                                          !endpoint_params_->allow_utf_grid)) return false;
    return true;
}

optional<MetatileId> TileHandler::GetMetatileId() noexcept {
    // Should be called only after onRequest()!
    assert(endpoint_params_);
    if (endpoint_params_->auto_metatile_size) {
        if (!data_provider_) {
            LOG(ERROR) << "Endpoint configured to use auto metatile size, but data provider missing!";
            return nullopt;
        }
        auto metatile_id = data_provider_->GetOptimalMetatileId(tile_id_, endpoint_params_->zoom_offset);
        if (!metatile_id) {
            LOG(ERROR) << "Error while computing optimal metatile id!";
        }
        return metatile_id;
    }
    return MetatileId(tile_id_, endpoint_params_->metatile_width, endpoint_params_->metatile_height);
}

void TileHandler::LoadTile() noexcept {
    assert(endpoint_params_);
    if (!(data_provider_ && data_provider_->HasVersion(data_version_))) {
        SendError(404);
        return;
    }
    TileId data_tile_id = tile_id_;
    int zoom_offset = endpoint_params_->zoom_offset;
    if (zoom_offset < 0) {
        data_tile_id = GetUpperZoom(data_tile_id, -zoom_offset);
    }
    auto load_task = data_provider_->GetTile(
                std::bind(&TileHandler::OnLoadSuccess, this, std::placeholders::_1),
                std::bind(&TileHandler::OnLoadError, this, std::placeholders::_1),
                data_tile_id, data_version_);
    ScheduleTaskTimeout(std::move(load_task), std::chrono::seconds(20));
}

void TileHandler::OnLoadSuccess(Tile&& tile) noexcept {
    CancelTaskTimeout();
    if (endpoint_params_->type == EndpointType::static_files) {
        OnProcessingSuccess(std::move(tile.data));
        return;
    }

    data_tile_ = std::make_shared<Tile>(std::move(tile));
    if (endpoint_params_->type == EndpointType::render) {
        ProcessRender();
    } else {
        ProcessMvt();
    }
}

void TileHandler::OnLoadError(LoadError err) noexcept {
    CancelTaskTimeout();
    UnlockCache();
    if (err == LoadError::not_found) {
        SendError(404);
    } else {
        SendError(500);
    }
}

void TileHandler::ProcessRender() noexcept {
    auto render_request = std::make_unique<RenderRequest>(tile_id_);
    assert(metatile_id_);
    render_request->metatile_id = *metatile_id_;
    render_request->style_name = endpoint_params_->style_name;
    render_request->data_tile = std::move(data_tile_);
    render_request->retina = tags_.find("retina") != tags_.end();
    auto render_task = rm_.Render(std::move(render_request),
                                   std::bind(&TileHandler::OnRenderingSuccess, this, std::placeholders::_1),
                                   std::bind(&TileHandler::OnProcessingError, this));
    ScheduleTaskTimeout(std::move(render_task), std::chrono::seconds(20));
}

void TileHandler::ProcessMvt() noexcept {
    auto subtile_request = std::make_unique<SubtileRequest>(std::move(*data_tile_), tile_id_);
    subtile_request->filter_table = endpoint_params_->filter_table;
    subtile_request->layers = std::move(layers_);

    auto subtile_task = rm_.MakeSubtile(std::move(subtile_request),
                                        std::bind(&TileHandler::OnRenderingSuccess, this, std::placeholders::_1),
                                        std::bind(&TileHandler::OnProcessingError, this));
    ScheduleTaskTimeout(std::move(subtile_task), std::chrono::seconds(5));
}

void TileHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {/**/}

void TileHandler::onSuccessEOM() noexcept { }

void TileHandler::OnRenderingSuccess(Metatile&& metatile) noexcept {
    CancelTaskTimeout();
    Tile* tile_ptr = nullptr;
    for (Tile& tile : metatile.tiles) {
        if (save_to_cache_ && cacher_) {
            auto cached_tile = std::make_shared<CachedTile>(CachedTile{tile.data});
            cacher_->Set(MakeCacherKey(tile.id, request_info_), cached_tile,
                         TTLPolicyToSeconds(cached_tile->policy), nullptr);
        }
        if (!tile_ptr && tile.id == tile_id_) {
            tile_ptr = &tile;
        }
    }
    if (!tile_ptr) {
        LOG(ERROR) << "Requested tile not found in generated metatiles!";
        SendError(500);
        return;
    }
    OnProcessingSuccess(std::string(tile_ptr->data));
}

void TileHandler::OnProcessingSuccess(std::string&& tile_data) noexcept {
    proxygen::ResponseBuilder rb(downstream_);
    rb.status(200, "OK");
    rb.header("Pragma", "public");
    rb.header("Cache-Control", "max-age=86400");
    if (ext_ == ExtensionType::png) {
        rb.header("Content-Type", "image/png");
    } else if (ext_ == ExtensionType::mvt) {
        rb.header("Content-Type", "application/x-protobuf");
    } else if (ext_ == ExtensionType::json) {
        rb.header("Content-Type", "application/json");
    } else if (ext_ == ExtensionType::html) {
        rb.header("Content-Type", "text/html");
    }
    rb.header("access-control-allow-origin", "*");
    buffer_ = std::move(tile_data);
    rb.body(folly::IOBuf::wrapBuffer(buffer_.data(), buffer_.size()));
    rb.sendWithEOM();
}

void TileHandler::OnProcessingError() noexcept {
    CancelTaskTimeout();
    SendError(500);
}

void TileHandler::UnlockCache() noexcept {
    if (cacher_ && !locked_cache_keys_.empty()) {
        cacher_->Unlock(locked_cache_keys_);
    }
}

void TileHandler::OnErrorSent(std::uint16_t err_code) noexcept {
    UnlockCache();
}
