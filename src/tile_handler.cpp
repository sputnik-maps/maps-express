#include "tile_handler.h"

#include <fstream>
#include <cctype>

#include <folly/io/async/EventBaseManager.h>

#include <proxygen/httpserver/ResponseBuilder.h>
#include <proxygen/lib/http/HTTPConnector.h>

#include "data_provider.h"
#include "session_wrapper.h"
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

static std::string MakeRequestInfoStr(const TileRequest& request, const std::string& ext_str) {
    std::string info_str;
    for (const std::string& tag : request.tags) {
        info_str.append(tag);
        info_str.append("/");
    }
    info_str.append(".");
    info_str.append(ext_str);
    info_str.append("/");
    info_str.append(request.data_version);
    info_str.append("/");
    info_str.append(std::to_string(request.metatile_id.width()));
    info_str.append("/");
    info_str.append(std::to_string(request.metatile_id.height()));
    info_str.append("/");
    if (request.layers) {
        info_str.append("l:");
        for (const std::string& layer_name : *request.layers) {
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

using util::ExtensionType;

static inline bool CheckParams(const TileId& tile_id, ExtensionType ext,
                               const EndpointParams& endpoint_params) noexcept {
    if (!tile_id.Valid()) return false;
    if (ext == ExtensionType::png && endpoint_params.type == EndpointType::mvt) return false;
    if (ext == ExtensionType::mvt && endpoint_params.type != EndpointType::mvt) return false;
    if (ext == ExtensionType::json && (endpoint_params.type != EndpointType::render ||
                                          !endpoint_params.allow_utf_grid)) return false;
    return true;
}

static inline bool IsInternalRequest(HTTPMessage& headers) {
    // TMP
    return headers.getDstPort() == "8081";
}

static optional<folly::SocketAddress> GetRenderNodeAddr(const NodesMonitor& monitor, const MetatileId& metatile_id) {
    auto nodes_vec = monitor.GetActiveNodes();
    if (!nodes_vec || nodes_vec->empty()) {
        return nullopt;
    }
    const TileId lt_tile_id = metatile_id.left_top();
    int i = (lt_tile_id.x + lt_tile_id.y) % nodes_vec->size();
    const NodesMonitor::addr_entry_t& addr_entry = (*nodes_vec)[i];
    if (addr_entry.second) {
        // Current node
        return nullopt;
    }
    return addr_entry.first;
}


TileHandler::TileHandler(folly::HHWheelTimer& timer,
                         std::shared_ptr<TileProcessor> tile_processor,
                         std::shared_ptr<const endpoints_map_t> endpoints,
                         std::shared_ptr<TileCacher> cacher,
                         NodesMonitor* nodes_monitor) :
        tile_processor_(std::move(tile_processor)),
        endpoints_(std::move(endpoints)),
        cacher_(std::move(cacher)),
        timer_(timer),
        nodes_monitor_(nodes_monitor) {
    assert(tile_processor_);
}

TileHandler::~TileHandler() {
    if (pending_work_) {
        pending_work_->cancel();
    }
    // TODO eliminate duplicate unlock!
    if (!locked_cache_keys_.empty()) {
        cacher_->Unlock(locked_cache_keys_);
    }
}

void TileHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    // TODO: add timeout for request!
    headers_ = std::move(headers);
    if (headers_->getMethod() != HTTPMethod::GET) {
        SendError(405);
        return;
    }

    std::vector<std::string> split_path;
    util::split(headers_->getPath(), split_path);
    const size_t num_segments = split_path.size();
    if (num_segments < 3) {
        SendError(400);
        return;
    }

    tile_request_ = std::make_shared<TileRequest>();

    endpoints_map_t::const_iterator endpoint_itr = endpoints_->end();
    if (num_segments > 3) {
        uint first_tag_pos = 0;
        if (is_version(split_path[0])) {
            tile_request_->data_version = std::move(split_path[0]);
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
            tile_request_->tags.insert(std::move(split_path[ti]));
        }
    }

    if (endpoint_itr == endpoints_->end()) {
        endpoint_itr = endpoints_->find("");
        if (endpoint_itr == endpoints_->end()) {
            SendError(404);
            return;
        }
    }

    std::string ext_str;
    uint x, y, z;
    const std::string& last_segment = split_path.back();
    size_t ext_pos = last_segment.find('.');
    if (ext_pos != std::string::npos) {
        if (ext_pos + 1 > last_segment.size()) {
            SendError(400);
            return;
        }
        ext_str = last_segment.substr(ext_pos + 1);
        y = std::atoi(last_segment.data());
    } else {
        y = std::atoi(last_segment.data());
    }
    x = std::atoi(split_path[num_segments-2].data());
    z = std::atoi(split_path[num_segments-3].data());

    tile_request_->tile_id = TileId(x, y, z);
    const TileId& tile_id = tile_request_->tile_id;

    ext_ = util::str2ext(ext_str);
    if (ext_ == ExtensionType::none) {
        SendError(404);
        return;
    }

    for (const auto& ep : endpoint_itr->second) {
        if (ep->minzoom <= tile_id.z && ep->maxzoom >= tile_id.z) {
            tile_request_->endpoint_params = ep;
            break;
        }
    }
    if (!tile_request_->endpoint_params) {
        SendError(404);
        return;
    }
    const EndpointParams& endpoint_params = *tile_request_->endpoint_params;

    if (!CheckParams(tile_id, ext_, endpoint_params)) {
        SendError(400);
        return;
    }

    if (endpoint_params.allow_layers_query) {
        const std::string& requested_layers_param = headers_->getQueryParam("layers");
        if (!requested_layers_param.empty()) {
            tile_request_->layers = util::ParseArray(requested_layers_param);
        }
    }

    if (endpoint_params.auto_metatile_size) {
        if (!endpoint_params.data_provider) {
            LOG(ERROR) << "Endpoint configured to use auto metatile size, but data provider missing!";
            SendError(500);
            return;
        }
        auto metatile_id = endpoint_params.data_provider->GetOptimalMetatileId(
                    tile_id, endpoint_params.zoom_offset);
        if (!metatile_id) {
            LOG(ERROR) << "Error while computing optimal metatile id!";
            SendError(500);
            return;
        }
        tile_request_->metatile_id = *metatile_id;
    } else {
        tile_request_->metatile_id = MetatileId(tile_id, endpoint_params.metatile_width,
                                                endpoint_params.metatile_height);
    }

    if (cacher_) {
        request_info_str_ = MakeRequestInfoStr(*tile_request_, ext_str);
        is_internal_request_ = IsInternalRequest(*headers_);
        TryLoadFromCache();
    } else {
        GenerateTile();
    }
}

void TileHandler::TryLoadFromCache() noexcept {
    std::string key = MakeCacherKey(tile_request_->tile_id, request_info_str_);
    auto cacher_task = std::make_shared<TileCacher::GetTask>([this, key](std::shared_ptr<const CachedTile> tile) {
        pending_work_.reset();
        if (tile) {
            cacher_->Touch(key, TTLPolicyToSeconds(tile->policy));
            SendResponse(tile->data);
        } else {
            if (is_internal_request_ || !nodes_monitor_ ) {
                LockCacheAndGenerateTile();
                return;
            }
            auto render_addr = GetRenderNodeAddr(*nodes_monitor_, tile_request_->metatile_id);
            if (render_addr) {
                // Redirect to other render node
                ProxyToOtherNode(*render_addr);
            } else {
                LockCacheAndGenerateTile();
            }
        }
    }, [this]{
        pending_work_.reset();
        SendError(500);
    }, true);
    pending_work_ = cacher_task;
    cacher_->Get(key, std::move(cacher_task));
}


void TileHandler::GenerateTile() noexcept {
    assert(tile_request_);
    auto tile_task = std::make_shared<TileProcessor::TileTask>([this](Metatile&& metatile) {
        pending_work_.reset();
        for (Tile& tile : metatile.tiles) {
            if (tile.id == tile_request_->tile_id) {
                SendResponse(std::move(tile.data));
                return;
            }
        }
        SendError(500);
    }, [this](TileProcessor::Error err) {
        pending_work_.reset();
        if (err == TileProcessor::Error::not_found) {
            SendError(404);
        } else {
            SendError(500);
        }
    }, true);

    pending_work_ = tile_task;
    tile_processor_->GetMetatile(tile_request_, std::move(tile_task));
}

void TileHandler::LoadFromCacheOrError() {
    std::string key = MakeCacherKey(tile_request_->tile_id, request_info_str_);
    auto cacher_task = std::make_shared<TileCacher::GetTask>([this, key](std::shared_ptr<const CachedTile> tile) {
        pending_work_.reset();
        if (tile) {
            cacher_->Touch(key, TTLPolicyToSeconds(tile->policy));
            SendResponse(tile->data);
        } else {
            SendError(500);
        }
    }, [this]{
        pending_work_.reset();
        SendError(500);
    }, true);
    pending_work_ = cacher_task;
    cacher_->Get(key, std::move(cacher_task));
}

void TileHandler::LockCacheAndGenerateTile() {
    assert(tile_request_);
    assert(cacher_);
    assert(!request_info_str_.empty());
    const auto tiles_ids = tile_request_->metatile_id.TileIds();
    for (const TileId& tile_id: tiles_ids) {
        locked_cache_keys_.push_back(MakeCacherKey(tile_id, request_info_str_));
    }
    if (!cacher_->LockUntilSet(locked_cache_keys_)) {
        // If rendering already started on other thread,
        // wait until tiles will be set to cache or report error
        locked_cache_keys_.clear();
        LoadFromCacheOrError();
        return;
    }

    // responce_task will be cancelled in case of connection timeout,
    // but tile_task will continue execution
    auto responce_task = std::make_shared<AsyncTask<std::string, TileProcessor::Error>>([this](std::string tile_data) {
        pending_work_.reset();
        locked_cache_keys_.clear();
        if (tile_data.empty()) {
            SendError(500);
        } else {
            SendResponse(std::move(tile_data));
        }
    }, [this](TileProcessor::Error err) {
        pending_work_.reset();
        cacher_->Unlock(locked_cache_keys_);
        locked_cache_keys_.clear();
        if (err == TileProcessor::Error::not_found) {
            SendError(404);
        } else {
            SendError(500);
        }
    }, true);
    pending_work_ = responce_task;

    auto tile_task = std::make_shared<TileProcessor::TileTask>(
            [responce_task, cacher = cacher_, request_info_str = request_info_str_, tile_id = tile_request_->tile_id]
                (Metatile&& metatile) {
        bool response_sent = false;
        for (Tile& tile : metatile.tiles) {
            if (!response_sent && tile.id == tile_id) {
                responce_task->SetResult(tile.data);
            }
            // TODO: Calculate cache policy
            auto cached_tile = std::make_shared<CachedTile>(CachedTile{std::move(tile.data)});
            cacher->Set(MakeCacherKey(tile.id, request_info_str), cached_tile,
                         TTLPolicyToSeconds(cached_tile->policy), nullptr);
        }
        if (!response_sent) {
            responce_task->SetResult("");
        }
    }, [responce_task](TileProcessor::Error err) {
        responce_task->NotifyError(err);
    }, false);

    tile_processor_->GetMetatile(tile_request_, std::move(tile_task));
}

void TileHandler::ProxyToOtherNode(const folly::SocketAddress& addr) noexcept {
    assert(headers_);
    proxy_handler_ = std::make_unique<ProxyHandler>(*this, timer_, addr, std::move(headers_), *downstream_);
}


void TileHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {/**/}

void TileHandler::onSuccessEOM() noexcept { }

void TileHandler::SendResponse(std::string tile_data) noexcept {
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

void TileHandler::OnProxyEom() noexcept {}

void TileHandler::OnProxyError() noexcept {
    SendError(500);
}
