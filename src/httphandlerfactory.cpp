#include "httphandlerfactory.h"

#include "couchbase_cacher.h"
#include "json_util.h"
#include "mon_handler.h"
#include "tile_handler.h"
#include "util.h"


using json_util::FromJson;
using json_util::FromJsonOrErr;

using endpoint_t = HttpHandlerFactory::endpoint_t;
using endpoints_map_t = std::unordered_map<std::string, endpoint_t>;


class ServerUpdateObserver : public Config::ConfigObserver {
public:
    ServerUpdateObserver(HttpHandlerFactory& hhf) : hhf_(hhf) {}

private:
    void OnUpdate(std::shared_ptr<Json::Value> value) override {
        hhf_.UpdateConfig(std::move(value));
    }

    HttpHandlerFactory& hhf_;
};

static FilterTable::zoom_groups_t MakeZoomGroups(uint min_z, uint max_z) {
    FilterTable::zoom_groups_t zoom_groups;
    for (uint i = min_z; i <= max_z; ++i) {
        zoom_groups.insert(i);
    }
    return zoom_groups;
}

static std::shared_ptr<endpoints_map_t> ParseEndpoints(const Json::Value jendpoints,
                                                       DataManager& data_manager) {
    if (!jendpoints.isObject()) {
        return nullptr;
    }

    auto endpoints_map = std::make_shared<endpoints_map_t>(jendpoints.size());
    for (auto itr = jendpoints.begin() ; itr != jendpoints.end() ; ++itr) {
        const std::string endpoint_path = itr.key().asString();
        if (endpoints_map->find(endpoint_path) != endpoints_map->end()) {
            LOG(ERROR) << "Duplicate endpoint path: " << endpoint_path;
            continue;
        }
        endpoint_t endpoint;
        const Json::Value& jendpoint = *itr;
        for (const auto& jparams : jendpoint) {
            auto params = std::make_shared<EndpointParams>();
            params->minzoom = FromJson<int>(jparams["minzoom"], 0);
            params->maxzoom = FromJson<int>(jparams["maxzoom"], 19);
            int zoom_offset = FromJson<int>(jparams["data_zoom_offset"], 0);
            if (zoom_offset > 0) {
                LOG(ERROR) << "\"data_zoom_offset must be negative o zero";
                LOG(ERROR) << "Skipping endpoint \"" << endpoint_path << '"';
                continue;
            }
            params->zoom_offset = static_cast<uint>(-zoom_offset);
            std::string provider_name = FromJson<std::string>(jparams["data_provider"], "");
            if (!provider_name.empty()) {
                auto data_provider = data_manager.GetProvider(provider_name);
                if (!data_provider) {
                    LOG(ERROR) << "Data provider \"" << provider_name << "\" for endpoint \""
                               << endpoint_path << "\" not found!";
                    LOG(ERROR) << "Skipping endpoint \"" << endpoint_path << '"';
                    continue;
                }
                params->data_provider = std::move(data_provider);
            }
            params->style_name = FromJson<std::string>(jparams["style"], "");
            params->allow_layers_query = FromJson<bool>(jparams["allow_layers_query"], false);
            std::string type = FromJson<std::string>(jparams["type"], "static");
            if (type == "static") {
                params->type = EndpointType::static_files;
                if (!params->data_provider) {
                    LOG(ERROR) << "No data provider for endpoint '" << endpoint_path << "' specified!";
                    LOG(ERROR) << "Skipping endpoint \"" << endpoint_path << '"';
                    continue;
                }
            } else if (type == "render") {
                params->type = EndpointType::render;
                params->allow_utf_grid = FromJson<bool>(jparams["allow_utfgrid"], false);
                params->utfgrid_key = FromJson<std::string>(jparams["utfgrid_key"], "");
                if (params->allow_utf_grid && params->utfgrid_key.empty()) {
                    LOG(ERROR) << "No utfgrid key for endpoint '" << endpoint_path << "' provided!";
                    params->allow_utf_grid = false;
                }
                if (params->style_name.empty()) {
                    LOG(ERROR) << "No style name for endpoint '" << endpoint_path << "' provided!";
                    LOG(ERROR) << "Skipping endpoint \"" << endpoint_path << '"';
                    continue;
                }
            } else if (type == "mvt") {
                params->type = EndpointType::mvt;
                if (!params->data_provider) {
                    LOG(ERROR) << "No data provider for endpoint '" << endpoint_path << "' specified!";
                    LOG(ERROR) << "Skipping endpoint \"" << endpoint_path << '"';
                    continue;
                }
                auto filter_map_path = FromJson<std::string>(jparams["filter_map"]);
                if (filter_map_path) {
                    uint last_zoom = FromJson<uint>(jparams["last_zoom"], params->maxzoom + 1);
                    FilterTable::zoom_groups_t zoom_groups = MakeZoomGroups(params->minzoom, params->maxzoom);
                    params->filter_table = FilterTable::MakeFilterTable(*filter_map_path, &zoom_groups,
                                                                        1, params->minzoom, last_zoom);
                }
            } else {
                LOG(ERROR) << "Invalid type '" << type << "' for endpoint '" << endpoint_path << "' provided!";
                continue;
            }
            const Json::Value& jmetatile_size = jparams["metatile_size"];
            if (jmetatile_size.isString()) {
                if (jmetatile_size.asString() == "auto") {
                    if (!params->data_provider) {
                        LOG(ERROR) << "Auto metatile size can be used only with data provider!";
                    } else {
                        params->auto_metatile_size = true;
                    }
                }
            } else if (jmetatile_size.isUInt()) {
                uint metatile_size = jmetatile_size.asUInt();
                params->metatile_height = metatile_size;
                params->metatile_width = metatile_size;
            } else {
                params->metatile_height = FromJson<uint>(jparams["metatile_height"], 1);
                params->metatile_width = FromJson<uint>(jparams["metatile_width"], 1);
            }
            endpoint.push_back(std::move(params));
        }
        (*endpoints_map)[endpoint_path] = std::move(endpoint);
    }
    return endpoints_map;
}

HttpHandlerFactory::HttpHandlerFactory(Config& config, std::shared_ptr<StatusMonitor> monitor,
                                       std::string internal_port, NodesMonitor* nodes_monitor) :
        monitor_(std::move(monitor)),
        render_manager_(config),
        data_manager_(config),
        internal_port_(std::move(internal_port)),
        config_(config),
        nodes_monitor_(nodes_monitor)
{
    update_observer_ = std::make_unique<ServerUpdateObserver>(*this);
    std::shared_ptr<const Json::Value> jserver_ptr = config.GetValue("server", update_observer_.get());
    assert(jserver_ptr);
    const Json::Value& jserver = *jserver_ptr;

    const Json::Value& jendpoints = jserver["endpoints"];
    auto endpoints_ptr = ParseEndpoints(jendpoints, data_manager_);
    if (!endpoints_ptr || endpoints_ptr->empty()) {
        LOG(WARNING) << "No endpoints provided";
    }
    std::atomic_store(&endpoints_, endpoints_ptr);

    auto jcacher_ptr = config.GetValue("cacher");
    if (jcacher_ptr) {
        const Json::Value& jcacher = *jcacher_ptr;
        const Json::Value& jconn_str= jcacher["conn_str"];
        if (!jconn_str.isString()) {
            LOG(FATAL) << "No connection string for Couchbase provided!";
        }
        std::string conn_str = jconn_str.asString();
        std::string user = FromJson<std::string>(jcacher["user"], "");
        std::string password = FromJson<std::string>(jcacher["password"], "");
        uint num_workers = FromJson<uint>(jcacher["workers"], 2);
        auto cb_cacher = std::make_shared<CouchbaseCacher>(conn_str, user, password, num_workers);
        cb_cacher->WaitForInit();
        cacher_ = std::move(cb_cacher);
    }
    if (!cacher_) {
        LOG(INFO) << "Starting without cacher";
    }
    render_manager_.WaitForInit();
}

HttpHandlerFactory::~HttpHandlerFactory() {}

void HttpHandlerFactory::onServerStart(folly::EventBase* evb) noexcept {
    timer_->timer = folly::HHWheelTimer::newTimer(
                evb,
                std::chrono::milliseconds(folly::HHWheelTimer::DEFAULT_TICK_INTERVAL),
                folly::AsyncTimeout::InternalEnum::NORMAL,
                std::chrono::seconds(60));
}

void HttpHandlerFactory::onServerStop() noexcept {
    if (nodes_monitor_) {
        nodes_monitor_->Unregister();
    }
    timer_->timer.reset();
}

proxygen::RequestHandler* HttpHandlerFactory::onRequest(proxygen::RequestHandler*,
                                                        proxygen::HTTPMessage* msg) noexcept {
    const std::string& path = msg->getPath();
//    if (path.back() != '/') {
//        path.push_back('/');
//    }
    const auto method = msg->getMethod();
    using proxygen::HTTPMethod;
    if (method == HTTPMethod::GET && path == "/mon") {
        return new MonHandler(monitor_);
    }
    auto endpoints = std::atomic_load(&endpoints_);
    return new TileHandler(internal_port_, *timer_->timer, render_manager_,
                           endpoints, cacher_, nodes_monitor_);
}


bool HttpHandlerFactory::UpdateConfig(std::shared_ptr<Json::Value> update) {
    auto endpoints_map = ParseEndpoints((*update)["endpoints"], data_manager_);
    if (!endpoints_map) {
        return false;
    }
    std::atomic_store(&endpoints_, endpoints_map);
    return true;
}
