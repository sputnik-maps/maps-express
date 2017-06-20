#pragma once

#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include <proxygen/httpserver/RequestHandler.h>

#include "config.h"
#include "data_manager.h"
#include "endpoint.h"
#include "nodes_monitor.h"
#include "status_monitor.h"
#include "rendermanager.h"
#include "tile_cacher.h"


class ServerUpdateObserver;

class HttpHandlerFactory : public proxygen::RequestHandlerFactory {
public:
    explicit HttpHandlerFactory(Config& config, std::shared_ptr<StatusMonitor> monitor,
                                NodesMonitor* nodes_monitor = nullptr);
    ~HttpHandlerFactory();

    void onServerStart(folly::EventBase* evb) noexcept override;

    void onServerStop() noexcept override;

    proxygen::RequestHandler* onRequest(proxygen::RequestHandler*, proxygen::HTTPMessage*) noexcept override;

    bool UpdateConfig(std::shared_ptr<Json::Value> update);

    using endpoint_t = std::vector<std::shared_ptr<EndpointParams>>;

private:
    std::shared_ptr<StatusMonitor> monitor_;
    RenderManager render_manager_;
    DataManager data_manager_;
    using endpoints_map_t = std::unordered_map<std::string, endpoint_t>;
    std::shared_ptr<endpoints_map_t> endpoints_;
    std::unique_ptr<ServerUpdateObserver> update_observer_;
    std::unique_ptr<TileCacher> cacher_;
    Config& config_;
    NodesMonitor* nodes_monitor_{nullptr};
    bool allow_style_updates_{false};
};
