#pragma once

#include <folly/ThreadLocal.h>

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
                                std::string internal_port,
                                NodesMonitor* nodes_monitor = nullptr);
    ~HttpHandlerFactory();

    void onServerStart(folly::EventBase* evb) noexcept override;

    void onServerStop() noexcept override;

    proxygen::RequestHandler* onRequest(proxygen::RequestHandler*, proxygen::HTTPMessage*) noexcept override;

    bool UpdateConfig(std::shared_ptr<Json::Value> update);

    using endpoint_t = std::vector<std::shared_ptr<EndpointParams>>;

private:
    using endpoints_map_t = std::unordered_map<std::string, endpoint_t>;
    struct TimerWrapper {
        folly::HHWheelTimer::UniquePtr timer;
    };

    std::shared_ptr<StatusMonitor> monitor_;
    RenderManager render_manager_;
    DataManager data_manager_;
    std::shared_ptr<endpoints_map_t> endpoints_;
    std::shared_ptr<TileCacher> cacher_;
    std::unique_ptr<ServerUpdateObserver> update_observer_;
    folly::ThreadLocal<TimerWrapper> timer_;
    std::string internal_port_;
    Config& config_;
    NodesMonitor* nodes_monitor_{nullptr};
    bool allow_style_updates_{false};
};
