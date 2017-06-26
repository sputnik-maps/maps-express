#pragma once

#include <mapnik/config.hpp>

#include <folly/io/IOBuf.h>

#include "async_task_handler.h"
#include "endpoint.h"
#include "nodes_monitor.h"
#include "proxy_handler.h"
#include "status_monitor.h"
#include "tile.h"
#include "tile_cacher.h"
#include "tile_processor.h"
#include "util.h"

class ProxyHandler;

class TileHandler : public AsyncTaskHandler, public ProxyHandler::Callbacks {
public:
    using endpoint_t =  std::vector<std::shared_ptr<EndpointParams>>;
    using endpoints_map_t = std::unordered_map<std::string, endpoint_t>;

    explicit TileHandler(const std::string& internal_port,
                         folly::HHWheelTimer& timer,
                         std::shared_ptr<TileProcessor> tile_processor,
                         std::shared_ptr<const endpoints_map_t> endpoints,
                         std::shared_ptr<TileCacher> cacher = nullptr,
                         NodesMonitor* nodes_monitor = nullptr);

    ~TileHandler();

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;
    void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;
    void onSuccessEOM() noexcept override;

    void OnProxyEom() noexcept override;
    void OnProxyError() noexcept override;

private:
    void TryLoadFromCache() noexcept;
    void ProxyToOtherNode(const folly::SocketAddress& addr) noexcept;
    void GenerateTile() noexcept;
    void LockCacheAndGenerateTile();
    void LoadFromCacheOrError();
    void SendResponse(std::string tile_data) noexcept;

    std::shared_ptr<TileProcessor> tile_processor_;
    std::shared_ptr<const endpoints_map_t> endpoints_;
    std::shared_ptr<TileCacher> cacher_;
    std::unique_ptr<proxygen::HTTPMessage> headers_;
    std::unique_ptr<ProxyHandler> proxy_handler_;
    folly::HHWheelTimer& timer_;
    NodesMonitor* nodes_monitor_{nullptr};


    std::vector<std::string> locked_cache_keys_;
    std::shared_ptr<TileRequest> tile_request_;
    std::shared_ptr<AsyncTaskBase> pending_work_;
    std::string request_info_str_;
    std::string buffer_;
    std::string internal_port_;
    util::ExtensionType ext_{util::ExtensionType::none};
    bool save_to_cache_{false};
    bool is_internal_request_{false};

    friend class ProxyHandler;
};
