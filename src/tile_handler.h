#pragma once

#include <mapnik/config.hpp>

#include <folly/Memory.h>

#include "async_task_handler.h"
#include "data_manager.h"
#include "endpoint.h"
#include "rendermanager.h"
#include "status_monitor.h"
#include "tile.h"
#include "tile_cacher.h"
#include "util.h"

class TileHandler : public AsyncTaskHandler {
public:
    using endpoint_t =  std::vector<std::shared_ptr<EndpointParams>>;
    using endpoints_map_t = std::unordered_map<std::string, endpoint_t>;

    explicit TileHandler(RenderManager& render_manager,
                         DataManager& datd_manager,
                         std::shared_ptr<const endpoints_map_t> endpoints,
                         TileCacher* cacher = nullptr);

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;
    void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;
    void onSuccessEOM() noexcept override;

    void OnCachedTileLoaded(std::shared_ptr<CachedTile> tile) noexcept;
    void OnCacherError() noexcept;

    void OnLoadSuccess(Tile&& tile) noexcept;
    void OnLoadError(LoadError err) noexcept;

    void OnRenderingSuccess(Metatile&& metatile) noexcept;

    void OnProcessingSuccess(std::string&& tile_data) noexcept;
    void OnProcessingError() noexcept;

private:
    using ExtensionType = util::ExtensionType;

    virtual void OnErrorSent(std::uint16_t err_code) noexcept override;

    bool CheckParams() noexcept;
    std::experimental::optional<MetatileId> GetMetatileId() noexcept;
    void LoadFromCacheOrGenerate() noexcept;
    void LoadFromCacheOrError(const std::string& key) noexcept;
    void GenerateTile() noexcept;
    void LoadTile() noexcept;
    void ProcessRender() noexcept;
    void ProcessMvt() noexcept;
    void UnlockCache() noexcept;

    RenderManager& rm_;
    DataManager& dm_;
    std::unique_ptr<folly::IOBuf> response_body_;
    std::shared_ptr<const endpoints_map_t> endpoints_;
    TileCacher* cacher_{nullptr};

    std::vector<std::string> locked_cache_keys_;
    TileId tile_id_;
    std::set<std::string> tags_;
    std::experimental::optional<MetatileId> metatile_id_;
    std::shared_ptr<EndpointParams> endpoint_params_;
    std::shared_ptr<DataProvider> data_provider_;
    std::shared_ptr<Tile> data_tile_;
    std::unique_ptr<std::set<std::string>> layers_;
    std::string data_version_;
    std::string buffer_;
    std::string request_info_;
    ExtensionType ext_{ExtensionType::none};
    bool save_to_cache_{false};
};
