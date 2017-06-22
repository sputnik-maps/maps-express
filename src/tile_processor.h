#pragma once

#include <set>

#include "async_task.h"
#include "endpoint.h"
#include "tile.h"


class RenderManager;
class DataManager;


struct TileRequest {
    TileId tile_id;
    MetatileId metatile_id;
    std::set<std::string> tags;
    std::shared_ptr<EndpointParams> endpoint_params;
    std::unique_ptr<std::set<std::string>> layers;
    std::string data_version;
};


class TileProcessor {
public:
    enum class Error {
        internal,
        not_found,
        rendering
    };

    using TileTask = AsyncTask<Metatile&&, Error>;

    TileProcessor(RenderManager& render_manager);
    ~TileProcessor();

    void GetMetatile(std::shared_ptr<TileRequest> request, std::shared_ptr<TileTask> task);

    void CancelProcessing();

private:
    void LoadTile();
    void OnLoadSuccess(Tile&& tile);
    void ProcessRender();
    void ProcessMvt();
    void OnRenderSuccess(Metatile&& result);
    void OnRenderError();

    RenderManager& render_manager_;

    std::shared_ptr<TileTask> tile_task_;
    std::shared_ptr<TileRequest> tile_request_;
    std::shared_ptr<Tile> data_tile_;
    std::shared_ptr<AsyncTaskBase> pending_work_;
};
