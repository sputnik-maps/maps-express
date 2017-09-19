#pragma once

#include <list>
#include <mutex>
#include <set>

#include "async_task.h"
#include "endpoint.h"
#include "tile.h"


struct TileRequest {
    TileId tile_id;
    MetatileId metatile_id;
    std::set<std::string> tags;
    std::shared_ptr<EndpointParams> endpoint_params;
    std::unique_ptr<std::set<std::string>> layers;
    std::string data_version;
};


class RenderManager;
class TileProcessor;

class TileProcessingManager {
public:

    enum class Error {
        internal,
        not_found,
        rendering,
        processors_limit
    };

    using TileTask = AsyncTask<Metatile&&, Error>;
    using processors_store_t = std::list<std::unique_ptr<TileProcessor>>;

    TileProcessingManager(RenderManager& render_manager, uint max_processors, uint unlock_threshold);
    ~TileProcessingManager();

    bool GetMetatile(std::shared_ptr<TileRequest> request, std::shared_ptr<TileTask> task);

    void NotifyDone(TileProcessor& processor);

    inline RenderManager& render_manager() const noexcept {
        return render_manager_;
    }

private:
    processors_store_t processors_;
    std::mutex mux_;
    RenderManager& render_manager_;
    uint max_processors_;
    uint unlock_threshold_;
    uint num_processors_{0};
    bool locked_{false};
};
