#pragma once

#include <experimental/optional>
#include <memory>
#include <set>

#include "tile_loader.h"
#include "tile.h"

class DataProvider {
public:
    using zoom_groups_t = std::set<uint, std::greater<uint>>;
    using success_cb_t = LoadTask::result_cb_t;
    using error_cb_t = LoadTask::error_cb_t;

    DataProvider(std::shared_ptr<TileLoader> loader, uint min_zoom, uint max_zoom,
                 std::shared_ptr<zoom_groups_t> zoom_groups = nullptr);

    std::shared_ptr<LoadTask> GetTile(success_cb_t success_cb, error_cb_t error_cb, const TileId& tile_id,
                                      const std::string& version = "");

    void GetTile(std::shared_ptr<LoadTask> task, const TileId& tile_id, const std::string& version = "");

    std::experimental::optional<MetatileId> GetOptimalMetatileId(const TileId& tile_id, int zoom_offset = 0);

    inline bool HasVersion(const std::string& version) const {
        return loader_->HasVersion(version);
    }

private:

    std::experimental::optional<TileId> CalculateBaseTileId(const TileId& tile_id);

    std::shared_ptr<TileLoader> loader_;
    std::shared_ptr<zoom_groups_t> zoom_groups_;
    uint min_zoom_;
    uint max_zoom_;
};
