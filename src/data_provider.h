#pragma once

#include <experimental/optional>
#include <memory>
#include <set>

#include "tile_loader.h"
#include "tile.h"

class DataProvider {
public:
    using zoom_groups_t = std::set<uint>;
    using success_cb_t = TileLoader::LoadTask::result_cb_t;
    using error_cb_t = TileLoader::LoadTask::error_cb_t;

    DataProvider(std::shared_ptr<TileLoader> loader, uint min_zoom, uint max_zoom,
                 std::shared_ptr<zoom_groups_t> zoom_groups = nullptr);

    std::shared_ptr<TileLoader::LoadTask> GetTile(success_cb_t success_cb, error_cb_t error_cb,
                                                  const TileId& tile_id, uint zoom_offset = 0,
                                                  const std::string& version = "");

    void GetTile(std::shared_ptr<TileLoader::LoadTask> task, const TileId& tile_id,
                 uint zoom_offset = 0, const std::string& version = "");

    std::experimental::optional<MetatileId> GetOptimalMetatileId(const TileId& tile_id, uint zoom_offset = 0);

    inline bool HasVersion(const std::string& version) const {
        return loader_->HasVersion(version);
    }

private:
    std::experimental::optional<uint> GetBaseZoom(uint tile_zoom, uint zoom_offset);
    std::experimental::optional<TileId> CalculateBaseTileId(const TileId& tile_id, uint zoom_offset);

    std::shared_ptr<TileLoader> loader_;
    std::shared_ptr<zoom_groups_t> zoom_groups_;
    uint min_zoom_;
    uint max_zoom_;
};
