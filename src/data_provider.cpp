#include "data_provider.h"


using std::experimental::optional;
using std::experimental::nullopt;

DataProvider::DataProvider(std::shared_ptr<TileLoader> loader, uint min_zoom, uint max_zoom,
                           std::shared_ptr<zoom_groups_t> zoom_groups) :
        loader_(std::move(loader)),
        zoom_groups_(std::move(zoom_groups)),
        min_zoom_(min_zoom),
        max_zoom_(max_zoom) {
    assert(loader_);
}

std::shared_ptr<LoadTask> DataProvider::GetTile(success_cb_t success_cb, error_cb_t error_cb,
                                                const TileId& tile_id, const std::string& version) {
    auto task = std::make_shared<LoadTask>(std::move(success_cb), std::move(error_cb), true);
    GetTile(task, tile_id, version);
    return task;
}

void DataProvider::GetTile(std::shared_ptr<LoadTask> task, const TileId& tile_id, const std::string& version) {
    auto base_tile = CalculateBaseTileId(tile_id);
    if (!base_tile) {
        task->NotifyError(LoadError::internal_error);
        return;
    }
    if (!loader_->HasVersion(version)) {
        task->NotifyError(LoadError::not_found);
        return;
    }
    loader_->Load(std::move(task), *base_tile, version);
}

optional<MetatileId> DataProvider::GetOptimalMetatileId(const TileId& tile_id, int zoom_offset) {
    assert(tile_id.Valid());
    int tile_zoom = static_cast<int>(tile_id.z);
    uint offseted_zoom = static_cast<uint>(tile_zoom >= zoom_offset ? tile_zoom + zoom_offset : 0);
    if (offseted_zoom < min_zoom_ || offseted_zoom > max_zoom_) {
        return nullopt;
    }
    if (!zoom_groups_ || zoom_groups_->empty()) {
        return MetatileId(tile_id, 1);
    }

    auto base_z_itr = zoom_groups_->lower_bound(offseted_zoom);
    if (base_z_itr == zoom_groups_->end()) {
        return nullopt;
    }
    uint base_z = *base_z_itr;
    uint dz = tile_id.z - base_z;
    // Limit metatile size to 8
    if (dz > 3) {
        dz = 3;
    }
    uint metatile_size = std::pow(2u, dz);
    return MetatileId(tile_id, metatile_size);
}

optional<TileId> DataProvider::CalculateBaseTileId(const TileId& tile_id) {
    assert(tile_id.Valid());
    uint zoom = tile_id.z;
    if (zoom < min_zoom_ || zoom > max_zoom_) {
        return nullopt;
    }

    if (!zoom_groups_ || zoom_groups_->empty()) {
        return tile_id;
    }
    auto base_z_itr = zoom_groups_->lower_bound(zoom);
    if (base_z_itr == zoom_groups_->end()) {
        return nullopt;
    }
    uint base_z = *base_z_itr;
    int coords_divider = pow(2, zoom - base_z);
    uint base_x = tile_id.x / coords_divider;
    uint base_y = tile_id.y / coords_divider;
    return TileId{base_x, base_y, base_z};

}
