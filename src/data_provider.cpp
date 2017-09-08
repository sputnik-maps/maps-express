#include "data_provider.h"

#include <glog/logging.h>


using std::experimental::optional;
using std::experimental::nullopt;
using LoadError = TileLoader::LoadError;

DataProvider::DataProvider(std::shared_ptr<TileLoader> loader, uint min_zoom, uint max_zoom,
                           std::shared_ptr<zoom_groups_t> zoom_groups) :
        loader_(std::move(loader)),
        zoom_groups_(std::move(zoom_groups)),
        min_zoom_(min_zoom),
        max_zoom_(max_zoom) {
    assert(loader_);
    if (zoom_groups && !zoom_groups->empty()) {
        min_zoom = *zoom_groups->begin();
        assert(min_zoom <= max_zoom);
        assert(max_zoom >= *zoom_groups->rbegin());
    }
}

std::shared_ptr<TileLoader::LoadTask> DataProvider::GetTile(success_cb_t success_cb, error_cb_t error_cb,
                                                            const TileId& tile_id, uint zoom_offset,
                                                            const std::string& version) {
    auto task = std::make_shared<TileLoader::LoadTask>(std::move(success_cb), std::move(error_cb), true);
    GetTile(task, tile_id, zoom_offset, version);
    return task;
}

void DataProvider::GetTile(std::shared_ptr<TileLoader::LoadTask> task, const TileId& tile_id,
                           uint zoom_offset, const std::string& version) {
    auto base_tile = CalculateBaseTileId(tile_id, zoom_offset);
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

static uint FindZoomGroup(const DataProvider::zoom_groups_t& zoom_groups, uint zoom) {
    assert(!zoom_groups.empty());
    assert(zoom >= *zoom_groups.begin());
    uint prev_z = 0;
    for (uint z : zoom_groups) {
        if (z == zoom) {
            return z;
        }
        if (z > zoom) {
            return prev_z;
        }
        prev_z = z;
    }
    return prev_z;
}

inline optional<uint> DataProvider::GetBaseZoom(uint tile_zoom, uint zoom_offset) {
    uint offseted_zoom = tile_zoom > zoom_offset ? tile_zoom - zoom_offset : 0;
    if (offseted_zoom < min_zoom_ || offseted_zoom > max_zoom_) {
        LOG(ERROR) << "Offseted zoom " << offseted_zoom
                   << " out of bounds [" << min_zoom_ << ", " << max_zoom_ << "]";
        return nullopt;
    }
    if (zoom_groups_ && !zoom_groups_->empty()) {
        return FindZoomGroup(*zoom_groups_, offseted_zoom);
    }
    return offseted_zoom;
}

optional<MetatileId> DataProvider::GetOptimalMetatileId(const TileId& tile_id, uint zoom_offset) {
    assert(tile_id.Valid());
    if (tile_id.z == min_zoom_) {
        return MetatileId(tile_id, 1);
    }
    auto base_zoom = GetBaseZoom(tile_id.z, zoom_offset);
    if (!base_zoom) {
        return nullopt;
    }

    uint dz = tile_id.z - *base_zoom;

    // Limit metatile size to 8
    if (dz > 3) {
        dz = 3;
    }
    uint metatile_size = std::pow(2u, dz);
    return MetatileId(tile_id, metatile_size);
}

optional<TileId> DataProvider::CalculateBaseTileId(const TileId& tile_id, uint zoom_offset) {
    assert(tile_id.Valid());
    auto base_z = GetBaseZoom(tile_id.z, zoom_offset);
    if (!base_z) {
        return nullopt;
    }
    int coords_divider = pow(2, tile_id.z - *base_z);
    uint base_x = tile_id.x / coords_divider;
    uint base_y = tile_id.y / coords_divider;
    return TileId{base_x, base_y, *base_z};

}
