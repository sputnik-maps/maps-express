#include "tile.h"

#include <cassert>
#include <ostream>

#include <mapnik/box2d.hpp>
#include <mapnik/well_known_srs.hpp>

#include <vector_tile_projection.hpp>


std::ostream& operator<<(std::ostream& os, const TileId& tile_id) {
    return os << "x: " << tile_id.x << " y: " << tile_id.y << " z: " << tile_id.z;
}

std::ostream& operator<<(std::ostream& os, const MetatileId& metatile_id) {
    return os << "left top: " << metatile_id.left_top() << " width: " << metatile_id.width()
              << " height: " << metatile_id.height();
}

void MetatileId::FromTileId(const TileId& id, uint width, uint height) {
    assert(id.Valid());
    uint zoom_size = std::pow(2u, id.z);

    uint x;
    if (width <= 1) {
        x = id.x;
        width_ = width;
    } else if (width > zoom_size) {
       x = 0;
       width_ = zoom_size;
    } else {
        x = id.x - id.x % width;
        uint dx = zoom_size - x;
        width_ = dx < width ? dx : width;
    }

    uint y;
    if (height <= 1) {
        y = id.y;
        height_ = height;
    } else if (height > zoom_size) {
       y = 0;
       height_ = zoom_size;
    } else {
        y = id.y - id.y % height;
        uint dy = zoom_size - y;
        height_ = dy < height ? dy : height;
    }

    lt_tile_ = TileId{x, y, id.z};
}


mapnik::box2d<double> MetatileId::GetBbox() const {
    const uint tile_size = 256;
    mapnik::vector_tile_impl::spherical_mercator merc(tile_size);
    double minx = lt_tile_.x * tile_size;
    double miny = (lt_tile_.y + height_) * tile_size;
    double maxx = (lt_tile_.x + width_) * tile_size;
    double maxy = lt_tile_.y * tile_size;
    double shift = std::pow(2.0, lt_tile_.z) * tile_size;
    merc.from_pixels(shift,minx,miny);
    merc.from_pixels(shift,maxx,maxy);
    mapnik::lonlat2merc(&minx,&miny,1);
    mapnik::lonlat2merc(&maxx,&maxy,1);
    return mapnik::box2d<double>(minx, miny, maxx, maxy);
}

bool Metatile::Validate() const noexcept {
    std::size_t num_tiles = tiles.size();
    if ((id.width() * id.height()) != num_tiles) {
        return false;
    }
    std::vector<TileId> ids = id.TileIds();
    assert(ids.size() == num_tiles);
    std::size_t i = 0;
    for (const TileId& tile_id : ids) {
        if (tile_id != tiles[i++].id) {
            return false;
        }
    }
    return true;
}
