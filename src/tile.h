#pragma once

#include <cmath>
#include <string>
#include <vector>


namespace mapnik {
template <typename T>
class box2d;
} // ns mapnik


struct TileId {
    TileId() = default;

    TileId(uint _x, uint _y, uint _z) : x(_x), y(_y), z(_z) {}

    inline bool Valid() const noexcept {
        uint max_coord = static_cast<uint>(std::pow(2, z));
        return x < max_coord && y < max_coord;
    }

    uint x{0};
    uint y{0};
    uint z{0};
};


inline bool operator==(const TileId& x, const TileId& y) {
    return x.x == y.x && x.y == y.y && x.z == y.z;
}

inline bool operator!=(const TileId& x, const TileId& y) {
    return !(x == y);
}

std::ostream& operator<<(std::ostream& os, const TileId& tile_id);


inline TileId GetUpperZoom(const TileId& tile_id, uint dz = 1) {
    if (dz == 1) {
        return TileId{tile_id.x / 2, tile_id.y / 2, tile_id.z - 1};
    }
    if (dz >= tile_id.z) {
        return TileId{0, 0, 0};
    }
    uint coord_div = std::pow(2u, dz);
    return TileId{tile_id.x / coord_div, tile_id.y / coord_div, tile_id.z - dz};
}


class MetatileId {
public:
    MetatileId() = default;

    MetatileId(const TileId& id, uint size = 1) {
        FromTileId(id, size, size);
    }

    MetatileId(const TileId& id, uint width, uint height) {
        FromTileId(id, width, height);
    }

    std::vector<TileId> TileIds() const noexcept {
        std::vector<TileId> ids;
        ids.reserve(width_ * height_);
        for (uint y = lt_tile_.y; y < lt_tile_.y + height_; ++y) {
            for (uint x = lt_tile_.x; x < lt_tile_.x + width_; ++x) {
                ids.emplace_back(x, y, lt_tile_.z);
            }
        }
        return ids;
    }

    inline const TileId& left_top() const noexcept {
        return lt_tile_;
    }

    inline uint width() const noexcept {
        return width_;
    }

    inline uint height() const noexcept {
        return height_;
    }

    inline bool contains(const TileId& tile_id) const noexcept {
        return tile_id.z == lt_tile_.z &&
                tile_id.x >= lt_tile_.x && tile_id.x < lt_tile_.x + width_ &&
                tile_id.y >= lt_tile_.y && tile_id.y < lt_tile_.y + height_;
    }

    mapnik::box2d<double> GetBbox() const;

private:
    void FromTileId(const TileId& id, uint width, uint height);

    TileId lt_tile_{0, 0, 0};
    uint width_{1};
    uint height_{1};
};

inline bool operator==(const MetatileId& x, const MetatileId& y) {
    return x.left_top() == y.left_top() && x.width() == y.width() && x.height() == y.height();
}

inline bool operator!=(const MetatileId& x, const MetatileId& y) {
    return !(x == y);
}

std::ostream& operator<<(std::ostream& os, const MetatileId& metatile_id);


struct Tile {
    TileId id;
    std::string data;
};

struct Metatile {
    Metatile() = default;

    Metatile(const MetatileId& _id) : id(_id), tiles(_id.width() * _id.height()) {
        // Populate tiles vector
        const TileId& left_top = id.left_top();
        std::size_t pos = 0;
        for (uint y = left_top.y; y < left_top.y + id.height(); ++y) {
            for (uint x = left_top.x; x < left_top.x + id.width(); ++x) {
                TileId& tile_id = tiles[pos++].id;
                tile_id.x = x;
                tile_id.y = y;
                tile_id.z = left_top.z;
            }
        }
    }

    bool Validate() const noexcept;

    MetatileId id;
    std::vector<Tile> tiles;
};
