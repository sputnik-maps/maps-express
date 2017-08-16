#include "load_map.h"

#include <stdexcept>

#include <mapnik/load_map.hpp>

#include <mapbox2mapnik/mapbox2mapnik.hpp>

namespace sputnik {

void load_map(mapnik::Map & map, std::string const& filename, bool strict, std::string base_path) {
    auto path_len = filename.size();
    if (path_len < 5) {
        throw std::runtime_error("Invalid map path: " + filename);
    }
    if (filename.substr(path_len - 5) == ".json") {
        sputnik::load_mapbox_map(map, filename, strict, base_path);
    } else {
        mapnik::load_map(map, filename, strict, base_path);
    }
}

void load_map_string(mapnik::Map & map, std::string const& str, bool strict, std::string base_path, bool mvt_style) {
//    if (mvt_style) {
//        load_mvt_map_string(map, str, strict, base_path);
//    } else {
//        mapnik::load_map_string(map, str, strict, base_path);
//    }
}

} // ns me
