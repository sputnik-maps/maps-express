#pragma once

#include <mapnik/map.hpp>

namespace me {

void load_map(mapnik::Map & map, std::string const& filename, bool strict = false, std::string base_path="");

void load_map_string(mapnik::Map & map, std::string const& str, bool strict = false,
                     std::string base_path="", bool mvt_style = false);

}
