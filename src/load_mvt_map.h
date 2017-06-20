#pragma once

#include <string>

#include <jsoncpp/json/json.h>
#include <jsoncpp/json/value.h>

#include <mapnik/map.hpp>
#include <mapnik/rule.hpp>
#include <mapnik/expression.hpp>

void load_mvt_map(mapnik::Map & map, std::string const& filename, bool strict = false,
                  const std::string& base_path="");

void load_mvt_map_string(mapnik::Map & map, std::string const& str, bool strict = false,
                         const std::string& base_path="");

constexpr double zoom2scale(double zoom)
{
    return 559082264. / std::pow(2, zoom);
}

void edit_placements(mapnik::text_placements_ptr& tpl, mapnik::text_placements_ptr& placements);
