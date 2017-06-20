#pragma once

#include <string>

#include <mapnik/grid/grid.hpp>

std::string encode_utfgrid(const mapnik::grid_view& utfgrid, uint size = 4);

