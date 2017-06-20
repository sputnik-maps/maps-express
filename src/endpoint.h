#pragma once

#include <memory>
#include <string>
#include <vector>


class FilterTable;

enum class EndpointType : uint8_t {
    static_files,
    render,
    mvt
};

struct EndpointParams {
    std::shared_ptr<FilterTable> filter_table;
    std::string style_name;
    std::string provider_name;
    std::string utfgrid_key;
    uint minzoom;
    uint maxzoom;
    uint metatile_height{1};
    uint metatile_width{1};
    int zoom_offset{0};
    EndpointType type;
    bool allow_layers_query{false};
    bool allow_utf_grid{false};
    bool auto_metatile_size{false};
};
