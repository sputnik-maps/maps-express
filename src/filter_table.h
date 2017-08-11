#pragma once

#include <mapnik/map.hpp>
#include <mapnik/expression.hpp>


class FilterTable {
public:    
    using zoom_groups_t = std::set<uint>;
    using filter_map_t = std::unordered_map<std::string, mapnik::expression_ptr>;

    static std::unique_ptr<FilterTable> MakeFilterTable(const std::string& map_path,
                                                        const zoom_groups_t* zoom_groups = nullptr,
                                                        int zoom_offset = 0, uint min_zoom = 0,
                                                        uint max_zoom = 22);

    static std::unique_ptr<FilterTable> MakeFilterTable(const mapnik::Map& map,
                                                        const zoom_groups_t* zoom_groups = nullptr,
                                                        int zoom_offset = 0, uint min_zoom = 0,
                                                        uint max_zoom = 22);

    const filter_map_t* GetFiltersMap(uint zoom) const noexcept;

    inline zoom_groups_t* zoom_groups() const noexcept {
        return zoom_groups_;
    }

    inline uint max_zoom() const noexcept {
        return max_zoom_;
    }

private:
    FilterTable(int zoom_offset, uint min_zoom = 0, uint max_zoom = 22);

    void ParseMap(const mapnik::Map& map, const zoom_groups_t* zoom_groups);

    std::map<uint, filter_map_t> filter_table_;
    const int zoom_offset_;
    const uint min_zoom_;
    const uint max_zoom_;
    zoom_groups_t* zoom_groups_{nullptr};
};
