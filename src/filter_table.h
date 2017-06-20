#pragma once

#include <mapnik/config.hpp>
#include <mapnik/map.hpp>
#include <mapnik/expression.hpp>
#include <mapnik/expression_node.hpp>
#include <mapnik/rule.hpp>

class FilterTable {
public:
    FilterTable(const std::string& map_path, uint merge_zoom = 22, uint max_zoom = 22);
    FilterTable(const mapnik::Map& map, uint merge_zoom = 22, uint max_zoom = 22);

    bool GetFilter(uint zoom, const std::string& layer_name, mapnik::expression_ptr* result) const noexcept;

    inline uint merge_zoom() const noexcept {
        return merge_zoom_;
    }

    inline uint max_zoom() const noexcept {
        return max_zoom_;
    }

private:
    void ParseMap(const mapnik::Map& map);
    mapnik::expression_ptr MergeFilters(const std::vector<mapnik::expression_ptr>& filters) const noexcept;

    inline mapnik::expression_ptr MergeFilters(const mapnik::expression_ptr& filter1,
                                               const mapnik::expression_ptr& filter2) const noexcept {
        mapnik::binary_node<mapnik::tags::logical_or> new_filter(*filter1, *filter2);
        return std::make_shared<mapnik::expr_node>(std::move(new_filter));
    }

    using collumn_t = std::map<std::string, mapnik::expression_ptr>;
    std::vector<collumn_t> filter_table_;
    const uint max_zoom_;
    const uint merge_zoom_;
};
