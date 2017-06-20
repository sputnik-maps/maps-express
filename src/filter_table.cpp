#include "filter_table.h"

#include <mapnik/feature_type_style.hpp>
#include <mapnik/layer.hpp>

#include <glog/logging.h>

#include "load_map.h"
#include "util.h"

FilterTable::FilterTable(const std::string &map_path, uint merge_zoom, uint max_zoom) : max_zoom_(max_zoom),
                                                                                        merge_zoom_(merge_zoom) {
    mapnik::Map map;
    try {
        me::load_map(map, map_path);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Error while loading map " << map_path << ": " << e.what();
        return;
    }
    ParseMap(map);
}

FilterTable::FilterTable(const mapnik::Map &map, uint merge_zoom, uint max_zoom) : max_zoom_(max_zoom),
                                                                                   merge_zoom_(merge_zoom) {
    ParseMap(map);
}

void FilterTable::ParseMap(const mapnik::Map &map) {
    filter_table_.reserve(merge_zoom_ + 1);
    const auto& styles = map.styles();
    for (uint zoom = 0; zoom <= merge_zoom_; ++zoom) {
        filter_table_.emplace_back();
        // TODO: offset -> config
        double max_sd = util::zoom_to_scale_denominator(zoom + 1);
        double min_sd = (zoom != merge_zoom_) ? max_sd : util::zoom_to_scale_denominator(max_zoom_ + 1);
        for (const mapnik::layer& layer : map.layers()) {
            std::vector<mapnik::expression_ptr> layer_filters;
            const std::string& lyr_name = layer.name();
            bool no_filters = false;
            for (const auto& style_name : layer.styles()) {
                const auto style_itr = styles.find(style_name);
                if (style_itr == styles.end()) {
                    continue;
                }
                for (const mapnik::rule& rule : style_itr->second.get_rules()) {
                    if (max_sd < rule.get_min_scale() - 1e-6 ||
                            min_sd >= rule.get_max_scale() + 1e-6 ||
                            rule.get_symbolizers().empty()) {
                        continue;
                    }
                    mapnik::expression_ptr filter = rule.get_filter();
                    if (filter == nullptr) {
                        no_filters = true;
                        break;
                    }
                    layer_filters.push_back(std::move(filter));
                }
                if (no_filters) {
                    break;
                }
            }
            if (no_filters || layer_filters.size() > 1000) {
                filter_table_.back()[lyr_name] = nullptr;
                LOG(WARNING) << "Skipping filtering of layer " << lyr_name << " at zoom " << zoom
                             << " due to large amount of filters: " << layer_filters.size();
            } else if (!layer_filters.empty()) {
                filter_table_.back()[lyr_name] = MergeFilters(layer_filters);
            }
        }
    }
}

mapnik::expression_ptr FilterTable::MergeFilters(const std::vector<mapnik::expression_ptr>& filters) const noexcept {
    if (filters.empty()) {
        return nullptr;
    }
    auto filter_itr = filters.begin();
    mapnik::expression_ptr result = *filter_itr;
    for (++filter_itr; filter_itr != filters.end(); ++filter_itr) {
        result = MergeFilters(*filter_itr, result);
    }
    return result;
}


bool FilterTable::GetFilter(uint zoom, const std::string &layer_name, mapnik::expression_ptr* result) const noexcept {
    if (zoom >= filter_table_.size()) {
        return false;
    }
    auto& filter_row = filter_table_[zoom];
    const auto filter_itr = filter_row.find(layer_name);
    if (filter_itr != filter_row.end()) {
        *result = filter_itr->second;
        return true;
    }
    return false;
}
