#include "renderworker.h"

#include <fstream>

#include <mapnik/config.hpp>
#include <mapnik/agg_renderer.hpp>
#include <mapnik/grid/grid_renderer.hpp>
#include <mapnik/grid/grid_view.hpp>
#include <mapnik/layer.hpp>
#include <mapnik/load_map.hpp>
#include <mapnik/image_view.hpp>
#include <mapnik/image_util.hpp>
#include <mapnik/feature_type_style.hpp>
#include <mapnik/rule.hpp>

#include <glog/logging.h>

#include <vector_tile_config.hpp>
#include <vector_tile_datasource_pbf.hpp>

#include "cached_datasource.h"
#include "load_map.h"
#include "load_mvt_map.h"
#include "subtiler.h"
#include "utfgrid_encode.h"

static const std::string kMapProj = "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0.0 +k=1.0 "
                                    "+units=m +nadgrids=@null +wktext +no_defs +over";

RenderWorker::RenderWorker(std::shared_ptr<const styles_t> styles) : styles_(std::move(styles)) {}

bool RenderWorker::Init() noexcept {
    if (!styles_) {
        return true;
    }
    for (const StyleInfo& style_info : *styles_) {
        auto map_info = LoadStyle(style_info);
        if (!map_info) {
            return false;
        }
        maps_[style_info.name] = std::move(map_info);

    }
    return true;
}

std::shared_ptr<RenderWorker::MapInfo> RenderWorker::LoadStyle(const StyleInfo& style_info) {
    if (style_info.name.empty()) {
        LOG(ERROR) << "Empty style name";
        return nullptr;
    }
    auto map_info = std::make_shared<MapInfo>(256, 256, kMapProj);
    map_info->allow_grid_render = style_info.allow_grid_render;
    map_info->version = style_info.version;
    mapnik::Map& map = map_info->map;
    try {
        if (!style_info.path.empty()) {
            me::load_map(map, style_info.path);
        } else if (style_info.data && !style_info.data->empty()){
            if (style_info.type == StyleInfo::Type::mapnik) {
                mapnik::load_map_string(map, *style_info.data, false, style_info.base_path);
            } else {
                load_mvt_map_string(map, *style_info.data, false, style_info.base_path);
            }
        } else {
            LOG(ERROR) << "No style path, nor style data provided!";
            return nullptr;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Error while loading style: " << e.what();
        return nullptr;
    }

    CalculateLayersSD(map);
    // Find mvt layers (layers without ds) and set 900913 proj for them!
    for (mapnik::layer& layer : map.layers()) {
        if (layer.datasource() == nullptr) {
            map_info->mvt_layers.push_back(&layer);
            layer.set_srs(map.srs());
        } else {
            map_info->standard_layers.push_back(&layer);
        }
    }
    return map_info;
}

void RenderWorker::ProcessTask(TileWorkTask task) noexcept {
    if (task.async_task->cancelled()) {
        return;
    }
    TileWorkRequest* request = task.request.get();
    RenderRequest* rr = dynamic_cast<RenderRequest*>(request);
    if (rr) {
        ProcessRender(*task.async_task, *rr);
        return;
    }
    SubtileRequest* sr = dynamic_cast<SubtileRequest*>(request);
    if (sr) {
        ProcessSubtile(*task.async_task, *sr);
        return;
    }
    LOG(ERROR) << "Invalid TileWorkRequest!";
    task.async_task->NotifyError();
}

static inline std::string GetTileData(std::size_t x, std::size_t y,
                                      std::size_t width, std::size_t height,
                                      const mapnik::image_rgba8& img) {
    mapnik::image_view<mapnik::image_rgba8> view(x, y, width, height, img);
    return mapnik::save_to_string(view, "png8:z=1");
}

static inline std::string GetTileData(std::size_t x, std::size_t y, std::size_t width,
                                      std::size_t height, const mapnik::grid& grid) {
    mapnik::grid_view view = const_cast<mapnik::grid&>(grid).get_view(x, y, width, height);
    return encode_utfgrid(view);
}


template <typename T>
static void SplitToTiles(const T& image, Metatile& metatile) {
    const MetatileId& metatile_id = metatile.id;
    assert(image.width() % metatile_id.width() == 0);
    assert(image.height() % metatile_id.height() == 0);
    std::size_t tile_image_width = image.width() / metatile_id.width();
    std::size_t tile_image_height = image.height() / metatile_id.height();
    std::size_t tile_i = 0;
    for (std::size_t y = 0; y < image.height(); y += tile_image_height) {
        for (std::size_t x = 0; x < image.width(); x += tile_image_width) {
            assert(tile_i < metatile.tiles.size());
            std::string& tile_data = metatile.tiles[tile_i++].data;
            tile_data = GetTileData(x, y, tile_image_width, tile_image_height, image);
        }
    }
}

void RenderWorker::ProcessRender(RenderTask& async_task, const RenderRequest& request) noexcept {
    if (async_task.cancelled()) {
        return;
    }

    auto map_info_itr = maps_.find(request.style_name);
    if (map_info_itr == maps_.end()) {
        LOG(ERROR) << "Style \"" << request.style_name << "\" not found!";
        async_task.NotifyError();
        return;
    }

    MapInfo& map_info = *map_info_itr->second;
    mapnik::Map& map = map_info.map;

    const MetatileId& metatile_id = request.metatile_id;
    const int buf_size = 256;
    const int scale = request.retina ? 2 : 1;
    const int map_width = 256 * metatile_id.width() * scale;
    const int map_height = 256 * metatile_id.height() * scale;

    mapnik::box2d<double> metatile_bbox = metatile_id.GetBbox();

    mapnik::request metatile_req(256, 256, metatile_bbox);
    metatile_req.set_buffer_size(128);
    mapnik::box2d<double> metatile_buf_bbox = metatile_req.get_buffered_extent();

    map.zoom_to_box(metatile_bbox);

    if (request.layers == nullptr) {
        for (auto layer : map_info.standard_layers) {
            layer->set_active(true);
        }
    } else {
        const auto& requested_layers = *request.layers;
        for (auto layer : map_info.standard_layers) {
            bool requested = (requested_layers.find(layer->name()) != requested_layers.end());
            layer->set_active(requested);
        }
    }

    if (!map_info.mvt_layers.empty()) {
        if (request.data_tile) {
            // Set mvt datasourse
            const Tile& data_tile = *request.data_tile;
            const TileId& data_tile_id = data_tile.id;
            int base_x = data_tile_id.x;
            int base_y = data_tile_id.y;
            int base_zoom = data_tile_id.z;

            std::unordered_map<std::string, mapnik::datasource_ptr> datasources;

            protozero::pbf_reader tile_message(data_tile.data);
            // loop through the layers of the vector tile!
            while (tile_message.next(mapnik::vector_tile_impl::Tile_Encoding::LAYERS))
            {
                auto data_pair = tile_message.get_data();
                protozero::pbf_reader layer_message(data_pair);
                if (!layer_message.next(mapnik::vector_tile_impl::Layer_Encoding::NAME))
                {
                    continue;
                }

                std::string layer_name = layer_message.get_string();
                if (request.layers != nullptr && request.layers->find(layer_name) == request.layers->end()) {
                    continue;
                }

                protozero::pbf_reader layer_pbf(data_pair);
                auto ds = std::make_shared<mapnik::vector_tile_impl::tile_datasource_pbf>(
                        layer_pbf,
                        base_x,
                        base_y,
                        base_zoom,
                        false);
                ds->set_envelope(metatile_buf_bbox);
                datasources[layer_name] = std::make_shared<CahedDataSource>(std::move(ds));
            }

            for (mapnik::layer* map_layer : map_info.mvt_layers) {
                auto ds_map_iter = datasources.find(map_layer->name());
                if (ds_map_iter != datasources.end()) {
                    map_layer->set_buffer_size(buf_size);
                    map_layer->set_datasource(ds_map_iter->second);
                    map_layer->set_active(true);
                } else {
                    map_layer->set_active(false);
                }
            }
        } else {
            for (mapnik::layer* map_layer : map_info.mvt_layers) {
                map_layer->set_active(false);
            }
        }
    }

    map.resize(map_width, map_height);

    if (async_task.cancelled()) {
        return;
    }

    Metatile metatile(metatile_id);
    try {
        if (request.render_type == RenderType::png) {
            mapnik::image_rgba8 image(map_width, map_height);
            mapnik::agg_renderer<mapnik::image_rgba8> ren(map, image, scale);
            ren.apply();
            SplitToTiles(image, metatile);
        } else {
            mapnik::grid utf_grid(map_width, map_height, request.utfgrid_key);
            mapnik::grid_renderer<mapnik::grid> ren(map, utf_grid, scale);
            ren.apply();
            SplitToTiles(utf_grid, metatile);
        }
    } catch(const std::exception& e) {
        LOG(ERROR) << "Mapnik render error: " << e.what() << " type: " <<
                      (request.render_type == RenderType::png ? "png" : "utfgrid") << metatile_id;
        async_task.NotifyError();
        return;
    }
    async_task.SetResult(std::move(metatile));
}

void RenderWorker::ProcessSubtile(RenderTask& async_task, SubtileRequest& request) noexcept {
    const int buf_size = 256;
    Subtiler subtiler(std::move(request.mvt_tile), request.filter_table);
    std::string result;
    try {
        result = subtiler.MakeSubtile(request.tile_id, 4096, buf_size, std::move(request.layers));
    } catch (...) {
        LOG(ERROR) << "MVT subtiling error: " << request.tile_id;
        async_task.NotifyError();
        return;
    }
    Metatile metatile;
    metatile.id = MetatileId(request.tile_id);
    metatile.tiles.push_back(Tile{request.tile_id, std::move(result)});
    async_task.SetResult(std::move(metatile));
}

void RenderWorker::CalculateLayersSD(mapnik::Map& map) {
    const auto& styles = map.styles();
    for (auto &layer : map.layers()) {
        double min_sd = 1000000000;
        double max_sd = 0;
        for (const auto &style_name : layer.styles()) {
            const auto style_itr = styles.find(style_name);
            if (style_itr == styles.end()) {
                continue;
            }
            const auto& style = style_itr->second;
            for (const auto &rule : style.get_rules()) {
                min_sd = std::min(min_sd, rule.get_min_scale());
                max_sd = std::max(max_sd, rule.get_max_scale());
            }
        }
        if (min_sd != 1000000000) {
            layer.set_minimum_scale_denominator(min_sd);
        }
        if (max_sd != 0) {
            layer.set_maximum_scale_denominator(max_sd);
        }
    }
}

bool RenderWorker::UpdateStyles(const styles_t& styles) {
    pending_update_ptr_ = &styles;
    for (const StyleInfo& style_info : styles) {
        auto map_info_itr = maps_.find(style_info.name);
        if (map_info_itr != maps_.end()) {
            std::shared_ptr<MapInfo>& map_info = map_info_itr->second;
            if (map_info->version == style_info.version) {
                updated_maps_[style_info.name] = map_info;
                continue;
            }
        }
        auto map_info = LoadStyle(style_info);
        if (!map_info) {
            return false;
        }
        updated_maps_[style_info.name] = std::move(map_info);
    }
    return true;
}

bool RenderWorker::CommitUpdate(const styles_t* update_ptr) {
    if (pending_update_ptr_ != update_ptr) {
        return false;
    }
    maps_.clear();
    maps_.swap(updated_maps_);
    pending_update_ptr_ = nullptr;
    return true;
}

bool RenderWorker::CancelUpdate(const styles_t* update_ptr) {
    if (pending_update_ptr_ != update_ptr) {
        return false;
    }
    updated_maps_.clear();
    pending_update_ptr_ = nullptr;
    return true;
}
