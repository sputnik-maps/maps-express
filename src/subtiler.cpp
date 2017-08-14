#include "subtiler.h"

#include <cmath>

#include <mapnik/expression_evaluator.hpp>

#include <vector_tile_geometry_clipper.hpp>
#include <glog/logging.h>
#include <iostream>
#include <clipper.hpp>

#include "bbox_clipper.h"
#include "util.h"

Subtiler::Subtiler(const Tile& base_tile, std::shared_ptr<const FilterTable> filter_table) :
        base_tile_(base_tile),
        filter_table_(filter_table),
        transcoder_("utf-8") {}

Subtiler::Subtiler(Tile&& base_tile, std::shared_ptr<const FilterTable> filter_table) :
        base_tile_(std::move(base_tile)),
        filter_table_(filter_table),
        transcoder_("utf-8") {
}

std::string Subtiler::MakeSubtile(const TileId& target_tile_id,
                                  uint target_extent, int buffer_size,
                                  std::unique_ptr<std::set<std::string>> layers) {
    std::string result;
    target_extent_ = target_extent;
    zoom_factor_ = std::pow(2, target_tile_id.z - base_tile_.id.z);
    clip_box_ = mapnik::box2d<int64_t>(-buffer_size , -buffer_size,
                                       target_extent + buffer_size, target_extent + buffer_size);

    clip_polygon_.reserve(5);
    clip_polygon_.emplace_back(clip_box_.minx(), clip_box_.miny());
    clip_polygon_.emplace_back(clip_box_.maxx(), clip_box_.miny());
    clip_polygon_.emplace_back(clip_box_.maxx(), clip_box_.maxy());
    clip_polygon_.emplace_back(clip_box_.minx(), clip_box_.maxy());
    clip_polygon_.emplace_back(clip_box_.minx(), clip_box_.miny());

//    result.reserve(tile_data_.size() / zoom_factor);

    protozero::pbf_reader tile_message(base_tile_.data);
    protozero::pbf_writer result_pbf(result);

    const FilterTable::filter_map_t* filter_map = nullptr;
    if (filter_table_) {
        filter_map = filter_table_->GetFiltersMap(target_tile_id.z);
        if (!filter_map) {
            LOG(ERROR) << "Filter map not found for zoom: " << std::to_string(target_tile_id.z);
            return "";
        }
    }

    // loop through the layers of the tile!
    while (tile_message.next(mapnik::vector_tile_impl::Tile_Encoding::LAYERS))
    {
        auto data_pair = tile_message.get_data();
        protozero::pbf_reader layer_message(data_pair);
        if (!layer_message.next(mapnik::vector_tile_impl::Layer_Encoding::NAME))
        {
            LOG(WARNING) << "Skipping layer without name!";
            continue;
        }
        std::string layer_name = layer_message.get_string();
        // If exact layers requested, process only requested layers
        if (layers != nullptr && layers->find(layer_name) == layers->end()) {
            continue;
        }
        if (filter_map) {
            auto filter_itr = filter_map->find(layer_name);
            if (filter_itr == filter_map->end()) {
                continue;
            }
            layer_filter_ = filter_itr->second;
        }
        if (!layer_message.next(mapnik::vector_tile_impl::Layer_Encoding::EXTENT))
        {
            LOG(WARNING) << "Skipping layer without extent: " << layer_name;
            continue;
        }
        uint layer_extent = layer_message.get_uint32();
        UpdateTargetParams(target_tile_id.x, target_tile_id.y, layer_extent, target_extent);
//        std::cout << clipping_box_ << std::endl;
        protozero::pbf_reader layer_pbf(data_pair);
        ProcessLayer(&layer_pbf, &result_pbf);
    }
    return result;
}

void Subtiler::UpdateTargetParams(uint target_x, uint target_y, uint source_extent, uint target_extent) {
    target_scale_ = target_extent * zoom_factor_ / static_cast<double>(source_extent);
    target_offset_x_ = static_cast<int>(std::round((target_x / static_cast<float>(zoom_factor_)
                                                    - base_tile_.id.x) * source_extent));
    target_offset_y_ = static_cast<int>(std::round((target_y / static_cast<float>(zoom_factor_)
                                                    - base_tile_.id.y) * source_extent));
}


void Subtiler::ProcessLayer(protozero::pbf_reader *layer_pbf, protozero::pbf_writer *output_pbf)
{
    using Layer_Encoding = mapnik::vector_tile_impl::Layer_Encoding;
    using Value_Encoding = mapnik::vector_tile_impl::Value_Encoding;
    using pbf_pair_t = std::pair<const char*, protozero::pbf_length_type>;
    std::string name;
    std::vector<pbf_pair_t> keys, values;
    uint version = 0;

    std::vector<protozero::pbf_reader> features;
    protozero::pbf_writer output_layer_pbf(*output_pbf, mapnik::vector_tile_impl::Tile_Encoding::LAYERS);

    layer_keys_.clear();
    layer_values_.clear();

    while (layer_pbf->next())
    {
        switch(layer_pbf->tag())
        {
            case Layer_Encoding::NAME:
                name = layer_pbf->get_string();
                break;
            case Layer_Encoding::FEATURES:
                features.push_back(layer_pbf->get_message());
                break;
            case Layer_Encoding::KEYS:
                if (layer_filter_ == nullptr) {
                    keys.push_back(layer_pbf->get_data());
                } else {
                    layer_keys_.push_back(layer_pbf->get_string());
                }
                break;
            case Layer_Encoding::VALUES:
                if (layer_filter_ == nullptr) {
                    values.push_back(layer_pbf->get_data());
                } else {
                    protozero::pbf_reader val_msg = layer_pbf->get_message();
                    while (val_msg.next())
                    {
                        switch(val_msg.tag()) {
                            case Value_Encoding::STRING:
                                layer_values_.push_back(val_msg.get_string());
                                break;
                            case Value_Encoding::FLOAT:
                                layer_values_.push_back(val_msg.get_float());
                                break;
                            case Value_Encoding::DOUBLE:
                                layer_values_.push_back(val_msg.get_double());
                                break;
                            case Value_Encoding::INT:
                                layer_values_.push_back(val_msg.get_int64());
                                break;
                            case Value_Encoding::UINT:
                                layer_values_.push_back(val_msg.get_uint64());
                                break;
                            case Value_Encoding::SINT:
                                layer_values_.push_back(val_msg.get_sint64());
                                break;
                            case Value_Encoding::BOOL:
                                layer_values_.push_back(val_msg.get_bool());
                                break;
                            default:
                                LOG(ERROR) << "unknown Value type " <<
                                           std::to_string(layer_pbf->tag()) << " in layer.values";
                        }
                    }
                }
                break;
            case Layer_Encoding::VERSION:
                version = layer_pbf->get_uint32();
                break;
            case Layer_Encoding::EXTENT:
                layer_pbf->get_uint32();
                break;
            default:
                LOG(ERROR) << "unknown field type " + std::to_string(layer_pbf->tag()) + " in layer " << name;
        }
    }

    num_keys_ = layer_keys_.size();
    num_values_ = layer_values_.size();
    std::unordered_map<mapnik::value, std::size_t> layer_new_tags;
    bool features_written = false;
    for (auto feature_pbf : features) {
        if (ProcessFeature(&feature_pbf, &layer_new_tags, &output_layer_pbf)) {
            features_written = true;
        }
    }

    if (!features_written) {
//        std::cout << "Skipped layer " << std::string(name.first, name.second) << std::endl;
        output_layer_pbf.rollback();
        return;
    }

    output_layer_pbf.add_message(Layer_Encoding::NAME, name);

    if (layer_filter_ == nullptr) {
        for (const auto &key : keys) {
            output_layer_pbf.add_message(Layer_Encoding::KEYS, key.first, key.second);
        }
        for (const auto &value : values) {
            output_layer_pbf.add_message(Layer_Encoding::VALUES, value.first, value.second);
        }
    } else {
        for (const auto &key : layer_keys_) {
            output_layer_pbf.add_message(Layer_Encoding::KEYS, key);
        }
        std::vector<mapnik::value> tags_vector;
        std::size_t num_tags = layer_new_tags.size();
        tags_vector.resize(num_tags);
        for (auto& tag_itr : layer_new_tags) {
            if (tag_itr.second < num_tags) {
                tags_vector[tag_itr.second] = std::move(tag_itr.first);
            } else {
                LOG(ERROR) << "Invalid tag value index " << tag_itr.second << " in layer " << name;
            }
        }
        for (const auto& tag_itr : tags_vector) {
            protozero::pbf_writer value_writer(output_layer_pbf, Layer_Encoding::VALUES);
            to_tile_value_pbf visitor(value_writer);
            mapnik::util::apply_visitor(visitor, tag_itr);
        }
    }
    output_layer_pbf.add_uint32(Layer_Encoding::EXTENT, static_cast<uint>(target_extent_));
    output_layer_pbf.add_uint32(Layer_Encoding::VERSION, version);
}

bool Subtiler::ProcessFeature(protozero::pbf_reader *feature_pbf,
                              std::unordered_map<mapnik::value, std::size_t>* layer_new_tags,
                              protozero::pbf_writer *output_layer_pbf)
{
    using Feature_Encoding = mapnik::vector_tile_impl::Feature_Encoding;
    uint64_t id = 0;
    int geom_type = 0;
    std::vector<packed_uint_32_t> tags, geometrys;
    std::unique_ptr<FeatureTags> decoded_tags;
    protozero::pbf_writer output_feature_pbf(*output_layer_pbf, mapnik::vector_tile_impl::Layer_Encoding::FEATURES);
    while (feature_pbf->next()) {
        switch (feature_pbf->tag()) {
            case Feature_Encoding::ID:
                id = feature_pbf->get_uint64();
                break;
            case Feature_Encoding::GEOMETRY:
                geometrys.push_back(std::move(feature_pbf->get_packed_uint32()));
                break;
            case Feature_Encoding::RASTER:
                LOG(WARNING) << "Raster clipping not implemented yet!";
                output_feature_pbf.rollback();
                return false;
            case Feature_Encoding::TAGS:
                if (layer_filter_ == nullptr) {
                    // If no filter_table provided, we don't need to decode features
                    tags.push_back(std::move(feature_pbf->get_packed_uint32()));
                } else {
                    // Decode features and apply filters
                    decoded_tags = DecodeFeatureTags(feature_pbf->get_packed_uint32());
                    mapnik::value_type result = mapnik::util::apply_visitor(mapnik::evaluate<FeatureTags, mapnik::value, vars_t>(*decoded_tags, vars_), *layer_filter_);
                    if (!result.to_bool()) {
                        output_feature_pbf.rollback();
                        return false;
                    }
                }
                break;
            case Feature_Encoding::TYPE:
                geom_type = feature_pbf->get_enum();
                break;
            default:
                LOG(ERROR) << "Vector Tile contains unknown field type " + std::to_string(feature_pbf->tag()) +" in feature";
                output_feature_pbf.rollback();
                return false;
        }
    }

    bool geometries_written = false;
    for (auto &geometry : geometrys) {
        if (ProcessGeometry(geometry, geom_type, &output_feature_pbf)) {
            geometries_written = true;
        }
    }
    if (!geometries_written) {
        output_feature_pbf.rollback();
        return false;
    }


    output_feature_pbf.add_uint64(Feature_Encoding::ID, id);
    output_feature_pbf.add_enum(Feature_Encoding::TYPE, geom_type);
    if (layer_filter_ == nullptr) {
        for (auto &tag : tags) {
            output_feature_pbf.add_packed_uint32(Feature_Encoding::TAGS, tag.first, tag.second);
        }
    } else if (decoded_tags != nullptr){
        WriteFeatureTags(*decoded_tags, layer_new_tags, &output_feature_pbf);
    }
    return true;
}

std::unique_ptr<FeatureTags> Subtiler::DecodeFeatureTags(const Subtiler::packed_uint_32_t &packed_tags) {
    auto feature_tags = util::make_unique<FeatureTags>();
    for (auto _i = packed_tags.begin(); _i != packed_tags.end();)
    {
        std::size_t key_index = *(_i++);
        if (_i == packed_tags.end())
        {
            LOG(ERROR) << "Vector Tile has a feature with an odd number of tags, therefore the tile is invalid.";
            return nullptr;
        }
        std::size_t key_value = *(_i++);
        if (key_index < num_keys_
            && key_value < num_values_)
        {
            std::string const& key_name = layer_keys_.at(key_index);
            const mapnik::vector_tile_impl::pbf_attr_value_type& val = layer_values_.at(key_value);
            feature_tags->push(key_name, key_index, val, transcoder_);
        } else {
            LOG(ERROR) << "Vector Tile has a feature with repeated attributes with an invalid key or value as it does not appear in the layer.";
        }
    }
    return feature_tags;
}

void Subtiler::WriteFeatureTags(const FeatureTags &feature_tags,
                                std::unordered_map<mapnik::value, std::size_t> *layer_new_tags,
                                protozero::pbf_writer *output_feature_pbf) {
    std::vector<std::uint32_t> encoded_feature_tags;
    encoded_feature_tags.reserve(feature_tags.tags_map().size());
    for (const auto tag_itr : feature_tags.tags_map()) {
        const tag_type& tag = tag_itr.second;
        if (tag.second.is_null()) {
            continue;
        }
        encoded_feature_tags.push_back(static_cast<uint32_t>(tag.first)); // push key index
        const auto val_itr = layer_new_tags->find(tag.second);
        if (val_itr == layer_new_tags->end()) {
            std::size_t index = layer_new_tags->size();
            layer_new_tags->emplace(tag.second, index);
            encoded_feature_tags.push_back(static_cast<uint32_t>(index)); // add value and push it's index
        } else {
            encoded_feature_tags.push_back(static_cast<uint32_t>(val_itr->second)); // push value index
        }
    }
    output_feature_pbf->add_packed_uint32(mapnik::vector_tile_impl::Feature_Encoding::TAGS,
                                          encoded_feature_tags.begin(), encoded_feature_tags.end());
}

bool Subtiler::ProcessGeometry(const Subtiler::packed_uint_32_t &packed_geometry,
                               int geom_type, protozero::pbf_writer *output_feature_pbf)
{
    protozero::packed_field_uint32 output_packed_geometry(*output_feature_pbf, mapnik::vector_tile_impl::Feature_Encoding::GEOMETRY);
    using Geometry_Type = mapnik::vector_tile_impl::Geometry_Type;
    bool geom_written = false;
    switch (geom_type) {
        case Geometry_Type::UNKNOWN:
            LOG(WARNING) << "Skipping unknown geomerty type";
            break;
        case Geometry_Type::POINT:
           if (ProcessPoint(packed_geometry, &output_packed_geometry)) {
               geom_written = true;
           }
            break;
        case Geometry_Type::LINESTRING:
            if (ProcessLinestring(packed_geometry, &output_packed_geometry)) {
                geom_written = true;
            }
            break;
        case Geometry_Type::POLYGON:
            if (ProcessPolygon(packed_geometry, &output_packed_geometry)) {
                geom_written = true;
            }
            break;
        default:
            LOG(ERROR) << "Vector Tile contains unknown geometry type " << geom_type;
            break;
    }
    if (!geom_written) {
        output_packed_geometry.rollback();
        return false;
    }
    return true;
}

bool Subtiler::ProcessPoint(const Subtiler::packed_uint_32_t &packed_points, protozero::packed_field_uint32 *output_geometry) {
    mapnik::vector_tile_impl::GeometryPBF point(packed_points);
    int64_t x, y;
    std::vector<point_t> points;
    while (point.point_next(x, y)) {
        ScaleAndOffset(&x, &y);
        if (clip_box_.contains(x, y)) {
            points.emplace_back(x, y);
        }
    }
    if (points.empty()) {
        return false;
    }
    WritePoints(points, output_geometry);
    return true;
}

bool Subtiler::ProcessLinestring(const Subtiler::packed_uint_32_t &packed_linestring, protozero::packed_field_uint32 *output_geometry) {
    using GeometryPBF = mapnik::vector_tile_impl::GeometryPBF;
    GeometryPBF linestring(packed_linestring);
    int64_t x0, y0, x1, y1;
    GeometryPBF::command cmd;
    cmd = linestring.line_next(x0, y0, false);
    if (cmd == GeometryPBF::end) {
        return false;
    }
    else if (cmd != GeometryPBF::move_to)
    {
        LOG(ERROR) << "Vector Tile has LINESTRING type geometry where the first command is not MOVETO.";
        return false;
    }
    mapnik::geometry::multi_line_string<int64_t> results;

    while (true)
    {
        cmd = linestring.line_next(x1, y1, true);
        if (cmd != GeometryPBF::line_to) {
            LOG(ERROR) << "Vector Tile has LINESTRING type geometry where the first command is not MOVETO.";
            return false;
        }
        mapnik::geometry::line_string<int64_t> line;
        // reserve prior
        line.reserve(linestring.get_length() + 2);
        ScaleAndOffset(&x0, &y0);
        line.add_coord(x0, y0);
        ScaleAndOffset(&x1, &y1);
        line.add_coord(x1, y1);
        while ((cmd = linestring.line_next(x1, y1, true)) == GeometryPBF::line_to) {
            ScaleAndOffset(&x1, &y1);
            line.add_coord(x1, y1);
        }

        bbox_clipper::ClipLineString(line, clip_box_, &results);

        if (cmd == GeometryPBF::end) {
            break;
        }
        // else we are guaranteed it is a moveto
        x0 = x1;
        y0 = y1;
    }

    return WriteLinestring(results, output_geometry);
}

inline bool ClipMultiPolygon (mapnik::geometry::multi_polygon<std::int64_t>& mp,
                              std::vector<std::unique_ptr<ClipperLib::PolyTree>>& output_polygons,
                              const mapnik::geometry::linear_ring<std::int64_t>& clip_polygon) {
    ClipperLib::Clipper clipper;
    clipper.StrictlySimple(true);

    for (auto& poly : mp) {
        ClipperLib::CleanPolygon(poly.exterior_ring, 1.415);
        double outer_area = ClipperLib::Area(poly.exterior_ring);
        if (std::abs(outer_area) < 0.1)
        {
            continue;
        }
        // The view transform inverts the y axis so this should be positive still despite now
        // being clockwise for the exterior ring. If it is not lets invert it.
        if (outer_area < 0)
        {
            std::reverse(poly.exterior_ring.begin(), poly.exterior_ring.end());
        }
        if(!clipper.AddPath(poly.exterior_ring, ClipperLib::ptSubject, true)) {
            continue;
        }
        for (auto &interior_ring : poly.interior_rings) {
            if (interior_ring.size() < 3)
            {
                continue;
            }
            ClipperLib::CleanPolygon(interior_ring, 1.415);
            double inner_area = ClipperLib::Area(interior_ring);
            if (std::abs(inner_area) < 0.1)
            {
                continue;
            }
            // This should be a negative area, the y axis is down, so the ring will be "CCW" rather
            // then "CW" after the view transform, but if it is not lets reverse it
            if (inner_area > 0)
            {
                std::reverse(interior_ring.begin(), interior_ring.end());
            }
            clipper.AddPath(interior_ring, ClipperLib::ptSubject, true);
        }
        if (!clipper.AddPath( clip_polygon, ClipperLib::ptClip, true )) {
            continue;
        }
        output_polygons.emplace_back(new ClipperLib::PolyTree());
        clipper.Execute(ClipperLib::ctIntersection, *output_polygons.back().get(), ClipperLib::pftPositive,
                        ClipperLib::pftEvenOdd);
        clipper.Clear();
    }
    return true;
}

bool Subtiler::ProcessPolygon(const packed_uint_32_t &packed_polygon, protozero::packed_field_uint32 *output_geometry) {
    using GeometryPBF = mapnik::vector_tile_impl::GeometryPBF;
    GeometryPBF::command cmd;
    int64_t x0, y0;
    int64_t x1, y1;
    int64_t x2, y2;

    // Tmp values for calculating ring area using original coords;
    int64_t x0_, y0_;
    int64_t x1_, y1_;
    int64_t x2_, y2_;

    GeometryPBF polygon(packed_polygon);

    bool first_ring = true;
    bool looking_for_exterior = true; // Assume that the first ring is exterior
    bool current_is_exterior;
    bool has_next_geometry = true;

    cmd = polygon.ring_next(x0, y0, false);
    if (cmd == GeometryPBF::end) {
        return false;
    }
    else if (cmd != GeometryPBF::move_to)
    {
        LOG(ERROR) << "Vector Tile has POLYGON type geometry where the first command is not MOVETO.";
        return false;
    }

//
    bool geometry_written = false;

    mapnik::geometry::multi_polygon<std::int64_t> decoded_mp;
    mapnik::geometry::polygon<std::int64_t>* decoded_polygon;

    while (has_next_geometry) {
        double ring_area = 0.0;
        mapnik::box2d<int64_t> part_env;
        // add new ring to start adding to
        mapnik::geometry::linear_ring<std::int64_t> decoded_ring;

        x0_ = x0;
        y0_ = y0;
        ScaleAndOffset(&x0, &y0);

        cmd = polygon.ring_next(x1, y1, true);
        if (cmd != GeometryPBF::line_to) {
            LOG(ERROR) << "Vector Tile has POLYGON type geometry has invalid command.";
            return false;
        }
        x1_ = x1;
        y1_ = y1;
        ScaleAndOffset(&x1, &y1);

        cmd = polygon.ring_next(x2, y2, true);
        if (cmd != GeometryPBF::line_to) {
            LOG(ERROR) << "Vector Tile has POLYGON type geometry has invalid command.";
            return false;
        }
        x2_ = x2;
        y2_ = y2;
        ScaleAndOffset(&x2, &y2);

        // reserve prior
        decoded_ring.reserve(polygon.get_length() + 4);
        // add moveto command position
        decoded_ring.add_coord(x0, y0);
        part_env.init(x0, y0, x0, y0);
        // add first lineto
        decoded_ring.add_coord(x1, y1);
        part_env.expand_to_include(x1, y1);
        ring_area += calculate_segment_area(x0_, y0_, x1_, y1_);
        // add second lineto
        decoded_ring.add_coord(x2, y2);
        part_env.expand_to_include(x2, y2);
        ring_area += calculate_segment_area(x1_, y1_, x2_, y2_);
        x1_ = x2_;
        y1_ = y2_;

        while ((cmd = polygon.ring_next(x2, y2, true)) == GeometryPBF::line_to) {
            x2_ = x2;
            y2_ = y2;
            ScaleAndOffset(&x2, &y2);
            decoded_ring.add_coord(x2, y2);
            part_env.expand_to_include(x2, y2);
            ring_area += calculate_segment_area(x1_, y1_, x2_, y2_);
            x1_ = x2_;
            y1_ = y2_;
        }
        // Make sure we are now on a close command
        if (cmd != GeometryPBF::close) {
            LOG(ERROR) << "Vector Tile has POLYGON type geometry with a ring not closed by a CLOSE command.";
            return false;
        }
        if (decoded_ring.back().x != x0 || decoded_ring.back().y != y0) {
            // If the previous lineto didn't already close the polygon (WHICH IT SHOULD NOT)
            // close out the polygon ring.
            decoded_ring.add_coord(x0, y0);
            ring_area += calculate_segment_area(x1_, y1_, x0_, y0_);
        }

        cmd = polygon.ring_next(x0, y0, false);
        if (cmd == GeometryPBF::end) {
            has_next_geometry = false;
        } else if (cmd != GeometryPBF::move_to) {
            LOG(ERROR) << "Vector Tile has POLYGON type geometry has invalid command after CLOSE command.";
            return false;
        }

        current_is_exterior = ring_area >= 0;

        if (first_ring) {
            first_ring = false;
            if (!current_is_exterior) {
                LOG(WARNING) << "First ring is CWW. Maybe wrong geometry... Skipping!";
                continue;
            }
        }

        if (!current_is_exterior && looking_for_exterior) {
            continue;
        }

        if (decoded_ring.size() > 2 && part_env.intersects(clip_box_)) {
            if (current_is_exterior) {
                decoded_mp.emplace_back();
                decoded_polygon = &decoded_mp.back();
                decoded_polygon->set_exterior_ring(std::move(decoded_ring));
                looking_for_exterior = false;
            } else {
                decoded_polygon->add_hole(std::move(decoded_ring));
            }
        } else if (current_is_exterior) {
            looking_for_exterior = true;
        }
    }

    std::vector<std::unique_ptr<ClipperLib::PolyTree>> output_polygons;
    if (!ClipMultiPolygon(decoded_mp, output_polygons, clip_polygon_)) {
        return false;
    }

    int64_t start_x = 0, start_y = 0;

    for (auto &polygons : output_polygons) {


        for (auto *polynode : polygons->Childs) {
            mapnik::geometry::multi_polygon<std::int64_t> new_mp;
            mapnik::vector_tile_impl::detail::process_polynode_branch(polynode, new_mp, 0.1);
            for (auto &new_polygon : new_mp) {
                if (!new_polygon.exterior_ring.empty() &&
                        WriteRing(new_polygon.exterior_ring, start_x, start_y, output_geometry)) {
                    geometry_written = true;
                    for (auto &interior_ring : new_polygon.interior_rings) {
                        WriteRing(interior_ring, start_x, start_y, output_geometry);
                    }
                }
            }
        }
    }
    return geometry_written;
}

void Subtiler::WritePoints(const std::vector<Subtiler::point_t> &points, protozero::packed_field_uint32 *output_geometry) {
    int64_t start_x = 0, start_y = 0;
    uint32_t num_points = static_cast<uint32_t>(points.size());
    output_geometry->add_element(1u | (num_points << 3));
    for (auto &p : points) {
        int32_t dx = static_cast<uint32_t>(p.first - start_x);
        int32_t dy = static_cast<uint32_t>(p.second - start_y);
        // Manual zigzag encoding.
        output_geometry->add_element(protozero::encode_zigzag32(dx));
        output_geometry->add_element(protozero::encode_zigzag32(dy));
        start_x = p.first;
        start_y = p.second;
    }
}

bool Subtiler::WriteLinestring(const mapnik::geometry::multi_line_string<int64_t> &multi_line,
                               protozero::packed_field_uint32 *output_geometry)
{
    bool success = false;
    int64_t start_x = 0, start_y = 0;
    for (auto const& line : multi_line) {
        std::size_t line_size = line.size();
        line_size -= repeated_point_count(line);
        if (line_size < 2)
        {
            continue;
        }

        success = true;

        unsigned line_to_length = static_cast<unsigned>(line_size) - 1;

        auto pt = line.begin();

        int64_t x = pt->x;
        int64_t y = pt->y;

        output_geometry->add_element(9); // move_to | (1 << 3)
        output_geometry->add_element(protozero::encode_zigzag32(static_cast<int32_t>(x - start_x)));
        output_geometry->add_element(protozero::encode_zigzag32(static_cast<int32_t>(y - start_y)));
        start_x = x;
        start_y = y;
        output_geometry->add_element(encode_length(line_to_length));
        for (++pt; pt != line.end(); ++pt) {
            x = pt->x;
            y = pt->y;
            int32_t dx = static_cast<int32_t>(x - start_x);
            int32_t dy = static_cast<int32_t>(y - start_y);
            if (dx == 0 && dy == 0) {
                continue;
            }
            output_geometry->add_element(protozero::encode_zigzag32(dx));
            output_geometry->add_element(protozero::encode_zigzag32(dy));
            start_x = x;
            start_y = y;
        }
    }
    return success;
}

bool Subtiler::WriteRing(const mapnik::geometry::linear_ring<std::int64_t> &linear_ring,
                         int64_t &start_x, int64_t &start_y,
                         protozero::packed_field_uint32 *output_geometry)
{
    std::size_t ring_size = linear_ring.size();
    ring_size -= repeated_point_count(linear_ring);
    if (ring_size < 3)
    {
        return false;
    }
    auto last_itr = linear_ring.end();
    if (linear_ring.front() == linear_ring.back())
    {
        --last_itr;
        --ring_size;
        if (ring_size < 3)
        {
            return false;
        }
    }
    unsigned line_to_length = static_cast<unsigned>(ring_size) - 1;
    auto pt = linear_ring.begin();
    int64_t x = pt->x;
    int64_t y = pt->y;
    output_geometry->add_element(9); // move_to | (1 << 3)
    output_geometry->add_element(protozero::encode_zigzag32(static_cast<int32_t>(x - start_x)));
    output_geometry->add_element(protozero::encode_zigzag32(static_cast<int32_t>(y - start_y)));
    start_x = x;
    start_y = y;
    output_geometry->add_element(encode_length(line_to_length));
    for (++pt; pt != last_itr; ++pt)
    {
        x = pt->x;
        y = pt->y;
        int32_t dx = static_cast<int32_t>(x - start_x);
        int32_t dy = static_cast<int32_t>(y - start_y);
        if (dx == 0 && dy == 0)
        {
            continue;
        }
        output_geometry->add_element(protozero::encode_zigzag32(dx));
        output_geometry->add_element(protozero::encode_zigzag32(dy));
        start_x = x;
        start_y = y;
    }
    output_geometry->add_element(15); // close_path
    return true;
}
