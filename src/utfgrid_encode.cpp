#include "utfgrid_encode.h"

#include <unordered_map>

#include <glog/logging.h>
#include <jsoncpp/json/value.h>
#include <mapnik/unicode.hpp>

struct value_to_json_visitor {
    Json::Value operator() (const mapnik::value_null& val) {return Json::Value();}
    Json::Value operator() (const mapnik::value_bool& val) {return Json::Value(val);}
    Json::Value operator() (const mapnik::value_integer& val) {return Json::Value(static_cast<uint>(val));}
    Json::Value operator() (const mapnik::value_double& val) {return Json::Value(val);}

    Json::Value operator() (const mapnik::value_unicode_string& val) {
        std::string utf8_str;
        mapnik::to_utf8(val, utf8_str);
        return Json::Value(utf8_str);
    }
};

std::string encode_utfgrid(const mapnik::grid_view& utfgrid, uint size) {
    Json::Value root(Json::objectValue);
    Json::Value& jgrid = root["grid"];
    jgrid = Json::Value(Json::arrayValue);

    using lookup_type = mapnik::grid::lookup_type;
    using value_type = mapnik::grid::value_type;
    using feature_type = mapnik::grid::feature_type;
    using keys_type = std::unordered_map<lookup_type, value_type>;

    std::vector<lookup_type> key_order;
    keys_type keys;

    const mapnik::grid::feature_key_type& feature_keys = utfgrid.get_feature_keys();

    std::uint16_t codepoint = 32;
    for (uint y = 0; y < utfgrid.height(); y += size) {
        std::string line;
        const value_type* row = utfgrid.get_row(y);
        for (uint x = 0; x < utfgrid.width(); x += size) {
            value_type feature_id = row[x];
            auto feature_itr = feature_keys.find(feature_id);
            lookup_type val;
            if (feature_itr == feature_keys.end()) {
                feature_id = mapnik::grid::base_mask;
            } else {
                val = feature_itr->second;
            }
            auto key_iter = keys.find(val);
            if (key_iter == keys.end()) {
                // Create a new entry for this key. Skip the codepoints that
                // can't be encoded directly in JSON.
                if (codepoint == 34) ++codepoint;      // Skip "
                else if (codepoint == 92) ++codepoint; // Skip backslash
                if (feature_id == mapnik::grid::base_mask) {
                    keys[""] = codepoint;
                    key_order.push_back("");
                } else {
                    keys[val] = codepoint;
                    key_order.push_back(val);
                }
                line.append(reinterpret_cast<char*>(&codepoint), sizeof(codepoint));
                ++codepoint;
            } else {
                line.append(reinterpret_cast<char*>(&key_iter->second), sizeof(key_iter->second));
            }
        }
        jgrid.append(Json::Value(line));
    }

    Json::Value& jkeys = root["keys"];
    jkeys = Json::Value(Json::arrayValue);
    for (const auto& key_id : key_order) {
        jkeys.append(key_id);
    }

    Json::Value& jdata = root["data"];

    const feature_type& g_features = utfgrid.get_grid_features();
    const std::set<std::string>& attributes = utfgrid.get_fields();
    feature_type::const_iterator feat_end = g_features.end();
    for (const std::string& key_item : key_order)
    {
        if (key_item.empty()) {
            continue;
        }

        feature_type::const_iterator feat_itr = g_features.find(key_item);
        if (feat_itr == feat_end) {
            continue;
        }

        bool found = false;
        Json::Value jfeature(Json::objectValue);
        mapnik::feature_ptr feature = feat_itr->second;
        for (const std::string& attr : attributes) {
            value_to_json_visitor val_to_json;
            if (attr == "__id__") {
                jfeature[attr] = static_cast<uint>(feature->id());
            } else if (feature->has_key(attr)) {
                found = true;
                jfeature[attr] = mapnik::util::apply_visitor(val_to_json, feature->get(attr));
            }
        }

        if (found) {
            jdata[feat_itr->first] = jfeature;
        }
    }

    return root.toStyledString();
}
