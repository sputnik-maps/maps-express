#include "util.h"

#include <string>

#include <vector_tile_compression.hpp>

namespace util {

void decompress(const char *data, size_t data_size, std::string& uncomp) {
    if (data_size > 2) {
        if ( (static_cast<uint8_t>(data[0]) == 0x78 && static_cast<uint8_t>(data[1])) ||
                (static_cast<uint8_t>(data[0]) == 0x1F && static_cast<uint8_t>(data[1])) ) {
            mapnik::vector_tile_impl::zlib_decompress(data, data_size, uncomp);
            return;
        }
    }
    uncomp.assign(data, data_size);
}

std::unique_ptr<std::set<std::string>> ParseArray(const std::string& layers) {
    auto layers_set = make_unique<std::set<std::string>>();
    split(layers, *layers_set, ",");
    return layers_set;
}

ExtensionType str2ext(std::string& ext) {
    if (ext == "png")
        return ExtensionType::png;
    else if (ext == "mvt")
        return ExtensionType::mvt;
    else if (ext == "json")
        return ExtensionType::json;
    return ExtensionType::none;

}
std::string ext2str(ExtensionType aExt) {
    switch(aExt) {
    case ExtensionType::html: { return "html"; } break;
    case ExtensionType::json: { return "json"; } break;
    case ExtensionType::mvt: { return "mvt"; } break;
    case ExtensionType::png: { return "png"; } break;
    default:
        return "unknown";
    }
}
    
} // ns util

