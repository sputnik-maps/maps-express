#include "fileloader.h"

#include <fstream>
#include <sstream>

#include "util.h"

FileLoader::FileLoader(const std::string& base_path, bool auto_version) : auto_version_(auto_version) {
    if (base_path.empty()) {
        base_path_ = "./";
    } else {
        base_path_ = base_path;
        if (base_path_.back() != '/') {
            base_path_.append("/");
        }
    }
}

void FileLoader::Load(std::shared_ptr<LoadTask> task, const TileId& tile_id, const std::string& version) {
    std::stringstream ss;
    ss << base_path_;
    if (auto_version_) {
        ss << version << '/';
    }
    ss << tile_id.z << '/' << tile_id.x << '/' << tile_id.y << ".mvt";
    std::string path = ss.str();
    std::ifstream file;
    file.open(path);
    if (file.is_open()) {
        std::string tile_data((std::istreambuf_iterator<char>(file)),
                              (std::istreambuf_iterator<char>()));
        file.close();
        Tile tile{tile_id, ""};
        util::decompress(tile_data, tile.data);
        task->SetResult(std::move(tile));
        return;
    }
    task->NotifyError(LoadError::internal_error);
}

bool FileLoader::HasVersion(const std::string& version) const {
    // TODO: Implement
    return true;
}
