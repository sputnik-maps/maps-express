#pragma once

#include "tile_loader.h"

class FileLoader : public TileLoader {
public:
    FileLoader(const std::string& base_path = "", bool auto_version = false);

    void Load(std::shared_ptr<LoadTask> task, const TileId& tile_id, const std::string& version = "") override;

    bool HasVersion(const std::string& version) const override;

private:
    std::string base_path_;
    bool auto_version_;
};
