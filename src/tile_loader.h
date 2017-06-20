#pragma once

#include <string>
#include <memory>

#include "async_task.h"
#include "tile.h"

enum class LoadError {
    internal_error,
    not_found
};

using LoadTask = AsyncTask<Tile&&, LoadError>;

class TileLoader {
public:
    virtual void Load(std::shared_ptr<LoadTask> task, const TileId& tile_id,
                      const std::string& version = "") = 0;

    virtual bool HasVersion(const std::string& version) const = 0;
};
