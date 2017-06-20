#pragma once

#include <list>
#include <set>
#include <string>

#include <mapnik/map.hpp>

#include "async_task.h"
#include "filter_table.h"
#include "tile.h"
#include "worker.h"

enum class RenderType : std::uint8_t {
    png,
    utfgrid
};

struct TileWorkRequest {
    virtual ~TileWorkRequest() {}
};

struct RenderRequest : public TileWorkRequest {

    RenderRequest() = default;

    RenderRequest(MetatileId _metatile_id) : metatile_id(_metatile_id) {}

    MetatileId metatile_id;
    std::string style_name;
    std::string utfgrid_key;
    std::shared_ptr<Tile> data_tile;
    std::unique_ptr<std::set<std::string>> layers;
    RenderType render_type{RenderType::png};
    bool retina{false};
};

struct SubtileRequest : public TileWorkRequest {

    SubtileRequest() = default;

    SubtileRequest(Tile mvt_tile_, TileId tile_id_) :
        mvt_tile(std::move(mvt_tile_)),
        tile_id(tile_id_) {}

    Tile mvt_tile;
    TileId tile_id;
    std::shared_ptr<FilterTable> filter_table;
    std::unique_ptr<std::set<std::string>> layers;
};

using RenderTask = AsyncTask<Metatile&&>;

struct TileWorkTask {
    std::shared_ptr<RenderTask> async_task;
    std::unique_ptr<TileWorkRequest> request;
};

struct StyleInfo {
    enum class Type : std::uint8_t {
        mapnik,
        mvt
    };

    std::string name;
    std::string path;
    std::string base_path;
    std::shared_ptr<std::string> data;
    uint version{0};
    Type type{Type::mapnik};
    bool allow_grid_render{false};
};


class RenderWorker : public Worker<TileWorkTask> {
public:
    using styles_t = std::vector<StyleInfo>;

    RenderWorker(std::shared_ptr<const styles_t> styles = nullptr);

    bool Init() noexcept override;
    virtual void ProcessTask(TileWorkTask task) noexcept override;

    static void CalculateLayersSD(mapnik::Map& map);

    bool UpdateStyles(const styles_t& styles);
    bool CommitUpdate(const styles_t* update_ptr);
    bool CancelUpdate(const styles_t* update_ptr);

private:
    struct MapInfo {
        MapInfo(int width, int height, std::string const& srs) : map(width, height, srs) {}

        mapnik::Map map;
        std::vector<mapnik::layer*> mvt_layers;
        std::vector<mapnik::layer*> standard_layers;
        uint version{0};
        bool allow_grid_render{false};
    };

    std::shared_ptr<MapInfo> LoadStyle(const StyleInfo& style_info);
    void ProcessRender(RenderTask& async_task, const RenderRequest& render_request) noexcept;
    void ProcessSubtile(RenderTask& async_task, SubtileRequest& subtile_request) noexcept;

    std::unordered_map<std::string, std::shared_ptr<MapInfo>> maps_;
    std::unordered_map<std::string, std::shared_ptr<MapInfo>> updated_maps_;
    std::shared_ptr<const styles_t> styles_;
    const styles_t* pending_update_ptr_{nullptr};

};
