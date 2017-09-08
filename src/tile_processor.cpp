#include "tile_processor.h"

#include <glog/logging.h>

#include "rendermanager.h"
#include "data_manager.h"


TileProcessor::TileProcessor(RenderManager& render_manager) : render_manager_(render_manager) {}

TileProcessor::~TileProcessor() {
    CancelProcessing();
}

void TileProcessor::GetMetatile(std::shared_ptr<TileRequest> request, std::shared_ptr<TileTask> task) {
#ifndef NDEBUG
    LOG(INFO) << "Starting processing of metatile: " << request->metatile_id
              << " style:" << request->endpoint_params->style_name;
#endif
    assert(request);
    tile_request_ = std::move(request);
    assert(task);
    tile_task_ = std::move(task);
    assert(tile_request_->endpoint_params);
    const EndpointParams& endpoint_params = *tile_request_->endpoint_params;
    if (endpoint_params.data_provider) {
        LoadTile();
    } else if (endpoint_params.type == EndpointType::render) {
        ProcessRender();
    } else if (endpoint_params.type == EndpointType::mvt) {
        ProcessMvt();
    } else {
        tile_task_->NotifyError(Error::internal);
        Finish();
    }
}

void TileProcessor::CancelProcessing() {
    if (pending_work_) {
        pending_work_->cancel();
    }
}

void TileProcessor::LoadTile() {
    const EndpointParams& endpoint_params = *tile_request_->endpoint_params;
    auto& data_provider = endpoint_params.data_provider;
    if (!(data_provider && data_provider->HasVersion(tile_request_->data_version))) {
        tile_task_->NotifyError(Error::not_found);
        Finish();
        return;
    }
    auto load_tile_task = std::make_shared<TileLoader::LoadTask>(
                std::bind(&TileProcessor::OnLoadSuccess, this, std::placeholders::_1),
                [this](TileLoader::LoadError err) {
#ifndef NDEBUG
                    LOG(ERROR) << "Error while getting data for metatile: " << tile_request_->metatile_id
                               << " style:" << tile_request_->endpoint_params->style_name;
#endif
                    if (err == TileLoader::LoadError::not_found) {
                        tile_task_->NotifyError(Error::not_found);
                    } else {
                        tile_task_->NotifyError(Error::internal);
                    }
                    Finish();
                }, false);
    pending_work_ = load_tile_task;
    data_provider->GetTile(std::move(load_tile_task), tile_request_->tile_id, endpoint_params.zoom_offset,
                           tile_request_->data_version);
}

void TileProcessor::OnLoadSuccess(Tile&& tile) {
#ifndef NDEBUG
    LOG(INFO) << "Successfully loaded tile: " << tile.id;
#endif
    pending_work_.reset();
    const EndpointParams& endpoint_params = *tile_request_->endpoint_params;
    if (endpoint_params.type == EndpointType::static_files) {
        Metatile metatile(tile_request_->metatile_id);
        assert(!metatile.tiles.empty());
        assert(metatile.tiles.front().id == tile.id);
        metatile.tiles.front().data = std::move(tile.data);
        tile_task_->SetResult(std::move(metatile));
        return;
    }

    data_tile_ = std::make_shared<Tile>(std::move(tile));
    if (endpoint_params.type == EndpointType::render) {
        ProcessRender();
    } else {
        ProcessMvt();
    }
}


void TileProcessor::ProcessRender() {
    const EndpointParams& endpoint_params = *tile_request_->endpoint_params;
    auto render_request = std::make_unique<RenderRequest>(tile_request_->metatile_id);
    render_request->style_name = endpoint_params.style_name;
    render_request->data_tile = std::move(data_tile_);
    render_request->retina = tile_request_->tags.find("retina") != tile_request_->tags.end();
    pending_work_ = render_manager_.Render(std::move(render_request),
                               std::bind(&TileProcessor::OnRenderSuccess, this, std::placeholders::_1),
                               std::bind(&TileProcessor::OnRenderError, this));
}

void TileProcessor::ProcessMvt() {
    auto subtile_request = std::make_unique<SubtileRequest>(std::move(*data_tile_), tile_request_->tile_id);
    subtile_request->filter_table = tile_request_->endpoint_params->filter_table;
    subtile_request->layers = std::move(tile_request_->layers);

    pending_work_ = render_manager_.MakeSubtile(std::move(subtile_request),
                                    std::bind(&TileProcessor::OnRenderSuccess, this, std::placeholders::_1),
                                    std::bind(&TileProcessor::OnRenderError, this));
}

void TileProcessor::OnRenderSuccess(Metatile&& result) {
#ifndef NDEBUG
    LOG(INFO) << "Successfully processed metatile: " << tile_request_->metatile_id
              << " style:" << tile_request_->endpoint_params->style_name;
#endif
    tile_task_->SetResult(std::move(result));
    Finish();
}

void TileProcessor::OnRenderError() {
#ifndef NDEBUG
    LOG(ERROR) << "Error while processing metatile: " << tile_request_->metatile_id
              << " style:" << tile_request_->endpoint_params->style_name;
#endif
    tile_task_->NotifyError(Error::rendering);
    Finish();
}

inline void TileProcessor::Finish() {
    pending_work_.reset();
    delete this;
}
