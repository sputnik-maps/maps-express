#include "tile_processor.h"

#include "rendermanager.h"
#include "data_manager.h"


TileProcessor::TileProcessor(RenderManager& render_manager) : render_manager_(render_manager) {}

TileProcessor::~TileProcessor() {
    CancelProcessing();
}

void TileProcessor::GetMetatile(std::shared_ptr<TileRequest> request, std::shared_ptr<TileTask> task) {
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
    }
}

uint TileProcessor::GetStyleVersion(const std::string& style_name) {
    return render_manager_.GetStyleVersion(style_name);
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
        return;
    }
    TileId data_tile_id = tile_request_->tile_id;
    int zoom_offset = endpoint_params.zoom_offset;
    if (zoom_offset < 0) {
        data_tile_id = GetUpperZoom(data_tile_id, -zoom_offset);
    }
    pending_work_ = data_provider->GetTile(
                std::bind(&TileProcessor::OnLoadSuccess, this, std::placeholders::_1),
                [this](LoadError err) {
                    if (err == LoadError::not_found) {
                        tile_task_->NotifyError(Error::not_found);
                    } else {
                        tile_task_->NotifyError(Error::internal);
                    }
                    pending_work_.reset();
                },
                data_tile_id,  tile_request_->data_version);
}

void TileProcessor::OnLoadSuccess(Tile&& tile) {
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
    tile_task_->SetResult(std::move(result));
    pending_work_.reset();
}

void TileProcessor::OnRenderError() {
    tile_task_->NotifyError(Error::rendering);
    pending_work_.reset();
}
