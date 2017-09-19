#include "tile_processing_manager.h"

#include <glog/logging.h>

#include "rendermanager.h"
#include "data_manager.h"


class TileProcessor {
public:
    using Error = TileProcessingManager::Error;
    using TileTask = TileProcessingManager::TileTask;
    using hook_t = TileProcessingManager::processors_store_t::iterator;

    TileProcessor(RenderManager& render_manager, TileProcessingManager& processing_manager, const hook_t& hook);
    ~TileProcessor();

    void GetMetatile(std::shared_ptr<TileRequest> request, std::shared_ptr<TileTask> task);

    void CancelProcessing();

private:
    void LoadTile();
    void OnLoadSuccess(Tile&& tile);
    void ProcessRender();
    void ProcessMvt();
    void OnRenderSuccess(Metatile&& result);
    void OnRenderError();
    void Finish();

    RenderManager& render_manager_;
    TileProcessingManager& processing_manager_;
    hook_t hook_;

    std::shared_ptr<TileTask> tile_task_;
    std::shared_ptr<TileRequest> tile_request_;
    std::shared_ptr<Tile> data_tile_;
    std::shared_ptr<AsyncTaskBase> pending_work_;

    friend class TileProcessingManager;
};

TileProcessor::TileProcessor(RenderManager& render_manager, TileProcessingManager& processing_manager,
                             const hook_t& hook) :
    render_manager_(render_manager),
    processing_manager_(processing_manager),
    hook_(hook) {}

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
    processing_manager_.NotifyDone(*this);
}


TileProcessingManager::TileProcessingManager(RenderManager& render_manager, uint max_processors,
                                             uint unlock_threshold) :
    render_manager_(render_manager),
    max_processors_(max_processors),
    unlock_threshold_(unlock_threshold) {}

TileProcessingManager::~TileProcessingManager() {

}

bool TileProcessingManager::GetMetatile(std::shared_ptr<TileRequest> request,
                                        std::shared_ptr<TileTask> task) {
    processors_store_t::iterator itr;
    bool locked = false;
    {
        std::lock_guard<std::mutex> lock(mux_);
        if (locked_) {
            return false;
        }
        processors_.emplace_front();
        itr = processors_.begin();
        ++num_processors_;
        if (num_processors_ >= max_processors_) {
            locked_ = true;
            locked = true;
        }
    }
    auto tile_processor = std::make_unique<TileProcessor>(render_manager_, *this, itr);
    *itr = std::move(tile_processor);
    (*itr)->GetMetatile(std::move(request), std::move(task));
    if (locked) {
        LOG(WARNING) << "Tile processing tasks limit (" << max_processors_ << ") exceeded!";
    }
    return true;
}

void TileProcessingManager::NotifyDone(TileProcessor& processor) {
    std::unique_ptr<TileProcessor> processor_ptr;
    bool unlocked = false;
    {
        std::lock_guard<std::mutex> lock(mux_);
        processor_ptr = std::move(*processor.hook_);
        processors_.erase(processor.hook_);
        --num_processors_;
        if (locked_ && num_processors_ <= unlock_threshold_) {
            locked_ = false;
            unlocked = true;
        }
    }
    if (unlocked) {
        LOG(INFO) << "Tile processing unlocked!";
    }
}


