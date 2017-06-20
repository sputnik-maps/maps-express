#pragma once

#include <functional>
#include <unordered_set>

#include "config.h"
#include "renderworker.h"
#include "thread_pool.h"

class RenderManager;
class StyleUpdateObserver : public Config::ConfigObserver {
public:
    StyleUpdateObserver(RenderManager& rm) : rm_(rm) {}

private:
    void OnUpdate(std::shared_ptr<Json::Value> value) override;

    RenderManager& rm_;
};

using render_result_t = Metatile&&;

class RenderManager {
public:
    RenderManager(Config& config);

    // If this method is called from event base thread, callbacks will be called in this thread too.
    std::shared_ptr<RenderTask> Render(std::unique_ptr<RenderRequest> request,
                                       std::function<void(render_result_t&&)> success_callback,
                                       std::function<void()> error_callback = std::function<void()>());

    bool RenderSync(std::unique_ptr<RenderRequest>, render_result_t& output);

    std::shared_ptr<RenderTask> MakeSubtile(std::unique_ptr<SubtileRequest> request,
                                            std::function<void(render_result_t&&)> success_callback,
                                            std::function<void()> error_callback = std::function<void()>());

    void PostStyleUpdate(std::shared_ptr<const Json::Value> jstyles);

    inline bool has_style(const std::string& style_name) {
        auto style_names = std::atomic_load(&style_names_);
        return style_names->find(style_name) != style_names->end();
    }


private:
    void TryProcessStyleUpdate();
    void UpdateWorker(RenderWorker& worker);
    void FinishUpdate();

    using render_pool_t = ThreadPool<RenderWorker, TileWorkTask>;
    render_pool_t render_pool_;
    std::shared_ptr<std::unordered_set<std::string>> style_names_;

    std::shared_ptr<const Json::Value> styles_update_;
    std::vector<StyleInfo> pending_update_;
    std::vector<const RenderWorker*> workers_to_update_;
    std::vector<const RenderWorker*> updated_workers_;
    std::atomic_bool updating_{false};
    std::atomic_bool inited_{false};

    StyleUpdateObserver update_observer_;

    Config& config_;
};
