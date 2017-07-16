#include "tile_cacher.h"

#include <folly/io/async/EventBaseManager.h>


TileCacher::~TileCacher() {
// TODO: maybe notify all waiters
}

void TileCacher::Get(const std::string& key, std::shared_ptr<GetTask> task) {
    assert(!key.empty());
    {
        std::unique_lock<std::mutex> lock(mux_);
        // First check tmp chache
        auto tmp_cache_itr = tmp_cache_.find(key);
        if (tmp_cache_itr != tmp_cache_.end()) {
            auto tile = tmp_cache_itr->second;
            lock.unlock();
            task->SetResult(std::move(tile));
            return;
        }
        // Check if this tile was locked until set operation
        auto locked_waiters_itr = set_waiters_.find(key);
        if (locked_waiters_itr != set_waiters_.end()) {
            waiters_vec_t& waiters_vec = locked_waiters_itr->second;
            waiters_vec.push_back(std::move(task));
            return;
        }
        // Check if this tile was alredy requested
        auto waiters_itr = get_waiters_.find(key);
        if (waiters_itr != get_waiters_.end()) {
            // Tile alredy requested
            waiters_vec_t& waiters_vec = waiters_itr->second;
            waiters_vec.push_back(std::move(task));
            return;
        } else {
            get_waiters_[key] = { task };
        }
    }
    GetImpl(key);
}

void TileCacher::Set(const std::string& key, std::shared_ptr<const CachedTile> cached_tile,
                          std::chrono::seconds expire_time, std::shared_ptr<SetTask> task) {
    // TODO: notify CacherSetTask
    assert(!key.empty());
    waiters_vec_t waiters_vec;
    {
        std::lock_guard<std::mutex> lock(mux_);
        tmp_cache_[key] = cached_tile;
        auto set_waiters_itr = set_waiters_.find(key);
        if (set_waiters_itr != set_waiters_.end()) {
            waiters_vec = std::move(set_waiters_itr->second);
        }
        set_waiters_.erase(set_waiters_itr);
    }
    for (auto get_task : waiters_vec) {
        get_task->SetResult(cached_tile);
    }
    SetImpl(key, cached_tile, expire_time);
    folly::EventBase* evb = folly::EventBaseManager::get()->getExistingEventBase();
    if (evb) {
        evb->runAfterDelay([this, key]{
            std::lock_guard<std::mutex> lock(mux_);
            tmp_cache_.erase(key);
        }, 60000);
    }
}

void TileCacher::Touch(const std::string& key, std::chrono::seconds expire_time) {
    assert(!key.empty());
    TouchImpl(key, expire_time);
}

std::unique_ptr<CacherLock> TileCacher::LockUntilSet(std::vector<std::string> keys) {
    bool locked = false;
    std::vector<std::string> locked_keys;
    locked_keys.reserve(keys.size());
    {
        std::lock_guard<std::mutex> lock(mux_);
        for (const std::string& key : keys) {
            if (set_waiters_.find(key) == set_waiters_.end()) {
                set_waiters_[key] = {};
                locked = true;
                locked_keys.push_back(key);
            }
        }
    }
    if (!locked) {
        return nullptr;
    }
    return std::make_unique<CacherLock>(*this, std::move(keys));
}

void TileCacher::Unlock(const std::vector<std::string>& keys) {
    for (const std::string& key : keys) {
        waiters_vec_t waiters;
        {
            std::lock_guard<std::mutex> lock(mux_);
            auto set_waiters_itr = set_waiters_.find(key);
            if (set_waiters_itr == set_waiters_.end()) {
                continue;
            }
            waiters = std::move(set_waiters_itr->second);
            set_waiters_.erase(set_waiters_itr);
        }
        for (auto& get_task : waiters) {
            get_task->NotifyError();
        }
    }
}

void TileCacher::OnTileRetrieved(const std::string& key, std::shared_ptr<CachedTile> cached_tile) {
    waiters_vec_t waiters;
    tmp_cache_t::iterator tmp_cache_itr;
    {
        std::lock_guard<std::mutex> lock(mux_);
        auto waiters_itr = get_waiters_.find(key);
        if (waiters_itr == get_waiters_.end()) {
            return;
        }
        tmp_cache_[key] = cached_tile;
        waiters = std::move(waiters_itr->second);
        get_waiters_.erase(waiters_itr);
    }
    for (auto& async_task : waiters) {
        async_task->SetResult(cached_tile);
    }
    std::lock_guard<std::mutex> lock(mux_);
    tmp_cache_.erase(key);
}

void TileCacher::OnRetrieveError(const std::string& key) {
    waiters_vec_t waiters;
    {
        std::lock_guard<std::mutex> lock(mux_);
        auto waiters_itr = get_waiters_.find(key);
        if (waiters_itr == get_waiters_.end()) {
            return;
        }
        waiters = std::move(waiters_itr->second);
        get_waiters_.erase(waiters_itr);
    }
    for (auto& async_task : waiters) {
        async_task->NotifyError();
    }
}

void TileCacher::OnTileSet(const std::string& key) {

}

void TileCacher::OnSetError(const std::string& key) {
    // TODO;
}
