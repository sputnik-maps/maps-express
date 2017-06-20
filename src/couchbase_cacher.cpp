#include "couchbase_cacher.h"

#include <glog/logging.h>


CouchbaseCacher::CouchbaseCacher(const std::vector<std::string>& hosts, const std::string& user,
                                 const std::string& password, uint num_workers) {
    std::string conn_str;
    if (hosts.empty()) {
        conn_str = "couchbase://localhost";
    } else {
        auto host_itr = hosts.begin();
        conn_str = "couchbase://" + *host_itr++;
        for ( ; host_itr != hosts.end(); ++host_itr) {
            conn_str.append(",");
            conn_str.append(*host_itr);
        }
    }

    for (uint i = 0; i < num_workers; ++i) {
        auto worker = std::make_unique<CouchbaseWorker>(*this, conn_str, user, password);
        workers_pool_.PushWorker(std::move(worker));
    }
}

CouchbaseCacher::~CouchbaseCacher() {
// TODO: maybe notify all waiters
}

void CouchbaseCacher::Get(const std::string& key, std::shared_ptr<GetTask> task) {
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
    CBWorkTask cb_task{nullptr, key, {}, CBWorkTask::Type::get};
    workers_pool_.PostTask(std::move(cb_task));
}

void CouchbaseCacher::Set(const std::string& key, std::shared_ptr<const CachedTile> cached_tile,
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
    CBWorkTask cb_task{cached_tile, key, expire_time, CBWorkTask::Type::set};
    workers_pool_.PostTask(std::move(cb_task));
    for (auto get_task : waiters_vec) {
        get_task->SetResult(cached_tile);
    }
}

void CouchbaseCacher::Touch(const std::string& key, std::chrono::seconds expire_time) {
    assert(!key.empty());
    CBWorkTask cb_task{nullptr, key, expire_time, CBWorkTask::Type::touch};
    workers_pool_.PostTask(std::move(cb_task));
}

bool CouchbaseCacher::LockUntilSet(const std::vector<std::string>& keys) {
    bool locked = false;
    std::lock_guard<std::mutex> lock(mux_);
    for (const std::string& key : keys) {
        if (set_waiters_.find(key) == set_waiters_.end()) {
            set_waiters_[key] = {};
            locked = true;
        }
    }
    return locked;
}

void CouchbaseCacher::Unlock(const std::vector<std::string>& keys) {
    for (const std::string& key : keys) {
        waiters_vec_t waiters;
        {
            std::lock_guard<std::mutex> lock(mux_);
            auto set_waiters_itr = set_waiters_.find(key);
            if (set_waiters_itr == set_waiters_.end()) {
                return;
            }
            waiters = std::move(set_waiters_itr->second);
            set_waiters_.erase(set_waiters_itr);
        }
        for (auto& get_task : waiters) {
            get_task->NotifyError();
        }
    }
}

void CouchbaseCacher::OnTileRetrieved(const std::string& key, std::shared_ptr<CachedTile> cached_tile) {
    waiters_vec_t waiters;
    tmp_cache_t::iterator tmp_cache_itr;
    {
        std::lock_guard<std::mutex> lock(mux_);
        auto waiters_itr = get_waiters_.find(key);
        if (waiters_itr == get_waiters_.end()) {
            return;
        }
        auto emplace_res = tmp_cache_.emplace(key, cached_tile);
        tmp_cache_itr = emplace_res.first;
        waiters = std::move(waiters_itr->second);
        get_waiters_.erase(waiters_itr);
    }
    for (auto& async_task : waiters) {
        async_task->SetResult(cached_tile);
    }
    std::lock_guard<std::mutex> lock(mux_);
    tmp_cache_.erase(tmp_cache_itr);
}

void CouchbaseCacher::OnRetrieveError(const std::string& key) {
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

void CouchbaseCacher::OnTileSet(const std::string& key) {
    std::lock_guard<std::mutex> lock(mux_);
    tmp_cache_.erase(key);
}

void CouchbaseCacher::OnSetError(const std::string& key) {
    std::lock_guard<std::mutex> lock(mux_);
    tmp_cache_.erase(key);
}
