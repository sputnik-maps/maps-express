#pragma once

#include <memory>
#include <string>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <libcouchbase/couchbase.h>

#include "config.h"
#include "couchbase_worker.h"
#include "thread_pool.h"
#include "tile_cacher.h"


class CouchbaseCacher : public TileCacher {
public:
    CouchbaseCacher(const std::vector<std::string>& hosts, const std::string& user = "",
                    const std::string& password = "", uint num_workers = 2);
    ~CouchbaseCacher();

    void Get(const std::string& key, std::shared_ptr<GetTask> task) override;
    void Set(const std::string& key, std::shared_ptr<const CachedTile> cached_tile,
             std::chrono::seconds expire_time, std::shared_ptr<SetTask> task) override;
    void Touch(const std::string& key, std::chrono::seconds expire_time) override;
    bool LockUntilSet(const std::vector<std::string>& keys) override;
    void Unlock(const std::vector<std::string>& keys) override;

    void OnTileRetrieved(const std::string& key, std::shared_ptr<CachedTile> cached_tile);
    void OnRetrieveError(const std::string& key);
    void OnTileSet(const std::string& key);
    void OnSetError(const std::string& key);


private:
    using workers_pool_t = ThreadPool<CouchbaseWorker, CBWorkTask>;
    using waiters_vec_t = std::vector<std::shared_ptr<GetTask>>;
    using tmp_cache_t = std::unordered_map<std::string, std::shared_ptr<const CachedTile>>;
    workers_pool_t workers_pool_;
    std::unordered_map<std::string, waiters_vec_t> get_waiters_;
    std::unordered_map<std::string, waiters_vec_t> set_waiters_;
    tmp_cache_t tmp_cache_;
    std::mutex mux_;
    std::mutex tmp_cache_mux_;
};


