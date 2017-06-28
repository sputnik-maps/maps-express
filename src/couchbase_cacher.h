#pragma once

#include <memory>
#include <string>
#include <vector>

#include "config.h"
#include "couchbase_worker.h"
#include "thread_pool.h"
#include "tile_cacher.h"


class CouchbaseCacher : public TileCacher {
public:
    CouchbaseCacher(const std::vector<std::string>& hosts, const std::string& user = "",
                    const std::string& password = "", uint num_workers = 2);
    ~CouchbaseCacher();

//    void Get(const std::string& key, std::shared_ptr<GetTask> task) override;
//    void Set(const std::string& key, std::shared_ptr<const CachedTile> cached_tile,
//             std::chrono::seconds expire_time, std::shared_ptr<SetTask> task) override;
//    void Touch(const std::string& key, std::chrono::seconds expire_time) override;
//    bool LockUntilSet(const std::vector<std::string>& keys) override;
//    void Unlock(const std::vector<std::string>& keys) override;

//    void OnTileRetrieved(const std::string& key, std::shared_ptr<CachedTile> cached_tile);
//    void OnRetrieveError(const std::string& key);
//    void OnTileSet(const std::string& key);
//    void OnSetError(const std::string& key);


private:
    void GetImpl(const std::string& key) override;
    void SetImpl(const std::string& key, std::shared_ptr<const CachedTile> cached_tile,
                         std::chrono::seconds expire_time) override;
    void TouchImpl(const std::string& key, std::chrono::seconds expire_time) override;

    using workers_pool_t = ThreadPool<CouchbaseWorker, CBWorkTask>;
    workers_pool_t workers_pool_;
};


