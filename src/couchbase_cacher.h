#pragma once

#include <memory>
#include <string>
#include <vector>

#include "config.h"
#include "couchbase_worker.h"
#include "thread_pool.h"
#include "tile_cacher.h"


namespace folly {
namespace fibers {
class Semaphore;
} // ns fibers
} // ns folly

class CouchbaseCacher : public TileCacher {
public:
    CouchbaseCacher(const std::string& conn_str, const std::string& user = "",
                    const std::string& password = "", uint num_workers = 2);
    ~CouchbaseCacher();

    void WaitForInit();

private:
    void GetImpl(const std::string& key) override;
    void SetImpl(const std::string& key, std::shared_ptr<const CachedTile> cached_tile,
                         std::chrono::seconds expire_time) override;
    void TouchImpl(const std::string& key, std::chrono::seconds expire_time) override;

    using workers_pool_t = ThreadPool<CouchbaseWorker, CBWorkTask>;
    workers_pool_t workers_pool_;
    std::unique_ptr<folly::fibers::Semaphore> sem_;
};


