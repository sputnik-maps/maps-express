#include "couchbase_cacher.h"

#include <folly/fibers/Semaphore.h>


CouchbaseCacher::CouchbaseCacher(const std::string& conn_str, const std::string& user,
                                 const std::string& password, uint num_workers) {
    sem_ = std::make_unique<folly::fibers::Semaphore>(num_workers);
    for (uint i = 0; i < num_workers; ++i) {
        auto worker = std::make_unique<CouchbaseWorker>(*this, conn_str, user, password);
        workers_pool_.PushWorker(std::move(worker), [&](workers_pool_t::worker_t*) { sem_->signal(); }, {});
    }
}

CouchbaseCacher::~CouchbaseCacher() { }


void CouchbaseCacher::WaitForInit() {
    sem_->wait();
}

void CouchbaseCacher::GetImpl(const std::string& key) {
    CBWorkTask cb_task{nullptr, key, {}, CBWorkTask::Type::get};
    workers_pool_.PostTask(std::move(cb_task));
}

void CouchbaseCacher::SetImpl(const std::string& key, std::shared_ptr<const CachedTile> cached_tile,
                              std::chrono::seconds expire_time) {
    CBWorkTask cb_task{cached_tile, key, expire_time, CBWorkTask::Type::set};
    workers_pool_.PostTask(std::move(cb_task));
}

void CouchbaseCacher::TouchImpl(const std::string& key, std::chrono::seconds expire_time) {
    CBWorkTask cb_task{nullptr, key, expire_time, CBWorkTask::Type::touch};
    workers_pool_.PostTask(std::move(cb_task));
}
