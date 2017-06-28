#include "couchbase_cacher.h"


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

CouchbaseCacher::~CouchbaseCacher() { }


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
