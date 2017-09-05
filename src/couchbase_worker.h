#pragma once

#include <chrono>

#include <libcouchbase/couchbase.h>

#include "worker.h"


struct CachedTile;
class CouchbaseCacher;

struct CBWorkTask {
    enum class Type : std::uint8_t {
        get,
        set,
        touch
    };

    std::shared_ptr<const CachedTile> tile;
    std::string key;
    std::chrono::seconds expire_time;
    Type type;
};


class CouchbaseWorker : public Worker<CBWorkTask> {
public:
    CouchbaseWorker(CouchbaseCacher& cacher, const std::string& conn_str,
                    const std::string& user = "", const std::string& password = "");
    ~CouchbaseWorker();

    bool Init() noexcept override;
    void ProcessTask(CBWorkTask task) noexcept override;

private:
    bool Connect();
    void ProcessGet(const std::string& key) noexcept;
    void ProcessSet(const std::string& key, const CachedTile& tile, std::chrono::seconds expire_time) noexcept;
    void ProcessTouch(const std::string& key, std::chrono::seconds expire_time) noexcept;

    std::string conn_str_;
    std::string user_;
    std::string password_;

    CouchbaseCacher& cacher_;

    lcb_t cb_instance_{nullptr};

};
