#pragma once

#include <memory>
#include <string>
#include <vector>

#include <libcouchbase/couchbase.h>

#include "async_task.h"


struct CachedTile {
    enum class TTLPolicy : std::int32_t {
        error,
        regular,
        extended
    };

    std::string data;
    std::vector<std::pair<std::string, std::string>> headers;
    TTLPolicy policy{TTLPolicy::regular};
};


class TileCacher {
public:
    using GetTask = AsyncTask<std::shared_ptr<const CachedTile>>;
    using SetTask = AsyncTask<bool>;

    virtual ~TileCacher() {}

    virtual void Get(const std::string& key, std::shared_ptr<GetTask> task) = 0;
    virtual void Set(const std::string& key, std::shared_ptr<const CachedTile> cached_tile,
                     std::chrono::seconds expire_time, std::shared_ptr<SetTask> task) = 0;
    virtual void Touch(const std::string& key, std::chrono::seconds expire_time) = 0;
    virtual bool LockUntilSet(const std::vector<std::string>& keys) = 0;
    virtual void Unlock(const std::vector<std::string>& keys) = 0;
};


