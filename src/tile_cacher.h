#pragma once

#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

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


class CacherLock;

class TileCacher {
public:
    using GetTask = AsyncTask<std::shared_ptr<const CachedTile>>;
    using SetTask = AsyncTask<bool>;

    virtual ~TileCacher();

    void Get(const std::string& key, std::shared_ptr<GetTask> task);
    void Set(const std::string& key, std::shared_ptr<const CachedTile> cached_tile,
             std::chrono::seconds expire_time, std::shared_ptr<SetTask> task);
    void Touch(const std::string& key, std::chrono::seconds expire_time);
    std::unique_ptr<CacherLock> LockUntilSet(std::vector<std::string> keys);
    void Unlock(const std::vector<std::string>& keys);

    void OnTileRetrieved(const std::string& key, std::shared_ptr<CachedTile> cached_tile);
    void OnRetrieveError(const std::string& key);
    void OnTileSet(const std::string& key);
    void OnSetError(const std::string& key);

private:
    virtual void GetImpl(const std::string& key) = 0;
    virtual void SetImpl(const std::string& key, std::shared_ptr<const CachedTile> cached_tile,
                         std::chrono::seconds expire_time) = 0;
    virtual void TouchImpl(const std::string& key, std::chrono::seconds expire_time) = 0;

    void ClearTmpCache();

    using tmp_cache_t = std::unordered_map<std::string, std::shared_ptr<const CachedTile>>;
    using waiters_vec_t = std::vector<std::shared_ptr<GetTask>>;

    std::unordered_map<std::string, waiters_vec_t> get_waiters_;
    std::unordered_map<std::string, waiters_vec_t> set_waiters_;
    tmp_cache_t tmp_cache_;
    std::list<std::pair<std::string, std::chrono::system_clock::time_point>> keys_to_remove_;
    std::mutex mux_;
    std::mutex tmp_cache_mux_;
};


class CacherLock {
public:
    explicit CacherLock(TileCacher& cacher, std::vector<std::string> keys) :
        locked_keys_(std::move(keys)), cacher_(cacher) { }

    ~CacherLock() {
        Unlock();
    }

    inline void Unlock() {
        if (locked_) {
            cacher_.Unlock(locked_keys_);
            locked_ = false;
        }
    }

    inline void Cancel() {
        locked_ = false;
    }

private:
    std::vector<std::string> locked_keys_;
    TileCacher& cacher_;
    bool locked_{true};
};
