#pragma once

#include <folly/io/async/EventBase.h>

#include <jsoncpp/json/value.h>

#include "async_task.h"
#include "http_client.h"

using folly::EventBase;

struct EtcdNode {
    std::string name;
    std::string value;
    std::vector<std::unique_ptr<EtcdNode>> subnodes;
    std::int64_t created_id;
    std::int64_t modified_id;
    bool is_dir{false};
};

using etcd_node_ptr = std::unique_ptr<EtcdNode>;

struct EtcdUpdate {
    enum class Type : uint8_t {
        set,
        remove,
        update
    };

    etcd_node_ptr new_node;
    etcd_node_ptr old_node;
    Type type;
};

enum class EtcdError : uint8_t {
    none,
    not_found,
    network_error,
    connection_timeout,
    server_error,
    wait_id_outdated,
    pending_shutdown
};

std::ostream& operator<<(std::ostream& os, EtcdError err);

using etcd_update_ptr = std::unique_ptr<EtcdUpdate>;

struct EtcdResponse {
    std::shared_ptr<EtcdNode> node;
    std::int64_t etcd_id{-1};
};

class EtcdClient {
public:
    EtcdClient(const std::string& host, uint16_t port = 2379u, std::uint8_t num_clients = 2);
    EtcdClient(std::shared_ptr<HTTPClient> http_client, const std::string& host, uint16_t port = 2379u);
    ~EtcdClient();

    // Methods are thread safe

    void Shutdown();

    using GetTask = AsyncTask<EtcdResponse, EtcdError>;
    void Get(std::shared_ptr<GetTask> task, const std::string& key, bool recursive = false);

    using WatchTask = AsyncTask<std::shared_ptr<EtcdUpdate>, EtcdError>;
    void Watch(std::shared_ptr<WatchTask> task, const std::string& key, std::int64_t modified_id = 0);

    using UpdateTask = AsyncTask<void, EtcdError>;
    void Set(std::shared_ptr<UpdateTask> task, const std::string& key, const std::string& value,
             uint ttl = 0, bool refresh = false);
    void Delete(std::shared_ptr<UpdateTask> task, const std::string& key);

    inline EventBase& get_event_base() const noexcept {
        return http_client_->get_event_base();
    }

private:
    void UpadateBaseUrl(const std::string& host, uint16_t port);
    std::string MakeUrl(const std::string& key);
    std::string MakeWatchUrl(const std::string& key, std::int64_t modified_id = 0);

    EventBase evb_;
    std::thread loop_thread_;
    std::shared_ptr<HTTPClient> http_client_;
    std::string base_url_;
    std::atomic_bool pending_shutdown_{false};
};
