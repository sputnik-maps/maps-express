#pragma once

#include <folly/Baton.h>

#include "config.h"


class EtcdClient;
class EtcdUpdate;
class EtcdNode;

namespace folly {
class EventBase;
} // ns folly

class EtcdConfig : public Config {
public:
    explicit EtcdConfig(const std::string& etcd_host, const std::string& root_node = "/");
    explicit EtcdConfig(std::shared_ptr<EtcdClient> etcd_client, const std::string& root_node = "/");
    ~EtcdConfig();

    // Blocks until first update finished or error occurs
    bool Valid() const override;

private:
    void UpdateAll();
    bool ParseAndeSet(const std::string& name, const std::string& json_str);
    bool ParseAndeSet(const std::vector<std::pair<std::string, std::string>>& mapping,
                      const EtcdNode& node);
    bool ProcessUpdate(std::shared_ptr<EtcdUpdate> update);
    void StartWatch();

    std::shared_ptr<EtcdClient> client_;
    std::string host_;
    std::string root_node_name_;
    mutable folly::Baton<> baton_;
    folly::EventBase* evb_{nullptr};
    std::int64_t update_id_{0};
    std::atomic_bool inited_{false};
    bool valid_{false};
};
