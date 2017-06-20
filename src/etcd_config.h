#pragma once

#include "config.h"


class EtcdClient;
class EtcdUpdate;
class EtcdNode;

class EtcdConfig : public Config {
public:
    explicit EtcdConfig(const std::string& etcd_host, const std::string& root_node = "/");
    explicit EtcdConfig(std::shared_ptr<EtcdClient> etcd_client, const std::string& root_node = "/");
    ~EtcdConfig();

    inline bool Valid() const override {
        return valid_;
    }

private:
    void Init();
    bool ParseAndeSet(const std::string& name, const std::string& json_str);
    bool ParseAndeSet(const std::vector<std::pair<std::string, std::string>>& mapping,
                      const EtcdNode& node);
    bool ProcessUpdate(std::shared_ptr<EtcdUpdate> update);
    bool UpdateAll();
    void StartWatch();

    std::shared_ptr<EtcdClient> client_;
    std::string host_;
    std::string root_node_name_;
    std::int64_t update_id_{0};
    bool valid_{false};
};
