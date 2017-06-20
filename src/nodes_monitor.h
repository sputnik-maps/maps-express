#pragma once

#include <folly/SocketAddress.h>

#include "etcd_client.h"


class NodesMonitor {
public:
    using addr_vec_t = std::vector<folly::SocketAddress>;

    NodesMonitor(const std::string& host, uint port, std::shared_ptr<EtcdClient> etcd_client);
    ~NodesMonitor();

    std::shared_ptr<const addr_vec_t> GetActiveNodes();

    void Register();
    void Unregister();

private:
    void UpdateAll();
    void Watch();
    void UpdateRegistration();

    std::shared_ptr<EtcdClient> etcd_client_;
    std::shared_ptr<addr_vec_t> addr_vec_;
    std::string etcd_key_;
    std::string host_port_;
    folly::EventBase& evb_;
    std::int64_t update_id_{0};
    std::atomic_bool registered_{false};
    std::atomic_bool pending_registration_{false};
};
