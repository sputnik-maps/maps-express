#pragma once

#include <folly/SocketAddress.h>

#include "etcd_client.h"


class NodesMonitor {
public:
    using addr_entry_t = std::pair<folly::SocketAddress, bool>;
    using addr_vec_t = std::vector<addr_entry_t>;

    NodesMonitor(const std::string& host, uint port, std::shared_ptr<EtcdClient> etcd_client);
    ~NodesMonitor();

    std::shared_ptr<const addr_vec_t> GetActiveNodes() const;

    void Register();
    void Unregister();

private:
    void UpdateAll();
    void Watch();
    void UpdateRegistration();

    addr_entry_t self_addr_;
    std::shared_ptr<EtcdClient> etcd_client_;
    std::shared_ptr<addr_vec_t> addr_vec_;
    std::string etcd_key_;
    std::string self_addr_str_;
    folly::EventBase& evb_;
    std::int64_t update_id_{0};
    std::atomic_bool registered_{false};
    std::atomic_bool pending_registration_{false};
};
