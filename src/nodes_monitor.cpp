#include "nodes_monitor.h"

#include <experimental/optional>

#include <folly/io/async/EventBaseManager.h>


using std::experimental::optional;
using std::experimental::nullopt;

const std::string kNodesKey = "nodes";

NodesMonitor::NodesMonitor(const std::string& host, uint port, std::shared_ptr<EtcdClient> etcd_client) :
        etcd_client_(std::move(etcd_client)),
        evb_(etcd_client_->get_event_base())
{
    assert(etcd_client_);
    etcd_key_ = kNodesKey + "/" + host;
    host_port_ = host + ':';
    host_port_.append(std::to_string(port));
    UpdateAll();
}

NodesMonitor::~NodesMonitor() {}

static optional<folly::SocketAddress> EtcdNodeToAddr(EtcdNode& node) {
    if (node.is_dir) {
        LOG(ERROR) << "Invalid type 'dir' of etcd node " << node.name << "!";
        return nullopt;
    }
    const std::string& value = node.value;
    auto delimiter_pos = value.find(':');
    if (delimiter_pos == value.npos) {
        LOG(ERROR) << "Invalid host:port value: " << value;
        return nullopt;
    }
    try {
        std::uint16_t port = std::stoi(value.substr(delimiter_pos + 1));
        return folly::SocketAddress(value.substr(0, delimiter_pos), port, true);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to resolve hostname \"" << node.value << "\": " << e.what();
    }
    return nullopt;
}

void NodesMonitor::UpdateAll() {
    auto task = std::make_shared<EtcdClient::GetTask>([this](EtcdResponse response){
        update_id_ = response.etcd_id + 1;
        EtcdNode& etcd_node = *response.node;
        auto addr_vec = std::make_shared<addr_vec_t>();
        addr_vec ->reserve(etcd_node.subnodes.size());
        for (auto& subnode : etcd_node.subnodes) {
            assert(subnode);
            if (subnode->value == host_port_) {
                continue;
            }
            auto addr = EtcdNodeToAddr(*subnode);
            if (addr) {
                addr_vec ->push_back(std::move(*addr));
            }
        }
        std::atomic_store(&addr_vec_, std::move(addr_vec ));
        Watch();
    }, [this](EtcdError err){
        if (err == EtcdError::pending_shutdown) {
            return;
        }
        LOG(ERROR) << err;

        UpdateAll();
    }, false);
    etcd_client_->Get(std::move(task), kNodesKey);
}

void NodesMonitor::Watch() {
    auto task = std::make_shared<EtcdClient::WatchTask>([this](std::shared_ptr<EtcdUpdate> update) {
        assert(update->new_node);
        EtcdNode& etcd_node = *update->new_node;
        update_id_ = etcd_node.modified_id + 1;
        if (etcd_node.value != host_port_) {
            if (update->type == EtcdUpdate::Type::set) {
                auto new_addr = EtcdNodeToAddr(etcd_node);
                if (new_addr) {
                    auto new_addr_vec = std::make_shared<addr_vec_t>(*addr_vec_);
                    new_addr_vec->push_back(std::move(*new_addr));
                    std::atomic_store(&addr_vec_, std::move(new_addr_vec));
                }
            } else if (update->type == EtcdUpdate::Type::remove) {
                auto new_addr_vec = std::make_shared<addr_vec_t>();
                if (!addr_vec_->empty()) {
                    new_addr_vec->reserve(addr_vec_->size() - 1);
                }
                for (folly::SocketAddress& addr : *addr_vec_) {
                    if ("/" + kNodesKey + "/" + addr.getAddressStr() == etcd_node.name) {
                        continue;
                    }
                    new_addr_vec->push_back(addr);
                }
                std::atomic_store(&addr_vec_, std::move(new_addr_vec));
            }
        }
        Watch();
    }, [this](EtcdError err){
        if (err == EtcdError::pending_shutdown) {
            return;
        }
        if (err == EtcdError::wait_id_outdated) {
            UpdateAll();
            return;
        }
        if (err == EtcdError::connection_timeout) {
            Watch();
        }
        LOG(ERROR) << err;
        evb_.runAfterDelay([this]{ Watch(); }, 500);
    }, false);
    etcd_client_->Watch(std::move(task), kNodesKey, update_id_);
}

void NodesMonitor::Register() {
    if (registered_) {
        return;
    }
    bool expected = false;
    if (!pending_registration_.compare_exchange_strong(expected, true)) {
        return;
    }
    if (registered_) {
        pending_registration_ = false;
    }
    auto task = std::make_shared<EtcdClient::UpdateTask>([this]{
        registered_ = true;
        pending_registration_ = false;
        evb_.runAfterDelay([this]{ UpdateRegistration(); }, 5000);
    }, [this](EtcdError err){
        if (err == EtcdError::pending_shutdown) {
            return;
        }
        pending_registration_ = false;
        Register();
    }, false);

    etcd_client_->Set(std::move(task), etcd_key_, host_port_, 10, false);
}

void NodesMonitor::UpdateRegistration() {
    if (!registered_) {
        return;
    }
    auto task = std::make_shared<EtcdClient::UpdateTask>([this]{
        if (registered_) {
            // Update only if still registered
            evb_.runAfterDelay([this]{ UpdateRegistration(); }, 5000);
        }
    }, [this](EtcdError err){
        if (err == EtcdError::pending_shutdown) {
            return;
        }
        evb_.runAfterDelay([this]{ UpdateRegistration(); }, 500);
    }, false);
    etcd_client_->Set(std::move(task), etcd_key_, host_port_, 10, true);
}

void NodesMonitor::Unregister() {
    if (!registered_) {
        return;
    }
    auto task = std::make_shared<EtcdClient::UpdateTask>([this]{
        registered_ = false;
    }, [this](EtcdError err){
        if (err == EtcdError::pending_shutdown) {
            return;
        }
        if (err == EtcdError::not_found) {
            registered_ = false;
            return;
        }
        LOG(ERROR) << err;
        Unregister();
    }, false);
    etcd_client_->Delete(std::move(task), etcd_key_);
}

std::shared_ptr<const NodesMonitor::addr_vec_t> NodesMonitor::GetActiveNodes() {
    return std::atomic_load(&addr_vec_);
}
