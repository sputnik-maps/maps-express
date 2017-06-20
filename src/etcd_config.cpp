#include "etcd_config.h"

#include <jsoncpp/json/reader.h>

#include "etcd_client.h"


// TODO: add validation


static const std::vector<std::pair<std::string, std::string>> kRootMapping = {
    {"/app", "app"},
    {"/server", "server"},
    {"/data", "data"},
    {"/cacher", "cacher"}
};

static const std::vector<std::pair<std::string, std::string>> kRenderMapping = {
    {"/render/workers", "render/workers"},
    {"/render/queue_limit", "render/queue_limit"},
    {"/render/styles", "render/styles"},
};

EtcdConfig::EtcdConfig(const std::string& etcd_host, const std::string& root_node) :
        host_(etcd_host), root_node_name_(root_node)
{
    client_ = std::make_unique<EtcdClient>(etcd_host);
    Init();
}

EtcdConfig::EtcdConfig(std::shared_ptr<EtcdClient> etcd_client, const std::string& root_node_) :
        client_(std::move(etcd_client)), root_node_name_(root_node_)
{
    Init();
}

EtcdConfig::~EtcdConfig() {}


void EtcdConfig::Init() {
    if(!UpdateAll()) {
        return;
    }
    valid_ = true;
    StartWatch();
}


bool EtcdConfig::UpdateAll() {
    std::shared_ptr<EtcdNode> etcd_node;
    while (true) {
        auto etcd_resp = client_->Get(root_node_name_, true);
        EtcdError err = etcd_resp.second;
        if (err == EtcdError::none) {
            EtcdResponse& response = etcd_resp.first;
            etcd_node = std::move(response.node);
            update_id_ = response.etcd_id + 1;
            break;
        }
        if (err == EtcdError::network_error || err == EtcdError::connection_timeout) {
            continue;
        }
        if (err == EtcdError::not_found) {
            LOG(ERROR) << "Node \"" << root_node_name_ << "\" not found on etcd server!";
            return false;
        }
        LOG(ERROR) << "Error while loading kv node " << root_node_name_;
        return false;
    }

    assert(etcd_node);
    for (auto& subnode : etcd_node->subnodes) {
        if (subnode->is_dir) {
            if (subnode->name == "/render") {
                for (auto& render_subnode : subnode->subnodes) {
                    ParseAndeSet(kRenderMapping, *render_subnode);
                }
            }
        } else {
            ParseAndeSet(kRootMapping, *subnode);
        }
    }

    return true;
}

void EtcdConfig::StartWatch() {
    auto watch_task = std::make_shared<EtcdClient::WatchTask>([&](std::shared_ptr<EtcdUpdate> update) {
            ProcessUpdate(std::move(update));
            StartWatch();
        }, [&](EtcdError err) {
            if (err == EtcdError::pending_shutdown) {
                return;
            }
            if (err == EtcdError::wait_id_outdated) {
                UpdateAll();
            }
            StartWatch();
        }, false);
    client_->Watch(std::move(watch_task), root_node_name_, update_id_);
}

bool EtcdConfig::ProcessUpdate(std::shared_ptr<EtcdUpdate> update) {
    update_id_ = update->new_node->modified_id + 1;
    if (update->type == EtcdUpdate::Type::remove || !update->new_node) {
        return false;
    }
    if (ParseAndeSet(kRootMapping, *update->new_node)) {
        return true;
    }
    if (ParseAndeSet(kRenderMapping, *update->new_node)) {
        return true;
    }
    return false;
}

bool EtcdConfig::ParseAndeSet(const std::vector<std::pair<std::string, std::string>>& mapping,
                              const EtcdNode& node) {
    for (const std::pair<std::string, std::string>& p : mapping) {
        if (node.name == p.first) {
            return ParseAndeSet(p.second, node.value);
        }
    }
    return false;
}

bool EtcdConfig::ParseAndeSet(const std::string& name, const std::string& json_str) {
    std::shared_ptr<Json::Value> jvalue = std::make_shared<Json::Value>();
    Json::Reader reader;
    try {
        reader.parse(json_str, *jvalue);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Error while parsing value of etcd node " << name << " " << e.what();
        return false;
    }
    SetValue(name, std::move(jvalue));
    return true;
}
