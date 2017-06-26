#include "etcd_client.h"

#include <jsoncpp/json/reader.h>


static proxygen::HTTPHeaders MakeDefaultPostHeaders() {
    proxygen::HTTPHeaders headers;
    headers.rawAdd("Content-Type", "application/x-www-form-urlencoded");
    return headers;
}

static const proxygen::HTTPHeaders kDefaultPostHeaders = MakeDefaultPostHeaders();


EtcdClient::EtcdClient(const std::string& host, uint16_t port, std::uint8_t num_clients) {
    UpadateBaseUrl(host, port);

    loop_thread_ = std::thread(&EventBase::loopForever, &evb_);
    evb_.waitUntilRunning();
    http_client_ = std::make_shared<HTTPClient>(evb_, host, port, num_clients);
}

EtcdClient::EtcdClient(std::shared_ptr<HTTPClient> http_client, const std::string& host, uint16_t port) {
    UpadateBaseUrl(host, port);
    http_client_ = std::move(http_client);
}


EtcdClient::~EtcdClient() {
    Shutdown();
}

void EtcdClient::Shutdown() {
    bool expected = false;
    if (!pending_shutdown_.compare_exchange_strong(expected, true)) {
        return;
    }
    // If we own EventBase and client, stop them
    if (loop_thread_.get_id() != std::thread::id()) {
        http_client_.reset();
        evb_.terminateLoopSoon();
        loop_thread_.join();
    }
}

void EtcdClient::UpadateBaseUrl(const std::string& host, uint16_t port) {
    base_url_ = "http://";
    base_url_.append(host);
    base_url_.append(":");
    base_url_.append(std::to_string(port));
    base_url_.append("/v2/keys");
}

static std::unique_ptr<EtcdNode> ParseEtcdNode(const Json::Value& jnode) {
    auto etcd_node = std::make_unique<EtcdNode>();
    const Json::Value& jname = jnode["key"];
    etcd_node->name = jname.isString() ? jname.asString() : "/" ;
    const Json::Value& jis_dir = jnode["dir"];
    if (jis_dir.isBool()) {
        etcd_node->is_dir = jis_dir.asBool();
    }

    if (etcd_node->is_dir) {
        const Json::Value& jnodes = jnode["nodes"];
        if (jnodes.isArray()) {
            auto& subnodes = etcd_node->subnodes;
            for (const Json::Value& jsubnode : jnodes) {
                auto subnode = ParseEtcdNode(jsubnode);
                if (!subnode) {
                    continue;
                }
                subnodes.push_back(std::move(subnode));
            }
        }
    } else {
        const Json::Value& jvalue = jnode["value"];
        if (!(jvalue.isString() || jvalue.isNull())) {
            LOG(ERROR) << "Error while parsing etcd respose: node is not directory, but does not have value!";
            return nullptr;
        }
        etcd_node->value = jvalue.asString();
    }

    const Json::Value& jcreated_id = jnode["createdIndex"];
    if (jcreated_id.isInt64()) {
        etcd_node->created_id = jcreated_id.asInt64();
    }
    const Json::Value& jmodified_id = jnode["modifiedIndex"];
    if (jmodified_id.isInt64()) {
        etcd_node->modified_id = jmodified_id.asInt64();
    }

    return etcd_node;
}

static inline std::int64_t GetEtcdId(http_response_ptr& response) {
    assert(response);
    proxygen::HTTPHeaders& headers = response->headers->getHeaders();
    const std::string& id_str = headers.getSingleOrEmpty("X-Etcd-Index");
    if (id_str.empty()) {
        return -1;
    }
    std::int64_t id;
    try {
        id = std::stol(id_str);
    } catch (...) {
        LOG(ERROR) << "Error while parsing etcd id: " << id_str;
        return -1;
    }
    return id;
}

static EtcdError CheckResponse(http_response_ptr response) {
    if (!response) {
        return EtcdError::network_error;
    }
    proxygen::HTTPMessage& headers = *response->headers;
    auto status = headers.getStatusCode();
    if (status == 404) {
        return EtcdError::not_found;
    }
    // TODO: maybe add more sophisticated check
    if (status == 400) {
        return EtcdError::wait_id_outdated;
    }
    if (!(status == 200 || status == 201)) {
        return EtcdError::server_error;
    }
    return EtcdError::none;
}

static EtcdError CheckResponseAndPaseBody(http_response_ptr response, Json::Value& jbody) {
    EtcdError err = CheckResponse(response);
    if (err != EtcdError::none) {
        return err;
    }

    if (!response->body) {
        return EtcdError::connection_timeout;
    }
    folly::IOBuf& body = *response->body;

    Json::Reader reader;
    folly::fbstring str_body = body.moveToFbString();
    try {
        reader.parse(str_body.data(), str_body.data() + str_body.size(), jbody);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Error while parsing etcd respose: " << e.what();
        return EtcdError::server_error;
    }
    return EtcdError::none;
}


static std::pair<EtcdResponse, EtcdError> ProcessGet(http_response_ptr response) {
    Json::Value jresponse;
    EtcdError err = CheckResponseAndPaseBody(response, jresponse);
    if (err != EtcdError::none) {
        return std::make_pair(EtcdResponse{}, err);
    }

    const Json::Value& jnode = jresponse["node"];
    if (!jnode.isObject()) {
        LOG(ERROR) << "Error while parsing etcd respose: \"node\" not found!";
        return std::make_pair(EtcdResponse{}, EtcdError::server_error);
    }

    etcd_node_ptr etcd_node = ParseEtcdNode(jnode);
    if (!etcd_node) {
        return std::make_pair(EtcdResponse{}, EtcdError::server_error);
    }
    std::int64_t etcd_id = GetEtcdId(response);
    return std::make_pair(EtcdResponse{std::move(etcd_node), etcd_id}, EtcdError::none);
}


std::pair<EtcdResponse, EtcdError> EtcdClient::Get(const std::string& key, bool recursive) {
    if (pending_shutdown_) {
        return std::make_pair(EtcdResponse{}, EtcdError::pending_shutdown);
    }
    std::string url = MakeUrl(key);
    if (recursive) {
        url.append("?recursive=true");
    }
    http_response_ptr response = http_client_->RequestAndWait(proxygen::HTTPMethod::GET, url);
    return ProcessGet(std::move(response));
}

void EtcdClient::Get(std::shared_ptr<GetTask> task, const std::string& key, bool recursive) {
    if (pending_shutdown_) {
        task->NotifyError(EtcdError::pending_shutdown);
        return;
    }
    std::string url = MakeUrl(key);
    if (recursive) {
        url.append("?recursive=true");
    }
    auto http_task = std::make_shared<HTTPTask>([task](http_response_ptr response) {
        auto result = ProcessGet(std::move(response));
        if (result.second != EtcdError::none) {
            task->NotifyError(result.second);
        } else {
            task->SetResult(std::move(result.first));
        }
    }, [task]{
        task->NotifyError(EtcdError::network_error);
    }, false);
    http_client_->Request(std::move(http_task), proxygen::HTTPMethod::GET, url);
}


static std::pair<EtcdUpdate, EtcdError> ProcessWatch(http_response_ptr response) {    
    Json::Value jresponse;
    EtcdError err = CheckResponseAndPaseBody(response, jresponse);
    if (err != EtcdError::none) {
        return std::make_pair(EtcdUpdate{}, err);
    }

    const Json::Value& jaction = jresponse["action"];
    if(!jaction.isString()) {
        LOG(ERROR) << "Error while parsing etcd wait respose: \"action\" not found!";
        return std::make_pair(EtcdUpdate{}, EtcdError::server_error);
    }

    EtcdUpdate etcd_update;

    std::string action = jaction.asString();
    if (action == "set") {
        etcd_update.type = EtcdUpdate::Type::set;
    } else if (action == "delete" || action == "expire") {
        etcd_update.type = EtcdUpdate::Type::remove;
    } else if (action == "update") {
        etcd_update.type = EtcdUpdate::Type::update;
    } else {
        LOG(ERROR) << "Error while parsing etcd wait respose: invalid \"action\" value: " << action;
        return std::make_pair(EtcdUpdate{}, EtcdError::server_error);
    }

    const Json::Value& jnode = jresponse["node"];
    if (!jnode.isObject()) {
        LOG(ERROR) << "Error while parsing etcd wait respose: \"node\" not found!";
        return std::make_pair(EtcdUpdate{}, EtcdError::server_error);
    }

    etcd_update.new_node = ParseEtcdNode(jnode);
    if (!etcd_update.new_node) {
        return std::make_pair(EtcdUpdate{}, EtcdError::server_error);
    }

    const Json::Value& jprev_node = jresponse["prevNode"];
    if (jprev_node.isObject()) {
        etcd_update.old_node = ParseEtcdNode(jprev_node);
        if (!etcd_update.new_node) {
            return std::make_pair(EtcdUpdate{}, EtcdError::server_error);
        }
    }

    return std::make_pair(std::move(etcd_update), EtcdError::none);
}

std::pair<EtcdUpdate, EtcdError> EtcdClient::Watch(const std::string& key, std::int64_t modified_id) {
    if (pending_shutdown_) {
        return std::make_pair(EtcdUpdate{}, EtcdError::pending_shutdown);
    }
    std::string url = MakeWatchUrl(key, modified_id);
    http_response_ptr response = http_client_->RequestAndWait(proxygen::HTTPMethod::GET, url);
    return ProcessWatch(std::move(response));
}

void EtcdClient::Watch(std::shared_ptr<WatchTask> task, const std::string& key, std::int64_t modified_id) {
    if (pending_shutdown_) {
        task->NotifyError(EtcdError::pending_shutdown);
        return;
    }
    std::string url = MakeWatchUrl(key, modified_id);

    auto http_task = std::make_shared<HTTPTask>([task](http_response_ptr response) {
        auto result = ProcessWatch(std::move(response));
        if (result.second != EtcdError::none) {
            task->NotifyError(result.second);
        } else {
            task->SetResult(std::make_shared<EtcdUpdate>(std::move(result.first)));
        }
    }, [task]{
        task->NotifyError(EtcdError::network_error);
    }, false);
    http_client_->Request(std::move(http_task), proxygen::HTTPMethod::GET, url);
}

void EtcdClient::Set(std::shared_ptr<UpdateTask> task, const std::string& key,
                     const std::string& value, uint ttl, bool refresh) {
    if (pending_shutdown_) {
        task->NotifyError(EtcdError::pending_shutdown);
        return;
    }
    std::string url = MakeUrl(key);
    std::string body = "value=" + value;
    if (ttl != 0) {
        body.append("&ttl=");
        body.append(std::to_string(ttl));
        if (refresh) {
            body.append("&refresh=true");
        }
    }
    auto body_buf = folly::IOBuf::copyBuffer(body);
    auto http_task = std::make_shared<HTTPTask>([task](http_response_ptr response) {
        EtcdError err = CheckResponse(response);
        if (err == EtcdError::none) {
            task->SetResult();
        } else {
            task->NotifyError(err);
        }
    }, [task]{
        task->NotifyError(EtcdError::network_error);
    }, false);

    http_client_->Request(std::move(http_task), proxygen::HTTPMethod::PUT, url,
                          &kDefaultPostHeaders, std::move(body_buf));
}

void EtcdClient::Delete(std::shared_ptr<UpdateTask> task, const std::string& key) {
    if (pending_shutdown_) {
        task->NotifyError(EtcdError::pending_shutdown);
        return;
    }
    std::string url = MakeUrl(key);
    auto http_task = std::make_shared<HTTPTask>([task](http_response_ptr response) {
        EtcdError err = CheckResponse(response);
        if (err == EtcdError::none) {
            task->SetResult();
        } else {
            task->NotifyError(err);
        }
    }, [task]{
        task->NotifyError(EtcdError::network_error);
    }, false);
    http_client_->Request(std::move(http_task), proxygen::HTTPMethod::DELETE, url);
}

std::string EtcdClient::MakeUrl(const std::string& key) {
    std::string url = base_url_;
    if (!key.empty() && key[0] != '/') {
        url.append("/");
    }
    url.append(key);
    return url;
}

std::string EtcdClient::MakeWatchUrl(const std::string& key, std::int64_t modified_id) {
    std::string url = MakeUrl(key);
    url.append("?wait=true&recursive=true");
    if (modified_id != 0) {
        url.append("&waitIndex=");
        url.append(std::to_string(modified_id));
    }
    return url;
}


std::ostream& operator<<(std::ostream& os, EtcdError err) {
    os << "EtcdError::";
    switch (err) {
    case EtcdError::none:
        return os << "none";
    case EtcdError::not_found:
        return os << "not_found";
    case EtcdError::network_error:
        return os << "network_error";
    case EtcdError::connection_timeout:
        return os << "connection_timeout";
    case EtcdError::server_error:
        return os << "server_error";
    case EtcdError::wait_id_outdated:
        return os << "wait_id_outdated";
    case EtcdError::pending_shutdown:
        return os << "pending_shutdown";
    }
    return os;
}
