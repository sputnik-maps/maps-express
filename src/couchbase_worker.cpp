#include "couchbase_worker.h"

#include <chrono>
#include <thread>

#include <glog/logging.h>

#include <protozero/pbf_reader.hpp>
#include <protozero/pbf_writer.hpp>

#include "couchbase_cacher.h"


using protozero::pbf_tag_type;

enum CachedTileEncoding : pbf_tag_type {
    kDataTag = 1,
    kTTLTag = 2,
    kHeadersTag = 3,
    kHeaderTag = 4,
    kNameTag = 5,
    kValueTag = 6
};

CouchbaseWorker::CouchbaseWorker(CouchbaseCacher& cacher, const std::string& conn_str,
                                 const std::string& user, const std::string& password) :
        conn_str_(conn_str),
        user_(user),
        password_(password),
        cacher_(cacher) {}

CouchbaseWorker::~CouchbaseWorker() {
    if (cb_instance_) {
        lcb_wait(cb_instance_);
        lcb_destroy(cb_instance_);
    }
}

static void GetCallback(lcb_t instance, int cbtype, const lcb_RESPBASE* resp) {
    CouchbaseCacher* cacher = static_cast<CouchbaseCacher*>(resp->cookie);
    std::string key(static_cast<const char*>(resp->key), resp->nkey);
    if (resp->rc != LCB_SUCCESS) {
        if (resp->rc == LCB_KEY_ENOENT) {
            // Tile not found
            cacher->OnTileRetrieved(key, nullptr);
            return;
        }
        LOG(ERROR) << lcb_strerror(instance, resp->rc);
        cacher->OnRetrieveError(key);
        return;
    }
    assert(cbtype == LCB_CALLBACK_GET);
    const lcb_RESPGET* rg = reinterpret_cast<const lcb_RESPGET*>(resp);
    if (rg->nvalue == 0) {
        // Tile not found
        cacher->OnTileRetrieved(key, nullptr);
        return;
    }
    // Decode tile
    auto tile = std::make_shared<CachedTile>();
    protozero::pbf_reader reader(static_cast<const char*>(rg->value), rg->nvalue);
    while(reader.next()) {
        switch (reader.tag()) {
        case kDataTag:
            tile->data = reader.get_string();
            break;
        case kTTLTag:
            tile->policy = CachedTile::TTLPolicy(reader.get_enum());
            break;
        case kHeadersTag: {
            protozero::pbf_reader headers_reader = reader.get_message();
            while (headers_reader.next(kHeaderTag)) {
                protozero::pbf_reader header_reader = headers_reader.get_message();
                std::string name;
                std::string value;
                while (header_reader.next()) {
                    switch (header_reader.tag()) {
                    case kNameTag:
                        name = headers_reader.get_string();
                        break;
                    case kValueTag:
                        value = headers_reader.get_string();
                        break;
                    }
                }
                tile->headers.emplace_back(std::move(name), std::move(value));
            }
            break;
        }
        default:
            LOG(ERROR) << "Error while decoding couchbase tile: Unknown tag: " << reader.tag();
            break;
        }
    }
    cacher->OnTileRetrieved(key, std::move(tile));
}

static void SetCallback(lcb_t instance, int cbtype, const lcb_RESPBASE* resp) {
    CouchbaseCacher* cacher = static_cast<CouchbaseCacher*>(resp->cookie);
    std::string key(static_cast<const char*>(resp->key), resp->nkey);
    if (resp->rc != LCB_SUCCESS) {
        LOG(ERROR) << lcb_strerror(instance, resp->rc);
        cacher->OnSetError(key);
        return;
    }
    assert(cbtype == LCB_CALLBACK_STORE);
    cacher->OnTileSet(key);
}

bool CouchbaseWorker::Init() noexcept {
    lcb_create_st crst;
    crst.version = 3;
    crst.v.v3.connstr = conn_str_.c_str();
    crst.v.v3.username = user_.c_str();
    crst.v.v3.passwd = password_.c_str();

    lcb_create(&cb_instance_, &crst);
    lcb_install_callback3(cb_instance_, LCB_CALLBACK_GET, GetCallback);
    lcb_install_callback3(cb_instance_, LCB_CALLBACK_STORE, SetCallback);

    while (!Connect()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return true;
}

bool CouchbaseWorker::Connect() {
    lcb_error_t rc = lcb_connect(cb_instance_);
    if (rc != LCB_SUCCESS) {
       LOG(ERROR) << lcb_strerror(cb_instance_, rc);
       return false;
    }
    lcb_wait(cb_instance_);
    rc = lcb_get_bootstrap_status(cb_instance_);
    if (rc != LCB_SUCCESS) {
        LOG(ERROR) << lcb_strerror(cb_instance_, rc);
        return false;
    }
    return true;
}

void CouchbaseWorker::ProcessTask(CBWorkTask task) noexcept {
    assert(!task.key.empty());
    if (!cb_instance_) {
        LOG(ERROR) << "Couchbase not connected!";
        cacher_.OnRetrieveError(task.key);
        return;
    }
    switch (task.type) {
    case CBWorkTask::Type::get:
        ProcessGet(task.key);
        break;
    case CBWorkTask::Type::set:
        if (!task.tile) {
            LOG(ERROR) << "No tile provided!";
            cacher_.OnRetrieveError(task.key);
            return;
        }
        ProcessSet(task.key, *task.tile, task.expire_time);
        break;
    case CBWorkTask::Type::touch:
        ProcessTouch(task.key, task.expire_time);
        break;
    }
}

void CouchbaseWorker::ProcessGet(const std::string& key) noexcept {
    lcb_CMDGET gcmd = { 0 };
    LCB_CMD_SET_KEY(&gcmd, key.data(), key.size());
    lcb_get3(cb_instance_, &cacher_, &gcmd);
    lcb_wait(cb_instance_);
}

void CouchbaseWorker::ProcessSet(const std::string& key, const CachedTile& tile,
                                 std::chrono::seconds expire_time) noexcept {
    std::string buf;
    protozero::pbf_writer writer(buf);
    writer.add_string(kDataTag, tile.data);
    writer.add_enum(kTTLTag, static_cast<std::int32_t>(tile.policy));
    if (!tile.headers.empty()) {
        protozero::pbf_writer headers_writer(writer, kHeadersTag);
        for (const auto& header_pair : tile.headers) {
            protozero::pbf_writer header_writer(headers_writer, kHeaderTag);
            header_writer.add_string(kNameTag, header_pair.first);
            header_writer.add_string(kValueTag, header_pair.second);
        }
    }

    lcb_CMDSTORE scmd = { 0 };
    LCB_CMD_SET_KEY(&scmd, key.data(), key.size());
    LCB_CMD_SET_VALUE(&scmd, buf.data(), buf.size());
    scmd.exptime = static_cast<std::int32_t>(expire_time.count());
    scmd.operation = LCB_SET;
    lcb_store3(cb_instance_, &cacher_, &scmd);
    lcb_wait(cb_instance_);
}

void CouchbaseWorker::ProcessTouch(const std::string& key, std::chrono::seconds expire_time) noexcept {
    lcb_CMDTOUCH tcmd = { 0 };
    LCB_CMD_SET_KEY(&tcmd, key.data(), key.size());
    tcmd.exptime = static_cast<std::int32_t>(expire_time.count());
    lcb_touch3(cb_instance_, nullptr, &tcmd);
    lcb_wait(cb_instance_);
}

