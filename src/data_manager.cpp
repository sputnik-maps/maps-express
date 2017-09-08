#include "data_manager.h"

#include <algorithm>

#include <glog/logging.h>

#include "cassandraloader.h"
#include "data_provider.h"
#include "fileloader.h"

DataManager::DataManager(Config& config) : config_(config) {
    std::shared_ptr<const Json::Value> jdata_ptr = config.GetValue("data");
    assert(jdata_ptr);
    const Json::Value& jdata = *jdata_ptr;
    if (!jdata.isObject()) {
        if (!jdata.isNull()) {
            LOG(ERROR) << "Invalid data section: " << jdata;
        }
        return;
    }

    const Json::Value& jloaders = jdata["loaders"];
    for (auto loader = jloaders.begin(); loader != jloaders.end(); ++loader) {
        const std::string loader_name = loader.key().asString();
        if (loaders_map_.find(loader_name) != loaders_map_.end()) {
            LOG(ERROR) << "Duplicate loader name: " << loader_name;
            continue;
        }
        const Json::Value& loader_params = *loader;

        const Json::Value& jversions = loader_params["versions"];
        std::vector<std::string> versions;
        if (jversions.isArray()) {
            for (const Json::Value& jversion : jversions) {
                if (!jversion.isString()) {
                    LOG(ERROR) << "Data version must have string type!";
                    continue;
                }
                versions.push_back(jversion.asString());
            }
        }
        if (versions.empty()) {
            LOG(WARNING) << "No versions for loader " << loader_name << " provided!";
        }
        std::string loader_type = loader_params.get("type", "").asString();
        if (loader_type == "cassandra") {
            AddCassandraLoader(loader_name, loader_params, std::move(versions));
        } else if (loader_type == "file") {
            AddFileLoader(loader_name, loader_params, std::move(versions));
        } else {
            LOG(ERROR) << "Invalid loader type: " << loader_type;
        }
    }

    const Json::Value& jproviders = jdata["providers"];
    for (auto jprovider = jproviders.begin(); jprovider != jproviders.end(); ++jprovider) {
        const std::string provider_name = jprovider.key().asString();
        const Json::Value& jprovider_params = *jprovider;
        AddDataProvider(provider_name, jprovider_params);
    }
}

static std::shared_ptr<DataProvider::zoom_groups_t> parse_zoom_groups(const Json::Value &jloader_params) {
    std::shared_ptr<DataProvider::zoom_groups_t> zoom_groups = nullptr;
    const Json::Value& jzoom_groups = jloader_params["zoom groups"];
    if (!jzoom_groups.empty()) {
        zoom_groups = std::make_shared<DataProvider::zoom_groups_t>();
        for (uint j=0; j < jzoom_groups.size(); ++j) {
            zoom_groups->insert(jzoom_groups[j].asUInt());
        }
    }
    return zoom_groups;
}

void DataManager::AddDataProvider(const std::string& provider_name, const Json::Value& jprovider_params) {
    if (providers_map_.find(provider_name) != providers_map_.end()) {
        LOG(ERROR) << "Duplicate provider name: " << provider_name;
        return;
    }

    const Json::Value& jloader_name = jprovider_params["loader"];
    if (jloader_name.isNull()) {
        LOG(ERROR) << "No loader specified for provider: " << provider_name;
        return;
    }
    if (!jloader_name.isString()) {
        LOG(ERROR) << "Loader name must have string type: " << jloader_name;
        return;
    }
    const std::string loader_name = jloader_name.asString();
    auto loader_itr = loaders_map_.find(loader_name);
    if (loader_itr == loaders_map_.end()) {
        LOG(ERROR) << "Loader \"" << loader_name << "\" not found!";
        return;
    }

    std::shared_ptr<TileLoader> loader = loader_itr->second;
    std::shared_ptr<DataProvider::zoom_groups_t> zoom_groups = parse_zoom_groups(jprovider_params);
    uint max_zoom = jprovider_params.get("max zoom", 19).asUInt();
    uint min_zoom = zoom_groups == nullptr ? jprovider_params.get("min zoom", 0).asUInt() : *zoom_groups->begin();
    if (max_zoom < min_zoom) {
        LOG(ERROR) << "Invalid max zoom: " << max_zoom;
        return;
    }
    auto provider = std::make_shared<DataProvider>(std::move(loader), min_zoom, max_zoom, std::move(zoom_groups));
    providers_map_.emplace(provider_name, std::move(provider));
}

void DataManager::AddCassandraLoader(const std::string &loader_name, const Json::Value &jloader_params,
                                     std::vector<std::string> versions) {
    int nworkers = jloader_params.get("workers", 32).asInt();
    if (nworkers < 0) {
        LOG(INFO) << "Number of workers must be positive integer" << std::endl;
        nworkers = 32;
    }
    const std::string table = jloader_params.get("table", "tiles").asString();
    const std::string contact_points = jloader_params.get("contact points", "").asString();
    if (contact_points.empty()) {
        LOG(ERROR) << "No contact points for loader " << loader_name << " provided. Skipping!";
        return;
    }

    auto cassandra_loader = std::make_shared<CassandraLoader>(contact_points, table, std::move(versions), nworkers);
    loaders_map_[loader_name] = std::move(cassandra_loader);
}


void DataManager::AddFileLoader(const std::string& loader_name, const Json::Value& jloader_params,
                                std::vector<std::string> versions) {
    if (loaders_map_.find(loader_name) != loaders_map_.end()) {
        LOG(ERROR) << "Duplicate loader name: " << loader_name;
        return;
    }

    const std::string base_path = jloader_params.get("base_path", "").asString();
    bool auto_version = jloader_params.get("auto_version", false).asBool();
    auto file_loader = std::make_shared<FileLoader>(base_path, auto_version);
    loaders_map_[loader_name] = std::move(file_loader);
}

std::shared_ptr<DataProvider> DataManager::GetProvider(const std::string& name) {
    auto provider_itr = providers_map_.find(name);
    if (provider_itr == providers_map_.end()) {
        return nullptr;
    }
    return provider_itr->second;
}
