#pragma once

#include <unordered_map>

#include <jsoncpp/json/json.h>

#include "data_provider.h"
#include "config.h"
#include "tile_loader.h"

class DataManager {
public:
    DataManager(Config& config);

    std::shared_ptr<DataProvider> GetProvider(const std::string& name);

private:
    using loaders_map_t = std::unordered_map<std::string, std::shared_ptr<TileLoader>>;
    using providers_map_t = std::unordered_map<std::string, std::shared_ptr<DataProvider>>;

    void AddDataProvider(const std::string& provider_name, const Json::Value& jprovider_params);
    void AddCassandraLoader(const std::string& loader_name, const Json::Value& jloader_params,
                            std::vector<std::string> versions);
    void AddFileLoader(const std::string& loader_name, const Json::Value& jloader_params,
                       std::vector<std::string> versions);

    loaders_map_t loaders_map_;
    providers_map_t providers_map_;
    Config& config_;
};
