#include "json_config.h"

#include <fstream>

#include <glog/logging.h>

#include <jsoncpp/json/reader.h>

// TODO: add validation

JsonConfig::JsonConfig(const std::string& file_path) {
    std::size_t path_len = file_path.size();
    if (path_len < 6 || file_path.substr(path_len - 5) != ".json") {
        LOG(ERROR) << "Invalid config file format! (Should be '.json')";
        return;
    }
    std::ifstream file;
    file.open(file_path);
    if (!file.is_open()) {
        LOG(ERROR) << "Unable to open config file: " << file_path << std::endl;
        return;
    }
    Json::Reader reader;
    Json::Value root;
    try {
        reader.parse(file, root);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Error while parsing config file: " << e.what() << std::endl;
        return;
    }
    SetValue("app", std::make_shared<Json::Value>(root["app"]));
    SetValue("render/workers", std::make_shared<Json::Value>(root["render"]["workers"]));
    SetValue("render/queue_limit", std::make_shared<Json::Value>(root["render"]["queue_limit"]));
    SetValue("render/styles", std::make_shared<Json::Value>(root["render"]["styles"]));
    SetValue("data", std::make_shared<Json::Value>(root["data"]));
    valid_ = true;
}
