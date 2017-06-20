#include "config.h"


Config::~Config() {}

void Config::SetValue(const std::string& name, std::shared_ptr<Json::Value> value) {
    std::unique_lock<std::mutex> lock(mux_);
    auto vh_itr = values_.find(name);
    if (vh_itr == values_.end()) {
        values_[name] = std::make_unique<ValueHolder>(std::move(value));
    } else {
        ValueHolder& vh = *vh_itr->second;
        vh.value = value;
        lock.unlock();
        // TODO: maybe race condition
        vh.NotifyObservers(std::move(value));
    }
}

std::shared_ptr<const Json::Value> Config::GetValue(const std::string name, ConfigObserver* observer) {
    std::lock_guard<std::mutex> lock(mux_);
    auto vh_itr = values_.find(name);
    if (vh_itr == values_.end()) {
        if (!observer) {
            auto vh = std::make_unique<ValueHolder>();
            vh->AttachObserver(observer);
            values_[name] = std::move(vh);
        }
        return nullptr;
    } else {
        ValueHolder& vh = *vh_itr->second;
        if (observer) {
            vh.AttachObserver(observer);
        }
        return vh.value;
    }
}
