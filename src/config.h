#pragma once

#include <memory>
#include <unordered_map>

#include <jsoncpp/json/value.h>

#include "observer.h"

class Config {
public:
    using ConfigObserver = Observer<std::shared_ptr<Json::Value>>;

    virtual ~Config();

    std::shared_ptr<const Json::Value> GetValue(const std::string name, ConfigObserver* observer = nullptr);

    virtual bool Valid() const = 0;

protected:
    void SetValue(const std::string& name, std::shared_ptr<Json::Value> value);

private:
    class ValueHolder : public Observable<std::shared_ptr<Json::Value>> {
    public:
        ValueHolder() = default;
        explicit ValueHolder(std::shared_ptr<Json::Value> v) : value(std::move(v)) {}

    private:
        std::shared_ptr<Json::Value> value;

        friend class Config;
    };

    std::unordered_map<std::string, std::unique_ptr<ValueHolder>> values_;
    std::mutex mux_;
};
