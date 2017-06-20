#pragma once

#include "config.h"


class JsonConfig : public Config {
public:
    explicit JsonConfig(const std::string& file_path);

    inline bool Valid() const override {
        return valid_;
    }

private:
    bool valid_{false};

};
