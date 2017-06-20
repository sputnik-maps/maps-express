#pragma once

#include <memory>

template <typename T>
class Worker {
public:
    virtual bool Init() noexcept { return true; }

    virtual void ProcessTask(T task) noexcept = 0;

protected:
    ~Worker() {}
};
