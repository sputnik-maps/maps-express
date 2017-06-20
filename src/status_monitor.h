#pragma once

#include <atomic>

class StatusMonitor {
public:
    enum class Status {
        fail,
        ok,
        maintenance
    };

    inline void set_status(Status status) {
        status_ = status;
    }

    inline Status exchange_status(Status status) {
        return status_.exchange(status);
    }

    inline Status status() const {
        return status_;
    }

private:
    std::atomic<Status> status_{Status::ok};
};
