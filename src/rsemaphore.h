#pragma once

#include <condition_variable>


class RSemaphore {
public:
    RSemaphore(uint val) : val_(val) {}

    void signal() {
        uint new_val;
        {
            std::lock_guard<std::mutex> lock(mux_);
            if (val_ == 0) {
                return;
            }
            new_val = --val_;
        }
        if (new_val == 0) {
            cv_.notify_all();
        }
    }

    void wait() {
        std::unique_lock<std::mutex> lock(mux_);
        while (val_ != 0) {
            cv_.wait(lock);
        }
    }

    void release_all() {
        {
            std::lock_guard<std::mutex> lock(mux_);
            if (val_ == 0) {
                return;
            }
            val_ = 0;
        }
        cv_.notify_all();
    }

private:
    std::condition_variable cv_;
    std::mutex mux_;
    uint val_{0};
};
