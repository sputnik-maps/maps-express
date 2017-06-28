#pragma once

#include <atomic>
#include <cassert>
#include <functional>
#include <memory>

namespace folly {
class EventBase;
} // ns folly

namespace detail {

template <typename T>
struct CallbackHelper {
    typedef std::function<void(T)> type;
};

template <>
struct CallbackHelper<void> {
    typedef std::function<void()> type;
};

} // ns detail

folly::EventBase* GetEventBase();
bool RunInEventBaseThread(folly::EventBase* evb, std::function<void()>&& func);

class AsyncTaskBase {
public:
    virtual ~AsyncTaskBase() {}

    virtual bool cancel() = 0;
};


template <typename Res = void, typename Err = void>
class AsyncTask : public AsyncTaskBase {
public:

    using result_t = Res;
    using error_t = Err;

    using result_cb_t = typename detail::CallbackHelper<Res>::type;
    using error_cb_t = typename detail::CallbackHelper<Err>::type;

    AsyncTask() = default;

    explicit AsyncTask(result_cb_t success_callback, bool cb_in_event_base_thread = false) :
                  success_callback_(std::move(success_callback)),
                  status_(std::make_shared<std::atomic<TaskStatus>>(TaskStatus::pending)) {
        if (cb_in_event_base_thread) {
            evb_ = GetEventBase();
        }
    }

    explicit AsyncTask(result_cb_t success_callback,
                       error_cb_t error_callback,
                       bool cb_in_event_base_thread = false) :
                  success_callback_(std::move(success_callback)),
                  error_callback_(std::move(error_callback)),
                  status_(std::make_shared<std::atomic<TaskStatus>>(TaskStatus::pending)) {
        if (cb_in_event_base_thread) {
            evb_ = GetEventBase();
        }
    }

    virtual ~AsyncTask() {}

    template <typename ...Args>
    void SetResult(Args&&... arg) {
        return SetDone<Res>(success_callback_, std::forward<Args>(arg)...);
    }

    template <typename ...Args>
    void NotifyError(Args&&... arg) {
        return SetDone<Err>(error_callback_, std::forward<Args>(arg)...);
    }

    inline bool finished() const noexcept {
        return *status_ != TaskStatus::pending;
    }

    inline bool cancelled() const noexcept {
        return *status_ == TaskStatus::cancelled;
    }

    inline bool cancel() noexcept override {
        return SetStatus(TaskStatus::cancelled);
    }

private:
    enum class TaskStatus : std::uint8_t {
        pending,
        done,
        cancelled
    };


    static inline bool SetStatus(std::atomic<TaskStatus>& status, TaskStatus new_status) {
        auto expected_status = TaskStatus::pending;
        return status.compare_exchange_strong(expected_status, new_status);
    }

    inline bool SetStatus(TaskStatus status) {
        return SetStatus(*status_, status);
    }

    template <typename T, typename CB, typename ...Args>
    inline void SetDone(CB& callback, Args&&... args) {
        if (callback) {
            InvokeCallback<T>(callback, std::forward<Args>(args)...);
        } else {
            SetStatus(TaskStatus::done);
        }
    }

    template <typename T, typename CB>
    inline typename std::enable_if<!std::is_same<CB, std::function<void()>>::value>::type
    InvokeCallback(CB& callback, T arg) {
        if (evb_) {
            RunInEventBaseThread(evb_, [current_status = status_, cb = std::move(callback),
                                        arg = std::move(arg)] () mutable {
                if (SetStatus(*current_status, TaskStatus::done)) {
                    cb(std::forward<T>(arg));
                }
            });
        } else {
            if (SetStatus(TaskStatus::done)) {
                callback(std::forward<T>(arg));
            }
        }
    }

    template <typename T, typename CB>
    inline typename std::enable_if<std::is_same<CB, std::function<void()>>::value>::type
    InvokeCallback(CB& callback) {
        if (evb_) {
            RunInEventBaseThread(evb_, [current_status = status_, cb = std::move(callback)] () mutable {
                if (SetStatus(*current_status, TaskStatus::done)) {
                    cb();
                }
            });
        } else {
            if (SetStatus(TaskStatus::done)) {
                callback();
            }
        }
    }

    result_cb_t success_callback_;
    error_cb_t error_callback_;
    std::shared_ptr<std::atomic<TaskStatus>> status_;
    folly::EventBase* evb_{nullptr};
};
