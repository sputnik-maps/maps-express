#pragma once

#include "async_task.h"
#include "base_handler.h"

class AsyncTaskHandler : public BaseHandler {
public:
    using EventBase = folly::EventBase;

    AsyncTaskHandler();

    template <typename T, typename U>
    void ScheduleTaskTimeout(std::shared_ptr<AsyncTask<T, U>> task, std::chrono::milliseconds timeout) {
        timeout_callback_.reset();
        assert(evb_->inRunningEventBaseThread());
        timeout_callback_ = std::make_unique<TimeoutCallback<T, U>>(*this, std::move(task));
        evb_->timer().scheduleTimeout(timeout_callback_.get(), timeout);
    }

    // Should be called from handler's thread
    inline void CancelTaskTimeout() {
        assert(evb_->inRunningEventBaseThread());
        // Callback detaches from timer in it's destructor
        timeout_callback_.reset();
    }

    void RunInHandlerThread(EventBase::Func f) {
        evb_->runInEventBaseThread(std::move(f));
    }

    // If this callback is called task callbacks are guaranteed not to be called
    virtual void onTaskTimeotExpired() noexcept;

private:
    using HHWheelTimer = folly::HHWheelTimer;

    template <typename T, typename U>
    class TimeoutCallback : public HHWheelTimer::Callback {
    public:
        TimeoutCallback(AsyncTaskHandler& handler, std::shared_ptr<AsyncTask<T, U>> task) :
            task_(std::move(task)), handler_(handler)
        {
            assert(task_);
        }

        void timeoutExpired() noexcept override {
            if (task_->cancel()) {
                handler_.onTaskTimeotExpired();
            }
        }

        void callbackCanceled() noexcept override { }

    private:
        std::shared_ptr<AsyncTask<T, U>> task_;
        AsyncTaskHandler& handler_;
    };

    std::unique_ptr<HHWheelTimer::Callback> timeout_callback_;
    EventBase* evb_;
};
