#pragma once

#include <condition_variable>
#include <deque>
#include <list>
#include <mutex>
#include <thread>

#include "async_task.h"
#include "worker.h"


template <typename Wrk, typename Task>
class ThreadPool {
    static_assert(std::is_base_of<Worker<Task>, Wrk>::value, "Actual worker have to subclass Worker class!");
    static_assert(std::is_nothrow_move_constructible<Task>::value &&
                  std::is_nothrow_default_constructible<Task>::value,
                  "Task should be nothrow default and nothrow move constructible!");

public:

    using worker_t = Wrk;
    using worker_fn_t = std::function<void(worker_t&)>;
    using task_t = Task;

    using WorkerInitTask = AsyncTask<worker_t*, worker_t*>;
    using success_init_cb_t = typename WorkerInitTask::result_cb_t;
    using fail_init_cb_t = typename WorkerInitTask::error_cb_t;

    ThreadPool(std::size_t queue_limit = 0) : queue_limit_(queue_limit) { }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&&) = delete;
    ThreadPool& operator=(ThreadPool&&) = delete;

    ~ThreadPool() {
        Stop();
    }

private:

    class WorkerHelper {
    public:
        WorkerHelper(std::unique_ptr<Wrk>&& wrk, std::shared_ptr<WorkerInitTask> init_task, std::deque<Task>& tasks,
                     std::mutex& mux, std::condition_variable& cv) :
                init_task_(std::move(init_task)),
                worker_(std::move(wrk)),
                tasks_(tasks),
                mux_(mux),
                cv_(cv) {
            assert(worker_);
            thread_ = std::thread(&WorkerHelper::Loop, this);
        }

        void Loop() {
            if (!worker_->Init()) {
                if (init_task_) {
                    init_task_->NotifyError(worker_.get());
                }
                return;
            }
            if (init_task_) {
                init_task_->SetResult(worker_.get());
            }

            Task task;
            worker_fn_t fn;
            bool process_task;
            while (!stop_flag_) {
                {
                    std::unique_lock<std::mutex> lock(mux_);
                    while (!stop_flag_ && functions_.empty() && tasks_.empty()) {
                        cv_.wait(lock);
                    }
                    if (stop_flag_) {
                        return;
                    }
                    if (!functions_.empty()) {
                        fn = std::move(functions_.front());
                        functions_.pop_front();
                        process_task = false;
                    } else {
                        task = std::move(tasks_.front());
                        tasks_.pop_front();
                        process_task = true;
                    }
                }
                if (process_task) {
                    worker_->ProcessTask(std::move(task));
                } else {
                    fn(*worker_);
                }
            }
        }

        inline const worker_t* worker_ptr() const noexcept {
            return worker_.get();
        }

        inline void stop() {
            stop_flag_ = true;
        }

        inline void join() {
            thread_.join();
        }

        std::thread thread_;
        std::deque<worker_fn_t> functions_;
        std::shared_ptr<WorkerInitTask> init_task_;
        std::unique_ptr<Wrk> worker_;
        std::deque<Task>& tasks_;
        std::mutex& mux_;
        std::condition_variable& cv_;
        std::atomic_bool stop_flag_{false};
    };

public:
    void Stop() {
        if (stopped_) {
            return;
        }
        for (auto& wh : workers_) {
            wh.stop();
        }
        wake_all();
        for (auto& wh : workers_) {
            wh.join();
        }
        stopped_ = true;
    }

    inline uint NumWorkers() const {
        std::lock_guard<std::mutex> lock(workers_mutex_);
        return workers_.size();
    }

    std::vector<const worker_t*> Workers() const {
        std::vector<const worker_t*> workers;
        std::lock_guard<std::mutex> lock(workers_mutex_);
        workers.reserve(workers_.size());
        for (const WorkerHelper& wh : workers_) {
            workers.push_back(wh.worker_ptr());
        }
        return workers;
    }

    inline void SetQueueLimit(std::size_t queue_limit) {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        queue_limit_ = queue_limit;
    }

    inline void PostTask(const task_t& task) {
        PostTaskImpl(task);
    }

    inline void PostTask(task_t&& task) {
        PostTaskImpl(std::move(task));
    }

    bool ExecuteOnWorker(worker_fn_t fn, const worker_t* const worker_ptr) {
        std::lock_guard<std::mutex> workers_lock(workers_mutex_);
        for (WorkerHelper& wh : workers_) {
            if (wh.worker_ptr() == worker_ptr) {
                std::lock_guard<std::mutex> queue_lock(queue_mutex_);
                wh.functions_.push_back(std::move(fn));
                wake_all();
                return true;
            }
        }
        return false;
    }

    void PushWorker(std::unique_ptr<worker_t> worker) {
        assert(worker);
        std::lock_guard<std::mutex> lock(workers_mutex_);
        workers_.emplace_back(std::move(worker), nullptr, tasks_, queue_mutex_, cv_);
    }

    void PushWorker(std::unique_ptr<Wrk> worker, std::shared_ptr<WorkerInitTask> init_task = nullptr) {
        assert(worker);
        std::lock_guard<std::mutex> lock(workers_mutex_);
        workers_.emplace_back(std::move(worker), std::move(init_task), tasks_, queue_mutex_, cv_);
    }

    void RemoveWorkers(uint num_workers) {
        if (num_workers == 0) {
            return;
        }
        std::list<WorkerHelper> wh_to_remove;
        {
            std::lock_guard<std::mutex> lock(workers_mutex_);
            typename std::list<WorkerHelper>::iterator end_iter;
            if (num_workers >= workers_.size()) {
                end_iter = workers_.end();
            } else {
                end_iter = workers_.begin() + (num_workers - 1);
            }
            wh_to_remove.splice(wh_to_remove.end(), workers_, workers_.begin(), end_iter);
        }
        for (WorkerHelper& wh : wh_to_remove) {
            wh.stop();
        }
        wake_all();
        for (WorkerHelper& wh : wh_to_remove) {
            wh.join();
        }
    }

    bool RemoveWorker(worker_t* wrk_ptr) {
        std::list<WorkerHelper> wh_to_remove;
        bool found = false;
        {
            std::lock_guard<std::mutex> lock(workers_mutex_);
            for (auto wh_itr = workers_.begin(); wh_itr != workers_.end(); ++wh_itr) {
                if (wh_itr->worker.get() == wrk_ptr) {
                    wh_to_remove.splice(wh_to_remove.end(), workers_, wh_itr);
                    found = true;
                    break;
                }
            }
        }
        if (found) {
            WorkerHelper& wh = wh_to_remove.front();
            wh.stop();
            wake_all();
            wh.join();
            return true;
        }
        return false;
    }


private:
    inline void wake_one() noexcept {
        cv_.notify_one();
    }

    inline void wake_all() noexcept {
        cv_.notify_all();
    }

    template <typename T>
    void PostTaskImpl(T&& task) {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (queue_limit_ && tasks_.size() >= queue_limit_) {
            tasks_.pop_front();
        }
        tasks_.push_back(std::forward<T>(task));
        wake_one();
    }

    std::deque<Task> tasks_;
    std::list<WorkerHelper> workers_;
    mutable std::mutex workers_mutex_;
    mutable std::mutex queue_mutex_;
    mutable std::condition_variable cv_;
    std::size_t queue_limit_;
    bool stopped_{false};
};
