#pragma once

#include <algorithm>
#include <list>
#include <mutex>


template <typename ...Args>
class Observable;

template <typename ...Args>
class Observer {
public:
    using observable_t = Observable<Args...>;

    virtual ~Observer() {
        auto observables = observables_;
        for (observable_t* observable : observables) {
            observable->DetachObserver(this);
        }
    }

private:
    virtual void OnUpdate(Args... args) = 0;

    std::list<observable_t*> observables_;

    friend class Observable<Args...>;
};


template <typename ...Args>
class Observable {
public:
    using observer_t = Observer<Args...>;

    virtual ~Observable() = default;

    bool AttachObserver(observer_t* observer) {
        if (!observer) {
            return false;
        }
        {
            std::lock_guard<std::mutex> lock(mux_);
            if (std::find(observers_.begin(), observers_.end(), observer) != observers_.end()) {
                return false;
            }
            observers_.push_back(observer);
        }
        observer->observables_.push_back(this);
        OnObserverAttached(observer);
        return true;
    }

    bool DetachObserver(observer_t* observer) {
        if (!observer) {
            return false;
        }
        {
            std::lock_guard<std::mutex> lock(mux_);
            if (std::find(observers_.begin(), observers_.end(), observer) == observers_.end()) {
                return false;
            }
            observers_.remove(observer);
        }
        observer->observables_.remove(this);
        OnObserverDetached(observer);
        return true;
    }

protected:
    void NotifyObservers(Args... args) {
        std::lock_guard<std::mutex> lock(mux_);
        for (observer_t* observer : observers_) {
            observer->OnUpdate(args...);
        }
    }

private:
    virtual void OnObserverAttached(observer_t* observer) {}
    virtual void OnObserverDetached(observer_t* observer) {}

    std::list<observer_t*> observers_;
    std::mutex mux_;

};
