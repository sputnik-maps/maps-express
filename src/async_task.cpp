#include "async_task.h"

#include "folly/io/async/EventBaseManager.h"

folly::EventBase* GetEventBase() {
    folly::EventBase* evb = folly::EventBaseManager::get()->getExistingEventBase();
    assert(evb);
    return evb;
}

bool RunInEventBaseThread(folly::EventBase* evb, std::function<void()>&& func) {
    assert(evb);
    return evb->runInEventBaseThread(std::move(func));
}


