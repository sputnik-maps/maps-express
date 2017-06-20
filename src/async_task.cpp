#include "async_task.h"

#include "folly/io/async/EventBaseManager.h"

folly::EventBase* GetEventBase() {
    return folly::EventBaseManager::get()->getExistingEventBase();
}

bool RunInEventBaseThread(folly::EventBase* evb, std::function<void()>&& func) {
    assert(evb);
    return evb->runInEventBaseThread(std::move(func));
}


