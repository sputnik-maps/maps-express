#include "async_task_handler.h"

#include <folly/io/async/EventBaseManager.h>

AsyncTaskHandler::AsyncTaskHandler() {
    evb_ = folly::EventBaseManager::get()->getEventBase();
}

void AsyncTaskHandler::onTaskTimeotExpired() noexcept {
    SendError(500);
}
