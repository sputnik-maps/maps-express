#include "mon_handler.h"

#include <proxygen/httpserver/ResponseBuilder.h>


MonHandler::MonHandler(std::shared_ptr<StatusMonitor> monitor) : monitor_(std::move(monitor)) { }

void MonHandler::onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept {
    if (monitor_ == nullptr) {
        SendError(500);
        return;
    }
    std::unique_ptr<folly::IOBuf> msg{nullptr};
    switch (monitor_->status()) {
    case StatusMonitor::Status::ok:
        msg = folly::IOBuf::copyBuffer("OK");
        break;
    case StatusMonitor::Status::maintenance:
        msg = folly::IOBuf::copyBuffer("MAINTENANCE");
        break;
    case StatusMonitor::Status::fail:
        msg = folly::IOBuf::copyBuffer("FAIL");
        break;
    }
    proxygen::ResponseBuilder rb(downstream_);
    rb.status(200, "OK");
    rb.body(std::move(msg));
    rb.sendWithEOM();
}

void MonHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept { }

void MonHandler::onSuccessEOM() noexcept { }
