#pragma once

#include "base_handler.h"
#include "status_monitor.h"

class MonHandler : public BaseHandler {
public:
    explicit MonHandler(std::shared_ptr<StatusMonitor> monitor);

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;
    void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;
    void onSuccessEOM() noexcept override;

private:
   std::shared_ptr<StatusMonitor> monitor_;
};
