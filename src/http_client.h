#pragma once

#include <folly/io/async/EventBase.h>

#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/utils/URL.h>

#include "async_task.h"

class HTTPWorker;
class RequestInfo;

struct HTTPResponse {
    std::unique_ptr<proxygen::HTTPMessage> headers;
    std::unique_ptr<folly::IOBuf> body;
};

using http_response_ptr = std::shared_ptr<HTTPResponse>;
using HTTPTask = AsyncTask<http_response_ptr>;
using http_task_ptr = std::shared_ptr<HTTPTask>;

class HTTPClient {
public:

    HTTPClient(folly::EventBase& evb, const std::string& host, uint16_t port = 80, uint8_t num_workers = 1);
    ~HTTPClient();

    // All methods are thread safe

    void Request(http_task_ptr async_task, proxygen::HTTPMethod method, const std::string& url,
                 const proxygen::HTTPHeaders* headers = nullptr, std::unique_ptr<folly::IOBuf> body = nullptr);

    // Should be called from thread other then HTTPClient event base thread
    http_response_ptr RequestAndWait(proxygen::HTTPMethod method, const std::string& url,
                                     const proxygen::HTTPHeaders* headers = nullptr,
                                     std::unique_ptr<folly::IOBuf> body = nullptr);

    void Shutdown();

    inline folly::EventBase& get_event_base() const noexcept {
        return evb_;
    }

private:
    std::vector<std::unique_ptr<HTTPWorker>> workers_pool_;
    std::deque<std::unique_ptr<RequestInfo>> pending_requests_;
    folly::EventBase& evb_;
    folly::HHWheelTimer::UniquePtr timer_;
    std::string host_;
    uint16_t port_;
    bool stopped_{false};
};
