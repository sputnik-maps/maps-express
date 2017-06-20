#pragma once

#include "base_handler.h"

class ProxyHandler : public BaseHandler {
public:
    explicit ProxyHandler();

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

    void onEOM() noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError err) noexcept override;

private:

};
