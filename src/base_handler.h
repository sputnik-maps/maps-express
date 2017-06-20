#pragma once

#include <proxygen/httpserver/RequestHandler.h>

class BaseHandler : public proxygen::RequestHandler {
public:
    // Will not be called if an error has been sent!
    virtual void onSuccessEOM() noexcept = 0;

    virtual void requestComplete() noexcept override;
    virtual void onError(proxygen::ProxygenError err) noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override;
    void onEOM() noexcept override;

    inline bool error_sent() noexcept {
        return error_sent_;
    }

protected:
    virtual void OnErrorSent(std::uint16_t err_code) noexcept;

    void SendError(std::uint16_t err_code);

private:
    bool error_sent_{false};
};
