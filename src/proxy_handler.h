#pragma once

#include <proxygen/lib/http/HTTPConnector.h>
#include <proxygen/lib/http/session/HTTPTransaction.h>
#include <proxygen/httpserver/ResponseHandler.h>

#include "session_wrapper.h"

class ProxyHandler : public proxygen::HTTPTransactionHandler,
                     private proxygen::HTTPConnector::Callback  {
public:
    class Callbacks {
    public:
        virtual void OnProxyEom() noexcept = 0;
        virtual void OnProxyError() noexcept = 0;
    };

    explicit ProxyHandler(Callbacks& callbacks, folly::HHWheelTimer& timer,
                          const folly::SocketAddress& addr, std::unique_ptr<proxygen::HTTPMessage> headers,
                          proxygen::ResponseHandler& downstream);
    ~ProxyHandler();

private:
    void connectSuccess(proxygen::HTTPUpstreamSession* session) override;
    void connectError(const folly::AsyncSocketException& ex) override;

    void setTransaction(proxygen::HTTPTransaction* txn) noexcept override;
    void detachTransaction() noexcept override;
    void onHeadersComplete(std::unique_ptr<proxygen::HTTPMessage> msg) noexcept override;
    void onBody(std::unique_ptr<folly::IOBuf> chain) noexcept override;
    void onTrailers(std::unique_ptr<proxygen::HTTPHeaders> trailers) noexcept override;
    void onEOM() noexcept override;
    void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;
    void onError(const proxygen::HTTPException& error) noexcept override;
    void onEgressPaused() noexcept override;
    void onEgressResumed() noexcept override;

    proxygen::HTTPConnector connector_;
    std::unique_ptr<proxygen::HTTPMessage> headers_;
    proxygen::HTTPTransaction* txn_{nullptr};
    SessionWrapper session_;
    Callbacks& callbacks_;
    folly::HHWheelTimer& timer_;
    proxygen::ResponseHandler& downstream_;
};
