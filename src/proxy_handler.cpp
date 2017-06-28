#include "proxy_handler.h"

#include <folly/io/async/EventBaseManager.h>


static const uint kMaxReconnects = 3;

ProxyHandler::ProxyHandler(Callbacks& callbacks, folly::HHWheelTimer& timer,
                      const folly::SocketAddress& addr, std::unique_ptr<proxygen::HTTPMessage> headers,
                      proxygen::ResponseHandler& downstream) :
        connector_(this, &timer),
        addr_(addr),
        headers_(std::move(headers)),
        callbacks_(callbacks),
        timer_(timer),
        downstream_(downstream)
{
    assert(headers_);
    headers_->setDstAddress(addr);
    Connect();
}

ProxyHandler::~ProxyHandler() {
    if (txn_) {
        txn_->sendAbort();
    }
}

void ProxyHandler::Connect() {
    const folly::AsyncSocket::OptionMap opts{
        {{SOL_SOCKET, SO_REUSEADDR}, 1}};
    connector_.reset();
    connector_.connect(folly::EventBaseManager::get()->getEventBase(), addr_,
                       std::chrono::seconds(20), opts);
}

void ProxyHandler::connectSuccess(proxygen::HTTPUpstreamSession* session) {
    session_ = SessionWrapper(session);
    txn_ = session->newTransaction(this);
    if (!txn_) {
        LOG(ERROR) << "Unable to create new transaction from " << session->getLocalAddress()
                   << " to " << session->getPeerAddress();
        callbacks_.OnProxyError();
        return;
    }
    txn_->sendHeadersWithEOM(*headers_);
    headers_sent_ = true;
}

void ProxyHandler::connectError(const folly::AsyncSocketException& ex) {
    LOG(ERROR) << ex;
    if (num_reconnects_ < kMaxReconnects) {
        ++num_reconnects_;
        Connect();
    } else {
        callbacks_.OnProxyConnectError();
    }
}

void ProxyHandler::setTransaction(proxygen::HTTPTransaction* txn) noexcept {}

void ProxyHandler::detachTransaction() noexcept {
    txn_ = nullptr;
}

void ProxyHandler::onHeadersComplete(std::unique_ptr<proxygen::HTTPMessage> msg) noexcept {
    downstream_.sendHeaders(*msg);
}

void ProxyHandler::onBody(std::unique_ptr<folly::IOBuf> chain) noexcept {
    downstream_.sendBody(std::move(chain));
}

void ProxyHandler::onTrailers(std::unique_ptr<proxygen::HTTPHeaders> trailers) noexcept {}

void ProxyHandler::onEOM() noexcept {
    downstream_.sendEOM();
    callbacks_.OnProxyEom();
}
void ProxyHandler::onUpgrade(proxygen::UpgradeProtocol protocol) noexcept {}

void ProxyHandler::onError(const proxygen::HTTPException& error) noexcept {
    callbacks_.OnProxyError();
}

void ProxyHandler::onEgressPaused() noexcept {
    downstream_.pauseIngress();
}
void ProxyHandler::onEgressResumed() noexcept {
    downstream_.resumeIngress();
}
