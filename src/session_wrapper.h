#pragma once

#include <proxygen/lib/http/session/HTTPUpstreamSession.h>

class SessionWrapper : public proxygen::HTTPSession::InfoCallback {
public:
    SessionWrapper() = default;

    explicit SessionWrapper(proxygen::HTTPUpstreamSession* session) : session_(session) {
        assert(session);
        session_->setInfoCallback(this);
    }

    SessionWrapper(const SessionWrapper&) = delete;
    SessionWrapper& operator=(const SessionWrapper&) = delete;

    SessionWrapper& operator=(SessionWrapper&& other) {
        if (session_) {
            session_->drain();
        }
        session_ = other.session_;
        other.session_ = nullptr;
        if (session_) {
            session_->setInfoCallback(this);
        }
        return *this;
    }

    ~SessionWrapper() {
        if (session_) {
            session_->drain();
        }
    }

    proxygen::HTTPUpstreamSession* operator->() const {
        return session_;
    }

    proxygen::HTTPUpstreamSession* get() const {
        return session_;
    }

    explicit operator bool() const {
        return session_;
    }

    // Note: you must not start any asynchronous work from onCreate()
    void onCreate(const proxygen::HTTPSession&) override {}
    void onIngressError(const proxygen::HTTPSession&,
                        proxygen::ProxygenError) override {}
    void onIngressEOF() override {}
    void onRead(const proxygen::HTTPSession&, size_t bytesRead) override {}
    void onWrite(const proxygen::HTTPSession&, size_t bytesWritten) override {}
    void onRequestBegin(const proxygen::HTTPSession&) override {}
    void onRequestEnd(const proxygen::HTTPSession&,
                      uint32_t maxIngressQueueSize) override {}
    void onActivateConnection(const proxygen::HTTPSession&) override {}
    void onDeactivateConnection(const proxygen::HTTPSession&) override {}
    // Note: you must not start any asynchronous work from onDestroy()
    void onDestroy(const proxygen::HTTPSession&) override {
        session_ = nullptr;
    }
    void onIngressMessage(const proxygen::HTTPSession&,
                          const proxygen::HTTPMessage&) override {}
    void onIngressLimitExceeded(const proxygen::HTTPSession&) override {}
    void onIngressPaused(const proxygen::HTTPSession&) override {}
    void onTransactionDetached(const proxygen::HTTPSession&) override {}
    void onPingReplySent(int64_t latency) override {}
    void onPingReplyReceived() override {}
    void onSettingsOutgoingStreamsFull(const proxygen::HTTPSession&) override {}
    void onSettingsOutgoingStreamsNotFull(const proxygen::HTTPSession&) override {}
    void onFlowControlWindowClosed(const proxygen::HTTPSession&) override {}
    void onEgressBuffered(const proxygen::HTTPSession&) override {}
    void onEgressBufferCleared(const proxygen::HTTPSession&) override {}

private:
    proxygen::HTTPUpstreamSession* session_{nullptr};
};
