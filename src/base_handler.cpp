#include "base_handler.h"

#include <glog/logging.h>

#include <proxygen/httpserver/ResponseBuilder.h>

#include "util.h"

void BaseHandler::requestComplete() noexcept {
    delete this;
}

void BaseHandler::onError(proxygen::ProxygenError err) noexcept {
    LOG(ERROR) << proxygen::getErrorString(err);
    delete this;
}

void BaseHandler::onEOM() noexcept {
    if (!error_sent_) {
        onSuccessEOM();
    }
}

void BaseHandler::SendError(uint16_t err_code) {
    error_sent_ = true;
    proxygen::ResponseBuilder(downstream_)
            .status(err_code, util::http_status_msg(err_code))
            .sendWithEOM();
    OnErrorSent(err_code);
}

void BaseHandler::OnErrorSent(std::uint16_t err_code) noexcept {}

void BaseHandler::onUpgrade(proxygen::UpgradeProtocol proto) noexcept {
    // handler doesn't support upgrades
}
