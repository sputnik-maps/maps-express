#pragma once

#include "base_handler.h"
#include "endpoint.h"
#include "rendermanager.h"

// TODO: maybe remove

class UpdateHandler : public BaseHandler {
public:
    using endpoint_t =  std::vector<std::shared_ptr<EndpointParams>>;
    using endpoints_map_t = std::unordered_map<std::string, endpoint_t>;

    explicit UpdateHandler(RenderManager* renderManager, endpoints_map_t& endpoints);

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;
    void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;
    void onSuccessEOM() noexcept override;

private:
    bool UpdateMvt(std::shared_ptr<std::string> new_style, uint zoom);
    bool UpdateRender(const std::string& style_name, std::shared_ptr<std::string> new_style);

    std::string map_base_path_;
    std::unique_ptr<folly::IOBuf> response_body_;
    std::unique_ptr<folly::IOBuf> request_body_;
    std::unique_ptr<std::set<std::string>> styles_to_update_;
    RenderManager* rm;
    endpoints_map_t& endpoints_;
    int zoom_{-1};
    bool mvt_map_{false};
};
