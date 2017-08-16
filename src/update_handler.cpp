#include "update_handler.h"

#include <jsoncpp/json/value.h>
#include <jsoncpp/json/json.h>
#include <proxygen/httpserver/ResponseBuilder.h>

#include "load_map.h"
#include "util.h"

UpdateHandler::UpdateHandler(RenderManager *renderManager, endpoints_map_t &endpoints) :
    rm(renderManager), endpoints_(endpoints), zoom_(-1) {}

void UpdateHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {
    if (error_sent()) {
        return;
    }
    if (request_body_) {
        request_body_->prependChain(std::move(body));
    } else {
        request_body_ = std::move(body);
    }
}

void UpdateHandler::onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept {
    const std::string& requested_styles = headers->getQueryParam("styles");
    if (requested_styles.empty()) {
        SendError(400);
        return;
    }
    styles_to_update_ = util::ParseArray(requested_styles);
    const std::string& zoom_param = headers->getQueryParam("z");
    if (!zoom_param.empty() && !(zoom_ = std::atoi(zoom_param.c_str()))) {
        SendError(400);
    }
    const std::string& map_type = headers->getQueryParam("type");
    if (map_type == "mvt") {
        mvt_map_ = true;
    }
    map_base_path_ = headers->getQueryParam("base_path");
}

void UpdateHandler::onSuccessEOM() noexcept {
    folly::fbstring new_style_fb = request_body_->moveToFbString();
    auto new_style = std::make_shared<std::string>(new_style_fb.begin(), new_style_fb.end());

    Json::Value jupdated_lyrs(Json::arrayValue);

    for (const auto& style_name : *styles_to_update_) {
        if (style_name == "mvt") {
            if (!UpdateMvt(new_style, zoom_)) {
                continue;
            }
        } else {
            if (!UpdateRender(style_name, new_style)) {
                continue;
            }
        }
        jupdated_lyrs.append(Json::Value(style_name));
    }

    if (jupdated_lyrs.empty()) {
        LOG(ERROR) << "No styles updated!";
        SendError(500);
        return;
    }

    Json::Value jresp(Json::objectValue);
    jresp["updated_layers"] = jupdated_lyrs;

    proxygen::ResponseBuilder(downstream_)
            .status(200, "OK")
            .body(jresp.toStyledString())
            .sendWithEOM();
}

bool UpdateHandler::UpdateMvt(std::shared_ptr<std::string> new_style, uint zoom) {
    std::unique_ptr<mapnik::Map> map = nullptr;
    bool result = false;
    bool exact_zoom = zoom > 0;
    for (auto& endpiont_params_itr : endpoints_) {
        endpoint_t& endpoint = endpiont_params_itr.second;
        if (endpoint.empty() || endpoint[0]->type != EndpointType::mvt) {
            continue;
        }
        for (std::shared_ptr<EndpointParams>& ep : endpoint) {
            if (exact_zoom && (ep->minzoom > zoom || ep->maxzoom < zoom)) {
                continue;
            }
            if (!map) {
                map = std::make_unique<mapnik::Map>();
                try {
                    sputnik::load_map_string(*map, *new_style, false, map_base_path_, mvt_map_);
                } catch (const std::exception& e) {
                    LOG(ERROR) << "Error while updating map: " << e.what();
                    return false;
                }
            }
            ep->filter_table = FilterTable::MakeFilterTable(*map, ep->filter_table->zoom_groups(), 1, ep->minzoom);
            if (exact_zoom) {
                return true;
            }
            result = true;
        }
    }
    return result;
}

bool UpdateHandler::UpdateRender(const std::string& style_name, std::shared_ptr<std::string> new_style) {
    // TODO
//    std::shared_ptr<StyleUpdate> style_update = std::make_shared<StyleUpdate>();
//    style_update->new_style = std::move(new_style);
//    style_update->mvt_style_format = mvt_map_;
//    style_update->base_path = map_base_path_;
//    return rm->Update(style_name, std::move(style_update));
    return false;
}
