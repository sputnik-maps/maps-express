#include <csignal>

#include <mapnik/config.hpp>
#include <mapnik/datasource_cache.hpp>

#include <folly/Memory.h>
#include <folly/io/async/EventBaseManager.h>
#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/httpserver/HTTPServer.h>

#include "etcd_config.h"
#include "httphandlerfactory.h"
#include "json_config.h"
#include "json_util.h"
#include "nodes_monitor.h"
#include "status_monitor.h"


using folly::EventBase;
using folly::EventBaseManager;
using folly::SocketAddress;

using HTTPServer = proxygen::HTTPServer;
using Protocol = HTTPServer::Protocol;

using json_util::FromJson;

static const double kVersion = 0.4;

static const uint kDefaultPort = 8080;
static const std::string kDefaultIp = "0.0.0.0";

static const std::string kHelpStr = R"help(
Maps Express.

Usage:
    maps-express <host>:<port> json <json-config-path> [OPTIONS]
    maps-express <host>:<port> etcd <etcd-host>  [OPTIONS]

Options:
    --internal-port <port>  Port for internode communications.
    --bind-addr <addr>      Bind address.
)help";

namespace {

class SignalHandler {
public:
    SignalHandler(std::shared_ptr<StatusMonitor> monitor, HTTPServer& server, NodesMonitor* nodes_monitor) :
            monitor_(std::move(monitor)),
            server_(server),
            nodes_monitor_(nodes_monitor) {}

    void HandleSighup() {
        StatusMonitor::Status prev_status = monitor_->exchange_status(StatusMonitor::Status::maintenance);
        if (prev_status == StatusMonitor::Status::maintenance) {
            return;
        }
        LOG(INFO) << "Switching to maintenance mode!";
        if (nodes_monitor_) {
            nodes_monitor_->Unregister();
        }
        std::this_thread::sleep_for(std::chrono::seconds(10));
        LOG(INFO) << "Stopping server!";
        server_.stop();
    }

private:
    std::shared_ptr<StatusMonitor> monitor_;
    HTTPServer& server_;
    NodesMonitor* nodes_monitor_;
};


class EtcdHelper {
public:
    // EtcdClient owns event base thread. EtcdConfig's and NodesMonitor's loops run in that thread.
    EtcdHelper(const std::string& etcd_host, const std::string& server_host, uint server_port) :
            client_(std::make_shared<EtcdClient>(etcd_host, 2379u, 3)),
            config(client_),
            nodes_monitor(server_host, server_port, client_) {}

    ~EtcdHelper() {
        // Stop EtcdClient, EtcdConfig's and NodesMonitor's loops will be stopped to.
        client_->Shutdown();
    }

private:
    std::shared_ptr<EtcdClient> client_;

public:
    EtcdConfig config;
    NodesMonitor nodes_monitor;
};

std::atomic<SignalHandler*> gSignalHandler{nullptr};

} // ns anonymous

void signal_handler(int signal) {
    SignalHandler* handler_ptr = gSignalHandler;
    if (handler_ptr != nullptr) {
        handler_ptr->HandleSighup();
    }
}


static void PrintHelpAndExit() {
    std::cout << kHelpStr << std::endl;
    std::exit(1);
}


struct ProgramOptions {
    ProgramOptions() = default;

    enum class ConfigType {
        etcd,
        json
    };

    std::string host;
    std::string bind_addr;
    std::string config_path;
    std::uint16_t port{0};
    std::uint16_t internal_http_port{0};
    ConfigType config_type;
};


static ProgramOptions ParseProgramOptions(int argc, char* argv[]) {
    ProgramOptions options;
    if (argc < 4) {
        PrintHelpAndExit();
    }

    const std::string host_port = argv[1];
    auto del_pos = host_port.find(':');
    if (del_pos == host_port.npos) {
        PrintHelpAndExit();
    }
    options.host = host_port.substr(0, del_pos);
    try {
        options.port = static_cast<std::uint16_t>(std::stoi(host_port.substr(del_pos + 1)));
    } catch (...) {
        PrintHelpAndExit();
    }

    const std::string config_type = argv[2];
    if (config_type == "json") {
        options.config_type = ProgramOptions::ConfigType::json;
    } else if (config_type == "etcd") {
        options.config_type = ProgramOptions::ConfigType::etcd;
    } else {
        std::cout << "Invlid config type: " << config_type << "\n" << std::endl;
        PrintHelpAndExit();
    }

    options.config_path = argv[3];

    int argpos = 4;
    while (argpos < argc) {
        const std::string opt_name = argv[argpos++];
        if (opt_name == "--internal-port") {
            if (argpos == argc) {
                PrintHelpAndExit();
            }
            try {
                options.internal_http_port = static_cast<std::uint16_t>(std::stoi(argv[argpos++]));
            } catch (...) {
                PrintHelpAndExit();
            }
        } else if (opt_name == "--bind-addr") {
            if (argpos == argc) {
                PrintHelpAndExit();
            }
            options.bind_addr = argv[argpos++];
        } else {
            std::cout << "Unknow option " << opt_name << std::endl;
            PrintHelpAndExit();
        }
    }
    if (options.internal_http_port == 0) {
        options.internal_http_port = options.port + 1;
    }

    return options;
}


int main(int argc, char* argv[]) {
    std::signal(SIGHUP, signal_handler);
    FLAGS_logtostderr = true;
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    ProgramOptions p_options = ParseProgramOptions(argc, argv);

    std::unique_ptr<JsonConfig> json_config;
    std::unique_ptr<EtcdHelper> etcd_helper;
    Config* config = nullptr;
    if (p_options.config_type == ProgramOptions::ConfigType::json) {
        json_config = std::make_unique<JsonConfig>(p_options.config_path);
        config = json_config.get();
    } else {
        etcd_helper = std::make_unique<EtcdHelper>(p_options.config_path, p_options.host,
                                                   p_options.internal_http_port);
        config = &etcd_helper->config;
    }

    assert(config);
    if (!config->Valid()) {
        LOG(FATAL) << "Unable to load config!";
        return -1;
    }

    std::shared_ptr<const Json::Value> japp_ptr = config->GetValue("app");
    assert(japp_ptr);
    const Json::Value& japp = *japp_ptr;
    const Json::Value& jlog_dir = japp["log_dir"];
    if (jlog_dir.isString()) {
        FLAGS_log_dir = jlog_dir.asCString();
        FLAGS_logtostderr = false;
    };

    auto dscache = mapnik::datasource_cache::instance;
    if(!dscache().register_datasources(MAPNIK_PLUGINDIR)){
        LOG(FATAL) << "could not register postgis plugin";
    }

    const std::string& bind_addr = p_options.bind_addr.empty() ? p_options.host : p_options.bind_addr;

    std::vector<HTTPServer::IPConfig> IPs = {
        {SocketAddress(bind_addr, p_options.port, true), Protocol::HTTP},
        {SocketAddress(bind_addr, p_options.internal_http_port, true), Protocol::HTTP},
    };

    auto monitor = std::make_shared<StatusMonitor>();
    NodesMonitor* nodes_monitor = nullptr;
    if (etcd_helper) {
        nodes_monitor = &etcd_helper->nodes_monitor;
    }

    proxygen::HTTPServerOptions options;
    options.threads = std::thread::hardware_concurrency();
    options.idleTimeout = std::chrono::seconds(30);
    options.shutdownOn = {SIGINT, SIGTERM};
    options.enableContentCompression = true;
    options.contentCompressionLevel = 5;
    options.handlerFactories = proxygen::RequestHandlerChain()
        .addThen<HttpHandlerFactory>(*config, monitor, std::to_string(p_options.internal_http_port), nodes_monitor)
        .build();

    LOG(INFO) << "starting... Maps Express " << kVersion << std::endl;

    HTTPServer server(std::move(options));
    server.bind(IPs);

    // Start HTTPServer mainloop in a separate thread
    std::thread t([&] () {
    LOG(INFO) << "running..." << std::endl;
        server.start();
    });

    SignalHandler sh(monitor, server, nodes_monitor);
    gSignalHandler.store(&sh);

    if (nodes_monitor) {
        nodes_monitor->Register();
    }

    t.join();
    std::signal(SIGHUP, SIG_DFL);

    return 0;
}
