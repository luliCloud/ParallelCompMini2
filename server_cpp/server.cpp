// Mini2 gRPC Server Main
// Phase 1: Ping, Query (local), Forward

#include <iostream>
#include <memory>
#include <filesystem>
#include <string>
#include <yaml-cpp/yaml.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/security/server_credentials.h>

#include "Mini2ServiceImpl.h"

using grpc::Server;
using grpc::ServerBuilder;
namespace fs = std::filesystem;

namespace {

fs::path ResolveConfigPath(const std::string& node_id) {
    const fs::path relative = fs::path("config") / ("node_" + node_id + ".yaml");
    if (fs::exists(relative)) {
        return relative;
    }

    const fs::path source_relative = fs::path(MINI2_SOURCE_DIR) / relative;
    if (fs::exists(source_relative)) {
        return source_relative;
    }

    return relative;
}

fs::path ResolveDatasetPath(const YAML::Node& config, const fs::path& config_path) {
    const fs::path configured = config["dataset_path"].as<std::string>();
    if (configured.is_absolute()) {
        return configured;
    }

    const fs::path from_cwd = fs::current_path() / configured;
    if (fs::exists(from_cwd)) {
        return from_cwd;
    }

    const fs::path from_source_dir = fs::path(MINI2_SOURCE_DIR) / configured;
    if (fs::exists(from_source_dir)) {
        return from_source_dir;
    }

    return config_path.parent_path() / configured;
}

std::string ResolveOptionalPath(
    const YAML::Node& config,
    const char* key,
    const fs::path& config_path) {
    if (!config[key]) {
        return "";
    }

    const fs::path configured = config[key].as<std::string>();
    if (configured.is_absolute()) {
        return configured.string();
    }

    const fs::path from_cwd = fs::current_path() / configured;
    if (fs::exists(from_cwd)) {
        return from_cwd.string();
    }

    const fs::path from_source_dir = fs::path(MINI2_SOURCE_DIR) / configured;
    if (fs::exists(from_source_dir)) {
        return from_source_dir.string();
    }

    return (config_path.parent_path() / configured).string();
}

}  // namespace

// ===== Server Startup =====
void RunServer(const std::string& node_id)
{
    const fs::path config_path = ResolveConfigPath(node_id);

    try
    {
        YAML::Node config = YAML::LoadFile(config_path.string());

        std::string host = config["host"].as<std::string>();
        uint16_t port = config["port"].as<uint16_t>();
        bool coordinator_only = config["coordinator_only"] && config["coordinator_only"].as<bool>();
        // Cache policy is configured per node in YAML instead of hardcoding node A in C++.
        bool enable_cache = config["enable_cache"] && config["enable_cache"].as<bool>();

        std::string server_address = host + ":" + std::to_string(port);
        Mini2ServiceImpl service(node_id, port, enable_cache);

        if (config["peers"]) {
            std::vector<PeerInfo> peers;
            for (auto const& peer_node : config["peers"]) {
                PeerInfo p;
                p.id = peer_node["id"].as<std::string>();
                p.address = peer_node["host"].as<std::string>() + ":" +
                           std::to_string(peer_node["port"].as<int>());
                peers.push_back(p);
            }
            service.SetPeers(peers);
        }

        // Initialize dataset
        if (!coordinator_only) {
            const fs::path dataset_path = ResolveDatasetPath(config, config_path);
            const std::string agency_dict_path =
                ResolveOptionalPath(config, "agency_dict_path", config_path);
            const std::string borough_dict_path =
                ResolveOptionalPath(config, "borough_dict_path", config_path);
            const std::string status_dict_path =
                ResolveOptionalPath(config, "status_dict_path", config_path);

            if (!service.Initialize(
                    dataset_path.string(),
                    agency_dict_path,
                    borough_dict_path,
                    status_dict_path)) {
                std::cerr << "Failed to initialize dataset at node: " << node_id << std::endl;
                return;
            }
        } else {
            std::cout << "Node " << node_id << " is coordinator-only, skipping dataset initialization." << std::endl;
        }

        // Build and start server
        ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);
        std::unique_ptr<Server> server(builder.BuildAndStart());

        std::cout << "\n============================================" << std::endl;
        std::cout << "Mini2 gRPC Server started" << std::endl;
        std::cout << "[" << node_id << "] Server listening on " << server_address << std::endl;
        std::cout << "============================================\n" << std::endl;

        server->Wait();
    } catch (const std::exception& e)
    {
        std::cerr << "Node " << node_id << " failed to load config from "
                  << config_path << ": " << e.what() << std::endl;
        return;
    }
}

// ===== Main Function =====
int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <NodeID>" << std::endl;
        return 1;
    }

    std::string node_id = argv[1];
    std::cout << "Starting node: " << node_id << std::endl;

    RunServer(node_id);
    return 0;
}

// Run command: ./build/bin/server A benchmarks/workload_100k.csv
