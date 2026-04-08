// Mini2 gRPC Server Main
// Phase 1: Ping, Query (local), Forward

#include <iostream>
#include <memory>
#include <string>
#include <yaml-cpp/yaml.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/security/server_credentials.h>

#include "Mini2ServiceImpl.h"

using grpc::Server;
using grpc::ServerBuilder;

// ===== Server Startup =====
void RunServer(const std::string& node_id, const std::string& dataset_path)
{
    std::string config_path = "config/node_" + node_id + ".yaml";

    try
    {
        YAML::Node config = YAML::LoadFile(config_path);

        std::string host = config["host"].as<std::string>();
        uint16_t port = config["port"].as<uint16_t>();

        std::string server_address = host + ":" + std::to_string(port);
        Mini2ServiceImpl service(node_id, port);

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
        if (!service.Initialize(dataset_path)) {
            std::cerr << "Failed to initialize dataset at node: " << node_id << std::endl;
            return;
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
        std::cerr << "Node " << node_id << " failed to load config from " << config_path << ": " << e.what() << std::endl;
        return;
    }
}

// ===== Main Function =====
int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <NodeID> <DatasetPath>" << std::endl;
        return 1;
    }

    std::string node_id = argv[1];
    std::string dataset_path = argv[2];

    std::cout << "Starting node: " << node_id << std::endl;
    std::cout << "Dataset: " << dataset_path << std::endl;

    RunServer(node_id, dataset_path);
    return 0;
}