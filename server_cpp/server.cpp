// Mini2 gRPC Server Main
// Phase 1: Ping, Query (local), Forward

#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/security/server_credentials.h>

#include "Mini2ServiceImpl.h"

using grpc::Server;
using grpc::ServerBuilder;

// ===== Server Startup =====
void RunServer(const std::string& node_id, const std::string& dataset_path) {
    // Default port mapping for nodes A-I
    uint16_t port = 50051;
    if (node_id == "A") port = 50051;
    else if (node_id == "B") port = 50052;
    else if (node_id == "C") port = 50053;
    else if (node_id == "D") port = 50054;
    else if (node_id == "E") port = 50055;
    else if (node_id == "F") port = 50056;
    else if (node_id == "G") port = 50057;
    else if (node_id == "H") port = 50058;
    else if (node_id == "I") port = 50059;

    std::string server_address = "0.0.0.0:" + std::to_string(port);
    Mini2ServiceImpl service(node_id, port);

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

    if (server) {
        server->Wait();
    } else {
        std::cerr << "Failed to start server at node: " << node_id << std::endl;
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