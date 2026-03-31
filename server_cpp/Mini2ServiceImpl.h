#pragma once

#include <string>
#include <grpcpp/server_context.h>
#include "mini2.grpc.pb.h"
#include "dataset.hpp"

using grpc::ServerContext;
using grpc::Status;
using mini2::NodeService;
using mini2::QueryRequest;
using mini2::QueryResponse;
using google::protobuf::Empty;

// Mini2 Node Service Implementation
class Mini2ServiceImpl final : public NodeService::Service {
public:
    explicit Mini2ServiceImpl(const std::string& node_id, uint16_t port);
    ~Mini2ServiceImpl() = default;

    // Initialization
    bool Initialize(const std::string& dataset_path);

    // Service methods (gRPC overrides)
    Status Ping(ServerContext* context, const Empty* request, 
                Empty* response) override;

    Status Query(ServerContext* context, const QueryRequest* request, 
                 QueryResponse* response) override;

    Status Forward(ServerContext* context, const QueryRequest* request, 
                   QueryResponse* response) override;

private:
    std::string node_id_;
    uint16_t port_;
    Dataset dataset_;
};
