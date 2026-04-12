#pragma once

#include <string>
#include <vector>

#include <grpcpp/server_context.h>

#include "mini2.grpc.pb.h"
#include "RequestJobQueue.h"
#include "dataset.hpp"

using grpc::ServerContext;
using grpc::Status;
using mini2::NodeService;
using mini2::QueryRequest;
using mini2::QueryResponse;
using mini2::PingRequest;
using mini2::PingResponse;

struct PeerInfo
{
    std::string id;
    std::string address;
};

struct ConnectedPeer {
    std::string id;
    std::unique_ptr<mini2::NodeService::Stub> stub;
};

// Mini2 Node Service Implementation
class Mini2ServiceImpl final : public NodeService::Service {
public:
    explicit Mini2ServiceImpl(const std::string& node_id, uint16_t port);
    ~Mini2ServiceImpl() override;

    // Initialization
    bool Initialize(const std::string& dataset_path);
    // Set peer and set up connection
    void SetPeers(const std::vector<PeerInfo>& peers);

    // Service methods (gRPC overrides)
    Status Ping(ServerContext* context, const PingRequest* request,
                PingResponse* response) override;

    Status Query(ServerContext* context, const QueryRequest* request,
                 QueryResponse* response) override;

    Status Forward(ServerContext* context, const QueryRequest* request,
                   QueryResponse* response) override;

private:
    // Execute local search for a Query job.
    QueryResponse ProcessQueryJob(const QueryRequest& request);
    // Execute local search + peer forwarding for a Forward job.
    QueryResponse ProcessForwardJob(const QueryRequest& request);
    // Dispatch a queued request to the right local processor.
    QueryResponse ProcessJob(JobType type, const QueryRequest& request);

    std::string node_id_;
    uint16_t port_;
    Dataset dataset_;
    std::vector<ConnectedPeer> connected_peers_;
    RequestJobQueue job_queue_;
};
