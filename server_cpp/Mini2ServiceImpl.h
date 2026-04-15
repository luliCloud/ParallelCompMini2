#pragma once

#include <string>
#include <vector>

#include <grpcpp/server_context.h>

#include "ForwardResponseCache.h"
#include "mini2.grpc.pb.h"
#include "RequestJobQueue.h"
#include "dataset.hpp"
#include "dataset_SOA.hpp"
#include "query_SOA.hpp"

using grpc::ServerContext;
using grpc::Status;
using mini2::NodeService;
using mini2::QueryRequest;
using mini2::QueryResponse;
using mini2::PingRequest;
using mini2::PingResponse;

// for SOA
using mini2::SOACountKind;
using mini2::SOACountRequest;
using mini2::SOACountResponse;
using mini2::SOAGroupByRequest;
using mini2::SOAGroupByResponse;
using mini2::SOATopKRequest;
using mini2::SOATopKResponse;


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
    Mini2ServiceImpl(
        const std::string& node_id,
        uint16_t port,
        bool enable_cache);
    ~Mini2ServiceImpl() override;

    // Initialization
    bool Initialize(
        const std::string& dataset_path,
        const std::string& agency_dict_path = "",
        const std::string& borough_dict_path = "",
        const std::string& status_dict_path = "");
    // Set peer and set up connection
    void SetPeers(const std::vector<PeerInfo>& peers);

    // Service methods (gRPC overrides)
    Status Ping(ServerContext* context, const PingRequest* request,
                PingResponse* response) override;

    Status Query(ServerContext* context, const QueryRequest* request,
                 QueryResponse* response) override;

    Status Forward(ServerContext* context, const QueryRequest* request,
                   QueryResponse* response) override;

    Status CountQuery(ServerContext* context, const SOACountRequest* request, 
                      SOACountResponse* response) override;

    // Status GroupByQuery(ServerContext* context, const SOAGroupByRequest* request, 
    //                     SOAGroupByResponse* response) override;
                
    // Status TopKQuery(ServerContext* context, const SOATopKRequest* request, 
    //                  SOATopKResponse* response) override;

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
    DatasetSOA dataset_soa_; // for SOA queries
    RequestJobQueue job_queue_;
    // Cache is config-driven through enable_cache in each node YAML.
    ForwardResponseCache forward_cache_;
};
