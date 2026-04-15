#pragma once

#include <string>
#include <vector>
#include <grpcpp/server_context.h>
#include "mini2.grpc.pb.h"
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
    explicit Mini2ServiceImpl(const std::string& node_id, uint16_t port);
    ~Mini2ServiceImpl() = default;

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
    std::string node_id_;
    uint16_t port_;
    Dataset dataset_;
    std::vector<ConnectedPeer> connected_peers_;
    DatasetSOA dataset_soa_; // for SOA queries
};
