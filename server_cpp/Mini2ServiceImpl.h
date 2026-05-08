#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <unordered_map>

#include <grpcpp/server_context.h>

#include "ForwardResponseCache.h"
#include "InsertRouteConfig.h"
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
using mini2::InsertRequest;
using mini2::InsertResponse;
using mini2::DeleteRequest;
using mini2::DeleteResponse;

// for SOA
using mini2::SOACountKind;
using mini2::SOACountRequest;
using mini2::SOACountResponse;
using mini2::SOAGroupByRequest;
using mini2::SOAGroupByResponse;
using mini2::SOATopKRequest;
using mini2::SOATopKResponse;

// for chunk streaming RPCs
using mini2::ChunkCancelRequest;
using mini2::ChunkCancelResponse;
using mini2::ChunkRequest;
using mini2::ChunkSessionResponse;
using mini2::QueryChunkResponse;

struct PeerInfo
{
    std::string id;
    std::string address;
};

struct ConnectedPeer {
    std::string id;
    std::unique_ptr<mini2::NodeService::Stub> stub;
};

enum class DatasetLoadMode {
    AOS,
    SOA,
    Both
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
        DatasetLoadMode dataset_load_mode,
        const std::string& agency_dict_path = "",
        const std::string& problem_dict_path = "",
        const std::string& borough_dict_path = "",
        const std::string& status_dict_path = "");
    // Set peer and set up connection
    void SetPeers(const std::vector<PeerInfo>& peers);
    void SetInsertRoutes(
        const std::vector<InsertRoute>& routes,
        const std::string& default_node_id);

    // Service methods (gRPC overrides)
    Status Ping(ServerContext* context, const PingRequest* request,
                PingResponse* response) override;

    Status Query(ServerContext* context, const QueryRequest* request,
                 QueryResponse* response) override;

    Status Forward(ServerContext* context, const QueryRequest* request,
                   QueryResponse* response) override;

    Status Insert(ServerContext* context, const InsertRequest* request,
                  InsertResponse* response) override;

    Status Delete(ServerContext* context, const DeleteRequest* request,
                  DeleteResponse* response) override;

    Status CountQuery(ServerContext* context, const SOACountRequest* request, 
                      SOACountResponse* response) override;

    Status GroupByQuery(ServerContext* context, const SOAGroupByRequest* request, 
                        SOAGroupByResponse* response) override;
                
    Status TopKQuery(ServerContext* context, const SOATopKRequest* request, 
                     SOATopKResponse* response) override;

    /** Streaming calls */
    Status StartForwardChunks(ServerContext* context, const QueryRequest* request,
                             ChunkSessionResponse* response) override;

    Status GetForwardChunk(ServerContext* context, const ChunkRequest* request,
                            QueryChunkResponse* response) override;

    Status CancelChunks(ServerContext* context, const ChunkCancelRequest* request,
                         ChunkCancelResponse* response) override;


private:
    // Execute local search for a Query job.
    QueryResponse ProcessQueryJob(const QueryRequest& request);
    // Execute local search + peer forwarding for a Forward job.
    QueryResponse ProcessForwardJob(const QueryRequest& request);
    // Dispatch a queued request to the right local processor.
    QueryResponse ProcessJob(JobType type, const QueryRequest& request);
    // Execute insert routing or local insert for an Insert job.
    InsertResponse ProcessInsertJob(const InsertRequest& request);
    // Execute broadcast delete and aggregate per-node delete counts.
    DeleteResponse ProcessDeleteJob(const DeleteRequest& request);
    std::string ChooseLeafNodeForInsert(int64_t created_date) const;
    InsertResponse StoreRecordLocally(const InsertRequest& request);
    std::uint64_t DeleteMatchingRecordsLocally(const DeleteRequest& request);

    // Streaming helper methods
    struct ChunkSession;
    std::string CreateChunkSessionId(const std::string& request_id) const;
    bool StreamLocalForwardChunks(
        ServerContext* context,
        const QueryRequest& request,
        std::uint32_t chunk_size,
        const std::function<bool(QueryChunkResponse*)>& emit_chunk,
        std::uint64_t* local_matched_record_count);
    bool StreamBufferedLocalForwardChunks(
        ServerContext* context,
        const QueryRequest& request,
        std::uint32_t chunk_size,
        const std::function<bool(QueryChunkResponse*)>& emit_chunk,
        std::uint64_t* local_matched_record_count);
    void ProduceForwardChunksForSession(
        std::shared_ptr<ChunkSession> session,
        QueryRequest request);
    bool PushChunkToSession(
        const std::shared_ptr<ChunkSession>& session,
        QueryChunkResponse chunk);
    void CloseChunkSession(
        const std::shared_ptr<ChunkSession>& session,
        const std::string& error_message = "");

    struct ChunkSession {
        std::string session_id;
        std::string request_id;
        std::string from_node;
        std::uint32_t total_chunks = 0;
        std::uint32_t chunk_size = 0;
        std::uint64_t total_records = 0;
        std::vector<QueryChunkResponse> chunks;
        std::mutex mutex;
        std::condition_variable ready;
        bool closed = false;
        bool cancelled = false;
        std::string error_message;
    };

    std::mutex chunk_sessions_mutex_;
    std::unordered_map<std::string, std::shared_ptr<ChunkSession>> chunk_sessions_; // session_id -> session info

    std::string node_id_;
    uint16_t port_;
    DatasetLoadMode dataset_load_mode_ = DatasetLoadMode::Both;
    Dataset dataset_;
    std::vector<ConnectedPeer> connected_peers_;
    std::vector<InsertRoute> insert_routes_;
    std::string default_insert_node_id_;
    DatasetSOA dataset_soa_; // for SOA queries
    std::mutex dataset_mutex_;
    RequestJobQueue job_queue_;
    // Cache is config-driven through enable_cache in each node YAML.
    ForwardResponseCache forward_cache_;
};
