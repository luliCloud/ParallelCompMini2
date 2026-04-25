#include "Mini2ServiceImpl.h"

#include <future>
#include <iostream>
#include <stdexcept>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <sstream>

#include <grpcpp/grpcpp.h>

namespace {

class NoInsertRouteError : public std::runtime_error {
public:
    explicit NoInsertRouteError(const std::string& message)
        : std::runtime_error(message) {}
};
constexpr std::uint32_t kDefaultChunkSize = 1000; // Default number of records per chunk for streaming

bool MatchesQuery(const Record& record, const QueryRequest& request) {
    if (request.has_agency_id() &&
        record.agency_id != static_cast<uint16_t>(request.agency_id())) {
        return false;
    }

    if (request.has_borough_id() &&
        record.borough_id != static_cast<uint8_t>(request.borough_id())) {
        return false;
    }

    if (request.has_zip_code() && record.zip_code != request.zip_code()) {
        return false;
    }

    if (request.has_lat_min() && record.latitude < request.lat_min()) {
        return false;
    }

    if (request.has_lat_max() && record.latitude > request.lat_max()) {
        return false;
    }

    if (request.has_lon_min() && record.longitude < request.lon_min()) {
        return false;
    }

    if (request.has_lon_max() && record.longitude > request.lon_max()) {
        return false;
    }

    return true;
}

void AppendRecord(const Record& record, QueryResponse* response) {
    auto* out = response->add_records();
    out->set_id(record.id);
    out->set_created_date(record.created_date);
    out->set_closed_date(record.closed_date);
    out->set_agency_id(record.agency_id);
    out->set_problem_id(record.problem_id);
    out->set_status_id(record.status_id);
    out->set_borough_id(record.borough_id);
    out->set_zip_code(record.zip_code);
    out->set_latitude(record.latitude);
    out->set_longitude(record.longitude);
}

Record BuildLocalRecord(const mini2::Record& source_record) {
    Record record{};
    record.id = source_record.id();
    record.created_date = source_record.created_date();
    record.closed_date = source_record.closed_date();
    record.agency_id = static_cast<uint16_t>(source_record.agency_id());
    record.problem_id = source_record.problem_id();
    record.status_id = static_cast<uint8_t>(source_record.status_id());
    record.borough_id = static_cast<uint8_t>(source_record.borough_id());
    record.zip_code = source_record.zip_code();
    record.latitude = source_record.latitude();
    record.longitude = source_record.longitude();
    return record;
}

struct LocalSearchResult {
    std::vector<Record> records;
    std::size_t matched_record_count = 0;
};

LocalSearchResult CollectLocalMatches(const Dataset& dataset,
                                      const QueryRequest& request) {
    LocalSearchResult result;

    for (const auto& record : dataset.get_records()) {
        if (!MatchesQuery(record, request)) {
            continue;
        }
        result.records.push_back(record);
        result.matched_record_count++;
    }

    return result;
}
}  // namespace

Mini2ServiceImpl::Mini2ServiceImpl(
    const std::string& node_id,
    uint16_t port,
    bool enable_cache)
    : node_id_(node_id),
      port_(port),
      job_queue_(node_id, [this](JobType type, const QueryRequest& request) {
          return ProcessJob(type, request);
      }, [this](const InsertRequest& request) {
          return ProcessInsertJob(request);
      }),
      forward_cache_(node_id, enable_cache) {}

Mini2ServiceImpl::~Mini2ServiceImpl() = default;

bool Mini2ServiceImpl::Initialize(
    const std::string& dataset_path,
    const std::string& agency_dict_path,
    const std::string& borough_dict_path,
    const std::string& status_dict_path) {
    try {
        if (!dataset_.load_csv(
                dataset_path,
                agency_dict_path,
                borough_dict_path,
                status_dict_path)) {
            std::cerr << "Failed to load dataset from " << dataset_path 
                      << " at node: " << node_id_ << std::endl;
            return false;
        }
        std::cout << "[" << node_id_ << "] Loaded " << dataset_.size()
                  << " records" << std::endl;

        if (!dataset_soa_.load_csv(
                dataset_path,
                agency_dict_path,
                borough_dict_path,
                status_dict_path)) {
            std::cerr << "Failed to load dataset for SOA from " << dataset_path 
                      << " at node: " << node_id_ << std::endl;
            return false;
        }
        std::cout << "[" << node_id_ << "] Loaded dataset for SOA with " << dataset_soa_.size()
                  << " records" << std::endl;
        /** TODO: Need to remove AOS load later to avoid memory overflow */          
        return true;
    } catch (const std::exception& ex) {
        std::cerr << "Exception while loading dataset at node: " << node_id_
                  << ". Error: " << ex.what() << std::endl;
        return false;
    }
}

void Mini2ServiceImpl::SetPeers(const std::vector<PeerInfo>& peers) {
    connected_peers_.clear();
    for (const auto& peer_info : peers) {
        auto channel = grpc::CreateChannel(peer_info.address, grpc::InsecureChannelCredentials());

        ConnectedPeer connected_peer;
        connected_peer.id = peer_info.id;
        connected_peer.stub = mini2::NodeService::NewStub(channel);

        connected_peers_.push_back(std::move(connected_peer));
    }
}

void Mini2ServiceImpl::SetInsertRoutes(
    const std::vector<InsertRoute>& routes,
    const std::string& default_node_id) {
    insert_routes_ = routes;
    default_insert_node_id_ = default_node_id;
}

Status Mini2ServiceImpl::Ping(ServerContext* context, const PingRequest* request,
                             PingResponse* response) {
    (void)context;
    std::cout << "[" << node_id_ << "] Received Ping request" << std::endl;
    response->set_request_id(request->request_id());
    response->add_active_nodes(node_id_);

    if (!connected_peers_.empty())
    {
        std::vector<std::future<PingResponse>> peer_response_futures;

        for (const auto& peer : connected_peers_)
        {
            auto* stub = peer.stub.get();
            std::string peer_id = peer.id;

            peer_response_futures.push_back(std::async(std::launch::async, [request, stub, peer_id]()
            {
                PingResponse peer_response;
                grpc::ClientContext peer_context;
                peer_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));

                grpc::Status status = stub->Ping(&peer_context, *request, &peer_response);
                if (!status.ok())
                {
                    std::cout << "Failed to ping peer " << peer_id << ": " << status.error_code() << " " << status.error_message() << std::endl;
                    return PingResponse();
                }
                return peer_response;
            }));
        }

        for (auto& peer_response_future : peer_response_futures)
        {
            PingResponse peer_response = peer_response_future.get();
            for (const auto& node_name : peer_response.active_nodes())
            {
                response->add_active_nodes(node_name);
            }
        }
    }
    return Status::OK;
}

Status Mini2ServiceImpl::Query(ServerContext* context, const QueryRequest* request,
                              QueryResponse* response) {
    (void)context;
    std::cout << "[" << node_id_ << "] Received Query request: "
              << request->request_id() << std::endl;

    try {
        QueryResponse result = job_queue_.EnqueueAndWait(JobType::Query, *request);
        *response = std::move(result);
        return Status::OK;
    } catch (const std::exception& ex) {
        std::cerr << "[" << node_id_ << "] Query job failed: " << ex.what() << std::endl;
        return Status(grpc::StatusCode::INTERNAL, ex.what());
    }
}

Status Mini2ServiceImpl::Forward(ServerContext* context, const QueryRequest* request,
                                QueryResponse* response) {
    (void)context;
    std::cout << "[" << node_id_ << "] Received Forward request: "
              << request->request_id() << std::endl;

    try {
        QueryResponse result = job_queue_.EnqueueAndWait(JobType::Forward, *request);
        *response = std::move(result);
        return Status::OK;
    } catch (const std::exception& ex) {
        std::cerr << "[" << node_id_ << "] Forward job failed: " << ex.what() << std::endl;
        return Status(grpc::StatusCode::INTERNAL, ex.what());
    }
}

Status Mini2ServiceImpl::Insert(ServerContext* context, const InsertRequest* request,
                                InsertResponse* response) {
    (void)context;
    std::cout << "[" << node_id_ << "] Received Insert request: "
              << request->request_id() << std::endl;

    try {
        InsertResponse result = job_queue_.EnqueueAndWait(*request);
        *response = std::move(result);
        return Status::OK;
    } catch (const NoInsertRouteError& ex) {
        std::cerr << "[" << node_id_ << "] Insert job failed: "
                  << ex.what() << std::endl;
        return Status(grpc::StatusCode::FAILED_PRECONDITION, ex.what());
    } catch (const std::exception& ex) {
        std::cerr << "[" << node_id_ << "] Insert job failed: "
                  << ex.what() << std::endl;
        return Status(grpc::StatusCode::INTERNAL, ex.what());
    }
}

InsertResponse Mini2ServiceImpl::ProcessInsertJob(const InsertRequest& request) {
    InsertRequest forwarded_request = request;
    if (forwarded_request.target_node_id().empty()) {
        forwarded_request.set_target_node_id(
            ChooseLeafNodeForInsert(forwarded_request.record().created_date()));
    }

    if (node_id_ == forwarded_request.target_node_id()) {
        InsertResponse response = StoreRecordLocally(forwarded_request);
        forward_cache_.Clear();
        return response;
    }

    if (connected_peers_.empty()) {
        throw NoInsertRouteError(
            "No route found for target node " + forwarded_request.target_node_id());
    }

    const std::string target_node_id = forwarded_request.target_node_id();

    auto forward_to_peer = [&](const ConnectedPeer& peer, InsertResponse* peer_response) {
        std::cout << "[" << node_id_ << "] Try Insert peer "
                  << peer.id << " for target " << target_node_id
                  << " request " << forwarded_request.request_id() << std::endl;

        grpc::ClientContext peer_context;
        peer_context.set_deadline(
            std::chrono::system_clock::now() + std::chrono::seconds(5));

        return peer.stub->Insert(&peer_context, forwarded_request, peer_response);
    };

    for (const auto& peer : connected_peers_) {
        if (peer.id != target_node_id) {
            continue;
        }

        InsertResponse peer_response;
        grpc::Status status = forward_to_peer(peer, &peer_response);
        if (status.ok()) {
            peer_response.set_request_id(request.request_id());
            peer_response.set_from_node(node_id_);
            forward_cache_.Clear();
            return peer_response;
        }

        throw std::runtime_error(
            "Insert failed at peer " + peer.id + ": " + status.error_message());
    }

    for (const auto& peer : connected_peers_) {
        InsertResponse peer_response;
        grpc::Status status = forward_to_peer(peer, &peer_response);
        if (status.ok()) {
            peer_response.set_request_id(request.request_id());
            peer_response.set_from_node(node_id_);
            forward_cache_.Clear();
            return peer_response;
        }
    }

    throw NoInsertRouteError(
        "No route found for target node " + target_node_id);
}

QueryResponse Mini2ServiceImpl::ProcessJob(JobType type, const QueryRequest& request) {
    switch (type) {
        case JobType::Query:
            return ProcessQueryJob(request);
        case JobType::Forward:
            return ProcessForwardJob(request);
        case JobType::Insert:
            break;
    }

    throw std::runtime_error("Unsupported queued job type");
}

QueryResponse Mini2ServiceImpl::ProcessQueryJob(const QueryRequest& request) {
    QueryResponse response;
    response.set_request_id(request.request_id());
    response.set_from_node(node_id_);

    std::size_t matched_record_count = 0;
    std::lock_guard<std::mutex> lock(dataset_mutex_);
    for (const auto& record : dataset_.get_records()) {
        if (!MatchesQuery(record, request)) {
            continue;
        }
        AppendRecord(record, &response);
        matched_record_count++;
    }

    std::cout << "[" << node_id_ << "] Query return " << matched_record_count
              << " records" << std::endl;
    return response;
}

QueryResponse Mini2ServiceImpl::ProcessForwardJob(const QueryRequest& request) {
    QueryResponse response;
    response.set_request_id(request.request_id());
    response.set_from_node(node_id_);

    const std::string request_id = request.request_id();

    if (forward_cache_.TryGet(request, &response)) {
        return response;
    }

    std::cout << "[" << node_id_ << "] Forward local search started: "
              << request_id << std::endl;
    auto local_future = std::async(std::launch::async, [this, &request]() {
        std::lock_guard<std::mutex> lock(dataset_mutex_);
        return CollectLocalMatches(dataset_, request);
    });

    std::vector<std::future<QueryResponse>> peer_response_futures;
    if (!connected_peers_.empty()) {
        std::cout << "[" << node_id_ << "] Forward peer fanout started: "
                  << request_id << " (peer_count=" << connected_peers_.size() << ")" << std::endl;

        for (const auto& peer : connected_peers_) {
            auto* stub = peer.stub.get();
            std::string peer_id = peer.id;

            peer_response_futures.push_back(std::async(std::launch::async, [&request, stub, peer_id]() {
                QueryResponse peer_response;
                grpc::ClientContext peer_context;
                peer_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

                grpc::Status status = stub->Forward(&peer_context, request, &peer_response);
                if (!status.ok())
                {
                    std::cout << "Failed to forward request to peer " << peer_id << ": " << status.error_code() << " " << status.error_message() << std::endl;
                    return QueryResponse();
                }
                return peer_response;
            }));
        }
    }

    LocalSearchResult local_result = local_future.get();
    for (const auto& record : local_result.records) {
        AppendRecord(record, &response);
    }
    std::cout << "[" << node_id_ << "] Forward local search completed: "
              << request_id << " (local_matched=" << local_result.matched_record_count << ")" << std::endl;

    std::size_t total_peer_record_count = 0;
    for (auto& peer_response_future : peer_response_futures) {
        QueryResponse peer_response = peer_response_future.get();
        total_peer_record_count += static_cast<std::size_t>(peer_response.records_size());
        for (const auto& peer_record : peer_response.records()) {
            response.add_records()->CopyFrom(peer_record);
        }
    }

    if (!connected_peers_.empty()) {
        std::cout << "[" << node_id_ << "] Forward peer fanout completed: "
                  << request_id << " (peer_count=" << connected_peers_.size()
                  << ", peer_records=" << total_peer_record_count << ")" << std::endl;
    }

    forward_cache_.Store(request, response);

    std::cout << "[" << node_id_ << "] Forward returning " << response.records_size()
              << " total records (Local matched: " << local_result.matched_record_count << ")" << std::endl;

    return response;
}

/** Distributed fan-out SOA count query. Every node handles its local data and forwards requests to peers, regarding coordinator and workers */
Status Mini2ServiceImpl::CountQuery(ServerContext* context, const SOACountRequest* request,
                                    SOACountResponse* response) {
    (void)context;
    std::cout << "[" << node_id_ << "] Received CountQuery request: "
              << request->request_id() << std::endl;
    response->set_request_id(request->request_id());
    response->set_from_node(node_id_); // node itself, tracing purpose. reply sent back to the original requester 
    uint64_t local_count = 0;
    try {
        std::lock_guard<std::mutex> lock(dataset_mutex_);
        QuerySOA query(dataset_soa_);
        switch (request->kind())
        {
        case mini2::SOA_COUNT_CREATED_DATE_RANGE:
            local_count = query.count_in_created_date_range(
                request->created_date_start(), 
            request->created_date_end());
        break;
        case mini2::SOA_COUNT_BY_AGENCY_AND_CREATED_DATE_RANGE:
            local_count = query.count_by_agency_and_created_date_range(
                static_cast<uint16_t>(request->agency_id()), 
                request->created_date_start(), 
                request->created_date_end());
            break;
        case mini2::SOA_COUNT_BY_STATUS_AND_CREATED_DATE_RANGE:
            local_count = query.count_by_status_and_created_date_range(
                static_cast<uint8_t>(request->status_id()), 
                request->created_date_start(), 
                request->created_date_end());
            break;
        default:
            return Status(grpc::StatusCode::INVALID_ARGUMENT, 
                        "Unsupported count query kind");
        }
    } catch (const std::invalid_argument& ex) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, ex.what());
    } catch (const std::exception& ex) {
        return Status(grpc::StatusCode::INTERNAL, ex.what());
    }

    uint64_t total_count = local_count;
    if (!connected_peers_.empty()) {
        std::vector<std::future<uint64_t>> futures;

        for (const auto& peer : connected_peers_) {
            auto* stub = peer.stub.get();
            std::string peer_id = peer.id;

            futures.push_back(std::async(std::launch::async, [request, stub, peer_id]() {
                SOACountResponse peer_res;
                grpc::ClientContext peer_ctx;
                peer_ctx.set_deadline(
                    std::chrono::system_clock::now() + std::chrono::seconds(5));

                grpc::Status status = stub->CountQuery(&peer_ctx, *request, &peer_res);
                if (!status.ok()) {
                    std::cout << "Failed to forward count query to peer " << peer_id << ": " << status.error_code() << " " << status.error_message() << std::endl;
                    return static_cast<uint64_t>(0);
                }
                return peer_res.count(); // return the count result from peer response
                
            })); // end of async
        } // end of for loop (each peer)

        for (auto& f : futures) {
            total_count += f.get();
        }
    }

    response->set_count(total_count);
    std::cout << "[" << node_id_ << "] CountQuery returning total count: " << total_count << std::endl;
    return Status::OK;
}

std::string Mini2ServiceImpl::ChooseLeafNodeForInsert(int64_t created_date) const {
    for (const auto& route : insert_routes_) {
        if (created_date <= route.max_created_date) {
            return route.node_id;
        }
    }

    return default_insert_node_id_;
}

InsertResponse Mini2ServiceImpl::StoreRecordLocally(const InsertRequest& request) {
    const Record local_record = BuildLocalRecord(request.record());

    {
        std::lock_guard<std::mutex> lock(dataset_mutex_);
        dataset_.append_record(local_record);
        dataset_soa_.append_record(
            local_record.id,
            local_record.created_date,
            local_record.closed_date,
            local_record.agency_id,
            local_record.problem_id,
            local_record.status_id,
            local_record.borough_id,
            local_record.zip_code,
            local_record.latitude,
            local_record.longitude);
    }

    InsertResponse response;
    response.set_request_id(request.request_id());
    response.set_from_node(node_id_);
    response.set_stored_at_node(node_id_);
    response.set_inserted(true);

    std::cout << "[" << node_id_ << "] Insert stored record id "
              << local_record.id << " request "
              << request.request_id() << std::endl;
    return response;
Status Mini2ServiceImpl::StartForwardChunks(
    ServerContext* context, const QueryRequest* request,
    ChunkSessionResponse* response) {
    (void)context;

    const std::uint32_t chunk_size = 
        request->has_chunk_size() && request->chunk_size() > 0
            ? request->chunk_size()
            : kDefaultChunkSize;  
    
    // fan-out. 
    /** TODO: 1st version will get complete result internally, then return to client chunk by chunk
     * 2nd version will stream chunks to client as soon as local search is done and peer responses start coming in, without waiting for the complete result. This will require more complex session and state management
     */
    QueryResponse full_result = job_queue_.EnqueueAndWait(JobType::Forward, *request);

    const std::uint64_t total_records = full_result.records_size();
    const std::uint32_t total_chunks = total_records == 0 ? 0 : static_cast<std::uint32_t>((total_records + chunk_size - 1) / chunk_size);

    std::string session_id = CreateChunkSessionId(request->request_id());

    ChunkSession session;
    session.request_id = request->request_id();
    session.from_node = node_id_;
    session.total_chunks = total_chunks;
    session.chunk_size = chunk_size;

    for (const auto& record : full_result.records()) {
        session.records.push_back(record);
    }

    {
        std::lock_guard<std::mutex> lock(chunk_sessions_mutex_);
        chunk_sessions_[session_id] = std::move(session);
    }

    response->set_session_id(session_id);
    response->set_request_id(request->request_id());
    response->set_from_node(node_id_);
    response->set_total_chunks(total_chunks);
    response->set_chunk_size(chunk_size);
    response->set_total_records(total_records);

    return Status::OK;
}

Status Mini2ServiceImpl::GetForwardChunk(
    ServerContext* context, const ChunkRequest* request,
    QueryChunkResponse* response) {
    (void)context;

    std::lock_guard<std::mutex> lock(chunk_sessions_mutex_);
    auto it = chunk_sessions_.find(request->session_id());
    if (it == chunk_sessions_.end()) {
        return Status(grpc::StatusCode::NOT_FOUND, "Chunk session not found");
    }

    const ChunkSession& session = it->second;
    const std::uint64_t start = static_cast<std::uint64_t>(request->chunk_index()) * session.chunk_size;
    if (start >= session.records.size()) {
        return Status(grpc::StatusCode::OUT_OF_RANGE, "Chunk index out of range");
    }   
    const std::uint64_t end = std::min(start + session.chunk_size, static_cast<std::uint64_t>(session.records.size()));

    response->set_request_id(session.request_id);
    response->set_session_id(request->session_id());
    response->set_from_node(session.from_node);
    response->set_chunk_index(request->chunk_index());
    response->set_chunk_size(session.chunk_size);
    response->set_total_chunks(session.total_chunks);
    response->set_total_records(session.records.size());

    for (std::uint64_t i = start; i < end; ++i) {
        response->add_records()->CopyFrom(session.records[static_cast<std::size_t>(i)]); 
    }

    response->set_done(request->chunk_index() + 1 >= session.total_chunks); // determine if this is the last chunk using >=
    return Status::OK;
}

// clean up session info and cached data for the session if client received all data or cancel request
Status Mini2ServiceImpl::CancelChunks(
    ServerContext* context, const ChunkCancelRequest* request,
    ChunkCancelResponse* response) {
    (void)context;

    std::lock_guard<std::mutex> lock(chunk_sessions_mutex_);
    bool cancelled = chunk_sessions_.erase(request->session_id()) > 0;

    response->set_session_id(request->session_id());
    response->set_cancelled(cancelled);
    return Status::OK;
}

/** Session id helper functions 
 * e.g.: A-client-query-123-1776900000000000
 */
std::string Mini2ServiceImpl::CreateChunkSessionId(const std::string& request_id) const {
    auto now = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    std::stringstream out;
    out << node_id_ << "_" << request_id << "_" << now;
    return out.str();
}
