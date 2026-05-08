#include "Mini2ServiceImpl.h"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <iostream>
#include <queue>
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
constexpr auto kForwardPeerTimeout = std::chrono::seconds(120);
constexpr int kMaxGrpcMessageBytes = 64 * 1024 * 1024;

class ForwardStreamChunkQueue {
public:
    void Push(QueryChunkResponse chunk) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            chunks_.push_back(std::move(chunk));
        }
        not_empty_.notify_one();
    }

    bool Pop(QueryChunkResponse* chunk) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [this] {
            return closed_ || !chunks_.empty();
        });

        if (chunks_.empty()) {
            return false;
        }

        *chunk = std::move(chunks_.front());
        chunks_.pop_front();
        return true;
    }

    void Close() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        not_empty_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable not_empty_;
    std::deque<QueryChunkResponse> chunks_;
    bool closed_ = false;
};

bool LoadsAOS(DatasetLoadMode mode) {
    return mode == DatasetLoadMode::AOS || mode == DatasetLoadMode::Both;
}

bool LoadsSOA(DatasetLoadMode mode) {
    return mode == DatasetLoadMode::SOA || mode == DatasetLoadMode::Both;
}

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

void AppendRecord(const Record& record, QueryChunkResponse* response) {
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

bool HasDeleteFilters(const DeleteRequest& request) {
    return request.has_record_id() ||
           request.has_created_date() ||
           request.has_closed_date() ||
           request.has_agency_id() ||
           request.has_problem_id() ||
           request.has_status_id() ||
           request.has_borough_id() ||
           request.has_zip_code() ||
           request.has_latitude() ||
           request.has_longitude();
}

bool MatchesDeleteFilter(const Record& record, const DeleteRequest& request) {
    if (request.delete_all()) {
        return true;
    }

    if (request.has_record_id() && record.id != request.record_id()) {
        return false;
    }
    if (request.has_created_date() &&
        record.created_date != request.created_date()) {
        return false;
    }
    if (request.has_closed_date() &&
        record.closed_date != request.closed_date()) {
        return false;
    }
    if (request.has_agency_id() &&
        static_cast<std::uint32_t>(record.agency_id) != request.agency_id()) {
        return false;
    }
    if (request.has_problem_id() &&
        record.problem_id != request.problem_id()) {
        return false;
    }
    if (request.has_status_id() &&
        static_cast<std::uint32_t>(record.status_id) != request.status_id()) {
        return false;
    }
    if (request.has_borough_id() &&
        static_cast<std::uint32_t>(record.borough_id) != request.borough_id()) {
        return false;
    }
    if (request.has_zip_code() && record.zip_code != request.zip_code()) {
        return false;
    }
    if (request.has_latitude() && record.latitude != request.latitude()) {
        return false;
    }
    if (request.has_longitude() && record.longitude != request.longitude()) {
        return false;
    }

    return true;
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
      }, [this](const DeleteRequest& request) {
          return ProcessDeleteJob(request);
      }),
      forward_cache_(node_id, enable_cache) {}

Mini2ServiceImpl::~Mini2ServiceImpl() = default;

bool Mini2ServiceImpl::Initialize(
    const std::string& dataset_path,
    DatasetLoadMode dataset_load_mode,
    const std::string& agency_dict_path,
    const std::string& problem_dict_path,
    const std::string& borough_dict_path,
    const std::string& status_dict_path) {
    try {
        dataset_load_mode_ = dataset_load_mode;
        const auto cold_start_begin = std::chrono::steady_clock::now();

        std::cout << "[" << node_id_ << "] Dataset load mode = ";
        if (dataset_load_mode_ == DatasetLoadMode::AOS) {
            std::cout << "aos";
        } else if (dataset_load_mode_ == DatasetLoadMode::SOA) {
            std::cout << "soa";
        } else {
            std::cout << "both";
        }
        std::cout << std::endl;

        if (LoadsAOS(dataset_load_mode_)) {
            const auto aos_begin = std::chrono::steady_clock::now();
            if (!dataset_.load_csv(
                    dataset_path,
                    agency_dict_path,
                    problem_dict_path,
                    borough_dict_path,
                    status_dict_path)) {
                std::cerr << "Failed to load AOS dataset from " << dataset_path 
                          << " at node: " << node_id_ << std::endl;
                return false;
            }
            const double aos_ms = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - aos_begin).count();
            std::cout << "[" << node_id_ << "] Loaded AOS dataset with "
                      << dataset_.size() << " records in "
                      << aos_ms << " ms" << std::endl;
        }

        if (LoadsSOA(dataset_load_mode_)) {
            const auto soa_begin = std::chrono::steady_clock::now();
            if (!dataset_soa_.load_csv(
                    dataset_path,
                    agency_dict_path,
                    problem_dict_path,
                    borough_dict_path,
                    status_dict_path)) {
                std::cerr << "Failed to load SOA dataset from " << dataset_path 
                          << " at node: " << node_id_ << std::endl;
                return false;
            }
            const double soa_ms = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - soa_begin).count();
            std::cout << "[" << node_id_ << "] Loaded SOA dataset with "
                      << dataset_soa_.size() << " records in "
                      << soa_ms << " ms" << std::endl;
        }

        const double cold_start_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - cold_start_begin).count();
        std::cout << "[" << node_id_ << "] Cold start dataset load completed in "
                  << cold_start_ms << " ms" << std::endl;
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
        grpc::ChannelArguments channel_args;
        channel_args.SetMaxReceiveMessageSize(kMaxGrpcMessageBytes);
        channel_args.SetMaxSendMessageSize(kMaxGrpcMessageBytes);
        auto channel = grpc::CreateCustomChannel(
            peer_info.address,
            grpc::InsecureChannelCredentials(),
            channel_args);

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
        if (!LoadsAOS(dataset_load_mode_)) {
            return Status(
                grpc::StatusCode::FAILED_PRECONDITION,
                "Query requires dataset_mode aos or both");
        }
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
        if (!LoadsAOS(dataset_load_mode_)) {
            return Status(
                grpc::StatusCode::FAILED_PRECONDITION,
                "Forward requires dataset_mode aos or both");
        }
        QueryResponse result = job_queue_.EnqueueAndWait(JobType::Forward, *request);
        *response = std::move(result);
        return Status::OK;
    } catch (const std::exception& ex) {
        std::cerr << "[" << node_id_ << "] Forward job failed: " << ex.what() << std::endl;
        return Status(grpc::StatusCode::INTERNAL, ex.what());
    }
}

Status Mini2ServiceImpl::ForwardStream(
    ServerContext* context,
    const QueryRequest* request,
    ServerWriter<QueryChunkResponse>* writer) {
    std::cout << "[" << node_id_ << "] Received ForwardStream request: "
              << request->request_id() << std::endl;

    if (!LoadsAOS(dataset_load_mode_)) {
        return Status(
            grpc::StatusCode::FAILED_PRECONDITION,
            "ForwardStream requires dataset_mode aos or both");
    }

    const std::uint32_t chunk_size =
        request->has_chunk_size() && request->chunk_size() > 0
            ? request->chunk_size()
            : kDefaultChunkSize;
    const std::string request_id = request->request_id();
    const auto stream_begin = std::chrono::steady_clock::now();

    ForwardStreamChunkQueue outbound_chunks;
    std::mutex chunk_index_mutex;
    std::uint32_t next_chunk_index = 0;
    std::uint64_t local_matched_record_count = 0;
    std::vector<std::future<void>> producer_futures;
    std::atomic<int> producers_remaining(
        static_cast<int>(connected_peers_.size()) + 1);

    auto finish_producer = [&outbound_chunks, &producers_remaining]() {
        if (producers_remaining.fetch_sub(1) == 1) {
            outbound_chunks.Close();
        }
    };

    auto emit_chunk = [&](QueryChunkResponse* chunk) {
        if (context->IsCancelled()) {
            return false;
        }
        {
            std::lock_guard<std::mutex> lock(chunk_index_mutex);
            chunk->set_chunk_index(next_chunk_index++);
        }
        chunk->set_done(false);
        outbound_chunks.Push(std::move(*chunk));
        return true;
    };

    if (!connected_peers_.empty()) {
        std::cout << "[" << node_id_ << "] ForwardStream peer fanout started: "
                  << request_id << " (peer_count=" << connected_peers_.size()
                  << ")" << std::endl;

        for (const auto& peer : connected_peers_) {
            auto* stub = peer.stub.get();
            std::string peer_id = peer.id;
            producer_futures.push_back(std::async(
                std::launch::async,
                [context,
                 request,
                 stub,
                 peer_id,
                 &emit_chunk,
                 &finish_producer,
                 node_id = node_id_]() {
                    const auto peer_begin = std::chrono::steady_clock::now();
                    grpc::ClientContext peer_context;
                    peer_context.set_deadline(context->deadline());

                    std::unique_ptr<grpc::ClientReader<QueryChunkResponse>> reader(
                        stub->ForwardStream(&peer_context, *request));

                    QueryChunkResponse peer_chunk;
                    std::uint64_t peer_chunks = 0;
                    std::uint64_t peer_records = 0;
                    while (!context->IsCancelled() && reader->Read(&peer_chunk)) {
                        if (peer_chunk.records_size() == 0 && peer_chunk.done()) {
                            continue;
                        }

                        ++peer_chunks;
                        peer_records += static_cast<std::uint64_t>(
                            peer_chunk.records_size());
                        if (!emit_chunk(&peer_chunk)) {
                            peer_context.TryCancel();
                            break;
                        }
                    }

                    grpc::Status status = reader->Finish();
                    if (!status.ok()) {
                        std::cout << "[" << node_id
                                  << "] ForwardStream peer " << peer_id
                                  << " failed: " << status.error_code() << " "
                                  << status.error_message() << std::endl;
                    }
                    const double peer_ms = std::chrono::duration<double, std::milli>(
                        std::chrono::steady_clock::now() - peer_begin).count();
                    std::cout << "[" << node_id
                              << "] ForwardStream peer reader completed: "
                              << request->request_id()
                              << " peer=" << peer_id
                              << " chunks=" << peer_chunks
                              << " records=" << peer_records
                              << " elapsed_ms=" << peer_ms << std::endl;
                    finish_producer();
                }));
        }
    }

    std::cout << "[" << node_id_ << "] ForwardStream local search started: "
              << request_id << std::endl;
    producer_futures.push_back(std::async(
        std::launch::async,
        [this,
         context,
         request,
         chunk_size,
         &emit_chunk,
         &finish_producer,
         &local_matched_record_count,
         request_id]() {
            const auto local_begin = std::chrono::steady_clock::now();
            const bool use_leaf_buffer =
                request->leaf_buffered_streaming() && connected_peers_.empty();
            const bool still_writing = use_leaf_buffer
                ? StreamBufferedLocalForwardChunks(
                    context,
                    *request,
                    chunk_size,
                    emit_chunk,
                    &local_matched_record_count)
                : StreamLocalForwardChunks(
                    context,
                    *request,
                    chunk_size,
                    emit_chunk,
                    &local_matched_record_count);
            if (!still_writing) {
                std::cout << "[" << node_id_
                          << "] ForwardStream stopped while streaming local chunks: "
                          << request_id << std::endl;
            }
            const double local_ms = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - local_begin).count();
            std::cout << "[" << node_id_
                      << "] ForwardStream local producer completed: "
                      << request_id
                      << " mode=" << (use_leaf_buffer ? "leaf-buffered" : "pure-stream")
                      << " matched=" << local_matched_record_count
                      << " elapsed_ms=" << local_ms << std::endl;
            finish_producer();
        }));

    QueryChunkResponse outbound_chunk;
    const auto writer_begin = std::chrono::steady_clock::now();
    std::uint64_t written_chunks = 0;
    std::uint64_t written_records = 0;
    while (!context->IsCancelled() && outbound_chunks.Pop(&outbound_chunk)) {
        if (!writer->Write(outbound_chunk)) {
            break;
        }
        ++written_chunks;
        written_records += static_cast<std::uint64_t>(
            outbound_chunk.records_size());
    }
    const double writer_ms = std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - writer_begin).count();

    for (auto& producer_future : producer_futures) {
        producer_future.get();
    }

    QueryChunkResponse done_chunk;
    done_chunk.set_request_id(request_id);
    done_chunk.set_from_node(node_id_);
    done_chunk.set_chunk_size(chunk_size);
    done_chunk.set_done(true);
    {
        std::lock_guard<std::mutex> lock(chunk_index_mutex);
        done_chunk.set_chunk_index(next_chunk_index);
        done_chunk.set_total_chunks(next_chunk_index);
    }
    if (!context->IsCancelled()) {
        writer->Write(done_chunk);
    }

    std::cout << "[" << node_id_ << "] ForwardStream completed: "
              << request_id << " (local_matched="
              << local_matched_record_count
              << ", written_chunks=" << written_chunks
              << ", written_records=" << written_records
              << ", writer_ms=" << writer_ms
              << ", total_ms="
              << std::chrono::duration<double, std::milli>(
                     std::chrono::steady_clock::now() - stream_begin).count()
              << ")" << std::endl;

    return Status::OK;
}

Status Mini2ServiceImpl::Insert(ServerContext* context, const InsertRequest* request,
                                InsertResponse* response) {
    (void)context;
    std::cout << "[" << node_id_ << "] Received Insert request: "
              << request->request_id() << std::endl;

    try {
        if (!LoadsAOS(dataset_load_mode_)) {
            return Status(
                grpc::StatusCode::FAILED_PRECONDITION,
                "Insert requires dataset_mode aos or both");
        }
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

Status Mini2ServiceImpl::Delete(ServerContext* context, const DeleteRequest* request,
                                DeleteResponse* response) {
    (void)context;
    std::cout << "[" << node_id_ << "] Received Delete request: "
              << request->request_id() << std::endl;

    const bool has_filters = HasDeleteFilters(*request);
    if (!request->delete_all() && !has_filters) {
        return Status(
            grpc::StatusCode::INVALID_ARGUMENT,
            "Delete requires at least one filter, or set delete_all=true");
    }
    if (request->delete_all() && has_filters) {
        return Status(
            grpc::StatusCode::INVALID_ARGUMENT,
            "delete_all cannot be combined with attribute filters");
    }

    try {
        DeleteResponse result = job_queue_.EnqueueAndWait(*request);
        *response = std::move(result);
        return Status::OK;
    } catch (const std::exception& ex) {
        std::cerr << "[" << node_id_ << "] Delete job failed: "
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

DeleteResponse Mini2ServiceImpl::ProcessDeleteJob(const DeleteRequest& request) {
    DeleteResponse response;
    response.set_request_id(request.request_id());

    const std::uint64_t local_deleted = DeleteMatchingRecordsLocally(request);
    {
        auto* node_count = response.add_node_counts();
        node_count->set_node_id(node_id_);
        node_count->set_deleted_count(local_deleted);
    }
    std::uint64_t total_deleted = local_deleted;

    std::vector<std::future<DeleteResponse>> peer_response_futures;
    for (const auto& peer : connected_peers_) {
        auto* stub = peer.stub.get();
        const std::string peer_id = peer.id;

        peer_response_futures.push_back(std::async(
            std::launch::async,
            [request, stub, peer_id]() {
                DeleteResponse peer_response;
                grpc::ClientContext peer_context;
                peer_context.set_deadline(
                    std::chrono::system_clock::now() + std::chrono::seconds(5));

                grpc::Status status = stub->Delete(&peer_context, request, &peer_response);
                if (!status.ok()) {
                    std::cout << "Failed to forward delete request to peer "
                              << peer_id << ": " << status.error_code() << " "
                              << status.error_message() << std::endl;
                    DeleteResponse empty_response;
                    empty_response.set_request_id(request.request_id());
                    return empty_response;
                }
                return peer_response;
            }));
    }

    for (auto& peer_response_future : peer_response_futures) {
        DeleteResponse peer_response = peer_response_future.get();
        for (const auto& node_count : peer_response.node_counts()) {
            response.add_node_counts()->CopyFrom(node_count);
            total_deleted += node_count.deleted_count();
        }
    }

    if (total_deleted > 0) {
        forward_cache_.Clear();
    }

    return response;
}

QueryResponse Mini2ServiceImpl::ProcessJob(JobType type, const QueryRequest& request) {
    switch (type) {
        case JobType::Query:
            return ProcessQueryJob(request);
        case JobType::Forward:
            return ProcessForwardJob(request);
        case JobType::Insert:
        case JobType::Delete:
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
                peer_context.set_deadline(std::chrono::system_clock::now() + kForwardPeerTimeout);

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

bool Mini2ServiceImpl::StreamLocalForwardChunks(
    ServerContext* context,
    const QueryRequest& request,
    std::uint32_t chunk_size,
    const std::function<bool(QueryChunkResponse*)>& emit_chunk,
    std::uint64_t* local_matched_record_count) {
    QueryChunkResponse chunk;
    chunk.set_request_id(request.request_id());
    chunk.set_from_node(node_id_);
    chunk.set_chunk_size(chunk_size);
    chunk.mutable_records()->Reserve(static_cast<int>(chunk_size));

    std::lock_guard<std::mutex> dataset_lock(dataset_mutex_);
    for (const auto& record : dataset_.get_records()) {
        if (context->IsCancelled()) {
            return false;
        }
        if (!MatchesQuery(record, request)) {
            continue;
        }

        AppendRecord(record, &chunk);
        ++(*local_matched_record_count);

        if (static_cast<std::uint32_t>(chunk.records_size()) < chunk_size) {
            continue;
        }

        if (!emit_chunk(&chunk)) {
            return false;
        }

        chunk.Clear();
        chunk.set_request_id(request.request_id());
        chunk.set_from_node(node_id_);
        chunk.set_chunk_size(chunk_size);
        chunk.mutable_records()->Reserve(static_cast<int>(chunk_size));
    }

    if (chunk.records_size() > 0) {
        if (!emit_chunk(&chunk)) {
            return false;
        }
    }

    return true;
}

bool Mini2ServiceImpl::StreamBufferedLocalForwardChunks(
    ServerContext* context,
    const QueryRequest& request,
    std::uint32_t chunk_size,
    const std::function<bool(QueryChunkResponse*)>& emit_chunk,
    std::uint64_t* local_matched_record_count) {
    LocalSearchResult local_result;
    {
        std::lock_guard<std::mutex> dataset_lock(dataset_mutex_);
        local_result = CollectLocalMatches(dataset_, request);
    }

    *local_matched_record_count = local_result.matched_record_count;

    QueryChunkResponse chunk;
    chunk.set_request_id(request.request_id());
    chunk.set_from_node(node_id_);
    chunk.set_chunk_size(chunk_size);
    chunk.mutable_records()->Reserve(static_cast<int>(chunk_size));

    for (const auto& record : local_result.records) {
        if (context->IsCancelled()) {
            return false;
        }

        AppendRecord(record, &chunk);
        if (static_cast<std::uint32_t>(chunk.records_size()) < chunk_size) {
            continue;
        }

        if (!emit_chunk(&chunk)) {
            return false;
        }

        chunk.Clear();
        chunk.set_request_id(request.request_id());
        chunk.set_from_node(node_id_);
        chunk.set_chunk_size(chunk_size);
        chunk.mutable_records()->Reserve(static_cast<int>(chunk_size));
    }

    if (chunk.records_size() > 0) {
        if (!emit_chunk(&chunk)) {
            return false;
        }
    }

    return true;
}

/** Distributed fan-out SOA count query. Every node handles its local data and forwards requests to peers, regarding coordinator and workers */
Status Mini2ServiceImpl::CountQuery(ServerContext* context, const SOACountRequest* request,
                                    SOACountResponse* response) {
    (void)context;
    std::cout << "[" << node_id_ << "] Received CountQuery request: "
              << request->request_id() << std::endl;
    response->set_request_id(request->request_id());
    response->set_from_node(node_id_); // node itself, tracing purpose. reply sent back to the original requester 
    if (!LoadsSOA(dataset_load_mode_)) {
        return Status(
            grpc::StatusCode::FAILED_PRECONDITION,
            "CountQuery requires dataset_mode soa or both");
    }

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

Status Mini2ServiceImpl::TopKQuery(ServerContext* context, const SOATopKRequest* request,
                                   SOATopKResponse* response) {
    (void)context;
    std::cout << "[" << node_id_ << "] Received TopKQuery request: "
              << request->request_id() << std::endl;

    response->set_request_id(request->request_id());
    response->set_from_node(node_id_);

    if (!LoadsSOA(dataset_load_mode_)) {
        return Status(
            grpc::StatusCode::FAILED_PRECONDITION,
            "TopKQuery requires dataset_mode soa or both");
    }
    if (request->created_date_start() <= 0 ||
        request->created_date_end() <= 0 ||
        request->created_date_end() < request->created_date_start()) {
        return Status(
            grpc::StatusCode::INVALID_ARGUMENT,
            "created date range must be > 0 and start <= end");
    }
    if (!request->return_all_counts() && request->top_k() == 0) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, "top_k must be > 0");
    }

    std::unordered_map<uint32_t, std::uint64_t> merged_counts;
    try {
        std::lock_guard<std::mutex> lock(dataset_mutex_);
        QuerySOA query(dataset_soa_);
        merged_counts = query.get_complaint_counts_in_created_date_range(
            request->created_date_start(),
            request->created_date_end());
    } catch (const std::invalid_argument& ex) {
        return Status(grpc::StatusCode::INVALID_ARGUMENT, ex.what());
    } catch (const std::exception& ex) {
        return Status(grpc::StatusCode::INTERNAL, ex.what());
    }

    std::vector<std::future<SOATopKResponse>> peer_futures;
    if (!connected_peers_.empty()) {
        SOATopKRequest peer_request = *request;
        peer_request.set_return_all_counts(true);

        for (const auto& peer : connected_peers_) {
            auto* stub = peer.stub.get();
            const std::string peer_id = peer.id;

            peer_futures.push_back(std::async(
                std::launch::async,
                [peer_request, stub, peer_id]() {
                    SOATopKResponse peer_response;
                    grpc::ClientContext peer_context;
                    peer_context.set_deadline(
                        std::chrono::system_clock::now() + kForwardPeerTimeout);

                    grpc::Status status =
                        stub->TopKQuery(&peer_context, peer_request, &peer_response);
                    if (!status.ok()) {
                        std::cout << "Failed to forward top-k query to peer "
                                  << peer_id << ": " << status.error_code()
                                  << " " << status.error_message() << std::endl;
                        return SOATopKResponse();
                    }
                    return peer_response;
                }));
        }
    }

    for (auto& peer_future : peer_futures) {
        SOATopKResponse peer_response = peer_future.get();
        for (const auto& entry : peer_response.entries()) {
            merged_counts[entry.key()] += entry.count();
        }
    }

    if (request->return_all_counts()) {
        for (const auto& [key, count] : merged_counts) {
            auto* entry = response->add_entries();
            entry->set_key(key);
            entry->set_count(count);
        }
    } else {
        using TopKItem = std::pair<std::uint64_t, uint32_t>;
        std::priority_queue<TopKItem> heap;
        for (const auto& [key, count] : merged_counts) {
            heap.emplace(count, key);
        }

        const std::size_t limit = std::min<std::size_t>(
            static_cast<std::size_t>(request->top_k()),
            heap.size());
        for (std::size_t i = 0; i < limit; ++i) {
            const auto [count, key] = heap.top();
            heap.pop();
            auto* entry = response->add_entries();
            entry->set_key(key);
            entry->set_count(count);
        }
    }

    std::cout << "[" << node_id_ << "] TopKQuery returning "
              << response->entries_size() << " entries" << std::endl;
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
        if (LoadsSOA(dataset_load_mode_)) {
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
}

std::uint64_t Mini2ServiceImpl::DeleteMatchingRecordsLocally(
    const DeleteRequest& request) {
    std::lock_guard<std::mutex> lock(dataset_mutex_);

    const auto& records = dataset_.get_records();
    std::vector<std::size_t> matching_indices;
    matching_indices.reserve(records.size());

    for (std::size_t i = 0; i < records.size(); ++i) {
        if (MatchesDeleteFilter(records[i], request)) {
            matching_indices.push_back(i);
        }
    }

    if (matching_indices.empty()) {
        return 0;
    }

    const std::size_t deleted_from_aos =
        dataset_.erase_records_by_indices(matching_indices);
    const std::size_t deleted_from_soa =
        dataset_soa_.erase_records_by_indices(matching_indices);
    if (deleted_from_aos != deleted_from_soa) {
        throw std::runtime_error("AOS and SOA delete counts diverged");
    }

    std::cout << "[" << node_id_ << "] Deleted " << deleted_from_aos
              << " records for request " << request.request_id() << std::endl;
    return static_cast<std::uint64_t>(deleted_from_aos);
}

Status Mini2ServiceImpl::StartForwardChunks(
    ServerContext* context, const QueryRequest* request,
    ChunkSessionResponse* response) {
    (void)context;

    const std::uint32_t chunk_size = 
        request->has_chunk_size() && request->chunk_size() > 0
            ? request->chunk_size()
            : kDefaultChunkSize;  

    if (!LoadsAOS(dataset_load_mode_)) {
        return Status(
            grpc::StatusCode::FAILED_PRECONDITION,
            "StartForwardChunks requires dataset_mode aos or both");
    }
    
    QueryResponse full_result = job_queue_.EnqueueAndWait(JobType::Forward, *request);
    const std::uint64_t total_records =
        static_cast<std::uint64_t>(full_result.records_size());
    const std::uint32_t total_chunks = total_records == 0
        ? 0
        : static_cast<std::uint32_t>((total_records + chunk_size - 1) / chunk_size);

    const std::string session_id = CreateChunkSessionId(request->request_id());

    ChunkSession session;
    session.request_id = request->request_id();
    session.from_node = node_id_;
    session.total_chunks = total_chunks;
    session.chunk_size = chunk_size;
    session.records.reserve(full_result.records_size());
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
    const std::uint64_t start =
        static_cast<std::uint64_t>(request->chunk_index()) * session.chunk_size;
    if (start >= session.records.size()) {
        return Status(grpc::StatusCode::OUT_OF_RANGE, "Chunk index out of range");
    }
    const std::uint64_t end = std::min<std::uint64_t>(
        start + session.chunk_size,
        static_cast<std::uint64_t>(session.records.size()));

    response->set_request_id(session.request_id);
    response->set_session_id(request->session_id());
    response->set_from_node(session.from_node);
    response->set_chunk_index(request->chunk_index());
    response->set_chunk_size(session.chunk_size);
    response->set_total_chunks(session.total_chunks);
    response->set_total_records(
        static_cast<std::uint64_t>(session.records.size()));

    for (std::uint64_t i = start; i < end; ++i) {
        response->add_records()->CopyFrom(
            session.records[static_cast<std::size_t>(i)]);
    }

    response->set_done(request->chunk_index() + 1 >= session.total_chunks);
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
