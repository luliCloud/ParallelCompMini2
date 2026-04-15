#include "Mini2ServiceImpl.h"
#include <iostream>
#include <future>
#include <grpcpp/grpcpp.h>

namespace {

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

}  // namespace

Mini2ServiceImpl::Mini2ServiceImpl(const std::string& node_id, uint16_t port)
    : node_id_(node_id), port_(port) {}

bool Mini2ServiceImpl::Initialize(
    const std::string& dataset_path,
    const std::string& agency_dict_path,
    const std::string& borough_dict_path) {
    try {
        if (!dataset_.load_csv(dataset_path, agency_dict_path, borough_dict_path)) {
            std::cerr << "Failed to load dataset from " << dataset_path 
                      << " at node: " << node_id_ << std::endl;
            return false;
        }
        std::cout << "[" << node_id_ << "] Loaded " << dataset_.size()
                  << " records" << std::endl;

        if (!dataset_soa_.load_csv(dataset_path, agency_dict_path, borough_dict_path)) {
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
    for (const auto& p : peers) {
        // Create the channel
        auto channel = grpc::CreateChannel(p.address, grpc::InsecureChannelCredentials());

        // Create the stub
        ConnectedPeer connected;
        connected.id = p.id;
        connected.stub = mini2::NodeService::NewStub(channel);

        connected_peers_.push_back(std::move(connected));
    }
}

Status Mini2ServiceImpl::Ping(ServerContext* context, const PingRequest* request,
                             PingResponse* response) {
    (void)context;
    std::cout << "[" << node_id_ << "] Received Ping request" << std::endl;
    response->set_request_id(request->request_id());
    response->add_active_nodes(node_id_);

    if (!connected_peers_.empty())
    {
        std::vector<std::future<PingResponse>> futures;

        for (const auto& peer : connected_peers_)
        {
            auto* stub = peer.stub.get();
            std::string peer_id = peer.id;

            futures.push_back(std::async(std::launch::async, [request, stub, peer_id]()
            {
                PingResponse peer_res;
                grpc::ClientContext peer_ctx;
                peer_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));

                grpc::Status status = stub->Ping(&peer_ctx, *request, &peer_res);
                if (!status.ok())
                {
                    std::cout << "Failed to ping peer " << peer_id << ": " << status.error_code() << " " << status.error_message() << std::endl;
                    return PingResponse();
                }
                return peer_res;
            }));
        }

        for (auto& f : futures)
        {
            PingResponse peer_res = f.get();
            for (const auto& node_name : peer_res.active_nodes())
            {
                response->add_active_nodes(node_name);
            }
        }
    }
    return Status::OK;
}

Status Mini2ServiceImpl::Query(ServerContext* context, const QueryRequest* request, 
                              QueryResponse* response) {
    std::cout << "[" << node_id_ << "] Received Query request: " 
              << request->request_id() << std::endl;

    response->set_request_id(request->request_id());
    response->set_from_node(node_id_);

    std::size_t matched = 0;
    for (const auto& record : dataset_.get_records()) {
        if (!MatchesQuery(record, *request)) {
            continue;
        }
        AppendRecord(record, response);
        matched++;
    }

    std::cout << "[" << node_id_ << "] Query return " << matched
              << " records" << std::endl;
    return Status::OK;
}

Status Mini2ServiceImpl::Forward(ServerContext* context, const QueryRequest* request, 
                                QueryResponse* response) {
    (void)context;
    std::cout << "[" << node_id_ << "] Received Forward request: "
              << request->request_id() << std::endl;

    response->set_request_id(request->request_id());
    response->set_from_node(node_id_);

    // Search locally
    std::size_t local_matched = 0;
    for (const auto& record : dataset_.get_records()) {
        if (!MatchesQuery(record, *request)) {
            continue;
        }
        AppendRecord(record, response);
        local_matched++;
    }

    // Perform parallel forwarding search request to peers when result is not found in the local memory
    if (local_matched == 0 && !connected_peers_.empty()) {
        std::vector<std::future<QueryResponse>> futures;

        for (const auto& peer : connected_peers_) {
            auto* stub = peer.stub.get();
            std::string peer_id = peer.id;

            // Launch each peer request in a separate thread for performance
            futures.push_back(std::async(std::launch::async, [request, stub, peer_id]() {
                QueryResponse peer_res;
                grpc::ClientContext peer_ctx;
                peer_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

                grpc::Status status = stub->Forward(&peer_ctx, *request, &peer_res);
                if (!status.ok())
                {
                    std::cout << "Failed to forward request to peer " << peer_id << ": " << status.error_code() << " " << status.error_message() << std::endl;
                    return QueryResponse();
                }
                return peer_res;
            }));
        }

        // Wait for all peer requests to complete and aggregate results
        for (auto& f : futures) {
            QueryResponse peer_res = f.get();
            for (const auto& peer_rec : peer_res.records()) {
                // Combine peer records into the final response
                response->add_records()->CopyFrom(peer_rec);
            }
        }
    }

    std::cout << "[" << node_id_ << "] Forward returning " << response->records_size()
              << " total records (Local matched: " << local_matched << ")" << std::endl;

    return Status::OK;
}

/** Distributed fan-out SOA count query. Every node handles its local data and forwards requests to peers, regarding coordinator and workers */
Status Mini2ServiceImpl::CountQuery(ServerContext* context, const SOACountRequest* request,
                                    SOACountResponse* response) {
    (void)context;
    std::cout << "[" << node_id_ << "] Received CountQuery request: "
              << request->request_id() << std::endl;
    response->set_request_id(request->request_id());
    response->set_from_node(node_id_); // node itself, tracing purpose. reply sent back to the original requester 
    QuerySOA query(dataset_soa_);

    uint64_t local_count = 0;
    try {
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
