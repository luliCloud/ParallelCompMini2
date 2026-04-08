#include "Mini2ServiceImpl.h"
#include <iostream>

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

bool Mini2ServiceImpl::Initialize(const std::string& dataset_path) {
    try {
        if (!dataset_.load_csv(dataset_path)) {
            std::cerr << "Failed to load dataset from " << dataset_path 
                      << " at node: " << node_id_ << std::endl;
            return false;
        }
        std::cout << "[" << node_id_ << "] Loaded " << dataset_.size()
                  << " records" << std::endl;
        return true;
    } catch (const std::exception& ex) {
        std::cerr << "Exception while loading dataset at node: " << node_id_
                  << ". Error: " << ex.what() << std::endl;
        return false;
    }
}

Status Mini2ServiceImpl::Ping(ServerContext* context, const Empty* request, 
                             Empty* response) {
    std::cout << "[" << node_id_ << "] Received Ping request" << std::endl;
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
    std::cout << "[" << node_id_ << "] Received Forward request: " 
              << request->request_id() << std::endl;

    response->set_request_id(request->request_id());
    response->set_from_node(node_id_);

    // TODO: Phase 2 - Implement forward query logic

    return Status::OK;
}
