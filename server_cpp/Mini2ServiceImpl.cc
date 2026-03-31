#include "Mini2ServiceImpl.h"
#include <iostream>

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

    // TODO: Phase 1: Basic query implementation
    // For now, just return empty response
    std::cout << "[" << node_id_ << "] Query return 0 records" << std::endl;
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
