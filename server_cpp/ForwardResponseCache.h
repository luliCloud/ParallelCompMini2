#pragma once

#include <cstddef>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>

#include "mini2.grpc.pb.h"

class ForwardResponseCache {
public:
    // node_id is only used for logs.
    // enabled lets YAML decide which node should cache Forward results.
    ForwardResponseCache(
        std::string node_id,
        bool enabled,
        std::string policy,
        std::size_t max_entries = 32);

    // Return a cached distributed response when the same filters were seen before.
    bool TryGet(
        const mini2::QueryRequest& request,
        mini2::QueryResponse* response);

    // Save the latest distributed response and keep the cache bounded.
    void Store(
        const mini2::QueryRequest& request,
        const mini2::QueryResponse& response);

    void Clear();

private:
    struct CachedResponseEntry {
        mini2::QueryResponse response;
        std::list<std::string>::iterator order_position;
    };

    // request_id is excluded so repeated queries with different IDs can still hit the cache.
    std::string BuildCacheKey(const mini2::QueryRequest& request) const;

    std::string node_id_;
    bool enabled_;
    std::string policy_;
    std::size_t max_entries_;
    std::mutex cache_mutex_;
    std::list<std::string> cache_order_;
    std::unordered_map<std::string, CachedResponseEntry> cached_responses_;
};
