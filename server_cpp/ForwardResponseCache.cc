#include "ForwardResponseCache.h"

#include <iostream>
#include <sstream>
#include <utility>

ForwardResponseCache::ForwardResponseCache(
    std::string node_id,
    bool enabled,
    std::size_t max_entries)
    : node_id_(std::move(node_id)),
      enabled_(enabled),
      max_entries_(max_entries) {}

bool ForwardResponseCache::TryGet(
    const mini2::QueryRequest& request,
    mini2::QueryResponse* response) {
    if (!enabled_) {
        return false;
    }

    std::lock_guard<std::mutex> lock(cache_mutex_);

    const std::string cache_key = BuildCacheKey(request);
    auto cached_entry = cached_responses_.find(cache_key);
    if (cached_entry == cached_responses_.end()) {
        std::cout << "[" << node_id_ << "] Forward cache miss: "
                  << request.request_id() << std::endl;
        return false;
    }

    // Move a hit to the front so the least recently used entry stays at the back.
    cache_order_.erase(cached_entry->second.order_position);
    cache_order_.push_front(cache_key);
    cached_entry->second.order_position = cache_order_.begin();

    *response = cached_entry->second.response;
    // Cached payload is reused, but the response should still carry the current request ID.
    response->set_request_id(request.request_id());
    response->set_from_node(node_id_);

    std::cout << "[" << node_id_ << "] Forward cache hit: "
              << request.request_id() << std::endl;
    return true;
}

void ForwardResponseCache::Store(
    const mini2::QueryRequest& request,
    const mini2::QueryResponse& response) {
    if (!enabled_) {
        return;
    }

    std::lock_guard<std::mutex> lock(cache_mutex_);

    const std::string cache_key = BuildCacheKey(request);
    auto existing_entry = cached_responses_.find(cache_key);
    if (existing_entry != cached_responses_.end()) {
        cache_order_.erase(existing_entry->second.order_position);
        cached_responses_.erase(existing_entry);
    }

    // Drop the oldest entry when the cache is full.
    if (cached_responses_.size() >= max_entries_ &&
        !cache_order_.empty()) {
        const std::string oldest_cache_key = cache_order_.back();
        cache_order_.pop_back();
        cached_responses_.erase(oldest_cache_key);
    }

    cache_order_.push_front(cache_key);

    CachedResponseEntry cached_entry;
    cached_entry.response = response;
    // request_id must not be part of cached content because each caller has its own ID.
    cached_entry.response.clear_request_id();
    cached_entry.order_position = cache_order_.begin();

    cached_responses_.emplace(cache_key, std::move(cached_entry));

    std::cout << "[" << node_id_ << "] Forward cache stored: "
              << request.request_id() << std::endl;
}

void ForwardResponseCache::Clear() {
    if (!enabled_) {
        return;
    }

    std::lock_guard<std::mutex> lock(cache_mutex_);
    cache_order_.clear();
    cached_responses_.clear();

    std::cout << "[" << node_id_ << "] Forward cache cleared" << std::endl;
}

std::string ForwardResponseCache::BuildCacheKey(
    const mini2::QueryRequest& request) const {
    std::ostringstream cache_key;
    cache_key << "forward";

    // Only semantic filters are included in the key.
    if (request.has_agency_id()) {
        cache_key << "|agency_id:" << request.agency_id();
    }
    if (request.has_borough_id()) {
        cache_key << "|borough_id:" << request.borough_id();
    }
    if (request.has_zip_code()) {
        cache_key << "|zip_code:" << request.zip_code();
    }
    if (request.has_lat_min()) {
        cache_key << "|lat_min:" << request.lat_min();
    }
    if (request.has_lat_max()) {
        cache_key << "|lat_max:" << request.lat_max();
    }
    if (request.has_lon_min()) {
        cache_key << "|lon_min:" << request.lon_min();
    }
    if (request.has_lon_max()) {
        cache_key << "|lon_max:" << request.lon_max();
    }

    return cache_key.str();
}
