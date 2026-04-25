#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <future>
#include <mutex>
#include <string>
#include <thread>

#include "mini2.grpc.pb.h"

using mini2::QueryRequest;
using mini2::QueryResponse;
using mini2::InsertRequest;
using mini2::InsertResponse;

enum class JobType { Query, Forward, Insert };

class RequestJobQueue {
public:
    using QueryJobProcessor = std::function<QueryResponse(JobType, const QueryRequest&)>;
    using InsertJobProcessor = std::function<InsertResponse(const InsertRequest&)>;

    RequestJobQueue(
        std::string node_id,
        QueryJobProcessor query_processor,
        InsertJobProcessor insert_processor);
    ~RequestJobQueue();

    QueryResponse EnqueueAndWait(JobType type, const QueryRequest& request);
    InsertResponse EnqueueAndWait(const InsertRequest& request);

private:
    struct QueuedJob {
        JobType type;
        QueryRequest query_request;
        InsertRequest insert_request;
        std::promise<QueryResponse> query_promise;
        std::promise<InsertResponse> insert_promise;
        std::size_t queue_order = 0;
    };

    void WorkerLoop();

    std::string node_id_;
    QueryJobProcessor query_processor_;
    InsertJobProcessor insert_processor_;
    std::deque<QueuedJob> request_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_condition_;
    std::thread worker_thread_;
    bool stop_worker_ = false;
    std::size_t next_queue_order_ = 1;
};
