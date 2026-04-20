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

enum class JobType { Query, Forward };

class RequestJobQueue {
public:
    using JobProcessor = std::function<QueryResponse(JobType, const QueryRequest&)>;

    RequestJobQueue(std::string node_id, JobProcessor processor);
    ~RequestJobQueue();

    QueryResponse EnqueueAndWait(JobType type, const QueryRequest& request);

private:
    struct QueuedJob {
        JobType type;
        QueryRequest request;
        std::promise<QueryResponse> promise;
        std::size_t queue_order = 0;
    };

    void WorkerLoop();

    std::string node_id_;
    JobProcessor job_processor_;
    std::deque<QueuedJob> request_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_condition_;
    std::thread worker_thread_;
    bool stop_worker_ = false;
    std::size_t next_queue_order_ = 1;
};
