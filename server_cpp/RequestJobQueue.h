#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "mini2.grpc.pb.h"

using mini2::QueryRequest;
using mini2::QueryResponse;
using mini2::InsertRequest;
using mini2::InsertResponse;
using mini2::DeleteRequest;
using mini2::DeleteResponse;

enum class JobType { Query, Forward, Insert, Delete };
enum class QueueMode { FIFO, Priority };

class RequestJobQueue {
public:
    using QueryJobProcessor = std::function<QueryResponse(JobType, const QueryRequest&)>;
    using InsertJobProcessor = std::function<InsertResponse(const InsertRequest&)>;
    using DeleteJobProcessor = std::function<DeleteResponse(const DeleteRequest&)>;

    RequestJobQueue(
        std::string node_id,
        QueueMode queue_mode,
        QueryJobProcessor query_processor,
        InsertJobProcessor insert_processor,
        DeleteJobProcessor delete_processor);
    ~RequestJobQueue();

    QueryResponse EnqueueAndWait(JobType type, const QueryRequest& request);
    InsertResponse EnqueueAndWait(const InsertRequest& request);
    DeleteResponse EnqueueAndWait(const DeleteRequest& request);

private:
    struct QueuedJob {
        JobType type;
        QueryRequest query_request;
        InsertRequest insert_request;
        DeleteRequest delete_request;
        std::promise<QueryResponse> query_promise;
        std::promise<InsertResponse> insert_promise;
        std::promise<DeleteResponse> delete_promise;
        std::size_t queue_order = 0;
        int priority = 0;
        std::size_t scheduling_priority = 0;
    };

    struct QueuedJobCompare {
        bool operator()(
            const std::shared_ptr<QueuedJob>& left,
            const std::shared_ptr<QueuedJob>& right) const;
    };

    void WorkerLoop();
    std::shared_ptr<QueuedJob> PopNextJob();
    int GetJobPriority(const QueuedJob& job) const;
    int GetQueryPriority(const QueryRequest& request) const;
    std::size_t GetSchedulingPriority(const QueuedJob& job) const;

    std::string node_id_;
    QueueMode queue_mode_;
    QueryJobProcessor query_processor_;
    InsertJobProcessor insert_processor_;
    DeleteJobProcessor delete_processor_;
    std::priority_queue<
        std::shared_ptr<QueuedJob>,
        std::vector<std::shared_ptr<QueuedJob>>,
        QueuedJobCompare> request_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_condition_;
    std::thread worker_thread_;
    bool stop_worker_ = false;
    std::size_t next_queue_order_ = 1;
};
