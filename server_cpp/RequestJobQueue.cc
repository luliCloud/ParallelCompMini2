#include "RequestJobQueue.h"

#include <iostream>

namespace {

const char* GetJobTypeName(JobType type) {
    if (type == JobType::Query) {
        return "Query";
    }
    if (type == JobType::Forward) {
        return "Forward";
    }
    return "Insert";
}

}  // namespace

RequestJobQueue::RequestJobQueue(
    std::string node_id,
    QueryJobProcessor query_processor,
    InsertJobProcessor insert_processor)
    : node_id_(std::move(node_id)),
      query_processor_(std::move(query_processor)),
      insert_processor_(std::move(insert_processor)) {
    worker_thread_ = std::thread(&RequestJobQueue::WorkerLoop, this);
}

RequestJobQueue::~RequestJobQueue() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        stop_worker_ = true;
    }
    queue_condition_.notify_all();
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

QueryResponse RequestJobQueue::EnqueueAndWait(JobType type, const QueryRequest& request) {
    QueuedJob queued_job;
    queued_job.type = type;
    queued_job.query_request = request;
    std::future<QueryResponse> response_future = queued_job.query_promise.get_future();

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        queued_job.queue_order = next_queue_order_;
        next_queue_order_ += 1;
        request_queue_.push_back(std::move(queued_job));

        const QueuedJob& last_queued_job = request_queue_.back();
        std::cout << "Node " << node_id_
                  << " enqueued " << GetJobTypeName(type)
                  << " request " << request.request_id()
                  << " queue_order " << last_queued_job.queue_order << std::endl;
    }

    queue_condition_.notify_one();

    return response_future.get();
}

InsertResponse RequestJobQueue::EnqueueAndWait(const InsertRequest& request) {
    QueuedJob queued_job;
    queued_job.type = JobType::Insert;
    queued_job.insert_request = request;
    std::future<InsertResponse> response_future = queued_job.insert_promise.get_future();

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        queued_job.queue_order = next_queue_order_;
        next_queue_order_ += 1;
        request_queue_.push_back(std::move(queued_job));

        const QueuedJob& last_queued_job = request_queue_.back();
        std::cout << "Node " << node_id_
                  << " enqueued Insert request " << request.request_id()
                  << " queue_order " << last_queued_job.queue_order << std::endl;
    }

    queue_condition_.notify_one();

    return response_future.get();
}

void RequestJobQueue::WorkerLoop() {
    while (true) {
        QueuedJob queued_job;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_condition_.wait(lock, [this] {
                return stop_worker_ || !request_queue_.empty();
            });
            if (stop_worker_ && request_queue_.empty()) {
                return;
            }

            queued_job = std::move(request_queue_.front());
            request_queue_.pop_front();
        }

        const std::string request_id =
            queued_job.type == JobType::Insert
                ? queued_job.insert_request.request_id()
                : queued_job.query_request.request_id();
        std::cout << "Node " << node_id_
                  << " worker processing " << GetJobTypeName(queued_job.type)
                  << " request " << request_id
                  << " queue_order " << queued_job.queue_order << std::endl;

        try {
            if (queued_job.type == JobType::Insert) {
                InsertResponse response = insert_processor_(queued_job.insert_request);
                queued_job.insert_promise.set_value(std::move(response));
            } else {
                QueryResponse response =
                    query_processor_(queued_job.type, queued_job.query_request);
                queued_job.query_promise.set_value(std::move(response));
            }
        } catch (...) {
            try {
                if (queued_job.type == JobType::Insert) {
                    queued_job.insert_promise.set_exception(std::current_exception());
                } else {
                    queued_job.query_promise.set_exception(std::current_exception());
                }
            } catch (...) {
                std::cerr << "Node " << node_id_
                          << " failed to propagate exception for request "
                          << request_id << std::endl;
            }
        }

        std::cout << "Node " << node_id_
                  << " worker completed " << GetJobTypeName(queued_job.type)
                  << " request " << request_id
                  << " queue_order " << queued_job.queue_order << std::endl;
    }
}
