#include "RequestJobQueue.h"

#include <iostream>

namespace {

const char* GetJobTypeName(JobType type) {
    return type == JobType::Query ? "Query" : "Forward";
}

}  // namespace

RequestJobQueue::RequestJobQueue(std::string node_id, JobProcessor processor)
    : node_id_(std::move(node_id)), job_processor_(std::move(processor)) {
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
    queued_job.request = request;
    std::future<QueryResponse> response_future = queued_job.promise.get_future();

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

        const std::string request_id = queued_job.request.request_id();
        std::cout << "Node " << node_id_
                  << " worker processing " << GetJobTypeName(queued_job.type)
                  << " request " << request_id
                  << " queue_order " << queued_job.queue_order << std::endl;

        try {
            QueryResponse response = job_processor_(queued_job.type, queued_job.request);
            queued_job.promise.set_value(std::move(response));
        } catch (...) {
            try {
                queued_job.promise.set_exception(std::current_exception());
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
