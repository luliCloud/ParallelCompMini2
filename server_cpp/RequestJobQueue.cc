#include "RequestJobQueue.h"

#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>

namespace {

// Convert enum value to a human-readable name for logs.
const char* GetJobTypeName(JobType type) {
    return type == JobType::Query ? "Query" : "Forward";
}

// Return elapsed time in milliseconds between two timestamps.
double GetElapsedMilliseconds(std::chrono::steady_clock::time_point start_time,
                              std::chrono::steady_clock::time_point end_time) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count() / 1000.0;
}

// Format timing values with two decimal places for logs.
std::string FormatMilliseconds(double elapsed_milliseconds) {
    std::ostringstream output;
    output << std::fixed << std::setprecision(2) << elapsed_milliseconds;
    return output.str();
}

}  // namespace

RequestJobQueue::RequestJobQueue(std::string node_id, JobProcessor processor)
    : node_id_(std::move(node_id)), job_processor_(std::move(processor)) {
    // Start one background worker for this node.
    worker_thread_ = std::thread(&RequestJobQueue::WorkerLoop, this);
}

RequestJobQueue::~RequestJobQueue() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        // Tell the worker to stop once the queue becomes empty.
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
    // Copy the request into the queue item. We should not keep a pointer
    // to the gRPC-owned request object after the handler returns.
    queued_job.request = request;
    queued_job.enqueue_time = std::chrono::steady_clock::now();
    std::future<QueryResponse> response_future = queued_job.promise.get_future();
    std::size_t queue_size_after_push = 0;

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        // Push the new job to the back of the FIFO queue.
        request_queue_.push_back(std::move(queued_job));
        queue_size_after_push = request_queue_.size();
    }

    // Wake the worker so it can start processing this request.
    queue_condition_.notify_one();

    std::cout << "Node " << node_id_
              << " enqueued " << GetJobTypeName(type)
              << " request " << request.request_id()
              << " queue_size " << queue_size_after_push << std::endl;

    // The RPC handler waits here until the worker finishes the job.
    return response_future.get();
}

void RequestJobQueue::WorkerLoop() {
    while (true) {
        QueuedJob queued_job;
        std::size_t queue_size_after_pop = 0;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            // Sleep until a new job arrives or shutdown begins.
            queue_condition_.wait(lock, [this] {
                return stop_worker_ || !request_queue_.empty();
            });
            if (stop_worker_ && request_queue_.empty()) {
                return;
            }

            // Pop the next request from the front of the FIFO queue.
            queued_job = std::move(request_queue_.front());
            request_queue_.pop_front();
            queue_size_after_pop = request_queue_.size();
        }

        const std::string request_id = queued_job.request.request_id();
        const auto worker_start_time = std::chrono::steady_clock::now();
        const double queue_wait_milliseconds =
            GetElapsedMilliseconds(queued_job.enqueue_time, worker_start_time);
        std::cout << "Node " << node_id_
                  << " worker processing " << GetJobTypeName(queued_job.type)
                  << " request " << request_id
                  << " queue_wait_ms " << FormatMilliseconds(queue_wait_milliseconds)
                  << " queue_remaining " << queue_size_after_pop << std::endl;

        try {
            // Run the actual job logic provided by Mini2ServiceImpl.
            QueryResponse response = job_processor_(queued_job.type, queued_job.request);
            queued_job.promise.set_value(std::move(response));
        } catch (...) {
            try {
                // Propagate worker exceptions back to the waiting RPC handler.
                queued_job.promise.set_exception(std::current_exception());
            } catch (...) {
                std::cerr << "Node " << node_id_
                          << " failed to propagate exception for request "
                          << request_id << std::endl;
            }
        }

        const auto worker_end_time = std::chrono::steady_clock::now();
        const double service_time_milliseconds =
            GetElapsedMilliseconds(worker_start_time, worker_end_time);
        const double total_queue_time_milliseconds =
            GetElapsedMilliseconds(queued_job.enqueue_time, worker_end_time);
        std::cout << "Node " << node_id_
                  << " worker completed " << GetJobTypeName(queued_job.type)
                  << " request " << request_id
                  << " service_time_ms " << FormatMilliseconds(service_time_milliseconds)
                  << " total_queue_time_ms "
                  << FormatMilliseconds(total_queue_time_milliseconds)
                  << std::endl;
    }
}
