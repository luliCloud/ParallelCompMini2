#include "RequestJobQueue.h"

#include <iostream>
#include <utility>

namespace {

const char* GetJobTypeName(JobType type) {
    if (type == JobType::Query) {
        return "Query";
    }
    if (type == JobType::Forward) {
        return "Forward";
    }
    if (type == JobType::Insert) {
        return "Insert";
    }
    return "Delete";
}

const char* GetQueueModeName(QueueMode mode) {
    return mode == QueueMode::Priority ? "priority" : "fifo";
}

}  // namespace

RequestJobQueue::RequestJobQueue(
    std::string node_id,
    QueueMode queue_mode,
    QueryJobProcessor query_processor,
    InsertJobProcessor insert_processor,
    DeleteJobProcessor delete_processor)
    : node_id_(std::move(node_id)),
      queue_mode_(queue_mode),
      query_processor_(std::move(query_processor)),
      insert_processor_(std::move(insert_processor)),
      delete_processor_(std::move(delete_processor)) {
    std::cout << "Node " << node_id_
              << " queue mode " << GetQueueModeName(queue_mode_) << std::endl;
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

bool RequestJobQueue::QueuedJobCompare::operator()(
    const std::shared_ptr<QueuedJob>& left,
    const std::shared_ptr<QueuedJob>& right) const {
    if (left->scheduling_priority != right->scheduling_priority) {
        return left->scheduling_priority > right->scheduling_priority;
    }
    return left->queue_order > right->queue_order;
}

QueryResponse RequestJobQueue::EnqueueAndWait(JobType type, const QueryRequest& request) {
    auto queued_job = std::make_shared<QueuedJob>();
    queued_job->type = type;
    queued_job->query_request = request;
    std::future<QueryResponse> response_future = queued_job->query_promise.get_future();

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        queued_job->queue_order = next_queue_order_;
        next_queue_order_ += 1;
        queued_job->priority = GetJobPriority(*queued_job);
        queued_job->scheduling_priority = GetSchedulingPriority(*queued_job);
        request_queue_.push(queued_job);

        std::cout << "Node " << node_id_
                  << " enqueued " << GetJobTypeName(type)
                  << " request " << request.request_id()
                  << " queue_order " << queued_job->queue_order
                  << " priority " << queued_job->priority << std::endl;
    }

    queue_condition_.notify_one();

    return response_future.get();
}

InsertResponse RequestJobQueue::EnqueueAndWait(const InsertRequest& request) {
    auto queued_job = std::make_shared<QueuedJob>();
    queued_job->type = JobType::Insert;
    queued_job->insert_request = request;
    std::future<InsertResponse> response_future = queued_job->insert_promise.get_future();

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        queued_job->queue_order = next_queue_order_;
        next_queue_order_ += 1;
        queued_job->priority = GetJobPriority(*queued_job);
        queued_job->scheduling_priority = GetSchedulingPriority(*queued_job);
        request_queue_.push(queued_job);

        std::cout << "Node " << node_id_
                  << " enqueued Insert request " << request.request_id()
                  << " queue_order " << queued_job->queue_order
                  << " priority " << queued_job->priority << std::endl;
    }

    queue_condition_.notify_one();

    return response_future.get();
}

DeleteResponse RequestJobQueue::EnqueueAndWait(const DeleteRequest& request) {
    auto queued_job = std::make_shared<QueuedJob>();
    queued_job->type = JobType::Delete;
    queued_job->delete_request = request;
    std::future<DeleteResponse> response_future = queued_job->delete_promise.get_future();

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        queued_job->queue_order = next_queue_order_;
        next_queue_order_ += 1;
        queued_job->priority = GetJobPriority(*queued_job);
        queued_job->scheduling_priority = GetSchedulingPriority(*queued_job);
        request_queue_.push(queued_job);

        std::cout << "Node " << node_id_
                  << " enqueued Delete request " << request.request_id()
                  << " queue_order " << queued_job->queue_order
                  << " priority " << queued_job->priority << std::endl;
    }

    queue_condition_.notify_one();

    return response_future.get();
}

void RequestJobQueue::WorkerLoop() {
    while (true) {
        std::shared_ptr<QueuedJob> queued_job;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_condition_.wait(lock, [this] {
                return stop_worker_ || !request_queue_.empty();
            });
            if (stop_worker_ && request_queue_.empty()) {
                return;
            }

            queued_job = PopNextJob();
        }

        const std::string request_id =
            queued_job->type == JobType::Insert
                ? queued_job->insert_request.request_id()
                : (queued_job->type == JobType::Delete
                       ? queued_job->delete_request.request_id()
                       : queued_job->query_request.request_id());
        std::cout << "Node " << node_id_
                  << " worker processing " << GetJobTypeName(queued_job->type)
                  << " request " << request_id
                  << " queue_order " << queued_job->queue_order
                  << " priority " << queued_job->priority << std::endl;

        try {
            if (queued_job->type == JobType::Insert) {
                InsertResponse response = insert_processor_(queued_job->insert_request);
                queued_job->insert_promise.set_value(std::move(response));
            } else if (queued_job->type == JobType::Delete) {
                DeleteResponse response = delete_processor_(queued_job->delete_request);
                queued_job->delete_promise.set_value(std::move(response));
            } else {
                QueryResponse response =
                    query_processor_(queued_job->type, queued_job->query_request);
                queued_job->query_promise.set_value(std::move(response));
            }
        } catch (...) {
            try {
                if (queued_job->type == JobType::Insert) {
                    queued_job->insert_promise.set_exception(std::current_exception());
                } else if (queued_job->type == JobType::Delete) {
                    queued_job->delete_promise.set_exception(std::current_exception());
                } else {
                    queued_job->query_promise.set_exception(std::current_exception());
                }
            } catch (...) {
                std::cerr << "Node " << node_id_
                          << " failed to propagate exception for request "
                          << request_id << std::endl;
            }
        }

        std::cout << "Node " << node_id_
                  << " worker completed " << GetJobTypeName(queued_job->type)
                  << " request " << request_id
                  << " queue_order " << queued_job->queue_order
                  << " priority " << queued_job->priority << std::endl;
    }
}

std::shared_ptr<RequestJobQueue::QueuedJob> RequestJobQueue::PopNextJob() {
    auto job = request_queue_.top();
    request_queue_.pop();
    return job;
}

int RequestJobQueue::GetJobPriority(const QueuedJob& job) const {
    if (job.type == JobType::Insert || job.type == JobType::Delete) {
        return 0;
    }
    return GetQueryPriority(job.query_request);
}

int RequestJobQueue::GetQueryPriority(const QueryRequest& request) const {
    if (request.has_zip_code()) {
        return 1;
    }
    if (request.has_lat_min() || request.has_lat_max() ||
        request.has_lon_min() || request.has_lon_max()) {
        return 2;
    }
    if (request.has_borough_id() || request.has_agency_id()) {
        return 3;
    }
    return 4;
}

std::size_t RequestJobQueue::GetSchedulingPriority(const QueuedJob& job) const {
    if (queue_mode_ == QueueMode::FIFO) {
        return job.queue_order;
    }
    return static_cast<std::size_t>(job.priority);
}
