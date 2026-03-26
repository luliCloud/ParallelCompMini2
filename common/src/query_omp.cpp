#include "query_omp.hpp"
#include <algorithm>
#include <queue>
#include <stdexcept>
#include <unordered_set>

size_t QueryOMP::count_in_created_date_range(int64_t start, int64_t end) const {
    size_t count = 0;
    #pragma omp parallel for reduction(+:count)
    for (size_t i = 0; i < records_.size(); ++i) {
        const auto& record = records_[i];
        if (record.created_date >= start && record.created_date <= end) {
            count++;
        }
    }
    return count;
}

size_t QueryOMP::count_resolved_complaints_by_closed_date_range(
    uint8_t closed_id, int64_t start, int64_t end) const {
    if (start <= 0 || end <= 0 || end < start) {
        throw std::invalid_argument("start and end must greater than 0 and start <= end");
    }

    size_t count = 0;
    #pragma omp parallel for reduction(+:count)
    for (size_t i = 0; i < records_.size(); ++i) {
        const auto& record = records_[i];
        if (record.status_id == closed_id &&
            record.closed_date >= start &&
            record.closed_date <= end) {
            count++;
        }
    }
    return count;
}

/** how many complaints has been created for a specific agency */
size_t QueryOMP::count_by_agency_and_created_date_range(uint16_t agency_id, int64_t start, int64_t end) const {
    size_t count = 0;
    #pragma omp parallel for reduction(+:count)
    for (size_t i = 0; i < records_.size(); ++i) {
        const auto& record = records_[i];
        if (record.agency_id == agency_id &&
            record.created_date >= start &&
            record.created_date <= end) {
                count++;
        }
    }
    return count;
}

/** how many complaints has been created or resolved or [other_status] for a specific problem type 
 * TODO: check how many status in this dataset.
*/
size_t QueryOMP::count_by_status_and_created_date_range(uint8_t status_id, int64_t start, int64_t end) const {
    size_t count = 0;
    #pragma omp parallel for reduction(+:count)
    for (size_t i = 0; i < records_.size(); ++i) {
        const auto& record = records_[i];
        if (record.status_id == status_id &&
            record.created_date >= start &&
            record.created_date <= end) {
                count++;
        }
    }
    return count;
}

// borough_id -> (complaint_count, unique_agency_count) in [start, end]
// Parallel mechanism:
// 1) Each thread aggregates into local maps (no lock in hot loop).
// 2) Threads merge local maps into global maps in one critical section.
// This keeps serial-equivalent logic while reducing lock contention.
std::unordered_map<uint8_t, std::tuple<std::size_t, std::size_t>>
QueryOMP::get_borough_counts_in_created_date_range(int64_t start, int64_t end) const {
    const int64_t n = static_cast<int64_t>(records_.size());

    // borough_id -> (complaint_count, unique_agency_count)
    std::unordered_map<uint8_t, std::tuple<std::size_t, std::size_t>> summary;
    // borough_id -> set of unique agency_id seen in the time range
    std::unordered_map<uint8_t, std::unordered_set<uint16_t>> agencies_by_borough;

    #pragma omp parallel
    {
        std::unordered_map<uint8_t, std::size_t> local_counts; // complain num by borough
        std::unordered_map<uint8_t, std::unordered_set<uint16_t>> local_agencies_by_borough; // agency set by borough

        #pragma omp for nowait
        for (int64_t i = 0; i < n; ++i) {
            const auto &rec = records_[i];
            if (rec.created_date < start || rec.created_date > end) {
                continue;
            }
            local_counts[rec.borough_id]++;
            local_agencies_by_borough[rec.borough_id].insert(rec.agency_id);
        }

        #pragma omp critical
        {
            for (const auto &[borough_id, complaint_count] : local_counts) {
                std::get<0>(summary[borough_id]) += complaint_count;
            }

            for (const auto &[borough_id, local_agencies] : local_agencies_by_borough) {
                auto &global_agencies = agencies_by_borough[borough_id];
                const std::size_t before = global_agencies.size();
                global_agencies.insert(local_agencies.begin(), local_agencies.end());
                std::get<1>(summary[borough_id]) += (global_agencies.size() - before);
            }
        }
    }

    return summary;
}

// For one borough in [start, end], return (complaint_count, unique_agency_count)
// Parallel mechanism:
// 1) Each thread computes local complaint count + local unique agency set.
// 2) Merge local results in one critical section.
std::tuple<std::size_t, std::size_t>
QueryOMP::get_borough_counts(uint8_t borough_id, int64_t start, int64_t end) const {
    const int64_t n = static_cast<int64_t>(records_.size());

    std::size_t complaint_count = 0;
    std::unordered_set<uint16_t> agencies;

    #pragma omp parallel
    {
        std::size_t local_count = 0;
        std::unordered_set<uint16_t> local_agencies;

        #pragma omp for nowait
        for (int64_t i = 0; i < n; ++i) {
            const auto &rec = records_[i];
            if (rec.borough_id != borough_id) {
                continue;
            }
            if (rec.created_date < start || rec.created_date > end) {
                continue;
            }
            local_count++;
            local_agencies.insert(rec.agency_id);
        }
        /** nowait key words remove hidden barrier of omp here. 
         * If no added, all threads need to wait here for other thread to finish for loop. Then compete for the lock of critical region.
         */

        #pragma omp critical
        {
            complaint_count += local_count;
            agencies.insert(local_agencies.begin(), local_agencies.end());
        }
    }

    return {complaint_count, agencies.size()};
}

// complaint_id(problem_id) -> complaint_count in [start, end]
// Parallel mechanism:
// 1) Each thread counts complaint types in a local map.
// 2) Merge local maps in one critical section.
std::unordered_map<uint32_t, std::size_t>
QueryOMP::get_complaint_counts_in_created_date_range(int64_t start, int64_t end) const {
    const int64_t n = static_cast<int64_t>(records_.size());
    std::unordered_map<uint32_t, std::size_t> counts;

    #pragma omp parallel
    {
        std::unordered_map<uint32_t, std::size_t> local_counts;

        #pragma omp for nowait
        for (int64_t i = 0; i < n; ++i) {
            const auto &rec = records_[i];
            if (rec.created_date < start || rec.created_date > end) {
                continue;
            }
            local_counts[rec.problem_id]++;
        }

        #pragma omp critical
        {
            for (const auto &[problem_id, complaint_count] : local_counts) {
                counts[problem_id] += complaint_count;
            }
        }
    }

    return counts;
}

// Top-k complaint types by count in [start, end]
// Mechanism:
// 1) Reuse parallel complaint counts.
// 2) Build top-k with a heap in serial (small key space, simple and stable).
std::vector<std::tuple<uint32_t, std::size_t>>
QueryOMP::get_top_complaints_in_created_date_range(int64_t start, int64_t end, std::size_t top_k) const {
    const auto counts = get_complaint_counts_in_created_date_range(start, end);

    using HeapItem = std::pair<std::size_t, uint32_t>;
    std::priority_queue<HeapItem> heap;
    for (const auto &[complaint_id, complaint_count] : counts) {
        heap.emplace(complaint_count, complaint_id);
    }

    std::vector<std::tuple<uint32_t, std::size_t>> items;
    const std::size_t limit = std::min(top_k, heap.size());
    items.reserve(limit);
    for (std::size_t i = 0; i < limit; ++i) {
        const auto top = heap.top();
        heap.pop();
        items.emplace_back(top.second, top.first);
    }
    return items;
}

// For one complaint type in [start, end], return borough_id -> complaint_count
// Parallel mechanism:
// 1) Each thread counts borough totals in a local map.
// 2) Merge local maps in one critical section.
std::unordered_map<uint8_t, std::size_t>
QueryOMP::get_borough_counts_by_complaint_in_created_date_range(uint32_t complaint_id, int64_t start, int64_t end) const {
    const int64_t n = static_cast<int64_t>(records_.size());
    std::unordered_map<uint8_t, std::size_t> counts;

    #pragma omp parallel
    {
        std::unordered_map<uint8_t, std::size_t> local_counts;

        #pragma omp for nowait
        for (int64_t i = 0; i < n; ++i) {
            const auto &rec = records_[i];
            if (rec.problem_id != complaint_id) {
                continue;
            }
            if (rec.created_date < start || rec.created_date > end) {
                continue;
            }
            local_counts[rec.borough_id]++;
        }

        #pragma omp critical
        {
            for (const auto &[borough_id, complaint_count] : local_counts) {
                counts[borough_id] += complaint_count;
            }
        }
    }

    return counts;
}

// agency_id -> complaint_count in [start, end]
// Parallel mechanism:
// 1) Each thread counts agency totals in a local map.
// 2) Merge local maps in one critical section.
std::unordered_map<uint16_t, std::size_t>
QueryOMP::get_agency_counts_in_created_date_range(int64_t start, int64_t end) const {
    const int64_t n = static_cast<int64_t>(records_.size());
    std::unordered_map<uint16_t, std::size_t> counts;

    #pragma omp parallel
    {
        std::unordered_map<uint16_t, std::size_t> local_counts;

        #pragma omp for nowait
        for (int64_t i = 0; i < n; ++i) {
            const auto &rec = records_[i];
            if (rec.created_date < start || rec.created_date > end) {
                continue;
            }
            local_counts[rec.agency_id]++;
        }

        #pragma omp critical
        {
            for (const auto &[agency_id, complaint_count] : local_counts) {
                counts[agency_id] += complaint_count;
            }
        }
    }

    return counts;
}

// For one complaint type in [start, end], return zip_code -> complaint_count
// Parallel mechanism:
// 1) Each thread counts zip totals in a local map.
// 2) Merge local maps in one critical section.
std::unordered_map<uint32_t, std::size_t>
QueryOMP::get_zipcode_counts_by_complaint_in_created_date_range(uint32_t complaint_id, int64_t start, int64_t end) const {
    const int64_t n = static_cast<int64_t>(records_.size());
    std::unordered_map<uint32_t, std::size_t> counts;

    #pragma omp parallel
    {
        std::unordered_map<uint32_t, std::size_t> local_counts;

        #pragma omp for nowait
        for (int64_t i = 0; i < n; ++i) {
            const auto &rec = records_[i];
            if (rec.problem_id != complaint_id) {
                continue;
            }
            if (rec.created_date < start || rec.created_date > end) {
                continue;
            }
            local_counts[rec.zip_code]++;
        }

        #pragma omp critical
        {
            for (const auto &[zip_code, complaint_count] : local_counts) {
                counts[zip_code] += complaint_count;
            }
        }
    }

    return counts;
}

// For one complaint type in [start, end], return (lat,long) -> complaint_count
// Parallel mechanism:
// 1) Each thread counts (lat,long) totals in a local map.
// 2) Merge local maps in one critical section.
std::map<std::pair<float, float>, std::size_t>
QueryOMP::get_latlong_counts_by_complaint_in_created_date_range(uint32_t complaint_id, int64_t start, int64_t end) const {
    const int64_t n = static_cast<int64_t>(records_.size());
    std::map<std::pair<float, float>, std::size_t> counts;

    #pragma omp parallel
    {
        std::map<std::pair<float, float>, std::size_t> local_counts;

        #pragma omp for nowait
        for (int64_t i = 0; i < n; ++i) {
            const auto &rec = records_[i];
            if (rec.problem_id != complaint_id) {
                continue;
            }
            if (rec.created_date < start || rec.created_date > end) {
                continue;
            }
            local_counts[{rec.latitude, rec.longitude}]++;
        }

        #pragma omp critical
        {
            for (const auto &[latlong, complaint_count] : local_counts) {
                counts[latlong] += complaint_count;
            }
        }
    }

    return counts;
}
