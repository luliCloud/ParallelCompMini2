#include "query_SOA.hpp"
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>

#ifdef _OPENMP
#include <omp.h>
#endif

std::size_t QuerySOA::count_in_created_date_range(int64_t start, int64_t end) const {
    if (start <= 0 || end <= 0 || end < start) {
        throw std::invalid_argument("start and end must be > 0 and start <= end");
    }
    const auto& created_date = ds_.created_date(); 
    const std::size_t n = created_date.size();

    std::size_t count = 0;

    #pragma omp parallel for simd reduction(+:count) // simd, let one core do batch comparison
    // i.e., outer layer, thread distribution. inter layer. thread vectorization simd
    /** Reason for adding simd: now we store created date in continuous memory
     * Every time it doesn load a record but a batch of (e.g. 32) created_date data. It is good for 
     * omp to scan 24, compare 24, and calcualte local total count. Adding to global count.
     */
    for (std::size_t i = 0; i < n; ++i) {
        // no branch. Better for simd.
        count += (created_date[i] >= start && created_date[i] <= end) ? 1 : 0;
    }

    return count;
}

std::size_t QuerySOA::count_by_agency_and_created_date_range(
    uint16_t agency_id, int64_t start, int64_t end) const {
    if (start <= 0 || end <= 0 || end < start) {
        throw std::invalid_argument("start and end must be > 0 and start <= end");
    }

    const auto& created_date = ds_.created_date();
    const auto& agency = ds_.agency_id();
    const std::size_t n = created_date.size();

    std::size_t count = 0;

    #pragma omp parallel for simd reduction(+:count)
    for (std::size_t i = 0; i < n; ++i) {
        count += (agency[i] == agency_id &&
                  created_date[i] >= start &&
                  created_date[i] <= end) ? 1 : 0;
    }

    return count;
}

std::size_t QuerySOA::count_by_status_and_created_date_range(
    uint8_t status_id, int64_t start, int64_t end) const {
    if (start <= 0 || end <= 0 || end < start) {
        throw std::invalid_argument("start and end must be > 0 and start <= end");
    }

    const auto& created_date = ds_.created_date();
    const auto& status = ds_.status_id();
    const std::size_t n = created_date.size();

    std::size_t count = 0;

    #pragma omp parallel for simd reduction(+:count)
    for (std::size_t i = 0; i < n; ++i) {
        count += (status[i] == status_id &&
                  created_date[i] >= start &&
                  created_date[i] <= end) ? 1 : 0;
    }

    return count;
}

std::unordered_map<uint32_t, std::uint64_t>
QuerySOA::get_borough_summary_counts_in_created_date_range(int64_t start, int64_t end) const {
    if (start <= 0 || end <= 0 || end < start) {
        throw std::invalid_argument("start and end must be > 0 and start <= end");
    }

    const auto& created_date = ds_.created_date();
    const auto& borough_id = ds_.borough_id();
    const std::size_t n = created_date.size();

    std::unordered_map<uint32_t, std::uint64_t> counts;

    #ifdef _OPENMP
    const int max_threads = omp_get_max_threads();
    std::vector<std::unordered_map<uint32_t, std::uint64_t>> local_counts(
        static_cast<std::size_t>(max_threads));

    #pragma omp parallel
    {
        const int tid = omp_get_thread_num();
        auto& local = local_counts[static_cast<std::size_t>(tid)];

        #pragma omp for nowait
        for (std::size_t i = 0; i < n; ++i) {
            if (created_date[i] >= start && created_date[i] <= end) {
                local[static_cast<uint32_t>(borough_id[i])]++;
            }
        }
    }

    for (const auto& local : local_counts) {
        for (const auto& [key, value] : local) {
            counts[key] += value;
        }
    }
    #else
    for (std::size_t i = 0; i < n; ++i) {
        if (created_date[i] >= start && created_date[i] <= end) {
            counts[static_cast<uint32_t>(borough_id[i])]++;
        }
    }
    #endif

    return counts;
}

std::unordered_map<uint32_t, std::uint64_t>
QuerySOA::get_complaint_counts_in_created_date_range(int64_t start, int64_t end) const {
    if (start <= 0 || end <= 0 || end < start) {
        throw std::invalid_argument("start and end must be > 0 and start <= end");
    }

    const auto& created_date = ds_.created_date();
    const auto& problem_id = ds_.problem_id();
    const std::size_t n = created_date.size();

    std::unordered_map<uint32_t, std::uint64_t> counts;

    #ifdef _OPENMP
    const int max_threads = omp_get_max_threads();
    std::vector<std::unordered_map<uint32_t, std::uint64_t>> local_counts(
        static_cast<std::size_t>(max_threads));

    #pragma omp parallel
    {
        const int tid = omp_get_thread_num();
        auto& local = local_counts[static_cast<std::size_t>(tid)];

        #pragma omp for nowait
        for (std::size_t i = 0; i < n; ++i) {
            if (created_date[i] >= start && created_date[i] <= end) {
                local[problem_id[i]]++;
            }
        }
    }

    for (const auto& local : local_counts) {
        for (const auto& [key, value] : local) {
            counts[key] += value;
        }
    }
    #else
    for (std::size_t i = 0; i < n; ++i) {
        if (created_date[i] >= start && created_date[i] <= end) {
            counts[problem_id[i]]++;
        }
    }
    #endif

    return counts;
}
