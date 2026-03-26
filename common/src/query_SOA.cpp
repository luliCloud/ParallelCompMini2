#include "query_SOA.hpp"
#include <cstddef>
#include <cstdint>

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
