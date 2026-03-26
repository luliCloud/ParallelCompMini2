#include "query_base.hpp"

#include <algorithm>
#include <queue>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>


QueryBase::QueryBase(const std::vector<Record>& records) : records_(records) {}

/** Get record by ID. Return nullptr if not found. */
const Record* QueryBase::get_record_by_id(uint32_t id) const {
    for (const auto& rec : records_) {
        if (rec.id == id) {
            return &rec;
        }
    }
    return nullptr;
}

/** Get records for a specific agency.
 * Return: a vector of records matching the agency_id. If no records match, return an empty vector.
 */
std::vector<Record> QueryBase::get_records_by_agency(uint16_t agency_id) const {
    std::vector<Record> matched_records;
    matched_records.reserve(records_.size());
    for (const auto& rec : records_) {
        if (rec.agency_id == agency_id) {
            matched_records.push_back(rec);
        }
    }
    return matched_records;
}

/** Get records for a specific status. */
std::vector<Record> QueryBase::get_records_by_status(uint8_t status_id) const {
    std::vector<Record> matched_records;
    matched_records.reserve(records_.size());
    for (const auto& rec : records_) {
        if (rec.status_id == status_id) {
            matched_records.push_back(rec);
        }
    }
    return matched_records;
}

/** Get records for a specific borough. */
std::vector<Record> QueryBase::get_records_by_borough(uint8_t borough_id) const {
    std::vector<Record> matched_records;
    matched_records.reserve(records_.size());
    for (const auto& rec : records_) {
        if (rec.borough_id == borough_id) {
            matched_records.push_back(rec);
        }
    }
    return matched_records;
}

/** Get records created in the date range [start, end]. */
std::vector<Record> QueryBase::get_records_in_created_date_range(int64_t start, int64_t end) const {
    std::vector<Record> matched_records;
    matched_records.reserve(records_.size());
    for (const auto& rec : records_) {
        if (rec.created_date >= start && rec.created_date <= end) {
            matched_records.push_back(rec);
        }
    }
    return matched_records;
}

/** Get records for a specific agency created in the date range [start, end]. */
std::vector<Record> QueryBase::get_records_by_agency_and_created_date_range(
    uint16_t agency_id, int64_t start, int64_t end) const {
    std::vector<Record> matched_records;
    matched_records.reserve(records_.size());
    for (const auto& rec : records_) {
        if (rec.agency_id == agency_id &&
            rec.created_date >= start &&
            rec.created_date <= end) {
            matched_records.push_back(rec);
        }
    }
    return matched_records;
}

/** Get records for a specific status created in the date range [start, end]. */
std::vector<Record> QueryBase::get_records_by_status_and_created_date_range(
    uint8_t status_id, int64_t start, int64_t end) const {
    std::vector<Record> matched_records;
    matched_records.reserve(records_.size());
    for (const auto& rec : records_) {
        if (rec.status_id == status_id &&
            rec.created_date >= start &&
            rec.created_date <= end) {
            matched_records.push_back(rec);
        }
    }
    return matched_records;
}

/** Count records created in the date range [start, end]. */
size_t QueryBase::count_in_created_date_range(int64_t start, int64_t end) const {
    size_t count = 0;
    for (const auto& rec : records_) {
        if (rec.created_date >= start && rec.created_date <= end) {
            count++;
        }
    }
    return count;
}

/** Count records for a specific agency created in the date range [start, end]. */
size_t QueryBase::count_by_agency_and_created_date_range(uint16_t agency_id, int64_t start, int64_t end) const {
    size_t count = 0;
    for (const auto& rec : records_) {
        if (rec.agency_id == agency_id &&
            rec.created_date >= start &&
            rec.created_date <= end) {
            count++;
        }
    }
    return count;
}

/** Count records for a specific status created in the date range [start, end]. */
size_t QueryBase::count_by_status_and_created_date_range(uint8_t status_id, int64_t start, int64_t end) const {
    size_t count = 0;
    for (const auto& rec : records_) {
        if (rec.status_id == status_id &&
            rec.created_date >= start &&
            rec.created_date <= end) {
            count++;
        }
    }
    return count;
}

/** Count resolved complaints closed in the date range [start, end]. */
size_t QueryBase::count_resolved_complaints_by_closed_date_range(const uint8_t closed_id, const int64_t start, const int64_t end) const {
    size_t count = 0;
    if (start <= 0 || end <= 0 || end < start) throw std::invalid_argument("start and end must greater than 0 and start <= end");

    for (const auto& rec : records_) {
        if (rec.status_id == closed_id &&
            rec.closed_date >= start &&
            rec.closed_date <= end) {
            count++;
        }
    }
    return count;
}

/**
 * Aggregate borough-level counts in created_date range [start, end].
 * Returns: borough_id -> (complaint_count, unique_agency_count).
 */
std::unordered_map<uint8_t, std::tuple<std::size_t, std::size_t>>
QueryBase::get_borough_counts_in_created_date_range(int64_t start, int64_t end) const {
    // borough_id -> (complaint_count, unique_agency_count)
    std::unordered_map<uint8_t, std::tuple<std::size_t, std::size_t>> summary;
    // borough_id -> set of unique agency_id seen in the time range
    std::unordered_map<uint8_t, std::unordered_set<uint16_t>> agencies_by_borough;

    for (const auto& rec : records_) {
        if (rec.created_date < start || rec.created_date > end) {
            continue;
        }
        auto& counts = summary[rec.borough_id]; // get current {complain number, agency num} for this borough
        std::get<0>(counts)++; // complain num++

        auto& agencies = agencies_by_borough[rec.borough_id]; // get curr set of agency in this borough
        if (agencies.insert(rec.agency_id).second) { // if not exist in curr set, then increase agnecy number in corresponding borough
            std::get<1>(counts)++;
        }
    }

    return summary;
}

/**
 * Aggregate counts for one borough in created_date range [start, end].
 * Returns: (complaint_count, unique_agency_count).
 */
std::tuple<std::size_t, std::size_t>
QueryBase::get_borough_counts(uint8_t borough_id, int64_t start, int64_t end) const {
    // (complaint_count, unique_agency_count) for the requested borough
    std::tuple<std::size_t, std::size_t> counts = {0, 0};
    // Track unique agency_id for this borough in [start, end]
    std::unordered_set<uint16_t> agencies;

    for (const auto& rec : records_) {
        if (rec.borough_id != borough_id) {
            continue;
        }
        if (rec.created_date < start || rec.created_date > end) {
            continue;
        }
        std::get<0>(counts)++;
        agencies.insert(rec.agency_id); // establish set of agencies separately
    }

    std::get<1>(counts) = agencies.size();
    return counts;
}

/**
 * Aggregate complaint type counts in created_date range [start, end].
 * Returns: complaint_id -> complaint_count.
 */
std::unordered_map<uint32_t, std::size_t>
QueryBase::get_complaint_counts_in_created_date_range(int64_t start, int64_t end) const {
    std::unordered_map<uint32_t, std::size_t> counts;
    for (const auto& rec : records_) {
        if (rec.created_date < start || rec.created_date > end) {
            continue;
        }
        counts[rec.problem_id]++;
    }

    return counts;
}

/**
 * Return top-k complaint types by count in created_date range [start, end].
 * Returns vector of tuples: (complaint_id, complaint_count), sorted desc by count.
 * TODO: should we do a summary to see the top 3 complain in NYC, not by borough?
 */
std::vector<std::tuple<uint32_t, std::size_t>>
QueryBase::get_top_complaints_in_created_date_range(int64_t start, int64_t end, std::size_t top_k) const {
    const auto counts = get_complaint_counts_in_created_date_range(start, end); // get result from this func

    // (count, complaint_id) so default max-heap pops largest count first.
    using HeapItem = std::pair<std::size_t, uint32_t>;
    std::priority_queue<HeapItem> heap; // will auto-sort based on count

    for (const auto& [complaint_id, count] : counts) {
        heap.emplace(count, complaint_id);
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

/**
 * Aggregate borough counts for one complaint type in created_date range [start, end].
 * Returns: borough_id -> complaint_count.
 * TODO: I will use this for vectorization. Know borough and easy to set array for atomic writing by thread, then global area.
 */
std::unordered_map<uint8_t, std::size_t>
QueryBase::get_borough_counts_by_complaint_in_created_date_range(uint32_t complaint_id, int64_t start, int64_t end) const {
    std::unordered_map<uint8_t, std::size_t> counts;
    for (const auto& rec : records_) {
        if (rec.problem_id != complaint_id) {
            continue;
        }
        if (rec.created_date < start || rec.created_date > end) {
            continue;
        }
        counts[rec.borough_id]++;
    }
    return counts;
}

/**
 * Aggregate agency counts in created_date range [start, end].
 * Returns: agency_id -> complaint_count.
 */
std::unordered_map<uint16_t, std::size_t>
QueryBase::get_agency_counts_in_created_date_range(int64_t start, int64_t end) const {
    std::unordered_map<uint16_t, std::size_t> counts;
    for (const auto& rec : records_) {
        if (rec.created_date < start || rec.created_date > end) {
            continue;
        }
        counts[rec.agency_id]++;
    }
    return counts;
}

/**
 * Aggregate zipcode counts for one complaint type in created_date range [start, end].
 * Returns: zip_code -> complaint_count.
 */
std::unordered_map<uint32_t, std::size_t>
QueryBase::get_zipcode_counts_by_complaint_in_created_date_range(uint32_t complaint_id, int64_t start, int64_t end) const {
    std::unordered_map<uint32_t, std::size_t> counts;
    for (const auto& rec : records_) {
        if (rec.problem_id != complaint_id) {
            continue;
        }
        if (rec.created_date < start || rec.created_date > end) {
            continue;
        }
        counts[rec.zip_code]++;
    }
    return counts;
}

/**
 * Aggregate latitude/longitude counts for one complaint type in created_date range [start, end].
 * Returns: (latitude, longitude) -> complaint_count.
 */
std::map<std::pair<float, float>, std::size_t>
QueryBase::get_latlong_counts_by_complaint_in_created_date_range(uint32_t complaint_id, int64_t start, int64_t end) const {
    std::map<std::pair<float, float>, std::size_t> counts;
    for (const auto& rec : records_) {
        if (rec.problem_id != complaint_id) {
            continue;
        }
        if (rec.created_date < start || rec.created_date > end) {
            continue;
        }
        counts[{rec.latitude, rec.longitude}]++; // every pair of {long, lat} will be a unique key
    }
    return counts;
}
