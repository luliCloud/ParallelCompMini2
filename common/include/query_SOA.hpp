#pragma once
#include "dataset_SOA.hpp"

#include <cstdint>
#include <cstddef>
#include <unordered_map>
#include <vector>

// we can't use interface from query_base. Cause we cannot initiate the records_. We will have complete different start point of vectors
class QuerySOA {
public:
    explicit QuerySOA(const DatasetSOA& ds) : ds_(ds) {}

    size_t count_in_created_date_range(int64_t start, int64_t end) const;
    size_t count_resolved_complaints_by_closed_date_range(
        uint8_t closed_id, int64_t start, int64_t end) const;

    size_t count_by_agency_and_created_date_range(uint16_t agency_id, int64_t start, int64_t end) const;
    size_t count_by_status_and_created_date_range(uint8_t status_id, int64_t start, int64_t end) const;

    /** TODO: borough need further extension */
    std::unordered_map<uint8_t, std::size_t> get_borough_counts_by_complaint_in_created_date_range(uint32_t complaint_id, int64_t start, int64_t end) const;
    std::unordered_map<uint32_t, std::uint64_t> get_complaint_counts_in_created_date_range(int64_t start, int64_t end) const;
private: 
    DatasetSOA ds_;
};
