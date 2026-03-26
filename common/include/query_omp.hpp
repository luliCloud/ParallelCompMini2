#pragma once
#include "query_base.hpp"

class QueryOMP : public QueryBase {
public:
    explicit QueryOMP(const std::vector<Record> &records) : QueryBase(records) {}

    size_t count_in_created_date_range(int64_t start, int64_t end) const override;
    size_t count_resolved_complaints_by_closed_date_range(
        uint8_t closed_id, int64_t start, int64_t end) const override;

    size_t count_by_agency_and_created_date_range(uint16_t agency_id, int64_t start, int64_t end) const override;
    size_t count_by_status_and_created_date_range(uint8_t status_id, int64_t start, int64_t end) const override;

    std::unordered_map<uint8_t, std::tuple<std::size_t, std::size_t>> get_borough_counts_in_created_date_range(int64_t start, int64_t end) const override;
    std::tuple<std::size_t, std::size_t> get_borough_counts(uint8_t borough_id, int64_t start, int64_t end) const override;
    std::unordered_map<uint32_t, std::size_t> get_complaint_counts_in_created_date_range(int64_t start, int64_t end) const override;
    std::vector<std::tuple<uint32_t, std::size_t>> get_top_complaints_in_created_date_range(int64_t start, int64_t end, std::size_t top_k = 3) const override;
    std::unordered_map<uint8_t, std::size_t> get_borough_counts_by_complaint_in_created_date_range(uint32_t complaint_id, int64_t start, int64_t end) const override;
    std::unordered_map<uint16_t, std::size_t> get_agency_counts_in_created_date_range(int64_t start, int64_t end) const override;
    std::unordered_map<uint32_t, std::size_t> get_zipcode_counts_by_complaint_in_created_date_range(uint32_t complaint_id, int64_t start, int64_t end) const override;
    std::map<std::pair<float, float>, std::size_t> get_latlong_counts_by_complaint_in_created_date_range(uint32_t complaint_id, int64_t start, int64_t end) const override;

    // TODO: more dimensions for parallel trial
    //size_t count_by_agency_and_status_and_created_date_range(uint16_t agency_id, uint8_t status_id, int64_t start, int64_t end) const override;
    //size_t count_by_agency_and_borough_and_created_date_range(uint16_t agency_id, uint8_t borough_id, int64_t start, int64_t end) const override;
};
