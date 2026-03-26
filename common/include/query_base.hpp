#pragma once
#include "iQuery.hpp"

#include <cstdint>
#include <map>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

/**
 * QueryBase provides the serial baseline implementation for IQuery.
 */
class QueryBase : public IQuery
{
public:
    explicit QueryBase(const std::vector<Record> &records);
    // Implementations of the virtual functions declared in IQuery
    const Record *get_record_by_id(uint32_t id) const override;
    std::vector<Record> get_records_by_agency(uint16_t agency_id) const override;
    std::vector<Record> get_records_by_status(uint8_t status_id) const override;
    std::vector<Record> get_records_by_borough(uint8_t borough_id) const override;
    std::vector<Record> get_records_in_created_date_range(int64_t start, int64_t end) const override;
    std::vector<Record> get_records_by_agency_and_created_date_range(uint16_t agency_id, int64_t start, int64_t end) const override;
    std::vector<Record> get_records_by_status_and_created_date_range(uint8_t status_id, int64_t start, int64_t end) const override;
    /** TODO: More dimensions: compare five boroughs efficiency*/
    
    // how many complaints were created in the date range [start, end]?
    size_t count_in_created_date_range(int64_t start, int64_t end) const override;

    // how many complaints for a specific agency were created in the date range [start, end]? Compare all agency?
    size_t count_by_agency_and_created_date_range(uint16_t agency_id, int64_t start, int64_t end) const override;

    // how many complaints for a specific status were created in the date range [start, end]? may be status do categorical is better.
    /** We can start here for the first categorical paralleled trial */
    size_t count_by_status_and_created_date_range(uint8_t status_id, int64_t start, int64_t end) const override;

    // how many resolved complaints were closed in the date range [start, end]? The efficiency of every city/borough?
    size_t count_resolved_complaints_by_closed_date_range(uint8_t closed_id, int64_t start, int64_t end) const override;

    // how many complain in each borough, how many relevant agencies involved in a given date range
    // For example, 1 -> (20, 4) indicating borough 1 has 20 complaints, from 4 agencies.
    std::unordered_map<uint8_t, std::tuple<std::size_t, std::size_t>> get_borough_counts_in_created_date_range(int64_t start, int64_t end) const override;

    // single borough in the above case. Only check given borough
    std::tuple<std::size_t, std::size_t> get_borough_counts(uint8_t borough_id, int64_t start, int64_t end) const override;

    // is this one same as size_t count_in_created_date_range(int64_t start, int64_t end). No
    // This return the count number of each complain in a given date range. 
    std::unordered_map<uint32_t, std::size_t> get_complaint_counts_in_created_date_range(int64_t start, int64_t end) const override;

    // Return the top 3 comlain type in a given date range. Call the above function
    std::vector<std::tuple<uint32_t, std::size_t>> get_top_complaints_in_created_date_range(int64_t start, int64_t end, std::size_t top_k = 3) const override;

    // Fix a complain type, and date range. How many cases happened in each borough.
    /** TODO: this could be interesting */
    std::unordered_map<uint8_t, std::size_t> get_borough_counts_by_complaint_in_created_date_range(uint32_t complaint_id, int64_t start, int64_t end) const override;

    // How many number were handeled by each agency in a given time range. Diff from count_by_agency_and_created_date_range, which only have one agency
    std::unordered_map<uint16_t, std::size_t> get_agency_counts_in_created_date_range(int64_t start, int64_t end) const override;

    // How many case in each zip code, in a given date range and complain type
    /** TODO: interesting practically */
    std::unordered_map<uint32_t, std::size_t> get_zipcode_counts_by_complaint_in_created_date_range(uint32_t complaint_id, int64_t start, int64_t end) const override;

    // Fix a complaint type, how many appear in each long/lat.  start and end are still time range. The result will returned as long/lat. 
    /** TODO: interesting to have map hot spot analysis */
    std::map<std::pair<float, float>, std::size_t> get_latlong_counts_by_complaint_in_created_date_range(uint32_t complaint_id, int64_t start, int64_t end) const override;

protected:
    const std::vector<Record> &records_;
};
