/** Abstraction class for query, phase 1,2 3 */
#pragma once
#include "dataset.hpp"
#include <cstdint>
#include <map>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

/**** Query class provides an API for searching and filtering the dataset. ****/
class IQuery {
public:
    virtual ~IQuery() = default;
    // virtual will allow the runtime to determine which function to call based on the actual type of the object, enabling polymorphism.
    virtual const Record* get_record_by_id(uint32_t id) const = 0; // = 0 means this is a pure virtual function, making IQuery an abstract class that cannot be instantiated directly. Derived classes must provide an implementation for this function.
    virtual std::vector<Record> get_records_by_agency(uint16_t agency_id) const = 0;
    virtual std::vector<Record> get_records_by_status(uint8_t status_id) const = 0;
    virtual std::vector<Record> get_records_by_borough(uint8_t borough_id) const = 0;
    virtual std::vector<Record> get_records_in_created_date_range(int64_t start, int64_t end) const = 0;
    virtual std::vector<Record> get_records_by_agency_and_created_date_range(uint16_t agency_id, int64_t start, int64_t end) const = 0;
    virtual std::vector<Record> get_records_by_status_and_created_date_range(uint8_t status_id, int64_t start, int64_t end) const = 0;

    // how many complaints were created in the date range [start, end]?
    virtual size_t count_in_created_date_range(int64_t start, int64_t end) const = 0;

    // how many complaints for a specific agency were created in the date range [start, end]? Compare all agency?
    virtual size_t count_by_agency_and_created_date_range(uint16_t agency_id, int64_t start, int64_t end) const = 0;

    // how many complaints for a specific status were created in the date range [start, end]
    virtual size_t count_by_status_and_created_date_range(uint8_t status_id, int64_t start, int64_t end) const = 0;

    // how many resolved complaints were closed in the date range [start, end]? The efficiency of every city/borough?
    virtual size_t count_resolved_complaints_by_closed_date_range(const uint8_t closed_id, const int64_t start, const int64_t end) const = 0;

    virtual std::unordered_map<uint8_t, std::tuple<std::size_t, std::size_t>>
    get_borough_counts_in_created_date_range(int64_t start, int64_t end) const = 0;

    virtual std::tuple<std::size_t, std::size_t>
    get_borough_counts(uint8_t borough_id, int64_t start, int64_t end) const = 0;

    virtual std::unordered_map<uint32_t, std::size_t>
    get_complaint_counts_in_created_date_range(int64_t start, int64_t end) const = 0;

    virtual std::vector<std::tuple<uint32_t, std::size_t>>
    get_top_complaints_in_created_date_range(int64_t start, int64_t end, std::size_t top_k = 3) const = 0;

    virtual std::unordered_map<uint8_t, std::size_t>
    get_borough_counts_by_complaint_in_created_date_range(uint32_t complaint_id, int64_t start, int64_t end) const = 0;

    virtual std::unordered_map<uint16_t, std::size_t>
    get_agency_counts_in_created_date_range(int64_t start, int64_t end) const = 0;

    virtual std::unordered_map<uint32_t, std::size_t>
    get_zipcode_counts_by_complaint_in_created_date_range(uint32_t complaint_id, int64_t start, int64_t end) const = 0;

    virtual std::map<std::pair<float, float>, std::size_t>
    get_latlong_counts_by_complaint_in_created_date_range(uint32_t complaint_id, int64_t start, int64_t end) const = 0;
};
