/** We design to do cleaning table when loading csv. */
#include "dataset_SOA.hpp"
#include "csv_parser.hpp"
#include "dataset_utils.hpp"

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

bool DatasetSOA::load_csv(const std::string& path) {
    id_.clear();
    created_date_.clear();
    closed_date_.clear();
    agency_id_.clear();
    problem_id_.clear();
    status_id_.clear();
    borough_id_.clear();
    zip_code_.clear();
    latitude_.clear();
    longitude_.clear();

    agency_dict_.clear();
    problem_dict_.clear();
    status_dict_.clear();
    borough_dict_.clear();

    CSVParser parser(path);
    const auto& header = parser.header();
    if (header.empty()) {
        std::cerr << "Failed to read header from CSV file: " << path << std::endl; 
        return false;
    }
    // find which col representing which feature
    const int idx_unique_key = dataset_utils::find_column_idx(header, "Unique Key");
    const int idx_created = dataset_utils::find_column_idx(header, "Created Date");
    const int idx_closed = dataset_utils::find_column_idx(header, "Closed Date");
    const int idx_agency = dataset_utils::find_column_idx(header, "Agency");
    const int idx_problem =
        dataset_utils::find_column_idx(header, "Problem (formerly Complaint Type)");
    const int idx_status = dataset_utils::find_column_idx(header, "Status");
    const int idx_borough = dataset_utils::find_column_idx(header, "Borough");
    const int idx_zip = dataset_utils::find_column_idx(header, "Incident Zip");
    const int idx_latitude = dataset_utils::find_column_idx(header, "Latitude");
    const int idx_longitude = dataset_utils::find_column_idx(header, "Longitude");

    if (idx_unique_key < 0 || idx_created < 0 || idx_closed < 0 || idx_agency < 0 ||
        idx_problem < 0 || idx_status < 0 || idx_borough < 0 || idx_zip < 0 ||
        idx_latitude < 0 || idx_longitude < 0) {
        for (const auto& col_name : {
                 "Unique Key",
                 "Created Date",
                 "Closed Date",
                 "Agency",
                 "Problem (formerly Complaint Type)",
                 "Status",
                 "Borough",
                 "Incident Zip",
                 "Latitude",
                 "Longitude"}) {
            if (dataset_utils::find_column_idx(header, col_name) < 0) {
                std::cerr << "Missing required column in header: " << col_name << std::endl; // print out detailed information
            }
        }
        return false;
    }

    std::vector<std::string> row;
    int line_num = 0;

    while (parser.read_row(row)) {
        
        line_num++;

        if (row.size() != header.size()) {
            std::cerr << "Skipping malformed row: line " << line_num
                      << " with incorrect number of fields: " << row.size()
                      << " (expected " << header.size() << ")" << std::endl;
            continue;
        }

        uint32_t parsed_id = 0;
        if (!dataset_utils::parse_uint32(row[idx_unique_key], parsed_id)) {
            std::cerr << "Skipping row with invalid Unique Key at line "
                      << line_num << ": " << row[idx_unique_key] << std::endl;
            continue;
        }

        const int64_t parsed_created_date =
            dataset_utils::parse_datetime(row[idx_created]);

        if (parsed_created_date == 0) {
            std::cerr << "Skipping row with invalid Created Date at line "
                      << line_num << ": " << row[idx_created] << std::endl;
            continue;
        }

        const int64_t parsed_closed_date =
            dataset_utils::parse_datetime(row[idx_closed]);

        uint32_t parsed_zip_code = 0;
        if (!dataset_utils::parse_uint32(row[idx_zip], parsed_zip_code)) {
            parsed_zip_code = 0;
        }

        float parsed_latitude = 0.0f;
        if (!dataset_utils::parse_float(row[idx_latitude], parsed_latitude)) {
            parsed_latitude = 0.0f;
        }

        float parsed_longitude = 0.0f;
        if (!dataset_utils::parse_float(row[idx_longitude], parsed_longitude)) {
            parsed_longitude = 0.0f;
        }

        const std::string agency_value =
            row[idx_agency].empty() ? "UNKNOWN" : row[idx_agency];
        const std::string problem_value =
            row[idx_problem].empty() ? "UNKNOWN" : row[idx_problem];
        const std::string status_value =
            row[idx_status].empty() ? "UNKNOWN" : row[idx_status];
        const std::string borough_value =
            row[idx_borough].empty() ? "UNKNOWN" : row[idx_borough];

        const bool agency_existed =
            (agency_dict_.find(agency_value) != agency_dict_.end());
        const bool problem_existed =
            (problem_dict_.find(problem_value) != problem_dict_.end());
        const bool status_existed =
            (status_dict_.find(status_value) != status_dict_.end());
        const bool borough_existed =
            (borough_dict_.find(borough_value) != borough_dict_.end());

        uint16_t parsed_agency_id = 0;
        uint32_t parsed_problem_id = 0;
        uint8_t parsed_status_id = 0;
        uint8_t parsed_borough_id = 0;

        try {
            parsed_agency_id =
                dataset_utils::encode_id<uint16_t>(agency_dict_, agency_value);
            parsed_problem_id =
                dataset_utils::encode_id<uint32_t>(problem_dict_, problem_value);
            parsed_status_id =
                dataset_utils::encode_id<uint8_t>(status_dict_, status_value);
            parsed_borough_id =
                dataset_utils::encode_id<uint8_t>(borough_dict_, borough_value);
        } catch (const std::runtime_error& e) {
            if (!agency_existed) {
                agency_dict_.erase(agency_value);
            }
            if (!problem_existed) {
                problem_dict_.erase(problem_value);
            }
            if (!status_existed) {
                status_dict_.erase(status_value);
            }
            if (!borough_existed) {
                borough_dict_.erase(borough_value);
            }

            std::cerr << "Encoding error at line " << line_num << ": "
                      << e.what() << ". Skipping row." << std::endl;
            continue;
        }

        // vectorization here.
        id_.push_back(parsed_id);
        created_date_.push_back(parsed_created_date);
        closed_date_.push_back(parsed_closed_date);
        agency_id_.push_back(parsed_agency_id);
        problem_id_.push_back(parsed_problem_id);
        status_id_.push_back(parsed_status_id);
        borough_id_.push_back(parsed_borough_id);
        zip_code_.push_back(parsed_zip_code);
        latitude_.push_back(parsed_latitude);
        longitude_.push_back(parsed_longitude);
    }
    // ensure all col having same size

    
    return !created_date_.empty() &&
       id_.size() == created_date_.size() &&
       created_date_.size() == closed_date_.size() &&
       closed_date_.size() == agency_id_.size() &&
       agency_id_.size() == problem_id_.size() &&
       problem_id_.size() == status_id_.size() &&
       status_id_.size() == borough_id_.size() &&
       borough_id_.size() == zip_code_.size() &&
       zip_code_.size() == latitude_.size() &&
       latitude_.size() == longitude_.size();

}
