#include "dataset.hpp"
#include "csv_parser.hpp"
#include "dataset_utils.hpp"

#include <utility>
/**
 * Malformed handeled rules:
 * 1. skip whole row if the number of fields is not correct or Unique Key is invalid (not a valid uint32_t).
 * 2. For other fields, if the value is invalid, we will fill in a default value (e.g., 0 for zip code, 0.0f for latitude/longitude, 0 for created_date/closed_date)
 * 3. Global hard-fail access: Missing required columns when read header.
 */



/** Load the whole csv.
 * 1. Call CSV parser to read the file line by line.
 * 2. For each line, parse the fields and populate a Record struct.
 * 3. Store the Record in the records_ vector.
 * 4. Encode in dictionary for categorical columns (agency_id, problem_id, status_id, borough_id).
 */
bool Dataset::load_csv(
    const std::string& path,
    const std::string& agency_dict_path,
    const std::string& borough_dict_path,
    const std::string& status_dict_path) {
    records_.clear();
    agency_dict_.clear();
    problem_dict_.clear();
    status_dict_.clear();
    borough_dict_.clear();

    const bool use_predefined_agency = !agency_dict_path.empty();
    const bool use_predefined_borough = !borough_dict_path.empty();
    const bool use_predefined_status = !status_dict_path.empty();

    if (use_predefined_agency &&
        !dataset_utils::load_predefined_ids<uint16_t>(
            agency_dict_path, agency_dict_, "Agency")) {
        return false;
    }
    if (use_predefined_borough &&
        !dataset_utils::load_predefined_ids<uint8_t>(
            borough_dict_path, borough_dict_, "Borough")) {
        return false;
    }
    if (use_predefined_status &&
        !dataset_utils::load_predefined_ids<uint8_t>(
            status_dict_path, status_dict_, "Status")) {
        return false;
    }

    CSVParser parser(path); // already read the header in the constructor of CSVParser. getline will start from row 1 (the first data row).
    const auto& header = parser.header();
    if (header.empty()) {
        std::cerr << "Failed to read header from CSV file: " << path << std::endl;
        return false; // Failed to read header  
    }

    const int idx_unique_key = dataset_utils::find_column_idx(header, "Unique Key");
    const int idx_created = dataset_utils::find_column_idx(header, "Created Date");
    const int idx_closed = dataset_utils::find_column_idx(header, "Closed Date");
    const int idx_agency = dataset_utils::find_column_idx(header, "Agency"); // we don't use full name here cause it is too long. Agency is acronym for agency.: e,g,, ALC
    const int idx_problem = dataset_utils::find_column_idx(header, "Problem (formerly Complaint Type)");
    const int idx_status = dataset_utils::find_column_idx(header, "Status");
    const int idx_borough = dataset_utils::find_column_idx(header, "Borough");
    const int idx_zip = dataset_utils::find_column_idx(header, "Incident Zip");
    const int idx_latitude = dataset_utils::find_column_idx(header, "Latitude");
    const int idx_longitude = dataset_utils::find_column_idx(header, "Longitude");

    if (idx_unique_key < 0 || idx_created < 0 || idx_closed < 0 || idx_agency < 0 ||
        idx_problem < 0 || idx_status < 0 || idx_borough < 0 || idx_zip < 0 ||
        idx_latitude < 0 || idx_longitude < 0) {
        // Missing required columns
        for (const auto& col_name : {"Unique Key", "Created Date", "Closed Date", "Agency", "Problem (formerly Complaint Type)", "Status", "Borough", "Incident Zip", "Latitude", "Longitude"}) {
            if (dataset_utils::find_column_idx(header, col_name) < 0) {
                std::cerr << "Missing required column in header: " << col_name << std::endl;
            }
        }
        return false;
    }

    int line_num = 0; // will start from 1 to account for header
    std::vector<std::string> row;
    while (parser.read_row(row)) {
        line_num++;
        if (row.size() != header.size()) {
            std::cerr << "Skipping malformed row: line " << line_num << " with incorrect number of fields: " 
            << row.size() << " (expected " << header.size() << ")" << std::endl;
            continue;
        }

        Record record = {};
        if (!dataset_utils::parse_uint32(row[idx_unique_key], record.id)) {
            std::cerr << "Skipping row with invalid Unique Key at line " << line_num << ": " << row[idx_unique_key] << std::endl;
            continue;
        }
        // fill unrecognized zipcode by 0
        uint32_t zipcode = 0;
        if (dataset_utils::parse_uint32(row[idx_zip], zipcode)) {
            record.zip_code = zipcode;
        } else {
            record.zip_code = 0; // Use 0 to represent missing or invalid zip code
        }

        // fill unrecognized latitude and longitude by 0.0f
        float latitude = 0.0f;
        if (dataset_utils::parse_float(row[idx_latitude], latitude)) {
            record.latitude = latitude;
        } else {
            record.latitude = 0.0f; // Use 0.0f to represent missing or invalid latitude
        }

        float longitude = 0.0f;
        if (dataset_utils::parse_float(row[idx_longitude], longitude)) {
            record.longitude = longitude;
        } else {
            record.longitude = 0.0f; // Use 0.0f to represent missing or invalid longitude
        }

        record.created_date = dataset_utils::parse_datetime(row[idx_created]);
        record.closed_date = dataset_utils::parse_datetime(row[idx_closed]);
        // check whether any field of this record already encoded into the dictionary, if not, we will add it to the dictionary. 
        // If the encoding fails (e.g., exceed the limit of uint type), we will roll back any changes to the dictionary 
        // and skip this row to keep the dictionary consistent with records_.
        const std::string agency_value =
            row[idx_agency].empty() ? "UNKNOWN" : row[idx_agency];
        const std::string problem_value =
            row[idx_problem].empty() ? "UNKNOWN" : row[idx_problem];
        const std::string status_value =
            row[idx_status].empty() ? "UNKNOWN" : row[idx_status];
        const std::string borough_value =
            row[idx_borough].empty() ? "UNKNOWN" : row[idx_borough];

        const bool agency_existed = (agency_dict_.find(agency_value) != agency_dict_.end());
        const bool problem_existed = (problem_dict_.find(problem_value) != problem_dict_.end());
        const bool status_existed = (status_dict_.find(status_value) != status_dict_.end());
        const bool borough_existed = (borough_dict_.find(borough_value) != borough_dict_.end());

        try {
            // encode_id<> : encode new {k, v} into dict
            record.agency_id = dataset_utils::lookup_or_encode_id<uint16_t>(
                agency_dict_, agency_value, use_predefined_agency, "Agency");
            record.problem_id = dataset_utils::encode_id<uint32_t>(problem_dict_, problem_value);
            record.status_id = dataset_utils::lookup_or_encode_id<uint8_t>(
                status_dict_, status_value, use_predefined_status, "Status");
            record.borough_id = dataset_utils::lookup_or_encode_id<uint8_t>(
                borough_dict_, borough_value, use_predefined_borough, "Borough");
        } catch (const std::runtime_error& e) {
            // Roll back any values introduced by this row so dictionaries stay
            // consistent with records_ when we skip malformed rows.
            if (!use_predefined_agency && !agency_existed) { // agency_existed == false: no dict entry for this idx existed before. 
                agency_dict_.erase(agency_value);
            }
            if (!problem_existed) {
                problem_dict_.erase(problem_value);
            }
            if (!use_predefined_status && !status_existed) {
                status_dict_.erase(status_value);
            }
            if (!use_predefined_borough && !borough_existed) {
                borough_dict_.erase(borough_value);
            }
            std::cerr << "Encoding error at line " << line_num << ": " << e.what()
                      << ". Skipping row." << std::endl;
            continue;
        }

        records_.push_back(record);
    }
    return !records_.empty();
}

std::size_t Dataset::erase_records_by_indices(
    const std::vector<std::size_t>& indices) {
    if (indices.empty()) {
        return 0;
    }

    std::vector<std::uint8_t> delete_mask(records_.size(), 0);
    std::size_t removed_count = 0;

    for (const std::size_t index : indices) {
        if (index >= records_.size()) {
            throw std::out_of_range("erase_records_by_indices index out of range");
        }
        if (delete_mask[index] != 0) {
            continue;
        }
        delete_mask[index] = 1;
        removed_count += 1;
    }

    std::size_t write_index = 0;
    for (std::size_t read_index = 0; read_index < records_.size(); ++read_index) {
        if (delete_mask[read_index] != 0) {
            continue;
        }
        if (write_index != read_index) {
            records_[write_index] = std::move(records_[read_index]);
        }
        write_index += 1;
    }

    records_.resize(write_index);
    return removed_count;
}

      
