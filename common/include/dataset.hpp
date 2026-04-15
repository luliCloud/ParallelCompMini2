/** This is the header file for the dataset class and Generate row-based record 
 * TODO: Malformed record will be skipped for Phase 1. 
 * But we need to handle the error filed later by still extracting needed info from this row.
 * TODO: Will do pre cleaning for phase 3
*/
#pragma once
#include <cstdint>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>
struct Record {
    uint32_t id;

    int64_t created_date;
    int64_t closed_date;

    uint16_t agency_id; // "NYPD" -> 0, "DOT"  -> 1, "DSNY" -> 2
    uint32_t problem_id;

    uint8_t status_id;
    uint8_t borough_id;

    uint32_t zip_code;

    float latitude;
    float longitude;
};

class Dataset {
public:
    Dataset() = default;
    ~Dataset() = default;

    bool load_csv(
        const std::string& path,
        const std::string& agency_dict_path = "",
        const std::string& borough_dict_path = "",
        const std::string& status_dict_path = ""); // call CSV parser and populate records_
    size_t size() const { return records_.size(); }

    const std::vector<Record>& get_records() const { return records_; }

    /** Retrieves the encoded ID for a specific attribute value.*/
    template <typename UInt>
    UInt get_encoded_attribute_id(const std::string& attribute_name, const std::string& value) const {
        if (attribute_name == "Agency") {
            auto it = agency_dict_.find(value);
            if (it != agency_dict_.end()) return static_cast<UInt>(it->second);
        } else if (attribute_name == "Problem") {
            auto it = problem_dict_.find(value);
            if (it != problem_dict_.end()) return static_cast<UInt>(it->second);
        } else if (attribute_name == "Status") {
            auto it = status_dict_.find(value);
            if (it != status_dict_.end()) return static_cast<UInt>(it->second);
        } else if (attribute_name == "Borough") {
            auto it = borough_dict_.find(value);
            if (it != borough_dict_.end()) return static_cast<UInt>(it->second);
        } else {
            throw std::invalid_argument("Invalid attribute name: " + attribute_name);
        }

        throw std::invalid_argument("Non-existent attribute value: " + value + " in " + attribute_name);
    }

private:
    std::vector<Record> records_;

    // Dictionaries for encoding categorical columns. 
    /** Will this be permanent? Yes. We don't set manual clear after CSV load*/
    std::unordered_map<std::string, uint16_t> agency_dict_;
    std::unordered_map<std::string, uint32_t> problem_dict_;
    std::unordered_map<std::string, uint8_t> status_dict_;
    std::unordered_map<std::string, uint8_t> borough_dict_;

    // TODO: 
    // 1. CSV parser
    // 2. Dictionary encoding for categorical columns (agency_id, problem_id, status_id, borough_id)
    // 3. Date parsing for created_date and closed_date (e.g., convert to Unix timestamp)
    // 4. Error handling for malformed rows, missing values, etc.
    // 5. Memory management and performance optimizations (e.g., pre-allocating vector capacity)
};
