#pragma once

#include <charconv> // for std::from_chars. Convert string to numeric types with error handling. 
#include <cstdint>
#include <fstream>
#include <iostream>
#include <ctime>
#include <iomanip>
#include <limits>
#include <locale>
#include <sstream>
#include <unordered_map>
#include <string>
#include <vector>
#include <cstddef>

#include <stdexcept> // for std::runtime_error when encoding categorical values exceed the limit of uint type.
#include <cstdlib> // for std::strtof to parse float with error handling.

namespace dataset_utils {
    inline std::string trim_copy(const std::string& input) {
        const auto begin = input.find_first_not_of(" \t\r\n");
        if (begin == std::string::npos) {
            return "";
        }
        const auto end = input.find_last_not_of(" \t\r\n");
        return input.substr(begin, end - begin + 1);
    }

    inline std::string unquote_copy(const std::string& input) {
        if (input.size() >= 2 && input.front() == '"' && input.back() == '"') {
            return input.substr(1, input.size() - 2);
        }
        return input;
    }

    /* using filed name as key to find column index */ 
    inline int find_column_idx(const std::vector<std::string>& header, const std::string& name) {
        for (size_t i = 0; i < header.size(); ++i) {
            if (header[i] == name) {
                return static_cast<int>(i); // Found the column index
            }
        }
        return -1;
    }

    /* parse string to uint32_t with error handling. 
    For where should be uint32_t, like id, zip.
     */
    inline bool parse_uint32(const std::string& in, uint32_t& out) {
        if (in.empty()) {
            return false; // Empty string cannot be parsed 
        }
    
        uint64_t tmp = 0; // we will use uint32_t to store the result, but we need a wider type to check for overflow
        const char* begin = in.data();
        const char* end = in.data() + in.size();
        const auto [ptr, ec] = std::from_chars(begin, end, tmp);
        /** errc: encode into number successfully.  */
        if (ec != std::errc() || ptr != end || tmp > std::numeric_limits<uint32_t>::max()) {
            return false; // Parsing failed or overflow occurred
        }
        out = static_cast<uint32_t>(tmp); // encode to uint32_t
        return true;
    }

    /** parse into float for latitude and longitude */
    inline bool parse_float(const std::string& in, float& out) {
        if (in.empty()) {
            return false;
        }

        char* end = nullptr;
        out = std::strtof(in.c_str(), &end);
        if (end == in.c_str() || *end != '\0') {
            return false; // Parsing failed
        }
        return true;
    }

    /** parse datetime string into Unix timestamp (int64_t) */
    inline int64_t parse_datetime(const std::string& in) {
        if (in.empty()) {
            return 0;
        }

        // Example format: "01/01/2020 12:00:00 PM"
        std::tm parsed_time = {};
        std::istringstream iss(in);
        iss.imbue(std::locale::classic());
        iss >> std::get_time(&parsed_time, "%m/%d/%Y %I:%M:%S %p");
        if (iss.fail()) {
            return 0; // Parsing failed
        }
        iss >> std::ws;
        if (!iss.eof()) {
            return 0; // Reject trailing junk
        }
        parsed_time.tm_isdst = -1; // Let mktime determine if DST is in effect
        // Convert to time_t (Unix timestamp). In sec (general), for calculating time difference, we can use int64_t to store the timestamp.
        const std::time_t timestamp = std::mktime(&parsed_time);
        if (timestamp < 0) {
            return 0; // mktime failed
        }
        return static_cast<int64_t>(timestamp);
    }

    /** Convert problem filed into an problem_id (representing this problem type).
    Stored in unordered_map for problem 
    Usage: 
    rec.agency_id  = encode_id<uint16_t>(agency_dict, row[idx_agency]);
    rec.problem_id = encode_id<uint32_t>(problem_dict, row[idx_problem]);
    rec.status_id  = encode_id<uint8_t>(status_dict, row[idx_status]);
    rec.borough_id = encode_id<uint8_t>(borough_dict, row[idx_borough]);*/
    template <typename UInt, typename Dict>
    inline UInt encode_id(Dict& dict, const std::string& key) {
        auto it = dict.find(key);
        // if already encode this key, return the existing id. Otherwise, assign a new id and store it in the dictionary.
        if (it != dict.end()) { return it->second; }
        if (dict.size() > std::numeric_limits<UInt>::max()) {
            throw std::runtime_error("Exceeded maximum number of unique values for this column. Consider using a larger uint type for encoding.");
        } // consider a larger uint type for this data type

        UInt id = static_cast<UInt>(dict.size()); // run-time determine 
        dict.emplace(key, id);
        return id;
    }

    template <typename UInt, typename Dict>
    inline bool load_predefined_ids(const std::string& path, Dict& dict, const std::string& label) {
        std::ifstream input(path);
        if (!input.is_open()) {
            std::cerr << "Failed to open predefined " << label
                      << " dictionary: " << path << std::endl;
            return false;
        }

        std::string line;
        int line_num = 0;
        while (std::getline(input, line)) {
            line_num++;
            const std::string trimmed = trim_copy(line);
            if (trimmed.empty()) {
                continue;
            }
            if (line_num == 1 && trimmed == "id,value") {
                continue;
            }

            const std::size_t comma = trimmed.find(',');
            if (comma == std::string::npos) {
                std::cerr << "Malformed dictionary entry at line " << line_num
                          << " in " << path << std::endl;
                return false;
            }

            const std::string id_str = trim_copy(trimmed.substr(0, comma));
            const std::string raw_value = trim_copy(trimmed.substr(comma + 1));
            const std::string value = unquote_copy(raw_value);

            uint32_t parsed_id = 0;
            if (!parse_uint32(id_str, parsed_id) ||
                parsed_id > static_cast<uint32_t>(std::numeric_limits<UInt>::max())) {
                std::cerr << "Invalid predefined id at line " << line_num
                          << " in " << path << std::endl;
                return false;
            }

            const auto [_, inserted] = dict.emplace(value, static_cast<UInt>(parsed_id));
            if (!inserted) {
                std::cerr << "Duplicate predefined value '" << value
                          << "' in " << path << std::endl;
                return false;
            }
        }

        return true;
    }

    template <typename UInt, typename Dict>
    inline UInt lookup_or_encode_id(
        Dict& dict,
        const std::string& key,
        bool use_predefined_mapping,
        const std::string& label) {
        if (!use_predefined_mapping) {
            return encode_id<UInt>(dict, key);
        }

        auto it = dict.find(key);
        if (it == dict.end()) {
            throw std::runtime_error(
                "Missing predefined " + label + " mapping for value: " + key);
        }
        return static_cast<UInt>(it->second);
    }
} // namespace ends here
