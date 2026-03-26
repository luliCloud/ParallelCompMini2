#pragma once
#include <cstdint>
#include <stdexcept>
#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

// exclude Record for now. We do vectorization in phase 3
class DatasetSOA {
public:
    DatasetSOA() = default;
    ~DatasetSOA() = default;

    bool load_csv(const std::string& path);
    std::size_t size() const { return created_date_.size(); }

    // getter
    const std::vector<uint32_t>& id() const {return id_; }
    const std::vector<int64_t>& created_date() const { return created_date_; }
    const std::vector<int64_t>& closed_date() const { return closed_date_; }
    const std::vector<uint16_t>& agency_id() const { return agency_id_; }
    const std::vector<uint32_t>& problem_id() const { return problem_id_; }
    const std::vector<uint8_t>& status_id() const { return status_id_; }
    const std::vector<uint8_t>& borough_id() const { return borough_id_; }
    const std::vector<uint32_t>& zip_code() const { return zip_code_; }
    const std::vector<float>& latitude() const { return latitude_; }
    const std::vector<float>& longitude() const { return longitude_; }

    /** TODO: if we already stored as vectorization, do we still need to have map to store these info? */
    template <typename UInt>
    UInt get_encoded_attribute_id(const std::string& attribute_name, const std::string& value) const {
        if (attribute_name == "Agency") {
            auto it = agency_dict_.find(value); // return ptr to {key, val} in the map
            if (it != agency_dict_.end()) {
                return static_cast<UInt>(it->second);
            }
        } else if (attribute_name == "Problem") {
            auto it = problem_dict_.find(value);
            if (it != problem_dict_.end()) {
                return static_cast<UInt>(it->second);
            }
        } else if (attribute_name == "Status") {
            auto it = status_dict_.find(value);
            if (it != status_dict_.end()) {
                return static_cast<UInt>(it->second);
            }
        } else if (attribute_name == "Borough") {
            auto it = borough_dict_.find(value);
            if (it != borough_dict_.end()) {
                return static_cast<UInt>(it->second);
            }
        }
        throw std::invalid_argument("Invalid attribute lookup");
    }
private:
    std::vector<uint32_t> id_;
    std::vector<int64_t> created_date_;
    std::vector<int64_t> closed_date_;
    std::vector<uint16_t> agency_id_;
    std::vector<uint32_t> problem_id_;
    std::vector<uint8_t> status_id_;
    std::vector<uint8_t> borough_id_;
    std::vector<uint32_t> zip_code_;
    std::vector<float> latitude_;
    std::vector<float> longitude_;

    std::unordered_map<std::string, uint16_t> agency_dict_;
    std::unordered_map<std::string, uint32_t> problem_dict_;
    std::unordered_map<std::string, uint8_t> status_dict_;
    std::unordered_map<std::string, uint8_t> borough_dict_;
};
