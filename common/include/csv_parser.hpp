#pragma once
#include <fstream>
#include <string>
#include <vector>

class CSVParser {
public:
    explicit CSVParser(const std::string& path); // Construct and read header
    ~CSVParser() = default;
    bool read_row(std::vector<std::string>& fields); // Each time read one row, push Record into Dataset
    const std::vector<std::string>& header() const {return header_;}; // Getter: the header of the CSV file
private: 
    std::ifstream file_; // RAII: close file when destructing
    std::vector<std::string> header_;
};
