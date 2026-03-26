#include "csv_parser.hpp"

namespace {
/** parse one row.  44 fields in total. Seperated by ','*/
void split_csv_line(const std::string& row, std::vector<std::string>& fields) {
    std::string curr; // curr filed
    bool in_quotes = false; // whether we are currently inside a " " field

    for (size_t i = 0; i < row.size(); i++) {
        const char c = row[i];
        // handle " and "" in CSV. Example "234-16 LINDEN BOULEVARD, QUEENS (CAMBRIA HEIGHTS), NY, 11411"
        if (c == '"') { // left "
            if (in_quotes && i + 1 < row.size() && row[i + 1] == '"') {
                curr.push_back('"');
                i++;
            } else { // right "
                in_quotes = !in_quotes;
            }
            continue;
        }

        if (c == ',' && !in_quotes) {
            fields.push_back(curr);
            curr.clear();
            continue; // next field
        }

        // normal char
        curr.push_back(c);
    }

    fields.push_back(curr); // last field
}
}

/** Init header_ in constructor */
CSVParser::CSVParser(const std::string& path) : file_(path) {
    if (!file_.is_open()) {return;}

    std::string header_line;
    if (std::getline(file_, header_line)) {
        split_csv_line(header_line, header_);
    }
}

/** read one row when calling this func. Call multi-time of this func in dataset loading func */
bool CSVParser::read_row(std::vector<std::string>& fields) {
    fields.clear();

    if (!file_.is_open()) {return false;}

    std::string line;
    if (!std::getline(file_, line)) {return false;}

    split_csv_line(line, fields);
    return true;
}
