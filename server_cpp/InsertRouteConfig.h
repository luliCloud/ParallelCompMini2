#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include <yaml-cpp/yaml.h>

struct InsertRoute {
    int64_t max_created_date;
    std::string node_id;
};

struct InsertRouteConfig {
    std::filesystem::path path;
    std::vector<InsertRoute> routes;
    std::string default_node_id;

    bool loaded() const {
        return !path.empty();
    }
};

InsertRouteConfig LoadInsertRouteConfig(
    const YAML::Node& node_config,
    const std::filesystem::path& node_config_path);
