#include "InsertRouteConfig.h"

#include <algorithm>

namespace fs = std::filesystem;

namespace {

fs::path ResolveInsertRoutePath(
    const YAML::Node& node_config,
    const fs::path& node_config_path) {
    if (node_config["insert_route_path"]) {
        const fs::path configured = node_config["insert_route_path"].as<std::string>();
        if (configured.is_absolute()) {
            return configured;
        }

        const fs::path from_cwd = fs::current_path() / configured;
        if (fs::exists(from_cwd)) {
            return from_cwd;
        }

        const fs::path from_source_dir = fs::path(MINI2_SOURCE_DIR) / configured;
        if (fs::exists(from_source_dir)) {
            return from_source_dir;
        }

        return node_config_path.parent_path() / configured;
    }

    const fs::path relative = fs::path("config") / "insert_routes.yaml";
    if (fs::exists(relative)) {
        return relative;
    }

    const fs::path source_relative = fs::path(MINI2_SOURCE_DIR) / relative;
    if (fs::exists(source_relative)) {
        return source_relative;
    }

    return {};
}

}  // namespace

InsertRouteConfig LoadInsertRouteConfig(
    const YAML::Node& node_config,
    const fs::path& node_config_path) {
    InsertRouteConfig result;
    result.path = ResolveInsertRoutePath(node_config, node_config_path);
    if (result.path.empty()) {
        return result;
    }

    YAML::Node route_config = YAML::LoadFile(result.path.string());
    if (route_config["routes"]) {
        for (const auto& route_node : route_config["routes"]) {
            InsertRoute route;
            route.node_id = route_node["node_id"].as<std::string>();
            route.max_created_date = route_node["max_created_date"].as<int64_t>();
            result.routes.push_back(route);
        }
    }

    if (route_config["default_node_id"]) {
        result.default_node_id = route_config["default_node_id"].as<std::string>();
    }

    std::sort(result.routes.begin(), result.routes.end(),
              [](const InsertRoute& left, const InsertRoute& right) {
                  return left.max_created_date < right.max_created_date;
              });

    return result;
}
