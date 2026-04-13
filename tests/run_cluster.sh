#!/bin/bash

# ============================================================================
# Mini2 9-Node Cluster Runner
# Starts all 9 nodes, waits for readiness, and prints C++ client commands.
# ============================================================================

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${PROJECT_DIR}/build"
SERVER_EXEC="${BUILD_DIR}/bin/server"
CLIENT_EXEC="${BUILD_DIR}/bin/client"
LOG_DIR="/tmp/mini2_cluster_logs"

SERVER_TIMEOUT=15

NODES=(A B C D E F G H I)
PIDS=()

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

log_error() {
    echo -e "${RED}[✗]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

cleanup() {
    log_info "Cleaning up cluster..."
    for pid in "${PIDS[@]:-}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
}

trap cleanup EXIT

wait_for_node() {
    local node_id="$1"
    local pid="$2"
    local log_file="${LOG_DIR}/node_${node_id}.log"
    local elapsed=0

    while [ "$elapsed" -lt "$SERVER_TIMEOUT" ]; do
        if grep -q "Server listening on" "$log_file" 2>/dev/null; then
            return 0
        fi

        if ! kill -0 "$pid" 2>/dev/null; then
            log_error "Node ${node_id} exited early. Log:"
            cat "$log_file"
            return 1
        fi

        sleep 0.5
        elapsed=$((elapsed + 1))
    done

    log_warning "Node ${node_id} readiness timed out. Recent log:"
    tail -20 "$log_file" 2>/dev/null || true
    return 1
}

print_client_commands() {
    echo ""
    log_info "Cluster is up. Try these C++ client commands from ${PROJECT_DIR}:"
    echo ""
    echo "cd ${PROJECT_DIR}"
    echo "./build/bin/client -s localhost:50051 ping"
    echo "./build/bin/client -s localhost:50051 query --request-id q-local"
    echo "./build/bin/client -s localhost:50051 forward --request-id f-all"
    echo "./build/bin/client -s localhost:50051 forward --borough-id 1 --request-id f-borough"
    echo "./build/bin/client -s localhost:50051 forward --agency-id 10 --request-id f-agency"
    echo "./build/bin/client -s localhost:50051 forward --lat-min 40.7 --lat-max 40.8 --lon-min -74.0 --lon-max -73.9 --request-id f-geo"
    echo ""
    log_info "Logs are in ${LOG_DIR}"
    log_info "Press Ctrl+C to stop all 9 nodes."
}

log_info "=========================================="
log_info "Mini2 9-Node Cluster Runner"
log_info "=========================================="

if [ ! -f "$SERVER_EXEC" ]; then
    log_error "Server executable not found: $SERVER_EXEC"
    log_info "Build first: cd ${PROJECT_DIR} && mkdir -p build && cd build && cmake .. && cmake --build ."
    exit 1
fi

if [ ! -f "$CLIENT_EXEC" ]; then
    log_error "Client executable not found: $CLIENT_EXEC"
    log_info "Build first: cd ${PROJECT_DIR} && mkdir -p build && cd build && cmake .. && cmake --build ."
    exit 1
fi

mkdir -p "$LOG_DIR"
rm -f "${LOG_DIR}"/node_*.log

cd "$PROJECT_DIR"

log_info "Starting nodes: ${NODES[*]}"
for node_id in "${NODES[@]}"; do
    log_info "Starting node ${node_id}"
    "$SERVER_EXEC" "$node_id" > "${LOG_DIR}/node_${node_id}.log" 2>&1 &
    pid=$!
    PIDS+=("$pid")
done

for i in "${!NODES[@]}"; do
    wait_for_node "${NODES[$i]}" "${PIDS[$i]}"
    log_success "Node ${NODES[$i]} is ready"
done

print_client_commands

while true; do
    sleep 1
done

# run_cluster.sh logic:
# 1. run build/bin/server A 
# 2. server.cpp::main()
# 3. server.cpp::Runserver("A")
# 4. read config/mini2_config/node_A.yaml
# 5. create Mini2Server with node_id "A" and config
# 6. Mini2Server::Start() -> starts gRPC server and background threads
# 7. gRPC server listens on port from config (e.g., 50051)
# 8. when RPC coming, using Mini2Server::HandleRPC() to handle request

# how to run
#cd /Users/luli/Documents/sjsu/CMPE275/ParallelCompMini2
#./build/bin/client -s localhost:50051 ping
#./build/bin/client -s localhost:50051 query --request-id q-local
#./build/bin/client -s localhost:50051 forward --request-id f-all
#./build/bin/client -s localhost:50051 forward --borough-id 1 --request-id f-borough
#./build/bin/client -s localhost:50051 forward --agency-id 10 --request-id f-agency
#./build/bin/client -s localhost:50051 forward --lat-min 40.7 --lat-max 40.8 --lon-min -74.0 --lon-max -73.9 --request-id f-geo

# test points:
# 1. ping. active nodes: A, B, C, D, E, F, G, H, I. indicating all nodes are up and can respond to ping.
# 2. query --request-id q-local. from_node = A, records_returnd = 0. Query only check local now. no forward. cause A has no data, return 0. 
# 3. forward --agency-id 10 from_node = A, records_returned = 534. Request first goes to A, A forward to children nodes. Summing up at root node and return to client. This tests forward logic and aggregation logic.
# 4. forward --lat-min 40.7 --lat-max 40.8 --lon-min -74.0 --lon-max -73.9 from_node = A, records_returned = 16010. Similar to #3 but with geo filter. Tests geo filtering logic.
