#!/bin/bash

# ============================================================================
# E2E Test Runner for Mini2 gRPC Server
# Starts server, runs tests, and cleans up
# ============================================================================

set -e

# Get the project root (parent of tests directory)
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${PROJECT_DIR}/build"
VENV_DIR="${PROJECT_DIR}/.venv"
SERVER_EXEC="${BUILD_DIR}/bin/server"
TEST_SCRIPT="${PROJECT_DIR}/tests/test_client.py"
DATA_PATH="${PROJECT_DIR}/benchmarks/workload_100k.csv"

# Configuration
NODE_ID="A"
SERVER_TIMEOUT=10
TEST_TIMEOUT=30

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ============================================================================
# Helper Functions
# ============================================================================

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
    log_info "Cleaning up..."
    
    # Kill server if running
    if [ ! -z "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        log_info "Stopping server (PID: $SERVER_PID)"
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
    fi
}

# Set cleanup on exit
trap cleanup EXIT

# ============================================================================
# Pre-flight Checks
# ============================================================================

log_info "=========================================="
log_info "Mini2 E2E Test Runner"
log_info "=========================================="

# Check if server executable exists
if [ ! -f "$SERVER_EXEC" ]; then
    log_error "Server executable not found: $SERVER_EXEC"
    log_info "Please run: cd $PROJECT_DIR && mkdir -p build && cd build && cmake .. && cmake --build ."
    exit 1
fi

# Check if test script exists
if [ ! -f "$TEST_SCRIPT" ]; then
    log_error "Test script not found: $TEST_SCRIPT"
    exit 1
fi

# Check if dataset exists
if [ ! -f "$DATA_PATH" ]; then
    log_warning "Dataset not found: $DATA_PATH"
    log_info "Tests will still run, but server may not load data"
fi

# Check virtual environment
if [ ! -d "$VENV_DIR" ]; then
    log_error "Virtual environment not found: $VENV_DIR"
    log_info "Please run: cd $PROJECT_DIR && python3 -m venv .venv && .venv/bin/pip install -r client_py/requirements.txt"
    exit 1
fi

# ============================================================================
# Step 1: Start Server
# ============================================================================

log_info "=========================================="
log_info "Step 1: Starting gRPC Server"
log_info "=========================================="

log_info "Server: $SERVER_EXEC"
log_info "Node ID: $NODE_ID"
log_info "Dataset: $DATA_PATH"

# Start server in background
"$SERVER_EXEC" "$NODE_ID" "$DATA_PATH" > /tmp/mini2_server.log 2>&1 &
SERVER_PID=$!

log_success "Server started with PID: $SERVER_PID"

# Wait for server to be ready
log_info "Waiting for server to be ready (max ${SERVER_TIMEOUT}s)..."
READY=false
ELAPSED=0

while [ $ELAPSED -lt $SERVER_TIMEOUT ]; do
    if grep -q "Server ready\|listening on" /tmp/mini2_server.log 2>/dev/null; then
        READY=true
        break
    fi
    
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        log_error "Server process died. Check logs:"
        cat /tmp/mini2_server.log
        exit 1
    fi
    
    sleep 0.5
    ELAPSED=$((ELAPSED + 1))
done

if [ "$READY" = false ]; then
    log_warning "Server startup timeout (logs may indicate it's ready anyway)"
    cat /tmp/mini2_server.log
fi

log_success "Server is ready"

# ============================================================================
# Step 2: Run Tests
# ============================================================================

log_info "=========================================="
log_info "Step 2: Running Test Suite"
log_info "=========================================="

# Activate virtual environment and run tests
source "$VENV_DIR/bin/activate"

test_timeout_cmd=""
if command -v timeout &> /dev/null; then
    test_timeout_cmd="timeout $TEST_TIMEOUT"
fi

$test_timeout_cmd python3 "$TEST_SCRIPT" -s "localhost:50051" -t 5
TEST_RESULT=$?

deactivate 2>/dev/null || true

# ============================================================================
# Step 3: Results
# ============================================================================

log_info "=========================================="
log_info "Step 3: Test Results"
log_info "=========================================="

# Print server logs (last 20 lines)
echo ""
log_info "Server log (last 20 lines):"
tail -20 /tmp/mini2_server.log

echo ""

if [ $TEST_RESULT -eq 0 ]; then
    log_success "=========================================="
    log_success "All tests passed! ✓"
    log_success "=========================================="
    exit 0
else
    log_error "=========================================="
    log_error "Some tests failed ✗"
    log_error "=========================================="
    exit 1
fi

# cd /ParallelCompMini2
# generate mini2_pb2.py and mini2_pb2_grpc.py from proto/mini2.proto
# python3 -m grpc_tools.protoc -I./proto --python_out=./client_py --grpc_python_out=./client_py ./proto/mini2.proto
# python3 -m venv .venv
# .venv/bin/pip install -r client_py/requirements.txt
# bash tests/run_test_client.sh