#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "mini2.grpc.pb.h"

namespace {

using Clock = std::chrono::steady_clock;
using mini2::NodeService;
using mini2::ChunkCancelRequest;
using mini2::ChunkCancelResponse;
using mini2::ChunkRequest;
using mini2::ChunkSessionResponse;
using mini2::PingRequest;
using mini2::PingResponse;
using mini2::QueryChunkResponse;
using mini2::QueryRequest;
using mini2::QueryResponse;
// for SOA
using mini2::SOACountKind;
using mini2::SOACountRequest;
using mini2::SOACountResponse;

struct Options {
    std::string server;
    double timeout_seconds = 5.0;
    std::string command;
    std::optional<std::string> request_id;
    std::optional<std::uint32_t> agency_id;
    std::optional<std::uint32_t> borough_id;
    std::optional<std::uint32_t> zip_code;
    std::optional<float> lat_min;
    std::optional<float> lat_max;
    std::optional<float> lon_min;
    std::optional<float> lon_max;
    std::optional<std::uint32_t> chunk_size;
    // SOA query
    std::optional<std::int64_t> created_date_start;
    std::optional<std::int64_t> created_date_end;
    std::optional<std::uint32_t> status_id; // 0 for In Progress, 1 for Closed
};

bool IsCommand(std::string_view token) {
    return token == "ping" || token == "query" || token == "forward"
        || token == "forward-chunked"
        || token == "count-created-date-range"
        || token == "count-by-agency-and-created-date-range"
        || token == "count-by-status-and-created-date-range";
}

std::string GenerateRequestId(std::string_view prefix) {
    const auto now = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    std::ostringstream out;
    out << prefix << "-" << now;
    return out.str();
}

[[noreturn]] void ThrowUsageError(const std::string& message) {
    throw std::runtime_error(message + "\nUsage: client -s <host:port> [-t <seconds>] <ping|query|forward|forward-chunked> [options]");
}

std::string RequireValue(int& index, int argc, char** argv, std::string_view flag) {
    if (index + 1 >= argc) {
        ThrowUsageError("Missing value for " + std::string(flag));
    }
    ++index;
    return argv[index];
}

std::uint32_t ParseUint32(const std::string& value, std::string_view flag) {
    std::size_t consumed = 0;
    unsigned long parsed = 0;
    try {
        parsed = std::stoul(value, &consumed, 10);
    } catch (const std::exception&) {
        ThrowUsageError("Invalid integer for " + std::string(flag) + ": " + value);
    }

    if (consumed != value.size() || parsed > std::numeric_limits<std::uint32_t>::max()) {
        ThrowUsageError("Invalid integer for " + std::string(flag) + ": " + value);
    }

    return static_cast<std::uint32_t>(parsed);
}

std::int64_t ParseInt64(const std::string& value, std::string_view flag) {
    std::size_t consumed = 0;
    long long parsed = 0;
    try {
        parsed = std::stoll(value, &consumed, 10);
    } catch (const std::exception&) {
        ThrowUsageError("Invalid integer for " + std::string(flag) + ": " + value);
    }

    if (consumed != value.size() || parsed > std::numeric_limits<std::int64_t>::max() || parsed < std::numeric_limits<std::int64_t>::min()) {
        ThrowUsageError("Invalid integer for " + std::string(flag) + ": " + value);
    }

    return static_cast<std::int64_t>(parsed);
}

float ParseFloat(const std::string& value, std::string_view flag) {
    std::size_t consumed = 0;
    float parsed = 0.0f;
    try {
        parsed = std::stof(value, &consumed);
    } catch (const std::exception&) {
        ThrowUsageError("Invalid float for " + std::string(flag) + ": " + value);
    }

    if (consumed != value.size()) {
        ThrowUsageError("Invalid float for " + std::string(flag) + ": " + value);
    }

    return parsed;
}

Options ParseArgs(int argc, char** argv) {
    if (argc < 2) {
        ThrowUsageError("Missing arguments");
    }

    Options options;

    int index = 1;
    for (; index < argc; ++index) {
        const std::string token = argv[index];
        if (IsCommand(token)) {
            options.command = token;
            ++index;
            break;
        }

        if (token == "-s" || token == "--server") {
            options.server = RequireValue(index, argc, argv, token);
        } else if (token == "-t" || token == "--timeout") {
            options.timeout_seconds = std::stod(RequireValue(index, argc, argv, token));
        } else {
            ThrowUsageError("Unknown option: " + token);
        }
    }

    if (options.server.empty()) {
        ThrowUsageError("Missing required option: --server");
    }
    if (options.command.empty()) {
        ThrowUsageError("Missing command");
    }

    for (; index < argc; ++index) {
        // parse arg input for query conditions. e.g. --agency-id 1, --lat-min 40.0
        const std::string token = argv[index];
        if (token == "--request-id") {
            options.request_id = RequireValue(index, argc, argv, token);
        } else if (token == "--agency-id") {
            options.agency_id = ParseUint32(RequireValue(index, argc, argv, token), token);
        } else if (token == "--borough-id") {
            options.borough_id = ParseUint32(RequireValue(index, argc, argv, token), token);
        } else if (token == "--zip-code") {
            options.zip_code = ParseUint32(RequireValue(index, argc, argv, token), token);
        } else if (token == "--lat-min") {
            options.lat_min = ParseFloat(RequireValue(index, argc, argv, token), token);
        } else if (token == "--lat-max") {
            options.lat_max = ParseFloat(RequireValue(index, argc, argv, token), token);
        } else if (token == "--lon-min") {
            options.lon_min = ParseFloat(RequireValue(index, argc, argv, token), token);
        } else if (token == "--lon-max") {
            options.lon_max = ParseFloat(RequireValue(index, argc, argv, token), token);
        } else if (token == "--chunk-size") {
            options.chunk_size = ParseUint32(RequireValue(index, argc, argv, token), token);
        } else if (token == "--created-date-start") {
            options.created_date_start = ParseInt64(RequireValue(index, argc, argv, token), token);
        } else if (token == "--created-date-end") {
            options.created_date_end = ParseInt64(RequireValue(index, argc, argv, token), token);
        } else if (token == "--status-id") {
            options.status_id = ParseUint32(RequireValue(index, argc, argv, token), token);
        } else {
            ThrowUsageError("Unknown command option: " + token);
        }
    }

    return options;
}

void PrintClientHeader(const Options& options, double connect_ms) {
    std::cout << "client:\n";
    std::cout << "   server = " << options.server << '\n';
    std::cout << "   command = " << options.command << '\n';
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "   connect_time_ms = " << connect_ms << '\n';
}

PingRequest BuildPingRequest(const Options& options) {
    PingRequest request;
    request.set_request_id(options.request_id.value_or(GenerateRequestId("client-ping")));
    return request;
}

QueryRequest BuildQueryRequest(const Options& options) {
    QueryRequest request;
    request.set_request_id(options.request_id.value_or(GenerateRequestId("client-query")));
    if (options.agency_id) {
        request.set_agency_id(*options.agency_id);
    }
    if (options.borough_id) {
        request.set_borough_id(*options.borough_id);
    }
    if (options.zip_code) {
        request.set_zip_code(*options.zip_code);
    }
    if (options.lat_min) {
        request.set_lat_min(*options.lat_min);
    }
    if (options.lat_max) {
        request.set_lat_max(*options.lat_max);
    }
    if (options.lon_min) {
        request.set_lon_min(*options.lon_min);
    }
    if (options.lon_max) {
        request.set_lon_max(*options.lon_max);
    }
    if (options.chunk_size) {
        request.set_chunk_size(*options.chunk_size);
    }
    return request;
}

SOACountRequest BuildCountCreatedDateRangeRequest(const Options& options) {
    if (!options.created_date_start || !options.created_date_end) {
        ThrowUsageError("Missing required options for count-created-date-range: --created-date-start and --created-date-end");
    }
    SOACountRequest request;
    request.set_request_id(options.request_id.value_or(GenerateRequestId("client-soa-count")));
    request.set_kind(SOACountKind::SOA_COUNT_CREATED_DATE_RANGE);
    request.set_created_date_start(*options.created_date_start);
    request.set_created_date_end(*options.created_date_end);
    return request;
}

SOACountRequest BuildCountByAgencyAndCreatedDateRangeRequest(const Options& options) {
    if (!options.agency_id || !options.created_date_start || !options.created_date_end) {
        ThrowUsageError("Missing required options for count-by-agency-and-created-date-range: --agency-id, --created-date-start and --created-date-end");
    }
    SOACountRequest request;
    request.set_request_id(options.request_id.value_or(GenerateRequestId("client-soa-count")));
    request.set_kind(SOACountKind::SOA_COUNT_BY_AGENCY_AND_CREATED_DATE_RANGE);
    request.set_agency_id(*options.agency_id);
    request.set_created_date_start(*options.created_date_start);
    request.set_created_date_end(*options.created_date_end);
    return request;
}

SOACountRequest BuildCountByStatusAndCreatedDateRangeRequest(const Options& options) {
    if (!options.created_date_start || !options.created_date_end) {
        ThrowUsageError("Missing required options for count-by-status-and-created-date-range: --created-date-start and --created-date-end");
    }
    SOACountRequest request;
    request.set_request_id(options.request_id.value_or(GenerateRequestId("client-soa-count")));
    request.set_kind(SOACountKind::SOA_COUNT_BY_STATUS_AND_CREATED_DATE_RANGE);
    request.set_created_date_start(*options.created_date_start);
    request.set_created_date_end(*options.created_date_end);
    request.set_status_id(options.status_id.value_or(0));  // Assuming status_id is optional and defaults to 0 if not provided  
    return request;
}

void PrintQueryRequest(const QueryRequest& request) {
    std::cout << "query request:\n";
    std::cout << "   request_id = " << request.request_id() << '\n';
    if (request.has_agency_id()) {
        std::cout << "   agency_id = " << request.agency_id() << '\n';
    }
    if (request.has_borough_id()) {
        std::cout << "   borough_id = " << request.borough_id() << '\n';
    }
    if (request.has_zip_code()) {
        std::cout << "   zip_code = " << request.zip_code() << '\n';
    }
    if (request.has_lat_min()) {
        std::cout << "   lat_min = " << request.lat_min() << '\n';
    }
    if (request.has_lat_max()) {
        std::cout << "   lat_max = " << request.lat_max() << '\n';
    }
    if (request.has_lon_min()) {
        std::cout << "   lon_min = " << request.lon_min() << '\n';
    }
    if (request.has_lon_max()) {
        std::cout << "   lon_max = " << request.lon_max() << '\n';
    }
    if (request.has_chunk_size()) {
        std::cout << "   chunk_size = " << request.chunk_size() << '\n';
    }
}

void PrintPingResponse(const PingResponse& response, double elapsed_ms) {
    std::cout << "ping response:\n";
    std::cout << "   response_request_id = " << response.request_id() << '\n';
    std::cout << "   active nodes: ";
    for (const auto& node : response.active_nodes()) {
        std::cout << node << ' ';
    }
    std::cout << "\n   ping_rtt_ms = " << elapsed_ms << '\n';
}

void PrintQueryResponse(std::string_view label, const QueryResponse& response, double elapsed_ms) {
    std::cout << label << " response:\n";
    std::cout << "   response_request_id = " << response.request_id() << '\n';
    std::cout << "   from_node = " << response.from_node() << '\n';
    std::cout << "   records_returned = " << response.records_size() << '\n';
    std::cout << "   " << label << "_rtt_ms = " << elapsed_ms << '\n';
}

void PrintChunkSessionResponse(const ChunkSessionResponse& response, double elapsed_ms) {
    std::cout << "forward-chunked session:\n";
    std::cout << "   response_request_id = " << response.request_id() << '\n';
    std::cout << "   session_id = " << response.session_id() << '\n';
    std::cout << "   from_node = " << response.from_node() << '\n';
    std::cout << "   chunk_size = " << response.chunk_size() << '\n';
    std::cout << "   total_chunks = " << response.total_chunks() << '\n';
    std::cout << "   total_records = " << response.total_records() << '\n';
    std::cout << "   start_forward_chunks_rtt_ms = " << elapsed_ms << '\n';
}

void PrintChunkResponse(const QueryChunkResponse& response, double elapsed_ms) {
    std::cout << "forward chunk:\n";
    std::cout << "   session_id = " << response.session_id() << '\n';
    std::cout << "   chunk_index = " << response.chunk_index() << '\n';
    std::cout << "   records_returned = " << response.records_size() << '\n';
    std::cout << "   total_chunks = " << response.total_chunks() << '\n';
    std::cout << "   done = " << response.done() << '\n';
    std::cout << "   get_forward_chunk_rtt_ms = " << elapsed_ms << '\n';
}

void PrintCountResponse(const SOACountResponse& response, double elapsed_ms) {
    std::cout << "SOA count response:\n";
    std::cout << "   response_request_id = " << response.request_id() << '\n';
    std::cout << "   from_node = " << response.from_node() << '\n';
    std::cout << "   count = " << response.count() << '\n';
    std::cout << "   count_query_rtt_ms = " << elapsed_ms << '\n';
}

void ConfigureContext(grpc::ClientContext& context, double timeout_seconds) {
    const auto timeout = std::chrono::duration_cast<std::chrono::system_clock::duration>(
        std::chrono::duration<double>(timeout_seconds));
    context.set_deadline(std::chrono::system_clock::now() + timeout);
}

void EnsureOk(const grpc::Status& status) {
    if (!status.ok()) {
        throw std::runtime_error(
            "RPC failed: " + std::to_string(status.error_code()) + " - " + status.error_message());
    }
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const Options options = ParseArgs(argc, argv);
        const auto start_total = Clock::now();
        const auto start_connect = Clock::now();
        auto channel = grpc::CreateChannel(options.server, grpc::InsecureChannelCredentials());
        const auto connect_deadline = std::chrono::system_clock::now() +
            std::chrono::duration_cast<std::chrono::system_clock::duration>(
                std::chrono::duration<double>(options.timeout_seconds));

        if (!channel->WaitForConnected(connect_deadline)) {
            std::cerr << "\nFailed to connect/send request: timed out connecting to "
                      << options.server << "\n\n";
            return 1;
        }

        auto stub = NodeService::NewStub(channel);
        const double connect_ms = std::chrono::duration<double, std::milli>(
            Clock::now() - start_connect).count();

        PrintClientHeader(options, connect_ms);

        if (options.command == "ping") {
            const PingRequest request = BuildPingRequest(options);
            std::cout << "Ping request: \n";
            std::cout << "   request_id = " << request.request_id() << '\n';

            PingResponse response;
            grpc::ClientContext context;
            ConfigureContext(context, options.timeout_seconds);
            const auto start_rpc = Clock::now();
            const grpc::Status status = stub->Ping(&context, request, &response);
            const double rpc_ms = std::chrono::duration<double, std::milli>(
                Clock::now() - start_rpc).count();
            EnsureOk(status);
            PrintPingResponse(response, rpc_ms);
        } else if (options.command == "query" || options.command == "forward") {
            const QueryRequest request = BuildQueryRequest(options);
            PrintQueryRequest(request);

            QueryResponse response;
            grpc::ClientContext context;
            ConfigureContext(context, options.timeout_seconds);
            const auto start_rpc = Clock::now();
            grpc::Status status;
            if (options.command == "query") {
                status = stub->Query(&context, request, &response);
            } else {
                status = stub->Forward(&context, request, &response);
            }
            const double rpc_ms = std::chrono::duration<double, std::milli>(
                Clock::now() - start_rpc).count();
            EnsureOk(status);
            PrintQueryResponse(options.command, response, rpc_ms);
        } else if (options.command == "forward-chunked") {
            const QueryRequest request = BuildQueryRequest(options);
            PrintQueryRequest(request);

            ChunkSessionResponse session_response;
            grpc::ClientContext start_context;
            ConfigureContext(start_context, options.timeout_seconds);

            const auto start_rpc = Clock::now();
            grpc::Status status = stub->StartForwardChunks(
                &start_context,
                request,
                &session_response);
            const double start_rpc_ms = std::chrono::duration<double, std::milli>(
                Clock::now() - start_rpc).count();
            EnsureOk(status);
            PrintChunkSessionResponse(session_response, start_rpc_ms);

            std::uint64_t total_records_received = 0;
            for (std::uint32_t chunk_index = 0;
                 chunk_index < session_response.total_chunks();
                 ++chunk_index) {
                ChunkRequest chunk_request;
                chunk_request.set_session_id(session_response.session_id());
                chunk_request.set_chunk_index(chunk_index);

                QueryChunkResponse chunk_response;
                grpc::ClientContext chunk_context;
                ConfigureContext(chunk_context, options.timeout_seconds);

                const auto start_chunk_rpc = Clock::now();
                status = stub->GetForwardChunk(
                    &chunk_context,
                    chunk_request,
                    &chunk_response);
                const double chunk_rpc_ms = std::chrono::duration<double, std::milli>(
                    Clock::now() - start_chunk_rpc).count();
                EnsureOk(status);

                total_records_received +=
                    static_cast<std::uint64_t>(chunk_response.records_size());
                PrintChunkResponse(chunk_response, chunk_rpc_ms);

                if (chunk_response.done()) {
                    break;
                }
            }

            ChunkCancelRequest cancel_request;
            cancel_request.set_session_id(session_response.session_id());
            ChunkCancelResponse cancel_response;
            grpc::ClientContext cancel_context;
            ConfigureContext(cancel_context, options.timeout_seconds);
            status = stub->CancelChunks(&cancel_context, cancel_request, &cancel_response);
            EnsureOk(status);

            std::cout << "forward-chunked response:\n";
            std::cout << "   records_received = " << total_records_received << '\n';
            std::cout << "   session_cancelled = " << cancel_response.cancelled() << '\n';
        } else if (options.command == "count-created-date-range") {
            const auto request = BuildCountCreatedDateRangeRequest(options);
            std::cout << "SOA Count Created Date Range request: \n";
            std::cout << "   request_id = " << request.request_id() << '\n';
            std::cout << "   created_date_start = " << request.created_date_start() << '\n';
            std::cout << "   created_date_end = " << request.created_date_end() << '\n';

            SOACountResponse response;
            grpc::ClientContext context;
            ConfigureContext(context, options.timeout_seconds);

            const auto start_rpc = Clock::now();
            grpc::Status status = stub->CountQuery(&context, request, &response);
            const double rpc_ms = std::chrono::duration<double, std::milli>(
                Clock::now() - start_rpc).count();
            EnsureOk(status);
            PrintCountResponse(response, rpc_ms);
        } else if (options.command == "count-by-agency-and-created-date-range") {
            const auto request = BuildCountByAgencyAndCreatedDateRangeRequest(options);
            std::cout << "SOA Count By Agency And Created Date Range request: \n";
            std::cout << "   request_id = " << request.request_id() << '\n';
            std::cout << "   agency_id = " << request.agency_id() << '\n';
            std::cout << "   created_date_start = " << request.created_date_start() << '\n';
            std::cout << "   created_date_end = " << request.created_date_end() << '\n';

            SOACountResponse response;
            grpc::ClientContext context;
            ConfigureContext(context, options.timeout_seconds);

            const auto start_rpc = Clock::now();
            grpc::Status status = stub->CountQuery(&context, request, &response);
            const double rpc_ms = std::chrono::duration<double, std::milli>(
                Clock::now() - start_rpc).count();
            EnsureOk(status);
            PrintCountResponse(response, rpc_ms);
        } else if (options.command == "count-by-status-and-created-date-range") {
            const auto request = BuildCountByStatusAndCreatedDateRangeRequest(options);
            std::cout << "SOA Count By Status And Created Date Range request: \n";
            std::cout << "   request_id = " << request.request_id() << '\n';
            std::cout << "   created_date_start = " << request.created_date_start() << '\n';
            std::cout << "   created_date_end = " << request.created_date_end() << '\n';

            SOACountResponse response;
            grpc::ClientContext context;
            ConfigureContext(context, options.timeout_seconds);

            const auto start_rpc = Clock::now();
            grpc::Status status = stub->CountQuery(&context, request, &response);
            const double rpc_ms = std::chrono::duration<double, std::milli>(
                Clock::now() - start_rpc).count();
            EnsureOk(status);
            PrintCountResponse(response, rpc_ms);
        } else {
            ThrowUsageError("Unknown command: " + options.command);
        }

        const double total_ms = std::chrono::duration<double, std::milli>(
            Clock::now() - start_total).count();
        std::cout << "   total_time_ms = " << total_ms << '\n';
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "\nFailed to connect/send request: " << ex.what() << "\n\n";
        return 1;
    }
}

// Examples of run commands:
// ./build/bin/client -s localhost:50051 ping
// ./build/bin/client -s localhost:50051 ping --request-id test-ping-1
// ./build/bin/client -s localhost:50051 query --agency-id 1
// ./build/bin/client -s localhost:50051 forward --borough-id 2

/** distributed 
 * ./build/bin/client -s localhost:50051 count-created-date-range \
  --created-date-start 1770249600 \
  --created-date-end 1770335999 \
  --request-id test-count-a

 */
