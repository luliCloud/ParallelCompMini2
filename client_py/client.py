#
# client to our c++ server.
#
# Reading:
# https://grpc.io/docs/languages/python/basics/

import argparse
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc


CLIENT_DIR = Path(__file__).resolve().parent
if str(CLIENT_DIR) not in sys.path:
    sys.path.insert(0, str(CLIENT_DIR))

import mini2_pb2
import mini2_pb2_grpc


def make_request_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def add_query_filters(parser, include_chunk_size: bool = False):
    parser.add_argument("--agency-id", type=int, default=None)
    parser.add_argument("--borough-id", type=int, default=None)
    parser.add_argument("--zip-code", type=int, default=None)
    parser.add_argument("--lat-min", type=float, default=None)
    parser.add_argument("--lat-max", type=float, default=None)
    parser.add_argument("--lon-min", type=float, default=None)
    parser.add_argument("--lon-max", type=float, default=None)
    if include_chunk_size:
        parser.add_argument("--chunk-size", type=int, default=None)


def parse_args():
    parser = argparse.ArgumentParser(description="Mini2 Python client")
    parser.add_argument(
        "-s",
        "--server",
        required=True,
        help="Server address in host:port form, for example localhost:50051.",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=float,
        default=5.0,
        help="Timeout in seconds for connection and RPC.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    ping_parser = subparsers.add_parser("ping", help="Send Ping to the server")
    ping_parser.add_argument(
        "--request-id",
        default=None,
        help="Request ID for the ping. If omitted, a random one is generated.",
    )

    query_parser = subparsers.add_parser("query", help="Send QueryRequest to the server")
    query_parser.add_argument(
        "--request-id",
        default=None,
        help="Request ID for the query. If omitted, a random one is generated.",
    )
    add_query_filters(query_parser, include_chunk_size=True)

    forward_parser = subparsers.add_parser("forward", help="Send Forward request to the server")
    forward_parser.add_argument(
        "--request-id",
        default=None,
        help="Request ID for the forward request. If omitted, a random one is generated.",
    )
    add_query_filters(forward_parser, include_chunk_size=True)

    insert_parser = subparsers.add_parser("insert", help="Send Insert request to the server")
    insert_parser.add_argument(
        "--request-id",
        default=None,
        help="Request ID for the insert request. If omitted, a random one is generated.",
    )
    insert_parser.add_argument("--record-id", type=int, required=True)
    insert_parser.add_argument("--created-date", type=int, required=True)
    insert_parser.add_argument("--agency-id", type=int, required=True)
    insert_parser.add_argument("--problem-id", type=int, required=True)
    insert_parser.add_argument("--status-id", type=int, required=True)
    insert_parser.add_argument("--borough-id", type=int, required=True)
    insert_parser.add_argument("--closed-date", type=int, default=None)
    insert_parser.add_argument("--zip-code", type=int, default=None)
    insert_parser.add_argument("--latitude", type=float, default=None)
    insert_parser.add_argument("--longitude", type=float, default=None)

    delete_parser = subparsers.add_parser("delete", help="Send Delete request to the server")
    delete_parser.add_argument(
        "--request-id",
        default=None,
        help="Request ID for the delete request. If omitted, a random one is generated.",
    )
    delete_parser.add_argument("--record-id", type=int, default=None)
    delete_parser.add_argument("--created-date", type=int, default=None)
    delete_parser.add_argument("--closed-date", type=int, default=None)
    delete_parser.add_argument("--agency-id", type=int, default=None)
    delete_parser.add_argument("--problem-id", type=int, default=None)
    delete_parser.add_argument("--status-id", type=int, default=None)
    delete_parser.add_argument("--borough-id", type=int, default=None)
    delete_parser.add_argument("--zip-code", type=int, default=None)
    delete_parser.add_argument("--latitude", type=float, default=None)
    delete_parser.add_argument("--longitude", type=float, default=None)
    delete_parser.add_argument(
        "--all",
        action="store_true",
        help="Delete all records cluster-wide. Cannot be combined with other filters.",
    )

    chunked_parser = subparsers.add_parser("forward-chunked", help="Forward with chunked pull")
    chunked_parser.add_argument(
        "--request-id",
        default=None,
        help="Request ID for the chunked forward request. If omitted, a random one is generated.",
    )
    add_query_filters(chunked_parser, include_chunk_size=True)

    count_created_range_parser = subparsers.add_parser(
        "count-created-date-range",
        help="SOA count in created date range",
    )
    count_created_range_parser.add_argument("--request-id", default=None)
    count_created_range_parser.add_argument("--created-date-start", type=int, required=True)
    count_created_range_parser.add_argument("--created-date-end", type=int, required=True)

    count_by_agency_parser = subparsers.add_parser(
        "count-by-agency-and-created-date-range",
        help="SOA count by agency in created date range",
    )
    count_by_agency_parser.add_argument("--request-id", default=None)
    count_by_agency_parser.add_argument("--agency-id", type=int, required=True)
    count_by_agency_parser.add_argument("--created-date-start", type=int, required=True)
    count_by_agency_parser.add_argument("--created-date-end", type=int, required=True)

    count_by_status_parser = subparsers.add_parser(
        "count-by-status-and-created-date-range",
        help="SOA count by status in created date range",
    )
    count_by_status_parser.add_argument("--request-id", default=None)
    count_by_status_parser.add_argument("--created-date-start", type=int, required=True)
    count_by_status_parser.add_argument("--created-date-end", type=int, required=True)
    count_by_status_parser.add_argument("--status-id", type=int, default=None)

    return parser.parse_args()


def connect(server_address: str, timeout: float):
    start_connect = time.perf_counter()
    channel = grpc.insecure_channel(server_address)
    grpc.channel_ready_future(channel).result(timeout=timeout)
    connect_ms = (time.perf_counter() - start_connect) * 1000
    stub = mini2_pb2_grpc.NodeServiceStub(channel)
    return channel, stub, connect_ms


def submit_unary_rpc(executor, rpc_method, request, timeout):
    def invoke():
        start_rpc = time.perf_counter()
        response = rpc_method(request, timeout=timeout)
        rpc_ms = (time.perf_counter() - start_rpc) * 1000
        return response, rpc_ms

    return executor.submit(invoke)


def run_ping(stub, args, executor):
    request = build_ping_request(args)

    print("Ping request: ")
    print(f"   request_id = {request.request_id}")

    response, ping_ms = submit_unary_rpc(
        executor, stub.Ping, request, args.timeout
    ).result()

    print_ping_response("ping", response, ping_ms)
    # dir(response)


def build_query_request(args):
    request = mini2_pb2.QueryRequest()
    request.request_id = args.request_id or make_request_id("client-query")

    if args.agency_id is not None:
        request.agency_id = args.agency_id
    if args.borough_id is not None:
        request.borough_id = args.borough_id
    if args.zip_code is not None:
        request.zip_code = args.zip_code
    if args.lat_min is not None:
        request.lat_min = args.lat_min
    if args.lat_max is not None:
        request.lat_max = args.lat_max
    if args.lon_min is not None:
        request.lon_min = args.lon_min
    if args.lon_max is not None:
        request.lon_max = args.lon_max
    if getattr(args, "chunk_size", None) is not None:
        request.chunk_size = args.chunk_size

    return request


def build_ping_request(args):
    request = mini2_pb2.PingRequest()
    request.request_id = args.request_id or make_request_id("client-ping")
    return request


def build_insert_request(args):
    request = mini2_pb2.InsertRequest()
    request.request_id = args.request_id or make_request_id("client-insert")

    request.record.id = args.record_id
    request.record.created_date = args.created_date
    request.record.closed_date = args.closed_date if args.closed_date is not None else 0
    request.record.agency_id = args.agency_id
    request.record.problem_id = args.problem_id
    request.record.status_id = args.status_id
    request.record.borough_id = args.borough_id
    request.record.zip_code = args.zip_code if args.zip_code is not None else 0
    request.record.latitude = args.latitude if args.latitude is not None else 0.0
    request.record.longitude = args.longitude if args.longitude is not None else 0.0
    return request


def build_delete_request(args):
    request = mini2_pb2.DeleteRequest()
    request.request_id = args.request_id or make_request_id("client-delete")

    has_filter = False

    if args.record_id is not None:
        request.record_id = args.record_id
        has_filter = True
    if args.created_date is not None:
        request.created_date = args.created_date
        has_filter = True
    if args.closed_date is not None:
        request.closed_date = args.closed_date
        has_filter = True
    if args.agency_id is not None:
        request.agency_id = args.agency_id
        has_filter = True
    if args.problem_id is not None:
        request.problem_id = args.problem_id
        has_filter = True
    if args.status_id is not None:
        request.status_id = args.status_id
        has_filter = True
    if args.borough_id is not None:
        request.borough_id = args.borough_id
        has_filter = True
    if args.zip_code is not None:
        request.zip_code = args.zip_code
        has_filter = True
    if args.latitude is not None:
        request.latitude = args.latitude
        has_filter = True
    if args.longitude is not None:
        request.longitude = args.longitude
        has_filter = True

    if args.all and has_filter:
        raise ValueError("--all cannot be combined with other delete filters")
    if not args.all and not has_filter:
        raise ValueError("Delete requires at least one filter, or pass --all")

    request.delete_all = args.all
    return request


def build_count_created_date_range_request(args):
    request = mini2_pb2.SOACountRequest()
    request.request_id = args.request_id or make_request_id("client-soa-count")
    request.kind = mini2_pb2.SOA_COUNT_CREATED_DATE_RANGE
    request.created_date_start = args.created_date_start
    request.created_date_end = args.created_date_end
    return request


def build_count_by_agency_created_date_range_request(args):
    request = mini2_pb2.SOACountRequest()
    request.request_id = args.request_id or make_request_id("client-soa-count")
    request.kind = mini2_pb2.SOA_COUNT_BY_AGENCY_AND_CREATED_DATE_RANGE
    request.agency_id = args.agency_id
    request.created_date_start = args.created_date_start
    request.created_date_end = args.created_date_end
    return request


def build_count_by_status_created_date_range_request(args):
    request = mini2_pb2.SOACountRequest()
    request.request_id = args.request_id or make_request_id("client-soa-count")
    request.kind = mini2_pb2.SOA_COUNT_BY_STATUS_AND_CREATED_DATE_RANGE
    request.created_date_start = args.created_date_start
    request.created_date_end = args.created_date_end
    request.status_id = args.status_id if args.status_id is not None else 0
    return request


def print_query_request(request):
    print("query request:")
    print(f"   request_id = {request.request_id}")
    if request.HasField("agency_id"):
        print(f"   agency_id = {request.agency_id}")
    if request.HasField("borough_id"):
        print(f"   borough_id = {request.borough_id}")
    if request.HasField("zip_code"):
        print(f"   zip_code = {request.zip_code}")
    if request.HasField("lat_min"):
        print(f"   lat_min = {request.lat_min}")
    if request.HasField("lat_max"):
        print(f"   lat_max = {request.lat_max}")
    if request.HasField("lon_min"):
        print(f"   lon_min = {request.lon_min}")
    if request.HasField("lon_max"):
        print(f"   lon_max = {request.lon_max}")
    if request.HasField("chunk_size"):
        print(f"   chunk_size = {request.chunk_size}")


def print_query_response(label, response, elapsed_ms):
    print(f"{label} response:")
    print(f"   response_request_id = {response.request_id}")
    print(f"   from_node = {response.from_node}")
    print(f"   records_returned = {len(response.records)}")
    print(f"   {label}_rtt_ms = {elapsed_ms:.2f}")


def print_ping_response(label, response, elapsed_ms):
    print(f"{label} response:")
    print(f"   response_request_id = {response.request_id}")
    print("   active nodes: ", end="")
    for node in response.active_nodes:
        print(f"{node} ", end="")
    print(f"\n   {label}_rtt_ms = {elapsed_ms:.2f}")


def print_insert_request(request):
    print("insert request:")
    print(f"   request_id = {request.request_id}")
    print(f"   record_id = {request.record.id}")
    print(f"   created_date = {request.record.created_date}")
    print(f"   agency_id = {request.record.agency_id}")
    print(f"   problem_id = {request.record.problem_id}")
    print(f"   status_id = {request.record.status_id}")
    print(f"   borough_id = {request.record.borough_id}")
    print(f"   zip_code = {request.record.zip_code}")


def print_insert_response(response, elapsed_ms):
    print("insert response:")
    print(f"   response_request_id = {response.request_id}")
    print(f"   from_node = {response.from_node}")
    print(f"   stored_at_node = {response.stored_at_node}")
    print(f"   inserted = {'true' if response.inserted else 'false'}")
    print(f"   insert_rtt_ms = {elapsed_ms:.2f}")


def print_delete_request(request):
    print("delete request:")
    print(f"   request_id = {request.request_id}")
    print(f"   delete_all = {request.delete_all}")
    if request.HasField("record_id"):
        print(f"   record_id = {request.record_id}")
    if request.HasField("created_date"):
        print(f"   created_date = {request.created_date}")
    if request.HasField("closed_date"):
        print(f"   closed_date = {request.closed_date}")
    if request.HasField("agency_id"):
        print(f"   agency_id = {request.agency_id}")
    if request.HasField("problem_id"):
        print(f"   problem_id = {request.problem_id}")
    if request.HasField("status_id"):
        print(f"   status_id = {request.status_id}")
    if request.HasField("borough_id"):
        print(f"   borough_id = {request.borough_id}")
    if request.HasField("zip_code"):
        print(f"   zip_code = {request.zip_code}")
    if request.HasField("latitude"):
        print(f"   latitude = {request.latitude}")
    if request.HasField("longitude"):
        print(f"   longitude = {request.longitude}")


def print_delete_response(response, elapsed_ms):
    print("delete response:")
    print(f"   response_request_id = {response.request_id}")
    total_deleted = 0
    for node_count in sorted(response.node_counts, key=lambda item: item.node_id):
        print(f"   node {node_count.node_id} deleted = {node_count.deleted_count}")
        total_deleted += node_count.deleted_count
    print(f"   total_deleted = {total_deleted}")
    print(f"   delete_rtt_ms = {elapsed_ms:.2f}")


def print_chunk_session_response(response, elapsed_ms):
    print("forward-chunked session:")
    print(f"   response_request_id = {response.request_id}")
    print(f"   session_id = {response.session_id}")
    print(f"   from_node = {response.from_node}")
    print(f"   chunk_size = {response.chunk_size}")
    print(f"   total_chunks = {response.total_chunks}")
    print(f"   total_records = {response.total_records}")
    print(f"   start_forward_chunks_rtt_ms = {elapsed_ms:.2f}")


def print_chunk_response(response, elapsed_ms):
    print("forward chunk:")
    print(f"   session_id = {response.session_id}")
    print(f"   chunk_index = {response.chunk_index}")
    print(f"   records_returned = {len(response.records)}")
    print(f"   total_chunks = {response.total_chunks}")
    print(f"   done = {response.done}")
    print(f"   get_forward_chunk_rtt_ms = {elapsed_ms:.2f}")


def print_count_response(response, elapsed_ms):
    print("SOA count response:")
    print(f"   response_request_id = {response.request_id}")
    print(f"   from_node = {response.from_node}")
    print(f"   count = {response.count}")
    print(f"   count_query_rtt_ms = {elapsed_ms:.2f}")


def run_query(stub, args, executor):
    request = build_query_request(args)
    print_query_request(request)

    response, query_ms = submit_unary_rpc(
        executor, stub.Query, request, args.timeout
    ).result()

    print_query_response("query", response, query_ms)


def run_forward(stub, args, executor):
    request = build_query_request(args)
    print_query_request(request)

    response, forward_ms = submit_unary_rpc(
        executor, stub.Forward, request, args.timeout
    ).result()

    print_query_response("forward", response, forward_ms)


def run_insert(stub, args, executor):
    request = build_insert_request(args)
    print_insert_request(request)

    response, insert_ms = submit_unary_rpc(
        executor, stub.Insert, request, args.timeout
    ).result()

    print_insert_response(response, insert_ms)


def run_delete(stub, args, executor):
    request = build_delete_request(args)
    print_delete_request(request)

    response, delete_ms = submit_unary_rpc(
        executor, stub.Delete, request, args.timeout
    ).result()

    print_delete_response(response, delete_ms)


def run_forward_chunked(stub, args, executor):
    request = build_query_request(args)
    print_query_request(request)

    session_response, session_ms = submit_unary_rpc(
        executor, stub.StartForwardChunks, request, args.timeout
    ).result()
    print_chunk_session_response(session_response, session_ms)

    chunk_futures = []
    for chunk_index in range(session_response.total_chunks):
        chunk_request = mini2_pb2.ChunkRequest(
            session_id=session_response.session_id,
            chunk_index=chunk_index,
        )
        chunk_futures.append(
            submit_unary_rpc(executor, stub.GetForwardChunk, chunk_request, args.timeout)
        )

    total_records_received = 0
    for future in chunk_futures:
        chunk_response, chunk_ms = future.result()

        total_records_received += len(chunk_response.records)
        print_chunk_response(chunk_response, chunk_ms)

        if chunk_response.done:
            break

    cancel_request = mini2_pb2.ChunkCancelRequest(session_id=session_response.session_id)
    cancel_response, _ = submit_unary_rpc(
        executor, stub.CancelChunks, cancel_request, args.timeout
    ).result()

    print("forward-chunked response:")
    print(f"   records_received = {total_records_received}")
    print(f"   session_cancelled = {cancel_response.cancelled}")


def run_count_created_date_range(stub, args, executor):
    request = build_count_created_date_range_request(args)
    print("SOA Count Created Date Range request: ")
    print(f"   request_id = {request.request_id}")
    print(f"   created_date_start = {request.created_date_start}")
    print(f"   created_date_end = {request.created_date_end}")

    response, count_ms = submit_unary_rpc(
        executor, stub.CountQuery, request, args.timeout
    ).result()
    print_count_response(response, count_ms)


def run_count_by_agency_and_created_date_range(stub, args, executor):
    request = build_count_by_agency_created_date_range_request(args)
    print("SOA Count By Agency And Created Date Range request: ")
    print(f"   request_id = {request.request_id}")
    print(f"   agency_id = {request.agency_id}")
    print(f"   created_date_start = {request.created_date_start}")
    print(f"   created_date_end = {request.created_date_end}")

    response, count_ms = submit_unary_rpc(
        executor, stub.CountQuery, request, args.timeout
    ).result()
    print_count_response(response, count_ms)


def run_count_by_status_and_created_date_range(stub, args, executor):
    request = build_count_by_status_created_date_range_request(args)
    print("SOA Count By Status And Created Date Range request: ")
    print(f"   request_id = {request.request_id}")
    print(f"   created_date_start = {request.created_date_start}")
    print(f"   created_date_end = {request.created_date_end}")

    response, count_ms = submit_unary_rpc(
        executor, stub.CountQuery, request, args.timeout
    ).result()
    print_count_response(response, count_ms)


def run():
    args = parse_args()
    start_total = time.perf_counter()

    channel = None
    try:
        channel, stub, connect_ms = connect(args.server, args.timeout)

        print("client:")
        print(f"   server = {args.server}")
        print(f"   command = {args.command}")
        print(f"   connect_time_ms = {connect_ms:.2f}")

        with ThreadPoolExecutor(max_workers=8) as executor:
            if args.command == "ping":
                run_ping(stub, args, executor)
            elif args.command == "query":
                run_query(stub, args, executor)
            elif args.command == "forward":
                run_forward(stub, args, executor)
            elif args.command == "insert":
                run_insert(stub, args, executor)
            elif args.command == "delete":
                run_delete(stub, args, executor)
            elif args.command == "forward-chunked":
                run_forward_chunked(stub, args, executor)
            elif args.command == "count-created-date-range":
                run_count_created_date_range(stub, args, executor)
            elif args.command == "count-by-agency-and-created-date-range":
                run_count_by_agency_and_created_date_range(stub, args, executor)
            elif args.command == "count-by-status-and-created-date-range":
                run_count_by_status_and_created_date_range(stub, args, executor)
            else:
                raise ValueError(f"Unknown command: {args.command}")

        total_ms = (time.perf_counter() - start_total) * 1000
        print(f"   total_time_ms = {total_ms:.2f}")
    except grpc.RpcError as exc:
        print(f"\nRPC failed: {exc.code().name} - {exc.details()}\n", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"\nFailed to connect/send request: {exc}\n", file=sys.stderr)
        return 1
    finally:
        if channel is not None:
            channel.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(run())

# Run command examples:
# python3 client_py/client.py -s localhost:50051 ping
# python3 client_py/client.py -s localhost:50051 query --agency-id 1
# python3 client_py/client.py -s localhost:50051 forward --agency-id 1
# python3 client_py/client.py -s localhost:50051 insert --record-id 1 --created-date 1770249600 --agency-id 1 --problem-id 2 --status-id 0 --borough-id 3
# python3 client_py/client.py -s localhost:50051 forward-chunked --agency-id 1 --chunk-size 500
# python3 client_py/client.py -s localhost:50051 count-created-date-range --created-date-start 1770249600 --created-date-end 1770335999
# Deletion
# python3 client_py/client.py -s localhost:50051 delete --record-id 910001234
# python3 client_py/client.py -s localhost:50051 delete --zip-code 11215 --borough-id 3
# python3 client_py/client.py -s localhost:50051 delete --all       # Delete all records