#
# client to our c++ server.
#
# Reading:
# https://grpc.io/docs/languages/python/basics/

import argparse
import sys
import time
import uuid
from pathlib import Path

import grpc
from google.protobuf.empty_pb2 import Empty


CLIENT_DIR = Path(__file__).resolve().parent
if str(CLIENT_DIR) not in sys.path:
    sys.path.insert(0, str(CLIENT_DIR))

import mini2_pb2
import mini2_pb2_grpc


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

    subparsers.add_parser("ping", help="Send Ping to the server")

    query_parser = subparsers.add_parser("query", help="Send QueryRequest to the server")
    query_parser.add_argument(
        "--request-id",
        default=None,
        help="Request ID for the query. If omitted, a random one is generated.",
    )
    query_parser.add_argument("--agency-id", type=int, default=None)
    query_parser.add_argument("--borough-id", type=int, default=None)
    query_parser.add_argument("--zip-code", type=int, default=None)
    query_parser.add_argument("--lat-min", type=float, default=None)
    query_parser.add_argument("--lat-max", type=float, default=None)
    query_parser.add_argument("--lon-min", type=float, default=None)
    query_parser.add_argument("--lon-max", type=float, default=None)

    forward_parser = subparsers.add_parser("forward", help="Send Forward request to the server")
    forward_parser.add_argument(
        "--request-id",
        default=None,
        help="Request ID for the forward request. If omitted, a random one is generated.",
    )
    forward_parser.add_argument("--agency-id", type=int, default=None)
    forward_parser.add_argument("--borough-id", type=int, default=None)
    forward_parser.add_argument("--zip-code", type=int, default=None)
    forward_parser.add_argument("--lat-min", type=float, default=None)
    forward_parser.add_argument("--lat-max", type=float, default=None)
    forward_parser.add_argument("--lon-min", type=float, default=None)
    forward_parser.add_argument("--lon-max", type=float, default=None)

    return parser.parse_args()


def connect(server_address: str, timeout: float):
    start_connect = time.perf_counter()
    channel = grpc.insecure_channel(server_address)
    grpc.channel_ready_future(channel).result(timeout=timeout)
    connect_ms = (time.perf_counter() - start_connect) * 1000
    stub = mini2_pb2_grpc.NodeServiceStub(channel)
    return channel, stub, connect_ms


def run_ping(stub, timeout: float):
    start_ping = time.perf_counter()
    response = stub.Ping(Empty(), timeout=timeout)
    ping_ms = (time.perf_counter() - start_ping) * 1000

    print("ping result:")
    print("   status = success")
    print(f"   ping_rtt_ms = {ping_ms:.2f}")

    dir(response)


def build_query_request(args):
    request = mini2_pb2.QueryRequest()
    request.request_id = args.request_id or f"client-query-{uuid.uuid4().hex[:8]}"

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


def print_query_response(label: str, response, elapsed_ms: float):
    print(f"{label} response:")
    print(f"   response_request_id = {response.request_id}")
    print(f"   from_node = {response.from_node}")
    print(f"   records_returned = {len(response.records)}")
    print(f"   {label}_rtt_ms = {elapsed_ms:.2f}")


def run_query(stub, args):
    request = build_query_request(args)
    print_query_request(request)

    start_query = time.perf_counter()
    response = stub.Query(request, timeout=args.timeout)
    query_ms = (time.perf_counter() - start_query) * 1000

    print_query_response("query", response, query_ms)


def run_forward(stub, args):
    request = build_query_request(args)
    print_query_request(request)

    start_forward = time.perf_counter()
    response = stub.Forward(request, timeout=args.timeout)
    forward_ms = (time.perf_counter() - start_forward) * 1000

    print_query_response("forward", response, forward_ms)


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

        if args.command == "ping":
            run_ping(stub, args.timeout)
        elif args.command == "query":
            run_query(stub, args)
        elif args.command == "forward":
            run_forward(stub, args)

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