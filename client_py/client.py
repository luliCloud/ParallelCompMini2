#
# client to our c++ server.
#
# Reading:
# https://grpc.io/docs/languages/python/basics/

import sys
import time
from pathlib import Path

import grpc

from google.protobuf.empty_pb2 import Empty


CLIENT_DIR = Path(__file__).resolve().parent
if str(CLIENT_DIR) not in sys.path:
    sys.path.insert(0, str(CLIENT_DIR))

import mini2_pb2_grpc


SERVER_ADDRESS = "localhost:50051"
TIMEOUT_SECONDS = 5.0


def run():
    try:
        start_total = time.perf_counter()

        channel = grpc.insecure_channel(SERVER_ADDRESS)
        stub = mini2_pb2_grpc.NodeServiceStub(channel)

        start_connect = time.perf_counter()
        grpc.channel_ready_future(channel).result(timeout=TIMEOUT_SECONDS)
        connect_ms = (time.perf_counter() - start_connect) * 1000

        print("\nping test:")
        print(f"   target node = A")
        print(f"   server = {SERVER_ADDRESS}")

        start_ping = time.perf_counter()
        response = stub.Ping(Empty(), timeout=TIMEOUT_SECONDS)
        ping_ms = (time.perf_counter() - start_ping) * 1000
        dir(response)  # should there be one?

        total_ms = (time.perf_counter() - start_total) * 1000

        print("   ping = success")
        print(f"   connect_time_ms = {connect_ms:.2f}")
        print(f"   ping_rtt_ms = {ping_ms:.2f}")
        print(f"   total_time_ms = {total_ms:.2f}")

        channel.close()
    except:
        print("\n\n** failed to connect/send to node A **\n\n")


if __name__ == "__main__":
    run()
