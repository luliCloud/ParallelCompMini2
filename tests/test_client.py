#!/usr/bin/env python3
"""
E2E Test Suite for Mini2 gRPC Server
Tests basic server functionality: Ping, Query, Forward, Insert
"""

import grpc
import sys
import time
import argparse
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "client_py"))

try:
    import mini2_pb2
    import mini2_pb2_grpc
except ImportError as e:
    print(f"Error: Proto files not generated. Run build first.")
    print(f"Details: {e}")
    sys.exit(1)


class Mini2TestClient:
    def __init__(self, server_address="localhost:50051", timeout=5, expected_nodes=None):
        self.server_address = server_address
        self.timeout = timeout
        self.expected_nodes = set(expected_nodes or [])
        self.channel = None
        self.stub = None
        self.passed = 0
        self.failed = 0
        self.total = 0

    def connect(self):
        """Connect to server with retry logic"""
        max_retries = 10
        retry_delay = 0.5

        for attempt in range(max_retries):
            try:
                self.channel = grpc.insecure_channel(self.server_address)
                # Try a simple call to verify connection
                self.stub = mini2_pb2_grpc.NodeServiceStub(self.channel)
                grpc.channel_ready_future(self.channel).result(timeout=1)
                print(f"Connected to server: {self.server_address}")
                return True
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Retry {attempt + 1}/{max_retries}: waiting for server...")
                    time.sleep(retry_delay)
                else:
                    print(f"Failed to connect: {e}")
                    return False
        return False

    def close(self):
        """Close connection"""
        if self.channel:
            self.channel.close()

    def test(self, name, test_func):
        """Run a test and track results"""
        self.total += 1
        try:
            test_func()
            self.passed += 1
            print(f"PASS: {name}")
            return True
        except AssertionError as e:
            self.failed += 1
            print(f"FAIL: {name} - {e}")
            return False
        except Exception as e:
            self.failed += 1
            print(f"ERROR: {name} - {e}")
            return False

    # ====================================================================
    # Test Cases
    # ====================================================================

    def test_ping(self):
        """Test Ping RPC"""
        print("\n[TEST 1] Ping Request")
        
        def run_test():
            request = mini2_pb2.PingRequest()
            request.request_id = "test_ping_001"

            response = self.stub.Ping(request, timeout=self.timeout)
            assert response is not None, "Response is None"
            assert response.request_id == request.request_id, "Request ID mismatch"

            active_nodes = list(response.active_nodes)
            active_node_set = set(active_nodes)
            assert active_nodes, "active_nodes is empty"
            assert len(active_nodes) == len(active_node_set), (
                f"Duplicate nodes in ping response: {active_nodes}"
            )

            if self.expected_nodes:
                missing = sorted(self.expected_nodes - active_node_set)
                unexpected = sorted(active_node_set - self.expected_nodes)
                assert not missing, f"Missing nodes in ping response: {missing}"
                assert not unexpected, f"Unexpected nodes in ping response: {unexpected}"

            print(f"  Request ID: {request.request_id}")
            print(f"  Active nodes: {', '.join(sorted(active_node_set))}")

        self.test("Ping", run_test)

    def test_query_empty(self):
        """Test Query with no filters"""
        print("\n[TEST 2] Query Request (no filters)")
        
        def run_test():
            request = mini2_pb2.QueryRequest()
            request.request_id = "test_query_001"
            
            response = self.stub.Query(request, timeout=self.timeout)
            
            assert response is not None, "Response is None"
            assert response.request_id == "test_query_001", "Request ID mismatch"
            assert response.from_node != "", "from_node is empty"
            
            print(f"  Request ID: {request.request_id}")
            print(f"  Response from node: {response.from_node}")
            print(f"  Records returned: {len(response.records)}")

        self.test("Query (empty)", run_test)

    def test_query_with_borough(self):
        """Test Query with borough filter"""
        print("\n[TEST 3] Query Request (borough filter)")
        
        def run_test():
            request = mini2_pb2.QueryRequest()
            request.request_id = "test_query_borough_001"
            request.borough_id = 1
            
            response = self.stub.Query(request, timeout=self.timeout)
            
            assert response is not None, "Response is None"
            assert response.request_id == "test_query_borough_001", "Request ID mismatch"
            
            print(f"  Request ID: {request.request_id}")
            print(f"  Filter: borough_id = {request.borough_id}")
            print(f"  Response from node: {response.from_node}")
            print(f"  Records returned: {len(response.records)}")

        self.test("Query (borough)", run_test)

    def test_query_with_agency(self):
        """Test Query with agency filter"""
        print("\n[TEST 4] Query Request (agency filter)")
        
        def run_test():
            request = mini2_pb2.QueryRequest()
            request.request_id = "test_query_agency_001"
            request.agency_id = 10
            
            response = self.stub.Query(request, timeout=self.timeout)
            
            assert response is not None, "Response is None"
            assert response.request_id == "test_query_agency_001", "Request ID mismatch"
            
            print(f"  Request ID: {request.request_id}")
            print(f"  Filter: agency_id = {request.agency_id}")
            print(f"  Response from node: {response.from_node}")
            print(f"  Records returned: {len(response.records)}")

        self.test("Query (agency)", run_test)

    def test_query_geographic(self):
        """Test Query with latitude/longitude filter"""
        print("\n[TEST 5] Query Request (latitude/longitude filter)")
        
        def run_test():
            request = mini2_pb2.QueryRequest()
            request.request_id = "test_query_geo_001"
            request.lat_min = 40.7
            request.lat_max = 40.8
            request.lon_min = -74.0
            request.lon_max = -73.9
            
            response = self.stub.Query(request, timeout=self.timeout)
            
            assert response is not None, "Response is None"
            assert response.request_id == "test_query_geo_001", "Request ID mismatch"
            
            print(f"  Request ID: {request.request_id}")
            print(f"  Latitude filter:  [{request.lat_min}, {request.lat_max}]")
            print(f"  Longitude filter: [{request.lon_min}, {request.lon_max}]")
            print(f"  Response from node: {response.from_node}")
            print(f"  Records returned: {len(response.records)}")

        self.test("Query (latitude/longitude)", run_test)

    def test_forward(self):
        """Test Forward RPC"""
        print("\n[TEST 6] Forward Request")
        
        def run_test():
            request = mini2_pb2.QueryRequest()
            request.request_id = "test_forward_001"
            request.borough_id = 2
            
            response = self.stub.Forward(request, timeout=self.timeout)
            
            assert response is not None, "Response is None"
            assert response.request_id == "test_forward_001", "Request ID mismatch"
            
            print(f"  Request ID: {request.request_id}")
            print(f"  Response from node: {response.from_node}")
            print(f"  Records returned: {len(response.records)}")

        self.test("Forward", run_test)

    def test_insert(self):
        """Test Insert RPC and verify with Forward"""
        print("\n[TEST 7] Insert Request")

        def run_test():
            suffix = time.time_ns() % 100000
            record_id = 900000000 + suffix
            zip_code = 98000 + (suffix % 1000)
            created_date = 1770631498
            expected_node = "G"

            before_request = mini2_pb2.QueryRequest()
            before_request.request_id = f"test_insert_before_{suffix}"
            before_request.zip_code = zip_code
            before_response = self.stub.Forward(before_request, timeout=self.timeout)
            before_count = len(before_response.records)

            insert_request = mini2_pb2.InsertRequest()
            insert_request.request_id = f"test_insert_{suffix}"
            insert_request.record.id = record_id
            insert_request.record.created_date = created_date
            insert_request.record.closed_date = 0
            insert_request.record.agency_id = 0
            insert_request.record.problem_id = 0
            insert_request.record.status_id = 0
            insert_request.record.borough_id = 0
            insert_request.record.zip_code = zip_code
            insert_request.record.latitude = 40.7
            insert_request.record.longitude = -73.9

            insert_response = self.stub.Insert(insert_request, timeout=self.timeout)

            assert insert_response is not None, "Insert response is None"
            assert insert_response.request_id == insert_request.request_id, (
                "Insert request ID mismatch"
            )
            assert insert_response.inserted, "Insert response inserted is false"
            assert insert_response.stored_at_node == expected_node, (
                f"Record stored at {insert_response.stored_at_node}, "
                f"expected {expected_node}"
            )

            after_request = mini2_pb2.QueryRequest()
            after_request.request_id = f"test_insert_after_{suffix}"
            after_request.zip_code = zip_code
            after_response = self.stub.Forward(after_request, timeout=self.timeout)
            after_count = len(after_response.records)

            assert after_count == before_count + 1, (
                f"Expected {before_count + 1} records after insert, got {after_count}"
            )

            print(f"  Insert request ID: {insert_request.request_id}")
            print(f"  Record ID: {record_id}")
            print(f"  Created date: {created_date}")
            print(f"  Zip code: {zip_code}")
            print(f"  Stored at node: {insert_response.stored_at_node}")
            print(f"  Records before insert: {before_count}")
            print(f"  Records after insert: {after_count}")

        self.test("Insert", run_test)

    def run_all_tests(self):
        """Run all test cases"""
        print("Mini2 gRPC Server - E2E Test Suite")

        if not self.connect():
            print("\nCannot proceed without server connection.")
            return False

        try:
            self.test_ping()
            self.test_query_empty()
            self.test_query_with_borough()
            self.test_query_with_agency()
            self.test_query_geographic()
            self.test_forward()
            self.test_insert()
        finally:
            self.close()

        # Print summary
        print("")
        print("Test Summary")
        print(f"Total:  {self.total}")
        print(f"Passed: {self.passed}")
        print(f"Failed: {self.failed}")
        
        if self.failed == 0:
            print("\nResult: all tests passed.")
            return True
        else:
            print(f"\nResult: {self.failed} test(s) failed.")
            return False


def main():
    parser = argparse.ArgumentParser(
        description="E2E Test Suite for Mini2 gRPC Server"
    )
    parser.add_argument(
        "-s", "--server",
        default="localhost:50051",
        help="Server address (default: localhost:50051)"
    )
    parser.add_argument(
        "-t", "--timeout",
        type=int,
        default=5,
        help="Request timeout in seconds (default: 5)"
    )
    parser.add_argument(
        "--expected-nodes",
        default="A,B,C,D,E,F,G,H,I",
        help="Comma-separated list of nodes expected in Ping response "
             "(default: A,B,C,D,E,F,G,H,I)"
    )

    args = parser.parse_args()

    expected_nodes = [
        node.strip() for node in args.expected_nodes.split(",") if node.strip()
    ]
    client = Mini2TestClient(args.server, args.timeout, expected_nodes)
    success = client.run_all_tests()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
