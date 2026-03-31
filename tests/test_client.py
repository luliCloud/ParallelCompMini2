#!/usr/bin/env python3
"""
E2E Test Suite for Mini2 gRPC Server
Tests basic server functionality: Ping, Query, Forward
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
    from google.protobuf.empty_pb2 import Empty
except ImportError as e:
    print(f"Error: Proto files not generated. Run build first.")
    print(f"Details: {e}")
    sys.exit(1)


class Mini2TestClient:
    def __init__(self, server_address="localhost:50051", timeout=5):
        self.server_address = server_address
        self.timeout = timeout
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
                print(f"✓ Connected to {self.server_address}")
                return True
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"  Retry {attempt + 1}/{max_retries}: Waiting for server...")
                    time.sleep(retry_delay)
                else:
                    print(f"✗ Failed to connect: {e}")
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
            print(f"✓ {name}: PASSED")
            return True
        except AssertionError as e:
            self.failed += 1
            print(f"✗ {name}: FAILED - {e}")
            return False
        except Exception as e:
            self.failed += 1
            print(f"✗ {name}: ERROR - {e}")
            return False

    # ====================================================================
    # Test Cases
    # ====================================================================

    def test_ping(self):
        """Test Ping RPC"""
        print("\n[TEST 1] Ping Request")
        
        def run_test():
            response = self.stub.Ping(Empty(), timeout=self.timeout)
            assert response is not None, "Response is None"
            print("  Request: Empty()")
            print("  Response: OK")

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
        """Test Query with geographic filter"""
        print("\n[TEST 5] Query Request (geographic filter)")
        
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
            print(f"  Geographic bounds: lat=[{request.lat_min}, {request.lat_max}], "
                  f"lon=[{request.lon_min}, {request.lon_max}]")
            print(f"  Response from node: {response.from_node}")
            print(f"  Records returned: {len(response.records)}")

        self.test("Query (geographic)", run_test)

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

    def run_all_tests(self):
        """Run all test cases"""
        print("=" * 60)
        print("Mini2 gRPC Server - E2E Test Suite")
        print("=" * 60)

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
        finally:
            self.close()

        # Print summary
        print("\n" + "=" * 60)
        print("Test Summary")
        print("=" * 60)
        print(f"Total:  {self.total}")
        print(f"Passed: {self.passed} ✓")
        print(f"Failed: {self.failed} ✗")
        
        if self.failed == 0:
            print("\n🎉 All tests passed!")
            return True
        else:
            print(f"\n⚠️  {self.failed} test(s) failed.")
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

    args = parser.parse_args()

    client = Mini2TestClient(args.server, args.timeout)
    success = client.run_all_tests()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
