# Mini 2 — Distributed gRPC Network

## Repository Structure

```
mini2/
│
├── proto/
│   └── mini2.proto                  # gRPC schema (shared by C++ and Python)
│
├── config/
│   ├── node_A.yaml                  # port 50051 — portal, peers: B H I
│   ├── node_B.yaml                  # port 50052 — peers: C D E
│   ├── node_C.yaml                  # port 50053 — leaf
│   ├── node_D.yaml                  # port 50054 — leaf
│   ├── node_E.yaml                  # port 50055 — peers: F G
│   ├── node_F.yaml                  # port 50056 — leaf
│   ├── node_G.yaml                  # port 50057 — leaf
│   ├── node_H.yaml                  # port 50058 — leaf
│   └── node_I.yaml                  # port 50059 — leaf
│
├── data/
│   └── dataset.csv                  # NYC 311 dataset (single shared file)
│
├── common/
│   ├── include/
│   │   ├── csv_parser.hpp           # read CSV row by row
│   │   ├── dataset.hpp              # load full dataset + dict encoding
│   │   ├── dataset_SOA.hpp          # struct-of-arrays layout
│   │   ├── dataset_utils.hpp        # parse helpers (datetime, uint, float)
│   │   ├── iQuery.hpp               # query interface
│   │   ├── query_base.hpp           # base query implementation
│   │   ├── query_omp.hpp            # OpenMP parallel query
│   │   ├── query_SOA.hpp            # SOA query implementation
│   │   └── timer.hpp                # timing utility
│   └── src/
│       ├── csv_parser.cpp
│       ├── dataset.cpp
│       ├── dataset_SOA.cpp
│       ├── query_base.cpp
│       ├── query_omp.cpp
│       └── query_SOA.cpp
│
├── server_cpp/                      # C++ gRPC server — runs all 9 nodes
│   ├── CMakeLists.txt               # build with gRPC + protobuf + yaml-cpp
│   └── server.cpp                   # Phase 1: Ping
│                                    # Phase 2: Query, Forward, chunked response
│                                    # Phase 3: Cancel, fairness scheduler
│
├── client_py/                       # Python gRPC client — connects to A only
│   ├── requirements.txt
│   ├── client.py                    # Phase 1: Ping test
│   │                                # Phase 2: Query + paginate chunks
│   │                                # Phase 3: Cancel, concurrent requests
│   ├── mini2_pb2.py
│   └── mini2_pb2_grpc.py
│
├── tests/
│   ├── test_phase1_ping.py          # ping all 9 nodes, verify all respond
│   ├── test_phase2_query.py         # send query to A, verify fan-out + response
│   └── test_phase3_fairness.py      # concurrent clients, cancel mid-request
│
├── .gitignore
└── README.md
```

## Tree Overlay

```
           A  (portal — port 50051, receives client queries, no data)
         / | \
        B  H  I
       /|\
      C D E
         / \
        F   G
```

- **C++ server**: all 9 nodes (A, B, C, D, E, F, G, H, I)
- **Python client**: external caller, connects only to A (port 50051)

## Record Schema (NYC 311 dataset)

| Field        | Type   |
| ------------ | ------ |
| id           | uint32 |
| created_date | int64  |
| closed_date  | int64  |
| agency_id    | uint32 |
| problem_id   | uint32 |
| status_id    | uint32 |
| borough_id   | uint32 |
| zip_code     | uint32 |
| latitude     | float  |
| longitude    | float  |

## Phases

| Phase        | What                                              | Key files                                         |
| ------------ | ------------------------------------------------- | ------------------------------------------------- |
| 1 — Basecamp | All 9 nodes start, Ping works across tree         | `server.cpp` Ping, `test_phase1_ping.py`          |
| 2 — Data     | Query fan-out A→peers, chunked response to client | `server.cpp` Query/Forward, `client.py`           |
| 3 — Fairness | Concurrent clients, cancel, dynamic chunk size    | `server.cpp` scheduler, `test_phase3_fairness.py` |
