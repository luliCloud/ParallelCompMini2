# Mini 2 — Distributed gRPC Network

A 9-node distributed gRPC cluster over the NYC 311 dataset. Node A is the
client-facing coordinator; the other nodes form a tree and own data shards.
Supports forwarding / chunked streaming, SOA analytical queries
(count / group-by / top-k), pluggable LRU/LFU result cache, FIFO or
priority job scheduling, and inserts routed to the correct shard by
`created_date`.

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

| Node | Port  | Role            | Peers     |
| ---- | ----- | --------------- | --------- |
| A    | 50051 | coordinator     | B, H, I   |
| B    | 50052 | inner           | C, D, E   |
| C    | 50053 | leaf (data)     | —         |
| D    | 50054 | leaf (data)     | —         |
| E    | 50055 | inner           | F, G      |
| F    | 50056 | leaf (data)     | —         |
| G    | 50057 | leaf (data)     | —         |
| H    | 50058 | leaf (data)     | —         |
| I    | 50059 | leaf (data)     | —         |

- **C++ server** (`build/bin/server`): runs one node per process — `./build/bin/server <id>`.
- **C++ client** (`build/bin/client`) and **Python client** (`client_py/client.py`):
  external callers, normally talk to A on port 50051.

## Repository Structure

```
mini2/
├── proto/mini2.proto              # gRPC schema
│
├── config/
│   ├── node_A.yaml … node_I.yaml  # per-node config (host, port, peers, queue, cache)
│   ├── insert_routes.yaml         # created_date → owning leaf node
│   ├── global_agency_ids.csv      # shared dictionaries (agency / problem / borough / status)
│   ├── global_problem_ids.csv
│   ├── global_borough_ids.csv
│   └── global_status_ids.csv
│
├── common/                         # shared C++ library (dataset, queries)
│   ├── include/
│   │   ├── csv_parser.hpp
│   │   ├── dataset.hpp             # AOS dataset + dictionary encoding
│   │   ├── dataset_SOA.hpp         # struct-of-arrays layout
│   │   ├── dataset_utils.hpp
│   │   ├── iQuery.hpp / query_base.hpp
│   │   ├── query_omp.hpp           # OpenMP-parallel queries
│   │   ├── query_SOA.hpp           # SOA count / group-by / top-k
│   │   └── timer.hpp
│   └── src/                        # implementations
│
├── server_cpp/
│   ├── server.cpp                  # entry point: load config, start gRPC server
│   ├── Mini2ServiceImpl.{h,cc}     # all RPC handlers
│   ├── RequestJobQueue.{h,cc}      # FIFO / priority job scheduler
│   ├── ForwardResponseCache.{h,cc} # LRU / LFU result cache
│   └── InsertRouteConfig.{h,cc}    # routes Insert to the right leaf
│
├── client_cpp/client.cpp           # C++ CLI client
├── client_py/client.py             # Python CLI client
│
├── tests/
│   ├── README.md                   # extensive client command examples
│   ├── run_cluster.sh              # bring up all 9 nodes locally
│   ├── run_test_client.sh
│   ├── test_client.py              # Ping / Query / Forward / Insert / Delete
│   ├── test_cache_demo.py          # cache hit/miss demo
│   ├── test_queue_demo.py          # FIFO vs priority scheduler demo
│   ├── benchmark_cache.py
│   ├── benchmark_chunk_sizes.py
│   └── run_benchmarks.py           # full benchmark runner (StartForwardChunks)
│
├── benchmarks/                     # input shards + workloads (gitignored CSVs)
├── tools/                          # dataset sharding helpers
└── CMakeLists.txt
```

## RPC Surface (`proto/mini2.proto`)

| RPC                  | Type   | Purpose                                                                 |
| -------------------- | ------ | ----------------------------------------------------------------------- |
| `Ping`               | unary  | Liveness — coordinator returns the list of reachable nodes.             |
| `Query`              | unary  | Filter records on the receiving node only (no fan-out).                 |
| `Forward`            | unary  | Coordinator fans out to peers, aggregates full result.                  |
| `Insert`             | unary  | Insert one record; routed to owning leaf via `insert_routes.yaml`.      |
| `Delete`             | unary  | Delete by predicate; fan-out, returns per-node delete counts.           |
| `StartForwardChunks` | unary  | Start a chunked Forward session; returns a `session_id`.                |
| `GetForwardChunk`    | unary  | Pull one chunk of a session by index.                                   |
| `CancelChunks`       | unary  | Cancel an in-flight chunked session.                                    |
| `CountQuery`         | unary  | SOA count (created-date range, by-agency, by-status).                   |
| `GroupByQuery`       | unary  | SOA group-by (borough/zipcode × complaint, agency counts, etc.).        |
| `TopKQuery`          | unary  | SOA top-K complaints in a created-date range.                           |

`QueryRequest` carries optional filters (`agency_id`, `borough_id`, `zip_code`,
`lat/lon` bbox), a `chunk_size`, and two streaming-flow flags:

- `leaf_buffered_streaming` — leaf buffers all matches, parent pulls in chunks.
- `internal_full_streaming` — every tree edge pulls chunk-by-chunk with unary RPCs.

## Record Schema (NYC 311)

| Field        | Type   | Notes                              |
| ------------ | ------ | ---------------------------------- |
| `id`         | uint32 |                                    |
| `created_date` | int64 | epoch seconds (used for sharding) |
| `closed_date`  | int64 | epoch seconds                     |
| `agency_id`  | uint32 | dict-encoded                       |
| `problem_id` | uint32 | dict-encoded                       |
| `status_id`  | uint32 | dict-encoded                       |
| `borough_id` | uint32 | dict-encoded                       |
| `zip_code`   | uint32 |                                    |
| `latitude`   | float  |                                    |
| `longitude`  | float  |                                    |

## Per-Node Configuration (`config/node_*.yaml`)

| Key                  | Values                  | Notes                                                              |
| -------------------- | ----------------------- | ------------------------------------------------------------------ |
| `node_id`            | `"A"`–`"I"`             | Logical id, must match peer references in other configs.           |
| `host`, `port`       | string, int             | Bind address.                                                      |
| `coordinator_only`   | bool (A only)           | Skips dataset load on coordinator.                                 |
| `queue_mode`         | `fifo` \| `priority`    | Job scheduler (priority uses `QueryRequest` priority class).       |
| `enable_cache`       | bool                    | Toggle `ForwardResponseCache`.                                     |
| `cache_policy`       | `lru` \| `lfu`          | Eviction policy when cache is enabled.                             |
| `cache_max_entries`  | int                     | Cache capacity.                                                    |
| `dataset_mode`       | `aos` \| `soa` \| `both`| AOS for Forward/Query/Insert; SOA for Count/GroupBy/TopK.          |
| `dataset_path`       | path                    | Per-leaf shard CSV (one shard per leaf).                           |
| `*_dict_path`        | path                    | Shared dictionary CSVs in `config/`.                               |
| `peers`              | list of `{id,host,port}`| Children in the tree topology.                                     |

`config/insert_routes.yaml` maps `created_date` ranges to the owning leaf
(default node handles records past the last threshold). Edit this when you
re-shard the dataset.

## Build

Dependencies (macOS via Homebrew shown):

```sh
brew install grpc protobuf yaml-cpp cmake
# OpenMP is optional; on macOS:
brew install libomp
```

Build:

```sh
cmake -S . -B build
cmake --build build -j
# Outputs:
#   build/bin/server   — pass node id as argv[1]
#   build/bin/client   — C++ CLI client
```

## Running the Cluster

### All-in-one helper (local, all 9 nodes)

```sh
./tests/run_cluster.sh        # logs in /tmp/mini2_cluster_logs
```

### Manual (one node per process)

```sh
mkdir -p logs
for n in A B C D E F G H I; do
  nohup ./build/bin/server $n > logs/node_$n.log 2>&1 &
  echo $! > logs/node_$n.pid
done
# Wait for all 9 ports to be LISTEN (50051–50059):
until [ "$(lsof -nP -iTCP:50051 -iTCP:50052 -iTCP:50053 -iTCP:50054 \
  -iTCP:50055 -iTCP:50056 -iTCP:50057 -iTCP:50058 -iTCP:50059 \
  -sTCP:LISTEN 2>/dev/null | grep -c LISTEN)" = "9" ]; do sleep 3; done
```

Stop:

```sh
kill $(cat logs/node_*.pid) 2>/dev/null
```

## Client Examples

### C++ client (`build/bin/client`)

```sh
# Liveness
./build/bin/client -s localhost:50051 ping

# Local query (A has no data → 0)
./build/bin/client -s localhost:50051 query --request-id q-local

# Fan-out filters
./build/bin/client -s localhost:50051 forward --request-id f-all
./build/bin/client -s localhost:50051 forward --agency-id 10  --request-id f-agency
./build/bin/client -s localhost:50051 forward --borough-id 1  --request-id f-borough
./build/bin/client -s localhost:50051 forward \
  --lat-min 40.7 --lat-max 40.8 --lon-min -74.0 --lon-max -73.9 \
  --request-id f-geo

# SOA analytics (require dataset_mode soa or both on leaves)
./build/bin/client -s localhost:50051 count-created-date-range \
  --created-date-start 1577836800 --created-date-end 1609459199 \
  --request-id count-2020

./build/bin/client -s localhost:50051 count-by-agency-and-created-date-range \
  --agency-id 10 --created-date-start 1577836800 --created-date-end 1609459199 \
  --request-id count-agency10-2020

./build/bin/client -s localhost:50051 top-k-complaints \
  --created-date-start 1577836800 --created-date-end 1609459199 --top-k 10 \
  --request-id topk-2020

# Insert / Delete
./build/bin/client -s localhost:50051 insert --record '{...}'
./build/bin/client -s localhost:50051 delete --agency-id 10
```

See [tests/README.md](tests/README.md) for a comprehensive command reference
with example outputs.

### Python client (`client_py/client.py`)

```sh
pip install -r client_py/requirements.txt
python client_py/client.py -s localhost:50051 ping
python client_py/client.py -s localhost:50051 forward --agency-id 10 --request-id f-agency
```

## Tests & Benchmarks

| Script                              | Purpose                                                 |
| ----------------------------------- | ------------------------------------------------------- |
| `tests/test_client.py`              | Smoke tests — Ping/Query/Forward/Insert/Delete.         |
| `tests/test_cache_demo.py`          | Demonstrates LRU/LFU cache hit/miss behaviour.          |
| `tests/test_queue_demo.py`          | Demonstrates FIFO vs priority scheduling.               |
| `tests/run_benchmarks.py`           | Full StartForwardChunks benchmark suite.                |
| `tests/benchmark_cache.py`          | Cache-focused benchmark.                                |
| `tests/benchmark_chunk_sizes.py`    | Sweep of `chunk_size` values.                           |

Run a benchmark, for example:

```sh
python tests/run_benchmarks.py --help
```

## Sharding Helpers (`tools/`)

- `split_time_shards.py` — split a CSV by `created_date` into N shards.
- `split_sorted_311_time_shards.py` — same, requires presorted input (faster).
- `analyze_sharding.py` — print shard sizes / boundaries.

After re-sharding, update `config/insert_routes.yaml` and the per-leaf
`dataset_path` to match the new boundaries.

## Phases

| Phase        | Scope                                                     |
| ------------ | --------------------------------------------------------- |
| 1 — Basecamp | All 9 nodes start, `Ping` works across the tree.          |
| 2 — Data     | `Query` / `Forward` fan-out, chunked streaming responses. |
| 3 — Fairness | Concurrent clients, `CancelChunks`, FIFO/priority queue.  |
| 4 — Analytics| SOA `Count` / `GroupBy` / `TopK` with shared dictionaries.|
