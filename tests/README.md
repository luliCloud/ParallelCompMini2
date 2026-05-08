# Mini2 Streaming Test Commands

Start the 9-node cluster first:

```bash
./tests/run_cluster.sh

# single-node test (C only, full data). Need change yaml config to point to node_C.yaml and dataset.csv
./build/bin/server C
```

Run the following commands from the project root.

## SOA Related Function Tests

These functions require data nodes to load SOA data. Use `dataset_mode: "soa"`
or `dataset_mode: "both"` in the leaf node YAML files before starting the
cluster. Keep the global dictionary paths enabled on every data node so
multi-node results use the same categorical ids:

```yaml
agency_dict_path: "config/global_agency_ids.csv"
problem_dict_path: "config/global_problem_ids.csv"
borough_dict_path: "config/global_borough_ids.csv"
status_dict_path: "config/global_status_ids.csv"
```

### Count by created-date range

```bash
./build/bin/client -s localhost:50051 -t 120 count-created-date-range \
  --created-date-start 1577836800 \
  --created-date-end 1609459199 \
  --request-id count-created-2020

SOA count response:
   response_request_id = count-created-2020
   from_node = A
   count = 2939532
   count_query_rtt_ms = 292.43
   total_time_ms = 295.94

# single node (C only, full data)
./build/bin/client -s localhost:50053 -t 120 count-created-date-range \
  --created-date-start 1577836800 \
  --created-date-end 1609459199 \
  --request-id count-created-2020

SOA count response:
   response_request_id = count-created-2020
   from_node = C
   count = 2939532
   count_query_rtt_ms = 406.68
   total_time_ms = 409.92
```

### Count by agency and created-date range

```bash
./build/bin/client -s localhost:50051 -t 120 count-by-agency-and-created-date-range \
  --agency-id 10 \
  --created-date-start 1577836800 \
  --created-date-end 1609459199 \
  --request-id count-agency10-2020

SOA count response:
   response_request_id = count-agency10-2020
   from_node = A
   count = 10296
   count_query_rtt_ms = 334.01
   total_time_ms = 338.24

# single node (C only, full data)
./build/bin/client -s localhost:50053 -t 120 count-by-agency-and-created-date-range \
  --agency-id 10 \
  --created-date-start 1577836800 \
  --created-date-end 1609459199 \
  --request-id count-agency10-2020-single
  SOA count response:
   response_request_id = count-agency10-2020-single
   from_node = C
   count = 10296
   count_query_rtt_ms = 440.45
   total_time_ms = 443.79
```

### Count by status and created-date range

```bash
./build/bin/client -s localhost:50051 -t 120 count-by-status-and-created-date-range \
  --status-id 1 \
  --created-date-start 1577836800 \
  --created-date-end 1609459199 \
  --request-id count-status1-2020

SOA count response:
   response_request_id = count-status1-2020
   from_node = A
   count = 2881953
   count_query_rtt_ms = 303.48
   total_time_ms = 306.89

# single node (C only, full data)
./build/bin/client -s localhost:50053 -t 120 count-by-status-and-created-date-range \
  --status-id 1 \
  --created-date-start 1577836800 \
  --created-date-end 1609459199 \
  --request-id count-status1-2020-single

SOA count response:
   response_request_id = count-status1-2020-single
   from_node = C
   count = 2881953
   count_query_rtt_ms = 399.82
   total_time_ms = 403.29
```

### Group by borough summary count in created-date range

```bash
# multi-node cluster
./build/bin/client -s localhost:50051 -t 120 group-by-borough-created-date-range \
  --created-date-start 1577836800 \
  --created-date-end 1609459199 \
  --request-id groupby-borough-2020

# single node (C only, full data)
./build/bin/client -s localhost:50053 -t 120 group-by-borough-created-date-range \
  --created-date-start 1577836800 \
  --created-date-end 1609459199 \
  --request-id groupby-borough-2020-single
```

### Top-k complaint/problem types in created-date range

```bash
# multi-node cluster
./build/bin/client -s localhost:50051 -t 120 top-k-complaints \
  --created-date-start 1577836800 \
  --created-date-end 1609459199 \
  --top-k 10 \
  --request-id topk-complaints-2020

from_node = A
   entries_returned = 10
   problem_id = 156 count = 406378
   problem_id = 191 count = 281969
   problem_id = 157 count = 206606
   problem_id = 121 count = 194082
   problem_id = 104 count = 164741
   problem_id = 19 count = 116591
   problem_id = 159 count = 83924
   problem_id = 158 count = 81137
   problem_id = 53 count = 73068
   problem_id = 222 count = 65008
   top_k_query_rtt_ms = 350.94
   total_time_ms = 356.13

# single node (C only, full data)
./build/bin/client -s localhost:50053 -t 120 top-k-complaints \
  --created-date-start 1577836800 \
  --created-date-end 1609459199 \
  --top-k 10 \
  --request-id topk-complaints-2020-single
from_node = C
   entries_returned = 10
problem_id = 156 count = 406378
   problem_id = 191 count = 281969
   problem_id = 157 count = 206606
   problem_id = 121 count = 194082
   problem_id = 104 count = 164741
   problem_id = 19 count = 116591
   problem_id = 159 count = 83924
   problem_id = 158 count = 81137
   problem_id = 53 count = 73068
   problem_id = 222 count = 65008
   top_k_query_rtt_ms = 442.39
   total_time_ms = 448.31
```

Python client version:

```bash
.venv/bin/python client_py/client.py -s localhost:50051 -t 120 top-k-complaints \
  --created-date-start 1577836800 \
  --created-date-end 1609459199 \
  --top-k 10 \
  --request-id topk-complaints-2020
```

## Streaming Mode Summary

All three modes use unary RPCs only. The gRPC streaming RPC was removed.

### 1. Old chunked pull: `forward-chunked`

- Internal tree path: unary `Forward` RPC returning one large `QueryResponse`.
- A-to-client path: pull-based chunks through `StartForwardChunks` and
  `GetForwardChunk`.
- This is the old baseline. It does not use the gRPC streaming API, but large
  internal peer responses can still exceed the gRPC message-size limit.

### 2. Internal full streaming: `forward-chunked --internal-full-streaming`

- Internal tree path: application-level chunk pull through unary RPCs:
  `StartForwardChunks`, `GetForwardChunk`, and `CancelChunks`.
- A-to-client path: the same pull-based chunk API.
- This does not use the gRPC streaming API. Each node creates a chunk session,
  pulls chunks from its children, and exposes chunks to its parent/client.
- Default leaf behavior scans local data and builds protobuf chunks while
  scanning, similar to the old pure streaming mode.

### 3. Leaf-buffered streaming: `forward-chunked --leaf-buffered-streaming`

- Internal tree path: application-level chunk pull through unary RPCs.
- Leaf behavior: first collects local matches into a `std::vector<Record>`, then converts that vector into protobuf chunks.
- Middle nodes and A still forward chunks from child sessions.
- This is the practical hybrid version without gRPC streaming RPCs.

## Chunk Size Benchmark

Benchmark elapsed time across multiple chunk sizes. By default, this uses
`--quiet-chunks` to reduce terminal printing overhead and writes a CSV.
The benchmark script uses `forward-chunked` for every mode:

- `chunked`: old internal unary `Forward` large response.
- `stream-pure`: adds `--internal-full-streaming`.
- `stream-leaf`: adds `--leaf-buffered-streaming`.

Agency, all three modes:

```bash
python3 tests/benchmark_chunk_sizes.py \
  --query agency \
  --chunk-sizes 50,50,100,200,500,1000,2000,5000,10000,20000,50000 \
  --output tests/chunk_size_agency.csv
```

Borough, all three modes:

```bash
python3 tests/benchmark_chunk_sizes.py \
  --query borough \
  --chunk-sizes 50,50,100,200,500,1000,2000,5000,10000,20000,50000 \
  --output tests/chunk_size_borough.csv
```

Geo, all three modes:

```bash
python3 tests/benchmark_chunk_sizes.py \
  --query geo \
  --chunk-sizes 50,50,100,200,500,1000,2000,5000,10000,20000,50000 \
  --output tests/chunk_size_geo.csv
```

Add `--print-chunks` if you want the client to print every chunk during the
benchmark.

## Agency Query

### 1. Old chunked pull: internal unary large response

```bash
./build/bin/client -s localhost:50051 -t 120 forward-chunked \
  --agency-id 10 \
  --chunk-size 5000 \
  --quiet-chunks \
  --request-id agency-old-chunked

# single node
./build/bin/client -s localhost:50053 -t 120 forward-chunked \
  --agency-id 10 \
  --chunk-size 5000 \
  --quiet-chunks \
  --request-id agency-old-chunked-single
```

### 2. Internal full streaming: tree-wide unary chunk pull

```bash
./build/bin/client -s localhost:50051 -t 120 forward-chunked \
  --agency-id 10 \
  --chunk-size 5000 \
  --internal-full-streaming \
  --quiet-chunks \
  --request-id agency-internal-full

# single node
./build/bin/client -s localhost:50053 -t 120 forward-chunked \
  --agency-id 10 \
  --chunk-size 5000 \
  --internal-full-streaming \
  --quiet-chunks \
  --request-id agency-internal-full-single
```

### 3. Leaf-buffered streaming: tree-wide unary chunk pull

```bash
./build/bin/client -s localhost:50051 -t 120 forward-chunked \
  --agency-id 10 \
  --chunk-size 5000 \
  --leaf-buffered-streaming \
  --quiet-chunks \
  --request-id agency-leaf-buffered

# single node
./build/bin/client -s localhost:50053 -t 120 forward-chunked \
  --agency-id 10 \
  --chunk-size 5000 \
  --leaf-buffered-streaming \
  --quiet-chunks \
  --request-id agency-leaf-buffered-single
```

## Borough Query

### 1. Old chunked pull: internal unary large response

```bash
./build/bin/client -s localhost:50051 -t 120 forward-chunked \
  --borough-id 1 \
  --chunk-size 5000 \
  --quiet-chunks \
  --request-id borough-old-chunked

# This old baseline can fail or return partial results if an internal
# QueryResponse exceeds the gRPC message-size limit.

# single node
./build/bin/client -s localhost:50053 -t 120 forward-chunked \
  --borough-id 1 \
  --chunk-size 5000 \
  --quiet-chunks \
  --request-id borough-old-chunked-single
```

### 2. Internal full streaming: tree-wide unary chunk pull

```bash
./build/bin/client -s localhost:50051 -t 120 forward-chunked \
  --borough-id 1 \
  --chunk-size 5000 \
  --internal-full-streaming \
  --quiet-chunks \
  --request-id borough-internal-full

# single node
./build/bin/client -s localhost:50053 -t 120 forward-chunked \
  --borough-id 1 \
  --chunk-size 5000 \
  --internal-full-streaming \
  --quiet-chunks \
  --request-id borough-internal-full-single
```

### 3. Leaf-buffered streaming: tree-wide unary chunk pull

```bash
./build/bin/client -s localhost:50051 -t 120 forward-chunked \
  --borough-id 1 \
  --chunk-size 5000 \
  --leaf-buffered-streaming \
  --quiet-chunks \
  --request-id borough-leaf-buffered

# single node
./build/bin/client -s localhost:50053 -t 120 forward-chunked \
  --borough-id 1 \
  --chunk-size 5000 \
  --leaf-buffered-streaming \
  --quiet-chunks \
  --request-id borough-leaf-buffered-single
```

## Geo Query

### 1. Old chunked pull: internal unary large response

```bash
./build/bin/client -s localhost:50051 -t 120 forward-chunked \
  --lat-min 40.7 \
  --lat-max 40.8 \
  --lon-min -74.0 \
  --lon-max -73.9 \
  --chunk-size 5000 \
  --quiet-chunks \
  --request-id geo-old-chunked

# This old baseline can fail or return partial results if an internal
# QueryResponse exceeds the gRPC message-size limit.

# single node
./build/bin/client -s localhost:50053 -t 120 forward-chunked \
  --lat-min 40.7 \
  --lat-max 40.8 \
  --lon-min -74.0 \
  --lon-max -73.9 \
  --chunk-size 5000 \
  --quiet-chunks \
  --request-id geo-old-chunked-single
```

### 2. Internal full streaming: tree-wide unary chunk pull

```bash
./build/bin/client -s localhost:50051 -t 120 forward-chunked \
  --lat-min 40.7 \
  --lat-max 40.8 \
  --lon-min -74.0 \
  --lon-max -73.9 \
  --chunk-size 5000 \
  --internal-full-streaming \
  --quiet-chunks \
  --request-id geo-internal-full

# single node
./build/bin/client -s localhost:50053 -t 120 forward-chunked \
  --lat-min 40.7 \
  --lat-max 40.8 \
  --lon-min -74.0 \
  --lon-max -73.9 \
  --chunk-size 5000 \
  --internal-full-streaming \
  --quiet-chunks \
  --request-id geo-internal-full-single
```

### 3. Leaf-buffered streaming: tree-wide unary chunk pull

```bash
./build/bin/client -s localhost:50051 -t 120 forward-chunked \
  --lat-min 40.7 \
  --lat-max 40.8 \
  --lon-min -74.0 \
  --lon-max -73.9 \
  --chunk-size 5000 \
  --leaf-buffered-streaming \
  --quiet-chunks \
  --request-id geo-leaf-buffered

# single node
./build/bin/client -s localhost:50053 -t 120 forward-chunked \
  --lat-min 40.7 \
  --lat-max 40.8 \
  --lon-min -74.0 \
  --lon-max -73.9 \
  --chunk-size 5000 \
  --leaf-buffered-streaming \
  --quiet-chunks \
  --request-id geo-leaf-buffered-single
```
