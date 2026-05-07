# Mini2 Streaming Test Commands

Start the 9-node cluster first:

```bash
./tests/run_cluster.sh

# single-node test (C only, full data). Need change yaml config to point to node_C.yaml and dataset.csv
./build/bin/server C
```

Run the following commands from the project root.

## Agency Query

### Old chunked pull (cannot get full result due to timeout)

```bash
./build/bin/client -s localhost:50051 -t 120 forward-chunked \
  --agency-id 10 \
  --chunk-size 5000 \
  --request-id agency-chunked
# full result (maybe not exceed 64MB gRPC message limit):
#forward-chunked response:
#   records_received = 172402
#   session_cancelled = 1
#   total_time_ms = 236.12

# single node
./build/bin/client -s localhost:50053 -t 120 forward-chunked \
  --agency-id 10 \
  --chunk-size 5000 \
  --request-id agency-chunked-single
```

### Pure end-to-end streaming (can't get full result for 9 nodes, but can get full result for single node)

```bash
./build/bin/client -s localhost:50051 -t 120 forward-stream \
  --agency-id 10 \
  --chunk-size 5000 \
  --request-id agency-stream-pure
# example result: 
#forward-stream response:
#   chunks_received = 38
#   records_received = 172402
#   forward_stream_ms = 38600.38
#   total_time_ms = 38604.34

# single node
./build/bin/client -s localhost:50053 -t 120 forward-stream \
  --agency-id 10 \
  --chunk-size 5000 \
  --request-id agency-stream-pure-single  

#forward-stream chunk:
#   from_node = C
#   chunk_index = 28
#   records_returned = 5000
#   done = 0
#   elapsed_ms = 117014.05 
# Failed to connect/send request: RPC failed: 4 - Deadline Exceeded    
```

### Leaf-vector streaming

```bash
./build/bin/client -s localhost:50051 -t 120 forward-stream \
  --agency-id 10 \
  --chunk-size 5000 \
  --leaf-buffered-streaming \
  --request-id agency-stream-leaf
# successful response example:
# forward-stream response:
#   chunks_received = 38
#   records_received = 172402
#   forward_stream_ms = 629.10
#   total_time_ms = 632.71

# single node
./build/bin/client -s localhost:50053 -t 120 forward-stream \
  --agency-id 10 \
  --chunk-size 5000 \
  --leaf-buffered-streaming \
  --request-id agency-stream-leaf-single

# successful response example:
# forward-stream response:
#   chunks_received = 35
#   records_received = 172402
#   forward_stream_ms = 1677.03
#   total_time_ms = 1681.16
```

## Borough Query

### Old chunked pull

```bash
./build/bin/client -s localhost:50051 -t 120 forward-chunked \
  --borough-id 1 \
  --chunk-size 5000 \
  --request-id borough-chunked
  # partial results
  #forward-chunked response:
  # records_received = 1497106
  # session_cancelled = 1
  # total_time_ms = 1474.05

# single node
./build/bin/client -s localhost:50053 -t 120 forward-chunked \
    --borough-id 1 \    
    --chunk-size 5000 \
    --request-id borough-chunked-single
```

### Pure end-to-end streaming

```bash
./build/bin/client -s localhost:50051 -t 120 forward-stream \
  --borough-id 1 \
  --chunk-size 5000 \
  --request-id borough-stream-pure
# successful response example:
#forward-stream response:
#   chunks_received = 866
#   records_received = 4311938
#   forward_stream_ms = 38741.94
#   total_time_ms = 38745.56

# single node
./build/bin/client -s localhost:50053 -t 120 forward-stream \
  --borough-id 1 \
  --chunk-size 5000 \
  --request-id borough-stream-pure-single

# partial successful response example:
#forward-stream chunk:
#   from_node = C
#   chunk_index = 642
#   records_returned = 5000
#   done = 0
#   elapsed_ms = 119834.87
#Failed to connect/send request: RPC failed: 4 - Deadline Exceeded
```

### Leaf-vector streaming

```bash
./build/bin/client -s localhost:50051 -t 120 forward-stream \
  --borough-id 1 \
  --chunk-size 5000 \
  --leaf-buffered-streaming \
  --request-id borough-stream-leaf
  # successful response example:
  #forward-stream response:
  # chunks_received = 866
  # records_received = 4311938
  # forward_stream_ms = 9588.37
  # total_time_ms = 9591.89

# single node
./build/bin/client -s localhost:50053 -t 120 forward-stream \
  --borough-id 1 \
  --chunk-size 5000 \
  --leaf-buffered-streaming \
  --request-id borough-stream-leaf-single

# successful response example:
#forward-stream response:
#   chunks_received = 863
#   records_received = 4311938
#   forward_stream_ms = 37319.13
#   total_time_ms = 37322.99   
```

## Geo Query

### Old chunked pull

```bash
./build/bin/client -s localhost:50051 -t 120 forward-chunked \
  --lat-min 40.7 \
  --lat-max 40.8 \
  --lon-min -74.0 \
  --lon-max -73.9 \
  --chunk-size 5000 \
  --request-id geo-chunked
# partial result
#forward-chunked response:
#   records_received = 1221955
#   session_cancelled = 1
#   total_time_ms = 1122.61

# single node
./build/bin/client -s localhost:50053 -t 120 forward-chunked \
  --lat-min 40.7 \
  --lat-max 40.8 \
  --lon-min -74.0 \
  --lon-max -73.9 \
  --chunk-size 5000 \
  --request-id geo-chunked-single

```

### Pure end-to-end streaming

```bash
./build/bin/client -s localhost:50051 -t 120 forward-stream \
  --lat-min 40.7 \
  --lat-max 40.8 \
  --lon-min -74.0 \
  --lon-max -73.9 \
  --chunk-size 5000 \
  --request-id geo-stream-pure

# successful response example: (longer than leaf streaming.)
#forward-stream response:
#   chunks_received = 724
#   records_received = 3607904
#   forward_stream_ms = 38738.43
#   total_time_ms = 38741.46

# single node
./build/bin/client -s localhost:50053 -t 120 forward-stream \
  --lat-min 40.7 \
  --lat-max 40.8 \
  --lon-min -74.0 \
  --lon-max -73.9 \
  --chunk-size 5000 \
  --request-id geo-stream-pure

# forward-stream chunk:
#   from_node = C
#   chunk_index = 544
#   records_returned = 5000
#   done = 0
#   elapsed_ms = 119862.91
#Failed to connect/send request: RPC failed: 4 - Deadline Exceeded

```

### Leaf-vector streaming

```bash
./build/bin/client -s localhost:50051 -t 120 forward-stream \
  --lat-min 40.7 \
  --lat-max 40.8 \
  --lon-min -74.0 \
  --lon-max -73.9 \
  --chunk-size 5000 \
  --leaf-buffered-streaming \
  --request-id geo-stream-leaf
  # forward-stream response:
# chunks_received = 724
#   records_received = 3607904
#   forward_stream_ms = 7818.64
#   total_time_ms = 7822.48

# single node
./build/bin/client -s localhost:50053 -t 120 forward-stream \
  --lat-min 40.7 \
  --lat-max 40.8 \
  --lon-min -74.0 \
  --lon-max -73.9 \
  --chunk-size 5000 \
  --leaf-buffered-streaming \
  --request-id geo-stream-leaf

# successful response example:
# forward-stream response:
#   chunks_received = 722
#   records_received = 3607904
#   forward_stream_ms = 30483.96
#   total_time_ms = 30487.75

```
