# Mini2 Streaming Test Commands

Start the 9-node cluster first:

```bash
./tests/run_cluster.sh

# single-node test (C only, full data). Need change yaml config to point to node_C.yaml and dataset.csv
./build/bin/server C
```

Run the following commands from the project root.

## Chunk Size Benchmark

Benchmark elapsed time across multiple chunk sizes. By default, this uses
`--quiet-chunks` to reduce terminal printing overhead and writes a CSV.

Agency, all three modes:

```bash
python3 tests/benchmark_chunk_sizes.py \
  --query agency \
  --chunk-sizes 500,1000,2000,5000,10000,20000,50000 \
  --output tests/chunk_size_agency.csv
```

Borough, streaming modes only:

```bash
python3 tests/benchmark_chunk_sizes.py \
  --query borough \
  --mode stream-pure \
  --mode stream-leaf \
  --chunk-sizes 500,1000,2000,5000,10000,20000,50000 \
  --output tests/chunk_size_borough_streaming.csv
```

Geo, leaf-vector streaming only (main tests):

```bash
python3 tests/benchmark_chunk_sizes.py \
  --query geo \
  --mode stream-leaf \
  --chunk-sizes 50,50,100,250,500,1000,2000,5000,10000,20000,50000 \
  --output tests/chunk_size_geo_leaf.csv

  # result
  running query=geo mode=stream-leaf chunk_size=50 repeat=1
  records=3607904 chunks=72160 total_ms=9259.44
running query=geo mode=stream-leaf chunk_size=50 repeat=1
  records=3607904 chunks=72160 total_ms=8953.74
running query=geo mode=stream-leaf chunk_size=100 repeat=1
  records=3607904 chunks=36083 total_ms=8381.07
running query=geo mode=stream-leaf chunk_size=250 repeat=1
  records=3607904 chunks=14434 total_ms=7731.50
running query=geo mode=stream-leaf chunk_size=500 repeat=1
  records=3607904 chunks=7219 total_ms=7621.81
running query=geo mode=stream-leaf chunk_size=1000 repeat=1
  records=3607904 chunks=3611 total_ms=7482.74
running query=geo mode=stream-leaf chunk_size=2000 repeat=1
  records=3607904 chunks=1807 total_ms=7252.41
running query=geo mode=stream-leaf chunk_size=5000 repeat=1
  records=3607904 chunks=724 total_ms=7229.30
running query=geo mode=stream-leaf chunk_size=10000 repeat=1
  records=3607904 chunks=364 total_ms=7124.38
running query=geo mode=stream-leaf chunk_size=20000 repeat=1
  records=3607904 chunks=184 total_ms=7449.12
running query=geo mode=stream-leaf chunk_size=50000 repeat=1
  records=3607904 chunks=76 total_ms=7375.96
wrote tests/chunk_size_geo_leaf.csv

python3 tests/benchmark_chunk_sizes.py \
  --query borough \
  --mode stream-leaf \
  --chunk-sizes 50,50,100,250,500,1000,2000,5000,10000,20000,50000 \
  --output tests/chunk_size_borough_leaf.csv

running query=borough mode=stream-leaf chunk_size=50 repeat=1
  records=4311938 chunks=86242 total_ms=12090.71
running query=borough mode=stream-leaf chunk_size=50 repeat=1
  records=4311938 chunks=86242 total_ms=11409.76
running query=borough mode=stream-leaf chunk_size=100 repeat=1
  records=4311938 chunks=43123 total_ms=9617.53
running query=borough mode=stream-leaf chunk_size=250 repeat=1
  records=4311938 chunks=17251 total_ms=9390.08
running query=borough mode=stream-leaf chunk_size=500 repeat=1
  records=4311938 chunks=8627 total_ms=8871.03
running query=borough mode=stream-leaf chunk_size=1000 repeat=1
  records=4311938 chunks=4315 total_ms=8790.38
running query=borough mode=stream-leaf chunk_size=2000 repeat=1
  records=4311938 chunks=2160 total_ms=9143.01
running query=borough mode=stream-leaf chunk_size=5000 repeat=1
  records=4311938 chunks=866 total_ms=8744.51
running query=borough mode=stream-leaf chunk_size=10000 repeat=1
  records=4311938 chunks=435 total_ms=8604.32
running query=borough mode=stream-leaf chunk_size=20000 repeat=1
  records=4311938 chunks=219 total_ms=8547.50
running query=borough mode=stream-leaf chunk_size=50000 repeat=1
  records=4311938 chunks=90 total_ms=8608.15
wrote tests/chunk_size_borough_leaf.csv

python3 tests/benchmark_chunk_sizes.py \
  --query agency \
  --mode stream-leaf \
  --chunk-sizes 50,50,100,250,500,1000,2000,5000,10000,20000,50000 \
  --output tests/chunk_size_agency_leaf.csv
running query=agency mode=stream-leaf chunk_size=50 repeat=1
  records=172402 chunks=3451 total_ms=778.85
running query=agency mode=stream-leaf chunk_size=50 repeat=1
  records=172402 chunks=3451 total_ms=630.82
running query=agency mode=stream-leaf chunk_size=100 repeat=1
  records=172402 chunks=1727 total_ms=575.73
running query=agency mode=stream-leaf chunk_size=250 repeat=1
  records=172402 chunks=693 total_ms=540.39
running query=agency mode=stream-leaf chunk_size=500 repeat=1
  records=172402 chunks=348 total_ms=534.15
running query=agency mode=stream-leaf chunk_size=1000 repeat=1
  records=172402 chunks=176 total_ms=515.27
running query=agency mode=stream-leaf chunk_size=2000 repeat=1
  records=172402 chunks=89 total_ms=514.66
running query=agency mode=stream-leaf chunk_size=5000 repeat=1
  records=172402 chunks=38 total_ms=521.17
running query=agency mode=stream-leaf chunk_size=10000 repeat=1
  records=172402 chunks=20 total_ms=520.11
running query=agency mode=stream-leaf chunk_size=20000 repeat=1
  records=172402 chunks=11 total_ms=509.77
running query=agency mode=stream-leaf chunk_size=50000 repeat=1
  records=172402 chunks=7 total_ms=517.20
wrote tests/chunk_size_agency_leaf.csv
```

Add `--print-chunks` if you want the client to print every chunk during the
benchmark.

## Agency Query

### Old chunked pull (cannot get full result due to timeout)

```bash
./build/bin/client -s localhost:50051 -t 120 forward-chunked \
  --agency-id 10 \
  --chunk-size 2000 \
  --request-id agency-chunked
# full result (maybe not exceed 64MB gRPC message limit):
#forward-chunked response:
#   records_received = 172402
#   session_cancelled = 1
#   total_time_ms = 236.12

# single node
./build/bin/client -s localhost:50053 -t 120 forward-chunked \
  --agency-id 10 \
  --chunk-size 2000 \
  --request-id agency-chunked-single
```

### Pure end-to-end streaming (can't get full result for 9 nodes, but can get full result for single node)

```bash
./build/bin/client -s localhost:50051 -t 120 forward-stream \
  --agency-id 10 \
  --chunk-size 2000 \
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
  --chunk-size 2000 \
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
  --chunk-size 2000 \
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
  --chunk-size 2000 \
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
  --chunk-size 2000 \
  --request-id borough-chunked
  # partial results
  #forward-chunked response:
  # records_received = 1497106
  # session_cancelled = 1
  # total_time_ms = 1474.05

# single node
./build/bin/client -s localhost:50053 -t 120 forward-chunked \
    --borough-id 1 \    
    --chunk-size 2000 \
    --request-id borough-chunked-single
```

### Pure end-to-end streaming

```bash
./build/bin/client -s localhost:50051 -t 120 forward-stream \
  --borough-id 1 \
  --chunk-size 2000 \
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
  --chunk-size 2000 \
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
  --chunk-size 2000 \
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
  --chunk-size 2000 \
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
  --chunk-size 2000 \
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
  --chunk-size 2000 \
  --request-id geo-chunked-single

```

### Pure end-to-end streaming

```bash
./build/bin/client -s localhost:50051 -t 120 forward-stream \
  --lat-min 40.7 \
  --lat-max 40.8 \
  --lon-min -74.0 \
  --lon-max -73.9 \
  --chunk-size 2000 \
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
  --chunk-size 2000 \
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
  --chunk-size 2000 \
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
  --chunk-size 2000 \
  --leaf-buffered-streaming \
  --request-id geo-stream-leaf

# successful response example:
# forward-stream response:
#   chunks_received = 722
#   records_received = 3607904
#   forward_stream_ms = 30483.96
#   total_time_ms = 30487.75

```
