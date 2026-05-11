# Mini2 — Run Steps (WSL2)

All commands run in WSL bash at `/mnt/c/Users/tatda/Desktop/mini2`.

Ports: `A=50051`, `B=50052`, `C=50053`, `D=50054`, `E=50055`, `F=50056`, `G=50057`, `H=50058`, `I=50059`.
Tree: `A → B,H,I`; `B → C,D,E`; `E → F,G`.

```bash
cd /mnt/c/Users/tatda/Desktop/mini2
```

---

## 1. Build

```bash
cmake -S . -B build-wsl -DCMAKE_BUILD_TYPE=Release
cmake --build build-wsl -j8
```

Binary: `build-wsl/bin/server`. Rebuild with the second command only.

---

## 2. Run nodes

Start a subset (e.g. `B C G I`) in the background:

```bash
mkdir -p logs
for node in B C G I; do
  nohup setsid ./build-wsl/bin/server "$node" > "logs/mini2_${node}.log" 2>&1 < /dev/null &
done
```

Replace the node list with `A B C D E F G H I` for a full local cluster.

Wait until the expected ports listen:

```bash
while [ "$(ss -lntp 2>/dev/null | grep -cE ':(50052|50053|50057|50059)\b')" -lt 4 ]; do sleep 1; done
ss -lntp 2>/dev/null | grep -E ':5005[0-9]\b'
```

Leaf nodes load ~3.35M records (~80s cold start); coordinators (A, B, E) start instantly.

Kill all running servers:

```bash
ss -lntp 2>/dev/null | grep -oE 'pid=[0-9]+' | grep -oE '[0-9]+' | sort -u | xargs -r kill
```

---

## 3. Logs

```bash
tail -f logs/mini2_A.log              # follow one
rm -f logs/mini2_*.log                # clean before restart
```

Check readiness:

```bash
grep -E 'Server listening|loaded' logs/mini2_*.log
```

---

## 4. Python client

Regenerate stubs after editing `proto/mini2.proto`:

```bash
.venv-wsl/bin/python -m grpc_tools.protoc -I proto \
  --python_out=client_py --grpc_python_out=client_py proto/mini2.proto
```

Ping the portal (use `10.0.0.16:50051` for cross-LAN):

```bash
.venv-wsl/bin/python client_py/client.py -s localhost:50051 ping
```

Expected: `active nodes: A B C D E F G H I`.

Forward a query (use `forward-chunked` for results >4 MB):

```bash
.venv-wsl/bin/python client_py/client.py -s localhost:50051 forward-chunked \
  --agency-id 1 --chunk-size 5000
```

---

## 5. Benchmark

Edit `tests/run_benchmarks.py` `BenchmarkConfig` (around line 35):

```python
server: str = "localhost:50051"      # or "10.0.0.16:50051" for cross-LAN
agency_id: int = 10
chunk_size: int = 5000                                          # used by total + concurrent
chunk_sizes: tuple[int, ...] = (500, 1000, 2000, 5000, 10000, 20000, 50000)  # sweep
```

Run + capture to a timestamped txt:

```bash
ts=$(date +%Y%m%d_%H%M%S)
out="logs/run_benchmarks_${ts}.txt"
{
  echo "(.venv-wsl) $(whoami)@$(hostname) mini2 % python3 tests/run_benchmarks.py"
  echo
  .venv-wsl/bin/python tests/run_benchmarks.py
} 2>&1 | tee "$out"
```

---

## 6. Cross-LAN peer config

Each machine owns a subtree; peers on the other side must point to its IP.

Example — this machine hosts B, C, G, I; peer machine `10.0.0.16` hosts A, D, E, F, H:

```yaml
# config/node_B.yaml on this machine
peers:
  - id: "C"
    host: "localhost"
    port: 50053
  - id: "D"
    host: "10.0.0.16"
    port: 50054
  - id: "E"
    host: "10.0.0.16"
    port: 50055
```

On the peer machine:
- `node_A.yaml` peers B, I → this machine's IP
- `node_E.yaml` peer G → this machine's IP

Verify cross-machine data flow:

```bash
# direct query on local leaves
for entry in C:50053 G:50057 I:50059; do
  port=${entry#*:}
  .venv-wsl/bin/python client_py/client.py -s "localhost:$port" \
    query --agency-id 1 --chunk-size 100000 | grep records_returned
done

# total via remote portal
.venv-wsl/bin/python client_py/client.py -s 10.0.0.16:50051 \
  forward-chunked --agency-id 1 --chunk-size 50000 | grep records_received
```

`forward total > sum(local leaves)` confirms the remote machine is contributing.
