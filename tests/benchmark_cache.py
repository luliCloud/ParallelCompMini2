#!/usr/bin/env python3
import argparse
import subprocess
import time
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
CLIENT = ROOT_DIR / "build-wsl" / "bin" / "client"
SERVER = ROOT_DIR / "build-wsl" / "bin" / "server"
NODE_A_CONFIG = ROOT_DIR / "config" / "node_A.yaml"
LOG_DIR = ROOT_DIR / "logs"
NODES = ["A", "B", "C", "D", "E", "F", "G", "H", "I"]


def get_output_value(output_text, field_name):
    prefix = f"{field_name} = "

    for line in output_text.splitlines():
        line = line.strip()
        if line.startswith(prefix):
            return line[len(prefix):]

    return ""


def run_forward(server, timeout, request_id, filter_args):
    command = [
        str(CLIENT),
        "-s",
        server,
        "-t",
        str(timeout),
        "forward",
        "--request-id",
        request_id,
    ] + filter_args

    start_time = time.perf_counter()
    result = subprocess.run(
        command,
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout + 5,
    )
    process_time_ms = (time.perf_counter() - start_time) * 1000

    if result.returncode != 0:
        print(f"Request failed: {request_id}")
        print(result.stdout.strip())
        print(result.stderr.strip())
        raise SystemExit(1)

    records_text = get_output_value(result.stdout, "records_returned")
    forward_time_text = get_output_value(result.stdout, "forward_rtt_ms")

    records = int(records_text) if records_text else -1
    forward_time_ms = float(forward_time_text) if forward_time_text else process_time_ms

    return records, forward_time_ms


def set_node_a_cache(enabled):
    lines = NODE_A_CONFIG.read_text(encoding="utf-8").splitlines()
    new_lines = []

    for line in lines:
        if line.strip().startswith("enable_cache:"):
            new_lines.append(f"enable_cache: {'true' if enabled else 'false'}")
        else:
            new_lines.append(line)

    NODE_A_CONFIG.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def stop_cluster(processes):
    for process in processes:
        process.terminate()

    for process in processes:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def wait_for_node(node_id, process, timeout):
    log_path = LOG_DIR / f"mini2_{node_id}.log"
    start_time = time.time()

    while time.time() - start_time < timeout:
        if log_path.exists() and "Server listening" in log_path.read_text(errors="replace"):
            return

        if process.poll() is not None:
            print(f"Node {node_id} stopped early")
            if log_path.exists():
                print(log_path.read_text(errors="replace")[-2000:])
            raise SystemExit(1)

        time.sleep(0.2)

    print(f"Node {node_id} did not start in time")
    if log_path.exists():
        print(log_path.read_text(errors="replace")[-2000:])
    raise SystemExit(1)


def start_cluster(timeout):
    subprocess.run(["pkill", "-f", "build-wsl/bin/server"], check=False)
    time.sleep(0.5)

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    for log_path in LOG_DIR.glob("mini2_*.log"):
        log_path.unlink()

    processes = []
    for node_id in NODES:
        log_file = (LOG_DIR / f"mini2_{node_id}.log").open("w", encoding="utf-8")
        process = subprocess.Popen(
            [str(SERVER), node_id],
            cwd=ROOT_DIR,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        processes.append(process)

    try:
        for node_id, process in zip(NODES, processes):
            wait_for_node(node_id, process, timeout)
    except BaseException:
        stop_cluster(processes)
        raise

    return processes


def average(values):
    return sum(values) / len(values)


def get_cases(include_large):
    cases = [
        ("zip_code_11203", "zip_code=11203", ["--zip-code", "11203"]),
        ("zip_code_10033", "zip_code=10033", ["--zip-code", "10033"]),
        ("zip_code_10453", "zip_code=10453", ["--zip-code", "10453"]),
    ]

    if include_large:
        cases += [
            ("borough_brooklyn", "borough_id=0", ["--borough-id", "0"]),
            ("agency_nypd", "agency_id=0", ["--agency-id", "0"]),
            ("no_filter", "all_records", []),
        ]

    return cases


def run_cache_on_case(server, timeout, hit_runs, case_name, filter_args):
    run_id = str(int(time.time() * 1000))
    total_time_ms = 0.0
    records, miss_time_ms = run_forward(
        server,
        timeout,
        f"cache-on-{run_id}-{case_name}-miss",
        filter_args,
    )
    total_time_ms += miss_time_ms

    hit_times = []
    for hit_number in range(1, hit_runs + 1):
        _, hit_time_ms = run_forward(
            server,
            timeout,
            f"cache-on-{run_id}-{case_name}-hit-{hit_number}",
            filter_args,
        )
        hit_times.append(hit_time_ms)
        total_time_ms += hit_time_ms

    return records, miss_time_ms, average(hit_times), total_time_ms


def run_cache_off_case(server, timeout, hit_runs, case_name, filter_args):
    run_id = str(int(time.time() * 1000))
    times = []
    total_time_ms = 0.0
    records = -1

    total_runs = hit_runs + 1
    for run_number in range(1, total_runs + 1):
        records, no_cache_time_ms = run_forward(
            server,
            timeout,
            f"cache-off-{run_id}-{case_name}-{run_number}",
            filter_args,
        )
        times.append(no_cache_time_ms)
        total_time_ms += no_cache_time_ms

    return records, average(times), total_time_ms


def main():
    parser = argparse.ArgumentParser(description="Cache benchmark")
    parser.add_argument("-s", "--server", default="127.0.0.1:50051")
    parser.add_argument("-t", "--timeout", type=float, default=10.0)
    parser.add_argument("--hit-runs", type=int, default=3)
    parser.add_argument("--include-large", action="store_true")
    args = parser.parse_args()

    if not CLIENT.exists() or not SERVER.exists():
        print("Build first: cmake --build build-wsl -j4")
        return 1

    cases = get_cases(args.include_large)
    cache_results = {}
    no_cache_results = {}
    original_node_a_config = NODE_A_CONFIG.read_bytes()

    try:
        print("Running with cache on")
        set_node_a_cache(True)
        processes = start_cluster(args.timeout)
        try:
            for case_name, _, filter_args in cases:
                cache_results[case_name] = run_cache_on_case(
                    args.server,
                    args.timeout,
                    args.hit_runs,
                    case_name,
                    filter_args,
                )
        finally:
            stop_cluster(processes)

        print("Running with cache off")
        set_node_a_cache(False)
        processes = start_cluster(args.timeout)
        try:
            for case_name, _, filter_args in cases:
                no_cache_results[case_name] = run_cache_off_case(
                    args.server,
                    args.timeout,
                    args.hit_runs,
                    case_name,
                    filter_args,
                )
        finally:
            stop_cluster(processes)
    finally:
        NODE_A_CONFIG.write_bytes(original_node_a_config)

    print("")
    print("Cache enabled")
    request_count = args.hit_runs + 1
    miss_column = "1_miss_ms"
    hit_column = f"{args.hit_runs}_hit_avg_ms"
    print(
        f"{'case':<18} "
        f"{'request':<16} "
        f"{'requests':>8} "
        f"{'records':>8} "
        f"{miss_column:>10} "
        f"{hit_column:>14} "
        f"{'total_ms':>10}"
    )
    for case_name, request_label, _ in cases:
        records, miss_ms, hit_ms, cache_total_ms = cache_results[case_name]
        print(
            f"{case_name:<18} "
            f"{request_label:<16} "
            f"{request_count:>8} "
            f"{records:>8} "
            f"{miss_ms:>10.2f} "
            f"{hit_ms:>14.2f} "
            f"{cache_total_ms:>10.2f}"
        )

    print("")
    print("Cache disabled")
    no_cache_column = f"{request_count}_no_cache_avg_ms"
    print(
        f"{'case':<18} "
        f"{'request':<16} "
        f"{'requests':>8} "
        f"{'records':>8} "
        f"{no_cache_column:>20} "
        f"{'total_ms':>10}"
    )
    for case_name, request_label, _ in cases:
        records, no_cache_ms, no_cache_total_ms = no_cache_results[case_name]
        print(
            f"{case_name:<18} "
            f"{request_label:<16} "
            f"{request_count:>8} "
            f"{records:>8} "
            f"{no_cache_ms:>20.2f} "
            f"{no_cache_total_ms:>10.2f}"
        )

    print("")
    print("Final comparison")
    no_cache_column = f"{request_count}_no_cache_avg_ms"
    print(
        f"{'case':<18} "
        f"{'requests':>8} "
        f"{'cache_total_ms':>14} "
        f"{'no_cache_total_ms':>17} "
        f"{'total_saved_ms':>14} "
        f"{'miss_vs_hit':>11}"
    )

    for case_name, _, _ in cases:
        _, miss_ms, hit_ms, cache_total_ms = cache_results[case_name]
        _, no_cache_ms, no_cache_total_ms = no_cache_results[case_name]
        cache_speedup = miss_ms / hit_ms if hit_ms > 0 else 0
        total_saved_ms = no_cache_total_ms - cache_total_ms

        print(
            f"{case_name:<18} "
            f"{request_count:>8} "
            f"{cache_total_ms:>14.2f} "
            f"{no_cache_total_ms:>17.2f} "
            f"{total_saved_ms:>14.2f} "
            f"{cache_speedup:>10.2f}x"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
