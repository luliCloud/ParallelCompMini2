#!/usr/bin/env python3
"""
Simple queue test for Mini2.

What this file does:
1. Send 3 Forward requests to node A at the same time.
2. Print each response clearly.
3. Read node A log file.
4. Print queue lines for the same requests.

This file assumes the Mini2 cluster is already running.
"""

import argparse
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
CLIENT_BINARY = ROOT_DIR / "build-wsl" / "bin" / "client"


class ForwardResult:
    def __init__(
        self,
        request_id: str,
        zip_code: int,
        exit_code: int,
        stdout: str,
        stderr: str,
        elapsed_milliseconds: float,
    ):
        self.request_id = request_id
        self.zip_code = zip_code
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        self.elapsed_milliseconds = elapsed_milliseconds


def build_forward_command(server_address: str, request_id: str, zip_code: int):
    return [
        str(CLIENT_BINARY),
        "-s",
        server_address,
        "forward",
        "--request-id",
        request_id,
        "--zip-code",
        str(zip_code),
    ]


def run_forward_request(server_address: str, timeout_seconds: float, request_id: str, zip_code: int):
    command = build_forward_command(server_address, request_id, zip_code)

    start_time = time.perf_counter()
    completed_process = subprocess.run(
        command,
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout_seconds + 2,
    )
    elapsed_milliseconds = (time.perf_counter() - start_time) * 1000

    return ForwardResult(
        request_id=request_id,
        zip_code=zip_code,
        exit_code=completed_process.returncode,
        stdout=completed_process.stdout,
        stderr=completed_process.stderr,
        elapsed_milliseconds=elapsed_milliseconds,
    )


def get_value_from_client_output(output_text: str, field_name: str):
    prefix = f"{field_name} = "

    for line in output_text.splitlines():
        stripped_line = line.strip()
        if stripped_line.startswith(prefix):
            return stripped_line[len(prefix):]

    return "not found"


def print_result(result: ForwardResult):
    print(f"Request id: {result.request_id}")
    print(f"Zip code: {result.zip_code}")
    print(f"Exit code: {result.exit_code}")
    print(f"From node: {get_value_from_client_output(result.stdout, 'from_node')}")
    print(
        f"Records returned: "
        f"{get_value_from_client_output(result.stdout, 'records_returned')}"
    )
    print(f"Round trip time ms: {result.elapsed_milliseconds:.2f}")

    if result.stderr.strip():
        print("Error output:")
        print(result.stderr.strip())

    print("")


def clean_log_line(raw_line: str):
    cleaned_line = raw_line.replace("[A]", "Node A")
    cleaned_line = cleaned_line.replace(":", "")
    cleaned_line = cleaned_line.replace("(", "")
    cleaned_line = cleaned_line.replace(")", "")
    cleaned_line = " ".join(cleaned_line.split())

    node_position = cleaned_line.find("Node A")
    if node_position >= 0:
        cleaned_line = cleaned_line[node_position:]

    return cleaned_line


def read_queue_log_lines(log_path: Path, request_ids: list[str]):
    if not log_path.exists():
        return []

    log_lines = []
    wanted_parts = [
        "enqueued Forward request",
        "worker processing Forward request",
        "worker completed Forward request",
    ]

    with log_path.open("r", encoding="utf-8", errors="replace") as log_file:
        for raw_line in log_file:
            line = raw_line.rstrip("\n")

            has_request_id = False
            for request_id in request_ids:
                if request_id in line:
                    has_request_id = True
                    break

            if not has_request_id:
                continue

            has_queue_text = False
            for wanted_text in wanted_parts:
                if wanted_text in line:
                    has_queue_text = True
                    break

            if has_queue_text:
                log_lines.append(clean_log_line(line))

    return log_lines


def run_requests_in_parallel(server_address: str, timeout_seconds: float, request_cases):
    results = []

    with ThreadPoolExecutor(max_workers=len(request_cases)) as executor:
        future_list = []

        for request_id, zip_code in request_cases:
            future = executor.submit(
                run_forward_request,
                server_address,
                timeout_seconds,
                request_id,
                zip_code,
            )
            future_list.append(future)

        for future in future_list:
            results.append(future.result())

    results.sort(key=lambda item: item.request_id)
    return results


def print_log_lines(log_lines: list[str]):
    print("Node A log lines")

    if not log_lines:
        print("No matching log lines found")
        return

    for line in log_lines:
        print(line)


def main():
    parser = argparse.ArgumentParser(description="Simple queue test")
    parser.add_argument("-s", "--server", default="127.0.0.1:50051")
    parser.add_argument("-t", "--timeout", type=float, default=5.0)
    parser.add_argument(
        "--log-path",
        default=str(ROOT_DIR / "logs" / "mini2_A.log"),
        help="Path to node A log file.",
    )
    args = parser.parse_args()

    request_cases = [
        ("queue-demo-001", 10033),
        ("queue-demo-002", 10010),
        ("queue-demo-003", 10453),
    ]

    print("Queue test")
    print(f"Server: {args.server}")
    print("This test sends 3 forward requests at the same time")
    print("")

    results = run_requests_in_parallel(args.server, args.timeout, request_cases)

    print("Responses")
    print("")
    for result in results:
        print_result(result)

    time.sleep(0.5)

    request_ids = []
    for request_id, _zip_code in request_cases:
        request_ids.append(request_id)

    log_lines = read_queue_log_lines(Path(args.log_path), request_ids)
    print_log_lines(log_lines)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
