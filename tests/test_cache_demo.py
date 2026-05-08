#!/usr/bin/env python3
"""
Cache test for Mini2.

What this file does:
1. Send one Forward request.
2. Send the same Forward filter one more time.
3. Print both responses clearly.
4. Read node A log file.
5. Print cache lines for the same requests.

This file assumes the Mini2 cluster is already running.
"""

import argparse
import subprocess
import time
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


def print_result(title: str, result: ForwardResult):
    print(title)
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


def read_cache_log_lines(log_path: Path, request_ids: list[str]):
    if not log_path.exists():
        return []

    log_lines = []
    wanted_parts = [
        "Forward cache miss",
        "Forward cache stored",
        "Forward cache hit",
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

            has_cache_text = False
            for wanted_text in wanted_parts:
                if wanted_text in line:
                    has_cache_text = True
                    break

            if has_cache_text:
                log_lines.append(clean_log_line(line))

    return log_lines


def print_log_lines(log_lines: list[str]):
    print("Node A log lines")

    if not log_lines:
        print("No matching log lines found")
        return

    for line in log_lines:
        print(line)


def main():
    parser = argparse.ArgumentParser(description="Simple cache test")
    parser.add_argument("-s", "--server", default="127.0.0.1:50051")
    parser.add_argument("-t", "--timeout", type=float, default=5.0)
    parser.add_argument(
        "--log-path",
        default=str(ROOT_DIR / "logs" / "mini2_A.log"),
        help="Path to node A log file.",
    )
    parser.add_argument("--zip-code", type=int, default=11203)
    args = parser.parse_args()

    print("Cache test")
    print(f"Server: {args.server}")
    print(f"Zip code: {args.zip_code}")
    print("This test sends the same filter two times")
    print("")

    first_result = run_forward_request(
        args.server,
        args.timeout,
        "cache-demo-001",
        args.zip_code,
    )
    second_result = run_forward_request(
        args.server,
        args.timeout,
        "cache-demo-002",
        args.zip_code,
    )

    print_result("First request", first_result)
    print_result("Second request", second_result)

    time.sleep(0.5)

    request_ids = [first_result.request_id, second_result.request_id]
    log_lines = read_cache_log_lines(Path(args.log_path), request_ids)
    print_log_lines(log_lines)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
