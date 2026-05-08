#!/usr/bin/env python3
"""Benchmark Mini2 response time across different chunk sizes."""

import argparse
import csv
import re
import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CLIENT = PROJECT_ROOT / "build" / "bin" / "client"

STREAM_MODES = {"stream-pure", "stream-leaf"}
ALL_MODES = {"chunked", "stream-pure", "stream-leaf"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Mini2 C++ client benchmarks for multiple chunk sizes."
    )
    parser.add_argument("-s", "--server", default="localhost:50051")
    parser.add_argument("-t", "--timeout", type=float, default=120.0)
    parser.add_argument("--client", type=Path, default=DEFAULT_CLIENT)
    parser.add_argument(
        "--mode",
        choices=sorted(ALL_MODES),
        action="append",
        default=None,
        help="Benchmark mode. Can be repeated. Default: all modes.",
    )
    parser.add_argument(
        "--query",
        choices=["agency", "borough", "geo"],
        default="agency",
    )
    parser.add_argument("--agency-id", type=int, default=10)
    parser.add_argument("--borough-id", type=int, default=1)
    parser.add_argument("--lat-min", type=float, default=40.7)
    parser.add_argument("--lat-max", type=float, default=40.8)
    parser.add_argument("--lon-min", type=float, default=-74.0)
    parser.add_argument("--lon-max", type=float, default=-73.9)
    parser.add_argument(
        "--chunk-sizes",
        default="500,1000,2000,5000,10000,20000,50000",
        help="Comma-separated chunk sizes.",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=1,
        help="Number of times to run each mode/chunk-size pair.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / "tests" / "chunk_size_benchmark.csv",
    )
    parser.add_argument(
        "--print-chunks",
        action="store_true",
        help="Do not pass --quiet-chunks to the client.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on the first failed client command.",
    )
    return parser.parse_args()


def query_args(args: argparse.Namespace) -> list[str]:
    if args.query == "agency":
        return ["--agency-id", str(args.agency_id)]
    if args.query == "borough":
        return ["--borough-id", str(args.borough_id)]
    return [
        "--lat-min",
        str(args.lat_min),
        "--lat-max",
        str(args.lat_max),
        "--lon-min",
        str(args.lon_min),
        "--lon-max",
        str(args.lon_max),
    ]


def build_command(
    args: argparse.Namespace,
    mode: str,
    chunk_size: int,
    repeat_index: int,
) -> list[str]:
    command = [
        str(args.client),
        "-s",
        args.server,
        "-t",
        str(args.timeout),
    ]

    if mode == "chunked":
        command.append("forward-chunked")
    else:
        command.append("forward-stream")

    command.extend(query_args(args))
    command.extend(["--chunk-size", str(chunk_size)])
    if mode == "stream-leaf":
        command.append("--leaf-buffered-streaming")
    if not args.print_chunks:
        command.append("--quiet-chunks")
    command.extend(
        [
            "--request-id",
            f"bench-{args.query}-{mode}-{chunk_size}-r{repeat_index}",
        ]
    )
    return command


def parse_client_output(output: str) -> dict[str, str]:
    patterns = {
        "chunks_received": r"chunks_received\s*=\s*(\d+)",
        "records_received": r"records_received\s*=\s*(\d+)",
        "forward_stream_ms": r"forward_stream_ms\s*=\s*([0-9.]+)",
        "total_time_ms": r"total_time_ms\s*=\s*([0-9.]+)",
    }
    parsed = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, output)
        parsed[key] = match.group(1) if match else ""
    return parsed


def run_one(command: list[str], timeout_seconds: float) -> tuple[int, str, str, float]:
    start = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_seconds + 10,
        check=False,
    )
    elapsed_ms = (time.perf_counter() - start) * 1000
    return completed.returncode, completed.stdout, completed.stderr, elapsed_ms


def main() -> int:
    args = parse_args()
    modes = args.mode or ["chunked", "stream-pure", "stream-leaf"]
    chunk_sizes = [
        int(value.strip())
        for value in args.chunk_sizes.split(",")
        if value.strip()
    ]

    if not args.client.exists():
        print(f"Client executable not found: {args.client}", file=sys.stderr)
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "query",
        "mode",
        "chunk_size",
        "repeat",
        "returncode",
        "chunks_received",
        "records_received",
        "forward_stream_ms",
        "total_time_ms",
        "wall_time_ms",
        "error",
        "command",
    ]

    with args.output.open("w", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()

        for mode in modes:
            for chunk_size in chunk_sizes:
                for repeat_index in range(1, args.repeats + 1):
                    command = build_command(args, mode, chunk_size, repeat_index)
                    print(
                        f"running query={args.query} mode={mode} "
                        f"chunk_size={chunk_size} repeat={repeat_index}"
                    )
                    try:
                        returncode, stdout, stderr, wall_time_ms = run_one(
                            command,
                            args.timeout,
                        )
                    except subprocess.TimeoutExpired as exc:
                        returncode = 124
                        stdout = exc.stdout or ""
                        stderr = exc.stderr or "benchmark subprocess timed out"
                        wall_time_ms = (args.timeout + 10) * 1000

                    parsed = parse_client_output(stdout)
                    row = {
                        "query": args.query,
                        "mode": mode,
                        "chunk_size": chunk_size,
                        "repeat": repeat_index,
                        "returncode": returncode,
                        "chunks_received": parsed["chunks_received"],
                        "records_received": parsed["records_received"],
                        "forward_stream_ms": parsed["forward_stream_ms"],
                        "total_time_ms": parsed["total_time_ms"],
                        "wall_time_ms": f"{wall_time_ms:.2f}",
                        "error": stderr.strip().replace("\n", " "),
                        "command": " ".join(command),
                    }
                    writer.writerow(row)
                    output_file.flush()

                    if returncode != 0:
                        print(f"  failed: {row['error']}", file=sys.stderr)
                        if args.fail_fast:
                            return returncode
                    else:
                        print(
                            "  records="
                            f"{row['records_received'] or 'n/a'} "
                            "chunks="
                            f"{row['chunks_received'] or 'n/a'} "
                            "total_ms="
                            f"{row['total_time_ms'] or 'n/a'}"
                        )

    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
