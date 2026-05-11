#!/usr/bin/env python3
"""Mini2 benchmark runner.
Add new benchmark functions here and call them from main().
"""

# Run the cluster before running this benchmark
from __future__ import annotations

import importlib
import subprocess
import sys
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Callable, Sequence

import grpc


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROTO_FILE = PROJECT_ROOT / "proto" / "mini2.proto"
MAX_GRPC_MESSAGE_BYTES = 512 * 1024 * 1024
TOTAL_QUERY_COMPLETION_BENCHMARK = "total-query-completion-time"
CHUNK_SIZE_BENCHMARK = "chunk-size-performance"
CONCURRENT_REQUESTS_BENCHMARK = "concurrent-requests-performance"
REPORT_SEPARATOR = "=" * 96


@dataclass(frozen=True)
class BenchmarkConfig:
    server: str = "10.0.0.16:50051"
    timeout_seconds: float = 120.0
    warm_repeats: int = 3
    chunk_size: int = 5000
    chunk_sizes: tuple[int, ...] = (500, 1000, 2000, 5000, 10000, 20000, 50000)
    concurrent_requests: tuple[int, ...] = (1, 2, 4, 8)
    concurrent_agency_ids: tuple[int, ...] = tuple(range(20))
    concurrent_borough_ids: tuple[int, ...] = (1, 2, 3, 4, 5)
    query: str = "agency"
    agency_id: int = 10
    borough_id: int = 1
    lat_min: float = 40.7
    lat_max: float = 40.8
    lon_min: float = -74.0
    lon_max: float = -73.9
    request_prefix: str = "bench-dt"
    fail_fast: bool = True
    modes: tuple[str, ...] = (
        "unary-forward",
        "internal-full-streaming",
        "leaf-buffered-streaming",
    )


@dataclass(frozen=True)
class ModeSpec:
    name: str
    configure_request: Callable[[object], None]


@dataclass(frozen=True)
class QuerySpec:
    name: str
    configure_request: Callable[[object], None]


@dataclass(frozen=True)
class BenchmarkResult:
    benchmark: str
    query: str
    mode: str
    chunk_size: int
    repeat: int
    request_id: str
    returncode: int
    dt_ms: float | None
    start_forward_chunks_ms: float | None
    chunks_received: int
    records_received: int
    session_total_chunks: int
    session_total_records: int
    final_chunk_index: int
    error: str


@dataclass(frozen=True)
class ConcurrentBenchmarkResult:
    benchmark: str
    query: str
    mode: str
    concurrent_requests: int
    repeat: int
    returncode: int
    avg_dt_ms: float | None
    batch_ms: float | None
    records_received: int
    chunks_received: int
    error: str


def generate_proto_modules(output_dir: Path) -> tuple[ModuleType, ModuleType]:
    command = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"--proto_path={PROTO_FILE.parent}",
        f"--python_out={output_dir}",
        f"--grpc_python_out={output_dir}",
        str(PROTO_FILE),
    ]
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        message = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"failed to generate Python gRPC bindings: {message}")

    sys.path.insert(0, str(output_dir))
    importlib.invalidate_caches()
    mini2_pb2 = importlib.import_module("mini2_pb2")
    mini2_pb2_grpc = importlib.import_module("mini2_pb2_grpc")
    return mini2_pb2, mini2_pb2_grpc


def make_modes() -> dict[str, ModeSpec]:
    return {
        "unary-forward": ModeSpec(
            name="unary-forward",
            configure_request=lambda request: None,
        ),
        "internal-full-streaming": ModeSpec(
            name="internal-full-streaming",
            configure_request=lambda request: setattr(
                request, "internal_full_streaming", True
            ),
        ),
        "leaf-buffered-streaming": ModeSpec(
            name="leaf-buffered-streaming",
            configure_request=lambda request: setattr(
                request, "leaf_buffered_streaming", True
            ),
        ),
    }


def make_query_spec(config: BenchmarkConfig) -> QuerySpec:
    if config.query == "agency":
        return QuerySpec(
            name="agency",
            configure_request=lambda request: setattr(
                request, "agency_id", config.agency_id
            ),
        )
    if config.query == "borough":
        return QuerySpec(
            name="borough",
            configure_request=lambda request: setattr(
                request, "borough_id", config.borough_id
            ),
        )
    if config.query != "geo":
        raise ValueError(f"unknown query: {config.query}")
    return QuerySpec(
        name="geo",
        configure_request=lambda request: configure_geo_request(request, config),
    )


def configure_geo_request(request: object, config: BenchmarkConfig) -> None:
    request.lat_min = config.lat_min
    request.lat_max = config.lat_max
    request.lon_min = config.lon_min
    request.lon_max = config.lon_max


def connect(server: str, timeout_seconds: float, mini2_pb2_grpc: ModuleType) -> tuple[object, object]:
    channel = grpc.insecure_channel(
        server,
        options=[
            ("grpc.max_receive_message_length", MAX_GRPC_MESSAGE_BYTES),
            ("grpc.max_send_message_length", MAX_GRPC_MESSAGE_BYTES),
        ],
    )
    grpc.channel_ready_future(channel).result(timeout=timeout_seconds)
    return channel, mini2_pb2_grpc.NodeServiceStub(channel)


def build_query_request(
    mini2_pb2: ModuleType,
    query: QuerySpec,
    mode: ModeSpec,
    chunk_size: int,
    request_id: str,
    request_mutator: Callable[[object], None] | None = None,
) -> object:
    request = mini2_pb2.QueryRequest()
    request.request_id = request_id
    request.chunk_size = chunk_size
    query.configure_request(request)
    if request_mutator is not None:
        request_mutator(request)
    mode.configure_request(request)
    return request


def run_case(
    stub: object,
    mini2_pb2: ModuleType,
    query: QuerySpec,
    mode: ModeSpec,
    config: BenchmarkConfig,
    benchmark_name: str,
    chunk_size: int,
    repeat: int,
    request_mutator: Callable[[object], None] | None = None,
) -> BenchmarkResult:
    request_id = (
        f"{config.request_prefix}-{benchmark_name}-{query.name}-{mode.name}-"
        f"cs{chunk_size}-r{repeat}-{uuid.uuid4().hex[:8]}"
    )
    request = build_query_request(
        mini2_pb2=mini2_pb2,
        query=query,
        mode=mode,
        chunk_size=chunk_size,
        request_id=request_id,
        request_mutator=request_mutator,
    )
    session_id = ""

    try:
        start = time.perf_counter()
        session_start = time.perf_counter()
        session = stub.StartForwardChunks(request, timeout=config.timeout_seconds)
        session_id = session.session_id
        start_forward_chunks_ms = (time.perf_counter() - session_start) * 1000

        records_received = 0
        chunks_received = 0
        final_chunk_index = -1
        chunk_index = 0

        while True:
            chunk_request = mini2_pb2.ChunkRequest(
                session_id=session.session_id,
                chunk_index=chunk_index,
            )
            chunk = stub.GetForwardChunk(chunk_request, timeout=config.timeout_seconds)
            final_chunk_index = chunk.chunk_index

            if chunk.done:
                dt_ms = (time.perf_counter() - start) * 1000
                break

            records_received += len(chunk.records)
            chunks_received += 1
            chunk_index += 1

        cancel_session(stub, mini2_pb2, session_id, config.timeout_seconds)
        return BenchmarkResult(
            benchmark=benchmark_name,
            query=query.name,
            mode=mode.name,
            chunk_size=chunk_size,
            repeat=repeat,
            request_id=request_id,
            returncode=0,
            dt_ms=dt_ms,
            start_forward_chunks_ms=start_forward_chunks_ms,
            chunks_received=chunks_received,
            records_received=records_received,
            session_total_chunks=session.total_chunks,
            session_total_records=session.total_records,
            final_chunk_index=final_chunk_index,
            error="",
        )
    except Exception as exc:
        cancel_session(stub, mini2_pb2, session_id, config.timeout_seconds)
        return BenchmarkResult(
            benchmark=benchmark_name,
            query=query.name,
            mode=mode.name,
            chunk_size=chunk_size,
            repeat=repeat,
            request_id=request_id,
            returncode=1,
            dt_ms=None,
            start_forward_chunks_ms=None,
            chunks_received=0,
            records_received=0,
            session_total_chunks=0,
            session_total_records=0,
            final_chunk_index=-1,
            error=str(exc).replace("\n", " "),
        )


def cancel_session(
    stub: object,
    mini2_pb2: ModuleType,
    session_id: str,
    timeout_seconds: float,
) -> None:
    if not session_id:
        return
    try:
        request = mini2_pb2.ChunkCancelRequest(session_id=session_id)
        stub.CancelChunks(request, timeout=timeout_seconds)
    except Exception:
        pass


def selected_modes(config: BenchmarkConfig) -> list[ModeSpec]:
    modes = make_modes()
    return [modes[name] for name in config.modes]


def average_results(results: Sequence[BenchmarkResult]) -> list[BenchmarkResult]:
    averaged: list[BenchmarkResult] = []
    result_keys = list(
        dict.fromkeys((result.benchmark, result.chunk_size, result.mode) for result in results)
    )

    for benchmark, chunk_size, mode in result_keys:
        mode_results = [
            result
            for result in results
            if result.benchmark == benchmark
            and result.chunk_size == chunk_size
            and result.mode == mode
        ]
        successful = [result for result in mode_results if result.returncode == 0]

        if not successful:
            first = mode_results[0]
            averaged.append(
                BenchmarkResult(
                    benchmark=first.benchmark,
                    query=first.query,
                    mode=first.mode,
                    chunk_size=first.chunk_size,
                    repeat=len(mode_results),
                    request_id="average",
                    returncode=1,
                    dt_ms=None,
                    start_forward_chunks_ms=None,
                    chunks_received=0,
                    records_received=0,
                    session_total_chunks=0,
                    session_total_records=0,
                    final_chunk_index=-1,
                    error=first.error,
                )
            )
            continue

        averaged.append(
            BenchmarkResult(
                benchmark=successful[0].benchmark,
                query=successful[0].query,
                mode=mode,
                chunk_size=chunk_size,
                repeat=len(mode_results),
                request_id="average",
                returncode=0 if len(successful) == len(mode_results) else 1,
                dt_ms=sum(result.dt_ms or 0.0 for result in successful) / len(successful),
                start_forward_chunks_ms=sum(
                    result.start_forward_chunks_ms or 0.0 for result in successful
                )
                / len(successful),
                chunks_received=round(
                    sum(result.chunks_received for result in successful) / len(successful)
                ),
                records_received=round(
                    sum(result.records_received for result in successful) / len(successful)
                ),
                session_total_chunks=round(
                    sum(result.session_total_chunks for result in successful)
                    / len(successful)
                ),
                session_total_records=round(
                    sum(result.session_total_records for result in successful)
                    / len(successful)
                ),
                final_chunk_index=round(
                    sum(result.final_chunk_index for result in successful) / len(successful)
                ),
                error="",
            )
        )

    return averaged


def format_ms(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f}"


def format_throughput(records_received: int, dt_ms: float | None) -> str:
    if dt_ms is None or dt_ms <= 0:
        return "n/a"
    return f"{records_received / (dt_ms / 1000.0):.2f}"


def status_label(returncode: int) -> str:
    return "ok" if returncode == 0 else "failed"


def print_result_table(
    title: str,
    results: Sequence[BenchmarkResult],
    include_chunk_size: bool,
    include_mode: bool = True,
) -> None:
    columns = []
    if include_mode:
        columns.append("mode")
    if include_chunk_size:
        columns.append("chunk_size")
    columns.extend(["dt_ms", "start_ms", "records", "throughput_rps", "chunks", "status"])

    rows = []
    for result in results:
        row = {
            "dt_ms": format_ms(result.dt_ms),
            "start_ms": format_ms(result.start_forward_chunks_ms),
            "records": str(result.records_received),
            "throughput_rps": format_throughput(result.records_received, result.dt_ms),
            "chunks": str(result.chunks_received),
            "status": status_label(result.returncode),
        }
        if include_mode:
            row["mode"] = result.mode
        if include_chunk_size:
            row["chunk_size"] = str(result.chunk_size)
        rows.append(row)

    widths = {
        label: max([len(label), *(len(row[label]) for row in rows)])
        for label in columns
    }
    header = "  ".join(label.ljust(widths[label]) for label in columns)
    separator = "  ".join("-" * widths[label] for label in columns)

    if title:
        print(title)
    print(header)
    print(separator)
    for row in rows:
        print("  ".join(row[label].ljust(widths[label]) for label in columns))
    print()


def benchmark_total_query_completion_time(
    stub: object,
    mini2_pb2: ModuleType,
    config: BenchmarkConfig,
) -> tuple[list[BenchmarkResult], list[BenchmarkResult]]:
    """Measure StartForwardChunks through final done=true chunk receipt."""
    query = make_query_spec(config)
    cold_results: list[BenchmarkResult] = []
    warm_results: list[BenchmarkResult] = []

    for mode in selected_modes(config):
        cold_results.append(
            run_case(
                stub=stub,
                mini2_pb2=mini2_pb2,
                query=query,
                mode=mode,
                config=config,
                benchmark_name=TOTAL_QUERY_COMPLETION_BENCHMARK,
                chunk_size=config.chunk_size,
                repeat=1,
            )
        )
        if config.fail_fast and cold_results[-1].returncode != 0:
            return cold_results, warm_results

        for repeat in range(1, config.warm_repeats + 1):
            warm_results.append(
                run_case(
                    stub=stub,
                    mini2_pb2=mini2_pb2,
                    query=query,
                    mode=mode,
                    config=config,
                    benchmark_name=TOTAL_QUERY_COMPLETION_BENCHMARK,
                    chunk_size=config.chunk_size,
                    repeat=repeat,
                )
            )
            if config.fail_fast and warm_results[-1].returncode != 0:
                return cold_results, average_results(warm_results)

    return cold_results, average_results(warm_results)


def benchmark_chunk_sizes(
    stub: object,
    mini2_pb2: ModuleType,
    config: BenchmarkConfig,
) -> tuple[list[BenchmarkResult], list[BenchmarkResult]]:
    """Measure warm completion time across chunk sizes and modes."""
    query = make_query_spec(config)
    warm_results: list[BenchmarkResult] = []

    for chunk_size in config.chunk_sizes:
        for mode in selected_modes(config):
            for repeat in range(1, config.warm_repeats + 1):
                warm_results.append(
                    run_case(
                        stub=stub,
                        mini2_pb2=mini2_pb2,
                        query=query,
                        mode=mode,
                        config=config,
                        benchmark_name=CHUNK_SIZE_BENCHMARK,
                        chunk_size=chunk_size,
                        repeat=repeat,
                    )
                )
                if config.fail_fast and warm_results[-1].returncode != 0:
                    return [], average_results(warm_results)

    return [], average_results(warm_results)


def make_concurrent_request_mutator(
    config: BenchmarkConfig,
    variant_index: int,
) -> Callable[[object], None]:
    agency_id = config.concurrent_agency_ids[
        variant_index % len(config.concurrent_agency_ids)
    ]
    borough_id = config.concurrent_borough_ids[
        (variant_index // len(config.concurrent_agency_ids))
        % len(config.concurrent_borough_ids)
    ]

    def mutate(request: object) -> None:
        request.agency_id = agency_id
        request.borough_id = borough_id

    return mutate


def concurrent_variant_base(config: BenchmarkConfig, concurrent_requests: int) -> int:
    base = 0
    for configured_concurrency in config.concurrent_requests:
        if configured_concurrency == concurrent_requests:
            return base * config.warm_repeats
        base += configured_concurrency
    raise ValueError(f"unknown concurrency level: {concurrent_requests}")


def run_concurrent_batch(
    stub: object,
    mini2_pb2: ModuleType,
    query: QuerySpec,
    mode: ModeSpec,
    config: BenchmarkConfig,
    concurrent_requests: int,
    repeat: int,
) -> ConcurrentBenchmarkResult:
    variant_base = (
        concurrent_variant_base(config, concurrent_requests)
        + (repeat - 1) * concurrent_requests
    )
    batch_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrent_requests) as executor:
        futures = [
            executor.submit(
                run_case,
                stub,
                mini2_pb2,
                query,
                mode,
                config,
                CONCURRENT_REQUESTS_BENCHMARK,
                config.chunk_size,
                repeat,
                make_concurrent_request_mutator(config, variant_base + request_index),
            )
            for request_index in range(concurrent_requests)
        ]
        request_results = [future.result() for future in futures]
    batch_ms = (time.perf_counter() - batch_start) * 1000

    successful = [result for result in request_results if result.returncode == 0]
    returncode = 0 if len(successful) == len(request_results) else 1
    avg_dt_ms = None
    if successful:
        avg_dt_ms = sum(result.dt_ms or 0.0 for result in successful) / len(successful)

    return ConcurrentBenchmarkResult(
        benchmark=CONCURRENT_REQUESTS_BENCHMARK,
        query=query.name,
        mode=mode.name,
        concurrent_requests=concurrent_requests,
        repeat=repeat,
        returncode=returncode,
        avg_dt_ms=avg_dt_ms,
        batch_ms=batch_ms,
        records_received=sum(result.records_received for result in successful),
        chunks_received=sum(result.chunks_received for result in successful),
        error="; ".join(result.error for result in request_results if result.error),
    )


def average_concurrent_results(
    results: Sequence[ConcurrentBenchmarkResult],
) -> list[ConcurrentBenchmarkResult]:
    averaged: list[ConcurrentBenchmarkResult] = []
    result_keys = list(
        dict.fromkeys((result.mode, result.concurrent_requests) for result in results)
    )

    for mode, concurrent_requests in result_keys:
        group = [
            result
            for result in results
            if result.mode == mode and result.concurrent_requests == concurrent_requests
        ]
        successful = [result for result in group if result.returncode == 0]

        if not successful:
            first = group[0]
            averaged.append(
                ConcurrentBenchmarkResult(
                    benchmark=first.benchmark,
                    query=first.query,
                    mode=first.mode,
                    concurrent_requests=first.concurrent_requests,
                    repeat=len(group),
                    returncode=1,
                    avg_dt_ms=None,
                    batch_ms=None,
                    records_received=0,
                    chunks_received=0,
                    error=first.error,
                )
            )
            continue

        avg_batch_ms = sum(result.batch_ms or 0.0 for result in successful) / len(successful)
        averaged.append(
            ConcurrentBenchmarkResult(
                benchmark=successful[0].benchmark,
                query=successful[0].query,
                mode=mode,
                concurrent_requests=concurrent_requests,
                repeat=len(group),
                returncode=0 if len(successful) == len(group) else 1,
                avg_dt_ms=sum(result.avg_dt_ms or 0.0 for result in successful)
                / len(successful),
                batch_ms=avg_batch_ms,
                records_received=round(
                    sum(result.records_received for result in successful) / len(successful)
                ),
                chunks_received=round(
                    sum(result.chunks_received for result in successful) / len(successful)
                ),
                error="",
            )
        )

    return averaged


def benchmark_concurrent_requests(
    stub: object,
    mini2_pb2: ModuleType,
    config: BenchmarkConfig,
) -> list[ConcurrentBenchmarkResult]:
    """Measure warm performance under concurrent client requests."""
    query = make_query_spec(config)
    warm_results: list[ConcurrentBenchmarkResult] = []

    for mode in selected_modes(config):
        for concurrent_requests in config.concurrent_requests:
            for repeat in range(1, config.warm_repeats + 1):
                warm_results.append(
                    run_concurrent_batch(
                        stub=stub,
                        mini2_pb2=mini2_pb2,
                        query=query,
                        mode=mode,
                        config=config,
                        concurrent_requests=concurrent_requests,
                        repeat=repeat,
                    )
                )
                if config.fail_fast and warm_results[-1].returncode != 0:
                    return average_concurrent_results(warm_results)

    return average_concurrent_results(warm_results)


def print_benchmark_report(
    benchmark_name: str,
    config: BenchmarkConfig,
    cold_results: Sequence[BenchmarkResult],
    warm_average_results: Sequence[BenchmarkResult],
    include_chunk_size: bool = False,
) -> None:
    print(REPORT_SEPARATOR)
    print(f"Benchmark name: {benchmark_name}.")
    if cold_results:
        print_result_section(
            "Cold start",
            config,
            cold_results,
            repeated=1,
            include_chunk_size=include_chunk_size,
        )
    print_result_section(
        "Warm start",
        config,
        warm_average_results,
        repeated=config.warm_repeats,
        include_chunk_size=include_chunk_size,
    )


def print_chunk_size_benchmark_report(
    config: BenchmarkConfig,
    warm_average_results: Sequence[BenchmarkResult],
) -> None:
    print(REPORT_SEPARATOR)
    print(f"Benchmark name: {CHUNK_SIZE_BENCHMARK}.")
    print("Warm start")
    print(f"Parameters: agency_id: {config.agency_id}")
    print(f"repeated: {config.warm_repeats}")

    for mode in config.modes:
        mode_results = [result for result in warm_average_results if result.mode == mode]
        if not mode_results:
            continue
        print_result_table(
            mode,
            mode_results,
            include_chunk_size=True,
            include_mode=False,
        )


def print_concurrent_result_table(
    title: str,
    results: Sequence[ConcurrentBenchmarkResult],
) -> None:
    columns = [
        "concurrent_requests",
        "avg_dt_ms",
        "batch_ms",
        "records",
        "throughput_rps",
        "chunks",
        "status",
    ]
    rows = []
    for result in results:
        rows.append(
            {
                "concurrent_requests": str(result.concurrent_requests),
                "avg_dt_ms": format_ms(result.avg_dt_ms),
                "batch_ms": format_ms(result.batch_ms),
                "records": str(result.records_received),
                "throughput_rps": format_throughput(
                    result.records_received,
                    result.batch_ms,
                ),
                "chunks": str(result.chunks_received),
                "status": status_label(result.returncode),
            }
        )

    widths = {
        label: max([len(label), *(len(row[label]) for row in rows)])
        for label in columns
    }
    header = "  ".join(label.ljust(widths[label]) for label in columns)
    separator = "  ".join("-" * widths[label] for label in columns)

    print(title)
    print(header)
    print(separator)
    for row in rows:
        print("  ".join(row[label].ljust(widths[label]) for label in columns))
    print()


def print_concurrent_requests_benchmark_report(
    config: BenchmarkConfig,
    warm_average_results: Sequence[ConcurrentBenchmarkResult],
) -> None:
    print(REPORT_SEPARATOR)
    print(f"Benchmark name: {CONCURRENT_REQUESTS_BENCHMARK}.")
    print("Warm start")
    print(
        "Parameters: agency_id: varied per concurrent request, "
        "borough_id: varied per concurrent request, "
        f"chunk_size: {config.chunk_size}"
    )
    print(f"repeated: {config.warm_repeats}")

    for mode in config.modes:
        mode_results = [result for result in warm_average_results if result.mode == mode]
        if not mode_results:
            continue
        print_concurrent_result_table(mode, mode_results)


def print_result_section(
    title: str,
    config: BenchmarkConfig,
    results: Sequence[BenchmarkResult],
    repeated: int,
    include_chunk_size: bool,
) -> None:
    print(title)
    print(f"Parameters: agency_id: {config.agency_id}")
    print(f"repeated: {repeated}")
    print_result_table("", results, include_chunk_size=include_chunk_size)


def main() -> int:
    config = BenchmarkConfig()
    all_results: list[BenchmarkResult | ConcurrentBenchmarkResult] = []

    with tempfile.TemporaryDirectory(prefix="mini2_pb_") as generated_dir:
        mini2_pb2, mini2_pb2_grpc = generate_proto_modules(Path(generated_dir))
        channel, stub = connect(config.server, config.timeout_seconds, mini2_pb2_grpc)
        try:
            cold_results, warm_average_results = benchmark_total_query_completion_time(
                stub,
                mini2_pb2,
                config,
            )
            print_benchmark_report(
                TOTAL_QUERY_COMPLETION_BENCHMARK,
                config,
                cold_results,
                warm_average_results,
                include_chunk_size=False,
            )
            all_results.extend(cold_results)
            all_results.extend(warm_average_results)

            if not any(
                result.returncode != 0
                for result in list(cold_results) + list(warm_average_results)
            ):
                chunk_cold_results, chunk_warm_average_results = benchmark_chunk_sizes(
                    stub,
                    mini2_pb2,
                    config,
                )
                print_chunk_size_benchmark_report(config, chunk_warm_average_results)
                all_results.extend(chunk_cold_results)
                all_results.extend(chunk_warm_average_results)

            if not any(result.returncode != 0 for result in all_results):
                concurrent_warm_average_results = benchmark_concurrent_requests(
                    stub,
                    mini2_pb2,
                    config,
                )
                print_concurrent_requests_benchmark_report(
                    config,
                    concurrent_warm_average_results,
                )
                all_results.extend(concurrent_warm_average_results)
        finally:
            channel.close()

    return 1 if any(result.returncode != 0 for result in all_results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
