#!/usr/bin/env python3
"""Split a CSV into equal-load time-range shards by Created Date."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence, Tuple


DATE_FORMAT = "%m/%d/%Y %I:%M:%S %p"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split Mini2 CSV into time-range shards using Created Date."
    )
    parser.add_argument("csv_path", help="Path to input CSV.")
    parser.add_argument(
        "--shards",
        type=int,
        default=6,
        help="Number of output time shards. Default: 6.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory. Default: <csv parent>/<csv stem>_time_shards_<N>.",
    )
    return parser.parse_args()


def parse_datetime(value: str) -> Optional[int]:
    value = value.strip()
    if not value:
        return None
    try:
        return int(datetime.strptime(value, DATE_FORMAT).timestamp())
    except ValueError:
        return None


def quantile_boundaries(sorted_timestamps: Sequence[int], partitions: int) -> Tuple[int, ...]:
    if partitions <= 1:
        return tuple()
    boundaries: List[int] = []
    n = len(sorted_timestamps)
    for i in range(1, partitions):
        pos = (i * n + partitions - 1) // partitions - 1
        pos = max(0, min(pos, n - 1))
        boundaries.append(sorted_timestamps[pos])
    return tuple(boundaries)


def assign_time_bucket(ts: int, boundaries: Sequence[int]) -> int:
    for idx, boundary in enumerate(boundaries):
        if ts <= boundary:
            return idx
    return len(boundaries)


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv_path)
    if not csv_path.is_file():
        print(f"CSV file not found: {csv_path}")
        return 1
    if args.shards <= 0:
        print("--shards must be positive")
        return 1

    output_dir = Path(args.output_dir) if args.output_dir else (
        csv_path.parent / f"{csv_path.stem}_time_shards_{args.shards}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamps: List[int] = []
    fieldnames: Optional[List[str]] = None
    valid_rows = 0
    invalid_rows = 0

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        for row in reader:
            created_ts = parse_datetime(row.get("Created Date", ""))
            if created_ts is None:
                invalid_rows += 1
                continue
            timestamps.append(created_ts)
            valid_rows += 1

    if not fieldnames:
        print("CSV header missing or unreadable.")
        return 1
    if not timestamps:
        print("No valid rows found with parsable Created Date.")
        return 1

    boundaries = quantile_boundaries(sorted(timestamps), args.shards)
    writers = []
    files = []
    shard_counts = [0 for _ in range(args.shards)]
    try:
        for shard_idx in range(args.shards):
            out_path = output_dir / f"{csv_path.stem}_shard_{shard_idx:02d}.csv"
            out_handle = out_path.open("w", encoding="utf-8", newline="")
            writer = csv.DictWriter(out_handle, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            files.append(out_handle)
            writers.append(writer)

        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                created_ts = parse_datetime(row.get("Created Date", ""))
                if created_ts is None:
                    continue
                shard_idx = assign_time_bucket(created_ts, boundaries)
                writers[shard_idx].writerow(row)
                shard_counts[shard_idx] += 1
    finally:
        for out_handle in files:
            out_handle.close()

    print(f"Input: {csv_path}")
    print(f"Output directory: {output_dir}")
    print(f"Valid rows written: {valid_rows}")
    print(f"Skipped invalid Created Date rows: {invalid_rows}")
    print("Shard boundaries:")
    lower = None
    for idx, boundary in enumerate(boundaries):
        start_label = "-inf" if lower is None else fmt_ts(lower + 1)
        print(f"  shard_{idx:02d}: {start_label} -> {fmt_ts(boundary)}")
        lower = boundary
    if lower is None:
        print("  shard_00: all timestamps")
    else:
        print(f"  shard_{args.shards - 1:02d}: {fmt_ts(lower + 1)} -> +inf")

    print("Shard row counts:")
    for idx, count in enumerate(shard_counts):
        print(f"  shard_{idx:02d}: {count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
