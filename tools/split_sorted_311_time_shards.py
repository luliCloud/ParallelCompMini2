#!/usr/bin/env python3
"""Split a pre-sorted 311 CSV into equal-load time shards without re-sorting.

The input file is assumed to already be ordered by Created Date (typically
descending in the NYC 311 export). We preserve file order and divide valid rows
as evenly as possible across N shards, which keeps each shard as one continuous
time range.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split a pre-sorted 311 CSV into equal-load time shards."
    )
    parser.add_argument("csv_path", help="Path to the input CSV file.")
    parser.add_argument(
        "--shards",
        type=int,
        default=6,
        help="Number of output shards. Default: 6.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory. Default: <csv parent>/<csv stem>_sorted_time_shards_<N>.",
    )
    return parser.parse_args()


def split_quoted_row(line: str) -> List[str]:
    stripped = line.rstrip("\n")
    parts = stripped.split('","')
    if not parts:
        return []
    parts[0] = parts[0].lstrip('"')
    parts[-1] = parts[-1].rstrip('"\r')
    return parts


def count_valid_rows(csv_path: Path, created_idx: int) -> tuple[int, int]:
    valid_rows = 0
    invalid_rows = 0

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        header = handle.readline()
        if not header:
            return 0, 0

        for line in handle:
            parts = split_quoted_row(line)
            if len(parts) <= created_idx:
                invalid_rows += 1
                continue
            if not parts[created_idx].strip():
                invalid_rows += 1
                continue
            valid_rows += 1

    return valid_rows, invalid_rows


def build_targets(total_rows: int, shards: int) -> List[int]:
    base = total_rows // shards
    remainder = total_rows % shards
    return [base + (1 if idx < remainder else 0) for idx in range(shards)]


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
        csv_path.parent / f"{csv_path.stem}_sorted_time_shards_{args.shards}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        header_line = handle.readline()
        if not header_line:
            print("CSV header missing or unreadable.")
            return 1
        header_fields = split_quoted_row(header_line)

    try:
        created_idx = header_fields.index("Created Date")
    except ValueError:
        print("Required column 'Created Date' not found in CSV header.")
        return 1

    valid_rows, invalid_rows = count_valid_rows(csv_path, created_idx)
    if valid_rows == 0:
        print("No valid rows found with non-empty Created Date.")
        return 1

    targets = build_targets(valid_rows, args.shards)

    out_handles = []
    shard_paths: List[Path] = []
    shard_counts = [0 for _ in range(args.shards)]
    shard_first_created: List[Optional[str]] = [None for _ in range(args.shards)]
    shard_last_created: List[Optional[str]] = [None for _ in range(args.shards)]

    try:
        for shard_idx in range(args.shards):
            out_path = output_dir / f"{csv_path.stem}_shard_{shard_idx:02d}.csv"
            handle = out_path.open("w", encoding="utf-8", newline="")
            handle.write(header_line)
            out_handles.append(handle)
            shard_paths.append(out_path)

        current_shard = 0
        written_valid_rows = 0

        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            next(handle)  # skip header
            for line in handle:
                parts = split_quoted_row(line)
                if len(parts) <= created_idx or not parts[created_idx].strip():
                    continue

                while (
                    current_shard < args.shards - 1
                    and shard_counts[current_shard] >= targets[current_shard]
                ):
                    current_shard += 1

                out_handles[current_shard].write(line)
                shard_counts[current_shard] += 1
                written_valid_rows += 1

                created_value = parts[created_idx].strip()
                if shard_first_created[current_shard] is None:
                    shard_first_created[current_shard] = created_value
                shard_last_created[current_shard] = created_value
    finally:
        for handle in out_handles:
            handle.close()

    print(f"Input: {csv_path}")
    print(f"Output directory: {output_dir}")
    print(f"Valid rows written: {written_valid_rows}")
    print(f"Skipped invalid Created Date rows: {invalid_rows}")
    print("Shard targets and observed row counts:")
    for idx, target in enumerate(targets):
        print(f"  shard_{idx:02d}: target={target} actual={shard_counts[idx]}")

    print("Shard time ranges in file order:")
    print("  note: shard_00 is the first block in the file, so for a descending-sorted file it contains the newest records.")
    for idx in range(args.shards):
        print(
            f"  shard_{idx:02d}: "
            f"first_created='{shard_first_created[idx]}' "
            f"last_created='{shard_last_created[idx]}' "
            f"path={shard_paths[idx]}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
