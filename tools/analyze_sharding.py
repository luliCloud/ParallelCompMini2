#!/usr/bin/env python3
"""Analyze created-date and borough skewness for sharding decisions.

This script reads the NYC 311 CSV used by Mini2 and reports:
1. Time-range skewness for the first shard key (`Created Date`)
2. Borough skewness for the second shard key (`Borough` / encoded `borough_id`)
3. Suggested shard layouts for a given number of nodes

The borough_id encoding matches the server's current behavior: first-seen value
gets id 0, next unseen value gets id 1, and so on.
"""

from __future__ import annotations

import argparse
import csv
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


DATE_FORMAT = "%m/%d/%Y %I:%M:%S %p"


@dataclass(frozen=True)
class Row:
    created_ts: int
    borough_name: str
    borough_id: int


@dataclass(frozen=True)
class ShardPlan:
    time_shards: int
    borough_groups: int
    shard_counts: Tuple[int, ...]
    time_boundaries: Tuple[int, ...]
    borough_group_members: Tuple[Tuple[int, ...], ...]

    @property
    def total_shards(self) -> int:
        return self.time_shards * self.borough_groups


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze skewness and suggest sharding layouts for Mini2 CSV data."
    )
    parser.add_argument(
        "csv_path",
        help="Path to the input CSV file (for example benchmarks/workload_100k.csv).",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=6,
        help="Number of nodes / target shards to balance across. Default: 6.",
    )
    parser.add_argument(
        "--time-bins",
        type=int,
        default=None,
        help="Number of equal-width time bins for skewness reporting. Default: min(12, max(4, nodes)).",
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


def load_rows(csv_path: Path) -> Tuple[List[Row], Dict[int, str], int]:
    rows: List[Row] = []
    borough_name_to_id: Dict[str, int] = {}
    borough_id_to_name: Dict[int, str] = {}
    invalid_dates = 0

    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            created_ts = parse_datetime(raw.get("Created Date", ""))
            if created_ts is None:
                invalid_dates += 1
                continue

            borough_name = (raw.get("Borough", "") or "").strip() or "<EMPTY>"
            if borough_name not in borough_name_to_id:
                borough_id = len(borough_name_to_id)
                borough_name_to_id[borough_name] = borough_id
                borough_id_to_name[borough_id] = borough_name
            borough_id = borough_name_to_id[borough_name]
            rows.append(Row(created_ts=created_ts, borough_name=borough_name, borough_id=borough_id))

    return rows, borough_id_to_name, invalid_dates


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def gini(values: Sequence[int]) -> float:
    if not values:
        return 0.0
    total = sum(values)
    if total == 0:
        return 0.0
    sorted_values = sorted(values)
    n = len(sorted_values)
    weighted_sum = 0
    for i, value in enumerate(sorted_values, start=1):
        weighted_sum += i * value
    return (2 * weighted_sum) / (n * total) - (n + 1) / n


def cv(values: Sequence[int]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    if mean == 0:
        return 0.0
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(variance) / mean


def max_avg_ratio(values: Sequence[int]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    if mean == 0:
        return 0.0
    return max(values) / mean


def summarize_distribution(values: Sequence[int]) -> str:
    if not values:
        return "count=0"
    return (
        f"count={len(values)} min={min(values)} max={max(values)} avg={sum(values) / len(values):.2f} "
        f"max/avg={max_avg_ratio(values):.3f} cv={cv(values):.3f} gini={gini(values):.3f}"
    )


def equal_width_time_bins(rows: Sequence[Row], num_bins: int) -> List[Tuple[str, int]]:
    if not rows:
        return []

    min_ts = min(row.created_ts for row in rows)
    max_ts = max(row.created_ts for row in rows)
    if min_ts == max_ts:
        return [(f"{fmt_ts(min_ts)} -> {fmt_ts(max_ts)}", len(rows))]

    width = max(1, math.ceil((max_ts - min_ts + 1) / num_bins))
    counts = [0 for _ in range(num_bins)]

    for row in rows:
        idx = min((row.created_ts - min_ts) // width, num_bins - 1)
        counts[idx] += 1

    bins: List[Tuple[str, int]] = []
    for idx, count in enumerate(counts):
        start = min_ts + idx * width
        end = min(max_ts, start + width - 1)
        bins.append((f"{fmt_ts(start)} -> {fmt_ts(end)}", count))
    return bins


def quantile_boundaries(sorted_timestamps: Sequence[int], partitions: int) -> Tuple[int, ...]:
    if partitions <= 1:
        return tuple()
    boundaries: List[int] = []
    n = len(sorted_timestamps)
    for i in range(1, partitions):
        pos = math.ceil(i * n / partitions) - 1
        pos = max(0, min(pos, n - 1))
        boundaries.append(sorted_timestamps[pos])
    return tuple(boundaries)


def assign_time_bucket(ts: int, boundaries: Sequence[int]) -> int:
    for idx, boundary in enumerate(boundaries):
        if ts <= boundary:
            return idx
    return len(boundaries)


def greedy_group_counts(counts_by_id: Dict[int, int], groups: int) -> Tuple[Tuple[int, ...], ...]:
    buckets: List[List[int]] = [[] for _ in range(groups)]
    bucket_totals = [0 for _ in range(groups)]

    for borough_id, count in sorted(counts_by_id.items(), key=lambda item: (-item[1], item[0])):
        target = min(range(groups), key=lambda idx: (bucket_totals[idx], len(buckets[idx])))
        buckets[target].append(borough_id)
        bucket_totals[target] += count

    return tuple(tuple(sorted(bucket)) for bucket in buckets)


def evaluate_plan(rows: Sequence[Row], nodes: int, borough_ids: Iterable[int]) -> List[ShardPlan]:
    borough_ids = list(borough_ids)
    borough_count = len(borough_ids)
    time_sorted = sorted(row.created_ts for row in rows)
    borough_totals = Counter(row.borough_id for row in rows)

    plans: List[ShardPlan] = []
    for time_shards in range(1, nodes + 1):
        if nodes % time_shards != 0:
            continue
        borough_groups = nodes // time_shards
        if borough_groups > borough_count:
            continue

        boundaries = quantile_boundaries(time_sorted, time_shards)
        borough_group_members = greedy_group_counts(borough_totals, borough_groups)
        borough_to_group: Dict[int, int] = {}
        for group_idx, members in enumerate(borough_group_members):
            for borough_id in members:
                borough_to_group[borough_id] = group_idx

        shard_counts = [0 for _ in range(nodes)]
        for row in rows:
            time_idx = assign_time_bucket(row.created_ts, boundaries)
            borough_idx = borough_to_group[row.borough_id]
            shard_idx = time_idx * borough_groups + borough_idx
            shard_counts[shard_idx] += 1

        plans.append(
            ShardPlan(
                time_shards=time_shards,
                borough_groups=borough_groups,
                shard_counts=tuple(shard_counts),
                time_boundaries=boundaries,
                borough_group_members=borough_group_members,
            )
        )
    return plans


def best_plan(plans: Sequence[ShardPlan]) -> Optional[ShardPlan]:
    if not plans:
        return None
    return min(
        plans,
        key=lambda plan: (
            max_avg_ratio(plan.shard_counts),
            cv(plan.shard_counts),
            gini(plan.shard_counts),
            -plan.time_shards,
        ),
    )


def print_time_distribution(rows: Sequence[Row], num_bins: int) -> None:
    bins = equal_width_time_bins(rows, num_bins)
    counts = [count for _, count in bins]
    print("Time-range skewness (equal-width bins)")
    print(f"  {summarize_distribution(counts)}")
    for label, count in bins:
        print(f"  {label}: {count}")


def print_borough_distribution(rows: Sequence[Row], borough_id_to_name: Dict[int, str]) -> None:
    borough_counts = Counter(row.borough_id for row in rows)
    counts = [borough_counts[borough_id] for borough_id in sorted(borough_counts)]
    print("Borough skewness (server-compatible borough_id encoding)")
    print(f"  {summarize_distribution(counts)}")
    for borough_id in sorted(borough_counts):
        print(f"  borough_id={borough_id} name={borough_id_to_name[borough_id]} count={borough_counts[borough_id]}")


def print_plan(plan: ShardPlan, borough_id_to_name: Dict[int, str]) -> None:
    print(f"Recommended shard layout for {plan.total_shards} nodes")
    print(f"  first index: created_date into {plan.time_shards} quantile time shards")
    print(f"  second index: borough_id into {plan.borough_groups} borough groups")
    print(f"  shard load stats: {summarize_distribution(plan.shard_counts)}")

    if plan.time_boundaries:
        print("  time shard boundaries:")
        lower = None
        for idx, boundary in enumerate(plan.time_boundaries):
            start_label = "-inf" if lower is None else fmt_ts(lower + 1)
            print(f"    shard_t{idx}: {start_label} -> {fmt_ts(boundary)}")
            lower = boundary
        print(f"    shard_t{len(plan.time_boundaries)}: {fmt_ts(lower + 1)} -> +inf")
    else:
        print("  time shard boundaries:")
        print("    shard_t0: all timestamps")

    print("  borough groups:")
    for idx, group in enumerate(plan.borough_group_members):
        labels = ", ".join(f"{borough_id}:{borough_id_to_name[borough_id]}" for borough_id in group)
        print(f"    group_b{idx}: {labels}")

    print("  estimated shard loads:")
    for idx, count in enumerate(plan.shard_counts):
        time_idx = idx // plan.borough_groups
        borough_idx = idx % plan.borough_groups
        print(f"    shard[{time_idx}][{borough_idx}] -> {count}")


def print_other_plans(plans: Sequence[ShardPlan]) -> None:
    if not plans:
        return
    print("Alternative exact-node layouts")
    ranked = sorted(
        plans,
        key=lambda plan: (
            max_avg_ratio(plan.shard_counts),
            cv(plan.shard_counts),
            gini(plan.shard_counts),
            -plan.time_shards,
        ),
    )
    for plan in ranked:
        print(
            "  "
            f"{plan.time_shards} x {plan.borough_groups} -> "
            f"max/avg={max_avg_ratio(plan.shard_counts):.3f} "
            f"cv={cv(plan.shard_counts):.3f} "
            f"gini={gini(plan.shard_counts):.3f}"
        )


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv_path)
    if not csv_path.is_file():
        print(f"CSV file not found: {csv_path}")
        return 1
    if args.nodes <= 0:
        print("--nodes must be positive")
        return 1

    rows, borough_id_to_name, invalid_dates = load_rows(csv_path)
    if not rows:
        print("No valid rows found after parsing Created Date.")
        return 1

    time_bins = args.time_bins or min(12, max(4, args.nodes))

    min_ts = min(row.created_ts for row in rows)
    max_ts = max(row.created_ts for row in rows)
    borough_counts = Counter(row.borough_id for row in rows)

    print(f"Dataset: {csv_path}")
    print(f"Valid rows: {len(rows)}")
    print(f"Skipped rows with invalid Created Date: {invalid_dates}")
    print(f"Created Date range: {fmt_ts(min_ts)} -> {fmt_ts(max_ts)}")
    print(f"Distinct borough ids: {len(borough_counts)}")
    print("")

    print_time_distribution(rows, time_bins)
    print("")
    print_borough_distribution(rows, borough_id_to_name)
    print("")

    plans = evaluate_plan(rows, args.nodes, borough_id_to_name.keys())
    chosen = best_plan(plans)
    if chosen is None:
        print(f"No exact layout found for {args.nodes} nodes.")
        return 1

    print_plan(chosen, borough_id_to_name)
    print("")
    print_other_plans(plans)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
