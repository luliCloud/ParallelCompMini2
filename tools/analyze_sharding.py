#!/usr/bin/env python3
"""Analyze created-date and borough skewness for sharding decisions.

This version is streaming-friendly and can handle the full NYC 311 CSV.
It aggregates by day for time analysis, which is enough for shard planning
without storing every row in memory.
"""

from __future__ import annotations

import argparse
import math
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


DATE_FORMAT = "%m/%d/%Y %I:%M:%S %p"


@dataclass(frozen=True)
class DatasetStats:
    total_rows: int
    invalid_dates: int
    min_ts: int
    max_ts: int
    day_counts: Dict[int, int]
    day_borough_counts: Dict[Tuple[int, int], int]
    borough_counts: Dict[int, int]
    borough_id_to_name: Dict[int, str]


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
    parser.add_argument("csv_path", help="Path to the input CSV file.")
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
        help="Number of equal-width bins for reporting. Default: min(12, max(4, nodes)).",
    )
    return parser.parse_args()


def parse_datetime(value: str) -> Optional[Tuple[int, int]]:
    value = value.strip()
    if not value:
        return None
    try:
        month = int(value[0:2])
        day = int(value[3:5])
        year = int(value[6:10])
        hour = int(value[11:13])
        minute = int(value[14:16])
        second = int(value[17:19])
        suffix = value[20:22]

        if suffix == "AM":
            hour = 0 if hour == 12 else hour
        elif suffix == "PM":
            hour = 12 if hour == 12 else hour + 12
        else:
            return None

        current_date = date(year, month, day)
        current_dt = datetime(year, month, day, hour, minute, second)
        return current_date.toordinal(), int(current_dt.timestamp())
    except (ValueError, IndexError):
        return None


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def day_start_ts(day_ordinal: int) -> int:
    return int(datetime.combine(datetime.fromordinal(day_ordinal).date(), time.min).timestamp())


def day_end_ts(day_ordinal: int) -> int:
    return int(datetime.combine(datetime.fromordinal(day_ordinal).date(), time.max).timestamp())


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


def load_stats(csv_path: Path) -> DatasetStats:
    day_counts: Counter[int] = Counter()
    day_borough_counts: Counter[Tuple[int, int]] = Counter()
    borough_counts: Counter[int] = Counter()
    borough_name_to_id: Dict[str, int] = {}
    borough_id_to_name: Dict[int, str] = {}
    total_rows = 0
    invalid_dates = 0
    min_ts: Optional[int] = None
    max_ts: Optional[int] = None

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        header_line = handle.readline()
        if not header_line:
            raise ValueError("CSV file is empty.")

        header_fields = header_line.rstrip("\n").split('","')
        header_fields[0] = header_fields[0].lstrip('"')
        header_fields[-1] = header_fields[-1].rstrip('"\r')

        try:
            created_idx = header_fields.index("Created Date")
            borough_idx = header_fields.index("Borough")
        except ValueError as exc:
            raise ValueError("Required columns not found in CSV header.") from exc

        for line in handle:
            parts = line.rstrip("\n").split('","')
            if len(parts) <= max(created_idx, borough_idx):
                invalid_dates += 1
                continue

            parts[0] = parts[0].lstrip('"')
            parts[-1] = parts[-1].rstrip('"\r')

            parsed = parse_datetime(parts[created_idx])
            if parsed is None:
                invalid_dates += 1
                continue

            day_ordinal, created_ts = parsed
            total_rows += 1
            day_counts[day_ordinal] += 1

            borough_name = (parts[borough_idx] or "").strip() or "<EMPTY>"
            if borough_name not in borough_name_to_id:
                borough_id = len(borough_name_to_id)
                borough_name_to_id[borough_name] = borough_id
                borough_id_to_name[borough_id] = borough_name
            borough_id = borough_name_to_id[borough_name]
            borough_counts[borough_id] += 1
            day_borough_counts[(day_ordinal, borough_id)] += 1

            min_ts = created_ts if min_ts is None else min(min_ts, created_ts)
            max_ts = created_ts if max_ts is None else max(max_ts, created_ts)

    if total_rows == 0 or min_ts is None or max_ts is None:
        raise ValueError("No valid rows found after parsing Created Date.")

    return DatasetStats(
        total_rows=total_rows,
        invalid_dates=invalid_dates,
        min_ts=min_ts,
        max_ts=max_ts,
        day_counts=dict(day_counts),
        day_borough_counts=dict(day_borough_counts),
        borough_counts=dict(borough_counts),
        borough_id_to_name=borough_id_to_name,
    )


def equal_width_time_bins(stats: DatasetStats, num_bins: int) -> List[Tuple[str, int]]:
    if stats.min_ts == stats.max_ts:
        return [(f"{fmt_ts(stats.min_ts)} -> {fmt_ts(stats.max_ts)}", stats.total_rows)]

    width = max(1, math.ceil((stats.max_ts - stats.min_ts + 1) / num_bins))
    counts = [0 for _ in range(num_bins)]

    for day_ordinal, day_count in stats.day_counts.items():
        day_ts = day_start_ts(day_ordinal)
        idx = min((day_ts - stats.min_ts) // width, num_bins - 1)
        counts[idx] += day_count

    bins: List[Tuple[str, int]] = []
    for idx, count in enumerate(counts):
        start = stats.min_ts + idx * width
        end = min(stats.max_ts, start + width - 1)
        bins.append((f"{fmt_ts(start)} -> {fmt_ts(end)}", count))
    return bins


def quantile_boundaries(day_counts: Dict[int, int], partitions: int) -> Tuple[int, ...]:
    if partitions <= 1:
        return tuple()

    total = sum(day_counts.values())
    targets = [math.ceil(i * total / partitions) for i in range(1, partitions)]
    boundaries: List[int] = []
    cumulative = 0
    target_idx = 0

    for day_ordinal in sorted(day_counts):
        cumulative += day_counts[day_ordinal]
        while target_idx < len(targets) and cumulative >= targets[target_idx]:
            boundaries.append(day_end_ts(day_ordinal))
            target_idx += 1

    return tuple(boundaries)


def assign_time_bucket_for_day(day_ordinal: int, boundaries: Sequence[int]) -> int:
    ts = day_end_ts(day_ordinal)
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


def print_time_distribution(stats: DatasetStats, num_bins: int) -> None:
    bins = equal_width_time_bins(stats, num_bins)
    counts = [count for _, count in bins]
    print("Time-range skewness (equal-width bins)")
    print(f"  {summarize_distribution(counts)}")
    for label, count in bins:
        print(f"  {label}: {count}")


def print_borough_distribution(stats: DatasetStats) -> None:
    counts = [stats.borough_counts[borough_id] for borough_id in sorted(stats.borough_counts)]
    print("Borough skewness (server-compatible borough_id encoding)")
    print(f"  {summarize_distribution(counts)}")
    for borough_id in sorted(stats.borough_counts):
        print(
            f"  borough_id={borough_id} "
            f"name={stats.borough_id_to_name[borough_id]} "
            f"count={stats.borough_counts[borough_id]}"
        )


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


def estimate_plans_with_cross_counts(stats: DatasetStats, nodes: int) -> List[ShardPlan]:
    borough_ids = list(stats.borough_id_to_name.keys())
    borough_count = len(borough_ids)
    plans: List[ShardPlan] = []

    for time_shards in range(1, nodes + 1):
        if nodes % time_shards != 0:
            continue
        borough_groups = nodes // time_shards
        if borough_groups > borough_count:
            continue

        boundaries = quantile_boundaries(stats.day_counts, time_shards)
        borough_group_members = greedy_group_counts(stats.borough_counts, borough_groups)
        borough_to_group: Dict[int, int] = {}
        for group_idx, members in enumerate(borough_group_members):
            for borough_id in members:
                borough_to_group[borough_id] = group_idx

        shard_counts = [0 for _ in range(nodes)]
        for (day_ordinal, borough_id), count in stats.day_borough_counts.items():
            time_idx = assign_time_bucket_for_day(day_ordinal, boundaries)
            borough_idx = borough_to_group[borough_id]
            shard_idx = time_idx * borough_groups + borough_idx
            shard_counts[shard_idx] += count

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


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv_path)
    if not csv_path.is_file():
        print(f"CSV file not found: {csv_path}")
        return 1
    if args.nodes <= 0:
        print("--nodes must be positive")
        return 1

    stats = load_stats(csv_path)
    time_bins = args.time_bins or min(12, max(4, args.nodes))

    print(f"Dataset: {csv_path}")
    print(f"Valid rows: {stats.total_rows}")
    print(f"Skipped rows with invalid Created Date: {stats.invalid_dates}")
    print(f"Created Date range: {fmt_ts(stats.min_ts)} -> {fmt_ts(stats.max_ts)}")
    print(f"Distinct borough ids: {len(stats.borough_counts)}")
    print("")

    print_time_distribution(stats, time_bins)
    print("")
    print_borough_distribution(stats)
    print("")

    plans = estimate_plans_with_cross_counts(stats, args.nodes)
    chosen = best_plan(plans)
    if chosen is None:
        print(f"No exact layout found for {args.nodes} nodes.")
        return 1

    print_plan(chosen, stats.borough_id_to_name)
    print("")
    print_other_plans(plans)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
