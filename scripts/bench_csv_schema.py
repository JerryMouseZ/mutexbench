#!/usr/bin/env python3

import csv
from collections import defaultdict
from pathlib import Path


RAW_FIELDNAMES = [
    "threads",
    "critical_iters",
    "outside_iters",
    "repeat",
    "throughput_ops_per_sec",
    "elapsed_seconds",
    "total_operations",
    "avg_lock_hold_ns",
    "avg_lock_handoff_ns_estimated",
    "lock_hold_samples",
]

SUMMARY_FIELDNAMES = [
    "threads",
    "critical_iters",
    "outside_iters",
    "repeats",
    "mean_throughput_ops_per_sec",
    "elapsed_seconds",
    "total_operations",
    "avg_lock_hold_ns",
    "avg_lock_handoff_ns_estimated",
    "lock_hold_samples",
]

SUMMARY_REQUIRED = {
    "threads",
    "critical_iters",
    "outside_iters",
    "mean_throughput_ops_per_sec",
}

RAW_REQUIRED_FOR_PLOT = {
    "threads",
    "critical_iters",
    "outside_iters",
    "throughput_ops_per_sec",
}

RAW_REQUIRED_FOR_CANONICAL = {
    "threads",
    "critical_iters",
    "outside_iters",
    "repeat",
    "throughput_ops_per_sec",
    "elapsed_seconds",
    "total_operations",
    "avg_lock_hold_ns",
    "lock_hold_samples",
}


def read_csv_rows(path: str | Path) -> tuple[list[str], list[dict[str, str]]]:
    csv_path = Path(path)
    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


def normalize_raw_rows(
    fieldnames: list[str], rows: list[dict[str, str]], source: str | Path
) -> list[dict[str, str]]:
    field_set = set(fieldnames)
    missing = RAW_REQUIRED_FOR_CANONICAL - field_set
    if missing:
        raise ValueError(f"{source}: missing raw columns {sorted(missing)}")

    if "avg_lock_handoff_ns_estimated" in field_set:
        handoff_key = "avg_lock_handoff_ns_estimated"
    elif "avg_unlock_to_next_lock_ns_all" in field_set:
        handoff_key = "avg_unlock_to_next_lock_ns_all"
    else:
        raise ValueError(
            f"{source}: missing raw columns ['avg_lock_handoff_ns_estimated']"
        )

    out: list[dict[str, str]] = []
    for row in rows:
        normalized = {
            "threads": row["threads"].strip(),
            "critical_iters": row["critical_iters"].strip(),
            "outside_iters": row["outside_iters"].strip(),
            "repeat": row["repeat"].strip(),
            "throughput_ops_per_sec": row["throughput_ops_per_sec"].strip(),
            "elapsed_seconds": row["elapsed_seconds"].strip(),
            "total_operations": row["total_operations"].strip(),
            "avg_lock_hold_ns": row["avg_lock_hold_ns"].strip(),
            "avg_lock_handoff_ns_estimated": row[handoff_key].strip(),
            "lock_hold_samples": row["lock_hold_samples"].strip(),
        }
        missing_values = [k for k, v in normalized.items() if v == ""]
        if missing_values:
            raise ValueError(f"{source}: empty values for columns {missing_values}")
        out.append(normalized)
    return out


def aggregate_summary_rows(raw_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[tuple[int, int, int], dict[str, float]] = defaultdict(
        lambda: {
            "count": 0.0,
            "sum_tp": 0.0,
            "sum_elapsed": 0.0,
            "sum_total_ops": 0.0,
            "sum_lock_hold": 0.0,
            "sum_handoff": 0.0,
            "sum_lock_hold_samples": 0.0,
        }
    )

    for row in raw_rows:
        key = (
            int(row["threads"]),
            int(row["critical_iters"]),
            int(row["outside_iters"]),
        )
        agg = grouped[key]
        agg["count"] += 1.0
        agg["sum_tp"] += float(row["throughput_ops_per_sec"])
        agg["sum_elapsed"] += float(row["elapsed_seconds"])
        agg["sum_total_ops"] += float(row["total_operations"])
        agg["sum_lock_hold"] += float(row["avg_lock_hold_ns"])
        agg["sum_handoff"] += float(row["avg_lock_handoff_ns_estimated"])
        agg["sum_lock_hold_samples"] += float(row["lock_hold_samples"])

    out: list[dict[str, str]] = []
    for threads, critical, outside in sorted(grouped):
        agg = grouped[(threads, critical, outside)]
        count = agg["count"]
        out.append(
            {
                "threads": str(threads),
                "critical_iters": str(critical),
                "outside_iters": str(outside),
                "repeats": str(int(count)),
                "mean_throughput_ops_per_sec": f"{agg['sum_tp'] / count:.6f}",
                "elapsed_seconds": f"{agg['sum_elapsed'] / count:.6f}",
                "total_operations": f"{agg['sum_total_ops'] / count:.6f}",
                "avg_lock_hold_ns": f"{agg['sum_lock_hold'] / count:.6f}",
                "avg_lock_handoff_ns_estimated": f"{agg['sum_handoff'] / count:.6f}",
                "lock_hold_samples": f"{agg['sum_lock_hold_samples'] / count:.6f}",
            }
        )
    return out


def aggregate_plot_rows(
    fieldnames: list[str], rows: list[dict[str, str]], source: str | Path
) -> list[dict[str, str]]:
    field_set = set(fieldnames)
    missing = RAW_REQUIRED_FOR_PLOT - field_set
    if missing:
        raise ValueError(f"{source}: missing raw columns {sorted(missing)}")

    grouped: dict[tuple[int, int, int], list[float]] = defaultdict(list)
    for row in rows:
        key = (
            int(row["threads"]),
            int(row["critical_iters"]),
            int(row["outside_iters"]),
        )
        grouped[key].append(float(row["throughput_ops_per_sec"]))

    out: list[dict[str, str]] = []
    for threads, critical, outside in sorted(grouped):
        values = grouped[(threads, critical, outside)]
        mean_tp = sum(values) / len(values)
        out.append(
            {
                "threads": str(threads),
                "critical_iters": str(critical),
                "outside_iters": str(outside),
                "repeats": str(len(values)),
                "mean_throughput_ops_per_sec": f"{mean_tp:.6f}",
            }
        )
    return out


def load_plot_rows(lock_dir: str | Path) -> list[dict[str, str]]:
    lock_path = Path(lock_dir)
    summary_path = lock_path / "summary.csv"
    raw_path = lock_path / "raw.csv"
    errors: list[str] = []

    if summary_path.is_file():
        fieldnames, rows = read_csv_rows(summary_path)
        if rows and SUMMARY_REQUIRED.issubset(fieldnames):
            return rows
        if not rows:
            errors.append("summary.csv 为空")
        else:
            missing = sorted(SUMMARY_REQUIRED - set(fieldnames))
            errors.append(f"summary.csv 缺少列 {missing}")

    if raw_path.is_file():
        fieldnames, rows = read_csv_rows(raw_path)
        if rows and RAW_REQUIRED_FOR_PLOT.issubset(fieldnames):
            return aggregate_plot_rows(fieldnames, rows, raw_path)
        if not rows:
            errors.append("raw.csv 为空")
        else:
            missing = sorted(RAW_REQUIRED_FOR_PLOT - set(fieldnames))
            errors.append(f"raw.csv 缺少列 {missing}")

    detail = "；".join(errors) if errors else "未找到可用的 summary.csv 或 raw.csv"
    raise ValueError(f"锁 {lock_path.name} 无法加载数据: {detail}")
