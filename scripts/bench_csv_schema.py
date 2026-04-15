#!/usr/bin/env python3

import csv
from collections import defaultdict
from pathlib import Path


WAIT_FIELD = "avg_wait_ns_estimated"
HANDOFF_FIELD = "avg_lock_handoff_ns_estimated"
LEGACY_HANDOFF_FIELD = "avg_unlock_to_next_lock_ns_all"
THROUGHPUT_FIELD = "mean_throughput_ops_per_sec"
CPU_FIELD = "avg_cpu_pct"
LEGACY_CPU_FIELD = "cpu_pct"

RAW_FIELDNAMES = [
    "threads",
    "critical_iters",
    "outside_iters",
    "repeat",
    "throughput_ops_per_sec",
    "elapsed_seconds",
    "total_operations",
    "avg_lock_hold_ns",
    "avg_pre_front_wait_ns",
    "avg_front_wait_ns",
    "reacquire_acquisitions",
    "reacquire_rate",
    WAIT_FIELD,
    HANDOFF_FIELD,
    "lock_hold_samples",
    "phase_wait_samples",
    CPU_FIELD,
]

SUMMARY_FIELDNAMES = [
    "threads",
    "critical_iters",
    "outside_iters",
    "repeats",
    THROUGHPUT_FIELD,
    "elapsed_seconds",
    "total_operations",
    "avg_lock_hold_ns",
    "avg_pre_front_wait_ns",
    "avg_front_wait_ns",
    "reacquire_acquisitions",
    "reacquire_rate",
    WAIT_FIELD,
    HANDOFF_FIELD,
    "lock_hold_samples",
    "phase_wait_samples",
    CPU_FIELD,
]

SUMMARY_REQUIRED = {
    "threads",
    "critical_iters",
    "outside_iters",
    THROUGHPUT_FIELD,
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
    "avg_pre_front_wait_ns",
    "avg_front_wait_ns",
    "reacquire_acquisitions",
    "reacquire_rate",
    "lock_hold_samples",
    "phase_wait_samples",
}

DEFAULT_PLOT_REQUIRED_FIELDS = {
    "threads",
    "critical_iters",
    "outside_iters",
    THROUGHPUT_FIELD,
}

LATENCY_PLOT_REQUIRED_FIELDS = DEFAULT_PLOT_REQUIRED_FIELDS | {
    "avg_lock_hold_ns",
    "avg_pre_front_wait_ns",
    "avg_front_wait_ns",
    WAIT_FIELD,
    HANDOFF_FIELD,
}

CPU_PLOT_REQUIRED_FIELDS = DEFAULT_PLOT_REQUIRED_FIELDS | {CPU_FIELD}


def read_csv_rows(path: str | Path) -> tuple[list[str], list[dict[str, str]]]:
    csv_path = Path(path)
    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


def _format_float(value: float) -> str:
    return f"{value:.6f}"


def _estimate_wait_ns(
    threads: float, elapsed_seconds: float, total_operations: float, avg_lock_hold_ns: float
) -> float:
    if total_operations <= 0.0:
        return 0.0
    total_thread_elapsed_ns = threads * elapsed_seconds * 1e9
    estimated_total_lock_hold_ns = avg_lock_hold_ns * total_operations
    return max(total_thread_elapsed_ns - estimated_total_lock_hold_ns, 0.0) / total_operations


def _resolve_handoff_key(field_set: set[str], source: str | Path) -> str:
    if HANDOFF_FIELD in field_set:
        return HANDOFF_FIELD
    if LEGACY_HANDOFF_FIELD in field_set:
        return LEGACY_HANDOFF_FIELD
    raise ValueError(f"{source}: missing raw/summary columns ['{HANDOFF_FIELD}']")


def _resolve_cpu_key(field_set: set[str]) -> str | None:
    for candidate in (CPU_FIELD, LEGACY_CPU_FIELD):
        if candidate in field_set:
            return candidate
    return None


def _normalize_wait_value(
    row: dict[str, str],
    wait_key: str | None,
) -> str:
    if wait_key is not None:
        wait_value = row[wait_key].strip()
        if wait_value != "":
            return wait_value

    elapsed = row.get("elapsed_seconds", "").strip()
    total_operations = row.get("total_operations", "").strip()
    avg_lock_hold_ns = row.get("avg_lock_hold_ns", "").strip()
    threads = row.get("threads", "").strip()
    if not elapsed or not total_operations or not avg_lock_hold_ns or not threads:
        return ""
    return _format_float(
        _estimate_wait_ns(
            threads=float(threads),
            elapsed_seconds=float(elapsed),
            total_operations=float(total_operations),
            avg_lock_hold_ns=float(avg_lock_hold_ns),
        )
    )


def normalize_raw_rows(
    fieldnames: list[str], rows: list[dict[str, str]], source: str | Path
) -> list[dict[str, str]]:
    field_set = set(fieldnames)
    missing = RAW_REQUIRED_FOR_CANONICAL - field_set
    if missing:
        raise ValueError(f"{source}: missing raw columns {sorted(missing)}")

    handoff_key = _resolve_handoff_key(field_set, source)
    wait_key = WAIT_FIELD if WAIT_FIELD in field_set else None
    cpu_key = _resolve_cpu_key(field_set)

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
            "avg_pre_front_wait_ns": row["avg_pre_front_wait_ns"].strip(),
            "avg_front_wait_ns": row["avg_front_wait_ns"].strip(),
            "reacquire_acquisitions": row["reacquire_acquisitions"].strip(),
            "reacquire_rate": row["reacquire_rate"].strip(),
            WAIT_FIELD: _normalize_wait_value(row, wait_key),
            HANDOFF_FIELD: row[handoff_key].strip(),
            "lock_hold_samples": row["lock_hold_samples"].strip(),
            "phase_wait_samples": row["phase_wait_samples"].strip(),
            CPU_FIELD: row[cpu_key].strip() if cpu_key is not None else "",
        }
        required_values = RAW_REQUIRED_FOR_CANONICAL | {WAIT_FIELD, HANDOFF_FIELD}
        missing_values = [k for k in required_values if normalized.get(k, "") == ""]
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
            "sum_pre_front_wait": 0.0,
            "sum_front_wait": 0.0,
            "sum_reacquire_acquisitions": 0.0,
            "sum_reacquire_rate": 0.0,
            "sum_wait": 0.0,
            "sum_handoff": 0.0,
            "sum_lock_hold_samples": 0.0,
            "sum_phase_wait_samples": 0.0,
            "sum_cpu": 0.0,
            "count_cpu": 0.0,
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
        agg["sum_pre_front_wait"] += float(row["avg_pre_front_wait_ns"])
        agg["sum_front_wait"] += float(row["avg_front_wait_ns"])
        agg["sum_reacquire_acquisitions"] += float(row["reacquire_acquisitions"])
        agg["sum_reacquire_rate"] += float(row["reacquire_rate"])
        agg["sum_wait"] += float(row[WAIT_FIELD])
        agg["sum_handoff"] += float(row[HANDOFF_FIELD])
        agg["sum_lock_hold_samples"] += float(row["lock_hold_samples"])
        agg["sum_phase_wait_samples"] += float(row["phase_wait_samples"])
        cpu_value = row.get(CPU_FIELD, "").strip()
        if cpu_value:
            agg["sum_cpu"] += float(cpu_value)
            agg["count_cpu"] += 1.0

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
                THROUGHPUT_FIELD: _format_float(agg["sum_tp"] / count),
                "elapsed_seconds": _format_float(agg["sum_elapsed"] / count),
                "total_operations": _format_float(agg["sum_total_ops"] / count),
                "avg_lock_hold_ns": _format_float(agg["sum_lock_hold"] / count),
                "avg_pre_front_wait_ns": _format_float(agg["sum_pre_front_wait"] / count),
                "avg_front_wait_ns": _format_float(agg["sum_front_wait"] / count),
                "reacquire_acquisitions": _format_float(agg["sum_reacquire_acquisitions"] / count),
                "reacquire_rate": _format_float(agg["sum_reacquire_rate"] / count),
                WAIT_FIELD: _format_float(agg["sum_wait"] / count),
                HANDOFF_FIELD: _format_float(agg["sum_handoff"] / count),
                "lock_hold_samples": _format_float(agg["sum_lock_hold_samples"] / count),
                "phase_wait_samples": _format_float(agg["sum_phase_wait_samples"] / count),
                CPU_FIELD: (
                    _format_float(agg["sum_cpu"] / agg["count_cpu"]) if agg["count_cpu"] else ""
                ),
            }
        )
    return out


def normalize_summary_rows(
    fieldnames: list[str],
    rows: list[dict[str, str]],
    source: str | Path,
    required_fields: set[str] | None = None,
) -> list[dict[str, str]]:
    field_set = set(fieldnames)
    missing = SUMMARY_REQUIRED - field_set
    if missing:
        raise ValueError(f"{source}: missing summary columns {sorted(missing)}")

    required = set(required_fields or DEFAULT_PLOT_REQUIRED_FIELDS)
    handoff_key = None
    if HANDOFF_FIELD in field_set or LEGACY_HANDOFF_FIELD in field_set:
        handoff_key = _resolve_handoff_key(field_set, source)
    wait_key = WAIT_FIELD if WAIT_FIELD in field_set else None
    cpu_key = _resolve_cpu_key(field_set)

    out: list[dict[str, str]] = []
    for row in rows:
        normalized = {
            "threads": row["threads"].strip(),
            "critical_iters": row["critical_iters"].strip(),
            "outside_iters": row["outside_iters"].strip(),
            "repeats": row.get("repeats", "").strip(),
            THROUGHPUT_FIELD: row[THROUGHPUT_FIELD].strip(),
            "elapsed_seconds": row.get("elapsed_seconds", "").strip(),
            "total_operations": row.get("total_operations", "").strip(),
            "avg_lock_hold_ns": row.get("avg_lock_hold_ns", "").strip(),
            "avg_pre_front_wait_ns": row.get("avg_pre_front_wait_ns", "").strip(),
            "avg_front_wait_ns": row.get("avg_front_wait_ns", "").strip(),
            "reacquire_acquisitions": row.get("reacquire_acquisitions", "").strip(),
            "reacquire_rate": row.get("reacquire_rate", "").strip(),
            WAIT_FIELD: _normalize_wait_value(row, wait_key),
            HANDOFF_FIELD: row[handoff_key].strip() if handoff_key is not None else "",
            "lock_hold_samples": row.get("lock_hold_samples", "").strip(),
            "phase_wait_samples": row.get("phase_wait_samples", "").strip(),
            CPU_FIELD: row[cpu_key].strip() if cpu_key is not None else "",
        }

        missing_values = [name for name in required if normalized.get(name, "") == ""]
        if missing_values:
            raise ValueError(f"{source}: missing or empty columns {sorted(missing_values)}")
        out.append(normalized)

    return out


def _aggregate_minimal_plot_rows(
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
        out.append(
            {
                "threads": str(threads),
                "critical_iters": str(critical),
                "outside_iters": str(outside),
                "repeats": str(len(values)),
                THROUGHPUT_FIELD: _format_float(sum(values) / len(values)),
            }
        )
    return out


def aggregate_plot_rows(
    fieldnames: list[str],
    rows: list[dict[str, str]],
    source: str | Path,
    required_fields: set[str] | None = None,
) -> list[dict[str, str]]:
    required = set(required_fields or DEFAULT_PLOT_REQUIRED_FIELDS)
    try:
        normalized_raw = normalize_raw_rows(fieldnames, rows, source)
        return aggregate_summary_rows(normalized_raw)
    except ValueError:
        if required - DEFAULT_PLOT_REQUIRED_FIELDS:
            raise
        return _aggregate_minimal_plot_rows(fieldnames, rows, source)


def load_plot_rows(
    lock_dir: str | Path, required_fields: set[str] | None = None
) -> list[dict[str, str]]:
    required = set(required_fields or DEFAULT_PLOT_REQUIRED_FIELDS)
    lock_path = Path(lock_dir)
    summary_path = lock_path / "summary.csv"
    raw_path = lock_path / "raw.csv"
    errors: list[str] = []

    if summary_path.is_file():
        fieldnames, rows = read_csv_rows(summary_path)
        if rows:
            try:
                return normalize_summary_rows(fieldnames, rows, summary_path, required)
            except ValueError as exc:
                errors.append(str(exc))
        else:
            errors.append("summary.csv 为空")

    if raw_path.is_file():
        fieldnames, rows = read_csv_rows(raw_path)
        if rows:
            try:
                return aggregate_plot_rows(fieldnames, rows, raw_path, required)
            except ValueError as exc:
                errors.append(str(exc))
        else:
            errors.append("raw.csv 为空")

    detail = "；".join(errors) if errors else "未找到可用的 summary.csv 或 raw.csv"
    raise ValueError(f"锁 {lock_path.name} 无法加载数据: {detail}")
