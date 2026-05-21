#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import ctypes
import os
import sys
from pathlib import Path
from typing import Dict, List

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import sample_accordin_bpf as sampler


class CpuAdmissionDebug(ctypes.Structure):
    _fields_ = [
        ("inactive_enqueue", ctypes.c_uint64),
        ("inactive_local_dequeue", ctypes.c_uint64),
        ("inactive_steal_dequeue", ctypes.c_uint64),
        ("inactive_controlled_dequeue", ctypes.c_uint64),
        ("direct_grant", ctypes.c_uint64),
        ("token_limit_reject", ctypes.c_uint64),
        ("owner_busy_reject", ctypes.c_uint64),
        ("current_inactive_total", ctypes.c_uint32),
        ("max_inactive_total", ctypes.c_uint32),
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dump per-CPU Accordin admission/inactive debug counters."
    )
    parser.add_argument("--output", default="-", help="CSV output path, or - for stdout")
    parser.add_argument("--ops-name", default="accordin", help="Expected sched_ext ops name")
    parser.add_argument("--pid", type=int, help="Use a specific owner PID")
    parser.add_argument(
        "--discover-timeout-s",
        type=float,
        default=10.0,
        help="Seconds to wait for sched_ext owner maps to become visible",
    )
    parser.add_argument(
        "--nonzero-only",
        action="store_true",
        help="Only emit CPUs with at least one nonzero debug counter",
    )
    parser.add_argument(
        "--delay-s",
        type=float,
        default=0.0,
        help="Seconds to wait after map discovery before dumping counters",
    )
    return parser.parse_args()


def read_debug_rows(meta: sampler.MapMeta) -> List[Dict[str, int]]:
    rows: List[Dict[str, int]] = []
    cpu_count = min(sampler.possible_cpu_count(), meta.max_entries)
    struct_size = ctypes.sizeof(CpuAdmissionDebug)
    if meta.value_size != struct_size:
        raise SystemExit(
            f"cpu_adm_dbg_map value size mismatch: map={meta.value_size} local={struct_size}"
        )

    for cpu in range(cpu_count):
        item = CpuAdmissionDebug.from_buffer_copy(
            sampler.bpf_map_lookup(meta.fd, sampler.u32_key(cpu), meta.value_size)
        )
        local = int(item.inactive_local_dequeue)
        stolen = int(item.inactive_steal_dequeue)
        controlled = int(item.inactive_controlled_dequeue)
        enqueue = int(item.inactive_enqueue)
        rows.append(
            {
                "cpu": cpu,
                "inactive_enqueue": enqueue,
                "inactive_local_dequeue": local,
                "inactive_steal_dequeue": stolen,
                "inactive_controlled_dequeue": controlled,
                "inactive_dequeue_total": local + stolen + controlled,
                "direct_grant": int(item.direct_grant),
                "token_limit_reject": int(item.token_limit_reject),
                "owner_busy_reject": int(item.owner_busy_reject),
                "current_inactive_total": int(item.current_inactive_total),
                "max_inactive_total": int(item.max_inactive_total),
                "enqueue_dequeue_delta": enqueue - local - stolen - controlled,
            }
        )
    return rows


def add_ratios(rows: List[Dict[str, int]]) -> None:
    nonzero_enqueues = [row["inactive_enqueue"] for row in rows if row["inactive_enqueue"]]
    mean_enqueue = sum(nonzero_enqueues) / len(nonzero_enqueues) if nonzero_enqueues else 0.0

    nonzero_grants = [row["direct_grant"] for row in rows if row["direct_grant"]]
    mean_grant = sum(nonzero_grants) / len(nonzero_grants) if nonzero_grants else 0.0

    for row in rows:
        row["imbalance_ratio"] = (
            row["inactive_enqueue"] / mean_enqueue if mean_enqueue else 0.0
        )
        row["direct_grant_ratio"] = row["direct_grant"] / mean_grant if mean_grant else 0.0


def open_output(path: str):
    if path == "-":
        return sys.stdout, False
    return open(path, "w", newline="", encoding="utf-8"), True


def main() -> int:
    args = parse_args()
    sampler.require_root()
    owner_pid, maps = sampler.choose_pid_and_maps(args)
    sampler.ensure_sched_ext_ops(args.ops_name)
    meta = maps.get("cpu_adm_dbg_map")
    if meta is None:
        raise SystemExit("cpu_adm_dbg_map was not found; enable *_DEBUG_COUNTERS=1")
    if args.delay_s > 0:
        sampler.time.sleep(args.delay_s)

    rows = read_debug_rows(meta)
    add_ratios(rows)
    if args.nonzero_only:
        rows = [
            row
            for row in rows
            if row["inactive_enqueue"]
            or row["inactive_dequeue_total"]
            or row["direct_grant"]
            or row["token_limit_reject"]
            or row["owner_busy_reject"]
        ]

    fieldnames = [
        "cpu",
        "inactive_enqueue",
        "inactive_local_dequeue",
        "inactive_steal_dequeue",
        "inactive_controlled_dequeue",
        "inactive_dequeue_total",
        "direct_grant",
        "token_limit_reject",
        "owner_busy_reject",
        "current_inactive_total",
        "max_inactive_total",
        "enqueue_dequeue_delta",
        "imbalance_ratio",
        "direct_grant_ratio",
    ]

    out_fh, should_close = open_output(args.output)
    try:
        writer = csv.DictWriter(out_fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    finally:
        for map_meta in maps.values():
            try:
                os.close(map_meta.fd)
            except OSError:
                pass
        if should_close:
            out_fh.close()

    print(f"[dump_accordin_cpu_debug] owner_pid={owner_pid} rows={len(rows)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
