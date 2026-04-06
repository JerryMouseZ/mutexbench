#!/usr/bin/env python3
"""Sample lb_simple BPF runtime state at a fixed interval.

This script discovers the active lb_simple sched_ext owner process, opens the
scheduler's BPF maps, and periodically samples the scheduler's .data/.bss state.
The default interval is 100us.

Notes:
- Run as root (for bpftool + BPF map access), e.g. sudo python3 ...
- 100us sampling is best-effort in userspace. The loop uses a hybrid sleep/spin
  wait to stay close to the requested interval, but Linux scheduling jitter still
  applies.
"""

from __future__ import annotations

import argparse
import csv
import ctypes
import os
import re
import struct
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional


MAX_CPUS = 256
STAT_NR = 16
BPF_MAP_LOOKUP_ELEM = 1
BPF_MAP_GET_FD_BY_ID = 14
SYS_BPF_BY_ARCH = {
    "x86_64": 321,
    "aarch64": 280,
    "arm64": 280,
}
SCHED_EXT_STATE = Path("/sys/kernel/sched_ext/state")
SCHED_EXT_OPS = Path("/sys/kernel/sched_ext/root/ops")
FDINFO_ROOT = Path("/proc")

libc = ctypes.CDLL(None, use_errno=True)


class BpfAttrMapLookupElem(ctypes.Structure):
    _fields_ = [
        ("map_fd", ctypes.c_uint32),
        ("_pad", ctypes.c_uint32),
        ("key", ctypes.c_uint64),
        ("value", ctypes.c_uint64),
        ("flags", ctypes.c_uint64),
    ]


class BpfAttrGetFdById(ctypes.Structure):
    _fields_ = [
        ("map_id", ctypes.c_uint32),
        ("next_id", ctypes.c_uint32),
        ("open_flags", ctypes.c_uint32),
    ]


class LegacyDataGlobals(ctypes.Structure):
    _fields_ = [
        ("H_persist", ctypes.c_uint32),
        ("L_persist", ctypes.c_uint32),
        ("ssc_vote_window_ns", ctypes.c_uint64),
        ("ssc_active_count", ctypes.c_uint32),
        ("ssc_best_count", ctypes.c_uint32),
        ("ssc_refine_low", ctypes.c_uint32),
        ("ssc_refine_high", ctypes.c_uint32),
    ]


class UserExitInfo(ctypes.Structure):
    _fields_ = [
        ("kind", ctypes.c_int32),
        ("_pad_4", ctypes.c_uint8 * 4),
        ("exit_code", ctypes.c_int64),
        ("reason", ctypes.c_int8 * 128),
        ("msg", ctypes.c_int8 * 1024),
    ]


class CurrentDataGlobals(ctypes.Structure):
    _fields_ = [
        ("H_persist", ctypes.c_uint32),
        ("L_persist", ctypes.c_uint32),
        ("ssc_vote_window_ns", ctypes.c_uint64),
        ("ssc_active_count", ctypes.c_uint32),
        ("ssc_best_count", ctypes.c_uint32),
        ("ssc_refine_low", ctypes.c_uint32),
        ("ssc_refine_high", ctypes.c_uint32),
        ("uei", UserExitInfo),
    ]


class LegacyBssGlobals(ctypes.Structure):
    _fields_ = [
        ("consec_high", ctypes.c_uint32),
        ("consec_low", ctypes.c_uint32),
        ("dominant_node", ctypes.c_int32),
        ("ssc_cpu_count", ctypes.c_uint32),
        ("ssc_cpu_list", ctypes.c_uint32 * MAX_CPUS),
        ("ssc_cpu_rank", ctypes.c_uint16 * MAX_CPUS),
        ("forced_release_cnt", ctypes.c_uint64),
        ("stats_only_mode", ctypes.c_uint32),
        ("ssc_vote_epoch", ctypes.c_uint64),
        ("ssc_vote_start_ns", ctypes.c_uint64),
        ("ssc_vote_decided_epoch", ctypes.c_uint64),
        ("ssc_vote_sum_run", ctypes.c_uint64),
        ("ssc_vote_sum_wait", ctypes.c_uint64),
        ("ssc_vote_sum_unlock_count", ctypes.c_uint64),
        ("ssc_vote_publish_count", ctypes.c_uint32),
        ("ssc_vote_last_score", ctypes.c_uint64),
        ("ssc_vote_last_effective_score", ctypes.c_uint64),
        ("ssc_bootstrap_mature_windows", ctypes.c_uint32),
        ("ssc_pending_capped_grow", ctypes.c_uint32),
        ("ssc_vote_consec_grow", ctypes.c_uint32),
        ("ssc_vote_consec_shrink", ctypes.c_uint32),
        ("ssc_search_phase", ctypes.c_uint32),
        ("ssc_best_score", ctypes.c_uint64),
        ("ssc_best_candidate_count", ctypes.c_uint32),
        ("ssc_best_candidate_streak", ctypes.c_uint32),
        ("dbg_counters_enabled", ctypes.c_uint32),
        ("dbg_win_run", ctypes.c_uint64),
        ("dbg_win_wait", ctypes.c_uint64),
        ("dbg_acct_calls", ctypes.c_uint64),
        ("dbg_acct_read_ok", ctypes.c_uint64),
        ("dbg_refine_entries", ctypes.c_uint64),
        ("dbg_refine_single_point", ctypes.c_uint64),
        ("dbg_refine_noop_targets", ctypes.c_uint64),
        ("dbg_noop_resizes", ctypes.c_uint64),
        ("dbg_active_count_changes", ctypes.c_uint64),
        ("dbg_bad_steady_rebases", ctypes.c_uint64),
        ("dbg_task_ctx_creates", ctypes.c_uint64),
        ("dbg_task_ctx_misses", ctypes.c_uint64),
        ("dbg_grow_uses_capped_step", ctypes.c_uint64),
        ("dbg_last_grow_target", ctypes.c_uint64),
    ]


class CurrentBssGlobals(ctypes.Structure):
    _fields_ = [
        ("consec_high", ctypes.c_uint32),
        ("consec_low", ctypes.c_uint32),
        ("dominant_node", ctypes.c_int32),
        ("ssc_cpu_count", ctypes.c_uint32),
        ("ssc_cpu_list", ctypes.c_uint32 * MAX_CPUS),
        ("ssc_cpu_rank", ctypes.c_uint16 * MAX_CPUS),
        ("forced_release_cnt", ctypes.c_uint64),
        ("stats_only_mode", ctypes.c_uint32),
        ("_pad_1564", ctypes.c_uint8 * 4),
        ("ssc_vote_epoch", ctypes.c_uint64),
        ("ssc_vote_start_ns", ctypes.c_uint64),
        ("ssc_vote_decided_epoch", ctypes.c_uint64),
        ("ssc_vote_sum_run", ctypes.c_uint64),
        ("ssc_vote_sum_wait", ctypes.c_uint64),
        ("ssc_vote_sum_unlock_count", ctypes.c_uint64),
        ("ssc_vote_publish_count", ctypes.c_uint32),
        ("_pad_1620", ctypes.c_uint8 * 4),
        ("ssc_vote_last_score", ctypes.c_uint64),
        ("ssc_vote_last_effective_score", ctypes.c_uint64),
        ("ssc_bootstrap_mature_windows", ctypes.c_uint32),
        ("ssc_pending_capped_grow", ctypes.c_uint32),
        ("ssc_vote_consec_grow", ctypes.c_uint32),
        ("ssc_vote_consec_shrink", ctypes.c_uint32),
        ("ssc_search_phase", ctypes.c_uint32),
        ("_pad_1660", ctypes.c_uint8 * 4),
        ("ssc_best_score", ctypes.c_uint64),
        ("ssc_best_candidate_count", ctypes.c_uint32),
        ("ssc_best_candidate_streak", ctypes.c_uint32),
        ("dbg_counters_enabled", ctypes.c_uint32),
        ("_pad_1684", ctypes.c_uint8 * 4),
        ("dbg_win_run", ctypes.c_uint64),
        ("dbg_win_wait", ctypes.c_uint64),
        ("dbg_acct_calls", ctypes.c_uint64),
        ("dbg_acct_read_ok", ctypes.c_uint64),
        ("dbg_refine_entries", ctypes.c_uint64),
        ("dbg_refine_single_point", ctypes.c_uint64),
        ("dbg_refine_noop_targets", ctypes.c_uint64),
        ("dbg_noop_resizes", ctypes.c_uint64),
        ("dbg_active_count_changes", ctypes.c_uint64),
        ("dbg_bad_steady_rebases", ctypes.c_uint64),
        ("dbg_task_ctx_creates", ctypes.c_uint64),
        ("dbg_task_ctx_misses", ctypes.c_uint64),
        ("dbg_grow_uses_capped_step", ctypes.c_uint64),
        ("dbg_last_grow_target", ctypes.c_uint64),
    ]


@dataclass(frozen=True)
class LayoutProfile:
    name: str
    data_struct: type[ctypes.Structure]
    bss_struct: type[ctypes.Structure]


LAYOUT_PROFILES = {
    "v1": LayoutProfile(name="v1", data_struct=LegacyDataGlobals, bss_struct=LegacyBssGlobals),
    "v2": LayoutProfile(name="v2", data_struct=CurrentDataGlobals, bss_struct=CurrentBssGlobals),
}

LAYOUT_ALIASES = {
    "legacy": "v1",
    "current": "v2",
}


class AggPercpu(ctypes.Structure):
    _fields_ = [
        ("epoch", ctypes.c_uint64),
        ("run_ns", ctypes.c_uint64),
        ("wait_ns", ctypes.c_uint64),
        ("unlock_count", ctypes.c_uint64),
    ]


class SscVoteSlot(ctypes.Structure):
    _fields_ = [
        ("epoch", ctypes.c_uint64),
        ("last_run_ns", ctypes.c_uint64),
        ("last_wait_ns", ctypes.c_uint64),
        ("last_unlock_count", ctypes.c_uint64),
    ]


@dataclass
class MapMeta:
    map_id: int
    name: str
    map_type: str
    value_size: int
    max_entries: int
    fd: int
    owner_pid: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sample lb_simple BPF runtime maps every N microseconds."
    )
    parser.add_argument(
        "--interval-us",
        type=int,
        default=100,
        help="Sampling interval in microseconds (default: 100)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Stop after N samples (default: run until Ctrl-C)",
    )
    parser.add_argument(
        "--duration-s",
        type=float,
        default=0.0,
        help="Stop after this many seconds (default: run until Ctrl-C)",
    )
    parser.add_argument(
        "--output",
        default="-",
        help="CSV output path, or - for stdout (default: -)",
    )
    parser.add_argument(
        "--ops-name",
        default="lb_simple",
        help="Expected sched_ext ops name (default: lb_simple)",
    )
    parser.add_argument(
        "--layout",
        default="auto",
        choices=["auto", "v1", "v2", "legacy", "current"],
        help="BPF globals layout profile: auto, v1/legacy, or v2/current (default: auto)",
    )
    parser.add_argument(
        "--pid",
        type=int,
        help="Use a specific owner PID instead of auto-discovery",
    )
    parser.add_argument(
        "--include-agg",
        action="store_true",
        help="Also sample agg_percpu_map summary",
    )
    parser.add_argument(
        "--include-stats-map",
        action="store_true",
        help="Also sample stats_map[0..15]",
    )
    parser.add_argument(
        "--include-slots",
        action="store_true",
        help="Also sample ssc_vote_slot_map summary",
    )
    parser.add_argument(
        "--slot-limit",
        type=int,
        default=16,
        help="Maximum number of vote slots to inspect per sample (default: 16)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress discovery logs on stderr",
    )
    args = parser.parse_args()

    if args.interval_us <= 0:
        parser.error("--interval-us must be > 0")
    if args.count < 0:
        parser.error("--count must be >= 0")
    if args.duration_s < 0:
        parser.error("--duration-s must be >= 0")
    if args.slot_limit <= 0:
        parser.error("--slot-limit must be > 0")
    return args


def sys_bpf_nr() -> int:
    machine = os.uname().machine
    nr = SYS_BPF_BY_ARCH.get(machine)
    if nr is None:
        raise SystemExit(f"unsupported architecture for bpf syscall: {machine}")
    return nr


def require_root() -> None:
    if os.geteuid() != 0:
        raise SystemExit("run as root, e.g. sudo python3 bench/mutexbench/scripts/sample_lb_simple_bpf.py ...")


def read_text_if_exists(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return ""


def bpftool_text(args: List[str]) -> str:
    try:
        proc = subprocess.run(
            ["bpftool", *args],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise SystemExit("bpftool is required but was not found in PATH") from exc
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip()
        stdout = exc.stdout.strip()
        detail = stderr or stdout or str(exc)
        raise SystemExit(f"bpftool {' '.join(args)} failed: {detail}") from exc
    return proc.stdout


def parse_struct_ops_map_ids(text: str) -> List[int]:
    map_ids: List[int] = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[0].endswith(":") and parts[2] == "sched_ext_ops":
            try:
                map_ids.append(int(parts[0][:-1]))
            except ValueError:
                continue
    return map_ids


def parse_owner_pids(map_show_text: str) -> List[int]:
    return sorted({int(pid) for pid in re.findall(r"\((\d+)\)", map_show_text)})


def discover_owner_pids() -> List[int]:
    struct_ops_text = bpftool_text(["struct_ops", "show"])
    owner_pids = set()
    for map_id in parse_struct_ops_map_ids(struct_ops_text):
        owner_pids.update(parse_owner_pids(bpftool_text(["map", "show", "id", str(map_id)])))
    return sorted(owner_pids)


def collect_map_ids_from_pid(pid: int) -> List[int]:
    fdinfo_dir = FDINFO_ROOT / str(pid) / "fdinfo"
    if not fdinfo_dir.is_dir():
        return []

    map_ids = set()
    for fdinfo in fdinfo_dir.iterdir():
        try:
            text = fdinfo.read_text(encoding="utf-8")
        except (FileNotFoundError, PermissionError, OSError):
            continue
        match = re.search(r"^map_id:\s*(\d+)\s*$", text, re.MULTILINE)
        if match:
            map_ids.add(int(match.group(1)))
    return sorted(map_ids)


def parse_map_show(text: str, owner_pid: int, map_id: int, fd: int) -> Optional[MapMeta]:
    normalized = " ".join(text.split())
    tokens = normalized.split()
    if len(tokens) < 2 or not tokens[0].endswith(":"):
        return None

    map_type = tokens[1]
    name = ""
    if "name" in tokens:
        idx = tokens.index("name")
        if idx + 1 < len(tokens):
            name = tokens[idx + 1]

    value_match = re.search(r"\bvalue\s+(\d+)B\b", normalized)
    max_entries_match = re.search(r"\bmax_entries\s+(\d+)\b", normalized)
    if not value_match or not max_entries_match:
        return None

    return MapMeta(
        map_id=map_id,
        name=name,
        map_type=map_type,
        value_size=int(value_match.group(1)),
        max_entries=int(max_entries_match.group(1)),
        fd=fd,
        owner_pid=owner_pid,
    )


def canonical_map_name(name: str) -> str:
    if name.endswith(".bss") or name == ".bss" or name.endswith("bss"):
        return "bss"
    if name.endswith(".data") or name == ".data" or name.endswith("data"):
        return "data"
    if name.startswith("stats_map"):
        return "stats_map"
    if name.startswith("agg_percpu"):
        return "agg_percpu_map"
    if name.startswith("ssc_vote_slot"):
        return "ssc_vote_slot_map"
    if name.startswith("task_ctx_map"):
        return "task_ctx_map"
    if name.startswith("thread_ctx_addr"):
        return "thread_ctx_addr_map"
    if name.startswith("cpu_to_node"):
        return "cpu_to_node"
    return name


def bpf_map_get_fd_by_id(map_id: int) -> int:
    attr = BpfAttrGetFdById(map_id=map_id, next_id=0, open_flags=0)
    fd = libc.syscall(sys_bpf_nr(), BPF_MAP_GET_FD_BY_ID, ctypes.byref(attr), ctypes.sizeof(attr))
    if fd < 0:
        err = ctypes.get_errno()
        raise OSError(err, f"BPF_MAP_GET_FD_BY_ID({map_id}) failed: {os.strerror(err)}")
    return int(fd)


def discover_maps_for_pid(pid: int) -> Dict[str, MapMeta]:
    maps: Dict[str, MapMeta] = {}
    for map_id in collect_map_ids_from_pid(pid):
        try:
            fd = bpf_map_get_fd_by_id(map_id)
            meta = parse_map_show(bpftool_text(["map", "show", "id", str(map_id)]), pid, map_id, fd)
        except (OSError, SystemExit):
            continue
        if meta is None:
            os.close(fd)
            continue

        key = canonical_map_name(meta.name)
        if key in {
            "bss",
            "data",
            "stats_map",
            "agg_percpu_map",
            "ssc_vote_slot_map",
            "task_ctx_map",
            "thread_ctx_addr_map",
            "cpu_to_node",
        }:
            if key in maps:
                os.close(fd)
            else:
                maps[key] = meta
        else:
            os.close(fd)
    return maps


def sched_ext_ops_matches(expected_ops: str, actual_ops: str) -> bool:
    return actual_ops == expected_ops or actual_ops.startswith(f"{expected_ops}_")


def ensure_sched_ext_ops(expected_ops: str) -> None:
    state = read_text_if_exists(SCHED_EXT_STATE)
    ops = read_text_if_exists(SCHED_EXT_OPS)
    if state and state != "enabled":
        raise SystemExit(f"sched_ext is not enabled (state={state})")
    if ops and not sched_ext_ops_matches(expected_ops, ops):
        raise SystemExit(f"sched_ext active ops is {ops!r}, expected prefix {expected_ops!r}")


def choose_pid_and_maps(args: argparse.Namespace) -> tuple[int, Dict[str, MapMeta]]:
    if args.pid is not None:
        maps = discover_maps_for_pid(args.pid)
        if "bss" not in maps or "data" not in maps:
            raise SystemExit(f"PID {args.pid} does not expose the required lb_simple .bss/.data maps")
        return args.pid, maps

    owner_pids = discover_owner_pids()
    if not owner_pids:
        raise SystemExit("no sched_ext owner PID was found via bpftool struct_ops show")

    for pid in owner_pids:
        maps = discover_maps_for_pid(pid)
        if "bss" in maps and "data" in maps:
            return pid, maps

    raise SystemExit(
        "found sched_ext owner PID(s), but none exposed the required lb_simple .bss/.data maps"
    )


def bpf_map_lookup(fd: int, key_bytes: bytes, value_size: int) -> bytes:
    key_buf = ctypes.create_string_buffer(key_bytes, len(key_bytes))
    value_buf = ctypes.create_string_buffer(value_size)
    attr = BpfAttrMapLookupElem(
        map_fd=fd,
        _pad=0,
        key=ctypes.addressof(key_buf),
        value=ctypes.addressof(value_buf),
        flags=0,
    )
    ret = libc.syscall(sys_bpf_nr(), BPF_MAP_LOOKUP_ELEM, ctypes.byref(attr), ctypes.sizeof(attr))
    if ret != 0:
        err = ctypes.get_errno()
        raise OSError(err, f"BPF_MAP_LOOKUP_ELEM failed: {os.strerror(err)}")
    return value_buf.raw


def u32_key(key: int) -> bytes:
    return struct.pack("I", key)


def possible_cpu_count() -> int:
    possible = Path("/sys/devices/system/cpu/possible")
    text = read_text_if_exists(possible)
    if not text:
        return max(os.cpu_count() or 1, 1)

    count = 0
    for chunk in text.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "-" in chunk:
            start_s, end_s = chunk.split("-", 1)
            count += int(end_s) - int(start_s) + 1
        else:
            count += 1
    return max(count, 1)


def round_up_8(value: int) -> int:
    return (value + 7) & ~7


def parse_struct(raw: bytes, struct_type: type[ctypes.Structure]) -> ctypes.Structure:
    return struct_type.from_buffer_copy(raw)


def parse_u64(raw: bytes) -> int:
    return struct.unpack("Q", raw)[0]


def search_phase_name(phase: int) -> str:
    if phase == 0:
        return "seek"
    if phase == 1:
        return "refine"
    return f"unknown_{phase}"


def active_cpu_list(bss: ctypes.Structure, data: ctypes.Structure) -> List[int]:
    limit = min(int(data.ssc_active_count), int(bss.ssc_cpu_count), MAX_CPUS)
    return [int(bss.ssc_cpu_list[idx]) for idx in range(limit)]


def read_agg_summary(meta: MapMeta) -> Dict[str, int]:
    stride = round_up_8(ctypes.sizeof(AggPercpu))
    cpu_count = possible_cpu_count()
    raw = bpf_map_lookup(meta.fd, u32_key(0), stride * cpu_count)

    nonzero = 0
    epoch_max = 0
    run_sum = 0
    wait_sum = 0
    unlock_sum = 0
    struct_size = ctypes.sizeof(AggPercpu)

    for cpu in range(cpu_count):
        start = cpu * stride
        item = AggPercpu.from_buffer_copy(raw[start : start + struct_size])
        if item.epoch or item.run_ns or item.wait_ns or item.unlock_count:
            nonzero += 1
        epoch_max = max(epoch_max, int(item.epoch))
        run_sum += int(item.run_ns)
        wait_sum += int(item.wait_ns)
        unlock_sum += int(item.unlock_count)

    return {
        "agg_cpu_nonzero": nonzero,
        "agg_epoch_max": epoch_max,
        "agg_run_ns_sum": run_sum,
        "agg_wait_ns_sum": wait_sum,
        "agg_unlock_count_sum": unlock_sum,
    }


def read_stats_map(meta: MapMeta) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for idx in range(min(meta.max_entries, STAT_NR)):
        out[f"stat_{idx}"] = parse_u64(bpf_map_lookup(meta.fd, u32_key(idx), meta.value_size))
    return out


def read_slot_summary(meta: MapMeta, active_count: int, slot_limit: int) -> Dict[str, int]:
    count = min(max(active_count, 0), meta.max_entries, slot_limit)
    epoch_max = 0
    run_sum = 0
    wait_sum = 0
    unlock_sum = 0

    for idx in range(count):
        slot = parse_struct(bpf_map_lookup(meta.fd, u32_key(idx), meta.value_size), SscVoteSlot)
        epoch_max = max(epoch_max, int(slot.epoch))
        run_sum += int(slot.last_run_ns)
        wait_sum += int(slot.last_wait_ns)
        unlock_sum += int(slot.last_unlock_count)

    return {
        "slot_count": count,
        "slot_epoch_max": epoch_max,
        "slot_run_ns_sum": run_sum,
        "slot_wait_ns_sum": wait_sum,
        "slot_unlock_count_sum": unlock_sum,
    }


def normalize_layout_name(name: str) -> str:
    return LAYOUT_ALIASES.get(name, name)


def choose_layout_profile(args: argparse.Namespace, maps: Dict[str, MapMeta]) -> LayoutProfile:
    requested = normalize_layout_name(args.layout)
    if requested != "auto":
        return LAYOUT_PROFILES[requested]

    data_size = maps["data"].value_size
    if data_size >= ctypes.sizeof(CurrentDataGlobals):
        return LAYOUT_PROFILES["v2"]
    return LAYOUT_PROFILES["v1"]


def validate_struct_sizes(maps: Dict[str, MapMeta], layout: LayoutProfile) -> None:
    bss_size = ctypes.sizeof(layout.bss_struct)
    data_size = ctypes.sizeof(layout.data_struct)
    if maps["bss"].value_size < bss_size:
        raise SystemExit(
            f".bss size too small for layout {layout.name}: map={maps['bss'].value_size} local_struct={bss_size}"
        )
    if maps["data"].value_size < data_size:
        raise SystemExit(
            f".data size too small for layout {layout.name}: map={maps['data'].value_size} local_struct={data_size}"
        )
    if "agg_percpu_map" in maps and maps["agg_percpu_map"].value_size != ctypes.sizeof(AggPercpu):
        raise SystemExit(
            "agg_percpu_map value size mismatch; update AggPercpu layout before using --include-agg"
        )
    if "ssc_vote_slot_map" in maps and maps["ssc_vote_slot_map"].value_size != ctypes.sizeof(SscVoteSlot):
        raise SystemExit(
            "ssc_vote_slot_map value size mismatch; update SscVoteSlot layout before using --include-slots"
        )


def build_fieldnames(args: argparse.Namespace) -> List[str]:
    fields = [
        "sample_index",
        "monotonic_ns",
        "elapsed_ns",
        "scheduled_ns",
        "late_ns",
        "read_ns",
        "ops_name",
        "owner_pid",
        "layout_profile",
        "dominant_node",
        "ssc_cpu_count",
        "ssc_active_count",
        "ssc_best_count",
        "ssc_refine_low",
        "ssc_refine_high",
        "ssc_active_cpus",
        "stats_only_mode",
        "forced_release_cnt",
        "ssc_vote_window_ns",
        "ssc_vote_epoch",
        "ssc_vote_start_ns",
        "ssc_vote_decided_epoch",
        "ssc_vote_publish_count",
        "ssc_vote_sum_run",
        "ssc_vote_sum_wait",
        "ssc_vote_sum_unlock_count",
        "ssc_vote_last_score",
        "ssc_vote_last_effective_score",
        "ssc_bootstrap_mature_windows",
        "ssc_pending_capped_grow",
        "ssc_vote_consec_grow",
        "ssc_vote_consec_shrink",
        "ssc_search_phase",
        "ssc_search_phase_name",
        "ssc_best_score",
        "ssc_best_candidate_count",
        "ssc_best_candidate_streak",
        "dbg_counters_enabled",
        "dbg_win_run",
        "dbg_win_wait",
        "dbg_acct_calls",
        "dbg_acct_read_ok",
        "dbg_refine_entries",
        "dbg_refine_single_point",
        "dbg_refine_noop_targets",
        "dbg_noop_resizes",
        "dbg_active_count_changes",
        "dbg_bad_steady_rebases",
        "dbg_task_ctx_creates",
        "dbg_task_ctx_misses",
        "dbg_grow_uses_capped_step",
        "dbg_last_grow_target",
    ]
    if args.include_agg:
        fields.extend(
            [
                "agg_cpu_nonzero",
                "agg_epoch_max",
                "agg_run_ns_sum",
                "agg_wait_ns_sum",
                "agg_unlock_count_sum",
            ]
        )
    if args.include_stats_map:
        fields.extend([f"stat_{idx}" for idx in range(STAT_NR)])
    if args.include_slots:
        fields.extend(
            [
                "slot_count",
                "slot_epoch_max",
                "slot_run_ns_sum",
                "slot_wait_ns_sum",
                "slot_unlock_count_sum",
            ]
        )
    return fields


def take_sample(
    maps: Dict[str, MapMeta],
    args: argparse.Namespace,
    layout: LayoutProfile,
    sample_index: int,
    owner_pid: int,
    start_ns: int,
    scheduled_ns: int,
) -> Dict[str, object]:
    sample_start_ns = time.monotonic_ns()
    data = parse_struct(
        bpf_map_lookup(maps["data"].fd, u32_key(0), maps["data"].value_size),
        layout.data_struct,
    )
    bss = parse_struct(
        bpf_map_lookup(maps["bss"].fd, u32_key(0), maps["bss"].value_size),
        layout.bss_struct,
    )
    sample_end_ns = time.monotonic_ns()

    row: Dict[str, object] = {
        "sample_index": sample_index,
        "monotonic_ns": sample_start_ns,
        "elapsed_ns": sample_start_ns - start_ns,
        "scheduled_ns": scheduled_ns,
        "late_ns": max(sample_start_ns - scheduled_ns, 0),
        "read_ns": sample_end_ns - sample_start_ns,
        "ops_name": args.ops_name,
        "owner_pid": owner_pid,
        "layout_profile": layout.name,
        "dominant_node": int(bss.dominant_node),
        "ssc_cpu_count": int(bss.ssc_cpu_count),
        "ssc_active_count": int(data.ssc_active_count),
        "ssc_best_count": int(data.ssc_best_count),
        "ssc_refine_low": int(data.ssc_refine_low),
        "ssc_refine_high": int(data.ssc_refine_high),
        "ssc_active_cpus": ";".join(str(cpu) for cpu in active_cpu_list(bss, data)),
        "stats_only_mode": int(bss.stats_only_mode),
        "forced_release_cnt": int(bss.forced_release_cnt),
        "ssc_vote_window_ns": int(data.ssc_vote_window_ns),
        "ssc_vote_epoch": int(bss.ssc_vote_epoch),
        "ssc_vote_start_ns": int(bss.ssc_vote_start_ns),
        "ssc_vote_decided_epoch": int(bss.ssc_vote_decided_epoch),
        "ssc_vote_publish_count": int(bss.ssc_vote_publish_count),
        "ssc_vote_sum_run": int(bss.ssc_vote_sum_run),
        "ssc_vote_sum_wait": int(bss.ssc_vote_sum_wait),
        "ssc_vote_sum_unlock_count": int(bss.ssc_vote_sum_unlock_count),
        "ssc_vote_last_score": int(bss.ssc_vote_last_score),
        "ssc_vote_last_effective_score": int(bss.ssc_vote_last_effective_score),
        "ssc_bootstrap_mature_windows": int(bss.ssc_bootstrap_mature_windows),
        "ssc_pending_capped_grow": int(bss.ssc_pending_capped_grow),
        "ssc_vote_consec_grow": int(bss.ssc_vote_consec_grow),
        "ssc_vote_consec_shrink": int(bss.ssc_vote_consec_shrink),
        "ssc_search_phase": int(bss.ssc_search_phase),
        "ssc_search_phase_name": search_phase_name(int(bss.ssc_search_phase)),
        "ssc_best_score": int(bss.ssc_best_score),
        "ssc_best_candidate_count": int(bss.ssc_best_candidate_count),
        "ssc_best_candidate_streak": int(bss.ssc_best_candidate_streak),
        "dbg_counters_enabled": int(bss.dbg_counters_enabled),
        "dbg_win_run": int(bss.dbg_win_run),
        "dbg_win_wait": int(bss.dbg_win_wait),
        "dbg_acct_calls": int(bss.dbg_acct_calls),
        "dbg_acct_read_ok": int(bss.dbg_acct_read_ok),
        "dbg_refine_entries": int(bss.dbg_refine_entries),
        "dbg_refine_single_point": int(bss.dbg_refine_single_point),
        "dbg_refine_noop_targets": int(bss.dbg_refine_noop_targets),
        "dbg_noop_resizes": int(bss.dbg_noop_resizes),
        "dbg_active_count_changes": int(bss.dbg_active_count_changes),
        "dbg_bad_steady_rebases": int(bss.dbg_bad_steady_rebases),
        "dbg_task_ctx_creates": int(bss.dbg_task_ctx_creates),
        "dbg_task_ctx_misses": int(bss.dbg_task_ctx_misses),
        "dbg_grow_uses_capped_step": int(bss.dbg_grow_uses_capped_step),
        "dbg_last_grow_target": int(bss.dbg_last_grow_target),
    }

    if args.include_agg:
        meta = maps.get("agg_percpu_map")
        if meta is None:
            raise SystemExit("--include-agg was requested, but agg_percpu_map was not found")
        row.update(read_agg_summary(meta))

    if args.include_stats_map:
        meta = maps.get("stats_map")
        if meta is None:
            raise SystemExit("--include-stats-map was requested, but stats_map was not found")
        row.update(read_stats_map(meta))

    if args.include_slots:
        meta = maps.get("ssc_vote_slot_map")
        if meta is None:
            raise SystemExit("--include-slots was requested, but ssc_vote_slot_map was not found")
        row.update(read_slot_summary(meta, int(data.ssc_active_count), args.slot_limit))

    return row


def poll_until(deadline_ns: int) -> None:
    while time.monotonic_ns() < deadline_ns:
        pass


def log(msg: str, quiet: bool) -> None:
    if not quiet:
        print(msg, file=sys.stderr, flush=True)


def open_output(path: str):
    if path == "-":
        return sys.stdout, False
    return open(path, "w", newline="", encoding="utf-8"), True


def main() -> int:
    args = parse_args()
    require_root()
    ensure_sched_ext_ops(args.ops_name)
    owner_pid, maps = choose_pid_and_maps(args)
    layout = choose_layout_profile(args, maps)
    validate_struct_sizes(maps, layout)

    log(
        (
            f"[sample_lb_simple_bpf] owner_pid={owner_pid} interval_us={args.interval_us} "
            f"layout={layout.name} maps={','.join(sorted(maps))} output={args.output}"
        ),
        args.quiet,
    )

    out_fh, should_close = open_output(args.output)
    try:
        writer = csv.DictWriter(out_fh, fieldnames=build_fieldnames(args))
        writer.writeheader()
        out_fh.flush()

        interval_ns = args.interval_us * 1_000
        start_ns = time.monotonic_ns()
        next_deadline_ns = start_ns
        duration_ns = int(args.duration_s * 1_000_000_000)
        sample_index = 0

        while True:
            row = take_sample(maps, args, layout, sample_index, owner_pid, start_ns, next_deadline_ns)
            writer.writerow(row)
            out_fh.flush()

            sample_index += 1
            if args.count and sample_index >= args.count:
                break
            if duration_ns and (time.monotonic_ns() - start_ns) >= duration_ns:
                break

            next_deadline_ns += interval_ns
            poll_until(next_deadline_ns)
    except KeyboardInterrupt:
        log("[sample_lb_simple_bpf] interrupted", args.quiet)
    finally:
        for meta in maps.values():
            try:
                os.close(meta.fd)
            except OSError:
                pass
        if should_close:
            out_fh.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
