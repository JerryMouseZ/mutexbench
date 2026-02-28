#!/usr/bin/env python3
"""Recommend thread count for a given lock/critical_iters/outside_iters.

The script works in two stages:
1) Exact lookup on measured points (best fidelity when config exists).
2) Rule fallback when exact point is missing, using bins learned from data.

Rationale for the rule design is documented inline in comments.
"""

from __future__ import annotations

import argparse
import csv
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Sequence, Tuple


Thread = int
Critical = int
Outside = int
LockName = str
Pair = Tuple[Critical, Outside]


# Keep a stable thread order for "smallest thread that already works" logic.
THREAD_ORDER: Tuple[Thread, ...] = (1, 2, 4, 8, 16, 32, 48, 64, 80, 96, 128, 160)
THREAD_RANK = {t: i for i, t in enumerate(THREAD_ORDER)}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Recommend thread count from mutex benchmark summaries "
            "(exact point first, rule fallback second)."
        )
    )
    p.add_argument(
        "--results-root",
        default="results-new",
        help="Root directory that contains <lock>/summary.csv (default: results-new)",
    )
    p.add_argument(
        "--lock",
        required=True,
        help='Lock name (e.g. clh|mcs|hapax|reciprocating) or "all"',
    )
    p.add_argument("--critical-iters", type=int, required=True, help="critical_iters")
    p.add_argument("--outside-iters", type=int, required=True, help="outside_iters")
    p.add_argument(
        "--neighbors",
        type=int,
        default=4,
        help="How many nearest measured points to print for context (default: 4)",
    )
    return p.parse_args()


def list_locks(results_root: Path) -> List[LockName]:
    locks: List[LockName] = []
    for child in sorted(results_root.iterdir()):
        if child.is_dir() and (child / "summary.csv").is_file():
            locks.append(child.name)
    return locks


def read_summary(path: Path) -> List[Tuple[Thread, Critical, Outside, float]]:
    rows: List[Tuple[Thread, Critical, Outside, float]] = []
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        required = {
            "threads",
            "critical_iters",
            "outside_iters",
            "mean_throughput_ops_per_sec",
        }
        if not required.issubset(reader.fieldnames or set()):
            missing = sorted(required - set(reader.fieldnames or []))
            raise SystemExit(f"{path}: missing columns: {', '.join(missing)}")
        for r in reader:
            rows.append(
                (
                    int(r["threads"]),
                    int(r["critical_iters"]),
                    int(r["outside_iters"]),
                    float(r["mean_throughput_ops_per_sec"]),
                )
            )
    return rows


def choose_argmax(thread_to_tp: Mapping[Thread, float]) -> Thread:
    max_tp = max(thread_to_tp.values())
    # Tie-break to smaller thread count to avoid unnecessary oversubscription.
    best = [t for t, tp in thread_to_tp.items() if tp == max_tp]
    return sorted(best, key=lambda t: THREAD_RANK.get(t, 10**9))[0]


def choose_t95(thread_to_tp: Mapping[Thread, float]) -> Thread:
    max_tp = max(thread_to_tp.values())
    need = 0.95 * max_tp
    for t in THREAD_ORDER:
        tp = thread_to_tp.get(t)
        if tp is not None and tp >= need:
            return t
    return choose_argmax(thread_to_tp)


def build_pair_metrics(
    rows: Sequence[Tuple[Thread, Critical, Outside, float]]
) -> Dict[Pair, Dict[str, float]]:
    grouped: MutableMapping[Pair, Dict[Thread, float]] = defaultdict(dict)
    for t, c, o, tp in rows:
        grouped[(c, o)][t] = tp

    out: Dict[Pair, Dict[str, float]] = {}
    for (c, o), thread_to_tp in grouped.items():
        argmax_t = choose_argmax(thread_to_tp)
        t95 = choose_t95(thread_to_tp)
        out[(c, o)] = {
            "argmax": float(argmax_t),
            "t95": float(t95),
            "max_tp": max(thread_to_tp.values()),
        }
    return out


def ratio_bin(r: float) -> str:
    if r <= 1.0:
        return "<=1"
    if r <= 4.0:
        return "1-4"
    if r <= 16.0:
        return "4-16"
    return ">16"


def crit_bin(c: int) -> str:
    # Data has sampled critical sizes {10,50,100,200,400,...}.
    # For unseen c (e.g. 120), split at geometric midpoint sqrt(100*200) ~= 141
    # so interpolation is more balanced in log-space.
    return "<=100" if c <= 141 else ">=200"


def pick_mode(counter: Counter[int]) -> int:
    if not counter:
        raise ValueError("empty counter")
    # Prefer higher support; tie-break to smaller thread for efficiency.
    return sorted(counter.items(), key=lambda kv: (-kv[1], THREAD_RANK.get(kv[0], 10**9)))[0][0]


def build_rule_model(
    pair_metrics: Mapping[Pair, Mapping[str, float]]
) -> Dict[Tuple[str, str], Dict[str, int]]:
    # Why ratio+critical bins:
    # - ratio out/crit captures how often threads hit the lock.
    # - critical size changes lock-hold behavior even at same ratio.
    # This keeps the model simple and interpretable.
    bucket_t95: MutableMapping[Tuple[str, str], Counter[int]] = defaultdict(Counter)
    bucket_argmax: MutableMapping[Tuple[str, str], Counter[int]] = defaultdict(Counter)

    ratio_only_t95: MutableMapping[str, Counter[int]] = defaultdict(Counter)
    ratio_only_argmax: MutableMapping[str, Counter[int]] = defaultdict(Counter)
    global_t95: Counter[int] = Counter()
    global_argmax: Counter[int] = Counter()

    for (c, o), m in pair_metrics.items():
        rb = ratio_bin(o / c)
        cb = crit_bin(c)
        key = (cb, rb)
        t95 = int(m["t95"])
        argmax = int(m["argmax"])
        bucket_t95[key][t95] += 1
        bucket_argmax[key][argmax] += 1
        ratio_only_t95[rb][t95] += 1
        ratio_only_argmax[rb][argmax] += 1
        global_t95[t95] += 1
        global_argmax[argmax] += 1

    model: Dict[Tuple[str, str], Dict[str, int]] = {}
    for cb in ("<=100", ">=200"):
        for rb in ("<=1", "1-4", "4-16", ">16"):
            key = (cb, rb)
            if bucket_t95[key]:
                t95 = pick_mode(bucket_t95[key])
                argmax = pick_mode(bucket_argmax[key])
            elif ratio_only_t95[rb]:
                # Backoff to ratio-only if this crit+ratio bucket is sparse.
                t95 = pick_mode(ratio_only_t95[rb])
                argmax = pick_mode(ratio_only_argmax[rb])
            else:
                # Final fallback to global mode.
                t95 = pick_mode(global_t95)
                argmax = pick_mode(global_argmax)
            model[key] = {"t95": t95, "argmax": argmax}
    return model


def nearest_points(
    pair_metrics: Mapping[Pair, Mapping[str, float]],
    c: int,
    o: int,
    k: int,
) -> List[Tuple[Critical, Outside, int, int, float]]:
    out: List[Tuple[Critical, Outside, int, int, float]] = []
    qc = math.log2(max(c, 1))
    qo = math.log2(max(o, 1))
    for (pc, po), m in pair_metrics.items():
        dc = math.log2(max(pc, 1)) - qc
        do = math.log2(max(po, 1)) - qo
        d = math.sqrt(dc * dc + do * do)
        out.append((pc, po, int(m["t95"]), int(m["argmax"]), d))
    out.sort(key=lambda x: x[4])
    return out[: max(0, k)]


def recommend_for_lock(
    lock: str,
    pair_metrics: Mapping[Pair, Mapping[str, float]],
    c: int,
    o: int,
    neighbors: int,
) -> None:
    print(f"[{lock}]")
    r = o / c
    rb = ratio_bin(r)
    cb = crit_bin(c)
    print(f"  query: critical_iters={c}, outside_iters={o}, outside/critical={r:.4f}")

    exact = pair_metrics.get((c, o))
    if exact is not None:
        print("  exact_match: yes")
        print(f"  recommend_t95: {int(exact['t95'])}  (preferred stable choice)")
        print(f"  recommend_argmax: {int(exact['argmax'])}  (peak-throughput choice)")
    else:
        print("  exact_match: no")
        model = build_rule_model(pair_metrics)
        rec = model[(cb, rb)]
        print(f"  rule_bin: crit {cb}, ratio {rb}")
        print(f"  recommend_t95: {rec['t95']}  (preferred stable choice)")
        print(f"  recommend_argmax: {rec['argmax']}  (peak-throughput choice)")

    near = nearest_points(pair_metrics, c, o, neighbors)
    if near:
        print("  nearest_points:")
        for pc, po, t95, argmax, dist in near:
            print(
                "    "
                f"(c={pc}, o={po}) -> t95={t95}, argmax={argmax}, "
                f"log_distance={dist:.3f}"
            )
    print()


def main() -> None:
    args = parse_args()
    if args.critical_iters <= 0 or args.outside_iters <= 0:
        raise SystemExit("--critical-iters and --outside-iters must be > 0")

    root = Path(args.results_root)
    if not root.is_dir():
        raise SystemExit(f"results root not found: {root}")

    locks = list_locks(root)
    if not locks:
        raise SystemExit(f"no <lock>/summary.csv found under: {root}")

    if args.lock == "all":
        target_locks = locks
    else:
        if args.lock not in locks:
            raise SystemExit(
                f"unknown lock '{args.lock}'. available: {', '.join(locks)}"
            )
        target_locks = [args.lock]

    print(
        "Method: exact lookup first; if missing, use data-derived bin rules "
        "(crit-bin + out/crit-bin)."
    )
    print("Recommendation types: t95 (stable) and argmax (peak).")
    print()

    for lock in target_locks:
        rows = read_summary(root / lock / "summary.csv")
        pair_metrics = build_pair_metrics(rows)
        recommend_for_lock(
            lock=lock,
            pair_metrics=pair_metrics,
            c=args.critical_iters,
            o=args.outside_iters,
            neighbors=args.neighbors,
        )


if __name__ == "__main__":
    main()
