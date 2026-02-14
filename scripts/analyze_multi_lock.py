#!/usr/bin/env python3
"""Analyze multi-lock mutex benchmark results.

This script compares K>=2 locks under (critical_iters, outside_iters) workloads.
It outputs:
  - thread-level cell metrics with bootstrap CI
  - cell-level scaling metrics (AUC_eff) with bootstrap CI
  - pairwise ops/scaling ratios with BH-FDR significance
  - scenario summaries (6 scenarios: short/long x low/mid/high contention)
  - single-axis aggregated segments for ops/scaling trend stability
"""

from __future__ import annotations

import argparse
import csv
import math
import random
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


RepeatId = str
Thread = int
Critical = int
Outside = int
TcoKey = Tuple[Thread, Critical, Outside]
CoKey = Tuple[Critical, Outside]
LockName = str


SCENARIOS = (
    "short_low",
    "short_mid",
    "short_high",
    "long_low",
    "long_mid",
    "long_high",
)


@dataclass
class ThreadStats:
    mean: float
    stddev: float
    cv: float
    ci_low: float
    ci_high: float
    repeat_count: int
    unstable: bool
    missing: bool


@dataclass
class LockCellScore:
    score: float
    ci_low: float
    ci_high: float
    dist: List[float]
    unstable: bool


@dataclass
class CellSummary:
    metric: str
    critical: int
    outside: int
    lock_scores: Dict[LockName, LockCellScore]
    ranking: List[LockName]
    top1: LockName
    top1_score: float
    top1_ci_low: float
    top1_ci_high: float
    effect_ratio: float
    effect_ci_low: float
    effect_ci_high: float
    effect_log_var: float
    unstable: bool


@dataclass
class PairwiseResult:
    metric: str
    critical: int
    outside: int
    lock_a: LockName
    lock_b: LockName
    ratio: float
    ci_low: float
    ci_high: float
    p_raw: float
    p_adj: float
    significant: bool
    winner: str
    missing_cell: bool
    common_threads: int
    thread_list: str
    common_repeat_min: int


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze multi-lock throughput and scaling")
    p.add_argument("--results-root", default="results", help="Root results directory")
    p.add_argument("--locks", required=True, help="Comma-separated lock names")
    p.add_argument(
        "--threads",
        default="1,2,4,8,16,32,64",
        help="Comma-separated threads for scaling/ops aggregation",
    )
    p.add_argument("--alpha", type=float, default=0.05, help="Significance threshold")
    p.add_argument(
        "--fdr-method",
        default="bh",
        choices=("bh",),
        help="Multiple-testing correction method",
    )
    p.add_argument(
        "--adjacency",
        default="single-axis",
        choices=("single-axis",),
        help="Aggregation adjacency strategy",
    )
    p.add_argument(
        "--aggregate-threshold",
        type=float,
        default=10.0,
        help="Relative change threshold (%%) for adjacent-segment merge",
    )
    p.add_argument(
        "--out-dir",
        default="results/analysis_multi",
        help="Output directory",
    )
    p.add_argument(
        "--bootstrap-samples",
        type=int,
        default=2000,
        help="Bootstrap sample count",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility",
    )
    return p.parse_args()


def parse_csv_ints(csv_text: str, name: str) -> List[int]:
    vals = []
    for raw in csv_text.split(","):
        item = raw.strip()
        if not item:
            continue
        try:
            vals.append(int(item))
        except ValueError as exc:
            raise SystemExit(f"Invalid integer in {name}: {item}") from exc
    if not vals:
        raise SystemExit(f"No values in {name}")
    return sorted(set(vals))


def parse_locks(csv_text: str) -> List[str]:
    locks = [x.strip() for x in csv_text.split(",") if x.strip()]
    if len(locks) < 2:
        raise SystemExit("--locks requires at least two lock names")
    if len(set(locks)) != len(locks):
        raise SystemExit("--locks contains duplicate names")
    return locks


def mean(values: Sequence[float]) -> float:
    if not values:
        return float("nan")
    return sum(values) / float(len(values))


def pstdev(values: Sequence[float], m: Optional[float] = None) -> float:
    if not values:
        return float("nan")
    if len(values) == 1:
        return 0.0
    m = mean(values) if m is None else m
    var = sum((x - m) * (x - m) for x in values) / float(len(values))
    return math.sqrt(max(0.0, var))


def percentile(sorted_values: Sequence[float], q: float) -> float:
    if not sorted_values:
        return float("nan")
    if len(sorted_values) == 1:
        return sorted_values[0]
    q = min(max(q, 0.0), 1.0)
    pos = (len(sorted_values) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_values[lo]
    frac = pos - lo
    return sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac


def bootstrap_means(values: Sequence[float], b: int, rng: random.Random) -> List[float]:
    if not values:
        return []
    n = len(values)
    if n == 1:
        return [values[0]] * b
    out: List[float] = []
    for _ in range(b):
        s = 0.0
        for __ in range(n):
            s += values[rng.randrange(n)]
        out.append(s / float(n))
    return out


def ci95_from_dist(values: Sequence[float]) -> Tuple[float, float]:
    if not values:
        return float("nan"), float("nan")
    s = sorted(values)
    return percentile(s, 0.025), percentile(s, 0.975)


def geomean(values: Sequence[float]) -> float:
    if not values:
        return float("nan")
    if any(v <= 0.0 for v in values):
        return float("nan")
    return math.exp(sum(math.log(v) for v in values) / float(len(values)))


def log_var(values: Sequence[float]) -> float:
    logs = [math.log(v) for v in values if v > 0.0 and math.isfinite(v)]
    if len(logs) <= 1:
        return 0.0
    return pstdev(logs) ** 2


def benjamini_hochberg(results: List[PairwiseResult]) -> None:
    valid = [(i, r.p_raw) for i, r in enumerate(results) if math.isfinite(r.p_raw)]
    if not valid:
        return
    valid.sort(key=lambda x: x[1])
    m = len(valid)
    adj = [0.0] * m
    prev = 1.0
    for pos in range(m - 1, -1, -1):
        rank = pos + 1
        p = valid[pos][1]
        v = min(prev, p * m / float(rank))
        prev = v
        adj[pos] = min(1.0, v)
    for pos, (idx, _) in enumerate(valid):
        results[idx].p_adj = adj[pos]


def two_sided_p_from_dist_log_ratio(log_ratio_dist: Sequence[float]) -> float:
    if not log_ratio_dist:
        return float("nan")
    n = len(log_ratio_dist)
    le = sum(1 for x in log_ratio_dist if x <= 0.0) / float(n)
    ge = sum(1 for x in log_ratio_dist if x >= 0.0) / float(n)
    return min(1.0, 2.0 * min(le, ge))


def read_raw_csv(path: Path) -> Dict[TcoKey, Dict[RepeatId, float]]:
    if not path.exists():
        raise SystemExit(f"Missing raw file: {path}")
    out: Dict[TcoKey, Dict[RepeatId, float]] = defaultdict(dict)
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        required = {
            "threads",
            "critical_iters",
            "outside_iters",
            "repeat",
            "throughput_ops_per_sec",
        }
        missing_cols = required - set(reader.fieldnames or [])
        if missing_cols:
            raise SystemExit(f"{path}: missing columns: {sorted(missing_cols)}")
        for row in reader:
            t = int(row["threads"])
            c = int(row["critical_iters"])
            o = int(row["outside_iters"])
            repeat = row["repeat"].strip()
            v = float(row["throughput_ops_per_sec"])
            out[(t, c, o)][repeat] = v
    return out


def maybe_validate_summary(raw_map: Dict[TcoKey, Dict[RepeatId, float]], summary_path: Path) -> None:
    if not summary_path.exists():
        return
    mismatches = 0
    checked = 0
    with summary_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        required = {
            "threads",
            "critical_iters",
            "outside_iters",
            "mean_throughput_ops_per_sec",
        }
        missing_cols = required - set(reader.fieldnames or [])
        if missing_cols:
            print(f"[warn] {summary_path}: missing columns {sorted(missing_cols)}", file=sys.stderr)
            return
        for row in reader:
            k = (int(row["threads"]), int(row["critical_iters"]), int(row["outside_iters"]))
            if k not in raw_map:
                continue
            vals = list(raw_map[k].values())
            raw_mean = mean(vals)
            summary_mean = float(row["mean_throughput_ops_per_sec"])
            checked += 1
            tol = 1e-6 * max(1.0, abs(summary_mean))
            if abs(raw_mean - summary_mean) > tol:
                mismatches += 1
    if checked > 0 and mismatches > 0:
        print(
            f"[warn] {summary_path}: {mismatches}/{checked} rows mismatch raw mean",
            file=sys.stderr,
        )


def build_thread_stats(
    raw_by_lock: Dict[LockName, Dict[TcoKey, Dict[RepeatId, float]]],
    union_tco: List[TcoKey],
    boot_samples: int,
    rng: random.Random,
) -> Dict[LockName, Dict[TcoKey, ThreadStats]]:
    out: Dict[LockName, Dict[TcoKey, ThreadStats]] = {}
    for lock in sorted(raw_by_lock):
        lock_map = raw_by_lock[lock]
        stats_map: Dict[TcoKey, ThreadStats] = {}
        for key in union_tco:
            rep_map = lock_map.get(key)
            if not rep_map:
                stats_map[key] = ThreadStats(
                    mean=float("nan"),
                    stddev=float("nan"),
                    cv=float("nan"),
                    ci_low=float("nan"),
                    ci_high=float("nan"),
                    repeat_count=0,
                    unstable=True,
                    missing=True,
                )
                continue
            vals = [rep_map[r] for r in sorted(rep_map)]
            m = mean(vals)
            sd = pstdev(vals, m)
            cv = (sd / m) if m > 0.0 else float("inf")
            dist = bootstrap_means(vals, boot_samples, rng)
            ci_low, ci_high = ci95_from_dist(dist)
            stats_map[key] = ThreadStats(
                mean=m,
                stddev=sd,
                cv=cv,
                ci_low=ci_low,
                ci_high=ci_high,
                repeat_count=len(vals),
                unstable=bool(cv > 0.2),
                missing=False,
            )
        out[lock] = stats_map
    return out


def write_cell_metrics_csv(
    out_path: Path,
    locks: Sequence[LockName],
    union_tco: Sequence[TcoKey],
    stats_by_lock: Dict[LockName, Dict[TcoKey, ThreadStats]],
) -> None:
    fields = [
        "lock",
        "threads",
        "critical_iters",
        "outside_iters",
        "repeat_count",
        "mean_ops_per_sec",
        "stddev_ops_per_sec",
        "cv",
        "ci95_low",
        "ci95_high",
        "unstable",
        "missing_cell",
    ]
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for lock in sorted(locks):
            for (t, c, o) in union_tco:
                st = stats_by_lock[lock][(t, c, o)]
                w.writerow(
                    {
                        "lock": lock,
                        "threads": t,
                        "critical_iters": c,
                        "outside_iters": o,
                        "repeat_count": st.repeat_count,
                        "mean_ops_per_sec": f"{st.mean:.6f}" if not st.missing else "",
                        "stddev_ops_per_sec": f"{st.stddev:.6f}" if not st.missing else "",
                        "cv": f"{st.cv:.6f}" if not st.missing else "",
                        "ci95_low": f"{st.ci_low:.6f}" if not st.missing else "",
                        "ci95_high": f"{st.ci_high:.6f}" if not st.missing else "",
                        "unstable": int(st.unstable),
                        "missing_cell": int(st.missing),
                    }
                )


def compute_lock_scaling_row(
    lock: LockName,
    critical: int,
    outside: int,
    threads: Sequence[int],
    raw_map: Dict[TcoKey, Dict[RepeatId, float]],
    stats_map: Dict[TcoKey, ThreadStats],
    boot_samples: int,
    rng: random.Random,
) -> Dict[str, str]:
    means: Dict[int, float] = {}
    values_by_thread: Dict[int, List[float]] = {}
    unstable = False
    for t in threads:
        key = (t, critical, outside)
        rep_map = raw_map.get(key)
        if not rep_map:
            continue
        vals = [rep_map[r] for r in sorted(rep_map)]
        values_by_thread[t] = vals
        means[t] = mean(vals)
        unstable = unstable or stats_map[key].unstable

    row: Dict[str, str] = {
        "lock": lock,
        "critical_iters": str(critical),
        "outside_iters": str(outside),
        "threads_available": ",".join(str(t) for t in sorted(values_by_thread)),
        "auc_eff": "",
        "auc_ci95_low": "",
        "auc_ci95_high": "",
        "unstable": str(int(unstable)),
        "missing_cell": "0",
    }
    for t in threads:
        row[f"s_{t}"] = ""
        row[f"e_{t}"] = ""

    if 1 not in means:
        row["missing_cell"] = "1"
        return row

    base = means[1]
    if base <= 0.0:
        row["missing_cell"] = "1"
        return row

    aux = [t for t in threads if t != 1 and t in means]
    if not aux:
        row["missing_cell"] = "1"
        return row

    row["s_1"] = "1.000000"
    row["e_1"] = "1.000000"
    auc_terms: List[float] = []
    for t in aux:
        s = means[t] / base
        e = s / float(t)
        row[f"s_{t}"] = f"{s:.6f}"
        row[f"e_{t}"] = f"{e:.6f}"
        auc_terms.append(e)
    auc = mean(auc_terms)
    row["auc_eff"] = f"{auc:.6f}"

    dist: List[float] = []
    base_vals = values_by_thread[1]
    for _ in range(boot_samples):
        n1 = len(base_vals)
        base_sample = [base_vals[rng.randrange(n1)] for __ in range(n1)]
        m1 = mean(base_sample)
        if m1 <= 0.0:
            continue
        e_terms: List[float] = []
        ok = True
        for t in aux:
            vals = values_by_thread[t]
            nt = len(vals)
            sampled = [vals[rng.randrange(nt)] for __ in range(nt)]
            mt = mean(sampled)
            if mt <= 0.0:
                ok = False
                break
            e_terms.append((mt / m1) / float(t))
        if ok and e_terms:
            dist.append(mean(e_terms))
    lo, hi = ci95_from_dist(dist)
    if dist:
        row["auc_ci95_low"] = f"{lo:.6f}"
        row["auc_ci95_high"] = f"{hi:.6f}"
    return row


def write_cell_scaling_csv(
    out_path: Path,
    locks: Sequence[LockName],
    union_co: Sequence[CoKey],
    threads: Sequence[int],
    raw_by_lock: Dict[LockName, Dict[TcoKey, Dict[RepeatId, float]]],
    stats_by_lock: Dict[LockName, Dict[TcoKey, ThreadStats]],
    boot_samples: int,
    rng: random.Random,
) -> None:
    fields = [
        "lock",
        "critical_iters",
        "outside_iters",
        "threads_available",
        "auc_eff",
        "auc_ci95_low",
        "auc_ci95_high",
        "unstable",
        "missing_cell",
    ]
    for t in threads:
        fields.append(f"s_{t}")
        fields.append(f"e_{t}")

    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for lock in sorted(locks):
            for (c, o) in union_co:
                row = compute_lock_scaling_row(
                    lock,
                    c,
                    o,
                    threads,
                    raw_by_lock[lock],
                    stats_by_lock[lock],
                    boot_samples,
                    rng,
                )
                w.writerow(row)


def common_repeat_values(
    raw_by_lock: Dict[LockName, Dict[TcoKey, Dict[RepeatId, float]]],
    lock_a: str,
    lock_b: str,
    t: int,
    c: int,
    o: int,
) -> Tuple[List[float], List[float]]:
    map_a = raw_by_lock[lock_a].get((t, c, o))
    map_b = raw_by_lock[lock_b].get((t, c, o))
    if not map_a or not map_b:
        return [], []
    reps = sorted(set(map_a).intersection(map_b))
    if not reps:
        return [], []
    return [map_a[r] for r in reps], [map_b[r] for r in reps]


def pairwise_ops_for_cell(
    raw_by_lock: Dict[LockName, Dict[TcoKey, Dict[RepeatId, float]]],
    lock_a: str,
    lock_b: str,
    c: int,
    o: int,
    threads: Sequence[int],
    boot_samples: int,
    rng: random.Random,
) -> PairwiseResult:
    aligned: Dict[int, Tuple[List[float], List[float]]] = {}
    for t in threads:
        va, vb = common_repeat_values(raw_by_lock, lock_a, lock_b, t, c, o)
        if va and vb:
            aligned[t] = (va, vb)

    if not aligned:
        return PairwiseResult(
            metric="ops",
            critical=c,
            outside=o,
            lock_a=lock_a,
            lock_b=lock_b,
            ratio=float("nan"),
            ci_low=float("nan"),
            ci_high=float("nan"),
            p_raw=float("nan"),
            p_adj=float("nan"),
            significant=False,
            winner="missing",
            missing_cell=True,
            common_threads=0,
            thread_list="",
            common_repeat_min=0,
        )

    thread_list = sorted(aligned)
    means_a = [mean(aligned[t][0]) for t in thread_list]
    means_b = [mean(aligned[t][1]) for t in thread_list]
    score_a = geomean(means_a)
    score_b = geomean(means_b)
    ratio = score_a / score_b if score_b > 0.0 else float("nan")

    ratio_dist: List[float] = []
    log_ratio_dist: List[float] = []
    for _ in range(boot_samples):
        sampled_a: List[float] = []
        sampled_b: List[float] = []
        ok = True
        for t in thread_list:
            va, vb = aligned[t]
            n = len(va)
            if n == 0:
                ok = False
                break
            sa = 0.0
            sb = 0.0
            for __ in range(n):
                idx = rng.randrange(n)
                sa += va[idx]
                sb += vb[idx]
            ma = sa / float(n)
            mb = sb / float(n)
            if ma <= 0.0 or mb <= 0.0:
                ok = False
                break
            sampled_a.append(ma)
            sampled_b.append(mb)
        if not ok:
            continue
        ga = geomean(sampled_a)
        gb = geomean(sampled_b)
        if ga <= 0.0 or gb <= 0.0:
            continue
        r = ga / gb
        ratio_dist.append(r)
        log_ratio_dist.append(math.log(r))

    ci_low, ci_high = ci95_from_dist(ratio_dist)
    p_raw = two_sided_p_from_dist_log_ratio(log_ratio_dist)
    common_repeat_min = min(len(aligned[t][0]) for t in thread_list)
    return PairwiseResult(
        metric="ops",
        critical=c,
        outside=o,
        lock_a=lock_a,
        lock_b=lock_b,
        ratio=ratio,
        ci_low=ci_low,
        ci_high=ci_high,
        p_raw=p_raw,
        p_adj=float("nan"),
        significant=False,
        winner="ns",
        missing_cell=False,
        common_threads=len(thread_list),
        thread_list=",".join(str(t) for t in thread_list),
        common_repeat_min=common_repeat_min,
    )


def pairwise_scaling_for_cell(
    raw_by_lock: Dict[LockName, Dict[TcoKey, Dict[RepeatId, float]]],
    lock_a: str,
    lock_b: str,
    c: int,
    o: int,
    threads: Sequence[int],
    boot_samples: int,
    rng: random.Random,
) -> PairwiseResult:
    aligned: Dict[int, Tuple[List[float], List[float]]] = {}
    for t in threads:
        va, vb = common_repeat_values(raw_by_lock, lock_a, lock_b, t, c, o)
        if va and vb:
            aligned[t] = (va, vb)

    if 1 not in aligned:
        return PairwiseResult(
            metric="scaling",
            critical=c,
            outside=o,
            lock_a=lock_a,
            lock_b=lock_b,
            ratio=float("nan"),
            ci_low=float("nan"),
            ci_high=float("nan"),
            p_raw=float("nan"),
            p_adj=float("nan"),
            significant=False,
            winner="missing",
            missing_cell=True,
            common_threads=0,
            thread_list="",
            common_repeat_min=0,
        )
    aux = [t for t in sorted(aligned) if t != 1]
    if not aux:
        return PairwiseResult(
            metric="scaling",
            critical=c,
            outside=o,
            lock_a=lock_a,
            lock_b=lock_b,
            ratio=float("nan"),
            ci_low=float("nan"),
            ci_high=float("nan"),
            p_raw=float("nan"),
            p_adj=float("nan"),
            significant=False,
            winner="missing",
            missing_cell=True,
            common_threads=1,
            thread_list="1",
            common_repeat_min=0,
        )

    base_a = mean(aligned[1][0])
    base_b = mean(aligned[1][1])
    if base_a <= 0.0 or base_b <= 0.0:
        return PairwiseResult(
            metric="scaling",
            critical=c,
            outside=o,
            lock_a=lock_a,
            lock_b=lock_b,
            ratio=float("nan"),
            ci_low=float("nan"),
            ci_high=float("nan"),
            p_raw=float("nan"),
            p_adj=float("nan"),
            significant=False,
            winner="missing",
            missing_cell=True,
            common_threads=1 + len(aux),
            thread_list=",".join(str(t) for t in [1] + aux),
            common_repeat_min=min(len(aligned[t][0]) for t in [1] + aux),
        )

    e_a = [((mean(aligned[t][0]) / base_a) / float(t)) for t in aux]
    e_b = [((mean(aligned[t][1]) / base_b) / float(t)) for t in aux]
    auc_a = mean(e_a)
    auc_b = mean(e_b)
    ratio = auc_a / auc_b if auc_b > 0.0 else float("nan")

    ratio_dist: List[float] = []
    log_ratio_dist: List[float] = []
    for _ in range(boot_samples):
        va1, vb1 = aligned[1]
        n1 = len(va1)
        sa1 = 0.0
        sb1 = 0.0
        for __ in range(n1):
            idx = rng.randrange(n1)
            sa1 += va1[idx]
            sb1 += vb1[idx]
        ma1 = sa1 / float(n1)
        mb1 = sb1 / float(n1)
        if ma1 <= 0.0 or mb1 <= 0.0:
            continue
        terms_a: List[float] = []
        terms_b: List[float] = []
        ok = True
        for t in aux:
            va, vb = aligned[t]
            n = len(va)
            sa = 0.0
            sb = 0.0
            for __ in range(n):
                idx = rng.randrange(n)
                sa += va[idx]
                sb += vb[idx]
            ma = sa / float(n)
            mb = sb / float(n)
            if ma <= 0.0 or mb <= 0.0:
                ok = False
                break
            terms_a.append((ma / ma1) / float(t))
            terms_b.append((mb / mb1) / float(t))
        if not ok:
            continue
        ra = mean(terms_a)
        rb = mean(terms_b)
        if ra <= 0.0 or rb <= 0.0:
            continue
        rr = ra / rb
        ratio_dist.append(rr)
        log_ratio_dist.append(math.log(rr))

    ci_low, ci_high = ci95_from_dist(ratio_dist)
    p_raw = two_sided_p_from_dist_log_ratio(log_ratio_dist)
    all_threads = [1] + aux
    common_repeat_min = min(len(aligned[t][0]) for t in all_threads)
    return PairwiseResult(
        metric="scaling",
        critical=c,
        outside=o,
        lock_a=lock_a,
        lock_b=lock_b,
        ratio=ratio,
        ci_low=ci_low,
        ci_high=ci_high,
        p_raw=p_raw,
        p_adj=float("nan"),
        significant=False,
        winner="ns",
        missing_cell=False,
        common_threads=len(all_threads),
        thread_list=",".join(str(t) for t in all_threads),
        common_repeat_min=common_repeat_min,
    )


def finalize_pairwise(results: List[PairwiseResult], alpha: float) -> None:
    benjamini_hochberg(results)
    for r in results:
        if r.missing_cell or not math.isfinite(r.p_adj):
            r.significant = False
            r.winner = "missing" if r.missing_cell else "ns"
            continue
        r.significant = r.p_adj <= alpha
        if not r.significant:
            r.winner = "ns"
        else:
            if r.ratio > 1.0:
                r.winner = r.lock_a
            elif r.ratio < 1.0:
                r.winner = r.lock_b
            else:
                r.winner = "tie"


def write_pairwise_csv(out_path: Path, results: Sequence[PairwiseResult]) -> None:
    fields = [
        "metric",
        "critical_iters",
        "outside_iters",
        "lock_a",
        "lock_b",
        "ratio",
        "ci95_low",
        "ci95_high",
        "p_value_raw",
        "p_value_adj",
        "significant",
        "winner",
        "missing_cell",
        "common_threads",
        "thread_list",
        "common_repeat_min",
    ]
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in sorted(results, key=lambda x: (x.critical, x.outside, x.lock_a, x.lock_b)):
            w.writerow(
                {
                    "metric": r.metric,
                    "critical_iters": r.critical,
                    "outside_iters": r.outside,
                    "lock_a": r.lock_a,
                    "lock_b": r.lock_b,
                    "ratio": f"{r.ratio:.6f}" if math.isfinite(r.ratio) else "",
                    "ci95_low": f"{r.ci_low:.6f}" if math.isfinite(r.ci_low) else "",
                    "ci95_high": f"{r.ci_high:.6f}" if math.isfinite(r.ci_high) else "",
                    "p_value_raw": f"{r.p_raw:.6f}" if math.isfinite(r.p_raw) else "",
                    "p_value_adj": f"{r.p_adj:.6f}" if math.isfinite(r.p_adj) else "",
                    "significant": int(r.significant),
                    "winner": r.winner,
                    "missing_cell": int(r.missing_cell),
                    "common_threads": r.common_threads,
                    "thread_list": r.thread_list,
                    "common_repeat_min": r.common_repeat_min,
                }
            )


def build_cell_ops_summary(
    locks: Sequence[str],
    raw_by_lock: Dict[LockName, Dict[TcoKey, Dict[RepeatId, float]]],
    stats_by_lock: Dict[LockName, Dict[TcoKey, ThreadStats]],
    c: int,
    o: int,
    threads: Sequence[int],
    boot_samples: int,
    rng: random.Random,
) -> Optional[CellSummary]:
    threads_by_lock: Dict[str, List[int]] = {}
    for lock in locks:
        avail = []
        for t in threads:
            if (t, c, o) in raw_by_lock[lock]:
                avail.append(t)
        threads_by_lock[lock] = avail
    common_threads = sorted(set(threads_by_lock[locks[0]]).intersection(*(set(threads_by_lock[l]) for l in locks[1:])))
    if not common_threads:
        return None

    lock_scores: Dict[str, LockCellScore] = {}
    for lock in locks:
        vals_by_thread: Dict[int, List[float]] = {}
        unstable = False
        means = []
        for t in common_threads:
            rep = raw_by_lock[lock][(t, c, o)]
            vals = [rep[r] for r in sorted(rep)]
            vals_by_thread[t] = vals
            means.append(mean(vals))
            unstable = unstable or stats_by_lock[lock][(t, c, o)].unstable
        score = geomean(means)
        dist = []
        for _ in range(boot_samples):
            sample_means = []
            ok = True
            for t in common_threads:
                vals = vals_by_thread[t]
                n = len(vals)
                if n == 0:
                    ok = False
                    break
                s = 0.0
                for __ in range(n):
                    s += vals[rng.randrange(n)]
                m = s / float(n)
                if m <= 0.0:
                    ok = False
                    break
                sample_means.append(m)
            if ok and sample_means:
                dist.append(geomean(sample_means))
        lo, hi = ci95_from_dist(dist)
        lock_scores[lock] = LockCellScore(score=score, ci_low=lo, ci_high=hi, dist=dist, unstable=unstable)

    ranking = sorted(locks, key=lambda x: (-lock_scores[x].score, x))
    if len(ranking) < 2:
        return None
    top1 = ranking[0]
    top2 = ranking[1]
    s1 = lock_scores[top1].score
    s2 = lock_scores[top2].score
    effect = s1 / s2 if s2 > 0.0 else float("nan")
    d1 = lock_scores[top1].dist
    d2 = lock_scores[top2].dist
    ratio_dist = []
    for i in range(min(len(d1), len(d2))):
        if d1[i] > 0.0 and d2[i] > 0.0:
            ratio_dist.append(d1[i] / d2[i])
    e_lo, e_hi = ci95_from_dist(ratio_dist)
    e_log_var = log_var(ratio_dist)
    unstable_cell = any(lock_scores[l].unstable for l in locks)
    return CellSummary(
        metric="ops",
        critical=c,
        outside=o,
        lock_scores=lock_scores,
        ranking=ranking,
        top1=top1,
        top1_score=s1,
        top1_ci_low=lock_scores[top1].ci_low,
        top1_ci_high=lock_scores[top1].ci_high,
        effect_ratio=effect,
        effect_ci_low=e_lo,
        effect_ci_high=e_hi,
        effect_log_var=e_log_var,
        unstable=unstable_cell,
    )


def build_cell_scaling_summary(
    locks: Sequence[str],
    raw_by_lock: Dict[LockName, Dict[TcoKey, Dict[RepeatId, float]]],
    stats_by_lock: Dict[LockName, Dict[TcoKey, ThreadStats]],
    c: int,
    o: int,
    threads: Sequence[int],
    boot_samples: int,
    rng: random.Random,
) -> Optional[CellSummary]:
    threads_by_lock: Dict[str, List[int]] = {}
    for lock in locks:
        avail = []
        for t in threads:
            if (t, c, o) in raw_by_lock[lock]:
                avail.append(t)
        threads_by_lock[lock] = avail
    common_threads = sorted(set(threads_by_lock[locks[0]]).intersection(*(set(threads_by_lock[l]) for l in locks[1:])))
    if 1 not in common_threads:
        return None
    aux = [t for t in common_threads if t != 1]
    if not aux:
        return None

    lock_scores: Dict[str, LockCellScore] = {}
    for lock in locks:
        vals_by_thread: Dict[int, List[float]] = {}
        unstable = False
        for t in [1] + aux:
            rep = raw_by_lock[lock][(t, c, o)]
            vals = [rep[r] for r in sorted(rep)]
            vals_by_thread[t] = vals
            unstable = unstable or stats_by_lock[lock][(t, c, o)].unstable

        base = mean(vals_by_thread[1])
        if base <= 0.0:
            return None
        terms = []
        for t in aux:
            mt = mean(vals_by_thread[t])
            if mt <= 0.0:
                return None
            terms.append((mt / base) / float(t))
        score = mean(terms)

        dist = []
        for _ in range(boot_samples):
            base_vals = vals_by_thread[1]
            n1 = len(base_vals)
            s1 = 0.0
            for __ in range(n1):
                s1 += base_vals[rng.randrange(n1)]
            m1 = s1 / float(n1)
            if m1 <= 0.0:
                continue
            e_terms = []
            ok = True
            for t in aux:
                vals = vals_by_thread[t]
                n = len(vals)
                ss = 0.0
                for __ in range(n):
                    ss += vals[rng.randrange(n)]
                mt = ss / float(n)
                if mt <= 0.0:
                    ok = False
                    break
                e_terms.append((mt / m1) / float(t))
            if ok and e_terms:
                dist.append(mean(e_terms))
        lo, hi = ci95_from_dist(dist)
        lock_scores[lock] = LockCellScore(score=score, ci_low=lo, ci_high=hi, dist=dist, unstable=unstable)

    ranking = sorted(locks, key=lambda x: (-lock_scores[x].score, x))
    if len(ranking) < 2:
        return None
    top1 = ranking[0]
    top2 = ranking[1]
    s1 = lock_scores[top1].score
    s2 = lock_scores[top2].score
    effect = s1 / s2 if s2 > 0.0 else float("nan")
    d1 = lock_scores[top1].dist
    d2 = lock_scores[top2].dist
    ratio_dist = []
    for i in range(min(len(d1), len(d2))):
        if d1[i] > 0.0 and d2[i] > 0.0:
            ratio_dist.append(d1[i] / d2[i])
    e_lo, e_hi = ci95_from_dist(ratio_dist)
    e_log_var = log_var(ratio_dist)
    unstable_cell = any(lock_scores[l].unstable for l in locks)
    return CellSummary(
        metric="scaling",
        critical=c,
        outside=o,
        lock_scores=lock_scores,
        ranking=ranking,
        top1=top1,
        top1_score=s1,
        top1_ci_low=lock_scores[top1].ci_low,
        top1_ci_high=lock_scores[top1].ci_high,
        effect_ratio=effect,
        effect_ci_low=e_lo,
        effect_ci_high=e_hi,
        effect_log_var=e_log_var,
        unstable=unstable_cell,
    )


def kendall_tau(order_a: Sequence[str], order_b: Sequence[str]) -> float:
    if len(order_a) != len(order_b):
        return float("nan")
    n = len(order_a)
    if n < 2:
        return 1.0
    pos_a = {x: i for i, x in enumerate(order_a)}
    pos_b = {x: i for i, x in enumerate(order_b)}
    items = list(order_a)
    concordant = 0
    discordant = 0
    for i in range(n):
        for j in range(i + 1, n):
            a = items[i]
            b = items[j]
            s1 = pos_a[a] - pos_a[b]
            s2 = pos_b[a] - pos_b[b]
            prod = s1 * s2
            if prod > 0:
                concordant += 1
            elif prod < 0:
                discordant += 1
    denom = n * (n - 1) / 2.0
    if denom == 0.0:
        return 1.0
    return (concordant - discordant) / denom


def build_cell_summaries(
    locks: Sequence[str],
    union_co: Sequence[CoKey],
    raw_by_lock: Dict[LockName, Dict[TcoKey, Dict[RepeatId, float]]],
    stats_by_lock: Dict[LockName, Dict[TcoKey, ThreadStats]],
    threads: Sequence[int],
    boot_samples: int,
    rng: random.Random,
) -> Tuple[Dict[CoKey, CellSummary], Dict[CoKey, CellSummary]]:
    ops_map: Dict[CoKey, CellSummary] = {}
    scaling_map: Dict[CoKey, CellSummary] = {}
    for c, o in union_co:
        ops = build_cell_ops_summary(
            locks, raw_by_lock, stats_by_lock, c, o, threads, boot_samples, rng
        )
        if ops is not None:
            ops_map[(c, o)] = ops
        scaling = build_cell_scaling_summary(
            locks, raw_by_lock, stats_by_lock, c, o, threads, boot_samples, rng
        )
        if scaling is not None:
            scaling_map[(c, o)] = scaling
    return ops_map, scaling_map


def quantile(values: Sequence[float], q: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    return percentile(s, q)


def build_scenario_mapping(cells: Sequence[CoKey]) -> Dict[CoKey, str]:
    short_cells = []
    long_cells = []
    for c, o in cells:
        p = c / float(c + o)
        if c <= 100:
            short_cells.append((c, o, p))
        else:
            long_cells.append((c, o, p))
    if not short_cells or not long_cells:
        raise SystemExit("empty_bucket: short or long group has no cells")

    q1_short = quantile([x[2] for x in short_cells], 1.0 / 3.0)
    q2_short = quantile([x[2] for x in short_cells], 2.0 / 3.0)
    q1_long = quantile([x[2] for x in long_cells], 1.0 / 3.0)
    q2_long = quantile([x[2] for x in long_cells], 2.0 / 3.0)

    mapping: Dict[CoKey, str] = {}
    for c, o, p in short_cells:
        if p <= q1_short:
            band = "low"
        elif p <= q2_short:
            band = "mid"
        else:
            band = "high"
        mapping[(c, o)] = f"short_{band}"
    for c, o, p in long_cells:
        if p <= q1_long:
            band = "low"
        elif p <= q2_long:
            band = "mid"
        else:
            band = "high"
        mapping[(c, o)] = f"long_{band}"

    counts = defaultdict(int)
    for s in mapping.values():
        counts[s] += 1
    for scenario in SCENARIOS:
        if counts[scenario] == 0:
            raise SystemExit(f"empty_bucket: {scenario}")
    return mapping


def aggregate_scenario_scores(
    metric: str,
    locks: Sequence[str],
    cell_map: Dict[CoKey, CellSummary],
    scenario_map: Dict[CoKey, str],
    boot_samples: int,
    rng: random.Random,
) -> Dict[str, Dict[str, Tuple[float, float, float]]]:
    # return: scenario -> lock -> (score, ci_low, ci_high)
    out: Dict[str, Dict[str, Tuple[float, float, float]]] = {}
    for scenario in SCENARIOS:
        cells = sorted([k for k, s in scenario_map.items() if s == scenario and k in cell_map])
        if not cells:
            raise SystemExit(f"empty_bucket: {scenario} has no {metric} cells")
        lock_scores: Dict[str, Tuple[float, float, float]] = {}
        for lock in locks:
            points = [cell_map[k].lock_scores[lock].score for k in cells]
            score = geomean(points)
            dist: List[float] = []
            if len(points) == 1:
                dist = [points[0]] * boot_samples
            else:
                n = len(points)
                for _ in range(boot_samples):
                    sampled = [points[rng.randrange(n)] for __ in range(n)]
                    if all(x > 0.0 for x in sampled):
                        dist.append(geomean(sampled))
            lo, hi = ci95_from_dist(dist)
            lock_scores[lock] = (score, lo, hi)
        out[scenario] = lock_scores
    return out


def scenario_pairwise_wins(
    locks: Sequence[str],
    scenario_map: Dict[CoKey, str],
    pairwise_results: Sequence[PairwiseResult],
) -> Dict[str, Dict[Tuple[str, str], Tuple[int, int, int, int]]]:
    # return scenario -> (a,b) -> (total, wins_a, wins_b, ties)
    by_cell_pair: Dict[Tuple[CoKey, str, str], PairwiseResult] = {}
    for r in pairwise_results:
        if r.missing_cell:
            continue
        by_cell_pair[((r.critical, r.outside), r.lock_a, r.lock_b)] = r

    out: Dict[str, Dict[Tuple[str, str], Tuple[int, int, int, int]]] = {}
    for scenario in SCENARIOS:
        cells = [k for k, s in scenario_map.items() if s == scenario]
        mp: Dict[Tuple[str, str], Tuple[int, int, int, int]] = {}
        for i in range(len(locks)):
            for j in range(i + 1, len(locks)):
                a = locks[i]
                b = locks[j]
                total = 0
                wa = 0
                wb = 0
                ties = 0
                for cell in cells:
                    r = by_cell_pair.get((cell, a, b))
                    if r is None:
                        continue
                    total += 1
                    if not r.significant:
                        ties += 1
                    elif r.winner == a:
                        wa += 1
                    elif r.winner == b:
                        wb += 1
                    else:
                        ties += 1
                mp[(a, b)] = (total, wa, wb, ties)
        out[scenario] = mp
    return out


def write_scenario_summary_csv(
    out_path: Path,
    metric: str,
    locks: Sequence[str],
    agg_scores: Dict[str, Dict[str, Tuple[float, float, float]]],
    pair_wins: Dict[str, Dict[Tuple[str, str], Tuple[int, int, int, int]]],
    metric_conflict: Dict[str, bool],
) -> None:
    fields = [
        "metric",
        "scenario",
        "row_type",
        "lock",
        "rank",
        "score",
        "ci95_low",
        "ci95_high",
        "relative_to_winner",
        "top_lock",
        "metric_conflict",
        "lock_a",
        "lock_b",
        "total_cells",
        "win_count_a",
        "win_count_b",
        "tie_count",
        "win_rate_a",
        "win_rate_b",
        "tie_rate",
    ]
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for scenario in SCENARIOS:
            lock_score = agg_scores[scenario]
            ranking = sorted(locks, key=lambda x: (-lock_score[x][0], x))
            top = ranking[0]
            top_score = lock_score[top][0]
            for idx, lock in enumerate(ranking, start=1):
                s, lo, hi = lock_score[lock]
                rel = s / top_score if top_score > 0.0 else float("nan")
                w.writerow(
                    {
                        "metric": metric,
                        "scenario": scenario,
                        "row_type": "rank",
                        "lock": lock,
                        "rank": idx,
                        "score": f"{s:.6f}",
                        "ci95_low": f"{lo:.6f}" if math.isfinite(lo) else "",
                        "ci95_high": f"{hi:.6f}" if math.isfinite(hi) else "",
                        "relative_to_winner": f"{rel:.6f}" if math.isfinite(rel) else "",
                        "top_lock": top,
                        "metric_conflict": int(metric_conflict.get(scenario, False)),
                        "lock_a": "",
                        "lock_b": "",
                        "total_cells": "",
                        "win_count_a": "",
                        "win_count_b": "",
                        "tie_count": "",
                        "win_rate_a": "",
                        "win_rate_b": "",
                        "tie_rate": "",
                    }
                )
            for i in range(len(locks)):
                for j in range(i + 1, len(locks)):
                    a = locks[i]
                    b = locks[j]
                    total, wa, wb, ties = pair_wins[scenario][(a, b)]
                    wr_a = (wa / float(total)) if total else float("nan")
                    wr_b = (wb / float(total)) if total else float("nan")
                    tr = (ties / float(total)) if total else float("nan")
                    w.writerow(
                        {
                            "metric": metric,
                            "scenario": scenario,
                            "row_type": "pairwise",
                            "lock": "",
                            "rank": "",
                            "score": "",
                            "ci95_low": "",
                            "ci95_high": "",
                            "relative_to_winner": "",
                            "top_lock": "",
                            "metric_conflict": int(metric_conflict.get(scenario, False)),
                            "lock_a": a,
                            "lock_b": b,
                            "total_cells": total,
                            "win_count_a": wa,
                            "win_count_b": wb,
                            "tie_count": ties,
                            "win_rate_a": f"{wr_a:.6f}" if math.isfinite(wr_a) else "",
                            "win_rate_b": f"{wr_b:.6f}" if math.isfinite(wr_b) else "",
                            "tie_rate": f"{tr:.6f}" if math.isfinite(tr) else "",
                        }
                    )


def ci_overlap(a_lo: float, a_hi: float, b_lo: float, b_hi: float) -> bool:
    if not all(math.isfinite(x) for x in (a_lo, a_hi, b_lo, b_hi)):
        return False
    return max(a_lo, b_lo) <= min(a_hi, b_hi)


def can_merge_adjacent(
    prev_cell: CellSummary,
    next_cell: CellSummary,
    threshold_frac: float,
) -> bool:
    if prev_cell.top1 != next_cell.top1:
        return False
    tau = kendall_tau(prev_cell.ranking, next_cell.ranking)
    if not math.isfinite(tau) or tau < 0.8:
        return False
    if not (math.isfinite(prev_cell.effect_ratio) and math.isfinite(next_cell.effect_ratio)):
        return False
    if prev_cell.effect_ratio <= 0.0 or next_cell.effect_ratio <= 0.0:
        return False
    diff = abs(math.log(prev_cell.effect_ratio) - math.log(next_cell.effect_ratio))
    if diff > math.log(1.0 + threshold_frac):
        return False
    if not ci_overlap(
        prev_cell.top1_ci_low,
        prev_cell.top1_ci_high,
        next_cell.top1_ci_low,
        next_cell.top1_ci_high,
    ):
        return False
    if prev_cell.unstable or next_cell.unstable:
        return False
    return True


def finalize_segment(
    metric: str,
    axis: str,
    fixed_value: int,
    cells: List[CoKey],
    cell_map: Dict[CoKey, CellSummary],
) -> Dict[str, str]:
    summaries = [cell_map[c] for c in cells]
    top1 = summaries[0].top1
    logs = []
    weights = []
    for s in summaries:
        if s.effect_ratio > 0.0 and math.isfinite(s.effect_ratio):
            lv = math.log(s.effect_ratio)
            vv = s.effect_log_var
            if vv <= 1e-12 or not math.isfinite(vv):
                vv = 1.0
            logs.append(lv)
            weights.append(1.0 / vv)
    if not logs:
        seg_ratio = float("nan")
        lo = float("nan")
        hi = float("nan")
    else:
        ws = sum(weights)
        mean_log = sum(w * l for w, l in zip(weights, logs)) / ws
        se = math.sqrt(1.0 / ws)
        seg_ratio = math.exp(mean_log)
        lo = math.exp(mean_log - 1.96 * se)
        hi = math.exp(mean_log + 1.96 * se)

    if axis == "critical":
        varying = [c for c, _ in cells]
        fixed_name = "outside_iters"
        start_name = "critical_start"
        end_name = "critical_end"
    else:
        varying = [o for _, o in cells]
        fixed_name = "critical_iters"
        start_name = "outside_start"
        end_name = "outside_end"

    return {
        "metric": metric,
        "axis": axis,
        fixed_name: str(fixed_value),
        start_name: str(min(varying)),
        end_name: str(max(varying)),
        "num_cells": str(len(cells)),
        "top1_lock": top1,
        "segment_effect_ratio": f"{seg_ratio:.6f}" if math.isfinite(seg_ratio) else "",
        "segment_ci95_low": f"{lo:.6f}" if math.isfinite(lo) else "",
        "segment_ci95_high": f"{hi:.6f}" if math.isfinite(hi) else "",
        "all_non_unstable": str(int(all(not s.unstable for s in summaries))),
        "cells": ";".join(f"{c}:{o}" for c, o in cells),
    }


def build_aggregated_segments(
    metric: str,
    cell_map: Dict[CoKey, CellSummary],
    threshold_frac: float,
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    # axis: critical (fixed outside)
    by_outside: Dict[int, List[CoKey]] = defaultdict(list)
    for c, o in cell_map:
        by_outside[o].append((c, o))
    for outside, cells in sorted(by_outside.items()):
        line = sorted(cells, key=lambda x: x[0])
        if not line:
            continue
        seg = [line[0]]
        for cell in line[1:]:
            prev = seg[-1]
            if can_merge_adjacent(cell_map[prev], cell_map[cell], threshold_frac):
                seg.append(cell)
            else:
                rows.append(finalize_segment(metric, "critical", outside, seg, cell_map))
                seg = [cell]
        rows.append(finalize_segment(metric, "critical", outside, seg, cell_map))

    # axis: outside (fixed critical)
    by_critical: Dict[int, List[CoKey]] = defaultdict(list)
    for c, o in cell_map:
        by_critical[c].append((c, o))
    for critical, cells in sorted(by_critical.items()):
        line = sorted(cells, key=lambda x: x[1])
        if not line:
            continue
        seg = [line[0]]
        for cell in line[1:]:
            prev = seg[-1]
            if can_merge_adjacent(cell_map[prev], cell_map[cell], threshold_frac):
                seg.append(cell)
            else:
                rows.append(finalize_segment(metric, "outside", critical, seg, cell_map))
                seg = [cell]
        rows.append(finalize_segment(metric, "outside", critical, seg, cell_map))
    return rows


def write_segments_csv(out_path: Path, rows: Sequence[Dict[str, str]]) -> None:
    fields = [
        "metric",
        "axis",
        "critical_iters",
        "outside_iters",
        "critical_start",
        "critical_end",
        "outside_start",
        "outside_end",
        "num_cells",
        "top1_lock",
        "segment_effect_ratio",
        "segment_ci95_low",
        "segment_ci95_high",
        "all_non_unstable",
        "cells",
    ]
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            full = {k: "" for k in fields}
            full.update(row)
            w.writerow(full)


def main() -> None:
    args = parse_args()
    locks = parse_locks(args.locks)
    threads = parse_csv_ints(args.threads, "--threads")
    if 1 not in threads:
        raise SystemExit("--threads must include 1 for scaling metrics")
    threshold_frac = args.aggregate_threshold / 100.0
    if threshold_frac < 0.0:
        raise SystemExit("--aggregate-threshold must be >= 0")
    if args.bootstrap_samples <= 0:
        raise SystemExit("--bootstrap-samples must be > 0")

    rng = random.Random(args.seed)
    results_root = Path(args.results_root)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_by_lock: Dict[LockName, Dict[TcoKey, Dict[RepeatId, float]]] = {}
    for lock in locks:
        raw_path = results_root / lock / "raw.csv"
        summary_path = results_root / lock / "summary.csv"
        raw_map = read_raw_csv(raw_path)
        maybe_validate_summary(raw_map, summary_path)
        raw_by_lock[lock] = raw_map

    union_tco = sorted({k for lock in locks for k in raw_by_lock[lock]})
    union_co = sorted({(c, o) for (_, c, o) in union_tco})
    if not union_tco:
        raise SystemExit("No raw records found")

    stats_by_lock = build_thread_stats(raw_by_lock, union_tco, args.bootstrap_samples, rng)
    write_cell_metrics_csv(out_dir / "cell_metrics.csv", locks, union_tco, stats_by_lock)
    write_cell_scaling_csv(
        out_dir / "cell_scaling.csv",
        locks,
        union_co,
        threads,
        raw_by_lock,
        stats_by_lock,
        args.bootstrap_samples,
        rng,
    )

    pair_ops: List[PairwiseResult] = []
    pair_scaling: List[PairwiseResult] = []
    for c, o in union_co:
        for i in range(len(locks)):
            for j in range(i + 1, len(locks)):
                a = locks[i]
                b = locks[j]
                pair_ops.append(
                    pairwise_ops_for_cell(
                        raw_by_lock, a, b, c, o, threads, args.bootstrap_samples, rng
                    )
                )
                pair_scaling.append(
                    pairwise_scaling_for_cell(
                        raw_by_lock, a, b, c, o, threads, args.bootstrap_samples, rng
                    )
                )
    finalize_pairwise(pair_ops, args.alpha)
    finalize_pairwise(pair_scaling, args.alpha)
    write_pairwise_csv(out_dir / "pairwise_matrix_ops.csv", pair_ops)
    write_pairwise_csv(out_dir / "pairwise_matrix_scaling.csv", pair_scaling)

    cell_ops, cell_scaling = build_cell_summaries(
        locks,
        union_co,
        raw_by_lock,
        stats_by_lock,
        threads,
        args.bootstrap_samples,
        rng,
    )

    cells_both = sorted(set(cell_ops).intersection(cell_scaling))
    if not cells_both:
        raise SystemExit("No cells with both ops/scaling summaries")
    scenario_map = build_scenario_mapping(cells_both)

    ops_agg = aggregate_scenario_scores(
        "ops", locks, cell_ops, scenario_map, args.bootstrap_samples, rng
    )
    scaling_agg = aggregate_scenario_scores(
        "scaling", locks, cell_scaling, scenario_map, args.bootstrap_samples, rng
    )

    conflict: Dict[str, bool] = {}
    for scenario in SCENARIOS:
        ops_top = max(locks, key=lambda x: (ops_agg[scenario][x][0], x))
        scaling_top = max(locks, key=lambda x: (scaling_agg[scenario][x][0], x))
        conflict[scenario] = ops_top != scaling_top

    ops_pair_wins = scenario_pairwise_wins(locks, scenario_map, pair_ops)
    scaling_pair_wins = scenario_pairwise_wins(locks, scenario_map, pair_scaling)

    write_scenario_summary_csv(
        out_dir / "scenario_summary_ops.csv",
        "ops",
        locks,
        ops_agg,
        ops_pair_wins,
        conflict,
    )
    write_scenario_summary_csv(
        out_dir / "scenario_summary_scaling.csv",
        "scaling",
        locks,
        scaling_agg,
        scaling_pair_wins,
        conflict,
    )

    seg_ops = build_aggregated_segments("ops", cell_ops, threshold_frac)
    seg_scaling = build_aggregated_segments("scaling", cell_scaling, threshold_frac)
    write_segments_csv(out_dir / "aggregated_segments_ops.csv", seg_ops)
    write_segments_csv(out_dir / "aggregated_segments_scaling.csv", seg_scaling)

    print(f"[ok] wrote analysis outputs to {out_dir}", file=sys.stderr)


if __name__ == "__main__":
    main()
