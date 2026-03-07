#!/usr/bin/env python3
"""
compare_scheduler_results.py
对比两套调度器实验结果（standard vs scx_lavd），并生成对比图片。

目录约定：
  <results_root>/<lock>/summary.csv 或 <results_root>/<lock>/raw.csv

用法示例：
  python3 scripts/compare_scheduler_results.py \
    --standard-dir results_standard2 \
    --lavd-dir results_lavd2 \
    --out-dir results_compare2
"""

import argparse
import os
import sys

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
except ModuleNotFoundError as exc:
    if exc.name != "matplotlib":
        raise
    matplotlib = None
    plt = None
    ticker = None

from bench_csv_schema import load_plot_rows


_COLOR_POOL = [
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#d62728",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
]
_MARKER_POOL = ["o", "s", "^", "D", "v", "P", "X", "*"]


def require_matplotlib() -> None:
    if matplotlib is None or plt is None or ticker is None:
        sys.exit(
            "Error: 绘图需要 matplotlib，请先安装它，例如执行 "
            "`python3 -m pip install matplotlib`。"
        )


def format_lavd_label(lock: str, lavd_suffix: str) -> str:
    if lavd_suffix:
        return f"{lock}{lavd_suffix}"
    return f"{lock} (lavd)"


def parse_int_list(value: str | None) -> list[int] | None:
    if not value:
        return None
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def discover_locks(root: str) -> list[str]:
    if not os.path.isdir(root):
        sys.exit(f"Error: 目录不存在: {root}")
    locks = sorted(
        d.name
        for d in os.scandir(root)
        if d.is_dir()
        and (
            os.path.isfile(os.path.join(d.path, "summary.csv"))
            or os.path.isfile(os.path.join(d.path, "raw.csv"))
        )
    )
    if not locks:
        sys.exit(f"Error: 在 {root} 下未找到任何含 summary.csv 或 raw.csv 的锁目录")
    return locks


def load_results(root: str, locks: list[str]) -> tuple[dict, set[int], set[int], set[int]]:
    data = {}
    outs = set()
    crits = set()
    threads = set()

    for lock in locks:
        lock_dir = os.path.join(root, lock)
        try:
            rows = load_plot_rows(lock_dir)
        except ValueError as exc:
            sys.exit(f"Error: {exc}")

        for r in rows:
            t = int(r["threads"])
            c = int(r["critical_iters"])
            o = int(r["outside_iters"])
            tp = float(r["mean_throughput_ops_per_sec"]) / 1e6

            data[(lock, t, c, o)] = tp
            threads.add(t)
            crits.add(c)
            outs.add(o)

    return data, outs, crits, threads


def auto_select_crits(crit_values: list[int], out: int, n: int = 5) -> list[int]:
    min_ratio = 1 / (n + 1)
    targets = [(i + 1) / (n + 1) for i in range(n)]

    primary = [c for c in crit_values if c / (c + out) >= min_ratio]
    secondary = sorted(
        (c for c in crit_values if c / (c + out) < min_ratio),
        key=lambda c: c / (c + out),
        reverse=True,
    )

    selected = []
    pool = list(primary)
    for target in targets:
        if not pool:
            break
        best = min(pool, key=lambda c: abs(c / (c + out) - target))
        selected.append(best)
        pool.remove(best)

    for c in sorted(pool, key=lambda c: c / (c + out), reverse=True):
        if len(selected) >= n:
            break
        selected.append(c)

    for c in secondary:
        if len(selected) >= n:
            break
        selected.append(c)

    return sorted(selected)


def pick_values(
    available: list[int], requested: list[int] | None, value_name: str
) -> list[int]:
    if requested is None:
        return available
    missing = [v for v in requested if v not in available]
    if missing:
        sys.exit(
            f"Error: 请求的 {value_name} {missing} 不在可用集合内: {available}"
        )
    return requested


def intersect_or_single_side(
    lhs: set[int], rhs: set[int], value_name: str
) -> list[int]:
    if lhs and rhs:
        values = sorted(lhs & rhs)
        if not values:
            sys.exit(f"Error: 两边 {value_name} 无交集。")
        return values
    if lhs:
        return sorted(lhs)
    if rhs:
        return sorted(rhs)
    sys.exit(f"Error: 没有可用的 {value_name}。")


def build_styles(locks: list[str]) -> tuple[dict[str, str], dict[str, str]]:
    colors = {l: _COLOR_POOL[i % len(_COLOR_POOL)] for i, l in enumerate(locks)}
    markers = {l: _MARKER_POOL[i % len(_MARKER_POOL)] for i, l in enumerate(locks)}
    return colors, markers


def _plot_scheduler_overlay(
    out: int,
    crits: list[int],
    locks: list[str],
    threads: list[int],
    std_data: dict,
    lavd_data: dict,
    colors: dict,
    markers: dict,
    save_path: str,
    lavd_suffix: str,
    title_prefix: str = "Throughput Comparison",
) -> None:
    fig, axes = plt.subplots(1, len(crits), figsize=(5 * len(crits) + 4, 5.8))
    if len(crits) == 1:
        axes = [axes]

    fig.patch.set_facecolor("#F7F7F7")

    for ax, crit in zip(axes, crits):
        ratio = crit / (crit + out)
        ax.set_facecolor("white")
        ax.set_xscale("log", base=2)

        ymax = 0.0
        for lock in locks:
            xs_std, ys_std = [], []
            xs_lavd, ys_lavd = [], []
            for t in threads:
                v_std = std_data.get((lock, t, crit, out))
                v_lavd = lavd_data.get((lock, t, crit, out))
                if v_std is not None:
                    xs_std.append(t)
                    ys_std.append(v_std)
                    ymax = max(ymax, v_std)
                if v_lavd is not None:
                    xs_lavd.append(t)
                    ys_lavd.append(v_lavd)
                    ymax = max(ymax, v_lavd)

            if ys_std:
                ax.plot(
                    xs_std,
                    ys_std,
                    color=colors[lock],
                    marker=markers[lock],
                    linewidth=2.0,
                    markersize=5.2,
                    markerfacecolor="white",
                    markeredgewidth=1.5,
                    linestyle="-",
                    label=f"{lock} (standard)",
                    zorder=3,
                )
            if ys_lavd:
                ax.plot(
                    xs_lavd,
                    ys_lavd,
                    color=colors[lock],
                    marker=markers[lock],
                    linewidth=2.0,
                    markersize=5.2,
                    markerfacecolor=colors[lock],
                    markeredgewidth=1.0,
                    linestyle="--",
                    label=format_lavd_label(lock, lavd_suffix),
                    zorder=3,
                )

        ymax = 1.0 if ymax <= 0 else ymax * 1.12
        ax.set_ylim(0, ymax)
        ax.set_xlim(min(threads) * 0.8, max(threads) * 1.15)
        ax.set_xticks(threads)
        ax.xaxis.set_major_formatter(ticker.ScalarFormatter())
        ax.set_xticklabels([str(t) for t in threads], rotation=50, ha="right", fontsize=8)
        ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.2f"))
        ax.tick_params(axis="y", labelsize=8.5)
        ax.grid(True, linestyle=":", alpha=0.5, color="#DDDDDD", zorder=0)
        ax.spines[["top", "right"]].set_visible(False)
        ax.spines[["left", "bottom"]].set_color("#CCCCCC")
        ax.set_title(f"ratio={ratio:.2f} (crit={crit})", fontsize=11, fontweight="bold")
        ax.set_xlabel("Threads", fontsize=9.5)
        ax.set_ylabel("Throughput (Mops/s)", fontsize=9.5)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=min(max(4, len(locks)), 8),
        fontsize=10,
        frameon=True,
        framealpha=0.95,
        edgecolor="#CCCCCC",
        bbox_to_anchor=(0.5, -0.025),
        handlelength=2.4,
        columnspacing=1.8,
    )
    fig.suptitle(
        f"{title_prefix}  (outside_iters={out})",
        fontsize=13.5,
        fontweight="bold",
        y=1.02,
    )
    plt.tight_layout(rect=[0, 0.08, 1, 1])
    plt.savefig(save_path, dpi=170, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)


def _plot_vs_ops_raw(
    out: int,
    crits: list[int],
    locks: list[str],
    threads: list[int],
    std_data: dict,
    lavd_data: dict,
    colors: dict,
    markers: dict,
    save_path: str,
    lavd_suffix: str,
) -> None:
    _plot_scheduler_overlay(
        out=out,
        crits=crits,
        locks=locks,
        threads=threads,
        std_data=std_data,
        lavd_data=lavd_data,
        colors=colors,
        markers=markers,
        save_path=save_path,
        lavd_suffix=lavd_suffix,
        title_prefix="VS Raw Throughput",
    )


def _plot_all_locks_in_one(
    out: int,
    crits: list[int],
    locks: list[str],
    threads: list[int],
    std_data: dict,
    lavd_data: dict,
    save_path: str,
    lavd_suffix: str,
) -> None:
    impl_specs = (
        [(lock, False, lock) for lock in locks]
        + [(lock, True, format_lavd_label(lock, lavd_suffix)) for lock in locks]
    )
    impl_labels = [x[2] for x in impl_specs]
    colors, markers = build_styles(impl_labels)

    fig, axes = plt.subplots(1, len(crits), figsize=(5 * len(crits) + 4, 5.8))
    if len(crits) == 1:
        axes = [axes]

    fig.patch.set_facecolor("#F7F7F7")

    for ax, crit in zip(axes, crits):
        ratio = crit / (crit + out)
        ax.set_facecolor("white")
        ax.set_xscale("log", base=2)

        ymax = 0.0
        for lock, is_lavd, label in impl_specs:
            source = lavd_data if is_lavd else std_data
            xs, ys = [], []
            for t in threads:
                v = source.get((lock, t, crit, out))
                if v is None:
                    continue
                xs.append(t)
                ys.append(v)
                ymax = max(ymax, v)
            if ys:
                ax.plot(
                    xs,
                    ys,
                    color=colors[label],
                    marker=markers[label],
                    linewidth=2.0,
                    markersize=5.2,
                    markerfacecolor="white" if not is_lavd else colors[label],
                    markeredgewidth=1.2,
                    linestyle="-" if not is_lavd else "--",
                    label=label,
                    zorder=3,
                )

        ymax = 1.0 if ymax <= 0 else ymax * 1.12
        ax.set_ylim(0, ymax)
        ax.set_xlim(min(threads) * 0.8, max(threads) * 1.15)
        ax.set_xticks(threads)
        ax.xaxis.set_major_formatter(ticker.ScalarFormatter())
        ax.set_xticklabels([str(t) for t in threads], rotation=50, ha="right", fontsize=8)
        ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.2f"))
        ax.tick_params(axis="y", labelsize=8.5)
        ax.grid(True, linestyle=":", alpha=0.5, color="#DDDDDD", zorder=0)
        ax.spines[["top", "right"]].set_visible(False)
        ax.spines[["left", "bottom"]].set_color("#CCCCCC")
        ax.set_title(f"ratio={ratio:.2f} (crit={crit})", fontsize=11, fontweight="bold")
        ax.set_xlabel("Threads", fontsize=9.5)
        ax.set_ylabel("Throughput (Mops/s)", fontsize=9.5)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=min(max(4, len(impl_specs)), 10),
        fontsize=10,
        frameon=True,
        framealpha=0.95,
        edgecolor="#CCCCCC",
        bbox_to_anchor=(0.5, -0.025),
        handlelength=2.4,
        columnspacing=1.5,
    )
    fig.suptitle(
        f"All Locks In One Figure  (outside_iters={out})",
        fontsize=13.5,
        fontweight="bold",
        y=1.02,
    )
    plt.tight_layout(rect=[0, 0.08, 1, 1])
    plt.savefig(save_path, dpi=170, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)


def parse_args() -> argparse.Namespace:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_standard = os.path.join(script_dir, "..", "results_standard2")
    default_lavd = os.path.join(script_dir, "..", "results_lavd2")
    default_out_dir = os.path.join(script_dir, "..", "results_compare")

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--standard-dir",
        default=default_standard,
        help="标准调度器结果目录（默认：../results_standard2）",
    )
    p.add_argument(
        "--lavd-dir",
        default=default_lavd,
        help="scx_lavd 结果目录（默认：../results_lavd2）",
    )
    p.add_argument(
        "--out-dir",
        default=default_out_dir,
        help="输出目录（默认：../results_compare）",
    )
    p.add_argument(
        "--outs",
        default=None,
        help="逗号分隔 outside_iters 列表（默认使用两边交集）",
    )
    p.add_argument(
        "--crits",
        default=None,
        help="逗号分隔 critical_iters 列表（默认每个 out 自动选 5 个）",
    )
    p.add_argument(
        "--locks",
        default=None,
        help="逗号分隔锁名（默认使用两边并集；单锁对比图仅对可成对比较的锁生成）",
    )
    p.add_argument(
        "--lavd-suffix",
        default="_lavd",
        help="lavd 曲线名后缀（默认：_lavd，例如设为 _tse 后 mcs -> mcs_tse）",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    require_matplotlib()
    standard_dir = os.path.realpath(args.standard_dir)
    lavd_dir = os.path.realpath(args.lavd_dir)
    out_dir = os.path.realpath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    standard_locks_all = discover_locks(standard_dir)
    lavd_locks_all = discover_locks(lavd_dir)

    standard_lock_set = set(standard_locks_all)
    lavd_lock_set = set(lavd_locks_all)
    all_locks = sorted(standard_lock_set | lavd_lock_set)
    if not all_locks:
        sys.exit("Error: 两边都没有可用的锁实现。")

    requested_locks = None
    if args.locks:
        requested_locks = [x.strip() for x in args.locks.split(",") if x.strip()]
        missing = [x for x in requested_locks if x not in all_locks]
        if missing:
            sys.exit(f"Error: 请求的锁 {missing} 不在可用集合内: {all_locks}")
        selected_locks = requested_locks
    else:
        selected_locks = all_locks

    all_plot_locks = selected_locks
    comparable_locks = [
        lock for lock in selected_locks if lock in standard_lock_set and lock in lavd_lock_set
    ]
    standard_selected_locks = [lock for lock in selected_locks if lock in standard_lock_set]
    lavd_selected_locks = [lock for lock in selected_locks if lock in lavd_lock_set]

    if not standard_selected_locks and not lavd_selected_locks:
        sys.exit("Error: 选中的锁在两边结果目录中都不存在。")

    std_data, std_outs, std_crits, std_threads = load_results(
        standard_dir, standard_selected_locks
    )
    lavd_data, lavd_outs, lavd_crits, lavd_threads = load_results(
        lavd_dir, lavd_selected_locks
    )

    outs_available = intersect_or_single_side(std_outs, lavd_outs, "outside_iters")
    crits_available = intersect_or_single_side(std_crits, lavd_crits, "critical_iters")
    threads = intersect_or_single_side(std_threads, lavd_threads, "threads")

    requested_outs = parse_int_list(args.outs)
    requested_crits = parse_int_list(args.crits)

    outs = pick_values(outs_available, requested_outs, "outside_iters")
    fixed_crits = (
        pick_values(crits_available, requested_crits, "critical_iters")
        if requested_crits is not None
        else None
    )

    print(f"standard_dir : {standard_dir}")
    print(f"lavd_dir     : {lavd_dir}")
    print(f"output_dir   : {out_dir}")
    print(f"all_locks    : {all_plot_locks}")
    print(f"comparable   : {comparable_locks}")
    print(f"lavd_suffix  : {args.lavd_suffix}")
    print(f"outs         : {outs}")
    print(f"threads      : {threads}")

    generated = []
    for out in outs:
        crits = fixed_crits if fixed_crits else auto_select_crits(crits_available, out)
        if not crits:
            print(f"Skip out={out}: 未找到可用 critical_iters")
            continue

        path_all = os.path.join(out_dir, f"all_locks_out{out:04d}.png")
        _plot_all_locks_in_one(
            out,
            crits,
            all_plot_locks,
            threads,
            std_data,
            lavd_data,
            path_all,
            args.lavd_suffix,
        )
        generated.append(path_all)
        print(f"[out={out}] [all] saved: {path_all}")

        for lock in comparable_locks:
            path_vs = os.path.join(out_dir, f"vs_raw_{lock}_out{out:04d}.png")
            colors, markers = build_styles([lock])
            _plot_vs_ops_raw(
                out,
                crits,
                [lock],
                threads,
                std_data,
                lavd_data,
                colors,
                markers,
                path_vs,
                args.lavd_suffix,
            )
            generated.append(path_vs)
            print(f"[out={out}] [{lock}] saved: {path_vs}")

        if not comparable_locks:
            print(f"[out={out}] no comparable locks; skipped per-lock scheduler-vs-scheduler plots")

    if not generated:
        sys.exit("Error: 没有生成任何图片。")
    print(f"\n完成，共生成 {len(generated)} 张图片。")


if __name__ == "__main__":
    main()
