#!/usr/bin/env python3
"""
plot_throughput_by_ratio.py
绘制各互斥锁在不同临界区比例下的吞吐量 vs 线程数折线图，
并额外生成等待/持锁/锁交接时间分解图。

用法：
    python3 plot_throughput_by_ratio.py [--out OUT] [--data DIR] [--save PATH]

参数：
    --out OUT          固定的非临界区迭代次数（默认 400）。
                       若 OUT 不在数据集中，则在相邻两个可用值之间线性插值。
    --data DIR         results 根目录（默认：脚本所在目录的上级 results-new/）。
                       脚本会自动扫描该目录下含有 summary.csv 或 raw.csv 的子目录作为锁实现。
    --save PATH        吞吐量图片路径（默认：<data>/throughput_by_ratio.png）
    --save-latency PATH
                       时延分解图路径（默认：<data>/latency_breakdown_by_ratio.png）
    --crits C,…        逗号分隔的 critical_iters 列表（默认自动选 5 个）
    --no-show          不弹出交互式窗口（在无头环境下自动生效）
"""

import argparse
import os
import sys

from bench_csv_schema import LATENCY_PLOT_REQUIRED_FIELDS, load_plot_rows

try:
    import matplotlib
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
except ModuleNotFoundError as exc:
    if exc.name != "matplotlib":
        raise
    matplotlib = None
    plt = None
    ticker = None


THREADS_LIST = [1, 2, 4, 8, 16, 32, 48, 64, 80, 96, 128, 160]

_COLOR_POOL = [
    "#2196F3",
    "#FF9800",
    "#43A047",
    "#E53935",
    "#9C27B0",
    "#00BCD4",
    "#FF5722",
    "#607D8B",
]
_MARKER_POOL = ["o", "s", "^", "D", "v", "P", "X", "*"]

NCPUS = 96

THROUGHPUT_FIELD = "mean_throughput_ops_per_sec"
THROUGHPUT_SCALE = 1e6
LATENCY_METRICS = [
    ("avg_wait_ns_estimated", "Wait Approx (ns/op)"),
    ("avg_lock_hold_ns", "Hold Time (ns/op)"),
    ("avg_lock_handoff_ns_estimated", "Handoff Est. (ns/op)"),
]


def discover_locks(data_dir: str) -> list[str]:
    if not os.path.isdir(data_dir):
        sys.exit(f"Error: 数据目录不存在：{data_dir}")
    locks = sorted(
        entry.name
        for entry in os.scandir(data_dir)
        if entry.is_dir()
        and (
            os.path.isfile(os.path.join(entry.path, "summary.csv"))
            or os.path.isfile(os.path.join(entry.path, "raw.csv"))
        )
    )
    if not locks:
        sys.exit(f"Error: 在 {data_dir} 下未找到任何含 summary.csv 或 raw.csv 的子目录。")
    return locks


def build_styles(locks: list[str]) -> tuple[dict[str, str], dict[str, str]]:
    colors = {lock: _COLOR_POOL[i % len(_COLOR_POOL)] for i, lock in enumerate(locks)}
    markers = {lock: _MARKER_POOL[i % len(_MARKER_POOL)] for i, lock in enumerate(locks)}
    return colors, markers


def require_matplotlib() -> None:
    if matplotlib is None or plt is None or ticker is None:
        sys.exit(
            "Error: 绘图需要 matplotlib，请先安装它，例如执行 "
            "`python3 -m pip install matplotlib`。"
        )


def _load_lock_rows(
    data_dir: str, lock: str, required_fields: set[str] | None = None
) -> list[dict[str, str]]:
    try:
        return load_plot_rows(os.path.join(data_dir, lock), required_fields=required_fields)
    except ValueError as exc:
        sys.exit(f"Error: {exc}")


def load_data(
    data_dir: str, locks: list[str], required_fields: set[str] | None = None
) -> dict[str, list[dict[str, str]]]:
    return {
        lock: _load_lock_rows(data_dir, lock, required_fields=required_fields)
        for lock in locks
    }


def available_out_values(data: dict[str, list[dict[str, str]]]) -> list[int]:
    values = sorted({int(row["outside_iters"]) for rows in data.values() for row in rows})
    if not values:
        sys.exit("Error: 数据集中没有可用的 outside_iters。")
    return values


def available_crit_values(data: dict[str, list[dict[str, str]]]) -> list[int]:
    values = sorted({int(row["critical_iters"]) for rows in data.values() for row in rows})
    if not values:
        sys.exit("Error: 数据集中没有可用的 critical_iters。")
    return values


def _find_row(
    data: dict[str, list[dict[str, str]]], lock: str, threads: int, crit: int, out: int
) -> dict[str, str] | None:
    return next(
        (
            row
            for row in data[lock]
            if int(row["threads"]) == threads
            and int(row["critical_iters"]) == crit
            and int(row["outside_iters"]) == out
        ),
        None,
    )


def get_metric(
    data: dict[str, list[dict[str, str]]],
    lock: str,
    threads: int,
    crit: int,
    out: int,
    field: str,
    scale: float = 1.0,
) -> float | None:
    row = _find_row(data, lock, threads, crit, out)
    if row is None:
        return None
    value = row.get(field, "").strip()
    if value == "":
        return None
    return float(value) / scale


def get_metric_interp(
    data: dict[str, list[dict[str, str]]],
    lock: str,
    threads: int,
    crit: int,
    out: int,
    field: str,
    out_values: list[int],
    scale: float = 1.0,
) -> float | None:
    if out in out_values:
        return get_metric(data, lock, threads, crit, out, field, scale)

    lo = max((v for v in out_values if v < out), default=None)
    hi = min((v for v in out_values if v > out), default=None)

    if lo is None:
        return get_metric(data, lock, threads, crit, hi, field, scale)
    if hi is None:
        return get_metric(data, lock, threads, crit, lo, field, scale)

    lo_value = get_metric(data, lock, threads, crit, lo, field, scale)
    hi_value = get_metric(data, lock, threads, crit, hi, field, scale)
    if lo_value is None or hi_value is None:
        return None

    alpha = (out - lo) / (hi - lo)
    return lo_value + alpha * (hi_value - lo_value)


def get_tp_interp(
    data: dict[str, list[dict[str, str]]],
    lock: str,
    threads: int,
    crit: int,
    out: int,
    out_values: list[int],
) -> float | None:
    return get_metric_interp(
        data,
        lock,
        threads,
        crit,
        out,
        THROUGHPUT_FIELD,
        out_values,
        scale=THROUGHPUT_SCALE,
    )


def auto_select_crits(crit_values: list[int], out: int, n: int = 5) -> list[int]:
    min_ratio = 1 / (n + 1)
    targets = [(i + 1) / (n + 1) for i in range(n)]

    primary = [c for c in crit_values if c / (c + out) >= min_ratio]
    secondary = sorted(
        (c for c in crit_values if c / (c + out) < min_ratio),
        key=lambda c: c / (c + out),
        reverse=True,
    )

    selected: list[int] = []
    pool = list(primary)
    for target in targets:
        if not pool:
            break
        best = min(pool, key=lambda c: abs(c / (c + out) - target))
        selected.append(best)
        pool.remove(best)

    for crit in sorted(pool, key=lambda c: c / (c + out), reverse=True):
        if len(selected) >= n:
            break
        selected.append(crit)

    for crit in secondary:
        if len(selected) >= n:
            break
        selected.append(crit)

    return sorted(selected)


def print_table(
    data: dict[str, list[dict[str, str]]],
    locks: list[str],
    out: int,
    crits: list[int],
    out_values: list[int],
) -> None:
    col_w = 12
    thr_w = 8

    for crit in crits:
        ratio = crit / (crit + out)
        print(f"\n=== ratio = {ratio:.2f}  (crit={crit}, out={out}) ===")

        header = f"{'Threads':>{thr_w}}" + "".join(f"{lock:>{col_w}}" for lock in locks)
        sep = "-" * len(header)
        print(header)
        print(sep)

        for threads in THREADS_LIST:
            row = f"{threads:>{thr_w}}"
            for lock in locks:
                value = get_tp_interp(data, lock, threads, crit, out, out_values)
                row += f"{value:>{col_w}.3f}" if value is not None else f"{'N/A':>{col_w}}"
            print(row)

    print()


def _configure_x_axis(ax, ymax: float, annotate_cpus: bool) -> None:
    ax.set_xscale("log", base=2)
    ax.axvline(x=NCPUS, color="#AAAAAA", linewidth=1.4, linestyle="--", zorder=1)
    if annotate_cpus:
        ax.text(
            NCPUS * 1.05,
            ymax * 0.97,
            f"{NCPUS} CPUs",
            color="#999",
            fontsize=7.5,
            va="top",
            ha="left",
            style="italic",
        )
    ax.set_xlim(0.8, 200)
    ax.set_xticks(THREADS_LIST)
    ax.xaxis.set_major_formatter(ticker.ScalarFormatter())
    ax.set_xticklabels([str(t) for t in THREADS_LIST], rotation=50, ha="right", fontsize=8)


def _style_axis(ax, ymax: float, yfmt: str = "%.2f") -> None:
    ax.set_ylim(0, ymax)
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter(yfmt))
    ax.tick_params(axis="y", labelsize=8.5)
    ax.grid(True, linestyle=":", alpha=0.5, color="#DDDDDD", zorder=0)
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color("#CCCCCC")


def _style_log_y_axis(ax, ymin: float, ymax: float) -> None:
    safe_ymin = ymin if ymin > 0 else 1e-3
    safe_ymax = ymax if ymax > safe_ymin else safe_ymin * 10

    ax.set_yscale("log")
    ax.set_ylim(safe_ymin, safe_ymax)

    formatter = ticker.ScalarFormatter()
    formatter.set_scientific(False)
    formatter.set_useOffset(False)
    ax.yaxis.set_major_locator(ticker.LogLocator(base=10))
    ax.yaxis.set_major_formatter(formatter)
    ax.yaxis.set_minor_locator(ticker.LogLocator(base=10, subs=range(2, 10)))
    ax.yaxis.set_minor_formatter(ticker.NullFormatter())

    ax.tick_params(axis="y", labelsize=8.5)
    ax.grid(True, which="major", linestyle=":", alpha=0.5, color="#DDDDDD", zorder=0)
    ax.grid(True, which="minor", linestyle=":", alpha=0.25, color="#EEEEEE", zorder=0)
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color("#CCCCCC")


def plot(
    data: dict[str, list[dict[str, str]]],
    locks: list[str],
    colors: dict[str, str],
    markers: dict[str, str],
    out: int,
    crits: list[int],
    out_values: list[int],
    save_path: str,
    show: bool,
) -> None:
    require_matplotlib()

    interpolated = out not in out_values
    out_label = f"out={out}" + (" (interpolated)" if interpolated else "")
    ncols = max(4, len(locks))

    fig, axes = plt.subplots(1, len(crits), figsize=(5 * len(crits) + 4, 5.8))
    if len(crits) == 1:
        axes = [axes]
    fig.patch.set_facecolor("#F7F7F7")

    for ax, crit in zip(axes, crits):
        ratio = crit / (crit + out)
        ax.set_facecolor("white")

        series: dict[str, list[float | None]] = {}
        all_ys: list[float] = []
        for lock in locks:
            ys = [get_tp_interp(data, lock, threads, crit, out, out_values) for threads in THREADS_LIST]
            series[lock] = ys
            all_ys.extend(y for y in ys if y is not None)

        ymax = max(all_ys) * 1.12 if all_ys else 1.0
        _configure_x_axis(ax, ymax, annotate_cpus=True)

        for lock in locks:
            xs = [threads for threads, value in zip(THREADS_LIST, series[lock]) if value is not None]
            ys = [value for value in series[lock] if value is not None]
            ax.plot(
                xs,
                ys,
                color=colors[lock],
                marker=markers[lock],
                linewidth=2.2,
                markersize=6,
                markerfacecolor="white",
                markeredgewidth=1.8,
                label=lock,
                zorder=3,
            )

        _style_axis(ax, ymax)
        ax.set_title(f"ratio = {ratio:.2f}  (crit={crit})", fontsize=11, fontweight="bold", pad=10)
        ax.set_xlabel("Threads", fontsize=9.5, labelpad=4)
        ax.set_ylabel("Throughput (Mops/s)", fontsize=9.5)

    handles, labels_ = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels_,
        loc="lower center",
        ncol=ncols,
        fontsize=11,
        frameon=True,
        framealpha=0.95,
        edgecolor="#CCCCCC",
        bbox_to_anchor=(0.5, -0.02),
        handlelength=2.2,
        columnspacing=2.0,
    )

    fig.suptitle(
        f"Mutex Throughput vs. Thread Count  -  {out_label}",
        fontsize=13.5,
        fontweight="bold",
        y=1.02,
    )

    plt.tight_layout(rect=[0, 0.08, 1, 1])
    plt.savefig(save_path, dpi=160, bbox_inches="tight", facecolor=fig.get_facecolor())
    print(f"Saved: {save_path}")

    if show:
        plt.show()
    plt.close(fig)


def plot_latency_breakdown(
    data: dict[str, list[dict[str, str]]],
    locks: list[str],
    colors: dict[str, str],
    markers: dict[str, str],
    out: int,
    crits: list[int],
    out_values: list[int],
    save_path: str,
    show: bool,
) -> None:
    require_matplotlib()

    interpolated = out not in out_values
    out_label = f"out={out}" + (" (interpolated)" if interpolated else "")
    ncols = max(4, len(locks))

    fig, axes = plt.subplots(
        len(LATENCY_METRICS),
        len(crits),
        figsize=(5 * len(crits) + 4, 12.5),
        squeeze=False,
    )
    fig.patch.set_facecolor("#F7F7F7")

    for row_index, (field, ylabel) in enumerate(LATENCY_METRICS):
        for col_index, crit in enumerate(crits):
            ax = axes[row_index][col_index]
            ratio = crit / (crit + out)
            ax.set_facecolor("white")

            series: dict[str, list[float | None]] = {}
            all_ys: list[float] = []
            for lock in locks:
                ys = [
                    get_metric_interp(data, lock, threads, crit, out, field, out_values)
                    for threads in THREADS_LIST
                ]
                series[lock] = ys
                all_ys.extend(y for y in ys if y is not None and y > 0)

            ymin = min(all_ys) / 1.12 if all_ys else 1e-3
            ymax = max(all_ys) * 1.12 if all_ys else 1.0
            _configure_x_axis(ax, ymax, annotate_cpus=(row_index == 0))

            for lock in locks:
                xs = [
                    threads
                    for threads, value in zip(THREADS_LIST, series[lock])
                    if value is not None and value > 0
                ]
                ys = [value for value in series[lock] if value is not None and value > 0]
                ax.plot(
                    xs,
                    ys,
                    color=colors[lock],
                    marker=markers[lock],
                    linewidth=2.0,
                    markersize=5.6,
                    markerfacecolor="white",
                    markeredgewidth=1.6,
                    label=lock,
                    zorder=3,
                )

            _style_log_y_axis(ax, ymin, ymax)
            if row_index == 0:
                ax.set_title(
                    f"ratio = {ratio:.2f}  (crit={crit})",
                    fontsize=11,
                    fontweight="bold",
                    pad=10,
                )
            ax.set_ylabel(ylabel, fontsize=9.3)
            ax.set_xlabel("Threads", fontsize=9.3, labelpad=4)

    handles, labels_ = axes[0][0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels_,
        loc="lower center",
        ncol=ncols,
        fontsize=11,
        frameon=True,
        framealpha=0.95,
        edgecolor="#CCCCCC",
        bbox_to_anchor=(0.5, -0.015),
        handlelength=2.2,
        columnspacing=2.0,
    )
    fig.suptitle(
        f"Mutex Latency Breakdown vs. Thread Count  -  {out_label}",
        fontsize=13.5,
        fontweight="bold",
        y=1.01,
    )

    plt.tight_layout(rect=[0, 0.06, 1, 0.98])
    plt.savefig(save_path, dpi=160, bbox_inches="tight", facecolor=fig.get_facecolor())
    print(f"Saved: {save_path}")

    if show:
        plt.show()
    plt.close(fig)


def parse_args() -> argparse.Namespace:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_data = os.path.join(script_dir, "..", "results-new")

    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--out", type=int, default=400, help="固定的非临界区迭代次数（默认 400）")
    p.add_argument("--data", default=default_data, help="results 根目录（默认：../results-new/）")
    p.add_argument(
        "--save",
        default=None,
        help="吞吐量图片路径（默认：<data>/throughput_by_ratio.png）",
    )
    p.add_argument(
        "--save-latency",
        default=None,
        help="时延分解图路径（默认：<data>/latency_breakdown_by_ratio.png）",
    )
    p.add_argument("--crits", default=None, help="逗号分隔的 critical_iters 列表，例如 '10,100,800'")
    p.add_argument("--no-show", action="store_true", help="不弹出交互式窗口")
    return p.parse_args()


def main() -> None:
    require_matplotlib()
    args = parse_args()
    data_dir = os.path.realpath(args.data)

    try:
        matplotlib.use("TkAgg")
        import tkinter  # noqa: F401

        show = not args.no_show
    except Exception:
        matplotlib.use("Agg")
        show = False

    locks = discover_locks(data_dir)
    print(f"发现锁实现：{locks}")

    colors, markers = build_styles(locks)
    data = load_data(data_dir, locks, required_fields=LATENCY_PLOT_REQUIRED_FIELDS)
    out_values = available_out_values(data)
    crit_values = available_crit_values(data)

    if args.crits:
        crits = [int(c.strip()) for c in args.crits.split(",")]
        missing = [c for c in crits if c not in crit_values]
        if missing:
            sys.exit(f"Error: crit 值 {missing} 不在数据集中。可用值：{crit_values}")
    else:
        crits = auto_select_crits(crit_values, args.out)

    save_path = args.save or os.path.join(data_dir, "throughput_by_ratio.png")
    latency_save_path = args.save_latency or os.path.join(
        data_dir, "latency_breakdown_by_ratio.png"
    )

    if args.out not in out_values:
        lo = max((v for v in out_values if v < args.out), default=None)
        hi = min((v for v in out_values if v > args.out), default=None)
        print(f"注意：out={args.out} 不在数据集中，将在 {lo} 和 {hi} 之间线性插值。")

    print_table(data, locks, args.out, crits, out_values)
    plot(data, locks, colors, markers, args.out, crits, out_values, save_path, show)
    plot_latency_breakdown(
        data,
        locks,
        colors,
        markers,
        args.out,
        crits,
        out_values,
        latency_save_path,
        show,
    )


if __name__ == "__main__":
    main()
