#!/usr/bin/env python3
"""
plot_throughput_by_ratio.py
绘制各互斥锁在不同临界区比例下的吞吐量 vs 线程数折线图。

用法：
    python3 plot_throughput_by_ratio.py [--out OUT] [--data DIR] [--save PATH]

参数：
    --out OUT     固定的非临界区迭代次数（默认 400）。
                  若 OUT 不在数据集中，则在相邻两个可用值之间线性插值。
    --data DIR    results 根目录（默认：脚本所在目录的上级 results-new/）。
                  脚本会自动扫描该目录下含有 summary.csv 的子目录作为锁实现。
    --save PATH   输出图片路径（默认：<data>/throughput_by_ratio.png）
    --crits C,…   逗号分隔的 critical_iters 列表（默认自动选 5 个）
    --no-show     不弹出交互式窗口（在无头环境下自动生效）
"""

import argparse
import csv
import os
import sys

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


# ── 常量 ────────────────────────────────────────────────────────────────────

THREADS_LIST = [1, 2, 4, 8, 16, 32, 48, 64, 80, 96, 128, 160]

# 预设样式池：按顺序分配给自动发现的锁（超出后循环）
_COLOR_POOL  = ["#2196F3", "#FF9800", "#43A047", "#E53935",
                "#9C27B0", "#00BCD4", "#FF5722", "#607D8B"]
_MARKER_POOL = ["o", "s", "^", "D", "v", "P", "X", "*"]

# 系统逻辑 CPU 数（用于参考线）
NCPUS = 96


# ── 数据加载 ─────────────────────────────────────────────────────────────────

def discover_locks(data_dir: str) -> list[str]:
    """扫描 data_dir 下含有 summary.csv 的子目录，返回排序后的锁名称列表。"""
    if not os.path.isdir(data_dir):
        sys.exit(f"Error: 数据目录不存在：{data_dir}")
    locks = sorted(
        entry.name
        for entry in os.scandir(data_dir)
        if entry.is_dir()
        and os.path.isfile(os.path.join(entry.path, "summary.csv"))
    )
    if not locks:
        sys.exit(f"Error: 在 {data_dir} 下未找到任何含 summary.csv 的子目录。")
    return locks


def build_styles(locks: list[str]) -> tuple[dict, dict]:
    """为每个锁分配颜色和 marker，超出预设池时循环复用。"""
    colors  = {l: _COLOR_POOL[i % len(_COLOR_POOL)]  for i, l in enumerate(locks)}
    markers = {l: _MARKER_POOL[i % len(_MARKER_POOL)] for i, l in enumerate(locks)}
    return colors, markers


def load_data(data_dir: str, locks: list[str]) -> dict:
    data = {}
    for lock in locks:
        path = os.path.join(data_dir, lock, "summary.csv")
        with open(path) as f:
            data[lock] = list(csv.DictReader(f))
    return data


def available_out_values(data: dict) -> list[int]:
    sample = next(iter(data.values()))
    return sorted({int(r["outside_iters"]) for r in sample})


def available_crit_values(data: dict) -> list[int]:
    sample = next(iter(data.values()))
    return sorted({int(r["critical_iters"]) for r in sample})


# ── 吞吐量查询（支持插值） ──────────────────────────────────────────────────

def get_tp(data: dict, lock: str, threads: int, crit: int, out: int) -> float | None:
    """直接查找精确的 out 值对应的吞吐量。"""
    r = next(
        (r for r in data[lock]
         if int(r["threads"]) == threads
         and int(r["critical_iters"]) == crit
         and int(r["outside_iters"]) == out),
        None,
    )
    return float(r["mean_throughput_ops_per_sec"]) / 1e6 if r else None


def get_tp_interp(data: dict, lock: str, threads: int, crit: int,
                  out: int, out_values: list[int]) -> float | None:
    """当 out 不在数据集时，在相邻可用值间线性插值。"""
    if out in out_values:
        return get_tp(data, lock, threads, crit, out)

    lo = max((v for v in out_values if v < out), default=None)
    hi = min((v for v in out_values if v > out), default=None)

    if lo is None:
        return get_tp(data, lock, threads, crit, hi)
    if hi is None:
        return get_tp(data, lock, threads, crit, lo)

    tp_lo = get_tp(data, lock, threads, crit, lo)
    tp_hi = get_tp(data, lock, threads, crit, hi)
    if tp_lo is None or tp_hi is None:
        return None

    alpha = (out - lo) / (hi - lo)
    return tp_lo + alpha * (tp_hi - tp_lo)


# ── 自动选取 crit 值（覆盖不同 ratio 区间） ──────────────────────────────────

def auto_select_crits(crit_values: list[int], out: int, n: int = 5) -> list[int]:
    """从可用 crit 值中挑选 n 个，使 ratio 尽量均匀分布在 (0, 1)。"""
    ratios = [c / (c + out) for c in crit_values]
    targets = [(i + 1) / (n + 1) for i in range(n)]
    selected = []
    for target in targets:
        best = min(crit_values, key=lambda c: abs(c / (c + out) - target))
        if best not in selected:
            selected.append(best)
    # 若去重后不足 n 个，补充剩余
    for c in crit_values:
        if len(selected) >= n:
            break
        if c not in selected:
            selected.append(c)
    return sorted(selected)


# ── 绘图 ────────────────────────────────────────────────────────────────────

def plot(data: dict, locks: list[str], colors: dict, markers: dict,
         out: int, crits: list[int],
         out_values: list[int], save_path: str, show: bool) -> None:

    interpolated = out not in out_values
    out_label = f"out={out}" + (" (interpolated)" if interpolated else "")
    ncols = max(4, len(locks))  # 图例列数随锁数量调整

    fig, axes = plt.subplots(1, len(crits), figsize=(5 * len(crits) + 4, 5.8))
    if len(crits) == 1:
        axes = [axes]
    fig.patch.set_facecolor("#F7F7F7")

    for ax, crit in zip(axes, crits):
        ratio = crit / (crit + out)
        ax.set_facecolor("white")

        series = {}
        all_ys = []
        for lock in locks:
            ys = [get_tp_interp(data, lock, t, crit, out, out_values)
                  for t in THREADS_LIST]
            series[lock] = ys
            all_ys.extend(y for y in ys if y is not None)

        ymax = max(all_ys) * 1.12 if all_ys else 1.0

        # 对数横坐标：让低线程区和高线程区均匀分布
        ax.set_xscale("log", base=2)

        # 参考线：逻辑 CPU 数
        ax.axvline(x=NCPUS, color="#AAAAAA", linewidth=1.4, linestyle="--", zorder=1)
        ax.text(NCPUS * 1.05, ymax * 0.97, f"{NCPUS} CPUs",
                color="#999", fontsize=7.5, va="top", ha="left", style="italic")

        for lock in locks:
            xs = [t for t, y in zip(THREADS_LIST, series[lock]) if y is not None]
            ys = [y for y in series[lock] if y is not None]
            ax.plot(xs, ys,
                    color=colors[lock], marker=markers[lock],
                    linewidth=2.2, markersize=6,
                    markerfacecolor="white", markeredgewidth=1.8,
                    label=lock, zorder=3)

        ax.set_xlim(0.8, 200)
        ax.set_ylim(0, ymax)
        ax.set_xticks(THREADS_LIST)
        ax.xaxis.set_major_formatter(ticker.ScalarFormatter())
        ax.set_xticklabels([str(t) for t in THREADS_LIST],
                           rotation=50, ha="right", fontsize=8)
        ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.2f"))
        ax.tick_params(axis="y", labelsize=8.5)
        ax.grid(True, linestyle=":", alpha=0.5, color="#DDDDDD", zorder=0)
        ax.spines[["top", "right"]].set_visible(False)
        ax.spines[["left", "bottom"]].set_color("#CCCCCC")
        ax.set_title(f"ratio = {ratio:.2f}  (crit={crit})",
                     fontsize=11, fontweight="bold", pad=10)
        ax.set_xlabel("Threads", fontsize=9.5, labelpad=4)
        ax.set_ylabel("Throughput (Mops/s)", fontsize=9.5)

    handles, labels_ = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels_,
               loc="lower center", ncol=ncols, fontsize=11,
               frameon=True, framealpha=0.95, edgecolor="#CCCCCC",
               bbox_to_anchor=(0.5, -0.02),
               handlelength=2.2, columnspacing=2.0)

    fig.suptitle(f"Mutex Throughput vs. Thread Count  —  {out_label}",
                 fontsize=13.5, fontweight="bold", y=1.02)

    plt.tight_layout(rect=[0, 0.08, 1, 1])
    plt.savefig(save_path, dpi=160, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    print(f"Saved: {save_path}")

    if show:
        plt.show()
    plt.close(fig)


# ── CLI ─────────────────────────────────────────────────────────────────────

def parse_args():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_data = os.path.join(script_dir, "..", "results-new")

    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", type=int, default=400,
                   help="固定的非临界区迭代次数（默认 400）")
    p.add_argument("--data", default=default_data,
                   help="results 根目录（默认：../results-new/）")
    p.add_argument("--save", default=None,
                   help="输出图片路径（默认：<data>/throughput_by_ratio.png）")
    p.add_argument("--crits", default=None,
                   help="逗号分隔的 critical_iters 列表，例如 '10,100,800'")
    p.add_argument("--no-show", action="store_true",
                   help="不弹出交互式窗口")
    return p.parse_args()


def main():
    args = parse_args()
    data_dir = os.path.realpath(args.data)

    # 无头环境检测
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
    data = load_data(data_dir, locks)
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

    if args.out not in out_values:
        lo = max((v for v in out_values if v < args.out), default=None)
        hi = min((v for v in out_values if v > args.out), default=None)
        print(f"注意：out={args.out} 不在数据集中，将在 {lo} 和 {hi} 之间线性插值。")

    plot(data, locks, colors, markers, args.out, crits, out_values, save_path, show)


if __name__ == "__main__":
    main()
