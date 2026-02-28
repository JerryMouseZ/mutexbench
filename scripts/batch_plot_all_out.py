#!/usr/bin/env python3
"""
batch_plot_all_out.py
遍历数据集中所有可用的 outside_iters 值，为每个 out 生成一张吞吐量折线图。

用法：
    python3 batch_plot_all_out.py [--data DIR] [--out-dir DIR] [--crits C,...] [--jobs N]

参数：
    --data DIR    results 根目录（默认：脚本所在目录的上级 results-new/）
    --out-dir DIR 图片输出目录（默认：<data>/plots/）
    --crits C,…   逗号分隔的 critical_iters 列表（默认各 out 自动选 5 个）
    --jobs N      并行进程数（默认：1，顺序执行）
"""

import argparse
import os
import sys
import csv
from concurrent.futures import ProcessPoolExecutor, as_completed


# ── 工具函数 ─────────────────────────────────────────────────────────────────

def discover_out_values(data_dir: str) -> list[int]:
    """从任意一个锁的 summary.csv 中读取所有可用的 outside_iters。"""
    for entry in os.scandir(data_dir):
        if not entry.is_dir():
            continue
        path = os.path.join(entry.path, "summary.csv")
        if os.path.isfile(path):
            with open(path) as f:
                rows = list(csv.DictReader(f))
            return sorted({int(r["outside_iters"]) for r in rows})
    sys.exit(f"Error: 在 {data_dir} 下未找到任何含 summary.csv 的子目录。")


# ── 单任务入口（供子进程调用） ───────────────────────────────────────────────

def _run_one(data_dir: str, out_dir: str, out: int, crits_arg: str | None) -> str:
    """在独立进程中为单个 out 值生成图片，返回保存路径。"""
    # 延迟导入，避免主进程 matplotlib 状态污染子进程
    import matplotlib
    matplotlib.use("Agg")

    # 将 scripts/ 目录加入路径，直接复用绘图模块
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)

    from plot_throughput_by_ratio import (
        discover_locks, build_styles, load_data,
        available_out_values, available_crit_values,
        auto_select_crits, plot,
    )

    locks      = discover_locks(data_dir)
    colors, markers = build_styles(locks)
    data       = load_data(data_dir, locks)
    out_values = available_out_values(data)
    crit_values = available_crit_values(data)

    if crits_arg:
        crits = [int(c.strip()) for c in crits_arg.split(",")]
    else:
        crits = auto_select_crits(crit_values, out)

    save_path = os.path.join(out_dir, f"throughput_out{out:04d}.png")
    plot(data, locks, colors, markers, out, crits, out_values, save_path, show=False)
    return save_path


# ── CLI ─────────────────────────────────────────────────────────────────────

def parse_args():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_data = os.path.join(script_dir, "..", "results-new")

    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data", default=default_data,
                   help="results 根目录（默认：../results-new/）")
    p.add_argument("--out-dir", default=None,
                   help="图片输出目录（默认：<data>/plots/）")
    p.add_argument("--crits", default=None,
                   help="逗号分隔的 critical_iters 列表，例如 '10,100,800'")
    p.add_argument("--jobs", type=int, default=1,
                   help="并行进程数（默认 1）")
    return p.parse_args()


def main():
    args   = parse_args()
    data_dir = os.path.realpath(args.data)
    out_dir  = os.path.realpath(args.out_dir) if args.out_dir \
               else os.path.join(data_dir, "plots")
    os.makedirs(out_dir, exist_ok=True)

    out_values = discover_out_values(data_dir)
    print(f"数据目录  : {data_dir}")
    print(f"输出目录  : {out_dir}")
    print(f"out 值列表: {out_values}  ({len(out_values)} 个)")
    print(f"并行进程  : {args.jobs}")
    print()

    tasks = [(data_dir, out_dir, out, args.crits) for out in out_values]

    if args.jobs == 1:
        for i, (d, o, out, crits) in enumerate(tasks, 1):
            path = _run_one(d, o, out, crits)
            print(f"[{i}/{len(tasks)}] {path}")
    else:
        results = {}
        with ProcessPoolExecutor(max_workers=args.jobs) as pool:
            futures = {pool.submit(_run_one, *t): t[2] for t in tasks}
            done = 0
            for fut in as_completed(futures):
                out_val = futures[fut]
                done += 1
                try:
                    path = fut.result()
                    results[out_val] = path
                    print(f"[{done}/{len(tasks)}] out={out_val}  →  {path}")
                except Exception as exc:
                    print(f"[{done}/{len(tasks)}] out={out_val}  失败: {exc}",
                          file=sys.stderr)

    print(f"\n全部完成，共 {len(tasks)} 张图片保存至 {out_dir}/")


if __name__ == "__main__":
    main()
