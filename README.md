# mutexbench

一个用于评估不同互斥锁实现吞吐量与扩展性的 C++20 基准测试仓库。  
核心程序 `mutex_bench` 支持在可配置线程数、临界区开销与非临界区开销下测量吞吐，配套脚本可批量扫频、多锁对比、结果聚合和绘图。

## 功能概览

- 支持锁类型：`mutex`、`reciprocating`、`hapax`、`mcs`、`mcs-tas`、`mcs-tas-tse`、`mcstas-next`、`mcstas-next-tse`、`twa`、`clh`
- 指标输出：吞吐量、锁内持有时间、平均等待时间近似、解锁到下一次加锁时间估计
- 扫频脚本：自动生成 `raw.csv`（逐次运行）与 `summary.csv`（聚合统计）
- 多锁对比：支持内置锁、外部 interpose 脚本、`lb_simple` 预加载模式
- Python 工具：多锁统计分析、线程推荐、吞吐量曲线图批量生成

## 目录结构

```text
.
├── mutex_bench.cpp                     # 主基准程序
├── curve_bench.cpp                     # BurnIters 开销曲线测量
├── locks/                              # 各锁实现
├── bench/locks_bench/                  # 锁适配与调度
├── scripts/
│   ├── sweep_mutex_throughput.sh       # 单锁批量扫频
│   ├── sweep_mutex_throughput_multi_lock.sh  # 多锁批量扫频
│   ├── analyze_multi_lock.py           # 多锁统计分析
│   ├── recommend_threads.py            # 推荐线程数
│   ├── plot_throughput_by_ratio.py     # 单图绘制
│   └── batch_plot_all_out.py           # 批量绘图
└── results*/                           # 结果目录示例
```

## 环境要求

- Linux
- `g++`（支持 C++20）
- `make`
- Python 3（绘图脚本需要 `matplotlib`）
- 可选：`sudo`、`bpftool`（使用 `lb_simple`/部分锁脚本时可能需要）

## 构建

```bash
make
```

生成可执行文件：

- `./mutex_bench`
- `./curve_bench`

## 快速开始

### 1) 运行单次基准

```bash
./mutex_bench \
  --threads 4 \
  --duration-ms 1000 \
  --warmup-duration-ms 50 \
  --critical-iters 100 \
  --outside-iters 100 \
  --timing-sample-stride 8 \
  --lock-kind mutex \
  --timeslice-extension auto
```

常用参数：

- `--threads N`：线程数
- `--duration-ms N`：正式测量时长（毫秒）
- `--warmup-duration-ms N`：预热时长（毫秒）
- `--critical-iters N`：临界区 Burn 循环次数
- `--outside-iters N`：临界区外 Burn 循环次数
- `--timing-sample-stride N`：每 N 次操作采样一次时延
- `--lock-kind`：`mutex|reciprocating|hapax|mcs|mcs-tas|mcs-tas-tse|mcstas-next|mcstas-next-tse|twa|clh`
- `--timeslice-extension`：`off|auto|require`

### 1.1) 使用 timeslice extension

如果内核和 glibc 同时支持 RSEQ timeslice extension，可以在持锁临界区请求时间片延长，并在解锁后通过 `rseq_slice_yield()` 主动交还扩展时间片：

```bash
./mutex_bench \
  --threads 32 \
  --critical-iters 100 \
  --outside-iters 100 \
  --lock-kind mcs \
  --timeslice-extension auto
```

模式说明：

- `off`：关闭（默认）
- `auto`：尝试启用；当前环境不支持时自动回退
- `require`：必须启用；不支持时直接报错退出

注意：

- 该功能依赖线程已注册的 `rseq` 区域能够暴露 `slice_ctrl` 字段；旧 glibc 即使在新内核上也可能无法使用
- 更适合用户态自旋/队列锁（如 `mcs`、`mcs-tas`、`mcs-tas-tse`、`clh`、`twa`、`hapax`、`reciprocating`）

### 2) 单锁参数扫频（输出 raw + summary）

```bash
scripts/sweep_mutex_throughput.sh \
  --lock-kind mutex \
  --timeslice-extension auto \
  --threads 1,2,4,8,16,32 \
  --critical-iters 10,50,100,200,500 \
  --outside-iters 10,50,100,200,500 \
  --duration-ms 1000 \
  --warmup-duration-ms 50 \
  --repeats 3 \
  --output-raw results/mutex/raw.csv \
  --output-summary results/mutex/summary.csv
```

### 3) 多锁批量扫频

```bash
scripts/sweep_mutex_throughput_multi_lock.sh \
  --locks mutex,mcs,clh \
  --sudo-mode none \
  --timeslice-extension auto \
  --threads 1,2,4,8,16,32 \
  --critical-iters 10,50,100,200,500 \
  --outside-iters 10,50,100,200,500 \
  --duration-ms 1000 \
  --repeats 3 \
  --output-root results-new
```

`--locks` 支持：

- 内置锁名（如 `mutex,mcs,clh`）
- `native:<kind>`
- `name=/path/to/interpose_xxx.sh`
- `lb_simple`（通过 `LD_PRELOAD=liblb_simple.so`）

## 结果与指标

### `raw.csv`（逐次运行）

每行代表一个 `(threads, critical_iters, outside_iters, repeat)` 实验点，包含：

- `threads`
- `critical_iters`
- `outside_iters`
- `repeat`
- `throughput_ops_per_sec`
- `elapsed_seconds`
- `total_operations`
- `avg_lock_hold_ns`
- `avg_wait_ns_estimated`
- `avg_lock_handoff_ns_estimated`
- `lock_hold_samples`

### `summary.csv`（聚合结果）

按 `(threads, critical_iters, outside_iters)` 聚合，包含：

- `threads`
- `critical_iters`
- `outside_iters`
- `repeats`
- `mean_throughput_ops_per_sec`
- `elapsed_seconds`
- `total_operations`
- `avg_lock_hold_ns`
- `avg_wait_ns_estimated`
- `avg_lock_handoff_ns_estimated`
- `lock_hold_samples`

## 绘图与分析

### 吞吐量与时延分解图

```bash
python3 scripts/plot_throughput_by_ratio.py \
  --data results-new \
  --out 400 \
  --no-show
```

默认会同时生成：

- `throughput_by_ratio.png`
- `latency_breakdown_by_ratio.png`

其中时延分解图按 ratio 展示三类每-op 指标：

- `avg_wait_ns_estimated`
- `avg_lock_hold_ns`
- `avg_lock_handoff_ns_estimated`

批量按所有 `outside_iters` 出图：

```bash
python3 scripts/batch_plot_all_out.py \
  --data results-new \
  --out-dir results-new/plots \
  --jobs 4
```

### 多锁统计分析

```bash
python3 scripts/analyze_multi_lock.py \
  --results-root results \
  --locks mutex,mcs,flexguardall \
  --threads 1,2,4,8,16,32,64 \
  --out-dir results/analysis_multi
```

### 线程数推荐

```bash
python3 scripts/recommend_threads.py \
  --results-root results-new \
  --lock mcs \
  --critical-iters 100 \
  --outside-iters 400
```

## BurnIters 曲线测量（可选）

`curve_bench` 用于测量 `BurnIters(iters)` 的时间曲线，便于将 `critical_iters/outside_iters` 映射到实际开销量级。

```bash
./curve_bench --min-iters 0 --max-iters 10000 --step-iters 100 > curve.csv
```

## 清理

```bash
make clean
```
