# mutexbench

一个用于评估不同互斥锁实现吞吐量与扩展性的 C++20 基准测试仓库。  
核心程序 `mutex_bench` 支持在可配置线程数、临界区开销与非临界区开销下测量吞吐，`multilockbench` 支持 Zipfian 热点分布的多锁 workload，配套脚本可批量扫频、多锁对比、结果聚合和绘图。

## 锁的选择方式

基准程序本身不再实现任何锁算法。被测锁就是一个普通的 `pthread_mutex_t`，具体跑
哪个算法完全由 LD_PRELOAD 进来的 LiTL 库决定：

```bash
bash <repo>/third_party/litl/libmbmcs_original.sh ./mutex_bench --lock-kind mutex --threads 8
```

启动时 LiTL 会打印一行 `Using Lib... with waiting ...`，那行就是 preload 生效的证据。

`--lock-kind` 只接受两个值：

- `mutex`（默认）：普通 `pthread_mutex_t`，即 LiTL 当前 interpose 的算法
- `pthread_spinlock`：原生 `pthread_spinlock_t`；LiTL 的 direct 系列算法不会 interpose
  `pthread_spin_*`，因此它是一个不受 interpose 影响的对照组

传入其他值会直接报错，并提示用 LiTL launcher 选择算法。

LiTL launcher 位于 `<repo>/third_party/litl/lib<算法>.sh`，先用 `make -C third_party/litl
ALGORITHMS="<算法>" all` 构建。可用算法名包括：

- 从本仓库迁移过去的基线：`mbmcs`、`mbmcstas`、`mbmcstastse`、`mbmcstasnext`、
  `mbmcstasnexttse`、`mbclh`、`mbtwa`、`mbhapax`、`mbreciprocating`（launcher 文件名带
  `_original` 后缀，如 `libmbmcs_original.sh`）
- 其他基线：`gcr`、`cna`、`flexguard`
- LiTL 上游算法：`mcs_spinlock`、`clh_spinlock`、`malthusian_spinlock`、
  `mutexee_original`、`ticket_original` 等
- accordin：`mcsaccordin_original`、`mcstasaccordin_original`

旧 `--lock-kind` 取值到 LiTL 算法名的对应关系：

| 旧 `--lock-kind` | LiTL 算法 |
| --- | --- |
| `mcs` | `mbmcs` |
| `mcs-tas` | `mbmcstas` |
| `mcs-tas-tse` | `mbmcstastse` |
| `mcstas-next` | `mbmcstasnext` |
| `mcstas-next-tse` | `mbmcstasnexttse` |
| `clh` | `mbclh` |
| `twa` | `mbtwa` |
| `hapax` | `mbhapax` |
| `reciprocating` | `mbreciprocating` |
| `mcs_accordin_direct` | `mcsaccordin_original` |
| `mcs_tas_accordin_direct` | `mcstasaccordin_original` |

原先的 `--timeslice-extension` 选项只用于配置这些内置锁，已随之移除；该机制现在由 LiTL
一侧提供（例如 `mbmcstastse`）。

## 功能概览

- 指标输出：吞吐量、锁内持有时间、平均等待时间近似、解锁到下一次加锁时间估计
- 扫频脚本：自动生成 `raw.csv`（逐次运行）与 `summary.csv`（聚合统计）
- 多锁对比：支持 LiTL launcher、外部 interpose 脚本，以及 `mcs_tse` / `ttas_accordin` 预加载模式
- Python 工具：多锁统计分析、线程推荐、吞吐量曲线图批量生成

## 目录结构

```text
.
├── mutex_bench.cpp                     # 主基准程序
├── curve_bench.cpp                     # BurnIters 开销曲线测量
├── multilockbench.c                    # Zipfian 热点多锁基准程序
├── bench/locks_bench/                  # pthread mutex / spinlock 适配与条件变量后端
├── scripts/
│   ├── sweep_mutex_throughput.sh       # 单锁批量扫频
│   ├── sweep_multilockbench.sh         # Zipfian 多锁基准批量扫频
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
- `pidstat`（`scripts/sweep_mutex_throughput.sh` 需要，用于记录 steady CPU）
- LiTL：`<repo>/third_party/litl`，用于选择被测锁算法
- 可选：`sudo`、`bpftool`（使用 `mcs_tas_accordin`、`ttas_accordin` 或部分锁脚本时可能需要）
- 可选：`python3`（启用 `--sample-bpf` 时需要，用于记录 accordin 控制面 sampler CSV）

## 构建

```bash
make
```

生成可执行文件：

- `./mutex_bench`
- `./curve_bench`
- `./multilockbench`

## 快速开始

### 1) 运行单次基准

```bash
./mutex_bench \
  --threads 4 \
  --duration-ms 1000 \
  --warmup-duration-ms 50 \
  --critical-ns 100 \
  --outside-ns 100 \
  --timing-sample-stride 8 \
  --lock-kind mutex
```

换成某个具体算法，只要把命令包在 LiTL launcher 里：

```bash
bash <repo>/third_party/litl/libmbmcs_original.sh \
  ./mutex_bench --threads 4 --duration-ms 1000 --lock-kind mutex
```

常用参数：

- `--threads N`：线程数
- `--duration-ms N`：正式测量时长（毫秒）
- `--warmup-duration-ms N`：预热时长（毫秒）
- `--critical-ns N`：请求的临界区 Burn 时间（纳秒）
- `--outside-ns N`：请求的临界区外 Burn 时间（纳秒）
- `--timing-sample-stride N`：每 N 次操作采样一次时延
- `--lock-kind`：`mutex|pthread_spinlock`

兼容性说明：

- `--critical-iters` 仍可用，但现在只是 `--critical-ns` 的兼容别名

### 1.1) Zipfian 热点多锁基准

`multilockbench` 在同一个进程内创建多个独立锁，每次操作按 Zipfian 分布选择一个锁：rank 1 映射到 lock 0，因此 `per_lock_operations` 的第一个值就是默认热点锁计数。

```bash
./multilockbench \
  --threads 32 \
  --locks 64 \
  --zipf-alpha 1.2 \
  --duration-ms 5000 \
  --warmup-duration-ms 1000 \
  --critical-ns 300 \
  --outside-ns 3000 \
  --lock-kind mutex
```

同样地，换算法就是换 launcher：

```bash
bash <repo>/third_party/litl/libmbmcstas_original.sh \
  ./multilockbench --threads 32 --locks 64 --zipf-alpha 1.2 --lock-kind mutex
```

常用新增参数：

- `--locks N` / `--num-locks N`：独立锁数量
- `--zipf-alpha A`：热点偏斜程度，`0` 表示均匀分布，值越大越集中到低编号锁
- `--seed N`：每线程随机数种子的基础值，便于复现实验

### 1.2) Zipfian 多锁参数扫频

```bash
scripts/sweep_multilockbench.sh \
  --lock-kinds mutex \
  --litl-locks mbmcs_original,mbmcstas_original \
  --threads 16,32,64 \
  --lock-counts 16,64 \
  --zipf-alpha 0,1.2,2.0 \
  --critical-ns 300 \
  --outside-ns 3000 \
  --duration-ms 5000 \
  --warmup-duration-ms 1000 \
  --repeats 3 \
  --output-root results/multilockbench
```

该脚本输出 `raw.csv` 和 `summary.csv`；`raw.csv` 保留每次运行的 `per_lock_operations`，并用分号分隔各锁计数，便于直接检查热点分布。

`--lock-kinds` 只接受 `mutex` / `pthread_spinlock`；`--litl-locks` 里的每个名字都会成为一条
独立的扫频 arm，运行时用对应的 LiTL launcher 包住基准命令。`raw.csv` 的 `lock_kind` 列记录
的就是这条 arm 的名字。

### 2) 单锁参数扫频（输出 raw + summary）

```bash
scripts/sweep_mutex_throughput.sh \
  --lock-kind mutex \
  --litl-lock mbmcs_original \
  --threads 1,2,4,8,16,32 \
  --critical-ns 10,50,100,200,500 \
  --outside-ns 10,50,100,200,500 \
  --duration-ms 1000 \
  --warmup-duration-ms 50 \
  --repeats 3 \
  --output-raw results/mutex/raw.csv \
  --output-summary results/mutex/summary.csv
```

说明：该脚本会对每次运行启动 `pidstat -u -h -p <pid> 1` 采样 CPU，因此基准时长需要足够长，至少让 `pidstat` 产出一条 `%CPU` 样本。

若启用 `--sample-bpf`，脚本还会为每次运行启动 `scripts/sample_accordin_bpf.py`，并在 `raw.csv` 同目录输出 `t*_c*_o*_r*.bpf_samples.csv`。该功能要求当前 sweep 以 root 身份运行（例如通过 `sudo` 调用脚本），并且仅适用于 `mcs_tas_accordin`、`ttas_accordin`、`reciprocating_accordin` 这类 accordin sched_ext 锁。

### 3) 多锁批量扫频

```bash
scripts/sweep_mutex_throughput_multi_lock.sh \
  --locks mutex,litl:mbmcs_original,litl:mbclh_original \
  --sudo-mode none \
  --threads 1,2,4,8,16,32 \
  --critical-ns 10,50,100,200,500 \
  --outside-ns 10,50,100,200,500 \
  --duration-ms 1000 \
  --repeats 3 \
  --output-root results-new
```

如需同时记录 accordin 控制面 sampler：

```bash
scripts/sweep_mutex_throughput_multi_lock.sh \
  --locks ttas_accordin \
  --sudo-mode auto \
  --sample-bpf \
  --sample-bpf-layout auto \
  --sample-bpf-interval-us 500 \
  --threads 64 \
  --critical-ns 350 \
  --outside-ns 350 \
  --duration-ms 3000 \
  --warmup-duration-ms 50 \
  --repeats 1 \
  --output-root results-sampled
```

`--locks` 支持：

- 内置锁名：`mutex`、`pthread_spinlock`
- `native:<kind>`（`mutex` 或 `pthread_spinlock`）
- `litl:<算法>`（如 `litl:mbmcs_original`、`litl:mcs_spinlock`），用对应的 LiTL launcher 包住基准命令
- `name=/path/to/interpose_xxx.sh`
- `mcs_tse`（通过 `LD_PRELOAD=target/release/libmcs_tse.so` 运行 `mutex` lock kind；可用 `MCS_TSE_LIB` 覆盖库路径，默认按 `target/release`、`target/debug` 查找；不启用 sched_ext 冲突处理或 BPF sampler）
- `mcs_tas_accordin`（等价于 `litl:mcstasaccordin_original`，并附带 accordin 的环境变量；加 `--profile` 会保留每次运行的 `perf.data`，并生成可读的 `perf_reports/*.report.txt` / `*.script.txt`；加 `--sample-bpf` 会保留每次运行的 `*.bpf_samples.csv`）
- `mcs_tas_accordin_no_bpf`（同上，并设置 `MCS_TAS_ACCORDIN_DIRECT_DISABLE_BPF=1`；加 `--profile` 会保留 `perf.data` 并生成 `perf_reports/`）
- `ttas_accordin`（通过 `LD_PRELOAD=target/release/libttas_accordin.so`；加 `--profile` 会保留每次运行的 `perf.data` 并生成 `perf_reports/`；加 `--sample-bpf` 会保留每次运行的 `*.bpf_samples.csv`）
- `ttas_accordin_no_bpf`（通过 `LD_PRELOAD=target/release/libttas_accordin.so`，并设置 `TTAS_ACCORDIN_DISABLE_BPF=1`；加 `--profile` 会保留 `perf.data` 并生成 `perf_reports/`）

并发说明：

- `scripts/sweep_mutex_throughput_multi_lock.sh` 现在会通过 `flock` 使用全局锁文件 `/tmp/mutexbench-sweep-multi-lock.lock`
- 当多个来自不同目录、不同 worktree、甚至不同用户的实例同时启动时，后来的实例会阻塞排队，不会并行运行影响测量结果
- 如需在测试或隔离环境中覆盖锁文件路径，可设置环境变量 `MUTEXBENCH_MULTI_LOCK_LOCK_FILE=/path/to/lock`

### 4) 同进程 two-lock workload

`mutex_bench` 支持 `--workload two-lock`，用于构造两个独立锁 `L1/L2`：
总线程数一分为二，Group A 只访问 `L1`，Group B 只访问 `L2`，并且两组可以使用不同
CS/NCS。

```bash
./mutex_bench \
  --workload two-lock \
  --threads 64 \
  --lock-kind mutex \
  --group-a-critical-ns 3000 \
  --group-a-outside-ns 300 \
  --group-b-critical-ns 100 \
  --group-b-outside-ns 3000 \
  --duration-ms 5000 \
  --warmup-duration-ms 1000
```

该模式会额外输出 `group_a_*`、`group_b_*`、`fairness_jain`、
`group_a_normalized_slowdown` 和 `group_b_normalized_slowdown`。论文实验入口是
`experiments/run_experiment_six.py`，默认运行 homogeneous、heterogeneous mild 和
heterogeneous extreme 三组 two-lock case。

## 结果与指标

### `raw.csv`（逐次运行）

每行代表一个 `(threads, critical_iters, outside_iters, repeat)` 实验点，包含：

说明：为兼容现有分析脚本，CSV 头部暂时沿用 `critical_iters/outside_iters` 字段名，但其数值含义已经是请求的 `critical_ns/outside_ns`。

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
- `avg_cpu_pct`
- 可选：`perf_data_path`（启用 `--profile` 时）
- 可选：`bpf_samples_path`、`bpf_layout`、`bpf_interval_us`（启用 `--sample-bpf` 时）

其中 `avg_cpu_pct` 表示该次运行的 steady `%CPU` 均值：脚本从该 PID 的 `pidstat` 输出中取最后 `duration-ms` 对应的样本窗口再求平均，以避免把启动、预热前等待或退出清理阶段计入稳态 CPU 使用率。

启用 `--sample-bpf` 时，每次运行会在 `raw.csv` 同目录额外生成一个 `t*_c*_o*_r*.bpf_samples.csv`，用于后续分析 accordin 控制面趋势。

启用 `--profile` 时，多锁脚本会在每个 lock 目录下生成 `perf_reports/`：

- `t*_c*_o*_r*.report.txt`：`perf report --stdio -f` 的文本结果
- `t*_c*_o*_r*.script.txt`：`perf script --demangle -f -F ip,sym,dso` 的逐样本结果
- `index.csv`：把每个 `(threads, critical_iters, outside_iters, repeat)` 映射到对应的 `perf.data`、report 和 script

如果 `/proc/sys/kernel/kptr_restrict` 限制普通用户读取内核符号，多锁脚本会优先使用 `sudo -n perf ... -f` 生成 report；手动分析已有结果时也应使用：

```bash
sudo perf report --stdio -f -i results/<lock>/t*_c*_o*_r*.perf.data
```

对默认路径下的 Accordin Rust 锁，`--profile` 会自动用 `--features perf-symbols` 重建对应库，避免内部热点函数被 inline 后只显示成外层 C wrapper。若显式设置了 `MCS_TAS_ACCORDIN_DIRECT_LIB`、`TTAS_ACCORDIN_LIB` 等库路径，脚本会使用调用者提供的库，不会自动重建。

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
- `avg_cpu_pct`

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
- `cpu_by_ratio.png`

其中时延分解图按 ratio 展示三类每-op 指标：

- `avg_wait_ns_estimated`
- `avg_lock_hold_ns`
- `avg_lock_handoff_ns_estimated`

CPU 图按 ratio 展示 `avg_cpu_pct`，同时兼容旧/别名列 `cpu_pct`。

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
  --outside-ns 400
```

## BurnIters 曲线测量（可选）

`curve_bench` 用于测量 `BurnIters(iters)` 的时间曲线，便于将 `critical_ns/outside_ns` 校准到实际开销量级。

```bash
./curve_bench --min-iters 0 --max-iters 10000 --step-iters 100 > curve.csv
```

也可以直接用校准脚本拟合 `ns/iter`，并输出建议的源码校准系数与 CLI iter 映射倍数：

```bash
python3 scripts/calibrate_iters.py \
  --mode curve \
  --max-iters 4000 \
  --step-iters 200 \
  --map-ns 10,50,100,200,400,800 \
  --write-config
```

校准完成后，`mutex_bench` 和 `curve_bench` 会默认读取可执行文件同目录下的 `iter_calibration.cfg`。

如果希望直接按 `mutex_bench` 的持锁时间路径做校准，可以切到 `mutex` 模式：

```bash
python3 scripts/calibrate_iters.py \
  --mode mutex \
  --min-iters 100 \
  --max-iters 2000 \
  --step-iters 100 \
  --outside-ns 0 \
  --write-config
```

## 清理

```bash
make clean
```
