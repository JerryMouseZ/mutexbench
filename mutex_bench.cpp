#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#if defined(__x86_64__) || defined(__i386__)
#include <emmintrin.h>
#include <immintrin.h>
#endif
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "bench/burn_calibration.hpp"
#include "bench/locks_bench/cond_bench.hpp"
#include "bench/locks_bench/lock_bench.hpp"
#include "bench/locks_bench/lock_dispatch.hpp"
#include "bench/locks_bench/lock_kind.hpp"

using Clock = std::chrono::steady_clock;

enum class WorkloadMode {
  kSingle,
  kTwoLock,
  kCvPingPong,
  kCvBroadcast,
};

bool IsCondWorkload(WorkloadMode workload) {
  return workload == WorkloadMode::kCvPingPong ||
         workload == WorkloadMode::kCvBroadcast;
}

inline void SpinPause() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  _mm_pause();
#elif defined(__aarch64__) || defined(__arm__)
  asm volatile("yield" ::: "memory");
#else
  std::this_thread::yield();
#endif
}

struct Config {
  static constexpr uint64_t kDefaultBurnCalibrationNumerator = 9;
  static constexpr uint64_t kDefaultBurnCalibrationDenominator = 32;

  int threads = 4;
  uint64_t duration_ms = 1000;
  uint64_t warmup_duration_ms = 0;
  uint64_t critical_ns = 100;
  uint64_t outside_ns = 100;
  uint64_t group_a_critical_ns = 100;
  uint64_t group_a_outside_ns = 100;
  uint64_t group_b_critical_ns = 100;
  uint64_t group_b_outside_ns = 100;
  bool group_a_critical_explicit = false;
  bool group_a_outside_explicit = false;
  bool group_b_critical_explicit = false;
  bool group_b_outside_explicit = false;
  uint64_t timing_sample_stride = 8;
  uint64_t broadcast_interval_us = 200;
  std::string calibration_config_path;
  bool calibration_config_explicit = false;
  burn_calibration::Calibration burn_calibration{
      kDefaultBurnCalibrationNumerator, kDefaultBurnCalibrationDenominator};
  std::string burn_calibration_source = "compiled-default";
  locks_bench::LockKind lock_kind = locks_bench::LockKind::kMutex;
  locks_bench::TimesliceExtensionMode timeslice_extension_mode =
      locks_bench::TimesliceExtensionMode::kOff;
  WorkloadMode workload = WorkloadMode::kSingle;
};

[[noreturn]] void PrintUsageAndExit(const char *prog) {
  std::cerr
      << "Usage: " << prog
      << " [--threads N] [--duration-ms N] [--warmup-duration-ms N]"
      << " [--critical-ns N] [--outside-ns N] [--timing-sample-stride "
         "N] [--lock-kind mutex|pthread_spinlock|reciprocating|hapax|mcs|"
         "mcs_accordin_direct|mcs-tas|mcs-tas-tse|mcs_tas_accordin_direct|mcstas-next|"
         "mcstas-next-tse|twa|clh]"
      << " [--timeslice-extension off|auto|require]"
      << " [--workload single|two-lock|cv-pingpong|cv-broadcast]"
      << " [--broadcast-interval-us N]"
      << " [--group-a-critical-ns N] [--group-a-outside-ns N]"
      << " [--group-b-critical-ns N] [--group-b-outside-ns N]\n"
      << "  --threads N       Number of worker threads (default: 4)\n"
      << "  --duration-ms N   Measurement duration in milliseconds (default: "
         "1000)\n"
      << "  --warmup-duration-ms N  Warmup duration in milliseconds (default: "
         "0)\n"
      << "  --critical-ns N   Requested critical-section burn time in "
         "nanoseconds (default: 100)\n"
      << "  --outside-ns N    Requested non-critical-section burn time in "
         "nanoseconds (default: 100)\n"
      << "  --critical-iters N  Legacy alias for --critical-ns\n"
      << "  --workload MODE  Workload shape: single, two-lock, cv-pingpong, or "
         "cv-broadcast (default: single)\n"
      << "                   cv-pingpong: token ring over one mutex and one "
         "condition variable per thread; every handoff is wait -> signal\n"
      << "                   cv-broadcast: waiters block on one condition "
         "variable until a coordinator bumps a generation and broadcasts\n"
      << "                   cv workloads support these lock kinds: "
      << locks_bench::SupportedCondLockKinds() << "\n"
      << "  --broadcast-interval-us N  Coordinator broadcast period for the "
         "cv-broadcast workload (default: 200)\n"
      << "  --group-a-critical-ns N  Group A critical-section burn time for "
         "two-lock workload\n"
      << "  --group-a-outside-ns N   Group A non-critical-section burn time for "
         "two-lock workload\n"
      << "  --group-b-critical-ns N  Group B critical-section burn time for "
         "two-lock workload\n"
      << "  --group-b-outside-ns N   Group B non-critical-section burn time for "
         "two-lock workload\n"
      << "  --timing-sample-stride N  Measure timing every N ops (default: "
         "8)\n"
      << "  --calibration-config PATH  Optional iter calibration config "
         "(default: <binary-dir>/iter_calibration.cfg)\n"
      << "  --lock-kind K      Lock kind: "
         "mutex|pthread_spinlock|reciprocating|hapax|mcs|mcs_accordin_direct|mcs-tas|"
         "mcs-tas-tse|mcs_tas_accordin_direct|mcstas-next|mcstas-next-tse|"
         "twa|clh (default: "
         "mutex)\n"
      << "  --timeslice-extension M  off|auto|require (default: off)\n";
  std::exit(1);
}

uint64_t ParseU64(const std::string &s, const char *flag) {
  try {
    size_t idx = 0;
    unsigned long long v = std::stoull(s, &idx, 10);
    if (idx != s.size()) {
      std::cerr << "Invalid value for " << flag << ": " << s << "\n";
      std::exit(1);
    }
    return static_cast<uint64_t>(v);
  } catch (...) {
    std::cerr << "Invalid value for " << flag << ": " << s << "\n";
    std::exit(1);
  }
}

Config ParseArgs(int argc, char *argv[]) {
  Config cfg;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    auto need_next = [&](const char *flag) -> std::string {
      if (i + 1 >= argc) {
        std::cerr << "Missing value for " << flag << "\n";
        PrintUsageAndExit(argv[0]);
      }
      return argv[++i];
    };

    if (arg == "--threads") {
      cfg.threads =
          static_cast<int>(ParseU64(need_next("--threads"), "--threads"));
    } else if (arg == "--duration-ms") {
      cfg.duration_ms = ParseU64(need_next("--duration-ms"), "--duration-ms");
    } else if (arg == "--warmup-duration-ms") {
      cfg.warmup_duration_ms =
          ParseU64(need_next("--warmup-duration-ms"), "--warmup-duration-ms");
    } else if (arg == "--critical-ns" || arg == "--critical-iters") {
      cfg.critical_ns =
          ParseU64(need_next("--critical-ns"), "--critical-ns");
    } else if (arg == "--outside-ns") {
      cfg.outside_ns = ParseU64(need_next("--outside-ns"), "--outside-ns");
    } else if (arg == "--workload") {
      const std::string workload = need_next("--workload");
      if (workload == "single") {
        cfg.workload = WorkloadMode::kSingle;
      } else if (workload == "two-lock") {
        cfg.workload = WorkloadMode::kTwoLock;
      } else if (workload == "cv-pingpong") {
        cfg.workload = WorkloadMode::kCvPingPong;
      } else if (workload == "cv-broadcast") {
        cfg.workload = WorkloadMode::kCvBroadcast;
      } else {
        std::cerr << "Invalid value for --workload: " << workload
                  << " (expected: single, two-lock, cv-pingpong, or "
                     "cv-broadcast)\n";
        std::exit(1);
      }
    } else if (arg == "--broadcast-interval-us") {
      cfg.broadcast_interval_us = ParseU64(need_next("--broadcast-interval-us"),
                                           "--broadcast-interval-us");
    } else if (arg == "--group-a-critical-ns") {
      cfg.group_a_critical_ns =
          ParseU64(need_next("--group-a-critical-ns"), "--group-a-critical-ns");
      cfg.group_a_critical_explicit = true;
    } else if (arg == "--group-a-outside-ns") {
      cfg.group_a_outside_ns =
          ParseU64(need_next("--group-a-outside-ns"), "--group-a-outside-ns");
      cfg.group_a_outside_explicit = true;
    } else if (arg == "--group-b-critical-ns") {
      cfg.group_b_critical_ns =
          ParseU64(need_next("--group-b-critical-ns"), "--group-b-critical-ns");
      cfg.group_b_critical_explicit = true;
    } else if (arg == "--group-b-outside-ns") {
      cfg.group_b_outside_ns =
          ParseU64(need_next("--group-b-outside-ns"), "--group-b-outside-ns");
      cfg.group_b_outside_explicit = true;
    } else if (arg == "--timing-sample-stride") {
      cfg.timing_sample_stride = ParseU64(need_next("--timing-sample-stride"),
                                          "--timing-sample-stride");
    } else if (arg == "--calibration-config") {
      cfg.calibration_config_path = need_next("--calibration-config");
      cfg.calibration_config_explicit = true;
    } else if (arg == "--lock-kind") {
      const std::string lock_kind = need_next("--lock-kind");
      if (!locks_bench::TryParseLockKind(lock_kind, cfg.lock_kind)) {
        std::cerr << "Invalid value for --lock-kind: " << lock_kind
                  << " (expected: mutex, pthread_spinlock, reciprocating, hapax, mcs, "
                     "mcs_accordin_direct, mcs-tas, mcs-tas-tse, "
                     "mcs_tas_accordin_direct, mcstas-next, mcstas-next-tse, "
                     "twa, or clh)\n";
        std::exit(1);
      }
    } else if (arg == "--timeslice-extension") {
      const std::string mode = need_next("--timeslice-extension");
      if (mode == "off") {
        cfg.timeslice_extension_mode =
            locks_bench::TimesliceExtensionMode::kOff;
      } else if (mode == "auto") {
        cfg.timeslice_extension_mode =
            locks_bench::TimesliceExtensionMode::kAuto;
      } else if (mode == "require") {
        cfg.timeslice_extension_mode =
            locks_bench::TimesliceExtensionMode::kRequire;
      } else {
        std::cerr << "Invalid value for --timeslice-extension: " << mode
                  << " (expected: off, auto, or require)\n";
        std::exit(1);
      }
    } else if (arg == "--help" || arg == "-h") {
      PrintUsageAndExit(argv[0]);
    } else {
      std::cerr << "Unknown argument: " << arg << "\n";
      PrintUsageAndExit(argv[0]);
    }
  }

  if (cfg.threads <= 0) {
    std::cerr << "--threads must be > 0\n";
    std::exit(1);
  }
  if (cfg.duration_ms == 0) {
    std::cerr << "--duration-ms must be > 0\n";
    std::exit(1);
  }
  if (cfg.timing_sample_stride == 0) {
    std::cerr << "--timing-sample-stride must be > 0\n";
    std::exit(1);
  }
  if (!cfg.group_a_critical_explicit) {
    cfg.group_a_critical_ns = cfg.critical_ns;
  }
  if (!cfg.group_a_outside_explicit) {
    cfg.group_a_outside_ns = cfg.outside_ns;
  }
  if (!cfg.group_b_critical_explicit) {
    cfg.group_b_critical_ns = cfg.critical_ns;
  }
  if (!cfg.group_b_outside_explicit) {
    cfg.group_b_outside_ns = cfg.outside_ns;
  }
  if (cfg.workload == WorkloadMode::kTwoLock && (cfg.threads % 2) != 0) {
    std::cerr << "--workload two-lock requires an even --threads value so "
                 "each group receives half of the workers\n";
    std::exit(1);
  }
  if (cfg.workload == WorkloadMode::kCvPingPong && cfg.threads < 2) {
    std::cerr << "--workload cv-pingpong requires --threads >= 2 so the token "
                 "can be handed off\n";
    std::exit(1);
  }
  if (cfg.workload == WorkloadMode::kCvBroadcast &&
      cfg.broadcast_interval_us == 0) {
    std::cerr << "--broadcast-interval-us must be > 0\n";
    std::exit(1);
  }
  if (IsCondWorkload(cfg.workload) &&
      !locks_bench::CondBenchSupportsLockKind(cfg.lock_kind)) {
    std::cerr << "--lock-kind " << locks_bench::LockKindToString(cfg.lock_kind)
              << " has no condition-variable backend; condition-variable "
                 "workloads support these lock kinds: "
              << locks_bench::SupportedCondLockKinds() << "\n";
    std::exit(1);
  }
  if (IsCondWorkload(cfg.workload) &&
      cfg.timeslice_extension_mode != locks_bench::TimesliceExtensionMode::kOff) {
    std::cerr << "--timeslice-extension is not supported by condition-variable "
                 "workloads; use --timeslice-extension off\n";
    std::exit(1);
  }
  return cfg;
}

bool ApplyCalibrationConfig(Config *cfg, const char *argv0) {
  const std::filesystem::path config_path =
      cfg->calibration_config_explicit
          ? std::filesystem::path(cfg->calibration_config_path)
          : burn_calibration::DefaultConfigPath(argv0);
  if (!cfg->calibration_config_explicit) {
    std::error_code ec;
    if (!std::filesystem::exists(config_path, ec) || ec) {
      return true;
    }
  }

  const auto load =
      burn_calibration::LoadCalibration(config_path, "mutex_bench");
  switch (load.status) {
    case burn_calibration::LoadStatus::kLoaded:
      cfg->burn_calibration = load.calibration;
      cfg->burn_calibration_source = load.path.string();
      cfg->calibration_config_path = load.path.string();
      return true;
    case burn_calibration::LoadStatus::kMissingFile:
      if (cfg->calibration_config_explicit) {
        std::cerr << "Calibration config not found: " << config_path << "\n";
        return false;
      }
      return true;
    case burn_calibration::LoadStatus::kMissingSection:
      if (cfg->calibration_config_explicit) {
        std::cerr << "Calibration config does not contain mutex_bench.* keys: "
                  << config_path << "\n";
        return false;
      }
      return true;
    case burn_calibration::LoadStatus::kError:
      std::cerr << load.error << "\n";
      return false;
  }
  return false;
}

inline void BurnIters(uint64_t iters,
                      const burn_calibration::Calibration &calibration) {
  if (iters == 0) {
    return;
  }
  const uint64_t raw_iters = std::max<uint64_t>(
      1, (iters * calibration.numerator + (calibration.denominator / 2)) /
             calibration.denominator);
  volatile uint64_t x = 0;
  for (uint64_t i = 0; i < raw_iters; ++i) {
    x = (x * 1664525u) + 1013904223u + i;
  }
}

struct WorkloadTiming {
  uint64_t critical_ns = 0;
  uint64_t outside_ns = 0;
};

struct GroupCounters {
  std::atomic<uint64_t> total_ops{0};
  std::atomic<uint64_t> total_lock_hold_ns{0};
  std::atomic<uint64_t> total_lock_hold_samples{0};
  std::atomic<uint64_t> total_thread_elapsed_ns{0};
};

struct GroupSnapshot {
  int threads = 0;
  uint64_t critical_ns = 0;
  uint64_t outside_ns = 0;
  uint64_t ops = 0;
  uint64_t lock_hold_ns = 0;
  uint64_t lock_hold_samples = 0;
  uint64_t thread_elapsed_ns_total = 0;
  double throughput_ops_per_sec = 0.0;
  double avg_lock_hold_ns = 0.0;
  double avg_wait_ns_estimated = 0.0;
  double avg_lock_handoff_ns_estimated = 0.0;
  double ideal_throughput_ops_per_sec = 0.0;
  double normalized_efficiency = 0.0;
  double normalized_slowdown = 0.0;
};

double SafeDivide(double numerator, double denominator) {
  return denominator > 0.0 ? numerator / denominator : 0.0;
}

GroupSnapshot SnapshotGroup(const GroupCounters &counters,
                            const WorkloadTiming &timing, int threads,
                            double elapsed_s, double elapsed_ns) {
  GroupSnapshot snapshot;
  snapshot.threads = threads;
  snapshot.critical_ns = timing.critical_ns;
  snapshot.outside_ns = timing.outside_ns;
  snapshot.ops = counters.total_ops.load(std::memory_order_relaxed);
  snapshot.lock_hold_ns =
      counters.total_lock_hold_ns.load(std::memory_order_relaxed);
  snapshot.lock_hold_samples =
      counters.total_lock_hold_samples.load(std::memory_order_relaxed);
  snapshot.thread_elapsed_ns_total =
      counters.total_thread_elapsed_ns.load(std::memory_order_relaxed);
  snapshot.throughput_ops_per_sec = SafeDivide(snapshot.ops, elapsed_s);
  snapshot.avg_lock_hold_ns =
      snapshot.lock_hold_samples
          ? SafeDivide(snapshot.lock_hold_ns, snapshot.lock_hold_samples)
          : 0.0;

  const double estimated_total_lock_hold_ns =
      snapshot.avg_lock_hold_ns * static_cast<double>(snapshot.ops);
  snapshot.avg_wait_ns_estimated =
      snapshot.ops ? SafeDivide(std::max(static_cast<double>(
                                             snapshot.thread_elapsed_ns_total) -
                                             estimated_total_lock_hold_ns,
                                         0.0),
                                static_cast<double>(snapshot.ops))
                   : 0.0;
  snapshot.avg_lock_handoff_ns_estimated =
      snapshot.ops ? SafeDivide(std::max(elapsed_ns - estimated_total_lock_hold_ns,
                                         0.0),
                                static_cast<double>(snapshot.ops))
                   : 0.0;

  const double requested_op_ns =
      static_cast<double>(timing.critical_ns + timing.outside_ns);
  snapshot.ideal_throughput_ops_per_sec =
      requested_op_ns > 0.0
          ? static_cast<double>(threads) * 1000000000.0 / requested_op_ns
          : 0.0;
  snapshot.normalized_efficiency =
      SafeDivide(snapshot.throughput_ops_per_sec,
                 snapshot.ideal_throughput_ops_per_sec);
  snapshot.normalized_slowdown =
      SafeDivide(snapshot.ideal_throughput_ops_per_sec,
                 snapshot.throughput_ops_per_sec);
  return snapshot;
}

void PrintGroupSnapshot(const char *prefix, const GroupSnapshot &snapshot) {
  std::cout << prefix << "_threads: " << snapshot.threads << "\n";
  std::cout << prefix << "_critical_ns: " << snapshot.critical_ns << "\n";
  std::cout << prefix << "_outside_ns: " << snapshot.outside_ns << "\n";
  std::cout << prefix << "_total_operations: " << snapshot.ops << "\n";
  std::cout << prefix
            << "_throughput_ops_per_sec: " << snapshot.throughput_ops_per_sec
            << "\n";
  std::cout << prefix
            << "_lock_hold_samples: " << snapshot.lock_hold_samples << "\n";
  std::cout << prefix << "_avg_lock_hold_ns: " << snapshot.avg_lock_hold_ns
            << "\n";
  std::cout << prefix
            << "_avg_wait_ns_estimated: " << snapshot.avg_wait_ns_estimated
            << "\n";
  std::cout << prefix << "_avg_lock_handoff_ns_estimated: "
            << snapshot.avg_lock_handoff_ns_estimated << "\n";
  std::cout << prefix << "_ideal_throughput_ops_per_sec: "
            << snapshot.ideal_throughput_ops_per_sec << "\n";
  std::cout << std::setprecision(6);
  std::cout << prefix
            << "_normalized_efficiency: " << snapshot.normalized_efficiency
            << "\n";
  std::cout << prefix
            << "_normalized_slowdown: " << snapshot.normalized_slowdown
            << "\n";
  std::cout << std::setprecision(2);
}

double JainFairness(double a, double b) {
  const double denominator = 2.0 * ((a * a) + (b * b));
  return denominator > 0.0 ? ((a + b) * (a + b)) / denominator : 0.0;
}

void PrintPerThreadOperations(const std::vector<uint64_t> &per_thread_ops) {
  std::cout << "per_thread_operations: ";
  for (size_t i = 0; i < per_thread_ops.size(); ++i) {
    if (i != 0) {
      std::cout << ",";
    }
    std::cout << per_thread_ops[i];
  }
  std::cout << "\n";
}

// Keeps a bounded, uniformly decimated sample of a latency stream so long runs
// cannot grow without bound while percentiles still cover the whole run.
class LatencySamples {
public:
  void Add(uint64_t value_ns) {
    ++observed_;
    if (++since_kept_ < stride_) {
      return;
    }
    since_kept_ = 0;
    samples_.push_back(value_ns);
    if (samples_.size() >= kMaxSamples) {
      Decimate();
    }
  }

  uint64_t observed() const { return observed_; }
  const std::vector<uint64_t> &samples() const { return samples_; }

private:
  static constexpr size_t kMaxSamples = 1u << 20;

  void Decimate() {
    size_t out = 0;
    for (size_t i = 1; i < samples_.size(); i += 2) {
      samples_[out++] = samples_[i];
    }
    samples_.resize(out);
    stride_ *= 2;
  }

  std::vector<uint64_t> samples_;
  uint64_t observed_ = 0;
  uint64_t since_kept_ = 0;
  uint64_t stride_ = 1;
};

struct LatencyStats {
  uint64_t observed = 0;
  uint64_t samples = 0;
  double avg_ns = 0.0;
  uint64_t min_ns = 0;
  uint64_t p50_ns = 0;
  uint64_t p90_ns = 0;
  uint64_t p99_ns = 0;
  uint64_t p999_ns = 0;
  uint64_t max_ns = 0;
};

uint64_t PercentileNs(const std::vector<uint64_t> &sorted, uint64_t per_mille) {
  if (sorted.empty()) {
    return 0;
  }
  const uint64_t count = static_cast<uint64_t>(sorted.size());
  uint64_t rank = (count * per_mille + 999) / 1000;
  if (rank == 0) {
    rank = 1;
  }
  if (rank > count) {
    rank = count;
  }
  return sorted[static_cast<size_t>(rank - 1)];
}

LatencyStats ComputeLatencyStats(std::vector<uint64_t> values,
                                 uint64_t observed) {
  LatencyStats stats;
  stats.observed = observed;
  stats.samples = static_cast<uint64_t>(values.size());
  if (values.empty()) {
    return stats;
  }
  std::sort(values.begin(), values.end());
  double sum = 0.0;
  for (const uint64_t value : values) {
    sum += static_cast<double>(value);
  }
  stats.avg_ns = sum / static_cast<double>(values.size());
  stats.min_ns = values.front();
  stats.max_ns = values.back();
  stats.p50_ns = PercentileNs(values, 500);
  stats.p90_ns = PercentileNs(values, 900);
  stats.p99_ns = PercentileNs(values, 990);
  stats.p999_ns = PercentileNs(values, 999);
  return stats;
}

LatencyStats MergeLatencyStats(const std::vector<LatencySamples> &per_thread) {
  std::vector<uint64_t> merged;
  uint64_t observed = 0;
  size_t total = 0;
  for (const auto &thread_samples : per_thread) {
    total += thread_samples.samples().size();
  }
  merged.reserve(total);
  for (const auto &thread_samples : per_thread) {
    observed += thread_samples.observed();
    merged.insert(merged.end(), thread_samples.samples().begin(),
                  thread_samples.samples().end());
  }
  return ComputeLatencyStats(std::move(merged), observed);
}

void PrintLatencyStats(const char *prefix, const LatencyStats &stats) {
  std::cout << prefix << "_observed: " << stats.observed << "\n";
  std::cout << prefix << "_samples: " << stats.samples << "\n";
  std::cout << prefix << "_avg_ns: " << stats.avg_ns << "\n";
  std::cout << prefix << "_min_ns: " << stats.min_ns << "\n";
  std::cout << prefix << "_p50_ns: " << stats.p50_ns << "\n";
  std::cout << prefix << "_p90_ns: " << stats.p90_ns << "\n";
  std::cout << prefix << "_p99_ns: " << stats.p99_ns << "\n";
  std::cout << prefix << "_p999_ns: " << stats.p999_ns << "\n";
  std::cout << prefix << "_max_ns: " << stats.max_ns << "\n";
}

template <typename LockBenchT> int RunSingleLockBenchmarkForLock(const Config &cfg) {
  static_assert(locks_bench::LockBench<LockBenchT>);

  LockBenchT lock_bench(
      locks_bench::LockBenchOptions{cfg.timeslice_extension_mode});
  std::atomic<uint64_t> total_ops{0};
  std::atomic<uint64_t> total_lock_hold_ns{0};
  std::atomic<uint64_t> total_lock_hold_samples{0};
  std::atomic<uint64_t> total_thread_elapsed_ns{0};
  std::atomic<int> workers_ready{0};
  std::atomic<int> warmup_done{0};
  std::atomic<bool> warmup_start{false};
  std::atomic<bool> warmup_stop{false};
  std::atomic<bool> measure_start{false};
  std::atomic<bool> measure_stop{false};
  std::vector<uint64_t> per_thread_ops(static_cast<size_t>(cfg.threads), 0);

  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(cfg.threads));

  for (int t = 0; t < cfg.threads; ++t) {
    workers.emplace_back([&, thread_index = t]() {
      lock_bench.prepare_thread();

      uint64_t local_lock_hold_ns = 0;
      uint64_t local_lock_hold_samples = 0;
      static thread_local uint64_t local_ops = 0;
      local_ops = 0;

      workers_ready.fetch_add(1, std::memory_order_release);
      while (!warmup_start.load(std::memory_order_acquire)) {
        SpinPause();
      }

      if (cfg.warmup_duration_ms > 0) {
        while (!warmup_stop.load(std::memory_order_acquire)) {
          auto guard_state = lock_bench.lock();
          BurnIters(cfg.critical_ns, cfg.burn_calibration);
          lock_bench.unlock(guard_state);
          BurnIters(cfg.outside_ns, cfg.burn_calibration);
        }
      }

      warmup_done.fetch_add(1, std::memory_order_release);
      while (!measure_start.load(std::memory_order_acquire)) {
        SpinPause();
      }

      const auto thread_measure_start = Clock::now();
      uint64_t sample_countdown =
          static_cast<uint64_t>(thread_index) % cfg.timing_sample_stride;
      while (!measure_stop.load(std::memory_order_acquire)) {
        const bool do_timing_sample = (sample_countdown == 0);
        if (sample_countdown == 0) {
          sample_countdown = cfg.timing_sample_stride - 1;
        } else {
          --sample_countdown;
        }

        Clock::time_point after_lock;
        Clock::time_point before_unlock;

        auto guard_state = lock_bench.lock();
        if (do_timing_sample) {
          after_lock = Clock::now();
        }
        BurnIters(cfg.critical_ns, cfg.burn_calibration);
        if (do_timing_sample) {
          before_unlock = Clock::now();
        }
        lock_bench.unlock(guard_state);

        if (do_timing_sample) {
          const auto hold_ns =
              std::chrono::duration_cast<std::chrono::nanoseconds>(
                  before_unlock - after_lock)
                  .count();
          if (hold_ns >= 0) {
            local_lock_hold_ns += static_cast<uint64_t>(hold_ns);
            ++local_lock_hold_samples;
          }
        }
        BurnIters(cfg.outside_ns, cfg.burn_calibration);
        ++local_ops;
      }
      const auto thread_measure_end = Clock::now();
      const auto local_thread_elapsed_ns = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              thread_measure_end - thread_measure_start)
              .count());

      total_lock_hold_ns.fetch_add(local_lock_hold_ns,
                                   std::memory_order_relaxed);
      total_lock_hold_samples.fetch_add(local_lock_hold_samples,
                                        std::memory_order_relaxed);
      total_thread_elapsed_ns.fetch_add(local_thread_elapsed_ns,
                                        std::memory_order_relaxed);
      per_thread_ops[static_cast<size_t>(thread_index)] = local_ops;
      total_ops.fetch_add(local_ops, std::memory_order_relaxed);
    });
  }

  while (workers_ready.load(std::memory_order_acquire) < cfg.threads) {
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }

  warmup_start.store(true, std::memory_order_release);
  if (cfg.warmup_duration_ms > 0) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(cfg.warmup_duration_ms));
    warmup_stop.store(true, std::memory_order_release);
  }

  while (warmup_done.load(std::memory_order_acquire) < cfg.threads) {
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }

  const auto start = Clock::now();
  measure_start.store(true, std::memory_order_release);
  std::this_thread::sleep_for(std::chrono::milliseconds(cfg.duration_ms));
  measure_stop.store(true, std::memory_order_release);

  for (auto &th : workers) {
    th.join();
  }
  const auto end = Clock::now();

  const double elapsed_s =
      std::chrono::duration_cast<std::chrono::duration<double>>(end - start)
          .count();
  const double elapsed_ns =
      std::chrono::duration_cast<std::chrono::duration<double, std::nano>>(
          end - start)
          .count();
  const uint64_t ops = total_ops.load(std::memory_order_relaxed);
  const uint64_t lock_hold_ns =
      total_lock_hold_ns.load(std::memory_order_relaxed);
  const uint64_t lock_hold_samples =
      total_lock_hold_samples.load(std::memory_order_relaxed);
  const uint64_t thread_elapsed_ns_total =
      total_thread_elapsed_ns.load(std::memory_order_relaxed);
  const double throughput = ops / elapsed_s;
  const double avg_lock_hold_ns =
      lock_hold_samples ? static_cast<double>(lock_hold_ns) /
                              static_cast<double>(lock_hold_samples)
                        : 0.0;
  const double estimated_total_lock_hold_ns =
      avg_lock_hold_ns * static_cast<double>(ops);
  const double avg_wait_ns_estimated =
      ops ? std::max(static_cast<double>(thread_elapsed_ns_total) -
                         estimated_total_lock_hold_ns,
                     0.0) /
                static_cast<double>(ops)
          : 0.0;
  const double avg_lock_handoff_ns_estimated =
      ops ? std::max(elapsed_ns - estimated_total_lock_hold_ns, 0.0) /
                static_cast<double>(ops)
          : 0.0;
  std::cout << "workload: single\n";
  std::cout << "threads: " << cfg.threads << "\n";
  std::cout << "critical_ns: " << cfg.critical_ns << "\n";
  std::cout << "outside_ns: " << cfg.outside_ns << "\n";
  std::cout << "burn_calibration: "
            << burn_calibration::ToString(cfg.burn_calibration) << "\n";
  std::cout << "burn_calibration_source: " << cfg.burn_calibration_source
            << "\n";
  std::cout << "total_operations: " << ops << "\n";
  PrintPerThreadOperations(per_thread_ops);
  std::cout << std::fixed << std::setprecision(6);
  std::cout << "elapsed_seconds: " << elapsed_s << "\n";
  std::cout << std::setprecision(2);
  std::cout << "throughput_ops_per_sec: " << throughput << "\n";
  std::cout << "lock_hold_samples: " << lock_hold_samples << "\n";
  std::cout << "avg_lock_hold_ns: " << avg_lock_hold_ns << "\n";
  std::cout << "avg_wait_ns_estimated: " << avg_wait_ns_estimated << "\n";
  std::cout << "avg_lock_handoff_ns_estimated: "
            << avg_lock_handoff_ns_estimated << "\n";
  return 0;
}

template <typename LockBenchT> int RunTwoLockBenchmarkForLock(const Config &cfg) {
  static_assert(locks_bench::LockBench<LockBenchT>);

  LockBenchT group_a_lock(
      locks_bench::LockBenchOptions{cfg.timeslice_extension_mode});
  LockBenchT group_b_lock(
      locks_bench::LockBenchOptions{cfg.timeslice_extension_mode});
  const int group_threads = cfg.threads / 2;
  const WorkloadTiming group_a_timing{cfg.group_a_critical_ns,
                                      cfg.group_a_outside_ns};
  const WorkloadTiming group_b_timing{cfg.group_b_critical_ns,
                                      cfg.group_b_outside_ns};
  GroupCounters group_a_counters;
  GroupCounters group_b_counters;
  std::atomic<int> workers_ready{0};
  std::atomic<int> warmup_done{0};
  std::atomic<bool> warmup_start{false};
  std::atomic<bool> warmup_stop{false};
  std::atomic<bool> measure_start{false};
  std::atomic<bool> measure_stop{false};
  std::vector<uint64_t> per_thread_ops(static_cast<size_t>(cfg.threads), 0);

  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(cfg.threads));

  auto spawn_group = [&](LockBenchT &lock_bench, const WorkloadTiming timing,
                         GroupCounters &counters, int first_thread_index) {
    for (int i = 0; i < group_threads; ++i) {
      workers.emplace_back([&, timing, thread_index = first_thread_index + i]() {
        lock_bench.prepare_thread();

        uint64_t local_lock_hold_ns = 0;
        uint64_t local_lock_hold_samples = 0;
        static thread_local uint64_t local_ops = 0;
        local_ops = 0;

        workers_ready.fetch_add(1, std::memory_order_release);
        while (!warmup_start.load(std::memory_order_acquire)) {
          SpinPause();
        }

        if (cfg.warmup_duration_ms > 0) {
          while (!warmup_stop.load(std::memory_order_acquire)) {
            auto guard_state = lock_bench.lock();
            BurnIters(timing.critical_ns, cfg.burn_calibration);
            lock_bench.unlock(guard_state);
            BurnIters(timing.outside_ns, cfg.burn_calibration);
          }
        }

        warmup_done.fetch_add(1, std::memory_order_release);
        while (!measure_start.load(std::memory_order_acquire)) {
          SpinPause();
        }

        const auto thread_measure_start = Clock::now();
        uint64_t sample_countdown =
            static_cast<uint64_t>(thread_index) % cfg.timing_sample_stride;
        while (!measure_stop.load(std::memory_order_acquire)) {
          const bool do_timing_sample = (sample_countdown == 0);
          if (sample_countdown == 0) {
            sample_countdown = cfg.timing_sample_stride - 1;
          } else {
            --sample_countdown;
          }

          Clock::time_point after_lock;
          Clock::time_point before_unlock;

          auto guard_state = lock_bench.lock();
          if (do_timing_sample) {
            after_lock = Clock::now();
          }
          BurnIters(timing.critical_ns, cfg.burn_calibration);
          if (do_timing_sample) {
            before_unlock = Clock::now();
          }
          lock_bench.unlock(guard_state);

          if (do_timing_sample) {
            const auto hold_ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    before_unlock - after_lock)
                    .count();
            if (hold_ns >= 0) {
              local_lock_hold_ns += static_cast<uint64_t>(hold_ns);
              ++local_lock_hold_samples;
            }
          }
          BurnIters(timing.outside_ns, cfg.burn_calibration);
          ++local_ops;
        }
        const auto thread_measure_end = Clock::now();
        const auto local_thread_elapsed_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                thread_measure_end - thread_measure_start)
                .count());

        counters.total_lock_hold_ns.fetch_add(local_lock_hold_ns,
                                              std::memory_order_relaxed);
        counters.total_lock_hold_samples.fetch_add(local_lock_hold_samples,
                                                   std::memory_order_relaxed);
        counters.total_thread_elapsed_ns.fetch_add(local_thread_elapsed_ns,
                                                   std::memory_order_relaxed);
        per_thread_ops[static_cast<size_t>(thread_index)] = local_ops;
        counters.total_ops.fetch_add(local_ops, std::memory_order_relaxed);
      });
    }
  };

  spawn_group(group_a_lock, group_a_timing, group_a_counters, 0);
  spawn_group(group_b_lock, group_b_timing, group_b_counters, group_threads);

  while (workers_ready.load(std::memory_order_acquire) < cfg.threads) {
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }

  warmup_start.store(true, std::memory_order_release);
  if (cfg.warmup_duration_ms > 0) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(cfg.warmup_duration_ms));
    warmup_stop.store(true, std::memory_order_release);
  }

  while (warmup_done.load(std::memory_order_acquire) < cfg.threads) {
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }

  const auto start = Clock::now();
  measure_start.store(true, std::memory_order_release);
  std::this_thread::sleep_for(std::chrono::milliseconds(cfg.duration_ms));
  measure_stop.store(true, std::memory_order_release);

  for (auto &th : workers) {
    th.join();
  }
  const auto end = Clock::now();

  const double elapsed_s =
      std::chrono::duration_cast<std::chrono::duration<double>>(end - start)
          .count();
  const double elapsed_ns =
      std::chrono::duration_cast<std::chrono::duration<double, std::nano>>(
          end - start)
          .count();
  const GroupSnapshot group_a =
      SnapshotGroup(group_a_counters, group_a_timing, group_threads, elapsed_s,
                    elapsed_ns);
  const GroupSnapshot group_b =
      SnapshotGroup(group_b_counters, group_b_timing, group_threads, elapsed_s,
                    elapsed_ns);
  const uint64_t ops = group_a.ops + group_b.ops;
  const uint64_t lock_hold_samples =
      group_a.lock_hold_samples + group_b.lock_hold_samples;
  const double throughput = SafeDivide(ops, elapsed_s);
  const double avg_lock_hold_ns =
      lock_hold_samples
          ? SafeDivide(group_a.lock_hold_ns + group_b.lock_hold_ns,
                       lock_hold_samples)
          : 0.0;
  const double avg_wait_ns_estimated =
      SafeDivide(group_a.avg_wait_ns_estimated * static_cast<double>(group_a.ops) +
                     group_b.avg_wait_ns_estimated *
                         static_cast<double>(group_b.ops),
                 static_cast<double>(ops));
  const double avg_lock_handoff_ns_estimated =
      SafeDivide(group_a.avg_lock_handoff_ns_estimated *
                     static_cast<double>(group_a.ops) +
                     group_b.avg_lock_handoff_ns_estimated *
                         static_cast<double>(group_b.ops),
                 static_cast<double>(ops));
  const double fairness_jain =
      JainFairness(group_a.normalized_efficiency, group_b.normalized_efficiency);

  std::cout << "workload: two-lock\n";
  std::cout << "threads: " << cfg.threads << "\n";
  std::cout << "critical_ns: mixed\n";
  std::cout << "outside_ns: mixed\n";
  std::cout << "burn_calibration: "
            << burn_calibration::ToString(cfg.burn_calibration) << "\n";
  std::cout << "burn_calibration_source: " << cfg.burn_calibration_source
            << "\n";
  std::cout << "total_operations: " << ops << "\n";
  PrintPerThreadOperations(per_thread_ops);
  std::cout << std::fixed << std::setprecision(6);
  std::cout << "elapsed_seconds: " << elapsed_s << "\n";
  std::cout << std::setprecision(2);
  std::cout << "throughput_ops_per_sec: " << throughput << "\n";
  std::cout << "lock_hold_samples: " << lock_hold_samples << "\n";
  std::cout << "avg_lock_hold_ns: " << avg_lock_hold_ns << "\n";
  std::cout << "avg_wait_ns_estimated: " << avg_wait_ns_estimated << "\n";
  std::cout << "avg_lock_handoff_ns_estimated: "
            << avg_lock_handoff_ns_estimated << "\n";
  PrintGroupSnapshot("group_a", group_a);
  PrintGroupSnapshot("group_b", group_b);
  std::cout << std::setprecision(6);
  std::cout << "fairness_jain: " << fairness_jain << "\n";
  return 0;
}

// Threads form a ring: the token owner burns the critical section, hands the
// token to the next thread and signals that thread's condition variable, so
// every operation is a wait -> signal handoff over one shared mutex.
template <typename CondBenchT>
int RunCvPingPongBenchmarkForCond(const Config &cfg) {
  static_assert(locks_bench::CondBench<CondBenchT>);

  const size_t thread_count = static_cast<size_t>(cfg.threads);
  CondBenchT cv(thread_count);

  size_t token_owner = 0;
  bool stop = false;
  std::vector<Clock::time_point> signal_ts(thread_count);

  std::atomic<int> workers_ready{0};
  std::atomic<bool> run_start{false};
  std::atomic<bool> measuring{false};
  std::vector<uint64_t> per_thread_ops(thread_count, 0);
  std::vector<LatencySamples> wake_latency(thread_count);

  std::vector<std::thread> workers;
  workers.reserve(thread_count);

  for (size_t t = 0; t < thread_count; ++t) {
    workers.emplace_back([&, thread_index = t]() {
      cv.prepare_thread();
      uint64_t local_ops = 0;
      LatencySamples &local_wake_latency = wake_latency[thread_index];

      workers_ready.fetch_add(1, std::memory_order_release);
      while (!run_start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }

      bool skip_latency = (thread_index == 0);
      cv.lock();
      for (;;) {
        while (token_owner != thread_index && !stop) {
          cv.wait(thread_index);
        }
        if (stop) {
          break;
        }

        const auto acquired = Clock::now();
        if (measuring.load(std::memory_order_relaxed) && !skip_latency) {
          const auto wake_ns =
              std::chrono::duration_cast<std::chrono::nanoseconds>(
                  acquired - signal_ts[thread_index])
                  .count();
          if (wake_ns >= 0) {
            local_wake_latency.Add(static_cast<uint64_t>(wake_ns));
          }
          ++local_ops;
        }
        skip_latency = false;

        BurnIters(cfg.critical_ns, cfg.burn_calibration);

        const size_t next = (thread_index + 1) % thread_count;
        token_owner = next;
        signal_ts[next] = Clock::now();
        cv.signal(next);
        cv.unlock();

        BurnIters(cfg.outside_ns, cfg.burn_calibration);
        cv.lock();
      }
      cv.unlock();

      per_thread_ops[thread_index] = local_ops;
    });
  }

  while (workers_ready.load(std::memory_order_acquire) <
         static_cast<int>(thread_count)) {
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }

  run_start.store(true, std::memory_order_release);
  if (cfg.warmup_duration_ms > 0) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(cfg.warmup_duration_ms));
  }

  const auto start = Clock::now();
  measuring.store(true, std::memory_order_relaxed);
  std::this_thread::sleep_for(std::chrono::milliseconds(cfg.duration_ms));
  measuring.store(false, std::memory_order_relaxed);
  const auto end = Clock::now();

  cv.lock();
  stop = true;
  for (size_t i = 0; i < thread_count; ++i) {
    cv.broadcast(i);
  }
  cv.unlock();

  for (auto &th : workers) {
    th.join();
  }

  const double elapsed_s =
      std::chrono::duration_cast<std::chrono::duration<double>>(end - start)
          .count();
  uint64_t handoffs = 0;
  for (const uint64_t ops : per_thread_ops) {
    handoffs += ops;
  }
  const LatencyStats wake_stats = MergeLatencyStats(wake_latency);
  const double throughput = SafeDivide(handoffs, elapsed_s);

  std::cout << "workload: cv-pingpong\n";
  std::cout << "cond_backend: " << CondBenchT::name() << "\n";
  std::cout << "threads: " << cfg.threads << "\n";
  std::cout << "critical_ns: " << cfg.critical_ns << "\n";
  std::cout << "outside_ns: " << cfg.outside_ns << "\n";
  std::cout << "burn_calibration: "
            << burn_calibration::ToString(cfg.burn_calibration) << "\n";
  std::cout << "burn_calibration_source: " << cfg.burn_calibration_source
            << "\n";
  std::cout << "total_operations: " << handoffs << "\n";
  PrintPerThreadOperations(per_thread_ops);
  std::cout << std::fixed << std::setprecision(6);
  std::cout << "elapsed_seconds: " << elapsed_s << "\n";
  std::cout << std::setprecision(2);
  std::cout << "throughput_ops_per_sec: " << throughput << "\n";
  std::cout << "handoffs: " << handoffs << "\n";
  std::cout << "handoffs_per_sec: " << throughput << "\n";
  PrintLatencyStats("wake_latency", wake_stats);
  return 0;
}

// One coordinator bumps a generation counter and broadcasts on a fixed period;
// every waiter must re-acquire the shared mutex, observe the new generation and
// go back to waiting.
template <typename CondBenchT>
int RunCvBroadcastBenchmarkForCond(const Config &cfg) {
  static_assert(locks_bench::CondBench<CondBenchT>);

  constexpr size_t kWaiterCond = 0;
  constexpr size_t kCoordinatorCond = 1;
  const size_t waiter_count = static_cast<size_t>(cfg.threads);
  CondBenchT cv(2);

  uint64_t generation = 0;
  size_t arrived = 0;
  size_t registered = 0;
  bool stop = false;
  Clock::time_point broadcast_ts;
  LatencySamples completion_latency;

  std::atomic<int> workers_ready{0};
  std::atomic<bool> run_start{false};
  std::atomic<bool> measuring{false};
  std::atomic<uint64_t> total_broadcasts{0};
  std::vector<uint64_t> per_thread_ops(waiter_count, 0);
  std::vector<LatencySamples> wake_latency(waiter_count);

  std::vector<std::thread> workers;
  workers.reserve(waiter_count + 1);

  for (size_t t = 0; t < waiter_count; ++t) {
    workers.emplace_back([&, thread_index = t]() {
      cv.prepare_thread();
      uint64_t local_ops = 0;
      LatencySamples &local_wake_latency = wake_latency[thread_index];

      workers_ready.fetch_add(1, std::memory_order_release);
      while (!run_start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }

      cv.lock();
      uint64_t observed_generation = generation;
      ++registered;
      if (registered == waiter_count) {
        cv.signal(kCoordinatorCond);
      }

      for (;;) {
        while (generation == observed_generation && !stop) {
          cv.wait(kWaiterCond);
        }
        if (stop) {
          break;
        }

        const auto acquired = Clock::now();
        observed_generation = generation;
        const auto wake_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(acquired -
                                                                 broadcast_ts)
                .count();
        const bool measure = measuring.load(std::memory_order_relaxed);
        if (measure) {
          if (wake_ns >= 0) {
            local_wake_latency.Add(static_cast<uint64_t>(wake_ns));
          }
          ++local_ops;
        }

        ++arrived;
        if (arrived == waiter_count) {
          if (measure && wake_ns >= 0) {
            completion_latency.Add(static_cast<uint64_t>(wake_ns));
          }
          cv.signal(kCoordinatorCond);
        }

        BurnIters(cfg.critical_ns, cfg.burn_calibration);
        cv.unlock();

        BurnIters(cfg.outside_ns, cfg.burn_calibration);
        cv.lock();
      }
      cv.unlock();

      per_thread_ops[thread_index] = local_ops;
    });
  }

  workers.emplace_back([&]() {
    cv.prepare_thread();
    uint64_t local_broadcasts = 0;

    workers_ready.fetch_add(1, std::memory_order_release);
    while (!run_start.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }

    cv.lock();
    while (registered < waiter_count && !stop) {
      cv.wait(kCoordinatorCond);
    }
    arrived = waiter_count;

    for (;;) {
      while (arrived < waiter_count && !stop) {
        cv.wait(kCoordinatorCond);
      }
      if (stop) {
        break;
      }
      cv.unlock();

      std::this_thread::sleep_for(
          std::chrono::microseconds(cfg.broadcast_interval_us));

      cv.lock();
      if (stop) {
        break;
      }
      arrived = 0;
      ++generation;
      broadcast_ts = Clock::now();
      if (measuring.load(std::memory_order_relaxed)) {
        ++local_broadcasts;
      }
      cv.broadcast(kWaiterCond);
    }
    cv.unlock();

    total_broadcasts.store(local_broadcasts, std::memory_order_relaxed);
  });

  while (workers_ready.load(std::memory_order_acquire) <
         static_cast<int>(waiter_count + 1)) {
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }

  run_start.store(true, std::memory_order_release);
  if (cfg.warmup_duration_ms > 0) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(cfg.warmup_duration_ms));
  }

  const auto start = Clock::now();
  measuring.store(true, std::memory_order_relaxed);
  std::this_thread::sleep_for(std::chrono::milliseconds(cfg.duration_ms));
  measuring.store(false, std::memory_order_relaxed);
  const auto end = Clock::now();

  cv.lock();
  stop = true;
  cv.broadcast(kWaiterCond);
  cv.broadcast(kCoordinatorCond);
  cv.unlock();

  for (auto &th : workers) {
    th.join();
  }

  const double elapsed_s =
      std::chrono::duration_cast<std::chrono::duration<double>>(end - start)
          .count();
  uint64_t wakeups = 0;
  for (const uint64_t ops : per_thread_ops) {
    wakeups += ops;
  }
  const uint64_t broadcasts = total_broadcasts.load(std::memory_order_relaxed);
  const LatencyStats wake_stats = MergeLatencyStats(wake_latency);
  const LatencyStats completion_stats = ComputeLatencyStats(
      completion_latency.samples(), completion_latency.observed());
  const double throughput = SafeDivide(wakeups, elapsed_s);

  std::cout << "workload: cv-broadcast\n";
  std::cout << "cond_backend: " << CondBenchT::name() << "\n";
  std::cout << "threads: " << cfg.threads << "\n";
  std::cout << "cv_waiter_threads: " << waiter_count << "\n";
  std::cout << "cv_coordinator_threads: 1\n";
  std::cout << "broadcast_interval_us: " << cfg.broadcast_interval_us << "\n";
  std::cout << "critical_ns: " << cfg.critical_ns << "\n";
  std::cout << "outside_ns: " << cfg.outside_ns << "\n";
  std::cout << "burn_calibration: "
            << burn_calibration::ToString(cfg.burn_calibration) << "\n";
  std::cout << "burn_calibration_source: " << cfg.burn_calibration_source
            << "\n";
  std::cout << "total_operations: " << wakeups << "\n";
  PrintPerThreadOperations(per_thread_ops);
  std::cout << std::fixed << std::setprecision(6);
  std::cout << "elapsed_seconds: " << elapsed_s << "\n";
  std::cout << std::setprecision(2);
  std::cout << "throughput_ops_per_sec: " << throughput << "\n";
  std::cout << "broadcasts: " << broadcasts << "\n";
  std::cout << "broadcasts_per_sec: " << SafeDivide(broadcasts, elapsed_s)
            << "\n";
  PrintLatencyStats("broadcast_completion", completion_stats);
  PrintLatencyStats("wake_latency", wake_stats);
  return 0;
}

int main(int argc, char *argv[]) {
  Config cfg = ParseArgs(argc, argv);
  if (!ApplyCalibrationConfig(&cfg, argv[0])) {
    return 1;
  }

  if (cfg.timeslice_extension_mode !=
      locks_bench::TimesliceExtensionMode::kOff) {
    const auto status = locks_bench::CurrentThreadTimesliceExtensionStatus(
        cfg.timeslice_extension_mode);
    if (!status.enabled) {
      if (cfg.timeslice_extension_mode ==
          locks_bench::TimesliceExtensionMode::kRequire) {
        std::cerr << "timeslice extension is required but unavailable";
        if (status.reason != nullptr) {
          std::cerr << ": " << status.reason;
        }
        if (status.error_number != 0) {
          std::cerr << " (errno=" << status.error_number << ", "
                    << std::strerror(status.error_number) << ")";
        }
        std::cerr << "\n";
        return 1;
      }
      if (status.reason != nullptr) {
        std::cerr << "Warning: timeslice extension is unavailable; "
                     "continuing without it: "
                  << status.reason;
        if (status.error_number != 0) {
          std::cerr << " (errno=" << status.error_number << ", "
                    << std::strerror(status.error_number) << ")";
        }
        std::cerr << "\n";
      }
    }
  }

  if (IsCondWorkload(cfg.workload)) {
    return locks_bench::DispatchByCondLockKind(
        cfg.lock_kind, [&]<typename CondBenchT>() {
          if (cfg.workload == WorkloadMode::kCvBroadcast) {
            return RunCvBroadcastBenchmarkForCond<CondBenchT>(cfg);
          }
          return RunCvPingPongBenchmarkForCond<CondBenchT>(cfg);
        });
  }

  return locks_bench::DispatchByLockKind(cfg.lock_kind, [&]<typename LockBenchT>() {
    if (cfg.workload == WorkloadMode::kTwoLock) {
      return RunTwoLockBenchmarkForLock<LockBenchT>(cfg);
    }
    return RunSingleLockBenchmarkForLock<LockBenchT>(cfg);
  });
}
