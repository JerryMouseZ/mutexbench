#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <emmintrin.h>
#include <iomanip>
#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "bench/burn_calibration.hpp"
#include "bench/locks_bench/lock_bench.hpp"
#include "bench/locks_bench/lock_dispatch.hpp"
#include "bench/locks_bench/lock_kind.hpp"

using Clock = std::chrono::steady_clock;

inline uint64_t ReadTsc() {
#if defined(__x86_64__) || defined(__i386__)
  return __rdtsc();
#else
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          Clock::now().time_since_epoch())
          .count());
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
  uint64_t timing_sample_stride = 8;
  std::string calibration_config_path;
  bool calibration_config_explicit = false;
  burn_calibration::Calibration burn_calibration{
      kDefaultBurnCalibrationNumerator, kDefaultBurnCalibrationDenominator};
  std::string burn_calibration_source = "compiled-default";
  locks_bench::LockKind lock_kind = locks_bench::LockKind::kMutex;
  locks_bench::TimesliceExtensionMode timeslice_extension_mode =
      locks_bench::TimesliceExtensionMode::kOff;
};

[[noreturn]] void PrintUsageAndExit(const char *prog) {
  std::cerr
      << "Usage: " << prog
      << " [--threads N] [--duration-ms N] [--warmup-duration-ms N]"
      << " [--critical-ns N] [--outside-ns N] [--timing-sample-stride "
         "N] [--lock-kind mutex|reciprocating|hapax|mcs|mcs-tas|mcs-tas-tse|"
         "mcstas-next|mcstas-next-tse|twa|clh]"
      << " [--timeslice-extension off|auto|require]\n"
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
      << "  --outside-iters N   Legacy alias for --outside-ns\n"
      << "  --timing-sample-stride N  Measure timing every N ops (default: "
         "8)\n"
      << "  --calibration-config PATH  Optional iter calibration config "
         "(default: <binary-dir>/iter_calibration.cfg)\n"
      << "  --lock-kind K      Lock kind: "
         "mutex|reciprocating|hapax|mcs|mcs-tas|mcs-tas-tse|mcstas-next|"
         "mcstas-next-tse|twa|clh (default: "
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
    } else if (arg == "--outside-ns" || arg == "--outside-iters") {
      cfg.outside_ns = ParseU64(need_next("--outside-ns"), "--outside-ns");
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
                  << " (expected: mutex, reciprocating, hapax, mcs, mcs-tas, "
                     "mcs-tas-tse, mcstas-next, mcstas-next-tse, twa, or "
                     "clh)\n";
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

template <typename LockBenchT> int RunBenchmarkForLock(const Config &cfg) {
  static_assert(locks_bench::LockBench<LockBenchT>);

  LockBenchT lock_bench(
      locks_bench::LockBenchOptions{cfg.timeslice_extension_mode});
  std::atomic<uint64_t> total_ops{0};
  std::atomic<uint64_t> total_lock_hold_cycles{0};
  std::atomic<uint64_t> total_lock_hold_samples{0};
  std::atomic<uint64_t> total_thread_elapsed_ns{0};
  std::atomic<int> workers_ready{0};
  std::atomic<int> warmup_done{0};
  std::atomic<bool> warmup_start{false};
  std::atomic<bool> warmup_stop{false};
  std::atomic<bool> measure_start{false};
  std::atomic<bool> measure_stop{false};

  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(cfg.threads));

  for (int t = 0; t < cfg.threads; ++t) {
    workers.emplace_back([&, thread_index = t]() {
      lock_bench.prepare_thread();

      uint64_t local_lock_hold_cycles = 0;
      uint64_t local_lock_hold_samples = 0;
      uint64_t local_ops = 0;

      workers_ready.fetch_add(1, std::memory_order_release);
      while (!warmup_start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
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
        _mm_pause();
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

        uint64_t after_lock = 0;
        uint64_t before_unlock = 0;

        auto guard_state = lock_bench.lock();
        if (do_timing_sample) {
          after_lock = ReadTsc();
        }
        BurnIters(cfg.critical_ns, cfg.burn_calibration);
        if (do_timing_sample) {
          before_unlock = ReadTsc();
        }
        lock_bench.unlock(guard_state);

        if (do_timing_sample) {
          if (before_unlock >= after_lock) {
            local_lock_hold_cycles += (before_unlock - after_lock);
            ++local_lock_hold_samples;
          }
        }
        BurnIters(cfg.outside_ns, cfg.burn_calibration);
        ++local_ops;
      }
      const auto thread_measure_end = Clock::now();
      const auto local_thread_elapsed_ns =
          static_cast<uint64_t>(std::chrono::duration_cast<
                                    std::chrono::nanoseconds>(
                                    thread_measure_end - thread_measure_start)
                                    .count());

      total_lock_hold_cycles.fetch_add(local_lock_hold_cycles,
                                       std::memory_order_relaxed);
      total_lock_hold_samples.fetch_add(local_lock_hold_samples,
                                        std::memory_order_relaxed);
      total_thread_elapsed_ns.fetch_add(local_thread_elapsed_ns,
                                        std::memory_order_relaxed);
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
  const uint64_t tsc_start = ReadTsc();
  measure_start.store(true, std::memory_order_release);
  std::this_thread::sleep_for(std::chrono::milliseconds(cfg.duration_ms));
  measure_stop.store(true, std::memory_order_release);

  for (auto &th : workers) {
    th.join();
  }
  const uint64_t tsc_end = ReadTsc();
  const auto end = Clock::now();

  const double elapsed_s =
      std::chrono::duration_cast<std::chrono::duration<double>>(end - start)
          .count();
  const double elapsed_ns =
      std::chrono::duration_cast<std::chrono::duration<double, std::nano>>(
          end - start)
          .count();
  const uint64_t elapsed_cycles =
      (tsc_end > tsc_start) ? (tsc_end - tsc_start) : 0;
  const double ns_per_cycle =
      elapsed_cycles ? (elapsed_ns / static_cast<double>(elapsed_cycles)) : 0.0;
  const uint64_t ops = total_ops.load(std::memory_order_relaxed);
  const uint64_t lock_hold_cycles =
      total_lock_hold_cycles.load(std::memory_order_relaxed);
  const uint64_t lock_hold_samples =
      total_lock_hold_samples.load(std::memory_order_relaxed);
  const uint64_t thread_elapsed_ns_total =
      total_thread_elapsed_ns.load(std::memory_order_relaxed);
  const double throughput = ops / elapsed_s;
  const double avg_lock_hold_cycles =
      lock_hold_samples ? static_cast<double>(lock_hold_cycles) /
                              static_cast<double>(lock_hold_samples)
                        : 0.0;
  const double avg_lock_hold_ns = avg_lock_hold_cycles * ns_per_cycle;
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
  std::cout << "threads: " << cfg.threads << "\n";
  std::cout << "critical_ns: " << cfg.critical_ns << "\n";
  std::cout << "outside_ns: " << cfg.outside_ns << "\n";
  std::cout << "burn_calibration: "
            << burn_calibration::ToString(cfg.burn_calibration) << "\n";
  std::cout << "burn_calibration_source: " << cfg.burn_calibration_source
            << "\n";
  std::cout << "total_operations: " << ops << "\n";
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

  return locks_bench::DispatchByLockKind(
      cfg.lock_kind, [&]<typename LockBenchT>() {
        return RunBenchmarkForLock<LockBenchT>(cfg);
      });
}
