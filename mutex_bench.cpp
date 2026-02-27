#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif
#include <iostream>
#include <string>
#include <thread>
#include <vector>

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
  int threads = 4;
  uint64_t duration_ms = 1000;
  uint64_t warmup_duration_ms = 0;
  uint64_t critical_iters = 100;
  uint64_t outside_iters = 100;
  uint64_t timing_sample_stride = 8;
  locks_bench::LockKind lock_kind = locks_bench::LockKind::kMutex;
};

[[noreturn]] void PrintUsageAndExit(const char *prog) {
  std::cerr
      << "Usage: " << prog
      << " [--threads N] [--duration-ms N] [--warmup-duration-ms N]"
      << " [--critical-iters N] [--outside-iters N] [--timing-sample-stride "
         "N] [--lock-kind mutex|reciprocating|hapax|mcs|twa|clh]\n"
      << "  --threads N       Number of worker threads (default: 4)\n"
      << "  --duration-ms N   Measurement duration in milliseconds (default: "
         "1000)\n"
      << "  --warmup-duration-ms N  Warmup duration in milliseconds (default: "
         "0)\n"
      << "  --critical-iters N  Loop iterations in critical section (default: "
         "100)\n"
      << "  --outside-iters N   Loop iterations outside lock (default: 100)\n"
      << "  --timing-sample-stride N  Measure timing every N ops (default: "
         "8)\n"
      << "  --lock-kind K      Lock kind: mutex|reciprocating|hapax|mcs|twa|clh (default: "
         "mutex)\n";
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
    } else if (arg == "--critical-iters") {
      cfg.critical_iters =
          ParseU64(need_next("--critical-iters"), "--critical-iters");
    } else if (arg == "--outside-iters") {
      cfg.outside_iters =
          ParseU64(need_next("--outside-iters"), "--outside-iters");
    } else if (arg == "--timing-sample-stride") {
      cfg.timing_sample_stride = ParseU64(need_next("--timing-sample-stride"),
                                          "--timing-sample-stride");
    } else if (arg == "--lock-kind") {
      const std::string lock_kind = need_next("--lock-kind");
      if (!locks_bench::TryParseLockKind(lock_kind, cfg.lock_kind)) {
        std::cerr << "Invalid value for --lock-kind: " << lock_kind
                  << " (expected: mutex, reciprocating, hapax, mcs, twa, or clh)\n";
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

inline void BurnIters(uint64_t iters) {
  volatile uint64_t x = 0;
  for (uint64_t i = 0; i < iters; ++i) {
    x = (x * 1664525u) + 1013904223u + i;
  }
}

template <typename LockBenchT> int RunBenchmarkForLock(const Config &cfg) {
  static_assert(locks_bench::LockBench<LockBenchT>);

  LockBenchT lock_bench;
  uint64_t protected_counter = 0;
  std::atomic<uint64_t> total_ops{0};
  std::atomic<uint64_t> total_lock_hold_cycles{0};
  std::atomic<uint64_t> total_lock_hold_samples{0};
  std::atomic<uint64_t> total_unlock_to_next_lock_cycles_w0{0};
  std::atomic<uint64_t> total_unlock_to_next_lock_samples_w0{0};
  std::atomic<uint64_t> total_unlock_to_next_lock_cycles_w_gt0{0};
  std::atomic<uint64_t> total_unlock_to_next_lock_samples_w_gt0{0};
  std::atomic<int64_t> lock_waiters{0};
  std::atomic<uint64_t> total_waiters_before_lock{0};
  std::atomic<int> workers_ready{0};
  std::atomic<int> warmup_done{0};
  std::atomic<bool> warmup_start{false};
  std::atomic<bool> warmup_stop{false};
  std::atomic<bool> measure_start{false};
  std::atomic<bool> measure_stop{false};
  // Protected by lock_bench; tracks unlock timestamp of previous lock owner.
  uint64_t global_last_before_unlock = 0;
  bool has_global_last_before_unlock = false;

  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(cfg.threads));

  for (int t = 0; t < cfg.threads; ++t) {
    workers.emplace_back([&, thread_index = t]() {
      uint64_t local_lock_hold_cycles = 0;
      uint64_t local_lock_hold_samples = 0;
      uint64_t local_unlock_to_next_lock_cycles_w0 = 0;
      uint64_t local_unlock_to_next_lock_samples_w0 = 0;
      uint64_t local_unlock_to_next_lock_cycles_w_gt0 = 0;
      uint64_t local_unlock_to_next_lock_samples_w_gt0 = 0;
      uint64_t local_waiters_before_lock = 0;
      uint64_t local_ops = 0;

      workers_ready.fetch_add(1, std::memory_order_release);
      while (!warmup_start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }

      if (cfg.warmup_duration_ms > 0) {
        while (!warmup_stop.load(std::memory_order_acquire)) {
          lock_waiters.fetch_add(1, std::memory_order_relaxed);
          auto guard_state = lock_bench.lock();
          BurnIters(cfg.critical_iters);
          lock_bench.unlock(guard_state);
          lock_waiters.fetch_sub(1, std::memory_order_relaxed);
          BurnIters(cfg.outside_iters);
        }
      }

      warmup_done.fetch_add(1, std::memory_order_release);
      while (!measure_start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }

      uint64_t sample_countdown =
          static_cast<uint64_t>(thread_index) % cfg.timing_sample_stride;
      while (!measure_stop.load(std::memory_order_acquire)) {
        const int64_t waiters_before_lock =
            lock_waiters.fetch_add(1, std::memory_order_relaxed);
        local_waiters_before_lock += static_cast<uint64_t>(waiters_before_lock);
        const bool do_timing_sample = (sample_countdown == 0);
        if (sample_countdown == 0) {
          sample_countdown = cfg.timing_sample_stride - 1;
        } else {
          --sample_countdown;
        }

        uint64_t after_lock = 0;
        uint64_t before_unlock = 0;
        uint64_t prev_global_before_unlock = 0;
        bool has_prev_global_before_unlock = false;

        auto guard_state = lock_bench.lock();
        if (do_timing_sample) {
          after_lock = ReadTsc();
          if (has_global_last_before_unlock) {
            prev_global_before_unlock = global_last_before_unlock;
            has_prev_global_before_unlock = true;
          }
        }
        BurnIters(cfg.critical_iters);
        ++protected_counter;
        before_unlock = ReadTsc();
        global_last_before_unlock = before_unlock;
        has_global_last_before_unlock = true;
        lock_bench.unlock(guard_state);

        lock_waiters.fetch_sub(1, std::memory_order_relaxed);
        if (do_timing_sample) {
          if (has_prev_global_before_unlock &&
              after_lock >= prev_global_before_unlock) {
            const uint64_t delta_cycles =
                (after_lock - prev_global_before_unlock);
            if (waiters_before_lock == 0) {
              local_unlock_to_next_lock_cycles_w0 += delta_cycles;
              ++local_unlock_to_next_lock_samples_w0;
            } else {
              local_unlock_to_next_lock_cycles_w_gt0 += delta_cycles;
              ++local_unlock_to_next_lock_samples_w_gt0;
            }
          }
          if (before_unlock >= after_lock) {
            local_lock_hold_cycles += (before_unlock - after_lock);
            ++local_lock_hold_samples;
          }
        }
        BurnIters(cfg.outside_iters);
        ++local_ops;
      }

      total_lock_hold_cycles.fetch_add(local_lock_hold_cycles,
                                       std::memory_order_relaxed);
      total_lock_hold_samples.fetch_add(local_lock_hold_samples,
                                        std::memory_order_relaxed);
      total_unlock_to_next_lock_cycles_w0.fetch_add(
          local_unlock_to_next_lock_cycles_w0, std::memory_order_relaxed);
      total_unlock_to_next_lock_samples_w0.fetch_add(
          local_unlock_to_next_lock_samples_w0, std::memory_order_relaxed);
      total_unlock_to_next_lock_cycles_w_gt0.fetch_add(
          local_unlock_to_next_lock_cycles_w_gt0, std::memory_order_relaxed);
      total_unlock_to_next_lock_samples_w_gt0.fetch_add(
          local_unlock_to_next_lock_samples_w_gt0, std::memory_order_relaxed);
      total_waiters_before_lock.fetch_add(local_waiters_before_lock,
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
  const uint64_t unlock_to_next_lock_cycles_w0 =
      total_unlock_to_next_lock_cycles_w0.load(std::memory_order_relaxed);
  const uint64_t unlock_to_next_lock_samples_w0 =
      total_unlock_to_next_lock_samples_w0.load(std::memory_order_relaxed);
  const uint64_t unlock_to_next_lock_cycles_w_gt0 =
      total_unlock_to_next_lock_cycles_w_gt0.load(std::memory_order_relaxed);
  const uint64_t unlock_to_next_lock_samples_w_gt0 =
      total_unlock_to_next_lock_samples_w_gt0.load(std::memory_order_relaxed);
  const uint64_t waiters_before_lock_total =
      total_waiters_before_lock.load(std::memory_order_relaxed);
  const double throughput = ops / elapsed_s;
  const double avg_lock_hold_cycles =
      lock_hold_samples ? static_cast<double>(lock_hold_cycles) /
                              static_cast<double>(lock_hold_samples)
                        : 0.0;
  const double avg_unlock_to_next_lock_cycles_w0 =
      unlock_to_next_lock_samples_w0
          ? static_cast<double>(unlock_to_next_lock_cycles_w0) /
                static_cast<double>(unlock_to_next_lock_samples_w0)
          : 0.0;
  const double avg_unlock_to_next_lock_cycles_w_gt0 =
      unlock_to_next_lock_samples_w_gt0
          ? static_cast<double>(unlock_to_next_lock_cycles_w_gt0) /
                static_cast<double>(unlock_to_next_lock_samples_w_gt0)
          : 0.0;
  const double avg_lock_hold_ns = avg_lock_hold_cycles * ns_per_cycle;
  const double avg_unlock_to_next_lock_ns_w0 =
      avg_unlock_to_next_lock_cycles_w0 * ns_per_cycle;
  const double avg_unlock_to_next_lock_ns_w_gt0 =
      avg_unlock_to_next_lock_cycles_w_gt0 * ns_per_cycle;
  const uint64_t unlock_to_next_lock_samples_total =
      unlock_to_next_lock_samples_w0 + unlock_to_next_lock_samples_w_gt0;
  const double avg_unlock_to_next_lock_ns_all =
      unlock_to_next_lock_samples_total
          ? ((static_cast<double>(unlock_to_next_lock_samples_w0) *
              avg_unlock_to_next_lock_ns_w0) +
             (static_cast<double>(unlock_to_next_lock_samples_w_gt0) *
              avg_unlock_to_next_lock_ns_w_gt0)) /
                static_cast<double>(unlock_to_next_lock_samples_total)
          : 0.0;
  const double avg_waiters_before_lock =
      ops ? static_cast<double>(waiters_before_lock_total) /
                static_cast<double>(ops)
          : 0.0;

  std::cout << "=== Lock Benchmark ===\n";
  std::cout << "lock_kind: " << locks_bench::LockKindToString(cfg.lock_kind)
            << "\n";
  std::cout << "threads: " << cfg.threads << "\n";
  std::cout << "duration_ms: " << cfg.duration_ms << "\n";
  std::cout << "warmup_duration_ms: " << cfg.warmup_duration_ms << "\n";
  std::cout << "critical_iters: " << cfg.critical_iters << "\n";
  std::cout << "outside_iters: " << cfg.outside_iters << "\n";
  std::cout << "timing_sample_stride: " << cfg.timing_sample_stride << "\n";
  std::cout << "total_operations: " << ops << "\n";
  std::cout << "protected_counter: " << protected_counter << "\n";
  std::cout << std::fixed << std::setprecision(6);
  std::cout << "elapsed_seconds: " << elapsed_s << "\n";
  std::cout << std::setprecision(2);
  std::cout << "throughput_ops_per_sec: " << throughput << "\n";
  std::cout << "lock_hold_samples: " << lock_hold_samples << "\n";
  std::cout << "avg_lock_hold_ns: " << avg_lock_hold_ns << "\n";
  std::cout << "unlock_to_next_lock_samples_w0: "
            << unlock_to_next_lock_samples_w0 << "\n";
  std::cout << "avg_unlock_to_next_lock_ns_w0: "
            << avg_unlock_to_next_lock_ns_w0 << "\n";
  std::cout << "unlock_to_next_lock_samples_w_gt0: "
            << unlock_to_next_lock_samples_w_gt0 << "\n";
  std::cout << "avg_unlock_to_next_lock_ns_w_gt0: "
            << avg_unlock_to_next_lock_ns_w_gt0 << "\n";
  std::cout << "avg_unlock_to_next_lock_ns_all: "
            << avg_unlock_to_next_lock_ns_all << "\n";
  std::cout << "avg_waiters_before_lock: " << avg_waiters_before_lock << "\n";

  return 0;
}

int main(int argc, char *argv[]) {
  Config cfg = ParseArgs(argc, argv);
  return locks_bench::DispatchByLockKind(
      cfg.lock_kind, [&]<typename LockBenchT>() {
        return RunBenchmarkForLock<LockBenchT>(cfg);
      });
}
