#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

inline uint64_t ReadTsc() {
#if defined(__x86_64__) || defined(__i386__)
  _mm_lfence();
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
  uint64_t iterations_per_thread = 1'000'000;
  uint64_t warmup_iterations_per_thread = 0;
  uint64_t critical_iters = 100;
  uint64_t outside_iters = 100;
};

[[noreturn]] void PrintUsageAndExit(const char* prog) {
  std::cerr
      << "Usage: " << prog
      << " [--threads N] [--iterations N] [--warmup-iterations N]"
      << " [--critical-iters N] [--outside-iters N]\n"
      << "  --threads N       Number of worker threads (default: 4)\n"
      << "  --iterations N    Iterations per thread (default: 1000000)\n"
      << "  --warmup-iterations N  Warmup iterations per thread (default: 0)\n"
      << "  --critical-iters N  Loop iterations in critical section (default: 100)\n"
      << "  --outside-iters N   Loop iterations outside lock (default: 100)\n";
  std::exit(1);
}

uint64_t ParseU64(const std::string& s, const char* flag) {
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

Config ParseArgs(int argc, char* argv[]) {
  Config cfg;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    auto need_next = [&](const char* flag) -> std::string {
      if (i + 1 >= argc) {
        std::cerr << "Missing value for " << flag << "\n";
        PrintUsageAndExit(argv[0]);
      }
      return argv[++i];
    };

    if (arg == "--threads") {
      cfg.threads = static_cast<int>(ParseU64(need_next("--threads"), "--threads"));
    } else if (arg == "--iterations") {
      cfg.iterations_per_thread = ParseU64(need_next("--iterations"), "--iterations");
    } else if (arg == "--warmup-iterations") {
      cfg.warmup_iterations_per_thread =
          ParseU64(need_next("--warmup-iterations"), "--warmup-iterations");
    } else if (arg == "--critical-iters") {
      cfg.critical_iters = ParseU64(need_next("--critical-iters"), "--critical-iters");
    } else if (arg == "--outside-iters") {
      cfg.outside_iters = ParseU64(need_next("--outside-iters"), "--outside-iters");
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
  return cfg;
}

inline void BurnIters(uint64_t iters) {
  volatile uint64_t x = 0;
  for (uint64_t i = 0; i < iters; ++i) {
    x = (x * 1664525u) + 1013904223u + i;
  }
}

int main(int argc, char* argv[]) {
  Config cfg = ParseArgs(argc, argv);

  std::mutex mu;
  uint64_t protected_counter = 0;
  std::atomic<uint64_t> total_ops{0};
  std::atomic<uint64_t> total_lock_hold_cycles{0};
  std::atomic<uint64_t> total_unlock_to_next_lock_cycles{0};
  std::atomic<uint64_t> total_unlock_to_next_lock_samples{0};
  std::atomic<int> warmup_done{0};
  std::atomic<bool> measure_start{false};

  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(cfg.threads));

  for (int t = 0; t < cfg.threads; ++t) {
    workers.emplace_back([&]() {
      uint64_t local_lock_hold_cycles = 0;
      uint64_t local_unlock_to_next_lock_cycles = 0;
      uint64_t local_unlock_to_next_lock_samples = 0;

      for (uint64_t i = 0; i < cfg.warmup_iterations_per_thread; ++i) {
        {
          std::lock_guard<std::mutex> lock(mu);
          BurnIters(cfg.critical_iters);
        }
        BurnIters(cfg.outside_iters);
      }

      warmup_done.fetch_add(1, std::memory_order_release);
      while (!measure_start.load(std::memory_order_acquire)) {
      }

      uint64_t last_before_unlock = 0;
      bool has_last_before_unlock = false;

      for (uint64_t i = 0; i < cfg.iterations_per_thread; ++i) {
        {
          std::lock_guard<std::mutex> lock(mu);
          const uint64_t after_lock = ReadTsc();
          if (has_last_before_unlock) {
            local_unlock_to_next_lock_cycles += (after_lock - last_before_unlock);
            ++local_unlock_to_next_lock_samples;
          }
          BurnIters(cfg.critical_iters);
          ++protected_counter;
          const uint64_t before_unlock = ReadTsc();
          local_lock_hold_cycles += (before_unlock - after_lock);
          last_before_unlock = before_unlock;
          has_last_before_unlock = true;
        }
        BurnIters(cfg.outside_iters);
        total_ops.fetch_add(1, std::memory_order_relaxed);
      }

      total_lock_hold_cycles.fetch_add(local_lock_hold_cycles, std::memory_order_relaxed);
      total_unlock_to_next_lock_cycles.fetch_add(local_unlock_to_next_lock_cycles,
                                                 std::memory_order_relaxed);
      total_unlock_to_next_lock_samples.fetch_add(local_unlock_to_next_lock_samples,
                                                  std::memory_order_relaxed);
    });
  }

  while (warmup_done.load(std::memory_order_acquire) < cfg.threads) {
  }
  const auto start = Clock::now();
  const uint64_t tsc_start = ReadTsc();
  measure_start.store(true, std::memory_order_release);

  for (auto& th : workers) {
    th.join();
  }
  const uint64_t tsc_end = ReadTsc();
  const auto end = Clock::now();

  const double elapsed_s =
      std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();
  const double elapsed_ns = std::chrono::duration_cast<std::chrono::duration<double, std::nano>>(
                                end - start)
                                .count();
  const uint64_t elapsed_cycles = (tsc_end > tsc_start) ? (tsc_end - tsc_start) : 0;
  const double ns_per_cycle =
      elapsed_cycles ? (elapsed_ns / static_cast<double>(elapsed_cycles)) : 0.0;
  const uint64_t ops = total_ops.load(std::memory_order_relaxed);
  const uint64_t lock_hold_cycles = total_lock_hold_cycles.load(std::memory_order_relaxed);
  const uint64_t unlock_to_next_lock_cycles =
      total_unlock_to_next_lock_cycles.load(std::memory_order_relaxed);
  const uint64_t unlock_to_next_lock_samples =
      total_unlock_to_next_lock_samples.load(std::memory_order_relaxed);
  const double throughput = ops / elapsed_s;
  const double avg_lock_hold_cycles =
      ops ? static_cast<double>(lock_hold_cycles) / static_cast<double>(ops) : 0.0;
  const double avg_unlock_to_next_lock_cycles =
      unlock_to_next_lock_samples
          ? static_cast<double>(unlock_to_next_lock_cycles) /
                static_cast<double>(unlock_to_next_lock_samples)
          : 0.0;
  const double avg_lock_hold_ns = avg_lock_hold_cycles * ns_per_cycle;
  const double avg_unlock_to_next_lock_ns =
      avg_unlock_to_next_lock_cycles * ns_per_cycle;

  std::cout << "=== Mutex Benchmark ===\n";
  std::cout << "threads: " << cfg.threads << "\n";
  std::cout << "iterations_per_thread: " << cfg.iterations_per_thread << "\n";
  std::cout << "warmup_iterations_per_thread: " << cfg.warmup_iterations_per_thread
            << "\n";
  std::cout << "critical_iters: " << cfg.critical_iters << "\n";
  std::cout << "outside_iters: " << cfg.outside_iters << "\n";
  std::cout << "total_operations: " << ops << "\n";
  std::cout << "protected_counter: " << protected_counter << "\n";
  std::cout << std::fixed << std::setprecision(6);
  std::cout << "elapsed_seconds: " << elapsed_s << "\n";
  std::cout << std::setprecision(2);
  std::cout << "throughput_ops_per_sec: " << throughput << "\n";
  std::cout << "avg_lock_hold_ns: " << avg_lock_hold_ns << "\n";
  std::cout << "avg_unlock_to_next_lock_ns: " << avg_unlock_to_next_lock_ns << "\n";

  return 0;
}
