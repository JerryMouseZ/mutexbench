#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>

using Clock = std::chrono::steady_clock;

struct Config {
  uint64_t min_iters = 0;
  uint64_t max_iters = 10000;
  uint64_t step_iters = 100;
  uint64_t batch = 10000;
  uint64_t repeats = 20;
  uint64_t warmup_batches = 5;
};

[[noreturn]] void PrintUsageAndExit(const char* prog) {
  std::cerr << "Usage: " << prog
            << " [--min-iters N] [--max-iters N] [--step-iters N]"
               " [--batch N] [--repeats N] [--warmup-batches N]\n"
            << "  --min-iters N      Min loop iterations (default: 0)\n"
            << "  --max-iters N      Max loop iterations (default: 10000)\n"
            << "  --step-iters N     Step size on iterations axis (default: 100)\n"
            << "  --batch N          Calls per timing batch (default: 10000)\n"
            << "  --repeats N        Timed batches per point (default: 20)\n"
            << "  --warmup-batches N Warmup batches before timing (default: 5)\n";
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

    if (arg == "--min-iters") {
      cfg.min_iters = ParseU64(need_next("--min-iters"), "--min-iters");
    } else if (arg == "--max-iters") {
      cfg.max_iters = ParseU64(need_next("--max-iters"), "--max-iters");
    } else if (arg == "--step-iters") {
      cfg.step_iters = ParseU64(need_next("--step-iters"), "--step-iters");
    } else if (arg == "--batch") {
      cfg.batch = ParseU64(need_next("--batch"), "--batch");
    } else if (arg == "--repeats") {
      cfg.repeats = ParseU64(need_next("--repeats"), "--repeats");
    } else if (arg == "--warmup-batches") {
      cfg.warmup_batches = ParseU64(need_next("--warmup-batches"), "--warmup-batches");
    } else if (arg == "--help" || arg == "-h") {
      PrintUsageAndExit(argv[0]);
    } else {
      std::cerr << "Unknown argument: " << arg << "\n";
      PrintUsageAndExit(argv[0]);
    }
  }

  if (cfg.step_iters == 0 || cfg.batch == 0 || cfg.repeats == 0) {
    std::cerr << "--step-iters, --batch, --repeats must be > 0\n";
    std::exit(1);
  }
  if (cfg.min_iters > cfg.max_iters) {
    std::cerr << "--min-iters must be <= --max-iters\n";
    std::exit(1);
  }
  return cfg;
}

volatile uint64_t g_sink = 0;

inline void BurnIters(uint64_t iters) {
  uint64_t x = g_sink;
  for (uint64_t i = 0; i < iters; ++i) {
    x = (x * 1664525u) + 1013904223u + i;
  }
  g_sink = x;
}

struct Point {
  uint64_t iters = 0;
  double avg_batch_ns = 0.0;
  double min_batch_ns = 0.0;
  double max_batch_ns = 0.0;
  double avg_call_ns = 0.0;
};

Point MeasurePoint(uint64_t iters, const Config& cfg) {
  for (uint64_t w = 0; w < cfg.warmup_batches; ++w) {
    for (uint64_t i = 0; i < cfg.batch; ++i) {
      BurnIters(iters);
    }
  }

  double total_ns = 0.0;
  double min_ns = 0.0;
  double max_ns = 0.0;
  for (uint64_t r = 0; r < cfg.repeats; ++r) {
    const auto start = Clock::now();
    for (uint64_t i = 0; i < cfg.batch; ++i) {
      BurnIters(iters);
    }
    const auto end = Clock::now();
    const double ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    total_ns += ns;
    if (r == 0) {
      min_ns = ns;
      max_ns = ns;
    } else {
      min_ns = std::min(min_ns, ns);
      max_ns = std::max(max_ns, ns);
    }
  }

  Point p;
  p.iters = iters;
  p.avg_batch_ns = total_ns / static_cast<double>(cfg.repeats);
  p.min_batch_ns = min_ns;
  p.max_batch_ns = max_ns;
  p.avg_call_ns = p.avg_batch_ns / static_cast<double>(cfg.batch);
  return p;
}

int main(int argc, char* argv[]) {
  const Config cfg = ParseArgs(argc, argv);

  std::cerr << "Measuring curve with min_iters=" << cfg.min_iters
            << ", max_iters=" << cfg.max_iters << ", step_iters=" << cfg.step_iters
            << ", batch=" << cfg.batch << ", repeats=" << cfg.repeats
            << ", warmup_batches=" << cfg.warmup_batches << "\n";

  std::cout << "iters,avg_batch_ns,min_batch_ns,max_batch_ns,avg_call_ns\n";
  for (uint64_t iters = cfg.min_iters; iters <= cfg.max_iters; iters += cfg.step_iters) {
    const Point p = MeasurePoint(iters, cfg);
    std::cout << p.iters << "," << p.avg_batch_ns << "," << p.min_batch_ns << ","
              << p.max_batch_ns << "," << p.avg_call_ns << "\n";
    if (cfg.max_iters - iters < cfg.step_iters) {
      break;
    }
  }
  return 0;
}
