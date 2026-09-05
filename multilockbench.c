#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>

#if defined(__x86_64__) || defined(__i386__)
#include <emmintrin.h>
#include <immintrin.h>
#endif

#include "bench/burn_calibration.hpp"
#include "bench/locks_bench/lock_bench.hpp"
#include "bench/locks_bench/lock_dispatch.hpp"
#include "bench/locks_bench/lock_kind.hpp"

using Clock = std::chrono::steady_clock;

inline void SpinPause() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  _mm_pause();
#elif defined(__aarch64__) || defined(__arm__)
  asm volatile("yield" ::: "memory");
#else
  std::this_thread::yield();
#endif
}

enum class Workload { kZipfMultilock, kChain };

enum class WorkKind { kSpin, kNanosleep, kPread };

enum class PoolSelect { kUniform, kZipf };

const char *WorkloadToString(Workload workload) {
  switch (workload) {
    case Workload::kZipfMultilock:
      return "zipf-multilock";
    case Workload::kChain:
      return "chain";
  }
  return "unknown";
}

const char *WorkKindToString(WorkKind kind) {
  switch (kind) {
    case WorkKind::kSpin:
      return "spin";
    case WorkKind::kNanosleep:
      return "nanosleep";
    case WorkKind::kPread:
      return "pread";
  }
  return "unknown";
}

const char *PoolSelectToString(PoolSelect select) {
  switch (select) {
    case PoolSelect::kUniform:
      return "uniform";
    case PoolSelect::kZipf:
      return "zipf";
  }
  return "unknown";
}

struct Config {
  static constexpr uint64_t kDefaultBurnCalibrationNumerator = 9;
  static constexpr uint64_t kDefaultBurnCalibrationDenominator = 32;
  static constexpr uint64_t kDefaultChainCriticalNs = 150;

  int threads = 4;
  uint64_t lock_count = 16;
  uint64_t duration_ms = 1000;
  uint64_t warmup_duration_ms = 0;
  uint64_t critical_ns = 100;
  uint64_t outside_ns = 100;
  uint64_t timing_sample_stride = 8;
  uint64_t seed = 1;
  double zipf_alpha = 1.2;
  Workload workload = Workload::kZipfMultilock;
  uint64_t chain_hot_locks = 1;
  uint64_t chain_pool_a_locks = 17;
  uint64_t chain_pool_b_locks = 17;
  PoolSelect chain_select = PoolSelect::kUniform;
  WorkKind work_kind = WorkKind::kSpin;
  uint64_t work_ns = 30000;
  uint64_t work_file_mb = 64;
  std::string work_file_dir;
  bool sync_start = false;
  bool work_bimodal = false;
  uint64_t work_bimodal_pct = 0;
  uint64_t work_bimodal_short_ns = 0;
  uint64_t work_bimodal_long_ns = 0;
  bool critical_ns_explicit = false;
  bool work_ns_explicit = false;
  bool lock_count_explicit = false;
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
      << " [--threads N] [--locks N] [--zipf-alpha A] [--seed N]"
      << " [--duration-ms N] [--warmup-duration-ms N]"
      << " [--critical-ns N] [--outside-ns N]"
      << " [--timing-sample-stride N]"
      << " [--chain] [--workload zipf-multilock|chain]"
      << " [--chain-hot-locks N] [--chain-pool-a N] [--chain-pool-b N]"
      << " [--chain-select uniform|zipf]"
      << " [--work-kind spin|nanosleep|pread] [--work-ns N]"
      << " [--work-bimodal P,SHORT_NS,LONG_NS] [--sync-start]"
      << " [--work-file-dir PATH] [--work-file-mb N]"
      << " [--lock-kind mutex|pthread_spinlock|reciprocating|hapax|mcs|"
         "mcs_accordin_direct|mcs-tas|mcs-tas-tse|"
         "mcs_tas_accordin_direct|mcstas-next|mcstas-next-tse|twa|clh]"
      << " [--timeslice-extension off|auto|require]\n"
      << "  --threads N       Number of worker threads (default: 4)\n"
      << "  --locks N         Number of independent locks (default: 16)\n"
      << "  --num-locks N     Alias for --locks\n"
      << "  --zipf-alpha A    Zipf skew parameter; 0 is uniform "
         "(default: 1.2)\n"
      << "  --seed N          Base seed for per-thread lock selection "
         "(default: 1)\n"
      << "  --duration-ms N   Measurement duration in milliseconds "
         "(default: 1000)\n"
      << "  --warmup-duration-ms N  Warmup duration in milliseconds "
         "(default: 0)\n"
      << "  --critical-ns N   Requested critical-section burn time in "
         "nanoseconds (default: 100)\n"
      << "  --outside-ns N    Requested non-critical-section burn time in "
         "nanoseconds (default: 100)\n"
      << "  --critical-iters N  Legacy alias for --critical-ns\n"
      << "  --timing-sample-stride N  Measure timing every N ops "
         "(default: 8)\n"
      << "  --calibration-config PATH  Optional iter calibration config "
         "(default: <binary-dir>/iter_calibration.cfg)\n"
      << "  --chain           Alias for --workload chain\n"
      << "  --workload W      zipf-multilock|chain (default: zipf-multilock)\n"
      << "  --chain-hot-locks N  Hot lock pool size (default: 1)\n"
      << "  --chain-pool-a N  Pool A lock count (default: 17)\n"
      << "  --chain-pool-b N  Pool B lock count (default: 17)\n"
      << "  --chain-select S  Per-pool lock selection: uniform|zipf "
         "(default: uniform)\n"
      << "  --work-kind K     Work phase kind: spin|nanosleep|pread "
         "(default: spin)\n"
      << "  --work-ns N       Work phase duration in nanoseconds for spin and "
         "nanosleep; pread takes as long as the syscall (default: 30000)\n"
      << "  --work-bimodal P,SHORT_NS,LONG_NS  Draw the work phase duration "
         "per op: P percent of ops burn SHORT_NS, the rest LONG_NS; "
         "conflicts with --work-ns and with --work-kind pread\n"
      << "  --sync-start      Release all workers from a shared "
         "pthread condition variable instead of warming up; the warmup phase "
         "is skipped and the measurement clock starts at the broadcast\n"
      << "  --work-file-dir PATH  Directory for pread work files "
         "(default: $TMPDIR or /tmp)\n"
      << "  --work-file-mb N  Per-thread pread work file size in MiB "
         "(default: 64)\n"
      << "  --lock-kind K      Lock kind: "
         "mutex|pthread_spinlock|reciprocating|hapax|mcs|mcs_accordin_direct|"
         "mcs-tas|mcs-tas-tse|mcs_tas_accordin_direct|mcstas-next|"
         "mcstas-next-tse|twa|clh (default: mutex)\n"
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

double ParseDouble(const std::string &s, const char *flag) {
  errno = 0;
  char *end = nullptr;
  const double value = std::strtod(s.c_str(), &end);
  if (errno != 0 || end == s.c_str() || *end != '\0' ||
      !std::isfinite(value)) {
    std::cerr << "Invalid value for " << flag << ": " << s << "\n";
    std::exit(1);
  }
  return value;
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
    } else if (arg == "--locks" || arg == "--num-locks") {
      cfg.lock_count = ParseU64(need_next(arg.c_str()), arg.c_str());
      cfg.lock_count_explicit = true;
    } else if (arg == "--zipf-alpha") {
      cfg.zipf_alpha = ParseDouble(need_next("--zipf-alpha"), "--zipf-alpha");
    } else if (arg == "--seed") {
      cfg.seed = ParseU64(need_next("--seed"), "--seed");
    } else if (arg == "--duration-ms") {
      cfg.duration_ms = ParseU64(need_next("--duration-ms"), "--duration-ms");
    } else if (arg == "--warmup-duration-ms") {
      cfg.warmup_duration_ms =
          ParseU64(need_next("--warmup-duration-ms"), "--warmup-duration-ms");
    } else if (arg == "--critical-ns" || arg == "--critical-iters") {
      cfg.critical_ns = ParseU64(need_next("--critical-ns"), "--critical-ns");
      cfg.critical_ns_explicit = true;
    } else if (arg == "--outside-ns") {
      cfg.outside_ns = ParseU64(need_next("--outside-ns"), "--outside-ns");
    } else if (arg == "--timing-sample-stride") {
      cfg.timing_sample_stride = ParseU64(need_next("--timing-sample-stride"),
                                          "--timing-sample-stride");
    } else if (arg == "--calibration-config") {
      cfg.calibration_config_path = need_next("--calibration-config");
      cfg.calibration_config_explicit = true;
    } else if (arg == "--chain") {
      cfg.workload = Workload::kChain;
    } else if (arg == "--workload") {
      const std::string workload = need_next("--workload");
      if (workload == "zipf-multilock") {
        cfg.workload = Workload::kZipfMultilock;
      } else if (workload == "chain") {
        cfg.workload = Workload::kChain;
      } else {
        std::cerr << "Invalid value for --workload: " << workload
                  << " (expected: zipf-multilock or chain)\n";
        std::exit(1);
      }
    } else if (arg == "--chain-hot-locks") {
      cfg.chain_hot_locks =
          ParseU64(need_next("--chain-hot-locks"), "--chain-hot-locks");
    } else if (arg == "--chain-pool-a") {
      cfg.chain_pool_a_locks =
          ParseU64(need_next("--chain-pool-a"), "--chain-pool-a");
    } else if (arg == "--chain-pool-b") {
      cfg.chain_pool_b_locks =
          ParseU64(need_next("--chain-pool-b"), "--chain-pool-b");
    } else if (arg == "--chain-select") {
      const std::string select = need_next("--chain-select");
      if (select == "uniform") {
        cfg.chain_select = PoolSelect::kUniform;
      } else if (select == "zipf") {
        cfg.chain_select = PoolSelect::kZipf;
      } else {
        std::cerr << "Invalid value for --chain-select: " << select
                  << " (expected: uniform or zipf)\n";
        std::exit(1);
      }
    } else if (arg == "--work-kind") {
      const std::string kind = need_next("--work-kind");
      if (kind == "spin") {
        cfg.work_kind = WorkKind::kSpin;
      } else if (kind == "nanosleep") {
        cfg.work_kind = WorkKind::kNanosleep;
      } else if (kind == "pread") {
        cfg.work_kind = WorkKind::kPread;
      } else {
        std::cerr << "Invalid value for --work-kind: " << kind
                  << " (expected: spin, nanosleep, or pread)\n";
        std::exit(1);
      }
    } else if (arg == "--work-ns") {
      cfg.work_ns = ParseU64(need_next("--work-ns"), "--work-ns");
      cfg.work_ns_explicit = true;
    } else if (arg == "--work-bimodal") {
      const std::string spec = need_next("--work-bimodal");
      const size_t first = spec.find(',');
      const size_t second =
          first == std::string::npos ? std::string::npos
                                     : spec.find(',', first + 1);
      if (first == std::string::npos || second == std::string::npos) {
        std::cerr << "Invalid value for --work-bimodal: " << spec
                  << " (expected: P,SHORT_NS,LONG_NS)\n";
        std::exit(1);
      }
      cfg.work_bimodal_pct =
          ParseU64(spec.substr(0, first), "--work-bimodal");
      cfg.work_bimodal_short_ns =
          ParseU64(spec.substr(first + 1, second - first - 1),
                   "--work-bimodal");
      cfg.work_bimodal_long_ns =
          ParseU64(spec.substr(second + 1), "--work-bimodal");
      cfg.work_bimodal = true;
    } else if (arg == "--sync-start") {
      cfg.sync_start = true;
    } else if (arg == "--work-file-dir") {
      cfg.work_file_dir = need_next("--work-file-dir");
    } else if (arg == "--work-file-mb") {
      cfg.work_file_mb = ParseU64(need_next("--work-file-mb"), "--work-file-mb");
    } else if (arg == "--lock-kind") {
      const std::string lock_kind = need_next("--lock-kind");
      if (!locks_bench::TryParseLockKind(lock_kind, cfg.lock_kind)) {
        std::cerr << "Invalid value for --lock-kind: " << lock_kind
                  << " (expected: mutex, pthread_spinlock, reciprocating, "
                     "hapax, mcs, mcs_accordin_direct, mcs-tas, "
                     "mcs-tas-tse, mcs_tas_accordin_direct, mcstas-next, "
                     "mcstas-next-tse, twa, or clh)\n";
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
  if (cfg.lock_count == 0) {
    std::cerr << "--locks must be > 0\n";
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
  if (cfg.zipf_alpha < 0.0) {
    std::cerr << "--zipf-alpha must be >= 0\n";
    std::exit(1);
  }

  if (cfg.workload != Workload::kChain) {
    if (cfg.sync_start) {
      std::cerr << "--sync-start requires chain mode\n";
      std::exit(1);
    }
    if (cfg.work_bimodal) {
      std::cerr << "--work-bimodal requires chain mode\n";
      std::exit(1);
    }
  }

  if (cfg.workload == Workload::kChain) {
    if (cfg.work_bimodal) {
      if (cfg.work_ns_explicit) {
        std::cerr << "--work-bimodal and --work-ns are mutually exclusive\n";
        std::exit(1);
      }
      if (cfg.work_kind == WorkKind::kPread) {
        std::cerr << "--work-bimodal has no effect with --work-kind pread; "
                     "the read duration is set by the syscall\n";
        std::exit(1);
      }
      if (cfg.work_bimodal_pct > 100) {
        std::cerr << "--work-bimodal percentage must be <= 100\n";
        std::exit(1);
      }
    }
    if (cfg.chain_hot_locks == 0 || cfg.chain_pool_a_locks == 0 ||
        cfg.chain_pool_b_locks == 0) {
      std::cerr << "--chain-hot-locks, --chain-pool-a and --chain-pool-b "
                   "must be > 0\n";
      std::exit(1);
    }
    if (cfg.work_kind == WorkKind::kPread && cfg.work_file_mb == 0) {
      std::cerr << "--work-file-mb must be > 0 for --work-kind pread\n";
      std::exit(1);
    }
    if (cfg.lock_count_explicit) {
      std::cerr << "Warning: --locks is ignored in chain mode; the lock count "
                   "is derived from the pool sizes\n";
    }
    cfg.lock_count =
        cfg.chain_hot_locks + cfg.chain_pool_a_locks + cfg.chain_pool_b_locks;
    if (!cfg.critical_ns_explicit) {
      cfg.critical_ns = Config::kDefaultChainCriticalNs;
    }
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

uint64_t SplitMix64(uint64_t *state) {
  uint64_t z = (*state += 0x9e3779b97f4a7c15ULL);
  z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
  z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
  return z ^ (z >> 31);
}

uint64_t ThreadSeed(uint64_t seed, uint64_t thread_index) {
  uint64_t state = seed + (thread_index + 1) * 0x9e3779b97f4a7c15ULL;
  return SplitMix64(&state);
}

class FastRng {
public:
  explicit FastRng(uint64_t seed) : state_(seed == 0 ? 1 : seed) {}

  uint64_t NextU64() {
    uint64_t x = state_;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    state_ = x;
    return x * 2685821657736338717ULL;
  }

  double NextUnit() {
    return static_cast<double>(NextU64() >> 11) * 0x1.0p-53;
  }

private:
  uint64_t state_;
};

class ZipfDistribution {
public:
  ZipfDistribution(uint64_t lock_count, double alpha) {
    cdf_.reserve(static_cast<size_t>(lock_count));
    long double normalizer = 0.0L;
    for (uint64_t rank = 1; rank <= lock_count; ++rank) {
      normalizer += 1.0L / std::pow(static_cast<long double>(rank), alpha);
    }

    long double cumulative = 0.0L;
    for (uint64_t rank = 1; rank <= lock_count; ++rank) {
      cumulative += 1.0L / std::pow(static_cast<long double>(rank), alpha);
      cdf_.push_back(static_cast<double>(cumulative / normalizer));
    }
    cdf_.back() = 1.0;
  }

  size_t Sample(double unit_value) const {
    const auto it =
        std::lower_bound(cdf_.begin(), cdf_.end(), unit_value);
    if (it == cdf_.end()) {
      return cdf_.size() - 1;
    }
    return static_cast<size_t>(it - cdf_.begin());
  }

private:
  std::vector<double> cdf_;
};

struct Counters {
  std::atomic<uint64_t> total_lock_hold_ns{0};
  std::atomic<uint64_t> total_lock_hold_samples{0};
  std::atomic<uint64_t> total_thread_elapsed_ns{0};
};

constexpr size_t kPreadBlockBytes = 4096;
constexpr size_t kMaxOpLatencySamplesPerThread = 1u << 17;
constexpr size_t kMinTailPercentileSamples = 1000;

// Mirrors the db_bench SharedState handshake: every worker takes a shared
// mutex, registers itself, and parks on a shared condition variable until the
// coordinator sets the start predicate and broadcasts, at which point all
// workers enter the lock chain at once. The mutex and condition variable are
// raw pthread objects, so under LD_PRELOAD they are interposed like the chain
// locks and form their own lock class, as SharedState::mu does in db_bench.
class SyncStartBarrier {
public:
  explicit SyncStartBarrier(int total) : total_(total) {}

  void WorkerArriveAndWait() {
    pthread_mutex_lock(&mu_);
    ++num_initialized_;
    if (num_initialized_ >= total_) {
      pthread_cond_broadcast(&cv_);
    }
    while (!start_) {
      pthread_cond_wait(&cv_, &mu_);
    }
    pthread_mutex_unlock(&mu_);
  }

  Clock::time_point ReleaseAll() {
    pthread_mutex_lock(&mu_);
    while (num_initialized_ < total_) {
      pthread_cond_wait(&cv_, &mu_);
    }
    const auto broadcast_at = Clock::now();
    start_ = true;
    pthread_cond_broadcast(&cv_);
    pthread_mutex_unlock(&mu_);
    return broadcast_at;
  }

private:
  pthread_mutex_t mu_ = PTHREAD_MUTEX_INITIALIZER;
  pthread_cond_t cv_ = PTHREAD_COND_INITIALIZER;
  int total_;
  int num_initialized_ = 0;
  bool start_ = false;
};

// Reservoir sampling keeps the retained per-op latencies representative of the
// whole run once the per-thread sample budget is exhausted.
void RecordLatencySample(std::vector<uint64_t> *samples, uint64_t *seen,
                         FastRng *rng, uint64_t value) {
  ++(*seen);
  if (samples->size() < kMaxOpLatencySamplesPerThread) {
    samples->push_back(value);
    return;
  }
  const uint64_t slot = rng->NextU64() % *seen;
  if (slot < kMaxOpLatencySamplesPerThread) {
    (*samples)[static_cast<size_t>(slot)] = value;
  }
}

// One private, already-populated file per worker thread backs the pread work
// phase; the files are unlinked as soon as they are opened so that they vanish
// when the process exits.
class ChainWorkFiles {
public:
  ~ChainWorkFiles() {
    for (const int fd : fds_) {
      if (fd >= 0) {
        ::close(fd);
      }
    }
  }

  bool Create(const std::string &dir, uint64_t file_bytes, int threads) {
    file_bytes_ = file_bytes;
    directory_ = dir;
    std::vector<char> block(kBlockBytes, 0);
    for (int t = 0; t < threads; ++t) {
      std::string path = dir + "/multilockbench_work_XXXXXX";
      std::vector<char> path_buf(path.begin(), path.end());
      path_buf.push_back('\0');
      const int fd = ::mkstemp(path_buf.data());
      if (fd < 0) {
        std::cerr << "Failed to create work file in " << dir << ": "
                  << std::strerror(errno) << "\n";
        return false;
      }
      fds_.push_back(fd);
      if (::unlink(path_buf.data()) != 0) {
        std::cerr << "Failed to unlink work file " << path_buf.data() << ": "
                  << std::strerror(errno) << "\n";
        return false;
      }
      for (uint64_t offset = 0; offset < file_bytes; offset += kBlockBytes) {
        for (size_t i = 0; i < block.size(); ++i) {
          block[i] = static_cast<char>((offset + i + t) & 0xff);
        }
        const uint64_t chunk = std::min<uint64_t>(kBlockBytes,
                                                  file_bytes - offset);
        if (::pwrite(fd, block.data(), static_cast<size_t>(chunk),
                     static_cast<off_t>(offset)) !=
            static_cast<ssize_t>(chunk)) {
          std::cerr << "Failed to write work file: " << std::strerror(errno)
                    << "\n";
          return false;
        }
      }
    }
    return true;
  }

  int fd(size_t thread_index) const {
    return thread_index < fds_.size() ? fds_[thread_index] : -1;
  }

  uint64_t file_bytes() const { return file_bytes_; }
  const std::string &directory() const { return directory_; }

private:
  static constexpr size_t kBlockBytes = 1u << 20;

  std::vector<int> fds_;
  uint64_t file_bytes_ = 0;
  std::string directory_;
};

std::string WorkFileDirectory(const Config &cfg) {
  if (!cfg.work_file_dir.empty()) {
    return cfg.work_file_dir;
  }
  const char *tmpdir = std::getenv("TMPDIR");
  if (tmpdir != nullptr && tmpdir[0] != '\0') {
    return tmpdir;
  }
  return "/tmp";
}

double SafeDivide(double numerator, double denominator) {
  return denominator > 0.0 ? numerator / denominator : 0.0;
}

uint64_t Percentile(const std::vector<uint64_t> &sorted, double fraction) {
  if (sorted.empty()) {
    return 0;
  }
  const double position =
      fraction * static_cast<double>(sorted.size() - 1);
  const size_t index = static_cast<size_t>(position + 0.5);
  return sorted[std::min(index, sorted.size() - 1)];
}

void PrintCsv(const char *key, const std::vector<uint64_t> &values) {
  std::cout << key << ": ";
  for (size_t i = 0; i < values.size(); ++i) {
    if (i != 0) {
      std::cout << ",";
    }
    std::cout << values[i];
  }
  std::cout << "\n";
}

template <typename LockBenchT>
int RunBenchmarkForLock(const Config &cfg, const ChainWorkFiles &work_files) {
  static_assert(locks_bench::LockBench<LockBenchT>);

  std::vector<std::unique_ptr<LockBenchT>> locks;
  locks.reserve(static_cast<size_t>(cfg.lock_count));
  for (uint64_t i = 0; i < cfg.lock_count; ++i) {
    locks.push_back(std::make_unique<LockBenchT>(
        locks_bench::LockBenchOptions{cfg.timeslice_extension_mode}));
  }

  const bool chain = cfg.workload == Workload::kChain;
  const size_t hot_base = 0;
  const size_t pool_a_base = static_cast<size_t>(cfg.chain_hot_locks);
  const size_t pool_b_base =
      pool_a_base + static_cast<size_t>(cfg.chain_pool_a_locks);

  const ZipfDistribution zipf(cfg.lock_count, cfg.zipf_alpha);
  std::unique_ptr<ZipfDistribution> zipf_hot;
  std::unique_ptr<ZipfDistribution> zipf_pool_a;
  std::unique_ptr<ZipfDistribution> zipf_pool_b;
  if (chain && cfg.chain_select == PoolSelect::kZipf) {
    zipf_hot =
        std::make_unique<ZipfDistribution>(cfg.chain_hot_locks, cfg.zipf_alpha);
    zipf_pool_a = std::make_unique<ZipfDistribution>(cfg.chain_pool_a_locks,
                                                     cfg.zipf_alpha);
    zipf_pool_b = std::make_unique<ZipfDistribution>(cfg.chain_pool_b_locks,
                                                     cfg.zipf_alpha);
  }

  Counters counters;
  SyncStartBarrier sync_start_barrier(cfg.threads);
  std::atomic<uint64_t> pread_failures{0};
  std::atomic<int> workers_ready{0};
  std::atomic<int> warmup_done{0};
  std::atomic<bool> warmup_start{false};
  std::atomic<bool> warmup_stop{false};
  std::atomic<bool> measure_start{false};
  std::atomic<bool> measure_stop{false};
  std::vector<uint64_t> per_thread_ops(static_cast<size_t>(cfg.threads), 0);
  std::vector<uint64_t> per_thread_acquisitions(
      static_cast<size_t>(cfg.threads), 0);
  std::vector<std::vector<uint64_t>> per_thread_lock_ops(
      static_cast<size_t>(cfg.threads),
      std::vector<uint64_t>(static_cast<size_t>(cfg.lock_count), 0));
  std::vector<std::vector<uint64_t>> per_thread_op_latency_ns(
      static_cast<size_t>(cfg.threads));
  std::vector<uint64_t> per_thread_short_ops(static_cast<size_t>(cfg.threads),
                                             0);
  std::vector<uint64_t> per_thread_long_ops(static_cast<size_t>(cfg.threads),
                                            0);
  std::vector<Clock::time_point> per_thread_first_op(
      static_cast<size_t>(cfg.threads));

  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(cfg.threads));

  for (int t = 0; t < cfg.threads; ++t) {
    workers.emplace_back([&, thread_index = t]() {
      for (auto &lock_bench : locks) {
        lock_bench->prepare_thread();
      }

      FastRng rng(ThreadSeed(cfg.seed, static_cast<uint64_t>(thread_index)));
      // The work phase and the latency reservoir draw from their own streams so
      // that lock-selection sequences stay identical across work kinds.
      FastRng latency_rng(
          ThreadSeed(cfg.seed + 0x5bf03635ULL,
                     static_cast<uint64_t>(thread_index)));
      FastRng work_rng(
          ThreadSeed(cfg.seed + 0x2545f491ULL,
                     static_cast<uint64_t>(thread_index)));
      FastRng bimodal_rng(
          ThreadSeed(cfg.seed + 0x1b56c4e9ULL,
                     static_cast<uint64_t>(thread_index)));
      uint64_t local_lock_hold_ns = 0;
      uint64_t local_lock_hold_samples = 0;
      uint64_t local_ops = 0;
      uint64_t local_acquisitions = 0;
      uint64_t local_latency_seen = 0;
      uint64_t local_short_ops = 0;
      uint64_t local_long_ops = 0;
      auto &local_per_lock_ops =
          per_thread_lock_ops[static_cast<size_t>(thread_index)];
      auto &local_op_latency_ns =
          per_thread_op_latency_ns[static_cast<size_t>(thread_index)];

      const int work_fd = work_files.fd(static_cast<size_t>(thread_index));
      const uint64_t work_offset_span =
          work_files.file_bytes() > kPreadBlockBytes
              ? (work_files.file_bytes() - kPreadBlockBytes) / kPreadBlockBytes
              : 1;
      alignas(4096) char work_buffer[kPreadBlockBytes];

      auto select_in_pool = [&](size_t base, uint64_t pool_size,
                                const ZipfDistribution *pool_zipf) -> size_t {
        if (pool_size <= 1) {
          return base;
        }
        if (pool_zipf != nullptr) {
          return base + pool_zipf->Sample(rng.NextUnit());
        }
        return base + static_cast<size_t>(rng.NextU64() % pool_size);
      };

      auto chain_step = [&](size_t lock_index, bool do_timing_sample,
                            bool record) {
        auto &lock_bench = *locks[lock_index];
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
        if (!record) {
          return;
        }
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
        ++local_per_lock_ops[lock_index];
        ++local_acquisitions;
      };

      // The work duration comes from its own stream so that the lock-selection
      // sequence is seed-identical with and without bimodal durations.
      auto draw_work_ns = [&](bool record) -> uint64_t {
        if (!cfg.work_bimodal) {
          return cfg.work_ns;
        }
        const bool is_short =
            (bimodal_rng.NextU64() % 100) < cfg.work_bimodal_pct;
        if (record) {
          if (is_short) {
            ++local_short_ops;
          } else {
            ++local_long_ops;
          }
        }
        return is_short ? cfg.work_bimodal_short_ns : cfg.work_bimodal_long_ns;
      };

      auto chain_work = [&](uint64_t work_ns) {
        switch (cfg.work_kind) {
          case WorkKind::kSpin:
            BurnIters(work_ns, cfg.burn_calibration);
            break;
          case WorkKind::kNanosleep: {
            struct timespec deadline;
            if (clock_gettime(CLOCK_MONOTONIC, &deadline) != 0) {
              break;
            }
            deadline.tv_nsec += static_cast<long>(work_ns % 1000000000ULL);
            deadline.tv_sec +=
                static_cast<time_t>(work_ns / 1000000000ULL) +
                (deadline.tv_nsec >= 1000000000L ? 1 : 0);
            if (deadline.tv_nsec >= 1000000000L) {
              deadline.tv_nsec -= 1000000000L;
            }
            int rc = 0;
            do {
              rc = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &deadline,
                                   nullptr);
            } while (rc == EINTR);
            break;
          }
          case WorkKind::kPread: {
            const off_t offset = static_cast<off_t>(
                (work_rng.NextU64() % work_offset_span) * kPreadBlockBytes);
            ssize_t got = 0;
            do {
              got = ::pread(work_fd, work_buffer, kPreadBlockBytes, offset);
            } while (got < 0 && errno == EINTR);
            if (got != static_cast<ssize_t>(kPreadBlockBytes)) {
              pread_failures.fetch_add(1, std::memory_order_relaxed);
            }
            break;
          }
        }
      };

      auto chain_op = [&](bool do_timing_sample, bool record) {
        const size_t hot_index =
            select_in_pool(hot_base, cfg.chain_hot_locks, zipf_hot.get());
        const size_t a_index = select_in_pool(pool_a_base,
                                              cfg.chain_pool_a_locks,
                                              zipf_pool_a.get());
        const size_t b_index = select_in_pool(pool_b_base,
                                              cfg.chain_pool_b_locks,
                                              zipf_pool_b.get());
        const uint64_t work_ns = draw_work_ns(record);
        chain_step(hot_index, do_timing_sample, record);
        chain_step(a_index, do_timing_sample, record);
        chain_step(b_index, do_timing_sample, record);
        chain_work(work_ns);
        chain_step(b_index, do_timing_sample, record);
        chain_step(a_index, do_timing_sample, record);
        chain_step(hot_index, do_timing_sample, record);
      };

      if (cfg.sync_start) {
        sync_start_barrier.WorkerArriveAndWait();
      } else {
        workers_ready.fetch_add(1, std::memory_order_release);
        while (!warmup_start.load(std::memory_order_acquire)) {
          SpinPause();
        }

        if (cfg.warmup_duration_ms > 0) {
          while (!warmup_stop.load(std::memory_order_acquire)) {
            if (chain) {
              chain_op(/*do_timing_sample=*/false, /*record=*/false);
              BurnIters(cfg.outside_ns, cfg.burn_calibration);
              continue;
            }
            const size_t lock_index = zipf.Sample(rng.NextUnit());
            auto &lock_bench = *locks[lock_index];
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
      }

      const auto thread_measure_start = Clock::now();
      per_thread_first_op[static_cast<size_t>(thread_index)] =
          thread_measure_start;
      uint64_t sample_countdown =
          static_cast<uint64_t>(thread_index) % cfg.timing_sample_stride;
      while (!measure_stop.load(std::memory_order_acquire)) {
        const bool do_timing_sample = (sample_countdown == 0);
        if (sample_countdown == 0) {
          sample_countdown = cfg.timing_sample_stride - 1;
        } else {
          --sample_countdown;
        }

        if (chain) {
          // Op latency is sampled on every op, not on the hold-sampling
          // stride, so that percentiles stay meaningful when throughput
          // collapses to a few hundred ops per second.
          const auto op_start = Clock::now();
          chain_op(do_timing_sample, /*record=*/true);
          const auto op_ns =
              std::chrono::duration_cast<std::chrono::nanoseconds>(
                  Clock::now() - op_start)
                  .count();
          if (op_ns >= 0) {
            RecordLatencySample(&local_op_latency_ns, &local_latency_seen,
                                &latency_rng, static_cast<uint64_t>(op_ns));
          }
          BurnIters(cfg.outside_ns, cfg.burn_calibration);
          ++local_ops;
          continue;
        }

        Clock::time_point after_lock;
        Clock::time_point before_unlock;
        const size_t lock_index = zipf.Sample(rng.NextUnit());
        auto &lock_bench = *locks[lock_index];

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
        ++local_acquisitions;
        ++local_per_lock_ops[lock_index];
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
      per_thread_acquisitions[static_cast<size_t>(thread_index)] =
          local_acquisitions;
      per_thread_short_ops[static_cast<size_t>(thread_index)] =
          local_short_ops;
      per_thread_long_ops[static_cast<size_t>(thread_index)] = local_long_ops;
    });
  }

  Clock::time_point start;
  if (cfg.sync_start) {
    // No warmup: the workers park on the shared condition variable and the
    // measurement clock starts at the broadcast that releases them.
    start = sync_start_barrier.ReleaseAll();
  } else {
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

    start = Clock::now();
    measure_start.store(true, std::memory_order_release);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(cfg.duration_ms));
  measure_stop.store(true, std::memory_order_release);

  for (auto &th : workers) {
    th.join();
  }
  const auto end = Clock::now();

  std::vector<uint64_t> per_lock_ops(static_cast<size_t>(cfg.lock_count), 0);
  uint64_t ops = 0;
  uint64_t acquisitions = 0;
  std::vector<uint64_t> op_latency_ns;
  for (size_t thread_index = 0; thread_index < per_thread_ops.size();
       ++thread_index) {
    ops += per_thread_ops[thread_index];
    acquisitions += per_thread_acquisitions[thread_index];
    const auto &thread_latencies = per_thread_op_latency_ns[thread_index];
    op_latency_ns.insert(op_latency_ns.end(), thread_latencies.begin(),
                         thread_latencies.end());
    for (size_t lock_index = 0; lock_index < per_lock_ops.size();
         ++lock_index) {
      per_lock_ops[lock_index] += per_thread_lock_ops[thread_index][lock_index];
    }
  }
  std::sort(op_latency_ns.begin(), op_latency_ns.end());
  uint64_t short_ops = 0;
  uint64_t long_ops = 0;
  for (size_t thread_index = 0; thread_index < per_thread_ops.size();
       ++thread_index) {
    short_ops += per_thread_short_ops[thread_index];
    long_ops += per_thread_long_ops[thread_index];
  }
  const auto first_op_bounds = std::minmax_element(
      per_thread_first_op.begin(), per_thread_first_op.end());
  const int64_t first_op_spread_ns =
      per_thread_first_op.empty()
          ? 0
          : std::chrono::duration_cast<std::chrono::nanoseconds>(
                *first_op_bounds.second - *first_op_bounds.first)
                .count();
  const int64_t last_wake_delay_ns =
      per_thread_first_op.empty()
          ? 0
          : std::chrono::duration_cast<std::chrono::nanoseconds>(
                *first_op_bounds.second - start)
                .count();
  const size_t distinct_locks_touched = static_cast<size_t>(
      std::count_if(per_lock_ops.begin(), per_lock_ops.end(),
                    [](uint64_t count) { return count > 0; }));

  const double elapsed_s =
      std::chrono::duration_cast<std::chrono::duration<double>>(end - start)
          .count();
  const uint64_t lock_hold_ns =
      counters.total_lock_hold_ns.load(std::memory_order_relaxed);
  const uint64_t lock_hold_samples =
      counters.total_lock_hold_samples.load(std::memory_order_relaxed);
  const uint64_t thread_elapsed_ns_total =
      counters.total_thread_elapsed_ns.load(std::memory_order_relaxed);
  const double throughput = SafeDivide(static_cast<double>(ops), elapsed_s);
  const double avg_lock_hold_ns =
      lock_hold_samples
          ? SafeDivide(static_cast<double>(lock_hold_ns),
                       static_cast<double>(lock_hold_samples))
          : 0.0;
  const double estimated_total_lock_hold_ns =
      avg_lock_hold_ns * static_cast<double>(acquisitions);
  // Outside chain mode this is the classic wait estimate: one acquisition per
  // op, so everything not spent holding a lock is contention plus the outside
  // burn. In chain mode the same residual also covers the lock-free work
  // phase, so it is reported under a name that does not claim to be wait time.
  const double avg_non_hold_ns_per_op =
      ops ? SafeDivide(std::max(static_cast<double>(thread_elapsed_ns_total) -
                                    estimated_total_lock_hold_ns,
                                0.0),
                       static_cast<double>(ops))
          : 0.0;
  const auto hotspot_it =
      std::max_element(per_lock_ops.begin(), per_lock_ops.end());
  const size_t hotspot_lock =
      static_cast<size_t>(hotspot_it - per_lock_ops.begin());
  const uint64_t hotspot_ops =
      hotspot_it != per_lock_ops.end() ? *hotspot_it : 0;
  const double hotspot_pct =
      SafeDivide(static_cast<double>(hotspot_ops) * 100.0,
                 static_cast<double>(acquisitions));

  std::cout << "workload: " << WorkloadToString(cfg.workload) << "\n";
  std::cout << "threads: " << cfg.threads << "\n";
  std::cout << "locks: " << cfg.lock_count << "\n";
  if (chain) {
    std::cout << "chain_hot_locks: " << cfg.chain_hot_locks << "\n";
    std::cout << "chain_pool_a_locks: " << cfg.chain_pool_a_locks << "\n";
    std::cout << "chain_pool_b_locks: " << cfg.chain_pool_b_locks << "\n";
    std::cout << "chain_select: " << PoolSelectToString(cfg.chain_select)
              << "\n";
    std::cout << "work_kind: " << WorkKindToString(cfg.work_kind) << "\n";
    if (cfg.work_bimodal) {
      std::cout << "work_bimodal_pct: " << cfg.work_bimodal_pct << "\n";
      std::cout << "work_bimodal_short_ns: " << cfg.work_bimodal_short_ns
                << "\n";
      std::cout << "work_bimodal_long_ns: " << cfg.work_bimodal_long_ns << "\n";
    } else {
      std::cout << "work_ns: " << cfg.work_ns << "\n";
    }
    if (cfg.sync_start) {
      std::cout << "sync_start: 1\n";
      std::cout << "warmup_skipped: 1\n";
    }
    if (cfg.work_kind == WorkKind::kPread) {
      std::cout << "work_file_mb: " << cfg.work_file_mb << "\n";
      std::cout << "work_total_mb: "
                << cfg.work_file_mb * static_cast<uint64_t>(cfg.threads)
                << "\n";
      std::cout << "work_file_dir: " << work_files.directory() << "\n";
      std::cout << "work_pread_failures: "
                << pread_failures.load(std::memory_order_relaxed) << "\n";
    }
  }
  std::cout << std::fixed << std::setprecision(6);
  std::cout << "zipf_alpha: " << cfg.zipf_alpha << "\n";
  std::cout << std::defaultfloat;
  std::cout << "seed: " << cfg.seed << "\n";
  std::cout << "critical_ns: " << cfg.critical_ns << "\n";
  std::cout << "outside_ns: " << cfg.outside_ns << "\n";
  std::cout << "lock_kind: " << locks_bench::LockKindToString(cfg.lock_kind)
            << "\n";
  std::cout << "timeslice_extension: "
            << locks_bench::TimesliceExtensionModeToString(
                   cfg.timeslice_extension_mode)
            << "\n";
  std::cout << "burn_calibration: "
            << burn_calibration::ToString(cfg.burn_calibration) << "\n";
  std::cout << "burn_calibration_source: " << cfg.burn_calibration_source
            << "\n";
  std::cout << "total_operations: " << ops << "\n";
  std::cout << "total_lock_acquisitions: " << acquisitions << "\n";
  std::cout << "distinct_locks_touched: " << distinct_locks_touched << "\n";
  PrintCsv("per_thread_operations", per_thread_ops);
  PrintCsv("per_lock_operations", per_lock_ops);
  std::cout << std::fixed << std::setprecision(6);
  std::cout << "elapsed_seconds: " << elapsed_s << "\n";
  std::cout << std::setprecision(2);
  std::cout << "throughput_ops_per_sec: " << throughput << "\n";
  std::cout << "lock_hold_samples: " << lock_hold_samples << "\n";
  std::cout << "avg_lock_hold_ns: " << avg_lock_hold_ns << "\n";
  if (chain) {
    std::cout << "avg_non_hold_ns_per_op: " << avg_non_hold_ns_per_op << "\n";
  } else {
    std::cout << "avg_wait_ns_estimated: " << avg_non_hold_ns_per_op << "\n";
  }
  std::cout << "hotspot_lock: " << hotspot_lock << "\n";
  std::cout << "hotspot_lock_operations: " << hotspot_ops << "\n";
  std::cout << "hotspot_lock_operation_pct: " << hotspot_pct << "\n";
  if (chain) {
    std::cout << "op_latency_samples: " << op_latency_ns.size() << "\n";
    std::cout << "op_latency_p50_ns: " << Percentile(op_latency_ns, 0.50)
              << "\n";
    // Tail percentiles degenerate to the maximum when the sample count is
    // small, so they are reported as unavailable rather than as a number.
    if (op_latency_ns.size() >= kMinTailPercentileSamples) {
      std::cout << "op_latency_p99_ns: " << Percentile(op_latency_ns, 0.99)
                << "\n";
      std::cout << "op_latency_p999_ns: " << Percentile(op_latency_ns, 0.999)
                << "\n";
    } else {
      std::cout << "op_latency_p99_ns: insufficient_samples\n";
      std::cout << "op_latency_p999_ns: insufficient_samples\n";
    }
    if (cfg.work_bimodal) {
      std::cout << "work_short_ops: " << short_ops << "\n";
      std::cout << "work_long_ops: " << long_ops << "\n";
      std::cout << "work_short_op_pct: "
                << SafeDivide(static_cast<double>(short_ops) * 100.0,
                              static_cast<double>(short_ops + long_ops))
                << "\n";
    }
    if (cfg.sync_start) {
      std::cout << "sync_start_first_op_spread_ns: " << first_op_spread_ns
                << "\n";
      std::cout << "sync_start_last_wake_delay_ns: " << last_wake_delay_ns
                << "\n";
    }
  }
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

  ChainWorkFiles work_files;
  if (cfg.workload == Workload::kChain &&
      cfg.work_kind == WorkKind::kPread) {
    if (!work_files.Create(WorkFileDirectory(cfg),
                           cfg.work_file_mb * (1ULL << 20), cfg.threads)) {
      return 1;
    }
  }

  return locks_bench::DispatchByLockKind(
      cfg.lock_kind, [&]<typename LockBenchT>() {
        return RunBenchmarkForLock<LockBenchT>(cfg, work_files);
      });
}
