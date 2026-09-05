#pragma once

#include <concepts>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <pthread.h>
#include <string>
#include <utility>
#include <vector>

#include "lock_kind.hpp"
#include "mcs_tas_accordin_direct_lock_bench.hpp"

namespace locks_bench {

// A condition-variable backend owns one mutex plus a fixed number of condition
// variables sharing that mutex, so a workload can address them by index.
template <typename CondBenchT>
concept CondBench = requires(CondBenchT bench, size_t index) {
  { CondBenchT{index} };
  { bench.prepare_thread() } -> std::same_as<void>;
  { bench.lock() } -> std::same_as<void>;
  { bench.unlock() } -> std::same_as<void>;
  { bench.wait(index) } -> std::same_as<void>;
  { bench.signal(index) } -> std::same_as<void>;
  { bench.broadcast(index) } -> std::same_as<void>;
  { CondBenchT::name() } -> std::same_as<const char *>;
};

class PthreadCondBench {
public:
  explicit PthreadCondBench(size_t cond_count) : conds_(cond_count) {
    check(pthread_mutex_init(&mutex_, nullptr), "pthread_mutex_init");
    for (auto &slot : conds_) {
      check(pthread_cond_init(&slot.cond, nullptr), "pthread_cond_init");
    }
  }

  PthreadCondBench(const PthreadCondBench &) = delete;
  PthreadCondBench &operator=(const PthreadCondBench &) = delete;

  ~PthreadCondBench() {
    for (auto &slot : conds_) {
      pthread_cond_destroy(&slot.cond);
    }
    pthread_mutex_destroy(&mutex_);
  }

  static const char *name() { return "pthread"; }

  void prepare_thread() {}

  void lock() { check(pthread_mutex_lock(&mutex_), "pthread_mutex_lock"); }

  void unlock() { check(pthread_mutex_unlock(&mutex_), "pthread_mutex_unlock"); }

  void wait(size_t index) {
    check(pthread_cond_wait(&conds_[index].cond, &mutex_), "pthread_cond_wait");
  }

  void signal(size_t index) {
    check(pthread_cond_signal(&conds_[index].cond), "pthread_cond_signal");
  }

  void broadcast(size_t index) {
    check(pthread_cond_broadcast(&conds_[index].cond), "pthread_cond_broadcast");
  }

private:
  struct alignas(64) CondSlot {
    pthread_cond_t cond;
  };

  [[noreturn]] static void fail(const std::string &message) {
    std::cerr << "pthread_cond: " << message << "\n";
    std::exit(1);
  }

  static void check(int ret, const char *operation) {
    if (ret == 0) {
      return;
    }
    fail(std::string(operation) + " failed: ret=" + std::to_string(ret) + " (" +
         std::strerror(ret) + ")");
  }

  pthread_mutex_t mutex_;
  std::vector<CondSlot> conds_;
};

class McsTasAccordinDirectCondBench {
public:
  explicit McsTasAccordinDirectCondBench(size_t cond_count)
      : lib_(McsTasAccordinDirectLibrary::instance()), mutex_(lib_.create()) {
    if (!lib_.has_cond()) {
      McsTasAccordinDirectLibrary::fail(
          "the loaded library does not export mcs_tas_accordin_direct_cond_*; "
          "condition-variable workloads need a newer library build");
    }
    if (mutex_ == nullptr) {
      McsTasAccordinDirectLibrary::fail("mutex_create returned null");
    }
    conds_.reserve(cond_count);
    for (size_t i = 0; i < cond_count; ++i) {
      McsTasAccordinDirectLibrary::CondPtr cond = lib_.cond_create();
      if (cond == nullptr) {
        McsTasAccordinDirectLibrary::fail("cond_create returned null");
      }
      conds_.push_back(cond);
    }
  }

  McsTasAccordinDirectCondBench(const McsTasAccordinDirectCondBench &) = delete;
  McsTasAccordinDirectCondBench &
  operator=(const McsTasAccordinDirectCondBench &) = delete;

  ~McsTasAccordinDirectCondBench() {
    for (auto *cond : conds_) {
      lib_.cond_destroy(cond);
    }
    if (mutex_ != nullptr) {
      lib_.destroy(mutex_);
    }
  }

  static const char *name() { return "mcs_tas_accordin_direct"; }

  void prepare_thread() {}

  void lock() { check(lib_.lock(mutex_), "mutex_lock"); }

  void unlock() { check(lib_.unlock(mutex_), "mutex_unlock"); }

  void wait(size_t index) {
    check(lib_.cond_wait(conds_[index], mutex_), "cond_wait");
  }

  void signal(size_t index) {
    check(lib_.cond_signal(conds_[index]), "cond_signal");
  }

  void broadcast(size_t index) {
    check(lib_.cond_broadcast(conds_[index]), "cond_broadcast");
  }

private:
  static void check(int ret, const char *operation) {
    if (ret == 0) {
      return;
    }
    McsTasAccordinDirectLibrary::fail(std::string(operation) +
                                      " failed: ret=" + std::to_string(ret) +
                                      " (" + std::strerror(ret) + ")");
  }

  const McsTasAccordinDirectLibrary &lib_;
  McsTasAccordinDirectLibrary::MutexPtr mutex_;
  std::vector<McsTasAccordinDirectLibrary::CondPtr> conds_;
};

inline bool CondBenchSupportsLockKind(LockKind kind) {
  return kind == LockKind::kMutex || kind == LockKind::kMcsTasAccordinDirect;
}

inline const char *SupportedCondLockKinds() {
  return "mutex, mcs_tas_accordin_direct";
}

template <typename Fn>
decltype(auto) DispatchByCondLockKind(LockKind kind, Fn &&fn) {
  switch (kind) {
  case LockKind::kMutex:
    return std::forward<Fn>(fn).template operator()<PthreadCondBench>();
  case LockKind::kMcsTasAccordinDirect:
    return std::forward<Fn>(fn)
        .template operator()<McsTasAccordinDirectCondBench>();
  default:
    break;
  }
  std::abort();
}

} // namespace locks_bench
