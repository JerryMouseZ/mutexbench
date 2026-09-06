#pragma once

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <pthread.h>
#include <string>
#include <vector>

#include "lock_kind.hpp"

namespace locks_bench {

// One pthread mutex plus a fixed number of condition variables sharing that
// mutex, addressed by index. Interposition decides which lock and condition
// variable implementation actually runs.
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

inline bool CondBenchSupportsLockKind(LockKind kind) {
  return kind == LockKind::kMutex;
}

inline const char *SupportedCondLockKinds() { return "mutex"; }

} // namespace locks_bench
