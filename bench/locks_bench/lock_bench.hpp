#pragma once

#include <concepts>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <pthread.h>
#include <string>

namespace locks_bench {

template <typename LockBenchT>
concept LockBench = requires(LockBenchT lock) {
  { LockBenchT{} };
  { lock.lock() } -> std::same_as<void>;
  { lock.unlock() } -> std::same_as<void>;
};

[[noreturn]] inline void FailLockCall(const char *operation, int ret) {
  std::cerr << "lock_bench: " << operation << " failed: ret=" << ret << " ("
            << std::strerror(ret) << ")\n";
  std::exit(1);
}

inline void CheckLockCall(int ret, const char *operation) {
  if (ret != 0) {
    FailLockCall(operation, ret);
  }
}

// A plain pthread_mutex_t. Which algorithm is measured is decided entirely by
// the LiTL library preloaded into the process.
class PthreadMutexLockBench {
public:
  PthreadMutexLockBench() {
    CheckLockCall(pthread_mutex_init(&mutex_, nullptr), "pthread_mutex_init");
  }

  ~PthreadMutexLockBench() { pthread_mutex_destroy(&mutex_); }

  PthreadMutexLockBench(const PthreadMutexLockBench &) = delete;
  PthreadMutexLockBench &operator=(const PthreadMutexLockBench &) = delete;

  void lock() {
    CheckLockCall(pthread_mutex_lock(&mutex_), "pthread_mutex_lock");
  }

  void unlock() {
    CheckLockCall(pthread_mutex_unlock(&mutex_), "pthread_mutex_unlock");
  }

private:
  pthread_mutex_t mutex_;
};

// A native pthread_spinlock_t control arm that interposition leaves alone.
class PthreadSpinLockBench {
public:
  PthreadSpinLockBench() {
    CheckLockCall(pthread_spin_init(&spin_, PTHREAD_PROCESS_PRIVATE),
                  "pthread_spin_init");
  }

  ~PthreadSpinLockBench() { pthread_spin_destroy(&spin_); }

  PthreadSpinLockBench(const PthreadSpinLockBench &) = delete;
  PthreadSpinLockBench &operator=(const PthreadSpinLockBench &) = delete;

  void lock() { CheckLockCall(pthread_spin_lock(&spin_), "pthread_spin_lock"); }

  void unlock() {
    CheckLockCall(pthread_spin_unlock(&spin_), "pthread_spin_unlock");
  }

private:
  pthread_spinlock_t spin_;
};

} // namespace locks_bench
