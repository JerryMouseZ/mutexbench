#pragma once

#include <pthread.h>

#include <system_error>

#include "timeslice_extension.hpp"

namespace locks_bench {

struct PthreadSpinLockBench {
  struct GuardState {};

  explicit PthreadSpinLockBench(const LockBenchOptions &options = {})
      : timeslice_(options.timeslice_extension_mode) {
    const int ret = pthread_spin_init(&spin_, PTHREAD_PROCESS_PRIVATE);
    if (ret != 0) {
      throw std::system_error(ret, std::generic_category(),
                              "pthread_spin_init");
    }
  }

  ~PthreadSpinLockBench() { pthread_spin_destroy(&spin_); }

  PthreadSpinLockBench(const PthreadSpinLockBench &) = delete;
  PthreadSpinLockBench &operator=(const PthreadSpinLockBench &) = delete;

  void prepare_thread() { timeslice_.prepare_thread(); }

  [[nodiscard]] GuardState lock() {
    const int ret = pthread_spin_lock(&spin_);
    if (ret != 0) {
      throw std::system_error(ret, std::generic_category(),
                              "pthread_spin_lock");
    }
    timeslice_.on_critical_section_enter();
    return {};
  }

  void unlock(GuardState &) {
    timeslice_.on_critical_section_exit();
    const int ret = pthread_spin_unlock(&spin_);
    if (ret != 0) {
      throw std::system_error(ret, std::generic_category(),
                              "pthread_spin_unlock");
    }
  }

private:
  CriticalSectionTimesliceExtension timeslice_;
  pthread_spinlock_t spin_;
};

} // namespace locks_bench
