#pragma once

#include <mutex>

#include "timeslice_extension.hpp"

namespace locks_bench {

struct StdMutexLockBench {
  struct GuardState {};

  explicit StdMutexLockBench(const LockBenchOptions &options = {})
      : timeslice_(options.timeslice_extension_mode) {}

  void prepare_thread() { timeslice_.prepare_thread(); }

  [[nodiscard]] GuardState lock() {
    mu_.lock();
    timeslice_.on_critical_section_enter();
    return {};
  }

  void unlock(GuardState &) {
    mu_.unlock();
    timeslice_.on_critical_section_exit();
  }

private:
  CriticalSectionTimesliceExtension timeslice_;
  std::mutex mu_;
};

} // namespace locks_bench
