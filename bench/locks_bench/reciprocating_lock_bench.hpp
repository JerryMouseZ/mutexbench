#pragma once

#include "timeslice_extension.hpp"

#include "../../locks/reciprocating.hpp"

namespace locks_bench {

struct ReciprocatingLockBench {
  struct GuardState {};

  explicit ReciprocatingLockBench(const LockBenchOptions &options = {})
      : timeslice_(options.timeslice_extension_mode) {}

  void prepare_thread() { timeslice_.prepare_thread(); }

  [[nodiscard]] GuardState lock() {
    lock_.lock();
    timeslice_.on_critical_section_enter();
    return {};
  }

  void unlock(GuardState &) {
    lock_.unlock();
    timeslice_.on_critical_section_exit();
  }

private:
  CriticalSectionTimesliceExtension timeslice_;
  ReciprocatingLock lock_;
};

} // namespace locks_bench
