#pragma once

#include "timeslice_extension.hpp"

#include "../../locks/clh.hpp"

namespace locks_bench {

struct ClhLockBench {
  using GuardState = ClhLock::LockState;

  explicit ClhLockBench(const LockBenchOptions &options = {})
      : timeslice_(options.timeslice_extension_mode) {}

  void prepare_thread() { timeslice_.prepare_thread(); }

  [[nodiscard]] GuardState lock() {
    GuardState state = lock_.lock();
    timeslice_.on_critical_section_enter();
    return state;
  }

  void unlock(GuardState &state) {
    lock_.unlock(state);
    timeslice_.on_critical_section_exit();
  }

private:
  CriticalSectionTimesliceExtension timeslice_;
  ClhLock lock_;
};

} // namespace locks_bench
