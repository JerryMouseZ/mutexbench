#pragma once

#include "timeslice_extension.hpp"

#include "../../locks/mcs_tas.hpp"

namespace locks_bench {

struct McsTasLockBench {
  using GuardState = McsTasLock::LockState;

  explicit McsTasLockBench(const LockBenchOptions &options = {})
      : timeslice_(options.timeslice_extension_mode) {}

  void prepare_thread() { timeslice_.prepare_thread(); }

  void set_sampling(bool enabled) { lock_.set_sampling(enabled); }

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
  McsTasLock lock_;
};

} // namespace locks_bench
