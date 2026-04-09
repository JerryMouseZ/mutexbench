#pragma once

#include "timeslice_extension.hpp"

#include "../../locks/mcs_tas_tse.hpp"

namespace locks_bench {

struct McsTasTseLockBench {
  using GuardState = McsTasTseLock::LockState;

  explicit McsTasTseLockBench(const LockBenchOptions & = {}) {}

  void prepare_thread() { lock_.prepare_thread(); }

  [[nodiscard]] GuardState lock() { return lock_.lock(); }

  void unlock(GuardState &state) { lock_.unlock(state); }

private:
  McsTasTseLock lock_{};
};

} // namespace locks_bench
