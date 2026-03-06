#pragma once

#include "../../locks/mcs_tas.hpp"

namespace locks_bench {

struct McsTasLockBench {
  using GuardState = McsTasLock::LockState;

  [[nodiscard]] GuardState lock() { return lock_.lock(); }

  void unlock(GuardState &state) { lock_.unlock(state); }

private:
  McsTasLock lock_;
};

} // namespace locks_bench
