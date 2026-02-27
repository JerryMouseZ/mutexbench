#pragma once

#include "../../locks/mcs.hpp"

namespace locks_bench {

struct McsLockBench {
  using GuardState = McsLock::LockState;

  [[nodiscard]] GuardState lock() { return lock_.lock(); }

  void unlock(GuardState &state) { lock_.unlock(state); }

private:
  McsLock lock_;
};

} // namespace locks_bench
