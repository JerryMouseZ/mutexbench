#pragma once

#include "../../locks/twa.hpp"

namespace locks_bench {

struct TwaLockBench {
  using GuardState = TwaLock::LockState;

  [[nodiscard]] GuardState lock() { return lock_.lock(); }

  void unlock(GuardState &state) { lock_.unlock(state); }

private:
  TwaLock lock_;
};

} // namespace locks_bench
