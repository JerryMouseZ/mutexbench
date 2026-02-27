#pragma once

#include "../../locks/hapax.hpp"

namespace locks_bench {

struct HapaxLockBench {
  using GuardState = HapaxVW::LockState;

  [[nodiscard]] GuardState lock() { return lock_.lock(); }

  void unlock(GuardState &state) { lock_.unlock(state); }

private:
  HapaxVW lock_;
};

} // namespace locks_bench
