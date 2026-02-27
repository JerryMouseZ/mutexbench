#pragma once

#include "../../locks/clh.hpp"

namespace locks_bench {

struct ClhLockBench {
  using GuardState = ClhLock::LockState;

  [[nodiscard]] GuardState lock() { return lock_.lock(); }

  void unlock(GuardState &state) { lock_.unlock(state); }

private:
  ClhLock lock_;
};

} // namespace locks_bench
