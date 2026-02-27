#pragma once

#include "../../locks/reciprocating.hpp"

namespace locks_bench {

struct ReciprocatingLockBench {
  using GuardState = ReciprocatingLock::LockState;

  [[nodiscard]] GuardState lock() { return lock_.lock(); }

  void unlock(GuardState &state) { lock_.unlock(state); }

private:
  ReciprocatingLock lock_;
};

} // namespace locks_bench
