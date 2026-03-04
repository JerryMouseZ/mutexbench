#pragma once

#include "../../locks/reciprocating.hpp"

namespace locks_bench {

struct ReciprocatingLockBench {
  struct GuardState {};

  [[nodiscard]] GuardState lock() {
    lock_.lock();
    return {};
  }

  void unlock(GuardState &) { lock_.unlock(); }

private:
  ReciprocatingLock lock_;
};

} // namespace locks_bench
