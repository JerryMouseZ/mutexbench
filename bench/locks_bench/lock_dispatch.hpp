#pragma once

#include <cstdlib>
#include <utility>

#include "hapax_lock_bench.hpp"
#include "lock_kind.hpp"
#include "mcs_lock_bench.hpp"
#include "reciprocating_lock_bench.hpp"
#include "std_mutex_lock_bench.hpp"

namespace locks_bench {

template <typename Fn> decltype(auto) DispatchByLockKind(LockKind kind, Fn &&fn) {
  switch (kind) {
  case LockKind::kMutex:
    return std::forward<Fn>(fn).template operator()<StdMutexLockBench>();
  case LockKind::kReciprocating:
    return std::forward<Fn>(fn).template operator()<ReciprocatingLockBench>();
  case LockKind::kHapax:
    return std::forward<Fn>(fn).template operator()<HapaxLockBench>();
  case LockKind::kMcs:
    return std::forward<Fn>(fn).template operator()<McsLockBench>();
  }
  std::abort();
}

} // namespace locks_bench
