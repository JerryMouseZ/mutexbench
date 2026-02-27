#pragma once

#include <concepts>

namespace locks_bench {

template <typename LockBenchT>
concept LockBench = requires(LockBenchT lock,
                             typename LockBenchT::GuardState state) {
  typename LockBenchT::GuardState;
  { lock.lock() } -> std::same_as<typename LockBenchT::GuardState>;
  { lock.unlock(state) } -> std::same_as<void>;
};

} // namespace locks_bench
