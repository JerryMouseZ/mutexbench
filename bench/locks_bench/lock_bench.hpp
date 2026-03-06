#pragma once

#include <concepts>

#include "timeslice_extension.hpp"

namespace locks_bench {

template <typename LockBenchT>
concept LockBench = requires(LockBenchT lock,
                             const LockBenchOptions &options,
                             typename LockBenchT::GuardState state) {
  typename LockBenchT::GuardState;
  { LockBenchT{options} };
  { lock.prepare_thread() } -> std::same_as<void>;
  { lock.lock() } -> std::same_as<typename LockBenchT::GuardState>;
  { lock.unlock(state) } -> std::same_as<void>;
};

} // namespace locks_bench
