#pragma once

#include "timeslice_extension.hpp"

#include "../../locks/mcstas_next_tse.hpp"

namespace locks_bench {

struct McsTasNextTseLockBench {
  using GuardState = McsTasNextTseLock::LockState;

  explicit McsTasNextTseLockBench(const LockBenchOptions &options = {})
      : lock_(options.timeslice_extension_mode) {}

  void prepare_thread() { lock_.prepare_thread(); }

  [[nodiscard]] GuardState lock() { return lock_.lock(); }

  void unlock(GuardState &state) { lock_.unlock(state); }

private:
  McsTasNextTseLock lock_;
};

} // namespace locks_bench
