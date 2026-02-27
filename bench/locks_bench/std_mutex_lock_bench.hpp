#pragma once

#include <mutex>

namespace locks_bench {

struct StdMutexLockBench {
  struct GuardState {};

  [[nodiscard]] GuardState lock() {
    mu_.lock();
    return {};
  }

  void unlock(GuardState &) { mu_.unlock(); }

private:
  std::mutex mu_;
};

} // namespace locks_bench
