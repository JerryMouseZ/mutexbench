#pragma once

#include <string>

namespace locks_bench {

// The benchmark no longer implements lock algorithms. `mutex` measures a plain
// pthread_mutex_t, so an LD_PRELOAD interposition library (LiTL) decides which
// algorithm runs. `pthread_spinlock` is a non-interposed control: LiTL's direct
// algorithms leave pthread_spin_* native.
enum class LockKind {
  kMutex,
  kPthreadSpinlock,
};

inline const char *LockKindToString(LockKind kind) {
  switch (kind) {
  case LockKind::kMutex:
    return "mutex";
  case LockKind::kPthreadSpinlock:
    return "pthread_spinlock";
  }
  return "unknown";
}

inline bool TryParseLockKind(const std::string &value, LockKind &out) {
  if (value == "mutex") {
    out = LockKind::kMutex;
    return true;
  }
  if (value == "pthread_spinlock" || value == "pthread-spinlock") {
    out = LockKind::kPthreadSpinlock;
    return true;
  }
  return false;
}

inline const char *LockKindRejectionHint() {
  return "expected: mutex or pthread_spinlock. Lock algorithms are selected by "
         "a LiTL launcher instead, e.g. "
         "third_party/litl/libmbmcs_original.sh ./mutex_bench --lock-kind mutex";
}

} // namespace locks_bench
