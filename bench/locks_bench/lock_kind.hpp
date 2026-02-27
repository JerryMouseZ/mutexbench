#pragma once

#include <string>

namespace locks_bench {

enum class LockKind {
  kMutex,
  kReciprocating,
  kHapax,
};

inline const char *LockKindToString(LockKind kind) {
  switch (kind) {
  case LockKind::kMutex:
    return "mutex";
  case LockKind::kReciprocating:
    return "reciprocating";
  case LockKind::kHapax:
    return "hapax";
  }
  return "unknown";
}

inline bool TryParseLockKind(const std::string &value, LockKind &out) {
  if (value == "mutex") {
    out = LockKind::kMutex;
    return true;
  }
  if (value == "reciprocating") {
    out = LockKind::kReciprocating;
    return true;
  }
  if (value == "hapax") {
    out = LockKind::kHapax;
    return true;
  }
  return false;
}

} // namespace locks_bench
