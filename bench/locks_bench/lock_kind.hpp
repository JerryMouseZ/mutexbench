#pragma once

#include <string>

namespace locks_bench {

enum class LockKind {
  kMutex,
  kReciprocating,
  kHapax,
  kMcs,
  kTwa,
};

inline const char *LockKindToString(LockKind kind) {
  switch (kind) {
  case LockKind::kMutex:
    return "mutex";
  case LockKind::kReciprocating:
    return "reciprocating";
  case LockKind::kHapax:
    return "hapax";
  case LockKind::kMcs:
    return "mcs";
  case LockKind::kTwa:
    return "twa";
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
  if (value == "mcs") {
    out = LockKind::kMcs;
    return true;
  }
  if (value == "twa") {
    out = LockKind::kTwa;
    return true;
  }
  return false;
}

} // namespace locks_bench
