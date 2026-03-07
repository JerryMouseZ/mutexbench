#pragma once

#include <string>

namespace locks_bench {

enum class LockKind {
  kMutex,
  kReciprocating,
  kHapax,
  kMcs,
  kMcsTas,
  kMcsTasTse,
  kMcsTasNext,
  kMcsTasNextTse,
  kTwa,
  kClh,
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
  case LockKind::kMcsTas:
    return "mcs-tas";
  case LockKind::kMcsTasTse:
    return "mcs-tas-tse";
  case LockKind::kMcsTasNext:
    return "mcstas-next";
  case LockKind::kMcsTasNextTse:
    return "mcstas-next-tse";
  case LockKind::kTwa:
    return "twa";
  case LockKind::kClh:
    return "clh";
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
  if (value == "mcs-tas") {
    out = LockKind::kMcsTas;
    return true;
  }
  if (value == "mcs-tas-tse") {
    out = LockKind::kMcsTasTse;
    return true;
  }
  if (value == "mcstas-next") {
    out = LockKind::kMcsTasNext;
    return true;
  }
  if (value == "mcstas-next-tse") {
    out = LockKind::kMcsTasNextTse;
    return true;
  }
  if (value == "twa") {
    out = LockKind::kTwa;
    return true;
  }
  if (value == "clh") {
    out = LockKind::kClh;
    return true;
  }
  return false;
}

} // namespace locks_bench
