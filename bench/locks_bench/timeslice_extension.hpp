#pragma once

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/prctl.h>
#include <sys/rseq.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace locks_bench {

enum class TimesliceExtensionMode {
  kOff,
  kAuto,
  kRequire,
};

struct LockBenchOptions {
  TimesliceExtensionMode timeslice_extension_mode =
      TimesliceExtensionMode::kOff;
};

inline const char *TimesliceExtensionModeToString(TimesliceExtensionMode mode) {
  switch (mode) {
  case TimesliceExtensionMode::kOff:
    return "off";
  case TimesliceExtensionMode::kAuto:
    return "auto";
  case TimesliceExtensionMode::kRequire:
    return "require";
  }
  return "unknown";
}

namespace detail {

#ifndef PR_RSEQ_SLICE_EXTENSION
#define PR_RSEQ_SLICE_EXTENSION 79
#define PR_RSEQ_SLICE_EXTENSION_GET 1
#define PR_RSEQ_SLICE_EXTENSION_SET 2
#define PR_RSEQ_SLICE_EXT_ENABLE 0x01
#endif

#ifndef RSEQ_CS_FLAG_SLICE_EXT_AVAILABLE
#define RSEQ_CS_FLAG_SLICE_EXT_AVAILABLE (1U << 4)
#define RSEQ_CS_FLAG_SLICE_EXT_ENABLED (1U << 5)
#endif

#ifndef ENOTSUPP
#define ENOTSUPP 524
#endif

#if defined(SYS_rseq_slice_yield)
inline constexpr long kRseqSliceYieldSyscallNumber = SYS_rseq_slice_yield;
inline constexpr bool kHasRseqSliceYieldSyscallNumber = true;
#elif defined(__NR_rseq_slice_yield)
inline constexpr long kRseqSliceYieldSyscallNumber = __NR_rseq_slice_yield;
inline constexpr bool kHasRseqSliceYieldSyscallNumber = true;
#elif defined(__x86_64__)
inline constexpr long kRseqSliceYieldSyscallNumber = 471;
inline constexpr bool kHasRseqSliceYieldSyscallNumber = true;
#else
inline constexpr long kRseqSliceYieldSyscallNumber = -1;
inline constexpr bool kHasRseqSliceYieldSyscallNumber = false;
#endif

struct RseqSliceCtrl {
  union {
    uint32_t all{0};
    struct {
      uint8_t request;
      uint8_t granted;
      uint16_t reserved;
    };
  };
};

struct RseqWithSliceCtrl {
  uint32_t cpu_id_start;
  uint32_t cpu_id;
  uint64_t rseq_cs;
  uint32_t flags;
  uint32_t node_id;
  uint32_t mm_cid;
  RseqSliceCtrl slice_ctrl;
};

static_assert(offsetof(RseqWithSliceCtrl, slice_ctrl) == 28);

inline constexpr size_t kRseqSliceCtrlEnd =
    offsetof(RseqWithSliceCtrl, slice_ctrl) + sizeof(RseqSliceCtrl);

inline void CompilerBarrier() noexcept {
  std::atomic_signal_fence(std::memory_order_seq_cst);
}

inline bool IsUnsupportedErrno(int err) noexcept {
  return err == EOPNOTSUPP || err == ENOTSUPP || err == ENOSYS || err == EINVAL;
}

struct TimesliceThreadState {
  bool initialized{false};
  bool enabled{false};
  int error_number{0};
  const char *reason{nullptr};
  RseqWithSliceCtrl *rseq{nullptr};
};

inline TimesliceThreadState &CurrentThreadTimesliceState() {
  static thread_local TimesliceThreadState state;
  return state;
}

inline RseqWithSliceCtrl *CurrentThreadRseqWithSliceCtrl() noexcept {
  if (__rseq_size < kRseqSliceCtrlEnd) {
    return nullptr;
  }

  char *thread_pointer = static_cast<char *>(__builtin_thread_pointer());
  return reinterpret_cast<RseqWithSliceCtrl *>(thread_pointer + __rseq_offset);
}

inline TimesliceThreadState InitTimesliceThreadState() noexcept {
  TimesliceThreadState state;
  state.initialized = true;

  if (!kHasRseqSliceYieldSyscallNumber) {
    state.reason = "user-space headers do not expose rseq_slice_yield";
    return state;
  }

  if (__rseq_size == 0) {
    state.reason = "glibc did not register rseq for this thread";
    return state;
  }

  state.rseq = CurrentThreadRseqWithSliceCtrl();
  if (state.rseq == nullptr) {
    state.reason =
        "glibc rseq area is too small to expose slice_ctrl (need 32 bytes)";
    return state;
  }

  if ((state.rseq->flags & RSEQ_CS_FLAG_SLICE_EXT_AVAILABLE) == 0) {
    state.reason = "kernel did not advertise rseq slice extension";
    return state;
  }

  errno = 0;
  const int current =
      prctl(PR_RSEQ_SLICE_EXTENSION, PR_RSEQ_SLICE_EXTENSION_GET, 0, 0, 0);
  if (current == -1) {
    state.error_number = errno;
    state.reason = "prctl(PR_RSEQ_SLICE_EXTENSION_GET) failed";
    return state;
  }

  if ((current & PR_RSEQ_SLICE_EXT_ENABLE) == 0) {
    errno = 0;
    if (prctl(PR_RSEQ_SLICE_EXTENSION, PR_RSEQ_SLICE_EXTENSION_SET,
              PR_RSEQ_SLICE_EXT_ENABLE, 0, 0) == -1) {
      state.error_number = errno;
      state.reason = "prctl(PR_RSEQ_SLICE_EXTENSION_SET) failed";
      return state;
    }
  }

  state.rseq->slice_ctrl.all = 0;
  CompilerBarrier();
  state.enabled = true;
  return state;
}

inline const TimesliceThreadState &EnsureTimesliceThreadState() noexcept {
  TimesliceThreadState &state = CurrentThreadTimesliceState();
  if (!state.initialized) {
    state = InitTimesliceThreadState();
  }
  return state;
}

[[noreturn]] inline void AbortTimesliceExtension(const char *context,
                                                 const char *reason,
                                                 int error_number) {
  if (error_number != 0) {
    std::fprintf(stderr, "timeslice extension %s failed: %s (errno=%d, %s)\n",
                 context, reason, error_number, std::strerror(error_number));
  } else {
    std::fprintf(stderr, "timeslice extension %s failed: %s\n", context,
                 reason);
  }
  std::abort();
}

} // namespace detail

struct TimesliceExtensionStatus {
  bool enabled{false};
  const char *reason{nullptr};
  int error_number{0};
};

inline TimesliceExtensionStatus
CurrentThreadTimesliceExtensionStatus(TimesliceExtensionMode mode) noexcept {
  if (mode == TimesliceExtensionMode::kOff) {
    return {};
  }

  const auto &state = detail::EnsureTimesliceThreadState();
  return {
      .enabled = state.enabled,
      .reason = state.reason,
      .error_number = state.error_number,
  };
}

class CriticalSectionTimesliceExtension {
public:
  explicit CriticalSectionTimesliceExtension(
      TimesliceExtensionMode mode = TimesliceExtensionMode::kOff)
      : mode_(mode) {}

  void prepare_thread() const {
    if (mode_ == TimesliceExtensionMode::kOff) {
      return;
    }

    const auto &state = detail::EnsureTimesliceThreadState();
    if (!state.enabled && mode_ == TimesliceExtensionMode::kRequire) {
      detail::AbortTimesliceExtension("enable", state.reason,
                                      state.error_number);
    }
  }

  void on_critical_section_enter() const noexcept {
    if (mode_ == TimesliceExtensionMode::kOff) {
      return;
    }

    const auto &state = detail::EnsureTimesliceThreadState();
    if (!state.enabled) {
      return;
    }

    detail::CompilerBarrier();
    state.rseq->slice_ctrl.request = 1;
    detail::CompilerBarrier();
  }

  void on_critical_section_exit() const noexcept {
    if (mode_ == TimesliceExtensionMode::kOff) {
      return;
    }

    auto &state = detail::CurrentThreadTimesliceState();
    if (!state.initialized || !state.enabled) {
      return;
    }

    detail::CompilerBarrier();
    state.rseq->slice_ctrl.request = 0;

    if (state.rseq->slice_ctrl.granted == 0) {
      return;
    }

    errno = 0;
    if (syscall(detail::kRseqSliceYieldSyscallNumber) == 0) {
      return;
    }

    if (detail::IsUnsupportedErrno(errno)) {
      state.enabled = false;
      state.error_number = errno;
      state.reason = "rseq_slice_yield is unavailable";
      if (mode_ == TimesliceExtensionMode::kRequire) {
        detail::AbortTimesliceExtension("yield", state.reason,
                                        state.error_number);
      }
    }
  }

private:
  TimesliceExtensionMode mode_;
};

} // namespace locks_bench
