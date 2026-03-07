#pragma once

#include <atomic>
#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#else
#include <thread>
#endif

#include "../bench/locks_bench/timeslice_extension.hpp"

struct McsTasTseLock {
  struct alignas(64) Node {
    std::atomic<Node *> next{nullptr};
    std::atomic<bool> waiting{false};
  };

  struct LockState {
    bool timeslice_requested{false};
  };

  explicit McsTasTseLock(
      locks_bench::TimesliceExtensionMode timeslice_mode =
          locks_bench::TimesliceExtensionMode::kOff)
      : timeslice_mode_(timeslice_mode) {}

  void prepare_thread() const {
    if (timeslice_mode_ == locks_bench::TimesliceExtensionMode::kOff) {
      return;
    }
    ThreadTimesliceExtension(timeslice_mode_).prepare_thread();
  }

  [[nodiscard]] inline LockState lock() {
    // Fast path: single TAS probe.
    if (!locked_.exchange(true, std::memory_order_acquire)) {
      return {};
    }

    // Slow path: MCS queue to serialize contenders.
    Node &my_node = ThreadNode();
    my_node.next.store(nullptr, std::memory_order_relaxed);
    my_node.waiting.store(false, std::memory_order_relaxed);

    Node *pred = tail_.exchange(&my_node, std::memory_order_acq_rel);
    if (pred != nullptr) {
      my_node.waiting.store(true, std::memory_order_relaxed);
      pred->next.store(&my_node, std::memory_order_release);
      while (my_node.waiting.load(std::memory_order_acquire)) {
        Pause();
      }
    }

    // Request a slice extension once this thread becomes the designated
    // spinner that is about to inherit the lock.
    bool timeslice_requested = false;
    if (timeslice_mode_ != locks_bench::TimesliceExtensionMode::kOff) {
      ThreadTimesliceExtension(timeslice_mode_).on_critical_section_enter();
      timeslice_requested = true;
    }
    while (locked_.exchange(true, std::memory_order_acquire)) {
      Pause();
    }

    // Wake the next queued waiter, if any, so only one queued thread at a
    // time spins on TAS.
    Node *succ = my_node.next.load(std::memory_order_acquire);
    if (succ == nullptr) {
      Node *expected = &my_node;
      if (!tail_.compare_exchange_strong(expected, nullptr,
                                         std::memory_order_acq_rel,
                                         std::memory_order_acquire)) {
        while ((succ = my_node.next.load(std::memory_order_acquire)) ==
               nullptr) {
          Pause();
        }
      }
    }
    if (succ != nullptr) {
      succ->waiting.store(false, std::memory_order_release);
    }
    return {.timeslice_requested = timeslice_requested};
  }

  inline void unlock(LockState &state) {
    locked_.store(false, std::memory_order_release);
    if (state.timeslice_requested) {
      ThreadTimesliceExtension(timeslice_mode_).on_critical_section_exit();
      state.timeslice_requested = false;
    }
  }

private:
  [[nodiscard]] static inline Node &ThreadNode() {
    static thread_local Node my_node{};
    return my_node;
  }

  [[nodiscard]] static inline locks_bench::CriticalSectionTimesliceExtension &
  ThreadTimesliceExtension(locks_bench::TimesliceExtensionMode mode) {
    struct ThreadTimesliceState {
      bool configured{false};
      locks_bench::TimesliceExtensionMode mode{
          locks_bench::TimesliceExtensionMode::kOff};
      locks_bench::CriticalSectionTimesliceExtension extension{};
    };

    static thread_local ThreadTimesliceState state{};
    if (!state.configured || state.mode != mode) {
      state.mode = mode;
      state.extension = locks_bench::CriticalSectionTimesliceExtension(mode);
      state.configured = true;
    }
    return state.extension;
  }

  static inline void Pause() {
#if defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#else
    std::this_thread::yield();
#endif
  }

  alignas(64) std::atomic<Node *> tail_{nullptr};
  alignas(64) std::atomic<bool> locked_{false};
  locks_bench::TimesliceExtensionMode timeslice_mode_;
};
