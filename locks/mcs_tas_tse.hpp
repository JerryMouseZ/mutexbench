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

  McsTasTseLock() = default;

  void prepare_thread() const {
    ThreadSliceExtension().prepare_thread();
  }

  [[nodiscard]] inline LockState lock() {
    // Fast path: single TAS probe.
    if (!locked_.exchange(true, std::memory_order_acquire)) {
      ThreadSliceExtension().on_critical_section_enter();
      return {.timeslice_requested = true};
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
    ThreadSliceExtension().on_critical_section_enter();
    const bool timeslice_requested = true;
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
      ThreadSliceExtension().on_critical_section_exit();
      state.timeslice_requested = false;
    }
  }

private:
  [[nodiscard]] static inline Node &ThreadNode() {
    static thread_local Node my_node{};
    return my_node;
  }

  [[nodiscard]] static inline locks_bench::CriticalSectionTimesliceExtension &
  ThreadSliceExtension() {
    static thread_local locks_bench::CriticalSectionTimesliceExtension extension{
        locks_bench::TimesliceExtensionMode::kRequire};
    return extension;
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
};
