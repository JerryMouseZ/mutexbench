#pragma once

#include <atomic>
#include <chrono>
#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#else
#include <thread>
#endif

#include "../bench/locks_bench/timeslice_extension.hpp"

struct McsTasNextTseLock {
  struct alignas(64) Node {
    std::atomic<Node *> next{nullptr};
    std::atomic<bool> waiting{false};
    std::atomic<bool> is_next{false};
  };

  struct LockState {
    Node *node{nullptr};
    bool timeslice_requested{false};
    uint64_t pre_front_wait_cycles{0};
    uint64_t front_wait_cycles{0};
    uint64_t phase_wait_samples{0};
  };

  McsTasNextTseLock() = default;

  void prepare_thread() const {
    ThreadSliceExtension().prepare_thread();
  }

  void set_sampling(bool enabled) const { SamplingEnabled() = enabled; }

  [[nodiscard]] inline LockState lock() {
    // Fast path: single TAS probe.
    if (!locked_.exchange(true, std::memory_order_acquire)) {
      ThreadSliceExtension().on_critical_section_enter();
      return {.timeslice_requested = true};
    }

    const bool sample_phase_wait = SamplingEnabled();
    uint64_t pre_front_wait_cycles = 0;
    uint64_t front_wait_cycles = 0;
    uint64_t front_wait_start = 0;

    // Slow path: MCS queue to serialize contenders.
    Node &my_node = ThreadNode();
    my_node.next.store(nullptr, std::memory_order_relaxed);
    my_node.waiting.store(false, std::memory_order_relaxed);
    my_node.is_next.store(false, std::memory_order_relaxed);

    Node *pred = tail_.exchange(&my_node, std::memory_order_acq_rel);
    bool timeslice_requested = false;
    if (pred != nullptr) {
      my_node.waiting.store(true, std::memory_order_relaxed);
      pred->next.store(&my_node, std::memory_order_release);
      const uint64_t pre_front_wait_start =
          sample_phase_wait ? ReadCycles() : 0;
      while (!my_node.is_next.load(std::memory_order_acquire)) {
        Pause();
      }
      if (sample_phase_wait) {
        const uint64_t pre_front_wait_end = ReadCycles();
        if (pre_front_wait_end >= pre_front_wait_start) {
          pre_front_wait_cycles = pre_front_wait_end - pre_front_wait_start;
        }
        front_wait_start = ReadCycles();
      }

      // Request a slice extension once this thread becomes the designated
      // waiter that is next in line to inherit the lock.
      timeslice_requested = RequestTimesliceExtension();
      while (my_node.waiting.load(std::memory_order_acquire)) {
        Pause();
      }
      if (sample_phase_wait) {
        const uint64_t front_wait_end = ReadCycles();
        if (front_wait_end >= front_wait_start) {
          front_wait_cycles = front_wait_end - front_wait_start;
        }
      }
    } else {
      if (sample_phase_wait) {
        front_wait_start = ReadCycles();
      }
      timeslice_requested = RequestTimesliceExtension();
      while (locked_.exchange(true, std::memory_order_acquire)) {
        Pause();
      }
      if (sample_phase_wait) {
        const uint64_t front_wait_end = ReadCycles();
        if (front_wait_end >= front_wait_start) {
          front_wait_cycles = front_wait_end - front_wait_start;
        }
      }
    }

    SignalSuccessorIfPresent(my_node);
    return {.node = &my_node,
            .timeslice_requested = timeslice_requested,
            .pre_front_wait_cycles = pre_front_wait_cycles,
            .front_wait_cycles = front_wait_cycles,
            .phase_wait_samples = sample_phase_wait ? 1ULL : 0ULL};
  }

  inline void unlock(LockState &state) {
    Node *node = state.node;
    if (node == nullptr) {
      locked_.store(false, std::memory_order_release);
      FinishTimesliceExtension(state);
      return;
    }

    Node *succ = node->next.load(std::memory_order_acquire);
    if (succ == nullptr) {
      Node *expected = node;
      if (tail_.compare_exchange_strong(expected, nullptr,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire)) {
        locked_.store(false, std::memory_order_release);
        FinishTimesliceExtension(state);
        state.node = nullptr;
        return;
      }
      while ((succ = node->next.load(std::memory_order_acquire)) == nullptr) {
        Pause();
      }
    }

    succ->is_next.store(true, std::memory_order_release);
    succ->waiting.store(false, std::memory_order_release);
    FinishTimesliceExtension(state);
    state.node = nullptr;
  }

private:
  [[nodiscard]] static inline Node &ThreadNode() {
    static thread_local Node my_node{};
    return my_node;
  }

  [[nodiscard]] static inline bool &SamplingEnabled() {
    static thread_local bool enabled = false;
    return enabled;
  }

  [[nodiscard]] inline bool RequestTimesliceExtension() const {
    ThreadSliceExtension().on_critical_section_enter();
    return true;
  }

  inline void FinishTimesliceExtension(LockState &state) const {
    if (!state.timeslice_requested) {
      return;
    }
    ThreadSliceExtension().on_critical_section_exit();
    state.timeslice_requested = false;
  }

  [[nodiscard]] static inline locks_bench::CriticalSectionTimesliceExtension &
  ThreadSliceExtension() {
    static thread_local locks_bench::CriticalSectionTimesliceExtension extension{
        locks_bench::TimesliceExtensionMode::kRequire};
    return extension;
  }

  [[nodiscard]] static inline uint64_t ReadCycles() {
#if defined(__x86_64__) || defined(__i386__)
    return __rdtsc();
#else
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
#endif
  }

  inline void SignalSuccessorIfPresent(Node &node) {
    Node *succ = node.next.load(std::memory_order_acquire);
    if (succ == nullptr && tail_.load(std::memory_order_acquire) != &node) {
      while ((succ = node.next.load(std::memory_order_acquire)) == nullptr) {
        Pause();
      }
    }
    if (succ != nullptr) {
      succ->is_next.store(true, std::memory_order_release);
    }
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
