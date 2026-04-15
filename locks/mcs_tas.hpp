#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#else
#include <thread>
#endif

struct McsTasLock {
  struct alignas(64) Node {
    std::atomic<Node *> next{nullptr};
    std::atomic<bool> waiting{false};
  };

  struct LockState {
    bool reacquired_after_own_unlock{false};
    uint64_t pre_front_wait_cycles{0};
    uint64_t front_wait_cycles{0};
    uint64_t phase_wait_samples{0};
  };

  void set_sampling(bool enabled) const { SamplingEnabled() = enabled; }

  [[nodiscard]] inline LockState lock() {
    // Fast path: single TAS probe.
    if (!locked_.exchange(true, std::memory_order_acquire)) {
      return FinishAcquire(0, 0, 0);
    }

    const bool sample_phase_wait = SamplingEnabled();
    uint64_t pre_front_wait_cycles = 0;
    uint64_t front_wait_cycles = 0;
    uint64_t front_wait_start = 0;

    // Slow path: MCS queue to serialize contenders.
    Node &my_node = ThreadNode();
    my_node.next.store(nullptr, std::memory_order_relaxed);
    my_node.waiting.store(false, std::memory_order_relaxed);

    Node *pred = tail_.exchange(&my_node, std::memory_order_acq_rel);
    if (pred != nullptr) {
      my_node.waiting.store(true, std::memory_order_relaxed);
      pred->next.store(&my_node, std::memory_order_release);
      const uint64_t pre_front_wait_start =
          sample_phase_wait ? ReadCycles() : 0;
      while (my_node.waiting.load(std::memory_order_acquire)) {
        Pause();
      }
      if (sample_phase_wait) {
        const uint64_t pre_front_wait_end = ReadCycles();
        if (pre_front_wait_end >= pre_front_wait_start) {
          pre_front_wait_cycles = pre_front_wait_end - pre_front_wait_start;
        }
        front_wait_start = ReadCycles();
      }
    } else if (sample_phase_wait) {
      front_wait_start = ReadCycles();
    }

    while (locked_.exchange(true, std::memory_order_acquire)) {
      Pause();
    }
    if (sample_phase_wait) {
      const uint64_t front_wait_end = ReadCycles();
      if (front_wait_end >= front_wait_start) {
        front_wait_cycles = front_wait_end - front_wait_start;
      }
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

    return FinishAcquire(pre_front_wait_cycles, front_wait_cycles,
                         sample_phase_wait ? 1ULL : 0ULL);
  }

  inline void unlock(LockState &) {
    last_unlocker_tid_.store(CurrentThreadId(), std::memory_order_relaxed);
    locked_.store(false, std::memory_order_release);
  }

private:
  [[nodiscard]] inline LockState FinishAcquire(uint64_t pre_front_wait_cycles,
                                               uint64_t front_wait_cycles,
                                               uint64_t phase_wait_samples) {
    const uint32_t current_tid = CurrentThreadId();
    const bool reacquired_after_own_unlock =
        (last_unlocker_tid_.load(std::memory_order_relaxed) == current_tid);
    return {.reacquired_after_own_unlock = reacquired_after_own_unlock,
            .pre_front_wait_cycles = pre_front_wait_cycles,
            .front_wait_cycles = front_wait_cycles,
            .phase_wait_samples = phase_wait_samples};
  }

  [[nodiscard]] static inline Node &ThreadNode() {
    static thread_local Node my_node{};
    return my_node;
  }

  [[nodiscard]] static inline bool &SamplingEnabled() {
    static thread_local bool enabled = false;
    return enabled;
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

  static inline void Pause() {
#if defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#else
    std::this_thread::yield();
#endif
  }

  [[nodiscard]] static inline uint32_t CurrentThreadId() {
    static std::atomic<uint32_t> next_tid{1};
    static thread_local const uint32_t tid =
        next_tid.fetch_add(1, std::memory_order_relaxed);
    return tid;
  }

  alignas(64) std::atomic<Node *> tail_{nullptr};
  alignas(64) std::atomic<bool> locked_{false};
  alignas(64) std::atomic<uint32_t> last_unlocker_tid_{0};
};
