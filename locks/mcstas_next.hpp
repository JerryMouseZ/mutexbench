#pragma once

#include <atomic>
#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#else
#include <thread>
#endif

struct McsTasNextLock {
  struct alignas(64) Node {
    std::atomic<Node *> next{nullptr};
    std::atomic<bool> waiting{false};
    std::atomic<bool> is_next{false};
  };

  struct LockState {
    Node *node{nullptr};
  };

  [[nodiscard]] inline LockState lock() {
    // Fast path: single TAS probe.
    if (!locked_.exchange(true, std::memory_order_acquire)) {
      return {};
    }

    // Slow path: MCS queue to serialize contenders.
    Node &my_node = ThreadNode();
    my_node.next.store(nullptr, std::memory_order_relaxed);
    my_node.waiting.store(false, std::memory_order_relaxed);
    my_node.is_next.store(false, std::memory_order_relaxed);

    Node *pred = tail_.exchange(&my_node, std::memory_order_acq_rel);
    if (pred != nullptr) {
      my_node.waiting.store(true, std::memory_order_relaxed);
      pred->next.store(&my_node, std::memory_order_release);
      while (!my_node.is_next.load(std::memory_order_acquire)) {
        Pause();
      }
      while (my_node.waiting.load(std::memory_order_acquire)) {
        Pause();
      }
    } else {
      while (locked_.exchange(true, std::memory_order_acquire)) {
        Pause();
      }
    }

    SignalSuccessorIfPresent(my_node);
    return {&my_node};
  }

  inline void unlock(LockState &state) {
    Node *node = state.node;
    if (node == nullptr) {
      locked_.store(false, std::memory_order_release);
      return;
    }

    Node *succ = node->next.load(std::memory_order_acquire);
    if (succ == nullptr) {
      Node *expected = node;
      if (tail_.compare_exchange_strong(expected, nullptr,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire)) {
        locked_.store(false, std::memory_order_release);
        state.node = nullptr;
        return;
      }
      while ((succ = node->next.load(std::memory_order_acquire)) == nullptr) {
        Pause();
      }
    }

    succ->is_next.store(true, std::memory_order_release);
    succ->waiting.store(false, std::memory_order_release);
    state.node = nullptr;
  }

private:
  [[nodiscard]] static inline Node &ThreadNode() {
    static thread_local Node my_node{};
    return my_node;
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
