#pragma once

#include <atomic>
#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

struct McsLock {
  struct alignas(64) Node {
    std::atomic<Node *> next{nullptr};
    std::atomic<bool> locked{false};
  };

  std::atomic<Node *> tail{nullptr};

  struct LockState {
    Node *node;
  };

  [[nodiscard]] inline LockState lock() {
    static thread_local Node my_node;
    my_node.next.store(nullptr, std::memory_order_relaxed);
    my_node.locked.store(true, std::memory_order_relaxed);

    Node *prev = tail.exchange(&my_node, std::memory_order_acq_rel);
    if (prev != nullptr) {
      prev->next.store(&my_node, std::memory_order_release);
      while (my_node.locked.load(std::memory_order_acquire)) {
#if defined(__x86_64__) || defined(__i386__)
        _mm_pause();
#else
        std::this_thread::yield();
#endif
      }
    }
    return {&my_node};
  }

  inline void unlock(LockState state) {
    Node *node = state.node;
    Node *succ = node->next.load(std::memory_order_acquire);
    if (succ == nullptr) {
      Node *expected = node;
      if (tail.compare_exchange_strong(expected, nullptr,
                                       std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
        return;
      }
      // A new waiter linked in; wait for it to set our next pointer.
      // Use pause to spin tightly — this window is very short and we want
      // to hand off the lock as quickly as possible.
      while ((succ = node->next.load(std::memory_order_acquire)) == nullptr) {
#if defined(__x86_64__) || defined(__i386__)
        _mm_pause();
#else
        std::this_thread::yield();
#endif
      }
    }
    succ->locked.store(false, std::memory_order_release);
  }
};
