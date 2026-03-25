#pragma once

#include <atomic>
#include <cassert>
#include <concepts>
#include <functional>
#include <thread>
#include <utility>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

struct ClhLock {
  struct alignas(64) Node {
    std::atomic<bool> locked{false};
  };

  struct LockState {
    Node *pred{nullptr};
  };

  ClhLock() : tail_(&sentinel_) {
    sentinel_.locked.store(false, std::memory_order_relaxed);
  }

  [[nodiscard]] inline LockState lock() {
    Node *&my_node = ThreadNodeRef();
    my_node->locked.store(true, std::memory_order_relaxed);

    Node *pred = tail_.exchange(my_node, std::memory_order_acq_rel);
    assert(pred != nullptr);
    while (pred->locked.load(std::memory_order_acquire)) {
      Pause();
    }

    return LockState{pred};
  }

  inline void unlock(LockState state) {
    Node *&my_node = ThreadNodeRef();
    assert(my_node != nullptr);
    assert(state.pred != nullptr);

    my_node->locked.store(false, std::memory_order_release);
    my_node = state.pred;
  }

  template <typename Fn>
    requires std::invocable<Fn &&>
  inline void operator+(Fn &&csfn) {
    LockState state = lock();
    std::invoke(std::forward<Fn>(csfn));
    unlock(state);
  }

private:
  [[nodiscard]] static inline Node *&ThreadNodeRef() {
    static thread_local Node *my_node = [] {
      Node *node = new Node{};
      node->locked.store(false, std::memory_order_relaxed);
      return node;
    }();
    return my_node;
  }

  static inline void Pause() {
#if defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#else
    std::this_thread::yield();
#endif
  }

  alignas(64) Node sentinel_{};
  std::atomic<Node *> tail_;
};
