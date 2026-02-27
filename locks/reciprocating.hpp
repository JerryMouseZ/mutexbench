#include <atomic>
#include <cassert>
#include <concepts>
#include <cstdint>
#include <functional>
#include <thread>
#include <utility>

struct ReciprocatingLock {
  struct alignas(128) WaitElement {
    std::atomic<WaitElement *> Gate{nullptr};
  };

  static inline WaitElement *const LOCKEDEMPTY =
      reinterpret_cast<WaitElement *>(static_cast<std::uintptr_t>(1));

  // Arrivals encoding:
  //   nullptr      -> unlocked
  //   LOCKEDEMPTY  -> locked, arrival list empty
  //   T|1          -> locked, arrival stack populated (T = newest arrival)
  std::atomic<WaitElement *> Arrivals{nullptr};

  struct LockState {
    WaitElement *succ{nullptr};
    WaitElement *eos{nullptr};
    WaitElement *self{nullptr};
  };

  [[nodiscard]] inline LockState lock() {
    // Acquire phase.
    alignas(128) static thread_local WaitElement E{};
    E.Gate.store(nullptr, std::memory_order_relaxed);

    LockState state{};
    state.self = &E;
    state.eos = &E; // fast-path assumption

    WaitElement *tail = Arrivals.exchange(&E, std::memory_order_acq_rel);
    assert(tail != &E);
    if (tail != nullptr) {
      // Coerce LOCKEDEMPTY to nullptr by masking out the low bit.
      state.succ = reinterpret_cast<WaitElement *>(
          reinterpret_cast<std::uintptr_t>(tail) &
          ~static_cast<std::uintptr_t>(1));
      assert(state.succ != &E);

      // Contended wait.
      for (;;) {
        state.eos = E.Gate.load(std::memory_order_acquire);
        if (state.eos != nullptr) {
          break;
        }
        std::this_thread::yield();
      }

      assert(state.eos != &E);
      if (state.succ == state.eos) {
        // Logical end-of-segment marker.
        state.succ = nullptr;
        state.eos = LOCKEDEMPTY;
      }
    }

    assert(state.eos != nullptr);
    assert(Arrivals.load(std::memory_order_acquire) != nullptr);
    return state;
  }

  inline void unlock(LockState state) {
    WaitElement *succ = state.succ;
    WaitElement *eos = state.eos;
    WaitElement *self = state.self;

    assert(self != nullptr);
    assert(eos != nullptr);
    assert(Arrivals.load(std::memory_order_acquire) != nullptr);

    // Release phase.
    if (succ != nullptr) {
      assert(eos != self);
      assert(succ->Gate.load(std::memory_order_relaxed) == nullptr);
      succ->Gate.store(eos, std::memory_order_release);
      return;
    }

    assert(eos == LOCKEDEMPTY || eos == self);
    WaitElement *v = eos;
    if (Arrivals.compare_exchange_strong(v, nullptr, std::memory_order_acq_rel,
                                         std::memory_order_acquire)) {
      return;
    }

    WaitElement *w = Arrivals.exchange(LOCKEDEMPTY, std::memory_order_acq_rel);
    assert(w != nullptr && w != LOCKEDEMPTY && w != self);
    assert(w->Gate.load(std::memory_order_relaxed) == nullptr);
    w->Gate.store(eos, std::memory_order_release);
  }

  template <typename Fn>
    requires std::invocable<Fn &&>
  inline void operator+(Fn &&csfn) {
    LockState state = lock();
    std::invoke(std::forward<Fn>(csfn));
    unlock(state);
  }
};
