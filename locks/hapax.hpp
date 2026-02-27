#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <thread>
#include <utility>

struct HapaxVW {
  struct alignas(64) Slot {
    std::atomic<std::uint64_t> VisibleWaiter{0};
  };

  static constexpr std::size_t kSlotCount = 256;
  static_assert((kSlotCount & (kSlotCount - 1)) == 0,
                "kSlotCount must be a power of two");

  std::array<Slot, kSlotCount> Waiting{};
  std::atomic<std::uint64_t> Arrive{0}; // ingress
  std::atomic<std::uint64_t> Depart{0}; // egress

  [[nodiscard]] static inline std::uint64_t Mix(std::uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
  }

  [[nodiscard]] inline Slot *ToSlot(std::uint64_t hapax) {
    static constexpr std::uint32_t ArraySize = 4096;
    static_assert(ArraySize > 0 && (ArraySize & (ArraySize - 1)) == 0,
                  "ArraySize must be a power of two");
    alignas(4096) static Slot WaitingArray[ArraySize]{};

    const auto salt =
        static_cast<std::uint32_t>(reinterpret_cast<std::uintptr_t>(this));
    const std::uint32_t ix =
        ((salt + static_cast<std::uint32_t>(hapax >> 16)) * 17u) &
        (ArraySize - 1u);
    return WaitingArray + ix;
  }

  [[nodiscard]] static inline std::uint64_t NextHapax() {
    static constinit thread_local std::uint64_t PrivateHapax = 0;
    alignas(128) static constinit std::atomic<std::uint64_t> HapaxAllocator{0};

    // Create a unique hapax identity value.
    // Hapax is single-use and specific to this thread, this lock, and
    // this lock-unlock episode.
    std::uint64_t hapax = PrivateHapax++;
    if ((hapax & 0xFFFFu) == 0) [[unlikely]] {
      // Current block of hapax values is exhausted so must reprovision.
      // High 48-bits of the 64-bit hapax encode the thread "zone" and the
      // low 16 are the sub-sequence from which the thread can allocate locally.
      hapax = HapaxAllocator.fetch_add(1, std::memory_order_relaxed) + 1;
      assert(hapax != 0);
      hapax <<= 16;
      // PrivateHapax was post-incremented above; equality means normal
      // contiguous rollover into the next 16-bit sub-sequence.
      assert(hapax + 1 >= PrivateHapax);
      PrivateHapax = hapax + 1;
    }

    assert(hapax != 0); // by convention, 0 is reserved
    return hapax;
  }

  static inline void Pause() { std::this_thread::yield(); }

public:
  struct LockState {
    std::uint64_t hapax{0};
  };

  [[nodiscard]] inline LockState lock() {
    const std::uint64_t hapax = NextHapax();
    const std::uint64_t pred =
        Arrive.exchange(hapax, std::memory_order_acq_rel);
    assert(pred != hapax);

    if (Depart.load(std::memory_order_acquire) != pred) {
      Slot *slot = ToSlot(pred);
      std::uint64_t expected = 0;

      if (!slot->VisibleWaiter.compare_exchange_strong(
              expected, pred, std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        // Collision on the visible-waiter slot; wait via global depart value.
        while (Depart.load(std::memory_order_acquire) != pred) {
          Pause();
        }
      } else if (Depart.load(std::memory_order_acquire) == pred) {
        // Raced with unlock(); release the slot and proceed.
        expected = pred;
        slot->VisibleWaiter.compare_exchange_strong(
            expected, 0, std::memory_order_acq_rel, std::memory_order_acquire);
      } else {
        // Preferred path: wait to be handed over via this slot.
        while (slot->VisibleWaiter.load(std::memory_order_acquire) == pred) {
          Pause();
        }
      }
    }

    return LockState{hapax};
  }

  inline void unlock(LockState state) {
    const std::uint64_t hapax = state.hapax;
    assert(hapax != 0);

    Slot *slot = ToSlot(hapax);
    std::uint64_t expected = hapax;
    if (slot->VisibleWaiter.compare_exchange_strong(
            expected, 0, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return;
    }

    Depart.store(hapax, std::memory_order_release);
    expected = hapax;
    slot->VisibleWaiter.compare_exchange_strong(
        expected, 0, std::memory_order_acq_rel, std::memory_order_acquire);
  }

  template <typename Fn>
    requires std::invocable<Fn &&>
  inline void operator+(Fn &&csfn) {
    LockState state = lock();
    std::invoke(std::forward<Fn>(csfn));
    unlock(state);
  }
};
