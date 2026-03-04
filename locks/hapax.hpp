#pragma once

#include <atomic>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <thread>
#include <utility>
#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

struct HapaxVW {
  struct alignas(64) Slot {
    std::atomic<std::uint64_t> VisibleWaiter{0};
  };

  static constexpr std::uint32_t kWaitingArraySize = 4096;
  static_assert(kWaitingArraySize > 0 &&
                    (kWaitingArraySize & (kWaitingArraySize - 1)) == 0,
                "kWaitingArraySize must be a power of two");

  alignas(64) std::atomic<std::uint64_t> Arrive{0}; // ingress
  alignas(64) std::atomic<std::uint64_t> Depart{0}; // egress

  [[nodiscard]] inline Slot *ToSlot(std::uint64_t hapax) {
    alignas(4096) static Slot waiting_array[kWaitingArraySize]{};
    const auto salt =
        static_cast<std::uint32_t>(reinterpret_cast<std::uintptr_t>(this));
    const std::uint32_t ix =
        ((salt + static_cast<std::uint32_t>(hapax >> 16)) * 17u) &
        (kWaitingArraySize - 1u);
    return waiting_array + ix;
  }

  [[nodiscard]] static inline std::uint64_t NextHapax() {
    static constinit thread_local std::uint64_t PrivateHapax = 0;
    alignas(128) static constinit std::atomic<std::uint64_t> HapaxAllocator{0};

    std::uint64_t hapax = PrivateHapax++;
    if ((hapax & 0xFFFFu) == 0) [[unlikely]] {
      hapax = HapaxAllocator.fetch_add(1, std::memory_order_relaxed) + 1;
      assert(hapax != 0);
      hapax <<= 16;
      assert(hapax + 1 >= PrivateHapax);
      PrivateHapax = hapax + 1;
    }

    assert(hapax != 0);
    return hapax;
  }

  static inline void Pause(std::uint32_t spin_count) {
#if defined(__x86_64__) || defined(__i386__)
    _mm_pause();
#else
    if ((spin_count & 0xFFu) == 0) {
      std::this_thread::yield();
    }
#endif
  }

  // [新增] store->load fence：用于 unlock 慢路径中关闭 “tardy waiter”
  // 架构竞态窗口 论文 Listing 5 明确提到需要某种 store-load
  // fence（或等价语义）来避免竞态。
  static inline void StoreLoadFence() noexcept {
    std::atomic_thread_fence(std::memory_order_acq_rel);
  }

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

      // 论文要求：用 CAS(0->pred) 注册 visible waiter；不改成
      // TTAS（load-before-CAS）
      if (!slot->VisibleWaiter.compare_exchange_strong(
              expected, pred, std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        // Collision：退化为对 Depart 的全局自旋（论文 Listing 5 的 fallback）
        std::uint32_t spin_count = 0;
        while (Depart.load(std::memory_order_acquire) != pred) {
          Pause(++spin_count);
        }
      } else if (Depart.load(std::memory_order_acquire) == pred) {
        // ratify：与 unlock() 竞态；必须用 CAS(pred->0)，不能用 store(0)
        expected = pred;
        (void)slot->VisibleWaiter.compare_exchange_strong(
            expected, 0, std::memory_order_acq_rel, std::memory_order_acquire);
      } else {
        // 正常：等待 slot 变化（被前驱 CAS 清零）
        std::uint32_t spin_count = 0;
        while (slot->VisibleWaiter.load(std::memory_order_acquire) == pred) {
          Pause(++spin_count);
        }
      }
    }

    return LockState{hapax};
  }

  inline void unlock(LockState state) {
    const std::uint64_t hapax = state.hapax;
    assert(hapax != 0);

    Slot *slot = ToSlot(hapax);

    // [可选但更贴合论文语义] unlock 的 CAS 关键是“发布 + 交接”，成功语义只需
    // release； 等待者在 lock 侧用 acquire-load 观察 slot
    // 变化即可建立同步。:contentReference[oaicite:5]{index=5}
    std::uint64_t expected = hapax;
    if (slot->VisibleWaiter.compare_exchange_strong(
            expected, 0,
            std::memory_order_release,  // success: publish CS + handover
            std::memory_order_relaxed)) // fail: just a probe
    {
      return; // assured positive handover：跳过 Depart.store
    }

    // 慢路径：必须 store Depart
    Depart.store(hapax, std::memory_order_release);

    // [关键修改] 论文指出这里存在潜在“架构级”竞态，需要 store->load fence
    // 来关闭窗口。
    StoreLoadFence(); // store Depart; fence; (then) CAS/load slot
                      // :contentReference[oaicite:6]{index=6}

    // 再次 CAS(hapax->0)：关闭 tardy waiter 竞态窗口（论文 Listing 5 line
    // 156-166）
    expected = hapax;
    (void)slot->VisibleWaiter.compare_exchange_strong(
        expected, 0, std::memory_order_release, std::memory_order_relaxed);
  }

  template <typename Fn>
    requires std::invocable<Fn &&>
  inline void operator+(Fn &&csfn) {
    LockState state = lock();
    std::invoke(std::forward<Fn>(csfn));
    unlock(state);
  }
};
