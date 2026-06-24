#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <vector>

namespace spsc {

// Cache line size used to keep the producer's and consumer's indices apart.
// Hardcoded to 64 (every current x86-64 and AArch64 line) instead of
// std::hardware_destructive_interference_size, which gcc warns can change between
// compiler versions and so is unsafe to bake into anything ABI-facing.
inline constexpr std::size_t kCacheLine = 64;

// Bounded, wait-free single-producer/single-consumer ring buffer.
//
// One thread calls push, exactly one other calls pop. Capacity is fixed at
// construction and rounded up to a power of two so the slot index is a bitmask of
// a free-running counter rather than a modulo.
template <typename T>
class Ring {
public:
    explicit Ring(std::size_t capacity)
        : capacity_(std::bit_ceil(capacity == 0 ? std::size_t{1} : capacity)),
          mask_(capacity_ - 1),
          slots_(capacity_) {}

    Ring(const Ring&) = delete;
    Ring& operator=(const Ring&) = delete;

    // Push one element. Returns false if the ring is full.
    //
    // Only the producer writes tail, so its own value is read relaxed. head is
    // loaded acquire to synchronize with the consumer's release of a freed slot,
    // and the new tail is published release so the slot store above it is visible
    // to the consumer's matching acquire load.
    bool push(const T& item) {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        // Check space against the cached head first; only reload the real atomic
        // (paying a cross-core read) when the cache claims the ring is full.
        if (tail - head_cache_ == capacity_) {
            head_cache_ = head_.load(std::memory_order_acquire);
            if (tail - head_cache_ == capacity_) {
                return false;
            }
        }
        slots_[tail & mask_] = item;
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    // Pop one element into out. Returns false if the ring is empty.
    //
    // Mirror of push: head is the consumer's own index (relaxed), tail is acquire
    // to observe the producer's published element, and the advanced head is
    // released to free the slot.
    bool pop(T& out) {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        if (head == tail_cache_) {
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if (head == tail_cache_) {
                return false;
            }
        }
        out = slots_[head & mask_];
        head_.store(head + 1, std::memory_order_release);
        return true;
    }

    std::size_t capacity() const { return capacity_; }

    std::size_t size() const {
        return tail_.load(std::memory_order_acquire) -
               head_.load(std::memory_order_acquire);
    }

    bool empty() const { return size() == 0; }

private:
    std::size_t capacity_;
    std::size_t mask_;
    std::vector<T> slots_;

    // head and tail live on separate cache lines: otherwise the producer's store
    // to tail and the consumer's store to head keep invalidating the same line
    // on the other core (false sharing). Each index shares its line with the
    // cache used by the same thread, so a hot push/pop touches only its own line.
    alignas(kCacheLine) std::atomic<std::size_t> head_{0};
    std::size_t tail_cache_ = 0;  // consumer-only copy of tail
    alignas(kCacheLine) std::atomic<std::size_t> tail_{0};
    std::size_t head_cache_ = 0;  // producer-only copy of head
};

}  // namespace spsc
