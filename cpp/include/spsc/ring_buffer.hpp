#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstring>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace spsc {

// Hardcoded to 64 (every current x86-64 and AArch64 line) rather than
// std::hardware_destructive_interference_size, which gcc warns can change between
// compiler versions and is unsafe to bake into anything ABI-facing.
inline constexpr std::size_t kCacheLine = 64;

template <typename T>
class Producer;
template <typename T>
class Consumer;
template <typename T>
std::pair<Producer<T>, Consumer<T>> channel(std::size_t capacity);

namespace detail {

// Raw storage for one T: push placement-news into it, pop moves out (and destroys
// it for non-trivial T), so T is never default-constructed.
template <typename T>
union Slot {
    T value;
    Slot() {}
    ~Slot() {}
};

// The shared buffer plus the two published indices. Owned jointly by the two
// handles; only the atomics cross threads.
template <typename T>
struct Ring {
    explicit Ring(std::size_t requested)
        : cap(std::bit_ceil(requested == 0 ? std::size_t{1} : requested)),
          mask(cap - 1),
          slots(cap),
          base(slots.data()) {}

    ~Ring() {
        if constexpr (!std::is_trivially_destructible_v<T>) {
            std::size_t head = head_idx.load(std::memory_order_relaxed);
            const std::size_t tail = tail_idx.load(std::memory_order_relaxed);
            while (head != tail) {
                base[head & mask].value.~T();
                ++head;
            }
        }
    }

    Ring(const Ring&) = delete;
    Ring& operator=(const Ring&) = delete;

    std::size_t cap;
    std::size_t mask;
    std::vector<Slot<T>> slots;
    Slot<T>* base;
    alignas(kCacheLine) std::atomic<std::size_t> head_idx{0};
    alignas(kCacheLine) std::atomic<std::size_t> tail_idx{0};
};

}  // namespace detail

// Bounded, wait-free single-producer/single-consumer ring buffer.
//
// `channel<T>(capacity)` returns a Producer/Consumer pair sharing a fixed buffer
// (capacity rounded up to a power of two). The producing endpoint owns its own
// index, the cached consumer index, and a raw pointer to the buffer, so push
// never reads the shared atomics for its own bookkeeping.
template <typename T>
class Producer {
public:
    Producer(Producer&&) noexcept = default;
    Producer& operator=(Producer&&) noexcept = default;
    Producer(const Producer&) = delete;
    Producer& operator=(const Producer&) = delete;

    bool push(const T& item) { return emplace(item); }
    bool push(T&& item) { return emplace(std::move(item)); }

    // Copy as many leading items as fit, publishing the whole batch with a single
    // release store. Returns the number pushed.
    std::size_t push_n(const T* items, std::size_t count) {
        static_assert(std::is_trivially_copyable_v<T>,
                      "push_n requires a trivially copyable T; use push");
        static_assert(sizeof(detail::Slot<T>) == sizeof(T));
        const std::size_t capacity = mask_ + 1;
        std::size_t free = capacity - (tail_ - head_cache_);
        if (free < count) {
            head_cache_ = ring_->head_idx.load(std::memory_order_acquire);
            free = capacity - (tail_ - head_cache_);
        }
        const std::size_t n = count < free ? count : free;
        if (n == 0) {
            return 0;
        }
        const std::size_t start = tail_ & mask_;
        const std::size_t first = n < capacity - start ? n : capacity - start;
        std::memcpy(&base_[start].value, items, first * sizeof(T));
        std::memcpy(&base_[0].value, items + first, (n - first) * sizeof(T));
        tail_ += n;
        ring_->tail_idx.store(tail_, std::memory_order_release);
        return n;
    }

    std::size_t capacity() const { return mask_ + 1; }
    std::size_t size() const { return tail_ - ring_->head_idx.load(std::memory_order_acquire); }
    bool full() const { return size() == capacity(); }

private:
    explicit Producer(std::shared_ptr<detail::Ring<T>> ring)
        : ring_(std::move(ring)), base_(ring_->base), mask_(ring_->mask) {}

    template <typename U>
    bool emplace(U&& item) {
        if (tail_ - head_cache_ == mask_ + 1) {
            head_cache_ = ring_->head_idx.load(std::memory_order_acquire);
            if (tail_ - head_cache_ == mask_ + 1) {
                return false;
            }
        }
        ::new (&base_[tail_ & mask_].value) T(std::forward<U>(item));
        tail_ += 1;
        ring_->tail_idx.store(tail_, std::memory_order_release);
        return true;
    }

    template <typename U>
    friend std::pair<Producer<U>, Consumer<U>> channel(std::size_t);

    std::shared_ptr<detail::Ring<T>> ring_;
    detail::Slot<T>* base_;
    std::size_t mask_;
    std::size_t tail_ = 0;
    std::size_t head_cache_ = 0;
};

template <typename T>
class Consumer {
public:
    Consumer(Consumer&&) noexcept = default;
    Consumer& operator=(Consumer&&) noexcept = default;
    Consumer(const Consumer&) = delete;
    Consumer& operator=(const Consumer&) = delete;

    // Pop one element into out. Returns false if the ring is empty.
    bool pop(T& out) {
        if (head_ == tail_cache_) {
            tail_cache_ = ring_->tail_idx.load(std::memory_order_acquire);
            if (head_ == tail_cache_) {
                return false;
            }
        }
        T& slot = base_[head_ & mask_].value;
        out = std::move(slot);
        if constexpr (!std::is_trivially_destructible_v<T>) {
            slot.~T();
        }
        head_ += 1;
        ring_->head_idx.store(head_, std::memory_order_release);
        return true;
    }

    // Copy up to `count` items into out, releasing the whole batch with a single
    // store. Returns the number popped.
    std::size_t pop_n(T* out, std::size_t count) {
        static_assert(std::is_trivially_copyable_v<T>,
                      "pop_n requires a trivially copyable T; use pop");
        static_assert(sizeof(detail::Slot<T>) == sizeof(T));
        std::size_t avail = tail_cache_ - head_;
        if (avail < count) {
            tail_cache_ = ring_->tail_idx.load(std::memory_order_acquire);
            avail = tail_cache_ - head_;
        }
        const std::size_t n = count < avail ? count : avail;
        if (n == 0) {
            return 0;
        }
        const std::size_t capacity = mask_ + 1;
        const std::size_t start = head_ & mask_;
        const std::size_t first = n < capacity - start ? n : capacity - start;
        std::memcpy(out, &base_[start].value, first * sizeof(T));
        std::memcpy(out + first, &base_[0].value, (n - first) * sizeof(T));
        head_ += n;
        ring_->head_idx.store(head_, std::memory_order_release);
        return n;
    }

    std::size_t capacity() const { return mask_ + 1; }
    std::size_t size() const { return ring_->tail_idx.load(std::memory_order_acquire) - head_; }
    bool empty() const { return size() == 0; }

private:
    explicit Consumer(std::shared_ptr<detail::Ring<T>> ring)
        : ring_(std::move(ring)), base_(ring_->base), mask_(ring_->mask) {}

    template <typename U>
    friend std::pair<Producer<U>, Consumer<U>> channel(std::size_t);

    std::shared_ptr<detail::Ring<T>> ring_;
    detail::Slot<T>* base_;
    std::size_t mask_;
    std::size_t head_ = 0;
    std::size_t tail_cache_ = 0;
};

// Create a ring holding at least `capacity` elements (rounded up to a power of
// two) and split it into its two endpoints.
template <typename T>
std::pair<Producer<T>, Consumer<T>> channel(std::size_t capacity) {
    auto ring = std::make_shared<detail::Ring<T>>(capacity);
    return {Producer<T>(ring), Consumer<T>(ring)};
}

}  // namespace spsc
