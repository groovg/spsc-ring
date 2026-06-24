#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <vector>

namespace spsc {

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
    bool push(const T& item) {
        const std::size_t tail = tail_.load(std::memory_order_seq_cst);
        const std::size_t head = head_.load(std::memory_order_seq_cst);
        if (tail - head == capacity_) {
            return false;
        }
        slots_[tail & mask_] = item;
        tail_.store(tail + 1, std::memory_order_seq_cst);
        return true;
    }

    // Pop one element into out. Returns false if the ring is empty.
    bool pop(T& out) {
        const std::size_t head = head_.load(std::memory_order_seq_cst);
        const std::size_t tail = tail_.load(std::memory_order_seq_cst);
        if (head == tail) {
            return false;
        }
        out = slots_[head & mask_];
        head_.store(head + 1, std::memory_order_seq_cst);
        return true;
    }

    std::size_t capacity() const { return capacity_; }

    std::size_t size() const {
        return tail_.load(std::memory_order_seq_cst) -
               head_.load(std::memory_order_seq_cst);
    }

    bool empty() const { return size() == 0; }

private:
    std::size_t capacity_;
    std::size_t mask_;
    std::vector<T> slots_;
    std::atomic<std::size_t> head_{0};
    std::atomic<std::size_t> tail_{0};
};

}  // namespace spsc
