//! Bounded, wait-free single-producer/single-consumer ring buffer.
//!
//! One thread pushes, exactly one other pops. Capacity is fixed at construction
//! and rounded up to a power of two so the slot index is a bitmask of a free-running
//! counter rather than a modulo.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Ring<T> {
    slots: Box<[UnsafeCell<T>]>,
    mask: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
}

// A single producer and a single consumer touch disjoint indices, so the only
// shared mutation is through the two atomics. T must be Send to cross the threads.
unsafe impl<T: Send> Sync for Ring<T> {}
unsafe impl<T: Send> Send for Ring<T> {}

impl<T: Copy + Default> Ring<T> {
    /// Create a ring that can hold at least `capacity` elements. The real capacity
    /// is `capacity` rounded up to the next power of two.
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be non-zero");
        let cap = capacity.next_power_of_two();
        let slots = (0..cap)
            .map(|_| UnsafeCell::new(T::default()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            slots,
            mask: cap - 1,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }

    /// Push one element. Returns `false` if the ring is full.
    pub fn push(&self, item: T) -> bool {
        let tail = self.tail.load(Ordering::SeqCst);
        let head = self.head.load(Ordering::SeqCst);
        if tail.wrapping_sub(head) == self.capacity() {
            return false;
        }
        unsafe {
            *self.slots[tail & self.mask].get() = item;
        }
        self.tail.store(tail.wrapping_add(1), Ordering::SeqCst);
        true
    }

    /// Pop one element. Returns `None` if the ring is empty.
    pub fn pop(&self) -> Option<T> {
        let head = self.head.load(Ordering::SeqCst);
        let tail = self.tail.load(Ordering::SeqCst);
        if head == tail {
            return None;
        }
        let item = unsafe { *self.slots[head & self.mask].get() };
        self.head.store(head.wrapping_add(1), Ordering::SeqCst);
        Some(item)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::SeqCst) == self.tail.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn len(&self) -> usize {
        let tail = self.tail.load(Ordering::SeqCst);
        let head = self.head.load(Ordering::SeqCst);
        tail.wrapping_sub(head)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_rounds_up_to_power_of_two() {
        assert_eq!(Ring::<u64>::with_capacity(3).capacity(), 4);
        assert_eq!(Ring::<u64>::with_capacity(16).capacity(), 16);
        assert_eq!(Ring::<u64>::with_capacity(17).capacity(), 32);
    }

    #[test]
    fn push_until_full_then_pop_until_empty() {
        let ring = Ring::<u64>::with_capacity(4);
        assert_eq!(ring.capacity(), 4);
        assert!(ring.is_empty());

        for i in 0..4 {
            assert!(ring.push(i), "push {i} should succeed");
        }
        assert!(!ring.push(99), "push into a full ring must fail");
        assert_eq!(ring.len(), 4);

        for i in 0..4 {
            assert_eq!(ring.pop(), Some(i));
        }
        assert_eq!(ring.pop(), None);
        assert!(ring.is_empty());
    }

    #[test]
    fn wraps_around_the_buffer() {
        let ring = Ring::<u64>::with_capacity(4);
        // Cycle through far more elements than the capacity to exercise the
        // free-running counters wrapping over the masked index.
        for round in 0..1000 {
            assert!(ring.push(round));
            assert_eq!(ring.pop(), Some(round));
        }
        assert!(ring.is_empty());
    }
}
