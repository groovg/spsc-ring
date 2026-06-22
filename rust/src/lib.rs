//! Bounded, wait-free single-producer/single-consumer ring buffer.
//!
//! One thread pushes, exactly one other pops. Capacity is fixed at construction
//! and rounded up to a power of two so the slot index is a bitmask of a free-running
//! counter rather than a modulo.

use std::cell::UnsafeCell;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Pads its contents to a full cache line so two fields written by different
/// cores never land on the same line. Without this, the producer's store to
/// `tail` and the consumer's store to `head` ping-pong the same line between
/// cores (false sharing).
#[repr(align(64))]
struct CachePadded<T>(T);

impl<T> Deref for CachePadded<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.0
    }
}

pub struct Ring<T> {
    slots: Box<[UnsafeCell<T>]>,
    mask: usize,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    // Each side caches the other index so the common case never touches the
    // remote atomic. `head_cache` is read/written only by the producer,
    // `tail_cache` only by the consumer — no cross-thread sharing.
    head_cache: CachePadded<UnsafeCell<usize>>,
    tail_cache: CachePadded<UnsafeCell<usize>>,
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
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            head_cache: CachePadded(UnsafeCell::new(0)),
            tail_cache: CachePadded(UnsafeCell::new(0)),
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }

    /// Push one element. Returns `false` if the ring is full.
    ///
    /// Only the producer writes `tail`, so its own value can be read `Relaxed`.
    /// The free space is checked against the cached `head` first; the real `head`
    /// atomic (loaded `Acquire`, synchronizing with the consumer's release of a
    /// freed slot) is only read when the cache claims the ring is full. The new
    /// `tail` is published `Release` so the element store is visible to the
    /// consumer's matching `Acquire` load.
    pub fn push(&self, item: T) -> bool {
        let tail = self.tail.load(Ordering::Relaxed);
        let mut head = unsafe { *self.head_cache.get() };
        if tail.wrapping_sub(head) == self.capacity() {
            head = self.head.load(Ordering::Acquire);
            unsafe { *self.head_cache.get() = head };
            if tail.wrapping_sub(head) == self.capacity() {
                return false;
            }
        }
        unsafe {
            *self.slots[tail & self.mask].get() = item;
        }
        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        true
    }

    /// Pop one element. Returns `None` if the ring is empty.
    ///
    /// Mirror of [`push`](Self::push): `head` is the consumer's own index
    /// (`Relaxed`), emptiness is checked against the cached `tail` first, and the
    /// real `tail` atomic is loaded `Acquire` (observing the producer's published
    /// element) only when the cache claims the ring is empty.
    pub fn pop(&self) -> Option<T> {
        let head = self.head.load(Ordering::Relaxed);
        let mut tail = unsafe { *self.tail_cache.get() };
        if head == tail {
            tail = self.tail.load(Ordering::Acquire);
            unsafe { *self.tail_cache.get() = tail };
            if head == tail {
                return None;
            }
        }
        let item = unsafe { *self.slots[head & self.mask].get() };
        self.head.store(head.wrapping_add(1), Ordering::Release);
        Some(item)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn len(&self) -> usize {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);
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
    fn single_producer_single_consumer_threads() {
        use std::sync::Arc;
        use std::thread;

        const N: u64 = 1_000_000;
        let ring = Arc::new(Ring::<u64>::with_capacity(1024));

        let producer = {
            let ring = Arc::clone(&ring);
            thread::spawn(move || {
                for i in 0..N {
                    while !ring.push(i) {
                        std::hint::spin_loop();
                    }
                }
            })
        };

        let consumer = thread::spawn(move || {
            let mut next = 0u64;
            while next < N {
                match ring.pop() {
                    Some(v) => {
                        assert_eq!(v, next, "values must arrive in order, no gaps or dupes");
                        next += 1;
                    }
                    None => std::hint::spin_loop(),
                }
            }
        });

        producer.join().unwrap();
        consumer.join().unwrap();
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
