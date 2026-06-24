//! Bounded, wait-free single-producer/single-consumer ring buffer.
//!
//! One thread pushes, exactly one other pops. Capacity is fixed at construction
//! and rounded up to a power of two so the slot index is a bitmask of a free-running
//! counter rather than a modulo.

use std::cell::Cell;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::Deref;

use sync::{Arc, AtomicUsize, Ordering, UnsafeCell};

#[cfg(not(loom))]
mod sync {
    pub use std::sync::atomic::{AtomicUsize, Ordering};
    pub use std::sync::Arc;

    /// Gives `std`'s `UnsafeCell` the same `with`/`with_mut` access API that
    /// `loom`'s instrumented cell exposes, so the ring body is byte-for-byte the
    /// same in the real build and under the loom model.
    #[derive(Debug)]
    pub struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

    impl<T> UnsafeCell<T> {
        #[inline]
        pub fn new(value: T) -> Self {
            Self(std::cell::UnsafeCell::new(value))
        }

        #[inline]
        pub fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
            f(self.0.get())
        }

        #[inline]
        pub fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
            f(self.0.get())
        }
    }
}

#[cfg(loom)]
mod sync {
    pub use loom::cell::UnsafeCell;
    pub use loom::sync::atomic::{AtomicUsize, Ordering};
    pub use loom::sync::Arc;
}

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
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
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

impl<T> Ring<T> {
    /// Create a ring that can hold at least `capacity` elements. The real capacity
    /// is `capacity` rounded up to the next power of two.
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be non-zero");
        let cap = capacity.next_power_of_two();
        let slots = (0..cap)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
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

    /// Push one element. Returns `Err(item)` (handing the value back) if the ring
    /// is full.
    ///
    /// Only the producer writes `tail`, so its own value can be read `Relaxed`.
    /// The free space is checked against the cached `head` first; the real `head`
    /// atomic (loaded `Acquire`, synchronizing with the consumer's release of a
    /// freed slot) is only read when the cache claims the ring is full. The new
    /// `tail` is published `Release` so the element store is visible to the
    /// consumer's matching `Acquire` load.
    pub fn push(&self, item: T) -> Result<(), T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let mut head = self.head_cache.with(|p| unsafe { *p });
        if tail.wrapping_sub(head) == self.capacity() {
            head = self.head.load(Ordering::Acquire);
            self.head_cache.with_mut(|p| unsafe { *p = head });
            if tail.wrapping_sub(head) == self.capacity() {
                return Err(item);
            }
        }
        self.slots[tail & self.mask].with_mut(|p| unsafe { (*p).write(item) });
        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Pop one element. Returns `None` if the ring is empty.
    ///
    /// Mirror of [`push`](Self::push): `head` is the consumer's own index
    /// (`Relaxed`), emptiness is checked against the cached `tail` first, and the
    /// real `tail` atomic is loaded `Acquire` (observing the producer's published
    /// element) only when the cache claims the ring is empty.
    pub fn pop(&self) -> Option<T> {
        let head = self.head.load(Ordering::Relaxed);
        let mut tail = self.tail_cache.with(|p| unsafe { *p });
        if head == tail {
            tail = self.tail.load(Ordering::Acquire);
            self.tail_cache.with_mut(|p| unsafe { *p = tail });
            if head == tail {
                return None;
            }
        }
        let item = self.slots[head & self.mask].with_mut(|p| unsafe { (*p).assume_init_read() });
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

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        // Slots in `[head, tail)` were written but never popped; drop them.
        // Uninitialized slots are left untouched. `drop` has exclusive access, so
        // a relaxed load of each index is enough.
        let mut head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        while head != tail {
            self.slots[head & self.mask].with_mut(|p| unsafe { (*p).assume_init_drop() });
            head = head.wrapping_add(1);
        }
    }
}

/// Create a ring with the given capacity and split it into the two endpoints.
///
/// [`Producer`] and [`Consumer`] are each `Send` but not `Sync`, so the compiler
/// enforces the single-producer/single-consumer contract: each endpoint lives on
/// exactly one thread.
pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let ring = Arc::new(Ring::with_capacity(capacity));
    (
        Producer {
            ring: Arc::clone(&ring),
            _not_sync: PhantomData,
        },
        Consumer {
            ring,
            _not_sync: PhantomData,
        },
    )
}

/// The producing endpoint of a [`channel`]. Owns the sole right to `push`.
pub struct Producer<T> {
    ring: Arc<Ring<T>>,
    _not_sync: PhantomData<Cell<()>>,
}

/// The consuming endpoint of a [`channel`]. Owns the sole right to `pop`.
pub struct Consumer<T> {
    ring: Arc<Ring<T>>,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T> Producer<T> {
    /// Push one element, handing it back as `Err(item)` when the ring is full.
    #[inline]
    pub fn push(&self, item: T) -> Result<(), T> {
        self.ring.push(item)
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.ring.len() == self.ring.capacity()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ring.len()
    }
}

impl<T> Consumer<T> {
    /// Pop one element, or `None` when the ring is empty.
    #[inline]
    pub fn pop(&self) -> Option<T> {
        self.ring.pop()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ring.len()
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
            assert!(ring.push(i).is_ok(), "push {i} should succeed");
        }
        assert_eq!(
            ring.push(99),
            Err(99),
            "push into a full ring hands the value back"
        );
        assert_eq!(ring.len(), 4);

        for i in 0..4 {
            assert_eq!(ring.pop(), Some(i));
        }
        assert_eq!(ring.pop(), None);
        assert!(ring.is_empty());
    }

    #[test]
    fn handles_non_copy_payload() {
        let ring = Ring::<String>::with_capacity(4);
        assert!(ring.push("hello".to_string()).is_ok());
        assert!(ring.push("world".to_string()).is_ok());
        assert_eq!(ring.pop().as_deref(), Some("hello"));
        assert_eq!(ring.pop().as_deref(), Some("world"));
        assert!(ring.pop().is_none());
    }

    #[test]
    fn drops_each_element_exactly_once() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        struct Counted(Arc<AtomicUsize>);
        impl Drop for Counted {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        {
            let ring = Ring::<Counted>::with_capacity(8);
            for _ in 0..5 {
                assert!(ring.push(Counted(Arc::clone(&drops))).is_ok());
            }
            // Two are popped (dropped here); three are left for Ring::drop.
            drop(ring.pop().unwrap());
            drop(ring.pop().unwrap());
            assert_eq!(drops.load(Ordering::SeqCst), 2);
        }
        assert_eq!(drops.load(Ordering::SeqCst), 5);
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
                    while ring.push(i).is_err() {
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
    fn channel_split_round_trip() {
        use std::thread;

        const N: u64 = 500_000;
        let (tx, rx) = channel::<u64>(256);

        let producer = thread::spawn(move || {
            for i in 0..N {
                let mut item = i;
                while let Err(returned) = tx.push(item) {
                    item = returned;
                    std::hint::spin_loop();
                }
            }
        });

        let consumer = thread::spawn(move || {
            let mut next = 0u64;
            while next < N {
                match rx.pop() {
                    Some(v) => {
                        assert_eq!(v, next);
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
            assert!(ring.push(round).is_ok());
            assert_eq!(ring.pop(), Some(round));
        }
        assert!(ring.is_empty());
    }
}
