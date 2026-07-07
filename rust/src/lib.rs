//! Bounded, wait-free single-producer/single-consumer ring buffer.
//!
//! [`channel`] returns a [`Producer`] / [`Consumer`] pair that share a fixed-size
//! buffer. Each is `Send` but not `Sync`, so the single-producer/single-consumer
//! contract is checked at compile time.

use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::Deref;

use sync::{Arc, AtomicUsize, Ordering, UnsafeCell};

#[cfg(not(loom))]
mod sync {
    pub use std::sync::atomic::{AtomicUsize, Ordering};
    pub use std::sync::Arc;

    // std's UnsafeCell with the with/with_mut access API loom's cell uses, so the
    // hot path is identical in both builds.
    #[derive(Debug)]
    #[repr(transparent)]
    pub struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

    impl<T> UnsafeCell<T> {
        #[inline]
        pub fn new(value: T) -> Self {
            Self(std::cell::UnsafeCell::new(value))
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

#[repr(align(64))]
struct CachePadded<T>(T);

impl<T> Deref for CachePadded<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.0
    }
}

type Slot<T> = UnsafeCell<MaybeUninit<T>>;

struct Ring<T> {
    slots: Box<[Slot<T>]>,
    mask: usize,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
}

impl<T> Ring<T> {
    fn with_capacity(capacity: usize) -> Self {
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
        }
    }
}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        let mut head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        while head != tail {
            self.slots[head & self.mask].with_mut(|p| unsafe { (*p).assume_init_drop() });
            head = head.wrapping_add(1);
        }
    }
}

/// Create a ring holding at least `capacity` elements (rounded up to a power of
/// two) and split it into its two endpoints.
pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let ring = Arc::new(Ring::with_capacity(capacity));
    let slots = ring.slots.as_ptr();
    let mask = ring.mask;
    let producer = Producer {
        slots,
        mask,
        tail: 0,
        head_cache: 0,
        ring: Arc::clone(&ring),
        _marker: PhantomData,
    };
    let consumer = Consumer {
        slots,
        mask,
        head: 0,
        tail_cache: 0,
        ring,
        _marker: PhantomData,
    };
    (producer, consumer)
}

/// The producing endpoint of a [`channel`].
pub struct Producer<T> {
    slots: *const Slot<T>,
    mask: usize,
    tail: usize,       // own index, mirrors ring.tail
    head_cache: usize, // last observed consumer index
    ring: Arc<Ring<T>>,
    _marker: PhantomData<T>,
}

/// The consuming endpoint of a [`channel`].
pub struct Consumer<T> {
    slots: *const Slot<T>,
    mask: usize,
    head: usize,       // own index, mirrors ring.head
    tail_cache: usize, // last observed producer index
    ring: Arc<Ring<T>>,
    _marker: PhantomData<T>,
}

// The raw slot pointer aliases the Arc'd buffer, which stays alive for the
// handle's lifetime. Each handle is owned by a single thread (the raw pointer
// keeps it !Sync); only T needs to be Send to cross threads.
unsafe impl<T: Send> Send for Producer<T> {}
unsafe impl<T: Send> Send for Consumer<T> {}

impl<T> Producer<T> {
    /// Push one element, handing it back as `Err(item)` when the ring is full.
    #[inline]
    pub fn push(&mut self, item: T) -> Result<(), T> {
        let capacity = self.mask + 1;
        if self.tail.wrapping_sub(self.head_cache) == capacity {
            self.head_cache = self.ring.head.load(Ordering::Acquire);
            if self.tail.wrapping_sub(self.head_cache) == capacity {
                return Err(item);
            }
        }
        let slot = unsafe { &*self.slots.add(self.tail & self.mask) };
        slot.with_mut(|p| unsafe { (*p).write(item) });
        self.tail = self.tail.wrapping_add(1);
        self.ring.tail.store(self.tail, Ordering::Release);
        Ok(())
    }

    /// Number of elements currently queued.
    #[inline]
    pub fn len(&self) -> usize {
        self.tail
            .wrapping_sub(self.ring.head.load(Ordering::Acquire))
    }

    /// Copy as many leading elements of `items` as fit, returning how many were
    /// pushed. The whole batch is published with a single release store.
    pub fn push_slice(&mut self, items: &[T]) -> usize
    where
        T: Copy,
    {
        let capacity = self.mask + 1;
        let mut free = capacity - self.tail.wrapping_sub(self.head_cache);
        if free < items.len() {
            self.head_cache = self.ring.head.load(Ordering::Acquire);
            free = capacity - self.tail.wrapping_sub(self.head_cache);
        }
        let n = items.len().min(free);
        // Two contiguous spans around the wrap point; Slot<T> is layout-transparent
        // over T. Under loom the copy goes through the modelled cells instead.
        #[cfg(not(loom))]
        unsafe {
            let base = self.slots as *mut Slot<T> as *mut T;
            let start = self.tail & self.mask;
            let first = n.min(self.mask + 1 - start);
            std::ptr::copy_nonoverlapping(items.as_ptr(), base.add(start), first);
            std::ptr::copy_nonoverlapping(items.as_ptr().add(first), base, n - first);
        }
        #[cfg(loom)]
        for (i, &item) in items[..n].iter().enumerate() {
            let slot = unsafe { &*self.slots.add(self.tail.wrapping_add(i) & self.mask) };
            slot.with_mut(|p| unsafe { (*p).write(item) });
        }
        if n > 0 {
            self.tail = self.tail.wrapping_add(n);
            self.ring.tail.store(self.tail, Ordering::Release);
        }
        n
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }
}

impl<T> Consumer<T> {
    /// Pop one element, or `None` when the ring is empty.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.head == self.tail_cache {
            self.tail_cache = self.ring.tail.load(Ordering::Acquire);
            if self.head == self.tail_cache {
                return None;
            }
        }
        let slot = unsafe { &*self.slots.add(self.head & self.mask) };
        let item = slot.with_mut(|p| unsafe { (*p).assume_init_read() });
        self.head = self.head.wrapping_add(1);
        self.ring.head.store(self.head, Ordering::Release);
        Some(item)
    }

    /// Copy up to `out.len()` elements into `out`, returning how many were
    /// popped. The whole batch is released with a single store.
    pub fn pop_slice(&mut self, out: &mut [T]) -> usize
    where
        T: Copy,
    {
        let mut avail = self.tail_cache.wrapping_sub(self.head);
        if avail < out.len() {
            self.tail_cache = self.ring.tail.load(Ordering::Acquire);
            avail = self.tail_cache.wrapping_sub(self.head);
        }
        let n = out.len().min(avail);
        #[cfg(not(loom))]
        unsafe {
            let base = self.slots as *const T;
            let start = self.head & self.mask;
            let first = n.min(self.mask + 1 - start);
            std::ptr::copy_nonoverlapping(base.add(start), out.as_mut_ptr(), first);
            std::ptr::copy_nonoverlapping(base, out.as_mut_ptr().add(first), n - first);
        }
        #[cfg(loom)]
        for (i, out_slot) in out[..n].iter_mut().enumerate() {
            let slot = unsafe { &*self.slots.add(self.head.wrapping_add(i) & self.mask) };
            *out_slot = slot.with_mut(|p| unsafe { (*p).assume_init_read() });
        }
        if n > 0 {
            self.head = self.head.wrapping_add(n);
            self.ring.head.store(self.head, Ordering::Release);
        }
        n
    }

    /// Number of elements currently available to pop.
    #[inline]
    pub fn len(&self) -> usize {
        self.ring
            .tail
            .load(Ordering::Acquire)
            .wrapping_sub(self.head)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_rounds_up_to_power_of_two() {
        assert_eq!(channel::<u64>(3).0.capacity(), 4);
        assert_eq!(channel::<u64>(16).0.capacity(), 16);
        assert_eq!(channel::<u64>(17).0.capacity(), 32);
    }

    #[test]
    fn push_until_full_then_pop_until_empty() {
        let (mut tx, mut rx) = channel::<u64>(4);
        assert!(rx.is_empty());
        for i in 0..4 {
            assert!(tx.push(i).is_ok());
        }
        assert_eq!(tx.push(99), Err(99));
        assert_eq!(tx.len(), 4);
        for i in 0..4 {
            assert_eq!(rx.pop(), Some(i));
        }
        assert_eq!(rx.pop(), None);
        assert!(rx.is_empty());
    }

    #[test]
    fn handles_non_copy_payload() {
        let (mut tx, mut rx) = channel::<String>(4);
        assert!(tx.push("hello".to_string()).is_ok());
        assert!(tx.push("world".to_string()).is_ok());
        assert_eq!(rx.pop().as_deref(), Some("hello"));
        assert_eq!(rx.pop().as_deref(), Some("world"));
        assert!(rx.pop().is_none());
    }

    #[test]
    fn drops_each_element_exactly_once() {
        use std::sync::atomic::AtomicUsize as StdAtomicUsize;
        use std::sync::Arc as StdArc;

        struct Counted(StdArc<StdAtomicUsize>);
        impl Drop for Counted {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let drops = StdArc::new(StdAtomicUsize::new(0));
        {
            let (mut tx, mut rx) = channel::<Counted>(8);
            for _ in 0..5 {
                assert!(tx.push(Counted(StdArc::clone(&drops))).is_ok());
            }
            drop(rx.pop().unwrap());
            drop(rx.pop().unwrap());
            assert_eq!(drops.load(Ordering::SeqCst), 2);
        }
        assert_eq!(drops.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn wraps_around_the_buffer() {
        let (mut tx, mut rx) = channel::<u64>(4);
        for round in 0..1000 {
            assert!(tx.push(round).is_ok());
            assert_eq!(rx.pop(), Some(round));
        }
        assert!(rx.is_empty());
    }

    #[test]
    fn slice_roundtrip_with_wrap() {
        let (mut tx, mut rx) = channel::<u64>(4);
        assert!(tx.push(0).is_ok());
        assert_eq!(rx.pop(), Some(0));

        assert_eq!(tx.push_slice(&[1, 2, 3, 4, 5]), 4);
        assert_eq!(tx.push_slice(&[9]), 0);
        let mut buf = [0u64; 8];
        assert_eq!(rx.pop_slice(&mut buf), 4);
        assert_eq!(&buf[..4], &[1, 2, 3, 4]);
        assert_eq!(rx.pop_slice(&mut buf), 0);
        assert_eq!(tx.push_slice(&[]), 0);
    }

    #[test]
    fn slices_interleave_with_single_ops() {
        let (mut tx, mut rx) = channel::<u64>(8);
        assert!(tx.push(1).is_ok());
        assert_eq!(tx.push_slice(&[2, 3, 4]), 3);
        assert_eq!(rx.pop(), Some(1));
        let mut buf = [0u64; 2];
        assert_eq!(rx.pop_slice(&mut buf), 2);
        assert_eq!(buf, [2, 3]);
        assert_eq!(rx.pop(), Some(4));
        assert!(rx.is_empty());
    }

    #[test]
    fn slice_stress_across_threads() {
        use std::thread;

        const N: u64 = 1_000_000;
        let (mut tx, mut rx) = channel::<u64>(1024);

        let producer = thread::spawn(move || {
            let mut next = 0u64;
            let mut chunk = [0u64; 64];
            while next < N {
                let want = ((N - next).min(64)) as usize;
                for (i, c) in chunk[..want].iter_mut().enumerate() {
                    *c = next + i as u64;
                }
                let mut sent = 0;
                while sent < want {
                    let pushed = tx.push_slice(&chunk[sent..want]);
                    if pushed == 0 {
                        std::hint::spin_loop();
                    }
                    sent += pushed;
                }
                next += want as u64;
            }
        });

        let mut expected = 0u64;
        let mut buf = [0u64; 64];
        while expected < N {
            let n = rx.pop_slice(&mut buf);
            for &v in &buf[..n] {
                assert_eq!(v, expected);
                expected += 1;
            }
            if n == 0 {
                std::hint::spin_loop();
            }
        }
        producer.join().unwrap();
    }

    #[test]
    fn single_producer_single_consumer_threads() {
        use std::thread;

        const N: u64 = 1_000_000;
        let (mut tx, mut rx) = channel::<u64>(1024);

        let producer = thread::spawn(move || {
            for i in 0..N {
                while tx.push(i).is_err() {
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
}
