//! Cross-thread conservation tests: everything pushed comes out exactly once, in
//! order, with no gaps or duplicates — across many full/empty transitions.

use std::sync::Arc;
use std::thread;

use spsc_ring::{channel, Ring};

#[test]
fn conservation_over_many_wraparounds() {
    // A deliberately tiny ring against a large item count, so the producer and
    // consumer spend most of their time at the full/empty boundaries.
    const N: u64 = 20_000_000;
    let (tx, rx) = channel::<u64>(16);

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
        let mut expected = 0u64;
        while expected < N {
            match rx.pop() {
                Some(v) => {
                    assert_eq!(v, expected, "value out of order");
                    expected = expected.wrapping_add(1);
                }
                None => std::hint::spin_loop(),
            }
        }
    });

    producer.join().unwrap();
    consumer.join().unwrap();
}

#[test]
fn conservation_with_heap_payload() {
    // Same idea with an owned, non-Copy payload to shake out any double-free or
    // leak in the MaybeUninit storage path.
    const N: usize = 200_000;
    let ring = Arc::new(Ring::<Box<usize>>::with_capacity(8));

    let producer = {
        let ring = Arc::clone(&ring);
        thread::spawn(move || {
            for i in 0..N {
                let mut item = Box::new(i);
                while let Err(returned) = ring.push(item) {
                    item = returned;
                    std::hint::spin_loop();
                }
            }
        })
    };

    let consumer = thread::spawn(move || {
        let mut expected = 0usize;
        while expected < N {
            match ring.pop() {
                Some(v) => {
                    assert_eq!(*v, expected);
                    expected += 1;
                }
                None => std::hint::spin_loop(),
            }
        }
    });

    producer.join().unwrap();
    consumer.join().unwrap();
}
