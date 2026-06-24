//! Cross-thread conservation: everything pushed comes out exactly once, in order,
//! across many full/empty transitions.

use std::thread;

use spsc_ring::channel;

#[test]
fn conservation_over_many_wraparounds() {
    // Tiny ring, large item count: both sides spend most of their time at the
    // full/empty boundary.
    const N: u64 = 20_000_000;
    let (mut tx, mut rx) = channel::<u64>(16);

    let producer = thread::spawn(move || {
        for i in 0..N {
            while tx.push(i).is_err() {
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
    const N: usize = 200_000;
    let (mut tx, mut rx) = channel::<Box<usize>>(8);

    let producer = thread::spawn(move || {
        for i in 0..N {
            let mut item = Box::new(i);
            while let Err(returned) = tx.push(item) {
                item = returned;
                std::hint::spin_loop();
            }
        }
    });

    let consumer = thread::spawn(move || {
        let mut expected = 0usize;
        while expected < N {
            match rx.pop() {
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
