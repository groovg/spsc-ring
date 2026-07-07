//! Exhaustive interleaving check of the ring's atomics under loom.
//!
//! loom replaces the standard atomics with a model that explores every legal
//! ordering of the producer's and consumer's operations, so this is a proof (for
//! the modelled size) that the acquire/release edges never let the consumer read
//! a slot before the producer has published it. Run with:
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test --release --test loom
//! ```

#![cfg(loom)]

use loom::thread;
use spsc_ring::channel;

#[test]
fn batch_publish_wraps_and_stays_ordered() {
    loom::model(|| {
        let (mut tx, mut rx) = channel::<usize>(2);
        // Advance both indices so the batch below straddles the wrap point.
        tx.push(9).expect("empty ring");
        assert_eq!(rx.pop(), Some(9));

        let producer = thread::spawn(move || {
            assert_eq!(tx.push_slice(&[1, 2]), 2);
        });

        let mut got = Vec::new();
        let mut buf = [0usize; 2];
        while got.len() < 2 {
            let n = rx.pop_slice(&mut buf);
            got.extend_from_slice(&buf[..n]);
            if n == 0 {
                loom::thread::yield_now();
            }
        }

        producer.join().unwrap();
        assert_eq!(got, [1, 2]);
    });
}

#[test]
fn spsc_publishes_in_order() {
    loom::model(|| {
        // Capacity 2 holds both items, so the producer never blocks; the consumer
        // is the only side that spins. This keeps the state space tractable while
        // still exercising the publish/consume happens-before edge.
        let (mut tx, mut rx) = channel::<usize>(2);

        let producer = thread::spawn(move || {
            for i in 0..2 {
                tx.push(i).expect("ring has room for both items");
            }
        });

        let mut received = Vec::new();
        while received.len() < 2 {
            match rx.pop() {
                Some(v) => received.push(v),
                None => loom::thread::yield_now(),
            }
        }

        producer.join().unwrap();
        assert_eq!(received, [0, 1]);
    });
}
