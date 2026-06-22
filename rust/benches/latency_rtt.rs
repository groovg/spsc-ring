//! Ping-pong round-trip latency. The main thread writes a token into one ring and
//! spins until an echo thread bounces it back through a second ring. The reported
//! number is the full round trip; halve it for one-way hand-off latency.
//!
//! Both threads busy-spin, so this measures the queue plus cross-core cache
//! coherency, not scheduler wakeups. Run it on a quiet machine with the two
//! threads pinned to sibling cores for stable tails.

use std::thread;

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_spsc_ring(c: &mut Criterion) {
    let (ping_tx, ping_rx) = spsc_ring::channel::<u64>(2);
    let (pong_tx, pong_rx) = spsc_ring::channel::<u64>(2);

    let echo = thread::spawn(move || loop {
        match ping_rx.pop() {
            Some(u64::MAX) => break,
            Some(v) => {
                while pong_tx.push(v).is_err() {
                    std::hint::spin_loop();
                }
            }
            None => std::hint::spin_loop(),
        }
    });

    c.bench_function("rtt/spsc-ring", |b| {
        b.iter(|| {
            while ping_tx.push(1).is_err() {
                std::hint::spin_loop();
            }
            loop {
                if pong_rx.pop().is_some() {
                    break;
                }
                std::hint::spin_loop();
            }
        })
    });

    while ping_tx.push(u64::MAX).is_err() {
        std::hint::spin_loop();
    }
    echo.join().unwrap();
}

fn bench_rtrb(c: &mut Criterion) {
    let (mut ping_tx, mut ping_rx) = rtrb::RingBuffer::<u64>::new(2);
    let (mut pong_tx, mut pong_rx) = rtrb::RingBuffer::<u64>::new(2);

    let echo = thread::spawn(move || loop {
        match ping_rx.pop() {
            Ok(u64::MAX) => break,
            Ok(v) => {
                while pong_tx.push(v).is_err() {
                    std::hint::spin_loop();
                }
            }
            Err(_) => std::hint::spin_loop(),
        }
    });

    c.bench_function("rtt/rtrb", |b| {
        b.iter(|| {
            while ping_tx.push(1).is_err() {
                std::hint::spin_loop();
            }
            loop {
                if pong_rx.pop().is_ok() {
                    break;
                }
                std::hint::spin_loop();
            }
        })
    });

    while ping_tx.push(u64::MAX).is_err() {
        std::hint::spin_loop();
    }
    echo.join().unwrap();
}

criterion_group!(benches, bench_spsc_ring, bench_rtrb);
criterion_main!(benches);
