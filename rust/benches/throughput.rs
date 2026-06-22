//! Sustained two-thread throughput: a producer pins values into the ring while a
//! consumer drains them. Measures the same workload across this crate, `rtrb`,
//! and `crossbeam`'s `ArrayQueue` so the numbers are directly comparable.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

const ITEMS: u64 = 4_000_000;
const CAPACITY: usize = 1024;

fn spsc_ring_run() -> Duration {
    let (tx, rx) = spsc_ring::channel::<u64>(CAPACITY);
    let start = Instant::now();
    let producer = thread::spawn(move || {
        for i in 0..ITEMS {
            let mut item = i;
            while let Err(returned) = tx.push(item) {
                item = returned;
                std::hint::spin_loop();
            }
        }
    });
    let mut received = 0u64;
    while received < ITEMS {
        if rx.pop().is_some() {
            received += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    producer.join().unwrap();
    start.elapsed()
}

fn rtrb_run() -> Duration {
    let (mut tx, mut rx) = rtrb::RingBuffer::<u64>::new(CAPACITY);
    let start = Instant::now();
    let producer = thread::spawn(move || {
        for i in 0..ITEMS {
            while tx.push(i).is_err() {
                std::hint::spin_loop();
            }
        }
    });
    let mut received = 0u64;
    while received < ITEMS {
        if rx.pop().is_ok() {
            received += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    producer.join().unwrap();
    start.elapsed()
}

fn crossbeam_run() -> Duration {
    let q = Arc::new(crossbeam_queue::ArrayQueue::<u64>::new(CAPACITY));
    let start = Instant::now();
    let producer = {
        let q = Arc::clone(&q);
        thread::spawn(move || {
            for i in 0..ITEMS {
                while q.push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        })
    };
    let mut received = 0u64;
    while received < ITEMS {
        if q.pop().is_some() {
            received += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    producer.join().unwrap();
    start.elapsed()
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Elements(ITEMS));

    group.bench_function("spsc-ring", |b| {
        b.iter_custom(|iters| (0..iters).map(|_| spsc_ring_run()).sum())
    });
    group.bench_function("rtrb", |b| {
        b.iter_custom(|iters| (0..iters).map(|_| rtrb_run()).sum())
    });
    group.bench_function("crossbeam-ArrayQueue", |b| {
        b.iter_custom(|iters| (0..iters).map(|_| crossbeam_run()).sum())
    });

    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
