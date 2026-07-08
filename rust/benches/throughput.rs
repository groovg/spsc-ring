//! Sustained two-thread throughput, this crate vs rtrb vs crossbeam's ArrayQueue.
//!
//! Both threads are pinned to distinct physical cores on the same CCD, and the
//! channel allocation and thread spawn happen before the timed region (a barrier
//! starts the clock only once both sides are ready). Reports the median of 15 runs.

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const ITEMS: u64 = 50_000_000;
const CAPACITY: usize = 1024;
const BATCH: usize = 64;
const PRODUCER_CORE: usize = 0;
const CONSUMER_CORE: usize = 2;
const RUNS: usize = 15;

fn pin(core: usize) {
    if let Some(ids) = core_affinity::get_core_ids() {
        if let Some(id) = ids.into_iter().find(|c| c.id == core) {
            core_affinity::set_for_current(id);
        }
    }
}

fn median_mops(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    samples[samples.len() / 2]
}

fn run_spsc_ring() -> f64 {
    let (mut tx, mut rx) = spsc_ring::channel::<u64>(CAPACITY);
    let gate = Arc::new(Barrier::new(2));
    let producer = {
        let gate = Arc::clone(&gate);
        thread::spawn(move || {
            pin(PRODUCER_CORE);
            gate.wait();
            for i in 0..ITEMS {
                while tx.push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        })
    };
    pin(CONSUMER_CORE);
    gate.wait();
    let start = Instant::now();
    let mut received = 0u64;
    while received < ITEMS {
        if rx.pop().is_some() {
            received += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    let secs = start.elapsed().as_secs_f64();
    producer.join().unwrap();
    ITEMS as f64 / secs / 1e6
}

fn run_spsc_ring_batch() -> f64 {
    let (mut tx, mut rx) = spsc_ring::channel::<u64>(CAPACITY);
    let gate = Arc::new(Barrier::new(2));
    let producer = {
        let gate = Arc::clone(&gate);
        thread::spawn(move || {
            pin(PRODUCER_CORE);
            gate.wait();
            let mut chunk = [0u64; BATCH];
            let mut next = 0u64;
            while next < ITEMS {
                let want = ((ITEMS - next) as usize).min(BATCH);
                for (i, c) in chunk[..want].iter_mut().enumerate() {
                    *c = next + i as u64;
                }
                let mut sent = 0;
                while sent < want {
                    let n = tx.push_slice(&chunk[sent..want]);
                    if n == 0 {
                        std::hint::spin_loop();
                    }
                    sent += n;
                }
                next += want as u64;
            }
        })
    };
    pin(CONSUMER_CORE);
    gate.wait();
    let start = Instant::now();
    let mut received = 0u64;
    let mut buf = [0u64; BATCH];
    while received < ITEMS {
        let n = rx.pop_slice(&mut buf);
        if n == 0 {
            std::hint::spin_loop();
        }
        received += n as u64;
    }
    let secs = start.elapsed().as_secs_f64();
    producer.join().unwrap();
    ITEMS as f64 / secs / 1e6
}

// Same data movement as the slice API: values staged in a local buffer first.
fn run_rtrb_chunk_staged() -> f64 {
    let (mut tx, mut rx) = rtrb::RingBuffer::<u64>::new(CAPACITY);
    let gate = Arc::new(Barrier::new(2));
    let producer = {
        let gate = Arc::clone(&gate);
        thread::spawn(move || {
            pin(PRODUCER_CORE);
            gate.wait();
            let mut next = 0u64;
            while next < ITEMS {
                let want = ((ITEMS - next) as usize).min(BATCH).min(tx.slots());
                if want == 0 {
                    std::hint::spin_loop();
                    continue;
                }
                let mut staged = [0u64; BATCH];
                for (i, s) in staged[..want].iter_mut().enumerate() {
                    *s = next + i as u64;
                }
                let chunk = tx.write_chunk_uninit(want).unwrap();
                next += chunk.fill_from_iter(staged[..want].iter().copied()) as u64;
            }
        })
    };
    pin(CONSUMER_CORE);
    gate.wait();
    let start = Instant::now();
    let mut received = 0u64;
    let mut buf = [0u64; BATCH];
    while received < ITEMS {
        let want = rx.slots().min(BATCH);
        if want == 0 {
            std::hint::spin_loop();
            continue;
        }
        let chunk = rx.read_chunk(want).unwrap();
        let (a, b) = chunk.as_slices();
        buf[..a.len()].copy_from_slice(a);
        buf[a.len()..a.len() + b.len()].copy_from_slice(b);
        received += (a.len() + b.len()) as u64;
        chunk.commit_all();
    }
    let secs = start.elapsed().as_secs_f64();
    producer.join().unwrap();
    ITEMS as f64 / secs / 1e6
}

// rtrb's idiomatic chunk use: values are constructed directly in the ring, no
// staging pass. A slice API cannot express this; see the README tradeoff note.
fn run_rtrb_chunk_inplace() -> f64 {
    let (mut tx, mut rx) = rtrb::RingBuffer::<u64>::new(CAPACITY);
    let gate = Arc::new(Barrier::new(2));
    let producer = {
        let gate = Arc::clone(&gate);
        thread::spawn(move || {
            pin(PRODUCER_CORE);
            gate.wait();
            let mut next = 0u64;
            while next < ITEMS {
                let want = ((ITEMS - next) as usize).min(BATCH).min(tx.slots());
                if want == 0 {
                    std::hint::spin_loop();
                    continue;
                }
                let chunk = tx.write_chunk_uninit(want).unwrap();
                next += chunk.fill_from_iter(next..) as u64;
            }
        })
    };
    pin(CONSUMER_CORE);
    gate.wait();
    let start = Instant::now();
    let mut received = 0u64;
    let mut buf = [0u64; BATCH];
    while received < ITEMS {
        let want = rx.slots().min(BATCH);
        if want == 0 {
            std::hint::spin_loop();
            continue;
        }
        let chunk = rx.read_chunk(want).unwrap();
        let (a, b) = chunk.as_slices();
        buf[..a.len()].copy_from_slice(a);
        buf[a.len()..a.len() + b.len()].copy_from_slice(b);
        received += (a.len() + b.len()) as u64;
        chunk.commit_all();
    }
    let secs = start.elapsed().as_secs_f64();
    producer.join().unwrap();
    ITEMS as f64 / secs / 1e6
}

fn run_rtrb() -> f64 {
    let (mut tx, mut rx) = rtrb::RingBuffer::<u64>::new(CAPACITY);
    let gate = Arc::new(Barrier::new(2));
    let producer = {
        let gate = Arc::clone(&gate);
        thread::spawn(move || {
            pin(PRODUCER_CORE);
            gate.wait();
            for i in 0..ITEMS {
                while tx.push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        })
    };
    pin(CONSUMER_CORE);
    gate.wait();
    let start = Instant::now();
    let mut received = 0u64;
    while received < ITEMS {
        if rx.pop().is_ok() {
            received += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    let secs = start.elapsed().as_secs_f64();
    producer.join().unwrap();
    ITEMS as f64 / secs / 1e6
}

fn run_crossbeam() -> f64 {
    let queue = Arc::new(crossbeam_queue::ArrayQueue::<u64>::new(CAPACITY));
    let gate = Arc::new(Barrier::new(2));
    let producer = {
        let queue = Arc::clone(&queue);
        let gate = Arc::clone(&gate);
        thread::spawn(move || {
            pin(PRODUCER_CORE);
            gate.wait();
            for i in 0..ITEMS {
                while queue.push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        })
    };
    pin(CONSUMER_CORE);
    gate.wait();
    let start = Instant::now();
    let mut received = 0u64;
    while received < ITEMS {
        if queue.pop().is_some() {
            received += 1;
        } else {
            std::hint::spin_loop();
        }
    }
    let secs = start.elapsed().as_secs_f64();
    producer.join().unwrap();
    ITEMS as f64 / secs / 1e6
}

fn report(name: &str, run: fn() -> f64) {
    let samples = (0..RUNS).map(|_| run()).collect::<Vec<_>>();
    println!("  {name:<22} {:8.1} Melem/s", median_mops(samples));
}

fn main() {
    println!(
        "throughput (median of {RUNS}, {ITEMS} items, capacity {CAPACITY}, cores {PRODUCER_CORE}->{CONSUMER_CORE})"
    );
    report("spsc-ring", run_spsc_ring);
    report("spsc-ring batch(64)", run_spsc_ring_batch);
    report("rtrb", run_rtrb);
    report("rtrb chunk(64) staged", run_rtrb_chunk_staged);
    report("rtrb chunk(64) in-place", run_rtrb_chunk_inplace);
    report("crossbeam-ArrayQueue", run_crossbeam);
}
