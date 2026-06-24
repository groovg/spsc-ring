//! Ping-pong round-trip latency, this crate vs rtrb.
//!
//! The main thread stamps a token into one ring and spins until an echo thread
//! bounces it back through a second ring; each round trip is recorded into an HDR
//! histogram. Both threads are pinned and busy-spin. Halve the figures for the
//! one-way hand-off. The per-round `Instant` pair adds a fixed timer overhead
//! (tens of ns) that is identical across contenders, so the comparison is fair.

use std::thread;
use std::time::Instant;

use hdrhistogram::Histogram;

const ROUNDS: u64 = 2_000_000;
const WARMUP: u64 = 100_000;
const PING_CORE: usize = 0;
const PONG_CORE: usize = 2;

fn pin(core: usize) {
    if let Some(ids) = core_affinity::get_core_ids() {
        if let Some(id) = ids.into_iter().find(|c| c.id == core) {
            core_affinity::set_for_current(id);
        }
    }
}

fn report(name: &str, hist: &Histogram<u64>) {
    // The mean is precise (quantization averages out over millions of samples);
    // the per-op percentiles are bucketed at the Windows timer granularity
    // (~100 ns) and so are only indicative until measured with a TSC clock.
    println!(
        "  {name:<12} mean {:>5.1} ns   p50 {:>4} ns   p99 {:>4} ns   max {:>6} ns",
        hist.mean(),
        hist.value_at_quantile(0.50),
        hist.value_at_quantile(0.99),
        hist.max(),
    );
}

fn bench_spsc_ring() -> Histogram<u64> {
    let (mut ping_tx, mut ping_rx) = spsc_ring::channel::<u64>(2);
    let (mut pong_tx, mut pong_rx) = spsc_ring::channel::<u64>(2);

    let echo = thread::spawn(move || {
        pin(PONG_CORE);
        loop {
            let value = loop {
                match ping_rx.pop() {
                    Some(v) => break v,
                    None => std::hint::spin_loop(),
                }
            };
            if value == u64::MAX {
                break;
            }
            while pong_tx.push(value).is_err() {
                std::hint::spin_loop();
            }
        }
    });

    pin(PING_CORE);
    let mut hist = Histogram::<u64>::new(3).unwrap();
    for i in 0..(WARMUP + ROUNDS) {
        let start = Instant::now();
        while ping_tx.push(1).is_err() {
            std::hint::spin_loop();
        }
        loop {
            if pong_rx.pop().is_some() {
                break;
            }
            std::hint::spin_loop();
        }
        if i >= WARMUP {
            hist.record(start.elapsed().as_nanos() as u64).unwrap();
        }
    }
    while ping_tx.push(u64::MAX).is_err() {
        std::hint::spin_loop();
    }
    echo.join().unwrap();
    hist
}

fn bench_rtrb() -> Histogram<u64> {
    let (mut ping_tx, mut ping_rx) = rtrb::RingBuffer::<u64>::new(2);
    let (mut pong_tx, mut pong_rx) = rtrb::RingBuffer::<u64>::new(2);

    let echo = thread::spawn(move || {
        pin(PONG_CORE);
        loop {
            let value = loop {
                match ping_rx.pop() {
                    Ok(v) => break v,
                    Err(_) => std::hint::spin_loop(),
                }
            };
            if value == u64::MAX {
                break;
            }
            while pong_tx.push(value).is_err() {
                std::hint::spin_loop();
            }
        }
    });

    pin(PING_CORE);
    let mut hist = Histogram::<u64>::new(3).unwrap();
    for i in 0..(WARMUP + ROUNDS) {
        let start = Instant::now();
        while ping_tx.push(1).is_err() {
            std::hint::spin_loop();
        }
        loop {
            if pong_rx.pop().is_ok() {
                break;
            }
            std::hint::spin_loop();
        }
        if i >= WARMUP {
            hist.record(start.elapsed().as_nanos() as u64).unwrap();
        }
    }
    while ping_tx.push(u64::MAX).is_err() {
        std::hint::spin_loop();
    }
    echo.join().unwrap();
    hist
}

fn main() {
    println!("ping-pong round trip ({ROUNDS} rounds, cores {PING_CORE}<->{PONG_CORE})");
    report("spsc-ring", &bench_spsc_ring());
    report("rtrb", &bench_rtrb());
}
