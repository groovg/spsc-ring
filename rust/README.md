# spsc-ring (Rust)

Bounded, wait-free single-producer/single-consumer ring buffer.

```rust
use spsc_ring::channel;

let (mut tx, mut rx) = channel::<u64>(1024);

std::thread::spawn(move || {
    for i in 0..1000 {
        while tx.push(i).is_err() {
            std::hint::spin_loop();
        }
    }
});

let mut received = 0;
while received < 1000 {
    if rx.pop().is_some() {
        received += 1;
    }
}
```

`channel` returns a `Producer` / `Consumer` pair. Each is `Send` but not `Sync`, so
the single-producer/single-consumer contract is enforced at compile time. `push`
hands the value back as `Err(item)` when the ring is full; `pop` returns `None`
when empty. Capacity is rounded up to a power of two and nothing allocates after
construction.

## Design

- Free-running `head`/`tail` counters indexed with a bitmask (no modulo, no
  "is full" flag).
- Each endpoint owns its index, a cached copy of the peer's index, a raw pointer
  to the buffer, and the mask. That state is thread-local, so the compiler keeps
  it in registers across the hot loop and `push`/`pop` never read a shared atomic
  for their own bookkeeping.
- Minimal acquire/release ordering: the producer publishes `tail` with `Release`,
  the consumer reads it with `Acquire`; `head` mirrors. That single edge is what
  makes a written slot visible before its index is.
- The two published atomics sit on separate cache lines to avoid false sharing.
- Elements live in `MaybeUninit`, so any `T` is supported and destructors run
  exactly once (verified with a drop-counting test).

## Test, benchmark, model-check

```sh
cargo test                                          # unit + stress tests
cargo bench --bench throughput                      # vs rtrb, crossbeam
cargo bench --bench latency_rtt                     # ping-pong RTT
RUSTFLAGS="--cfg loom" cargo test --release --test loom   # exhaustive interleavings
```

Benchmark numbers and the comparison table live in the [repository README](../README.md).
