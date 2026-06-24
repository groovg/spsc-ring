# spsc-ring

A bounded, wait-free **single-producer / single-consumer** ring buffer, implemented twice —
once in **Rust** and once in **C++20** — so the two can be compared directly.

This is the canonical low-latency hand-off: a market-data thread pushing ticks to a strategy
thread without a lock. One thread calls `push`, exactly one other calls `pop`. No mutexes, no
CAS — only acquire/release atomics on a pair of monotonic indices.

```
producer ──push──▶ [ ][x][x][x][ ][ ][ ][ ] ──pop──▶ consumer
                    tail ▲           ▲ head
```

## Layout

| Path    | Language | Build | Docs |
|---------|----------|-------|------|
| [`rust/`](rust/) | Rust  | `cargo` | [rust/README.md](rust/README.md) |
| [`cpp/`](cpp/)   | C++20 | CMake + Ninja | [cpp/README.md](cpp/README.md) |

Both expose the same shape: a fixed power-of-two capacity, `push` that signals when full,
`pop` that signals when empty, and zero heap allocation after construction.

## Design

The two implementations are deliberately the same design, so the differences are language
mechanics rather than algorithm:

1. **Free-running counters, masked indexing.** `head` and `tail` are monotonic counters that
   wrap naturally; the slot index is `counter & (capacity - 1)`. No modulo, and no separate
   "is full" flag — the ring is full when `tail - head == capacity`, empty when `head == tail`.
2. **Minimal memory ordering.** The producer reads its own `tail` relaxed, reads `head`
   acquire, writes the slot, then publishes `tail` with release. The consumer mirrors it.
   That single release/acquire edge is the whole correctness argument: it makes the slot write
   happen-before the consumer sees the index that exposes it.
3. **Cache-line isolation.** `head` and `tail` sit on separate cache lines, so the producer's
   and consumer's stores don't ping-pong one line between cores (false sharing).
4. **Cached remote index (the rigtorp trick).** Each side keeps a private copy of the other's
   index and only touches the remote atomic when the copy says full/empty. In steady state
   this removes nearly all cross-core atomic loads.
5. **Uninitialized storage.** Slots are `MaybeUninit<T>` (Rust) / raw union storage (C++), so
   any `T` is supported, nothing is constructed up front, and destructors run exactly once.

### Where the languages differ

- **Safety boundary.** Rust exposes a `channel()` returning `Producer` / `Consumer` handles
  that are `Send` but not `Sync`, so the single-producer/single-consumer contract is checked by
  the compiler. C++ leaves that contract to the caller, as the ecosystem expects.
- **Verification.** The Rust atomics are checked exhaustively with
  [`loom`](https://github.com/tokio-rs/loom), which explores every legal interleaving of the
  two threads' operations — the strongest correctness evidence available for lock-free code.
- **Storage.** Rust's `MaybeUninit` and C++'s placement-new-into-a-union solve the same
  "don't default-construct the buffer" problem with each language's idiom.

## Benchmarks

Measured on an **AMD Ryzen 9 9950X3D** (16C/32T), Windows 11, gcc 16.1 (`-O3 -march=native`)
and rustc 1.95 (LTO). Threads were **not** pinned and the machine was not isolated, so treat
these as indicative throughput, not tail-latency guarantees — on a tuned Linux box with
sibling-core pinning the tails tighten considerably.

**Throughput** — sustained two-thread `u64` hand-off, capacity 1024:

| Implementation                         | Throughput |
|----------------------------------------|-----------:|
| `spsc::Ring` (this repo, C++)          | 528 Melem/s |
| `rigtorp::SPSCQueue` (C++)             | 685 Melem/s |
| `spsc_ring::channel` (this repo, Rust) | 721 Melem/s |
| `rtrb` (Rust)                          | 1140 Melem/s |
| `crossbeam::ArrayQueue` (MPMC)         | 79 Melem/s |

**Ping-pong round-trip latency** (halve for one-way hand-off):

| Implementation                         | RTT |
|----------------------------------------|----:|
| `spsc::Ring` (this repo, C++)          | 105 ns |
| `rigtorp::SPSCQueue` (C++)             | 110 ns |
| `spsc_ring::channel` (this repo, Rust) | 92 ns |
| `rtrb` (Rust)                          | 83 ns |

The C++ ring matches `rigtorp` on round-trip latency and trails it by ~20% on raw throughput;
the Rust ring is competitive with `rtrb` on latency and trails it on throughput. The gap to
the best-in-class crates comes mainly from how aggressively they inline the slot access and
keep the cached index in the per-thread handle rather than in shared storage — see the note
below.

> Methodology differs slightly per language (Rust uses Criterion's median, C++ uses best-of-5),
> so read the Rust-vs-Rust and C++-vs-C++ rows as the meaningful comparisons rather than the
> cross-language ones.

## What I'd do differently in production

- **Pin and isolate.** Real numbers need the two threads on sibling cores with `isolcpus` /
  `taskset` (Linux); the figures above are from a shared desktop and the tails reflect that.
- **Close the throughput gap.** Hoisting the slot base pointer and the cached index into the
  per-thread handle (as `rtrb`/`rigtorp` do) removes a level of indirection from the hot path;
  this design keeps them in shared storage for a simpler, single-object API.
- **Batch.** A `push_n` / `pop_n` that reserves a contiguous span amortizes the index atomics
  across many elements and is usually a bigger win than micro-optimizing the single-element path.

## Running the checks

```sh
# Rust
cd rust
cargo test
cargo bench --bench throughput
RUSTFLAGS="--cfg loom" cargo test --release --test loom

# C++
cd cpp
cmake -S . -B build -G Ninja && cmake --build build
ctest --test-dir build --output-on-failure
./build/throughput && ./build/latency_rtt
```

## License

MIT — see [LICENSE](LICENSE).
