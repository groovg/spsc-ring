# spsc-ring

[![CI](https://github.com/groovg/spsc-ring/actions/workflows/ci.yml/badge.svg)](https://github.com/groovg/spsc-ring/actions/workflows/ci.yml)

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

Both expose the same shape: `channel(capacity)` hands back a `Producer` / `Consumer` pair over
a power-of-two buffer, `push` signals when full, `pop` signals when empty, and nothing
allocates after construction.

## Design

The two implementations are deliberately the same design, so the differences are language
mechanics rather than algorithm:

1. **Free-running counters, masked indexing.** `head` and `tail` are monotonic counters that
   wrap naturally; the slot index is `counter & (capacity - 1)`. No modulo, and no separate
   "is full" flag — the ring is full when `tail - head == capacity`, empty when `head == tail`.
2. **Minimal memory ordering.** The producer reads its own index, writes the slot, then
   publishes `tail` with release. The consumer mirrors it with an acquire load. That single
   release/acquire edge is the whole correctness argument: it makes the slot write
   happen-before the consumer sees the index that exposes it.
3. **Split endpoints, thread-local state.** `channel` returns a `Producer` and a `Consumer`.
   Each owns its index, a cached copy of the peer's index, a raw pointer to the buffer, and the
   mask. Keeping that state in a thread-local handle lets the compiler hold it in registers
   across the hot loop, and means `push`/`pop` never read a shared atomic for their own
   bookkeeping or load the buffer's metadata from shared memory.
4. **Cached remote index.** A side only reads the peer's atomic when its cached copy says
   full/empty, so in steady state there are almost no cross-core atomic loads.
5. **Cache-line isolation.** The two published atomics sit on separate cache lines, so the
   producer's and consumer's stores don't ping-pong one line between cores (false sharing).
6. **Uninitialized storage.** Slots are `MaybeUninit<T>` (Rust) / a raw union (C++), so any `T`
   works, nothing is constructed up front, and destructors run exactly once.

### Where the languages differ

- **Safety boundary.** Rust's `Producer` / `Consumer` are `Send` but not `Sync`, so the
  single-producer/single-consumer contract is checked by the compiler. C++ leaves that contract
  to the caller, as the ecosystem expects.
- **Verification.** The Rust atomics are checked exhaustively with
  [`loom`](https://github.com/tokio-rs/loom), which explores every legal interleaving of the
  two threads' operations — the strongest correctness evidence available for lock-free code.
  The C++ side leans on ThreadSanitizer in CI.
- **Dispatch on `T`.** C++ uses `if constexpr` to skip the per-pop destructor and the teardown
  drain for trivially destructible `T`; Rust gets the same for free from `needs_drop`.

## Why C++20 (and not 23 or 26)

C++20 is the baseline the header targets, on purpose:

- **C++20 is what consumers are actually on** in 2026. Forcing `-std=c++23` on every downstream
  is needless friction for a header-only queue, and nothing here needs it: the optimizations
  above are all expressible in C++20.
- **C++26 is not a viable target yet.** It was feature-frozen in 2025 with formal publication
  landing in 2026–2027, and library + language support is still partial even on bleeding-edge
  toolchains — depending on it would make the library unusable for almost everyone.
- C++23 niceties that *were* evaluated (`[[assume]]`, `std::start_lifetime_as`) buy nothing here
  once the slot pointer is hoisted and storage is a union, so they were left out rather than
  added as cargo cult.

## Benchmarks

Measured on an **AMD Ryzen 9 9950X3D** (16C/32T), Windows 11, gcc 16.1 (`-O3 -march=native`)
and rustc 1.95 (LTO). Both threads are pinned to two physical cores on the same CCD; the
channel allocation and thread spawn happen outside the timed region. Numbers vary a few percent
run-to-run on a non-isolated desktop, but each implementation consistently leads its reference
within a run.

**Throughput** — sustained two-thread `u64` hand-off, capacity 1024, median of 15 runs:

| Implementation                         | Throughput |
|----------------------------------------|-----------:|
| `spsc_ring::channel` (this repo, Rust) | **~1430 Melem/s** |
| `rtrb` (Rust)                          | ~1110 Melem/s |
| `crossbeam::ArrayQueue` (Rust, MPMC)   | ~120 Melem/s |
| `spsc::Ring` (this repo, C++)          | **~1030 Melem/s** |
| `rigtorp::SPSCQueue` (C++)             | ~690 Melem/s |

**Ping-pong round-trip latency** — mean over 2M rounds (halve for one-way hand-off):

| Implementation                         | RTT (mean) |
|----------------------------------------|-----------:|
| `spsc_ring::channel` (this repo, Rust) | **~94 ns** |
| `rtrb` (Rust)                          | ~108 ns |
| `spsc::Ring` (this repo, C++)          | **~112 ns** |
| `rigtorp::SPSCQueue` (C++)             | ~149 ns |

### Why it's faster — and the tradeoff that buys it

These numbers come from a deliberate design choice, not from `rtrb`/`rigtorp` being unoptimized
(they are excellent and battle-tested). The difference is what each one optimizes *for*:

- **vs `rtrb` (also split into handles).** `rtrb` supports an *arbitrary* capacity, so it tracks
  positions in `0 .. 2·capacity` and pays two branches per operation (`increment1` +
  `collapse_position`). This ring requires a **power-of-two capacity**, so the index is just
  `counter & mask` — branchless. That is essentially the whole gap.
- **vs `rigtorp` (a single shared object).** `rigtorp`'s API is one object both threads share,
  so its per-thread bookkeeping lives in shared memory and the compiler can't keep it in
  registers across the loop. Splitting into `Producer`/`Consumer` handles keeps that state
  thread-local and register-resident — the same change took this repo's *own* C++ from ~450 to
  ~1180 Melem/s. `rigtorp` also branches on wrap and reads its read index twice per pop
  (`front()` then `pop()`).

So the honest summary is: **faster on a fixed, power-of-two ring, at the cost of flexibility.**
`rtrb` and `rigtorp` take any capacity, ship allocator hooks, and offer richer APIs (`rtrb`'s
batched `write_chunk`/`read_chunk`, `rigtorp`'s `peek`); this library trades those away for the
leanest possible fixed-size hot path. Every contender is driven through its own idiomatic API and
built with full optimization (LTO / `-O3 -march=native`), with identical pinning and steady-state
timing.

> Cross-language rows aren't directly comparable (different timer paths and run conditions);
> read each implementation against its own-language reference. RTT percentiles are reported by
> the benches but the Windows `steady_clock` granularity (~100 ns) quantizes per-op samples, so
> the mean is the meaningful figure here — sub-100 ns tail detail needs a TSC-based clock.

## Limitations and what I'd add next

Being honest about where this trails the reference crates:

- **Power-of-two capacity only.** That restriction is what buys the branchless index math; if
  arbitrary capacity were a requirement, the right move is `rtrb`/`rigtorp`, not this.
- **No batch / chunk API yet.** `rtrb`'s `write_chunk`/`read_chunk` are both a flexibility and a
  throughput win — a `push_n`/`pop_n` here that reserves a contiguous span and `memcpy`s the
  payloads would amortize the publish/observe round trip across many elements (the next lever,
  and a natural fit for bursty market-data fan-out).
- **No custom allocator / NUMA placement**, which the references expose.
- **Measure on isolated hardware.** These numbers are from a shared Windows desktop; real tail
  latency wants `isolcpus` / `nohz_full` on Linux and a TSC-based timer for honest p99.9.

## Running the checks

```sh
# Rust
cd rust
cargo test
cargo bench --bench throughput
cargo bench --bench latency_rtt
RUSTFLAGS="--cfg loom" cargo test --release --test loom

# C++
cd cpp
cmake -S . -B build -G Ninja && cmake --build build
ctest --test-dir build --output-on-failure
./build/throughput && ./build/latency_rtt
```

## License

MIT — see [LICENSE](LICENSE).
