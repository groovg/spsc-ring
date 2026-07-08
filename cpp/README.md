# spsc-ring (C++)

Header-only, bounded, wait-free single-producer/single-consumer ring buffer
(C++20).

```cpp
#include "spsc/ring_buffer.hpp"

auto [tx, rx] = spsc::channel<std::uint64_t>(1024);

// producer thread (owns tx)
for (std::uint64_t i = 0; i < 1000; ++i) {
    while (!tx.push(i)) {
        // spin until the consumer frees a slot
    }
}

// consumer thread (owns rx)
std::uint64_t value;
while (rx.pop(value)) {
    use(value);
}
```

`channel<T>(capacity)` returns a `Producer` / `Consumer` pair (move-only, one per
thread). `push` returns `false` when full, `pop` returns `false` when empty. For
trivially copyable `T`, `push_n`/`pop_n` move a whole batch with a single release
store per call.
Capacity is rounded up to a power of two and nothing allocates after construction.

## Design

- Free-running `head`/`tail` counters indexed with a bitmask (no modulo, no
  "is full" flag).
- The buffer and the two published atomics live in a shared `Ring`; each handle
  owns its index, a cached copy of the peer's index, a raw slot pointer and the
  mask, so that per-thread state stays in registers across the hot loop.
- Minimal acquire/release ordering: the producer publishes `tail` with `release`,
  the consumer reads it with `acquire`; `head` mirrors.
- The two published atomics are `alignas(64)` on separate cache lines.
- Slots are raw union storage: `push` placement-news the element; `pop` moves it
  out and, for non-trivially-destructible `T`, runs the destructor (`if constexpr`
  elides it otherwise). Any `T` works and destructors fire exactly once.

## Why C++20 (not 23 / 26)

C++20 is the deliberate baseline — it is what downstream users are on in 2026, and
nothing here needs more. C++26 is not yet a viable dependency (feature-frozen 2025,
publication 2026–2027, partial toolchain support), and the C++23 features that were
evaluated (`[[assume]]`, `std::start_lifetime_as`) buy nothing once the slot pointer
is hoisted and storage is a union. See the [repository README](../README.md#why-c20-and-not-23-or-26).

## Build, test, benchmark

```sh
cmake -S . -B build -G Ninja
cmake --build build
ctest --test-dir build --output-on-failure

./build/throughput      # vs rigtorp::SPSCQueue
./build/latency_rtt
```

The benchmarks fetch `rigtorp::SPSCQueue` via CMake `FetchContent`. Configure with
`-DSPSC_BUILD_BENCHMARKS=OFF` to skip the download and build tests only.

Benchmark numbers and the comparison table live in the [repository README](../README.md).
