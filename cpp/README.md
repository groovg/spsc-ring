# spsc-ring (C++)

Header-only, bounded, wait-free single-producer/single-consumer ring buffer
(C++20).

```cpp
#include "spsc/ring_buffer.hpp"

spsc::Ring<std::uint64_t> ring(1024);

// producer thread
for (std::uint64_t i = 0; i < 1000; ++i) {
    while (!ring.push(i)) {
        // spin until the consumer frees a slot
    }
}

// consumer thread
std::uint64_t value;
while (ring.pop(value)) {
    use(value);
}
```

`push` returns `false` when full, `pop` returns `false` when empty. Capacity is
rounded up to a power of two and nothing allocates after construction.

## Design

- Free-running `head`/`tail` counters indexed with a bitmask (no modulo, no
  "is full" flag).
- Minimal acquire/release ordering: the producer publishes `tail` with `release`,
  the consumer reads it with `acquire`; `head` mirrors.
- `head` and `tail` are `alignas(64)` on separate cache lines; each shares its
  line with the cache the same thread uses, so a hot `push`/`pop` touches one line.
- Each side caches the other's index and only reads the remote atomic when the
  cache says full/empty.
- Slots are raw union storage: `push` placement-news the element, `pop` moves it
  out and runs the destructor, so any `T` works and destructors fire exactly once.

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
