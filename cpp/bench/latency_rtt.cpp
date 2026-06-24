// Ping-pong round-trip latency, this library vs rigtorp::SPSCQueue.
//
// The main thread stamps a token into one ring and spins until an echo thread
// bounces it back through a second ring; each round trip is recorded. Both
// threads are pinned and busy-spin. Halve the figures for the one-way hand-off.
// The mean is precise (quantization averages out); the per-op percentiles are
// bucketed at the Windows steady_clock granularity (~100 ns) and so are only
// indicative until measured with a TSC clock.

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <numeric>
#include <thread>
#include <vector>

#include <rigtorp/SPSCQueue.h>

#include "bench_util.hpp"
#include "spsc/ring_buffer.hpp"

namespace {

constexpr int kRounds = 2'000'000;
constexpr int kWarmup = 100'000;
constexpr unsigned kPingCore = 0;
constexpr unsigned kPongCore = 2;

using Clock = std::chrono::steady_clock;

void report(const char* name, std::vector<std::uint64_t> samples) {
    const double mean =
        static_cast<double>(std::accumulate(samples.begin(), samples.end(), std::uint64_t{0})) /
        samples.size();
    std::sort(samples.begin(), samples.end());
    std::printf("  %-12s mean %5.1f ns   p50 %4llu ns   p99 %4llu ns   max %6llu ns\n", name, mean,
                static_cast<unsigned long long>(bench::quantile(samples, 0.50)),
                static_cast<unsigned long long>(bench::quantile(samples, 0.99)),
                static_cast<unsigned long long>(samples.back()));
}

std::vector<std::uint64_t> bench_spsc_ring() {
    auto to_echo = spsc::channel<std::uint64_t>(2);
    auto from_echo = spsc::channel<std::uint64_t>(2);
    auto ping_tx = std::move(to_echo.first);
    auto pong_rx = std::move(from_echo.second);

    std::thread echo([ping_rx = std::move(to_echo.second),
                      pong_tx = std::move(from_echo.first)]() mutable {
        bench::pin_to_core(kPongCore);
        std::uint64_t value = 0;
        for (;;) {
            while (!ping_rx.pop(value)) {
            }
            if (value == UINT64_MAX) {
                break;
            }
            while (!pong_tx.push(value)) {
            }
        }
    });

    bench::pin_to_core(kPingCore);
    std::vector<std::uint64_t> samples;
    samples.reserve(kRounds);
    std::uint64_t echoed = 0;
    for (int i = 0; i < kWarmup + kRounds; ++i) {
        const auto start = Clock::now();
        while (!ping_tx.push(1)) {
        }
        while (!pong_rx.pop(echoed)) {
        }
        if (i >= kWarmup) {
            samples.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
        }
    }
    while (!ping_tx.push(UINT64_MAX)) {
    }
    echo.join();
    return samples;
}

std::vector<std::uint64_t> bench_rigtorp() {
    rigtorp::SPSCQueue<std::uint64_t> to_echo(2);
    rigtorp::SPSCQueue<std::uint64_t> from_echo(2);
    std::thread echo([&] {
        bench::pin_to_core(kPongCore);
        for (;;) {
            while (to_echo.front() == nullptr) {
            }
            const std::uint64_t value = *to_echo.front();
            to_echo.pop();
            if (value == UINT64_MAX) {
                break;
            }
            while (!from_echo.try_push(value)) {
            }
        }
    });

    bench::pin_to_core(kPingCore);
    std::vector<std::uint64_t> samples;
    samples.reserve(kRounds);
    for (int i = 0; i < kWarmup + kRounds; ++i) {
        const auto start = Clock::now();
        while (!to_echo.try_push(1)) {
        }
        while (from_echo.front() == nullptr) {
        }
        from_echo.pop();
        if (i >= kWarmup) {
            samples.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
        }
    }
    while (!to_echo.try_push(UINT64_MAX)) {
    }
    echo.join();
    return samples;
}

}  // namespace

int main() {
    std::printf("ping-pong round trip (%d rounds, cores %u<->%u)\n", kRounds, kPingCore, kPongCore);
    report("spsc::Ring", bench_spsc_ring());
    report("rigtorp::SPSCQueue", bench_rigtorp());
    return 0;
}
