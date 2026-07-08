// Ping-pong round-trip latency, this library vs rigtorp::SPSCQueue.
//
// The main thread stamps a token into one ring and spins until an echo thread
// bounces it back through a second ring; each round trip is recorded. Both
// threads are pinned and busy-spin. Halve the figures for the one-way hand-off.
// Timestamps come from tsc-latency's fenced RDTSC pair, so the percentiles are
// real (steady_clock on Windows quantizes at ~100 ns and only the mean survives).

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <numeric>
#include <thread>
#include <vector>

#include <rigtorp/SPSCQueue.h>
#include <tsclat/clock.hpp>
#include <tsclat/tsc.hpp>

#include "bench_util.hpp"
#include "spsc/ring_buffer.hpp"

namespace {

constexpr int kRounds = 2'000'000;
constexpr int kWarmup = 100'000;
constexpr unsigned kPingCore = 0;
constexpr unsigned kPongCore = 2;

void report(const char* name, std::vector<std::uint64_t> ticks) {
    const auto& clock = tsclat::TscClock::instance();
    const double mean =
        clock.to_ns(std::accumulate(ticks.begin(), ticks.end(), std::uint64_t{0})) /
        static_cast<double>(ticks.size());
    std::sort(ticks.begin(), ticks.end());
    std::printf(
        "  %-12s mean %5.1f ns   p50 %4.0f ns   p99 %4.0f ns   p99.9 %5.0f ns   max %6.0f ns\n",
        name, mean, clock.to_ns(bench::quantile(ticks, 0.50)),
        clock.to_ns(bench::quantile(ticks, 0.99)), clock.to_ns(bench::quantile(ticks, 0.999)),
        clock.to_ns(ticks.back()));
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
        const std::uint64_t start = tsclat::tsc_begin();
        while (!ping_tx.push(1)) {
        }
        while (!pong_rx.pop(echoed)) {
        }
        if (i >= kWarmup) {
            samples.push_back(tsclat::tsc_end() - start);
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
        const std::uint64_t start = tsclat::tsc_begin();
        while (!to_echo.try_push(1)) {
        }
        while (from_echo.front() == nullptr) {
        }
        from_echo.pop();
        if (i >= kWarmup) {
            samples.push_back(tsclat::tsc_end() - start);
        }
    }
    while (!to_echo.try_push(UINT64_MAX)) {
    }
    echo.join();
    return samples;
}

}  // namespace

int main() {
    const auto& clock = tsclat::TscClock::instance();
    std::printf(
        "ping-pong round trip (%d rounds, cores %u<->%u, invariant tsc: %s, %.4f ns/tick)\n",
        kRounds, kPingCore, kPongCore, tsclat::has_invariant_tsc() ? "yes" : "no",
        clock.ns_per_tick());
    report("spsc::Ring", bench_spsc_ring());
    report("rigtorp::SPSCQueue", bench_rigtorp());
    return 0;
}
