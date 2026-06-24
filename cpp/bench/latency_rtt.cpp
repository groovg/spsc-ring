// Ping-pong round-trip latency. The main thread writes a token into one ring and
// spins until an echo thread bounces it back through a second ring. The reported
// number is the full round trip; halve it for one-way hand-off latency.
//
// Both threads busy-spin, so this measures the queue plus cross-core cache
// coherency, not scheduler wakeups. Pin the two threads to sibling cores for
// stable tails.

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>

#include <rigtorp/SPSCQueue.h>

#include "spsc/ring_buffer.hpp"

namespace {

constexpr int kRounds = 5'000'000;
using Clock = std::chrono::steady_clock;

double ns_per_roundtrip(Clock::duration elapsed) {
    return std::chrono::duration<double, std::nano>(elapsed).count() / kRounds;
}

double run_spsc_ring() {
    spsc::Ring<std::uint64_t> to_echo(2);
    spsc::Ring<std::uint64_t> from_echo(2);

    std::thread echo([&] {
        std::uint64_t value = 0;
        for (int i = 0; i < kRounds; ++i) {
            while (!to_echo.pop(value)) {
            }
            while (!from_echo.push(value)) {
            }
        }
    });

    const auto start = Clock::now();
    std::uint64_t value = 0;
    for (int i = 0; i < kRounds; ++i) {
        while (!to_echo.push(1)) {
        }
        while (!from_echo.pop(value)) {
        }
    }
    const auto elapsed = Clock::now() - start;
    echo.join();
    return ns_per_roundtrip(elapsed);
}

double run_rigtorp() {
    rigtorp::SPSCQueue<std::uint64_t> to_echo(2);
    rigtorp::SPSCQueue<std::uint64_t> from_echo(2);

    std::thread echo([&] {
        for (int i = 0; i < kRounds; ++i) {
            while (to_echo.front() == nullptr) {
            }
            const std::uint64_t value = *to_echo.front();
            to_echo.pop();
            while (!from_echo.try_push(value)) {
            }
        }
    });

    const auto start = Clock::now();
    for (int i = 0; i < kRounds; ++i) {
        while (!to_echo.try_push(1)) {
        }
        while (from_echo.front() == nullptr) {
        }
        from_echo.pop();
    }
    const auto elapsed = Clock::now() - start;
    echo.join();
    return ns_per_roundtrip(elapsed);
}

}  // namespace

int main() {
    std::printf("ping-pong round trip (%d rounds)\n", kRounds);
    std::printf("  spsc::Ring            %6.1f ns\n", run_spsc_ring());
    std::printf("  rigtorp::SPSCQueue    %6.1f ns\n", run_rigtorp());
    return 0;
}
