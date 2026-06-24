// Sustained two-thread throughput: a producer pins values into the ring while a
// consumer drains them. The same workload runs against this library and
// rigtorp::SPSCQueue so the numbers are directly comparable.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>

#include <rigtorp/SPSCQueue.h>

#include "spsc/ring_buffer.hpp"

namespace {

constexpr std::uint64_t kItems = 50'000'000;
constexpr std::size_t kCapacity = 1024;

using Clock = std::chrono::steady_clock;

double throughput_from(Clock::duration elapsed) {
    const double seconds = std::chrono::duration<double>(elapsed).count();
    return static_cast<double>(kItems) / seconds / 1e6;  // Melem/s
}

double run_spsc_ring() {
    spsc::Ring<std::uint64_t> ring(kCapacity);
    const auto start = Clock::now();
    std::thread producer([&] {
        for (std::uint64_t i = 0; i < kItems; ++i) {
            while (!ring.push(i)) {
            }
        }
    });
    std::uint64_t received = 0;
    std::uint64_t value = 0;
    while (received < kItems) {
        if (ring.pop(value)) {
            ++received;
        }
    }
    producer.join();
    return throughput_from(Clock::now() - start);
}

double run_rigtorp() {
    rigtorp::SPSCQueue<std::uint64_t> queue(kCapacity);
    const auto start = Clock::now();
    std::thread producer([&] {
        for (std::uint64_t i = 0; i < kItems; ++i) {
            while (!queue.try_push(i)) {
            }
        }
    });
    std::uint64_t received = 0;
    while (received < kItems) {
        if (queue.front() != nullptr) {
            queue.pop();
            ++received;
        }
    }
    producer.join();
    return throughput_from(Clock::now() - start);
}

double best_of(double (*run)(), int rounds) {
    double best = 0.0;
    for (int i = 0; i < rounds; ++i) {
        best = std::max(best, run());
    }
    return best;
}

}  // namespace

int main() {
    constexpr int kRounds = 5;
    std::printf("throughput (best of %d, %llu items, capacity %zu)\n", kRounds,
                static_cast<unsigned long long>(kItems), kCapacity);
    std::printf("  spsc::Ring            %8.1f Melem/s\n",
                best_of(run_spsc_ring, kRounds));
    std::printf("  rigtorp::SPSCQueue    %8.1f Melem/s\n",
                best_of(run_rigtorp, kRounds));
    return 0;
}
