// Sustained two-thread throughput, this library vs rigtorp::SPSCQueue.
//
// Both threads are pinned to distinct physical cores on the same CCD; the queue
// allocation and thread spawn happen before the timed region (a barrier starts
// the clock once both sides are ready). Reports the median of 15 runs.

#include <algorithm>
#include <immintrin.h>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>

#include <rigtorp/SPSCQueue.h>

#include "bench_util.hpp"
#include "spsc/ring_buffer.hpp"

namespace {

constexpr std::uint64_t kItems = 50'000'000;
constexpr std::size_t kCapacity = 1024;
constexpr std::size_t kBatch = 64;
constexpr unsigned kProducerCore = 0;
constexpr unsigned kConsumerCore = 2;
constexpr int kRuns = 15;

using Clock = std::chrono::steady_clock;

double mops(Clock::duration elapsed) {
    return static_cast<double>(kItems) / std::chrono::duration<double>(elapsed).count() / 1e6;
}

double run_spsc_ring() {
    auto ch = spsc::channel<std::uint64_t>(kCapacity);
    auto rx = std::move(ch.second);
    std::barrier<> gate(2);
    std::thread producer([&gate, tx = std::move(ch.first)]() mutable {
        bench::pin_to_core(kProducerCore);
        gate.arrive_and_wait();
        for (std::uint64_t i = 0; i < kItems; ++i) {
            while (!tx.push(i)) {
            }
        }
    });
    bench::pin_to_core(kConsumerCore);
    gate.arrive_and_wait();
    const auto start = Clock::now();
    std::uint64_t received = 0;
    std::uint64_t value = 0;
    while (received < kItems) {
        if (rx.pop(value)) {
            ++received;
        }
    }
    const auto elapsed = Clock::now() - start;
    producer.join();
    return mops(elapsed);
}

double run_rigtorp() {
    rigtorp::SPSCQueue<std::uint64_t> queue(kCapacity);
    std::barrier<> gate(2);
    std::thread producer([&] {
        bench::pin_to_core(kProducerCore);
        gate.arrive_and_wait();
        for (std::uint64_t i = 0; i < kItems; ++i) {
            while (!queue.try_push(i)) {
            }
        }
    });
    bench::pin_to_core(kConsumerCore);
    gate.arrive_and_wait();
    const auto start = Clock::now();
    std::uint64_t received = 0;
    while (received < kItems) {
        if (queue.front() != nullptr) {
            queue.pop();
            ++received;
        }
    }
    const auto elapsed = Clock::now() - start;
    producer.join();
    return mops(elapsed);
}

double run_spsc_ring_batch() {
    auto ch = spsc::channel<std::uint64_t>(kCapacity);
    auto rx = std::move(ch.second);
    std::barrier<> gate(2);
    std::thread producer([&gate, tx = std::move(ch.first)]() mutable {
        bench::pin_to_core(kProducerCore);
        gate.arrive_and_wait();
        std::uint64_t chunk[kBatch];
        std::uint64_t next = 0;
        while (next < kItems) {
            const std::size_t want =
                static_cast<std::size_t>(std::min<std::uint64_t>(kBatch, kItems - next));
            for (std::size_t i = 0; i < want; ++i) {
                chunk[i] = next + i;
            }
            std::size_t sent = 0;
            while (sent < want) {
                const std::size_t n = tx.push_n(chunk + sent, want - sent);
                if (n == 0) {
                    _mm_pause();
                }
                sent += n;
            }
            next += want;
        }
    });
    bench::pin_to_core(kConsumerCore);
    gate.arrive_and_wait();
    const auto start = Clock::now();
    std::uint64_t received = 0;
    std::uint64_t buf[kBatch];
    while (received < kItems) {
        const std::size_t n = rx.pop_n(buf, kBatch);
        if (n == 0) {
            _mm_pause();
        }
        received += n;
    }
    const auto elapsed = Clock::now() - start;
    producer.join();
    return mops(elapsed);
}

void report(const char* name, double (*run)()) {
    std::vector<double> samples;
    samples.reserve(kRuns);
    for (int i = 0; i < kRuns; ++i) {
        samples.push_back(run());
    }
    std::printf("  %-22s %8.1f Melem/s\n", name, bench::median(std::move(samples)));
}

}  // namespace

int main() {
    std::printf("throughput (median of %d, %llu items, capacity %zu, cores %u->%u)\n", kRuns,
                static_cast<unsigned long long>(kItems), kCapacity, kProducerCore, kConsumerCore);
    report("spsc::Ring", run_spsc_ring);
    report("spsc::Ring batch(64)", run_spsc_ring_batch);
    report("rigtorp::SPSCQueue", run_rigtorp);
    std::printf("  (rigtorp::SPSCQueue has no batch API)\n");
    return 0;
}
