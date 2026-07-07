#include <algorithm>
#include <cstdint>
#include <string>
#include <thread>
#include <utility>

#include "check.hpp"
#include "spsc/ring_buffer.hpp"

using spsc::channel;

namespace {
int g_live = 0;

struct Counted {
    Counted() { ++g_live; }
    Counted(const Counted&) { ++g_live; }
    Counted(Counted&&) noexcept { ++g_live; }
    Counted& operator=(const Counted&) = default;
    Counted& operator=(Counted&&) noexcept = default;
    ~Counted() { --g_live; }
};
}  // namespace

int main() {
    {
        auto [tx, rx] = channel<uint64_t>(3);
        CHECK(tx.capacity() == 4);
        CHECK(rx.capacity() == 4);
    }

    {
        auto [tx, rx] = channel<uint64_t>(4);
        CHECK(rx.empty());
        for (uint64_t i = 0; i < 4; ++i) {
            CHECK(tx.push(i));
        }
        CHECK(!tx.push(99));
        CHECK(rx.size() == 4);

        uint64_t value = 0;
        for (uint64_t i = 0; i < 4; ++i) {
            CHECK(rx.pop(value));
            CHECK(value == i);
        }
        CHECK(!rx.pop(value));
        CHECK(rx.empty());
    }

    {
        // Cycle through far more elements than the capacity to exercise the
        // free-running counters wrapping over the masked index.
        auto [tx, rx] = channel<uint64_t>(4);
        uint64_t value = 0;
        for (uint64_t round = 0; round < 1000; ++round) {
            CHECK(tx.push(round));
            CHECK(rx.pop(value));
            CHECK(value == round);
        }
        CHECK(rx.empty());
    }

    {
        auto [tx, rx] = channel<std::string>(4);
        CHECK(tx.push(std::string("hello")));
        CHECK(tx.push(std::string("world")));
        std::string value;
        CHECK(rx.pop(value));
        CHECK(value == "hello");
        CHECK(rx.pop(value));
        CHECK(value == "world");
        CHECK(!rx.pop(value));
    }

    {
        // Every constructed element is destroyed exactly once, whether popped or
        // left in the ring at teardown.
        {
            auto [tx, rx] = channel<Counted>(8);
            for (int i = 0; i < 5; ++i) {
                CHECK(tx.push(Counted{}));
            }
            Counted out;
            CHECK(rx.pop(out));
            CHECK(rx.pop(out));
        }
        CHECK(g_live == 0);
    }

    {
        auto [tx, rx] = channel<uint64_t>(4);
        uint64_t value = 0;
        CHECK(tx.push(0));
        CHECK(rx.pop(value));

        const uint64_t in[5] = {1, 2, 3, 4, 5};
        CHECK(tx.push_n(in, 5) == 4);
        CHECK(tx.push_n(in, 1) == 0);
        uint64_t out[8] = {};
        CHECK(rx.pop_n(out, 8) == 4);
        CHECK(out[0] == 1 && out[1] == 2 && out[2] == 3 && out[3] == 4);
        CHECK(rx.pop_n(out, 8) == 0);
        CHECK(tx.push_n(in, 0) == 0);
    }

    {
        auto [tx, rx] = channel<uint64_t>(8);
        const uint64_t in[3] = {2, 3, 4};
        CHECK(tx.push(1));
        CHECK(tx.push_n(in, 3) == 3);
        uint64_t value = 0;
        CHECK(rx.pop(value));
        CHECK(value == 1);
        uint64_t out[2] = {};
        CHECK(rx.pop_n(out, 2) == 2);
        CHECK(out[0] == 2 && out[1] == 3);
        CHECK(rx.pop(value));
        CHECK(value == 4);
        CHECK(rx.empty());
    }

    {
        // Batched producer against batched consumer across threads.
        constexpr uint64_t kCount = 2'000'000;
        auto [tx0, rx0] = channel<uint64_t>(1024);

        std::thread producer([tx = std::move(tx0)]() mutable {
            uint64_t chunk[64];
            uint64_t next = 0;
            while (next < kCount) {
                const std::size_t want =
                    static_cast<std::size_t>(std::min<uint64_t>(64, kCount - next));
                for (std::size_t i = 0; i < want; ++i) {
                    chunk[i] = next + i;
                }
                std::size_t sent = 0;
                while (sent < want) {
                    sent += tx.push_n(chunk + sent, want - sent);
                }
                next += want;
            }
        });

        uint64_t expected = 0;
        std::thread consumer([rx = std::move(rx0), &expected]() mutable {
            uint64_t buf[64];
            while (expected < kCount) {
                const std::size_t n = rx.pop_n(buf, 64);
                for (std::size_t i = 0; i < n; ++i) {
                    if (buf[i] != expected) {
                        return;
                    }
                    ++expected;
                }
            }
        });

        producer.join();
        consumer.join();
        CHECK(expected == kCount);
    }

    {
        // Producer and consumer on separate threads: values arrive in order with
        // no gaps or duplicates.
        constexpr uint64_t kCount = 2'000'000;
        auto [tx0, rx0] = channel<uint64_t>(1024);

        std::thread producer([tx = std::move(tx0)]() mutable {
            for (uint64_t i = 0; i < kCount; ++i) {
                while (!tx.push(i)) {
                }
            }
        });

        uint64_t next = 0;
        std::thread consumer([rx = std::move(rx0), &next]() mutable {
            uint64_t value = 0;
            while (next < kCount) {
                if (rx.pop(value)) {
                    if (value != next) {
                        break;
                    }
                    ++next;
                }
            }
        });

        producer.join();
        consumer.join();
        CHECK(next == kCount);
    }

    RUN_END();
}
