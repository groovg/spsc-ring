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
