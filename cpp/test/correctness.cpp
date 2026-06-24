#include <cstdint>
#include <string>
#include <thread>

#include "check.hpp"
#include "spsc/ring_buffer.hpp"

using spsc::Ring;

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
    CHECK(Ring<uint64_t>(3).capacity() == 4);
    CHECK(Ring<uint64_t>(16).capacity() == 16);
    CHECK(Ring<uint64_t>(17).capacity() == 32);

    {
        Ring<uint64_t> ring(4);
        CHECK(ring.empty());
        for (uint64_t i = 0; i < 4; ++i) {
            CHECK(ring.push(i));
        }
        CHECK(!ring.push(99));
        CHECK(ring.size() == 4);

        uint64_t value = 0;
        for (uint64_t i = 0; i < 4; ++i) {
            CHECK(ring.pop(value));
            CHECK(value == i);
        }
        CHECK(!ring.pop(value));
        CHECK(ring.empty());
    }

    {
        // Cycle through far more elements than the capacity to exercise the
        // free-running counters wrapping over the masked index.
        Ring<uint64_t> ring(4);
        uint64_t value = 0;
        for (uint64_t round = 0; round < 1000; ++round) {
            CHECK(ring.push(round));
            CHECK(ring.pop(value));
            CHECK(value == round);
        }
        CHECK(ring.empty());
    }

    {
        // Non-trivial element type round-trips correctly.
        Ring<std::string> ring(4);
        CHECK(ring.push(std::string("hello")));
        CHECK(ring.push(std::string("world")));
        std::string value;
        CHECK(ring.pop(value));
        CHECK(value == "hello");
        CHECK(ring.pop(value));
        CHECK(value == "world");
        CHECK(!ring.pop(value));
    }

    {
        // Every constructed element is destroyed exactly once: nothing leaks and
        // nothing is double-freed, whether popped or left in the ring at teardown.
        {
            Ring<Counted> ring(8);
            for (int i = 0; i < 5; ++i) {
                CHECK(ring.push(Counted{}));
            }
            Counted out;
            CHECK(ring.pop(out));
            CHECK(ring.pop(out));
            // Three elements are left for ~Ring to destroy.
        }
        CHECK(g_live == 0);
    }

    {
        // Producer and consumer on separate threads: values must arrive in order
        // with no gaps or duplicates.
        constexpr uint64_t kCount = 2'000'000;
        Ring<uint64_t> ring(1024);

        std::thread producer([&] {
            for (uint64_t i = 0; i < kCount; ++i) {
                while (!ring.push(i)) {
                    // spin until the consumer frees a slot
                }
            }
        });

        uint64_t next = 0;
        uint64_t value = 0;
        while (next < kCount) {
            if (ring.pop(value)) {
                if (value != next) {
                    CHECK(value == next);
                    break;
                }
                ++next;
            }
        }
        producer.join();
        CHECK(next == kCount);
    }

    RUN_END();
}
