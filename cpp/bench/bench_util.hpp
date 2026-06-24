#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

#if defined(_WIN32)
#include <windows.h>
#else
#include <pthread.h>
#include <sched.h>
#endif

namespace bench {

inline void pin_to_core(unsigned core) {
#if defined(_WIN32)
    SetThreadAffinityMask(GetCurrentThread(), static_cast<DWORD_PTR>(1) << core);
#else
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
#endif
}

inline double median(std::vector<double> samples) {
    std::sort(samples.begin(), samples.end());
    return samples[samples.size() / 2];
}

inline std::uint64_t quantile(const std::vector<std::uint64_t>& sorted, double q) {
    if (sorted.empty()) {
        return 0;
    }
    return sorted[static_cast<std::size_t>(q * (sorted.size() - 1))];
}

}  // namespace bench
