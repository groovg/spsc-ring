cmake_minimum_required(VERSION 3.20)
project(spsc_ring CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Threads REQUIRED)

# Header-only library.
add_library(spsc_ring INTERFACE)
target_include_directories(spsc_ring INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(spsc_ring INTERFACE cxx_std_20)

enable_testing()

add_executable(correctness test/correctness.cpp)
target_link_libraries(correctness PRIVATE spsc_ring Threads::Threads)
target_compile_options(correctness PRIVATE -Wall -Wextra)
add_test(NAME correctness COMMAND correctness)

# Benchmarks pull in rigtorp::SPSCQueue for comparison, so they need network at
# configure time. Turn them off with -DSPSC_BUILD_BENCHMARKS=OFF when offline.
option(SPSC_BUILD_BENCHMARKS "Build the benchmarks (fetches rigtorp::SPSCQueue)" ON)

if(SPSC_BUILD_BENCHMARKS)
    include(FetchContent)
    FetchContent_Declare(
        rigtorp_spsc
        GIT_REPOSITORY https://github.com/rigtorp/SPSCQueue.git
        GIT_TAG v1.1
    )
    FetchContent_MakeAvailable(rigtorp_spsc)
    FetchContent_Declare(
        tsc_latency
        GIT_REPOSITORY https://github.com/groovg/tsc-latency.git
        GIT_TAG 8b432562323a9c2c5f9f54064d7c1127139daa4c)
    FetchContent_MakeAvailable(tsc_latency)

    foreach(bench throughput latency_rtt)
        add_executable(${bench} bench/${bench}.cpp)
        target_link_libraries(${bench} PRIVATE spsc_ring SPSCQueue tsclat Threads::Threads)
        target_compile_options(${bench} PRIVATE -O3 -march=native)
    endforeach()
endif()
