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
