cmake_minimum_required(VERSION 3.16)
project(distributed_kv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -pthread")

include_directories(include)

add_executable(node_server src/node_server.cpp)
add_executable(benchmark src/benchmark.cpp)

enable_testing()
add_executable(unit_tests tests/unit_tests.cpp)
add_test(NAME unit_tests COMMAND unit_tests)
