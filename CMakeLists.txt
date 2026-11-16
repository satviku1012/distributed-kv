cmake_minimum_required(VERSION 3.16)
project(simple_distributed_kv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -pthread")

include_directories(include)

# executables are added as their sources land
if(EXISTS ${CMAKE_SOURCE_DIR}/src/node_server.cpp)
    add_executable(node_server src/node_server.cpp)
endif()

if(EXISTS ${CMAKE_SOURCE_DIR}/src/benchmark.cpp)
    add_executable(benchmark src/benchmark.cpp)
endif()
