cmake_minimum_required(VERSION 3.17)
project(os_ex3)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS -pthread)
add_executable(os_ex3 SampleClient.cpp Barrier.cpp Barrier.h MapReduceClient.h MapReduceFramework.h MapReduceFramework.cpp JobContext.h JobContext.cpp)
