cmake_minimum_required(VERSION 3.17)
project(os_ex3)

set(CMAKE_CXX_STANDARD 11)

add_executable(os_ex3 Barrier.cpp Barrier.h MapReduceClient.h MapReduceFramework.h MapReduceFramework.cpp JobContext.h JobContext.cpp)