cmake_minimum_required(VERSION 3.10)
project(tzkv)

set(CMAKE_CXX_STANDARD 11)

include_directories(tzkv)
include_directories(tzkv/bPlusTree)

add_executable(tzkv
    tzkv/bPlusTree/bPlusTree.cpp
    tzkv/bPlusTree/bPlusTree.hpp
        tzkv/bPlusTree/Node.h
    tzkv/bPlusTree/Object.h
    tzkv/db_impl.cpp
    tzkv/db_impl.hpp
    tzkv/main.cpp)
