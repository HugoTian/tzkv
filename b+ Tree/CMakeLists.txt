cmake_minimum_required(VERSION 3.10)
project(b__Tree)

set(CMAKE_CXX_STANDARD 11)

include_directories("b+ Tree")

add_executable(b__Tree
        "b+ Tree/bPlusTree.cpp"
        "b+ Tree/bPlusTree.hpp"
        "b+ Tree/main.cpp" "b+ Tree/Node.h" "b+ Tree/Object.h")
