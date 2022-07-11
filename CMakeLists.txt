cmake_minimum_required(VERSION 3.4.1 FATAL_ERROR)
project(top-k-compress)

# include CMake modules
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmakemodules")

# set C++ build flags
set(CXX_STANDARD c++20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -std=gnu++20 ${GCC_WARNINGS}")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=native")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb")

# init git submodules
include(InitGitSubmodules)

# extlib
add_subdirectory(extlib)

# include
include_directories(${CMAKE_SOURCE_DIR}/include)

# subdirectories
add_subdirectory(src)
