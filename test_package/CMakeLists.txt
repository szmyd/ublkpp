cmake_minimum_required(VERSION 3.11)
project(test_package)

find_package(UblkPP QUIET REQUIRED)

add_executable(in_memory_disk in_memory_disk.cpp)
target_compile_features(in_memory_disk PUBLIC cxx_std_20)
target_link_libraries(in_memory_disk UblkPP::UblkPP)
