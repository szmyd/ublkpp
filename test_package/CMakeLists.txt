cmake_minimum_required(VERSION 3.11)
project(test_package)

find_package(UblkPP QUIET REQUIRED)

add_executable(ublkpp_disk ublkpp_disk.cpp)
target_compile_features(ublkpp_disk PUBLIC cxx_std_20)
target_link_libraries(ublkpp_disk UblkPP::UblkPP)
