cmake_minimum_required(VERSION 3.11)

add_executable(ublkpp_disk ublkpp_disk.cpp)
target_link_libraries(ublkpp_disk 
    ublkpp
)
