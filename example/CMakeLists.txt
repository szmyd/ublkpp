cmake_minimum_required(VERSION 3.11)

find_package(sisl REQUIRED)

if(${libiscsi_FOUND})
    add_compile_options(-DHAVE_ISCSI)
endif()

add_executable(ublkpp_disk ublkpp_disk.cpp)
target_link_libraries(ublkpp_disk
    sisl::http
    ublkpp
)
