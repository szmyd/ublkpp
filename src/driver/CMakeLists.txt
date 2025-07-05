cmake_minimum_required (VERSION 3.11)

add_library(fs_disk OBJECT)
target_sources(fs_disk PRIVATE
    fs_disk.cpp
)
target_link_libraries(fs_disk
    sisl::sisl
    ublksrv::ublksrv
)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR})
add_subdirectory (tests)
