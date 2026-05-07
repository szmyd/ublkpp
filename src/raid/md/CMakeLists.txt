cmake_minimum_required (VERSION 3.11)

add_library(md_disk OBJECT)
target_sources(md_disk PRIVATE
    md_disk.cpp
    md_superblock.cpp
)
target_link_libraries(md_disk
    sisl::cache
    ublksrv::ublksrv
)

if ((DEFINED ENABLE_TESTS) AND (${ENABLE_TESTS}))
add_subdirectory (tests)
endif()
