cmake_minimum_required (VERSION 3.11)
enable_testing()

find_package(GTest QUIET REQUIRED)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(test_md_disk)
target_sources(test_md_disk PRIVATE
    md_test.cpp
    $<TARGET_OBJECTS:ublk_disk>
    $<TARGET_OBJECTS:ublk_metrics>
    $<TARGET_OBJECTS:logging>
    $<TARGET_OBJECTS:md_disk>
    $<TARGET_OBJECTS:raid0>
    $<TARGET_OBJECTS:raid1>
)
target_link_libraries (test_md_disk
    GTest::gmock
    mock_ublksrv
    isa-l::isa-l
    sisl::cache
    ublksrv::ublksrv
    $<$<PLATFORM_ID:Linux>:atomic>
)
add_test(NAME MdDiskTest COMMAND test_md_disk -cv warning)
