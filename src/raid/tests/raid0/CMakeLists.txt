cmake_minimum_required (VERSION 3.11)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR})

list(APPEND RAID0_TEST_SRCS
    device_probe.cpp
    device_probe_differ.cpp
    discard_merge.cpp
    discard_retry.cpp
    open_devices.cpp
    split_retry.cpp
    split_write.cpp
    tuple_tests.cpp
)

add_subdirectory (simple)
add_subdirectory (superblock)

add_library(raid0_tests OBJECT)
target_sources(raid0_tests PRIVATE
    ${RAID0_TEST_SRCS}
)
target_link_libraries (raid0_tests
  GTest::gmock
  sisl::sisl
  ublksrv::ublksrv
)
