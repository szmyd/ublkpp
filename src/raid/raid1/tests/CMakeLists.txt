cmake_minimum_required (VERSION 3.11)
enable_testing()

find_package(GTest QUIET REQUIRED)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR})

list(APPEND RAID1_TEST_SRCS
  raid1_test.cpp
  device_probe.cpp
  device_probe_differ.cpp
  device_probe_exceed.cpp
  discard_retry_large.cpp
  discard_retry.cpp
  double_retry.cpp
  flush_retry.cpp
  immediate_fail_sb_update_fail.cpp
  open_devices.cpp
  read_degraded.cpp
  read_failover.cpp
  read_retry.cpp
  retry_failure_deva.cpp
  unknown_op.cpp
  write_fail.cpp
  write_retry.cpp
)

add_subdirectory (bitmap)
add_subdirectory (simple)
add_subdirectory (superblock)
add_subdirectory (syncio)

add_executable(test_raid1)
target_sources(test_raid1 PRIVATE
  ${RAID1_TEST_SRCS}
  $<TARGET_OBJECTS:ublk_disk>
  $<TARGET_OBJECTS:logging>
  $<TARGET_OBJECTS:raid1>
  $<TARGET_OBJECTS:ublk_disk>
)
target_link_libraries (test_raid1
  GTest::gmock
  isa-l::isa-l
  sisl::sisl
  ublksrv::ublksrv
)
add_test(NAME Raid1Test COMMAND test_raid1 -cv 2)
