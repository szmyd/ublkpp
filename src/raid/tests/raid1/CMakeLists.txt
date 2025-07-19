cmake_minimum_required (VERSION 3.11)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR})

list(APPEND RAID1_TEST_SRCS
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
  read_retry_deva.cpp
  read_retry_devb.cpp
  retry_deva.cpp
  retry_devb.cpp
  retry_failure_deva.cpp
  unknown_op.cpp
  write_fail_immediate_deva.cpp
  write_fail_immediate_devb.cpp
)

add_subdirectory (bitmap)
add_subdirectory (simple)
add_subdirectory (superblock)
add_subdirectory (syncio)

add_library(raid1_tests OBJECT)
target_sources(raid1_tests PRIVATE
    ${RAID1_TEST_SRCS}
)
target_link_libraries (raid1_tests
  GTest::gmock
  isa-l::isa-l
  sisl::sisl
  ublksrv::ublksrv
)
