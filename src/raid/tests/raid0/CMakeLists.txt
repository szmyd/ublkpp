cmake_minimum_required (VERSION 3.11)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR})

list(APPEND RAID0_TEST_SRCS
    tests.cpp
)

add_library(raid0_tests OBJECT)
target_sources(raid0_tests PRIVATE
    ${RAID0_TEST_SRCS}
)
target_link_libraries (raid0_tests
  GTest::gmock
  sisl::sisl
  ublksrv::ublksrv
)
