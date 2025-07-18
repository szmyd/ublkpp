cmake_minimum_required (VERSION 3.11)

list(APPEND RAID0_TEST_SRCS
  superblock/init_issues.cpp
)
set(RAID0_TEST_SRCS "${RAID0_TEST_SRCS}" PARENT_SCOPE)
