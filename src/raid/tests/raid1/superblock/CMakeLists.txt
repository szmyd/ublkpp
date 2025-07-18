cmake_minimum_required (VERSION 3.11)

list(APPEND RAID1_TEST_SRCS
  superblock/init_issues.cpp
  superblock/pick_super.cpp
)
set(RAID1_TEST_SRCS "${RAID1_TEST_SRCS}" PARENT_SCOPE)
