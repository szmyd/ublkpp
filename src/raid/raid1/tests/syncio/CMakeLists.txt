cmake_minimum_required (VERSION 3.11)

list(APPEND RAID1_TEST_SRCS
  syncio/double_fail.cpp
  syncio/fail_sb_update.cpp
  syncio/read_fail_both.cpp
  syncio/read_fail_deva.cpp
  syncio/write_fail_deva.cpp
  syncio/write_fail_devb.cpp
)
set(RAID1_TEST_SRCS "${RAID1_TEST_SRCS}" PARENT_SCOPE)
