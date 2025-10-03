cmake_minimum_required (VERSION 3.11)

list(APPEND RAID1_TEST_SRCS
  #failures/immediate_fail_sb_update_fail.cpp
  failures/read_degraded.cpp
  failures/unknown_op.cpp
  failures/write_fail.cpp
)
set(RAID1_TEST_SRCS "${RAID1_TEST_SRCS}" PARENT_SCOPE)
