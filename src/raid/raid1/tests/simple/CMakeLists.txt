cmake_minimum_required (VERSION 3.11)

list(APPEND RAID1_TEST_SRCS
  simple/syncio.cpp
)
set(RAID1_TEST_SRCS "${RAID1_TEST_SRCS}" PARENT_SCOPE)
