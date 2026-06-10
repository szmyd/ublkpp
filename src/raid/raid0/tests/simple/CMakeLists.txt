cmake_minimum_required (VERSION 3.11)

list(APPEND RAID0_TEST_SRCS
  simple/discard.cpp
  simple/get_device.cpp
  simple/syncio.cpp
)
set(RAID0_TEST_SRCS "${RAID0_TEST_SRCS}" PARENT_SCOPE)

