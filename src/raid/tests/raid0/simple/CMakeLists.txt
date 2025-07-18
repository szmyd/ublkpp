cmake_minimum_required (VERSION 3.11)

list(APPEND RAID0_TEST_SRCS
  simple/discard.cpp
  simple/flush.cpp
  simple/read.cpp
  simple/syncio.cpp
  simple/write.cpp
)
set(RAID0_TEST_SRCS "${RAID0_TEST_SRCS}" PARENT_SCOPE)

