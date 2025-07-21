cmake_minimum_required (VERSION 3.11)

list(APPEND RAID1_TEST_SRCS
  bitmap/calc_regions.cpp
  bitmap/cross_page.cpp
  bitmap/init_bitmap.cpp
  bitmap/load_bitmap.cpp
  bitmap/load_fail.cpp
  bitmap/mismatched.cpp
  bitmap/multiword.cpp
  bitmap/update_fail.cpp
)
set(RAID1_TEST_SRCS "${RAID1_TEST_SRCS}" PARENT_SCOPE)
