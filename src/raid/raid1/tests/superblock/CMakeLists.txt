cmake_minimum_required (VERSION 3.11)

list(APPEND RAID1_TEST_SRCS
  superblock/init.cpp
  superblock/init_issues.cpp
  superblock/new_device.cpp
  superblock/pick_super.cpp
  superblock/route_set.cpp
  superblock/wrong_uuid.cpp
)
set(RAID1_TEST_SRCS "${RAID1_TEST_SRCS}" PARENT_SCOPE)
