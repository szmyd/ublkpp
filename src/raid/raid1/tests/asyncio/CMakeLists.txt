cmake_minimum_required (VERSION 3.11)

list(APPEND RAID1_TEST_SRCS
  asyncio/bitmap.cpp
  asyncio/degraded.cpp
  asyncio/discard.cpp
  asyncio/failover.cpp
  asyncio/flush.cpp
  asyncio/idle.cpp
  asyncio/read_fail_degraded_dirty.cpp
  asyncio/read_write.cpp
  asyncio/retry.cpp
  asyncio/write_backup_fail_degrade_fail.cpp
)
set(RAID1_TEST_SRCS "${RAID1_TEST_SRCS}" PARENT_SCOPE)
