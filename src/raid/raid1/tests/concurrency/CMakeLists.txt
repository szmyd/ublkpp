cmake_minimum_required (VERSION 3.11)

list(APPEND RAID1_TEST_SRCS
  concurrency/concurrent_swap_io.cpp
  concurrency/io_during_swap.cpp
  concurrency/failover_retry_during_swap.cpp
  concurrency/api_calls_during_swap.cpp
  concurrency/multi_reader_swap.cpp
  concurrency/resync_relaunch_after_complete.cpp
)
set(RAID1_TEST_SRCS "${RAID1_TEST_SRCS}" PARENT_SCOPE)
