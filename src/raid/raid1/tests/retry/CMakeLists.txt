cmake_minimum_required (VERSION 3.11)

list(APPEND RAID1_TEST_SRCS
  retry/discard_retry_large.cpp
  retry/discard_retry.cpp
  retry/double_retry.cpp
  retry/flush_retry.cpp
  retry/read_failover.cpp
  retry/read_retry.cpp
  retry/retry_failure_deva.cpp
  retry/write_retry.cpp
)
set(RAID1_TEST_SRCS "${RAID1_TEST_SRCS}" PARENT_SCOPE)
