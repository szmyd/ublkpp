cmake_minimum_required(VERSION 3.11)

list(APPEND RAID1_ASYNC_TEST_SRCS
    async/bitmap.cpp
    async/degraded.cpp
    async/discard.cpp
    async/failover.cpp
    async/flush.cpp
    async/idle.cpp
    async/read_write.cpp
    async/retry.cpp
)
set(RAID1_ASYNC_TEST_SRCS "${RAID1_ASYNC_TEST_SRCS}" PARENT_SCOPE)
