cmake_minimum_required(VERSION 3.11)

list(APPEND RAID1_ASYNC_TEST_SRCS
    async/read_write.cpp
    async/failover.cpp
    async/flush.cpp
    async/discard.cpp
)
set(RAID1_ASYNC_TEST_SRCS "${RAID1_ASYNC_TEST_SRCS}" PARENT_SCOPE)
