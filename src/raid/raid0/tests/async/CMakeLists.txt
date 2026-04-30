cmake_minimum_required(VERSION 3.11)

list(APPEND RAID0_ASYNC_TEST_SRCS
    async/read_write.cpp
    async/flush.cpp
    async/discard.cpp
    async/error.cpp
)
set(RAID0_ASYNC_TEST_SRCS "${RAID0_ASYNC_TEST_SRCS}" PARENT_SCOPE)
