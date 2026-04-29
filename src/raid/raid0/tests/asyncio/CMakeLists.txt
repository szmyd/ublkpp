cmake_minimum_required(VERSION 3.11)

list(APPEND RAID0_TEST_SRCS
    asyncio/read_write.cpp
    asyncio/flush.cpp
    asyncio/discard.cpp
    asyncio/error.cpp
)
set(RAID0_TEST_SRCS "${RAID0_TEST_SRCS}" PARENT_SCOPE)
