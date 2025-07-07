cmake_minimum_required (VERSION 3.11)

add_library(raid0 OBJECT)
target_sources(raid0 PRIVATE
    raid0.cpp
)
target_link_libraries(raid0
    sisl::sisl
    ublksrv::ublksrv
)

add_library(raid1 OBJECT)
target_sources(raid1 PRIVATE
    raid1.cpp
)
target_link_libraries(raid1
    sisl::sisl
    ublksrv::ublksrv
)

add_subdirectory (tests)
