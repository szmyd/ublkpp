cmake_minimum_required (VERSION 3.11)

add_library(logging OBJECT)
target_sources(logging PRIVATE
  logging.cpp
)
target_link_libraries(logging
    sisl::sisl
)

add_library(ublk_disk OBJECT)
target_sources(ublk_disk PRIVATE
    ublk_disk.cpp
    ublk_disk_metrics.cpp
    ublkpp_metrics_utilities.cpp
)
target_link_libraries(ublk_disk
    sisl::sisl
    ublksrv::ublksrv
)

if ((DEFINED ENABLE_TESTS) AND (${ENABLE_TESTS}))
add_subdirectory (tests)
endif()
