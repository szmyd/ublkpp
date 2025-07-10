cmake_minimum_required (VERSION 3.11)

find_package(libiscsi REQUIRED)

add_library(fs_disk OBJECT)
target_sources(fs_disk PRIVATE
    fs_disk.cpp
)
target_link_libraries(fs_disk
    sisl::sisl
    ublksrv::ublksrv
)

add_library(iscsi_disk OBJECT)
target_sources(iscsi_disk PRIVATE
    iscsi_disk.cpp
)
target_link_libraries(iscsi_disk
    sisl::sisl
    ublksrv::ublksrv
    libiscsi::libiscsi
)

if (${homeblocks_FOUND})
  add_library(homeblk_disk OBJECT)
  target_sources(homeblk_disk PRIVATE
      homeblk_disk.cpp
  )
  target_link_libraries(homeblk_disk
      sisl::sisl
      ublksrv::ublksrv
      homeblocks::homestore
  )
endif()

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR})
add_subdirectory (tests)
