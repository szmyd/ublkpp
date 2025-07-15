cmake_minimum_required (VERSION 3.11)

add_compile_options(-Wall -Wextra -Werror)

find_package(sisl REQUIRED)
find_package(ublksrv REQUIRED)

find_package(libiscsi QUIET)
find_package(homeblocks QUIET)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory (lib)
add_subdirectory (raid)
add_subdirectory (driver)
add_subdirectory (target)

list(APPEND LIBRARY_OBJECTS
  $<TARGET_OBJECTS:raid0>
  $<TARGET_OBJECTS:raid1>
  $<TARGET_OBJECTS:fs_disk>
  $<TARGET_OBJECTS:ublkpp_tgt>
  $<TARGET_OBJECTS:ublk_disk>
  $<TARGET_OBJECTS:logging>
  )
list(APPEND LIBRARY_LINKS
    ublksrv::ublksrv
  )

if(${homeblocks_FOUND})
    list(APPEND LIBRARY_OBJECTS $<TARGET_OBJECTS:homeblk_disk>)
    list(APPEND LIBRARY_LINKS homeblocks::homestore)
endif()

if(${libiscsi_FOUND})
    list(APPEND LIBRARY_OBJECTS $<TARGET_OBJECTS:iscsi_disk>)
    list(APPEND LIBRARY_LINKS libiscsi::libiscsi)
endif()

add_library(ublkpp)
target_sources(ublkpp PRIVATE
    ${LIBRARY_OBJECTS}
  )
target_link_libraries(ublkpp
    INTERFACE sisl::sisl
    PUBLIC
      ${LIBRARY_LINKS}
  )
