cmake_minimum_required (VERSION 3.11)

add_compile_options(-Wall -Wextra -Werror)

find_package(sisl REQUIRED)
find_package(ublksrv REQUIRED)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_subdirectory (lib)
add_subdirectory (raid)
add_subdirectory (driver)
add_subdirectory (target)

add_library(ublkpp)
target_sources(ublkpp PRIVATE
  $<TARGET_OBJECTS:raid0>
  $<TARGET_OBJECTS:raid1>
  $<TARGET_OBJECTS:fs_disk>
  $<TARGET_OBJECTS:ublkpp_tgt>
  $<TARGET_OBJECTS:ublk_disk>
  $<TARGET_OBJECTS:logging>
  )
