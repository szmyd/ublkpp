cmake_minimum_required (VERSION 3.11)

enable_testing()
find_package(GTest QUIET REQUIRED)

add_compile_options(-Wno-error -pedantic)

add_subdirectory (raid0)
add_executable(test_raid0)
target_sources(test_raid0 PRIVATE
  raid0_test.cpp
  $<TARGET_OBJECTS:raid0_tests>
  $<TARGET_OBJECTS:logging>
  $<TARGET_OBJECTS:raid0>
  $<TARGET_OBJECTS:ublk_disk>
)
target_link_libraries (test_raid0
  GTest::gmock
  sisl::sisl
)
add_test(NAME Raid0Test COMMAND test_raid0 -cv 2)
