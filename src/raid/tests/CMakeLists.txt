cmake_minimum_required (VERSION 3.11)

enable_testing()
find_package(GTest QUIET REQUIRED)

add_compile_options(-Wno-unused-result -Wno-maybe-uninitialized)

add_executable(test_raid0)
target_sources(test_raid0 PRIVATE
   test_raid0.cpp
  $<TARGET_OBJECTS:logging>
  $<TARGET_OBJECTS:raid0>
  $<TARGET_OBJECTS:ublk_disk>
)
target_link_libraries (test_raid0
  GTest::gmock
  sisl::sisl
  ublksrv::ublksrv
)
add_test(NAME Raid0Test COMMAND test_raid0 -cv 2)

add_executable(test_raid1)
target_sources(test_raid1 PRIVATE
   test_raid1.cpp
  $<TARGET_OBJECTS:logging>
  $<TARGET_OBJECTS:raid1>
  $<TARGET_OBJECTS:ublk_disk>
)
target_link_libraries (test_raid1
  GTest::gmock
  sisl::sisl
  ublksrv::ublksrv
)
add_test(NAME Raid1Test COMMAND test_raid1 -cv 2)
