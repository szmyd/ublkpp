cmake_minimum_required (VERSION 3.11)

enable_testing()
find_package(GTest QUIET REQUIRED)

add_compile_options(-Wno-error -pedantic)

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

add_executable(test_raid1_read)
target_sources(test_raid1_read PRIVATE
   test_raid1_read.cpp
  $<TARGET_OBJECTS:logging>
  $<TARGET_OBJECTS:raid1>
  $<TARGET_OBJECTS:ublk_disk>
)
target_link_libraries (test_raid1_read
  GTest::gmock
  sisl::sisl
  ublksrv::ublksrv
)
add_test(NAME Raid1TestRead COMMAND test_raid1_read -cv 2)

add_executable(test_raid1_write)
target_sources(test_raid1_write PRIVATE
   test_raid1_write.cpp
  $<TARGET_OBJECTS:logging>
  $<TARGET_OBJECTS:raid1>
  $<TARGET_OBJECTS:ublk_disk>
)
target_link_libraries (test_raid1_write
  GTest::gmock
  sisl::sisl
  ublksrv::ublksrv
)
add_test(NAME Raid1TestWrite COMMAND test_raid1_write -cv 2)
