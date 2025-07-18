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
  raid1_bitmap_cross_page.cpp
  raid1_bitmap_multiword.cpp
  raid1_bitmap_update_fail.cpp
  raid1_simple_discard.cpp
  raid1_simple_syncio.cpp
  raid1_simple_write.cpp
  raid1_syncio_double_fail.cpp
  raid1_syncio_fail_deva.cpp
  raid1_syncio_fail_devb.cpp
  raid1_syncio_fail_sb_update.cpp
  raid1_discard_retry.cpp
  raid1_retry_deva.cpp
  raid1_retry_devb.cpp
  raid1_retry_failure_deva.cpp
  raid1_double_retry.cpp
  raid1_write_fail_immediate_deva.cpp
  raid1_write_fail_immediate_devb.cpp
  raid1_immediate_fail_sb_update_fail.cpp
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
