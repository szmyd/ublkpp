cmake_minimum_required (VERSION 3.11)

enable_testing()
find_package(GTest QUIET REQUIRED)

add_executable(test_ublkpp_tgt)
target_sources(test_ublkpp_tgt PRIVATE
   test_ublkpp_tgt.cpp
  $<TARGET_OBJECTS:ublkpp_tgt>
  $<TARGET_OBJECTS:ublk_disk>
  $<TARGET_OBJECTS:logging>
)
target_link_libraries (test_ublkpp_tgt
  GTest::gmock
  sisl::sisl
  ublksrv::ublksrv
)
add_test(NAME TgtTest COMMAND test_ublkpp_tgt -cv 2)
