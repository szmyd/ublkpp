cmake_minimum_required (VERSION 3.11)

enable_testing()
find_package(GTest QUIET REQUIRED)

add_executable(test_fsdisk)
target_sources(test_fsdisk PRIVATE
   test_fsdisk.cpp test_fs_disk_impl.cpp
  $<TARGET_OBJECTS:fs_disk>
  $<TARGET_OBJECTS:ublk_disk>
  $<TARGET_OBJECTS:ublk_metrics>
  $<TARGET_OBJECTS:logging>
)
target_link_libraries (test_fsdisk
  GTest::gmock
  sisl::cache
  ublksrv::ublksrv
)
add_test(NAME FSDiskTest COMMAND test_fsdisk -cv warning)

if(${libiscsi_FOUND})
    add_executable(test_iscsi_disk)
    target_sources(test_iscsi_disk PRIVATE
       test_iscsi_disk.cpp
      $<TARGET_OBJECTS:iscsi_disk>
      $<TARGET_OBJECTS:ublk_disk>
      $<TARGET_OBJECTS:ublk_metrics>
      $<TARGET_OBJECTS:logging>
    )
    target_link_libraries(test_iscsi_disk
      GTest::gmock
      sisl::cache
      ublksrv::ublksrv
      libiscsi::libiscsi
    )

    # tgtd lifecycle as CTest fixtures: start once before iSCSIDiskTest, tear down after.
    # Setup script soft-skips (exit 0) when tgtd is missing; the test then runtime-probes
    # 127.0.0.1:13260 and GTEST_SKIPs all cases if nothing is listening.
    set(_iscsi_setup_script "${CMAKE_CURRENT_SOURCE_DIR}/iscsi_tgtd_setup.sh")
    add_test(NAME ISCSISetup    COMMAND "${_iscsi_setup_script}" start)
    add_test(NAME ISCSITeardown COMMAND "${_iscsi_setup_script}" stop)
    set_tests_properties(ISCSISetup    PROPERTIES
        FIXTURES_SETUP iscsi_target
        SKIP_REGULAR_EXPRESSION "ISCSI_SOFT_SKIP")
    set_tests_properties(ISCSITeardown PROPERTIES FIXTURES_CLEANUP iscsi_target)

    add_test(NAME iSCSIDiskTest COMMAND test_iscsi_disk -cv warning)
    # When the C++ runtime probe finds no listener, every test case GTEST_SKIPs and
    # gtest reports "PASSED ] 0 tests" — match that to surface as Skipped in CTest.
    set_tests_properties(iSCSIDiskTest PROPERTIES
        FIXTURES_REQUIRED iscsi_target
        SKIP_REGULAR_EXPRESSION "PASSED  \\] 0 tests")
endif()
