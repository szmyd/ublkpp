cmake_minimum_required (VERSION 3.11)

enable_testing()
find_package(GTest QUIET REQUIRED)

add_compile_options(-Wno-unused-result -Wno-maybe-uninitialized)

add_executable(test_logging)
target_sources(test_logging PRIVATE
   test_logging.cpp
  $<TARGET_OBJECTS:logging>
  $<TARGET_OBJECTS:ublk_disk>
  $<TARGET_OBJECTS:ublk_metrics>
)
target_link_libraries (test_logging
  GTest::gmock
  sisl::cache
  ublksrv::ublksrv
)
add_test(NAME LoggingTest COMMAND test_logging -cv warning --log_mods ublksrv:0)

add_executable(test_cqe_state)
target_sources(test_cqe_state PRIVATE
   test_cqe_state.cpp
  $<TARGET_OBJECTS:logging>
)
target_link_libraries(test_cqe_state
  GTest::gmock
  sisl::cache
  ublksrv::ublksrv
)
add_test(NAME CqeStateTest COMMAND test_cqe_state -cv warning --log_mods ublksrv:0)
