cmake_minimum_required(VERSION 3.11)

include_directories(BEFORE ${CMAKE_SOURCE_DIR}/src)

add_subdirectory(mock_ublksrv)
add_subdirectory(fio_engine)
