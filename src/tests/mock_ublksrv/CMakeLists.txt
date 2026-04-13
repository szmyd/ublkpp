cmake_minimum_required(VERSION 3.11)

add_library(mock_ublksrv STATIC)
target_sources(mock_ublksrv PRIVATE
    mock_ublksrv.cpp
)
target_link_libraries(mock_ublksrv
    PUBLIC
        ublkpp
        ublksrv::ublksrv
)
target_include_directories(mock_ublksrv PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)
