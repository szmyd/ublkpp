cmake_minimum_required(VERSION 3.11)
project(ublkpp CXX)
enable_testing()

set(CMAKE_CXX_STANDARD 23)

if ((DEFINED BUILD_COVERAGE) AND (${BUILD_COVERAGE}))
  enable_testing()
  include (cmake/coverage.cmake)
  APPEND_COVERAGE_COMPILER_FLAGS()
  SETUP_TARGET_FOR_COVERAGE_GCOVR_XML(NAME coverage EXECUTABLE ctest DEPENDENCIES )
elseif ((DEFINED THREAD_SANITIZER_ON) AND (${THREAD_SANITIZER_ON}))
  set(SANITIZER_TYPE "thread")
  include (cmake/sanitize.cmake)
elseif ((DEFINED ADDRESS_SANITIZER_ON) AND (${ADDRESS_SANITIZER_ON}))
  set(SANITIZER_TYPE "address")
  include (cmake/sanitize.cmake)
endif()

find_program(CCACHE_FOUND ccache)
if (CCACHE_FOUND)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif ()

# stdexec's top-level CMakeLists.txt (nvhpc-* tags) requires rapids-cmake infrastructure
# which is not available in standalone builds. Bypass it: use FetchContent_Populate to
# download the source tree only, then expose the headers as a plain INTERFACE target.
# CMP0169 OLD is required because FetchContent_Populate is deprecated in CMake 3.30+.
if(POLICY CMP0169)
    cmake_policy(SET CMP0169 OLD)
endif()

include(FetchContent)
FetchContent_Declare(
    stdexec
    GIT_REPOSITORY https://github.com/NVIDIA/stdexec.git
    GIT_TAG        ea0d8788da51a1671daa00b88bf238725f0844a8  # nvhpc-25.09
    GIT_SHALLOW    TRUE
)
FetchContent_GetProperties(stdexec)
if(NOT stdexec_POPULATED)
    FetchContent_Populate(stdexec)  # download only — do not run stdexec's CMakeLists.txt
endif()

add_library(stdexec_iface INTERFACE)
target_include_directories(stdexec_iface INTERFACE ${stdexec_SOURCE_DIR}/include)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(stdexec_iface INTERFACE -Wno-empty-body)
endif()
# stdexec's <execution> include drags in libstdc++'s PSTL, which on gcc-15
# defaults to the TBB backend and emits unresolved tbb::detail::r1::* symbols.
target_compile_options(stdexec_iface INTERFACE -U_PSTL_PAR_BACKEND_TBB)
target_compile_definitions(stdexec_iface INTERFACE _PSTL_PAR_BACKEND_SERIAL)
add_library(STDEXEC::stdexec ALIAS stdexec_iface)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/include)

find_package(sisl REQUIRED)
find_package(ublksrv REQUIRED)
find_package(isa-l REQUIRED)
find_package(fio QUIET)

add_subdirectory (src)
if ((DEFINED ENABLE_TESTS) AND (${ENABLE_TESTS}))
add_subdirectory (example)
add_subdirectory (src/tests)
endif ()
