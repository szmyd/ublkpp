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

# stdexec's internal rapids-cmake/CPM uses FetchContent_Populate directly; suppress the CMP0169 warning
if(POLICY CMP0169)
    cmake_policy(SET CMP0169 OLD)
endif()

include(FetchContent)
FetchContent_Declare(
    stdexec
    GIT_REPOSITORY https://github.com/NVIDIA/stdexec.git
    GIT_TAG        nvhpc-25.09
    GIT_SHALLOW    TRUE
)
set(STDEXEC_BUILD_TESTS      OFF CACHE BOOL "" FORCE)
set(STDEXEC_BUILD_EXAMPLES   OFF CACHE BOOL "" FORCE)
set(STDEXEC_ENABLE_CUDA      OFF CACHE BOOL "" FORCE)
set(STDEXEC_ENABLE_TBB       OFF CACHE BOOL "" FORCE)
set(STDEXEC_ENABLE_IO_URING  OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(stdexec)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(stdexec INTERFACE -Wno-empty-body)
endif()

# stdexec's <execution> include drags in libstdc++'s PSTL, which on gcc-15
# defaults to the TBB backend and emits unresolved tbb::detail::r1::* symbols.
# We never use std::execution::par, so force the serial backend on every
# consumer to avoid the unwanted libtbb link dependency.
target_compile_options(stdexec INTERFACE -U_PSTL_PAR_BACKEND_TBB)
target_compile_definitions(stdexec INTERFACE _PSTL_PAR_BACKEND_SERIAL)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/include)

find_package(sisl REQUIRED)
find_package(ublksrv REQUIRED)
find_package(isa-l REQUIRED)
find_package(libiscsi QUIET)
find_package(fio QUIET)

add_subdirectory (src)
if ((DEFINED ENABLE_TESTS) AND (${ENABLE_TESTS}))
add_subdirectory (example)
add_subdirectory (src/tests)
endif ()
