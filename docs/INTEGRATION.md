# Integrating ublkpp

This guide shows how to consume ublkpp as a library in your own projects.

## Prerequisites

- **Linux kernel 5.19+** with ublk support
- **Conan 2.0+** package manager
- **CMake 3.22+** build system
- **C++23 compiler**: GCC 13+, Clang 17+, or equivalent

## Conan Integration

### Basic conanfile.txt

Create a `conanfile.txt` in your project:

```ini
[requires]
ublkpp/0.21.0@oss/main

[generators]
CMakeDeps
CMakeToolchain

[options]
ublkpp/*:shared=False
```

### With Optional Features

Enable iSCSI or HomeBlocks backends:

```ini
[requires]
ublkpp/0.21.0@oss/main

[generators]
CMakeDeps
CMakeToolchain

[options]
ublkpp/*:shared=False
ublkpp/*:iscsi=True
ublkpp/*:homeblocks=True
```

### Using conanfile.py

For more control, use a Python conanfile:

```python
from conan import ConanFile
from conan.tools.cmake import cmake_layout

class MyProjectConan(ConanFile):
    name = "myproject"
    version = "1.0"
    settings = "os", "compiler", "build_type", "arch"

    def requirements(self):
        self.requires("ublkpp/0.21.0@oss/main")

    def configure(self):
        # Optional: enable features
        self.options["ublkpp"].iscsi = True

    def layout(self):
        cmake_layout(self)
```

### Installing Dependencies

```bash
# Install dependencies
conan install . --output-folder=build --build=missing

# For debug builds
conan install . --output-folder=build --build=missing -s build_type=Debug

# For release builds
conan install . --output-folder=build --build=missing -s build_type=Release
```

## CMake Integration

### Basic CMakeLists.txt

```cmake
cmake_minimum_required(VERSION 3.22)
project(my_ublk_app CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include Conan toolchain
include(${CMAKE_BINARY_DIR}/generators/conan_toolchain.cmake)

find_package(UblkPP REQUIRED)

add_executable(my_app main.cpp)
target_link_libraries(my_app UblkPP::UblkPP)
```

### Complete Example

```cmake
cmake_minimum_required(VERSION 3.22)
project(my_ublk_storage VERSION 1.0 LANGUAGES CXX)

# C++23 required
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Conan integration
include(${CMAKE_BINARY_DIR}/generators/conan_toolchain.cmake)

# Find ublkpp
find_package(UblkPP REQUIRED)

# Your application
add_executable(storage_daemon
    src/main.cpp
    src/config.cpp
    src/device_manager.cpp
)

target_link_libraries(storage_daemon
    PRIVATE
        UblkPP::UblkPP
)

# Optional: compile options
target_compile_options(storage_daemon PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)
```

## Build Workflow

### Standard Build

```bash
# 1. Install dependencies with Conan
conan install . --output-folder=build --build=missing

# 2. Configure with CMake
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release

# 3. Build
cmake --build build

# 4. Run
./build/my_app
```

### Debug Build

```bash
conan install . --output-folder=build --build=missing -s build_type=Debug
cmake -B build -S . -DCMAKE_BUILD_TYPE=Debug
cmake --build build
```

### Using Presets (CMake 3.25+)

Create `CMakePresets.json`:

```json
{
  "version": 6,
  "cmakeMinimumRequired": {
    "major": 3,
    "minor": 25,
    "patch": 0
  },
  "configurePresets": [
    {
      "name": "release",
      "binaryDir": "${sourceDir}/build/Release",
      "cacheVariables": {
        "CMAKE_BUILD_TYPE": "Release"
      }
    },
    {
      "name": "debug",
      "binaryDir": "${sourceDir}/build/Debug",
      "cacheVariables": {
        "CMAKE_BUILD_TYPE": "Debug"
      }
    }
  ]
}
```

Then build with:
```bash
cmake --preset release
cmake --build build/Release
```

## Minimal Working Example

### main.cpp

```cpp
#include <iostream>
#include <memory>
#include <boost/uuid/uuid_generators.hpp>
#include <ublkpp/ublkpp.hpp>
#include <ublkpp/drivers/fs_disk.hpp>

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <device_path>\n";
        return 1;
    }

    // Initialize logging (SISL)
    sisl::logging::SetLogger("my_app");
    sisl::logging::SetModuleLogLevel("ublk_drivers", spdlog::level::info);

    // Create a file-backed block device
    auto disk = std::make_shared<ublkpp::FSDisk>(argv[1]);

    std::cout << "Device: " << disk->id() << "\n";
    std::cout << "Capacity: " << disk->capacity() << " bytes\n";
    std::cout << "Block size: " << disk->block_size() << " bytes\n";

    // Generate a volume UUID
    auto uuid = boost::uuids::random_generator()();

    // Expose to kernel (creates /dev/ublkbN)
    auto tgt_result = ublkpp::ublkpp_tgt::run(uuid, disk);
    if (!tgt_result) {
        std::cerr << "Failed to create target: "
                  << tgt_result.error().message() << "\n";
        return 1;
    }

    auto tgt = std::move(tgt_result.value());
    std::cout << "Created: " << tgt->device_path() << "\n";
    std::cout << "Press Ctrl+C to stop...\n";

    // Keep running (target runs in background threads)
    pause();

    return 0;
}
```

### CMakeLists.txt

```cmake
cmake_minimum_required(VERSION 3.22)
project(simple_ublk CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(${CMAKE_BINARY_DIR}/generators/conan_toolchain.cmake)

find_package(UblkPP REQUIRED)

add_executable(simple_ublk main.cpp)
target_link_libraries(simple_ublk UblkPP::UblkPP)
```

### conanfile.txt

```ini
[requires]
ublkpp/0.21.0@oss/main

[generators]
CMakeDeps
CMakeToolchain
```

### Build and Run

```bash
# Install dependencies
conan install . --output-folder=build --build=missing

# Build
cmake -B build -S .
cmake --build build

# Create a backing file
fallocate -l 1G /tmp/test.dat

# Run (requires sudo for ublk)
sudo ./build/simple_ublk /tmp/test.dat

# In another terminal, verify
lsblk | grep ublk
# ublkb0      259:3    0    1G  0 disk
```

## RAID Example

### RAID1 Application

```cpp
#include <ublkpp/ublkpp.hpp>
#include <ublkpp/drivers/fs_disk.hpp>
#include <ublkpp/raid/raid1.hpp>

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <device_a> <device_b>\n";
        return 1;
    }

    // Initialize logging
    sisl::logging::SetLogger("raid1_app");
    sisl::logging::SetModuleLogLevel("ublk_raid", spdlog::level::info);

    // Create backend devices
    auto disk_a = std::make_shared<ublkpp::FSDisk>(argv[1]);
    auto disk_b = std::make_shared<ublkpp::FSDisk>(argv[2]);

    std::cout << "Device A: " << disk_a->capacity() << " bytes\n";
    std::cout << "Device B: " << disk_b->capacity() << " bytes\n";

    // Create RAID1 mirror
    auto uuid = boost::uuids::random_generator()();
    auto raid = std::make_shared<ublkpp::Raid1Disk>(uuid, disk_a, disk_b);

    std::cout << "RAID1 capacity: " << raid->capacity() << " bytes\n";

    // Expose to kernel
    auto tgt_result = ublkpp::ublkpp_tgt::run(uuid, raid);
    if (!tgt_result) {
        std::cerr << "Failed: " << tgt_result.error().message() << "\n";
        return 1;
    }

    auto tgt = std::move(tgt_result.value());
    std::cout << "RAID1 device: " << tgt->device_path() << "\n";

    // Monitor array state
    while (true) {
        sleep(5);
        auto state = raid->replica_states();
        std::cout << "Device A: " << state.device_a
                  << ", Device B: " << state.device_b
                  << ", To sync: " << state.bytes_to_sync << " bytes\n";
    }

    return 0;
}
```

## Runtime Configuration

### SISL Options

ublkpp uses SISL for runtime configuration. Options can be set via:

**Environment Variables:**
```bash
export UBLK_NR_HW_QUEUES=4
export UBLK_QUEUE_DEPTH=256
export UBLK_MAX_IO_SIZE=1048576  # 1 MiB
```

**Command Line:**
```bash
./my_app --nr_hw_queues 4 --queue_depth 256 --max_io_size 1048576
```

**Programmatically:**
```cpp
#include <sisl/options/options.h>

SISL_OPTION_GROUP(ublkpp,
    (nr_hw_queues, "", "nr_hw_queues", "Number of I/O queues",
        ::cxxopts::value<uint32_t>()->default_value("1"), "count"),
    (queue_depth, "", "queue_depth", "Queue depth",
        ::cxxopts::value<uint32_t>()->default_value("128"), "count"),
    (max_io_size, "", "max_io_size", "Max I/O size",
        ::cxxopts::value<uint32_t>()->default_value("524288"), "bytes")
);

int main(int argc, char** argv) {
    SISL_OPTIONS_LOAD(argc, argv, ublkpp);
    // Options now available via SISL_OPTIONS
}
```

### Logging Configuration

```cpp
#include <sisl/logging/logging.h>

// Set global log level
sisl::logging::SetLogger("my_app");
sisl::logging::SetLogLevel(spdlog::level::info);

// Per-module levels
sisl::logging::SetModuleLogLevel("ublk_raid", spdlog::level::debug);
sisl::logging::SetModuleLogLevel("ublk_drivers", spdlog::level::info);
sisl::logging::SetModuleLogLevel("ublksrv", spdlog::level::warn);
```

**Available Modules:**
- `ublksrv`: ublk driver interface
- `ublk_tgt`: Target implementation
- `ublk_raid`: RAID layers
- `ublk_drivers`: Backend drivers
- `libiscsi`: iSCSI protocol (if enabled)

## Troubleshooting

### Kernel Module Not Loaded

```bash
# Error: "Failed to create target: No such device"
sudo modprobe ublk_drv
```

### Permission Denied

```bash
# ublk requires CAP_SYS_ADMIN
sudo ./my_app

# Or use capabilities
sudo setcap cap_sys_admin+ep ./my_app
./my_app
```

### Linker Errors

```
undefined reference to `ublkpp::FSDisk::FSDisk(...)`
```

**Solution:** Ensure CMake finds the package:
```cmake
find_package(UblkPP REQUIRED)
target_link_libraries(my_app UblkPP::UblkPP)
```

### Conan Package Not Found

```bash
# Add eBay OSS remote
conan remote add oss-remote <URL>

# Or build locally
cd ublkpp
conan create . --build=missing
```

## Best Practices

1. **Always check `std::expected` results** before using values
2. **Initialize SISL logging** before creating devices
3. **Run with appropriate privileges** (sudo or capabilities)
4. **Monitor device states** for RAID arrays
5. **Gracefully handle device removal** (especially for degraded RAID)
6. **Set resource limits** (queue depth, I/O size) based on workload
7. **Test with sanitizers** during development:
   ```bash
   conan install . -o ublkpp/*:sanitize=address --build=missing
   ```

## Next Steps

- **[Extension Guide](EXTENDING.md)**: Create custom drivers and RAID types
- **[Library Guide](LIBRARY.md)**: Understand architecture and concepts
- **[API Reference](API.md)**: Detailed API documentation
- **[Example Application](../example/)**: See `ublkpp_disk` for a complete example
