# ublkpp

[![Conan Build](https://github.com/szmyd/ublkpp/actions/workflows/merge_build.yml/badge.svg?branch=main)](https://github.com/szmyd/ublkpp/actions/workflows/merge_build.yml)
[![CodeCov](https://codecov.io/gh/szmyd/ublkpp/graph/badge.svg?token=2N5W3458RK)](https://codecov.io/gh/szmyd/ublkpp)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

> A high-performance C++23 library providing RAID0/1/10 support for Linux's userspace block (ublk) driver

## 🚀 Features

- **RAID Support**: Full implementation of RAID0 (striping), RAID1 (mirroring), and RAID10 (stripe of mirrors)
- **RAID1 Resilient Bitmap**: Memory-efficient dirty tracking (4 KiB page tracks 1 GiB data)
- **Hot Device Replacement**: Swap devices in degraded RAID1 arrays without downtime
- **Multiple Backends**: File system, iSCSI, and HomeBlocks support
- **Comprehensive Testing**: High test coverage with unit and integration tests
- **Modern C++**: Built with C++23, leveraging `std::expected` for error handling
- **Production Ready**: Thread-safe, handles degraded modes, optimistic recovery

## 📋 Table of Contents

- [Quick Start](#-quick-start)
- [Architecture](#-architecture)
- [RAID Features](#-raid-features)
- [Example Application](#-example-application)
- [Development](#-development)
- [Testing](#-testing)
- [Dependencies](#-dependencies)
- [License](#-license)

## 🏃 Quick Start

### Prerequisites

- Linux kernel with ublk support (5.19+)
- Conan 2.0+
- CMake 3.22+
- C++23 compatible compiler (GCC 13+, Clang 17+)

### Build Library

```bash
git clone https://github.com/szmyd/ublkpp
cd ublkpp
./prepare_v2.sh
conan build -s:h build_type=Debug --build missing ublkpp
```

### Build Options

```bash
# Release build
conan build -s:h build_type=Release --build missing ublkpp

# With coverage
conan build -s:h build_type=Debug -o coverage=True --build missing ublkpp

# With sanitizers (address or thread)
conan build -s:h build_type=Debug -o sanitize=address --build missing ublkpp
conan build -s:h build_type=Debug -o sanitize=thread --build missing ublkpp

# Optional backends
conan build -o homeblocks=True --build missing ublkpp
conan build -o iscsi=True --build missing ublkpp
```

## 🏗️ Architecture

### Project Structure

```
ublkpp/
├── include/ublkpp/       # Public headers
│   ├── raid/             # RAID0, RAID1 interfaces
│   └── drivers/          # FSDisk, iSCSIDisk, HomeBlkDisk
├── src/
│   ├── driver/           # Backend implementations
│   ├── lib/              # Core UblkDisk base classes
│   ├── metrics/          # I/O and RAID metrics
│   ├── raid/             # RAID logic (bitmap, superblock)
│   └── target/           # ublkpp_tgt
└── example/              # Sample applications
```

### Core Abstractions

- **`UblkDisk`**: Base class for all block devices
- **`FSDisk`**: File-based block device implementation
- **`Raid0Disk`**: Striping across multiple devices
- **`Raid1Disk`**: Mirroring with bitmap-based resilience
- **`UblkTarget`**: Exposes devices to kernel via ublk

## 💾 RAID Features

### RAID0 (Striping)

- Configurable stripe size (default: 128 KiB)
- Distributes data across devices for performance
- Linear capacity aggregation

### RAID1 (Mirroring)

**Key Features:**
- Two-way mirroring with dirty bitmap tracking
- Degraded mode operation (single device failure)
- Hot device replacement via `swap_device()`
- Optimistic recovery with INTERNAL writes
- Configurable read routing (PRIMARY, SECONDARY, EITHER)

**Bitmap Efficiency:**
- 4 KiB pages track 32 KiB chunks (default)
- Memory footprint: ~0.4% of capacity (e.g., 8 MiB for 2 TB)
- SuperBitmap optimization for fast initialization

**Resync Features:**
- Background resync with I/O coordination
- Atomic counter synchronization between I/O and resync
- Pause/resume on write operations
- Configurable delay intervals

### RAID10 (Stripe of Mirrors)

- RAID0 striping across RAID1 pairs
- Combines performance and redundancy
- Requires even number of devices (min: 4)

## 🖥️ Example Application

The `ublkpp_disk` application demonstrates all RAID capabilities with a single target.

### Build and Run

```bash
# Build release version
conan build -s:h build_type=Release --build missing ublkpp

# Load kernel module
sudo modprobe ublk_drv

# Create backing files
fallocate -l 2G file1.dat
fallocate -l 2G file2.dat
fallocate -l 2G file3.dat
fallocate -l 2G file4.dat

# Launch RAID10 device
sudo ublkpp/build/Release/example/ublkpp_disk --raid10 file1.dat,file2.dat,file3.dat,file4.dat
```

### Usage Examples

```bash
# Single device (loop mode)
sudo ublkpp_disk --loop /dev/sdb

# RAID0 (striping)
sudo ublkpp_disk --raid0 /dev/sdc,/dev/sdd --stripe_size 262144

# RAID1 (mirroring)
sudo ublkpp_disk --raid1 /dev/sde,/dev/sdf

# RAID10 (4+ devices)
sudo ublkpp_disk --raid10 file1.dat,file2.dat,file3.dat,file4.dat

# Recover existing device
sudo ublkpp_disk --device_id 0 --raid1 /dev/sde,/dev/sdf
```

### Verify Device

```bash
$ lsblk
NAME        MAJ:MIN RM  SIZE RO TYPE MOUNTPOINTS
...
ublkb0      259:3    0    4G  0 disk

# Make Filesystem
$ sudo mkfs.xfs /dev/ublkb0
$ sudo mount /dev/ublkb0 /mnt
```

## 🛠️ Development

### Code Style

- **Indentation**: 4 spaces
- **Line Length**: 120 characters
- **Pointers**: Left alignment (`Type* ptr`)
- **Standard**: C++23
- **Headers**: `#pragma once`

### Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Classes | PascalCase | `Raid1Disk` |
| Functions | snake_case | `async_iov()` |
| Members | _snake_case | `_device` |
| Constants | k_snake_case | `k_page_size` |
| Macros/Enums | SCREAMING_SNAKE_CASE | `UBLK_IO_OP_WRITE` |

### Workflow

```bash
# 1. Write code
# 2. Write tests (see Testing section)
# 3. Format code
./apply-clang-format.sh

# 4. Build and test
conan build -s:h build_type=Debug --build missing ublkpp
```

### Error Handling

Uses `std::expected<T, std::error_condition>` pattern:

```cpp
using io_result = std::expected<int, std::error_condition>;

io_result write_data(uint64_t addr, uint32_t len) {
    if (auto res = device->sync_iov(UBLK_IO_OP_WRITE, iov, 1, addr); !res) {
        DLOGE("Write failed at {:#x}: {}", addr, res.error().message());
        return res;
    }
    return len;
}
```

## 🧪 Testing

### Test Organization

```
src/<component>/tests/
├── test_*_common.hpp      # Shared test utilities
├── simple/                # Basic functionality tests
├── failures/              # Error handling tests
├── bitmap/                # RAID1 bitmap tests
└── superblock/            # Superblock I/O tests
```

### Running Tests

```bash
# Tests run automatically during build
conan build -s:h build_type=Debug --build missing ublkpp

# Coverage report
conan build -s:h build_type=Debug -o coverage=True --build missing ublkpp
# View: build/Debug/coverage_html/index.html

# Thread sanitizer
conan build -s:h build_type=Debug -o sanitize=thread --build missing ublkpp

# Address sanitizer
conan build -s:h build_type=Debug -o sanitize=address --build missing ublkpp
```

### Writing Tests

Framework: Google Test (GTest) with GMock

```cpp
#include "test_raid1_common.hpp"

TEST(Raid1, YourTestName) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = 2 * Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = 2 * Gi});

    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);

    auto raid = ublkpp::Raid1Disk(uuid, device_a, device_b);

    // Test logic...
    EXPECT_EQ(expected, actual);
}
```

## 📦 Dependencies

### Core Dependencies

- **[sisl](https://github.com/eBay/sisl)**: Logging, options, utilities
- **[ublksrv](https://github.com/ublk-org/ublksrv)**: ublk driver interface
- **boost**: UUID generation
- **liburing**: io_uring support

### Optional Dependencies

- **[iomgr](https://github.com/eBay/IOManager)**: Async I/O (for testing)
- **[homeblks](https://github.com/eBay/HomeBlocks)**: HomeBlocks backend
- **libiscsi**: iSCSI backend support

### Build Tools

- Conan 2.0+
- CMake 3.22+
- clang-format (code formatting)
- gcovr (coverage reporting)

## 📚 Documentation

### Using as a Library

ublkpp can be integrated into your project as a Conan package to build custom block devices:

- **[Library Guide](docs/LIBRARY.md)**: Architecture, core concepts, and design patterns
- **[Integration Guide](docs/INTEGRATION.md)**: Consuming via Conan/CMake with examples
- **[Extension Guide](docs/EXTENDING.md)**: Creating custom RAID types and backend drivers
- **[API Reference](docs/API.md)**: Complete API documentation

### Development & Contributing

- **[CHANGELOG.md](CHANGELOG.md)**: Version history and release notes
- **[CLAUDE.md](.claude/CLAUDE.md)**: Development guidelines and workflows
- **[Linux ublk Documentation](https://docs.kernel.org/block/ublk.html)**: Kernel driver details

## 🤝 Contributing

Contributions are welcome! Please:

1. Follow the code style (run `./apply-clang-format.sh`)
2. Add tests for new functionality
3. Update CHANGELOG.md and version in conanfile.py
4. Ensure all tests pass with sanitizers
5. Submit pull requests against `main`

## 📄 License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

**Primary Author**: [Brian Szmyd](https://github.com/szmyd)

---

**Links:**
- 🐛 [Report Issues](https://github.com/szmyd/ublkpp/issues)
- 💬 [Discussions](https://github.com/szmyd/ublkpp/discussions)
- 📖 [ublksrv GitHub](https://github.com/ublk-org/ublksrv)
