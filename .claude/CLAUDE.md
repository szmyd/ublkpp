# ublkpp - Userspace Block Device Driver

A C++ library providing RAID0/1/10 support for Linux's userspace block (ublk) driver.

## Claude Workflow

**CRITICAL: Task Execution Protocol**

For EVERY non-trivial task (code changes, bug fixes, features), follow this workflow:

### 1. **ALWAYS Start with Planning**
- Use `EnterPlanMode` for any code implementation task
- Explore the codebase to understand context
- Ask clarifying questions if requirements are unclear
- Design the approach before writing code
- Present the plan for approval

**When to plan:**
- Any code change affecting multiple files
- Bug fixes requiring investigation
- New features or functionality
- Refactoring existing code
- Performance improvements

**When to skip planning:**
- Simple one-line fixes (typos, obvious bugs)
- Documentation-only changes
- Pure research/exploration tasks

### 2. **Execute the Plan**
- Implement the approved approach
- Follow development workflow (see below)
- Stay focused on the plan

### 3. **ALWAYS Finish with Review & Analysis**
After completing ANY task, perform a self-review:

**Analyze what was done:**
- Review all code changes for correctness
- Check for potential issues (race conditions, memory leaks, edge cases)
- Verify tests cover the changes
- Confirm formatting was applied
- Look for security vulnerabilities
- Consider performance implications
- Validate error handling is complete

**Report findings:**
- Summarize what was implemented
- Call out any concerns or trade-offs
- Suggest follow-up improvements if needed
- Confirm all workflow steps were completed

**This review is MANDATORY** - never skip it, even for small changes.

## Quick Reference

**Note:** Build environment (local vs SSH) is configured in your personal `~/.claude/CLAUDE.md`.
The commands below are generic - Claude will execute them in your configured environment.

### Build Commands
```bash
# Debug build
conan build -s:h build_type=Debug --build missing ublkpp

# Release build
conan build -s:h build_type=Release --build missing ublkpp

# With coverage
conan build -s:h build_type=Debug -o coverage=True --build missing ublkpp

# With sanitizers
conan build -s:h build_type=Debug -o sanitize=True --build missing ublkpp
```

### Testing
```bash
# Run all tests
ctest

# Individual test binary
./build/Debug/test_raid0 -cv 2
```

### Code Formatting
```bash
# Apply clang-format
./apply-clang-format.sh

# Validate formatting only
./apply-clang-format.sh -v
```

**CRITICAL: Auto-format After Every Code Change**
- After making ANY code change (.cpp, .hpp files), automatically run:
  ```bash
  ./apply-clang-format.sh
  ```
- This ensures code style consistency
- Run AFTER writing tests, BEFORE committing

## Project Structure

```
src/
├── driver/        # Disk drivers (FSDisk, iSCSIDisk, HomeBlkDisk)
├── lib/           # Core library (UblkDisk base class, utilities)
├── metrics/       # Metrics collection (IO, FSDisk, RAID)
├── raid/          # RAID implementations
│   ├── raid0/     # RAID0 striping
│   └── raid1/     # RAID1 mirroring (bitmap, superblock)
└── target/        # UBlk target (ublkpp_tgt)

include/ublkpp/    # Public headers
example/           # Example application (ublkpp_disk)
```

## Code Conventions

### Style
- **Indentation**: 4 spaces
- **Line length**: 120 characters max
- **Header guards**: `#pragma once`
- **Pointer alignment**: Left (`Type* ptr`)
- **C++ Standard**: C++23

### Naming
- **Classes**: `PascalCase` (e.g., `Raid1Disk`, `UblkDisk`)
- **Functions**: `snake_case` (e.g., `async_iov`, `handle_flush`)
- **Member variables**: `_snake_case` with underscore prefix (e.g., `_device`, `_fd`)
- **Constants**: `k_snake_case` prefix (e.g., `k_page_size`, `k_max_time`)
- **Macros/Enums**: `SCREAMING_SNAKE_CASE`
- **Namespaces**: lowercase (e.g., `ublkpp`, `raid1`)

### Error Handling
- Use `std::expected<T, std::error_condition>` for fallible operations
- Return type alias: `io_result` = `std::expected<int, std::error_condition>`
- Return negative errno-style values on error
- Log errors before returning: `DLOGE("...", strerror(errno))`

### Logging
- Use SISL logging macros: `RLOGW`, `DLOGE`, `TLOGD`, `TLOGE`, `LOGINFO`
- Log modules: `ublksrv`, `ublk_tgt`, `ublk_raid`, `ublk_drivers`, `libiscsi`
- Verbose logging: `DLOGT`, `TLOGT`, `RLOGT`

## RAID Implementation

### Supported Types
- **RAID0**: Striping (up to 64 devices)
- **RAID1**: 2-way mirroring with bitmap-based dirty tracking
- **RAID10**: RAID0 of RAID1 pairs (requires even device count)

### RAID1 Specifics
- **Bitmap**: Sparse allocation, 4 KiB pages, 32 KiB chunks (default)
- **SuperBitmap**: 4,022 bytes, tracks which bitmap pages exist
- **Memory formula**: Each 4 KiB page tracks 1 GiB of data
  - Example: 2 TB volume = ~2,048 pages × 4 KiB = 8 MiB (worst-case, 100% dirty)
  - Typical (10% dirty): ~0.8 MiB
- **Resync**: Background thread, configurable priority level (0-32, default: 4)
- **Read routing**: EITHER (round-robin) | DEVA | DEVB

### Default Configuration
```cpp
max_io_size:   512 KiB  // Maximum I/O size before split
nr_hw_queues:  1        // Number of hardware queues per target
qdepth:        128      // I/O queue depth per target
chunk_size:    32 KiB   // RAID1 bitmap chunk size
stripe_size:   128 KiB  // RAID0 stripe size (example)
```

## Memory Calculations

### I/O Infrastructure (per volume)
```
I/O Buffers:         max_io_size × qdepth × nr_hw_queues
                     = 512 KiB × 128 × 1 = 64 MiB

Thread Stack:        8 MiB (I/O handler thread)

io_uring overhead:   ~40 KiB (SQ, CQ, descriptors)
```

### RAID1 Bitmap Memory
```
Formula: Device_TiB × 1024 × 4 KiB = worst-case bitmap pages

Example:
  2 TB volume  = 2,048 GiB ÷ 1 GiB/page = 2,048 pages
  Memory       = 2,048 × 4 KiB = 8 MiB (worst-case, 100% dirty)
               = ~0.8 MiB (typical, 10% dirty)
               = ~4 KiB (clean, just SuperBitmap)
```

### Total Memory (typical 2TB RAID1 volume)
```
I/O Infrastructure:  72 MiB
RAID1 Overhead:      8 MiB (resync thread, superblocks, metrics)
Bitmap:              0.8 MiB (10% dirty)
Malloc overhead:     4 MiB (5%)
────────────────────────────
TOTAL:               ~85 MiB per volume
```

## Development Workflow

### **CRITICAL: Required Steps for Every Code Change**

**1. Write the code change**

**2. Write tests** (UNLESS it's ublksrv calls, docs, or build config)
   - Create/update test in appropriate `tests/` directory
   - Test should verify the new/changed behavior
   - **Don't run tests manually** - they run automatically during build

**3. Apply clang-format** (ALWAYS for .cpp/.hpp changes)
   ```bash
   ./apply-clang-format.sh
   ```

**4. Build** (this automatically compiles and runs tests)
   ```bash
   conan build -s:h build_type=Debug --build missing ublkpp
   ```

**Complete workflow:**
```
Code Change → Write Test → Apply clang-format → Build (auto-runs tests) → Done
```

**Note:** Tests are executed automatically by the conan build command - no need to run ctest separately.

## Testing Guidelines

### **Test-First Development**
**For EVERY code change you make, write a test UNLESS:**
- The change is only to ublksrv calls (external library)
- The change is documentation-only
- The change is to build configuration

**Test writing workflow:**
1. Identify the changed functionality
2. Create/update test in appropriate `tests/` directory
3. Test should verify the new/changed behavior
4. Tests will run automatically during next build
5. If tests can't be written, explain why

### Test Organization
- Tests located in `src/<component>/tests/`
- Test file naming: `test_*.cpp` or `*_test.cpp`
- Test subdirectories by feature (e.g., `raid1/tests/bitmap/`, `raid1/tests/superblock/`)
- Common test utilities in `test_*_common.hpp` files

### Test Framework
- Google Test (GTest/GMock)
- Test macros: `TEST()`, `EXPECT_EQ()`, `ASSERT_*`
- Mock objects for disk operations
- Use `EXPECT_*` for non-fatal assertions, `ASSERT_*` for fatal ones

### Test Patterns
- **Unit tests**: Test individual functions/methods in isolation
- **Integration tests**: Test component interactions
- **Mock external dependencies**: Use GMock for disk I/O, external libraries
- **Test edge cases**: Boundary conditions, error paths, race conditions

### Coverage
- High threshold: 80%
- Medium threshold: 65%
- Generate with: `conan build -o coverage=True`
- Aim for 100% coverage of new code

## Dependencies

### Core Libraries
- `sisl` - Utility library (logging, options, thread factory)
- `ublksrv` - Userspace block server library (custom recipe in 3rd_party/)
- `iomgr` - I/O manager (test dependency)

### Build Tools
- Conan 2.0
- CMake 3.x
- C++23 compiler (GCC/Clang)
- clang-format (LLVM style)

## Common Tasks

### Adding a New RAID Type
1. Create directory: `src/raid/raidX/`
2. Implement `UblkDisk` subclass in `raidX.{cpp,hpp}`
3. Add superblock structure in `raidX_impl.hpp`
4. Add tests in `src/raid/raidX/tests/`
5. Update CMakeLists.txt to include new component

### Debugging Memory Usage
1. Check I/O buffer allocation: `max_io_size × qdepth × nr_hw_queues`
2. For RAID1, check bitmap: `dirty_pages()` method
3. Monitor RSS with: `ps -o rss,vsz,cmd -p $(pgrep ublkpp)`
4. Check malloc overhead with sanitizers or valgrind

## Git Workflow

- Main branch: `main`
- Recent commits show PR-based workflow with descriptive titles
- CHANGELOG.md updated with version changes
- Version updated in conanfile.py. Update minor
