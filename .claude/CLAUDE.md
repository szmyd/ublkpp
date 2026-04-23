# ublkpp - Userspace Block Device Driver

C++ library providing RAID0/1/10 support for Linux's userspace block (ublk) driver.

## Claude Workflow

For EVERY non-trivial task (code changes, bug fixes, features):

### 1. **ALWAYS Start with Planning**
- Use `EnterPlanMode` for any code implementation task
- Explore codebase to understand context
- **Ask questions during planning** - if anything is unclear, ask before committing
- Design approach before writing code
- Present plan for approval

**When to plan:** Multi-file changes, bug investigations, new features, refactoring, performance work
**Skip planning:** One-line fixes, docs-only, pure research

**Challenging proposals:**
- Think critically about user-proposed solutions during planning
- If the proposal seems suboptimal: explain concerns, present trade-offs, suggest alternatives, let user decide
- Constructive pushback is valuable - don't blindly accept

### 2. **Execute the Plan**
- Implement approved approach
- Follow development workflow below
- Stay focused on plan

### 3. **ALWAYS Finish with Review & Analysis**
After ANY task, perform self-review:
- Check for race conditions, memory leaks, edge cases
- Verify tests cover changes
- Confirm formatting applied
- Look for security vulnerabilities, performance issues
- Validate error handling
- Report findings, concerns, trade-offs

**This review is MANDATORY** - never skip.

## Quick Reference

Build environment configured in `~/.claude/CLAUDE.md` (local vs SSH).

```bash
# Build Debug (auto-runs tests)
conan build -s:h build_type=Debug --build missing .

# Release / Coverage / Sanitizers
conan build -s:h build_type=Release --build missing .
conan build -s:h build_type=Debug -o ublkpp/*:coverage=True --build missing .
conan build -s:h build_type=Debug -o ublkpp/*:sanitize=address --build missing .
conan build -s:h build_type=Debug -o ublkpp/*:sanitize=thread --build missing .

# Format code (applied automatically after edits to C/C++ files)
# Run on each modified file: clang-format -style=file -i -fallback-style=none file.cpp
```

## Code Conventions

**Style:** 4-space indent, 120-char lines, `#pragma once`, left pointer alignment (`Type* ptr`), C++23

**Naming:**
- Classes: `PascalCase` (Raid1Disk)
- Functions: `snake_case` (async_iov)
- Members: `_snake_case` (_device)
- Constants: `k_snake_case` (k_page_size)
- Macros/Enums: `SCREAMING_SNAKE_CASE`
- Namespaces: lowercase (ublkpp)

**Error Handling:**
- Use `std::expected<T, std::error_condition>`
- Type alias: `io_result = std::expected<int, std::error_condition>`
- Log errors before returning: `DLOGE("...", strerror(errno))`

**Logging:**
- Use SISL macros: `RLOGW`, `DLOGE`, `TLOGD`, `TLOGE`, `LOGINFO`
- Modules: `ublksrv`, `ublk_tgt`, `ublk_raid`, `ublk_drivers`, `libiscsi`

## Development Workflow

**Every code change:**
1. Write code
2. Write tests (UNLESS ublksrv calls, docs, or build config only)
3. Apply clang-format to edited files automatically (see below)
4. Build: `conan build -s:h build_type=Debug --build missing ublkpp` (auto-runs tests)

**Formatting:**
- ALWAYS run `clang-format -style=file -i -fallback-style=none` on each edited file
- Only format files you actually modified (not entire codebase)
- Applies to: `.c`, `.cpp`, `.cc`, `.cxx`, `.h`, `.hpp`, `.hxx`, `.ipp` files
- Run immediately after Edit or Write tool calls on these file types
- Example: After editing `src/raid/raid1.cpp`, run `clang-format -style=file -i -fallback-style=none src/raid/raid1.cpp`

## Testing Guidelines

**Organization:**
- Location: `src/<component>/tests/`
- Naming: `test_*.cpp` or `*_test.cpp`
- Common utilities: `test_*_common.hpp`

**Framework:** Google Test (GTest/GMock)
- Use `EXPECT_*` for non-fatal, `ASSERT_*` for fatal assertions
- Mock external dependencies with GMock

**Patterns:**
- Unit tests: individual functions in isolation
- Integration tests: component interactions
- Test edge cases: boundaries, errors, race conditions

**Coverage targets:**
- High: 80%, Medium: 65%, Goal: 100% for new code
- Generate: `conan build -o coverage=True`

## RAID Specifics

**Types:** RAID0 (striping), RAID1 (2-way mirroring + bitmap), RAID10 (RAID0 of RAID1 pairs)

**RAID1 Bitmap:**
- 4 KiB pages, 32 KiB chunks (default)
- Memory: Each 4 KiB page tracks 1 GiB data
- Example: 2 TB volume = 8 MiB worst-case (100% dirty), ~0.8 MiB typical (10% dirty)

**Default config:** max_io_size=512KiB, nr_hw_queues=1, qdepth=128, chunk_size=32KiB

## Dependencies

**Core:** `sisl` v14+ (logging, options, metrics, HTTP server), `ublksrv`, `isa-l`
**Optional:** `libiscsi` (iSCSI backend; `-o ublkpp/*:iscsi=True`)
**Build:** Conan 2.0, CMake 3.x, C++23 (GCC 14+ or Clang 17+), clang-format

## Project Structure

```
src/
├── driver/   # FSDisk, iSCSIDisk
├── lib/      # UblkDisk base, utilities
├── metrics/  # IO, FSDisk, RAID metrics
├── raid/     # RAID0, RAID1 (bitmap, superblock)
└── target/   # ublkpp_tgt

include/ublkpp/  # Public headers
example/         # ublkpp_disk
```

## CI

Four named jobs on `ubuntu-24.04`, triggered on push to `main` and PRs targeting `main`/`feature/*`. All use `conan-channel: "dev"` (sisl@oss/dev).

| Job | Compiler | Build type | Sanitizer |
|---|---|---|---|
| GccAddressSanitize | GCC | Debug | address |
| GccThreadSanitize | GCC | Debug | thread |
| GccCoverage | GCC | Debug | none (coverage=True) |
| ClangRelease | Clang | Release | none |

`SislDeps` (builds/caches sisl upstream) runs before `UblkPPDeps` on non-PR events; on PRs it still runs but skips the cache-save step.

## Git Workflow

- Main branch: `main`
- PR-based workflow with descriptive titles
- Update CHANGELOG.md and version in conanfile.py (minor for features, patch for fixes)

## Code Review

When asked to review a PR, use `gh pr view` and `gh pr diff` to fetch details, then work through these questions before posting comments.

### Questions to always ask
- Does this code break anything?
- Are there race conditions?
- Are there logic issues?
- Could this be done better or made smaller?
- Is this part designed correctly, or could it be improved?
- Are responsibilities correctly separated, or are two concerns coupled in one place?

### How to Post Comments
- Post comments on **specific diff lines**, not as top-level PR comments
- Discuss critical findings with the user before posting
- Always include a suggested fix when raising a bug, not just the problem
- Don't reject or approve PR yourself.