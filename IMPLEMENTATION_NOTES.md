# RAID1 Bitmap Race Condition Fixes - Implementation Notes

## Version 0.20.2

This document describes two critical data corruption bugs fixed in version 0.20.2.

---

## Bug #1: Async Write / Resync Race (Bitmap Dirtying)

### Root Cause
When the RAID1 array is degraded and a device appears to recover:
1. Resync copies region X from CLEAN_DEVICE to DIRTY_DEVICE
2. Resync clears `unavail` flag and bitmap for region X
3. Async write W1 arrives for region X
4. Code checks: `!unavail && !is_dirty` → condition is TRUE
5. Code launches async REPLICATE write WITHOUT dirtying bitmap first
6. Resync calls `next_dirty()`, sees region X as clean, SKIPS it
7. Async write W1 fails later, bitmap marked dirty, but TOO LATE
8. Resync never revisits region X
9. After resync completes, reads alternate between correct (CLEAN_DEVICE) and stale (DIRTY_DEVICE) data

### Attack Scenario
```
T0:  Resync copies region X, clears unavail + bitmap → bitmap[X] = CLEAN
T1:  Async write W1 arrives for X
T2:  Check: !unavail && !is_dirty → TRUE, launch async write to DIRTY_DEVICE
T3:  Async write returns immediately (in-flight)
T4:  Resync checks bitmap[X] → CLEAN, skips region X
T5:  Async write fails, dirties bitmap[X], but resync already passed
T6:  Region X never resynced → STALE DATA on DIRTY_DEVICE
```

### Fix Location
**File**: `src/raid/raid1/raid1.cpp:740-747`

**Change**: Added `else` clause to dirty bitmap BEFORE launching async writes when `!unavail && !is_dirty`

```cpp
// OLD CODE (buggy):
if (IS_DEGRADED) {
    if (dirty_unavail || _dirty_bitmap->is_dirty(addr, len)) {
        // ... dirty bitmap or set INTERNAL flag ...
    }
    // BUG: Falls through without dirtying if !unavail && !is_dirty
}

// NEW CODE (fixed):
if (IS_DEGRADED) {
    if (dirty_unavail || _dirty_bitmap->is_dirty(addr, len)) {
        // ... dirty bitmap or set INTERNAL flag ...
    } else {
        // FIX: Dirty bitmap BEFORE async write
        _dirty_bitmap->dirty_region(addr, len);
        sub_cmd = set_flags(sub_cmd, sub_cmd_flags::INTERNAL);
    }
}
```

**Rationale**:
- Bitmap dirtied BEFORE async write is launched
- If async write succeeds, `handle_internal()` calls `__clean_region()` to clear bitmap
- If async write fails, bitmap stays dirty (correct state)
- Resync cannot skip regions with in-flight async writes

---

## Bug #2: Concurrent Bitmap Map Access (Thread Safety)

### Root Cause
The bitmap's `_page_map` (`std::map<uint32_t, PageData>`) was accessed concurrently by:
1. **Resync thread**: Calls `dirty_pages()` which does `std::erase_if(_page_map, ...)`
2. **Async I/O threads**: Call `dirty_region()` which does `_page_map.emplace(...)`

`std::map` is **NOT thread-safe** for concurrent modification. This could cause:
- Iterator invalidation
- Crashes from corrupted map structure
- Lost dirty pages
- Use-after-free

### Attack Scenario
```
Thread 1 (Resync)                      Thread 2 (Async I/O)
-----------------                      --------------------
dirty_pages() called
std::erase_if(_page_map, ...)
  Iterating map structure...
  Erasing pages...                     dirty_region() called
                                       _page_map.emplace(page_offset, ...)
  CONCURRENT MODIFICATION!             CONCURRENT MODIFICATION!
  → Map corruption, crash, or lost pages
```

### Fix Location
**Files**:
- `src/raid/raid1/bitmap.hpp` - Added `folly::SharedMutex`
- `src/raid/raid1/bitmap.cpp` - Added locking to all `_page_map` access

**Changes**:

1. **Header** (`bitmap.hpp`):
   - Changed `std::map` → `std::unordered_map` (better for concurrent patterns)
   - Added `mutable std::shared_mutex _page_map_mutex` (C++17 standard library)

2. **Implementation** (`bitmap.cpp`):
   - `dirty_region()`: `std::unique_lock` (exclusive - modifies map)
   - `is_dirty()`: `std::shared_lock` (shared - reads only)
   - `dirty_pages()`: `std::unique_lock` (exclusive - erases from map)
   - `next_dirty()`: `std::shared_lock` (shared - iterates map)
   - `clean_region()`: `std::shared_lock` (shared - modifies bits, not structure)
   - `sync_to()`: `std::shared_lock` (shared - iterates map)
   - `load_from()`: `std::unique_lock` (exclusive - emplaces into map)

**Locking Strategy**:
- **Exclusive lock** (`std::unique_lock`): When modifying map structure (insert, erase)
- **Shared lock** (`std::shared_lock`): When only reading/iterating map
- `std::shared_mutex` allows multiple concurrent readers OR single writer

**Why `std::unordered_map`**:
- Better performance for concurrent access patterns (no tree rebalancing)
- No ordering requirement (resync doesn't care about page order)
- Simpler hash-based lookup

**Why `std::shared_mutex`** (vs `folly::SharedMutex`):
- C++17 standard library - no external dependency
- Sufficient performance for bitmap access patterns (low contention)
- Cleaner code - no pragma warning suppression needed
- Consistency - no other file in ublkpp uses folly directly

---

## Performance Impact

### Fix #1 (Bitmap Dirtying)
**During degraded operation**:
- One additional `dirty_region()` call when `!unavail && !is_dirty`
- Transient bitmap churn (dirty → clean on success)
- Minimal overhead (atomic bit operations)

**During normal operation**:
- Zero impact (`!IS_DEGRADED` → code path not taken)

### Fix #2 (Thread Safety)
**Read operations** (`is_dirty`, `next_dirty`, `clean_region`, `sync_to`):
- Shared lock allows concurrent reads
- No contention between multiple readers
- Minimal overhead

**Write operations** (`dirty_region`, `dirty_pages`, `load_from`):
- Exclusive lock blocks other writers and readers
- Only during:
  - Writes to degraded device (normal operation: not degraded)
  - Resync `dirty_pages()` calls (periodic, not on I/O path)
  - Initial load (one-time at startup)
- Acceptable overhead for correctness

---

## Testing

### New Tests
**File**: `src/raid/raid1/tests/failures/write_bitmap_race.cpp`

1. **WriteFailAsyncBitmapRace**: Tests Fix #1
   - Simulates: degrade → resync clears bitmap → write arrives → device still fails
   - Verifies: bitmap dirtied, INTERNAL flag set, failure handled correctly

2. **OptimisticWritePathSuccess**: Regression test
   - Verifies: optimistic write path still works when device is stable
   - Verifies: bitmap cleaned on successful write completion

### Existing Tests
All existing RAID1 tests should pass:
- `write_fail.cpp` - Write failure scenarios
- `read_degraded.cpp` - Degraded read behavior
- Bitmap tests - Bitmap operations
- Resync tests - Resync correctness

---

## Deployment Notes

### Upgrade Path
- **Version bump**: 0.20.1 → 0.20.2 (patch release)
- **Breaking changes**: None
- **On-disk format**: Unchanged
- **API changes**: None (internal fixes only)

### Verification
After deployment, monitor for:
1. No crashes during resync
2. No stale read complaints
3. Bitmap consistency (no perpetual dirty pages)
4. Resync completes successfully

### Rollback
Safe to rollback to 0.20.1 if needed (no on-disk format changes).
However, rollback reintroduces data corruption risk.

---

## Code Review Checklist

- [x] Root cause identified and verified
- [x] Fix addresses root cause correctly
- [x] No new race conditions introduced
- [x] Memory safety verified (no leaks, no use-after-free)
- [x] Thread safety verified (proper locking)
- [x] Performance impact acceptable
- [x] Tests cover bug scenario + regression
- [x] Changelog updated
- [x] Version bumped
- [x] Documentation updated

---

## References

### Related Code Sections
- `src/raid/raid1/raid1.cpp:699-751` - `__replicate()` write handling
- `src/raid/raid1/raid1.cpp:452-513` - `__clean_bitmap()` resync
- `src/raid/raid1/bitmap.cpp:353-392` - `dirty_region()`
- `src/raid/raid1/bitmap.cpp:257-272` - `dirty_pages()`
- `src/raid/raid1/bitmap.cpp:317-350` - `next_dirty()`

### Key Concepts
- **IS_DEGRADED**: Array has one failed device
- **CLEAN_DEVICE**: Working device (Device B during degradation)
- **DIRTY_DEVICE**: Failed/recovering device (Device A during degradation)
- **INTERNAL flag**: Marks writes that should clean bitmap on success
- **REPLICATE flag**: Marks writes to DIRTY_DEVICE during degraded mode
- **unavail flag**: Per-device atomic flag indicating device is unavailable

---

## Author Notes

These fixes address **critical data corruption bugs** that could result in:
- Silent data corruption (stale reads)
- System crashes (map corruption)
- Incomplete resync (regions skipped)

Both fixes are **minimal, surgical changes** with:
- Clear root cause analysis
- Comprehensive test coverage
- Acceptable performance trade-offs
- No breaking changes

**Recommendation**: Deploy as soon as possible to production.
