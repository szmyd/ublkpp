# Testing Notes for Hybrid Resync Implementation

## Quick Validation Checklist

After building, run these checks to verify the implementation works:

### 1. Compilation Check
```bash
# Should compile without errors
# Watch for any new warnings related to atomics or memory ordering
make clean && make
```

### 2. Existing Test Suite
```bash
# Run all RAID-1 tests - should pass without changes
make test
# or
./raid1_tests
```

### 3. Basic Functionality
- Device can enter degraded mode
- Resync task starts
- Resync completes successfully
- I/O works during resync
- Device becomes clean after resync

## New Behavior to Verify

### Expected Improvements

**Before (old implementation):**
- Resync stalls with ~100-500 IOPS trickling in
- Device mostly idle but dirty bitmap doesn't clear
- Completion time unpredictable

**After (hybrid implementation):**
- Resync makes steady progress regardless of I/O rate
- Clear progress even with constant low-rate I/O
- Predictable completion time based on dirty data size

### What Changed (User-Visible)

1. **Concurrency:** I/O to clean regions doesn't wait for resync
2. **Progress:** Resync progresses in 256MB batches with 1ms pauses
3. **Latency:** I/O only blocks when overlapping with active resync region

## Test Scenarios

### Scenario 1: Low-Rate I/O (Original Bug)
```
Goal: Verify resync completes despite trickling I/O
Setup:
  - Start with degraded RAID-1 device
  - Run background I/O at 100-200 IOPS (random reads/writes)
  - Rate high enough to prevent idle timer, but < 512 ops/batch

Expected:
  - Resync makes continuous forward progress
  - Completes within predictable time
  - I/O latency remains reasonable

How to measure:
  - Track dirty_pages count over time (should decrease steadily)
  - Monitor resync completion time
  - Compare to old implementation (if available)
```

### Scenario 2: High-Rate I/O
```
Goal: Verify I/O isn't starved by aggressive resync
Setup:
  - Start with degraded RAID-1 device
  - Run high-rate I/O workload (10K+ IOPS)

Expected:
  - I/O latency stays low
  - Resync pauses every 256MB for 1ms
  - Both I/O and resync make progress

How to measure:
  - Monitor I/O latency distribution
  - Check for any >10ms tail latencies
  - Verify resync still completes (slower than idle, but progresses)
```

### Scenario 3: Overlapping vs Non-Overlapping
```
Goal: Verify concurrent execution for non-overlapping regions
Setup:
  - Start with degraded RAID-1 device
  - Run I/O to specific regions:
    a) Far from dirty regions (should not block)
    b) Within dirty regions (may pause resync)

Expected:
  - Case (a): No resync pauses, concurrent execution
  - Case (b): Brief resync pauses during overlapping I/O

How to measure:
  - Add logging/metrics for pause events
  - Compare I/O latency between overlapping vs non-overlapping
```

## Debugging Tips

### If Resync Stalls

**Check:**
1. Is `_active_resync_region` being set/cleared properly?
   - Add debug logging in `__clean_bitmap()` to track region updates
2. Is resync stuck in PAUSE state?
   - Log state transitions
3. Are there many overlapping I/Os preventing progress?
   - Log overlap detection in `__pause_resync_if_overlap()`

**Workaround:**
- Reduce `RESYNC_BATCH_SIZE` to force more frequent yielding
- Increase batch pause duration from 1ms to higher value

### If I/O Latency Spikes

**Check:**
1. Are overlaps being detected correctly?
   - Verify `__regions_overlap()` logic
2. Is resync holding regions too long?
   - Check batch size and copy chunk sizes
3. Is the pause/resume logic balanced?
   - Every pause should have matching resume

**Workaround:**
- Reduce `RESYNC_BATCH_SIZE` for more frequent pauses
- Reduce `params()->basic.max_sectors` for smaller copy chunks

### If Corruption Occurs

**Check:**
1. Are pause/resume operations atomic?
   - Review memory ordering in atomic operations
2. Is region tracking accurate?
   - Verify region start/end calculations
3. Are there race conditions in overlap detection?
   - Review critical sections

**Debug:**
- Enable verbose logging for all I/O operations
- Add checksums to verify data integrity
- Run with single-threaded I/O first

## Performance Comparison

### Metrics to Collect

**Resync Performance:**
- Time to complete full resync (idle vs under load)
- Average resync throughput (MB/s)
- Number of pause events
- Average pause duration

**I/O Performance:**
- Latency (p50, p99, p99.9, max)
- Throughput (IOPS, MB/s)
- Queue depth

**System:**
- CPU utilization
- Context switches
- Lock contention

### Expected Results

Compared to old implementation:

**Improvements:**
- ✓ Faster resync under low-rate I/O (no starvation)
- ✓ Better I/O latency during resync (concurrent execution)
- ✓ More predictable completion time

**Possible Regressions:**
- Slightly higher CPU (more frequent checks)
- Slightly slower resync under 100% idle (batching overhead)
  - Should be <5% slower due to 1ms pauses every 256MB

## Known Issues / Edge Cases

### Region Tracking

**Stack-allocated region in `__clean_bitmap()`:**
- Region is stack-allocated and pointed to by atomic
- Only valid while `__clean_bitmap()` loop is active
- Cleared before function returns
- Should be safe since resync is single-threaded
- **TODO:** Consider heap allocation if issues arise

### Memory Ordering

**Current implementation uses:**
- `memory_order_acquire` for loads
- `memory_order_release` for stores
- `memory_order_relaxed` for counters

**Verify:**
- No torn reads/writes on region boundaries
- Proper happens-before relationships
- No reordering issues on ARM/weak memory models

### Overlap Detection

**Edge case:** I/O exactly at region boundary
- Current logic: `!(io_end <= region->start || addr >= region->end)`
- Should be correct (no gap or overlap)
- **Test:** Submit I/O at exact region boundaries

## Rollback Plan

If critical issues found:

### Option 1: Revert to Timer-Based (Option 1 from issue)
```cpp
// In idle_transition(), add time-based steal back:
auto now = std::chrono::steady_clock::now().time_since_epoch().count();
auto last = _last_resync_time.load(std::memory_order_relaxed);
if ((now - last) > 100'000'000) {  // 100ms
    if (_last_resync_time.compare_exchange_weak(last, now)) {
        _resync_state.compare_exchange_strong(cur_state,
                                             static_cast<uint8_t>(resync_state::IDLE));
    }
}
```

### Option 2: Disable Region Tracking Temporarily
```cpp
// In raid1_impl.hpp, comment out region tracking:
// std::atomic< ResyncRegion* > _active_resync_region{nullptr};

// In I/O paths, remove pause/resume calls:
// __pause_resync_if_overlap(...)
// __resume_resync_if_paused(...)
```

## Success Criteria

Implementation is successful if:

- [x] Code compiles without errors/warnings
- [ ] All existing tests pass
- [ ] Resync completes under low-rate I/O (<1000 IOPS)
- [ ] Resync completes under high-rate I/O (>10K IOPS)
- [ ] I/O latency <10% regression during resync
- [ ] No data corruption in stress tests
- [ ] No crashes or deadlocks in 24h stability test

## Questions for Review

1. **Is the batch size (256MB) appropriate for your workload?**
   - Consider making it configurable if different deployments need different values

2. **Is 1ms pause sufficient for I/O fairness?**
   - May need adjustment based on observed queue depths

3. **Should we add metrics for overlap detection?**
   - Would help monitor effectiveness of concurrent execution

4. **Remove `_io_op_cnt` in follow-up PR?**
   - No longer used, but kept for safety in initial implementation

5. **Thread safety review needed?**
   - Stack-allocated region pointer in atomic OK?
   - Memory ordering sufficient for all architectures?
