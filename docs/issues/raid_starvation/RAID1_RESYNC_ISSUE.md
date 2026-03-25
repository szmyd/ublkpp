# RAID-1 Resync Starvation Under Low-Rate I/O

## Problem Description

The current RAID-1 resync implementation suffers from a starvation issue when I/O arrives at a specific rate:

**Symptoms:**
- Resync makes minimal progress despite device being mostly idle
- Dirty bitmap remains dirty for extended periods
- Occurs when I/O rate is:
  - High enough to prevent idle timer from expiring
  - Low enough that steal-time logic (every 512th operation) doesn't trigger frequently

**Root Cause:**
The resync task and user I/O are mutually exclusive. The current mechanism relies on:
1. Timer-based idle transition to resume resync (blocked by trickling I/O)
2. Counter-based steal-time (every 512 ops) as fallback (insufficient for low-rate I/O)

## Current Architecture

### State Machine
```
IDLE → ACTIVE → SLEEPING → PAUSE → IDLE
```

### Flow
1. Every I/O calls `idle_transition(q, false)` → transitions resync to `PAUSE`
2. Resync waits for state to become `IDLE` before proceeding
3. `idle_transition(q, true)` called by ublksrv timer after idle period
4. Steal-time workaround: every 512th I/O forces resync to `IDLE`

### Code References
- State machine: `raid1_impl.hpp:20`
- I/O blocking: `raid1.cpp:565-591` (`idle_transition`)
- Resync task: `raid1.cpp:511-563` (`__resync_task`)
- Clean bitmap: `raid1.cpp:448-508` (`__clean_bitmap`)
- Steal-time logic: `raid1.cpp:577-582`

## Proposed Solutions

### Option 1: Time-Based Steal Logic ⚡ (Quick Fix)

**Goal:** Guarantee resync progress with minimal code changes.

**Implementation:**
- Replace I/O counter with timestamp tracking
- Force resync to `IDLE` every N milliseconds (e.g., 100ms)
- Still mutually exclusive, but guarantees regular timeslices

**Changes Required:**
```cpp
// In raid1_impl.hpp:
std::atomic<uint64_t> _last_resync_time{0};  // nanoseconds since epoch

// In idle_transition():
auto now = std::chrono::steady_clock::now().time_since_epoch().count();
auto last = _last_resync_time.load(std::memory_order_relaxed);
if ((now - last) > 100'000'000) {  // 100ms
    if (_last_resync_time.compare_exchange_weak(last, now)) {
        _resync_state.compare_exchange_strong(cur_state,
                                             static_cast<uint8_t>(resync_state::IDLE));
    }
}
```

**Pros:**
- Minimal changes (~10 lines)
- Guarantees forward progress
- Low risk

**Cons:**
- Still mutually exclusive
- Doesn't solve fundamental concurrency issue

---

### Option 2: Range-Based Concurrent Resync 🎯 (Recommended)

**Goal:** Allow concurrent I/O and resync for non-overlapping regions.

**Key Insight:**
- Only block I/O when it overlaps with the region currently being resynced
- Most I/O doesn't overlap → runs concurrently
- Eliminates timer dependency

**Implementation:**
```cpp
// Track active resync region
struct ResyncRegion {
    uint64_t start;
    uint64_t end;
};
std::atomic<ResyncRegion*> _active_resync_region{nullptr};

// Check for overlap
bool __regions_overlap(uint64_t addr, uint32_t len) const {
    auto* region = _active_resync_region.load(std::memory_order_acquire);
    if (!region) return false;
    uint64_t io_end = addr + len;
    return !(io_end <= region->start || addr >= region->end);
}

// Modified I/O path - only block if overlapping
io_result Raid1DiskImpl::async_iov(...) {
    if (__regions_overlap(addr, len)) {
        // Pause resync for this region
        auto cur_state = static_cast<uint8_t>(resync_state::ACTIVE);
        while (!_resync_state.compare_exchange_weak(cur_state,
                                                    static_cast<uint8_t>(resync_state::PAUSE))) {
            if (cur_state == static_cast<uint8_t>(resync_state::PAUSE)) break;
            std::this_thread::sleep_for(10us);
        }
    }

    auto result = __replicate(...);

    // Resume resync if we paused it
    if (__regions_overlap(addr, len)) {
        auto pause_state = static_cast<uint8_t>(resync_state::PAUSE);
        _resync_state.compare_exchange_strong(pause_state,
                                             static_cast<uint8_t>(resync_state::IDLE));
    }

    return result;
}

// Resync announces regions it's working on
resync_state Raid1DiskImpl::__clean_bitmap() {
    while (0 < nr_pages) {
        auto [logical_off, sz] = _dirty_bitmap->next_dirty();

        ResyncRegion region{logical_off, logical_off + sz};
        _active_resync_region.store(&region, std::memory_order_release);

        // ... copy region ...

        _active_resync_region.store(nullptr, std::memory_order_release);
        std::this_thread::sleep_for(10us);  // Brief yield
    }
}
```

**Pros:**
- Concurrent I/O and resync (non-overlapping regions)
- No timer dependency
- Better throughput under load
- Resync makes continuous progress

**Cons:**
- More complex implementation
- Requires careful memory ordering
- Need to handle edge cases (overlapping writes during resync)

---

### Option 3: Copy-on-Write Resync 🚀 (Most Aggressive)

**Goal:** Zero blocking - user writes help resync complete.

**Key Insight:**
- When user writes to dirty region during resync, write to BOTH devices
- Clear dirty bit immediately
- Resync skips already-clean regions
- User I/O becomes resync accelerator

**Implementation:**
```cpp
io_result Raid1DiskImpl::async_iov(...) {
    if (UBLK_IO_OP_WRITE == ublksrv_get_op(data->iod) && IS_DEGRADED) {
        // Write to both devices
        auto result = __replicate(sub_cmd, [&](UblkDisk& d, sub_cmd_t scmd) {
            return d.async_iov(q, data, scmd, iovecs, nr_vecs,
                              addr + reserved_size);
        }, addr, len, q, data);

        // If successful on both, clear dirty region
        if (result.value() > 0 &&
            !DIRTY_DEVICE->unavail.test(std::memory_order_acquire)) {
            _dirty_bitmap->clear_region(addr, len);
        }

        return result;
    }
}

// Resync becomes simpler - no coordination needed
```

**Pros:**
- Zero I/O blocking
- User writes accelerate resync completion
- Simplest concurrency model
- Fastest resync under write-heavy workloads

**Cons:**
- Writes hit degraded device during resync (may be slow if device struggling)
- More bitmap contention
- Changes write behavior during degraded mode

---

### Option 4: Hybrid Approach 🔧

**Goal:** Combine range-based concurrency with QoS guarantees.

**Implementation:**
- Use Option 2's region tracking for concurrency
- Add periodic batching to prevent I/O starvation of resync
- Every N bytes, briefly pause to ensure fairness

```cpp
std::atomic<uint64_t> _resync_bytes_since_pause{0};
constexpr uint64_t RESYNC_BATCH_SIZE = 256 << 20;  // 256MB

resync_state Raid1DiskImpl::__clean_bitmap() {
    while (0 < nr_pages) {
        // ... region-based logic ...

        _resync_bytes_since_pause.fetch_add(iov.iov_len);

        if (_resync_bytes_since_pause.load() >= RESYNC_BATCH_SIZE) {
            _active_resync_region.store(nullptr);
            std::this_thread::sleep_for(1ms);  // Let I/O catch up
            _resync_bytes_since_pause.store(0);
        }
    }
}
```

**Pros:**
- Best of both worlds: concurrency + fairness
- Tunable performance vs. QoS tradeoff
- Prevents resync from monopolizing device

**Cons:**
- Most complex implementation
- More tuning parameters

---

## Recommendation

**Phase 1:** Implement **Option 1** (time-based steal) as immediate fix
- Low risk, guarantees forward progress
- Can be deployed quickly

**Phase 2:** Implement **Option 2** (range-based concurrency) for long-term solution
- Addresses fundamental architectural issue
- Significant performance improvement
- Maintains safety guarantees

**Optional:** Evaluate **Option 3** (COW) if workload analysis shows:
- High write ratio to dirty regions during resync
- Degraded device can handle write load

## Testing Considerations

All solutions should be validated with:

1. **Starvation test:** Low-rate I/O (< 512 ops before idle timer) while dirty
2. **Concurrency test:** High I/O rate during resync (Option 2/3)
3. **Overlap test:** I/O to regions being resynced (Option 2/3)
4. **Crash consistency:** Verify bitmap integrity after interruption
5. **Performance:** Measure resync completion time under various I/O patterns

## Metrics to Add

Consider adding metrics to track:
- Resync pause events (how often I/O blocks resync)
- Resync active time vs. paused time ratio
- Average resync batch size before interruption
- I/O wait time due to resync overlap (Option 2)

---

**Related Code:**
- `src/raid/raid1/raid1_impl.hpp:20-47` - State machine and counters
- `src/raid/raid1/raid1.cpp:448-508` - Resync bitmap cleaning
- `src/raid/raid1/raid1.cpp:511-563` - Resync task main loop
- `src/raid/raid1/raid1.cpp:565-591` - Idle transition logic
- `src/raid/raid1/raid1.cpp:828-856` - I/O path (async_iov)
