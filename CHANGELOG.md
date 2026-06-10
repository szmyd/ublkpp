# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.33.0]
### Added
- md_disk support: One-way transformation to ublkpp RAID0+1 device.
- `ublkpp::md::probe(disk_handle)`: non-destructive superblock probe returning `disk_type`
  (`md_none`, `md_native`, `md_stamped`, `md_dirty`) so callers can route to the correct
  factory without external signalling.
- `ublkpp::make_raid10_disk`: RAID10 factory (RAID0 of RAID1 pairs) from a flat leaf vector.
  Pairs legs in order; assigns per-pair partition UUIDs via `name_generator("partition_K")`,
  matching the convention used by `make_md_raid10_disk` for UUID consistency.

## [0.32.11] - 2026-06-09

### Changed

- **RAID0 per-stripe iovec accumulators moved to coroutine frame**: `__distribute`'s
  `StripeAccum` buffer (previously a `thread_local` with `std::vector<iovec>` per slot) is
  now a caller-owned `std::array<StripeAccum, 64>` with a fixed `std::array<iovec, 16>` per
  slot (bound: max_io_size 1 MiB / min_stripe_size 64 KiB = 16). In `async_iov` the buffer
  lives in the coroutine frame (heap, stable for child lifetimes); in `sync_iov` it is a
  plain stack local. Eliminates the iovec lifetime hazard that required the eager
  `io_uring_submit` workaround in `async_iov` and the `iov_snap` copy in
  `Raid1Disk::__failover_read_async`. Removes `DirtyGuard`, `_max_iovecs_per_stripe`, and
  the `thread_local`; introduces `k_max_iovecs_per_stripe = 16` as a file-scope constant.

## [0.32.10] - 2026-06-05

### Fixed

- **RAID1 remount failure after resync-interrupted-by-stop race**: a race between the resync task and `stop()` in the destructor produced an on-disk state of `DEVB + clean_unmount=1 + empty superbitmap`. On second mount this hit an invariant check and threw `std::runtime_error`, making the volume unassemblable. Fixed with two cooperating changes: (1) `_start()` captures `dirty_pages()` before each `__run()` call; when STOPPING fires, if the count was non-zero and is now zero the resync cleared all chunks but was interrupted in `__yield()` — `complete()` is called immediately so the destructor sees `route=EITHER`; (2) the invariant check at mount time is replaced with a warn + `dirty_region(0, capacity())` fallback, providing defense-in-depth for any residual cases not prevented by (1). Addresses [issue #300](https://github.com/szmyd/ublkpp/issues/300).

## [0.32.9] - 2026-06-04

### Fixed

- **RAID1 read-determinism after unclean shutdown (both legs present)**: when both legs were present at startup with equal ages, `clean_unmount=0`, and `read_route=EITHER`, the code previously only logged a warning. Writes in-flight at crash time may have landed on one leg but not the other, so round-robin reads of the same LBA could return different bytes on alternating accesses — a violation of the block-device read-determinism contract that filesystems (XFS, etc.) depend on unconditionally. The fix detects this case and self-heals: manufactures a +16 age gap on the canonical leg (device_a), pins reads to it via `read_route::DEVA`, dirties the whole bitmap, and marks device_b stale (`unavail`) so writes skip it until `probe_mirror` confirms physical health. `__become_active` skips persisting device_b's superblock (new `unavail` guard), preserving the on-disk age gap so any crash mid-resync reassembles through the existing `new_device` path rather than back into this branch. Idempotent across repeated crashes; no new superblock field. Addresses [issue #290](https://github.com/szmyd/ublkpp/issues/290) Phase 1.

## [0.32.8] - 2026-06-01

### Fixed

- **P0 (raid1)**: `__become_clean()` could transition the route to `EITHER` while dirty bits remained in the bitmap, with no resync task to process them. A concurrent write whose backup leg fails calls `dirty_region()` after the resync task's `dirty_pages()==0` observation but before the CAS. Subsequent reads skipped the `is_dirty` guard (only active in degraded mode) and could be served from the stale backup — a write-acknowledgment boundary violation. Fixed by: (1) inverting the CAS/superblock write ordering; (2) re-checking `dirty_pages()` after the CAS — if dirty bits are found the transition is reversed and `__become_clean()` returns `false`; (3) `_start()` loops on the return value so the still-ACTIVE resync task re-runs `__run()` to pick up the newly-dirtied region, converging without requiring a re-launch.
- **SuperBitmap::set_bit()** now uses `memory_order_release` (was `relaxed`) to pair correctly with the `memory_order_acquire` loads in `next_set_bit()` and `dirty_pages()`, ensuring the dirty_pages() double-check in `__become_clean` sees concurrent `set_bit()` calls on all architectures.
- **bitmap.age revert** under SB write failure in `__become_degraded` was not guarded by `_ctrl_lock`, creating a data race with `__swap_device`'s concurrent `+16` age bump. The increment was already guarded; added the same guard to the revert.
- **`_clean_transition_mutex` in `async_iov` Site 1** was held across two `co_await *backup_task` suspensions. Narrowed the lock to cover only `dirty_region()` + `__become_degraded()`; backup drain `co_await` calls happen after lock release.
- **Liveness gap between `complete()==true` break and `ACTIVE→IDLE` CAS**: a write-fail during this window called `toggle_resync()` which `launch()`-EARLY_EXITs on `ACTIVE` state; dirty bits could be stranded with no resync running. Fixed by moving the `ACTIVE→IDLE` CAS inside the `_start()` while loop: after `complete()` confirms a clean bitmap the task CAS-es itself to IDLE, re-checks `dirty_pages()`, and if bits appeared in the gap attempts an `IDLE→ACTIVE` reclaim CAS to stay alive and drain them. If another `launch()` or `stop()` wins the IDLE slot, the new task handles the remaining bits.
- **H1 in `__become_clean()`** no longer attempts `write_superblock` on a missing-disk placeholder for the backup device.

## [0.32.7] - 2026-06-01

### Fixed

- **H3 (raid0)**: the array capacity was computed as `(min_leg_capacity - stripe_size) * leg_count`, which assumes every leg holds a whole number of stripes. When a leg's capacity is not a multiple of `stripe_size`, the partial trailing stripe (`leg_capacity % stripe_size`) was carried into the per-leg term and then multiplied by the leg count, over-reporting the device size by `(leg_capacity % stripe_size) * leg_count`. Reads into that phantom tail (e.g. backup-GPT / partition-table scans at the top of the device) mapped to a per-leg offset past the end of the backing device and returned EIO. The bug was latent because leg capacities happened to be stripe-aligned; raid1 SuperBlock v2 dropped the 512 KiB user-data alignment that had masked it, surfacing top-of-device read failures on RAID10 arrays (observed on a 50-leg array of 3 TiB legs over-reporting by ~3 MiB). Fixed by flooring the smallest leg to a whole number of stripes before reserving the head stripe and striping.

## [0.32.6] - 2026-05-26

### Fixed

- **H1 (raid0)**: `io_uring_submit` failure in `async_iov` left child coroutines waiting for CQEs that would never arrive, deadlocking the drain loop. Staged SQEs remaining in the ring would be submitted later with recycled `cqe_state` pointers, causing UAF. Fixed by retrying submit once; if still failing, synthetically completing pending `cqe_state` entries before draining.
- **M3 (raid0)**: `load_superblock` silently accepted on-disk superblock versions newer than `SB_VERSION`. A future format change adding fields before existing ones would corrupt data. Now throws `std::runtime_error` (returns `std::errc::not_supported`) for `sb_ver > SB_VERSION`.
- **L1 (raid0)**: `_stride_width` was `uint32_t`; for large configs (e.g. 128 MiB stripe × 64 disks = 8 GiB) the multiplication overflowed silently. Widened to `uint64_t` throughout, including `next_subcmd`/`merged_subcmds` signatures in `raid0_impl.hpp`.
- **L2 (raid0)**: Non-power-of-2 `stripe_size_bytes` was silently rounded down by `ilog2` (e.g. 6 KiB treated as 4 KiB), producing wrong geometry. Constructor now throws `std::invalid_argument` for non-power-of-2 stripe sizes.

## [0.32.5] - 2026-05-26

### Fixed

- **H5 (target)**: `ublksrv_ctrl_start_recovery` failure was logged but execution continued into subsequent recovery steps in an undefined state. Now returns `std::errc::operation_not_permitted` immediately on failure.
- **H6 (target)**: the io_uring completion handler only caught `std::exception`; any other thrown type would propagate through the `noexcept` coroutine boundary and terminate the process. Added a `catch (...)` fallback that completes the IO with `-EIO`.
- **M8 (target)**: `sem_destroy` was never called on the queue semaphore, leaking an OS resource on every device teardown.
- **M7 (driver)**: `FSDisk` could `throw` during `fstat`/`ioctl`/`fcntl` without closing `_fd`, leaking the file descriptor. Added an RAII `FdGuard` that closes `_fd` on any exception path.
- **L3 (driver)**: `ilog2(0)` is undefined behaviour (SIGFPE). Added explicit zero-checks for `lbs` and `pbs` before calling `ilog2` on both block-device and regular-file code paths.

## [0.32.4] - 2026-05-25

### Fixed
- raid1: Make stable copy of iovecs in __failover_read

## [0.32.3] - 2026-05-25
### Fixed
- raid1: Remove Optimistic write path which can now race with region tracked resync.

## [0.32.2] - 2026-05-24

### Changed
- **Ban `resync_level=0`**: construction now throws `std::runtime_error` if `resync_level` is set
  to 0. A zero level silently produced zero `copies_left`, causing the resync loop to stall with
  no forward progress. Valid range is 1–32.

## [0.32.1] - 2026-05-24
### Fixed
- Gcc-16 compilation errors

## [0.32.0] - 2026-05-20

### Changed
- **Fixed on-disk reserved region**: `_reserved_size` is now always
  `sizeof(SuperBlock) + k_superbitmap_bits × k_page_size` (~125.6 MiB) regardless of capacity,
  leaving headroom for future volume resize without a format change. `init_to` still writes only
  `_num_pages` (capacity-derived) zero pages — the remainder of the reserved region is claimed by
  layout, not pre-written.
- **Tighter user-data alignment (v2)**: `_reserved_size` padding now aligns to `logical_bs` (~4 KiB)
  instead of `max_sectors_bytes` (~512 KiB), reclaiming up to ~511 KiB of wasted tail space per
  device. v1 arrays keep the old alignment exactly.
- **`SB_VERSION` bumped 1 → 2**: new arrays are stamped v2. Existing v1 arrays open as-is;
  `__init_params` branches on the version field to reconstruct the original capacity-proportional
  `_reserved_size`, preserving the exact on-disk layout.
- Constructor call order fixed: `__load_and_select_superblock` now runs before `__init_params`
  so the SB version is available when choosing the alignment policy.

## 0.31.0 raid1: replace global PAUSE with lock-free per-region write tracker
- Replace global `PAUSE` state with `RegionTracker`: a lock-free flat slot array that tracks
  `(lba, len)` of each in-flight write. Resync now yields only for chunks that actually conflict
  with an in-flight write; unrelated regions copy without any yield.
- Two-phase conflict check: Phase 1 (`overlaps()`) before copy, Phase 2 (`overlaps()` +
  `completed_since()`) after copy. Shadow completion ring closes the race where a write arrives
  and completes entirely during the READ window.
- `resync_skip_from` cursor + `next_dirty_after()`: prevents low-LBA dirty runs from starving
  higher-LBA runs under sustained write pressure.
- Remove `PAUSE` state, `__pause()`, and `sisl::atomic_status_counter`; replace with plain
  `std::atomic<resync_state>` (4 states: IDLE, ACTIVE, SLEEPING, STOPPING).
- `copies_left` budget consumed only by actual copy attempts; Phase 1 skips are free.

## 0.30.0 refactor: async coroutine I/O, public API 1.0.0 uplift, and RAID1 hardening
 - Async I/O (Phases 1-11):
   - cqe_state pool per queue; raw cqe_state* encoded directly in SQE user_data
   - disk_task<T>/hot_task<T> coroutines with zero-overhead symmetric transfer
   - exec::task<void> + stdexec async_scope replace co_io_job; scope.on_empty() drains in-flight work
   - All disks implement pure-virtual async_iov; handle_io_async removed from vtable
   - ublkpp_tgt owns the io_uring CQE loop (run_queue_loop); destroy() renamed to remove()
   - ResyncWriteGuard RAII ensures enqueue/dequeue_write is always balanced in RAID1
 - Public API (1.0.0 prep):
   - make_fs_disk/make_raid0_disk/make_raid1_disk/make_missing_disk factories replace direct construction
   - ublk_disk base + disk_handle ownership alias; geometry through explicit getters
   - Public headers consolidated: drivers.hpp, raid.hpp, target.hpp
 - **Breaking**: removed `handle_rw`, `queue_tgt_io`, `handle_discard`, `sub_cmd_t`, and the `open_for_uring`/`handle_io_async`/`handle_iov_async` virtual surface -- replaced by `prepare` and `async_iov`
 - **Breaking**: updaetd sisl to v14.x which drops Folly and iomgr (in testing) dependencies.

## 0.22.2
- raid1: Fix `stop()` IDLE→STOPPING race - when the resync thread finishes naturally and
  `stop()` is called before `join()`, the `IDLE+joinable` handler now returns `SUCCESS` instead
  of `RETRY_WITH_SLEEP`, preventing an accidental `CAS(IDLE→STOPPING)` that left no thread to
  clear the state; subsequent `launch()` call in `swap_device()` would spin forever.

## 0.22.1
- raid1: Fix dequeue/resume race - `_resync_state` and `_outstanding_writes` are now packed into a single `sisl::atomic_status_counter` so the counter decrement and PAUSE→ACTIVE transition are one indivisible CAS; `__resume()` is removed.
- raid1: Fix enqueue/pause race - `enqueue_write()` now always calls `__pause()` on every enqueue, not only the first; previously a concurrent second enqueuer could skip `__pause()` while the first was still establishing it, allowing resync to overwrite an in-flight write with stale data
- raid1: Replace GCC `__builtin_popcount`/`__builtin_clz`/`__builtin_ctz` with C++23 `std::popcount`/`std::countl_zero`/`std::countr_zero`
- build: `libatomic` is now declared as a Conan system lib on Linux - propagated automatically to consumers, no downstream changes required

## 0.22.0
- raid1: Fix multi-queue idle probe race conditions - probes now start only when all queues are idle, mutex serializes concurrent launch/stop calls, `open_for_uring` counts queue threads for accurate `nr_hw_queues`
- **Breaking**: `UblkDisk::open_for_uring` signature changed from `(int)` to `(ublksrv_queue const*, int)` - out-of-tree subclasses must update their override

## 0.21.6
- raid0: Fix stale alive_cmds in __distribute() corrupting the next I/O on the same thread after a failed multi-stride operation

## 0.21.5
- raid1: Only log in reference to Resync if it was actually running.

## 0.21.x
- raid0: Fix stale alive_cmds in __distribute() corrupting the next I/O on the same thread after a failed multi-stride operation
- raid1: Only log in reference to Resync if it was actually running.
- raid1: Fix resync task hang when stop() is called during unavail wait loop
- raid1: Fix concurrent launch()/stop() race on resync task thread assignment
- raid1: Fix incorrect device logged as degraded in __become_degraded (DEVB route)
- raid1: Fix Raid1ResyncTask::launch() crash on joinable thread reassignment
- raid1: Fix remount failure when bringing up a degraded array with a defunct device
- fio_engine: Pre-populate backing files with zeros after allocation to convert unwritten extents to written state, eliminating first-write journal cost and page fault overhead that inflated write latency on fresh files. Raw block devices are detected and skipped.
- New functional testing framework. See `docs/functional_testing.md` for details.
- raid1: Introduce `UNAVAIL` replica state for transient read failures, routing I/O away from replicas that fail reads without marking them fully offline. A new periodic health monitor (`Raid1AvailProbeTask`) probes unavailable replicas during idle periods and restores them to active routing once they recover. Resync logic is updated to skip copies to unavailable mirrors and to wait for a device to become available before proceeding.

## 0.20.x
- raid1: Fix on_io_complete metrics mapping after device swap - I/O metrics were incorrectly attributed to the wrong device when completions occurred after swap_device() changed the route.
- Fix some more cases where __capture_route_state was being misused.
- raid1: Make Bitmap lock-free by using pre-allocated vector.
- RouteState atomicity: Introduced RouteState struct and __capture_route_state() to atomically capture all 
  route-derived values (active/backup devices, subcmds, degraded flag) at function entry
- Eliminated unsafe macros: Removed CLEAN_DEVICE, DIRTY_DEVICE, IS_DEGRADED, CLEAN_SUBCMD, DIRTY_SUBCMD macros that
  could evaluate differently mid-operation when swap_device changes the route
- raid1: Fix some device swap races by consolidating atomic loads
- raid1: Refactor the Raid1ResyncTask to its own component
- raid1: Consolidate state machine logic.
- raid1: Add outstanding write counter, trigger resync on this.
- **CRITICAL FIX**: raid1: Add thread-safe synchronization to bitmap page map
  - Protects bitmap `_page_map` structure from race conditions between resync thread and async I/O
  - Shared locks for readers (`is_dirty`, `next_dirty`, `clean_region`, `sync_to`) allow concurrent reads
  - Exclusive locks for writers (`dirty_region`, `dirty_pages`, `load_from`) prevent structural corruption
  - Fixes potential crashes and data corruption from concurrent map modification during resync
- raid1: Stop resync before touching bitmap during swap.
- raid1: Ability to bring RAID1 online with missing device.

## 0.19.x
- ublkpp_tgt: Remove DEFER_TASK from flags passed to ublksrv_queue_init
- ublkpp_tgt: Allow oom killing of process.

## 0.18.x
- ublkpp_tgt: Shutdown control device asynchronously.
- ublkpp_tgt: add get device destroy api
- ublkpp_tgt: add get device id api
- raid1: Fix data races accessing superblock read_route field 
- raid0: Increase route from 4 to 6 bits.
- bitmap: SuperBitMap introduction. Improve the bitmap load_from speed by reading superbitmap first.
- Feature: Device Recoverability

## 0.17.x
- bitmap: Increased shutdown speed by batching sync_to requests and tracking the unchanged pages
- iomgr: Shutdown during process termination.
- Introduce Metrics gathering

## 0.16.x
- fs_disk: Fix probing devices w/o partition.
- raid1: Swap with no changes should have zero impact.
- raid1: Reduce instance where a full copy is performed.
- raid1: Fix disk position identification.
- raid1: Improve disk recovery (non-new replace)
- fs_disk: Support probing of partitions (e.g. /dev/sda1)
- logging improvements
- raid1: Improve disk replacement logic (swap_device)
- raid1: Speed up Bitmap initialization
- raid0: Optimize OPT_IO and PHY_IO to match array layout.
- raid1: Fix reservation calculation to ensure user region is aligned to max i/o
- raid1: Reservation size is now dynamically calculated during init, prevents resize

## 0.15.x
- Fix homeblock_disk linkage
- Enable C++23 extensions
- Replace usage of folly::Expected with std::expected

## 0.14.x
- Update 3rd party libs (sisl, boost, folly)
- CI to build with GCC-13
- raid1: Remove UUID from Bitmap (found redundant)
- raid: Cleanup logging
- raid1: Track the amount of dirty data left in the bitmap
- raid1: I/O sharing between re-sync and user path

## 0.13.x
- raid1: Bitmap optimizations.
- raid1: Buffer cleaned BITMAP regions
- raid0: UINT32 truncation bug
- ublk_disk: Align device size to max_sector
- raid1: Buffered BITMAP

## 0.12.x
- raid1: Fix bug in Bitmap::load_pages where we used `&` instead of `*` for arithmetic
- raid1: Improve reading from degraded disk on startup
- raid1: Bitmap fix due to superblock mis-alignment
- fs_disk: Fix conditional on close
- raid1: Resync/Swap interaction fixes
- raid1: Retrieve underlying devices with `::replicas()`
- raid1: Fixed race with `::swap_device` and `__resync_task`

## 0.11.x
- raid1: Fix Bitmap bugs when representing > 4Gi
- ublkpp_disk: Support for HomeBlkDisk type
- raid1: Another resync_task termination fix
- raid1: Fix resync_task termination
- raid1: Resync task handles no-dirty pages
- raid1: Provide ability to gather replica states

## 0.10.x
- Cleanup Public API headers
- iscsi: Name thread
- libiscsi: Update to 1.20.3
- iscsi_disk : Fix iscsi session teardown
- iscsi_disk : Fix memleak in sync_iov
- ublkpp_tgt: Fix multi-device attach
- Fix shutdown
- Start device mgmt logic

## 0.9.x
- raid0: Fix leak on invalid superblock
- raid1: Avoid I/O attempts on known degraded devices

## 0.8.x
- raid1-bitmap: Fix word wrapping on dirty words
- raid1: Do not attempt discard on degraded devices
- raid1: Bugs discovered during testing
- raid1: Initiate resync task
- raid1: Improve latency on resync thread
- raid1: Active resync thread

## 0.7.x
- raid1: Give the BITMAP its own uuid
- raid1: Fix endian encoding of SuperBlock integers
- Fix re-loading dirty device after shutdown
- raid1: Make Bitmap atomic
- raid1: Initialize bitmaps on new array devices
- raid1: Fix degraded discard
- ublkpp_disk: Added "loop" mode
- raid1: Bitmap Load/Init
- General fixes
- raid1: Writes in degraded mode
- raid1: Extract BITMAP into its own class
- raid1: Reads in degraded mode

## 0.6.x
- raid1: Do not re-write unchanged pages
- raid1: Round-Robin reading
- homeblk_disk: Disable by default
- raid1: Calculate reserved area based on limits
- ublkpp_tgt: Clear async_event before calling process_result
- raid1: Bitmap words should be encoded as NETWORK byte order
- raid1: Records dirty chunks to the BITMAP pages

## 0.5.x
- homeblk_disk : introduced
- raid1: more intelligent retry handling
- ublkpp_tgt : fix narrowing conversion

## 0.4.x
- iscsi_disk : introduced

## 0.3.x
- ublkpp_tgt : improvments

## 0.2.x
- ublkpp_tgt : API changes

## 0.1.x
- fs_disk : introduced
- raid0/1 : introduced
