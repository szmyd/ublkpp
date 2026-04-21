# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.21.6
- raid0: Fix stale alive_cmds in __distribute() corrupting the next I/O on the same thread after a failed multi-stride operation

## 0.21.5
- raid1: Only log in reference to Resync if it was actually running.

## 0.21.4
- raid1: Fix resync task hang when stop() is called during unavail wait loop
- raid1: Fix concurrent launch()/stop() race on resync task thread assignment
- raid1: Fix incorrect device logged as degraded in __become_degraded (DEVB route)

## 0.21.3
- raid1: Fix Raid1ResyncTask::launch() crash on joinable thread reassignment

## 0.21.2
- raid1: Fix remount failure when bringing up a degraded array with a defunct device
- fio_engine: Pre-populate backing files with zeros after allocation to convert unwritten extents to written state, eliminating first-write journal cost and page fault overhead that inflated write latency on fresh files. Raw block devices are detected and skipped.

## 0.21.1
- New functional testing framework. See `docs/functional_testing.md` for details.

## 0.21.0
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
