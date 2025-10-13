#Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.14.1
- raid1: Track the amount of dirty data left in the bitmap

## 0.14.0
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
