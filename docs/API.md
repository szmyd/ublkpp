# API Reference

Complete API documentation for ublkpp library.

## Table of Contents

- [Core Types](#core-types)
- [UblkDisk Base Class](#ublkdisk-base-class)
- [RAID Types](#raid-types)
- [Driver Types](#driver-types)
- [Target Interface](#target-interface)
- [Error Codes](#error-codes)
- [Utility Functions](#utility-functions)

## Core Types

### io_result

Result type for all I/O operations:

```cpp
using io_result = std::expected<size_t, std::error_condition>;
```

**Returns:**
- `size_t`: Number of bytes transferred on success
- `std::error_condition`: Error code on failure

**Usage:**
```cpp
auto res = device->sync_iov(op, iovecs, nr_vecs, addr);
if (res) {
    size_t bytes = res.value();
} else {
    std::error_condition ec = res.error();
    DLOGE("I/O failed: {}", ec.message());
}
```

### sub_cmd_t

Sub-command routing and flags:

```cpp
using sub_cmd_t = uint16_t;
```

**Bit Layout:**
- Bits 0-7: Route (device-specific, e.g., stripe index)
- Bits 8-15: Flags (NONE, REPLICATE, RETRIED, DEPENDENT, INTERNAL)

**Flags:**
```cpp
enum class sub_cmd_flags : sub_cmd_t {
    NONE = 0,        // Normal user I/O
    REPLICATE = 1,   // Mirrored write (RAID1)
    RETRIED = 2,     // Retry after failure
    DEPENDENT = 4,   // Must succeed but doesn't affect user I/O result
    INTERNAL = 8     // Background operation (resync, bitmap update)
};
```

**Functions:**
```cpp
sub_cmd_t set_flags(sub_cmd_t sub_cmd, sub_cmd_flags flags);
sub_cmd_t unset_flags(sub_cmd_t sub_cmd, sub_cmd_flags flags);
bool test_flags(sub_cmd_t sub_cmd, sub_cmd_flags flags);

bool is_replicate(sub_cmd_t sub_cmd);
bool is_retry(sub_cmd_t sub_cmd);
bool is_dependent(sub_cmd_t sub_cmd);
bool is_internal(sub_cmd_t sub_cmd);

sub_cmd_t shift_route(sub_cmd_t sub_cmd, sub_cmd_t shift);
```

### async_result

Async I/O completion result:

```cpp
struct async_result {
    ublk_io_data const* io;  // I/O descriptor
    sub_cmd_t sub_cmd;        // Sub-command with routing
    int result;               // Result (bytes or -errno)
};
```

Returned by `collect_async()` for async I/O completion.

## UblkDisk Base Class

Base class for all block devices.

### Constructor / Destructor

```cpp
UblkDisk();
virtual ~UblkDisk();
```

### Device Properties

#### capacity()

```cpp
virtual uint64_t capacity() const noexcept = 0;
```

Returns device capacity in bytes.

**Pure virtual** - must be implemented by subclasses.

#### block_size()

```cpp
virtual uint32_t block_size() const noexcept;
```

Returns block size in bytes (default: 4096).

Override for non-4K block sizes.

#### max_tx()

```cpp
virtual uint32_t max_tx() const noexcept;
```

Returns maximum transfer size in bytes.

Default: from `ublk_params` (typically 512 KiB).

#### can_discard()

```cpp
virtual bool can_discard() const noexcept;
```

Returns `true` if device supports TRIM/discard (default: `false`).

#### id()

```cpp
virtual std::string id() const noexcept = 0;
```

Returns device identifier (e.g., path, UUID, descriptive name).

**Pure virtual** - must be implemented by subclasses.

### Public Members

```cpp
bool direct_io{false};
```

Set to `true` if device requires O_DIRECT semantics (aligned I/O).

```cpp
bool uses_ublk_iouring{true};
```

Set to `true` if device uses ublk's io_uring (requires `ublksrv_complete_io()` calls).

### I/O Operations

#### async_iov()

```cpp
virtual io_result async_iov(ublksrv_queue const* q,
                            ublk_io_data const* data,
                            sub_cmd_t sub_cmd,
                            iovec* iovecs,
                            uint32_t nr_vecs,
                            uint64_t addr) = 0;
```

Asynchronous I/O operation with scatter-gather vectors.

**Parameters:**
- `q`: Queue handling this I/O
- `data`: I/O descriptor from ublk
- `sub_cmd`: Sub-command with routing and flags
- `iovecs`: Array of I/O vectors
- `nr_vecs`: Number of vectors
- `addr`: Starting address (bytes)

**Returns:**
- `0` on success (will complete async)
- `>0` for immediate completion (bytes transferred)
- `std::unexpected(error)` on failure

**Pure virtual** - must be implemented by subclasses.

**Notes:**
- Must call `ublksrv_complete_io(q, data, result)` for async completion
- Can return immediately for synchronous completion

#### sync_iov()

```cpp
virtual io_result sync_iov(uint8_t op,
                           iovec* iovecs,
                           uint32_t nr_vecs,
                           off_t addr) noexcept = 0;
```

Synchronous I/O operation.

**Parameters:**
- `op`: Operation (UBLK_IO_OP_READ, UBLK_IO_OP_WRITE)
- `iovecs`: Array of I/O vectors
- `nr_vecs`: Number of vectors
- `addr`: Starting address (bytes)

**Returns:**
- Number of bytes transferred on success
- `std::unexpected(error)` on failure

**Pure virtual** - must be implemented by subclasses.

#### handle_flush()

```cpp
virtual io_result handle_flush(ublksrv_queue const* q,
                               ublk_io_data const* data,
                               sub_cmd_t sub_cmd) = 0;
```

Flush buffered writes to stable storage.

**Pure virtual** - must be implemented by subclasses.

#### handle_discard()

```cpp
virtual io_result handle_discard(ublksrv_queue const* q,
                                 ublk_io_data const* data,
                                 sub_cmd_t sub_cmd,
                                 uint32_t len,
                                 uint64_t addr) = 0;
```

TRIM/discard operation for SSD optimization.

**Pure virtual** - must be implemented by subclasses.

### Advanced Hooks

#### route_size()

```cpp
virtual uint8_t route_size() const noexcept;
```

Returns number of bits used for sub-command routing (default: 0).

Override for RAID devices that route to sub-devices.

**Example:**
```cpp
// RAID0 with up to 64 stripes
uint8_t route_size() const noexcept override {
    return ilog2(64);  // 6 bits
}
```

#### collect_async()

```cpp
virtual void collect_async(ublksrv_queue const* q,
                           std::list<async_result>& compl_list);
```

Collect completed async I/O operations (default: no-op).

Override for devices with async backends.

**Parameters:**
- `q`: Queue to collect from
- `compl_list`: Output list of completed I/Os

#### idle_transition()

```cpp
virtual void idle_transition(ublksrv_queue const* q, bool is_idle);
```

Called when queue transitions between idle and active (default: no-op).

**Parameters:**
- `q`: Queue that transitioned
- `is_idle`: `true` if now idle, `false` if now active

**Use cases:** Flush caches, start/stop background tasks

#### on_io_complete()

```cpp
virtual void on_io_complete(ublk_io_data const* data,
                            sub_cmd_t sub_cmd,
                            int res);
```

Called after every I/O completion (default: no-op).

**Use cases:** Metrics collection, state tracking

#### open_for_uring()

```cpp
virtual std::list<int> open_for_uring(int iouring_device);
```

Returns file descriptors to register with io_uring (default: empty).

**Parameters:**
- `iouring_device`: ublk device ID

**Returns:** List of file descriptors for io_uring registration

#### handle_internal()

```cpp
virtual io_result handle_internal(ublksrv_queue const* q,
                                  ublk_io_data const* data,
                                  sub_cmd_t sub_cmd,
                                  iovec* iovecs,
                                  uint32_t nr_vecs,
                                  uint64_t addr,
                                  int res);
```

Handle internal I/O completions (INTERNAL flag set).

**Parameters:**
- `res`: Result from sub-device

Default: completes to kernel with `res`

### Utility Methods

#### to_string()

```cpp
std::string to_string() const;
```

Returns string representation of device.

#### params()

```cpp
ublk_params* params() noexcept;
ublk_params const* params() const noexcept;
```

Access device parameters (block size, capacity, flags, etc.).

## RAID Types

### Raid0Disk

Striping across multiple devices for performance.

#### Constructor

```cpp
Raid0Disk(boost::uuids::uuid const& uuid,
          uint32_t stripe_size_bytes,
          std::vector<std::shared_ptr<UblkDisk>>&& disks);
```

**Parameters:**
- `uuid`: Volume UUID
- `stripe_size_bytes`: Stripe size (default: 128 KiB)
- `disks`: Vector of sub-devices (2-64 devices)

**Throws:** `std::invalid_argument` if constraints violated

**Constraints:**
- 2 ≤ disks ≤ 64
- All disks must have same capacity
- All disks must have same `direct_io` setting

#### Methods

```cpp
std::shared_ptr<UblkDisk> get_device(uint32_t stripe_offset) const noexcept;
```

Get sub-device at stripe index.

```cpp
uint32_t stripe_size() const noexcept;
```

Get configured stripe size in bytes.

```cpp
std::string id() const noexcept override;
```

Returns `"RAID0"`.

### Raid1Disk

Two-way mirroring with bitmap-based resilience.

#### Constructor

```cpp
Raid1Disk(boost::uuids::uuid const& uuid,
          std::shared_ptr<UblkDisk> dev_a,
          std::shared_ptr<UblkDisk> dev_b,
          std::string const& parent_id = "");
```

**Parameters:**
- `uuid`: Volume UUID
- `dev_a`: Primary device
- `dev_b`: Secondary device
- `parent_id`: Optional parent device ID (for metrics)

**Throws:** `std::invalid_argument` if constraints violated

**Constraints:**
- Both devices must have same capacity
- Both devices must have `direct_io = true`

#### Methods

```cpp
static uint64_t estimate_device_overhead(uint64_t volume_size) noexcept;
```

Estimate memory overhead (superblocks + worst-case bitmap).

**Parameters:**
- `volume_size`: Total volume size in bytes

**Returns:** Estimated memory in bytes

**Example:**
```cpp
uint64_t mem = Raid1Disk::estimate_device_overhead(2 * Ti);
// ~8 MiB for 2 TiB volume (worst-case 100% dirty)
```

```cpp
std::shared_ptr<UblkDisk> swap_device(
    std::string const& old_device_id,
    std::shared_ptr<UblkDisk> new_device);
```

Hot-swap a failed device.

**Parameters:**
- `old_device_id`: ID of device to replace
- `new_device`: New device (must have same capacity)

**Returns:** Removed device

**Throws:** `std::invalid_argument` if device ID not found or capacity mismatch

```cpp
raid1::array_state replica_states() const noexcept;
```

Get replica states.

**Returns:**
```cpp
struct array_state {
    replica_state device_a;  // CLEAN, SYNCING, ERROR
    replica_state device_b;
    uint64_t bytes_to_sync;  // Remaining bytes to resync
};
```

```cpp
std::pair<std::shared_ptr<UblkDisk>, std::shared_ptr<UblkDisk>>
replicas() const noexcept;
```

Get both replica devices.

**Returns:** `{device_a, device_b}`

```cpp
void toggle_resync(bool enable);
```

Enable/disable background resync.

```cpp
uint64_t reserved_size() const noexcept;
```

Get superblock and bitmap reserved space per device.

```cpp
std::string id() const noexcept override;
```

Returns `"RAID1"`.

## Driver Types

### FSDisk

File or block device backend.

#### Constructor

```cpp
explicit FSDisk(std::filesystem::path const& path,
                std::string const& parent_id = "");
```

**Parameters:**
- `path`: Path to file or block device
- `parent_id`: Optional parent device ID (for metrics)

**Throws:** `std::runtime_error` if path doesn't exist or can't be opened

**Behavior:**
- Opens with `O_RDWR | O_DIRECT` for block devices
- Opens with `O_RDWR` (no direct I/O) for regular files
- Sets `direct_io = true` for block devices

#### Methods

```cpp
std::string id() const noexcept override;
```

Returns native path string.

### iSCSIDisk

iSCSI target backend (requires `iscsi=True` build option).

#### Constructor

```cpp
iSCSIDisk(std::string const& target_url,
          std::string const& parent_id = "");
```

**Parameters:**
- `target_url`: iSCSI URL (`iscsi://host/target/lun`)
- `parent_id`: Optional parent device ID

**Example:**
```cpp
auto disk = std::make_shared<iSCSIDisk>(
    "iscsi://192.168.1.100/iqn.2024-01.com.example:storage/0");
```

### HomeBlkDisk

HomeBlocks backend (requires `homeblocks=True` build option).

#### Constructor

```cpp
HomeBlkDisk(std::shared_ptr<homeblocks::HomeBlks> hb,
            std::string const& vol_name,
            std::string const& parent_id = "");
```

**Parameters:**
- `hb`: HomeBlocks instance
- `vol_name`: Volume name
- `parent_id`: Optional parent device ID

### DefunctDisk

Placeholder for failed devices.

```cpp
DefunctDisk();
```

All I/O operations return `std::errc::no_such_device`.

**Use case:** RAID degraded mode after device failure.

## Target Interface

### ublkpp_tgt

Exposes UblkDisk to kernel as `/dev/ublkbN`.

#### run()

```cpp
static run_result_t run(boost::uuids::uuid const& vol_id,
                        std::shared_ptr<UblkDisk> device,
                        int device_id = -1);
```

Create and start a ublk target.

**Parameters:**
- `vol_id`: Volume UUID
- `device`: Root device to expose
- `device_id`: Specific device ID, or -1 for auto-assign

**Returns:**
- `std::unique_ptr<ublkpp_tgt>` on success
- `std::unexpected(error)` on failure

**Example:**
```cpp
auto uuid = boost::uuids::random_generator()();
auto disk = std::make_shared<FSDisk>("/dev/sda");

auto tgt_result = ublkpp_tgt::run(uuid, disk);
if (!tgt_result) {
    std::cerr << "Failed: " << tgt_result.error().message() << "\n";
    return 1;
}

auto tgt = std::move(tgt_result.value());
std::cout << "Device: " << tgt->device_path() << "\n";
```

#### Methods

```cpp
std::filesystem::path device_path() const;
```

Get kernel device path (e.g., `/dev/ublkb0`).

```cpp
std::shared_ptr<UblkDisk> device() const;
```

Get root device.

```cpp
int device_id() const;
```

Get ublk device ID.

```cpp
void destroy();
```

Destroy target and remove kernel device.

**Note:** Called automatically by destructor.

```cpp
static uint64_t estimate_queue_memory() noexcept;
```

Estimate target queue memory overhead.

**Returns:** Estimated memory in bytes based on SISL options:
- `nr_hw_queues` (default: 1)
- `queue_depth` (default: 128)
- `max_io_size` (default: 512 KiB)

**Example:**
```cpp
uint64_t queue_mem = ublkpp_tgt::estimate_queue_memory();
uint64_t raid_mem = Raid1Disk::estimate_device_overhead(2 * Ti);
uint64_t total = queue_mem + raid_mem;
// total ≈ 78 MiB for default settings with 2 TiB RAID1
```

## Error Codes

ublkpp uses `std::error_condition` from `<system_error>`:

### Common Error Codes

```cpp
std::errc::io_error              // Generic I/O error
std::errc::no_such_device        // Device not found
std::errc::no_space_on_device    // ENOSPC
std::errc::invalid_argument      // Invalid parameters
std::errc::permission_denied     // EACCES
std::errc::resource_unavailable_try_again  // EAGAIN
std::errc::operation_not_supported        // EOPNOTSUPP
```

### Error Handling Pattern

```cpp
auto res = device->sync_iov(op, iovs, nr_vecs, addr);
if (!res) {
    auto ec = res.error();

    if (ec == std::errc::no_space_on_device) {
        DLOGE("Disk full");
        return handle_full_disk();
    }

    if (ec == std::errc::io_error) {
        DLOGE("Hardware I/O error");
        return mark_device_degraded();
    }

    // Generic error
    DLOGE("I/O failed: {}", ec.message());
    return ec;
}
```

## Utility Functions

### Size Constants

```cpp
constexpr auto Ki = 1024UL;
constexpr auto Mi = Ki * Ki;
constexpr auto Gi = Mi * Ki;
constexpr auto Ti = Gi * Ki;
```

**Usage:**
```cpp
auto capacity = 2 * Ti;  // 2 TiB
auto stripe_size = 128 * Ki;  // 128 KiB
```

### ilog2()

```cpp
template<typename T>
constexpr T ilog2(T x);
```

Integer log base 2 (bit position of highest set bit).

**Example:**
```cpp
ilog2(64) == 6
ilog2(128) == 7
```

### Block Size Constants

```cpp
constexpr auto SECTOR_SIZE = 512UL;
constexpr auto SECTOR_SHIFT = 9;
constexpr auto DEFAULT_BLOCK_SIZE = 4 * Ki;  // 4096
constexpr auto DEFAULT_BS_SHIFT = 12;
```

## Type Aliases

```cpp
using run_result_t = std::expected<std::unique_ptr<ublkpp_tgt>,
                                   std::error_condition>;
```

Result type for `ublkpp_tgt::run()`.

## Next Steps

- **[Library Guide](LIBRARY.md)**: Understand architecture and concepts
- **[Integration Guide](INTEGRATION.md)**: Add ublkpp to your project
- **[Extension Guide](EXTENDING.md)**: Create custom devices
- **[Example Code](../example/)**: See `ublkpp_disk` for complete usage
