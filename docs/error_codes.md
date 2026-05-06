# RAID async_iov Error Code Reference

Error codes returned directly by `Raid0Disk::async_iov` and `Raid1Disk::async_iov`.
"Retry safe" means the caller can reissue the identical I/O without risk of data duplication or corruption.

## RAID0

| Op | Condition | Code | Retry safe | Notes |
|---|---|---|---|---|
| Any | `stripe_tasks.reserve()` throws `bad_alloc` | `-EAGAIN` | Yes | No SQEs submitted; no I/O started |
| Any | `__distribute` returns error (currently unreachable) | `-EIO` | No | Invariant: lambda always succeeds today; guard for future |
| READ | `io_uring_submit` fails after fan-out | `-EAGAIN` | Yes | SQEs in ring but not yet flushed; no data read |
| WRITE / DISCARD | `io_uring_submit` fails after fan-out | `-EIO` | No | Stripes may partially commit once ring is eventually flushed |

In all submit-failure cases the started stripe tasks are drained before returning to avoid dangling `cqe_state::_waiter` handles.

## RAID1

| Op | Condition | Code | Retry safe | Notes |
|---|---|---|---|---|
| Any unknown | Op is not READ / WRITE / DISCARD / WRITE_ZEROES / FLUSH | `-EINVAL` | Yes | Programming error; FLUSH returns 0 |
| READ | Primary fails; no failover device available | `-EAGAIN` | Yes | Both mirrors unavailable or dirty-region suppressed failover |
| READ | Primary fails; failover attempted | propagated | - | Failover result returned as-is |
| WRITE | Active disk fails; `__become_degraded` fails; backup succeeded | backup result | Yes | Backup holds valid data; array is in-memory degraded |
| WRITE | Active disk fails; `__become_degraded` fails; backup failed or skipped | `-EAGAIN` | Yes | No mirror holds a complete write; safe to retry |
| WRITE | Active disk fails; `__become_degraded` succeeds; backup succeeded | backup result | Yes | Array transitions to degraded; backup is authoritative |
| WRITE | Active disk fails; `__become_degraded` succeeds; backup failed | `-EAGAIN` | Yes | Active write failed, backup write failed; no data committed |
| WRITE | Active succeeds; backup mode SKIP (degraded + unavail/dirty) | active result | - | Degraded write; bitmap dirtied; resync will reconcile |
| WRITE | Active succeeds; backup fails; `__become_degraded` fails | `-EIO` | No | CAS lost or SB write failed; state indeterminate |
| WRITE | Active succeeds; backup fails; `__become_degraded` succeeds | active result | - | Array transitions to degraded; active write is authoritative |
| WRITE | Active and backup both succeed (OPTIMISTIC mode) | active result | - | Chunk cleaned in bitmap; backup marked available |

### Notes on RAID1 `-EAGAIN` vs `-EIO`

- `-EAGAIN` is used when **no mirror holds a committed write** - the caller can retry without risk of duplication.
- `-EIO` is used when the **superblock CAS/write fails** during `__become_degraded` after a backup failure - the on-disk array state is indeterminate and a retry could produce inconsistent data.
