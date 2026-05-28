# RAID1 Startup Case Analysis

This document covers every startup scenario: which superblock is chosen, how the dirty
bitmap is loaded, when a full resync is forced, and when the superbitmap shortcut applies.

---

## Key Data Structures

### SuperBlock (4 KiB at offset 0 on each physical device)

```
 Byte 0                                                      Byte 4095
 ┌──────────────────────────────────────────────────────────────────┐
 │ header (34 B)                                                    │
 │   magic[16]  version[2]  uuid[16]                               │
 ├──────────────────────────────────────────────────────────────────┤
 │ fields (40 B)                                                    │
 │   clean_unmount:1  read_route:2  device_b:1                     │
 │   _reserved[16]  chunk_size[4]  age[8]                          │
 ├──────────────────────────────────────────────────────────────────┤
 │ superbitmap_reserved (4022 B = 32,176 bits)                      │
 │   bit N = 1 → bitmap page N has dirty chunks                    │
 │   bit N = 0 → page N clean (or not yet persisted)               │
 └──────────────────────────────────────────────────────────────────┘
 ↑ offset 0 (superblock)   ↑ offset 74 (superbitmap starts here)
```

| Field | Set by | Meaning |
|---|---|---|
| `clean_unmount` | `__become_active`→0, destructor→1 | 1 = clean shutdown, 0 = active or crashed |
| `read_route` | `__become_degraded`, `__become_clean`, `pick_superblock` | Which physical slot to route reads to |
| `age` | Every degradation (+1), every swap (+16), new array (+16) | Monotonic version; higher = more authoritative |
| `superbitmap_reserved` | destructor only (`include_superbitmap=true`) | Index into bitmap pages; persisted only on clean shutdown |

### Disk Layout

```
 offset 0           sizeof(SuperBlock)=4KiB     reserved_size (~125 MiB v2)   capacity
 ┌──────────┬────────────────────────────────────────────┬────────────────────────┐
 │SuperBlock│  Bitmap pages  (4 KiB each, 1 per GiB)    │    User data           │
 │  4 KiB   │  page 0 │ page 1 │ page 2 │ ...           │  (UBLK I/O target)     │
 └──────────┴────────────────────────────────────────────┴────────────────────────┘
             ↑ k_page_size                               ↑ _reserved_size
```

Each bitmap page covers 1 GiB of user data (with 32 KiB chunks, 4 KiB page = 4096×8 bits
= 32768 chunks × 32 KiB = 1 GiB). The superbitmap inside the SuperBlock is a one-bit index
over all bitmap pages — if bit N is set, page N must be read from disk; if clear, the page
is known clean and can be skipped.

---

## Startup Sequence

```mermaid
flowchart TD
    A([Raid1Disk constructor]) --> B["① __load_and_select_superblock\nRead SBs from both disks\nSlot as device_a / device_b\nPick winning SB via pick_superblock\nMark stale device as new_device=true"]
    B --> C["② __init_params\nDerive capacity, reserved_size, alignment\nBranch on SB version for layout"]
    C --> D["③ __init_bitmap_and_degraded_route\nCore startup decision — see diagram below"]
    D --> E["④ Raid1ResyncTask constructed\nArmed with _dirty_bitmap and _reserved_size\nStarts when prepare() is called"]
    E --> F["⑤ __become_active\nclean_unmount = 0 on disk\nsuperbitmap = zeros on disk\n_sb→superbitmap_reserved in MEMORY unchanged"]

    style D fill:#ffe0b2,stroke:#e65100,color:#000000
    style F fill:#e3f2fd,stroke:#1565c0,color:#000000
```

> **Critical invariant set by step ⑤**: `write_superblock` always copies to a stack-local
> buffer before writing. The in-memory `_sb->superbitmap_reserved` is **never** modified by
> `__become_active`. Bits loaded from disk in step ③ survive in memory even after the disk
> shows zeros. This is what makes the superbitmap useful at the *next* startup.

---

## Phase 1: Superblock Selection (`pick_superblock`)

```mermaid
flowchart TD
    IN([Both SBs loaded]) --> AGE{"Compare age"}

    AGE -->|"age_A > age_B"| WIN_A["Winner = A\nread_route ← DEVA\nif gap > 1: B.new_device = true"]
    AGE -->|"age_B > age_A"| WIN_B["Winner = B\nread_route ← DEVB\nif gap > 1: A.new_device = true"]
    AGE -->|"equal"| CM{"clean_unmount\ndiffer?"}

    CM -->|Yes| CLEAN["Take the clean one\nRoute unchanged\nEqual age = mirrors are identical;\nno resync needed regardless of route"]
    CM -->|No| TAKE_A["Take A\nRoute unchanged"]

    WIN_A --> DONE([_sb = winner\n_read_route_cache = winner.read_route])
    WIN_B --> DONE
    CLEAN --> DONE
    TAKE_A --> DONE
```

**Age gap > 1** forces `new_device=true` on the lagging disk, bypassing incremental resync
and triggering a full copy at Branch 2 below. A gap of exactly 1 is allowed — it covers the
normal degradation event (+1) and means the lagging disk is only one step behind.

**Equal ages, asymmetric `clean_unmount`** means the clean-shutdown SB write succeeded on one
disk but not the other. Since ages are identical, both disks are bit-for-bit the same; no resync
is needed. The route field is left as-is from disk (not overridden here) — overriding it would
cause a spurious degraded-mode startup.

---

## Phase 2: Device Role Assignment

After `pick_superblock`, `_device_a` is normalized to the physical slot-A disk and `_device_b`
to slot-B using the `device_b` flag in the winning SB. This normalization ensures that
`read_route::DEVA` always means `_device_a` and `read_route::DEVB` always means `_device_b`.

---

## Phase 3: `__init_bitmap_and_degraded_route`

This is the heart of startup. All six branches are mutually exclusive (evaluated as `if / else if`).

```mermaid
flowchart TD
    START(["__init_bitmap_and_degraded_route"]) --> INIT_NEW["init_to each new_device slot\n(writes all-zero bitmap pages to disk)"]
    INIT_NEW --> C1{"is_missing(A)\nor is_missing(B)?"}

    %% Branch 1
    C1 -->|"Yes — Branch 1"| B1_ROUTE["Route to live slot\n(DEVA if B missing, DEVB if A missing)"]
    B1_ROUTE --> B1_WD["was_degraded =\n(route≠EITHER) OR (!clean_unmount)"]
    B1_WD --> B1_LOAD["load_from(live_disk, was_degraded)\n← see load_from diagram"]

    %% Branch 2
    C1 -->|No| C2{"new_device(A) XOR\nnew_device(B)?"}
    C2 -->|"Yes — Branch 2\nOne blank or stale disk"| B2["age += 16\ndirty_region(0, capacity())\nRoute to good slot\n→ Full resync"]

    %% Branch 3
    C2 -->|No| C3{"route ≠ EITHER\nAND clean_unmount = 0?"}
    C3 -->|"Yes — Branch 3\nDegraded crash"| B3["age += 16\ndirty_region(0, capacity())\n→ Full resync"]

    %% Branch 4
    C3 -->|No| C4{"route ≠ EITHER?"}
    C4 -->|"Yes — Branch 4\nDegraded clean shutdown\n(clean_unmount = 1 implied)"| B4["load_from(active_dev,\nwas_previously_degraded=true)\n→ Incremental or full resync\n   (see load_from diagram)"]

    %% Branch 5
    C4 -->|No| C5{"clean_unmount = 0?"}
    C5 -->|"Yes — Branch 5\nHealthy crash"| B5["Log warning only\nBitmap stays empty\n→ No resync\n(healthy mirrors always in sync)"]

    %% Branch 6
    C5 -->|"No — Branch 6\nHealthy clean shutdown"| B6["Nothing to do\nBitmap empty\n→ No resync"]

    style B2 fill:#ffcdd2,stroke:#b71c1c,color:#000000
    style B3 fill:#ffcdd2,stroke:#b71c1c,color:#000000
    style B4 fill:#fff9c4,stroke:#f57f17,color:#000000
    style B5 fill:#e8f5e9,stroke:#2e7d32,color:#000000
    style B6 fill:#e8f5e9,stroke:#2e7d32,color:#000000
    style B1_LOAD fill:#fff9c4,stroke:#f57f17,color:#000000
```

### Branch 1 sub-cases: Missing device

`was_degraded = (route ≠ EITHER) OR (clean_unmount = 0)`

```mermaid
flowchart TD
    B1(["Branch 1: one device missing\nload_from(live_disk, was_degraded)"]) --> WD{"was_degraded?"}

    WD -->|"false\nroute=EITHER AND clean_unmount=1\n(was healthy + clean shutdown)"| TRUST["superbitmap always trusted\nSuperbitmap = zeros\n(healthy: bitmap never dirtied)\n→ Zero pages scanned\n→ No resync when disk returns"]

    WD -->|"true — all other cases"| HAS_BITS{"superbitmap has\nany set bits?"}

    HAS_BITS -->|"Yes\n(new format: destructor flushed it)"| TARGETED["Targeted scan\nOnly superbitmap-indicated pages read\n→ Incremental resync when disk returns"]

    HAS_BITS -->|"No — all zeros\n(crash OR old-format SB)"| FULL["Full scan\nEvery bitmap page read from disk\n→ Full resync when disk returns"]

    style TRUST fill:#e8f5e9,stroke:#2e7d32,color:#000000
    style TARGETED fill:#fff9c4,stroke:#f57f17,color:#000000
    style FULL fill:#ffcdd2,stroke:#b71c1c,color:#000000
```

---

## `load_from` Logic: When Is the Superbitmap Trusted?

```mermaid
flowchart TD
    LF(["load_from(device, was_previously_degraded)"]) --> VALID{"superbitmap_valid =\n!was_previously_degraded\nOR next_set_bit < k_bits"}

    VALID -->|"was_previously_degraded=false\n→ valid=true always"| HEALTHY_LOOP

    VALID -->|"was_previously_degraded=true\nAND superbitmap all-zeros\n→ valid=false"| FULL_LOOP

    VALID -->|"was_previously_degraded=true\nAND superbitmap has bits\n→ valid=true"| HEALTHY_LOOP

    HEALTHY_LOOP["For each page 0..N-1:\n  if valid AND bit clear → SKIP page\n  if valid AND bit set  → read page from disk"] --> PAGE_CHECK

    FULL_LOOP["For each page 0..N-1:\n  read page from disk unconditionally"] --> PAGE_CHECK

    PAGE_CHECK{"Page content?"} -->|"All zeros"| CLEAR["clear_bit(pg)\nleave slot unallocated\n(confirms clean)"]
    PAGE_CHECK -->|"Has dirty data"| DIRTY["set_bit(pg)\nload into page_map\nmark loaded_from_disk=true"]

    style FULL_LOOP fill:#ffcdd2,stroke:#b71c1c,color:#000000
    style HEALTHY_LOOP fill:#e8f5e9,stroke:#2e7d32,color:#000000
```

**Why zeros trigger full scan for degraded but not healthy:**

- **Healthy** (`was_previously_degraded=false`): healthy writes always hit both mirrors.
  The bitmap is never dirtied during healthy operation. All-zeros is always correct — trust it.
- **Degraded** (`was_previously_degraded=true`): all-zeros could mean (a) old-format superblock
  that predates superbitmap persistence, or (b) a crash that prevented the destructor from
  flushing. Both require a full scan. A new-format clean shutdown always has set bits if
  dirty pages existed.

---

## `__become_active`: What Hits Disk

```mermaid
flowchart LR
    BA(["__become_active"]) --> SET0["_sb->clean_unmount = 0\nin memory"]
    SET0 --> WS["write_superblock\ninclude_superbitmap=false"]
    WS --> LOCAL["Stack-local copy:\n  copy header+fields (74 bytes)\n  superbitmap_reserved = 0x00…\n  write 4096 bytes to disk"]
    LOCAL --> DISK[("Disk:\nclean_unmount=0\nsuperbitmap=zeros")]
    LOCAL --> MEM[("Memory _sb:\nsuperbitmap_reserved unchanged\n← bits from load_from survive here")]

    style DISK fill:#ffcdd2,stroke:#b71c1c,color:#000000
    style MEM fill:#e8f5e9,stroke:#2e7d32,color:#000000
```

This zeroing is the safety net: if the process crashes after `__become_active`, the next
startup sees all-zero superbitmap on disk and falls back to the safe path (full scan for
degraded, no scan needed for healthy).

---

## `~Raid1Disk`: What Gets Persisted on Clean Shutdown

```mermaid
flowchart TD
    DT(["~Raid1Disk"]) --> STOP["_resync_task→stop()\nAll I/O coroutines halted"]
    STOP --> IS_DEG{"is_degraded?"}

    IS_DEG -->|Yes| SYNC["_dirty_bitmap→sync_to(active_disk)\nFlush dirty bitmap pages to disk\n[k_page_size, reserved_size)"]
    IS_DEG -->|No| SB_ACTIVE

    SYNC --> SB_ACTIVE["_sb->clean_unmount = 1"]

    SB_ACTIVE --> WS_ACTIVE["write_superblock(active, include_superbitmap=true)\nCopies all 4096 bytes:\n  clean_unmount=1\n  superbitmap bits = live in-memory bits"]

    WS_ACTIVE --> IS_DEG2{"is_degraded?"}
    IS_DEG2 -->|No| WS_BACKUP["write_superblock(backup, include_superbitmap=true)"]
    IS_DEG2 -->|Yes| DONE2(["Done"])
    WS_BACKUP --> DONE2

    style SYNC fill:#e3f2fd,stroke:#1565c0,color:#000000
    style WS_ACTIVE fill:#e3f2fd,stroke:#1565c0,color:#000000
    style WS_BACKUP fill:#e3f2fd,stroke:#1565c0,color:#000000
```

After a clean shutdown the disk holds:
- **Bitmap pages**: all dirty chunks from the session flushed by `sync_to`.
- **Superbitmap**: one bit per dirty page — exact index for the next startup's targeted scan.
- **`clean_unmount` = 1**: signals to next startup that superbitmap is trustworthy.

---

## On-Disk Superbitmap Lifecycle

```mermaid
stateDiagram-v2
    direction LR

    [*] --> AllZeros : Disk formatted / __become_active runs

    AllZeros --> ActiveSession : __become_active\nwrites clean_unmount=0\nsuperbitmap=zeros on disk

    ActiveSession --> AllZeros : CRASH\ndestructor never runs\ndisk stays zeros

    ActiveSession --> CleanShutdown : Destructor runs\nsync_to + write_superblock\ninclude_superbitmap=true

    CleanShutdown --> HasBits : Disk now has:\nclean_unmount=1\nsuperbitmap with set bits\nbitmaps pages flushed

    HasBits --> AllZeros : Next startup\n__become_active\nzeroes disk again\n(in-memory bits survive)
```

The cycle is: zeros → session (in-memory only) → crash (zeros remain) or clean shutdown
(bits written) → zeros again at next `__become_active`. The in-memory bits from `HasBits`
guide `load_from` between steps "HasBits" and "AllZeros".

---

## Returning Original Disk (`swap_device`)

Scenario: array runs degraded (B missing), writes happen (bitmap dirtied in memory and on
disk), the original disk B returns.

```mermaid
flowchart TD
    SD(["swap_device(missing_id, device_b)"]) --> MK["Create MirrorDevice for device_b\nload_superblock → new_device=false\n(original disk has valid SB)"]
    MK --> SW["__swap_device()\nCAS: cur_route → new_route\noutgoing_dev.swap(incoming_mirror)\nage += 16"]
    SW --> WR_STAY["write_superblock(staying_dev)\ncritical path — age committed"]
    WR_STAY --> WR_NEW["write_superblock(incoming_dev)\nbest-effort"]
    WR_NEW --> IS_NEW{"outgoing_dev->new_device?\n← this is now the INCOMING disk"}

    IS_NEW -->|"false — original disk\n(has valid SB)"| INCR["Bitmap unchanged\nResync only dirty regions\n→ Incremental resync"]

    IS_NEW -->|"true — blank new disk\n(no SB magic)"| FULL["dirty_region(0, capacity())\n→ Full resync"]

    INCR --> UNAVAIL["unavail.clear()\nOpens disk for resync I/O"]
    FULL --> UNAVAIL

    style INCR fill:#e8f5e9,stroke:#2e7d32,color:#000000
    style FULL fill:#ffcdd2,stroke:#b71c1c,color:#000000
```

**Why age is NOT bumped in Branch 1 (missing device startup)**: bumping age at startup would
not prevent incremental resync — the returning disk's age would still only lag by 1 (≤ 1
threshold passes). Not bumping keeps the invariant simple and avoids an unnecessary age skip.
The +16 bump in Branch 2 and Branch 3 signals a forced-full-sync event that must be visible
as a large age gap to any future analysis.

---

## Complete Case Reference

| Scenario | route | clean_unmount | new_device | Branch | Bitmap action | Resync at startup |
|---|---|---|---|---|---|---|
| Both new (first start) | — | — | both=true | pre (init_to) | dirty all | Full |
| One blank, one existing | — | — | one=true | 2 | dirty all | Full |
| Age gap > 1 (declared stale) | — | — | stale=true | 2 | dirty all | Full |
| **Healthy clean shutdown** | EITHER | 1 | both=false | 6 | — | **None** |
| **Healthy crash** | EITHER | 0 | both=false | 5 | — | **None** |
| Degraded clean shutdown (new SB) | DEVA/B | 1 | both=false | 4 | load_from targeted | Incremental |
| Degraded clean shutdown (old SB) | DEVA/B | 1 | both=false | 4 | load_from full scan | Full |
| **Degraded crash** | DEVA/B | 0 | both=false | 3 | dirty all | **Full** |
| Missing: was healthy, clean | EITHER | 1 | live=false | 1 | load_from no scan | None (when B returns) |
| Missing: was healthy, crash | EITHER | 0 | live=false | 1 | load_from full scan | Full (when B returns) |
| Missing: was degraded, clean (new SB) | DEVA/B | 1 | live=false | 1 | load_from targeted | Incremental (when B returns) |
| Missing: was degraded, clean (old SB) | DEVA/B | 1 | live=false | 1 | load_from full scan | Full (when B returns) |
| Missing: was degraded, crash | DEVA/B | 0 | live=false | 1 | load_from full scan | Full (when B returns) |
| Original disk returns via swap_device | — | — | false | swap | bitmap unchanged | Incremental |
| Blank new disk via swap_device | — | — | true | swap | dirty all | Full |

> **"Resync at startup"** refers to resync that begins immediately once `prepare()` is called.
> For the "missing device" rows, the resync only begins when the missing disk is replaced
> via `swap_device`.

---

## Summary: When Is the Superbitmap Trusted?

```mermaid
flowchart TD
    Q(["Does load_from trust\nthe in-memory superbitmap?"]) --> HD{"was_previously_degraded?"}

    HD -->|"false — healthy path"| T1["Always trusted\nAll-zeros = clean array\nNo pages read from disk"]

    HD -->|"true — degraded path"| HB{"Superbitmap has\nset bits?"}

    HB -->|"Yes — destructor ran\nand persisted bits"| T2["Trusted\nOnly indicated pages read\nTargeted scan"]

    HB -->|"No — all zeros\n(crash or old format)"| T3["Not trusted\nFull scan of all pages\nZero pages confirm clean;\nnon-zero pages loaded as dirty"]

    style T1 fill:#e8f5e9,stroke:#2e7d32,color:#000000
    style T2 fill:#fff9c4,stroke:#f57f17,color:#000000
    style T3 fill:#ffcdd2,stroke:#b71c1c,color:#000000
```
