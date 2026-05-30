# Bug Hunt — ublkpp

Systematic correctness audit using parallel discovery agents followed by independent
verification. Each finding must survive a separate clean-context challenge before it
is reported as real.

**Scope**: implementation files only (`src/`). Public headers (`include/ublkpp/`) and
interface-level contracts are out of scope for this hunt.

---

## Mindset — The Million-Dollar Rule

You are a professional C++ correctness engineer. Every real bug you find is worth
**$1,000,000**. Every false positive you report costs you **$1,000,000**.

Operate accordingly:
- A finding is only reported if you are **100% certain** it is a real bug
- If you are not certain, do NOT report it as a bug — formulate a precise, concrete
  scenario and ask the user: *"Is this execution sequence possible in your deployment?"*
- "This looks suspicious" is not a finding. "This exact sequence produces data loss:
  1. Thread A does X, 2. Thread B does Y, 3. result Z is wrong" is a finding.
- Removing a false positive is always correct. Keeping an uncertain finding is never correct.

---

## Phase 1 — Parallel Discovery (5 agents)

Spawn **5 agents in parallel** using the Agent tool. Send all 5 in a single message so
they run concurrently. Each agent is scoped to one subsystem, reads the relevant source
files in full, and writes its candidates to a dedicated output file.

| Agent | Scope | Output file |
|---|---|---|
| 1 | `src/raid/raid1/bitmap.cpp`, `bitmap.hpp`, `super_bitmap.*` | `/tmp/bh-agent1.md` |
| 2 | `src/raid/raid1/raid1.cpp` — **runtime** state transitions only (`__become_degraded`, `__become_clean`, `__swap_device`, `async_iov`, `prepare`); `src/raid/raid1/raid1_resync_task.*` | `/tmp/bh-agent2.md` |
| 3 | `src/raid/raid1/raid1_superblock.*`; startup path in `raid1.cpp` (`__init_*`, `__load_*`, **`__become_active`**) — pay particular attention to `__become_active` failing mid-startup and delegating to `__become_degraded` | `/tmp/bh-agent3.md` |
| 4 | `src/target/ublkpp_tgt.cpp`, `src/lib/disk_task.*` | `/tmp/bh-agent4.md` |
| 5 | `src/raid/raid0/raid0.cpp`, `src/driver/`, `src/metrics/` | `/tmp/bh-agent5.md` |

**Discovery agent prompt must include:**
- Assigned files (read them in full — do not skim)
- The Analysis Framework section below
- The million-dollar rule
- This output format — write raw candidates to the output file, no filtering yet:

```
## Candidate Findings — Agent [N] — [Subsystem]

### Candidate [N.X]: [Title]
**Location**: file.cpp:line
**Type**: race | power-loss | TOCTOU | wrong-device | sequencing | version-bounds | other
**Claim**: [one sentence — what goes wrong and what the impact is]
**Trigger sequence**:
  1. [exact step]
  2. [exact step]
  3. Result: [concrete wrong outcome]
**Evidence**: [CAS trace / code path / specific line numbers]
**Confidence**: High | Medium | Low
**Uncertainty**: [what you're not sure about, if anything]
```

---

## Phase 2 — Independent Verification

After all 5 discovery agents complete:
1. Read **all 5 output files** (`/tmp/bh-agent1.md` through `/tmp/bh-agent5.md`) and collect
   every candidate into a working list before spawning any verification agent.
2. Assign each candidate a flat sequential index (01, 02, 03 …) regardless of which discovery
   agent produced it.
3. Spawn verification agents in batches of **at most 8 at a time**. The runtime queues excess
   agents automatically, but keeping batches small avoids overwhelming the context budget.
   Batch Low-confidence candidates that target the same source file into a single agent.

Each verification agent:

- Has **no knowledge of what discovery agents found** — it receives only the candidate text,
  not the discovery agents' reasoning or other candidates
- Reads the candidate verbatim from the output file
- Reads the relevant source files from scratch, following call chains as needed
- Applies the million-dollar rule as the only criterion
- Delivers a verdict: **CONFIRM**, **REJECT**, or **ESCALATE**

**Verification protocol:**
1. Read every file mentioned in the candidate plus any callers/callees relevant to the claim
2. Trace the exact trigger sequence step by step through the actual code
3. Identify every guard — CAS gate, lock, ordering constraint — that could block the sequence
4. **REJECT** if any guard definitively prevents the sequence — name the guard, cite the line
5. **CONFIRM** if the sequence is definitively reachable and produces the claimed harm
6. **ESCALATE** if uncertain after full investigation — do not guess; ask the user

**Verification output per candidate** — write to `/tmp/bh-verify-NN.md` using the flat
sequential index assigned in step 2 above (e.g. `/tmp/bh-verify-01.md`, `/tmp/bh-verify-02.md`).
Batched candidates share one file; list all their IDs in the filename comment:
```
### [CONFIRMED | REJECTED | ESCALATE] Candidate [N.X]: [Title]

**Verdict**: ...
**Evidence**: [exact code path, line numbers, CAS values]
**REJECTED — blocked by**: [specific gate that prevents it]
**ESCALATE — scenario for user**: "Please consider: [precise, concrete sequence].
  Is this execution possible in your deployment?"
```

---

## Phase 3 — Final Report

After all verification agents complete, read all `/tmp/bh-verify-*.md` files and
synthesize into one consolidated report:

```
## Bug Hunt Report

**Scope**: [subsystems]
**Candidates**: N total — Confirmed: X | Rejected: Y | Escalated: Z

---

### Confirmed Bugs

#### [B1] Title — P0 | P1 | P2
<!-- Severity: P0 = data loss / corruption | P1 = crash / unavailability | P2 = extra resync / silent wrong behaviour -->

**Location**: file.cpp:line
**Type**: ...
**Trigger**:
  1. ...
  2. ...
  3. Result: ...
**Impact**: data loss | corruption | crash | silent failure | extra resync
**Fix direction**: ...

---

### Rejected (False Positives)

| ID | Title | Rejected because |
|---|---|---|
| N.X | ... | CAS at raid1.cpp:620 prevents concurrent entry |

---

### Escalations — Human Review Needed

#### [E1] Title

Please consider this scenario: [precise sequence]. Is this possible in your deployment?
```

---

## Analysis Framework

Used by both discovery and verification agents.

### Power-Loss Safety

Every line can be the last. The process runs in a pod and can die at any point.
Never assume line A+1 executed because line A did.

Walk every write to persistent state:
- What is on-disk if power is lost immediately **before** this write?
- What is on-disk if power is lost immediately **after** this write?
- Does the startup path handle both cases without data loss?

Core invariant: memory may be dirtier than disk (safe — causes extra resync).
Memory must never be cleaner than disk — that is data loss.

Danger patterns:
- "Destructor always runs at shutdown" — pod kill / OOM, it does not
- "Rollback will execute after the failed write" — crash between failure and rollback
- "If flag X is set, then Y also happened" — X set, crash, Y never executes

### CAS Gate Analysis

Before flagging a race, trace the full CAS chain:
1. Find every `compare_exchange` that guards entry to the code path
2. Determine the `old_val` required to succeed
3. Check whether any concurrent path has already advanced the atomic past that value

If yes → the second caller's CAS fails and it returns before touching shared state →
**the race is structurally impossible → REJECT**.

Common false positive: two functions both eventually call the same helper. Always verify
whether the CAS gate makes simultaneous execution impossible before writing it up.

### Atomic Write Boundaries

4 KiB aligned block writes are atomic at the device level — either the full write lands
or none of it does. Fields sharing a 4 KiB page are written atomically with respect to
each other.

`clean_unmount`, `read_route`, and `superbitmap_reserved` share the superblock's 4 KiB page.
If `clean_unmount=1` is on disk, the entire write that set it also landed — `superbitmap_reserved`
reflects what was written in that same call and is trustworthy.

### Race Direction

Before reporting a race, determine which direction it can go:
- **Set-only** (`fetch_or`, `set_bit`): memory gets dirtier than disk → **safe direction**
- **Clear** (`fetch_and`, `clear_bit`): memory gets cleaner than disk → **data loss direction**

A race that can only go in the safe direction is a TSan warning, not a correctness bug.
Document it accurately; do not overstate the risk.

### Write Acknowledgment Boundary

A write scenario is a correctness bug **only if** the caller could receive a successful
acknowledgment while the data is in an inconsistent state on disk.

**If the caller received EIO (or the process crashed before ACK), the upper layer owns
recovery — this is NOT a bug in the RAID/storage layer:**

- Journaling filesystems (XFS, ext4) come up after power loss, find unfinished journal
  entries, and replay them. They read state before trusting anything — they do not assume
  data is consistent just because RAID didn't return EIO.
- `USER_RECOVERY` mode resubmits outstanding I/Os after restart.
- "I never got an ACK back from this write; I should read before I assume anything" is the
  correct upper-layer semantic.

**The dangerous pattern** (real correctness bug) requires **both**:
1. The write was (or could be) ACK'd to the caller as successful
2. The on-disk data is inconsistent — caller reads stale data from a replica

**Common false alarm**: both-writes-in-flight + A-fails + crash-before-SB-write.
Even if B committed new data and the SB was never updated to show degraded mode,
the caller never got a success ACK for that write (they got EIO or no response).
On restart, a journaling filesystem treats that region as "unknown — must verify before use."
This is handled correctly at the filesystem layer and is not a RAID-level data-loss bug.

**What to check**: trace whether the ACK path (`co_return` with success,
`UBLK_IO_COMMIT_AND_FETCH_REQ`) can fire AFTER a write landed on only one replica with no
record of the inconsistency. If the ack fires only on the error path (EIO returned), it is safe.

### RAID1 Known False Positives — Verify Before Reporting

These patterns look dangerous on first inspection but are structurally impossible. Confirm each
applies before flagging a finding; if anything in the code path has changed, re-trace from scratch.

#### 1. Destructor races with IO coroutines (phantom)

`Raid1Disk::~Raid1Disk` is called **after** all IO queues are stopped.
`_resync_task->stop()` is the first call in the destructor — it joins the resync coroutine
and waits for all in-flight IO to drain. By the time any subsequent destructor line executes
there are **zero active IO coroutines**. Nothing in the destructor can race with anything:

- `_dirty_bitmap->sync_to(...)` — no concurrent `dirty_region` / `clean_region` / `set_bit`
- `write_superblock(..., include_superbitmap=true)` — no concurrent `SuperBitmap::set_bit`
- `_sb->fields.clean_unmount = 0x1` — no concurrent readers or writers

**REJECT** any candidate that claims a race between the destructor and IO coroutines.

#### 2. Concurrent `write_superblock` callers (phantom)

Two `write_superblock` calls cannot overlap — the state machine prevents it structurally:

- `__become_degraded` opens with `CAS(EITHER → DEVA/DEVB)`. It only proceeds past the CAS —
  and only reaches `write_superblock` — when it wins from `EITHER`.
- `__become_clean` only runs when `route != EITHER` (array already degraded).
- After `__become_degraded`'s CAS succeeds the route is DEVA/DEVB; any concurrent caller
  that tries `CAS(EITHER → ...)` sees a mismatch and returns before reaching `write_superblock`.
- The same CAS argument excludes `__swap_device`.

**What TSan actually detects is a different, safe-direction race:** the old code passed `_sb`
directly as `iov_base`, causing a non-atomic read of `superbitmap_reserved` (bytes 74–4095)
while IO coroutines call `SuperBitmap::set_bit` → `atomic_ref<uint8_t>::fetch_or` on those same
bytes. Under the core invariant this race can only go in the safe direction — `fetch_or` only
sets bits, so memory gets dirtier than disk, never cleaner. It is a TSan warning, not a
correctness bug. The fix (local-copy buffer stopping at byte 74) silences TSan and is a correct
cleanup, but the concurrent-callers framing is wrong.

**REJECT** any candidate claiming two concurrent `write_superblock` callers.

---

### Pattern Checklist

Check each explicitly during both discovery and verification:

**Concurrency**
- [ ] Non-atomic compound RMW: `check + act` where another thread can act between the two
  - `isal_zero_detect(page)` then `clear_bit` — concurrent `fetch_or` fires in the gap
  - `load()` then `fetch_sub()` — two callers both read the same value and both subtract
  - `dirty_pages() == 0` then `complete()` — new dirty page appears between the two calls
- [ ] CAS loser acts on context captured before the CAS (stale premise)
- [ ] Signal before check: `sem_post` / completion fires before the `if (success)` guard

**Error handling**
- [ ] Error from device A sets `unavail` / triggers `__become_degraded` on device B
- [ ] SB write failure ignored but CAS still fires (in-memory and on-disk states diverge)

**State machine**
- [ ] Redirect stores last-used state in a way that creates a stable wrong fixed point
- [ ] Rollback assumed to run after an error (crash between error and rollback)

**Persistence**
- [ ] Destructor-assumed side effect (sync, flush, write) that may not run on pod kill
- [ ] One-sided version bounds: `version < N` with no `version > MAX` upper-bound rejection
- [ ] Two writes that must be consistent but have no atomic boundary between them
