# Bug Hunt — ublkpp

> **KFP last verified: 2026-05-31.** Re-verify Known False Positive entries before running
> after any refactor touching `Raid1Disk`, `__become_*`, `write_superblock`, or `SuperBitmap`.

Systematic correctness audit using parallel discovery agents followed by independent
verification. Each finding must survive a separate clean-context challenge before it
is reported as real.

**Scope**: implementation files only (`src/`), including internal headers under `src/`.
Out of scope: public API headers (`include/ublkpp/`) and interface-level contracts.

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
- **"No bugs found" is a valid and valuable outcome.** If the code is correct, say so. Do
  not manufacture findings to justify the run — a clean report is the most useful output
  when the codebase is actually sound.

---

## Session Setup

Run the following via the Bash tool to get a unique session prefix (ublk is Linux-only,
so `shuf` is available; use the Python fallback in containers without util-linux):

```bash
shuf -i 1000000-9999999 -n1
# fallback: python3 -c "import random; print(random.randint(1000000,9999999))"
```

Use the output (e.g., `3847201`) as a prefix for **every** output file and shell command
in this run. This prevents silent overwrites on re-runs or concurrent hunts — a timestamp
suffix is not used because two runs starting in the same second would collide.

After generating the prefix, announce it prominently in your response so it stays visible
in the conversation context throughout the session:

```
SESSION PREFIX: 3847201 — substitute this value for every {PREFIX} in this workflow.
```

Then verify no prior run left conflicting files:

```bash
ls /tmp/bh-3847201-* 2>/dev/null && echo "conflict — regenerate prefix" || echo "prefix clear"
```

**{PREFIX} substitution is LLM-enforced**. Run this mechanical backstop after announcing
the prefix — the escaped-brace glob only matches if the literal string `{PREFIX}` leaked:

```bash
ls /tmp/bh-\{PREFIX\}-* 2>/dev/null && echo "LITERAL BRACE DETECTED — substitution failed" || true
```

If the literal string is passed to an agent, the sentinel check passes vacuously (no files
found) and Phase 2 will report all agents as unaudited. The announced prefix and this
check together are the guards — reference the prefix explicitly in every agent prompt.

Also check for KFP staleness before Phase 1. The KFP entries were last verified at
commit `f984348`. Run:
```bash
# KFP §1 (destructor) and §2a (CAS gate) — anchored to raid1.cpp:
git log f984348..HEAD --oneline src/raid/raid1/raid1.cpp
# KFP §2b (superbitmap safe-direction race) — anchored to super_bitmap.*:
git log f984348..HEAD --oneline src/raid/raid1/super_bitmap.cpp src/raid/raid1/super_bitmap.hpp
```
Non-empty output for the first command → re-verify §1 and §2a.
Non-empty output for the second → re-verify §2b.
If you re-verify and the entries still hold, update the anchor commit hash above.

Example: prefix `3847201` → `/tmp/bh-3847201-agent1.md`, `/tmp/bh-3847201-verify-01.md`,
`/tmp/bh-3847201-report.md`. The PREFIX is preserved in the report and escalations filenames
— these are the only files kept after cleanup, so the PREFIX remains recoverable.

---

## Phase 1 — Parallel Discovery (5 agents)

**Pre-spawn verification**: before spawning agents, list the 5 output filenames you will
tell each agent to write to and confirm none contains the literal string `{PREFIX}`. If any
does, the substitution failed — stop and re-announce the prefix.

Spawn **5 agents in parallel** using the Agent tool. Send all 5 in a single message so
they run concurrently. Each agent is scoped to one subsystem, reads the relevant source
files in full, and writes its candidates to a dedicated output file.

| Agent | Scope | Output file |
|---|---|---|
| 1 | `src/raid/raid1/bitmap.cpp`, `src/raid/raid1/bitmap.hpp`, `src/raid/raid1/super_bitmap.cpp`, `src/raid/raid1/super_bitmap.hpp` | `/tmp/bh-{PREFIX}-agent1.md` |
| 2 | `src/raid/raid1/raid1.cpp` — **runtime** state transitions only (`__become_degraded`, `__become_clean`, `__swap_device`, `async_iov`, `prepare`); `src/raid/raid1/raid1_resync_task.*`. **Owns `__become_degraded` — Agent 3 treats it as a black box.** | `/tmp/bh-{PREFIX}-agent2.md` |
| 3 | `src/raid/raid1/raid1_superblock.*`; startup path in `raid1.cpp` (`__init_*`, `__load_*`, **`__become_active`**) — pay particular attention to `__become_active` failing mid-startup and delegating to `__become_degraded`. **Treat `__become_degraded` as a black box; log cross-boundary concerns in the XB section (see output format below).** | `/tmp/bh-{PREFIX}-agent3.md` |
| 4 | `src/target/ublkpp_tgt.cpp`, `src/target/ublkpp_tgt_impl.hpp`, `src/lib/ublk_disk.cpp` | `/tmp/bh-{PREFIX}-agent4.md` |
| 5 | `src/raid/raid0/raid0.cpp`, `src/driver/`, `src/metrics/` | `/tmp/bh-{PREFIX}-agent5.md` |

**Discovery agent prompt must include:**
- Assigned files (read them in full — do not skim)
- The Analysis Framework section below — **paste it verbatim, do not summarize**
  (intentional: ~300 tokens × (5 discovery + N verify) agents; at 10 candidates ≈ 4.5k
   overhead; accepted cost
  for consistent reasoning)
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

**Last line of every agent output file must be the sentinel:**

```
<!-- END AGENT [N] -->
```

The orchestrator uses this to detect truncation — a file that exists but lacks the sentinel
was interrupted mid-run and should be treated as missing (report explicitly, do not skip).

**Agent 3 only** — append a cross-boundary section for anything that doesn't clear the
Candidate confidence bar but touches `__become_degraded`. Note: the XB channel is the
**only** path for startup-path findings that straddle the Agent 2/3 boundary — Agent 3
cannot confirm or reject such findings directly, only flag them for Agent 2 verification.
The channel is one-directional (Agent 3 → Agent 2 only). If Agent 2 finds something in
`__become_degraded` that needs startup-path context, it should write a High-confidence
candidate and note the dependency in its Uncertainty field.

```
## Cross-Boundary Concerns (for Agent 2 verification)

### XB-[N]: [Title]
**Concern**: [what needs investigating in __become_degraded]
**Context**: [what Agent 3 observed in __become_active that triggered this]
```

---

## Phase 2 — Independent Verification

After all 5 discovery agents complete:
1. Run a pre-read sentinel check before processing any file:
   ```bash
   grep -rL "END AGENT" /tmp/bh-{PREFIX}-agent*.md 2>/dev/null
   ```
   Any file listed by this command is missing its sentinel (truncated or failed). For each:
   ask the user whether to rerun that agent (with the same output file target) or continue
   without it. If an agent tool call failed entirely (no file at all), treat it the same way.
   Do not proceed until all 5 files are confirmed or the user explicitly accepts a gap.
2. Read the confirmed files and collect every candidate into a working list. Also collect any
   `## Cross-Boundary Concerns` sections from `agent3.md` — **XB items are appended to the
   candidate list before dedup, not after**.
   After collecting, **dedup before assigning flat indices**. Two candidates are the same
   issue if they point to the same code location AND the trigger sequence produces the same
   harmful outcome — merge them. Same location but different harmful outcome = keep both.
3. **If the working list is empty**: emit `Bug Hunt Report: 0 candidates found — no bugs
   detected in audited subsystems.` Clean up discovery files (`rm /tmp/bh-{PREFIX}-agent*.md`)
   and do not proceed to Phase 3.
4. **If there are more than 20 candidates total**: escalate all Low-confidence candidates
   directly to the report as ESCALATE (without spawning verifiers) — treat discovery-time
   Low-confidence as insufficient evidence for a full verification pass. The threshold of 20
   is chosen to keep Phase 2 within roughly 4 rounds of 8 agents; adjust if the framework
   is re-used on a larger codebase.
5. Sort remaining candidates by agent number, then candidate number, then assign a flat
   sequential index (01, 02, 03 …).
6. Prefer one candidate per agent. Run **≤ 8 agents per turn** to stay within the context
   window budget. If there are more than 8 candidates total, run them in rounds of 8 — read
   all outputs from round N, applying the same sentinel-check as for discovery files
   (`<!-- END VERIFY NN -->`, where NN is the first candidate's flat index in that file),
   before round N+1. If budget or candidate volume makes individual agents impractical, you
   may put multiple candidates in one agent, but never mix High or Medium candidates.

Each verification agent receives: the candidate text, the full Analysis Framework section
(**paste verbatim**), and the million-dollar rule. It has **no knowledge of what discovery
agents found** — no other candidates, no discovery reasoning.

The verification agent:
- Reads the candidate verbatim
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

**Verification output per candidate** — write to `/tmp/bh-{PREFIX}-verify-NN.md` where NN
is the **flat sequential index** assigned in step 3 above (e.g. `/tmp/bh-{PREFIX}-verify-01.md`).
For batched candidates, name the file after the **first** candidate's index and list all IDs
in a header comment. Use one verdict block per candidate, referenced by flat index:

```
<!-- Candidates: 04, 05, 06 -->

### [CONFIRMED] Candidate 04: [Title]
**Verdict**: CONFIRMED
**Evidence**: [exact code path, line numbers, CAS values]

### [REJECTED] Candidate 05: [Title]
**Verdict**: REJECTED
**Blocked by**: [specific gate that prevents it, cite the line]

### [ESCALATE] Candidate 06: [Title]
**Verdict**: ESCALATE
**Scenario for user**: "Please consider: [precise, concrete sequence].
  Is this execution possible in your deployment?"

<!-- END VERIFY NN -->
```

The sentinel `<!-- END VERIFY NN -->` is required on every verify file. NN must be the
**first** candidate's flat index (matching the filename, e.g., verify-04.md → sentinel
`<!-- END VERIFY 04 -->`). Using the last candidate's index would allow partial files to
pass the truncation check. Apply the sentinel check before proceeding to the next round.

---

## Phase 3 — Final Report

After all verification agents complete, read all `/tmp/bh-{PREFIX}-verify-*.md` files and
synthesize into one consolidated report:
1. **Write the report to `/tmp/bh-{PREFIX}-report.md`** and emit it as a response.
2. If any escalations exist, **also write them to `/tmp/bh-{PREFIX}-escalations.md`**
   (so the user can re-read them without parsing the full report). Omit this file if there
   are no escalations.
3. **Conditionally** clean up intermediate files — only if the report file exists, is ≥ 100
   bytes, and every verify file was read without exception. Substitute the actual prefix
   before running (same rule as Phase 1 prompts):
   `rm /tmp/bh-{PREFIX}-agent*.md /tmp/bh-{PREFIX}-verify-*.md`
   Note: the glob `verify-*.md` matches escalation re-verification files (`verify-ENN.md`)
   as well — this is intentional. If the `rm` fails (permissions, already deleted), note it
   but do not treat it as a Phase 3 failure — the report is already written. The report and
   escalations files are always kept. Do not run if Phase 3 failed mid-synthesis.

```
## Bug Hunt Report

**Scope**: [subsystems]
**Candidates**: N total — Confirmed: X | Rejected: Y | Escalated: Z
**Coverage**: All 5 agents audited | *or* Agent N unaudited (truncated/missing — [reason])

---

### Confirmed Bugs

#### [B1] Title — P0 | P1 | P2
<!-- Severity:
     P0 = data loss / corruption
     P1 = crash / unavailability
     P2 = extra resync / degraded performance / unnecessary I/O — no data loss or crash path -->

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
| 03 | ... | CAS in `__become_degraded` prevents concurrent entry |

---

### Escalations — Human Review Needed

List escalations here and in `/tmp/bh-{PREFIX}-escalations.md` (omit the file if none).
For each one, present the precise scenario question to the user. After the user responds,
spawn one verification agent per scenario confirmed as possible, passing the candidate text
plus the user's deployment context. Write each agent's output to
`/tmp/bh-{PREFIX}-verify-ENN.md` (E prefix for escalation re-verification, NN is the
escalation number). If confirmed, write findings to a **separate file**
`/tmp/bh-{PREFIX}-report-escalations.md` (do not append to the existing report — appending
requires read-modify-write which risks partial corruption if synthesis fails mid-write).
Tell the user to read both the main report and the escalations supplement together.
Do not re-run Phases 1–3.

#### [E1] Title

Please consider this scenario: [precise sequence]. Is this execution possible in your deployment?
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

**Note for Agent 5**: the KFP entries below are RAID1-specific and do not apply to
RAID0, driver, or metrics code. Skip this section when scoped to Agent 5.

**Staleness gate**: re-audit this entire section before running after any refactor touching
`Raid1Disk`, `__become_*`, `write_superblock`, or `SuperBitmap`. There is no automated
check — if you modify these functions, manually verify the relevant KFP entries still hold
before the next hunt run.

These patterns look dangerous on first inspection but are structurally impossible. Each entry
is anchored to a specific invariant — if that invariant changes in the code, re-trace from
scratch before applying a pre-emptive REJECT.

#### 1. Destructor races with IO coroutines (phantom)

<!-- Valid while: _resync_task->stop() is the first call in ~Raid1Disk and blocks until all coroutines and in-flight I/O drain before returning. If the destructor order or stop() semantics change, re-verify. -->

`Raid1Disk::~Raid1Disk` is called **after** all IO queues are stopped.
`_resync_task->stop()` is the first call in the destructor — it joins the resync coroutine
and waits for all in-flight IO to drain. By the time any subsequent destructor line executes
there are **zero active IO coroutines**. Nothing in the destructor can race with anything:

- `_dirty_bitmap->sync_to(...)` — no concurrent `dirty_region` / `clean_region` / `set_bit`
- `write_superblock(..., include_superbitmap=true)` — no concurrent `SuperBitmap::set_bit`
- `_sb->fields.clean_unmount = 0x1` — no concurrent readers or writers

**REJECT** any candidate that claims a race between the destructor and IO coroutines.

#### 2a. Concurrent `write_superblock` callers (phantom)

<!-- Valid while: __become_degraded opens with CAS(EITHER→DEVA/DEVB) and reaches write_superblock only on CAS success; __become_clean only runs when route≠EITHER; __swap_device is guarded by the same CAS. If any of these CAS gates are removed or bypassed, re-verify. -->

Two `write_superblock` calls cannot overlap — the state machine prevents it structurally:

- `__become_degraded` opens with `CAS(EITHER → DEVA/DEVB)`. It only proceeds past the CAS —
  and only reaches `write_superblock` — when it wins from `EITHER`.
- `__become_clean` only runs when `route != EITHER` (array already degraded).
- After `__become_degraded`'s CAS succeeds the route is DEVA/DEVB; any concurrent caller
  that tries `CAS(EITHER → ...)` sees a mismatch and returns before reaching `write_superblock`.
- The same CAS argument excludes `__swap_device`.

**REJECT** any candidate claiming two concurrent `write_superblock` callers.

#### 2b. TSan report on `superbitmap_reserved` during superblock write (safe direction)

<!-- Valid while: SuperBitmap::set_bit uses atomic_ref<uint8_t>::fetch_or (set-only); write_superblock copies the superblock buffer excluding superbitmap_reserved. If set_bit gains a clear path or write_superblock includes superbitmap_reserved in the copy, re-verify. -->

TSan may flag a race between `write_superblock` reading `_sb` as raw bytes (via `iov_base`)
and IO coroutines calling `SuperBitmap::set_bit` → `atomic_ref<uint8_t>::fetch_or` on the
`superbitmap_reserved` field of the same superblock page. This is a **safe-direction race**:
`fetch_or` only sets bits, so memory gets dirtier than disk, never cleaner. Under the core
invariant this cannot cause data loss. The fix (local-copy buffer stopping before
`superbitmap_reserved`) silences TSan and is a correct cleanup, but the race is not a
correctness bug.

**REJECT** any candidate framing this TSan report as a data-loss or corruption risk.

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
- [ ] Stale capture across `co_await`: value loaded before suspension used after resumption
  without re-read — another coroutine may have mutated shared state while this one was suspended
- [ ] Dangling reference into coroutine frame: `const T&` or pointer captured into a coroutine
  body may outlive the referent if the owning object is destroyed while the coroutine is suspended
- [ ] Relaxed atomic as a branch guard: `load(relaxed)` used as a condition without a subsequent
  `acquire` fence — the load can be reordered past dependent loads it was intended to gate
- [ ] Convention-only protection: field assumed single-threaded but guarded only by a
  "callers must ensure X" invariant with no lock or atomic — verify the invariant holds on
  every call path (these don't appear in TSan if the convention is always honored but are fragile)

**Error handling**
- [ ] Error from device A sets `unavail` / triggers `__become_degraded` on device B
- [ ] SB write failure ignored but CAS still fires (in-memory and on-disk states diverge)

**State machine**
- [ ] Redirect stores last-used state in a way that creates a stable wrong fixed point
- [ ] Rollback assumed to run after an error (crash between error and rollback)

**Persistence**
- [ ] Destructor-assumed side effect (sync, flush, write) that may not run on pod kill
  (but see KFP §1 — for ublkpp this is currently structurally blocked by resync drain)
- [ ] One-sided version bounds: `version < N` with no `version > MAX` upper-bound rejection
- [ ] Two writes that must be consistent but have no atomic boundary between them
