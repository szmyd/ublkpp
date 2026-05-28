# Removal / Iteration Plan Template

Use this template when identifying code that may be candidates for deletion or phase-out.

---

## Removal Candidate

**Location**: `path/to/file.cpp:line`
**Type**: `[ ] Unused code  [ ] Redundant duplicate  [ ] Dead feature flag  [ ] Obsolete workaround`

**Description**: What is this code, and why does it appear to be safe to remove?

**Evidence of non-use**:
- No callers found via `grep -r`
- Feature flag evaluates to `false` in all environments
- Replaced by `<new symbol>` in commit `<sha>`

**Risk assessment**:
- `[ ] Safe to delete now` — no callers, no side effects, test coverage confirms
- `[ ] Defer with plan` — callers exist but are themselves candidates; remove as a group

---

## Iteration Plan (for deferred removals)

Use when safe-delete-now is not possible: the code is entangled with other live code that must be migrated first.

### Step 1 — Deprecate
- Add a deprecation marker / log warning at call sites.
- Target date: `YYYY-MM-DD`
- Owner: `<name>`

### Step 2 — Migrate callers
- List each call site and the replacement:
  | Call site | Replacement |
  |---|---|
  | `foo.cpp:42` | `new_foo()` |
- Checkpoint: all tests green after migration, no remaining direct calls.

### Step 3 — Delete
- Remove the deprecated code.
- Remove the deprecation markers.
- Update any related documentation or CHANGELOG.
- Checkpoint: CI passes, coverage unchanged or improved.

### Step 4 — Verify
- Run integration tests or staging smoke tests.
- Confirm no runtime errors in logs for 1 deployment cycle.

---

## Notes

- Prefer deleting in a dedicated PR (not bundled with a feature) so the diff is reviewable.
- If deletion touches a public API, check downstream consumers and semver implications.
- If the code is behind a feature flag, coordinate flag retirement with the flag owner.
