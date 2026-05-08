# Code Review Expert

Perform a structured review of the current git changes with focus on SOLID, architecture, removal candidates, and security risks. Default to review-only output unless the user asks to implement changes.

## Severity Levels

| Level | Name | Description | Action |
|---|---|---|---|
| P0 | Critical | Security vulnerability, data loss risk, correctness bug | Must block merge |
| P1 | High | Logic error, significant SOLID violation, performance regression | Should fix before merge |
| P2 | Medium | Code smell, maintainability concern, minor SOLID violation | Fix in this PR or create follow-up |
| P3 | Low | Style, naming, minor suggestion | Optional improvement |

## Scope Constraint: Diff-First, Legacy-Separate, Legacy-Non-Blocking

**STRICT RULE — must be followed on every review:**

1. **Diff-First**: The primary review MUST focus only on the current code changes (the diff). Do not mix legacy findings into the main review findings.
2. **Legacy-Separate**: Existing/surrounding legacy code MAY be read for context. Any issues found in legacy code MUST be placed in a separate, dedicated section titled `## Legacy Code Observations` that appears after the main findings.
3. **Legacy-Non-Blocking**: Issues identified in legacy code MUST NOT block the current PR merge. They CANNOT be rated P0 or P1 for the current PR. They are treated as informational suggestions or tech-debt prompts only, suitable for future refactoring.

## Workflow

### 1) Preflight context

If reviewing a PR, use `gh pr view` and `gh pr diff` to fetch PR title, description, and diff. Otherwise use `git status -sb`, `git diff --stat`, and `git diff` to scope local changes.

If needed, use `rg` or `grep` to find related modules, usages, and contracts. Identify entry points, ownership boundaries, and critical paths.

Edge cases:
- **No changes**: If diff is empty, inform user and ask if they want to review staged changes or a specific commit range.
- **Large diff (>500 lines)**: Summarize by file first, then review in batches by module/feature area.
- **Mixed concerns**: Group findings by logical feature, not just file order.

### 2) SOLID + architecture smells

Read `.claude/references/solid-checklist.md` for specific prompts. Look for:
- **SRP**: Overloaded modules with unrelated responsibilities.
- **OCP**: Frequent edits to add behavior instead of extension points.
- **LSP**: Subclasses that break expectations or require type checks.
- **ISP**: Wide interfaces with unused methods.
- **DIP**: High-level logic tied to low-level implementations.

Also ask:
- Are responsibilities correctly separated, or are two concerns coupled in one place?
- Could this be done better or made smaller?
- Is this part designed correctly, or could it be improved?

When proposing a refactor, explain why it improves cohesion/coupling and outline a minimal, safe split. If non-trivial, propose an incremental plan instead of a large rewrite.

### 3) Removal candidates + iteration plan

Read `.claude/references/removal-plan.md` for template. Identify code that is unused, redundant, or feature-flagged off. Distinguish safe delete now vs defer with plan. Provide a follow-up plan with concrete steps and checkpoints.

### 4) Security and reliability scan

Read `.claude/references/security-checklist.md` for coverage. Check for:
- Command injection, path traversal in any shell or file operations
- Secret leakage or API keys in logs/env/files
- Unbounded loops, CPU/memory hotspots, missing resource cleanup
- Race conditions: concurrent access, check-then-act, TOCTOU, missing locks
- Weak crypto or insecure defaults

This is a C++ userspace block device driver — prioritize: memory safety, race conditions, resource leaks, and I/O correctness.

Call out both exploitability and impact.

### 5) Code quality scan

Read `.claude/references/code-quality-checklist.md` for coverage. Also ask:
- Does this code break anything?
- Are there race conditions?
- Are there logic issues?

Check for:
- **Error handling**: unchecked `std::expected` returns, missing `DLOGE` before returning errors, unhandled async errors
- **Performance**: CPU-intensive ops on hot I/O paths, missing cache, unbounded memory growth
- **Boundary conditions**: null/nullptr handling, empty collections, integer overflow/underflow, off-by-one, signed/unsigned mismatches (UB)
- **Hygiene**: unused `#include`s, empty files (except .gitkeep), files containing only commented-out code
- **Naming & conventions**: verify public API uses `lower_snake_case`, internal classes use `PascalCase`, members use `_snake_case`, constants use `k_snake_case` — per project conventions
- **Logging**: errors logged with SISL macros (`DLOGE`, `RLOGW`, etc.) before returning, correct module tag used

### 6) Commit message review

Retrieve commit messages using `git log --oneline` or `git log`. For each commit verify:
- **Clarity**: Clearly describes what was changed and why.
- **Meaningfulness**: Not vague (e.g., "fix", "update", "WIP", "misc changes").
- **Format**: Subject line ≤72 chars, imperative mood, not trailing off.
- **Scope**: Each commit represents a single coherent unit of work.

For unclear messages, suggest improvements:

```
❌ Original: <original message>
✅ Suggested: <improved message that explains what changed and why>
```

### 7) Output format

Structure your review as follows:

```
## Code Review Summary

**Files reviewed**: X files, Y lines changed
**Overall assessment**: [APPROVE / REQUEST_CHANGES / COMMENT]

---

## Findings

### P0 - Critical
(none or list)

### P1 - High
1. **[file:line]** Brief title
   - Description of issue
   - Suggested fix

### P2 - Medium
...

### P3 - Low
...

---

## Removal/Iteration Plan
(if applicable)

## Commit Message Review
(none or list with improvements)

## Additional Suggestions
(optional, non-blocking)

---

## Legacy Code Observations
<!-- NON-BLOCKING — informational tech-debt prompts only -->
(none or list)
```

**Clean review**: If no issues found, explicitly state what was checked, areas not covered, and residual risks.

### 8) Posting comments (PR reviews only)

- Post comments on **specific diff lines**, not as top-level PR comments.
- Discuss critical findings with the user **before** posting.
- Always include a **suggested fix** when raising a bug, not just the problem.
- Do NOT reject or approve the PR yourself.

### 9) Next steps confirmation

After presenting findings, ask:

```
---

## Next Steps

I found X issues (P0: _, P1: _, P2: _, P3: _).

**How would you like to proceed?**

1. **Fix all** - I'll implement all suggested fixes
2. **Fix P0/P1 only** - Address critical and high priority issues
3. **Fix specific items** - Tell me which issues to fix
4. **No changes** - Review complete, no implementation needed

Please choose an option or provide specific instructions.
```

**Important**: Do NOT implement any changes until user explicitly confirms. This is a review-first workflow.
