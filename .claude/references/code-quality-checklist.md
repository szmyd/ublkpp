# Code Quality Checklist

## Error Handling

- [ ] Are `std::expected` / `io_result` return values checked at every call site?
- [ ] Are errors logged with `DLOGE`/`TLOGE` (with context) before being returned?
- [ ] Are catch blocks specific — no bare `catch (...)` that hides the error type?
- [ ] In coroutines: are all `co_await` error paths handled, not just the happy path?
- [ ] Are destructors and RAII types used to guarantee cleanup, not just happy-path `free`?
- [ ] Are error messages actionable — do they say what failed and with what input?
- [ ] Is the correct SISL logging module tag used (`ublksrv`, `ublk_tgt`, `ublk_raid`, `ublk_drivers`)?

## Performance

- [ ] Are CPU-intensive operations (hashing, compression, bitmap scanning) on the hot I/O path?
- [ ] Are large allocations or buffer copies inside tight loops (should be outside or pooled)?
- [ ] Are memory-mapped or zero-copy paths used where appropriate for large I/O?
- [ ] Are lock contention hotspots visible (global mutex, coarse-grained lock on hot path)?
- [ ] Is bitmap dirty-tracking efficient — are chunk boundaries aligned, avoiding spurious write amplification?
- [ ] Are coroutine suspensions minimal on the critical I/O path?

## Boundary Conditions

- [ ] Is behavior defined for null/nullptr inputs?
- [ ] Are empty collections handled without panicking (e.g., `front()` on empty vector)?
- [ ] Are integer overflows, underflows, and wrap-around possible (especially with sizes and offsets)?
- [ ] Are off-by-one errors possible in range loops, chunk boundaries, or size comparisons?
- [ ] Are signed/unsigned comparison mismatches present (UB in C++)?
- [ ] Are division-by-zero paths possible (e.g., stripe width, chunk size as divisors)?
- [ ] Are I/O offset + length combinations validated to not exceed device bounds?

## Code Hygiene

- [ ] Are there unused `#include`s or forward declarations?
- [ ] Are there purely empty files (except intentional `.gitkeep`)?
- [ ] Are there files containing only commented-out code?
- [ ] Are there unreachable code blocks (after `return`, infinite loops, dead branches)?
- [ ] Are there TODO/FIXME/HACK comments that should be resolved or tracked?
- [ ] Are there magic numbers that should be named `k_` constants?
- [ ] Is dead code (unused functions, variables, types) present in the diff?

## Naming & Conventions

- [ ] Public API types in `include/ublkpp/`: `lower_snake_case` for classes and free functions.
- [ ] Internal classes in `src/`: `PascalCase`.
- [ ] Functions and methods: `snake_case`.
- [ ] Members: `_snake_case`.
- [ ] Constants: `k_snake_case`.
- [ ] Macros and enums: `SCREAMING_SNAKE_CASE`.
- [ ] Namespaces: lowercase (`ublkpp`, `ublkpp::raid1`).

## Testing

- [ ] Are the new code paths covered by tests in `src/<component>/tests/`?
- [ ] Do the tests exercise error/failure paths, not just the happy path?
- [ ] Are race conditions or async paths tested (e.g., resync completion races)?
- [ ] Are tests deterministic — no reliance on timing or external state?
- [ ] Are test names descriptive enough to diagnose a failure without reading the body?
- [ ] Coverage target: 100% for new code; 80% high, 65% medium for existing.

## Readability & Maintainability

- [ ] Are function/variable names self-explanatory without needing a comment?
- [ ] Are functions short enough to read in one screen (~40 lines guideline)?
- [ ] Is complex logic accompanied by a brief WHY comment — non-obvious invariants, lock ordering, workarounds?
- [ ] Is duplicated logic extracted into a shared utility?
