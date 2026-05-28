# Security Checklist

## Command & Path Injection

- [ ] Are shell calls using argument arrays, not interpolated strings with user input?
- [ ] Are user-supplied file paths resolved and checked against an allowed base directory?
- [ ] Is `..` stripped or rejected in any file path parameters?

## Secret Leakage

- [ ] Are API keys, credentials, or tokens hardcoded or committed to version control?
- [ ] Could secrets appear in logs, error messages, or debug output?

## Cryptography

- [ ] Is a strong, modern algorithm used (AES-256, SHA-256+)? No MD5/SHA1 for security purposes.
- [ ] Are cryptographic nonces/IVs unique per operation?
- [ ] Are checksums or integrity checks applied to on-disk structures (superblock, bitmap)?

## Race Conditions (primary concern for this codebase)

- [ ] Is shared mutable state protected by a mutex or atomics?
- [ ] Are check-then-act sequences (TOCTOU) atomic or otherwise safe?
- [ ] Are lock ordering conventions consistent across the codebase to prevent deadlock?
- [ ] Are signal handlers async-signal-safe?
- [ ] Are memory-mapped or shared memory regions properly synchronized?
- [ ] Can the IDLE→STOPPING or similar state machine transitions race? Are they guarded by atomic status counters?
- [ ] Are coroutine/async I/O completions properly sequenced — no access to freed state after `co_await`?

## Reliability / Resource Safety

- [ ] Are resource allocations bounded (max I/O size, max queue depth, max allocation)?
- [ ] Are unbounded loops or recursion possible from external input?
- [ ] Are file descriptors, memory maps, and allocated buffers released on all error paths (RAII)?
- [ ] Are retries capped to avoid infinite retry storms on persistent device errors?
- [ ] Is device error propagation correct — does a failed write reach the caller's `io_result`?

## Memory Safety

- [ ] Are raw pointer lifetimes clear and bounded by RAII or explicit ownership?
- [ ] Are there use-after-free or use-after-move risks (especially across `co_await` suspension points)?
- [ ] Are buffer sizes validated before memcpy/read/write to avoid overflow?
- [ ] Are integer conversions between signed and unsigned safe (no UB wrap-around affecting sizes)?
