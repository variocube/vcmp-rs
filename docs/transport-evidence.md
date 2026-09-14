# Bounded transport acceptance evidence

Companion implementation for [controller-rs #2](https://github.com/variocube/controller-rs/issues/2),
recorded 2026-09-14. The public contracts and host responsibilities are in
[README.md](../README.md#bounded-transport-and-lifecycle).

## Source provenance

- Rust review baseline: released `0.2.0`, `a55bde01c122be90412dd453368f52d4047e67fe`.
  The original working tree was clean; this change was developed in an isolated worktree.
- Java wire peer: released `vcmp-spring` `6.0.0`, `442d402673cea715804aba4ef07b83846317c42b`,
  built in another isolated worktree. Untracked Eclipse/build artifacts in the original checkout
  were not imported.
- JavaScript wire peers: npm `@variocube/vcmp` and `@variocube/vcmp-server` `4.0.0`, using
  the existing committed `contract/js/package-lock.json` integrity hashes.
- This transport change is committed unreleased code; no release or production deployment is claimed.

## Executed checks

Linux aarch64 development host, Rust/Cargo 1.98.0. `CARGO_INCREMENTAL=0`,
`CARGO_PROFILE_DEV_DEBUG=0` and `CARGO_PROFILE_TEST_DEBUG=0` kept temporary build storage bounded.
These switches change build artifact size, not transport limits.

| Check | Result |
|---|---|
| `cargo fmt --check` | Passed |
| `cargo clippy --all-features --all-targets -- -D warnings` | Passed |
| `cargo test --all-features` | 122 passed; 6 external contract tests separately executed below |
| JavaScript contracts, Rust client / bare server / axum server | 3 passed |
| Java contracts, Rust client / bare server / axum server | 3 passed |

`tests/bounds.rs` proves count and wire-byte accounting, explicit admission failure, caller abort
and deadline cleanup, no automatic replay, reserved control output under a slow consumer, bounded
stalled-writer teardown, handler cancellation/deadlines, oversized-message rejection, and redacted
connection diagnostics. Socket tests additionally cover both host admissions and recovery,
incomplete handshakes, client credential refresh, cancellation during header generation/open hooks,
and generation replacement before old handler work can resume.

The protocol peer suites retain malformed JSON/unknown message behavior, ACK/NAK payloads,
100 KiB/1 MiB payloads, concurrency, heartbeat and reconnect behavior. No physical board, fleet,
full-process RSS, flash-write or application recovery qualification was performed in this library
change. HTTP admission before an axum upgrade and blocking application workers are host-owned.

## PR review corrections

The follow-up fixes release the original outgoing payload before awaiting an acknowledgement and
preserve standalone listener operation when its `ServerHandle` is dropped. The allocation regression
in `tests/payload_retention.rs` and the detached-listener regression in `tests/server.rs` both fail
before their respective fixes and pass afterward.

Further lifecycle corrections keep canceled or timed-out writes accounted for until the sink drops
their buffered payloads, preserve client task cancellation across canceled/concurrent shutdown
waiters, and allow a disconnect hook to await server shutdown without waiting on itself. Seven
additional tests cover data/control admission through sink teardown, cancellation and replacement
of client close hooks, concurrent shutdown waiters, and server shutdown waiting for other hooks.
The buffered-write regressions fail against the previous PR head (`8ce3932`) and pass with the fix.

All 122 Rust tests pass, along with formatting, clippy with warnings denied, default-feature tests,
isolated-feature checks, and the client/server dependency isolation check. All three JS directions
and all three Java directions were rerun after these corrections and pass against the same peers
listed above. The installed Java peer's `vcmp.jar` matches the isolated released-source build by
SHA-256 (`5a1a6cf113398b4f630750adae7ae4f6df9348325e1601cf625b1674a4ceb241`).
