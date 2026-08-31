# vcmp-rs

Implementation of **VCMP** (Variocube Messaging Protocol) in Rust — client and server.

VCMP is a very simple, lightweight messaging protocol over WebSockets with per-message
acknowledgement and a mutual heartbeat. It is the message bus of every Variocube cube: drivers,
the controller, the unit service, app-host and center all speak it.

This is the Rust port of [`vcmp-js`](https://github.com/variocube/vcmp-js) and
[`vcmp-spring`](https://github.com/variocube/vcmp-spring). It stays **wire-compatible** with both —
a Rust peer talks to a Java or JavaScript peer without either side knowing the difference. That
compatibility is enforced by a cross-implementation [contract test suite](#contract-tests) that runs
the crate against the real other implementations in CI, in both directions.

## Status

Under evaluation (see the tracking issue). The crate is feature-complete for `0.1`: `frame`,
`error`, `session`, `client` and `server` with unit, loopback, socket-level and contract tests all
green. Not yet used in production. The point of the exercise — the memory footprint and
cross-compilation numbers — is in [Measurement](#measurement).

## Protocol summary

Text WebSocket frames. Each frame starts with a 3-letter type:

| Frame | Layout | Meaning |
|-------|--------|---------|
| `MSG` | `MSG` + 12-char id + JSON payload | A message. Payload is a JSON object with an `@type` field used for dispatch. |
| `ACK` | `ACK` + id + optional JSON payload | The peer's handler completed; payload is its result. |
| `NAK` | `NAK` + id + optional JSON payload | The peer's handler failed; payload is an RFC 7807-style problem detail (`title`, `status`, `detail`, …). |
| `HBT` | `HBT` + interval in ms (decimal) | Heartbeat. One side initiates; the receiver echoes after the interval; each side closes the session if no heartbeat arrives within 2 × interval. |

Message ids are 9 random bytes, base64url-encoded (12 chars). There is **no library-level
acknowledgement timeout** — a send stays pending until the session closes; bounding the wait is the
caller's decision (`tokio::time::timeout(d, session.send(&m))`).

## Using it

Add it as a git dependency (not published to crates.io):

```toml
[dependencies]
vcmp = { git = "https://github.com/variocube/vcmp-rs", tag = "0.1.0" }
```

Feature flags: `client` (default), `server`, `tls` (`wss://` via rustls, pure-Rust crypto only for
native builds — see [Cross-compilation](#cross-compilation)). The drivers only need `client`.

A message type is any `Serialize + Deserialize` whose `@type` is the serde tag:

```rust
use serde::{Deserialize, Serialize};
use vcmp::VcmpMessage;

#[derive(Serialize, Deserialize)]
#[serde(tag = "@type", rename = "device:DeviceAdded")]
struct DeviceAdded { id: String, vendor: String }

impl VcmpMessage for DeviceAdded {
    const TYPE: &'static str = "device:DeviceAdded";
}
```

### Client

```rust
use std::time::Duration;
use vcmp::{Backoff, VcmpClient, VcmpError};

let client = VcmpClient::builder("ws://localhost:2000/drivers/kerong")
    .header("Authorization", token)                                   // custom handshake headers
    .reconnect(Backoff::exponential(Duration::from_secs(1), Duration::from_secs(30)))
    .build();

// handler: Fn(M, Session) -> Future<Output = Result<impl Serialize, impl Into<VcmpError>>>
client.on::<OpenLock, _, _, _, _>(|open, _session| async move {
    hardware.open(&open.id).await?;
    Ok::<_, VcmpError>(())        // ACK without payload
});
client.on_open(|session| async move {
    session.send(&DeviceAdded { id: "lock-1".into(), vendor: "Kerong".into() }).await.ok();
});

client.start();                                                       // connects and reconnects
let result: serde_json::Value = client.send(&msg).await?;             // Result<Value, VcmpError>
let typed: Ack = client.send_as::<_, Ack>(&msg).await?;               // deserialized result
tokio::time::timeout(Duration::from_secs(20), client.send(&msg)).await??;   // caller-side bound
client.stop();                                                       // fails pending sends with 503
```

### Server

```rust
use std::time::Duration;
use vcmp::{VcmpServer, VcmpError};

let server = VcmpServer::builder().heartbeat_interval(Duration::from_secs(20)).build();
let drivers = server.endpoint("/drivers/{driver}");                  // path params are readable
drivers.on::<DeviceAdded, _, _, _, _>(|device, session| async move {
    let driver = session.connect_info().unwrap().param("driver").unwrap_or_default();
    tracing::info!(driver, id = device.id, "device added");
    Ok::<_, VcmpError>(())
});
drivers.on_session_connected(|session| async move { /* … */ });
drivers.on_session_disconnected(|session| async move { /* … */ });

let handle = server.bind("0.0.0.0:2000").await?;
let results = drivers.broadcast(&msg).await;                          // per-session Ok/Err, never fails as a whole
handle.stop().await;                                                 // closes every session
```

Errors: `VcmpError` *is* a `ProblemDetail` (+ an optional source). Any `std::error::Error` converts
to a `500` with `title` = the error's type name and `detail` = its message; a `503` is a local,
retryable transport condition (`Session not open` / `Session closed` / `Not connected`).

## Building

```bash
cargo build                 # client only (default)
cargo build --all-features  # client + server + tls
cargo test --all-features   # unit + loopback + socket-level tests
cargo clippy --all-features --all-targets
```

Formatting is `cargo fmt` (tabs, width 120 — see `rustfmt.toml`, matching the Variocube style).

## Contract tests

`contract/` holds a cross-implementation suite that runs the crate against the **real** other
implementations, both directions, so wire compatibility is a test rather than a hope:

| Rust side | Peer | Peer source |
|-----------|------|-------------|
| client | `vcmp-js` server | `contract/js/peer.js` (`@variocube/vcmp-server`) |
| server | `vcmp-js` client | `contract/js/peer.js` (`@variocube/vcmp`) |
| client | `vcmp-spring` server | `contract/java` Spring Boot app (`@VcmpEndpoint`) |
| server | `vcmp-spring` client | `contract/java` Spring Boot app (`BasicVcmpClient`) |

Each direction covers: send + ACK (with and without result); NAK with a problem detail
(status/title/detail preserved); unknown `@type` → empty NAK; 100 KB and 1 MB messages; 100
concurrent pending sends settled in any order; heartbeat exchange for ≥ 3 intervals; and a server
restart → reconnect → send-after-reconnect. They are `#[ignore]`d by default (they spawn `node` /
`java`); run them with:

```bash
contract/run.sh            # both peers (needs node, a JDK, and ../vcmp-spring)
contract/run.sh js         # only vcmp-js
```

Two things the contract suite established against the real peers:

- **A > 8 KB single-frame text message is accepted by the Java/Tomcat side.** This crate sends each
  message as one WebSocket frame; the 1 MB `contract` scenario round-trips through `vcmp-spring`
  unchanged. Outgoing 8 KB fragmentation (as the Java client does) is available behind
  `ClientBuilder::fragment_size` but is not needed for the controller.
- **Malformed JSON is NAKed with different statuses across implementations.** `vcmp-js` and this
  crate report `400 "Invalid message"`; `vcmp-spring` reports `500 "Message handling failed"` (its
  Jackson parse error is caught generically). A sender must treat a NAK as a failure regardless of
  status; the contract test accepts both.

## Measurement

`examples/echo-driver` is a minimal driver that connects to a controller at
`ws://localhost:2000/drivers/{name}`, answers the heartbeat and handles `echo` / `device:Restart` /
`locking:OpenLock`. Built with the release profile in `Cargo.toml` (`opt-level="z"`, fat LTO, one
codegen unit, `panic="abort"`, stripped) as a static musl binary:

| Target | Binary size | Idle RSS |
|--------|-------------|----------|
| `aarch64-unknown-linux-musl` | **≈ 0.80 MiB** | **≈ 1.0 MiB** (single thread) |
| `armv7-unknown-linux-musleabihf` | **≈ 0.74 MiB** | *(pending on-device)* |
| `x86_64-unknown-linux-musl` | ≈ 0.91 MiB | — |

Both are far inside the issue's targets (≤ 3 MB binary, ≤ 5 MB RSS). RSS was measured from
`/proc/<pid>/smaps_rollup` on an arm64 host at idle; the full 1 h on-device figures on an arm64 and
an armhf cube, next to the Deno driver's, go in the tracking issue. Reproduce with:

```bash
cargo build --release --example echo-driver --target aarch64-unknown-linux-musl
cargo build --release --example echo-server  --features server --target aarch64-unknown-linux-musl
./target/.../echo-server 127.0.0.1:2000 &
./target/.../echo-driver ws://127.0.0.1:2000/drivers/echo --announce --report
cat /proc/$(pgrep -f echo-driver)/smaps_rollup
```

## Cross-compilation

The default (`client`, `ws://`) build is **pure Rust**: `rust-lld` links the static musl binary with
no C cross-toolchain, for all three targets. CI (`.github/workflows/release.yml`) builds
`x86_64`, `aarch64` and `armv7` musl on every release and attaches the binaries.

```bash
rustup target add aarch64-unknown-linux-musl armv7-unknown-linux-musleabihf
cargo build --release --example echo-driver --target aarch64-unknown-linux-musl
```

The `tls` feature (`wss://`) pulls in `ring`, which needs a C compiler when cross-compiling — use
[`cross`](https://github.com/cross-rs/cross) or
[`cargo-zigbuild`](https://github.com/rust-cross/cargo-zigbuild) for that. Cube-internal driver ↔
controller traffic is `ws://` on localhost, so drivers do not need it.

## License

MIT — see [`LICENSE`](LICENSE).
