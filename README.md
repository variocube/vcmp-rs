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

Message ids are 9 random bytes, base64url-encoded (12 chars). Requests default to a 30-second
acknowledgement deadline. A caller can impose a shorter `tokio::time::timeout`; dropping the send
future releases correlation state immediately. Timeout or disconnect leaves delivery and mutation
outcome unknown: the library never replays messages automatically.

## Using it

Add it as a git dependency (not published to crates.io):

```toml
[dependencies]
vcmp = { git = "https://github.com/variocube/vcmp-rs", tag = "0.2.0" }
```

Feature flags: `client` (default), `server`, `axum` (includes `server`), `tls` (`wss://` via
rustls, pure-Rust crypto only for native builds — see [Cross-compilation](#cross-compilation)).
The drivers only need `client`. The axum/hyper dependencies are optional: builds with only
`client` or `server` retain the lightweight standalone transport.

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
client.stop_and_wait().await;                                        // fails pending sends and bounds shutdown
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

### Sharing an axum listener

Enable `axum` to mount VCMP endpoints in an axum 0.8 router alongside REST handlers, static
files and middleware. VCMP uses HTTP/1.1 WebSocket upgrades; the host application owns the
listener and HTTP shutdown:

```toml
[dependencies]
vcmp = { git = "https://github.com/variocube/vcmp-rs", tag = "0.2.0", default-features = false, features = ["axum"] }
axum = "0.8"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "net", "signal"] }
```

```rust
use std::net::SocketAddr;
use axum::{Router, routing::get};
use vcmp::VcmpServer;

let server = VcmpServer::builder().build();
let drivers = server.endpoint("/drivers/{driver}");
// Register VCMP message handlers and session hooks on `drivers` as above.
let app = Router::new()
    .route(drivers.path(), drivers.axum_route())
    .route("/health", get(|| async { "ok" }));
let listener = tokio::net::TcpListener::bind("0.0.0.0:2000").await?;
let result = axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
    .with_graceful_shutdown(async { tokio::signal::ctrl_c().await.ok(); })
    .await;
server.close_sessions().await;
result?;
```

`Endpoint::axum_route()` works with router state, nesting and tower middleware. Session
`ConnectInfo` contains the original request path, axum's decoded path parameters and request
headers. `remote_addr` is populated when the host uses
`into_make_service_with_connect_info::<SocketAddr>()`; otherwise it is `None`.

For application-specific authentication or other extractors, use `vcmp::axum::VcmpUpgrade` in a
custom handler and pass it to `Endpoint::on_upgrade` after checking the request:

```rust
use axum::{extract::State, http::StatusCode, response::{IntoResponse, Response}, routing::get};
use vcmp::{Endpoint, axum::VcmpUpgrade};

#[derive(Clone)]
struct AppState { drivers: Endpoint, authorization: String }

async fn driver_upgrade(State(state): State<AppState>, upgrade: VcmpUpgrade) -> Response {
    if upgrade.connect_info().header("authorization") != Some(state.authorization.as_str()) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    state.drivers.on_upgrade(upgrade)
}

// Register with `.route(drivers.path(), get(driver_upgrade)).with_state(state)`.
```

`VcmpUpgrade` uses hyper's HTTP upgrade and the existing tungstenite transport, preserving
`ServerBuilder::fragment_size`, message limits, VCMP heartbeat and session hooks. Axum's native
`WebSocketUpgrade` does not expose the raw frames needed for outgoing fragmentation and cannot
be passed to `Endpoint::on_upgrade`. Axum's graceful shutdown finishes HTTP connections;
call `server.close_sessions().await` afterward to close upgraded VCMP sessions and fail pending
sends. This also waits for already accepted upgrades to register their sessions or fail, so an
upgrade finishing during HTTP shutdown cannot leave a connection open. Session hooks finish
asynchronously. The standalone `server.bind(...)` API remains available with the `server` feature.

The complete [axum example](examples/axum-server.rs) serves `/drivers/{driver}`, the JSON REST
endpoint `/api/sessions` and a [static dashboard](examples/static/index.html) on the same port.
The example uses `tower-http` with its `fs` feature to serve the static directory:

```bash
cargo run --example axum-server --features axum -- 127.0.0.1:2000
# In another terminal:
curl http://127.0.0.1:2000/                  # static dashboard
curl http://127.0.0.1:2000/api/sessions      # JSON, initially []
cargo run --example echo-driver -- ws://127.0.0.1:2000/drivers/echo --announce
# Refresh the dashboard or GET /api/sessions to see the connected driver.
```

Errors: `VcmpError` *is* a `ProblemDetail` (+ an optional source). Any `std::error::Error` converts
to a `500` with `title` = the error's type name and `detail` = its message; a `503` is a local,
retryable transport condition (`Session not open` / `Session closed` / `Not connected`).

## Bounded transport and lifecycle

The transport foundation for [controller-rs #2](https://github.com/variocube/controller-rs/issues/2)
adds `SessionLimits`, `ResourceLimits` and `ResourceBudget`. This code is an unreleased successor
to released tag `0.2.0` (`a55bde01c122be90412dd453368f52d4047e67fe`); consumers must pin the
reviewed companion PR commit with `rev`, not assume these APIs are present in `0.2.0`. The
original checkout was clean at review; no uncommitted source changes were imported.

```rust
use vcmp::{ResourceBudget, ResourceLimits, SessionLimits, VcmpServer};

let process = ResourceBudget::new(ResourceLimits {
	connections: 32,
	queued_bytes: 8 << 20,
	..Default::default()
});
let server = VcmpServer::builder()
	.resource_budget(process.clone())
	.session_limits(SessionLimits::default())
	.build();
// Also pass process.clone() to every ClientBuilder in the same application.
let usage = process.snapshot();
```

| Limit | Per session default | Shared budget default |
|---|---:|---:|
| Connections and handshake attempts | One connection per session | 128 |
| Application frames queued or writing | 128 / 4 MiB | 1,024 / 16 MiB |
| Reserved ACK/NAK/heartbeat queue | 128 / 4 MiB | 1,024 / 8 MiB |
| Pending requests | 256 | 1,024 |
| Inbound handlers | 128 / 4 MiB wire payload | 256 / 16 MiB wire payload |
| Complete incoming/outgoing VCMP message | 2 MiB | Per-session maximum |

A builder shares its default budget across its endpoints or connection attempts. A directly
constructed `SessionOptions::default()` creates an independent budget. Reuse one explicit
`ResourceBudget` across all clients/servers/sessions for process admission. `Session::try_spawn`
returns `503 Transport overloaded` on exhausted connection admission; the compatibility `spawn`
wrapper panics on failed admission. `Session::resources()` reports local queue/request/handler
usage; connection admission is reported by the shared budget. Zero capacity denies admission;
channel storage uses at least one slot internally but cannot bypass that admission rule.

Requests and handlers default to 30-second deadlines, individual writes and close flushing to
five seconds. `session_limits` configures them; use positive durations. Oversized sends fail with
413 before queueing, and serialization stops growing its output at the message bound. Oversized
incoming frames disconnect. Queue/request admission returns 503 immediately. Handler admission
sends a 503 NAK; if a mandatory ACK/NAK/heartbeat cannot enter its reserved queue, the connection
closes. No business history is silently retained or dropped: callers own durable delivery,
reconciliation and resnapshot policies. Handler timeout is also an ambiguous mutation outcome.

The writer prioritizes control frames between application writes. The reader continues handling
ACKs and heartbeats while a transport write or handler is blocked. One already-started write
cannot be preempted by a heartbeat; the write/watchdog deadline disconnects an unresponsive peer.
Outgoing fragmentation is generated incrementally, avoiding a vector proportional to fragment
count. Each session owns and reaps its handler tasks; close aborts and joins them, aborts its
writer on deadline, drains queues and settles pending requests before `closed()` resolves. Session
ids identify connection generations. Replacing a client connection cancels its old hook/task and
closes its session; old completions cannot install or clear the replacement session.

`headers_per_attempt` generates synthetic or real credentials inside the connection deadline,
once for each attempt, replacing static headers with the same name:

```rust
let client = vcmp::VcmpClient::builder("wss://example.invalid/controller")
	.resource_budget(process.clone())
	.headers_per_attempt(|| async {
		// Generate a fresh proof using application-owned signing and bounded blocking workers.
		Ok(vec![("Authorization".to_owned(), "fresh-proof".to_owned())])
	})
	.build();
client.start();
client.stop_and_wait().await;
```

`stop()` signals cancellation synchronously; `stop_and_wait()` additionally awaits the bounded
client lifecycle. Concurrent shutdown waiters share completion; canceling a waiter keeps the old
task under client control so a later `start()` can still cancel its hooks.
Standalone servers admit a connection before spawning its handshake, time out
incomplete handshakes, stop accepting before shutdown, and await/cancel their connection tasks.
When a disconnect hook awaits `ServerHandle::stop()`, shutdown waits for the other connection
tasks and lets the calling hook finish within its existing handler deadline.
Axum rejects accepted upgrade admission with HTTP 503 and bounds pending upgrade waits; the host
must bound **HTTP connections and headers before the VCMP extractor** and stop its listener before
`close_sessions()`. Hooks have a handler deadline and use the shared handler-task budget. Overload
closes a session whose open hook cannot run; close hooks are best effort under overload and must
not be the only owner of required durable cleanup. Axum close hooks finish asynchronously within
the configured deadline, as in the existing integration contract.

Written requests release their serialized payloads before waiting for acknowledgements; only
correlation state remains pending. Dropping a standalone `ServerHandle` detaches the listener;
retain the handle and call `stop().await` to shut it down.

Resource snapshots count retained wire bytes and active reservations, including in-flight writes.
Canceling a write keeps its reservation until the sink flushes or drops the buffered payload.
Snapshots do not measure allocator overhead, JSON object expansion, kernel buffers or application
allocations. Each admitted WebSocket can additionally hold its bounded reassembly buffer. Actual
RSS, HTTP pre-upgrade resources, a bounded blocking/crypto worker pool and durable business queues
remain host responsibilities. Handler and hook futures must yield: async cancellation cannot
interrupt blocking code or undo a mutation already dispatched to another service. Do not spawn
untracked work from these callbacks. Routine transport logs exclude payloads, URLs and credentials;
`Debug` output redacts connection headers, paths/parameters and client URLs.

Executable coverage is in `tests/bounds.rs` (counts/bytes, non-ACKing peers, drop/deadline cleanup,
control priority, stalled writes, oversized frames and old-handler cancellation),
`tests/payload_retention.rs` (allocation release before acknowledgement), `tests/client.rs`
(fresh credentials, cancellation and replacement), and `tests/server.rs` (bare/axum admission,
slow handshakes, detached listeners and shutdown). The existing codec/property/socket and JS/Java
contract suites continue to check the deployed wire format. Application authorization and durable
replay/recovery are intentionally outside this protocol crate.

## Building

```bash
cargo build                 # client only (default)
cargo build --all-features  # client + server + axum + tls
cargo test --all-features   # unit + loopback + socket-level tests
cargo clippy --all-features --all-targets
```

Formatting is `cargo fmt` (tabs, width 120 — see `rustfmt.toml`, matching the Variocube style).

## Contract tests

`contract/` holds a cross-implementation suite that runs the crate against the **real** other
implementations, both directions, so wire compatibility is a test rather than a hope:

The Rust server scenarios run against both the bare listener and axum host. `contract/run.sh`
and CI enable the `axum` feature to exercise both.

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
