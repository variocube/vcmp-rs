# vcmp-rs

Implementation of **VCMP** (Variocube Messaging Protocol) in Rust — client and server.

VCMP is a very simple, lightweight messaging protocol over WebSockets with per-message
acknowledgement and a mutual heartbeat. It is the message bus of every Variocube cube: drivers,
the controller, the unit service, app-host and center all speak it.

This is the Rust port of [`vcmp-js`](https://github.com/variocube/vcmp-js) and
[`vcmp-spring`](https://github.com/variocube/vcmp-spring). It must stay **wire-compatible** with
both — a Rust peer talks to a Java or JavaScript peer without either side knowing the difference.

## Status

Under evaluation. The porting plan lives in the tracking issue; nothing here is used in
production yet. The purpose of this crate is to find out what the Variocube cube stack looks like
in Rust — memory footprint, binary size, cross-compilation to `armhf`/`arm64` — before deciding
whether to port the rest of the stack (drivers, unit, controller).

## Protocol summary

Text WebSocket frames. Each frame starts with a 3-letter type:

| Frame | Layout | Meaning |
|-------|--------|---------|
| `MSG` | `MSG` + 12-char id + JSON payload | A message. Payload is a JSON object with an `@type` field used for dispatch. |
| `ACK` | `ACK` + id + optional JSON payload | The peer's handler completed; payload is its result. |
| `NAK` | `NAK` + id + optional JSON payload | The peer's handler failed; payload is an RFC 7807-style problem detail (`title`, `status`, `detail`, …). |
| `HBT` | `HBT` + interval in ms (decimal) | Heartbeat. One side initiates; the receiver echoes after the interval; each side closes the session if no heartbeat arrives within 2 × interval. |

Message ids are 9 random bytes, base64url-encoded (12 chars). There is **no library-level
acknowledgement timeout** — a send stays pending until the session closes; bounding the wait is
the caller's decision (see the vcmp-js / vcmp-spring READMEs for the rationale).

## Building

```bash
cargo build
cargo test
```

Formatting is enforced by `devtools` (`./devtools.sh`).
