//! **VCMP** — the Variocube Messaging Protocol — in Rust.
//!
//! VCMP is a lightweight messaging protocol over WebSockets with per-message acknowledgement and
//! a mutual heartbeat. This crate is wire-compatible with
//! [`vcmp-js`](https://github.com/variocube/vcmp-js) and
//! [`vcmp-spring`](https://github.com/variocube/vcmp-spring).
//!
//! - [`frame`]: the codec (`MSG` / `ACK` / `NAK` / `HBT`).
//! - [`error`]: [`ProblemDetail`] and [`VcmpError`].
//! - [`session`]: the protocol state machine over an abstract text-frame duplex.
//! - [`client`] (feature `client`): a reconnecting WebSocket client.
//! - [`server`] (feature `server`): a WebSocket server with path-pattern endpoints.
//! - [`axum`] (feature `axum`): VCMP endpoints alongside HTTP routes in an axum router.
//!
//! ```ignore
//! use vcmp::{VcmpClient, VcmpMessage, Backoff};
//!
//! #[derive(serde::Serialize, serde::Deserialize)]
//! #[serde(tag = "@type", rename = "hello")]
//! struct Hello { from: String }
//! impl VcmpMessage for Hello { const TYPE: &'static str = "hello"; }
//!
//! let client = VcmpClient::builder("ws://localhost:2000/drivers/kerong")
//!     .header("Authorization", "Bearer …")
//!     .reconnect(Backoff::exponential(Duration::from_secs(1), Duration::from_secs(30)))
//!     .build();
//! client.on::<Hello, _, _, _, _>(|msg, _session| async move { Ok::<_, VcmpError>(serde_json::json!({"ok": true})) });
//! client.on_open(|_session| async move { tracing::info!("connected") });
//! client.start();
//! let result: serde_json::Value = client.send(&Hello { from: "driver".into() }).await?;
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod error;
pub mod frame;
pub mod resources;
pub mod session;

#[cfg(feature = "axum")]
pub mod axum;
#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "server")]
pub mod server;
#[cfg(any(feature = "client", feature = "server"))]
mod ws;

pub use error::{ProblemDetail, VcmpError};
pub use frame::Frame;
pub use resources::{ResourceBudget, ResourceLimits, ResourceSnapshot};
pub use session::{ConnectInfo, HandlerMap, Session, SessionLimits, SessionOptions, VcmpMessage};

#[cfg(feature = "client")]
pub use client::{Backoff, ClientBuilder, VcmpClient};
#[cfg(feature = "server")]
pub use server::{BroadcastResult, Endpoint, ServerBuilder, ServerHandle, VcmpServer};
