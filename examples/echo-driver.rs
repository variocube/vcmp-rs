//! The measurement binary from the tracking issue: a minimal "driver" that connects to a
//! controller at `ws://localhost:2000/drivers/{name}`, stays connected (answering heartbeats), and
//! handles the controller's messages.
//!
//! ```text
//! echo-driver [URL] [--announce]
//!
//!   URL         default ws://localhost:2000/drivers/echo
//!   --announce  send a `device:DeviceAdded` for a virtual device on every connect
//!   --report    log the process' RSS (from /proc/self/smaps_rollup) every minute
//!
//! Environment: RUST_LOG=trace|debug|info|warn|error (default info)
//! ```
//!
//! Handled messages: `echo` (ACKs with the message itself), `device:Restart` (ACK without payload),
//! `locking:OpenLock` (ACK without payload). Build for measurement with
//! `cargo build --release --example echo-driver --target aarch64-unknown-linux-musl`.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use tracing::{info, warn};
use vcmp::{Backoff, Session, VcmpClient, VcmpError, VcmpMessage};

#[derive(Debug, Serialize, Deserialize)]
struct DeviceAdded {
	id: String,
	types: Vec<String>,
	vendor: String,
	model: String,
	#[serde(rename = "serialNumber", skip_serializing_if = "Option::is_none")]
	serial_number: Option<String>,
	info: Value,
}

impl VcmpMessage for DeviceAdded {
	const TYPE: &'static str = "device:DeviceAdded";
}

#[derive(Debug, Serialize, Deserialize)]
struct RestartDevice {
	id: String,
}

impl VcmpMessage for RestartDevice {
	const TYPE: &'static str = "device:Restart";
}

#[derive(Debug, Serialize, Deserialize)]
struct OpenLock {
	id: String,
	#[serde(rename = "unlockTime", default)]
	unlock_time: Option<u32>,
}

impl VcmpMessage for OpenLock {
	const TYPE: &'static str = "locking:OpenLock";
}

/// Acknowledges an `echo` message with the message itself.
async fn echo(message: Value, _session: Session) -> Result<Value, VcmpError> {
	Ok(message)
}

async fn restart_device(restart: RestartDevice, _session: Session) -> Result<(), VcmpError> {
	info!(id = restart.id, "restart requested");
	Ok(())
}

async fn open_lock(open: OpenLock, _session: Session) -> Result<(), VcmpError> {
	info!(id = open.id, unlock_time = open.unlock_time, "open lock requested");
	Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
	let level = std::env::var("RUST_LOG").ok().and_then(|l| l.parse().ok()).unwrap_or(tracing::Level::INFO);
	tracing_subscriber::fmt().with_max_level(level).with_ansi(false).init();

	let args: Vec<String> = std::env::args().skip(1).collect();
	let announce = args.iter().any(|a| a == "--announce");
	let report = args.iter().any(|a| a == "--report");
	let url = args.iter().find(|a| !a.starts_with("--")).cloned().unwrap_or("ws://localhost:2000/drivers/echo".into());

	let client = VcmpClient::builder(&url)
		.reconnect(Backoff::exponential(Duration::from_secs(1), Duration::from_secs(30)))
		.build();
	client.on_type("echo", echo);
	client.on(restart_device);
	client.on(open_lock);
	client.on_connected(move |session| async move {
		info!("connected");
		if announce {
			let device = DeviceAdded {
				id: "echo-driver".into(),
				types: vec!["Locking".into()],
				vendor: "Variocube".into(),
				model: "echo-driver".into(),
				serial_number: None,
				info: Value::Null,
			};
			match session.send(&device).await {
				Ok(_) => info!("device announced"),
				Err(error) => warn!("could not announce device: {error}"),
			}
		}
	});
	client.on_disconnected(|session| async move { info!(session = session.id(), "disconnected") });
	client.start();

	if report {
		tokio::spawn(async {
			loop {
				tokio::time::sleep(Duration::from_secs(60)).await;
				if let Some(rss) = rss_kib() {
					info!(rss_kib = rss, "memory");
				}
			}
		});
	}

	tokio::signal::ctrl_c().await.ok();
	info!("stopping");
	client.stop();
}

/// The resident set size in KiB, from `/proc/self/smaps_rollup` (Linux only).
fn rss_kib() -> Option<u64> {
	let rollup = std::fs::read_to_string("/proc/self/smaps_rollup").ok()?;
	rollup.lines().find(|l| l.starts_with("Rss:"))?.split_whitespace().nth(1)?.parse().ok()
}
