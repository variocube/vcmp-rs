//! A minimal VCMP server: a controller stand-in for driver development.
//!
//! ```text
//! echo-server [ADDR]      default 0.0.0.0:2000
//! ```
//!
//! Serves `/drivers/{driver}` and `/echo`. Every `echo` message is acknowledged with itself; a
//! `device:DeviceAdded` is logged and acknowledged; other types are NAKed (no handler).

use serde_json::Value;
use std::time::Duration;
use tracing::info;
use vcmp::{VcmpError, VcmpServer};

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
	let level = std::env::var("RUST_LOG").ok().and_then(|l| l.parse().ok()).unwrap_or(tracing::Level::INFO);
	tracing_subscriber::fmt().with_max_level(level).with_ansi(false).init();
	let addr = std::env::args().nth(1).unwrap_or("0.0.0.0:2000".into());

	let server = VcmpServer::builder().heartbeat_interval(Duration::from_secs(20)).build();
	for endpoint in [server.endpoint("/drivers/{driver}"), server.endpoint("/echo")] {
		endpoint.on_type::<Value, _, _, _, _>("echo", |message, _| async move { Ok::<_, VcmpError>(message) });
		endpoint.on_type::<Value, _, _, _, _>("device:DeviceAdded", |device, session| async move {
			info!(session = session.id(), %device, "device added");
			Ok::<(), VcmpError>(())
		});
		endpoint.on_session_connected(|session| async move {
			let info = session.connect_info().cloned().unwrap_or_default();
			info!(session = session.id(), path = info.path, driver = info.param("driver"), "session connected");
		});
		endpoint.on_session_disconnected(|session| async move {
			info!(session = session.id(), "session disconnected");
		});
	}
	let handle = server.bind(&addr).await?;
	info!(addr = %handle.local_addr(), "listening");
	tokio::signal::ctrl_c().await.ok();
	handle.stop().await;
	Ok(())
}
