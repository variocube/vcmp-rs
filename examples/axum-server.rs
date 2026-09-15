//! VCMP, a REST endpoint and static files on one axum listener.
//!
//! ```text
//! cargo run --example axum-server --features axum -- [ADDR]
//! ```
//!
//! The default address is `127.0.0.1:2000`. Open `/` for the session dashboard,
//! GET `/api/sessions` for JSON, or connect a VCMP client to `/drivers/{driver}`.

use axum::{Router, extract::State, http::header, response::IntoResponse, routing::get};
use serde_json::{Value, json};
use std::{net::SocketAddr, time::Duration};
use tower_http::services::ServeDir;
use tracing::info;
use vcmp::VcmpServer;

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
	let level = std::env::var("RUST_LOG").ok().and_then(|l| l.parse().ok()).unwrap_or(tracing::Level::INFO);
	tracing_subscriber::fmt().with_max_level(level).with_ansi(false).init();
	let addr = std::env::args().nth(1).unwrap_or("127.0.0.1:2000".into());

	let server = VcmpServer::builder().heartbeat_interval(Duration::from_secs(20)).build();
	let drivers = server.endpoint("/drivers/{driver}");
	drivers.on_type("echo", |message: Value, _| async move { Ok(message) });
	drivers.on_type("device:DeviceAdded", |device: Value, session| async move {
		info!(session = session.id(), %device, "device added");
		Ok(())
	});
	drivers.on_connected(|session| async move {
		let info = session.connect_info().cloned().unwrap_or_default();
		info!(session = session.id(), path = info.path, driver = info.param("driver"), "session connected");
	});
	drivers.on_disconnected(|session| async move {
		info!(session = session.id(), "session disconnected");
	});

	let app = Router::new()
		.route(drivers.path(), drivers.axum_route())
		.route("/api/sessions", get(sessions))
		.fallback_service(ServeDir::new(concat!(env!("CARGO_MANIFEST_DIR"), "/examples/static")))
		.with_state(server.clone());
	let listener = tokio::net::TcpListener::bind(&addr).await?;
	info!(addr = %listener.local_addr()?, "VCMP, REST and static files listening");
	let result = axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
		.with_graceful_shutdown(async {
			tokio::signal::ctrl_c().await.ok();
		})
		.await;
	// Upgraded WebSockets outlive axum's HTTP connections; close the VCMP sessions explicitly.
	server.close_sessions().await;
	result
}

async fn sessions(State(server): State<VcmpServer>) -> impl IntoResponse {
	let sessions: Vec<Value> = server
		.endpoints()
		.into_iter()
		.flat_map(|endpoint| {
			endpoint.sessions().into_iter().map(move |session| {
				let info = session.connect_info();
				json!({
					"id": session.id(),
					"endpoint": endpoint.path(),
					"path": info.map(|info| &info.path),
					"remote_addr": info.and_then(|info| info.remote_addr),
				})
			})
		})
		.collect();
	([(header::CONTENT_TYPE, "application/json")], Value::Array(sessions).to_string())
}
