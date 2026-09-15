//! Shared helpers of the socket-level integration tests.
#![allow(dead_code)]

#[path = "../server_host/mod.rs"]
pub mod server_host;
pub(crate) use server_host::server_tests;
pub use server_host::{HostHandle, ServerHost};

use serde::{Deserialize, Serialize};
use std::time::Duration;
use vcmp::{Backoff, Endpoint, VcmpClient, VcmpError, VcmpMessage, VcmpServer};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Echo {
	pub payload: String,
}

impl VcmpMessage for Echo {
	const TYPE: &'static str = "test:Echo";
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Void {}

impl VcmpMessage for Void {
	const TYPE: &'static str = "test:Void";
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Fail {
	pub status: u16,
	pub title: String,
	pub detail: String,
}

impl VcmpMessage for Fail {
	const TYPE: &'static str = "test:Fail";
}

/// A message whose handler never acknowledges.
#[derive(Debug, Serialize, Deserialize)]
pub struct Never {}

impl VcmpMessage for Never {
	const TYPE: &'static str = "test:Never";
}

/// A message no peer registers a handler for.
#[derive(Debug, Serialize, Deserialize)]
pub struct Unknown {}

impl VcmpMessage for Unknown {
	const TYPE: &'static str = "test:Unknown";
}

/// Registers the standard test handlers (echo, void, fail, never) on a client or endpoint.
pub fn register(handlers: &vcmp::HandlerMap) {
	handlers.on(|echo: Echo, _| async move { Ok(echo.payload) });
	handlers.on(|_: Void, _| async { Ok(()) });
	handlers.on(|fail: Fail, _| async move {
		Err::<(), _>(VcmpError::new(fail.status, fail.title).with_detail(fail.detail))
	});
	handlers.on(|_: Never, _| std::future::pending::<Result<(), VcmpError>>());
}

pub fn init_tracing() {
	let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::DEBUG).with_test_writer().try_init();
}

/// Starts either host with a `/test/{name}` endpoint carrying the standard handlers.
pub async fn start_hosted_server(host: ServerHost, port: u16, heartbeat: Duration) -> (HostHandle, Endpoint) {
	let server = VcmpServer::builder().heartbeat_interval(heartbeat).build();
	let endpoint = server.endpoint("/test/{name}");
	register(endpoint.handlers());
	let handle = host.bind(server, port).await;
	(handle, endpoint)
}

/// Builds (but does not start) a client with fast reconnects and the standard handlers.
pub fn client(url: &str) -> VcmpClient {
	let client = VcmpClient::builder(url)
		.reconnect(Backoff::fixed(Duration::from_millis(100)))
		.initial_heartbeat_timeout(Duration::from_secs(5))
		.build();
	register(client.handlers());
	client
}

pub async fn connected(client: &VcmpClient) {
	tokio::time::timeout(Duration::from_secs(5), client.wait_connected()).await.expect("client did not connect");
}

pub async fn disconnected(client: &VcmpClient) {
	tokio::time::timeout(Duration::from_secs(5), client.wait_disconnected()).await.expect("client did not disconnect");
}

pub async fn wait_until(condition: impl Fn() -> bool) {
	tokio::time::timeout(Duration::from_secs(5), async {
		while !condition() {
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
	})
	.await
	.expect("condition not met in time");
}

pub fn free_port() -> u16 {
	std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}
