//! Shared helpers of the socket-level integration tests.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use std::time::Duration;
use vcmp::{Backoff, Endpoint, ServerHandle, VcmpClient, VcmpError, VcmpMessage, VcmpServer};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(tag = "@type", rename = "test:Echo")]
pub struct Echo {
	pub payload: String,
}

impl VcmpMessage for Echo {
	const TYPE: &'static str = "test:Echo";
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "@type", rename = "test:Void")]
pub struct Void {}

impl VcmpMessage for Void {
	const TYPE: &'static str = "test:Void";
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "@type", rename = "test:Fail")]
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
#[serde(tag = "@type", rename = "test:Never")]
pub struct Never {}

impl VcmpMessage for Never {
	const TYPE: &'static str = "test:Never";
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "@type", rename = "test:Unknown")]
pub struct Unknown {}

/// Registers the standard test handlers (echo, void, fail, never) on a client or endpoint.
pub fn register(handlers: &vcmp::HandlerMap) {
	handlers.on::<Echo, _, _, _, _>(|echo, _| async move { Ok::<_, VcmpError>(echo.payload) });
	handlers.on::<Void, _, _, _, _>(|_, _| async { Ok::<(), VcmpError>(()) });
	handlers.on::<Fail, _, _, _, _>(|fail, _| async move {
		Err::<(), _>(VcmpError::new(fail.status, fail.title).with_detail(fail.detail))
	});
	handlers.on::<Never, _, _, _, _>(|_, _| async {
		std::future::pending::<()>().await;
		Ok::<(), VcmpError>(())
	});
}

pub fn init_tracing() {
	let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::DEBUG).with_test_writer().try_init();
}

/// Starts a server with a `/test/{name}` endpoint carrying the standard handlers.
pub async fn start_server(heartbeat: Duration) -> (ServerHandle, Endpoint) {
	let server = VcmpServer::builder().heartbeat_interval(heartbeat).build();
	let endpoint = server.endpoint("/test/{name}");
	register(endpoint.handlers());
	let handle = server.bind("127.0.0.1:0").await.unwrap();
	(handle, endpoint)
}

/// Starts a server on a specific port (for restart scenarios).
pub async fn start_server_on(port: u16, heartbeat: Duration) -> (ServerHandle, Endpoint) {
	let server = VcmpServer::builder().heartbeat_interval(heartbeat).build();
	let endpoint = server.endpoint("/test/{name}");
	register(endpoint.handlers());
	let handle = server.bind(("127.0.0.1", port)).await.unwrap();
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
