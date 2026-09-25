//! Shared driver of the cross-implementation contract tests (`contract_js`, `contract_java`).
//!
//! A contract *peer* is a real `vcmp-js` / `vcmp-spring` process that implements the handlers
//! documented in `contract/js/peer.js`. The tests here are written once against that interface and
//! run in both directions: Rust client ↔ peer server, Rust server ↔ peer client.
#![allow(dead_code)]

#[path = "../server_host/mod.rs"]
pub mod server_host;
pub use server_host::{HostHandle, ServerHost};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use vcmp::{Backoff, Endpoint, ProblemDetail, Session, VcmpClient, VcmpError, VcmpMessage, VcmpServer};

pub const TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Echo {
	pub payload: String,
}

impl VcmpMessage for Echo {
	const TYPE: &'static str = "contract:Echo";
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Void {}

impl VcmpMessage for Void {
	const TYPE: &'static str = "contract:Void";
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Fail {
	pub status: u16,
	pub title: String,
	pub detail: String,
}

impl VcmpMessage for Fail {
	const TYPE: &'static str = "contract:Fail";
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Never {}

impl VcmpMessage for Never {
	const TYPE: &'static str = "contract:Never";
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Unknown {}

impl VcmpMessage for Unknown {
	const TYPE: &'static str = "contract:Unknown";
}

/// Asks the peer to perform a scenario against us and report the outcome.
#[derive(Debug, Serialize, Deserialize)]
pub struct Run {
	pub scenario: String,
	#[serde(flatten)]
	pub args: Value,
}

impl VcmpMessage for Run {
	const TYPE: &'static str = "contract:Run";
}

#[derive(Debug, Deserialize)]
pub struct Outcome {
	pub ok: bool,
	#[serde(default)]
	pub result: Value,
	#[serde(default)]
	pub error: Option<ProblemDetail>,
}

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

/// A peer process with its stdout events (READY / CONNECTED / DISCONNECTED).
pub struct PeerProcess {
	child: Child,
	events: Arc<Mutex<Vec<String>>>,
}

impl PeerProcess {
	pub async fn spawn(mut command: Command) -> Self {
		command.stdout(Stdio::piped()).stderr(Stdio::inherit()).kill_on_drop(true);
		let mut child = command.spawn().expect("could not spawn the peer process");
		let stdout = child.stdout.take().unwrap();
		let events = Arc::new(Mutex::new(Vec::new()));
		let sink = events.clone();
		tokio::spawn(async move {
			let mut lines = BufReader::new(stdout).lines();
			while let Ok(Some(line)) = lines.next_line().await {
				eprintln!("[peer] {line}");
				sink.lock().unwrap().push(line);
			}
		});
		PeerProcess { child, events }
	}

	pub fn count(&self, event: &str) -> usize {
		self.events.lock().unwrap().iter().filter(|e| e.trim() == event).count()
	}

	pub async fn wait_for(&self, event: &str, count: usize) {
		tokio::time::timeout(TIMEOUT, async {
			while self.count(event) < count {
				tokio::time::sleep(Duration::from_millis(20)).await;
			}
		})
		.await
		.unwrap_or_else(|_| panic!("peer did not report {event} x{count}"));
	}

	pub async fn kill(&mut self) {
		let _ = self.child.kill().await;
	}
}

pub async fn wait_until(condition: impl Fn() -> bool) {
	tokio::time::timeout(TIMEOUT, async {
		while !condition() {
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
	})
	.await
	.expect("condition not met in time");
}

pub fn free_port() -> u16 {
	std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

pub fn rust_client(url: &str) -> VcmpClient {
	let client = VcmpClient::builder(url)
		.reconnect(Backoff::fixed(Duration::from_millis(300)))
		.initial_heartbeat_timeout(Duration::from_secs(10))
		.build();
	register(client.handlers());
	client
}

pub async fn rust_server(host: ServerHost, port: u16, heartbeat: Duration) -> (HostHandle, Endpoint) {
	let server = VcmpServer::builder().heartbeat_interval(heartbeat).build();
	let endpoint = server.endpoint("/peer/{name}");
	register(endpoint.handlers());
	let handle = host.bind(server, port).await;
	(handle, endpoint)
}

/// The scenarios where *we* send and the peer answers.
pub async fn we_send(session: &Session) {
	assert_eq!(session.send(&Echo { payload: "hello".into() }).await.unwrap(), "hello");
	assert_eq!(session.send(&Void {}).await.unwrap(), Value::Null);

	for status in [422, 408, 503, 504] {
		let error =
			session.send(&Fail { status, title: "Peer failure".into(), detail: "nope".into() }).await.unwrap_err();
		assert_eq!(error.status(), status);
		assert_eq!(error.title(), "Peer failure");
		assert_eq!(error.detail(), Some("nope"));
		assert!(!error.is_transport());
	}

	let error = session.send(&Unknown {}).await.unwrap_err();
	assert_eq!(error.status(), 500, "{error}");

	// Malformed JSON: the peer must NAK, but the *status* differs between implementations —
	// vcmp-js (and this crate) report 400 "Invalid message", vcmp-spring reports 500 "Message
	// handling failed" (its Jackson parse error is caught generically). Both are accepted here;
	// this crate's own 400 is pinned by the loopback tests and the js peer_sends "malformed".
	let error = session.send_payload("{oops".into()).await.unwrap_err();
	assert!(error.status() == 400 || error.status() == 500, "expected 4xx/5xx, got {error}");

	for size in [100 * 1024, 1024 * 1024] {
		let payload = "x".repeat(size);
		assert_eq!(session.send_as::<_, String>(&Echo { payload: payload.clone() }).await.unwrap(), payload);
	}

	let sends: Vec<_> = (0..100)
		.map(|i| {
			let session = session.clone();
			tokio::spawn(async move { session.send_as::<_, String>(&Echo { payload: i.to_string() }).await })
		})
		.collect();
	for (i, send) in sends.into_iter().enumerate() {
		assert_eq!(send.await.unwrap().unwrap(), i.to_string());
	}
}

/// The scenarios where the *peer* sends (on our request) and we answer.
pub async fn peer_sends(session: &Session) {
	let run = |scenario: &str, args: Value| {
		let run = Run { scenario: scenario.into(), args };
		async move { session.send_as::<_, Outcome>(&run).await.unwrap() }
	};

	let outcome = run("echo", json!({"payload": "hello"})).await;
	assert!(outcome.ok, "{outcome:?}");
	assert_eq!(outcome.result, "hello");

	let outcome = run("void", json!({})).await;
	assert!(outcome.ok, "{outcome:?}");
	assert_eq!(outcome.result, Value::Null);

	let outcome = run("fail", json!({"status": 422, "title": "Unprocessable", "detail": "nope"})).await;
	assert!(!outcome.ok, "{outcome:?}");
	let error = outcome.error.unwrap();
	assert_eq!((error.status, error.title.as_str(), error.detail.as_deref()), (422, "Unprocessable", Some("nope")));

	let outcome = run("unknown", json!({})).await;
	assert!(!outcome.ok, "{outcome:?}");
	assert_eq!(outcome.error.unwrap().status, 500);

	for size in [100 * 1024, 1024 * 1024] {
		let outcome = run("big", json!({"size": size})).await;
		assert!(outcome.ok, "{outcome:?}");
		assert_eq!(outcome.result, size);
	}

	let outcome = run("concurrent", json!({"count": 100})).await;
	assert!(outcome.ok, "{outcome:?}");
	assert_eq!(outcome.result, 100);
}
