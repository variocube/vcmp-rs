//! Credentials are never logged: every authentication path runs under a global TRACE subscriber
//! whose output is searched for the credential. A test binary of its own, because a global
//! subscriber is needed to see every callsite (a thread-local one misses those cached as
//! uninterested by tests on other threads).

mod auth_common;

use auth_common::*;
use serde_json::json;
use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::time::advance;
use vcmp::Frame;

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl Write for Buffer {
	fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
		self.0.lock().unwrap().extend_from_slice(bytes);
		Ok(bytes.len())
	}
	fn flush(&mut self) -> std::io::Result<()> {
		Ok(())
	}
}

fn logs() -> &'static Buffer {
	static LOGS: OnceLock<Buffer> = OnceLock::new();
	LOGS.get_or_init(|| {
		let buffer = Buffer::default();
		let writer = buffer.clone();
		tracing_subscriber::fmt()
			.with_max_level(tracing::Level::TRACE)
			.with_writer(move || writer.clone())
			.with_ansi(false)
			.init();
		buffer
	})
}

#[tokio::test(start_paused = true)]
async fn credentials_are_never_logged() {
	const SECRET: &str = "s3cr3t-credential";
	let logs = logs();
	// Accepted, refreshed, rejected on refresh, principal change.
	let mut f = spawn(Some(authenticator()), None);
	authenticate(&mut f, json!({"token": format!("alice:{SECRET}"), "ttl": 1_000})).await;
	f.peer.inject(&aut(ID2, json!({"token": "bad", "secret": SECRET})));
	f.peer.next_frame().await;
	f.peer.inject(&aut(ID, json!({"token": format!("bob:{SECRET}")})));
	f.peer.closed().await;
	// Rejected first AUT, malformed credential, an invalid handshake credential.
	let mut f = spawn(Some(authenticator()), None);
	f.peer.inject(&aut(ID, json!({"token": "bad", "secret": SECRET})));
	f.peer.closed().await;
	let mut f = spawn(Some(authenticator()), None);
	f.peer.inject(&format!("AUT{ID}{SECRET}"));
	f.peer.closed().await;
	let mut f = spawn(Some(with_handshake()), Some(&format!("token=bad&secret={SECRET}")));
	f.peer.closed().await;
	// Without an authenticator, and the client side of a session.
	let mut f = spawn(None, None);
	f.peer.inject(&aut(ID, json!({"secret": SECRET})));
	f.peer.next_frame().await;
	let session = f.session.clone();
	let send =
		tokio::spawn(
			async move { session.send_authentication(&json!({"secret": SECRET}), Duration::from_secs(1)).await },
		);
	f.peer.next_frame().await;
	advance(Duration::from_secs(1)).await;
	send.await.unwrap().unwrap_err();
	tracing::info!(frame = %Frame::Authenticate { id: ID.into(), payload: SECRET.into() }, "display");
	tracing::info!(frame = ?Frame::Authenticate { id: ID.into(), payload: SECRET.into() }, "debug");

	let logs = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
	assert!(logs.contains("session authenticated"), "the capture works: {logs}");
	assert!(logs.contains("received frame"), "frame-level trace logging is captured: {logs}");
	assert!(logs.contains("AUT#abcdefghijkl:[redacted]"), "{logs}");
	assert!(!logs.contains(SECRET), "{logs}");
}
