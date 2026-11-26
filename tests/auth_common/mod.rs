//! Shared helpers of the loopback authentication tests: a raw frame-level peer and a test
//! authenticator.
#![allow(dead_code)]

use futures_channel::mpsc;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::timeout;
use vcmp::{
	Authentication, Authenticator, CloseReason, ConnectInfo, Frame, HandlerMap, Session, SessionLimits, SessionOptions,
	VcmpError, VcmpMessage,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Ping {}

impl VcmpMessage for Ping {
	const TYPE: &'static str = "test:Ping";
}

pub const PING: &str = r#"{"@type":"test:Ping"}"#;
pub const ID: &str = "abcdefghijkl";
pub const ID2: &str = "mnopqrstuvwx";

pub struct Peer {
	pub tx: mpsc::UnboundedSender<String>,
	pub rx: mpsc::UnboundedReceiver<String>,
}

impl Peer {
	pub fn inject(&self, raw: &str) {
		self.tx.unbounded_send(raw.to_owned()).unwrap();
	}

	pub async fn next_frame(&mut self) -> Frame {
		let raw = timeout(Duration::from_secs(2), self.rx.next())
			.await
			.expect("timed out waiting for a frame")
			.expect("transport closed");
		Frame::parse(&raw).unwrap()
	}

	/// Waits until the session closed its outgoing half; returns the frames it sent before.
	pub async fn closed(&mut self) -> Vec<Frame> {
		let mut frames = Vec::new();
		timeout(Duration::from_secs(2), async {
			while let Some(raw) = self.rx.next().await {
				frames.push(Frame::parse(&raw).unwrap());
			}
		})
		.await
		.expect("the session did not close");
		frames
	}

	/// The frames sent so far, without waiting.
	pub fn sent(&mut self) -> Vec<Frame> {
		let mut frames = Vec::new();
		while let Ok(raw) = self.rx.try_recv() {
			frames.push(Frame::parse(&raw).unwrap());
		}
		frames
	}
}

/// Principals are the token up to `:`, so `alice:2` is another credential of the same principal.
/// Tokens: `bad` → 401, `forbidden` → 403, `slow` → never answers, `panic` → panics (`sync-panic`: before
/// returning the future); `ttl` (ms) sets the expiry.
pub fn decide(credential: &Value) -> Result<Authentication<String>, VcmpError> {
	let token = credential["token"].as_str().unwrap_or_default();
	match token {
		"" | "bad" => Err(VcmpError::new(401, "Unauthorized")),
		"forbidden" => Err(VcmpError::new(403, "Forbidden")),
		token => {
			let principal = token.split(':').next().unwrap_or_default().to_owned();
			let mut authentication = Authentication::new(principal.clone()).ack_member("user", principal);
			if let Some(ttl) = credential["ttl"].as_u64() {
				authentication = authentication.expires_in(Duration::from_millis(ttl));
			}
			Ok(authentication)
		}
	}
}

pub fn authenticator() -> Authenticator {
	Authenticator::new(|_session: Session, credential: Value| {
		// A closure that panics before it returns its future.
		assert!(credential["token"] != "sync-panic", "synchronous authenticator bug");
		authenticate_async(credential)
	})
}

async fn authenticate_async(credential: Value) -> Result<Authentication<String>, VcmpError> {
	{
		if credential["token"] == "slow" {
			std::future::pending::<()>().await;
		}
		if credential["token"] == "panic" {
			panic!("authenticator bug");
		}
		decide(&credential)
	}
}

/// The handshake credential is the `token` query parameter.
pub fn with_handshake() -> Authenticator {
	Authenticator::with_handshake(
		|_session: Session, credential: Value| async move { decide(&credential) },
		|info: ConnectInfo| async move {
			let token = info.query.as_deref().and_then(|query| query.split('&').find_map(|p| p.strip_prefix("token=")));
			let Some(token) = token else {
				return Ok(None);
			};
			// Old clients send their first MSG at once; it must wait for this decision.
			tokio::time::sleep(Duration::from_millis(50)).await;
			decide(&json!({ "token": token })).map(Some)
		},
	)
}

pub struct Fixture {
	pub session: Session,
	pub peer: Peer,
	pub pings: Arc<AtomicUsize>,
}

pub fn spawn(authenticator: Option<Authenticator>, query: Option<&str>) -> Fixture {
	spawn_with(authenticator, query, SessionLimits::default())
}

pub fn spawn_with(authenticator: Option<Authenticator>, query: Option<&str>, limits: SessionLimits) -> Fixture {
	let pings = Arc::new(AtomicUsize::new(0));
	let handlers = HandlerMap::new();
	let counter = pings.clone();
	handlers.on(move |_: Ping, _| {
		counter.fetch_add(1, Ordering::SeqCst);
		async { Ok(()) }
	});
	let options = SessionOptions {
		handlers: Arc::new(handlers),
		connect_info: Some(ConnectInfo { path: "/test".into(), query: query.map(str::to_owned), ..Default::default() }),
		authenticator,
		limits,
		..Default::default()
	};
	let (to_session_tx, to_session_rx) = mpsc::unbounded::<String>();
	let (from_session_tx, from_session_rx) = mpsc::unbounded::<String>();
	let sink = from_session_tx.sink_map_err(|error| error.to_string());
	let session = Session::spawn(options, to_session_rx, sink);
	Fixture { session, peer: Peer { tx: to_session_tx, rx: from_session_rx }, pings }
}

pub async fn settle() {
	for _ in 0..20 {
		tokio::task::yield_now().await;
	}
}

pub fn aut(id: &str, credential: Value) -> String {
	format!("AUT{id}{credential}")
}

pub fn policy(reason: &'static str) -> Option<CloseReason> {
	Some(CloseReason { code: 1008, reason: reason.into() })
}

pub fn nak_status(frame: &Frame, expected_id: &str) -> u16 {
	let Frame::Nak { id, payload: Some(payload) } = frame else { panic!("expected a NAK, got {frame:?}") };
	assert_eq!(id, expected_id);
	serde_json::from_str::<Value>(payload).unwrap()["status"].as_u64().unwrap() as u16
}

pub fn ack_payload(frame: &Frame, expected_id: &str) -> Value {
	let Frame::Ack { id, payload } = frame else { panic!("expected an ACK, got {frame:?}") };
	assert_eq!(id, expected_id);
	payload.as_deref().map_or(Value::Null, |payload| serde_json::from_str(payload).unwrap())
}

/// Authenticates the fixture's session as `token`.
pub async fn authenticate(fixture: &mut Fixture, credential: Value) -> Value {
	fixture.peer.inject(&aut(ID, credential));
	let ack = fixture.peer.next_frame().await;
	ack_payload(&ack, ID)
}
