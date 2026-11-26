//! The session authentication (`AUT`) contract (variocube/vcmp-spring#34, cases 1–11), run against the real
//! peers in both directions: raw sockets and the Rust client against a peer server, and peer clients against
//! the Rust server.
//!
//! The contract credential scheme is shared by every peer (`contract/js/peer.js`, `AuthServerPeer.java`):
//! the `AUT` credential is `{"token": "<principal>[:<n>]", "ttl": <ms>?}`; the principal is the token up to
//! `:`; `bad` (or none) is rejected with 401; `ttl` sets the expiry (≤ 0: already over); the `ACK` carries
//! `user`. The handshake credential (transition mode) is the query parameter `token`. Peer servers time out
//! authentication after 1.5 s. A server closes with 1008, a client with 4008; close reasons are compared only
//! for the client's close (`peer_client_closes_with_4008`).

use super::*;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use vcmp::{Authentication, AuthenticationCounts, Authenticator, ConnectInfo};

/// The peer's auth timeout, see the module docs.
pub const AUTH_TIMEOUT: Duration = Duration::from_millis(1_500);
/// The lifetime used by the expiry scenarios.
/// Long enough that a peer starved by parallel JVM start-ups in CI still refreshes in time (default lead 600 ms).
pub const TTL: u64 = 3_000;

/// Where a peer server serves the contract endpoints.
#[derive(Clone, Debug)]
pub struct AuthTarget {
	/// The endpoint that requires `AUT`, without query.
	pub auth: String,
	/// An endpoint without the opt-in.
	pub open: String,
}

// --- raw sockets ---------------------------------------------------------------------------------------------

pub struct Raw {
	ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
	next_id: u32,
}

/// What a raw socket received next (heartbeats are answered and skipped).
#[derive(Debug, PartialEq)]
pub enum Received {
	Ack(String, Value),
	Nak(String, u16),
	Msg(String),
	Closed(Option<u16>),
}

impl Raw {
	pub async fn connect(url: &str) -> Raw {
		let (ws, _) = tokio::time::timeout(TIMEOUT, tokio_tungstenite::connect_async(url))
			.await
			.expect("connect timed out")
			.unwrap_or_else(|error| panic!("could not connect to {url}: {error}"));
		Raw { ws, next_id: 0 }
	}

	fn id(&mut self) -> String {
		self.next_id += 1;
		format!("raw{:09}", self.next_id)
	}

	pub async fn send(&mut self, text: String) {
		self.ws.send(Message::text(text)).await.unwrap();
	}

	/// Sends an `AUT` with the credential; returns its id.
	pub async fn aut(&mut self, credential: Value) -> String {
		let id = self.id();
		self.send(format!("AUT{id}{credential}")).await;
		id
	}

	/// Sends a WebSocket message as is (e.g. garbage or binary).
	pub async fn send_message(&mut self, message: Message) {
		self.ws.send(message).await.unwrap();
	}

	/// Sends a `contract:Echo`; returns its id.
	pub async fn echo(&mut self, payload: &str) -> String {
		let id = self.id();
		self.send(format!(r#"MSG{id}{{"@type":"contract:Echo","payload":"{payload}"}}"#)).await;
		id
	}

	pub async fn next(&mut self) -> Received {
		loop {
			let message = tokio::time::timeout(TIMEOUT, self.ws.next()).await.expect("nothing received in time");
			let text = match message {
				Some(Ok(Message::Text(text))) => text.to_string(),
				Some(Ok(Message::Close(frame))) => return Received::Closed(frame.map(|frame| u16::from(frame.code))),
				Some(Ok(_)) => continue,
				Some(Err(_)) | None => return Received::Closed(None),
			};
			if let Some(interval) = text.strip_prefix("HBT") {
				// Echo at once; the peer only checks that one arrives within 2 × interval.
				let _ = self.ws.send(Message::text(format!("HBT{interval}"))).await;
				continue;
			}
			let (kind, id, payload) = (&text[..3], text[3..15].to_owned(), &text[15..]);
			let parsed = || serde_json::from_str::<Value>(payload).unwrap_or(Value::Null);
			return match kind {
				"ACK" => Received::Ack(id, if payload.is_empty() { Value::Null } else { parsed() }),
				"NAK" => Received::Nak(id, parsed()["status"].as_u64().unwrap_or_default() as u16),
				"MSG" => Received::Msg(text),
				other => panic!("unexpected frame type {other}"),
			};
		}
	}

	pub async fn expect_ack(&mut self, id: &str) -> Value {
		match self.next().await {
			Received::Ack(ack_id, payload) if ack_id == id => payload,
			other => panic!("expected ACK {id}, got {other:?}"),
		}
	}

	pub async fn expect_nak(&mut self, id: &str) -> u16 {
		match self.next().await {
			Received::Nak(nak_id, status) if nak_id == id => status,
			other => panic!("expected NAK {id}, got {other:?}"),
		}
	}

	/// Expects the server to close the session with 1008; returns how long that took.
	pub async fn expect_policy_close(&mut self) -> Duration {
		let started = Instant::now();
		match self.next().await {
			Received::Closed(Some(1008)) => started.elapsed(),
			other => panic!("expected close 1008, got {other:?}"),
		}
	}
}

/// Cases 2–10 with raw sockets against a peer server (cases 1 and 11 need a real client:
/// [`rust_client_against_peer_server`]).
pub async fn raw_against_peer_server(target: &AuthTarget) {
	let auth = target.auth.as_str();

	// 1 (wire level): valid AUT → ACK with the endpoint's members, then messages flow.
	let mut raw = Raw::connect(auth).await;
	let id = raw.aut(json!({"token": "alice"})).await;
	assert_eq!(raw.expect_ack(&id).await, json!({"user": "alice"}), "case 1: ACK without expiresIn");
	let id = raw.echo("hi").await;
	assert_eq!(raw.expect_ack(&id).await, "hi", "case 1: MSG after the ACK");

	// 2: invalid AUT → NAK 401 + close 1008.
	let mut raw = Raw::connect(auth).await;
	let id = raw.aut(json!({"token": "bad"})).await;
	assert_eq!(raw.expect_nak(&id).await, 401, "case 2");
	raw.expect_policy_close().await;

	// 2 (expiry already over): an expiry that is not in the future → NAK 401 + close 1008, never ACK + close.
	let mut raw = Raw::connect(auth).await;
	let id = raw.aut(json!({"token": "alice", "ttl": -1_000})).await;
	assert_eq!(raw.expect_nak(&id).await, 401, "case 2: expired credential");
	raw.expect_policy_close().await;

	// 3: MSG before AUT → close 1008; so do an unparseable frame and a binary message.
	let mut raw = Raw::connect(auth).await;
	raw.echo("early").await;
	raw.expect_policy_close().await;
	let mut raw = Raw::connect(auth).await;
	raw.send_message(Message::text("garbage")).await;
	raw.expect_policy_close().await;
	let mut raw = Raw::connect(auth).await;
	raw.send_message(Message::binary(vec![1, 2, 3])).await;
	raw.expect_policy_close().await;

	// 4: no AUT within the timeout → close 1008.
	let mut raw = Raw::connect(auth).await;
	let elapsed = raw.expect_policy_close().await;
	assert!(elapsed >= AUTH_TIMEOUT - Duration::from_millis(300), "case 4: closed after {elapsed:?}");

	// 5: AUT on an endpoint without auth → NAK 400, the session stays.
	let mut raw = Raw::connect(&target.open).await;
	let id = raw.aut(json!({"token": "alice"})).await;
	assert_eq!(raw.expect_nak(&id).await, 400, "case 5");
	let id = raw.echo("still here").await;
	assert_eq!(raw.expect_ack(&id).await, "still here", "case 5: the session stays");

	// 6: expiry → close 1008 when expiresIn runs out.
	let mut raw = Raw::connect(auth).await;
	let id = raw.aut(json!({"token": "alice", "ttl": TTL})).await;
	let expires_in = raw.expect_ack(&id).await["expiresIn"].as_u64().expect("case 6: expiresIn");
	assert!((TTL - 500..=TTL).contains(&expires_in), "case 6: expiresIn {expires_in}");
	let elapsed = raw.expect_policy_close().await;
	assert!(elapsed >= Duration::from_millis(TTL - 500), "case 6: closed after {elapsed:?}");

	// 7: refresh with the same principal → ACK, expiry moved, survives the old expiry.
	let mut raw = Raw::connect(auth).await;
	let id = raw.aut(json!({"token": "alice", "ttl": TTL})).await;
	raw.expect_ack(&id).await;
	let started = Instant::now();
	tokio::time::sleep(Duration::from_millis(TTL / 2)).await;
	let id = raw.aut(json!({"token": "alice:2", "ttl": TTL})).await;
	assert_eq!(raw.expect_ack(&id).await["user"], "alice", "case 7");
	tokio::time::sleep(Duration::from_millis(TTL + 500).saturating_sub(started.elapsed())).await;
	let id = raw.echo("after the old expiry").await;
	assert_eq!(raw.expect_ack(&id).await, "after the old expiry", "case 7: survives the old expiry");
	raw.expect_policy_close().await;
	assert!(started.elapsed() >= Duration::from_millis(TTL * 3 / 2 - 500), "case 7: the new expiry applies");

	// 8: refresh with another principal → NAK 403 + close.
	let mut raw = Raw::connect(auth).await;
	let id = raw.aut(json!({"token": "alice"})).await;
	raw.expect_ack(&id).await;
	let id = raw.aut(json!({"token": "bob"})).await;
	assert_eq!(raw.expect_nak(&id).await, 403, "case 8");
	raw.expect_policy_close().await;

	// 9: failed refresh → NAK 401, the session stays open until the old expiry.
	let mut raw = Raw::connect(auth).await;
	let id = raw.aut(json!({"token": "alice", "ttl": TTL})).await;
	raw.expect_ack(&id).await;
	let started = Instant::now();
	let id = raw.aut(json!({"token": "bad"})).await;
	assert_eq!(raw.expect_nak(&id).await, 401, "case 9");
	// Check order: an expired credential for another principal is a 401, not a principal change.
	let id = raw.aut(json!({"token": "bob", "ttl": -1_000})).await;
	assert_eq!(raw.expect_nak(&id).await, 401, "case 9: expired credential of another principal");
	let id = raw.echo("still open").await;
	assert_eq!(raw.expect_ack(&id).await, "still open", "case 9: open after the failed refresh");
	raw.expect_policy_close().await;
	assert!(started.elapsed() >= Duration::from_millis(TTL - 500), "case 9: until the old expiry");

	// 10: transition. A valid handshake credential authenticates without AUT (an old client sends at once) …
	let mut raw = Raw::connect(&format!("{auth}?token=carol")).await;
	let id = raw.echo("old client").await;
	assert_eq!(raw.expect_ack(&id).await, "old client", "case 10a");
	// … AUT on it is a re-authentication (same principal ACK, another one NAK 403 + close) …
	let id = raw.aut(json!({"token": "carol:2"})).await;
	assert_eq!(raw.expect_ack(&id).await["user"], "carol", "case 10d");
	let id = raw.aut(json!({"token": "dave"})).await;
	assert_eq!(raw.expect_nak(&id).await, 403, "case 10d: principal changed");
	raw.expect_policy_close().await;
	// … an invalid one closes, without falling back to AUT …
	let mut raw = Raw::connect(&format!("{auth}?token=bad")).await;
	raw.expect_policy_close().await;
	// … and without one, AUT is required (covered by cases 3 and 4) and works.
	let mut raw = Raw::connect(&format!("{auth}?other=1")).await;
	let id = raw.aut(json!({"token": "erin"})).await;
	assert_eq!(raw.expect_ack(&id).await, json!({"user": "erin"}), "case 10c");
}

// --- the Rust client against a peer server --------------------------------------------------------------------

/// A Rust client whose credential provider returns `credentials` in turn (the last one repeated).
pub fn auth_client(url: &str, credentials: Vec<Value>) -> (VcmpClient, Arc<Mutex<Vec<u16>>>, Arc<AtomicUsize>) {
	let calls = Arc::new(AtomicUsize::new(0));
	let provider_calls = calls.clone();
	let credentials = Arc::new(credentials);
	let client = VcmpClient::builder(url)
		.reconnect(Backoff::fixed(Duration::from_millis(300)))
		.initial_heartbeat_timeout(Duration::from_secs(10))
		.authenticate(move || {
			let call = provider_calls.fetch_add(1, Ordering::SeqCst);
			let credential = credentials[call.min(credentials.len() - 1)].clone();
			async move { Ok(credential) }
		})
		.build();
	register(client.handlers());
	let errors = Arc::new(Mutex::new(Vec::new()));
	let record = errors.clone();
	client.on_auth_error(move |error| {
		record.lock().unwrap().push(error.status());
		async {}
	});
	(client, errors, calls)
}

/// Cases 1, 2, 7, 8, 9 and 11 with the Rust client against a peer server.
pub async fn rust_client_against_peer_server(target: &AuthTarget) {
	// 1: ACK, then messages flow both ways.
	let (client, errors, _) = auth_client(&target.auth, vec![json!({"token": "alice"})]);
	client.start();
	tokio::time::timeout(TIMEOUT, client.wait_connected()).await.expect("case 1: not connected");
	let session = client.session().unwrap();
	assert_eq!(session.authentication(), Some(json!({"user": "alice"})), "case 1: the ACK payload");
	assert_eq!(session.send(&Echo { payload: "hello".into() }).await.unwrap(), "hello", "case 1: we send");
	let outcome = session
		.send_as::<_, Outcome>(&Run { scenario: "echo".into(), args: json!({"payload": "back"}) })
		.await
		.unwrap();
	assert!(outcome.ok && outcome.result == "back", "case 1: the peer sends: {outcome:?}");
	assert!(errors.lock().unwrap().is_empty());
	client.stop_and_wait().await;

	// 2: rejected → an authentication error (401), never connected.
	let (client, errors, calls) = auth_client(&target.auth, vec![json!({"token": "bad"})]);
	client.start();
	wait_until(|| errors.lock().unwrap().len() >= 2).await;
	assert!(errors.lock().unwrap().iter().all(|status| *status == 401), "case 2: {:?}", errors.lock().unwrap());
	assert!(calls.load(Ordering::SeqCst) >= 2, "case 2: a fresh credential per attempt");
	assert!(!client.is_connected());
	client.stop_and_wait().await;

	// The application triggers re-authentication (`reauthenticate`); the client schedules nothing.
	// 7: re-authentication with the same principal → ACK, expiry moved; one session survives several expiries.
	let (client, errors, calls) = auth_client(&target.auth, vec![json!({"token": "alice", "ttl": TTL})]);
	client.start();
	tokio::time::timeout(TIMEOUT, client.wait_connected()).await.expect("case 7: not connected");
	let session = client.session().unwrap();
	let expires_in = session.authentication().unwrap()["expiresIn"].as_u64();
	assert!(expires_in.is_some_and(|millis| millis > TTL - 500), "case 7: expiresIn passed to the app");
	for _ in 0..3 {
		tokio::time::sleep(Duration::from_millis(TTL / 2)).await;
		let ack = client.reauthenticate().await.expect("case 7: re-authenticated").expect("case 7: an ACK");
		assert_eq!(ack["user"], "alice", "case 7");
		assert!(ack["expiresIn"].as_u64().is_some_and(|millis| millis > TTL - 500), "case 7: {ack}");
	}
	assert!(session.is_open() && client.session() == Some(session.clone()), "case 7: survives the old expiry");
	assert_eq!(calls.load(Ordering::SeqCst), 4, "case 7: a fresh credential per re-authentication");
	assert!(errors.lock().unwrap().is_empty(), "case 7: {:?}", errors.lock().unwrap());
	assert_eq!(session.send(&Echo { payload: "alive".into() }).await.unwrap(), "alive");
	client.stop_and_wait().await;

	// 8: re-authentication as another principal → NAK 403, the server closes, the client reconnects as that one.
	let credentials = vec![json!({"token": "alice", "ttl": TTL}), json!({"token": "bob", "ttl": TTL * 10})];
	let (client, errors, _) = auth_client(&target.auth, credentials);
	client.start();
	tokio::time::timeout(TIMEOUT, client.wait_connected()).await.expect("case 8: not connected");
	let first = client.session().unwrap();
	assert_eq!(client.reauthenticate().await.unwrap_err().status(), 403, "case 8");
	tokio::time::timeout(TIMEOUT, first.closed()).await.expect("case 8: the server closes the session");
	assert_eq!(*errors.lock().unwrap(), vec![403], "case 8: reported");
	wait_until(|| client.session().is_some_and(|session| session != first)).await;
	let second = client.session().unwrap();
	assert_eq!(second.authentication().unwrap()["user"], "bob", "case 8: reconnected as the new principal");
	client.stop_and_wait().await;

	// 9: a failed re-authentication (401) keeps the session until the old expiry, no automatic retry.
	let credentials = vec![json!({"token": "alice", "ttl": TTL}), json!({"token": "bad"})];
	let (client, errors, calls) = auth_client(&target.auth, credentials);
	client.start();
	tokio::time::timeout(TIMEOUT, client.wait_connected()).await.expect("case 9: not connected");
	let session = client.session().unwrap();
	let started = Instant::now();
	assert_eq!(client.reauthenticate().await.unwrap_err().status(), 401, "case 9");
	assert!(session.is_open(), "case 9: open after a failed re-authentication");
	assert_eq!(session.send(&Echo { payload: "open".into() }).await.unwrap(), "open");
	// Until shortly before the old expiry: nothing retried on its own.
	tokio::time::sleep(Duration::from_millis(TTL - 800).saturating_sub(started.elapsed())).await;
	assert!(session.is_open(), "case 9: open until the old expiry");
	assert_eq!(*errors.lock().unwrap(), vec![401], "case 9: reported once, not retried");
	assert_eq!(calls.load(Ordering::SeqCst), 2, "case 9: no automatic retry");
	tokio::time::timeout(TIMEOUT, session.closed()).await.expect("case 9: closed at the expiry");
	assert!(started.elapsed() >= Duration::from_millis(TTL - 500), "case 9: closed after {:?}", started.elapsed());
	client.stop_and_wait().await;

	// 11: an endpoint without the opt-in answers NAK 400: an authentication error, never connected.
	let (client, errors, _) = auth_client(&target.open, vec![json!({"token": "alice"})]);
	client.start();
	wait_until(|| !errors.lock().unwrap().is_empty()).await;
	assert_eq!(errors.lock().unwrap()[0], 400, "case 11");
	assert!(!client.is_connected());
	client.stop_and_wait().await;
}

// --- peer clients against raw Rust servers: the client's close code --------------------------------------------

/// Accepts one connection, answers its first frame (which must be an `AUT`) with `answer(id)` (or not at
/// all), and returns the close frame the client then sends: (code, reason).
async fn client_close_after(spawn: SpawnClient<'_>, answer: Option<fn(&str) -> String>) -> (u16, String) {
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
	let url = format!("ws://{}/auth/raw", listener.local_addr().unwrap());
	let mut peer = spawn(url, Some(json!([{"token": "alice"}])), true).await;
	let (stream, _) = tokio::time::timeout(TIMEOUT, listener.accept()).await.expect("no connection").unwrap();
	let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
	ws.send(Message::text("HBT20000")).await.unwrap();
	let close = tokio::time::timeout(TIMEOUT, async {
		while let Some(message) = ws.next().await {
			match message {
				Ok(Message::Text(text)) => {
					assert!(text.starts_with("AUT") || text.starts_with("HBT"), "nothing but AUT: {text}");
					if let (true, Some(answer)) = (text.starts_with("AUT"), answer) {
						ws.send(Message::text(answer(&text[3..15]))).await.unwrap();
					}
				}
				Ok(Message::Close(frame)) => {
					return frame.map(|frame| (u16::from(frame.code), frame.reason.to_string()));
				}
				Ok(_) => {}
				Err(_) => return None,
			}
		}
		None
	})
	.await
	.expect("the client did not close");
	assert_eq!(peer.count("CONNECTED") + peer.count("HELLO_OK") + peer.count("HELLO_FAILED"), 0, "never connected");
	peer.kill().await;
	close.expect("the client closes with a close frame")
}

/// A client closes for authentication reasons with 4008 (browsers can't close with 1008), the server's
/// reasons: a `NAK` 401 or 400 (case 11: the server keeps the session open) to the first `AUT` →
/// `authentication failed`; no answer within the answer timeout (peers: 1 s) → `authentication timeout`.
pub async fn peer_client_closes_with_4008(spawn: SpawnClient<'_>) {
	let nak_401: fn(&str) -> String = |id| format!(r#"NAK{id}{{"status":401,"title":"Unauthorized"}}"#);
	let close = client_close_after(spawn, Some(nak_401)).await;
	assert_eq!(close, (4008, "authentication failed".to_owned()), "case 2: the client's close after NAK 401");
	let nak_400: fn(&str) -> String = |id| format!(r#"NAK{id}{{"status":400,"title":"Authentication not supported"}}"#);
	let close = client_close_after(spawn, Some(nak_400)).await;
	assert_eq!(close, (4008, "authentication failed".to_owned()), "case 11: the client's close after NAK 400");
	let close = client_close_after(spawn, None).await;
	assert_eq!(close, (4008, "authentication timeout".to_owned()), "unanswered AUT: the client's close");
}

// --- peer clients against the Rust server ---------------------------------------------------------------------

/// The Rust server: `/peer/{name}` without the opt-in and `/auth/{name}` with the contract authenticator.
pub struct RustAuthServer {
	pub handle: HostHandle,
	pub auth: Endpoint,
	pub open: Endpoint,
	/// Principals of the sessions the `/auth` connect hook saw, in order.
	pub connected: Arc<Mutex<Vec<String>>>,
	/// Messages the `/peer` endpoint received.
	pub open_messages: Arc<AtomicUsize>,
}

fn decide(token: &str, ttl: Option<i64>) -> Result<Authentication<String>, VcmpError> {
	if token.is_empty() || token == "bad" {
		return Err(VcmpError::new(401, "Unauthorized"));
	}
	let principal = token.split(':').next().unwrap_or_default().to_owned();
	let authentication = Authentication::new(principal.clone()).ack_member("user", principal);
	Ok(match ttl {
		Some(ttl) if ttl > 0 => authentication.expires_in(Duration::from_millis(ttl.unsigned_abs())),
		// A ttl ≤ 0: an expiry that is not in the future.
		Some(ttl) => {
			authentication.expires_at(std::time::SystemTime::now() - Duration::from_millis(ttl.unsigned_abs()))
		}
		None => authentication,
	})
}

pub async fn rust_auth_server(host: ServerHost, port: u16) -> RustAuthServer {
	let server = VcmpServer::builder().heartbeat_interval(Duration::from_millis(500)).build();
	let open = server.endpoint("/peer/{name}");
	register(open.handlers());
	let open_messages = Arc::new(AtomicUsize::new(0));
	let count = open_messages.clone();
	open.on(move |echo: Echo, _| {
		count.fetch_add(1, Ordering::SeqCst);
		async move { Ok(echo.payload) }
	});
	let auth = server.endpoint("/auth/{name}");
	register(auth.handlers());
	auth.authenticate(
		Authenticator::with_handshake(
			|_session: Session, credential: Value| async move {
				decide(credential["token"].as_str().unwrap_or_default(), credential["ttl"].as_i64())
			},
			|info: ConnectInfo| async move {
				let token = info.query.as_deref().and_then(|query| {
					query.split('&').find_map(|parameter| parameter.strip_prefix("token=")).map(str::to_owned)
				});
				match token {
					Some(token) => decide(&token, None).map(Some),
					None => Ok(None),
				}
			},
		)
		.timeout(AUTH_TIMEOUT),
	);
	let connected = Arc::new(Mutex::new(Vec::new()));
	let record = connected.clone();
	auth.on_connected(move |session| {
		record.lock().unwrap().push(session.principal::<String>().map(|p| p.to_string()).unwrap_or_default());
		async {}
	});
	let handle = host.bind(server, port).await;
	RustAuthServer { handle, auth, open, connected, open_messages }
}

/// Spawns a peer client: `credentials` `None` is an old client without `AUT`.
pub type SpawnClient<'a> =
	&'a dyn Fn(String, Option<Value>, bool) -> futures_util::future::BoxFuture<'static, PeerProcess>;

/// Cases 1–4 and 6–11 with a peer client against the Rust server.
pub async fn peer_client_against_rust_server(host: ServerHost, spawn: SpawnClient<'_>) {
	let server = rust_auth_server(host, 0).await;
	let port = server.handle.local_addr().port();
	let auth = format!("ws://127.0.0.1:{port}/auth/peer");
	let open = format!("ws://127.0.0.1:{port}/peer/peer");
	let principals = |server: &RustAuthServer| server.connected.lock().unwrap().clone();

	// 1: ACK, then messages flow both ways.
	let mut peer = spawn(auth.clone(), Some(json!([{"token": "alice"}])), false).await;
	peer.wait_for("CONNECTED", 1).await;
	wait_until(|| server.auth.session_count() == 1).await;
	let session = server.auth.sessions().pop().unwrap();
	assert_eq!(session.principal::<String>().as_deref().map(String::as_str), Some("alice"), "case 1");
	assert_eq!(server.auth.authentication_counts(), AuthenticationCounts { handshake: 0, aut: 1 });
	we_send(&session).await;
	peer_sends(&session).await;
	peer.kill().await;
	wait_until(|| server.auth.session_count() == 0).await;

	// 2: rejected → the peer reports 401, the session never connects.
	let mut peer = spawn(auth.clone(), Some(json!([{"token": "bad"}])), true).await;
	peer.wait_for("AUTH_ERROR 401", 2).await;
	assert_eq!(peer.count("CONNECTED"), 0, "case 2");
	assert_eq!(peer.count("HELLO_OK") + peer.count("HELLO_FAILED"), 0, "case 2: never sends");
	assert_eq!(principals(&server), vec!["alice".to_owned()], "case 2: no connect hook");
	peer.kill().await;

	// 3: an old client that sends at once, without AUT → closed (1008), never connected.
	let mut peer = spawn(auth.clone(), None, true).await;
	peer.wait_for("DISCONNECTED", 1).await;
	assert_eq!(peer.count("HELLO_OK"), 0, "case 3: the MSG is never handled");
	peer.kill().await;

	// 4: an old client that sends nothing → closed after the auth timeout.
	let started = Instant::now();
	let mut peer = spawn(auth.clone(), None, false).await;
	peer.wait_for("DISCONNECTED", 1).await;
	assert!(started.elapsed() >= AUTH_TIMEOUT - Duration::from_millis(300), "case 4: {:?}", started.elapsed());
	peer.kill().await;
	assert_eq!(principals(&server), vec!["alice".to_owned()], "cases 3, 4: no connect hook");

	// The peer's application triggers re-authentication (stdin `REAUTH` → `reauthenticate()`); no client
	// schedules a refresh on its own.
	// 7 (+ 6): re-authentication with the same principal moves the expiry; one session survives several expiries,
	// and without it the server closes the session at its expiry.
	let credentials = json!([{"token": "frank:1", "ttl": TTL}, {"token": "frank:2", "ttl": TTL}]);
	let mut peer = spawn(auth.clone(), Some(credentials), false).await;
	peer.wait_for("CONNECTED", 1).await;
	wait_until(|| server.auth.session_count() == 1).await;
	let session = server.auth.sessions().pop().unwrap();
	for round in 1..=3 {
		tokio::time::sleep(Duration::from_millis(TTL / 2)).await;
		peer.command("REAUTH").await;
		peer.wait_for("REAUTH_OK", round).await;
	}
	assert!(session.is_open(), "case 7: survives the old expiry ({:?})", session.close_reason());
	assert_eq!(peer.count("DISCONNECTED") + peer.count("AUTH_ERROR 401"), 0, "case 7");
	let stopped = Instant::now();
	tokio::time::timeout(TIMEOUT, session.closed()).await.expect("case 6: closed at the expiry");
	assert_eq!(session.close_reason().unwrap().reason, "authentication expired", "case 6");
	assert!(stopped.elapsed() >= Duration::from_millis(TTL / 2 - 500), "case 6: {:?}", stopped.elapsed());
	peer.kill().await;
	wait_until(|| server.auth.session_count() == 0).await;

	// 8: re-authentication as another principal → NAK 403 + close; the peer reconnects as that principal.
	let credentials = json!([{"token": "gina", "ttl": TTL}, {"token": "hank"}]);
	let mut peer = spawn(auth.clone(), Some(credentials), false).await;
	peer.wait_for("CONNECTED", 1).await;
	wait_until(|| server.auth.session_count() == 1).await;
	let session = server.auth.sessions().pop().unwrap();
	peer.command("REAUTH").await;
	tokio::time::timeout(TIMEOUT, session.closed()).await.expect("case 8: closed");
	assert_eq!(session.close_reason().map(|reason| reason.code), Some(1008), "case 8");
	assert_eq!(session.close_reason().unwrap().reason, "principal changed", "case 8");
	peer.wait_for("REAUTH_ERROR 403", 1).await;
	peer.wait_for("AUTH_ERROR 403", 1).await;
	wait_until(|| principals(&server).last().map(String::as_str) == Some("hank")).await;
	peer.kill().await;
	wait_until(|| server.auth.session_count() == 0).await;

	// 9: a failed re-authentication (401) keeps the session until the old expiry, no automatic retry.
	let credentials = json!([{"token": "ivy", "ttl": TTL}, {"token": "bad"}]);
	let mut peer = spawn(auth.clone(), Some(credentials), false).await;
	peer.wait_for("CONNECTED", 1).await;
	wait_until(|| server.auth.session_count() == 1).await;
	let session = server.auth.sessions().pop().unwrap();
	let started = Instant::now();
	peer.command("REAUTH").await;
	peer.wait_for("REAUTH_ERROR 401", 1).await;
	peer.wait_for("AUTH_ERROR 401", 1).await;
	assert!(session.is_open(), "case 9: open after a failed re-authentication");
	// Until shortly before the old expiry: nothing retried on its own (after it, the peer reconnects with `bad`).
	tokio::time::sleep(Duration::from_millis(TTL - 800).saturating_sub(started.elapsed())).await;
	assert!(session.is_open(), "case 9: open until the old expiry");
	assert_eq!(peer.count("AUTH_ERROR 401"), 1, "case 9: not retried");
	tokio::time::timeout(TIMEOUT, session.closed()).await.expect("case 9: closed at the expiry");
	assert_eq!(session.close_reason().unwrap().reason, "authentication expired", "case 9");
	assert!(started.elapsed() >= Duration::from_millis(TTL - 500), "case 9: {:?}", started.elapsed());
	peer.kill().await;

	// Without an authenticated session (here: nothing to connect to), `reauthenticate()` has no outcome.
	// Port 1: nothing listens there and it is refused on every OS (a freed port could be taken by a parallel test).
	let mut peer = spawn("ws://127.0.0.1:1/auth/peer".into(), Some(json!([{"token": "alice"}])), false).await;
	peer.command("REAUTH").await;
	peer.wait_for("REAUTH_NONE", 1).await;
	peer.kill().await;

	// 10: transition. A valid handshake credential (old client, sends at once) …
	let before = server.auth.authentication_counts();
	let mut peer = spawn(format!("{auth}?token=jack"), None, true).await;
	peer.wait_for("HELLO_OK", 1).await;
	wait_until(|| principals(&server).last().map(String::as_str) == Some("jack")).await;
	assert_eq!(server.auth.authentication_counts().handshake, before.handshake + 1, "case 10a");
	peer.kill().await;
	// … an invalid one closes …
	let mut peer = spawn(format!("{auth}?token=bad"), None, true).await;
	peer.wait_for("DISCONNECTED", 1).await;
	assert_eq!(peer.count("HELLO_OK"), 0, "case 10b");
	peer.kill().await;
	// … and a new client on a handshake-authenticated session re-authenticates (same principal).
	let mut peer = spawn(format!("{auth}?token=kate"), Some(json!([{"token": "kate:2"}])), true).await;
	peer.wait_for("HELLO_OK", 1).await;
	wait_until(|| principals(&server).last().map(String::as_str) == Some("kate")).await;
	let session =
		server.auth.sessions().into_iter().find(|s| s.principal::<String>().as_deref() == Some(&"kate".into()));
	assert_eq!(session.unwrap().authentication_method(), Some(vcmp::AuthenticationMethod::Handshake), "case 10d");
	assert_eq!(peer.count("AUTH_ERROR 403"), 0, "case 10d");
	peer.kill().await;

	// 11: a client with authenticate against an endpoint without the opt-in → 400, never sends.
	let mut peer = spawn(open.clone(), Some(json!([{"token": "alice"}])), true).await;
	peer.wait_for("AUTH_ERROR 400", 1).await;
	tokio::time::sleep(Duration::from_millis(500)).await;
	assert_eq!(peer.count("CONNECTED"), 0, "case 11");
	assert_eq!(server.open_messages.load(Ordering::SeqCst), 0, "case 11: never sends MSG");
	peer.kill().await;

	server.handle.stop().await;
}
