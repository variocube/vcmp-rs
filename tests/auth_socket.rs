//! Socket-level tests of session authentication (`AUT`): the server endpoint (bare and axum hosts)
//! against raw WebSocket peers and the client, and the client against raw servers.

mod common;

use common::*;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use vcmp::{
	Authentication, AuthenticationCounts, Authenticator, ConnectInfo, Endpoint, Session, VcmpClient, VcmpError,
	VcmpServer,
};

server_tests!(
	hooks_and_session_set_see_only_authenticated_sessions,
	closes_with_policy_violation_after_the_nak,
	accepts_a_handshake_credential_in_the_query,
	client_authenticates_before_it_counts_as_connected,
	client_reports_nak_400_from_an_endpoint_without_auth_and_never_sends,
	client_keeps_a_session_alive_when_the_app_reauthenticates,
);

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn decide(token: &str, ttl: Option<u64>) -> Result<Authentication<String>, VcmpError> {
	match token {
		"" | "bad" => Err(VcmpError::new(401, "Unauthorized")),
		token => {
			let principal = token.split(':').next().unwrap_or_default().to_owned();
			let authentication = Authentication::new(principal.clone()).ack_member("user", principal);
			Ok(match ttl {
				Some(ttl) => authentication.expires_in(Duration::from_millis(ttl)),
				None => authentication,
			})
		}
	}
}

/// Tokens as in `decide`; `ttl` in the credential sets the expiry. The handshake credential is the
/// `token` query parameter.
fn authenticator() -> Authenticator {
	Authenticator::with_handshake(
		|_session: Session, credential: Value| async move {
			decide(credential["token"].as_str().unwrap_or_default(), credential["ttl"].as_u64())
		},
		|info: ConnectInfo| async move {
			match info.query.as_deref().and_then(|query| query.strip_prefix("token=")) {
				Some(token) => decide(token, None).map(Some),
				None => Ok(None),
			}
		},
	)
	.timeout(Duration::from_secs(2))
}

struct Hooks {
	connected: Arc<Mutex<Vec<String>>>,
	disconnected: Arc<AtomicUsize>,
}

fn record_hooks(endpoint: &Endpoint) -> Hooks {
	let connected = Arc::new(Mutex::new(Vec::new()));
	let disconnected = Arc::new(AtomicUsize::new(0));
	let record = connected.clone();
	endpoint.on_connected(move |session| {
		let principal = session.principal::<String>().map(|p| p.to_string()).unwrap_or_else(|| "-".into());
		record.lock().unwrap().push(principal);
		async {}
	});
	let count = disconnected.clone();
	endpoint.on_disconnected(move |_| {
		count.fetch_add(1, Ordering::SeqCst);
		async {}
	});
	Hooks { connected, disconnected }
}

async fn start(host: ServerHost, heartbeat: Duration, authenticate: bool) -> (HostHandle, Endpoint) {
	let server = VcmpServer::builder().heartbeat_interval(heartbeat).build();
	let endpoint = server.endpoint("/test/{name}");
	register(endpoint.handlers());
	if authenticate {
		endpoint.authenticate(authenticator());
	}
	(host.bind(server, 0).await, endpoint)
}

async fn raw(server: &HostHandle, query: &str) -> Ws {
	let url = format!("ws://{}/test/raw{query}", server.local_addr());
	tokio_tungstenite::connect_async(url).await.unwrap().0
}

/// The next text frame other than a heartbeat; `Err` with the close frame when the peer closed.
async fn next_text(ws: &mut Ws) -> Result<String, Option<CloseFrame>> {
	loop {
		let message = timeout(Duration::from_secs(5), ws.next()).await.expect("no frame in time");
		match message {
			Some(Ok(Message::Text(text))) if text.starts_with("HBT") => continue,
			Some(Ok(Message::Text(text))) => return Ok(text.to_string()),
			Some(Ok(Message::Close(frame))) => return Err(frame),
			Some(Ok(_)) => continue,
			Some(Err(_)) | None => return Err(None),
		}
	}
}

fn assert_policy_close(result: Result<String, Option<CloseFrame>>, reason: &str) {
	let frame = result.expect_err("expected a close frame").expect("the close frame carries a code");
	assert_eq!((u16::from(frame.code), frame.reason.as_str()), (1008, reason));
}

const ECHO: &str = r#"{"@type":"test:Echo","payload":"hi"}"#;

async fn hooks_and_session_set_see_only_authenticated_sessions(host: ServerHost) {
	init_tracing();
	let (server, endpoint) = start(host, Duration::from_secs(20), true).await;
	let hooks = record_hooks(&endpoint);

	let mut pending = raw(&server, "").await;
	let mut violator = raw(&server, "").await;
	tokio::time::sleep(Duration::from_millis(100)).await;
	assert_eq!(endpoint.session_count(), 0);
	assert!(endpoint.sessions().is_empty());
	assert!(endpoint.broadcast(&Echo { payload: "x".into() }).await.is_empty(), "broadcasts skip them");
	assert!(hooks.connected.lock().unwrap().is_empty());

	violator.send(Message::text(format!("MSGabcdefghijkl{ECHO}"))).await.unwrap();
	assert_policy_close(next_text(&mut violator).await, "authentication required");

	pending.send(Message::text(r#"AUTabcdefghijkl{"token":"alice"}"#)).await.unwrap();
	assert_eq!(next_text(&mut pending).await.unwrap(), r#"ACKabcdefghijkl{"user":"alice"}"#);
	wait_until(|| endpoint.session_count() == 1).await;
	wait_until(|| hooks.connected.lock().unwrap().len() == 1).await;
	assert_eq!(*hooks.connected.lock().unwrap(), vec!["alice".to_owned()]);
	let session = endpoint.sessions().pop().unwrap();
	assert_eq!(session.principal::<String>().as_deref().map(String::as_str), Some("alice"));
	assert_eq!(endpoint.authentication_counts(), AuthenticationCounts { handshake: 0, aut: 1 });

	pending.send(Message::text(format!("MSGmnopqrstuvwx{ECHO}"))).await.unwrap();
	assert_eq!(next_text(&mut pending).await.unwrap(), r#"ACKmnopqrstuvwx"hi""#);
	assert_eq!(hooks.disconnected.load(Ordering::SeqCst), 0, "the unauthenticated session was never connected");

	pending.close(None).await.unwrap();
	wait_until(|| hooks.disconnected.load(Ordering::SeqCst) == 1).await;
	server.stop().await;
}

async fn closes_with_policy_violation_after_the_nak(host: ServerHost) {
	init_tracing();
	let (server, endpoint) = start(host, Duration::from_secs(20), true).await;
	let hooks = record_hooks(&endpoint);

	let mut ws = raw(&server, "").await;
	ws.send(Message::text(r#"AUTabcdefghijkl{"token":"bad"}"#)).await.unwrap();
	let nak = next_text(&mut ws).await.unwrap();
	assert!(nak.starts_with("NAKabcdefghijkl"), "{nak}");
	assert_eq!(serde_json::from_str::<Value>(&nak[15..]).unwrap()["status"], 401);
	assert_policy_close(next_text(&mut ws).await, "authentication failed");

	let mut ws = raw(&server, "").await;
	assert_policy_close(next_text(&mut ws).await, "authentication timeout");

	let mut ws = raw(&server, "").await;
	ws.send(Message::text(r#"AUTabcdefghijkl{"token":"alice","ttl":300}"#)).await.unwrap();
	let started = Instant::now();
	let ack: Value = serde_json::from_str(&next_text(&mut ws).await.unwrap()[15..]).unwrap();
	assert!((250..=300).contains(&ack["expiresIn"].as_u64().unwrap()), "{ack}");
	assert_policy_close(next_text(&mut ws).await, "authentication expired");
	assert!(started.elapsed() >= Duration::from_millis(250), "{:?}", started.elapsed());

	wait_until(|| hooks.disconnected.load(Ordering::SeqCst) == 1).await;
	assert_eq!(hooks.connected.lock().unwrap().len(), 1);
	server.stop().await;
}

async fn accepts_a_handshake_credential_in_the_query(host: ServerHost) {
	init_tracing();
	let (server, endpoint) = start(host, Duration::from_secs(20), true).await;
	let hooks = record_hooks(&endpoint);

	// An old client: credential in the URL, sends at once, no AUT.
	let mut old = raw(&server, "?token=carol").await;
	old.send(Message::text(format!("MSGabcdefghijkl{ECHO}"))).await.unwrap();
	assert_eq!(next_text(&mut old).await.unwrap(), r#"ACKabcdefghijkl"hi""#);
	wait_until(|| hooks.connected.lock().unwrap().len() == 1).await;
	assert_eq!(*hooks.connected.lock().unwrap(), vec!["carol".to_owned()]);

	let mut invalid = raw(&server, "?token=bad").await;
	assert_policy_close(next_text(&mut invalid).await, "authentication failed");

	let mut new = raw(&server, "").await;
	new.send(Message::text(r#"AUTabcdefghijkl{"token":"dave"}"#)).await.unwrap();
	assert!(next_text(&mut new).await.unwrap().starts_with("ACK"));
	wait_until(|| endpoint.authentication_counts() == AuthenticationCounts { handshake: 1, aut: 1 }).await;
	server.stop().await;
}

fn auth_client(url: &str, provider_calls: Arc<AtomicUsize>, token: &'static str) -> VcmpClient {
	let builder = VcmpClient::builder(url)
		.reconnect(vcmp::Backoff::fixed(Duration::from_millis(100)))
		.initial_heartbeat_timeout(Duration::from_secs(5))
		.auth_timeout(Duration::from_secs(2))
		.authenticate(move || {
			let call = provider_calls.fetch_add(1, Ordering::SeqCst) + 1;
			async move { Ok(json!({ "token": format!("{token}:{call}") })) }
		});
	let client = builder.build();
	register(client.handlers());
	client
}

fn record_auth_errors(client: &VcmpClient) -> Arc<Mutex<Vec<u16>>> {
	let errors = Arc::new(Mutex::new(Vec::new()));
	let record = errors.clone();
	client.on_auth_error(move |error| {
		record.lock().unwrap().push(error.status());
		async {}
	});
	errors
}

async fn client_authenticates_before_it_counts_as_connected(host: ServerHost) {
	init_tracing();
	let (server, endpoint) = start(host, Duration::from_secs(20), true).await;
	let port = server.local_addr().port();
	let calls = Arc::new(AtomicUsize::new(0));
	let client = auth_client(&format!("ws://{}/test/a", server.local_addr()), calls.clone(), "erin");
	let connected_hook = Arc::new(Mutex::new(Vec::new()));
	let record = connected_hook.clone();
	client.on_connected(move |session| {
		record.lock().unwrap().push(session.authentication());
		async {}
	});
	client.start();
	connected(&client).await;
	assert_eq!(client.send(&Echo { payload: "hi".into() }).await.unwrap(), "hi");
	wait_until(|| connected_hook.lock().unwrap().len() == 1).await;
	assert_eq!(connected_hook.lock().unwrap()[0], Some(json!({"user": "erin"})), "the hook runs after the ACK");
	let session = endpoint.sessions().pop().unwrap();
	assert_eq!(session.principal::<String>().as_deref().map(String::as_str), Some("erin"));
	assert_eq!(calls.load(Ordering::SeqCst), 1);

	// A reconnect fetches a fresh credential.
	server.stop().await;
	disconnected(&client).await;
	let (server, _endpoint) = {
		let server_ = VcmpServer::builder().build();
		let endpoint = server_.endpoint("/test/{name}");
		register(endpoint.handlers());
		endpoint.authenticate(authenticator());
		(host.bind(server_, port).await, endpoint)
	};
	connected(&client).await;
	assert!(calls.load(Ordering::SeqCst) >= 2);
	assert_eq!(client.send(&Echo { payload: "again".into() }).await.unwrap(), "again");
	client.stop_and_wait().await;
	server.stop().await;
}

// Contract case 11 (client side): an endpoint without the opt-in answers NAK 400; the client reports
// an authentication error and never sends a MSG.
async fn client_reports_nak_400_from_an_endpoint_without_auth_and_never_sends(host: ServerHost) {
	init_tracing();
	let (server, endpoint) = start(host, Duration::from_secs(20), false).await;
	let received = Arc::new(AtomicUsize::new(0));
	let count = received.clone();
	endpoint.on_type("test:Count", move |_: Value, _| {
		count.fetch_add(1, Ordering::SeqCst);
		async { Ok(()) }
	});
	let calls = Arc::new(AtomicUsize::new(0));
	let client = auth_client(&format!("ws://{}/test/a", server.local_addr()), calls.clone(), "frank");
	let errors = record_auth_errors(&client);
	let hook_ran = Arc::new(AtomicUsize::new(0));
	let ran = hook_ran.clone();
	client.on_connected(move |session| {
		ran.fetch_add(1, Ordering::SeqCst);
		async move {
			let _ = session.send_value(&json!({"@type": "test:Count"})).await;
		}
	});
	client.start();
	wait_until(|| errors.lock().unwrap().len() >= 2).await;
	assert!(errors.lock().unwrap().iter().all(|status| *status == 400), "{:?}", errors.lock().unwrap());
	assert!(calls.load(Ordering::SeqCst) >= 2, "every reconnect fetches a fresh credential");
	assert!(!client.is_connected());
	let error = client.send_value(&json!({"@type": "test:Count"})).await.unwrap_err();
	assert_eq!(error.status(), 503);
	assert_eq!(hook_ran.load(Ordering::SeqCst), 0);
	assert_eq!(received.load(Ordering::SeqCst), 0);
	client.stop_and_wait().await;
	server.stop().await;
}

async fn client_keeps_a_session_alive_when_the_app_reauthenticates(host: ServerHost) {
	init_tracing();
	let server = VcmpServer::builder().build();
	let endpoint = server.endpoint("/test/{name}");
	register(endpoint.handlers());
	endpoint.authenticate(Authenticator::new(|_session: Session, credential: Value| async move {
		decide(credential["token"].as_str().unwrap_or_default(), Some(1_500))
	}));
	let server = host.bind(server, 0).await;
	let calls = Arc::new(AtomicUsize::new(0));
	let client = auth_client(&format!("ws://{}/test/a", server.local_addr()), calls.clone(), "gina");
	let errors = record_auth_errors(&client);
	client.start();
	connected(&client).await;
	let session = client.session().unwrap();
	// The application re-authenticates before each expiry; the session outlives several of them.
	for _ in 0..4 {
		tokio::time::sleep(Duration::from_millis(1_000)).await;
		let ack = client.reauthenticate().await.unwrap().expect("an ACK");
		assert_eq!(ack["user"], "gina");
		assert!(ack["expiresIn"].as_u64().is_some_and(|millis| millis > 1_000), "{ack}");
	}
	assert!(session.is_open());
	assert_eq!(client.session(), Some(session.clone()));
	assert_eq!(calls.load(Ordering::SeqCst), 5, "a fresh credential per re-authentication");
	assert!(errors.lock().unwrap().is_empty());
	assert_eq!(client.send(&Echo { payload: "hi".into() }).await.unwrap(), "hi");
	// Without re-authentication the server closes the session at its expiry; the client reconnects.
	timeout(Duration::from_secs(3), session.closed()).await.expect("closed at the expiry");
	wait_until(|| client.session().is_some_and(|current| current != session)).await;
	client.stop_and_wait().await;
	server.stop().await;
}

// --- The client against raw servers -----------------------------------------------------------------

/// A raw WebSocket server; every accepted connection is handed to the test.
async fn raw_server() -> (String, tokio::sync::mpsc::UnboundedReceiver<WebSocketStream<TcpStream>>) {
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let url = format!("ws://{}/raw", listener.local_addr().unwrap());
	let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
	tokio::spawn(async move {
		while let Ok((stream, _)) = listener.accept().await {
			if let Ok(ws) = tokio_tungstenite::accept_async(stream).await {
				let _ = tx.send(ws);
			}
		}
	});
	(url, rx)
}

async fn accept(
	rx: &mut tokio::sync::mpsc::UnboundedReceiver<WebSocketStream<TcpStream>>,
) -> WebSocketStream<TcpStream> {
	timeout(Duration::from_secs(5), rx.recv()).await.expect("no connection in time").unwrap()
}

async fn next_frame(ws: &mut WebSocketStream<TcpStream>, within: Duration) -> Option<String> {
	loop {
		match timeout(within, ws.next()).await {
			Ok(Some(Ok(Message::Text(text)))) => return Some(text.to_string()),
			Ok(Some(Ok(_))) => continue,
			_ => return None,
		}
	}
}

/// The close frame the client sends (code, reason), skipping text frames.
async fn close_frame(ws: &mut WebSocketStream<TcpStream>) -> Option<(u16, String)> {
	loop {
		match timeout(Duration::from_secs(5), ws.next()).await {
			Ok(Some(Ok(Message::Close(frame)))) => {
				return frame.map(|frame| (u16::from(frame.code), frame.reason.to_string()));
			}
			Ok(Some(Ok(_))) => continue,
			_ => panic!("no close frame"),
		}
	}
}

fn client_close(reason: &str) -> Option<(u16, String)> {
	Some((4008, reason.to_owned()))
}

/// Answers an AUT frame with the given ACK/NAK suffix; returns the AUT's credential.
async fn answer_aut(ws: &mut WebSocketStream<TcpStream>, answer: impl FnOnce(&str) -> String) -> Value {
	let aut = next_frame(ws, Duration::from_secs(5)).await.expect("expected an AUT");
	assert!(aut.starts_with("AUT"), "the first frame is the AUT: {aut}");
	let id = &aut[3..15];
	ws.send(Message::text(answer(id))).await.unwrap();
	serde_json::from_str(&aut[15..]).unwrap()
}

#[tokio::test]
async fn client_reports_a_timeout_when_the_server_never_answers_aut() {
	init_tracing();
	let (url, mut connections) = raw_server().await;
	let calls = Arc::new(AtomicUsize::new(0));
	let client = VcmpClient::builder(&url)
		.reconnect(vcmp::Backoff::fixed(Duration::from_millis(50)))
		.auth_timeout(Duration::from_secs(1))
		.authenticate({
			let calls = calls.clone();
			move || {
				calls.fetch_add(1, Ordering::SeqCst);
				async { Ok(json!({"token": "t"})) }
			}
		})
		.build();
	let errors = record_auth_errors(&client);
	client.start();
	// An older server: it heartbeats, but ignores the AUT it does not know.
	let mut ws = accept(&mut connections).await;
	ws.send(Message::text("HBT20000")).await.unwrap();
	let aut = next_frame(&mut ws, Duration::from_secs(2)).await.unwrap();
	assert!(aut.starts_with("AUT"), "{aut}");
	assert!(!client.is_connected());
	assert_eq!(close_frame(&mut ws).await, client_close("authentication timeout"), "no MSG; the client closes");
	wait_until(|| errors.lock().unwrap().first() == Some(&504)).await;
	// Reconnecting follows the normal rules, with a fresh credential.
	let mut ws = accept(&mut connections).await;
	assert!(next_frame(&mut ws, Duration::from_secs(2)).await.unwrap().starts_with("AUT"));
	assert!(calls.load(Ordering::SeqCst) >= 2);
	assert!(!client.is_connected());
	client.stop_and_wait().await;
}

#[tokio::test]
async fn client_holds_messages_until_the_ack() {
	init_tracing();
	let (url, mut connections) = raw_server().await;
	let client = VcmpClient::builder(&url).authenticate(|| async { Ok(json!({"token": "t"})) }).build();
	client.on_connected(|session| async move {
		let _ = session.send_value(&json!({"@type": "hello"})).await;
	});
	client.start();
	let mut ws = accept(&mut connections).await;
	let aut = next_frame(&mut ws, Duration::from_secs(2)).await.unwrap();
	assert_eq!(&aut[15..], r#"{"token":"t"}"#);
	assert_eq!(next_frame(&mut ws, Duration::from_millis(300)).await, None, "nothing before the ACK");
	assert!(!client.is_connected());
	assert_eq!(client.send_value(&json!({"@type": "x"})).await.unwrap_err().status(), 503);
	ws.send(Message::text(format!("ACK{}", &aut[3..15]))).await.unwrap();
	connected(&client).await;
	let hello = next_frame(&mut ws, Duration::from_secs(2)).await.unwrap();
	assert!(hello.starts_with("MSG") && hello.ends_with(r#"{"@type":"hello"}"#), "{hello}");
	assert_eq!(client.session().unwrap().authentication(), Some(Value::Null));
	client.stop_and_wait().await;
}

#[tokio::test]
async fn client_closes_and_reports_a_rejected_aut() {
	init_tracing();
	let (url, mut connections) = raw_server().await;
	let client = VcmpClient::builder(&url)
		.reconnect(vcmp::Backoff::fixed(Duration::from_secs(60)))
		.authenticate(|| async { Ok(json!({"token": "t"})) })
		.build();
	let errors = record_auth_errors(&client);
	client.start();
	let mut ws = accept(&mut connections).await;
	answer_aut(&mut ws, |id| format!(r#"NAK{id}{{"status":401,"title":"Unauthorized"}}"#)).await;
	assert_eq!(close_frame(&mut ws).await, client_close("authentication failed"), "the client closes the session");
	wait_until(|| *errors.lock().unwrap() == vec![401]).await;
	assert!(!client.is_connected());
	client.stop_and_wait().await;
}

#[tokio::test]
async fn client_accepts_a_malformed_ack_as_one_without_expiry() {
	init_tracing();
	for answer in [r#"{"expiresIn":"soon"}"#, r#"{"expiresIn":-5}"#, "[1]", "{oops"] {
		let (url, mut connections) = raw_server().await;
		let client = VcmpClient::builder(&url).authenticate(|| async { Ok(json!({"token": "t"})) }).build();
		let errors = record_auth_errors(&client);
		client.start();
		let mut ws = accept(&mut connections).await;
		answer_aut(&mut ws, |id| format!("ACK{id}{answer}")).await;
		connected(&client).await;
		let authentication = client.session().unwrap().authentication().unwrap();
		assert!(authentication.is_null() || authentication.is_object(), "{answer}: {authentication}");
		assert_eq!(next_frame(&mut ws, Duration::from_millis(1_200)).await, None, "{answer}: no refresh");
		assert!(errors.lock().unwrap().is_empty(), "{answer}");
		client.stop_and_wait().await;
	}
}

#[tokio::test]
async fn client_closes_when_the_credential_provider_fails_or_hangs() {
	init_tracing();
	let (url, mut connections) = raw_server().await;
	// A failing provider: an authentication error, close 4008 "authentication failed", no AUT.
	let client = VcmpClient::builder(&url)
		.reconnect(vcmp::Backoff::fixed(Duration::from_secs(60)))
		.authenticate(|| async { Err::<Value, _>(VcmpError::new(503, "Token service down")) })
		.build();
	let errors = record_auth_errors(&client);
	client.start();
	let mut ws = accept(&mut connections).await;
	assert_eq!(close_frame(&mut ws).await, client_close("authentication failed"));
	wait_until(|| *errors.lock().unwrap() == vec![500]).await;
	client.stop_and_wait().await;
	// A credential that is not a JSON object: the same, with 400.
	let client = VcmpClient::builder(&url)
		.reconnect(vcmp::Backoff::fixed(Duration::from_secs(60)))
		.authenticate(|| async { Ok(json!("token")) })
		.build();
	let errors = record_auth_errors(&client);
	client.start();
	let mut ws = accept(&mut connections).await;
	assert_eq!(close_frame(&mut ws).await, client_close("authentication failed"));
	wait_until(|| *errors.lock().unwrap() == vec![500]).await;
	client.stop_and_wait().await;
	// A provider that never completes: the answer timeout covers it, close 4008 "authentication timeout".
	let client = VcmpClient::builder(&url)
		.reconnect(vcmp::Backoff::fixed(Duration::from_secs(60)))
		.auth_timeout(Duration::from_secs(1))
		.authenticate(std::future::pending::<Result<Value, VcmpError>>)
		.build();
	let errors = record_auth_errors(&client);
	client.start();
	let mut ws = accept(&mut connections).await;
	let started = Instant::now();
	assert_eq!(close_frame(&mut ws).await, client_close("authentication timeout"));
	assert!(started.elapsed() >= Duration::from_millis(900), "{:?}", started.elapsed());
	wait_until(|| *errors.lock().unwrap() == vec![504]).await;
	client.stop_and_wait().await;
}

#[test]
#[should_panic(expected = "at least 1 s")]
fn an_answer_timeout_below_one_second_is_rejected() {
	let _ = VcmpClient::builder("ws://localhost/").auth_timeout(Duration::from_millis(999));
}

#[tokio::test]
async fn binary_and_unparseable_frames_before_authentication_close_the_session() {
	init_tracing();
	let (server, _endpoint) = start(ServerHost::Bare, Duration::from_secs(20), true).await;
	for message in [Message::binary(vec![1, 2, 3]), Message::text("garbage"), Message::text("MSG123")] {
		let mut ws = raw(&server, "").await;
		ws.send(message.clone()).await.unwrap();
		assert_policy_close(next_text(&mut ws).await, "authentication required");
	}
	// After authentication they are ignored, as on endpoints without auth.
	let mut ws = raw(&server, "").await;
	ws.send(Message::text(r#"AUTabcdefghijkl{"token":"alice"}"#)).await.unwrap();
	assert!(next_text(&mut ws).await.unwrap().starts_with("ACK"));
	ws.send(Message::binary(vec![1, 2, 3])).await.unwrap();
	ws.send(Message::text("garbage")).await.unwrap();
	ws.send(Message::text(format!("MSGmnopqrstuvwx{ECHO}"))).await.unwrap();
	assert_eq!(next_text(&mut ws).await.unwrap(), r#"ACKmnopqrstuvwx"hi""#);
	server.stop().await;
}

#[tokio::test]
async fn a_synchronously_panicking_handshake_authenticator_does_not_hang_stop() {
	init_tracing();
	let server = VcmpServer::builder().build();
	let endpoint = server.endpoint("/test/{name}");
	endpoint.authenticate(Authenticator::with_handshake(
		|_session: Session, credential: Value| async move { decide(credential["token"].as_str().unwrap_or(""), None) },
		|_info: ConnectInfo| -> std::future::Ready<Result<Option<Authentication<String>>, VcmpError>> {
			panic!("synchronous handshake bug")
		},
	));
	let server = ServerHost::Bare.bind(server, 0).await;
	let mut ws = raw(&server, "?token=alice").await;
	assert_policy_close(next_text(&mut ws).await, "authentication failed");
	timeout(Duration::from_secs(5), server.stop()).await.expect("stop() returns");
}

#[tokio::test]
async fn a_credential_provider_panicking_synchronously_is_a_provider_failure() {
	init_tracing();
	let (url, mut connections) = raw_server().await;
	let client = VcmpClient::builder(&url)
		.reconnect(vcmp::Backoff::fixed(Duration::from_millis(100)))
		.authenticate(|| -> std::future::Ready<Result<Value, VcmpError>> { panic!("synchronous provider bug") })
		.build();
	let errors = record_auth_errors(&client);
	client.start();
	let mut ws = accept(&mut connections).await;
	assert_eq!(close_frame(&mut ws).await, client_close("authentication failed"));
	wait_until(|| errors.lock().unwrap().first() == Some(&500)).await;
	// The client task survives and reconnects.
	let _ws = accept(&mut connections).await;
	client.stop_and_wait().await;
}

// --- Re-authentication: triggered by the application, never scheduled by the client ----------------

/// A client against a raw server, connected with `{"token": 1}`; the provider returns the call count.
async fn reauth_client(
	auth_timeout: Duration,
	provider: impl Fn(usize) -> futures_util::future::BoxFuture<'static, Result<Value, VcmpError>> + Send + Sync + 'static,
) -> (
	VcmpClient,
	WebSocketStream<TcpStream>,
	tokio::sync::mpsc::UnboundedReceiver<WebSocketStream<TcpStream>>,
	Arc<Mutex<Vec<u16>>>,
) {
	let (url, mut connections) = raw_server().await;
	let calls = Arc::new(AtomicUsize::new(0));
	let provider = Arc::new(provider);
	let client = VcmpClient::builder(&url)
		.reconnect(vcmp::Backoff::fixed(Duration::from_millis(50)))
		.auth_timeout(auth_timeout)
		.authenticate(move || provider(calls.fetch_add(1, Ordering::SeqCst) + 1))
		.build();
	let errors = record_auth_errors(&client);
	client.start();
	let mut ws = accept(&mut connections).await;
	answer_aut(&mut ws, |id| format!(r#"ACK{id}{{"expiresIn":60000}}"#)).await;
	connected(&client).await;
	(client, ws, connections, errors)
}

fn counting(call: usize) -> futures_util::future::BoxFuture<'static, Result<Value, VcmpError>> {
	Box::pin(async move { Ok(json!({ "token": call })) })
}

#[tokio::test]
async fn the_client_never_re_authenticates_by_itself() {
	init_tracing();
	let (url, mut connections) = raw_server().await;
	let client = VcmpClient::builder(&url).authenticate(|| async { Ok(json!({"token": "t"})) }).build();
	client.start();
	let mut ws = accept(&mut connections).await;
	answer_aut(&mut ws, |id| format!(r#"ACK{id}{{"expiresIn":1500}}"#)).await;
	connected(&client).await;
	assert_eq!(client.session().unwrap().authentication(), Some(json!({"expiresIn": 1500})), "passed to the app");
	assert_eq!(next_frame(&mut ws, Duration::from_millis(2_500)).await, None, "no AUT, also past the expiry");
	client.stop_and_wait().await;
}

#[tokio::test]
async fn reauthenticate_sends_one_aut_with_a_fresh_credential_and_returns_the_ack() {
	init_tracing();
	let (client, mut ws, _connections, errors) = reauth_client(Duration::from_secs(10), counting).await;
	let reauth = tokio::spawn({
		let client = client.clone();
		async move { client.reauthenticate().await }
	});
	let credential = answer_aut(&mut ws, |id| format!(r#"ACK{id}{{"expiresIn":5000,"user":"u"}}"#)).await;
	assert_eq!(credential, json!({"token": 2}));
	assert_eq!(reauth.await.unwrap().unwrap(), Some(json!({"expiresIn": 5000, "user": "u"})));
	assert_eq!(client.session().unwrap().authentication(), Some(json!({"expiresIn": 5000, "user": "u"})));
	assert_eq!(next_frame(&mut ws, Duration::from_millis(500)).await, None, "exactly one AUT");
	assert!(errors.lock().unwrap().is_empty());
	client.stop_and_wait().await;
}

#[tokio::test]
async fn a_naked_reauthentication_is_reported_keeps_the_session_and_is_not_retried() {
	init_tracing();
	let (client, mut ws, _connections, errors) = reauth_client(Duration::from_secs(10), counting).await;
	let session = client.session().unwrap();
	for status in [401, 403, 503] {
		let reauth = tokio::spawn({
			let client = client.clone();
			async move { client.reauthenticate().await }
		});
		answer_aut(&mut ws, |id| format!(r#"NAK{id}{{"status":{status},"title":"No"}}"#)).await;
		let error = reauth.await.unwrap().unwrap_err();
		assert_eq!((error.status(), error.is_transport()), (status, false));
	}
	assert_eq!(*errors.lock().unwrap(), vec![401, 403, 503], "reported");
	assert_eq!(next_frame(&mut ws, Duration::from_millis(1_500)).await, None, "no automatic retry");
	assert!(session.is_open() && client.session() == Some(session), "the session stays");
	client.stop_and_wait().await;
}

#[tokio::test]
async fn a_reauthentication_naked_and_closed_by_the_server_is_reported() {
	init_tracing();
	let (client, mut ws, _connections, errors) = reauth_client(Duration::from_secs(10), counting).await;
	let reauth = tokio::spawn({
		let client = client.clone();
		async move { client.reauthenticate().await }
	});
	let aut = next_frame(&mut ws, Duration::from_secs(2)).await.unwrap();
	// A principal change: NAK 403 and the close in one burst.
	ws.feed(Message::text(format!(r#"NAK{}{{"status":403,"title":"Forbidden"}}"#, &aut[3..15]))).await.unwrap();
	ws.feed(Message::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
		code: 1008.into(),
		reason: "principal changed".into(),
	})))
	.await
	.unwrap();
	ws.flush().await.unwrap();
	assert_eq!(reauth.await.unwrap().unwrap_err().status(), 403);
	wait_until(|| *errors.lock().unwrap() == vec![403]).await;
	client.stop_and_wait().await;
}

#[tokio::test]
async fn a_failing_or_hanging_provider_on_reauthentication_sends_nothing_and_keeps_the_session() {
	init_tracing();
	let (client, mut ws, _connections, errors) = reauth_client(Duration::from_secs(1), |call| match call {
		1 => counting(call),
		2 => Box::pin(async { Err(VcmpError::new(503, "Token service down")) }),
		3 => Box::pin(async { Ok(json!("not an object")) }),
		_ => Box::pin(std::future::pending()),
	})
	.await;
	let session = client.session().unwrap();
	assert_eq!(client.reauthenticate().await.unwrap_err().status(), 500, "a failed provider");
	assert_eq!(client.reauthenticate().await.unwrap_err().status(), 500, "a credential that is not an object");
	let started = Instant::now();
	let error = client.reauthenticate().await.unwrap_err();
	assert_eq!(error.status(), 504, "the answer timeout covers the provider");
	assert!(started.elapsed() >= Duration::from_millis(900), "{:?}", started.elapsed());
	assert_eq!(*errors.lock().unwrap(), vec![500, 500, 504]);
	assert_eq!(next_frame(&mut ws, Duration::from_millis(300)).await, None, "nothing sent");
	assert!(session.is_open() && client.session() == Some(session), "the session stays");
	client.stop_and_wait().await;
}

#[tokio::test]
async fn an_unanswered_reauthentication_closes_with_4008_and_reconnects() {
	init_tracing();
	let (client, mut ws, mut connections, errors) = reauth_client(Duration::from_secs(1), counting).await;
	let reauth = tokio::spawn({
		let client = client.clone();
		async move { client.reauthenticate().await }
	});
	assert!(next_frame(&mut ws, Duration::from_secs(2)).await.unwrap().starts_with("AUT"));
	assert_eq!(close_frame(&mut ws).await, client_close("authentication timeout"));
	let error = reauth.await.unwrap().unwrap_err();
	assert_eq!((error.status(), error.is_transport()), (504, true));
	assert_eq!(*errors.lock().unwrap(), vec![504]);
	let mut ws = accept(&mut connections).await;
	assert_eq!(answer_aut(&mut ws, |id| format!("ACK{id}")).await, json!({"token": 3}), "a fresh credential");
	connected(&client).await;
	client.stop_and_wait().await;
}

#[tokio::test]
async fn reauthentications_requested_while_an_aut_is_pending_coalesce() {
	init_tracing();
	let (client, mut ws, _connections, errors) = reauth_client(Duration::from_secs(10), counting).await;
	let call = || {
		let client = client.clone();
		tokio::spawn(async move { client.reauthenticate().await })
	};
	let first = call();
	let aut = next_frame(&mut ws, Duration::from_secs(2)).await.unwrap();
	assert_eq!(&aut[15..], r#"{"token":2}"#);
	// Three calls while that AUT is pending: no second AUT now, one further AUT after its answer.
	let waiting = [call(), call(), call()];
	assert_eq!(next_frame(&mut ws, Duration::from_millis(300)).await, None, "one AUT in flight");
	ws.send(Message::text(format!(r#"ACK{}{{"n":1}}"#, &aut[3..15]))).await.unwrap();
	assert_eq!(first.await.unwrap().unwrap(), Some(json!({"n": 1})));
	let credential = answer_aut(&mut ws, |id| format!(r#"ACK{id}{{"n":2}}"#)).await;
	assert_eq!(credential, json!({"token": 3}), "a fresh credential for the further AUT");
	for waiting in waiting {
		assert_eq!(waiting.await.unwrap().unwrap(), Some(json!({"n": 2})), "served by the same AUT");
	}
	assert_eq!(next_frame(&mut ws, Duration::from_millis(500)).await, None, "exactly one further AUT");
	assert!(errors.lock().unwrap().is_empty());
	client.stop_and_wait().await;
}

#[tokio::test]
async fn reauthenticate_without_an_authenticated_session_sends_nothing() {
	init_tracing();
	// Not started, and a client without authenticate.
	let (url, mut connections) = raw_server().await;
	let client = VcmpClient::builder(&url).authenticate(|| async { Ok(json!({"token": "t"})) }).build();
	assert_eq!(client.reauthenticate().await.unwrap(), None);
	assert_eq!(VcmpClient::builder(&url).build().reauthenticate().await.unwrap(), None);
	// While the first AUT is pending, the call is served by one further AUT after its answer …
	client.start();
	let mut ws = accept(&mut connections).await;
	let first = next_frame(&mut ws, Duration::from_secs(2)).await.unwrap();
	let reauth = tokio::spawn({
		let client = client.clone();
		async move { client.reauthenticate().await }
	});
	assert_eq!(next_frame(&mut ws, Duration::from_millis(300)).await, None, "not while the first AUT is pending");
	ws.send(Message::text(format!("ACK{}", &first[3..15]))).await.unwrap();
	answer_aut(&mut ws, |id| format!(r#"ACK{id}{{"again":true}}"#)).await;
	assert_eq!(reauth.await.unwrap().unwrap(), Some(json!({"again": true})));
	client.stop_and_wait().await;
	// … and when the first AUT fails, it sends nothing and resolves without an outcome.
	let client = VcmpClient::builder(&url)
		.reconnect(vcmp::Backoff::fixed(Duration::from_secs(60)))
		.authenticate(|| async { Ok(json!({"token": "t"})) })
		.build();
	client.start();
	let mut ws = accept(&mut connections).await;
	let first = next_frame(&mut ws, Duration::from_secs(2)).await.unwrap();
	let reauth = tokio::spawn({
		let client = client.clone();
		async move { client.reauthenticate().await }
	});
	tokio::time::sleep(Duration::from_millis(100)).await;
	ws.send(Message::text(format!(r#"NAK{}{{"status":401,"title":"No"}}"#, &first[3..15]))).await.unwrap();
	assert_eq!(reauth.await.unwrap().unwrap(), None);
	assert_eq!(close_frame(&mut ws).await, client_close("authentication failed"), "no AUT after the NAK");
	client.stop_and_wait().await;
}

// #34 client statuses: 503 when the session closed while the AUT was pending, reported on connect too.
#[tokio::test]
async fn a_session_closed_while_the_first_aut_is_pending_is_reported_with_503() {
	init_tracing();
	let (url, mut connections) = raw_server().await;
	let client = VcmpClient::builder(&url)
		.reconnect(vcmp::Backoff::fixed(Duration::from_millis(100)))
		.authenticate(|| async { Ok(json!({"token": "t"})) })
		.build();
	let errors = record_auth_errors(&client);
	client.start();
	// An older server that closes the session on the frame it does not know, again and again.
	for _ in 0..2 {
		let mut ws = accept(&mut connections).await;
		assert!(next_frame(&mut ws, Duration::from_secs(2)).await.unwrap().starts_with("AUT"));
		ws.close(None).await.unwrap();
	}
	wait_until(|| errors.lock().unwrap().len() >= 2).await;
	assert!(errors.lock().unwrap().iter().all(|status| *status == 503), "{:?}", errors.lock().unwrap());
	assert!(!client.is_connected());
	client.stop_and_wait().await;
}

#[tokio::test]
async fn a_session_closed_while_a_reauthentication_is_pending_is_reported_with_503() {
	init_tracing();
	let (client, mut ws, _connections, errors) = reauth_client(Duration::from_secs(10), counting).await;
	let reauth = tokio::spawn({
		let client = client.clone();
		async move { client.reauthenticate().await }
	});
	assert!(next_frame(&mut ws, Duration::from_secs(2)).await.unwrap().starts_with("AUT"));
	ws.close(None).await.unwrap();
	let error = reauth.await.unwrap().unwrap_err();
	assert_eq!(error.status(), 503);
	wait_until(|| *errors.lock().unwrap() == vec![503]).await;
	client.stop_and_wait().await;
}

// Re-entrancy: the app's hooks may call reauthenticate (and await it); callers queued meanwhile get an outcome.
#[tokio::test]
async fn hooks_may_reauthenticate_and_queued_callers_get_the_outcome() {
	init_tracing();
	let (url, mut connections) = raw_server().await;
	let calls = Arc::new(AtomicUsize::new(0));
	let client = VcmpClient::builder(&url)
		.authenticate({
			let calls = calls.clone();
			move || {
				let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
				async move { Ok(json!({ "token": call })) }
			}
		})
		.build();
	// on_auth_error retries at once and awaits the outcome; on_connected re-authenticates too.
	let hook_outcomes = Arc::new(Mutex::new(Vec::new()));
	client.on_auth_error({
		let client = client.clone();
		let outcomes = hook_outcomes.clone();
		move |_error| {
			let client = client.clone();
			let outcomes = outcomes.clone();
			async move {
				let outcome = client.reauthenticate().await;
				outcomes.lock().unwrap().push(("auth error", outcome.map(|ack| ack.is_some()).map_err(|e| e.status())));
			}
		}
	});
	client.on_connected({
		let client = client.clone();
		let outcomes = hook_outcomes.clone();
		move |_session| {
			let client = client.clone();
			let outcomes = outcomes.clone();
			async move {
				let outcome = client.reauthenticate().await;
				outcomes.lock().unwrap().push(("connected", outcome.map(|ack| ack.is_some()).map_err(|e| e.status())));
			}
		}
	});
	client.start();
	let mut ws = accept(&mut connections).await;
	let first = next_frame(&mut ws, Duration::from_secs(2)).await.unwrap();
	// A caller queued while the first AUT is pending.
	let queued = tokio::spawn({
		let client = client.clone();
		async move { client.reauthenticate().await }
	});
	tokio::time::sleep(Duration::from_millis(100)).await;
	ws.send(Message::text(format!("ACK{}", &first[3..15]))).await.unwrap();
	// The queued caller and the connect hook: served by AUTs that are actually sent and answered.
	let second = answer_aut(&mut ws, |id| format!(r#"NAK{id}{{"status":401,"title":"No"}}"#)).await;
	assert_eq!(second, json!({"token": 2}));
	// The NAK goes to on_auth_error, whose re-authentication joins the one the connect hook queued: one
	// further AUT serves both, without a stall (the hooks run after the AUT lock is released).
	let started = Instant::now();
	assert_eq!(answer_aut(&mut ws, |id| format!(r#"ACK{id}{{"ok":true}}"#)).await, json!({"token": 3}));
	assert!(started.elapsed() < Duration::from_secs(5), "no stall: {:?}", started.elapsed());
	assert_eq!(queued.await.unwrap().unwrap_err().status(), 401, "the queued caller gets that AUT's outcome");
	wait_until(|| hook_outcomes.lock().unwrap().len() == 2).await;
	let mut outcomes = hook_outcomes.lock().unwrap().clone();
	outcomes.sort();
	// The connect hook either joined the queued caller's AUT (its 401) or queued the third AUT: never "no
	// outcome" (Ok(false)) while an AUT is sent for it. Three AUTs in either order.
	assert_eq!(outcomes[0], ("auth error", Ok(true)));
	assert!(matches!(outcomes[1], ("connected", Ok(true) | Err(401))), "{outcomes:?}");
	assert_eq!(next_frame(&mut ws, Duration::from_millis(500)).await, None);
	client.stop_and_wait().await;
}

// Review R1: a re-authentication whose session closes ends at once and does not delay the next connection.
#[tokio::test]
async fn a_reauthentication_on_a_closed_session_does_not_delay_the_next_connection() {
	init_tracing();
	let (url, mut connections) = raw_server().await;
	let calls = Arc::new(AtomicUsize::new(0));
	let client = VcmpClient::builder(&url)
		.reconnect(vcmp::Backoff::fixed(Duration::from_millis(100)))
		.auth_timeout(Duration::from_secs(5))
		.authenticate({
			let calls = calls.clone();
			move || {
				let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
				async move {
					if call == 2 {
						// The re-authentication's token fetch hangs (e.g. the same outage that drops the session).
						tokio::time::sleep(Duration::from_millis(4_500)).await;
					}
					Ok(json!({ "token": call }))
				}
			}
		})
		.build();
	let errors = record_auth_errors(&client);
	client.start();
	let mut ws = accept(&mut connections).await;
	answer_aut(&mut ws, |id| format!("ACK{id}")).await;
	connected(&client).await;
	let started = Instant::now();
	let reauth = tokio::spawn({
		let client = client.clone();
		async move { client.reauthenticate().await }
	});
	tokio::time::sleep(Duration::from_millis(200)).await;
	ws.close(None).await.unwrap();
	// The re-authentication ends with the session (503), not when its provider finishes.
	let error = reauth.await.unwrap().unwrap_err();
	assert_eq!(error.status(), 503);
	assert!(started.elapsed() < Duration::from_secs(2), "ended with the session: {:?}", started.elapsed());
	// The reconnected session's first AUT is not held back by it.
	let mut ws = accept(&mut connections).await;
	let credential = answer_aut(&mut ws, |id| format!("ACK{id}")).await;
	assert_eq!(credential, json!({"token": 3}));
	assert!(started.elapsed() < Duration::from_secs(2), "first AUT of the next connection: {:?}", started.elapsed());
	connected(&client).await;
	tokio::time::sleep(Duration::from_millis(300)).await;
	assert_eq!(*errors.lock().unwrap(), vec![503], "exactly one 503");
	client.stop_and_wait().await;
}
