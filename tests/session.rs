//! Loopback tests of the session state machine over an in-memory duplex (no sockets).
//!
//! `Peer` is a raw frame-level counterpart: the test reads the frames the session sends and
//! injects frames the session receives, which covers every bullet of the *Session semantics*
//! section of the tracking issue.

use futures_channel::mpsc;
use futures_util::SinkExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::timeout;
use vcmp::{Frame, HandlerMap, Session, SessionOptions, VcmpError, VcmpMessage};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "@type", rename = "test:Ping")]
struct Ping {
	text: String,
}

impl VcmpMessage for Ping {
	const TYPE: &'static str = "test:Ping";
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "@type", rename = "test:Void")]
struct Void {}

impl VcmpMessage for Void {
	const TYPE: &'static str = "test:Void";
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "@type", rename = "test:Fail")]
struct Fail {
	status: u16,
}

impl VcmpMessage for Fail {
	const TYPE: &'static str = "test:Fail";
}

/// The raw peer of a session: what the session sends arrives at `rx`; what is sent on `tx`
/// arrives at the session.
struct Peer {
	tx: mpsc::UnboundedSender<String>,
	rx: mpsc::UnboundedReceiver<String>,
}

impl Peer {
	fn inject(&self, raw: &str) {
		self.tx.unbounded_send(raw.to_owned()).unwrap();
	}

	/// Simulates the transport closing (e.g. the peer disconnecting).
	fn disconnect(&mut self) {
		self.tx.close_channel();
	}

	async fn next_frame(&mut self) -> Frame {
		let raw = timeout(Duration::from_secs(2), futures_util::StreamExt::next(&mut self.rx))
			.await
			.expect("timed out waiting for a frame")
			.expect("transport closed");
		Frame::parse(&raw).unwrap()
	}

	/// Whether the session closed its outgoing half.
	async fn closed(&mut self) -> bool {
		timeout(Duration::from_secs(2), async { while futures_util::StreamExt::next(&mut self.rx).await.is_some() {} })
			.await
			.is_ok()
	}
}

fn loopback(handlers: Arc<HandlerMap>) -> (Session, Peer) {
	let (to_session_tx, to_session_rx) = mpsc::unbounded::<String>();
	let (from_session_tx, from_session_rx) = mpsc::unbounded::<String>();
	let sink = from_session_tx.sink_map_err(|error| error.to_string());
	let session = Session::spawn(SessionOptions::new(handlers), to_session_rx, sink);
	(session, Peer { tx: to_session_tx, rx: from_session_rx })
}

fn handlers() -> Arc<HandlerMap> {
	let handlers = HandlerMap::new();
	handlers.on::<Ping, _, _, _, _>(|ping, _session| async move { Ok::<_, VcmpError>(format!("pong:{}", ping.text)) });
	handlers.on::<Void, _, _, _, _>(|_, _| async { Ok::<(), VcmpError>(()) });
	handlers.on::<Fail, _, _, _, _>(|fail, _| async move {
		Err::<(), _>(VcmpError::new(fail.status, "Failed").with_detail("as requested"))
	});
	Arc::new(handlers)
}

#[tokio::test]
async fn send_resolves_with_the_ack_payload() {
	let (session, mut peer) = loopback(handlers());
	let send = tokio::spawn(async move { session.send(&Ping { text: "hi".into() }).await });
	let Frame::Message { id, payload } = peer.next_frame().await else { panic!("expected MSG") };
	assert_eq!(serde_json::from_str::<Ping>(&payload).unwrap(), Ping { text: "hi".into() });
	peer.inject(&format!("ACK{id}{{\"ok\":true}}"));
	assert_eq!(send.await.unwrap().unwrap(), serde_json::json!({"ok": true}));
}

#[tokio::test]
async fn send_resolves_with_null_when_the_ack_has_no_payload() {
	let (session, mut peer) = loopback(handlers());
	let send = tokio::spawn(async move { session.send(&Void {}).await });
	let Frame::Message { id, .. } = peer.next_frame().await else { panic!("expected MSG") };
	peer.inject(&format!("ACK{id}"));
	assert_eq!(send.await.unwrap().unwrap(), serde_json::Value::Null);
}

#[tokio::test]
async fn send_as_deserializes_the_result() {
	let (session, mut peer) = loopback(handlers());
	let send = tokio::spawn(async move { session.send_as::<_, Vec<u32>>(&Void {}).await });
	let Frame::Message { id, .. } = peer.next_frame().await else { panic!("expected MSG") };
	peer.inject(&format!("ACK{id}[1,2,3]"));
	assert_eq!(send.await.unwrap().unwrap(), vec![1, 2, 3]);
}

#[tokio::test]
async fn send_fails_with_the_nak_problem_detail() {
	let (session, mut peer) = loopback(handlers());
	let send = tokio::spawn(async move { session.send(&Void {}).await });
	let Frame::Message { id, .. } = peer.next_frame().await else { panic!("expected MSG") };
	peer.inject(&format!(r#"NAK{id}{{"title":"Bad Request","status":400,"detail":"This is bad","extra":1}}"#));
	let error = send.await.unwrap().unwrap_err();
	assert_eq!(error.status(), 400);
	assert_eq!(error.title(), "Bad Request");
	assert_eq!(error.detail(), Some("This is bad"));
	assert_eq!(error.problem().extra["extra"], 1);
}

#[tokio::test]
async fn send_fails_with_a_generic_500_on_an_empty_or_invalid_nak() {
	let (session, mut peer) = loopback(handlers());
	let s = session.clone();
	let send = tokio::spawn(async move { s.send(&Void {}).await });
	let Frame::Message { id, .. } = peer.next_frame().await else { panic!("expected MSG") };
	peer.inject(&format!("NAK{id}"));
	let error = send.await.unwrap().unwrap_err();
	assert_eq!(error.status(), 500);
	assert_eq!(error.title(), "Message handling failed");

	let send = tokio::spawn(async move { session.send(&Void {}).await });
	let Frame::Message { id, .. } = peer.next_frame().await else { panic!("expected MSG") };
	peer.inject(&format!("NAK{id}{{oops"));
	let error = send.await.unwrap().unwrap_err();
	assert_eq!(error.status(), 500);
	assert_eq!(error.detail(), Some("The NAK payload could not be parsed."));
}

#[tokio::test]
async fn send_fails_when_the_ack_payload_is_invalid() {
	let (session, mut peer) = loopback(handlers());
	let send = tokio::spawn(async move { session.send(&Void {}).await });
	let Frame::Message { id, .. } = peer.next_frame().await else { panic!("expected MSG") };
	peer.inject(&format!("ACK{id}{{oops"));
	let error = send.await.unwrap().unwrap_err();
	assert_eq!(error.status(), 500);
	assert_eq!(error.title(), "Invalid acknowledgement");
}

#[tokio::test]
async fn send_fails_with_503_when_the_session_is_closed() {
	let (session, mut peer) = loopback(handlers());
	session.close();
	session.closed().await;
	assert!(!session.is_open());
	assert!(peer.closed().await);
	let error = session.send(&Void {}).await.unwrap_err();
	assert_eq!(error.status(), 503);
	assert_eq!(error.title(), "Session not open");
}

#[tokio::test]
async fn pending_sends_fail_with_503_when_the_session_closes() {
	let (session, mut peer) = loopback(handlers());
	let sends: Vec<_> = (0..10)
		.map(|_| {
			let session = session.clone();
			tokio::spawn(async move { session.send(&Void {}).await })
		})
		.collect();
	for _ in 0..10 {
		peer.next_frame().await;
	}
	peer.disconnect();
	for send in sends {
		let error = send.await.unwrap().unwrap_err();
		assert_eq!(error.status(), 503);
		assert_eq!(error.title(), "Session closed");
	}
	assert!(!session.is_open());
}

#[tokio::test]
async fn concurrent_sends_settle_in_any_order() {
	let (session, mut peer) = loopback(handlers());
	let sends: Vec<_> = (0..100)
		.map(|i| {
			let session = session.clone();
			tokio::spawn(async move { session.send_as::<_, u32>(&Ping { text: i.to_string() }).await })
		})
		.collect();
	let mut frames = Vec::new();
	for _ in 0..100 {
		let Frame::Message { id, payload } = peer.next_frame().await else { panic!("expected MSG") };
		let ping: Ping = serde_json::from_str(&payload).unwrap();
		frames.push((id, ping.text));
	}
	frames.reverse();
	for (id, text) in frames {
		peer.inject(&format!("ACK{id}{text}"));
	}
	for (i, send) in sends.into_iter().enumerate() {
		assert_eq!(send.await.unwrap().unwrap(), i as u32);
	}
}

#[tokio::test]
async fn acks_an_incoming_message_with_the_handler_result() {
	let (_session, mut peer) = loopback(handlers());
	peer.inject(r#"MSGabcdefghijkl{"@type":"test:Ping","text":"x"}"#);
	assert_eq!(peer.next_frame().await, Frame::ack("abcdefghijkl", Some("\"pong:x\"".into())));
}

#[tokio::test]
async fn acks_without_payload_when_the_handler_returns_nothing() {
	let (_session, mut peer) = loopback(handlers());
	peer.inject(r#"MSGabcdefghijkl{"@type":"test:Void"}"#);
	assert_eq!(peer.next_frame().await, Frame::ack("abcdefghijkl", None));
}

#[tokio::test]
async fn naks_with_the_handler_error() {
	let (_session, mut peer) = loopback(handlers());
	peer.inject(r#"MSGabcdefghijkl{"@type":"test:Fail","status":403}"#);
	let Frame::Nak { id, payload } = peer.next_frame().await else { panic!("expected NAK") };
	assert_eq!(id, "abcdefghijkl");
	let problem: vcmp::ProblemDetail = serde_json::from_str(&payload.unwrap()).unwrap();
	assert_eq!(problem.status, 403);
	assert_eq!(problem.title, "Failed");
	assert_eq!(problem.detail.as_deref(), Some("as requested"));
}

#[tokio::test]
async fn naks_400_when_the_message_cannot_be_deserialized() {
	let (_session, mut peer) = loopback(handlers());
	peer.inject(r#"MSGabcdefghijkl{"@type":"test:Ping","text":42}"#);
	let Frame::Nak { payload, .. } = peer.next_frame().await else { panic!("expected NAK") };
	let problem: vcmp::ProblemDetail = serde_json::from_str(&payload.unwrap()).unwrap();
	assert_eq!(problem.status, 400);
}

#[tokio::test]
async fn naks_400_on_malformed_json() {
	let (_session, mut peer) = loopback(handlers());
	peer.inject("MSGabcdefghijkl{oops");
	let Frame::Nak { id, payload } = peer.next_frame().await else { panic!("expected NAK") };
	assert_eq!(id, "abcdefghijkl");
	let problem: vcmp::ProblemDetail = serde_json::from_str(&payload.unwrap()).unwrap();
	assert_eq!(problem.status, 400);
	assert_eq!(problem.detail.as_deref(), Some("The message payload could not be parsed."));
}

#[tokio::test]
async fn naks_400_when_the_message_has_no_type() {
	let (_session, mut peer) = loopback(handlers());
	peer.inject(r#"MSGabcdefghijkl{"foo":"bar"}"#);
	let Frame::Nak { payload, .. } = peer.next_frame().await else { panic!("expected NAK") };
	let problem: vcmp::ProblemDetail = serde_json::from_str(&payload.unwrap()).unwrap();
	assert_eq!(problem.status, 400);
	assert_eq!(problem.detail.as_deref(), Some("The message does not specify a type."));
	// an empty payload is treated as `{}`
	peer.inject("MSGabcdefghijkl");
	let Frame::Nak { payload, .. } = peer.next_frame().await else { panic!("expected NAK") };
	assert_eq!(serde_json::from_str::<vcmp::ProblemDetail>(&payload.unwrap()).unwrap().status, 400);
}

#[tokio::test]
async fn naks_without_payload_when_no_handler_is_registered() {
	let (_session, mut peer) = loopback(handlers());
	peer.inject(r#"MSGabcdefghijkl{"@type":"test:Unknown"}"#);
	assert_eq!(peer.next_frame().await, Frame::nak("abcdefghijkl", None));
}

#[tokio::test]
async fn naks_500_when_the_result_cannot_be_serialized() {
	let handlers = HandlerMap::new();
	handlers.on::<Void, _, _, _, _>(|_, _| async {
		let mut map = std::collections::HashMap::new();
		map.insert(vec![1u8], 1u8); // non-string keys cannot be serialized to JSON
		Ok::<_, VcmpError>(map)
	});
	let (_session, mut peer) = loopback(Arc::new(handlers));
	peer.inject(r#"MSGabcdefghijkl{"@type":"test:Void"}"#);
	let Frame::Nak { payload, .. } = peer.next_frame().await else { panic!("expected NAK") };
	let problem: vcmp::ProblemDetail = serde_json::from_str(&payload.unwrap()).unwrap();
	assert_eq!(problem.status, 500);
	assert_eq!(problem.title, "Message handling failed");
}

#[tokio::test]
async fn handlers_run_off_the_read_loop() {
	let handlers = HandlerMap::new();
	handlers.on::<Void, _, _, _, _>(|_, _| async {
		tokio::time::sleep(Duration::from_secs(3600)).await;
		Ok::<(), VcmpError>(())
	});
	let (session, mut peer) = loopback(Arc::new(handlers));
	peer.inject(r#"MSGabcdefghijkl{"@type":"test:Void"}"#);
	// while the handler is blocked, the session still answers ACKs and heartbeats
	let send = tokio::spawn(async move { session.send(&Void {}).await });
	let Frame::Message { id, .. } = peer.next_frame().await else { panic!("expected MSG") };
	peer.inject(&format!("ACK{id}"));
	send.await.unwrap().unwrap();
	peer.inject("HBT10");
	assert_eq!(peer.next_frame().await, Frame::heartbeat(10));
}

#[tokio::test]
async fn handlers_can_send_on_the_session() {
	let handlers = HandlerMap::new();
	handlers.on::<Ping, _, _, _, _>(|ping, session| async move {
		session.send(&Ping { text: format!("re:{}", ping.text) }).await?;
		Ok::<(), VcmpError>(())
	});
	let (_session, mut peer) = loopback(Arc::new(handlers));
	peer.inject(r#"MSGabcdefghijkl{"@type":"test:Ping","text":"x"}"#);
	let Frame::Message { id, payload } = peer.next_frame().await else { panic!("expected MSG") };
	assert!(payload.contains("re:x"));
	peer.inject(&format!("ACK{id}"));
	assert_eq!(peer.next_frame().await, Frame::ack("abcdefghijkl", None));
}

#[tokio::test]
async fn ignores_invalid_frames_without_closing() {
	let (session, mut peer) = loopback(handlers());
	peer.inject("XXXsomething");
	peer.inject("");
	peer.inject("HBT");
	peer.inject("HBTfoo");
	peer.inject("HBT0");
	tokio::time::sleep(Duration::from_millis(50)).await;
	assert!(session.is_open());
	peer.inject("HBT10");
	assert_eq!(peer.next_frame().await, Frame::heartbeat(10));
}

#[tokio::test]
async fn initiated_heartbeat_closes_the_session_when_never_answered() {
	let (session, mut peer) = loopback(handlers());
	let s = session.clone();
	let send = tokio::spawn(async move { s.send(&Void {}).await });
	peer.next_frame().await;
	let started = std::time::Instant::now();
	session.initiate_heartbeat(Duration::from_millis(100));
	assert_eq!(peer.next_frame().await, Frame::heartbeat(100));
	timeout(Duration::from_secs(2), session.closed()).await.unwrap();
	let elapsed = started.elapsed();
	assert!(elapsed >= Duration::from_millis(200) && elapsed < Duration::from_millis(1000), "{elapsed:?}");
	let error = send.await.unwrap().unwrap_err();
	assert_eq!(error.status(), 503);
	assert_eq!(error.title(), "Session closed");
}

#[tokio::test]
async fn heartbeat_exchange_keeps_the_session_open() {
	let (session, mut peer) = loopback(handlers());
	session.initiate_heartbeat(Duration::from_millis(50));
	for _ in 0..5 {
		assert_eq!(peer.next_frame().await, Frame::heartbeat(50));
		peer.inject("HBT50");
	}
	assert!(session.is_open());
	timeout(Duration::from_secs(1), async {
		while session.heartbeats_received() < 5 {
			tokio::time::sleep(Duration::from_millis(5)).await;
		}
	})
	.await
	.unwrap();
	session.close();
}

#[tokio::test]
async fn echoes_a_received_heartbeat_after_the_interval() {
	let (session, mut peer) = loopback(handlers());
	let started = std::time::Instant::now();
	peer.inject("HBT100");
	assert_eq!(peer.next_frame().await, Frame::heartbeat(100));
	assert!(started.elapsed() >= Duration::from_millis(100));
	// the echo armed a watchdog: no answer within 2 x interval closes the session
	timeout(Duration::from_secs(2), session.closed()).await.unwrap();
	assert!(started.elapsed() >= Duration::from_millis(300));
}

#[tokio::test]
async fn ignores_heartbeats_while_not_awaiting_one() {
	let (session, mut peer) = loopback(handlers());
	peer.inject("HBT100");
	peer.inject("HBT100");
	peer.inject("HBT100");
	assert_eq!(peer.next_frame().await, Frame::heartbeat(100));
	assert_eq!(session.heartbeats_received(), 1);
	session.close();
}

#[tokio::test]
async fn expected_heartbeat_closes_the_session_when_it_never_arrives() {
	let (session, mut peer) = loopback(handlers());
	session.expect_heartbeat(Duration::from_millis(50));
	timeout(Duration::from_secs(2), session.closed()).await.unwrap();
	assert!(peer.closed().await);
}

#[tokio::test]
async fn expected_heartbeat_keeps_the_session_open_when_it_arrives() {
	let (session, peer) = loopback(handlers());
	session.expect_heartbeat(Duration::from_millis(50));
	peer.inject("HBT1000");
	tokio::time::sleep(Duration::from_millis(150)).await;
	assert!(session.is_open());
	session.close();
}

#[tokio::test]
async fn late_initial_heartbeat_expectation_preserves_an_already_received_heartbeat() {
	let (session, mut peer) = loopback(handlers());
	peer.inject("HBT100");
	timeout(Duration::from_secs(2), async {
		while session.heartbeats_received() == 0 {
			tokio::task::yield_now().await;
		}
	})
	.await
	.unwrap();
	// A client can receive the server's immediate HBT before its connect task arms the initial watchdog.
	// That watchdog must not replace the accepted heartbeat's longer echo/response schedule.
	session.expect_heartbeat(Duration::from_millis(10));
	assert_eq!(peer.next_frame().await, Frame::heartbeat(100));
	assert!(session.is_open());
	session.close();
}

#[tokio::test]
async fn transport_write_failure_closes_the_session() {
	let (session, peer) = loopback(handlers());
	drop(peer);
	let error = timeout(Duration::from_secs(2), session.send(&Void {})).await.unwrap().unwrap_err();
	assert_eq!(error.status(), 503);
	timeout(Duration::from_secs(2), session.closed()).await.unwrap();
}

#[tokio::test]
async fn close_is_idempotent_and_settles_everything() {
	let counter = Arc::new(AtomicUsize::new(0));
	let handlers = HandlerMap::new();
	let c = counter.clone();
	handlers.on::<Void, _, _, _, _>(move |_, _| {
		c.fetch_add(1, Ordering::SeqCst);
		async { Ok::<(), VcmpError>(()) }
	});
	let (session, mut peer) = loopback(Arc::new(handlers));
	peer.inject(r#"MSGabcdefghijkl{"@type":"test:Void"}"#);
	peer.next_frame().await;
	session.close();
	session.close();
	session.closed().await;
	session.closed().await;
	assert_eq!(counter.load(Ordering::SeqCst), 1);
	assert!(peer.closed().await);
}
