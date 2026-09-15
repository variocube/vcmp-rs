//! Capacity, cancellation and lifecycle invariants under slow and non-cooperating peers.
use futures_channel::mpsc;
use futures_util::{Sink, SinkExt, StreamExt};
use serde_json::{Value, json};
use std::pin::Pin;
use std::sync::{
	Arc,
	atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::timeout;
use vcmp::{Frame, HandlerMap, ResourceBudget, ResourceLimits, Session, SessionLimits, SessionOptions, VcmpError};

fn limits() -> SessionLimits {
	SessionLimits {
		request_timeout: Duration::from_millis(100),
		handler_timeout: Duration::from_millis(100),
		write_timeout: Duration::from_millis(100),
		shutdown_timeout: Duration::from_millis(100),
		..Default::default()
	}
}

fn pair(options: SessionOptions) -> (Session, mpsc::UnboundedSender<String>, mpsc::UnboundedReceiver<String>) {
	let (incoming, stream) = mpsc::unbounded();
	let (sink, outgoing) = mpsc::unbounded::<String>();
	let session = Session::try_spawn(options, stream, sink.sink_map_err(|error| error.to_string())).unwrap();
	(session, incoming, outgoing)
}

async fn frame(rx: &mut mpsc::UnboundedReceiver<String>) -> Frame {
	Frame::parse(&timeout(Duration::from_secs(1), rx.next()).await.unwrap().unwrap()).unwrap()
}

async fn until(condition: impl Fn() -> bool) {
	timeout(Duration::from_secs(1), async {
		while !condition() {
			tokio::task::yield_now().await;
		}
	})
	.await
	.unwrap();
}

#[tokio::test]
async fn dropped_and_timed_out_waiters_release_correlation_without_replay() {
	let budget = ResourceBudget::default();
	let (session, peer, mut outgoing) =
		pair(SessionOptions { limits: limits(), budget: budget.clone(), ..Default::default() });
	let task = tokio::spawn({
		let session = session.clone();
		async move { session.send_value(&json!({"@type": "mutate"})).await }
	});
	let Frame::Message { id, .. } = frame(&mut outgoing).await else { panic!("MSG expected") };
	assert_eq!(session.resources().pending_requests, 1);
	task.abort();
	let _ = task.await;
	assert_eq!(session.resources().pending_requests, 0);
	assert_eq!(budget.snapshot().pending_requests, 0);
	peer.unbounded_send(Frame::ack(id, None).serialize()).unwrap();
	let error = session.send_value(&json!({"@type": "mutate"})).await.unwrap_err();
	assert_eq!(error.status(), 504);
	assert_eq!(session.resources().pending_requests, 0);
	assert!(matches!(frame(&mut outgoing).await, Frame::Message { .. }));
	assert!(timeout(Duration::from_millis(20), outgoing.next()).await.is_err());
	session.close();
	session.closed().await;
}

#[tokio::test]
async fn shared_pending_request_limit_is_enforced_across_sessions() {
	let budget = ResourceBudget::new(ResourceLimits { pending_requests: 1, ..Default::default() });
	let (first, _peer, mut outgoing) = pair(SessionOptions { budget: budget.clone(), ..Default::default() });
	let (second, _other_peer, _) = pair(SessionOptions { budget: budget.clone(), ..Default::default() });
	let task = tokio::spawn({
		let first = first.clone();
		async move { first.send_value(&json!({})).await }
	});
	frame(&mut outgoing).await;
	assert_eq!(second.send_value(&json!({})).await.unwrap_err().status(), 503);
	assert_eq!(budget.snapshot().pending_requests, 1);
	assert_eq!(budget.snapshot().overloads, 1);
	task.abort();
	let _ = task.await;
	first.close();
	second.close();
	first.closed().await;
	second.closed().await;
	until(|| budget.snapshot().connections == 0).await;
}

#[tokio::test]
async fn connection_admission_does_not_spawn_rejected_sessions() {
	let budget = ResourceBudget::new(ResourceLimits { connections: 1, ..Default::default() });
	let (session, _peer, _) = pair(SessionOptions { budget: budget.clone(), ..Default::default() });
	let result = Session::try_spawn(
		SessionOptions { budget: budget.clone(), ..Default::default() },
		futures_util::stream::pending::<String>(),
		futures_util::sink::drain::<String>(),
	);
	assert_eq!(result.unwrap_err().status(), 503);
	assert_eq!(budget.snapshot().connections, 1);
	session.close();
	session.closed().await;
	until(|| budget.snapshot().connections == 0).await;
}

struct CountDrop(Arc<AtomicUsize>);
impl Drop for CountDrop {
	fn drop(&mut self) {
		self.0.fetch_add(1, Ordering::SeqCst);
	}
}

#[tokio::test]
async fn overloaded_handlers_nak_while_heartbeats_progress_and_close_aborts_old_work() {
	let dropped = Arc::new(AtomicUsize::new(0));
	let handlers = Arc::new(HandlerMap::new());
	handlers.on_type("wait", {
		let dropped = dropped.clone();
		move |_: Value, _| {
			let dropped = dropped.clone();
			async move {
				let _guard = CountDrop(dropped);
				std::future::pending::<Result<(), VcmpError>>().await
			}
		}
	});
	let mut limits = limits();
	limits.resources.handler_tasks = 1;
	limits.handler_timeout = Duration::from_secs(10);
	let (session, peer, mut outgoing) = pair(SessionOptions { handlers, limits, ..Default::default() });
	peer.unbounded_send(Frame::message(r#"{"@type":"wait"}"#).serialize()).unwrap();
	until(|| session.resources().handler_tasks == 1).await;
	peer.unbounded_send(Frame::message(r#"{"@type":"wait"}"#).serialize()).unwrap();
	let Frame::Nak { payload: Some(payload), .. } = frame(&mut outgoing).await else { panic!("NAK expected") };
	assert_eq!(serde_json::from_str::<VcmpError>(&payload).unwrap().status(), 503);
	peer.unbounded_send(Frame::heartbeat(10).serialize()).unwrap();
	assert!(matches!(frame(&mut outgoing).await, Frame::Heartbeat { interval: 10 }));
	assert_eq!(session.heartbeats_received(), 1);
	session.close();
	session.closed().await;
	assert_eq!(dropped.load(Ordering::SeqCst), 1);
	assert_eq!(session.resources().handler_tasks, 0);
	assert_eq!(session.resources().handler_bytes, 0);
}

#[tokio::test]
async fn handler_deadline_releases_task_and_returns_504() {
	let handlers = Arc::new(HandlerMap::new());
	handlers.on_type("wait", |_: Value, _| std::future::pending::<Result<(), VcmpError>>());
	let (session, peer, mut outgoing) = pair(SessionOptions { handlers, limits: limits(), ..Default::default() });
	peer.unbounded_send(Frame::message(r#"{"@type":"wait"}"#).serialize()).unwrap();
	let Frame::Nak { payload: Some(payload), .. } = frame(&mut outgoing).await else { panic!("NAK expected") };
	assert_eq!(serde_json::from_str::<VcmpError>(&payload).unwrap().status(), 504);
	assert_eq!(session.resources().handler_tasks, 0);
	session.close();
	session.closed().await;
}

#[tokio::test]
async fn message_limit_rejects_serialization_and_disconnects_oversized_peer() {
	let limits = SessionLimits { max_message_bytes: 64, ..limits() };
	let (session, peer, _) = pair(SessionOptions { limits, ..Default::default() });
	assert_eq!(session.send_value(&"x".repeat(100_000)).await.unwrap_err().status(), 413);
	assert_eq!(session.resources().pending_requests, 0);
	peer.unbounded_send("x".repeat(65)).unwrap();
	timeout(Duration::from_secs(1), session.closed()).await.unwrap();
}

#[tokio::test]
async fn slow_writer_retains_bounded_bytes_and_reserves_control_progress() {
	let mut limits = limits();
	limits.resources.queued_messages = 2;
	limits.resources.queued_bytes = 40;
	limits.write_timeout = Duration::from_secs(1);
	let (peer, stream) = mpsc::unbounded();
	let (sink, mut outgoing) = mpsc::channel::<String>(0);
	let session =
		Session::spawn(SessionOptions { limits, ..Default::default() }, stream, sink.sink_map_err(|e| e.to_string()));
	assert!(session.send_raw("MSG123456789012{}".into()));
	assert!(session.send_raw("MSGabcdefghijkl{}".into()));
	assert!(!session.send_raw("MSG000000000000{}".into()));
	assert_eq!(session.resources().queued_messages, 2);
	assert_eq!(session.resources().queued_bytes, 34);
	peer.unbounded_send(Frame::heartbeat(10).serialize()).unwrap();
	until(|| session.resources().control_messages == 1).await;
	// The one in-flight data write completes before the reserved heartbeat, then queued data.
	assert!(outgoing.next().await.unwrap().starts_with("MSG"));
	assert_eq!(timeout(Duration::from_secs(1), outgoing.next()).await.unwrap().unwrap(), "HBT10");
	session.close();
	timeout(Duration::from_secs(1), session.closed()).await.unwrap();
	assert_eq!(session.resources().queued_messages, 0);
	assert_eq!(session.resources().queued_bytes, 0);
	assert_eq!(session.resources().control_messages, 0);
}

#[tokio::test]
async fn permanently_stalled_writer_is_cancelled_with_all_resources_released() {
	let (peer, stream) = mpsc::unbounded();
	let (sink, _outgoing) = mpsc::channel::<String>(0);
	let session = Session::spawn(
		SessionOptions { limits: limits(), ..Default::default() },
		stream,
		sink.sink_map_err(|e| e.to_string()),
	);
	assert!(session.send_raw("MSG123456789012{}".into()));
	peer.unbounded_send(Frame::heartbeat(10).serialize()).unwrap();
	timeout(Duration::from_secs(1), session.closed()).await.unwrap();
	assert_eq!(session.resources().queued_bytes, 0);
	assert_eq!(session.resources().control_bytes, 0);
}

#[derive(Default)]
struct BufferedWrite {
	retained: AtomicUsize,
	closing: AtomicBool,
	accounted_on_drop: AtomicUsize,
}

struct BufferedSink {
	state: Arc<BufferedWrite>,
	budget: ResourceBudget,
	control: bool,
	payload: Option<String>,
}

impl Drop for BufferedSink {
	fn drop(&mut self) {
		let usage = self.budget.snapshot();
		let accounted = if self.control { usage.control_bytes } else { usage.queued_bytes };
		self.state.accounted_on_drop.store(accounted, Ordering::SeqCst);
		self.payload.take();
		self.state.retained.store(0, Ordering::SeqCst);
	}
}

impl Sink<String> for BufferedSink {
	type Error = std::convert::Infallible;

	fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}

	fn start_send(mut self: Pin<&mut Self>, item: String) -> Result<(), Self::Error> {
		self.state.retained.store(item.len(), Ordering::SeqCst);
		self.payload = Some(item);
		Ok(())
	}

	fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Pending
	}

	fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		self.state.closing.store(true, Ordering::SeqCst);
		Poll::Pending
	}
}

async fn buffered_write_keeps_admission_until_dropped(control: bool, cancel: bool) {
	let raw = if control { "ACK123456789012{}" } else { "MSG123456789012{}" };
	let budget = ResourceBudget::new(ResourceLimits {
		queued_messages: 1,
		queued_bytes: raw.len(),
		control_messages: 1,
		control_bytes: raw.len(),
		..Default::default()
	});
	let state = Arc::new(BufferedWrite::default());
	let mut limits = limits();
	if cancel {
		limits.write_timeout = Duration::from_secs(10);
	}
	let session = Session::spawn(
		SessionOptions { budget: budget.clone(), limits, ..Default::default() },
		futures_util::stream::pending::<String>(),
		BufferedSink { state: state.clone(), budget: budget.clone(), control, payload: None },
	);
	let (other, _peer, mut outgoing) = pair(SessionOptions { budget: budget.clone(), ..Default::default() });
	assert!(session.send_raw(raw.into()));
	until(|| state.retained.load(Ordering::SeqCst) == raw.len()).await;
	if cancel {
		session.close();
	}
	until(|| state.closing.load(Ordering::SeqCst)).await;
	for usage in [session.resources(), budget.snapshot()] {
		let (messages, bytes) = if control {
			(usage.control_messages, usage.control_bytes)
		} else {
			(usage.queued_messages, usage.queued_bytes)
		};
		assert_eq!((messages, bytes), (1, raw.len()));
	}
	assert!(!other.send_raw(raw.into()), "buffered payload must still occupy shared admission");
	timeout(Duration::from_secs(1), session.closed()).await.unwrap();
	assert_eq!(state.retained.load(Ordering::SeqCst), 0);
	assert_eq!(state.accounted_on_drop.load(Ordering::SeqCst), raw.len());
	assert_eq!(budget.snapshot().queued_bytes, 0);
	assert_eq!(budget.snapshot().control_bytes, 0);
	assert!(other.send_raw(raw.into()), "dropping the sink must release admission");
	assert_eq!(timeout(Duration::from_secs(1), outgoing.next()).await.unwrap().unwrap(), raw);
	other.close();
	other.closed().await;
}

#[tokio::test]
async fn cancelled_writes_keep_buffered_payloads_accounted_until_shutdown() {
	for control in [false, true] {
		buffered_write_keeps_admission_until_dropped(control, true).await;
	}
}

#[tokio::test]
async fn timed_out_writes_keep_buffered_payloads_accounted_until_shutdown() {
	for control in [false, true] {
		buffered_write_keeps_admission_until_dropped(control, false).await;
	}
}

#[test]
fn connection_debug_redacts_paths_params_and_headers() {
	let info = vcmp::ConnectInfo {
		path: "/secret-path".into(),
		headers: vec![("authorization".into(), "secret-key".into())],
		..Default::default()
	};
	let debug = format!("{info:?}");
	assert!(!debug.contains("secret"));
}
