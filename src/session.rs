//! The VCMP session: the protocol state machine over an abstract duplex of text frames.
//!
//! A [`Session`] is transport-agnostic — it is driven by any [`Stream`] of incoming text frames
//! and any [`Sink`] for outgoing ones. The `client` and `server` features connect it to
//! `tokio-tungstenite`; the loopback tests drive it over in-memory channels.
//!
//! Semantics (shared with `vcmp-js` and `vcmp-spring`):
//!
//! - [`Session::send`] registers the pending entry *before* writing the `MSG`, and settles with
//!   the `ACK` payload or a [`VcmpError`] (the peer's `NAK` problem detail, `503 Session not open`
//!   when the session is not open at send time, or `503 Session closed` when the session closes
//!   while the send is outstanding). Requests have a configured acknowledgement timeout:
//!   requests use the configured deadline and dropping the waiter removes correlation state.
//! - Incoming `MSG` frames are dispatched by their `@type` to a handler that runs **off the read
//!   loop**, so long-running handlers never stall acknowledgements or heartbeats.
//! - Heartbeats: whoever calls [`Session::initiate_heartbeat`] sends `HBT<interval>` and arms a
//!   watchdog of 2 × interval; the receiver echoes it after `interval`. Missing a heartbeat closes
//!   the session, which fails every pending send.

use crate::error::VcmpError;
use crate::frame::Frame;
use crate::resources::{Permit, Resource, ResourceBudget, ResourceLimits, ResourceSnapshot};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::task::{AbortHandle, JoinSet};
use tracing::{debug, trace, warn};

/// A VCMP message type: a JSON object whose `@type` member is [`VcmpMessage::TYPE`].
///
/// The `@type` member is what the peer dispatches on, so the type must be part of the serialized
/// form. The idiomatic way is serde's internally tagged representation:
///
/// ```ignore
/// #[derive(Serialize, Deserialize)]
/// #[serde(tag = "@type", rename = "device:DeviceAdded")]
/// struct DeviceAdded { id: String }
///
/// impl VcmpMessage for DeviceAdded {
///     const TYPE: &'static str = "device:DeviceAdded";
/// }
/// ```
pub trait VcmpMessage: Serialize + DeserializeOwned {
	/// The value of the `@type` member.
	const TYPE: &'static str;
}

/// Information about how a server-side session was established.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ConnectInfo {
	/// The request path (without query string).
	pub path: String,
	/// The path parameters of the matched route or endpoint, e.g. `driver` for `/drivers/{driver}`.
	pub params: HashMap<String, String>,
	/// The request headers, as sent (names lower-cased; non-UTF-8 values are lossily converted).
	pub headers: Vec<(String, String)>,
	/// The peer's socket address, when known.
	pub remote_addr: Option<SocketAddr>,
}

impl ConnectInfo {
	/// The value of a path parameter.
	pub fn param(&self, name: &str) -> Option<&str> {
		self.params.get(name).map(String::as_str)
	}

	/// The value of the first header with the given (case-insensitive) name.
	pub fn header(&self, name: &str) -> Option<&str> {
		self.headers.iter().find(|(n, _)| n.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
	}
}

impl fmt::Debug for ConnectInfo {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ConnectInfo")
			.field("remote_addr", &self.remote_addr)
			.field("headers", &"[redacted]")
			.finish_non_exhaustive()
	}
}

type HandlerFuture = Pin<Box<dyn Future<Output = Result<Option<String>, VcmpError>> + Send>>;
type Handler = Arc<dyn Fn(Value, Session) -> HandlerFuture + Send + Sync>;

/// The message handlers of a client or endpoint, keyed by `@type`.
///
/// Shared between all sessions of a client/endpoint, so handlers can be registered or replaced
/// at any time, also after sessions are established.
#[derive(Default)]
pub struct HandlerMap {
	handlers: RwLock<HashMap<String, Handler>>,
}

impl HandlerMap {
	/// Creates an empty handler map.
	pub fn new() -> Self {
		Self::default()
	}

	/// Registers the handler for messages of type `M`, replacing any previous one.
	///
	/// The handler receives the deserialized message and the session it arrived on. Its `Ok`
	/// result is serialized as the `ACK` payload (a `()` / `null` result yields an `ACK` without
	/// payload); its error is converted into a [`VcmpError`] and sent as the `NAK` payload.
	pub fn on<M, F, Fut, R, E>(&self, handler: F)
	where
		M: VcmpMessage + Send + 'static,
		F: Fn(M, Session) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Result<R, E>> + Send + 'static,
		R: Serialize,
		E: Into<VcmpError>,
	{
		self.on_type::<M, F, Fut, R, E>(M::TYPE, handler);
	}

	/// Registers the handler for messages with the given `@type`, replacing any previous one.
	///
	/// Like [`HandlerMap::on`] for message types that do not implement [`VcmpMessage`], such as
	/// [`serde_json::Value`].
	pub fn on_type<M, F, Fut, R, E>(&self, type_: &str, handler: F)
	where
		M: DeserializeOwned + Send + 'static,
		F: Fn(M, Session) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Result<R, E>> + Send + 'static,
		R: Serialize,
		E: Into<VcmpError>,
	{
		let handler = Arc::new(handler);
		let erased: Handler = Arc::new(move |value, session| {
			let handler = handler.clone();
			Box::pin(async move {
				let message: M = serde_json::from_value(value).map_err(|error| {
					VcmpError::bad_request("Invalid message")
						.with_detail(format!("The message could not be deserialized: {error}"))
				})?;
				let limit = session.inner.limits.max_message_bytes.saturating_sub(15);
				let result = handler(message, session).await.map_err(Into::into)?;
				let payload = serialize_bounded(&result, limit).map_err(|error| {
					if error.status() == 413 {
						error
					} else {
						VcmpError::internal("Message handling failed")
							.with_detail("The handler result could not be serialized.")
					}
				})?;
				Ok(if payload == "null" { None } else { Some(payload) })
			})
		});
		self.handlers.write().unwrap_or_else(|e| e.into_inner()).insert(type_.to_owned(), erased);
	}

	/// Removes the handler for the given `@type`.
	pub fn off(&self, type_: &str) {
		self.handlers.write().unwrap_or_else(|e| e.into_inner()).remove(type_);
	}

	/// Whether a handler is registered for the given `@type`.
	pub fn has(&self, type_: &str) -> bool {
		self.handlers.read().unwrap_or_else(|e| e.into_inner()).contains_key(type_)
	}

	fn resolve(&self, type_: &str) -> Option<Handler> {
		self.handlers.read().unwrap_or_else(|e| e.into_inner()).get(type_).cloned()
	}
}

impl fmt::Debug for HandlerMap {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let handlers = self.handlers.read().unwrap_or_else(|e| e.into_inner());
		f.debug_struct("HandlerMap").field("types", &handlers.keys().collect::<Vec<_>>()).finish()
	}
}

/// Per-session capacity and lifecycle deadlines. All durations must be nonzero.
#[derive(Debug, Clone)]
pub struct SessionLimits {
	/// Counts and retained wire bytes for this session.
	pub resources: ResourceLimits,
	/// Maximum complete incoming or outgoing VCMP frame, including prefix and id.
	pub max_message_bytes: usize,
	/// Maximum wait for an acknowledgement. A timeout never authorizes mutation replay.
	pub request_timeout: Duration,
	/// Maximum lifetime of a handler or connection hook future.
	pub handler_timeout: Duration,
	/// Maximum time a single transport write may block.
	pub write_timeout: Duration,
	/// Maximum time to flush a transport close.
	pub shutdown_timeout: Duration,
}

impl Default for SessionLimits {
	fn default() -> Self {
		Self {
			resources: ResourceLimits {
				connections: 1,
				queued_messages: 128,
				queued_bytes: 4 << 20,
				control_messages: 128,
				control_bytes: 4 << 20,
				pending_requests: 256,
				handler_tasks: 128,
				handler_bytes: 4 << 20,
			},
			max_message_bytes: 2 << 20,
			request_timeout: Duration::from_secs(30),
			handler_timeout: Duration::from_secs(30),
			write_timeout: Duration::from_secs(5),
			shutdown_timeout: Duration::from_secs(5),
		}
	}
}

/// Options for [`Session::spawn`].
#[derive(Debug, Clone, Default)]
pub struct SessionOptions {
	/// The handlers incoming messages are dispatched to.
	pub handlers: Arc<HandlerMap>,
	/// How the session was established (server side).
	pub connect_info: Option<ConnectInfo>,
	/// Per-session resource limits and deadlines.
	pub limits: SessionLimits,
	/// Shared process budget; clone the same budget into all transports to enforce global limits.
	pub budget: ResourceBudget,
}

impl SessionOptions {
	/// Options with the given handlers and no connect info.
	pub fn new(handlers: Arc<HandlerMap>) -> Self {
		SessionOptions { handlers, ..Default::default() }
	}
}

struct Outgoing {
	text: String,
	_permits: (Permit, Permit),
}

type PendingSender = oneshot::Sender<Result<Option<String>, VcmpError>>;
type PendingEntry = (PendingSender, (Permit, Permit));

#[derive(Default)]
struct HeartbeatState {
	/// Whether we currently expect the peer to send a heartbeat. Initially true, so the peer can
	/// initiate the heartbeat.
	awaiting: bool,
	/// The watchdog that closes the session when the expected heartbeat does not arrive.
	watchdog: Option<AbortHandle>,
	/// The timer that sends the next heartbeat after the interval.
	echo: Option<AbortHandle>,
}

struct Inner {
	id: u64,
	handlers: Arc<HandlerMap>,
	connect_info: Option<ConnectInfo>,
	out_tx: mpsc::Sender<Outgoing>,
	control_tx: mpsc::Sender<Outgoing>,
	limits: SessionLimits,
	local: ResourceBudget,
	budget: ResourceBudget,
	/// Pending sends by frame id. `open` is checked under this lock in `send`, and cleared before
	/// the drain in `finish_close`, so no send can slip in between and pend forever.
	pending: Mutex<HashMap<String, PendingEntry>>,
	open: AtomicBool,
	closed: watch::Sender<bool>,
	close_requested: Notify,
	heartbeat: Mutex<HeartbeatState>,
	heartbeats_received: AtomicU64,
	ignored_frames: AtomicU64,
}

/// A VCMP session: one WebSocket connection with its pending sends and heartbeat state.
///
/// Cheap to clone (`Arc` inside); all clones refer to the same session. The session is driven by
/// a task spawned in [`Session::spawn`]; dropping the handles does not close it — call
/// [`Session::close`].
#[derive(Clone)]
pub struct Session {
	inner: Arc<Inner>,
}

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

impl Session {
	/// Spawns a session over the given transport: a stream of incoming text frames (ending when
	/// the connection closes) and a sink for outgoing ones.
	///
	/// Must be called from within a tokio runtime. The session is open immediately.
	pub fn spawn<S, K>(options: SessionOptions, stream: S, sink: K) -> Session
	where
		S: Stream<Item = String> + Send + Unpin + 'static,
		K: Sink<String> + Send + Unpin + 'static,
		K::Error: fmt::Display,
	{
		Self::try_spawn(options, stream, sink).expect("session connection budget exhausted; use Session::try_spawn")
	}

	/// Spawns a session, rejecting admission when the shared connection budget is exhausted.
	pub fn try_spawn<S, K>(options: SessionOptions, stream: S, sink: K) -> Result<Session, VcmpError>
	where
		S: Stream<Item = String> + Send + Unpin + 'static,
		K: Sink<String> + Send + Unpin + 'static,
		K::Error: fmt::Display,
	{
		let connection = options.budget.reserve(Resource::Connection, 0)?;
		Ok(Self::spawn_admitted(options, stream, sink, connection))
	}

	pub(crate) fn spawn_admitted<S, K>(options: SessionOptions, stream: S, sink: K, connection: Permit) -> Session
	where
		S: Stream<Item = String> + Send + Unpin + 'static,
		K: Sink<String> + Send + Unpin + 'static,
		K::Error: fmt::Display,
	{
		let (out_tx, out_rx) = mpsc::channel(options.limits.resources.queued_messages.max(1));
		let (control_tx, control_rx) = mpsc::channel(options.limits.resources.control_messages.max(1));
		let (closed, _) = watch::channel(false);
		let session = Session {
			inner: Arc::new(Inner {
				id: NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed),
				handlers: options.handlers,
				connect_info: options.connect_info,
				out_tx,
				control_tx,
				local: ResourceBudget::new(options.limits.resources.clone()),
				limits: options.limits,
				budget: options.budget,
				pending: Mutex::new(HashMap::new()),
				open: AtomicBool::new(true),
				closed,
				close_requested: Notify::new(),
				heartbeat: Mutex::new(HeartbeatState { awaiting: true, ..Default::default() }),
				heartbeats_received: AtomicU64::new(0),
				ignored_frames: AtomicU64::new(0),
			}),
		};
		let task_session = session.clone();
		tokio::spawn(async move {
			let _connection = connection;
			task_session.drive(stream, sink, out_rx, control_rx).await;
		});
		session
	}

	/// Current per-session transport resources.
	pub fn resources(&self) -> ResourceSnapshot {
		self.inner.local.snapshot()
	}

	fn reserve(&self, resource: Resource, bytes: usize) -> Result<(Permit, Permit), VcmpError> {
		let local = self.inner.local.reserve(resource, bytes)?;
		let global = self.inner.budget.reserve(resource, bytes)?;
		Ok((local, global))
	}

	/// A process-unique id of this session.
	pub fn id(&self) -> u64 {
		self.inner.id
	}

	/// How the session was established (server side only).
	pub fn connect_info(&self) -> Option<&ConnectInfo> {
		self.inner.connect_info.as_ref()
	}

	/// Whether the session is open, i.e. frames can be sent.
	pub fn is_open(&self) -> bool {
		self.inner.open.load(Ordering::SeqCst)
	}

	/// The number of heartbeats received so far.
	pub fn heartbeats_received(&self) -> u64 {
		self.inner.heartbeats_received.load(Ordering::Relaxed)
	}

	/// Invalid or late frames observed. Diagnostics are coalesced at powers of two.
	pub fn ignored_frames(&self) -> u64 {
		self.inner.ignored_frames.load(Ordering::Relaxed)
	}

	fn diagnose(&self, reason: &'static str) {
		let count = self.inner.ignored_frames.fetch_add(1, Ordering::Relaxed).saturating_add(1);
		if count.is_power_of_two() {
			warn!(session = self.id(), count, reason, "ignored or invalid frames");
		}
	}

	/// Resolves once the session has closed and all pending sends have been settled.
	pub async fn closed(&self) {
		let mut rx = self.inner.closed.subscribe();
		while !*rx.borrow_and_update() {
			if rx.changed().await.is_err() {
				return;
			}
		}
	}

	/// Sends a message and waits for the peer's acknowledgement.
	///
	/// Resolves with the parsed `ACK` payload ([`Value::Null`] when the `ACK` had none), or fails
	/// with the peer's `NAK` problem detail, `503 Session not open` when the session is not open,
	/// or `503 Session closed` when the session closes while the send is outstanding.
	///
	/// The configured request deadline applies. Dropping this future removes retained correlation state.
	pub async fn send<M: Serialize + ?Sized>(&self, message: &M) -> Result<Value, VcmpError> {
		let payload = serialize_bounded(message, self.inner.limits.max_message_bytes.saturating_sub(15))?;
		match self.send_payload(payload).await? {
			None => Ok(Value::Null),
			Some(ack) => serde_json::from_str(&ack).map_err(|error| {
				VcmpError::internal("Invalid acknowledgement")
					.with_detail("The ACK payload could not be parsed.")
					.with_source(error)
			}),
		}
	}

	/// Like [`Session::send`], deserializing the `ACK` payload into `R`.
	pub async fn send_as<M, R>(&self, message: &M) -> Result<R, VcmpError>
	where
		M: Serialize + ?Sized,
		R: DeserializeOwned,
	{
		let value = self.send(message).await?;
		serde_json::from_value(value).map_err(|error| {
			VcmpError::internal("Invalid acknowledgement")
				.with_detail(format!("The ACK payload could not be deserialized: {error}"))
				.with_source(error)
		})
	}

	/// Sends a `MSG` frame with the given raw payload and waits for the acknowledgement.
	///
	/// This is the primitive [`Session::send`] builds on; the payload is not validated. Mostly
	/// useful for tests that need to send malformed messages.
	#[doc(hidden)]
	pub async fn send_payload(&self, payload: String) -> Result<Option<String>, VcmpError> {
		if payload.len().saturating_add(15) > self.inner.limits.max_message_bytes {
			return Err(VcmpError::new(413, "Message too large"));
		}
		let permits = self.reserve(Resource::Request, 0)?;
		let frame = Frame::message(payload);
		let id = frame.id().expect("message frames have an id").to_owned();
		let (tx, rx) = oneshot::channel();
		{
			let mut pending = self.inner.pending.lock().unwrap_or_else(|e| e.into_inner());
			if !self.is_open() {
				return Err(VcmpError::session_not_open("Cannot send message: the WebSocket is not open."));
			}
			pending.insert(id.clone(), (tx, permits));
		}
		let _cleanup = PendingCleanup { session: self.clone(), id };
		self.enqueue(frame.serialize(), false)?;
		// Only the queued copy owns byte permits; do not retain the original payload while awaiting an ACK.
		drop(frame);
		match tokio::time::timeout(self.inner.limits.request_timeout, rx).await {
			Ok(Ok(result)) => result,
			Ok(Err(_)) => Err(VcmpError::session_closed("The session closed before acknowledgement.")),
			Err(_) => Err(VcmpError::new(504, "Acknowledgement timed out")
				.with_detail("Delivery or mutation outcome is unknown; do not automatically replay.")),
		}
	}

	/// Starts the heartbeat: sends `HBT<interval>` and expects the peer to echo it within
	/// 2 × interval. Typically the server side calls this once a session connected.
	pub fn initiate_heartbeat(&self, interval: Duration) {
		self.send_heartbeat(interval.as_millis().try_into().unwrap_or(u64::MAX));
	}

	/// Expects the peer to send a heartbeat within `timeout`, closing the session otherwise.
	///
	/// Meant for the non-initiating side, whose watchdog otherwise only starts with the first
	/// received heartbeat — without this, a peer that completes the handshake but never sends
	/// anything would go undetected.
	pub fn expect_heartbeat(&self, timeout: Duration) {
		if !self.is_open() {
			return;
		}
		let session = self.clone();
		let watchdog = tokio::spawn(async move {
			tokio::time::sleep(timeout).await;
			warn!(session = session.id(), "did not receive the expected heartbeat, closing session");
			session.close();
		})
		.abort_handle();
		let previous = self.inner.heartbeat.lock().unwrap_or_else(|e| e.into_inner()).watchdog.replace(watchdog);
		if let Some(previous) = previous {
			previous.abort();
		}
	}

	/// Closes the session. Every pending send fails with `503 Session closed`. Idempotent.
	pub fn close(&self) {
		if self.inner.open.swap(false, Ordering::SeqCst) {
			debug!(session = self.id(), "closing session");
			self.inner.close_requested.notify_one();
		}
	}

	/// Sends a raw text frame. Returns whether the frame was handed to the transport.
	#[doc(hidden)]
	pub fn send_raw(&self, raw: String) -> bool {
		let control = !raw.starts_with("MSG");
		self.enqueue(raw, control).is_ok()
	}

	fn enqueue(&self, text: String, control: bool) -> Result<(), VcmpError> {
		if !self.is_open() {
			return Err(VcmpError::session_not_open("The session is not open."));
		}
		if text.len() > self.inner.limits.max_message_bytes {
			return Err(VcmpError::new(413, "Message too large"));
		}
		let resource = if control { Resource::Control } else { Resource::Data };
		let permits = self.reserve(resource, text.len())?;
		let tx = if control { &self.inner.control_tx } else { &self.inner.out_tx };
		tx.try_send(Outgoing { text, _permits: permits }).map_err(|_| VcmpError::new(503, "Transport overloaded"))
	}

	fn write(&self, frame: Frame) -> bool {
		// Frame payloads may contain credentials or customer data; never include them in logs.
		if self.enqueue(frame.serialize(), true).is_err() {
			// Dropping a mandatory acknowledgement makes delivery ambiguous. Disconnect instead.
			self.close();
			false
		} else {
			true
		}
	}

	fn send_heartbeat(&self, interval: u64) {
		if !self.is_open() {
			warn!(session = self.id(), "not sending heartbeat: the session is not open");
			return;
		}
		debug!(session = self.id(), interval, "sending heartbeat");
		{
			let mut state = self.inner.heartbeat.lock().unwrap_or_else(|e| e.into_inner());
			state.awaiting = true;
		}
		if !self.write(Frame::heartbeat(interval)) {
			warn!(session = self.id(), "could not send heartbeat, closing session");
			self.close();
			return;
		}
		// Arm the watchdog after a successful send: if the peer never sends a heartbeat back
		// within 2 x interval, the connection is considered dead and closed.
		let session = self.clone();
		let watchdog = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(interval.saturating_mul(2))).await;
			warn!(session = session.id(), "did not receive heartbeat in time, closing session");
			session.close();
		})
		.abort_handle();
		let previous = self.inner.heartbeat.lock().unwrap_or_else(|e| e.into_inner()).watchdog.replace(watchdog);
		if let Some(previous) = previous {
			previous.abort();
		}
	}

	fn handle_heartbeat(&self, interval: u64) {
		if interval == 0 {
			self.diagnose("invalid heartbeat interval");
			return;
		}
		let mut state = self.inner.heartbeat.lock().unwrap_or_else(|e| e.into_inner());
		if !state.awaiting {
			self.diagnose("unexpected heartbeat");
			return;
		}
		state.awaiting = false;
		self.inner.heartbeats_received.fetch_add(1, Ordering::Relaxed);
		debug!(session = self.id(), interval, "received heartbeat");
		if let Some(watchdog) = state.watchdog.take() {
			watchdog.abort();
		}
		let session = self.clone();
		let echo = tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(interval)).await;
			session.send_heartbeat(interval);
		})
		.abort_handle();
		if let Some(previous) = state.echo.replace(echo) {
			previous.abort();
		}
	}

	fn handle_ack(&self, id: &str, payload: Option<String>) {
		if let Some(tx) = self.take_pending(id) {
			let _ = tx.send(Ok(payload));
		} else {
			self.diagnose("late or unknown ACK");
		}
	}

	fn handle_nak(&self, id: &str, payload: Option<String>) {
		if let Some(tx) = self.take_pending(id) {
			let error = match payload {
				None => {
					VcmpError::internal("Message handling failed").with_detail("Unspecified error in message handling.")
				}
				Some(payload) => serde_json::from_str::<VcmpError>(&payload).unwrap_or_else(|error| {
					VcmpError::internal("Message handling failed")
						.with_detail("The NAK payload could not be parsed.")
						.with_source(error)
				}),
			};
			let _ = tx.send(Err(error));
		} else {
			self.diagnose("late or unknown NAK");
		}
	}

	fn take_pending(&self, id: &str) -> Option<PendingSender> {
		self.inner.pending.lock().unwrap_or_else(|e| e.into_inner()).remove(id).map(|(sender, _)| sender)
	}

	fn handle_message(&self, id: String, payload: String, tasks: &mut JoinSet<()>) {
		let permits = match self.reserve(Resource::Handler, payload.len()) {
			Ok(permits) => permits,
			Err(error) => {
				self.nak(&id, &error);
				return;
			}
		};
		let message: Value = if payload.is_empty() {
			Value::Object(Default::default())
		} else {
			match serde_json::from_str(&payload) {
				Ok(message) => message,
				Err(error) => {
					let _ = error;
					self.diagnose("invalid JSON");
					self.nak(
						&id,
						&VcmpError::bad_request("Invalid message")
							.with_detail("The message payload could not be parsed."),
					);
					return;
				}
			}
		};
		let Some(type_) = message.get("@type").and_then(Value::as_str).filter(|t| !t.is_empty()) else {
			self.diagnose("missing message type");
			self.nak(
				&id,
				&VcmpError::bad_request("Invalid message").with_detail("The message does not specify a type."),
			);
			return;
		};
		let Some(handler) = self.inner.handlers.resolve(type_) else {
			self.write(Frame::nak(id, None));
			return;
		};
		// Run the handler off the read loop, so the session keeps processing ACKs and heartbeats.
		let session = self.clone();
		tasks.spawn(async move {
			let _permits = permits;
			match tokio::time::timeout(session.inner.limits.handler_timeout, handler(message, session.clone())).await {
				Ok(Ok(payload)) => {
					session.write(Frame::ack(id, payload));
				}
				Ok(Err(error)) => session.nak(&id, &error),
				Err(_) => session.nak(&id, &VcmpError::new(504, "Handler timed out")),
			}
		});
	}

	fn nak(&self, id: &str, error: &VcmpError) {
		let payload = serde_json::to_string(error).unwrap_or_else(|serialization_error| {
			let _ = serialization_error;
			let fallback = VcmpError::internal("Message handling failed")
				.with_detail("The handler error could not be serialized.");
			serde_json::to_string(&fallback).expect("a plain problem detail serializes")
		});
		self.write(Frame::nak(id, Some(payload)));
	}

	fn handle_incoming(&self, raw: String, tasks: &mut JoinSet<()>) {
		if raw.len() > self.inner.limits.max_message_bytes {
			self.close();
			return;
		}
		let frame = match Frame::parse(&raw) {
			Ok(frame) => frame,
			Err(error) => {
				// Invalid-frame diagnostics do not retain arbitrary peer payloads.
				let _ = error;
				self.diagnose("invalid frame");
				return;
			}
		};
		trace!(session = self.id(), bytes = raw.len(), "received frame");
		match frame {
			Frame::Heartbeat { interval } => self.handle_heartbeat(interval),
			Frame::Ack { id, payload } => self.handle_ack(&id, payload),
			Frame::Nak { id, payload } => self.handle_nak(&id, payload),
			Frame::Message { id, payload } => self.handle_message(id, payload, tasks),
		}
	}

	async fn drive<S, K>(
		self,
		mut stream: S,
		mut sink: K,
		mut out_rx: mpsc::Receiver<Outgoing>,
		mut control_rx: mpsc::Receiver<Outgoing>,
	) where
		S: Stream<Item = String> + Send + Unpin + 'static,
		K: Sink<String> + Send + Unpin + 'static,
		K::Error: fmt::Display,
	{
		let writer_session = self.clone();
		let mut closing = self.inner.closed.subscribe();
		let mut writer = tokio::spawn(async move {
			loop {
				let outgoing = tokio::select! {
					biased;
					_ = closing.changed() => break,
					outgoing = control_rx.recv() => outgoing,
					outgoing = out_rx.recv() => outgoing,
				};
				let Some(outgoing) = outgoing else {
					break;
				};
				if !writer_session.is_open() {
					break;
				}
				let result = tokio::select! {
					_ = closing.changed() => break,
					result = tokio::time::timeout(writer_session.inner.limits.write_timeout,
						sink.send(outgoing.text)) => result,
				};
				if !matches!(result, Ok(Ok(()))) {
					writer_session.close();
					break;
				}
			}
			let _ = tokio::time::timeout(writer_session.inner.limits.shutdown_timeout, sink.close()).await;
		});
		let mut tasks = JoinSet::new();
		loop {
			tokio::select! {
				biased;
				_ = self.inner.close_requested.notified() => break,
				_ = tasks.join_next(), if !tasks.is_empty() => {},
				incoming = stream.next() => match incoming {
					Some(text) => self.handle_incoming(text, &mut tasks),
					None => break,
				},
			}
		}
		drop(stream);
		self.close();
		tasks.abort_all();
		while tasks.join_next().await.is_some() {}
		// Notify writer to stop pending writes; final `closed()` notification follows actual cleanup.
		self.inner.closed.send_replace(false);
		if tokio::time::timeout(self.inner.limits.shutdown_timeout, &mut writer).await.is_err() {
			writer.abort();
			let _ = writer.await;
		}
		self.finish_close();
	}

	fn finish_close(&self) {
		self.inner.open.store(false, Ordering::SeqCst);
		{
			let mut heartbeat = self.inner.heartbeat.lock().unwrap_or_else(|e| e.into_inner());
			if let Some(watchdog) = heartbeat.watchdog.take() {
				watchdog.abort();
			}
			if let Some(echo) = heartbeat.echo.take() {
				echo.abort();
			}
		}
		let pending: Vec<_> = self.inner.pending.lock().unwrap_or_else(|e| e.into_inner()).drain().collect();
		if !pending.is_empty() {
			warn!(session = self.id(), count = pending.len(), "failing pending messages: session closed");
		}
		for (_, (tx, _permits)) in pending {
			let _ =
				tx.send(Err(VcmpError::session_closed("The session was closed before the message was acknowledged.")));
		}
		debug!(session = self.id(), "session closed");
		self.inner.closed.send_replace(true);
	}
}

fn serialize_bounded<M: Serialize + ?Sized>(message: &M, limit: usize) -> Result<String, VcmpError> {
	struct Output {
		bytes: Vec<u8>,
		limit: usize,
		full: bool,
	}
	impl std::io::Write for Output {
		fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
			if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
				self.full = true;
				return Err(std::io::Error::other("message limit exceeded"));
			}
			self.bytes.extend_from_slice(bytes);
			Ok(bytes.len())
		}
		fn flush(&mut self) -> std::io::Result<()> {
			Ok(())
		}
	}
	let mut output = Output { bytes: Vec::new(), limit, full: false };
	if serde_json::to_writer(&mut output, message).is_err() {
		return Err(if output.full {
			VcmpError::new(413, "Message too large")
		} else {
			VcmpError::internal("Message serialization failed")
		});
	}
	Ok(String::from_utf8(output.bytes).expect("JSON serialization produces UTF-8"))
}

struct PendingCleanup {
	session: Session,
	id: String,
}
impl Drop for PendingCleanup {
	fn drop(&mut self) {
		self.session.take_pending(&self.id);
	}
}

impl fmt::Debug for Session {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Session").field("id", &self.inner.id).field("open", &self.is_open()).finish()
	}
}

impl PartialEq for Session {
	fn eq(&self, other: &Self) -> bool {
		Arc::ptr_eq(&self.inner, &other.inner)
	}
}

impl Eq for Session {}

impl Hash for Session {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.inner.id.hash(state);
	}
}
