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
//!   requests use the configured deadline, or their own with [`Session::send_value_timeout`], and
//!   dropping the waiter removes correlation state.
//! - Incoming `MSG` frames are dispatched by their `@type` to a handler that runs **off the read
//!   loop**, so long-running handlers never stall acknowledgements or heartbeats.
//! - Every received frame gets an [`Arrival`] in receive order. A handler reads its message's arrival from
//!   [`Session::arrival`], and [`Session::send_with_arrival`] returns the `ACK`'s, so a caller can
//!   tell which of the two the peer sent first even though handlers run concurrently.
//! - Heartbeats: whoever calls [`Session::initiate_heartbeat`] sends `HBT<interval>` and arms a
//!   watchdog of 2 × interval; the receiver echoes it after `interval`. Missing a heartbeat closes
//!   the session, which fails every pending send.
//! - Authentication (`AUT`): a session spawned with [`SessionOptions::authenticator`] (feature
//!   `server`) accepts only `HBT` and `AUT` until a credential was accepted; see [`auth`]. Without
//!   one, an `AUT` is answered with `NAK` 400. [`Session::send_authentication`] is the client side.

#[cfg(feature = "server")]
pub mod auth;

use crate::error::VcmpError;
use crate::frame::Frame;
use crate::resources::{Permit, Resource, ResourceBudget, ResourceLimits, ResourceSnapshot};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::borrow::Cow;
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

/// A VCMP message type: a JSON object dispatched by the `@type` member [`VcmpMessage::TYPE`].
///
/// The library owns the `@type` member: [`Session::send`] adds it when serializing and
/// [`HandlerMap::on`] removes it before deserializing, so a message type is a plain serde struct
/// and its `TYPE` is the single place the tag is declared. The struct must serialize as a JSON
/// object (a struct with named fields, or a map).
///
/// ```ignore
/// #[derive(Serialize, Deserialize)]
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
	/// The query string of the request URI (without `?`), if any. May carry a credential: never log it.
	pub query: Option<String>,
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
			.field("query", &self.query.as_ref().map(|_| "[redacted]"))
			.field("headers", &"[redacted]")
			.finish_non_exhaustive()
	}
}

/// Where a received frame arrived: by session, then in receive order within the session.
///
/// Within one session, the order is the order in which the peer sent the frames. Sessions compare
/// by creation, which matches receive order for sessions that do not overlap, such as the successive
/// connections of one [`VcmpClient`](crate::VcmpClient).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Arrival {
	session: u64,
	frame: u64,
}

impl Arrival {
	/// The id of the session that received the frame, see [`Session::id`].
	pub fn session(&self) -> u64 {
		self.session
	}

	/// The frame's position among the frames this session received, starting at 1.
	pub fn frame(&self) -> u64 {
		self.frame
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
	/// A handler is any `async fn(M, Session) -> Result<R, VcmpError>` or closure of that shape.
	/// The `@type` member is removed before the message is deserialized into `M`. The `Ok`
	/// result is serialized as the `ACK` payload (a `()` / `null` result yields an `ACK` without
	/// payload); the error is sent as the `NAK` payload. Convert foreign errors with
	/// [`ResultExt::or_problem`](crate::ResultExt::or_problem) or [`VcmpError::from_error`].
	///
	/// ```ignore
	/// async fn open_lock(open: OpenLock, _session: Session) -> Result<(), VcmpError> {
	///     hardware.open(&open.id).await.or_problem(503, "Lock unreachable")?;
	///     Ok(())
	/// }
	/// handlers.on(open_lock);
	/// // or inline, with the message type on the closure parameter:
	/// handlers.on(|open: OpenLock, session| async move { Ok(()) });
	/// ```
	pub fn on<M, F, Fut, R>(&self, handler: F)
	where
		M: VcmpMessage + Send + 'static,
		F: Fn(M, Session) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Result<R, VcmpError>> + Send + 'static,
		R: Serialize,
	{
		self.register(M::TYPE, true, handler);
	}

	/// Registers the handler for messages with the given `@type`, replacing any previous one.
	///
	/// Like [`HandlerMap::on`] for message types that do not implement [`VcmpMessage`], such as
	/// [`serde_json::Value`]. The message is deserialized as received, including its `@type`.
	pub fn on_type<M, F, Fut, R>(&self, type_: &str, handler: F)
	where
		M: DeserializeOwned + Send + 'static,
		F: Fn(M, Session) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Result<R, VcmpError>> + Send + 'static,
		R: Serialize,
	{
		self.register(type_, false, handler);
	}

	fn register<M, F, Fut, R>(&self, type_: &str, strip_type: bool, handler: F)
	where
		M: DeserializeOwned + Send + 'static,
		F: Fn(M, Session) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Result<R, VcmpError>> + Send + 'static,
		R: Serialize,
	{
		let handler = Arc::new(handler);
		let erased: Handler = Arc::new(move |mut value, session| {
			let handler = handler.clone();
			Box::pin(async move {
				if strip_type {
					if let Value::Object(object) = &mut value {
						object.remove("@type");
					}
				}
				let message: M = serde_json::from_value(value).map_err(|error| {
					VcmpError::bad_request("Invalid message")
						.with_detail(format!("The message could not be deserialized: {error}"))
				})?;
				let limit = session.inner.limits.max_message_bytes.saturating_sub(15);
				let result = handler(message, session).await?;
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
	/// Requires the peer to authenticate the session (server side). `None`: an `AUT` is answered
	/// with `NAK` 400 and the session is authenticated from the start.
	#[cfg(feature = "server")]
	pub authenticator: Option<auth::Authenticator>,
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
	/// Close the session once this frame was written (a `NAK` that ends the session).
	close_after: bool,
}

/// Why a session was closed: the WebSocket close code and reason it sent to the peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseReason {
	/// The WebSocket close code, e.g. 1008 (policy violation) for authentication failures.
	pub code: u16,
	/// The close reason, informative only.
	pub reason: Cow<'static, str>,
}

/// The close code a server uses for authentication failures (policy violation).
pub const POLICY_VIOLATION: u16 = 1008;

/// The close code a client uses when it closes a session for authentication reasons. Browsers only let
/// clients close with 1000 or 3000–4999, so every VCMP client uses 4008; peers treat it like 1008.
pub const CLIENT_POLICY_VIOLATION: u16 = 4008;

pub(crate) type CloseSlot = Arc<Mutex<Option<CloseReason>>>;

/// The longest close reason a close frame can carry (125 bytes of payload minus the code).
const MAX_CLOSE_REASON_BYTES: usize = 123;

/// Whether an endpoint may send this close code (RFC 6455 §7.4).
fn sendable_close_code(code: u16) -> bool {
	matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999)
}

/// What the WebSocket transport passes on for a binary message, which can't be a VCMP frame. It can't
/// parse as a frame either, so it is never mistaken for one.
pub(crate) const BINARY_MESSAGE: &str = "\0binary";

struct AdmittedSink<K> {
	// Drop buffered wire payloads before releasing their permits, including when the writer is aborted.
	sink: K,
	permits: Option<(Permit, Permit)>,
}

type PendingSender = oneshot::Sender<Result<(Option<String>, Arrival), VcmpError>>;
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
	/// Frames received so far. Only the read loop counts, so the count follows receive order.
	frames_received: AtomicU64,
	/// Whether the session is authenticated; true from the start without an authenticator.
	authenticated: watch::Sender<bool>,
	/// The close code and reason sent to the peer, shared with the transport.
	close_reason: CloseSlot,
	/// The payload of the last `ACK` to an `AUT` this session sent (client side).
	authentication: Mutex<Option<Value>>,
	#[cfg(feature = "server")]
	auth: Option<auth::ServerAuth>,
}

/// A VCMP session: one WebSocket connection with its pending sends and heartbeat state.
///
/// Cheap to clone (`Arc` inside); all clones refer to the same session. The session is driven by
/// a task spawned in [`Session::spawn`]; dropping the handles does not close it — call
/// [`Session::close`].
#[derive(Clone)]
pub struct Session {
	inner: Arc<Inner>,
	/// Where the message arrived, on the handle passed to its handler.
	arrival: Option<Arrival>,
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
		Ok(Self::spawn_admitted(options, stream, sink, connection, CloseSlot::default()))
	}

	pub(crate) fn spawn_admitted<S, K>(
		options: SessionOptions,
		stream: S,
		sink: K,
		connection: Permit,
		close_reason: CloseSlot,
	) -> Session
	where
		S: Stream<Item = String> + Send + Unpin + 'static,
		K: Sink<String> + Send + Unpin + 'static,
		K::Error: fmt::Display,
	{
		let (out_tx, out_rx) = mpsc::channel(options.limits.resources.queued_messages.max(1));
		let (control_tx, control_rx) = mpsc::channel(options.limits.resources.control_messages.max(1));
		let (closed, _) = watch::channel(false);
		#[cfg(feature = "server")]
		let auth = options.authenticator.map(auth::ServerAuth::new);
		#[cfg(feature = "server")]
		let authenticated = auth.is_none();
		#[cfg(not(feature = "server"))]
		let authenticated = true;
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
				frames_received: AtomicU64::new(0),
				authenticated: watch::channel(authenticated).0,
				close_reason,
				authentication: Mutex::new(None),
				#[cfg(feature = "server")]
				auth,
			}),
			arrival: None,
		};
		#[cfg(feature = "server")]
		if let Some(auth) = &session.inner.auth {
			session.arm_authentication_timeout(auth);
		}
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

	/// Whether the session is authenticated. Always true for a session without
	/// [`SessionOptions::authenticator`]; messages can be sent only on authenticated sessions.
	pub fn is_authenticated(&self) -> bool {
		*self.inner.authenticated.borrow()
	}

	/// Resolves with `true` once the session is authenticated, or with `false` once it closed
	/// before that.
	pub async fn authenticated(&self) -> bool {
		let mut rx = self.inner.authenticated.subscribe();
		tokio::select! {
			biased;
			result = rx.wait_for(|authenticated| *authenticated) => result.is_ok(),
			_ = self.closed() => self.is_authenticated(),
		}
	}

	/// The payload of the last `ACK` to an `AUT` sent with [`Session::send_authentication`]
	/// ([`Value::Null`] for an `ACK` without payload); `None` before one was acknowledged.
	pub fn authentication(&self) -> Option<Value> {
		self.inner.authentication.lock().unwrap_or_else(|e| e.into_inner()).clone()
	}

	/// The close code and reason this session sent to its peer, if it closed with one.
	pub fn close_reason(&self) -> Option<CloseReason> {
		self.inner.close_reason.lock().unwrap_or_else(|e| e.into_inner()).clone()
	}

	/// Where the message being handled arrived. Set only on the handle passed to a message handler,
	/// and on its clones.
	pub fn arrival(&self) -> Option<Arrival> {
		self.arrival
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
	/// The `@type` member is added from [`VcmpMessage::TYPE`]. Resolves with the parsed `ACK`
	/// payload ([`Value::Null`] when the `ACK` had none), or fails with the peer's `NAK` problem
	/// detail, `503 Session not open` when the session is not open, or `503 Session closed` when
	/// the session closes while the send is outstanding.
	///
	/// The configured request deadline applies; [`Session::send_value_timeout`] sets one per request.
	/// A local acknowledgement timeout returns `504` with [`VcmpError::is_transport`] set to `true`;
	/// a peer NAK returns `false` for any status. Timeout or disconnect leaves delivery and mutation
	/// outcome unknown; retries are caller policy. Dropping this future removes retained correlation
	/// state.
	pub async fn send<M: VcmpMessage>(&self, message: &M) -> Result<Value, VcmpError> {
		self.send_value(&Tagged { type_: M::TYPE, message }).await
	}

	/// Like [`Session::send`], deserializing the `ACK` payload into `R`.
	pub async fn send_as<M, R>(&self, message: &M) -> Result<R, VcmpError>
	where
		M: VcmpMessage,
		R: DeserializeOwned,
	{
		let value = self.send(message).await?;
		serde_json::from_value(value).map_err(|error| {
			VcmpError::internal("Invalid acknowledgement")
				.with_detail(format!("The ACK payload could not be deserialized: {error}"))
				.with_source(error)
		})
	}

	/// Sends any serializable value as the message payload and waits for the acknowledgement.
	///
	/// Unlike [`Session::send`], nothing is added: the value must carry its own `@type` member
	/// for the peer to dispatch it. Meant for untyped messages such as [`serde_json::Value`].
	/// The session's request deadline applies; [`Session::send_value_timeout`] sets one for this send.
	pub async fn send_value<M: Serialize + ?Sized>(&self, message: &M) -> Result<Value, VcmpError> {
		Ok(self.send_with_arrival(message).await?.0)
	}

	/// Like [`Session::send_value`], waiting at most `timeout` for the acknowledgement instead of
	/// the session's [`SessionLimits::request_timeout`]. The deadline may be shorter or longer.
	///
	/// Meant for a session with a long or unbounded request deadline that still needs to bound
	/// one send. On expiry the send fails with the same `504 Acknowledgement timed out` as the
	/// session deadline, with [`VcmpError::is_transport`] set to `true` (kept by clones), and its
	/// correlation state is removed, so a late `ACK` is ignored. Delivery and mutation outcome are
	/// unknown then. Unlike a caller-side `tokio::time::timeout`, the error is VCMP's own. Every
	/// other failure is that of [`Session::send_value`].
	///
	/// The timeout should be nonzero, like the durations of [`SessionLimits`]: a zero timeout still
	/// sends the message and then fails with `504` at once.
	pub async fn send_value_timeout<M: Serialize + ?Sized>(
		&self,
		message: &M,
		timeout: Duration,
	) -> Result<Value, VcmpError> {
		Ok(self.send_until(message, timeout).await?.0)
	}

	/// Like [`Session::send_value`], also returning where the `ACK` arrived. Compare it with a
	/// handler's [`Session::arrival`] to learn whether the peer sent that message before or after
	/// acknowledging this one.
	pub async fn send_with_arrival<M: Serialize + ?Sized>(&self, message: &M) -> Result<(Value, Arrival), VcmpError> {
		self.send_until(message, self.inner.limits.request_timeout).await
	}

	/// Sends a value, waiting at most `timeout` for the `ACK` and its arrival.
	async fn send_until<M: Serialize + ?Sized>(
		&self,
		message: &M,
		timeout: Duration,
	) -> Result<(Value, Arrival), VcmpError> {
		let payload = serialize_bounded(message, self.inner.limits.max_message_bytes.saturating_sub(15))?;
		let (ack, arrival) = self.exchange(payload, timeout).await?;
		let value = match ack {
			None => Value::Null,
			Some(ack) => serde_json::from_str(&ack).map_err(|error| {
				VcmpError::internal("Invalid acknowledgement")
					.with_detail("The ACK payload could not be parsed.")
					.with_source(error)
			})?,
		};
		Ok((value, arrival))
	}

	/// Sends a `MSG` frame with the given raw payload and waits for the acknowledgement.
	///
	/// This is the primitive [`Session::send`] builds on; the payload is not validated. Mostly
	/// useful for tests that need to send malformed messages.
	#[doc(hidden)]
	pub async fn send_payload(&self, payload: String) -> Result<Option<String>, VcmpError> {
		Ok(self.exchange(payload, self.inner.limits.request_timeout).await?.0)
	}

	/// Sends a `MSG` frame with the given raw payload and waits at most `timeout` for the `ACK` and its arrival.
	async fn exchange(&self, payload: String, timeout: Duration) -> Result<(Option<String>, Arrival), VcmpError> {
		if !self.is_authenticated() {
			// Server side: nothing reaches a session before it is authenticated.
			return Err(VcmpError::session_not_open("Cannot send message: the session is not authenticated."));
		}
		self.exchange_frame(Frame::message(payload), timeout).await
	}

	/// Authenticates this session with its peer (client side): sends an `AUT` frame with the
	/// credential and waits at most `timeout` for the answer.
	///
	/// Resolves with the `ACK` payload ([`Value::Null`] without one, or when it is not a JSON object; an
	/// invalid `expiresIn` is removed), which is also kept as
	/// [`Session::authentication`]. Fails with the peer's `NAK` (`400`, `401` or `403`), `400` when
	/// the credential is not a JSON object, or a local transport error such as
	/// `504 Acknowledgement timed out` when the peer does not answer (an older peer that does not
	/// know `AUT`). The credential is never logged.
	pub async fn send_authentication(&self, credential: &Value, timeout: Duration) -> Result<Value, VcmpError> {
		if !credential.is_object() {
			return Err(
				VcmpError::bad_request("Invalid credential").with_detail("The credential must be a JSON object.")
			);
		}
		let payload = serialize_bounded(credential, self.inner.limits.max_message_bytes.saturating_sub(15))?;
		let (ack, _) = self.exchange_frame(Frame::authenticate(payload), timeout).await?;
		// #34 "Malformed ACK": a payload that is not a JSON object counts as no payload, an expiresIn that
		// is not a non-negative integer as absent; the session is authenticated either way.
		let value = match ack.map(|ack| serde_json::from_str::<Value>(&ack)) {
			None => Value::Null,
			Some(Ok(Value::Object(mut ack))) => {
				if ack.get("expiresIn").is_some_and(|expires_in| expires_in.as_u64().is_none()) {
					warn!(session = self.id(), "ignoring an invalid expiresIn in the authentication ACK");
					ack.remove("expiresIn");
				}
				Value::Object(ack)
			}
			Some(_) => {
				warn!(session = self.id(), "ignoring an authentication ACK payload that is not a JSON object");
				Value::Null
			}
		};
		*self.inner.authentication.lock().unwrap_or_else(|e| e.into_inner()) = Some(value.clone());
		Ok(value)
	}

	/// Sends a `MSG` or `AUT` frame and waits at most `timeout` for the `ACK` and its arrival.
	async fn exchange_frame(&self, frame: Frame, timeout: Duration) -> Result<(Option<String>, Arrival), VcmpError> {
		if frame.serialize_len() > self.inner.limits.max_message_bytes {
			return Err(VcmpError::new(413, "Message too large"));
		}
		let permits = self.reserve(Resource::Request, 0)?;
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
		// Arm the answer timer before the frame can reach the transport.
		let deadline = tokio::time::Instant::now() + timeout;
		self.enqueue(frame.serialize(), false)?;
		// Only the queued copy owns byte permits; do not retain the original payload while awaiting an ACK.
		drop(frame);
		match tokio::time::timeout_at(deadline, rx).await {
			Ok(Ok(result)) => result,
			Ok(Err(_)) => Err(VcmpError::session_closed("The session closed before acknowledgement.")),
			Err(_) => Err(VcmpError::acknowledgement_timeout()),
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
	/// anything would go undetected. If the first heartbeat has already arrived, its negotiated
	/// schedule remains in effect and this initial expectation does nothing.
	pub fn expect_heartbeat(&self, timeout: Duration) {
		let mut heartbeat = self.inner.heartbeat.lock().unwrap_or_else(|e| e.into_inner());
		// The transport reader can accept the server's first HBT before the client's connect task reaches here.
		// Check and install under the same lock as handle_heartbeat so neither order can leave a stale watchdog.
		if !self.is_open() || self.inner.heartbeats_received.load(Ordering::Relaxed) != 0 {
			return;
		}
		let session = self.clone();
		let watchdog = tokio::spawn(async move {
			tokio::time::sleep(timeout).await;
			warn!(session = session.id(), "did not receive the expected heartbeat, closing session");
			session.close();
		})
		.abort_handle();
		let previous = heartbeat.watchdog.replace(watchdog);
		if let Some(previous) = previous {
			previous.abort();
		}
	}

	/// Closes the session with a WebSocket close code and reason (e.g. [`POLICY_VIOLATION`]).
	/// The first code set wins; see [`Session::close`].
	///
	/// The reason is truncated to 123 bytes (at a character boundary), the most a close frame can
	/// carry. A code that may not be sent (outside 1000–1003, 1007–1014 and 3000–4999, e.g. 1005,
	/// 1006 or 1015) is not: the session closes without a code then, as with [`Session::close`].
	pub fn close_with(&self, code: u16, reason: impl Into<Cow<'static, str>>) {
		if sendable_close_code(code) {
			let mut reason = reason.into();
			if reason.len() > MAX_CLOSE_REASON_BYTES {
				let mut end = MAX_CLOSE_REASON_BYTES;
				while !reason.is_char_boundary(end) {
					end -= 1;
				}
				reason = Cow::Owned(reason[..end].to_owned());
			}
			self.set_close_reason(code, reason);
		} else {
			warn!(session = self.id(), code, "not sending a close code that may not be sent");
		}
		self.close();
	}

	fn set_close_reason(&self, code: u16, reason: Cow<'static, str>) {
		let mut slot = self.inner.close_reason.lock().unwrap_or_else(|e| e.into_inner());
		if slot.is_none() && self.is_open() {
			*slot = Some(CloseReason { code, reason });
		}
	}

	/// Sends a `NAK` and closes the session with the given code once it was written. Closes at
	/// once when the `NAK` cannot be queued.
	#[cfg_attr(not(feature = "server"), allow(dead_code))]
	fn nak_and_close(&self, id: &str, error: &VcmpError, code: u16, reason: &'static str) {
		self.set_close_reason(code, Cow::Borrowed(reason));
		let text = Frame::nak(id, Some(problem_payload(error))).serialize();
		if self.enqueue_with(text, true, true).is_err() {
			self.close();
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
		let message = raw.starts_with("MSG");
		if message && !self.is_authenticated() {
			return false;
		}
		let control = !message && !raw.starts_with("AUT");
		self.enqueue(raw, control).is_ok()
	}

	fn enqueue(&self, text: String, control: bool) -> Result<(), VcmpError> {
		self.enqueue_with(text, control, false)
	}

	fn enqueue_with(&self, text: String, control: bool, close_after: bool) -> Result<(), VcmpError> {
		if !self.is_open() {
			return Err(VcmpError::session_not_open("The session is not open."));
		}
		if text.len() > self.inner.limits.max_message_bytes {
			return Err(VcmpError::new(413, "Message too large"));
		}
		let resource = if control { Resource::Control } else { Resource::Data };
		let permits = self.reserve(resource, text.len())?;
		let tx = if control { &self.inner.control_tx } else { &self.inner.out_tx };
		tx.try_send(Outgoing { text, _permits: permits, close_after }).map_err(|_| VcmpError::transport_overloaded())
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

	fn handle_ack(&self, id: &str, payload: Option<String>, arrival: Arrival) {
		if let Some(tx) = self.take_pending(id) {
			let _ = tx.send(Ok((payload, arrival)));
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
			let _ = tx.send(Err(error.into_peer_error()));
		} else {
			self.diagnose("late or unknown NAK");
		}
	}

	fn take_pending(&self, id: &str) -> Option<PendingSender> {
		self.inner.pending.lock().unwrap_or_else(|e| e.into_inner()).remove(id).map(|(sender, _)| sender)
	}

	fn handle_message(&self, id: String, payload: String, arrival: Arrival, tasks: &mut JoinSet<()>) {
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
		let session = Session { inner: self.inner.clone(), arrival: Some(arrival) };
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
		self.write(Frame::nak(id, Some(problem_payload(error))));
	}

	fn handle_incoming(&self, raw: String, tasks: &mut JoinSet<()>) {
		if raw.len() > self.inner.limits.max_message_bytes {
			self.close();
			return;
		}
		#[cfg(feature = "server")]
		if let Some(auth) = &self.inner.auth {
			if !self.admit_raw(auth, &raw) {
				return;
			}
		}
		if raw == BINARY_MESSAGE {
			// Unsupported binary messages are dropped (handled above while authentication is pending).
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
		// Never log an AUT payload: it is a credential.
		trace!(session = self.id(), kind = frame.kind(), bytes = raw.len(), "received frame");
		let arrival =
			Arrival { session: self.inner.id, frame: self.inner.frames_received.fetch_add(1, Ordering::Relaxed) + 1 };
		#[cfg(feature = "server")]
		if let Some(auth) = &self.inner.auth {
			let Some(frame) = self.admit(auth, frame, tasks) else {
				return;
			};
			return self.dispatch(frame, arrival, tasks);
		}
		self.dispatch(frame, arrival, tasks);
	}

	fn dispatch(&self, frame: Frame, arrival: Arrival, tasks: &mut JoinSet<()>) {
		match frame {
			Frame::Heartbeat { interval } => self.handle_heartbeat(interval),
			Frame::Ack { id, payload } => self.handle_ack(&id, payload, arrival),
			Frame::Nak { id, payload } => self.handle_nak(&id, payload),
			Frame::Message { id, payload } => self.handle_message(id, payload, arrival, tasks),
			Frame::Authenticate { id, .. } => {
				// This side does not require authentication (a client, or an endpoint without the opt-in).
				self.nak(
					&id,
					&VcmpError::bad_request("Authentication not supported")
						.with_detail("This endpoint does not require authentication."),
				);
			}
		}
	}

	async fn drive<S, K>(
		self,
		mut stream: S,
		sink: K,
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
			let mut transport = AdmittedSink { sink, permits: None };
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
				let close_after = outgoing.close_after;
				transport.permits = Some(outgoing._permits);
				let result = tokio::select! {
					_ = closing.changed() => break,
					result = tokio::time::timeout(writer_session.inner.limits.write_timeout,
						transport.sink.send(outgoing.text)) => result,
				};
				if !matches!(result, Ok(Ok(()))) {
					writer_session.close();
					break;
				}
				transport.permits = None;
				if close_after {
					writer_session.close();
					break;
				}
			}
			let _ = tokio::time::timeout(writer_session.inner.limits.shutdown_timeout, transport.sink.close()).await;
		});
		let mut tasks = JoinSet::new();
		#[cfg(feature = "server")]
		let reading = match &self.inner.auth {
			Some(auth) => self.start_authentication(auth).await,
			None => true,
		};
		#[cfg(not(feature = "server"))]
		let reading = true;
		if reading {
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
		#[cfg(feature = "server")]
		if let Some(auth) = &self.inner.auth {
			auth.finish();
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

/// A message with its `@type` member, as sent on the wire.
#[derive(Serialize)]
struct Tagged<'a, M: Serialize> {
	#[serde(rename = "@type")]
	type_: &'static str,
	#[serde(flatten)]
	message: &'a M,
}

/// A problem detail as `NAK` payload.
fn problem_payload(error: &VcmpError) -> String {
	serde_json::to_string(error).unwrap_or_else(|serialization_error| {
		let _ = serialization_error;
		let fallback =
			VcmpError::internal("Message handling failed").with_detail("The handler error could not be serialized.");
		serde_json::to_string(&fallback).expect("a plain problem detail serializes")
	})
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
