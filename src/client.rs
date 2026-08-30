//! A reconnecting VCMP WebSocket client (feature `client`).
//!
//! ```ignore
//! let client = VcmpClient::builder("ws://localhost:2000/drivers/kerong")
//!     .header("Authorization", token)
//!     .reconnect(Backoff::exponential(Duration::from_secs(1), Duration::from_secs(30)))
//!     .build();
//! client.on::<Hello, _, _, _, _>(|msg, session| async move { Ok::<_, VcmpError>(()) });
//! client.on_open(|session| async move { /* announce devices */ });
//! client.start();
//! let result = client.send(&Hello { from: "driver".into() }).await?;
//! client.stop();
//! ```
//!
//! Lifecycle: [`VcmpClient::start`] connects and keeps reconnecting (with backoff) whenever the
//! session closes, until [`VcmpClient::stop`]. The server initiates the heartbeat; the client
//! expects the first `HBT` within [`ClientBuilder::initial_heartbeat_timeout`] and closes (and
//! reconnects) otherwise, so a half-open connection never leaves sends pending forever.

use crate::error::VcmpError;
use crate::session::{HandlerMap, Session, SessionOptions, VcmpMessage};
use crate::ws::{TransportOptions, spawn_session};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tracing::{debug, info, warn};

/// The reconnect delay policy.
#[derive(Debug, Clone, PartialEq)]
pub struct Backoff {
	/// The delay before the first reconnect attempt.
	pub initial: Duration,
	/// The upper bound of the delay.
	pub max: Duration,
	/// The factor the delay grows by per consecutive failed attempt.
	pub multiplier: f64,
	/// Whether to randomize each delay within `[delay / 2, delay]` (recommended, so a fleet
	/// does not reconnect in lockstep after a server restart).
	pub jitter: bool,
}

impl Backoff {
	/// Exponential backoff (factor 2) with jitter from `initial` up to `max`.
	pub fn exponential(initial: Duration, max: Duration) -> Self {
		Backoff { initial, max: max.max(initial), multiplier: 2.0, jitter: true }
	}

	/// A fixed delay without jitter.
	pub fn fixed(delay: Duration) -> Self {
		Backoff { initial: delay, max: delay, multiplier: 1.0, jitter: false }
	}

	/// Disables jitter.
	pub fn without_jitter(mut self) -> Self {
		self.jitter = false;
		self
	}

	/// The delay before the given (zero-based) consecutive attempt.
	pub fn delay(&self, attempt: u32) -> Duration {
		let factor = self.multiplier.max(1.0).powi(attempt.min(64) as i32);
		let delay = Duration::from_secs_f64((self.initial.as_secs_f64() * factor).min(self.max.as_secs_f64()));
		if self.jitter && !delay.is_zero() {
			let random = (getrandom::u64().unwrap_or(0) % 1_000_000) as f64 / 1_000_000.0;
			delay.mul_f64(0.5 + random / 2.0)
		} else {
			delay
		}
	}
}

impl Default for Backoff {
	/// Exponential from 1 s to 30 s with jitter.
	fn default() -> Self {
		Backoff::exponential(Duration::from_secs(1), Duration::from_secs(30))
	}
}

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type OpenHook = Arc<dyn Fn(Session) -> BoxFuture + Send + Sync>;
type CloseHook = Arc<dyn Fn() -> BoxFuture + Send + Sync>;

/// Configures a [`VcmpClient`].
#[derive(Debug, Clone)]
pub struct ClientBuilder {
	url: String,
	headers: Vec<(String, String)>,
	backoff: Backoff,
	initial_heartbeat_timeout: Duration,
	connect_timeout: Duration,
	transport: TransportOptions,
}

impl ClientBuilder {
	/// Adds a header to the WebSocket handshake request (e.g. `Authorization`).
	pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
		self.headers.push((name.into(), value.into()));
		self
	}

	/// The reconnect policy. Default: [`Backoff::default`] (exponential 1 s → 30 s with jitter).
	pub fn reconnect(mut self, backoff: Backoff) -> Self {
		self.backoff = backoff;
		self
	}

	/// The time within which the server must initiate the heartbeat after the connection opens;
	/// the session is closed (and reconnected) otherwise. Zero disables the expectation.
	/// Default: 60 s.
	pub fn initial_heartbeat_timeout(mut self, timeout: Duration) -> Self {
		self.initial_heartbeat_timeout = timeout;
		self
	}

	/// The timeout for establishing a connection (TCP + WebSocket handshake). Default: 30 s.
	pub fn connect_timeout(mut self, timeout: Duration) -> Self {
		self.connect_timeout = timeout;
		self
	}

	/// Fragments outgoing messages into WebSocket frames of at most `size` bytes (the Java client
	/// uses 8 KB). Default: no fragmentation — each message is sent as one frame.
	pub fn fragment_size(mut self, size: Option<usize>) -> Self {
		self.transport.fragment_size = size;
		self
	}

	/// The maximum size of an incoming message. Default: 64 MiB.
	pub fn max_message_size(mut self, size: usize) -> Self {
		self.transport.max_message_size = Some(size);
		self
	}

	/// Builds the client. It is not started; call [`VcmpClient::start`].
	pub fn build(self) -> VcmpClient {
		let (connected, _) = watch::channel(false);
		VcmpClient {
			inner: Arc::new(ClientInner {
				config: self,
				handlers: Arc::new(HandlerMap::new()),
				on_open: Mutex::new(None),
				on_close: Mutex::new(None),
				state: Mutex::new(ClientState::default()),
				connected,
			}),
		}
	}
}

#[derive(Default)]
struct ClientState {
	running: bool,
	generation: u64,
	task: Option<JoinHandle<()>>,
	stop: Option<watch::Sender<Stop>>,
	session: Option<Session>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
	No,
	/// `stop()`: close the session, notify `on_close`.
	Notify,
	/// `start()` replacing a running client: close the old session silently.
	Quiet,
}

struct ClientInner {
	config: ClientBuilder,
	handlers: Arc<HandlerMap>,
	on_open: Mutex<Option<OpenHook>>,
	on_close: Mutex<Option<CloseHook>>,
	state: Mutex<ClientState>,
	connected: watch::Sender<bool>,
}

/// A VCMP client that connects to a URL and reconnects until stopped.
///
/// Cheap to clone; all clones control the same client.
#[derive(Clone)]
pub struct VcmpClient {
	inner: Arc<ClientInner>,
}

impl VcmpClient {
	/// Starts configuring a client for the given `ws://` or `wss://` URL.
	pub fn builder(url: impl Into<String>) -> ClientBuilder {
		ClientBuilder {
			url: url.into(),
			headers: Vec::new(),
			backoff: Backoff::default(),
			initial_heartbeat_timeout: Duration::from_secs(60),
			connect_timeout: Duration::from_secs(30),
			transport: TransportOptions::default(),
		}
	}

	/// The URL this client connects to.
	pub fn url(&self) -> &str {
		&self.inner.config.url
	}

	/// Registers the handler for messages of type `M`. See [`HandlerMap::on`].
	pub fn on<M, F, Fut, R, E>(&self, handler: F) -> &Self
	where
		M: VcmpMessage + Send + 'static,
		F: Fn(M, Session) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Result<R, E>> + Send + 'static,
		R: Serialize,
		E: Into<VcmpError>,
	{
		self.inner.handlers.on::<M, F, Fut, R, E>(handler);
		self
	}

	/// Registers the handler for messages with the given `@type`. See [`HandlerMap::on_type`].
	pub fn on_type<M, F, Fut, R, E>(&self, type_: &str, handler: F) -> &Self
	where
		M: DeserializeOwned + Send + 'static,
		F: Fn(M, Session) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Result<R, E>> + Send + 'static,
		R: Serialize,
		E: Into<VcmpError>,
	{
		self.inner.handlers.on_type::<M, F, Fut, R, E>(type_, handler);
		self
	}

	/// Removes the handler for the given `@type`.
	pub fn off(&self, type_: &str) -> &Self {
		self.inner.handlers.off(type_);
		self
	}

	/// The handler map, shared with every session of this client.
	pub fn handlers(&self) -> &Arc<HandlerMap> {
		&self.inner.handlers
	}

	/// Sets the hook that runs each time a connection is established, with the new session.
	pub fn on_open<F, Fut>(&self, hook: F) -> &Self
	where
		F: Fn(Session) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = ()> + Send + 'static,
	{
		*self.inner.on_open.lock().unwrap_or_else(|e| e.into_inner()) =
			Some(Arc::new(move |session| Box::pin(hook(session))));
		self
	}

	/// Sets the hook that runs each time the connection closes (also on [`VcmpClient::stop`]).
	pub fn on_close<F, Fut>(&self, hook: F) -> &Self
	where
		F: Fn() -> Fut + Send + Sync + 'static,
		Fut: Future<Output = ()> + Send + 'static,
	{
		*self.inner.on_close.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(move || Box::pin(hook())));
		self
	}

	/// Connects and keeps the connection alive until [`VcmpClient::stop`].
	///
	/// Calling `start` on a running client replaces its connection: a pending reconnect is
	/// cancelled and the current session is closed without notifying `on_close`. Must be called
	/// from within a tokio runtime.
	pub fn start(&self) {
		let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
		Self::stop_locked(&mut state, Stop::Quiet);
		state.running = true;
		state.generation += 1;
		let generation = state.generation;
		let (stop_tx, stop_rx) = watch::channel(Stop::No);
		state.stop = Some(stop_tx);
		info!(url = self.url(), "starting client");
		state.task = Some(tokio::spawn(Self::run(self.inner.clone(), generation, stop_rx)));
	}

	/// Closes the connection, fails pending sends with `503 Session closed` and stops
	/// reconnecting. In-flight handlers are not awaited.
	pub fn stop(&self) {
		let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
		if state.running {
			info!(url = self.url(), "stopping client");
		}
		Self::stop_locked(&mut state, Stop::Notify);
	}

	fn stop_locked(state: &mut ClientState, how: Stop) {
		state.running = false;
		if let Some(stop) = state.stop.take() {
			let _ = stop.send(how);
		}
		// The run task ends on its own after closing the session; the handle is not awaited so
		// that stop() stays synchronous, but a leftover task is dropped (not aborted — it must
		// run its shutdown).
		state.task.take();
	}

	/// Whether a session is currently open.
	pub fn is_connected(&self) -> bool {
		self.session().is_some_and(|session| session.is_open())
	}

	/// The current session, if connected.
	pub fn session(&self) -> Option<Session> {
		self.inner.state.lock().unwrap_or_else(|e| e.into_inner()).session.clone()
	}

	/// Resolves once the client is connected (immediately if it already is).
	pub async fn wait_connected(&self) {
		self.wait_connection_state(true).await;
	}

	/// Resolves once the client is disconnected (immediately if it already is).
	pub async fn wait_disconnected(&self) {
		self.wait_connection_state(false).await;
	}

	async fn wait_connection_state(&self, connected: bool) {
		let mut rx = self.inner.connected.subscribe();
		while *rx.borrow_and_update() != connected {
			if rx.changed().await.is_err() {
				return;
			}
		}
	}

	/// Sends a message on the current session. See [`Session::send`].
	///
	/// Fails with `503 Not connected` when there is no session.
	pub async fn send<M: Serialize + ?Sized>(&self, message: &M) -> Result<Value, VcmpError> {
		self.current_session()?.send(message).await
	}

	/// Sends a message on the current session, deserializing the result. See [`Session::send_as`].
	pub async fn send_as<M, R>(&self, message: &M) -> Result<R, VcmpError>
	where
		M: Serialize + ?Sized,
		R: DeserializeOwned,
	{
		self.current_session()?.send_as(message).await
	}

	fn current_session(&self) -> Result<Session, VcmpError> {
		self.session().ok_or_else(|| VcmpError::not_connected("Cannot send message: the client has no session."))
	}

	async fn run(inner: Arc<ClientInner>, generation: u64, mut stop: watch::Receiver<Stop>) {
		let mut attempt = 0u32;
		loop {
			let connect = tokio::time::timeout(inner.config.connect_timeout, Self::connect(&inner));
			let connected = tokio::select! {
				result = connect => result,
				_ = stop.changed() => break,
			};
			match connected {
				Ok(Ok(session)) => {
					attempt = 0;
					inner.set_session(generation, Some(session.clone()));
					if !inner.config.initial_heartbeat_timeout.is_zero() {
						session.expect_heartbeat(inner.config.initial_heartbeat_timeout);
					}
					if let Some(hook) = inner.open_hook() {
						hook(session.clone()).await;
					}
					tokio::select! {
						_ = session.closed() => {}
						_ = stop.changed() => {
							session.close();
							session.closed().await;
						}
					}
					inner.set_session(generation, None);
					let how = *stop.borrow();
					if how != Stop::Quiet {
						debug!(url = inner.config.url, "session closed");
						if let Some(hook) = inner.close_hook() {
							hook().await;
						}
					}
				}
				Ok(Err(error)) => {
					if attempt == 0 {
						warn!(url = inner.config.url, "failed to connect: {error}");
					} else {
						debug!(url = inner.config.url, attempt, "failed to connect: {error}");
					}
				}
				Err(_) => warn!(url = inner.config.url, "timed out connecting"),
			}
			if *stop.borrow() != Stop::No {
				break;
			}
			let delay = inner.config.backoff.delay(attempt);
			attempt = attempt.saturating_add(1);
			debug!(url = inner.config.url, ?delay, "scheduling reconnect");
			tokio::select! {
				_ = tokio::time::sleep(delay) => {}
				_ = stop.changed() => break,
			}
		}
		debug!(url = inner.config.url, "client task ended");
	}

	async fn connect(inner: &Arc<ClientInner>) -> Result<Session, ConnectError> {
		let config = &inner.config;
		let mut request = config.url.as_str().into_client_request()?;
		for (name, value) in &config.headers {
			let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| ConnectError::Header(name.clone()))?;
			let value = HeaderValue::from_str(value).map_err(|_| ConnectError::Header(name.to_string()))?;
			request.headers_mut().append(name, value);
		}
		debug!(url = config.url, "connecting");
		let (ws, _response) =
			tokio_tungstenite::connect_async_with_config(request, Some(config.transport.websocket_config()), false)
				.await?;
		info!(url = config.url, "connected");
		let session = spawn_session(SessionOptions::new(inner.handlers.clone()), &config.transport, ws);
		Ok(session)
	}
}

impl ClientInner {
	fn open_hook(&self) -> Option<OpenHook> {
		self.on_open.lock().unwrap_or_else(|e| e.into_inner()).clone()
	}

	fn close_hook(&self) -> Option<CloseHook> {
		self.on_close.lock().unwrap_or_else(|e| e.into_inner()).clone()
	}

	/// Sets the current session, unless a later `start()` has replaced this generation.
	fn set_session(&self, generation: u64, session: Option<Session>) {
		let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
		if state.generation != generation {
			debug!("ignoring session event of a replaced connection");
			return;
		}
		let connected = session.is_some();
		state.session = session;
		drop(state);
		self.connected.send_replace(connected);
	}
}

impl fmt::Debug for VcmpClient {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("VcmpClient").field("url", &self.url()).field("connected", &self.is_connected()).finish()
	}
}

#[derive(Debug, thiserror::Error)]
enum ConnectError {
	#[error("invalid header {0:?}")]
	Header(String),
	#[error(transparent)]
	WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn backoff_grows_and_caps() {
		let backoff = Backoff::exponential(Duration::from_secs(1), Duration::from_secs(30)).without_jitter();
		assert_eq!(backoff.delay(0), Duration::from_secs(1));
		assert_eq!(backoff.delay(1), Duration::from_secs(2));
		assert_eq!(backoff.delay(4), Duration::from_secs(16));
		assert_eq!(backoff.delay(5), Duration::from_secs(30));
		assert_eq!(backoff.delay(1000), Duration::from_secs(30));
	}

	#[test]
	fn backoff_jitter_stays_within_half_to_full() {
		let backoff = Backoff::exponential(Duration::from_secs(8), Duration::from_secs(8));
		for _ in 0..100 {
			let delay = backoff.delay(3);
			assert!(delay >= Duration::from_secs(4) && delay <= Duration::from_secs(8), "{delay:?}");
		}
	}

	#[test]
	fn fixed_backoff_is_constant() {
		let backoff = Backoff::fixed(Duration::from_secs(10));
		assert_eq!(backoff.delay(0), Duration::from_secs(10));
		assert_eq!(backoff.delay(7), Duration::from_secs(10));
	}
}
