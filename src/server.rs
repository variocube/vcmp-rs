//! A VCMP WebSocket server (feature `server`).
//!
//! A [`VcmpServer`] accepts WebSocket upgrades and routes them by path to [`Endpoint`]s, each with
//! its own handlers, session set and connect/disconnect hooks — like `@VcmpEndpoint(path = …)`
//! classes in `vcmp-spring`. Path patterns may contain parameters (`/drivers/{driver}`), which are
//! available together with the request headers in [`ConnectInfo`].
//!
//! ```ignore
//! let server = VcmpServer::builder().heartbeat_interval(Duration::from_secs(20)).build();
//! let drivers = server.endpoint("/drivers/{driver}");
//! drivers.on::<DeviceAdded, _, _, _, _>(|msg, session| async move { Ok::<_, VcmpError>(()) });
//! drivers.on_session_connected(|session| async move {
//!     tracing::info!(driver = session.connect_info().unwrap().param("driver"), "driver connected");
//! });
//! let handle = server.bind("0.0.0.0:2000").await?;
//! ```
//!
//! The server initiates the heartbeat on every session (20 s by default).

use crate::error::VcmpError;
use crate::session::{ConnectInfo, HandlerMap, Session, SessionOptions, VcmpMessage};
use crate::ws::{Headers, TransportOptions, spawn_session};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::task::JoinHandle;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tracing::{debug, info, warn};

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type SessionHook = Arc<dyn Fn(Session) -> BoxFuture + Send + Sync>;

/// Configures a [`VcmpServer`].
#[derive(Debug, Clone)]
pub struct ServerBuilder {
	heartbeat_interval: Duration,
	transport: TransportOptions,
}

impl ServerBuilder {
	/// The heartbeat interval the server initiates on every session. Default: 20 s.
	pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
		self.heartbeat_interval = interval;
		self
	}

	/// Fragments outgoing messages into WebSocket frames of at most `size` bytes.
	/// `None` (the default) or `Some(0)` disables fragmentation.
	pub fn fragment_size(mut self, size: Option<usize>) -> Self {
		self.transport.fragment_size = size;
		self
	}

	/// The maximum size of an incoming message. Default: 64 MiB.
	pub fn max_message_size(mut self, size: usize) -> Self {
		self.transport.max_message_size = Some(size);
		self
	}

	/// Builds the server. Add endpoints, then bind it or mount them in an axum router.
	pub fn build(self) -> VcmpServer {
		VcmpServer { inner: Arc::new(ServerInner { config: self, endpoints: RwLock::new(Vec::new()) }) }
	}
}

struct ServerInner {
	config: ServerBuilder,
	endpoints: RwLock<Vec<Endpoint>>,
}

/// A VCMP server: a set of endpoints served on one listening socket.
#[derive(Clone)]
pub struct VcmpServer {
	inner: Arc<ServerInner>,
}

impl VcmpServer {
	/// Starts configuring a server.
	pub fn builder() -> ServerBuilder {
		ServerBuilder { heartbeat_interval: Duration::from_secs(20), transport: TransportOptions::default() }
	}

	/// Adds an endpoint for the given path pattern (e.g. `/drivers/{driver}`) and returns it.
	///
	/// The bare server matches patterns segment by segment in registration order; `{name}`
	/// matches a single non-empty segment. When mounted in axum, its router controls matching.
	pub fn endpoint(&self, pattern: &str) -> Endpoint {
		let endpoint = Endpoint {
			inner: Arc::new(EndpointInner {
				config: self.inner.config.clone(),
				pattern: PathPattern::parse(pattern),
				handlers: Arc::new(HandlerMap::new()),
				sessions: Mutex::new(HashMap::new()),
				on_connected: Mutex::new(None),
				on_disconnected: Mutex::new(None),
			}),
		};
		self.inner.endpoints.write().unwrap_or_else(|e| e.into_inner()).push(endpoint.clone());
		endpoint
	}

	/// The endpoints of this server.
	pub fn endpoints(&self) -> Vec<Endpoint> {
		self.inner.endpoints.read().unwrap_or_else(|e| e.into_inner()).clone()
	}

	/// Closes all currently connected sessions, failing their pending sends.
	///
	/// This does not stop the listener or prevent new sessions. When hosting with axum, stop
	/// accepting requests before calling this; upgraded connections outlive the HTTP server.
	/// Pending sends are settled before returning; disconnect hooks finish asynchronously.
	pub async fn close_sessions(&self) {
		let sessions: Vec<_> = self.endpoints().iter().flat_map(Endpoint::sessions).collect();
		for session in &sessions {
			session.close();
		}
		for session in &sessions {
			session.closed().await;
		}
	}

	/// Binds the listening socket and starts accepting connections in a background task.
	pub async fn bind(&self, addr: impl ToSocketAddrs) -> std::io::Result<ServerHandle> {
		let listener = TcpListener::bind(addr).await?;
		let local_addr = listener.local_addr()?;
		info!(%local_addr, "server listening");
		let server = self.inner.clone();
		let accept = tokio::spawn(async move {
			loop {
				match listener.accept().await {
					Ok((stream, remote_addr)) => {
						tokio::spawn(Self::handle_connection(server.clone(), stream, remote_addr));
					}
					Err(error) => {
						warn!("error accepting connection: {error}");
						tokio::time::sleep(Duration::from_millis(100)).await;
					}
				}
			}
		});
		Ok(ServerHandle { server: self.inner.clone(), local_addr, accept })
	}

	async fn handle_connection(server: Arc<ServerInner>, stream: TcpStream, remote_addr: SocketAddr) {
		let mut matched: Option<(Endpoint, ConnectInfo)> = None;
		#[allow(clippy::result_large_err)] // tungstenite's callback signature
		let callback = |request: &Request, response: Response| -> Result<Response, ErrorResponse> {
			let path = request.uri().path();
			let endpoints = server.endpoints.read().unwrap_or_else(|e| e.into_inner());
			match endpoints.iter().find_map(|endpoint| endpoint.inner.pattern.matches(path).map(|p| (endpoint, p))) {
				Some((endpoint, params)) => {
					matched = Some((
						endpoint.clone(),
						ConnectInfo {
							path: path.to_owned(),
							params,
							headers: Headers(request.headers()).to_vec(),
							remote_addr: Some(remote_addr),
						},
					));
					Ok(response)
				}
				None => {
					debug!(path, %remote_addr, "no endpoint for path");
					let mut response = ErrorResponse::new(Some("No such endpoint".to_owned()));
					*response.status_mut() = StatusCode::NOT_FOUND;
					Err(response)
				}
			}
		};
		let config = server.config.transport.websocket_config();
		let ws = match tokio_tungstenite::accept_hdr_async_with_config(stream, callback, Some(config)).await {
			Ok(ws) => ws,
			Err(error) => {
				debug!(%remote_addr, "handshake failed: {error}");
				return;
			}
		};
		let Some((endpoint, connect_info)) = matched else {
			return;
		};
		endpoint.serve_websocket(connect_info, ws).await;
	}
}

impl fmt::Debug for VcmpServer {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("VcmpServer").field("endpoints", &self.endpoints()).finish()
	}
}

/// A running server. Dropping the handle does not stop it; call [`ServerHandle::stop`].
pub struct ServerHandle {
	server: Arc<ServerInner>,
	local_addr: SocketAddr,
	accept: JoinHandle<()>,
}

impl fmt::Debug for ServerHandle {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ServerHandle").field("local_addr", &self.local_addr).finish()
	}
}

impl ServerHandle {
	/// The address the server listens on.
	pub fn local_addr(&self) -> SocketAddr {
		self.local_addr
	}

	/// Stops accepting connections and closes every session (failing their pending sends).
	pub async fn stop(self) {
		self.accept.abort();
		let _ = self.accept.await;
		VcmpServer { inner: self.server }.close_sessions().await;
		info!(local_addr = %self.local_addr, "server stopped");
	}
}

struct EndpointInner {
	config: ServerBuilder,
	pattern: PathPattern,
	handlers: Arc<HandlerMap>,
	sessions: Mutex<HashMap<u64, Session>>,
	on_connected: Mutex<Option<SessionHook>>,
	on_disconnected: Mutex<Option<SessionHook>>,
}

impl EndpointInner {
	fn hook(&self, slot: &Mutex<Option<SessionHook>>) -> Option<SessionHook> {
		slot.lock().unwrap_or_else(|e| e.into_inner()).clone()
	}
}

/// An endpoint of a [`VcmpServer`]: a path pattern with handlers, hooks and the set of connected
/// sessions. Cheap to clone.
#[derive(Clone)]
pub struct Endpoint {
	inner: Arc<EndpointInner>,
}

impl Endpoint {
	#[cfg(feature = "axum")]
	pub(crate) fn transport(&self) -> &TransportOptions {
		&self.inner.config.transport
	}

	pub(crate) async fn serve_websocket<S>(&self, connect_info: ConnectInfo, ws: WebSocketStream<S>)
	where
		S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
	{
		let remote_addr = connect_info.remote_addr;
		let options = SessionOptions { handlers: self.inner.handlers.clone(), connect_info: Some(connect_info) };
		let session = spawn_session(options, &self.inner.config.transport, ws);
		info!(session = session.id(), ?remote_addr, path = self.path(), "session connected");
		self.inner.sessions.lock().unwrap_or_else(|e| e.into_inner()).insert(session.id(), session.clone());
		session.initiate_heartbeat(self.inner.config.heartbeat_interval);
		if let Some(hook) = self.inner.hook(&self.inner.on_connected) {
			hook(session.clone()).await;
		}
		session.closed().await;
		self.inner.sessions.lock().unwrap_or_else(|e| e.into_inner()).remove(&session.id());
		info!(session = session.id(), ?remote_addr, path = self.path(), "session disconnected");
		if let Some(hook) = self.inner.hook(&self.inner.on_disconnected) {
			hook(session).await;
		}
	}

	/// The path pattern of this endpoint.
	pub fn path(&self) -> &str {
		&self.inner.pattern.source
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

	/// The handler map, shared with every session of this endpoint.
	pub fn handlers(&self) -> &Arc<HandlerMap> {
		&self.inner.handlers
	}

	/// Sets the hook that runs when a session connected (after the heartbeat was initiated).
	pub fn on_session_connected<F, Fut>(&self, hook: F) -> &Self
	where
		F: Fn(Session) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = ()> + Send + 'static,
	{
		*self.inner.on_connected.lock().unwrap_or_else(|e| e.into_inner()) =
			Some(Arc::new(move |session| Box::pin(hook(session))));
		self
	}

	/// Sets the hook that runs when a session disconnected (after it was removed from the set).
	pub fn on_session_disconnected<F, Fut>(&self, hook: F) -> &Self
	where
		F: Fn(Session) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = ()> + Send + 'static,
	{
		*self.inner.on_disconnected.lock().unwrap_or_else(|e| e.into_inner()) =
			Some(Arc::new(move |session| Box::pin(hook(session))));
		self
	}

	/// The currently connected sessions.
	pub fn sessions(&self) -> Vec<Session> {
		self.inner.sessions.lock().unwrap_or_else(|e| e.into_inner()).values().cloned().collect()
	}

	/// The number of connected sessions.
	pub fn session_count(&self) -> usize {
		self.inner.sessions.lock().unwrap_or_else(|e| e.into_inner()).len()
	}

	/// Sends a message to every connected session and waits for all acknowledgements.
	///
	/// Never fails as a whole: the result carries one entry per session with that session's
	/// `ACK` payload or error (a `NAK`, or the session closing mid-broadcast).
	pub async fn broadcast<M: Serialize + ?Sized>(&self, message: &M) -> Vec<BroadcastResult> {
		let sessions = self.sessions();
		let sends = sessions.iter().map(|session| session.send(message));
		let results = futures_util::future::join_all(sends).await;
		sessions
			.into_iter()
			.zip(results)
			.map(|(session, result)| {
				if let Err(error) = &result {
					warn!(session = session.id(), "broadcast send failed: {error}");
				}
				BroadcastResult { session, result }
			})
			.collect()
	}
}

impl fmt::Debug for Endpoint {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Endpoint").field("path", &self.path()).field("sessions", &self.session_count()).finish()
	}
}

/// The outcome of a broadcast for one session.
#[derive(Debug)]
pub struct BroadcastResult {
	/// The session the message was sent to.
	pub session: Session,
	/// The session's `ACK` payload, or its error.
	pub result: Result<Value, VcmpError>,
}

#[derive(Debug, Clone)]
enum Segment {
	Literal(String),
	Param(String),
}

#[derive(Debug, Clone)]
struct PathPattern {
	source: String,
	segments: Vec<Segment>,
}

impl PathPattern {
	fn parse(pattern: &str) -> Self {
		let segments = pattern
			.split('/')
			.filter(|s| !s.is_empty())
			.map(|s| match s.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
				Some(name) => Segment::Param(name.to_owned()),
				None => Segment::Literal(s.to_owned()),
			})
			.collect();
		PathPattern { source: pattern.to_owned(), segments }
	}

	fn matches(&self, path: &str) -> Option<HashMap<String, String>> {
		let path = path.split('?').next().unwrap_or(path);
		let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
		if segments.len() != self.segments.len() {
			return None;
		}
		let mut params = HashMap::new();
		for (pattern, actual) in self.segments.iter().zip(segments) {
			match pattern {
				Segment::Literal(literal) if literal == actual => {}
				Segment::Literal(_) => return None,
				Segment::Param(name) => {
					params.insert(name.clone(), actual.to_owned());
				}
			}
		}
		Some(params)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn matches_literal_paths() {
		let pattern = PathPattern::parse("/keypad");
		assert_eq!(pattern.matches("/keypad"), Some(HashMap::new()));
		assert_eq!(pattern.matches("/keypad/"), Some(HashMap::new()));
		assert_eq!(pattern.matches("/keypad?token=x"), Some(HashMap::new()));
		assert_eq!(pattern.matches("/keypads"), None);
		assert_eq!(pattern.matches("/keypad/1"), None);
		assert_eq!(pattern.matches("/"), None);
	}

	#[test]
	fn extracts_path_parameters() {
		let pattern = PathPattern::parse("/drivers/{driver}");
		let params = pattern.matches("/drivers/kerong").unwrap();
		assert_eq!(params["driver"], "kerong");
		assert_eq!(pattern.matches("/drivers"), None);
		assert_eq!(pattern.matches("/drivers/"), None);
		assert_eq!(pattern.matches("/drivers/a/b"), None);
	}

	#[test]
	fn matches_the_root() {
		let pattern = PathPattern::parse("/");
		assert_eq!(pattern.matches("/"), Some(HashMap::new()));
		assert_eq!(pattern.matches(""), Some(HashMap::new()));
		assert_eq!(pattern.matches("/x"), None);
	}
}
