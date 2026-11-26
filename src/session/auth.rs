//! Session authentication on the server side: the `AUT` frame (feature `server`).
//!
//! An endpoint opts in with an [`Authenticator`]: an async function that gets the session and the
//! credential (the `AUT` payload, a JSON object whose shape the endpoint defines) and returns an
//! [`Authentication`] — the principal, optionally an expiry and endpoint-defined `ACK` members — or
//! rejects with a [`VcmpError`] (`401` invalid credential, `403` valid but not allowed).
//!
//! Rules (shared with `vcmp-spring` and `vcmp-js`, close code always 1008):
//!
//! - Until the session is authenticated, only `HBT` and `AUT` are accepted. A `MSG`, `ACK` or
//!   `NAK` closes the session (`authentication required`), as does no `ACK` within the
//!   [timeout](Authenticator::timeout) (`authentication timeout`, default 10 s from connect; it also
//!   covers an authenticator that is still working).
//! - A `NAK` to the first `AUT` closes the session after the `NAK` (`authentication failed`).
//! - An authenticator that panics or exceeds the handler deadline, or a failure after it accepted (the
//!   principal comparison panics), is answered with `NAK` 500 and fails closed like a rejection. The
//!   session counts as authenticated only once the `ACK` is queued; no state is committed before.
//! - Only one `AUT` at a time: a second one while one is pending closes the session
//!   (`concurrent authentication`).
//! - A credential that is not a JSON object is answered with `NAK` 400.
//! - With an expiry, the session closes at that instant (`authentication expired`) unless a
//!   re-authentication moved it. An `AUT` on an authenticated session is a re-authentication:
//!   the same principal (by its `PartialEq`) replaces the expiry; another principal is answered
//!   with `NAK` 403 and closes the session (`principal changed`); any rejection (the authenticator's
//!   `401`, `403`, …, or `500`) is answered with that `NAK` and the session stays open until its
//!   current expiry.
//! - Messages to the session ([`Session::send`], broadcasts) fail until it is authenticated, and an
//!   endpoint's connect hook runs only once it is.
//!
//! During a migration, [`Authenticator::with_handshake`] additionally accepts a credential in the
//! WebSocket handshake (query, path parameters, headers) from old clients.

use super::{POLICY_VIOLATION, Session};
use crate::error::VcmpError;
use crate::resources::Resource;
use crate::session::ConnectInfo;
use futures_util::FutureExt;
use serde_json::{Map, Value};
use std::any::Any;
use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::task::{AbortHandle, JoinSet};
use tokio::time::Instant;
use tracing::{debug, info, warn};

/// The default time a session has from connect to authenticate.
pub const DEFAULT_AUTH_TIMEOUT: Duration = Duration::from_secs(10);

/// A successful authentication: the principal, an optional expiry and endpoint-defined members of
/// the `ACK` payload.
#[derive(Debug, Clone, PartialEq)]
pub struct Authentication<P> {
	principal: P,
	expires_at: Option<Instant>,
	ack: Map<String, Value>,
}

impl<P> Authentication<P> {
	/// An authentication of `principal` that does not expire.
	pub fn new(principal: P) -> Self {
		Authentication { principal, expires_at: None, ack: Map::new() }
	}

	/// The authentication expires after `expires_in`, counted from now: the session is closed then,
	/// unless the client re-authenticates. A duration too long to represent means no expiry. Sent to
	/// the client as `expiresIn` (milliseconds) in the `ACK`. An expiry that is not in the future when
	/// the `ACK` is built rejects the credential (401).
	pub fn expires_in(mut self, expires_in: Duration) -> Self {
		// Too far in the future to represent: no expiry.
		self.expires_at = Instant::now().checked_add(expires_in);
		self
	}

	/// The authentication expires at the given wall-clock time (e.g. a JWT's `exp`); converted to an
	/// instant now. A time that is not in the future rejects the credential (401).
	pub fn expires_at(mut self, expires_at: SystemTime) -> Self {
		let now = Instant::now();
		self.expires_at = match expires_at.duration_since(SystemTime::now()) {
			Ok(remaining) => now.checked_add(remaining),
			Err(_) => Some(now),
		};
		self
	}

	/// Adds a member to the `ACK` payload, e.g. what the session is authenticated as. The member
	/// `expiresIn` is reserved for the protocol and is replaced.
	pub fn ack_member(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
		self.ack.insert(name.into(), value.into());
		self
	}

	/// The principal.
	pub fn principal(&self) -> &P {
		&self.principal
	}
}

/// How a session was authenticated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthenticationMethod {
	/// With a credential in the WebSocket handshake (transition mode for old clients).
	Handshake,
	/// With an `AUT` frame.
	Aut,
}

/// How many sessions of an endpoint were authenticated by handshake and by `AUT` (re-authentications
/// are not counted). Once `handshake` stops growing, the last old client is gone.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AuthenticationCounts {
	/// Sessions authenticated by a handshake credential.
	pub handshake: u64,
	/// Sessions authenticated by an `AUT` frame.
	pub aut: u64,
}

type Principal = Arc<dyn Any + Send + Sync>;
type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type AuthenticateFn = Arc<dyn Fn(Session, Value) -> BoxFuture<Result<Granted, VcmpError>> + Send + Sync>;
type HandshakeFn = Arc<dyn Fn(ConnectInfo) -> BoxFuture<Result<Option<Granted>, VcmpError>> + Send + Sync>;
type SameFn = fn(&(dyn Any + Send + Sync), &(dyn Any + Send + Sync)) -> bool;

/// A type-erased [`Authentication`].
struct Granted {
	principal: Principal,
	expires_at: Option<Instant>,
	ack: Map<String, Value>,
}

impl Granted {
	/// The remaining lifetime, computed once: `Ok(None)` without expiry, `Err` when it is not in the
	/// future (or would be `expiresIn` 0).
	fn remaining(&self) -> Result<Option<Duration>, VcmpError> {
		let Some(expires_at) = self.expires_at else {
			return Ok(None);
		};
		let remaining = expires_at.saturating_duration_since(Instant::now());
		if remaining.as_millis() == 0 {
			return Err(VcmpError::new(401, "Unauthorized").with_detail("The credential has expired."));
		}
		Ok(Some(remaining))
	}
}

impl<P: Send + Sync + 'static> From<Authentication<P>> for Granted {
	fn from(authentication: Authentication<P>) -> Self {
		Granted {
			principal: Arc::new(authentication.principal),
			expires_at: authentication.expires_at,
			ack: authentication.ack,
		}
	}
}

fn same<P: PartialEq + 'static>(a: &(dyn Any + Send + Sync), b: &(dyn Any + Send + Sync)) -> bool {
	matches!((a.downcast_ref::<P>(), b.downcast_ref::<P>()), (Some(a), Some(b)) if a == b)
}

/// Requires the peer to authenticate a session; see the [module documentation](self).
///
/// ```ignore
/// endpoint.authenticate(Authenticator::new(|session: Session, credential: serde_json::Value| async move {
///     let token = credential["token"].as_str().ok_or_else(|| VcmpError::new(401, "Unauthorized"))?;
///     let account = accounts.by_token(token).await.ok_or_else(|| VcmpError::new(401, "Unauthorized"))?;
///     Ok(Authentication::new(account.id))
/// }));
/// ```
#[derive(Clone)]
pub struct Authenticator {
	authenticate: AuthenticateFn,
	handshake: Option<HandshakeFn>,
	same: SameFn,
	timeout: Duration,
}

impl Authenticator {
	/// Authenticates sessions by `AUT`. The authenticator gets the session (with its
	/// [`ConnectInfo`]) and the credential, and runs within the handler deadline. It rejects with a
	/// [`VcmpError`], normally `401` (invalid credential) or `403` (valid but not allowed); a panic is
	/// answered with `500`. Principals are compared by `P`'s `PartialEq` on re-authentication; the type
	/// system guarantees every authentication has a principal.
	pub fn new<P, F, Fut>(authenticate: F) -> Self
	where
		P: PartialEq + Send + Sync + 'static,
		F: Fn(Session, Value) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Result<Authentication<P>, VcmpError>> + Send + 'static,
	{
		let authenticate = Arc::new(authenticate);
		Authenticator {
			// The closure is called inside the boxed future, so a synchronous panic is caught like an
			// asynchronous one (guarded's catch_unwind covers the whole future).
			authenticate: Arc::new(move |session, credential| {
				let authenticate = authenticate.clone();
				Box::pin(async move { authenticate(session, credential).await.map(Granted::from) })
			}),
			handshake: None,
			same: same::<P>,
			timeout: DEFAULT_AUTH_TIMEOUT,
		}
	}

	/// Like [`Authenticator::new`], also accepting a credential in the WebSocket handshake from
	/// old clients (transition mode). The handshake authenticator runs before any frame is read:
	///
	/// - `Ok(Some(authentication))`: the session is authenticated at connect, no `AUT` expected.
	/// - `Ok(None)`: the handshake carries no credential; the session waits for `AUT`.
	/// - `Err(_)`: the credential is invalid; the session is closed (1008), without a fallback to `AUT`.
	///
	/// Old clients cannot refresh, so a handshake authentication should not expire without a reason.
	/// [`Endpoint::authentication_counts`](crate::Endpoint::authentication_counts) tells when the
	/// last old client is gone.
	pub fn with_handshake<P, F, Fut, H, HFut>(authenticate: F, handshake: H) -> Self
	where
		P: PartialEq + Send + Sync + 'static,
		F: Fn(Session, Value) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Result<Authentication<P>, VcmpError>> + Send + 'static,
		H: Fn(ConnectInfo) -> HFut + Send + Sync + 'static,
		HFut: Future<Output = Result<Option<Authentication<P>>, VcmpError>> + Send + 'static,
	{
		let handshake = Arc::new(handshake);
		let mut authenticator = Self::new(authenticate);
		authenticator.handshake = Some(Arc::new(move |info| {
			let handshake = handshake.clone();
			Box::pin(async move { handshake(info).await.map(|authentication| authentication.map(Granted::from)) })
		}));
		authenticator
	}

	/// The time from connect within which a session must authenticate. Default: 10 s.
	pub fn timeout(mut self, timeout: Duration) -> Self {
		self.timeout = timeout;
		self
	}
}

impl fmt::Debug for Authenticator {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Authenticator")
			.field("handshake", &self.handshake.is_some())
			.field("timeout", &self.timeout)
			.finish_non_exhaustive()
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
	/// Waiting for the first successful authentication.
	Pending,
	Authenticated,
	/// Decided to close; every further frame is ignored.
	Closing,
}

struct AuthState {
	phase: Phase,
	/// An `AUT` is being authenticated.
	in_flight: bool,
	principal: Option<Principal>,
	method: Option<AuthenticationMethod>,
	/// The expiry timer and its generation, so a timer replaced while it fires does nothing.
	expiry: Option<AbortHandle>,
	expiry_generation: u64,
	timeout: Option<AbortHandle>,
}

/// The authentication state of a session with an [`Authenticator`].
pub(crate) struct ServerAuth {
	authenticator: Authenticator,
	state: Mutex<AuthState>,
}

impl ServerAuth {
	pub(super) fn new(authenticator: Authenticator) -> Self {
		ServerAuth {
			authenticator,
			state: Mutex::new(AuthState {
				phase: Phase::Pending,
				in_flight: false,
				principal: None,
				method: None,
				expiry: None,
				expiry_generation: 0,
				timeout: None,
			}),
		}
	}

	fn state(&self) -> std::sync::MutexGuard<'_, AuthState> {
		self.state.lock().unwrap_or_else(|e| e.into_inner())
	}

	/// Stops the timers of a closed session.
	pub(super) fn finish(&self) {
		let mut state = self.state();
		state.phase = Phase::Closing;
		for timer in [state.expiry.take(), state.timeout.take()].into_iter().flatten() {
			timer.abort();
		}
	}
}

/// `NAK` 500: the authenticator, or completing an accepted authentication, failed unexpectedly.
fn unexpected_failure() -> VcmpError {
	VcmpError::internal("Authentication failed").with_detail("The authentication could not be completed.")
}

/// Runs an authenticator within the handler deadline; a panic or the deadline is an unexpected failure (500).
async fn guarded<T>(
	session: &Session,
	deadline: Duration,
	authenticator: impl Future<Output = Result<T, VcmpError>>,
) -> Result<T, VcmpError> {
	match tokio::time::timeout(deadline, AssertUnwindSafe(authenticator).catch_unwind()).await {
		Ok(Ok(result)) => result,
		Ok(Err(panic)) => {
			let message = panic
				.downcast_ref::<&str>()
				.copied()
				.or_else(|| panic.downcast_ref::<String>().map(String::as_str))
				.unwrap_or("non-string panic");
			warn!(session = session.id(), message, "authenticator panicked");
			Err(unexpected_failure())
		}
		Err(_) => {
			warn!(session = session.id(), "authenticator did not answer within the handler deadline");
			Err(unexpected_failure())
		}
	}
}

/// The `ACK` payload: endpoint members and the reserved `expiresIn`; `None` without members.
fn ack_payload(expires_in: Option<Duration>, mut ack: Map<String, Value>) -> Result<Option<String>, VcmpError> {
	ack.remove("expiresIn");
	if let Some(expires_in) = expires_in {
		let millis = u64::try_from(expires_in.as_millis()).unwrap_or(u64::MAX);
		ack.insert("expiresIn".to_owned(), Value::from(millis));
	}
	if ack.is_empty() {
		return Ok(None);
	}
	serde_json::to_string(&ack).map(Some).map_err(|_| unexpected_failure())
}

const REQUIRED: &str = "authentication required";
const TIMEOUT: &str = "authentication timeout";
const FAILED: &str = "authentication failed";
const EXPIRED: &str = "authentication expired";
const PRINCIPAL_CHANGED: &str = "principal changed";
const CONCURRENT: &str = "concurrent authentication";

impl Session {
	/// The principal this session is authenticated as, if it is authenticated as a `P`.
	pub fn principal<P: Send + Sync + 'static>(&self) -> Option<Arc<P>> {
		let principal = self.inner.auth.as_ref()?.state().principal.clone()?;
		principal.downcast::<P>().ok()
	}

	/// How this session was authenticated; `None` before, and for a session without an authenticator.
	pub fn authentication_method(&self) -> Option<AuthenticationMethod> {
		self.inner.auth.as_ref()?.state().method
	}

	/// Arms the authentication timeout, counted from connect (spawn).
	pub(super) fn arm_authentication_timeout(&self, auth: &ServerAuth) {
		let session = self.clone();
		let deadline = Instant::now() + auth.authenticator.timeout;
		let timer = tokio::spawn(async move {
			tokio::time::sleep_until(deadline).await;
			session.authentication_timed_out();
		});
		auth.state().timeout = Some(timer.abort_handle());
	}

	/// Runs the handshake authenticator, if any. Returns whether the session goes on to read frames.
	pub(super) async fn start_authentication(&self, auth: &ServerAuth) -> bool {
		let (Some(handshake), Some(info)) = (&auth.authenticator.handshake, self.connect_info()) else {
			return true;
		};
		// Frames stay unread until the handshake is decided, so an old client's first MSG is not
		// taken for a MSG before authentication.
		let result = tokio::select! {
			biased;
			_ = self.inner.close_requested.notified() => return false,
			result = guarded(self, self.inner.limits.handler_timeout, handshake(info.clone())) => result,
		};
		match result {
			Ok(None) => true,
			Ok(Some(granted)) => {
				let expires_in = match granted.remaining() {
					Ok(expires_in) => expires_in,
					Err(_) => {
						debug!(session = self.id(), "handshake credential expired");
						self.violate(auth, FAILED);
						return false;
					}
				};
				let state = auth.state();
				if state.phase != Phase::Pending {
					return false;
				}
				self.commit(state, granted.principal, expires_in, AuthenticationMethod::Handshake);
				self.inner.authenticated.send_replace(true);
				true
			}
			Err(error) => {
				debug!(session = self.id(), status = error.status(), "handshake credential rejected");
				self.violate(auth, FAILED);
				false
			}
		}
	}

	/// Before authentication, a frame that can't be parsed, or a binary message, closes the session
	/// like any frame other than `HBT`/`AUT`. Returns whether the raw frame goes on to be parsed.
	pub(super) fn admit_raw(&self, auth: &ServerAuth, raw: &str) -> bool {
		let phase = auth.state().phase;
		match phase {
			Phase::Closing => false,
			Phase::Authenticated => true,
			Phase::Pending if raw != super::BINARY_MESSAGE && crate::frame::Frame::parse(raw).is_ok() => true,
			Phase::Pending => {
				debug!(session = self.id(), "unparseable or binary frame before authentication, closing session");
				self.violate(auth, REQUIRED);
				false
			}
		}
	}

	/// Applies the authentication rules to a received frame; returns it if it is to be dispatched.
	pub(super) fn admit(
		&self,
		auth: &ServerAuth,
		frame: crate::frame::Frame,
		tasks: &mut JoinSet<()>,
	) -> Option<crate::frame::Frame> {
		use crate::frame::Frame;
		let phase = auth.state().phase;
		match (phase, frame) {
			(Phase::Closing, _) => None,
			(_, Frame::Authenticate { id, payload }) => {
				self.handle_authenticate(auth, id, payload, tasks);
				None
			}
			(_, frame @ Frame::Heartbeat { .. }) | (Phase::Authenticated, frame) => Some(frame),
			(Phase::Pending, _) => {
				debug!(session = self.id(), "frame before authentication, closing session");
				self.violate(auth, REQUIRED);
				None
			}
		}
	}

	fn handle_authenticate(&self, auth: &ServerAuth, id: String, payload: String, tasks: &mut JoinSet<()>) {
		let mut state = auth.state();
		if state.in_flight {
			drop(state);
			debug!(session = self.id(), "second AUT while one is pending, closing session");
			self.violate(auth, CONCURRENT);
			return;
		}
		let first = state.phase == Phase::Pending;
		let credential = match serde_json::from_str::<Value>(&payload) {
			Ok(credential @ Value::Object(_)) => credential,
			_ => {
				drop(state);
				let error = VcmpError::bad_request("Invalid credential")
					.with_detail("The authentication payload must be a JSON object.");
				self.reject(auth, &id, &error, first);
				return;
			}
		};
		let permits = match self.reserve(Resource::Handler, payload.len()) {
			Ok(permits) => permits,
			Err(error) => {
				drop(state);
				self.reject(auth, &id, &error, first);
				return;
			}
		};
		state.in_flight = true;
		drop(state);
		drop(payload);
		let authenticate = auth.authenticator.authenticate.clone();
		let session = self.clone();
		tasks.spawn(async move {
			let _permits = permits;
			let deadline = session.inner.limits.handler_timeout;
			let result = guarded(&session, deadline, authenticate(session.clone(), credential)).await;
			session.complete_authentication(id, result);
		});
	}

	fn complete_authentication(&self, id: String, result: Result<Granted, VcmpError>) {
		let Some(auth) = &self.inner.auth else {
			return;
		};
		let mut state = auth.state();
		state.in_flight = false;
		let first = state.phase == Phase::Pending;
		let granted = match (state.phase, result) {
			(Phase::Closing, _) => return,
			(_, Err(error)) => {
				drop(state);
				debug!(session = self.id(), status = error.status(), first, "authentication rejected");
				// A credential that was valid is not revoked by a failed refresh: keep the expiry.
				return self.reject(auth, &id, &error, first);
			}
			(_, Ok(granted)) => granted,
		};
		// Check order (#34): principal present (by construction) → expiry in the future → same principal.
		let expires_in = match granted.remaining() {
			Ok(expires_in) => expires_in,
			Err(error) => {
				drop(state);
				debug!(session = self.id(), first, "credential already expired");
				return self.reject(auth, &id, &error, first);
			}
		};
		if !first {
			let comparison = state.principal.clone().map(|current| {
				std::panic::catch_unwind(AssertUnwindSafe(|| (auth.authenticator.same)(&*current, &*granted.principal)))
			});
			match comparison {
				Some(Ok(true)) => {}
				Some(Ok(false)) => {
					state.phase = Phase::Closing;
					drop(state);
					warn!(session = self.id(), "re-authentication as another principal, closing session");
					let error = VcmpError::new(403, "Forbidden")
						.with_detail("A session cannot change its principal; reconnect instead.");
					return self.nak_and_close(&id, &error, POLICY_VIOLATION, PRINCIPAL_CHANGED);
				}
				Some(Err(_)) | None => {
					drop(state);
					warn!(session = self.id(), "comparing the principals failed");
					return self.reject(auth, &id, &unexpected_failure(), first);
				}
			}
		}
		let ack = match ack_payload(expires_in, granted.ack) {
			Ok(ack) => ack,
			Err(error) => {
				drop(state);
				return self.reject(auth, &id, &error, first);
			}
		};
		// Commit only once the ACK is queued (the writer sends queued frames in order), while still holding
		// the state lock, so a frame the peer sends in reaction to the ACK sees the committed state.
		if self.enqueue_with(crate::frame::Frame::ack(id, ack).serialize(), true, false).is_err() {
			state.phase = Phase::Closing;
			drop(state);
			self.close_with(POLICY_VIOLATION, Cow::Borrowed(FAILED));
			return;
		}
		if first {
			self.commit(state, granted.principal, expires_in, AuthenticationMethod::Aut);
			self.inner.authenticated.send_replace(true);
		} else {
			self.arm_expiry(&mut state, expires_in);
			drop(state);
			debug!(session = self.id(), "re-authenticated");
		}
	}

	/// Marks the session authenticated (the `ACK`, if any, is already queued). The caller announces it.
	fn commit(
		&self,
		mut state: std::sync::MutexGuard<'_, AuthState>,
		principal: Principal,
		expires_in: Option<Duration>,
		method: AuthenticationMethod,
	) {
		state.phase = Phase::Authenticated;
		state.principal = Some(principal);
		state.method = Some(method);
		if let Some(timer) = state.timeout.take() {
			timer.abort();
		}
		self.arm_expiry(&mut state, expires_in);
		drop(state);
		info!(session = self.id(), ?method, "session authenticated");
	}

	/// Replaces the expiry timer.
	fn arm_expiry(&self, state: &mut AuthState, expires_in: Option<Duration>) {
		if let Some(timer) = state.expiry.take() {
			timer.abort();
		}
		state.expiry_generation += 1;
		if let Some(expires_in) = expires_in {
			let generation = state.expiry_generation;
			let session = self.clone();
			// Beyond what an Instant can represent: never expires.
			if let Some(deadline) = Instant::now().checked_add(expires_in) {
				let timer = tokio::spawn(async move {
					tokio::time::sleep_until(deadline).await;
					session.authentication_expired(generation);
				});
				state.expiry = Some(timer.abort_handle());
			}
		}
	}

	fn authentication_timed_out(&self) {
		let Some(auth) = &self.inner.auth else {
			return;
		};
		if auth.state().phase == Phase::Pending {
			debug!(session = self.id(), "not authenticated in time, closing session");
			self.violate(auth, TIMEOUT);
		}
	}

	fn authentication_expired(&self, generation: u64) {
		let Some(auth) = &self.inner.auth else {
			return;
		};
		let state = auth.state();
		if state.phase == Phase::Authenticated && state.expiry_generation == generation {
			drop(state);
			info!(session = self.id(), "authentication expired, closing session");
			self.violate(auth, EXPIRED);
		}
	}

	/// Answers an `AUT` with a `NAK`; closes the session afterwards if it is not authenticated.
	fn reject(&self, auth: &ServerAuth, id: &str, error: &VcmpError, first: bool) {
		if first {
			auth.state().phase = Phase::Closing;
			self.nak_and_close(id, error, POLICY_VIOLATION, FAILED);
		} else {
			self.nak(id, error);
		}
	}

	/// Closes the session at once with 1008 and the given reason.
	fn violate(&self, auth: &ServerAuth, reason: &'static str) {
		auth.state().phase = Phase::Closing;
		self.close_with(POLICY_VIOLATION, Cow::Borrowed(reason));
	}
}
