//! Serve VCMP endpoints in an axum router alongside REST routes and static files (feature `axum`).
//!
//! Mount an [`Endpoint`] with [`Endpoint::axum_route`], or accept [`VcmpUpgrade`] in a custom
//! handler and pass it to [`Endpoint::on_upgrade`] after checking the request. The router supplies
//! path parameters; use `into_make_service_with_connect_info::<SocketAddr>()` when serving to
//! also record the peer address.
//!
//! This extractor handles HTTP/1.1 WebSocket upgrades using the same tungstenite transport as the
//! bare server. Axum's native `WebSocketUpgrade` hides raw frames, so it cannot preserve outgoing
//! fragmentation. [`VcmpUpgrade`] retains support for [`crate::ServerBuilder::fragment_size`]
//! and shares message limits, heartbeat, handlers, session tracking and hooks with the bare server.
//!
//! The application owns its HTTP listener. After stopping it, call
//! [`crate::VcmpServer::close_sessions`] to close the upgraded connections as well.

use crate::Endpoint;
use crate::session::ConnectInfo;
use crate::ws::Headers;
use ::axum::body::Body;
use ::axum::extract::{ConnectInfo as PeerAddr, FromRequestParts, OriginalUri, Path};
use ::axum::http::{Request, StatusCode, Version, request::Parts};
use ::axum::response::{IntoResponse, Response};
use ::axum::routing::{MethodRouter, get};
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::{handshake::server::create_response, protocol::Role};
use tracing::debug;

/// An axum extractor for an HTTP/1.1 VCMP WebSocket upgrade and its connection metadata.
///
/// Inspect [`Self::connect_info`] before accepting with [`Endpoint::on_upgrade`], for example
/// to authorize a driver. Invalid handshakes or invalid UTF-8 path parameters are rejected before
/// any session is created. Headers are taken directly from the request; proxy headers are not
/// interpreted as the peer address.
#[derive(Debug)]
pub struct VcmpUpgrade {
	on_upgrade: OnUpgrade,
	response: Response,
	connect_info: ConnectInfo,
}

impl VcmpUpgrade {
	/// The request path, router parameters, headers and optional peer address.
	pub fn connect_info(&self) -> &ConnectInfo {
		&self.connect_info
	}
}

impl<S> FromRequestParts<S> for VcmpUpgrade
where
	S: Send + Sync,
{
	type Rejection = Response;

	async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
		if parts.version != Version::HTTP_11 {
			return Err((StatusCode::BAD_REQUEST, "VCMP requires an HTTP/1.1 WebSocket upgrade").into_response());
		}
		let mut request = Request::new(());
		*request.method_mut() = parts.method.clone();
		*request.version_mut() = parts.version;
		*request.uri_mut() = parts.uri.clone();
		*request.headers_mut() = parts.headers.clone();
		let response = create_response(&request)
			.map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()).into_response())?
			.map(|()| Body::empty());

		let Path(params) = Path::<HashMap<String, String>>::from_request_parts(parts, state)
			.await
			.map_err(IntoResponse::into_response)?;
		let path = parts.extensions.get::<OriginalUri>().map_or(&parts.uri, |original| &original.0).path().to_owned();
		let connect_info = ConnectInfo {
			path,
			params,
			headers: Headers(&parts.headers).to_vec(),
			remote_addr: parts.extensions.get::<PeerAddr<SocketAddr>>().map(|addr| addr.0),
		};
		let on_upgrade = parts.extensions.remove::<OnUpgrade>().ok_or_else(|| {
			(StatusCode::BAD_REQUEST, "WebSocket upgrade is not available on this connection").into_response()
		})?;
		Ok(Self { on_upgrade, response, connect_info })
	}
}

impl Endpoint {
	/// An axum GET route that upgrades requests to VCMP sessions on this endpoint.
	///
	/// ```ignore
	/// let app = axum::Router::new().route(drivers.path(), drivers.axum_route());
	/// ```
	///
	/// Axum controls route matching, nesting and path parameter decoding. The path passed to
	/// `Router::route` may differ from [`Self::path`]. Normal axum middleware can protect the route.
	pub fn axum_route<S>(&self) -> MethodRouter<S>
	where
		S: Clone + Send + Sync + 'static,
	{
		let endpoint = self.clone();
		get(move |upgrade: VcmpUpgrade| async move { endpoint.on_upgrade(upgrade) })
	}

	/// Accepts an extracted VCMP upgrade and runs the endpoint's session lifecycle in the background.
	///
	/// Use this from a custom axum handler when it needs to inspect the request before upgrading:
	///
	/// ```ignore
	/// async fn driver(
	///     axum::extract::State(endpoint): axum::extract::State<vcmp::Endpoint>,
	///     upgrade: vcmp::axum::VcmpUpgrade,
	/// ) -> axum::response::Response {
	///     // Check upgrade.connect_info() here before accepting the connection.
	///     endpoint.on_upgrade(upgrade)
	/// }
	/// ```
	pub fn on_upgrade(&self, upgrade: VcmpUpgrade) -> Response {
		let endpoint = self.clone();
		let pending = endpoint.begin_upgrade();
		tokio::spawn(async move {
			let io = match upgrade.on_upgrade.await {
				Ok(io) => io,
				Err(error) => {
					debug!("WebSocket upgrade failed: {error}");
					return;
				}
			};
			let ws = WebSocketStream::from_raw_socket(
				TokioIo::new(io),
				Role::Server,
				Some(endpoint.transport().websocket_config()),
			)
			.await;
			let session = endpoint.start_session(upgrade.connect_info, ws);
			// Release shutdown only after insertion, without waiting for application hooks.
			drop(pending);
			endpoint.serve_session(session).await;
		});
		upgrade.response
	}
}
