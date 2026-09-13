//! Socket-level tests of the server: endpoints, connect info, hooks and broadcast.

mod common;

use common::*;
use futures_util::{SinkExt, StreamExt};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_tungstenite::tungstenite::{Message, protocol::WebSocketConfig};
use vcmp::{ConnectInfo, Frame, VcmpClient, VcmpError, VcmpServer};

server_tests!(
	routes_by_path_and_exposes_connect_info,
	broadcasts_and_reports_per_session_results,
	server_initiates_the_heartbeat_and_closes_silent_peers,
	stop_closes_all_sessions,
	server_side_sends_are_handled_by_the_client,
	max_message_size_rejects_oversized_messages,
	fragments_outgoing_messages,
);

async fn routes_by_path_and_exposes_connect_info(host: ServerHost) {
	init_tracing();
	let server = VcmpServer::builder().build();
	let drivers = server.endpoint("/drivers/{driver}");
	let keypad = server.endpoint("/keypad");
	register(drivers.handlers());
	keypad.on::<Echo, _, _, _, _>(|_, _| async { Ok::<_, VcmpError>("keypad") });

	let infos: Arc<Mutex<Vec<ConnectInfo>>> = Arc::default();
	let connected_infos = infos.clone();
	drivers.on_session_connected(move |session| {
		let infos = connected_infos.clone();
		async move {
			infos.lock().unwrap().push(session.connect_info().unwrap().clone());
		}
	});
	let disconnected = Arc::new(Mutex::new(0));
	let d = disconnected.clone();
	drivers.on_session_disconnected(move |_| {
		let d = d.clone();
		async move {
			*d.lock().unwrap() += 1;
		}
	});
	let handle = host.bind(server, 0).await;

	let driver = VcmpClient::builder(format!("ws://{}/drivers/kerong?x=1", handle.local_addr()))
		.header("Authorization", "Bearer secret")
		.build();
	driver.start();
	connected(&driver).await;
	wait_until(|| drivers.session_count() == 1).await;
	assert_eq!(driver.send(&Echo { payload: "x".into() }).await.unwrap(), "x");

	let keypad_client = client(&format!("ws://{}/keypad", handle.local_addr()));
	keypad_client.start();
	connected(&keypad_client).await;
	assert_eq!(keypad_client.send(&Echo { payload: "x".into() }).await.unwrap(), "keypad");
	assert_eq!(keypad.session_count(), 1);
	assert_eq!(drivers.session_count(), 1);

	let info = infos.lock().unwrap().pop().unwrap();
	assert_eq!(info.path, "/drivers/kerong");
	assert_eq!(info.param("driver"), Some("kerong"));
	assert_eq!(info.header("authorization"), Some("Bearer secret"));
	assert_eq!(info.header("Authorization"), Some("Bearer secret"));
	assert!(info.remote_addr.is_some());

	driver.stop();
	wait_until(|| *disconnected.lock().unwrap() == 1).await;
	assert_eq!(drivers.session_count(), 0);
	keypad_client.stop();
	handle.stop().await;
}

async fn broadcasts_and_reports_per_session_results(host: ServerHost) {
	init_tracing();
	let (server, endpoint) = start_hosted_server(host, 0, Duration::from_secs(20)).await;
	let ok = client(&format!("ws://{}/test/ok", server.local_addr()));
	let failing = VcmpClient::builder(format!("ws://{}/test/failing", server.local_addr())).build();
	register(failing.handlers());
	failing.on::<Echo, _, _, _, _>(|_, _| async { Err::<(), _>(VcmpError::new(409, "Conflict")) });
	ok.start();
	failing.start();
	connected(&ok).await;
	connected(&failing).await;
	wait_until(|| endpoint.session_count() == 2).await;

	let results = endpoint.broadcast(&Echo { payload: "all".into() }).await;
	assert_eq!(results.len(), 2);
	let mut oks = 0;
	let mut errors = 0;
	for result in &results {
		match &result.result {
			Ok(value) => {
				assert_eq!(value, "all");
				oks += 1;
			}
			Err(error) => {
				assert_eq!(error.status(), 409);
				assert_eq!(result.session.connect_info().unwrap().param("name"), Some("failing"));
				errors += 1;
			}
		}
	}
	assert_eq!((oks, errors), (1, 1));

	assert!(endpoint.broadcast(&Void {}).await.iter().all(|r| r.result.is_ok()));
	ok.stop();
	failing.stop();
	wait_until(|| endpoint.session_count() == 0).await;
	assert!(endpoint.broadcast(&Void {}).await.is_empty());
	server.stop().await;
}

async fn server_initiates_the_heartbeat_and_closes_silent_peers(host: ServerHost) {
	init_tracing();
	let (server, endpoint) = start_hosted_server(host, 0, Duration::from_millis(100)).await;
	// a raw peer that never answers the heartbeat
	let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/test/silent", server.local_addr())).await.unwrap();
	wait_until(|| endpoint.session_count() == 1).await;
	let session = endpoint.sessions().pop().unwrap();
	let pending = tokio::spawn(async move { session.send(&Void {}).await });
	let error = tokio::time::timeout(Duration::from_secs(2), pending).await.unwrap().unwrap().unwrap_err();
	assert_eq!((error.status(), error.title()), (503, "Session closed"));
	wait_until(|| endpoint.session_count() == 0).await;
	drop(ws);
	server.stop().await;
}

async fn stop_closes_all_sessions(host: ServerHost) {
	init_tracing();
	let (server, endpoint) = start_hosted_server(host, 0, Duration::from_secs(20)).await;
	let clients: Vec<_> = (0..3)
		.map(|i| {
			let client = VcmpClient::builder(format!("ws://{}/test/{i}", server.local_addr()))
				.reconnect(vcmp::Backoff::fixed(Duration::from_secs(60)))
				.build();
			client.start();
			client
		})
		.collect();
	for client in &clients {
		connected(client).await;
	}
	wait_until(|| endpoint.session_count() == 3).await;
	server.stop().await;
	for client in &clients {
		disconnected(client).await;
		client.stop();
	}
	assert_eq!(endpoint.session_count(), 0);
}

async fn server_side_sends_are_handled_by_the_client(host: ServerHost) {
	init_tracing();
	let (server, endpoint) = start_hosted_server(host, 0, Duration::from_secs(20)).await;
	let client = client(&format!("ws://{}/test/a", server.local_addr()));
	client.start();
	connected(&client).await;
	wait_until(|| endpoint.session_count() == 1).await;
	let session = endpoint.sessions().pop().unwrap();
	assert_eq!(session.send(&Echo { payload: "down".into() }).await.unwrap(), "down");
	let error = session.send(&Unknown {}).await.unwrap_err();
	assert_eq!(error.status(), 500);
	client.stop();
	server.stop().await;
}

async fn max_message_size_rejects_oversized_messages(host: ServerHost) {
	for fragment_size in [None, Some(64)] {
		let server = VcmpServer::builder().max_message_size(256).build();
		let endpoint = server.endpoint("/test/{name}");
		register(endpoint.handlers());
		let server = host.bind(server, 0).await;
		let client = VcmpClient::builder(format!("ws://{}/test/size", server.local_addr()))
			.fragment_size(fragment_size)
			.reconnect(vcmp::Backoff::fixed(Duration::from_secs(60)))
			.build();
		client.start();
		connected(&client).await;
		assert_eq!(client.send(&Echo { payload: "small".into() }).await.unwrap(), "small");
		let error = tokio::time::timeout(Duration::from_secs(5), client.send(&Echo { payload: "x".repeat(1024) }))
			.await
			.unwrap()
			.unwrap_err();
		assert_eq!((error.status(), error.title()), (503, "Session closed"));
		disconnected(&client).await;
		wait_until(|| endpoint.session_count() == 0).await;
		client.stop();
		server.stop().await;
	}
}

async fn fragments_outgoing_messages(host: ServerHost) {
	let server = VcmpServer::builder().fragment_size(Some(32)).build();
	let endpoint = server.endpoint("/test/{name}");
	register(endpoint.handlers());
	let server = host.bind(server, 0).await;
	// A peer accepting at most 32 bytes per frame proves the server actually fragments the reply.
	let config = WebSocketConfig::default().max_frame_size(Some(32));
	let (mut peer, _) = tokio_tungstenite::connect_async_with_config(
		format!("ws://{}/test/fragmented", server.local_addr()),
		Some(config),
		false,
	)
	.await
	.unwrap();
	let payload = "ä水🦀".repeat(256);
	let message = Frame::message(serde_json::to_string(&Echo { payload: payload.clone() }).unwrap());
	peer.send(Message::Text(message.serialize().into())).await.unwrap();
	let reply = tokio::time::timeout(Duration::from_secs(5), async {
		loop {
			let Message::Text(text) = peer.next().await.unwrap().unwrap() else { continue };
			match Frame::parse(&text).unwrap() {
				Frame::Heartbeat { .. } => continue,
				frame => break frame,
			}
		}
	})
	.await
	.unwrap();
	assert_eq!(reply, Frame::ack(message.id().unwrap(), Some(serde_json::to_string(&payload).unwrap())));
	peer.close(None).await.unwrap();
	wait_until(|| endpoint.session_count() == 0).await;
	server.stop().await;
}

#[cfg(feature = "axum")]
async fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
	use tokio::io::{AsyncReadExt, AsyncWriteExt};
	let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
	stream
		.write_all(format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes())
		.await
		.unwrap();
	let mut response = String::new();
	tokio::time::timeout(Duration::from_secs(5), stream.read_to_string(&mut response)).await.unwrap().unwrap();
	response
}

#[cfg(feature = "axum")]
#[tokio::test]
async fn axum_shares_a_port_with_rest_and_static_files_and_preserves_nested_params() {
	use axum::{Router, extract::State, routing::get};
	use tower_http::services::ServeDir;

	let server = VcmpServer::builder().build();
	let endpoint = server.endpoint("/drivers/{driver}");
	register(endpoint.handlers());
	let router = Router::new()
		.route("/health", get(|State(state): State<String>| async move { state }))
		.nest_service("/static", ServeDir::new(concat!(env!("CARGO_MANIFEST_DIR"), "/examples/static")))
		.nest("/sites/{site}", Router::new().route(endpoint.path(), endpoint.axum_route()))
		.with_state("ready".to_owned());
	let handle = HostHandle::axum(server, router, 0).await;
	let path = "/sites/vienna/drivers/kerong%20one";
	let client = VcmpClient::builder(format!("ws://{}{path}?x=1", handle.local_addr()))
		.header("Authorization", "Bearer secret")
		.build();
	client.start();
	connected(&client).await;
	wait_until(|| endpoint.session_count() == 1).await;
	let session = endpoint.sessions().pop().unwrap();
	let info = session.connect_info().unwrap();
	assert_eq!(info.path, path);
	assert_eq!(info.param("site"), Some("vienna"));
	assert_eq!(info.param("driver"), Some("kerong one"));
	assert_eq!(info.header("AUTHORIZATION"), Some("Bearer secret"));
	assert!(info.remote_addr.unwrap().ip().is_loopback());

	let response = http_get(handle.local_addr(), "/health").await;
	assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
	assert!(response.ends_with("ready"), "{response}");
	let response = http_get(handle.local_addr(), "/static/index.html").await;
	assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
	assert!(response.ends_with(include_str!("../examples/static/index.html")), "{response}");
	assert_eq!(client.send(&Echo { payload: "same port".into() }).await.unwrap(), "same port");

	// Ordinary HTTP requests cannot create sessions, and undecodable route captures fail the upgrade.
	let response = http_get(handle.local_addr(), path).await;
	assert!(response.starts_with("HTTP/1.1 400 Bad Request\r\n"), "{response}");
	let error = tokio_tungstenite::connect_async(format!("ws://{}/sites/vienna/drivers/%FF", handle.local_addr()))
		.await
		.unwrap_err();
	let tokio_tungstenite::tungstenite::Error::Http(response) = error else { panic!("{error}") };
	assert_eq!(response.status(), 400);
	assert_eq!(endpoint.session_count(), 1);
	client.stop();
	handle.stop().await;
}

#[cfg(feature = "axum")]
#[tokio::test]
async fn axum_upgrade_helper_works_in_a_handler_with_application_state() {
	use axum::{
		Router,
		extract::State,
		http::{HeaderMap, StatusCode},
		response::IntoResponse,
		routing::get,
	};
	use vcmp::axum::VcmpUpgrade;

	let server = VcmpServer::builder().build();
	let endpoint = server.endpoint("/drivers/{driver}");
	register(endpoint.handlers());
	let route_endpoint = endpoint.clone();
	let router = Router::new()
		.route(
			endpoint.path(),
			get(move |State(token): State<String>, headers: HeaderMap, upgrade: VcmpUpgrade| {
				let endpoint = route_endpoint.clone();
				async move {
					if headers.get("authorization").and_then(|value| value.to_str().ok()) != Some(token.as_str()) {
						return StatusCode::UNAUTHORIZED.into_response();
					}
					endpoint.on_upgrade(upgrade)
				}
			}),
		)
		.with_state("Bearer secret".to_owned());
	let handle = HostHandle::axum(server, router, 0).await;
	let url = format!("ws://{}/drivers/authorized", handle.local_addr());
	let error = tokio_tungstenite::connect_async(&url).await.unwrap_err();
	let tokio_tungstenite::tungstenite::Error::Http(response) = error else { panic!("{error}") };
	assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
	assert_eq!(endpoint.session_count(), 0);

	let client = VcmpClient::builder(url).header("Authorization", "Bearer secret").build();
	client.start();
	connected(&client).await;
	assert_eq!(client.send(&Echo { payload: "authorized".into() }).await.unwrap(), "authorized");
	client.stop();
	handle.stop().await;
}

#[cfg(feature = "axum")]
#[tokio::test]
async fn axum_root_route_works_without_peer_address_extension() {
	let server = VcmpServer::builder().build();
	let endpoint = server.endpoint("/");
	register(endpoint.handlers());
	let router = axum::Router::new().route(endpoint.path(), endpoint.axum_route());
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
	let local_addr = listener.local_addr().unwrap();
	let accept = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
	let handle = HostHandle::Axum { server, local_addr, accept };
	let client = client(&format!("ws://{local_addr}/"));
	client.start();
	connected(&client).await;
	wait_until(|| endpoint.session_count() == 1).await;
	let session = endpoint.sessions().pop().unwrap();
	let info = session.connect_info().unwrap();
	assert_eq!(info.path, "/");
	assert!(info.params.is_empty());
	assert_eq!(info.remote_addr, None);
	assert_eq!(client.send(&Echo { payload: "root".into() }).await.unwrap(), "root");
	client.stop();
	handle.stop().await;
}
