//! Contract tests against the real `vcmp-js` packages (`contract/js/peer.js`).
//!
//! Requires `node` and `npm ci` in `contract/js`. Ignored by default; run with
//! `cargo test --test contract_js -- --ignored` (or `contract/run.sh`).

mod contract;

use contract::*;
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::Command;

fn peer_dir() -> PathBuf {
	let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("contract/js");
	assert!(dir.join("node_modules").is_dir(), "run `npm ci` in {}", dir.display());
	dir
}

fn node(args: &[&str]) -> Command {
	let mut command = Command::new("node");
	command.arg(peer_dir().join("peer.js")).args(args);
	command
}

/// A js server peer on `port`; `0` lets it pick a free one (see [`PeerProcess::listening_port`]).
async fn js_server(port: u16, heartbeat_ms: u64) -> PeerProcess {
	let peer = PeerProcess::spawn(node(&["server", &port.to_string(), &heartbeat_ms.to_string()])).await;
	peer.wait_for("READY", 1).await;
	peer
}

async fn js_auth_server() -> PeerProcess {
	let peer = PeerProcess::spawn(node(&["server-auth", "0", "20000"])).await;
	peer.wait_for("READY", 1).await;
	peer
}

/// A js client: with `credentials` (a JSON array) it authenticates with AUT, else it is an old client.
async fn js_auth_client(url: String, credentials: Option<serde_json::Value>, hello: bool) -> PeerProcess {
	let credentials = credentials.map(|credentials| credentials.to_string());
	let mut args = match &credentials {
		Some(credentials) => vec!["client-auth", url.as_str(), credentials.as_str()],
		None => vec!["client", url.as_str()],
	};
	if hello {
		args.push("--hello");
	}
	let peer = PeerProcess::spawn(node(&args)).await;
	peer.wait_for("READY", 1).await;
	peer
}

async fn js_client(url: &str) -> PeerProcess {
	let peer = PeerProcess::spawn(node(&["client", url])).await;
	peer.wait_for("READY", 1).await;
	peer
}

#[tokio::test]
#[ignore = "needs node and the vcmp-js packages"]
async fn rust_client_against_js_server() {
	init_tracing();
	let mut server = js_server(0, 200).await;
	let port = server.listening_port();
	let client = rust_client(&format!("ws://127.0.0.1:{port}/"));
	client.start();
	tokio::time::timeout(TIMEOUT, client.wait_connected()).await.unwrap();
	let session = client.session().unwrap();

	we_send(&session).await;
	peer_sends(&session).await;
	// malformed-from-peer → our server NAKs 400 (raw-socket scenario, js peer only)
	let outcome = session
		.send_as::<_, Outcome>(&Run { scenario: "malformed".into(), args: serde_json::json!({}) })
		.await
		.unwrap();
	assert!(!outcome.ok);
	assert_eq!(outcome.error.unwrap().status, 400);

	// heartbeat exchange runs for >= 3 intervals
	tokio::time::sleep(Duration::from_millis(1000)).await;
	assert!(client.is_connected());
	assert!(session.heartbeats_received() >= 3, "{}", session.heartbeats_received());

	// server restart → client reconnects and a send after reconnect succeeds
	server.kill().await;
	tokio::time::timeout(TIMEOUT, client.wait_disconnected()).await.unwrap();
	// The restarted peer binds the same port again (the client's URL is fixed): it was the peer's until the kill, so
	// only a process taking it in that gap could fail the restart — an accepted residual risk.
	let mut server = js_server(port, 200).await;
	tokio::time::timeout(TIMEOUT, client.wait_connected()).await.unwrap();
	assert_eq!(client.send(&Echo { payload: "again".into() }).await.unwrap(), "again");

	// stop() fails pending sends with 503 and does not reconnect
	let pending = {
		let client = client.clone();
		tokio::spawn(async move { client.send(&Never {}).await })
	};
	tokio::time::sleep(Duration::from_millis(200)).await;
	client.stop();
	let error = pending.await.unwrap().unwrap_err();
	assert_eq!((error.status(), error.title()), (503, "Session closed"));
	server.wait_for("DISCONNECTED", 1).await;
	tokio::time::sleep(Duration::from_millis(700)).await;
	assert_eq!(server.count("CONNECTED"), 1);
	server.kill().await;
}

#[tokio::test]
#[ignore = "needs node and the vcmp-js packages"]
async fn rust_server_against_js_client() {
	server_against_js_client(ServerHost::Bare).await;
}

#[cfg(feature = "axum")]
#[tokio::test]
#[ignore = "needs node and the vcmp-js packages"]
async fn axum_server_against_js_client() {
	server_against_js_client(ServerHost::Axum).await;
}

async fn server_against_js_client(host: ServerHost) {
	init_tracing();
	let (server, endpoint) = rust_server(host, 0, Duration::from_millis(200)).await;
	let port = server.local_addr().port();
	let url = format!("ws://127.0.0.1:{port}/peer/js");
	let mut client = js_client(&url).await;
	client.wait_for("CONNECTED", 1).await;
	wait_until(|| endpoint.session_count() == 1).await;
	let session = endpoint.sessions().pop().unwrap();
	assert_eq!(session.connect_info().unwrap().param("name"), Some("js"));

	we_send(&session).await;
	peer_sends(&session).await;

	tokio::time::sleep(Duration::from_millis(1000)).await;
	assert!(session.is_open());
	assert!(session.heartbeats_received() >= 3, "{}", session.heartbeats_received());

	// server restart → the js client reconnects and a send after reconnect succeeds
	server.stop().await;
	client.wait_for("DISCONNECTED", 1).await;
	// The restart binds the same port again (the peer's URL is fixed): it was ours until stop(), so only a process
	// taking it in that gap could fail the bind — an accepted residual risk.
	let (server, endpoint) = rust_server(host, port, Duration::from_millis(200)).await;
	client.wait_for("CONNECTED", 2).await;
	wait_until(|| endpoint.session_count() == 1).await;
	let session = endpoint.sessions().pop().unwrap();
	assert_eq!(session.send(&Echo { payload: "again".into() }).await.unwrap(), "again");

	// the peer's pending send fails when we close the session
	let pending = tokio::spawn({
		let session = session.clone();
		async move { session.send(&Never {}).await }
	});
	tokio::time::sleep(Duration::from_millis(200)).await;
	session.close();
	let error = pending.await.unwrap().unwrap_err();
	assert_eq!((error.status(), error.title()), (503, "Session closed"));
	client.wait_for("DISCONNECTED", 2).await;
	client.kill().await;
	server.stop().await;
}

// Session authentication (AUT), contract cases 1–11 of variocube/vcmp-spring#34, see tests/contract/auth.rs.

#[tokio::test]
#[ignore = "needs node and the vcmp-js packages"]
async fn aut_against_js_server() {
	init_tracing();
	let mut auth = js_auth_server().await;
	let mut open = js_server(0, 20000).await;
	let (auth_port, open_port) = (auth.listening_port(), open.listening_port());
	let target = auth::AuthTarget {
		auth: format!("ws://127.0.0.1:{auth_port}/auth/raw"),
		open: format!("ws://127.0.0.1:{open_port}/peer/raw"),
	};
	auth::raw_against_peer_server(&target).await;
	auth::rust_client_against_peer_server(&target).await;
	auth.kill().await;
	open.kill().await;
}

#[tokio::test]
#[ignore = "needs node and the vcmp-js packages"]
async fn aut_js_client_against_rust_server() {
	init_tracing();
	auth::peer_client_against_rust_server(ServerHost::Bare, &|url, credentials, hello| {
		Box::pin(js_auth_client(url, credentials, hello))
	})
	.await;
}

#[tokio::test]
#[ignore = "needs node and the vcmp-js packages"]
async fn aut_js_client_closes_with_4008() {
	init_tracing();
	auth::peer_client_closes_with_4008(&|url, credentials, hello| Box::pin(js_auth_client(url, credentials, hello)))
		.await;
}
