//! Contract tests against the real `vcmp-spring`, via the Spring Boot peer in `contract/java`.
//!
//! Requires a JDK and the sibling `vcmp-spring` checkout, and the peer to be built first:
//! `./gradlew -p contract/java installDist`. Ignored by default; run with
//! `cargo test --test contract_java -- --ignored` (or `contract/run.sh`).

mod contract;

use contract::*;
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::Command;

fn peer_binary() -> PathBuf {
	let bin = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
		.join("contract/java/build/install/vcmp-rs-contract-java/bin/vcmp-rs-contract-java");
	assert!(bin.is_file(), "build the peer first: ./gradlew -p contract/java installDist ({} missing)", bin.display());
	bin
}

fn peer(args: &[&str]) -> Command {
	let mut command = Command::new(peer_binary());
	command.args(args);
	command
}

async fn java_server(port: u16, heartbeat_ms: u64) -> PeerProcess {
	let peer = PeerProcess::spawn(peer(&["server", &port.to_string(), &heartbeat_ms.to_string()])).await;
	peer.wait_for("READY", 1).await;
	// Spring's ready-gate accepts handshakes only from ApplicationReadyEvent, which is READY here.
	peer
}

async fn java_client(url: &str) -> PeerProcess {
	let peer = PeerProcess::spawn(peer(&["client", url])).await;
	peer.wait_for("READY", 1).await;
	peer
}

#[tokio::test]
#[ignore = "needs a JDK and the vcmp-spring checkout"]
async fn rust_client_against_spring_server() {
	init_tracing();
	let port = free_port();
	let mut server = java_server(port, 200).await;
	let client = rust_client(&format!("ws://127.0.0.1:{port}/peer/rust"));
	client.start();
	tokio::time::timeout(TIMEOUT, client.wait_connected()).await.unwrap();
	let session = client.session().unwrap();

	we_send(&session).await;
	peer_sends(&session).await;

	tokio::time::sleep(Duration::from_millis(1000)).await;
	assert!(client.is_connected());
	assert!(session.heartbeats_received() >= 3, "{}", session.heartbeats_received());

	// server restart → client reconnects and a send after reconnect succeeds
	server.kill().await;
	tokio::time::timeout(TIMEOUT, client.wait_disconnected()).await.unwrap();
	let mut server = java_server(port, 200).await;
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
	server.kill().await;
}

#[tokio::test]
#[ignore = "needs a JDK and the vcmp-spring checkout"]
async fn rust_server_against_spring_client() {
	init_tracing();
	let port = free_port();
	let (server, endpoint) = rust_server(port, Duration::from_millis(200)).await;
	let url = format!("ws://127.0.0.1:{port}/peer/java");
	let mut client = java_client(&url).await;
	client.wait_for("CONNECTED", 1).await;
	wait_until(|| endpoint.session_count() == 1).await;
	let session = endpoint.sessions().pop().unwrap();
	assert_eq!(session.connect_info().unwrap().param("name"), Some("java"));

	we_send(&session).await;
	peer_sends(&session).await;

	tokio::time::sleep(Duration::from_millis(1000)).await;
	assert!(session.is_open());
	assert!(session.heartbeats_received() >= 3, "{}", session.heartbeats_received());

	// server restart → the Spring client reconnects and a send after reconnect succeeds
	server.stop().await;
	client.wait_for("DISCONNECTED", 1).await;
	let (server, endpoint) = rust_server(port, Duration::from_millis(200)).await;
	client.wait_for("CONNECTED", 2).await;
	wait_until(|| endpoint.session_count() == 1).await;
	let session = endpoint.sessions().pop().unwrap();
	assert_eq!(session.send(&Echo { payload: "again".into() }).await.unwrap(), "again");

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
