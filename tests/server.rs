//! Socket-level tests of the server: endpoints, connect info, hooks and broadcast.

mod common;

use common::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use vcmp::{ConnectInfo, VcmpClient, VcmpError, VcmpServer};

#[tokio::test]
async fn routes_by_path_and_exposes_connect_info() {
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
	let handle = server.bind("127.0.0.1:0").await.unwrap();

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

#[tokio::test]
async fn broadcasts_and_reports_per_session_results() {
	init_tracing();
	let (server, endpoint) = start_server(Duration::from_secs(20)).await;
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

#[tokio::test]
async fn server_initiates_the_heartbeat_and_closes_silent_peers() {
	init_tracing();
	let (server, endpoint) = start_server(Duration::from_millis(100)).await;
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

#[tokio::test]
async fn stop_closes_all_sessions() {
	init_tracing();
	let (server, endpoint) = start_server(Duration::from_secs(20)).await;
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

#[tokio::test]
async fn server_side_sends_are_handled_by_the_client() {
	init_tracing();
	let (server, endpoint) = start_server(Duration::from_secs(20)).await;
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
