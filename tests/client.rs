//! Socket-level tests of the client against the Rust server and against raw WebSocket peers.

mod common;

use common::*;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use vcmp::{Backoff, VcmpClient};

server_tests!(
	sends_and_receives_acknowledgements,
	transfers_large_messages,
	transfers_fragmented_messages,
	settles_concurrent_sends_in_any_order,
	exchanges_heartbeats,
	reconnects_after_a_server_restart,
	stop_fails_pending_sends_and_does_not_reconnect,
	start_cancels_a_pending_reconnect_and_replaces_the_session,
	keeps_retrying_when_the_server_is_unreachable_or_rejects,
);

async fn sends_and_receives_acknowledgements(host: ServerHost) {
	init_tracing();
	let (server, _endpoint) = start_hosted_server(host, 0, Duration::from_secs(20)).await;
	let client = client(&format!("ws://{}/test/a", server.local_addr()));
	client.start();
	connected(&client).await;

	assert_eq!(client.send(&Echo { payload: "hi".into() }).await.unwrap(), "hi");
	assert_eq!(client.send_as::<_, String>(&Echo { payload: "typed".into() }).await.unwrap(), "typed");
	assert_eq!(client.send(&Void {}).await.unwrap(), serde_json::Value::Null);

	for status in [422, 408, 503, 504] {
		let error =
			client.send(&Fail { status, title: "Peer failure".into(), detail: "nope".into() }).await.unwrap_err();
		assert_eq!((error.status(), error.title(), error.detail()), (status, "Peer failure", Some("nope")));
		assert!(!error.is_transport());
	}

	let error = client.send(&Unknown {}).await.unwrap_err();
	assert_eq!((error.status(), error.title()), (500, "Message handling failed"));
	assert!(!error.is_transport());

	let error = client.session().unwrap().send_payload("{oops".into()).await.unwrap_err();
	assert_eq!((error.status(), error.title()), (400, "Invalid message"));
	assert!(!error.is_transport());

	client.stop();
	server.stop().await;
}

async fn transfers_large_messages(host: ServerHost) {
	init_tracing();
	let (server, endpoint) = start_hosted_server(host, 0, Duration::from_secs(20)).await;
	let client = client(&format!("ws://{}/test/a", server.local_addr()));
	client.start();
	connected(&client).await;
	for size in [100 * 1024, 1024 * 1024] {
		let payload = "x".repeat(size);
		let result = client.send_as::<_, String>(&Echo { payload: payload.clone() }).await.unwrap();
		assert_eq!(result.len(), size);
		// and in the other direction
		let session = endpoint.sessions().pop().unwrap();
		let result = session.send_as::<_, String>(&Echo { payload: payload.clone() }).await.unwrap();
		assert_eq!(result, payload);
	}
	client.stop();
	server.stop().await;
}

async fn transfers_fragmented_messages(host: ServerHost) {
	init_tracing();
	let (server, endpoint) = start_hosted_server(host, 0, Duration::from_secs(20)).await;
	let client = VcmpClient::builder(format!("ws://{}/test/a", server.local_addr())).fragment_size(Some(8192)).build();
	register(client.handlers());
	client.start();
	connected(&client).await;
	let payload = "ä".repeat(100 * 1024);
	assert_eq!(client.send_as::<_, String>(&Echo { payload: payload.clone() }).await.unwrap(), payload);
	assert_eq!(endpoint.sessions()[0].send_as::<_, String>(&Echo { payload: payload.clone() }).await.unwrap(), payload);
	client.stop();
	server.stop().await;
}

async fn settles_concurrent_sends_in_any_order(host: ServerHost) {
	init_tracing();
	let (server, _endpoint) = start_hosted_server(host, 0, Duration::from_secs(20)).await;
	let client = client(&format!("ws://{}/test/a", server.local_addr()));
	client.start();
	connected(&client).await;
	let sends = (0..100).map(|i| {
		let client = client.clone();
		tokio::spawn(async move { client.send_as::<_, String>(&Echo { payload: i.to_string() }).await })
	});
	for (i, send) in sends.enumerate() {
		assert_eq!(send.await.unwrap().unwrap(), i.to_string());
	}
	client.stop();
	server.stop().await;
}

async fn exchanges_heartbeats(host: ServerHost) {
	init_tracing();
	let (server, endpoint) = start_hosted_server(host, 0, Duration::from_millis(100)).await;
	let client = client(&format!("ws://{}/test/a", server.local_addr()));
	client.start();
	connected(&client).await;
	tokio::time::sleep(Duration::from_millis(650)).await;
	assert!(client.is_connected());
	let client_session = client.session().unwrap();
	let server_session = endpoint.sessions().pop().unwrap();
	assert!(client_session.heartbeats_received() >= 3, "{}", client_session.heartbeats_received());
	assert!(server_session.heartbeats_received() >= 3, "{}", server_session.heartbeats_received());
	client.stop();
	server.stop().await;
}

/// A raw WebSocket server that runs `handler` for each accepted connection.
async fn raw_server<F, Fut>(handler: F) -> (std::net::SocketAddr, Arc<AtomicUsize>)
where
	F: Fn(tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>) -> Fut + Send + Sync + 'static,
	Fut: std::future::Future<Output = ()> + Send + 'static,
{
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let addr = listener.local_addr().unwrap();
	let connections = Arc::new(AtomicUsize::new(0));
	let counter = connections.clone();
	tokio::spawn(async move {
		let handler = Arc::new(handler);
		while let Ok((stream, _)) = listener.accept().await {
			counter.fetch_add(1, Ordering::SeqCst);
			let handler = handler.clone();
			tokio::spawn(async move {
				if let Ok(ws) = tokio_tungstenite::accept_async(stream).await {
					handler(ws).await;
				}
			});
		}
	});
	(addr, connections)
}

#[tokio::test]
async fn closes_when_the_peer_stops_answering_heartbeats() {
	init_tracing();
	// The peer initiates the heartbeat once and then never answers the echo.
	let (addr, _) = raw_server(|mut ws| async move {
		ws.send(Message::Text("HBT100".into())).await.unwrap();
		while let Some(Ok(_)) = ws.next().await {}
	})
	.await;
	let client =
		VcmpClient::builder(format!("ws://{addr}/")).reconnect(Backoff::fixed(Duration::from_secs(60))).build();
	client.start();
	connected(&client).await;
	let started = Instant::now();
	let pending = {
		let client = client.clone();
		tokio::spawn(async move { client.send(&Void {}).await })
	};
	disconnected(&client).await;
	// echo after 100 ms, then the watchdog fires 2 x 100 ms later
	let elapsed = started.elapsed();
	assert!(elapsed >= Duration::from_millis(300) && elapsed < Duration::from_secs(2), "{elapsed:?}");
	let error = pending.await.unwrap().unwrap_err();
	assert_eq!((error.status(), error.title()), (503, "Session closed"));
	assert!(error.is_transport());
	client.stop();
}

#[tokio::test]
async fn closes_when_no_initial_heartbeat_arrives_and_reconnects() {
	init_tracing();
	let (addr, connections) = raw_server(|mut ws| async move { while let Some(Ok(_)) = ws.next().await {} }).await;
	let client = VcmpClient::builder(format!("ws://{addr}/"))
		.initial_heartbeat_timeout(Duration::from_millis(200))
		.reconnect(Backoff::fixed(Duration::from_millis(50)))
		.build();
	client.start();
	connected(&client).await;
	let started = Instant::now();
	disconnected(&client).await;
	assert!(started.elapsed() >= Duration::from_millis(200));
	// ... and the client reconnects
	wait_until(|| connections.load(Ordering::SeqCst) >= 2).await;
	client.stop();
}

#[tokio::test]
async fn initial_heartbeat_expectation_can_be_disabled() {
	init_tracing();
	let (addr, _) = raw_server(|mut ws| async move { while let Some(Ok(_)) = ws.next().await {} }).await;
	let client = VcmpClient::builder(format!("ws://{addr}/")).initial_heartbeat_timeout(Duration::ZERO).build();
	client.start();
	connected(&client).await;
	tokio::time::sleep(Duration::from_millis(300)).await;
	assert!(client.is_connected());
	client.stop();
}

async fn reconnects_after_a_server_restart(host: ServerHost) {
	init_tracing();
	let port = free_port();
	let (server, _endpoint) = start_hosted_server(host, port, Duration::from_secs(20)).await;
	let client = client(&format!("ws://127.0.0.1:{port}/test/a"));
	let opens = Arc::new(AtomicUsize::new(0));
	let closes = Arc::new(AtomicUsize::new(0));
	let closed_sessions: Arc<std::sync::Mutex<Vec<vcmp::Session>>> = Arc::default();
	let (o, c, cs) = (opens.clone(), closes.clone(), closed_sessions.clone());
	client.on_connected(move |_| {
		let o = o.clone();
		async move {
			o.fetch_add(1, Ordering::SeqCst);
		}
	});
	client.on_disconnected(move |session| {
		let c = c.clone();
		let cs = cs.clone();
		async move {
			assert!(!session.is_open());
			cs.lock().unwrap().push(session);
			c.fetch_add(1, Ordering::SeqCst);
		}
	});
	client.start();
	connected(&client).await;
	let first_session = client.session().unwrap();
	assert_eq!(client.send(&Void {}).await.unwrap(), serde_json::Value::Null);

	server.stop().await;
	disconnected(&client).await;
	wait_until(|| closes.load(Ordering::SeqCst) == 1).await;
	assert_eq!(closed_sessions.lock().unwrap().as_slice(), &[first_session]);
	let error = client.send(&Void {}).await.unwrap_err();
	assert_eq!((error.status(), error.title()), (503, "Not connected"));
	assert!(error.is_transport());

	let (server, _endpoint) = start_hosted_server(host, port, Duration::from_secs(20)).await;
	connected(&client).await;
	assert_eq!(client.send(&Void {}).await.unwrap(), serde_json::Value::Null);
	assert_eq!(opens.load(Ordering::SeqCst), 2);
	assert_eq!(closes.load(Ordering::SeqCst), 1);

	client.stop();
	wait_until(|| closes.load(Ordering::SeqCst) == 2).await;
	server.stop().await;
}

async fn stop_fails_pending_sends_and_does_not_reconnect(host: ServerHost) {
	init_tracing();
	let (server, endpoint) = start_hosted_server(host, 0, Duration::from_secs(20)).await;
	let client = client(&format!("ws://{}/test/a", server.local_addr()));
	client.start();
	connected(&client).await;
	let pending = {
		let client = client.clone();
		tokio::spawn(async move { client.send(&Never {}).await })
	};
	tokio::time::sleep(Duration::from_millis(100)).await;
	client.stop();
	let error = pending.await.unwrap().unwrap_err();
	assert_eq!((error.status(), error.title()), (503, "Session closed"));
	assert!(error.is_transport());
	wait_until(|| endpoint.session_count() == 0).await;
	tokio::time::sleep(Duration::from_millis(400)).await;
	assert!(!client.is_connected());
	assert_eq!(endpoint.session_count(), 0);
	let error = client.send(&Void {}).await.unwrap_err();
	assert_eq!(error.status(), 503);
	assert!(error.is_transport());
	server.stop().await;
}

async fn start_cancels_a_pending_reconnect_and_replaces_the_session(host: ServerHost) {
	init_tracing();
	let port = free_port();
	let (server, _endpoint) = start_hosted_server(host, port, Duration::from_secs(20)).await;
	let client = VcmpClient::builder(format!("ws://127.0.0.1:{port}/test/a"))
		.reconnect(Backoff::fixed(Duration::from_secs(60)))
		.build();
	client.start();
	connected(&client).await;
	server.stop().await;
	disconnected(&client).await;
	// a reconnect is pending in 60 s; start() must connect right away instead
	let (server, endpoint) = start_hosted_server(host, port, Duration::from_secs(20)).await;
	client.start();
	connected(&client).await;
	assert_eq!(client.send(&Void {}).await.unwrap(), serde_json::Value::Null);
	// start() on a connected client replaces the session exactly once
	let first = client.session().unwrap();
	client.start();
	connected(&client).await;
	wait_until(|| client.session().is_some_and(|s| s != first && s.is_open())).await;
	wait_until(|| endpoint.session_count() == 1).await;
	tokio::time::sleep(Duration::from_millis(300)).await;
	assert_eq!(endpoint.session_count(), 1);
	client.stop();
	server.stop().await;
}

async fn keeps_retrying_when_the_server_is_unreachable_or_rejects(host: ServerHost) {
	init_tracing();
	let port = free_port();
	let client = client(&format!("ws://127.0.0.1:{port}/nope"));
	client.start();
	tokio::time::sleep(Duration::from_millis(300)).await;
	assert!(!client.is_connected());
	// a server without that endpoint rejects with 404; the client keeps retrying
	let (server, _endpoint) = start_hosted_server(host, port, Duration::from_secs(20)).await;
	tokio::time::sleep(Duration::from_millis(300)).await;
	assert!(!client.is_connected());
	client.stop();
	server.stop().await;
}

#[tokio::test]
async fn send_without_session_fails_with_not_connected() {
	let client = VcmpClient::builder("ws://127.0.0.1:1/").build();
	let error = client.send(&Void {}).await.unwrap_err();
	assert_eq!((error.status(), error.title()), (503, "Not connected"));
	assert!(error.is_transport());
}

#[tokio::test]
async fn reconnect_generates_new_credentials_for_each_attempt() {
	let server = vcmp::VcmpServer::builder().build();
	let endpoint = server.endpoint("/fresh");
	let (headers, mut received) = tokio::sync::mpsc::channel(4);
	endpoint.on_connected(move |session| {
		let headers = headers.clone();
		async move {
			headers.send(session.connect_info().unwrap().header("authorization").unwrap().to_owned()).await.unwrap();
		}
	});
	let handle = server.bind("127.0.0.1:0").await.unwrap();
	let attempts = Arc::new(AtomicUsize::new(0));
	let client = VcmpClient::builder(format!("ws://{}/fresh", handle.local_addr()))
		.header("authorization", "stale")
		.headers_per_attempt({
			let attempts = attempts.clone();
			move || {
				let attempt = attempts.fetch_add(1, Ordering::SeqCst);
				async move { Ok(vec![("authorization".into(), format!("fresh-{attempt}"))]) }
			}
		})
		.reconnect(Backoff::fixed(Duration::from_millis(10)))
		.build();
	client.start();
	assert_eq!(received.recv().await.unwrap(), "fresh-0");
	endpoint.sessions()[0].close();
	assert_eq!(timeout(Duration::from_secs(2), received.recv()).await.unwrap().unwrap(), "fresh-1");
	client.stop_and_wait().await;
	handle.stop().await;
}

#[tokio::test]
async fn stop_cancels_an_open_hook_and_replacement_cancels_old_handlers() {
	let (server, endpoint) = start_hosted_server(ServerHost::Bare, 0, Duration::from_secs(20)).await;
	let client = client(&format!("ws://{}/test/a", server.local_addr()));
	let old_work = Arc::new(AtomicUsize::new(0));
	let release = Arc::new(tokio::sync::Notify::new());
	client.on({
		let old_work = old_work.clone();
		let release = release.clone();
		move |_: Never, _| {
			let old_work = old_work.clone();
			let release = release.clone();
			async move {
				release.notified().await;
				old_work.fetch_add(1, Ordering::SeqCst);
				Ok(())
			}
		}
	});
	client.on_connected(|_| std::future::pending());
	client.start();
	connected(&client).await;
	let old_session = client.session().unwrap();
	wait_until(|| endpoint.session_count() == 1).await;
	let peer = endpoint.sessions()[0].clone();
	let send = tokio::spawn(async move { peer.send(&Never {}).await });
	wait_until(|| old_session.resources().handler_tasks == 1).await;
	client.start();
	wait_until(|| client.session().is_some_and(|session| session.id() != old_session.id())).await;
	old_session.closed().await;
	release.notify_waiters();
	assert!(send.await.unwrap().is_err());
	assert_eq!(old_work.load(Ordering::SeqCst), 0);
	timeout(Duration::from_secs(1), client.stop_and_wait()).await.unwrap();
	assert!(!client.is_connected());
	server.stop().await;
}

#[tokio::test]
async fn stop_cancels_pending_credential_generation_and_releases_connection_budget() {
	let budget = vcmp::ResourceBudget::default();
	let started = Arc::new(AtomicUsize::new(0));
	let client = VcmpClient::builder("ws://127.0.0.1:1/secret?token=secret")
		.resource_budget(budget.clone())
		.headers_per_attempt({
			let started = started.clone();
			move || {
				started.fetch_add(1, Ordering::SeqCst);
				std::future::pending()
			}
		})
		.build();
	assert!(!format!("{client:?}").contains("secret"));
	client.start();
	wait_until(|| started.load(Ordering::SeqCst) == 1).await;
	timeout(Duration::from_secs(1), client.stop_and_wait()).await.unwrap();
	assert_eq!(budget.snapshot().connections, 0);
}

struct NotifyOnDrop(Arc<tokio::sync::Notify>);

impl Drop for NotifyOnDrop {
	fn drop(&mut self) {
		self.0.notify_one();
	}
}

#[tokio::test]
async fn canceled_stop_wait_keeps_old_close_hook_under_client_control() {
	restart_cancels_close_hook_while_stopping(true).await;
}

#[tokio::test]
async fn restart_cancels_old_close_hook_during_an_active_stop_wait() {
	restart_cancels_close_hook_while_stopping(false).await;
}

async fn restart_cancels_close_hook_while_stopping(cancel_wait: bool) {
	let (server, _) = start_hosted_server(ServerHost::Bare, 0, Duration::from_secs(20)).await;
	let client = client(&format!("ws://{}/test/a", server.local_addr()));
	let entered = Arc::new(tokio::sync::Notify::new());
	let release = Arc::new(tokio::sync::Notify::new());
	let dropped = Arc::new(tokio::sync::Notify::new());
	let completed = Arc::new(AtomicUsize::new(0));
	client.on_disconnected({
		let entered = entered.clone();
		let release = release.clone();
		let dropped = dropped.clone();
		let completed = completed.clone();
		move |_| {
			let entered = entered.clone();
			let release = release.clone();
			let dropped = dropped.clone();
			let completed = completed.clone();
			async move {
				let _guard = NotifyOnDrop(dropped);
				entered.notify_one();
				release.notified().await;
				completed.fetch_add(1, Ordering::SeqCst);
			}
		}
	});
	client.start();
	connected(&client).await;
	let previous = client.session().unwrap();
	let mut stopped = Some(Box::pin(client.stop_and_wait()));
	timeout(Duration::from_secs(1), async {
		tokio::select! {
			_ = entered.notified() => {},
			_ = stopped.as_mut().unwrap() => panic!("shutdown returned before its close hook finished"),
		}
	})
	.await
	.unwrap();
	if cancel_wait {
		drop(stopped.take());
	}
	client.start();
	// The previous generation must end even though its callback has not been released.
	timeout(Duration::from_secs(1), dropped.notified()).await.unwrap();
	if let Some(stopped) = stopped {
		timeout(Duration::from_secs(1), stopped).await.unwrap();
	}
	connected(&client).await;
	assert_ne!(client.session().unwrap(), previous);
	release.notify_one();
	assert_eq!(completed.load(Ordering::SeqCst), 0);
	assert_eq!(client.send(&Void {}).await.unwrap(), serde_json::Value::Null);
	client.on_disconnected(|_| async {});
	client.stop_and_wait().await;
	server.stop().await;
}

#[tokio::test]
async fn concurrent_stop_waiters_both_wait_for_the_close_hook() {
	let (server, _) = start_hosted_server(ServerHost::Bare, 0, Duration::from_secs(20)).await;
	let client = client(&format!("ws://{}/test/a", server.local_addr()));
	let entered = Arc::new(tokio::sync::Notify::new());
	let release = Arc::new(tokio::sync::Notify::new());
	client.on_disconnected({
		let entered = entered.clone();
		let release = release.clone();
		move |_| {
			let entered = entered.clone();
			let release = release.clone();
			async move {
				entered.notify_one();
				release.notified().await;
			}
		}
	});
	client.start();
	connected(&client).await;
	let first = client.stop_and_wait();
	tokio::pin!(first);
	timeout(Duration::from_secs(1), async {
		tokio::select! {
			_ = entered.notified() => {},
			_ = &mut first => panic!("shutdown returned before its close hook finished"),
		}
	})
	.await
	.unwrap();
	let second = client.stop_and_wait();
	tokio::pin!(second);
	std::future::poll_fn(|cx| {
		use std::future::Future;
		assert!(first.as_mut().poll(cx).is_pending());
		assert!(second.as_mut().poll(cx).is_pending(), "every shutdown waiter must await the old task");
		std::task::Poll::Ready(())
	})
	.await;
	release.notify_one();
	timeout(Duration::from_secs(1), async { tokio::join!(first, second) }).await.unwrap();
	assert!(!client.is_connected());
	server.stop().await;
}
