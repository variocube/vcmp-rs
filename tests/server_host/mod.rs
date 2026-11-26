//! Runs the same socket and interoperability contracts against either server host.
#![allow(dead_code, unused_macros, unused_imports)]

use std::net::SocketAddr;
use tokio::net::TcpSocket;
use vcmp::{ServerHandle, VcmpServer};

/// Reserves a port for a server a test starts later, after a client already has its URL: a socket bound to
/// `127.0.0.1:0` that does not listen, so nothing else can bind the port while it lives. Drop it right before binding
/// the server; that gap is the only window in which another process could take the port. Connections to it meanwhile
/// fail, but how depends on the OS: Linux refuses them, BSD/macOS drop the SYN, so the connect hangs until its timeout.
/// For a URL that only has to be unreachable, use port 1 instead (nothing listens there, and it is refused everywhere).
pub struct ReservedPort {
	socket: TcpSocket,
}

impl ReservedPort {
	pub fn bind() -> Self {
		let socket = TcpSocket::new_v4().unwrap();
		socket.bind(([127, 0, 0, 1], 0).into()).unwrap();
		Self { socket }
	}

	pub fn port(&self) -> u16 {
		self.socket.local_addr().unwrap().port()
	}
}

#[derive(Clone, Copy, Debug)]
pub enum ServerHost {
	Bare,
	#[cfg(feature = "axum")]
	Axum,
}

impl ServerHost {
	/// Binds `127.0.0.1:port`; port `0` picks a free one, read it from [`HostHandle::local_addr`].
	pub async fn bind(self, server: VcmpServer, port: u16) -> HostHandle {
		match self {
			Self::Bare => HostHandle::Bare(server.bind(("127.0.0.1", port)).await.unwrap()),
			#[cfg(feature = "axum")]
			Self::Axum => {
				let router = server
					.endpoints()
					.iter()
					.fold(axum::Router::new(), |router, endpoint| router.route(endpoint.path(), endpoint.axum_route()));
				HostHandle::axum(server, router, port).await
			}
		}
	}
}

pub enum HostHandle {
	Bare(ServerHandle),
	#[cfg(feature = "axum")]
	Axum {
		server: VcmpServer,
		local_addr: SocketAddr,
		accept: tokio::task::JoinHandle<()>,
	},
}

impl HostHandle {
	#[cfg(feature = "axum")]
	pub async fn axum(server: VcmpServer, router: axum::Router, port: u16) -> Self {
		let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await.unwrap();
		let local_addr = listener.local_addr().unwrap();
		let accept = tokio::spawn(async move {
			axum::serve(listener, router.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
		});
		Self::Axum { server, local_addr, accept }
	}

	pub fn local_addr(&self) -> SocketAddr {
		match self {
			Self::Bare(handle) => handle.local_addr(),
			#[cfg(feature = "axum")]
			Self::Axum { local_addr, .. } => *local_addr,
		}
	}

	pub async fn stop(self) {
		match self {
			Self::Bare(handle) => handle.stop().await,
			#[cfg(feature = "axum")]
			Self::Axum { server, accept, .. } => {
				accept.abort();
				let _ = accept.await;
				server.close_sessions().await;
			}
		}
	}
}

macro_rules! server_tests {
	($($scenario:ident),+ $(,)?) => {
		mod bare {
			$(
				#[tokio::test]
				async fn $scenario() {
					super::$scenario(super::ServerHost::Bare).await;
				}
			)+
		}

		#[cfg(feature = "axum")]
		mod axum_hosted {
			$(
				#[tokio::test]
				async fn $scenario() {
					super::$scenario(super::ServerHost::Axum).await;
				}
			)+
		}
	};
}

pub(crate) use server_tests;
