//! Runs the same socket and interoperability contracts against either server host.
#![allow(dead_code, unused_macros, unused_imports)]

use std::net::SocketAddr;
use vcmp::{ServerHandle, VcmpServer};

#[derive(Clone, Copy, Debug)]
pub enum ServerHost {
	Bare,
	#[cfg(feature = "axum")]
	Axum,
}

impl ServerHost {
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
