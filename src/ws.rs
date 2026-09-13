//! Adapters between `tokio-tungstenite` and the transport-agnostic [`Session`](crate::Session).

use crate::session::{Session, SessionOptions};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{Sink, SinkExt, Stream, StreamExt, future};
use std::fmt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::protocol::frame::Frame as WsFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::{Data, OpCode};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tracing::{debug, warn};

/// Transport settings shared by client and server.
#[derive(Debug, Clone)]
pub(crate) struct TransportOptions {
	/// Split outgoing text messages into WebSocket frames of at most this many bytes.
	/// `None` sends every message as a single frame.
	pub fragment_size: Option<usize>,
	/// The maximum size of an incoming message.
	pub max_message_size: Option<usize>,
}

impl Default for TransportOptions {
	fn default() -> Self {
		TransportOptions { fragment_size: None, max_message_size: Some(64 << 20) }
	}
}

impl TransportOptions {
	pub(crate) fn websocket_config(&self) -> WebSocketConfig {
		WebSocketConfig::default().max_message_size(self.max_message_size).max_frame_size(self.max_message_size)
	}
}

/// Spawns a [`Session`] over an established WebSocket.
pub(crate) fn spawn_session<S>(options: SessionOptions, transport: &TransportOptions, ws: WebSocketStream<S>) -> Session
where
	S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
	let (sink, stream) = ws.split();
	Session::spawn(options, incoming(stream), outgoing(sink, transport.fragment_size))
}

/// Maps the WebSocket stream to VCMP's text frames: text messages pass through, binary frames
/// are logged and dropped (unsupported, like in the Java implementation), control frames are
/// handled by tungstenite, and a close frame or error ends the stream.
fn incoming<S>(stream: SplitStream<WebSocketStream<S>>) -> impl Stream<Item = String> + Send + Unpin + 'static
where
	S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
	stream
		.take_while(|message| {
			future::ready(match message {
				Ok(Message::Close(frame)) => {
					debug!("received close frame: {frame:?}");
					false
				}
				Err(error) => {
					debug!("transport error: {error}");
					false
				}
				_ => true,
			})
		})
		.filter_map(|message| {
			future::ready(match message {
				Ok(Message::Text(text)) => Some(text.to_string()),
				Ok(Message::Binary(_)) => {
					warn!("received binary message, which is unsupported");
					None
				}
				_ => None,
			})
		})
}

/// Maps VCMP's text frames to WebSocket messages, optionally fragmenting large ones.
fn outgoing<S>(
	sink: SplitSink<WebSocketStream<S>, Message>,
	fragment_size: Option<usize>,
) -> impl Sink<String, Error = WsError> + Send + Unpin + 'static
where
	S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
	sink.with_flat_map(move |text: String| {
		futures_util::stream::iter(messages(text, fragment_size).into_iter().map(Ok))
	})
}

/// Splits a text message into WebSocket frames of at most `fragment_size` bytes, preferring
/// character boundaries. If a character cannot fit, its bytes span multiple frames; WebSocket
/// text messages need valid UTF-8 only after reassembly.
fn messages(text: String, fragment_size: Option<usize>) -> Vec<Message> {
	match fragment_size {
		Some(size) if size > 0 && text.len() > size => {
			let mut frames = Vec::with_capacity(text.len().div_ceil(size));
			let mut start = 0;
			while start < text.len() {
				let limit = start + (text.len() - start).min(size);
				let mut end = limit;
				while end > start && !text.is_char_boundary(end) {
					end -= 1;
				}
				if end == start {
					end = limit;
				}
				let opcode = OpCode::Data(if start == 0 { Data::Text } else { Data::Continue });
				let chunk = text.as_bytes()[start..end].to_vec();
				frames.push(Message::Frame(WsFrame::message(chunk, opcode, end == text.len())));
				start = end;
			}
			frames
		}
		_ => vec![Message::Text(text.into())],
	}
}

/// A displayable wrapper for the header list of a connect info.
pub(crate) struct Headers<'a>(pub &'a tokio_tungstenite::tungstenite::http::HeaderMap);

impl Headers<'_> {
	pub(crate) fn to_vec(&self) -> Vec<(String, String)> {
		self.0
			.iter()
			.map(|(name, value)| (name.as_str().to_owned(), String::from_utf8_lossy(value.as_bytes()).into_owned()))
			.collect()
	}
}

impl fmt::Debug for Headers<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_list().entries(self.to_vec()).finish()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn sends_small_messages_as_one_text_frame() {
		let frames = messages("hello".into(), Some(8));
		assert!(matches!(&frames[..], [Message::Text(text)] if text.as_str() == "hello"));
		let frames = messages("x".repeat(100), None);
		assert_eq!(frames.len(), 1);
	}

	#[test]
	fn fragments_large_messages_on_char_boundaries() {
		let text = "aä".repeat(10); // 30 bytes, 20 chars
		let frames = messages(text.clone(), Some(4));
		let mut reassembled = String::new();
		for (i, frame) in frames.iter().enumerate() {
			let Message::Frame(frame) = frame else { panic!("expected a raw frame") };
			assert!(frame.payload().len() <= 4);
			let expected = OpCode::Data(if i == 0 { Data::Text } else { Data::Continue });
			assert_eq!(frame.header().opcode, expected);
			assert_eq!(frame.header().is_final, i == frames.len() - 1);
			reassembled.push_str(frame.to_text().unwrap());
		}
		assert_eq!(reassembled, text);
	}

	#[test]
	fn fragments_characters_larger_than_the_frame_limit() {
		let text = "ä水🦀aä水🦀";
		for size in 1..=3 {
			let frames = messages(text.into(), Some(size));
			let mut reassembled = Vec::new();
			for (i, frame) in frames.iter().enumerate() {
				let Message::Frame(frame) = frame else { panic!("expected a raw frame") };
				assert!(!frame.payload().is_empty());
				assert!(frame.payload().len() <= size);
				let expected = OpCode::Data(if i == 0 { Data::Text } else { Data::Continue });
				assert_eq!(frame.header().opcode, expected);
				assert_eq!(frame.header().is_final, i == frames.len() - 1);
				reassembled.extend_from_slice(frame.payload());
			}
			assert_eq!(String::from_utf8(reassembled).unwrap(), text);
		}
	}

	#[tokio::test]
	async fn peer_reassembles_text_split_inside_characters() {
		use tokio_tungstenite::tungstenite::protocol::Role;

		let text = "ä水🦀aä水🦀";
		for size in 1..=3 {
			let (sender, receiver) = tokio::io::duplex(4096);
			let mut sender = WebSocketStream::from_raw_socket(sender, Role::Server, None).await;
			let config = WebSocketConfig::default().max_frame_size(Some(size));
			let mut receiver = WebSocketStream::from_raw_socket(receiver, Role::Client, Some(config)).await;
			for frame in messages(text.into(), Some(size)) {
				sender.send(frame).await.unwrap();
			}
			let message = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.next())
				.await
				.unwrap()
				.unwrap()
				.unwrap();
			assert!(matches!(message, Message::Text(received) if received == text));
		}
	}
}
