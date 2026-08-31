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

/// Splits a text message into WebSocket frames of at most `fragment_size` bytes (on character
/// boundaries), like the Java client's 8 KB `MAX_TEXT_MESSAGE_BUFFER_SIZE` does.
fn messages(text: String, fragment_size: Option<usize>) -> Vec<Message> {
	match fragment_size {
		Some(size) if size > 0 && text.len() > size => {
			let mut frames = Vec::with_capacity(text.len().div_ceil(size));
			let mut rest = text.as_str();
			let mut first = true;
			while !rest.is_empty() {
				let mut end = rest.len().min(size);
				while !rest.is_char_boundary(end) {
					end -= 1;
				}
				let (chunk, tail) = rest.split_at(end);
				rest = tail;
				let opcode = OpCode::Data(if first { Data::Text } else { Data::Continue });
				first = false;
				frames.push(Message::Frame(WsFrame::message(chunk.to_owned(), opcode, rest.is_empty())));
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
}
