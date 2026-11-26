//! Adapters between `tokio-tungstenite` and the transport-agnostic [`Session`](crate::Session).

use crate::resources::{Permit, ResourceBudget};
use crate::session::{BINARY_MESSAGE, CloseSlot, Session, SessionLimits, SessionOptions};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{Sink, Stream, StreamExt, future, ready};
#[cfg(feature = "server")]
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::protocol::frame::coding::{CloseCode, Data, OpCode};
use tokio_tungstenite::tungstenite::protocol::frame::{CloseFrame, Frame as WsFrame};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tracing::debug;

/// Transport settings shared by client and server.
#[derive(Debug, Clone)]
pub(crate) struct TransportOptions {
	/// Split outgoing text messages into WebSocket frames of at most this many bytes.
	/// `None` sends every message as a single frame.
	pub fragment_size: Option<usize>,
	/// The maximum size of an incoming message.
	pub max_message_size: Option<usize>,
	pub limits: SessionLimits,
	pub budget: ResourceBudget,
}

impl Default for TransportOptions {
	fn default() -> Self {
		TransportOptions {
			fragment_size: None,
			max_message_size: Some(2 << 20),
			limits: SessionLimits::default(),
			budget: ResourceBudget::default(),
		}
	}
}

impl TransportOptions {
	pub(crate) fn websocket_config(&self) -> WebSocketConfig {
		WebSocketConfig::default().max_message_size(self.max_message_size).max_frame_size(self.max_message_size)
	}
}

/// Spawns a [`Session`] over an established WebSocket.
pub(crate) fn spawn_session<S>(
	mut options: SessionOptions,
	transport: &TransportOptions,
	ws: WebSocketStream<S>,
	connection: Permit,
) -> Session
where
	S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
	let (sink, stream) = ws.split();
	options.limits = transport.limits.clone();
	options.budget = transport.budget.clone();
	let close = CloseSlot::default();
	let outgoing = Outgoing { sink, fragment_size: transport.fragment_size, buffered: None, close: close.clone() };
	Session::spawn_admitted(options, incoming(stream), outgoing, connection, close)
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
				Ok(Message::Close(_)) => {
					debug!("received close frame");
					false
				}
				Err(_) => {
					debug!("transport error");
					false
				}
				_ => true,
			})
		})
		.filter_map(|message| {
			future::ready(match message {
				Ok(Message::Text(text)) => Some(text.to_string()),
				Ok(Message::Binary(_)) => {
					// Unsupported: only a marker enters the session (its payload never does), which drops it, or
					// closes a session that is not authenticated yet.
					Some(BINARY_MESSAGE.to_owned())
				}
				_ => None,
			})
		})
}

/// Maps VCMP's text frames to WebSocket messages, optionally fragmenting large ones. Closing
/// sends the session's close code and reason, if it set one.
struct Outgoing<S> {
	sink: SplitSink<WebSocketStream<S>, Message>,
	fragment_size: Option<usize>,
	/// The fragments of the message being sent.
	buffered: Option<Fragments>,
	/// Taken when the close frame is sent.
	close: CloseSlot,
}

impl<S> Outgoing<S>
where
	S: AsyncRead + AsyncWrite + Unpin,
{
	fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
		while let Some(fragments) = &mut self.buffered {
			if fragments.done {
				self.buffered = None;
				break;
			}
			ready!(Pin::new(&mut self.sink).poll_ready(cx))?;
			if let Some(message) = fragments.next() {
				Pin::new(&mut self.sink).start_send(message)?;
			}
		}
		Poll::Ready(Ok(()))
	}
}

impl<S> Sink<String> for Outgoing<S>
where
	S: AsyncRead + AsyncWrite + Unpin,
{
	type Error = WsError;

	fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
		self.get_mut().poll_drain(cx)
	}

	fn start_send(self: Pin<&mut Self>, text: String) -> Result<(), WsError> {
		let this = self.get_mut();
		this.buffered = Some(Fragments::new(text, this.fragment_size));
		Ok(())
	}

	fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
		let this = self.get_mut();
		ready!(this.poll_drain(cx))?;
		Pin::new(&mut this.sink).poll_flush(cx)
	}

	fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
		let this = self.get_mut();
		ready!(this.poll_drain(cx))?;
		let reason = this.close.lock().unwrap_or_else(|e| e.into_inner()).clone();
		if let Some(reason) = reason {
			ready!(Pin::new(&mut this.sink).poll_ready(cx))?;
			// Detach from the slot so that a re-polled close does not send the frame again; the
			// session keeps the reason for Session::close_reason.
			let frame = CloseFrame { code: CloseCode::from(reason.code), reason: reason.reason.into_owned().into() };
			this.close = CloseSlot::default();
			Pin::new(&mut this.sink).start_send(Message::Close(Some(frame)))?;
		}
		Pin::new(&mut this.sink).poll_close(cx)
	}
}

/// Splits a text message into WebSocket frames of at most `fragment_size` bytes, preferring
/// character boundaries. If a character cannot fit, its bytes span multiple frames; WebSocket
/// text messages need valid UTF-8 only after reassembly.
struct Fragments {
	text: String,
	start: usize,
	size: usize,
	done: bool,
}

impl Fragments {
	fn new(text: String, fragment_size: Option<usize>) -> Self {
		let size = fragment_size.filter(|size| *size > 0).unwrap_or(usize::MAX);
		Fragments { text, start: 0, size, done: false }
	}
}

impl Iterator for Fragments {
	type Item = Message;

	fn next(&mut self) -> Option<Message> {
		if self.done {
			return None;
		}
		let (text, start, size) = (&self.text, self.start, self.size);
		if start == 0 && text.len() <= size {
			self.done = true;
			return Some(Message::Text(std::mem::take(&mut self.text).into()));
		}
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
		self.done = end == text.len();
		self.start = end;
		if self.done {
			// Release the message as soon as its last fragment is handed to the transport.
			self.text = String::new();
		}
		Some(Message::Frame(WsFrame::message(chunk, opcode, self.done)))
	}
}

#[cfg(test)]
fn messages(text: String, fragment_size: Option<usize>) -> Vec<Message> {
	Fragments::new(text, fragment_size).collect()
}

/// A displayable wrapper for the header list of a connect info.
#[cfg(feature = "server")]
pub(crate) struct Headers<'a>(pub &'a tokio_tungstenite::tungstenite::http::HeaderMap);

#[cfg(feature = "server")]
impl Headers<'_> {
	pub(crate) fn to_vec(&self) -> Vec<(String, String)> {
		self.0
			.iter()
			.map(|(name, value)| (name.as_str().to_owned(), String::from_utf8_lossy(value.as_bytes()).into_owned()))
			.collect()
	}
}

#[cfg(feature = "server")]
impl fmt::Debug for Headers<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("Headers([redacted])")
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use futures_util::SinkExt;

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
