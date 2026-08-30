//! The VCMP frame codec.
//!
//! Every VCMP frame is a single WebSocket *text* message that starts with a three-letter type:
//!
//! | Frame | Layout                                                          |
//! |-------|-----------------------------------------------------------------|
//! | `MSG` | `MSG` + id (12 chars) + JSON payload                            |
//! | `ACK` | `ACK` + id + optional JSON payload                              |
//! | `NAK` | `NAK` + id + optional JSON payload (a [`ProblemDetail`])         |
//! | `HBT` | `HBT` + heartbeat interval in milliseconds (decimal ASCII)      |
//!
//! Ids are 9 random bytes, base64url-encoded without padding — exactly 12 characters.
//!
//! [`ProblemDetail`]: crate::ProblemDetail

use std::fmt;

/// Length of the frame type prefix.
pub const TYPE_LENGTH: usize = 3;
/// Length of a frame id.
pub const ID_LENGTH: usize = 12;
/// Number of random bytes in a frame id (encodes to [`ID_LENGTH`] base64url characters).
const ID_BYTES: usize = 9;
const PAYLOAD_INDEX: usize = TYPE_LENGTH + ID_LENGTH;

/// A parsed VCMP frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
	/// A message that expects an `ACK` or `NAK` with the same id.
	Message {
		/// The frame id.
		id: String,
		/// The JSON payload. May be empty on the wire; the session treats that as `{}`.
		payload: String,
	},
	/// Positive acknowledgement of a message, optionally carrying the handler's serialized result.
	Ack {
		/// The id of the acknowledged message.
		id: String,
		/// The serialized result, `None` when the handler returned nothing.
		payload: Option<String>,
	},
	/// Negative acknowledgement of a message, optionally carrying a serialized problem detail.
	Nak {
		/// The id of the rejected message.
		id: String,
		/// The serialized problem detail; `None` means "no handler for this type".
		payload: Option<String>,
	},
	/// A heartbeat with the interval (in milliseconds) after which the receiver echoes it.
	Heartbeat {
		/// The heartbeat interval in milliseconds. Zero is syntactically valid but ignored.
		interval: u64,
	},
}

/// Why a raw text frame could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
	/// The frame is shorter than the type prefix.
	#[error("frame too short: {0} bytes")]
	TooShort(usize),
	/// The type prefix is none of `MSG`, `ACK`, `NAK`, `HBT`.
	#[error("invalid frame type: {0:?}")]
	InvalidType(String),
	/// A `MSG`/`ACK`/`NAK` frame does not carry a full 12-character id.
	#[error("frame id is incomplete")]
	IncompleteId,
	/// The `HBT` interval is not a decimal number.
	#[error("invalid heartbeat interval: {0:?}")]
	InvalidInterval(String),
}

impl Frame {
	/// Creates a `MSG` frame with a freshly generated id.
	pub fn message(payload: impl Into<String>) -> Self {
		Frame::Message { id: generate_id(), payload: payload.into() }
	}

	/// Creates an `ACK` frame. An empty payload is normalized to `None`, matching the wire format
	/// (where the two are indistinguishable).
	pub fn ack(id: impl Into<String>, payload: Option<String>) -> Self {
		Frame::Ack { id: id.into(), payload: payload.filter(|p| !p.is_empty()) }
	}

	/// Creates a `NAK` frame. An empty payload is normalized to `None`.
	pub fn nak(id: impl Into<String>, payload: Option<String>) -> Self {
		Frame::Nak { id: id.into(), payload: payload.filter(|p| !p.is_empty()) }
	}

	/// Creates a `HBT` frame.
	pub fn heartbeat(interval: u64) -> Self {
		Frame::Heartbeat { interval }
	}

	/// The three-letter type of this frame.
	pub fn kind(&self) -> &'static str {
		match self {
			Frame::Message { .. } => "MSG",
			Frame::Ack { .. } => "ACK",
			Frame::Nak { .. } => "NAK",
			Frame::Heartbeat { .. } => "HBT",
		}
	}

	/// The id of a `MSG`/`ACK`/`NAK` frame; `None` for heartbeats.
	pub fn id(&self) -> Option<&str> {
		match self {
			Frame::Message { id, .. } | Frame::Ack { id, .. } | Frame::Nak { id, .. } => Some(id),
			Frame::Heartbeat { .. } => None,
		}
	}

	/// Parses a raw text frame.
	pub fn parse(raw: &str) -> Result<Frame, FrameError> {
		let kind = raw.get(..TYPE_LENGTH).ok_or(FrameError::TooShort(raw.len()))?;
		match kind {
			"HBT" => {
				let interval = &raw[TYPE_LENGTH..];
				interval
					.parse::<u64>()
					.map(|interval| Frame::Heartbeat { interval })
					.map_err(|_| FrameError::InvalidInterval(interval.to_owned()))
			}
			"MSG" | "ACK" | "NAK" => {
				// Ids are 12 ASCII characters in every implementation; the reference implementations
				// slice by (UTF-16) characters, which coincides with bytes exactly for ASCII.
				let id = raw
					.get(TYPE_LENGTH..PAYLOAD_INDEX)
					.filter(|id| id.is_ascii())
					.ok_or(FrameError::IncompleteId)?
					.to_owned();
				let payload = raw.get(PAYLOAD_INDEX..).unwrap_or_default();
				Ok(match kind {
					"MSG" => Frame::Message { id, payload: payload.to_owned() },
					"ACK" => Frame::ack(id, Some(payload.to_owned())),
					_ => Frame::nak(id, Some(payload.to_owned())),
				})
			}
			other => Err(FrameError::InvalidType(other.to_owned())),
		}
	}

	/// Serializes the frame into its wire representation.
	pub fn serialize(&self) -> String {
		let mut out = String::with_capacity(PAYLOAD_INDEX + self.payload_len());
		out.push_str(self.kind());
		match self {
			Frame::Message { id, payload } => {
				out.push_str(id);
				out.push_str(payload);
			}
			Frame::Ack { id, payload } | Frame::Nak { id, payload } => {
				out.push_str(id);
				if let Some(payload) = payload {
					out.push_str(payload);
				}
			}
			Frame::Heartbeat { interval } => {
				out.push_str(&interval.to_string());
			}
		}
		out
	}

	fn payload_len(&self) -> usize {
		match self {
			Frame::Message { payload, .. } => payload.len(),
			Frame::Ack { payload, .. } | Frame::Nak { payload, .. } => payload.as_ref().map_or(0, String::len),
			Frame::Heartbeat { .. } => 20,
		}
	}
}

impl fmt::Display for Frame {
	/// A log-friendly rendering: type, id and a truncated payload (like the Java `toString`).
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Frame::Heartbeat { interval } => write!(f, "HBT {interval}ms"),
			_ => {
				write!(f, "{}#{}", self.kind(), self.id().unwrap_or_default())?;
				let payload = match self {
					Frame::Message { payload, .. } => Some(payload.as_str()),
					Frame::Ack { payload, .. } | Frame::Nak { payload, .. } => payload.as_deref(),
					Frame::Heartbeat { .. } => None,
				};
				if let Some(payload) = payload {
					let end = payload.char_indices().nth(36).map_or(payload.len(), |(i, _)| i);
					write!(f, ":{}{}", &payload[..end], if end < payload.len() { "..." } else { "" })?;
				}
				Ok(())
			}
		}
	}
}

/// Generates a frame id: 9 random bytes, base64url without padding (12 characters).
pub fn generate_id() -> String {
	let mut bytes = [0u8; ID_BYTES];
	// Failing to obtain OS randomness is an environment error we cannot recover from meaningfully;
	// it never happens on the Linux targets this crate is built for.
	getrandom::fill(&mut bytes).expect("OS random number generator unavailable");
	base64url_no_pad(&bytes)
}

const BASE64URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Base64url (RFC 4648 §5) encoding without padding. Only used for ids, whose 9 bytes encode to
/// full 4-character groups, so no padding logic is needed for them — but partial groups are
/// handled for completeness.
fn base64url_no_pad(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
	for chunk in bytes.chunks(3) {
		let b0 = chunk[0] as u32;
		let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
		let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
		let triple = (b0 << 16) | (b1 << 8) | b2;
		let chars = chunk.len() + 1;
		for i in 0..chars {
			let index = (triple >> (18 - 6 * i)) & 0x3f;
			out.push(BASE64URL[index as usize] as char);
		}
	}
	out
}

#[cfg(test)]
mod tests {
	use super::*;
	use proptest::prelude::*;

	#[test]
	fn generates_ids_of_12_url_safe_chars() {
		for _ in 0..1000 {
			let id = generate_id();
			assert_eq!(id.len(), ID_LENGTH);
			assert!(id.bytes().all(|b| BASE64URL.contains(&b)), "{id}");
		}
	}

	#[test]
	fn generates_distinct_ids() {
		let ids: std::collections::HashSet<_> = (0..1000).map(|_| generate_id()).collect();
		assert_eq!(ids.len(), 1000);
	}

	#[test]
	fn base64url_matches_reference_vectors() {
		// RFC 4648 test vectors, with the url-safe alphabet and no padding.
		assert_eq!(base64url_no_pad(b""), "");
		assert_eq!(base64url_no_pad(b"f"), "Zg");
		assert_eq!(base64url_no_pad(b"fo"), "Zm8");
		assert_eq!(base64url_no_pad(b"foo"), "Zm9v");
		assert_eq!(base64url_no_pad(b"foobar"), "Zm9vYmFy");
		// A 9-byte value whose standard base64 contains '+' and '/'.
		assert_eq!(base64url_no_pad(&[0xfb, 0xff, 0xbf, 0xfb, 0xff, 0xbf, 0xfb, 0xff, 0xbf]), "-_-_-_-_-_-_");
	}

	#[test]
	fn serializes_like_the_reference_implementations() {
		assert_eq!(
			Frame::Message { id: "123456789012".into(), payload: "foo".into() }.serialize(),
			"MSG123456789012foo"
		);
		assert_eq!(Frame::ack("123456789012", None).serialize(), "ACK123456789012");
		assert_eq!(Frame::ack("123456789012", Some("bar".into())).serialize(), "ACK123456789012bar");
		assert_eq!(Frame::nak("123456789012", None).serialize(), "NAK123456789012");
		assert_eq!(Frame::nak("123456789012", Some("foo".into())).serialize(), "NAK123456789012foo");
		assert_eq!(Frame::heartbeat(1000).serialize(), "HBT1000");
	}

	#[test]
	fn parses_like_the_reference_implementations() {
		assert_eq!(
			Frame::parse("MSG123456789012foo").unwrap(),
			Frame::Message { id: "123456789012".into(), payload: "foo".into() }
		);
		assert_eq!(Frame::parse("ACK123456789012").unwrap(), Frame::ack("123456789012", None));
		assert_eq!(Frame::parse("ACK123456789012foo").unwrap(), Frame::ack("123456789012", Some("foo".into())));
		assert_eq!(Frame::parse("NAK123456789012").unwrap(), Frame::nak("123456789012", None));
		assert_eq!(Frame::parse("NAK123456789012foo").unwrap(), Frame::nak("123456789012", Some("foo".into())));
		assert_eq!(Frame::parse("HBT1000").unwrap(), Frame::heartbeat(1000));
		assert_eq!(Frame::parse("HBT0").unwrap(), Frame::heartbeat(0));
	}

	#[test]
	fn parses_an_empty_message_payload() {
		assert_eq!(
			Frame::parse("MSG123456789012").unwrap(),
			Frame::Message { id: "123456789012".into(), payload: String::new() }
		);
	}

	#[test]
	fn rejects_invalid_frames() {
		assert_eq!(Frame::parse("").unwrap_err(), FrameError::TooShort(0));
		assert_eq!(Frame::parse("MS").unwrap_err(), FrameError::TooShort(2));
		assert_eq!(Frame::parse("XXXsomething").unwrap_err(), FrameError::InvalidType("XXX".into()));
		assert_eq!(Frame::parse("MSG12345").unwrap_err(), FrameError::IncompleteId);
		assert_eq!(Frame::parse("MSGäääääää").unwrap_err(), FrameError::IncompleteId);
		assert_eq!(Frame::parse("HBT").unwrap_err(), FrameError::InvalidInterval(String::new()));
		assert_eq!(Frame::parse("HBTfoo").unwrap_err(), FrameError::InvalidInterval("foo".into()));
		assert_eq!(Frame::parse("HBT-5").unwrap_err(), FrameError::InvalidInterval("-5".into()));
	}

	#[test]
	fn displays_a_truncated_payload() {
		let frame = Frame::Message { id: "123456789012".into(), payload: "x".repeat(50) };
		assert_eq!(frame.to_string(), format!("MSG#123456789012:{}...", "x".repeat(36)));
		assert_eq!(Frame::heartbeat(20000).to_string(), "HBT 20000ms");
		assert_eq!(Frame::ack("123456789012", None).to_string(), "ACK#123456789012");
	}

	fn id_strategy() -> impl Strategy<Value = String> {
		proptest::collection::vec(proptest::sample::select(BASE64URL.to_vec()), ID_LENGTH)
			.prop_map(|bytes| String::from_utf8(bytes).unwrap())
	}

	fn frame_strategy() -> impl Strategy<Value = Frame> {
		prop_oneof![
			(id_strategy(), ".*").prop_map(|(id, payload)| Frame::Message { id, payload }),
			(id_strategy(), proptest::option::of(".+")).prop_map(|(id, payload)| Frame::ack(id, payload)),
			(id_strategy(), proptest::option::of(".+")).prop_map(|(id, payload)| Frame::nak(id, payload)),
			any::<u64>().prop_map(Frame::heartbeat),
		]
	}

	proptest! {
		#[test]
		fn parse_serialize_roundtrip(frame in frame_strategy()) {
			prop_assert_eq!(Frame::parse(&frame.serialize()).unwrap(), frame);
		}

		#[test]
		fn parse_never_panics(raw in ".*") {
			let _ = Frame::parse(&raw);
		}
	}
}
