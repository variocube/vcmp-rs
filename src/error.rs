//! Error types: the RFC 7807-style [`ProblemDetail`] carried by `NAK` frames and the
//! [`VcmpError`] every fallible operation of this crate reports.

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use std::error::Error as StdError;
use std::fmt;

/// The problem detail carried by a `NAK` frame (RFC 7807 shape, as in `vcmp-js` and Spring's
/// `ProblemDetail`).
///
/// Status codes describe the failure, not its origin: a peer may return the same status as a
/// local failure. Use [`VcmpError::is_transport`] to identify locally detected transport failures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProblemDetail {
	/// A short, human-readable summary of the problem type.
	#[serde(default, deserialize_with = "null_to_default")]
	pub title: String,
	/// The HTTP status code the problem maps to.
	#[serde(default = "default_status", deserialize_with = "null_to_status")]
	pub status: u16,
	/// A URI reference identifying the problem type.
	#[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
	pub type_: Option<String>,
	/// A URI reference identifying the specific occurrence.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub instance: Option<String>,
	/// A human-readable explanation specific to this occurrence.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub detail: Option<String>,
	/// Any additional members (both implementations allow arbitrary extra fields).
	#[serde(flatten)]
	pub extra: Map<String, Value>,
}

fn default_status() -> u16 {
	500
}

fn null_to_default<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
	Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

fn null_to_status<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u16, D::Error> {
	Ok(Option::<u16>::deserialize(deserializer)?.unwrap_or_else(default_status))
}

impl ProblemDetail {
	/// Creates a problem detail with the given status and title.
	pub fn new(status: u16, title: impl Into<String>) -> Self {
		ProblemDetail { title: title.into(), status, type_: None, instance: None, detail: None, extra: Map::new() }
	}

	/// Sets the `detail` member.
	pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
		self.detail = Some(detail.into());
		self
	}

	/// Sets the `type` member.
	pub fn with_type(mut self, type_: impl Into<String>) -> Self {
		self.type_ = Some(type_.into());
		self
	}

	/// Sets the `instance` member.
	pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
		self.instance = Some(instance.into());
		self
	}

	/// Adds an additional member.
	pub fn with_extra(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
		self.extra.insert(key.into(), value.into());
		self
	}
}

impl fmt::Display for ProblemDetail {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{} ({})", self.title, self.status)?;
		if let Some(detail) = &self.detail {
			write!(f, ": {detail}")?;
		}
		Ok(())
	}
}

/// The error type of every fallible VCMP operation.
///
/// Contains a [`ProblemDetail`] (the one sent in / received from a `NAK`), an optional source,
/// and a private origin classification used by [`Self::is_transport`]. Only the problem detail
/// is serialized. Cloning preserves the classification; converting to a problem detail or
/// serializing and deserializing loses it. Neither peer fields nor status codes can establish
/// local transport origin.
#[derive(Debug)]
pub struct VcmpError {
	// Boxed to keep `Result<_, VcmpError>` small (the problem detail carries a map).
	problem: Box<ProblemDetail>,
	source: Option<Box<dyn StdError + Send + Sync + 'static>>,
	kind: ErrorKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorKind {
	Other,
	LocalTransport,
	PeerNak,
}

impl VcmpError {
	/// Creates an application error with the given status and title. Even a `503` or `504`
	/// created here is not a local transport failure.
	pub fn new(status: u16, title: impl Into<String>) -> Self {
		ProblemDetail::new(status, title).into()
	}

	/// `400 Bad Request` with the given title.
	pub fn bad_request(title: impl Into<String>) -> Self {
		Self::new(400, title)
	}

	/// `500 Internal Server Error` with the given title.
	pub fn internal(title: impl Into<String>) -> Self {
		Self::new(500, title)
	}

	/// Sets the `detail` member.
	pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
		self.problem.detail = Some(detail.into());
		self
	}

	/// Sets the `type` member.
	pub fn with_type(mut self, type_: impl Into<String>) -> Self {
		self.problem.type_ = Some(type_.into());
		self
	}

	/// Sets the `instance` member.
	pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
		self.problem.instance = Some(instance.into());
		self
	}

	/// Adds an additional member.
	pub fn with_extra(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
		self.problem.extra.insert(key.into(), value.into());
		self
	}

	/// Attaches the underlying cause (not serialized).
	pub fn with_source(mut self, source: impl StdError + Send + Sync + 'static) -> Self {
		self.source = Some(Box::new(source));
		self
	}

	/// Converts any error into a `500` problem detail, with `title` = the error's type name and
	/// `detail` = its message (the same rule as `createVcmpError` in `vcmp-js`).
	///
	/// A `VcmpError` passed in is returned unchanged.
	pub fn from_error<E: StdError + Send + Sync + 'static>(error: E) -> Self {
		let mut any: Box<dyn std::any::Any> = Box::new(error);
		match any.downcast::<VcmpError>() {
			Ok(vcmp) => *vcmp,
			Err(other) => {
				any = other;
				let error = *any.downcast::<E>().expect("the value was boxed from E");
				VcmpError::internal(short_type_name::<E>()).with_detail(error.to_string()).with_source(error)
			}
		}
	}

	/// The problem detail (title, status, detail, …).
	pub fn problem(&self) -> &ProblemDetail {
		&self.problem
	}

	/// Consumes the error, returning its problem detail without its source or origin classification.
	pub fn into_problem(self) -> ProblemDetail {
		*self.problem
	}

	/// The `status` member.
	pub fn status(&self) -> u16 {
		self.problem.status
	}

	/// The `title` member.
	pub fn title(&self) -> &str {
		&self.problem.title
	}

	/// The `detail` member.
	pub fn detail(&self) -> Option<&str> {
		self.problem.detail.as_deref()
	}

	/// Whether VCMP detected a local transport failure: no connection, a closed session,
	/// acknowledgement timeout, or transport resource admission overload.
	///
	/// Peer NAKs and application-created errors return `false` regardless of status, title, or
	/// extra properties. Serialization, message size, and ACK decoding errors also return `false`.
	///
	/// This is not a replay guarantee: after a timeout or disconnect the peer may already have
	/// processed the message. The caller owns retry policy, including any retries of peer NAKs.
	pub fn is_transport(&self) -> bool {
		self.kind == ErrorKind::LocalTransport
	}

	fn transport(status: u16, title: &str) -> Self {
		let mut error = Self::new(status, title);
		error.kind = ErrorKind::LocalTransport;
		error
	}

	pub(crate) fn into_peer_error(mut self) -> Self {
		self.kind = ErrorKind::PeerNak;
		self
	}

	pub(crate) fn session_not_open(detail: &str) -> Self {
		Self::transport(503, "Session not open").with_detail(detail)
	}

	pub(crate) fn session_closed(detail: &str) -> Self {
		Self::transport(503, "Session closed").with_detail(detail)
	}

	pub(crate) fn not_connected(detail: &str) -> Self {
		Self::transport(503, "Not connected").with_detail(detail)
	}

	pub(crate) fn transport_overloaded() -> Self {
		Self::transport(503, "Transport overloaded")
	}

	pub(crate) fn acknowledgement_timeout() -> Self {
		Self::transport(504, "Acknowledgement timed out")
			.with_detail("Delivery or mutation outcome is unknown; do not automatically replay.")
	}
}

/// Converts the error of a `Result` into a [`VcmpError`] with a chosen status and title.
///
/// Rust has no blanket `From<E: Error>` for `VcmpError`, so `?` on a foreign error inside a
/// handler does not compile on its own. This gives it a status and title, keeping the error's
/// message as `detail` and the error itself as the source:
///
/// ```ignore
/// use vcmp::ResultExt;
///
/// async fn open_lock(open: OpenLock, _session: Session) -> Result<(), VcmpError> {
///     port.write_all(&command).await.or_problem(503, "Lock unreachable")?;
///     Ok(())
/// }
/// ```
///
/// The resulting error is an application error, regardless of the chosen status or source.
/// For a plain `500` with the error's type name as title, use `.map_err(VcmpError::from_error)?`.
pub trait ResultExt<T> {
	/// Maps the error to a problem detail with the given status and title.
	fn or_problem(self, status: u16, title: impl Into<String>) -> Result<T, VcmpError>;
}

impl<T, E: StdError + Send + Sync + 'static> ResultExt<T> for Result<T, E> {
	fn or_problem(self, status: u16, title: impl Into<String>) -> Result<T, VcmpError> {
		self.map_err(|error| VcmpError::new(status, title).with_detail(error.to_string()).with_source(error))
	}
}

fn short_type_name<T: ?Sized>() -> &'static str {
	let name = std::any::type_name::<T>();
	// Strip generic arguments first, then take the last path segment.
	let base = name.split('<').next().unwrap_or(name);
	base.rsplit("::").next().unwrap_or(base)
}

impl fmt::Display for VcmpError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.problem.fmt(f)
	}
}

impl StdError for VcmpError {
	fn source(&self) -> Option<&(dyn StdError + 'static)> {
		self.source.as_deref().map(|source| source as &(dyn StdError + 'static))
	}
}

impl Clone for VcmpError {
	/// Clones the problem detail and origin classification; the (non-clonable) source is dropped.
	fn clone(&self) -> Self {
		VcmpError { problem: self.problem.clone(), source: None, kind: self.kind }
	}
}

impl PartialEq for VcmpError {
	/// Compares the problem details only, ignoring source and origin classification.
	fn eq(&self, other: &Self) -> bool {
		self.problem == other.problem
	}
}

impl Serialize for VcmpError {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		self.problem.serialize(serializer)
	}
}

impl<'de> Deserialize<'de> for VcmpError {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		ProblemDetail::deserialize(deserializer).map(Into::into)
	}
}

impl From<ProblemDetail> for VcmpError {
	/// Wraps a problem detail without inferring local transport origin from its contents.
	fn from(problem: ProblemDetail) -> Self {
		VcmpError { problem: Box::new(problem), source: None, kind: ErrorKind::Other }
	}
}

impl From<VcmpError> for ProblemDetail {
	/// Extracts the problem detail, discarding the source and origin classification.
	fn from(error: VcmpError) -> Self {
		*error.problem
	}
}

impl From<String> for VcmpError {
	/// A plain message becomes `500 Error` with the message as `detail` (like `vcmp-js`).
	fn from(detail: String) -> Self {
		VcmpError::internal("Error").with_detail(detail)
	}
}

impl From<&str> for VcmpError {
	fn from(detail: &str) -> Self {
		detail.to_owned().into()
	}
}

impl From<Box<dyn StdError + Send + Sync + 'static>> for VcmpError {
	/// A boxed error becomes `500 Error` with its message as `detail`; a boxed `VcmpError` is
	/// unwrapped.
	fn from(error: Box<dyn StdError + Send + Sync + 'static>) -> Self {
		match error.downcast::<VcmpError>() {
			Ok(vcmp) => *vcmp,
			Err(other) => {
				let detail = other.to_string();
				VcmpError {
					problem: Box::new(ProblemDetail::new(500, "Error").with_detail(detail)),
					source: Some(other),
					kind: ErrorKind::Other,
				}
			}
		}
	}
}

impl From<serde_json::Error> for VcmpError {
	fn from(error: serde_json::Error) -> Self {
		VcmpError::from_error(error)
	}
}

impl From<std::io::Error> for VcmpError {
	fn from(error: std::io::Error) -> Self {
		VcmpError::from_error(error)
	}
}

impl From<std::convert::Infallible> for VcmpError {
	fn from(never: std::convert::Infallible) -> Self {
		match never {}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn local_transport_classification_does_not_depend_on_status() {
		let errors = [
			VcmpError::session_not_open("closed before sending"),
			VcmpError::session_closed("closed before acknowledgement"),
			VcmpError::not_connected("no session"),
			VcmpError::transport_overloaded(),
			VcmpError::acknowledgement_timeout(),
		];
		for error in errors {
			assert!(error.is_transport(), "{error}");
			assert!(!VcmpError::new(error.status(), error.title()).is_transport());
			assert!(!error.into_peer_error().is_transport());
		}
	}

	#[test]
	fn cloning_and_preserving_conversions_keep_local_origin() {
		let error = VcmpError::session_closed("closed")
			.with_type("about:blank")
			.with_instance("/request/1")
			.with_extra("request", "1")
			.with_source(std::io::Error::other("connection lost"));
		let cloned = error.clone();
		assert!(cloned.is_transport());
		assert!(cloned.source().is_none());
		assert_eq!(cloned.problem(), error.problem());
		let preserved = VcmpError::from_error(error);
		assert!(preserved.is_transport());
		assert!(preserved.source().is_some());
		let boxed: Box<dyn StdError + Send + Sync> = Box::new(preserved);
		let unboxed = VcmpError::from(boxed);
		assert!(unboxed.is_transport());
		assert!(unboxed.source().is_some());
	}

	#[test]
	fn problem_detail_and_serialization_round_trips_discard_origin() {
		let error = VcmpError::acknowledgement_timeout().with_extra("request", "1");
		let wire = serde_json::to_value(&error).unwrap();
		assert_eq!(wire, serde_json::to_value(error.problem()).unwrap());
		let decoded: VcmpError = serde_json::from_value(wire).unwrap();
		assert!(!decoded.is_transport());
		assert_eq!(decoded, error); // Equality continues to compare only the problem detail.
		assert!(!VcmpError::from(error.clone().into_problem()).is_transport());
		let problem: ProblemDetail = error.into();
		assert!(!VcmpError::from(problem).is_transport());
	}

	#[test]
	fn forged_properties_cannot_establish_local_origin() {
		for status in [408, 503, 504] {
			let problem = ProblemDetail::new(status, "Session closed")
				.with_extra("vcmp-local-connection", true)
				.with_extra("kind", "LocalTransport");
			let error = VcmpError::from(problem.clone());
			assert!(!error.is_transport());
			let decoded: VcmpError = serde_json::from_value(serde_json::to_value(&problem).unwrap()).unwrap();
			assert!(!decoded.is_transport());
			assert_eq!(decoded.problem(), &problem);
		}
	}

	#[test]
	fn application_errors_do_not_inherit_transport_origin_from_their_source() {
		let error = VcmpError::new(503, "Application failed").with_source(VcmpError::session_closed("closed"));
		assert!(!error.is_transport());
		let remapped = Err::<(), _>(VcmpError::session_closed("closed")).or_problem(503, "Application failed");
		assert!(!remapped.unwrap_err().is_transport());
		assert!(!VcmpError::from(std::io::Error::other("disk failed")).is_transport());
		let boxed: Box<dyn StdError + Send + Sync> = Box::new(std::io::Error::other("disk failed"));
		assert!(!VcmpError::from(boxed).is_transport());
	}

	#[test]
	fn or_problem_maps_foreign_errors() {
		let result: Result<i32, _> = "x".parse::<i32>();
		let error = result.or_problem(503, "Lock unreachable").unwrap_err();
		assert_eq!((error.status(), error.title()), (503, "Lock unreachable"));
		assert_eq!(error.detail(), Some("invalid digit found in string"));
		assert!(StdError::source(&error).is_some());
		assert_eq!(Ok::<i32, std::num::ParseIntError>(1).or_problem(500, "unused").unwrap(), 1);
	}

	#[test]
	fn serializes_like_the_reference_implementations() {
		let error = VcmpError::bad_request("Invalid message").with_detail("The message does not specify a type.");
		assert_eq!(
			serde_json::to_value(&error).unwrap(),
			serde_json::json!({
				"title": "Invalid message",
				"status": 400,
				"detail": "The message does not specify a type."
			})
		);
	}

	#[test]
	fn deserializes_spring_problem_details() {
		let json = r#"{"type":"about:blank","title":"Bad Request","status":400,"detail":"This is bad","instance":"/x","foo":1}"#;
		let problem: ProblemDetail = serde_json::from_str(json).unwrap();
		assert_eq!(problem.status, 400);
		assert_eq!(problem.title, "Bad Request");
		assert_eq!(problem.detail.as_deref(), Some("This is bad"));
		assert_eq!(problem.type_.as_deref(), Some("about:blank"));
		assert_eq!(problem.instance.as_deref(), Some("/x"));
		assert_eq!(problem.extra["foo"], 1);
	}

	#[test]
	fn tolerates_missing_or_null_members() {
		let problem: ProblemDetail = serde_json::from_str(r#"{"title":null,"status":null}"#).unwrap();
		assert_eq!(problem.title, "");
		assert_eq!(problem.status, 500);
		let problem: ProblemDetail = serde_json::from_str("{}").unwrap();
		assert_eq!(problem.status, 500);
	}

	#[test]
	fn converts_std_errors_to_500_with_type_name_title() {
		let io = std::io::Error::other("disk on fire");
		let error = VcmpError::from_error(io);
		assert_eq!(error.status(), 500);
		assert_eq!(error.title(), "Error");
		assert_eq!(error.detail(), Some("disk on fire"));
		assert!(error.source().is_some());

		#[derive(Debug, thiserror::Error)]
		#[error("custom failure")]
		struct MyDriverError;
		let error = VcmpError::from_error(MyDriverError);
		assert_eq!(error.title(), "MyDriverError");
		assert_eq!(error.detail(), Some("custom failure"));
	}

	#[test]
	fn keeps_vcmp_errors_unchanged() {
		let original = VcmpError::new(403, "Forbidden").with_detail("nope");
		assert_eq!(VcmpError::from_error(original.clone()), original);
		let boxed: Box<dyn StdError + Send + Sync> = Box::new(original.clone());
		assert_eq!(VcmpError::from(boxed), original);
	}

	#[test]
	fn converts_strings() {
		let error: VcmpError = "boom".into();
		assert_eq!(error.status(), 500);
		assert_eq!(error.title(), "Error");
		assert_eq!(error.detail(), Some("boom"));
	}

	#[test]
	fn displays_title_status_and_detail() {
		assert_eq!(VcmpError::new(503, "Session closed").with_detail("bye").to_string(), "Session closed (503): bye");
		assert_eq!(VcmpError::new(503, "Session closed").to_string(), "Session closed (503)");
	}
}
