//! Error types: the RFC 7807-style [`ProblemDetail`] carried by `NAK` frames and the
//! [`VcmpError`] every fallible operation of this crate reports.

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use std::error::Error as StdError;
use std::fmt;

/// The problem detail carried by a `NAK` frame (RFC 7807 shape, as in `vcmp-js` and Spring's
/// `ProblemDetail`).
///
/// Status codes in use across the implementations: `400` invalid message, `500` handler failure,
/// `503` local transport condition (session not open / closed, not connected).
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
/// A `VcmpError` *is* a [`ProblemDetail`] (the one sent in / received from a `NAK`), plus an
/// optional source error for local failures. It serializes exactly like the problem detail.
#[derive(Debug)]
pub struct VcmpError {
	// Boxed to keep `Result<_, VcmpError>` small (the problem detail carries a map).
	problem: Box<ProblemDetail>,
	source: Option<Box<dyn StdError + Send + Sync + 'static>>,
}

impl VcmpError {
	/// Creates an error with the given status and title.
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

	/// Consumes the error, returning its problem detail.
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

	/// Whether this is a local transport condition (`503`: session not open / closed, not
	/// connected) rather than a peer-side rejection. These are the retryable errors.
	pub fn is_transport(&self) -> bool {
		self.problem.status == 503
	}

	pub(crate) fn session_not_open(detail: &str) -> Self {
		VcmpError::new(503, "Session not open").with_detail(detail)
	}

	pub(crate) fn session_closed(detail: &str) -> Self {
		VcmpError::new(503, "Session closed").with_detail(detail)
	}

	pub(crate) fn not_connected(detail: &str) -> Self {
		VcmpError::new(503, "Not connected").with_detail(detail)
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
	/// Clones the problem detail; the (non-clonable) source is dropped.
	fn clone(&self) -> Self {
		VcmpError { problem: self.problem.clone(), source: None }
	}
}

impl PartialEq for VcmpError {
	/// Compares the problem details only.
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
	fn from(problem: ProblemDetail) -> Self {
		VcmpError { problem: Box::new(problem), source: None }
	}
}

impl From<VcmpError> for ProblemDetail {
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
