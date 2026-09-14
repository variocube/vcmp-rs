//! Shared admission limits and current transport resource accounting.
use crate::VcmpError;
use std::sync::{Arc, Mutex};

/// Maximum simultaneously retained transport resources. Clone one [`ResourceBudget`] into all
/// clients and servers to enforce one process-wide budget.
#[derive(Debug, Clone)]
pub struct ResourceLimits {
	/// Live sessions and connection attempts.
	pub connections: usize,
	/// Queued or currently writing application frames.
	pub queued_messages: usize,
	/// Wire bytes of queued or currently writing application frames.
	pub queued_bytes: usize,
	/// Reserved ACK/NAK/heartbeat queue entries, separate from application frames.
	pub control_messages: usize,
	/// Reserved ACK/NAK/heartbeat wire bytes.
	pub control_bytes: usize,
	/// Requests awaiting an acknowledgement.
	pub pending_requests: usize,
	/// Concurrent inbound message handlers.
	pub handler_tasks: usize,
	/// Wire payload bytes retained by inbound handlers.
	pub handler_bytes: usize,
}

impl Default for ResourceLimits {
	fn default() -> Self {
		Self {
			connections: 128,
			queued_messages: 1024,
			queued_bytes: 16 << 20,
			control_messages: 1024,
			control_bytes: 8 << 20,
			pending_requests: 1024,
			handler_tasks: 256,
			handler_bytes: 16 << 20,
		}
	}
}

/// Current usage. Byte counts measure retained wire payloads, not allocator or application memory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceSnapshot {
	/// Live sessions or admitted connection attempts.
	pub connections: usize,
	/// Application frames queued or currently writing.
	pub queued_messages: usize,
	/// Application wire bytes queued or currently writing.
	pub queued_bytes: usize,
	/// Control frames queued or currently writing.
	pub control_messages: usize,
	/// Control wire bytes queued or currently writing.
	pub control_bytes: usize,
	/// Requests awaiting an acknowledgement.
	pub pending_requests: usize,
	/// Active inbound handlers.
	pub handler_tasks: usize,
	/// Inbound wire payload bytes owned by active handlers.
	pub handler_bytes: usize,
	/// Admission rejections, monotonically increasing.
	pub overloads: u64,
}

#[derive(Debug)]
struct Inner {
	limits: ResourceLimits,
	usage: Mutex<ResourceSnapshot>,
}

/// A cloneable admission budget. Defaults are shared by all sessions within one client/server;
/// explicitly reuse a budget to share limits across multiple clients and servers.
#[derive(Debug, Clone)]
pub struct ResourceBudget(Arc<Inner>);

impl Default for ResourceBudget {
	fn default() -> Self {
		Self::new(ResourceLimits::default())
	}
}

impl ResourceBudget {
	/// Creates an independent budget. Zero denies all admissions for that resource.
	pub fn new(limits: ResourceLimits) -> Self {
		Self(Arc::new(Inner { limits, usage: Mutex::new(ResourceSnapshot::default()) }))
	}

	/// Returns a consistent snapshot of this budget's current usage.
	pub fn snapshot(&self) -> ResourceSnapshot {
		*self.0.usage.lock().unwrap_or_else(|e| e.into_inner())
	}

	pub(crate) fn reserve(&self, resource: Resource, bytes: usize) -> Result<Permit, VcmpError> {
		let mut usage = self.0.usage.lock().unwrap_or_else(|e| e.into_inner());
		let limits = &self.0.limits;
		let admitted = match resource {
			Resource::Connection => usage.connections < limits.connections,
			Resource::Request => usage.pending_requests < limits.pending_requests,
			Resource::Data => {
				usage.queued_messages < limits.queued_messages
					&& bytes <= limits.queued_bytes.saturating_sub(usage.queued_bytes)
			}
			Resource::Control => {
				usage.control_messages < limits.control_messages
					&& bytes <= limits.control_bytes.saturating_sub(usage.control_bytes)
			}
			Resource::Handler => {
				usage.handler_tasks < limits.handler_tasks
					&& bytes <= limits.handler_bytes.saturating_sub(usage.handler_bytes)
			}
		};
		if !admitted {
			usage.overloads = usage.overloads.saturating_add(1);
			return Err(VcmpError::new(503, "Transport overloaded"));
		}
		match resource {
			Resource::Connection => usage.connections += 1,
			Resource::Request => usage.pending_requests += 1,
			Resource::Data => {
				usage.queued_messages += 1;
				usage.queued_bytes += bytes;
			}
			Resource::Control => {
				usage.control_messages += 1;
				usage.control_bytes += bytes;
			}
			Resource::Handler => {
				usage.handler_tasks += 1;
				usage.handler_bytes += bytes;
			}
		}
		Ok(Permit { budget: self.clone(), resource, bytes })
	}
}

#[derive(Clone, Copy)]
pub(crate) enum Resource {
	Connection,
	Request,
	Data,
	Control,
	Handler,
}

pub(crate) struct Permit {
	budget: ResourceBudget,
	resource: Resource,
	bytes: usize,
}

impl Drop for Permit {
	fn drop(&mut self) {
		let mut usage = self.budget.0.usage.lock().unwrap_or_else(|e| e.into_inner());
		match self.resource {
			Resource::Connection => usage.connections -= 1,
			Resource::Request => usage.pending_requests -= 1,
			Resource::Data => {
				usage.queued_messages -= 1;
				usage.queued_bytes -= self.bytes;
			}
			Resource::Control => {
				usage.control_messages -= 1;
				usage.control_bytes -= self.bytes;
			}
			Resource::Handler => {
				usage.handler_tasks -= 1;
				usage.handler_bytes -= self.bytes;
			}
		}
	}
}
