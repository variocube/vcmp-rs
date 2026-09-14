//! Verify the allocation lifetime as well as the reported queue accounting.
//! This separate test binary keeps its tracking allocator isolated from other tests.
use futures_channel::mpsc;
use futures_util::{SinkExt, StreamExt};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::timeout;
use vcmp::{Frame, ResourceBudget, Session, SessionOptions};

static TRACKED_PAYLOAD: AtomicUsize = AtomicUsize::new(0);
static PAYLOAD_FREED: AtomicBool = AtomicBool::new(false);

struct TrackingAllocator;

// Allocation behavior is unchanged: the allocator only records when the tracked payload is freed.
unsafe impl GlobalAlloc for TrackingAllocator {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		unsafe { System.alloc(layout) }
	}

	unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
		if pointer as usize == TRACKED_PAYLOAD.load(Ordering::SeqCst) {
			PAYLOAD_FREED.store(true, Ordering::SeqCst);
		}
		unsafe { System.dealloc(pointer, layout) }
	}
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

#[tokio::test]
async fn written_request_releases_its_payload_before_acknowledgement() {
	let budget = ResourceBudget::default();
	let (peer, stream) = mpsc::unbounded();
	let (sink, mut outgoing) = mpsc::unbounded::<String>();
	let session = Session::spawn(
		SessionOptions { budget: budget.clone(), ..Default::default() },
		stream,
		sink.sink_map_err(|error| error.to_string()),
	);
	let payload = "x".repeat(1 << 20);
	TRACKED_PAYLOAD.store(payload.as_ptr() as usize, Ordering::SeqCst);
	let waiter = tokio::spawn({
		let session = session.clone();
		async move { session.send_payload(payload).await }
	});
	let wire = timeout(Duration::from_secs(1), outgoing.next()).await.unwrap().unwrap();
	let id = wire[3..15].to_owned();
	drop(wire);
	timeout(Duration::from_secs(1), async {
		while session.resources().queued_messages != 0 {
			tokio::task::yield_now().await;
		}
	})
	.await
	.unwrap();
	assert_eq!(session.resources().queued_bytes, 0);
	assert_eq!(budget.snapshot().queued_bytes, 0);
	assert_eq!(session.resources().pending_requests, 1);
	let released_before_ack = PAYLOAD_FREED.load(Ordering::SeqCst);

	peer.unbounded_send(Frame::ack(id, None).serialize()).unwrap();
	assert_eq!(timeout(Duration::from_secs(1), waiter).await.unwrap().unwrap().unwrap(), None);
	assert_eq!(session.resources().pending_requests, 0);
	session.close();
	session.closed().await;
	TRACKED_PAYLOAD.store(0, Ordering::SeqCst);
	assert!(released_before_ack, "the original payload must be freed when the queue's byte permits are released");
}
