//! Loopback tests of session authentication (`AUT`) over an in-memory duplex, in paused time.
//!
//! Each test pins one rule of variocube/vcmp-spring#34 at the session level: the peer is a raw
//! frame-level counterpart, so every frame the session sends and the close reason are observable.

mod auth_common;

use auth_common::*;
use serde_json::{Value, json};
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::advance;
use vcmp::{Authentication, AuthenticationMethod, Authenticator, ConnectInfo, Frame, Session};

// Contract case 1: valid AUT → ACK, then messages flow both ways.
#[tokio::test(start_paused = true)]
async fn valid_aut_is_acknowledged_and_then_messages_flow() {
	let mut f = spawn(Some(authenticator()), None);
	assert!(!f.session.is_authenticated());
	assert_eq!(authenticate(&mut f, json!({"token": "alice"})).await, json!({"user": "alice"}));
	assert!(f.session.is_authenticated());
	assert_eq!(f.session.principal::<String>().as_deref().map(String::as_str), Some("alice"));
	assert_eq!(f.session.principal::<u32>(), None);
	assert_eq!(f.session.authentication_method(), Some(AuthenticationMethod::Aut));

	f.peer.inject(&format!("MSG{ID2}{PING}"));
	assert_eq!(f.peer.next_frame().await, Frame::ack(ID2, None));
	assert_eq!(f.pings.load(Ordering::SeqCst), 1);

	let session = f.session.clone();
	let send = tokio::spawn(async move { session.send(&Ping {}).await });
	let Frame::Message { id, .. } = f.peer.next_frame().await else { panic!("expected MSG") };
	f.peer.inject(&format!("ACK{id}"));
	send.await.unwrap().unwrap();
}

// Contract case 2: invalid AUT → NAK 401, then close 1008; the NAK is written before the close.
#[tokio::test(start_paused = true)]
async fn rejected_first_aut_is_naked_then_closed() {
	let mut f = spawn(Some(authenticator()), None);
	f.peer.inject(&aut(ID, json!({"token": "bad"})));
	let frames = f.peer.closed().await;
	assert_eq!(frames.len(), 1, "{frames:?}");
	assert_eq!(nak_status(&frames[0], ID), 401);
	assert_eq!(f.session.close_reason(), policy("authentication failed"));
	assert!(!f.session.is_authenticated());
}

#[tokio::test(start_paused = true)]
async fn authenticator_status_is_passed_through_on_the_first_aut() {
	let mut f = spawn(Some(authenticator()), None);
	f.peer.inject(&aut(ID, json!({"token": "forbidden"})));
	let frames = f.peer.closed().await;
	assert_eq!(nak_status(&frames[0], ID), 403);
	assert_eq!(f.session.close_reason(), policy("authentication failed"));
}

// Contract case 3: MSG (and ACK/NAK) before AUT → close 1008, never dispatched.
#[tokio::test(start_paused = true)]
async fn frames_other_than_hbt_and_aut_before_authentication_close_the_session() {
	for raw in [format!("MSG{ID}{PING}"), format!("ACK{ID}"), format!("NAK{ID}")] {
		let mut f = spawn(Some(authenticator()), None);
		f.peer.inject(&raw);
		assert_eq!(f.peer.closed().await, vec![], "{raw}");
		assert_eq!(f.session.close_reason(), policy("authentication required"), "{raw}");
		assert_eq!(f.pings.load(Ordering::SeqCst), 0);
	}
}

#[tokio::test(start_paused = true)]
async fn msg_while_the_first_aut_is_pending_closes_the_session() {
	let mut f = spawn(Some(authenticator()), None);
	f.peer.inject(&aut(ID, json!({"token": "slow"})));
	settle().await;
	f.peer.inject(&format!("MSG{ID2}{PING}"));
	assert_eq!(f.peer.closed().await, vec![]);
	assert_eq!(f.session.close_reason(), policy("authentication required"));
	assert_eq!(f.pings.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn heartbeats_are_accepted_before_authentication() {
	let mut f = spawn(Some(authenticator()), None);
	f.peer.inject("HBT1000");
	advance(Duration::from_millis(1000)).await;
	settle().await;
	assert_eq!(f.peer.next_frame().await, Frame::heartbeat(1000));
	assert!(f.session.is_open());
	assert_eq!(f.session.heartbeats_received(), 1);
}

// Contract case 4: no AUT within the timeout → close 1008, counted from connect.
#[tokio::test(start_paused = true)]
async fn no_authentication_within_the_timeout_closes_the_session() {
	let mut f = spawn(Some(authenticator().timeout(Duration::from_secs(3))), None);
	advance(Duration::from_millis(2_999)).await;
	settle().await;
	assert!(f.session.is_open());
	advance(Duration::from_millis(1)).await;
	settle().await;
	assert!(!f.session.is_open());
	assert_eq!(f.peer.closed().await, vec![]);
	assert_eq!(f.session.close_reason(), policy("authentication timeout"));
}

#[tokio::test(start_paused = true)]
async fn the_default_timeout_is_ten_seconds_and_a_pending_aut_does_not_extend_it() {
	let mut f = spawn(Some(authenticator()), None);
	f.peer.inject(&aut(ID, json!({"token": "slow"})));
	advance(Duration::from_millis(9_999)).await;
	settle().await;
	assert!(f.session.is_open());
	advance(Duration::from_millis(1)).await;
	settle().await;
	assert!(!f.session.is_open());
	assert_eq!(f.peer.closed().await, vec![]);
	assert_eq!(f.session.close_reason(), policy("authentication timeout"));
}

#[tokio::test(start_paused = true)]
async fn authentication_stops_the_timeout() {
	let mut f = spawn(Some(authenticator()), None);
	authenticate(&mut f, json!({"token": "alice"})).await;
	advance(Duration::from_secs(60)).await;
	settle().await;
	assert!(f.session.is_open());
}

// Contract case 5: AUT on an endpoint without auth → NAK 400, the session stays.
#[tokio::test(start_paused = true)]
async fn aut_without_an_authenticator_is_naked_with_400_and_the_session_stays() {
	let mut f = spawn(None, None);
	assert!(f.session.is_authenticated());
	f.peer.inject(&aut(ID, json!({"token": "alice"})));
	assert_eq!(nak_status(&f.peer.next_frame().await, ID), 400);
	f.peer.inject(&format!("MSG{ID2}{PING}"));
	assert_eq!(f.peer.next_frame().await, Frame::ack(ID2, None));
	assert!(f.session.is_open());
	assert_eq!(f.session.close_reason(), None);
}

#[tokio::test(start_paused = true)]
async fn malformed_credentials_are_naked_with_400() {
	for payload in ["", "[]", "\"token\"", "{oops", "null"] {
		let mut f = spawn(Some(authenticator()), None);
		f.peer.inject(&format!("AUT{ID}{payload}"));
		let frames = f.peer.closed().await;
		assert_eq!(nak_status(&frames[0], ID), 400, "{payload:?}");
		assert_eq!(f.session.close_reason(), policy("authentication failed"));
	}
	// On an authenticated session, a malformed refresh does not close it.
	let mut f = spawn(Some(authenticator()), None);
	authenticate(&mut f, json!({"token": "alice"})).await;
	f.peer.inject(&format!("AUT{ID2}[]"));
	assert_eq!(nak_status(&f.peer.next_frame().await, ID2), 400);
	settle().await;
	assert!(f.session.is_open());
}

// Contract case 6: expiry → close 1008 when expiresIn runs out.
#[tokio::test(start_paused = true)]
async fn the_session_closes_when_its_authentication_expires() {
	let mut f = spawn(Some(authenticator()), None);
	let ack = authenticate(&mut f, json!({"token": "alice", "ttl": 5_000})).await;
	assert_eq!(ack, json!({"user": "alice", "expiresIn": 5_000}));
	advance(Duration::from_millis(4_999)).await;
	settle().await;
	assert!(f.session.is_open());
	advance(Duration::from_millis(1)).await;
	settle().await;
	assert!(!f.session.is_open());
	assert_eq!(f.peer.closed().await, vec![]);
	assert_eq!(f.session.close_reason(), policy("authentication expired"));
}

// Contract case 7: refresh with the same principal → ACK, expiry moved.
#[tokio::test(start_paused = true)]
async fn a_refresh_with_the_same_principal_moves_the_expiry() {
	let mut f = spawn(Some(authenticator()), None);
	authenticate(&mut f, json!({"token": "alice", "ttl": 5_000})).await;
	advance(Duration::from_millis(4_000)).await;
	f.peer.inject(&aut(ID2, json!({"token": "alice:2", "ttl": 5_000})));
	assert_eq!(ack_payload(&f.peer.next_frame().await, ID2), json!({"user": "alice", "expiresIn": 5_000}));
	advance(Duration::from_millis(4_999)).await;
	settle().await;
	assert!(f.session.is_open(), "the old expiry must not close the session");
	advance(Duration::from_millis(1)).await;
	settle().await;
	assert!(!f.session.is_open());
	assert_eq!(f.session.close_reason(), policy("authentication expired"));
}

#[tokio::test(start_paused = true)]
async fn a_refresh_without_expiry_removes_it() {
	let mut f = spawn(Some(authenticator()), None);
	authenticate(&mut f, json!({"token": "alice", "ttl": 5_000})).await;
	f.peer.inject(&aut(ID2, json!({"token": "alice:2"})));
	assert_eq!(ack_payload(&f.peer.next_frame().await, ID2), json!({"user": "alice"}));
	advance(Duration::from_secs(3_600)).await;
	settle().await;
	assert!(f.session.is_open());
}

// Contract case 8: refresh with another principal → NAK 403 + close.
#[tokio::test(start_paused = true)]
async fn a_refresh_with_another_principal_closes_the_session() {
	let mut f = spawn(Some(authenticator()), None);
	authenticate(&mut f, json!({"token": "alice"})).await;
	f.peer.inject(&aut(ID2, json!({"token": "bob"})));
	let frames = f.peer.closed().await;
	assert_eq!(frames.len(), 1, "{frames:?}");
	assert_eq!(nak_status(&frames[0], ID2), 403);
	assert_eq!(f.session.close_reason(), policy("principal changed"));
	assert_eq!(f.session.principal::<String>().as_deref().map(String::as_str), Some("alice"));
}

// Contract case 9: failed refresh → NAK 401, open until the old expiry.
#[tokio::test(start_paused = true)]
async fn a_failed_refresh_keeps_the_session_until_its_expiry() {
	let mut f = spawn(Some(authenticator()), None);
	authenticate(&mut f, json!({"token": "alice", "ttl": 5_000})).await;
	advance(Duration::from_millis(1_000)).await;
	f.peer.inject(&aut(ID2, json!({"token": "bad"})));
	assert_eq!(nak_status(&f.peer.next_frame().await, ID2), 401);
	f.peer.inject(&format!("MSG{ID}{PING}"));
	assert_eq!(f.peer.next_frame().await, Frame::ack(ID, None));
	advance(Duration::from_millis(3_999)).await;
	settle().await;
	assert!(f.session.is_open());
	advance(Duration::from_millis(1)).await;
	settle().await;
	assert!(!f.session.is_open());
	assert_eq!(f.session.close_reason(), policy("authentication expired"));
}

#[tokio::test(start_paused = true)]
async fn messages_keep_flowing_during_a_refresh() {
	let mut f = spawn(Some(authenticator()), None);
	authenticate(&mut f, json!({"token": "alice"})).await;
	f.peer.inject(&aut(ID2, json!({"token": "slow"})));
	settle().await;
	f.peer.inject(&format!("MSG{ID}{PING}"));
	assert_eq!(f.peer.next_frame().await, Frame::ack(ID, None));
	assert!(f.session.is_open());
}

#[tokio::test(start_paused = true)]
async fn a_second_aut_while_one_is_pending_closes_the_session() {
	// Before authentication …
	let mut f = spawn(Some(authenticator()), None);
	f.peer.inject(&aut(ID, json!({"token": "slow"})));
	settle().await;
	f.peer.inject(&aut(ID2, json!({"token": "alice"})));
	assert_eq!(f.peer.closed().await, vec![]);
	assert_eq!(f.session.close_reason(), policy("concurrent authentication"));
	// … and during a refresh.
	let mut f = spawn(Some(authenticator()), None);
	authenticate(&mut f, json!({"token": "alice"})).await;
	f.peer.inject(&aut(ID2, json!({"token": "slow"})));
	settle().await;
	f.peer.inject(&aut(ID, json!({"token": "alice"})));
	assert_eq!(f.peer.closed().await, vec![]);
	assert_eq!(f.session.close_reason(), policy("concurrent authentication"));
}

#[tokio::test(start_paused = true)]
async fn nothing_is_sent_to_a_session_before_it_is_authenticated() {
	let mut f = spawn(Some(authenticator()), None);
	let error = f.session.send(&Ping {}).await.unwrap_err();
	assert_eq!(error.status(), 503);
	assert!(error.is_transport());
	assert!(!f.session.send_raw(format!("MSG{ID}{PING}")));
	settle().await;
	assert_eq!(f.peer.sent(), vec![]);
	authenticate(&mut f, json!({"token": "alice"})).await;
	assert!(f.session.send_raw(format!("MSG{ID}{PING}")));
}

#[tokio::test(start_paused = true)]
async fn the_protocol_owns_the_expires_in_ack_member() {
	let authenticator = Authenticator::new(|_session: Session, _credential: Value| async move {
		Ok(Authentication::new(1u32).ack_member("expiresIn", "forged"))
	});
	let mut f = spawn(Some(authenticator), None);
	assert_eq!(authenticate(&mut f, json!({})).await, Value::Null, "an ACK without members has no payload");
}

#[tokio::test(start_paused = true)]
async fn authenticated_resolves_on_authentication_or_close() {
	let mut f = spawn(Some(authenticator()), None);
	let session = f.session.clone();
	let waiter = tokio::spawn(async move { session.authenticated().await });
	settle().await;
	assert!(!waiter.is_finished());
	authenticate(&mut f, json!({"token": "alice"})).await;
	assert!(waiter.await.unwrap());

	let f = spawn(Some(authenticator()), None);
	let session = f.session.clone();
	let waiter = tokio::spawn(async move { session.authenticated().await });
	f.session.close();
	assert!(!waiter.await.unwrap());
}

// Contract case 10: transition mode.
#[tokio::test(start_paused = true)]
async fn a_valid_handshake_credential_authenticates_without_aut() {
	let mut f = spawn(Some(with_handshake()), Some("token=alice"));
	// An old client sends at once; the frame waits for the handshake decision instead of closing.
	f.peer.inject(&format!("MSG{ID}{PING}"));
	assert_eq!(f.peer.next_frame().await, Frame::ack(ID, None));
	assert!(f.session.is_authenticated());
	assert_eq!(f.session.authentication_method(), Some(AuthenticationMethod::Handshake));
	assert_eq!(f.session.principal::<String>().as_deref().map(String::as_str), Some("alice"));
	advance(Duration::from_secs(60)).await;
	settle().await;
	assert!(f.session.is_open(), "the AUT timeout does not apply");
}

#[tokio::test(start_paused = true)]
async fn an_invalid_handshake_credential_closes_without_falling_back_to_aut() {
	let mut f = spawn(Some(with_handshake()), Some("token=bad"));
	f.peer.inject(&aut(ID, json!({"token": "alice"})));
	assert_eq!(f.peer.closed().await, vec![], "the AUT is never answered");
	assert_eq!(f.session.close_reason(), policy("authentication failed"));
	assert!(!f.session.is_authenticated());
}

#[tokio::test(start_paused = true)]
async fn without_a_handshake_credential_aut_is_required() {
	let mut f = spawn(Some(with_handshake()), None);
	f.peer.inject(&format!("MSG{ID}{PING}"));
	assert_eq!(f.peer.closed().await, vec![]);
	assert_eq!(f.session.close_reason(), policy("authentication required"));

	let mut f = spawn(Some(with_handshake()), Some("other=1"));
	authenticate(&mut f, json!({"token": "alice"})).await;
	assert_eq!(f.session.authentication_method(), Some(AuthenticationMethod::Aut));
}

#[tokio::test(start_paused = true)]
async fn aut_after_a_handshake_authentication_is_a_re_authentication() {
	let mut f = spawn(Some(with_handshake()), Some("token=alice"));
	assert!(f.session.authenticated().await);
	assert_eq!(authenticate(&mut f, json!({"token": "alice:2", "ttl": 1_000})).await["expiresIn"], 1_000);
	assert_eq!(f.session.authentication_method(), Some(AuthenticationMethod::Handshake));
	f.peer.inject(&aut(ID2, json!({"token": "bob"})));
	let frames = f.peer.closed().await;
	assert_eq!(nak_status(&frames[0], ID2), 403);
	assert_eq!(f.session.close_reason(), policy("principal changed"));
}

// Client side of a session: send_authentication.
#[tokio::test(start_paused = true)]
async fn send_authentication_sends_aut_and_resolves_with_the_ack() {
	let mut f = spawn(None, None);
	let session = f.session.clone();
	let send =
		tokio::spawn(async move { session.send_authentication(&json!({"token": "t"}), Duration::from_secs(10)).await });
	let Frame::Authenticate { id, payload } = f.peer.next_frame().await else { panic!("expected AUT") };
	assert_eq!(payload, r#"{"token":"t"}"#);
	f.peer.inject(&format!(r#"ACK{id}{{"expiresIn":5000,"user":"u"}}"#));
	assert_eq!(send.await.unwrap().unwrap(), json!({"expiresIn": 5000, "user": "u"}));
	assert_eq!(f.session.authentication(), Some(json!({"expiresIn": 5000, "user": "u"})));
}

#[tokio::test(start_paused = true)]
async fn send_authentication_fails_with_the_nak_or_after_the_timeout() {
	let mut f = spawn(None, None);
	let session = f.session.clone();
	let send = tokio::spawn(async move { session.send_authentication(&json!({}), Duration::from_secs(10)).await });
	let Frame::Authenticate { id, .. } = f.peer.next_frame().await else { panic!("expected AUT") };
	f.peer.inject(&format!(r#"NAK{id}{{"status":401,"title":"Unauthorized"}}"#));
	let error = send.await.unwrap().unwrap_err();
	assert_eq!((error.status(), error.is_transport()), (401, false));
	assert_eq!(f.session.authentication(), None);

	// A peer that never answers (an older server that does not know AUT).
	let session = f.session.clone();
	let send = tokio::spawn(async move { session.send_authentication(&json!({}), Duration::from_secs(10)).await });
	let Frame::Authenticate { .. } = f.peer.next_frame().await else { panic!("expected AUT") };
	advance(Duration::from_secs(10)).await;
	let error = send.await.unwrap().unwrap_err();
	assert_eq!((error.status(), error.is_transport()), (504, true));

	let error = f.session.send_authentication(&json!("token"), Duration::from_secs(10)).await.unwrap_err();
	assert_eq!(error.status(), 400);
	assert_eq!(f.peer.sent(), vec![], "a credential that is not an object is not sent");
}

// Fail closed: rejections and unexpected failures.
#[tokio::test(start_paused = true)]
async fn a_refresh_rejected_with_403_keeps_the_session_until_its_expiry() {
	let mut f = spawn(Some(authenticator()), None);
	authenticate(&mut f, json!({"token": "alice", "ttl": 5_000})).await;
	f.peer.inject(&aut(ID2, json!({"token": "forbidden"})));
	assert_eq!(nak_status(&f.peer.next_frame().await, ID2), 403);
	advance(Duration::from_millis(4_999)).await;
	settle().await;
	assert!(f.session.is_open(), "only a principal change closes on a refresh");
	advance(Duration::from_millis(1)).await;
	settle().await;
	assert_eq!(f.session.close_reason(), policy("authentication expired"));
}

#[tokio::test(start_paused = true)]
async fn a_panicking_authenticator_is_naked_with_500_and_fails_closed() {
	let mut f = spawn(Some(authenticator()), None);
	f.peer.inject(&aut(ID, json!({"token": "panic"})));
	let frames = f.peer.closed().await;
	assert_eq!(frames.len(), 1, "{frames:?}");
	assert_eq!(nak_status(&frames[0], ID), 500);
	assert_eq!(f.session.close_reason(), policy("authentication failed"));
	assert!(!f.session.is_authenticated());

	let mut f = spawn(Some(authenticator()), None);
	authenticate(&mut f, json!({"token": "alice", "ttl": 5_000})).await;
	f.peer.inject(&aut(ID2, json!({"token": "panic"})));
	assert_eq!(nak_status(&f.peer.next_frame().await, ID2), 500);
	f.peer.inject(&format!("MSG{ID}{PING}"));
	assert_eq!(f.peer.next_frame().await, Frame::ack(ID, None), "the previous authentication stays");
	advance(Duration::from_millis(5_000)).await;
	settle().await;
	assert_eq!(f.session.close_reason(), policy("authentication expired"), "until its old expiry");
}

#[tokio::test(start_paused = true)]
async fn a_hanging_authenticator_on_a_refresh_is_naked_with_500_at_the_handler_deadline() {
	let limits = vcmp::SessionLimits { handler_timeout: Duration::from_secs(2), ..Default::default() };
	let mut f = spawn_with(Some(authenticator()), None, limits);
	authenticate(&mut f, json!({"token": "alice"})).await;
	f.peer.inject(&aut(ID2, json!({"token": "slow"})));
	settle().await;
	advance(Duration::from_millis(1_999)).await;
	settle().await;
	assert_eq!(f.peer.sent(), vec![]);
	advance(Duration::from_millis(1)).await;
	assert_eq!(nak_status(&f.peer.next_frame().await, ID2), 500);
	settle().await;
	assert!(f.session.is_open());
	// The next AUT is not concurrent: the failed one is over.
	f.peer.inject(&aut(ID, json!({"token": "alice:2"})));
	assert_eq!(ack_payload(&f.peer.next_frame().await, ID), json!({"user": "alice"}));
}

#[tokio::test(start_paused = true)]
async fn a_panicking_principal_comparison_is_naked_with_500_and_keeps_the_previous_state() {
	#[derive(Debug)]
	struct Fragile(u32);
	impl PartialEq for Fragile {
		fn eq(&self, other: &Self) -> bool {
			assert!(self.0 != 13 && other.0 != 13, "comparison bug");
			self.0 == other.0
		}
	}
	let authenticator = Authenticator::new(|_session: Session, credential: Value| async move {
		Ok(Authentication::new(Fragile(credential["id"].as_u64().unwrap_or_default() as u32))
			.expires_in(Duration::from_secs(5)))
	});
	let mut f = spawn(Some(authenticator), None);
	authenticate(&mut f, json!({"id": 1})).await;
	f.peer.inject(&aut(ID2, json!({"id": 13})));
	assert_eq!(nak_status(&f.peer.next_frame().await, ID2), 500);
	settle().await;
	assert!(f.session.is_open());
	assert_eq!(f.session.principal::<Fragile>().map(|p| p.0), Some(1));
}

#[tokio::test(start_paused = true)]
async fn a_panicking_handshake_authenticator_closes_the_session() {
	let authenticator = Authenticator::with_handshake(
		|_session: Session, credential: Value| async move { decide(&credential) },
		|_info: ConnectInfo| async move {
			panic!("handshake bug");
			#[allow(unreachable_code)]
			Ok(None)
		},
	);
	let mut f = spawn(Some(authenticator), Some("token=alice"));
	assert_eq!(f.peer.closed().await, vec![]);
	assert_eq!(f.session.close_reason(), policy("authentication failed"));
}

// #34 "Expiry already over" and "Check order".
#[tokio::test(start_paused = true)]
async fn a_credential_whose_expiry_is_not_in_the_future_is_naked_with_401() {
	let authenticator = Authenticator::new(|_session: Session, credential: Value| async move {
		let principal = credential["user"].as_str().unwrap_or_default().to_owned();
		let authentication = Authentication::new(principal);
		Ok(match credential["expired"].as_bool() {
			Some(true) => authentication.expires_at(std::time::SystemTime::now() - Duration::from_secs(1)),
			_ => authentication.expires_in(Duration::from_millis(credential["ttl"].as_u64().unwrap_or(5_000))),
		})
	});
	// First AUT: rejected (also an expiresIn that rounds to 0 ms), the session is closed.
	for credential in [json!({"user": "alice", "expired": true}), json!({"user": "alice", "ttl": 0})] {
		let mut f = spawn(Some(authenticator.clone()), None);
		f.peer.inject(&aut(ID, credential.clone()));
		let frames = f.peer.closed().await;
		assert_eq!(nak_status(&frames[0], ID), 401, "{credential}");
		assert_eq!(f.session.close_reason(), policy("authentication failed"));
		assert!(!f.session.is_authenticated());
	}
	// Refresh: an expired credential, even for another principal, is a 401, not a principal change.
	let mut f = spawn(Some(authenticator), None);
	authenticate(&mut f, json!({"user": "alice", "ttl": 5_000})).await;
	f.peer.inject(&aut(ID2, json!({"user": "bob", "expired": true})));
	assert_eq!(nak_status(&f.peer.next_frame().await, ID2), 401);
	settle().await;
	assert!(f.session.is_open(), "the session stays until its old expiry");
	advance(Duration::from_millis(5_000)).await;
	settle().await;
	assert_eq!(f.session.close_reason(), policy("authentication expired"));
}

#[tokio::test(start_paused = true)]
async fn an_expired_handshake_credential_closes_the_session() {
	let authenticator = Authenticator::with_handshake(
		|_session: Session, credential: Value| async move { decide(&credential) },
		|_info: ConnectInfo| async move { Ok(Some(Authentication::new("alice".to_owned()).expires_in(Duration::ZERO))) },
	);
	let mut f = spawn(Some(authenticator), Some("token=alice"));
	assert_eq!(f.peer.closed().await, vec![]);
	assert_eq!(f.session.close_reason(), policy("authentication failed"));
}

#[tokio::test(start_paused = true)]
async fn an_unparseable_frame_before_authentication_closes_the_session() {
	for raw in ["garbage", "MSG123", "HBTx", ""] {
		let mut f = spawn(Some(authenticator()), None);
		f.peer.inject(raw);
		assert_eq!(f.peer.closed().await, vec![], "{raw:?}");
		assert_eq!(f.session.close_reason(), policy("authentication required"), "{raw:?}");
	}
	// Without an authenticator, and after authentication, it is ignored as before.
	let mut f = spawn(None, None);
	f.peer.inject("garbage");
	f.peer.inject(&format!("MSG{ID}{PING}"));
	assert_eq!(f.peer.next_frame().await, Frame::ack(ID, None));
	let mut f = spawn(Some(authenticator()), None);
	authenticate(&mut f, json!({"token": "alice"})).await;
	f.peer.inject("garbage");
	f.peer.inject(&format!("MSG{ID2}{PING}"));
	assert_eq!(f.peer.next_frame().await, Frame::ack(ID2, None));
	assert_eq!(f.session.ignored_frames(), 1);
}

// #34 "Synchronous failures": a closure that panics before it returns its future.
#[tokio::test(start_paused = true)]
async fn an_authenticator_panicking_synchronously_is_naked_with_500() {
	let mut f = spawn(Some(authenticator()), None);
	f.peer.inject(&aut(ID, json!({"token": "sync-panic"})));
	let frames = f.peer.closed().await;
	assert_eq!(frames.len(), 1, "{frames:?}");
	assert_eq!(nak_status(&frames[0], ID), 500);
	assert_eq!(f.session.close_reason(), policy("authentication failed"));

	let mut f = spawn(Some(authenticator()), None);
	authenticate(&mut f, json!({"token": "alice", "ttl": 5_000})).await;
	f.peer.inject(&aut(ID2, json!({"token": "sync-panic"})));
	assert_eq!(nak_status(&f.peer.next_frame().await, ID2), 500);
	// Not stuck in flight: the next refresh is answered, not taken for a concurrent AUT.
	f.peer.inject(&aut(ID, json!({"token": "alice:2"})));
	assert_eq!(ack_payload(&f.peer.next_frame().await, ID), json!({"user": "alice"}));
	assert!(f.session.is_open());
}

#[tokio::test(start_paused = true)]
async fn a_handshake_authenticator_panicking_synchronously_closes_the_session() {
	let authenticator = Authenticator::with_handshake(
		|_session: Session, credential: Value| async move { decide(&credential) },
		|_info: ConnectInfo| -> std::future::Ready<Result<Option<Authentication<String>>, vcmp::VcmpError>> {
			panic!("synchronous handshake bug")
		},
	);
	let mut f = spawn(Some(authenticator), Some("token=alice"));
	assert_eq!(f.peer.closed().await, vec![]);
	assert_eq!(f.session.close_reason(), policy("authentication failed"));
	f.session.closed().await;
}

#[tokio::test(start_paused = true)]
async fn an_expiry_too_far_to_represent_means_none() {
	let authenticator = Authenticator::new(|_session: Session, _credential: Value| async move {
		Ok(Authentication::new(1u32).expires_in(Duration::MAX))
	});
	let mut f = spawn(Some(authenticator), None);
	assert_eq!(authenticate(&mut f, json!({})).await, Value::Null, "no expiresIn");
	advance(Duration::from_secs(86_400)).await;
	settle().await;
	assert!(f.session.is_open());
}

// #34 NAK 503: the server can't authenticate right now (no handler capacity); fails closed like the others.
#[tokio::test(start_paused = true)]
async fn an_aut_without_handler_capacity_is_naked_with_503() {
	let mut limits = vcmp::SessionLimits::default();
	limits.resources.handler_tasks = 0;
	let mut f = spawn_with(Some(authenticator()), None, limits);
	f.peer.inject(&aut(ID, json!({"token": "alice"})));
	let frames = f.peer.closed().await;
	assert_eq!(nak_status(&frames[0], ID), 503);
	assert_eq!(f.session.close_reason(), policy("authentication failed"));
}

#[tokio::test(start_paused = true)]
async fn close_with_keeps_close_frames_valid() {
	let f = spawn(None, None);
	f.session.close_with(4000, "ä".repeat(100));
	let reason = f.session.close_reason().unwrap();
	assert_eq!((reason.code, reason.reason.len()), (4000, 122), "truncated at a character boundary");
	for reserved in [1004, 1005, 1006, 1015, 999, 2000, 5000] {
		let f = spawn(None, None);
		f.session.close_with(reserved, "reserved");
		assert_eq!(f.session.close_reason(), None, "{reserved}");
		assert!(!f.session.is_open());
	}
}
