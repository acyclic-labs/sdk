//! The socket lifecycle's synchronisation state machine, driven with plain loom threads.
//!
//! The cases below cover the properties the old publication-builder model claimed, without
//! building the actor runtime future:
//!
//! * exactly one Close owns a socket after its Open settles (peer, actor, backpressure, drain
//!   and recovery all claim the same lifecycle);
//! * a Close that arrives while the Open commit is in flight is held, then delivered once;
//! * a peer Close is terminal: no message is admitted after it;
//! * a message and a Close are linearised: once the Close owns, no message is admitted;
//! * admission of the Open happens once.

use super::*;
use loom::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

fn open_lifecycle() -> Arc<Mutex<SocketLifecycle>> {
    let lifecycle = Arc::new(Mutex::new(SocketLifecycle::new()));
    {
        let mut state = lifecycle.lock().unwrap();
        assert!(state.admit());
        assert!(state.commit_open().is_none());
        assert!(state.settle_open());
    }
    lifecycle
}

fn close(code: u16) -> Close {
    Close::new(code, "close", SocketCloseInitiator::Actor)
}

#[test]
fn exactly_one_close_owns_after_open() {
    loom::model(|| {
        let lifecycle = open_lifecycle();
        let owners = Arc::new(AtomicUsize::new(0));
        let threads = [1000u16, 1006].map(|code| {
            let lifecycle = Arc::clone(&lifecycle);
            let owners = Arc::clone(&owners);
            loom::thread::spawn(move || {
                if matches!(
                    lifecycle.lock().unwrap().claim_close(&close(code)),
                    CloseClaim::Owned
                ) {
                    owners.fetch_add(1, Ordering::SeqCst);
                }
            })
        });
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(owners.load(Ordering::SeqCst), 1);
        assert!(!lifecycle.lock().unwrap().message_allowed());
    });
}

#[test]
fn close_during_open_is_pending_and_delivered_once() {
    loom::model(|| {
        let lifecycle = Arc::new(Mutex::new(SocketLifecycle::new()));
        {
            let mut state = lifecycle.lock().unwrap();
            assert!(state.admit());
        }
        let held = close(1006);
        assert!(matches!(
            lifecycle.lock().unwrap().claim_close(&held),
            CloseClaim::Pending
        ));
        {
            let mut state = lifecycle.lock().unwrap();
            let pending = state.commit_open().expect("the held close is delivered");
            assert_eq!(pending.code, held.code);
            assert!(state.commit_open().is_none());
        }
        assert!(matches!(
            lifecycle.lock().unwrap().claim_close(&held),
            CloseClaim::Owned
        ));
        assert!(!lifecycle.lock().unwrap().message_allowed());
    });
}

#[test]
fn peer_close_is_terminal_for_messages() {
    loom::model(|| {
        let lifecycle = open_lifecycle();
        let closed = Arc::clone(&lifecycle);
        let closer = loom::thread::spawn(move || {
            closed.lock().unwrap().peer_close();
        });
        // A message may be admitted before the peer close; after it, admission is terminal.
        let _ = lifecycle.lock().unwrap().message_allowed();
        closer.join().unwrap();
        assert!(!lifecycle.lock().unwrap().message_allowed());
    });
}

#[test]
fn message_and_close_are_linearised() {
    loom::model(|| {
        let lifecycle = open_lifecycle();
        let owners = Arc::new(AtomicUsize::new(0));
        let closer = {
            let lifecycle = Arc::clone(&lifecycle);
            let owners = Arc::clone(&owners);
            loom::thread::spawn(move || {
                if matches!(
                    lifecycle.lock().unwrap().claim_close(&close(1000)),
                    CloseClaim::Owned
                ) {
                    owners.fetch_add(1, Ordering::SeqCst);
                }
            })
        };
        let admitted = lifecycle.lock().unwrap().message_allowed();
        closer.join().unwrap();
        assert_eq!(owners.load(Ordering::SeqCst), 1);
        if admitted {
            // The message was admitted before the close; the close still owns and terminal
            // admission is closed afterwards.
            assert!(!lifecycle.lock().unwrap().message_allowed());
        }
        assert!(!lifecycle.lock().unwrap().message_allowed());
    });
}

#[test]
fn admission_happens_once() {
    loom::model(|| {
        let lifecycle = Arc::new(Mutex::new(SocketLifecycle::new()));
        let admissions = Arc::new(AtomicUsize::new(0));
        let threads = [0u8, 1].map(|_| {
            let lifecycle = Arc::clone(&lifecycle);
            let admissions = Arc::clone(&admissions);
            loom::thread::spawn(move || {
                if lifecycle.lock().unwrap().admit() {
                    admissions.fetch_add(1, Ordering::SeqCst);
                }
            })
        });
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(admissions.load(Ordering::SeqCst), 1);
    });
}
