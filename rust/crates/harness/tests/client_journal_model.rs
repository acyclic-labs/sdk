//! Production journal with its crate-local file-lock shim instrumented by loom.
#![cfg(acyclic_harness_loom)]
use acyclic_harness::{
    Error, Result,
    client::{ClientSession, ClientStateStore, SessionRecord},
};
use loom::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

#[path = "support/client_command.rs"]
mod client_command;
// Compile the production path with the dedicated acyclic_harness_loom cfg.
// The crate-local shim instruments exclusion; no Tokio locks are assumed modeled.
#[path = "../src/client/journal.rs"]
mod journal;
#[path = "../src/client/journal_lock.rs"]
mod journal_lock;

#[test]
fn checkpoint_and_paused_opener_preserve_exclusion_and_every_committed_command() {
    check(false);
}

#[test]
#[should_panic(expected = "two writers")]
fn negative_control_detects_missing_exclusion() {
    check(true);
}

fn check(broken: bool) {
    loom::model(move || {
        if broken {
            journal_lock::disable_exclusion();
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state");
        let mut writer =
            ClientSession::open(journal::FileClientState::open(&path).unwrap()).unwrap();
        writer.queue(client_command::command()).unwrap();
        let active = Arc::new(AtomicUsize::new(1));
        let accepted = Arc::new(AtomicUsize::new(0));
        let opener = {
            let path = path.clone();
            let active = active.clone();
            let accepted = accepted.clone();
            thread::spawn(move || match journal::FileClientState::open(&path) {
                Err(Error::Conflict(_)) => {}
                Err(error) => panic!("unexpected open error: {error}"),
                Ok(store) => {
                    if active.fetch_add(1, Ordering::SeqCst) != 0 {
                        accepted.store(2, Ordering::SeqCst);
                        active.fetch_sub(1, Ordering::SeqCst);
                        drop(store);
                        return;
                    }
                    let mut writer = ClientSession::open(store).unwrap();
                    let mut command = client_command::command();
                    command.operation.as_mut().unwrap().operation_id =
                        "00000000-0000-0000-0000-000000000002".into();
                    command.operation.as_mut().unwrap().idempotency_key = "second".into();
                    writer.queue(command).unwrap();
                    accepted.store(1, Ordering::SeqCst);
                    writer.checkpoint().unwrap();
                    assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
                    drop(writer);
                }
            })
        };
        writer.checkpoint().unwrap();
        assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
        drop(writer);
        opener.join().unwrap();
        assert_eq!(active.load(Ordering::SeqCst), 0);
        let accepted = accepted.load(Ordering::SeqCst);
        assert_ne!(accepted, 2, "two writers");
        let recovered =
            ClientSession::open(journal::FileClientState::open(&path).unwrap()).unwrap();
        assert_eq!(recovered.pending().count(), 1 + accepted);
        assert!(
            recovered
                .pending()
                .any(|command| command == &client_command::command())
        );
    });
}
