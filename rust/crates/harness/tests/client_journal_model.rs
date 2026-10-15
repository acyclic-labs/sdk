//! Exhaustive schedules around the real stable-lock/replace implementation.
use acyclic_harness::{
    Error, Result,
    client::{ClientSession, ClientStateStore, SessionRecord},
};
use loom::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
};

#[path = "support/client_command.rs"]
mod client_command;
// Compile the production file, with only its two filesystem boundary yields enabled.
#[path = "../src/client/journal.rs"]
mod journal;
fn journal_schedule() {
    thread::yield_now();
}

#[test]
fn checkpoint_and_paused_opener_preserve_exclusion_and_every_committed_command() {
    loom::model(|| {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state");
        let mut writer =
            ClientSession::open(journal::FileClientState::open(&path).unwrap()).unwrap();
        writer.queue(client_command::command()).unwrap();
        let active = Arc::new(AtomicUsize::new(1));
        let accepted = Arc::new(AtomicBool::new(false));
        let opener = {
            let path = path.clone();
            let active = active.clone();
            let accepted = accepted.clone();
            thread::spawn(move || match journal::FileClientState::open(&path) {
                Err(Error::Conflict(_)) => {}
                Err(error) => panic!("unexpected open error: {error}"),
                Ok(store) => {
                    assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0, "two writers");
                    let mut writer = ClientSession::open(store).unwrap();
                    let mut command = client_command::command();
                    command.operation.as_mut().unwrap().operation_id =
                        "00000000-0000-0000-0000-000000000002".into();
                    command.operation.as_mut().unwrap().idempotency_key = "second".into();
                    writer.queue(command).unwrap();
                    accepted.store(true, Ordering::SeqCst);
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
        let recovered =
            ClientSession::open(journal::FileClientState::open(&path).unwrap()).unwrap();
        assert_eq!(
            recovered.pending().count(),
            1 + usize::from(accepted.load(Ordering::SeqCst))
        );
        assert!(
            recovered
                .pending()
                .any(|command| command == &client_command::command())
        );
    });
}
