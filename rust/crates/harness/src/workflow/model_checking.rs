use super::*;
use loom::{sync::Arc as LoomArc, thread};
use serde_json::json;
use std::{
    future::Future,
    task::{Context, Poll},
};

fn ready<F: Future>(future: F) -> F::Output {
    match Box::pin(future)
        .as_mut()
        .poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("memory workflow journal unexpectedly suspended"),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Fault {
    None,
    SkipCursor,
    SplitCommit,
}

loom::thread_local! {
    static FAULT: std::cell::Cell<Fault> = std::cell::Cell::new(Fault::None);
}

pub(super) fn fault_is(fault: Fault) -> bool {
    FAULT.with(|current| current.get() == fault)
}

fn set_fault(fault: Fault) {
    FAULT.with(|current| current.set(fault));
}

fn expect_failure(f: impl FnOnce() + std::panic::UnwindSafe, expected: &str) {
    let error = std::panic::catch_unwind(f).expect_err("broken workflow passed");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains(expected),
        "unexpected model failure: {message}"
    );
}

fn admission() -> WorkflowAdmission {
    WorkflowAdmission {
        operation_id: OperationId::from_bytes([99; 16]),
        request_digest: [2; 32],
        initial: MachineCheckpoint {
            machine: MachineIdentity {
                name: "test.outbox".into(),
                version: "1".into(),
                digest: [1; 32],
            },
            revision: 0,
            state: json!(0),
        },
    }
}

fn record(revision: u8) -> WorkflowRecord {
    let mut prior = admission().initial;
    prior.revision = u64::from(revision);
    prior.state = json!(revision);
    WorkflowRecord {
        operation_id: OperationId::from_bytes([revision + 1; 16]),
        idempotency_key: IdempotencyKey::new(format!("step-{revision}")).unwrap(),
        prior,
        input: json!(revision),
        transition: MachineTransition {
            state: json!(revision + 1),
            commands: vec![WorkflowCommand {
                operation_id: OperationId::from_bytes([revision + 11; 16]),
                kind: "test.deliver".into(),
                payload: payload(),
            }],
            status: MachineStatus::Suspended,
        },
    }
}

fn payload() -> FileRef {
    use crate::{
        conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
        resources::ProviderRef,
    };
    let volume = VolumeRef::new(
        ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None).unwrap(),
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
        &mut None,
    )
    .unwrap();
    FileRef::new(
        volume,
        "commands/first.json".into(),
        "generation-1".into(),
        FileDescriptor::from_bytes(b"{}", "application/json", &mut None).unwrap(),
        "first.json".into(),
        &mut None,
    )
    .unwrap()
}

fn deliver(journal: &MemoryWorkflowJournal, cursor: &mut u64, seen: &mut Vec<WorkflowRecord>) {
    let (page, _) = ready(journal.replay(*cursor, 1, None).unwrap()).unwrap();
    for entry in page {
        assert_eq!(entry.prior.revision, *cursor, "cursor skipped an outbox");
        assert_eq!(entry.transition.commands.len(), 1);
        *cursor += 1;
        seen.push(entry);
    }
}

fn cursor_model(fault: Fault) {
    loom::model(move || {
        let journal = MemoryWorkflowJournal::default();
        ready(journal.admit(admission())).unwrap();
        let journal = LoomArc::new(journal);
        let publisher = {
            let journal = journal.clone();
            thread::spawn(move || {
                for revision in 0..2 {
                    let entry = record(revision);
                    assert_eq!(
                        ready(journal.commit(
                            u64::from(revision),
                            entry.idempotency_key.clone(),
                            entry.clone()
                        ))
                        .unwrap(),
                        entry
                    );
                    // Lost acknowledgement: retry after the subscriber may have read it.
                    assert_eq!(
                        ready(journal.commit(
                            u64::from(revision),
                            entry.idempotency_key.clone(),
                            entry.clone()
                        ))
                        .unwrap(),
                        entry
                    );
                }
            })
        };
        let subscriber = {
            let journal = journal.clone();
            thread::spawn(move || {
                set_fault(fault);
                let mut cursor = 0;
                let mut seen = Vec::new();
                deliver(&journal, &mut cursor, &mut seen);
                deliver(&journal, &mut cursor, &mut seen);
                (cursor, seen)
            })
        };
        publisher.join().unwrap();
        let (mut cursor, mut seen) = subscriber.join().unwrap();
        set_fault(fault);
        // Reconnect from the retained cursor and drain the final published prefix.
        for _ in 0..2 {
            deliver(&journal, &mut cursor, &mut seen);
        }
        deliver(&journal, &mut cursor, &mut seen);
        assert_eq!(seen, vec![record(0), record(1)], "cursor skipped an outbox");
        assert_eq!(cursor, 2);
    });
}

fn checkpoint_model(fault: Fault) {
    loom::model(move || {
        let journal = MemoryWorkflowJournal::default();
        ready(journal.admit(admission())).unwrap();
        let journal = LoomArc::new(journal);
        let writers: Vec<_> = [false, true]
            .into_iter()
            .map(|other| {
                let journal = journal.clone();
                thread::spawn(move || {
                    set_fault(fault);
                    let mut entry = record(0);
                    if other {
                        entry.operation_id = OperationId::from_bytes([90; 16]);
                        entry.idempotency_key = IdempotencyKey::new("rival").unwrap();
                        entry.transition.state = json!(90);
                    }
                    ready(journal.commit(0, entry.idempotency_key.clone(), entry)).is_ok()
                })
            })
            .collect();
        let committed = writers
            .into_iter()
            .map(|writer| writer.join().unwrap())
            .filter(|committed| *committed)
            .count();
        assert_eq!(committed, 1, "split commit accepted competing checkpoints");
        let (page, _) = ready(journal.replay(0, 2, None).unwrap()).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].transition.commands, record(0).transition.commands);
        assert_eq!(page[0].checkpoint().unwrap().1, 1);
        let mut duplicate = record(1);
        duplicate.prior.state = page[0].transition.state.clone();
        duplicate.transition.commands = record(0).transition.commands;
        assert!(ready(journal.commit(1, duplicate.idempotency_key.clone(), duplicate)).is_err());
        assert_eq!(ready(journal.replay(0, 2, None).unwrap()).unwrap().0, page);
    });
}

#[test]
fn outbox_retry_and_cursor_delivery_have_no_gaps_or_duplicates() {
    cursor_model(Fault::None);
    expect_failure(
        || cursor_model(Fault::SkipCursor),
        "cursor skipped an outbox",
    );
}

#[test]
fn competing_outboxes_commit_one_complete_checkpoint() {
    checkpoint_model(Fault::None);
    expect_failure(
        || checkpoint_model(Fault::SplitCommit),
        "split commit accepted competing checkpoints",
    );
}
