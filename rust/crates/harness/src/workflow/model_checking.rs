use super::*;
use futures::executor::block_on;
use loom::{
    sync::{Arc as LoomArc, Mutex},
    thread,
};
use serde_json::json;

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

fn deliver(
    journal: &Mutex<MemoryWorkflowJournal>,
    cursor: &mut u64,
    seen: &mut Vec<WorkflowRecord>,
) {
    let journal = journal.lock().unwrap();
    let (page, _) = block_on(journal.replay(*cursor, 1, None).unwrap()).unwrap();
    for entry in page {
        assert_eq!(entry.prior.revision, *cursor);
        assert_eq!(entry.transition.commands.len(), 1);
        *cursor += 1;
        seen.push(entry);
    }
}

#[test]
fn outbox_retry_and_cursor_delivery_have_no_gaps_or_duplicates() {
    loom::model(|| {
        let journal = MemoryWorkflowJournal::default();
        block_on(journal.admit(admission())).unwrap();
        let journal = LoomArc::new(Mutex::new(journal));
        let publisher = {
            let journal = journal.clone();
            thread::spawn(move || {
                for revision in 0..2 {
                    let entry = record(revision);
                    {
                        let journal = journal.lock().unwrap();
                        assert_eq!(
                            block_on(journal.commit(
                                u64::from(revision),
                                entry.idempotency_key.clone(),
                                entry.clone()
                            ))
                            .unwrap(),
                            entry
                        );
                    }
                    // Lost acknowledgement: retry after the subscriber may have read it.
                    let journal = journal.lock().unwrap();
                    assert_eq!(
                        block_on(journal.commit(
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
                let mut cursor = 0;
                let mut seen = Vec::new();
                deliver(&journal, &mut cursor, &mut seen);
                deliver(&journal, &mut cursor, &mut seen);
                (cursor, seen)
            })
        };
        publisher.join().unwrap();
        let (mut cursor, mut seen) = subscriber.join().unwrap();
        // Reconnect from the retained cursor and drain the final published prefix.
        for _ in 0..2 {
            deliver(&journal, &mut cursor, &mut seen);
        }
        deliver(&journal, &mut cursor, &mut seen);
        assert_eq!(seen, vec![record(0), record(1)]);
        assert_eq!(cursor, 2);
    });
}

#[test]
fn competing_outboxes_commit_one_complete_checkpoint() {
    loom::model(|| {
        let journal = MemoryWorkflowJournal::default();
        block_on(journal.admit(admission())).unwrap();
        let journal = LoomArc::new(Mutex::new(journal));
        let writers: Vec<_> = [false, true]
            .into_iter()
            .map(|other| {
                let journal = journal.clone();
                thread::spawn(move || {
                    let mut entry = record(0);
                    if other {
                        entry.operation_id = OperationId::from_bytes([90; 16]);
                        entry.idempotency_key = IdempotencyKey::new("rival").unwrap();
                        entry.transition.state = json!(90);
                    }
                    let journal = journal.lock().unwrap();
                    block_on(journal.commit(0, entry.idempotency_key.clone(), entry)).is_ok()
                })
            })
            .collect();
        let committed = writers
            .into_iter()
            .map(|writer| writer.join().unwrap())
            .filter(|committed| *committed)
            .count();
        assert_eq!(committed, 1);
        let journal = journal.lock().unwrap();
        let (page, _) = block_on(journal.replay(0, 2, None).unwrap()).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].transition.commands, record(0).transition.commands);
        assert_eq!(page[0].checkpoint().unwrap().1, 1);
        let mut duplicate = record(1);
        duplicate.prior.state = page[0].transition.state.clone();
        duplicate.transition.commands = record(0).transition.commands;
        assert!(block_on(journal.commit(1, duplicate.idempotency_key.clone(), duplicate)).is_err());
        assert_eq!(
            block_on(journal.replay(0, 2, None).unwrap()).unwrap().0,
            page
        );
    });
}
