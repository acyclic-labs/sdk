use super::*;
use crate::distributed::JournalWrite;
use acyclic_stream::{CommitMutation, CommitOutcome, CommitRequest, StreamPath};
use futures::executor::block_on;
use loom::sync::Mutex;
use loom::thread;

fn journal_tail(client: &MemoryStream) -> u64 {
    match block_on(Box::pin(
        client.tail(StreamPath::new("harness/test/journal").unwrap()),
    )) {
        Ok(tail) => tail,
        Err(StreamError::NotFound) => 0,
        Err(error) => panic!("journal tail: {error}"),
    }
}

#[test]
fn coordinator_future_sizes() {
    fn report(name: &str, future: impl std::future::Future) {
        eprintln!("{name}: {} bytes", std::mem::size_of_val(&future));
        assert!(
            std::mem::size_of_val(&future) <= 4096,
            "{name} exceeds the 4 KiB future budget"
        );
    }
    let client = Arc::new(MemoryStream::default());
    report("fixture open", open(&client));
    report(
        "open",
        DistributedCoordinator::open(
            &client,
            Arc::new(TestContentVerifier::default()),
            None,
            &mut None,
        ),
    );
    let mut coordinator = block_on(Box::pin(open(&client))).unwrap();
    report("refresh", coordinator.refresh(&mut None));
    let worker = worker("worker");
    report("pull", coordinator.pull(&worker));
    report("fixture start", start(&mut coordinator, &worker, "start"));
    let task = OperationId::from_bytes([41; 16]);
    report(
        "fixture declare",
        declare(&mut coordinator, spec(task, 0).unwrap(), "task"),
    );
    let (owner, issuer) = owner_issuer();
    let scope = grant(
        &issuer,
        "declare",
        &["operation:declare", "operation:observe"],
    )
    .unwrap();
    let verifier = issuer.verifier(&mut None).unwrap();
    report(
        "declare_operation",
        coordinator.declare_operation(
            &owner,
            &scope,
            &verifier,
            spec(task, 0).unwrap(),
            IdempotencyKey::new("declare").unwrap(),
            None,
            &mut None,
        ),
    );
    report(
        "apply",
        coordinator.apply(
            IdempotencyKey::new("cancel").unwrap(),
            SchedulerEvent::CancellationRequested {
                operation_id: task,
                recursive: false,
            },
            &mut None,
        ),
    );
    let fence = LeaseFence {
        reservation_id: "lease".into(),
        placement: "worker".into(),
    };
    report(
        "journal_condition",
        coordinator.journal_condition(
            &owner,
            &scope,
            &verifier,
            task,
            &fence,
            JournalWrite::Fresh,
            &mut None,
        ),
    );
}

// Storage transactions are linearizable; loom schedules the harness protocol's
// separate owner-check, coordinator mutation, and conditional journal commit.
#[test]
fn host_publication_cannot_cross_lease_release_or_cancellation() {
    for release in [false, true] {
        for settlement in [false, true] {
            loom::model(move || {
                let client = Arc::new(MemoryStream::default());
                let mut original = Box::new(block_on(Box::pin(open(&client))).unwrap());
                let task = OperationId::from_bytes([41; 16]);
                block_on(declare(&mut original, spec(task, 0).unwrap(), "task")).unwrap();
                let lease =
                    Box::new(block_on(start(&mut original, &worker("worker"), "start")).unwrap());
                let fence = LeaseFence::from(&lease.reservation);
                let (owner, issuer) = owner_issuer();
                let scope = grant(&issuer, "observe", &["operation:observe"]).unwrap();
                assert!(
                    block_on(Box::pin(original.journal_condition(
                        &owner,
                        &scope,
                        &issuer.verifier(&mut None).unwrap(),
                        task,
                        &fence,
                        JournalWrite::Fresh,
                        &mut None
                    )))
                    .is_ok()
                );
                let mut rival = Box::new(block_on(Box::pin(open(&client))).unwrap());
                let transactions = loom::sync::Arc::new(Mutex::new(()));
                let writer = {
                    let transactions = transactions.clone();
                    let client = client.clone();
                    let fence = fence.clone();
                    thread::spawn(move || {
                        let (owner, issuer) = owner_issuer();
                        let scope = grant(&issuer, "observe", &["operation:observe"]).unwrap();
                        let verifier = issuer.verifier(&mut None).unwrap();
                        let condition = {
                            let _transaction = transactions.lock().unwrap();
                            block_on(Box::pin(original.journal_condition(
                                &owner,
                                &scope,
                                &verifier,
                                task,
                                &fence,
                                if settlement {
                                    JournalWrite::Settlement
                                } else {
                                    JournalWrite::Fresh
                                },
                                &mut None,
                            )))
                        };
                        let Ok(condition) = condition else {
                            return false;
                        };
                        let _transaction = transactions.lock().unwrap();
                        let result = block_on(Box::pin(client.commit(CommitRequest {
                            conditions: vec![
                                condition,
                                acyclic_stream::CommitCondition::Tail {
                                    path: StreamPath::new("harness/test/journal").unwrap(),
                                    expected: 0,
                                },
                            ],
                            mutations: vec![CommitMutation::Append {
                                path: StreamPath::new("harness/test/journal").unwrap(),
                                records: vec![OwnedBytes::from_static(b"result")],
                            }],
                            idempotency_key: StreamIdempotencyKey::new("journal").unwrap(),
                        })))
                        .unwrap();
                        matches!(result, CommitOutcome::Committed(_))
                    })
                };
                let revocation = {
                    let transactions = transactions.clone();
                    thread::spawn(move || {
                        let _transaction = transactions.lock().unwrap();
                        let event = if release {
                            SchedulerEvent::LeaseReleased {
                                operation_id: task,
                                fence,
                            }
                        } else {
                            SchedulerEvent::CancellationRequested {
                                operation_id: task,
                                recursive: false,
                            }
                        };
                        block_on(publish(&mut rival, "revoke", event)).unwrap();
                        if release {
                            let replacement =
                                block_on(start(&mut rival, &worker("replacement"), "replace"))
                                    .unwrap();
                            assert_ne!(replacement.reservation.placement, "worker");
                        }
                        let tail = journal_tail(&rival.client);
                        // A commit already visible here preceded revocation.
                        tail
                    })
                };
                let committed = writer.join().unwrap();
                let prior = revocation.join().unwrap();
                assert_eq!(journal_tail(&client), u64::from(committed));
                let mut reopened = Box::new(block_on(Box::pin(open(&client))).unwrap());
                let (owner, issuer) = owner_issuer();
                let scope = grant(&issuer, "observe", &["operation:observe"]).unwrap();
                let verifier = issuer.verifier(&mut None).unwrap();
                let fresh = block_on(Box::pin(reopened.journal_condition(
                    &owner,
                    &scope,
                    &verifier,
                    task,
                    &LeaseFence::from(&lease.reservation),
                    JournalWrite::Fresh,
                    &mut None,
                )));
                assert!(fresh.is_err());
                // Cancellation permits the current owner's settlement, but a
                // released lease permits neither fresh work nor settlement.
                let retained = block_on(Box::pin(reopened.journal_condition(
                    &owner,
                    &scope,
                    &verifier,
                    task,
                    &LeaseFence::from(&lease.reservation),
                    JournalWrite::Settlement,
                    &mut None,
                )));
                assert_eq!(retained.is_ok(), !release);
                if release || !settlement {
                    assert_eq!(u64::from(committed), prior);
                }
            });
        }
    }
}
