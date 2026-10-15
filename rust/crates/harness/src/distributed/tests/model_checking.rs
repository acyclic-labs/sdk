use super::*;
use crate::distributed::JournalWrite;
use acyclic_stream::{CommitMutation, CommitOutcome, CommitRequest, StreamPath};
use futures::executor::block_on;
use loom::sync::Mutex;
use loom::thread;

// Keep opaque provider futures off loom's coroutine stack. Workers access no
// modeled primitives; concurrent callers hold the transaction's loom mutex.
fn transaction<T: Send>(run: impl FnOnce() -> T + Send) -> T {
    std::thread::scope(|scope| scope.spawn(run).join().unwrap())
}

// Storage transactions are linearizable; loom schedules the harness protocol's
// separate owner-check, coordinator mutation, and conditional journal commit.
#[test]
fn host_publication_cannot_cross_lease_release_or_cancellation() {
    for release in [false, true] {
        for settlement in [false, true] {
            loom::model(move || {
                let (client, mut original, mut rival, lease) = transaction(|| {
                    let client = Arc::new(MemoryStream::default());
                    let mut original = block_on(open(&client)).unwrap();
                    let task = OperationId::from_bytes([41; 16]);
                    block_on(declare(&mut original, spec(task, 0).unwrap(), "task")).unwrap();
                    let lease = block_on(start(&mut original, &worker("worker"), "start")).unwrap();
                    let (owner, issuer) = owner_issuer();
                    let scope = grant(&issuer, "observe", &["operation:observe"]).unwrap();
                    assert!(
                        block_on(original.journal_condition(
                            &owner,
                            &scope,
                            &issuer.verifier(&mut None).unwrap(),
                            task,
                            &LeaseFence::from(&lease.reservation),
                            JournalWrite::Fresh,
                            &mut None
                        ))
                        .is_ok()
                    );
                    let rival = block_on(open(&client)).unwrap();
                    (client, Box::new(original), Box::new(rival), lease)
                });
                let task = lease.operation.operation_id;
                let fence = LeaseFence::from(&lease.reservation);
                let transactions = loom::sync::Arc::new(Mutex::new(()));
                let writer = {
                    let transactions = transactions.clone();
                    let client = client.clone();
                    let fence = fence.clone();
                    thread::spawn(move || {
                        let condition = {
                            let _transaction = transactions.lock().unwrap();
                            transaction(|| {
                                let (owner, issuer) = owner_issuer();
                                let scope =
                                    grant(&issuer, "observe", &["operation:observe"]).unwrap();
                                let verifier = issuer.verifier(&mut None).unwrap();
                                block_on(original.journal_condition(
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
                                ))
                            })
                        };
                        let Ok(condition) = condition else {
                            return false;
                        };
                        let _transaction = transactions.lock().unwrap();
                        transaction(|| {
                            let result = block_on(client.commit(CommitRequest {
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
                            }))
                            .unwrap();
                            matches!(result, CommitOutcome::Committed(_))
                        })
                    })
                };
                let revocation = {
                    let transactions = transactions.clone();
                    thread::spawn(move || {
                        let _transaction = transactions.lock().unwrap();
                        transaction(|| {
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
                            // A journal append already visible here preceded revocation.
                            block_on(
                                rival
                                    .client
                                    .tail(StreamPath::new("harness/test/journal").unwrap()),
                            )
                            .unwrap()
                        })
                    })
                };
                let committed = writer.join().unwrap();
                let prior = revocation.join().unwrap();
                transaction(|| {
                    assert_eq!(
                        block_on(client.tail(StreamPath::new("harness/test/journal").unwrap()))
                            .unwrap(),
                        u64::from(committed)
                    );
                    let mut reopened = block_on(open(&client)).unwrap();
                    let (owner, issuer) = owner_issuer();
                    let scope = grant(&issuer, "observe", &["operation:observe"]).unwrap();
                    let verifier = issuer.verifier(&mut None).unwrap();
                    let fresh = block_on(reopened.journal_condition(
                        &owner,
                        &scope,
                        &verifier,
                        task,
                        &LeaseFence::from(&lease.reservation),
                        JournalWrite::Fresh,
                        &mut None,
                    ));
                    assert!(fresh.is_err());
                    // Cancellation permits the current owner's settlement, but a
                    // released lease permits neither fresh work nor settlement.
                    let retained = block_on(reopened.journal_condition(
                        &owner,
                        &scope,
                        &verifier,
                        task,
                        &LeaseFence::from(&lease.reservation),
                        JournalWrite::Settlement,
                        &mut None,
                    ));
                    assert_eq!(retained.is_ok(), !release);
                });
                if release || !settlement {
                    assert_eq!(u64::from(committed), prior);
                }
            });
        }
    }
}
