use super::*;
use crate::distributed::JournalWrite;
use acyclic_stream::{CommitCondition, CommitOutcome, MemoryLimits, StreamPath};
use futures::TryStreamExt;
use loom::sync::atomic::{AtomicU64, Ordering};
use loom::thread;

loom::thread_local! {
    static OMIT_OWNER_CONDITION: std::cell::Cell<bool> = std::cell::Cell::new(false);
}

pub(in crate::distributed) fn omit_owner_condition() -> bool {
    OMIT_OWNER_CONDITION.with(std::cell::Cell::get)
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

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
    loom::model(|| {
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
    });
}

struct TickClock(AtomicU64);
impl acyclic_stream::UnixMillisClock for TickClock {
    fn now_unix_millis(&self) -> u64 {
        self.0.fetch_add(1, Ordering::SeqCst)
    }
}

fn committed_at(client: &MemoryStream, path: &str, sequence: u64) -> u64 {
    block_on(async {
        client
            .read(acyclic_stream::ReadRequest {
                path: StreamPath::new(path).unwrap(),
                from: sequence,
                limit: 1,
            })
            .await
            .unwrap()
            .try_next()
            .await
            .unwrap()
            .unwrap()
            .committed_at_micros
    })
}

fn fencing_model(release: bool, settlement: bool, broken: bool) {
    loom::model(move || {
        let client = Arc::new(MemoryStream::new_with_clock(
            MemoryLimits::default(),
            Arc::new(TickClock(AtomicU64::new(1))),
        ));
        let mut original = Box::new(block_on(open(&client)).unwrap());
        let task = OperationId::from_bytes([41; 16]);
        block_on(declare(&mut original, spec(task, 0).unwrap(), "task")).unwrap();
        let lease = block_on(start(&mut original, &worker("worker"), "start")).unwrap();
        let fence = LeaseFence::from(&lease.reservation);
        let (owner, issuer) = owner_issuer();
        let scope = grant(&issuer, "observe", &["operation:observe"]).unwrap();
        // Start at the vulnerable boundary: a valid owner check has completed,
        // but its conditional journal publication races revocation.
        let condition = block_on(original.journal_condition(
            &owner,
            &scope,
            &issuer.verifier(&mut None).unwrap(),
            task,
            &fence,
            if settlement {
                JournalWrite::Settlement
            } else {
                JournalWrite::Fresh
            },
            &mut None,
        ))
        .unwrap();
        let mut rival = Box::new(block_on(open(&client)).unwrap());
        let writer = thread::spawn({
            let client = client.clone();
            move || {
                OMIT_OWNER_CONDITION.with(|fault| fault.set(broken));
                let mut work = None;
                let mut request =
                    commit_request(StreamIdempotencyKey::new("journal").unwrap(), 0, &mut work);
                add_condition(&mut request, condition).unwrap();
                add_condition(
                    &mut request,
                    CommitCondition::Absent {
                        path: StreamPath::new("harness/test/journal").unwrap(),
                    },
                )
                .unwrap();
                add_record(
                    &mut request,
                    StreamPath::new("harness/test/journal").unwrap(),
                    OwnedBytes::from_static(b"result"),
                )
                .unwrap();
                let (outcome, _) = block_on(commit(request, &client, Some(task), None)).unwrap();
                matches!(outcome, CommitOutcome::Committed(_))
            }
        });
        let revocation = thread::spawn(move || {
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
            assert!(block_on(publish(&mut rival, "revoke", event)).unwrap());
            let revoked_at = committed_at(&rival.client, COORDINATOR_PATH, rival.revision - 1);
            if release {
                let replacement =
                    block_on(start(&mut rival, &worker("replacement"), "replace")).unwrap();
                assert_ne!(replacement.reservation.placement, "worker");
            }
            revoked_at
        });
        let committed = writer.join().unwrap();
        let revoked_at = revocation.join().unwrap();
        assert_eq!(journal_tail(&client), u64::from(committed));
        if committed {
            assert!(
                committed_at(&client, "harness/test/journal", 0) < revoked_at,
                "revoked owner published a journal record"
            );
        }
        let mut reopened = Box::new(block_on(open(&client)).unwrap());
        let verifier = issuer.verifier(&mut None).unwrap();
        assert!(
            block_on(reopened.journal_condition(
                &owner,
                &scope,
                &verifier,
                task,
                &LeaseFence::from(&lease.reservation),
                JournalWrite::Fresh,
                &mut None,
            ))
            .is_err()
        );
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
}

#[test]
fn host_publication_cannot_cross_lease_release_or_cancellation() {
    for release in [false, true] {
        for settlement in [false, true] {
            fencing_model(release, settlement, false);
            let error = std::panic::catch_unwind(|| fencing_model(release, settlement, true))
                .expect_err("omitting the production owner condition must be detected");
            let message = error
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| error.downcast_ref::<&str>().copied())
                .unwrap_or("");
            assert!(message.contains("revoked owner published"), "{message}");
        }
    }
}
