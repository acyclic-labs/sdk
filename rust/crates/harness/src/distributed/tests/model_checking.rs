use super::model_provider::{
    Pending, PublicationCut, declare_fixture, pause_before_publish, publish_until_cut,
    start_fixture,
};
use super::*;
use crate::distributed::JournalWrite;
use acyclic_stream::{CommitCondition, CommitOutcome, MemoryLimits, StreamPath};
use futures::TryStreamExt;
use loom::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use loom::thread;

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
        report(
            "orchestrate",
            coordinator.orchestrate(
                OperationId::from_bytes([41; 16]),
                IdempotencyKey::new("orchestrate").unwrap(),
                None,
            ),
        );
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

fn fencing_model<const FENCE: bool>(release: bool, settlement: bool) {
    loom::model(move || {
        let client = Arc::new(MemoryStream::new_with_clock(
            MemoryLimits::default(),
            Arc::new(TickClock(AtomicU64::new(1))),
        ));
        let mut original = Box::new(block_on(open(&client)).unwrap());
        let task = OperationId::from_bytes([41; 16]);
        declare_fixture(&mut original, spec(task, 0).unwrap(), "task").unwrap();
        let lease = start_fixture(&mut original, task, &worker("worker"), "start").unwrap();
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
        let revocation_sequence = original.revision;
        let writer_phase = Arc::new(AtomicU8::new(0));
        let writer_client = Arc::new(PublicationCut {
            inner: (*client).clone(),
            phase: writer_phase.clone(),
            omit_condition: false,
        });
        let revoker_phase = Arc::new(AtomicU8::new(0));
        let revoker_client = Arc::new(PublicationCut {
            inner: (*client).clone(),
            phase: revoker_phase.clone(),
            omit_condition: false,
        });
        let mut rival = Box::new(block_on(open(&revoker_client)).unwrap());
        let writer: Pending<bool> = Box::pin(async move {
            let mut work = None;
            let request = journal_request::<FENCE>(
                StreamIdempotencyKey::new("journal").unwrap(),
                0,
                condition,
                CommitCondition::Absent {
                    path: StreamPath::new("harness/test/journal").unwrap(),
                },
                StreamPath::new("harness/test/journal").unwrap(),
                OwnedBytes::from_static(b"result"),
                &mut work,
            )
            .unwrap();
            let (outcome, _) = commit(request, &writer_client, Some(task), None)
                .await
                .unwrap();
            matches!(outcome, CommitOutcome::Committed(_))
        });
        let writer = pause_before_publish(writer, &writer_phase);
        let revocation: Pending<()> = Box::pin(async move {
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
            assert!(publish(&mut rival, "revoke", event).await.unwrap());
            if release {
                let replacement =
                    start_fixture(&mut rival, task, &worker("replacement"), "replace").unwrap();
                assert_ne!(replacement.reservation.placement, "worker");
            }
        });
        let revocation = pause_before_publish(revocation, &revoker_phase);
        let writer = thread::spawn(move || publish_until_cut(writer, writer_phase));
        let revocation = thread::spawn(move || publish_until_cut(revocation, revoker_phase));
        let writer = writer.join().unwrap();
        let revocation = revocation.join().unwrap();
        // Both real storage calls have finished. Acknowledgement/replay cannot
        // change the linearization order recorded by their committed records.
        let published = journal_tail(&client);
        let revoked_at = committed_at(&client, COORDINATOR_PATH, revocation_sequence);
        if published != 0 {
            assert!(
                committed_at(&client, "harness/test/journal", 0) < revoked_at,
                "revoked owner published a journal record"
            );
        }
        let committed = block_on(writer);
        block_on(revocation);
        assert_eq!(published, u64::from(committed));
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
            fencing_model::<true>(release, settlement);
            let error = std::panic::catch_unwind(|| fencing_model::<false>(release, settlement))
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

fn origin_model<const FENCE: bool>() {
    loom::model(|| {
        let client = Arc::new(MemoryStream::default());
        let writers: Vec<_> = (0..2)
            .map(|writer| {
                let client = Arc::clone(&client);
                thread::spawn(move || {
                    let operation = OperationId::from_bytes([140 + writer as u8; 16]);
                    let stream = Stream::new(Arc::clone(&client), COORDINATOR_PATH).unwrap();
                    let event = SchedulerEvent::Declared {
                        spec: Box::new(spec(operation, 0).unwrap()),
                    };
                    let origin = origins::event_path(&event, &mut None).unwrap().unwrap();
                    let key = format!("origin-{writer}");
                    let (key, _) = stream_key(key.len(), format_args!("{key}"), &mut None).unwrap();
                    // Only storage exclusion is under test: canonical decoding is
                    // exercised by ordinary tests against the same publication.
                    block_on(origins::append_origin::<FENCE, _, 1>(
                        (&stream, &client),
                        [OwnedBytes::from_static(b"event")],
                        0,
                        0,
                        key,
                        operation,
                        origin,
                        &mut None,
                    ))
                })
            })
            .collect();
        for writer in writers {
            let _ = writer.join().unwrap();
        }
        assert_eq!(
            block_on(client.tail(StreamPath::new(COORDINATOR_PATH).unwrap())).unwrap(),
            1,
            "stale writer published an origin"
        );
    });
}

#[test]
fn origin_publication_fences_competing_writers() {
    origin_model::<true>();
    let error = std::panic::catch_unwind(origin_model::<false>)
        .expect_err("same detector must reject re-fencing a stale publication");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("stale writer published an origin"),
        "{message}"
    );
}
