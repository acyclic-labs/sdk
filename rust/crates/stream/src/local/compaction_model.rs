use super::*;
use crate::verification::block_on;
use std::task::{Context, Poll};

fn request() -> CommitRequest {
    CommitRequest {
        conditions: vec![
            crate::CommitCondition::Tail {
                path: StreamPath::new("agents/a").unwrap(),
                expected: 1,
            },
            crate::CommitCondition::Absent {
                path: StreamPath::new("agents/b").unwrap(),
            },
        ],
        mutations: ["agents/a", "agents/b"]
            .into_iter()
            .map(|path| crate::CommitMutation::Append {
                path: StreamPath::new(path).unwrap(),
                records: vec![crate::OwnedBytes::from_static(b"accepted")],
            })
            .collect(),
        idempotency_key: IdempotencyKey::new(crate::OwnedBytes::from_static(b"compacting"))
            .unwrap(),
    }
}
fn poll<F: Future>(future: F, wake: &std::task::Waker) -> Poll<F::Output> {
    Box::pin(future)
        .as_mut()
        .poll(&mut Context::from_waker(wake))
}

// Two actors race compaction with accepted reads, cancellation and the next
// mutation's real admission. The device gate fixes the held-I/O cut; all lock
// interleavings run without caps. No later mutation work can precede admission.
pub(crate) fn verify(fault: u8) {
    loom::model(move || {
        let provider = query_model::fixture(false).unwrap();
        let path = StreamPath::new("agents/a").unwrap();
        let seeded = block_on(provider.append(AppendRequest {
            path: path.clone(),
            records: vec![crate::OwnedBytes::from_static(b"seed")],
            if_tail: None,
            idempotency_key: None,
        }))
        .unwrap();
        let AppendOutcome::Committed(seed) = seeded else {
            unreachable!()
        };
        assert_eq!(
            block_on(provider.watermarks(path.clone())).unwrap().durable,
            1
        );
        let mut records =
            block_on(provider.follow(path.clone(), 1, crate::FollowFilter::default())).unwrap();
        let mut cancelled =
            block_on(provider.follow(path.clone(), 1, crate::FollowFilter::default())).unwrap();
        let mut children = block_on(provider.children(ChildrenRequest {
            parent: Some(StreamPath::new("agents").unwrap()),
            limit: 1,
        }))
        .unwrap();
        block_on(children.next()).unwrap().unwrap();
        let (_, wake) = crate::discovery_verification::probe();
        assert!(poll(records.next(), &wake).is_pending());
        assert!(poll(cancelled.next(), &wake).is_pending());
        assert!(poll(children.next(), &wake).is_pending());
        let (entered, waiting) = loom::sync::mpsc::channel();
        let (release, released) = loom::sync::mpsc::channel();
        let writer = loom::thread::spawn({
            let provider = provider.clone();
            move || {
                block_on(async {
                    let mut visibility = Some(provider.inner.visibility.write().await);
                    if fault == 1 {
                        crate::verification::set_fault(
                            crate::verification::Fault::HoldPublicationDuringMaintenance,
                        );
                    }
                    let request = request();
                    let publication = provider.inner.publication.write().await;
                    let frame = provider.prepare(&Command::Commit(request.clone())).unwrap();
                    provider.inner.provider.commit(request).await.unwrap();
                    provider.persist(frame).await.unwrap();
                    let _retained = provider.release_publication(publication);
                    // Physical device ownership is external to the async model;
                    // keep the real journal lock and mutation cut while gated.
                    {
                        let mut journal = provider.inner.journal.lock().unwrap();
                        if fault == 2 {
                            drop(visibility.take());
                        }
                        entered.send(()).unwrap();
                        released.recv().unwrap();
                        let target = journal.appended;
                        provider.inner.publish_synced(&mut journal, target);
                    }
                    provider.inner.commit_clock.unpin();
                })
            }
        });
        waiting.recv().unwrap();
        // A second mutation cannot pass its real outer acquisition. Keeping
        // its acquisition on the reader actor removes irrelevant post-admission
        // work: until the cut closes, no later provider operation is reachable.
        let (admission_probe, admission_wake) = crate::discovery_verification::probe();
        let mut admission = Box::pin(provider.inner.visibility.write());
        let attempt = admission
            .as_mut()
            .poll(&mut Context::from_waker(&admission_wake));
        let deferred = attempt.is_pending();
        drop(attempt);
        // Poll once at the known held cut; no timeout or repeated-poll guess.
        let record = poll(records.next(), &wake);
        let opened = poll(
            provider.follow_checked(
                path.clone(),
                1,
                seed.commit_id,
                crate::FollowFilter::default(),
            ),
            &wake,
        );
        let child = poll(children.next(), &wake);
        let marks = poll(provider.watermarks(path.clone()), &wake);
        let later_visible = block_on(
            provider
                .inner
                .provider
                .tail(StreamPath::new("agents/c").unwrap()),
        );
        let subscriptions = provider.inner.failed.receiver_count();
        drop(cancelled);
        assert_eq!(provider.inner.failed.receiver_count(), subscriptions - 1);
        release.send(()).unwrap();
        writer.join().unwrap();
        if deferred {
            assert!(
                admission_probe.0.load(loom::sync::atomic::Ordering::SeqCst) > 0,
                "compaction lost the waiting mutation wakeup"
            );
            drop(block_on(admission));
        } else {
            drop(admission);
        }
        block_on(deferring_durability(provider.append(AppendRequest {
            path: StreamPath::new("agents/c").unwrap(),
            records: vec![crate::OwnedBytes::from_static(b"later")],
            if_tail: None,
            idempotency_key: None,
        })))
        .unwrap();
        assert!(
            deferred,
            "compaction must preserve accepted reads and the snapshot cut"
        );

        assert_eq!(
            later_visible,
            Err(StreamError::NotFound),
            "compaction must preserve accepted reads and the snapshot cut"
        );
        assert!(
            record.is_ready()
                && matches!(opened, Poll::Ready(Ok(_)))
                && child.is_ready()
                && marks.is_ready(),
            "compaction must preserve accepted reads and the snapshot cut"
        );
        assert!(matches!(record, Poll::Ready(Some(Ok(record))) if record.sequence == 1));
        assert!(matches!(child, Poll::Ready(Some(Ok(child))) if child.path.as_str() == "agents/b"));
        assert_eq!(
            marks,
            Poll::Ready(Ok(crate::StreamWatermarks {
                accepted: 2,
                durable: 1
            }))
        );
        let journal = provider.inner.journal.lock().unwrap();
        assert_eq!((journal.appended, journal.synced), (3, 2));
        assert_eq!(*provider.inner.revision.borrow(), 3);
    });
}
