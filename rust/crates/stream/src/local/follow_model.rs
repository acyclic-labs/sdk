use super::*;
use crate::OwnedBytes as Bytes;
use crate::verification::block_on;
use std::{mem::ManuallyDrop, task::Context};

// The writer executes accepted publication under its real publication fence.
// Its outer mutation fence has no competing writer here; compaction_model
// covers that fence with a waiting mutation. Journal/device work does not register or wake record
// waiters; acceptance/durability separation is also covered by held-flush tests.
// Lock order: visibility -> publication -> memory state -> path watch.
// Failure-watch borrows end before publication; cancellation owns no lock.
pub(crate) fn registration(omit_wake: bool) {
    loom::model(move || {
        let provider = query_model::fixture(omit_wake).unwrap();
        let keeper = ManuallyDrop::new(provider.clone());
        let path = StreamPath::new("watched").unwrap();
        let seed = block_on(provider.inner.provider.append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"anchor")],
            if_tail: None,
            idempotency_key: None,
        }))
        .unwrap();
        let AppendOutcome::Committed(seed) = seed else {
            unreachable!()
        };
        let writer = loom::thread::spawn({
            let provider = provider.clone();
            let path = path.clone();
            move || {
                block_on(async {
                    let _publication = provider.inner.publication.write().await;
                    provider
                        .inner
                        .provider
                        .append(AppendRequest {
                            path,
                            records: vec![Bytes::from_static(b"accepted")],
                            if_tail: Some(1),
                            idempotency_key: None,
                        })
                        .await
                        .unwrap();
                })
            }
        });
        block_on(async {
            let mut follow = provider
                .follow_checked(path, 1, seed.commit_id)
                .await
                .unwrap();
            let record = follow.next().await.unwrap().unwrap();
            assert_eq!(record.value.as_slice(), b"accepted");
        });
        writer.join().unwrap();
        assert_eq!(provider.inner.failed.receiver_count(), 0);
        drop(ManuallyDrop::into_inner(keeper));
    });
}

pub(crate) fn cancellation(retain: bool) {
    loom::model(move || {
        let provider = query_model::fixture(false).unwrap();
        let keeper = ManuallyDrop::new(provider.clone());
        let path = StreamPath::new("watched").unwrap();
        block_on(provider.inner.provider.append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"seed")],
            if_tail: None,
            idempotency_key: None,
        }))
        .unwrap();
        let mut cancelled = block_on(provider.follow(path.clone(), 1)).unwrap();
        let mut kept = block_on(provider.follow(path.clone(), 1)).unwrap();
        let (_, wake) = crate::discovery_verification::probe();
        assert!(
            cancelled
                .as_mut()
                .poll_next(&mut Context::from_waker(&wake))
                .is_pending()
        );
        assert!(
            kept.as_mut()
                .poll_next(&mut Context::from_waker(&wake))
                .is_pending()
        );
        let dropping = loom::thread::spawn(move || {
            if retain {
                let _leaked = ManuallyDrop::new(cancelled);
            } else {
                drop(cancelled);
            }
        });
        block_on(async {
            let _publication = provider.inner.publication.write().await;
            provider
                .inner
                .provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![Bytes::from_static(b"next")],
                    if_tail: Some(1),
                    idempotency_key: None,
                })
                .await
                .unwrap();
        });
        dropping.join().unwrap();
        assert_eq!(
            provider.inner.failed.receiver_count(),
            1,
            "cancelled follow retained its failure subscription"
        );
        let record = block_on(kept.next()).unwrap().unwrap();
        assert_eq!(record.sequence, 1);
        drop(kept);
        assert_eq!(provider.inner.failed.receiver_count(), 0);
        drop(ManuallyDrop::into_inner(keeper));
    });
}

// Keep the executor's parker outside Loom-managed allocations so a deliberately
// deadlocked negative control can unwind without dropping a Loom Notify guard.
pub(super) fn park<F: Future>(future: F) -> F::Output {
    struct Wake(loom::thread::Thread);
    impl std::task::Wake for Wake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = std::task::Waker::from(Arc::new(Wake(loom::thread::current())));
    let mut future = ManuallyDrop::new(Box::pin(future));
    loop {
        if let std::task::Poll::Ready(value) =
            future.as_mut().poll(&mut Context::from_waker(&waker))
        {
            drop(ManuallyDrop::into_inner(future));
            return value;
        }
        loom::thread::park();
    }
}

// Exercise the real visibility/failure lock ordering in both directions.
// The mutant retains the real failure-watch borrow across publication's await;
// a writer holding publication while poisoning must expose that cycle.
pub(crate) fn failure_lock_order(hold_borrow: bool) {
    loom::model(move || {
        let provider = query_model::fixture(false).unwrap();
        let keeper = ManuallyDrop::new(provider.clone());
        let path = StreamPath::new("watched").unwrap();
        let seed = block_on(provider.inner.provider.append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"seed")],
            if_tail: None,
            idempotency_key: None,
        }))
        .unwrap();
        let AppendOutcome::Committed(seed) = seed else {
            unreachable!()
        };
        let writer = loom::thread::spawn({
            let provider = provider.clone();
            move || {
                // Preserve guards during a deliberately deadlocked model's
                // unwind; releasing them after Loom aborts would double-panic.
                let mut operation = ManuallyDrop::new(Box::pin(async {
                    let _publication = provider.inner.publication.write().await;
                    provider.inner.poison();
                }));
                park(operation.as_mut());
                drop(ManuallyDrop::into_inner(operation));
            }
        });
        if hold_borrow {
            // Mutate the exact check_available -> visibility sequence by
            // retaining its real watch borrow across the publication await.
            let held = ManuallyDrop::new(provider.inner.failed.borrow());
            let visibility = park(provider.inner.publication.read());
            drop(visibility);
            drop(ManuallyDrop::into_inner(held));
        } else {
            block_on(async {
                match provider.follow_checked(path, 1, seed.commit_id).await {
                    Err(error) => assert_eq!(error, StreamError::Unavailable),
                    Ok(mut follow) => {
                        assert_eq!(follow.next().await, Some(Err(StreamError::Unavailable)))
                    }
                }
            });
        }
        writer.join().unwrap();
        drop(ManuallyDrop::into_inner(keeper));
    });
}
