use super::*;
use crate::{
    AppendRequest, IdempotencyKey, MemoryStream, ReadRequest, StreamPath, StreamProvider,
    StreamProviderExt as _,
};
use futures::StreamExt;

#[test]
fn keyed_readers_keep_independent_wakes_and_share_ready_delivery() {
    use crate::{FollowRequest, RecordStream};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake as _, Waker};

    struct Parent(AtomicUsize);
    impl std::task::Wake for Parent {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    futures::executor::block_on(async {
        let provider = MemoryStream::default();
        let mut sources = ReadySources::<RecordStream, SourceReader>::with_readers();
        let parents: [_; 3] = std::array::from_fn(|_| Arc::new(Parent(AtomicUsize::new(0))));
        let wakers = parents.each_ref().map(|parent| Waker::from(parent.clone()));
        let mut contexts = wakers.each_ref().map(Context::from_waker);
        let paths: [_; 2] =
            std::array::from_fn(|id| StreamPath::new(format!("ready/keyed/{id}")).unwrap());
        for (id, path) in paths.iter().enumerate() {
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![b"initial".as_slice().into()],
                    if_tail: Some(0),
                    idempotency_key: IdempotencyKey::unique(),
                })
                .await
                .unwrap();
            sources.register(
                wire::SessionChannel {
                    channel_id: id as u64,
                    generation: 1,
                },
                crate::records_only(
                    provider
                        .follow(FollowRequest::new(path.clone(), 1))
                        .await
                        .unwrap(),
                ),
            );
            assert!(sources.poll_one(id as u64, &mut contexts[id]).is_pending());
        }
        assert!(sources.poll_next(&mut contexts[2]).is_pending());
        for parent in &parents {
            parent.0.store(0, Ordering::SeqCst);
        }
        provider
            .append(AppendRequest {
                path: paths[0].clone(),
                records: vec![b"first".as_slice().into()],
                if_tail: Some(1),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await
            .unwrap();
        assert!(
            parents[0].0.load(Ordering::SeqCst) > 0,
            "a different keyed reader must not replace this wake"
        );
        assert_eq!(parents[1].0.load(Ordering::SeqCst), 0);
        assert!(parents[2].0.load(Ordering::SeqCst) > 0);
        let Poll::Ready(Ok(Some(Ok(record)))) = sources.poll_one(0, &mut contexts[0]) else {
            panic!("committed first record must be ready");
        };
        assert_eq!(record.value.as_slice(), b"first");
        assert!(sources.poll_one(0, &mut contexts[0]).is_pending());
        assert!(sources.poll_next(&mut contexts[2]).is_pending());
        parents[0].0.store(0, Ordering::SeqCst);
        parents[2].0.store(0, Ordering::SeqCst);
        sources.remove(0);
        assert!(
            parents[0].0.load(Ordering::SeqCst) > 0,
            "removal must settle the original keyed wait"
        );
        assert!(
            parents[2].0.load(Ordering::SeqCst) > 0,
            "removal must wake the combined reader too"
        );
        assert!(matches!(
            sources.poll_one(0, &mut contexts[0]),
            Poll::Ready(Ok(None))
        ));
        assert!(sources.poll_next(&mut contexts[2]).is_pending());
        provider
            .append(AppendRequest {
                path: paths[1].clone(),
                records: vec![b"second".as_slice().into()],
                if_tail: Some(1),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await
            .unwrap();
        assert!(parents[1].0.load(Ordering::SeqCst) > 0);
        let Poll::Ready(Ok(Some(Ok(record)))) = sources.poll_next(&mut contexts[2]) else {
            panic!("committed second record must be ready");
        };
        assert_eq!(record.value.as_slice(), b"second");
        sources.remove(1);
        // Exhaust the actual immutable provider read through the same owner.
        let records: Vec<_> = provider
            .read(ReadRequest {
                path: paths[1].clone(),
                from: 1,
                limit: 1,
                key: None,
                at_least: Default::default(),
            })
            .await
            .unwrap()
            .collect()
            .await;
        sources.register(
            wire::SessionChannel {
                channel_id: 2,
                generation: 1,
            },
            Box::pin(futures::stream::iter(records)),
        );
        assert!(matches!(
            sources.poll_next(&mut contexts[2]),
            Poll::Ready(Ok(Some(Ok(_))))
        ));
        assert!(
            matches!(sources.poll_next(&mut contexts[2]), Poll::Ready(Ok(None))),
            "the last source EOF has no later producer to wake it"
        );
    });
}

#[test]
fn late_cancelled_wakes_cannot_disarm_a_live_source() {
    use crate::{FollowRequest, RecordStream};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake as _, Waker};

    struct Parent(AtomicUsize);
    impl std::task::Wake for Parent {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    futures::executor::block_on(async {
        for replace in [false, true] {
            let provider = MemoryStream::default();
            let path = StreamPath::new("ready/cancelled").unwrap();
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![b"initial".as_slice().into()],
                    if_tail: Some(0),
                    idempotency_key: IdempotencyKey::new(b"initial".to_vec()).unwrap(),
                })
                .await
                .unwrap();
            let mut sources = ReadySources::<RecordStream>::new();
            sources.register(
                wire::SessionChannel {
                    channel_id: 0,
                    generation: 1,
                },
                Box::pin(futures::stream::pending()),
            );
            let cancelled = sources.entries[&0].wake.clone();
            sources.remove(0);
            let mut records = crate::records_only(
                provider
                    .follow(FollowRequest::new(path.clone(), 1))
                    .await
                    .unwrap(),
            );
            let mut late = Some(cancelled);
            sources.register(
                wire::SessionChannel {
                    channel_id: u64::from(!replace),
                    generation: 2,
                },
                Box::pin(futures::stream::poll_fn(move |cx| {
                    let result = records.poll_next_unpin(cx);
                    if let Some(cancelled) = late.take() {
                        // A retained provider callback can outlive removal or
                        // replacement and notify during the last live poll.
                        super::Wake::wake_by_ref(&cancelled);
                    }
                    result
                })),
            );
            let parent = Arc::new(Parent(AtomicUsize::new(0)));
            let waker = Waker::from(parent.clone());
            let mut cx = Context::from_waker(&waker);
            assert!(sources.poll_next(&mut cx).is_pending());
            assert_eq!(parent.0.load(Ordering::SeqCst), 0);
            provider
                .append(AppendRequest {
                    path,
                    records: vec![b"changed".as_slice().into()],
                    if_tail: Some(1),
                    idempotency_key: IdempotencyKey::new(b"changed".to_vec()).unwrap(),
                })
                .await
                .unwrap();
            assert!(
                parent.0.load(Ordering::SeqCst) > 0,
                "a late cancelled wake must not disarm the live provider"
            );
            let Poll::Ready(Ok(Some(Ok(record)))) = sources.poll_next(&mut cx) else {
                panic!("the live provider's committed record must be ready");
            };
            assert_eq!(record.sequence, 1);
            assert_eq!(record.value.as_slice(), b"changed");
        }
    });
}

#[test]
fn ready_records_reuse_the_registered_source_node() {
    futures::executor::block_on(async {
        let provider = MemoryStream::default();
        let path = StreamPath::new("ready/allocation").unwrap();
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![b"committed".as_slice().into(); 32],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::new(b"ready allocation".to_vec()).unwrap(),
            })
            .await
            .unwrap();
        // Capture actual committed provider output before measuring scheduling.
        // Both schedulers transfer the same immutable records, without measuring
        // provider reads or setup allocations as scheduler work.
        let records: Vec<_> = provider
            .read(ReadRequest {
                path,
                from: 0,
                limit: 32,
                key: None,
                at_least: Default::default(),
            })
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect()
            .await;
        assert_eq!(records.len(), 32);
        let mut shared = ReadySources::new();
        shared.register(
            wire::SessionChannel {
                channel_id: 0,
                generation: 1,
            },
            futures::stream::iter(records.clone()),
        );
        let mut recreated = futures::stream::SelectAll::new();
        recreated.push(futures::stream::iter(records));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        // Warm allocator measurement before either arm.
        acyclic_test_support::measure(|| {});
        let mut before = 0;
        let mut after = 0;
        for sequence in 0..32 {
            let mut old = Poll::Pending;
            let allocation = acyclic_test_support::measure(|| {
                old = Pin::new(&mut recreated).poll_next(&mut cx);
            });
            before += allocation.count_total;
            let mut current = Poll::Pending;
            let allocation = acyclic_test_support::measure(|| {
                current = shared.poll_next(&mut cx);
            });
            after += allocation.count_total;
            let Poll::Ready(Some(old)) = old else {
                panic!("committed record must be ready");
            };
            let Poll::Ready(Ok(Some(current))) = current else {
                panic!("committed record must be ready");
            };
            assert_eq!(current, old);
            assert_eq!(current.sequence, sequence);
            assert_eq!(allocation.count_total, 0);
        }
        assert_eq!(
            before, 32,
            "recreated task must fail the zero-allocation bound"
        );
        assert_eq!(after, 0);
        eprintln!(
            "ready_source records=32 task_allocations_before={before} task_allocations_after={after}"
        );
    });
}
