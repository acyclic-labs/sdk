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
    use std::task::Waker;

    struct Parent(AtomicUsize);
    impl std::task::Wake for Parent {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let original = |channel_id| wire::SessionChannel {
        channel_id,
        generation: 1,
    };
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
            assert!(
                sources
                    .poll_one(original(id as u64), &mut contexts[id])
                    .is_pending()
            );
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
        let Poll::Ready(Ok(Some(Ok(record)))) = sources.poll_one(original(0), &mut contexts[0])
        else {
            panic!("committed first record must be ready");
        };
        assert_eq!(record.value.as_slice(), b"first");
        assert!(sources.poll_one(original(0), &mut contexts[0]).is_pending());
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
        // Reuse the number before the removed waiter resumes. Its original
        // generation cannot consume or wait on the replacement's records.
        let replacement = wire::SessionChannel {
            channel_id: 0,
            generation: 2,
        };
        sources.register(
            replacement,
            provider
                .read(ReadRequest {
                    path: paths[0].clone(),
                    from: 0,
                    limit: 1,
                    key: None,
                    at_least: Default::default(),
                })
                .await
                .unwrap(),
        );
        assert!(matches!(
            sources.poll_one(original(0), &mut contexts[0]),
            Poll::Ready(Ok(None))
        ));
        let Poll::Ready(Ok(Some(Ok(record)))) = sources.poll_one(replacement, &mut contexts[0])
        else {
            panic!("replacement retains its own original record");
        };
        assert_eq!(record.value.as_slice(), b"initial");
        assert!(matches!(
            sources.poll_one(replacement, &mut contexts[0]),
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
        let records = provider
            .read(ReadRequest {
                path: paths[1].clone(),
                from: 1,
                limit: 1,
                key: None,
                at_least: Default::default(),
            })
            .await
            .unwrap();
        sources.register(
            wire::SessionChannel {
                channel_id: 2,
                generation: 1,
            },
            records,
        );
        assert!(matches!(
            sources.poll_next(&mut contexts[2]),
            Poll::Ready(Ok(Some(Ok(_))))
        ));
        assert!(
            matches!(sources.poll_next(&mut contexts[2]), Poll::Ready(Ok(None))),
            "the last source EOF has no later producer to wake it"
        );
        // The original follow notifications also fund bounded keyed turns.
        // Increasing unrelated ready peers cannot turn one read into a drain.
        for count in [8_u64, 64] {
            let from = provider.tail(paths[1].clone()).await.unwrap();
            let mut peers = ReadySources::<RecordStream, SourceReader>::with_readers();
            for id in 0..count {
                peers.register(
                    wire::SessionChannel {
                        channel_id: id,
                        generation: 1,
                    },
                    crate::records_only(
                        provider
                            .follow(FollowRequest::new(paths[1].clone(), from))
                            .await
                            .unwrap(),
                    ),
                );
                assert!(peers.poll_one(original(id), &mut contexts[0]).is_pending());
            }
            provider
                .append(AppendRequest {
                    path: paths[1].clone(),
                    records: vec![b"bounded keyed turn".as_slice().into()],
                    if_tail: Some(from),
                    idempotency_key: IdempotencyKey::unique(),
                })
                .await
                .unwrap();
            let pending = peers.signal.pending();
            assert_eq!(pending, count as usize);
            let Poll::Ready(Ok(Some(Ok(record)))) = peers.poll_one(original(0), &mut contexts[0])
            else {
                panic!("original keyed follow must deliver its committed record");
            };
            assert_eq!(record.sequence, from);
            assert_eq!(record.value.as_slice(), b"bounded keyed turn");
            assert_eq!(
                peers.signal.pending(),
                pending - 2,
                "one keyed read drained unrelated original notifications"
            );
        }
    });
}

fn late_cancelled_wakes_cannot_disarm_a_live_source() {
    use crate::FollowRequest;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Waker;

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
            let mut sources = ReadySources::<
                crate::BoxProviderStream<'static, Result<crate::Record, StreamError>>,
            >::new();
            sources.register(
                wire::SessionChannel {
                    channel_id: 0,
                    generation: 1,
                },
                Box::pin(futures::stream::pending()),
            );
            let cancelled = sources.entries.get(&0).unwrap().wake.clone();
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
    late_cancelled_wakes_cannot_disarm_a_live_source();
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
        // Measure only transfer of the immutable records, excluding provider
        // reads and registration allocations from scheduler work.
        let records: Vec<_> = provider
            .read(ReadRequest {
                path: path.clone(),
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
        // Cancel real captured record cursors while retaining their callbacks.
        // A burst of stale provider hints must not delay a ready live record.
        let mut cancelled = Vec::new();
        for (channel_id, record) in (1_u64..).zip(&records) {
            shared.register(
                wire::SessionChannel {
                    channel_id,
                    generation: 1,
                },
                futures::stream::iter(vec![record.clone()]),
            );
            cancelled.push(shared.entries.get(&channel_id).unwrap().wake.clone());
            shared.remove(channel_id);
        }
        for wake in cancelled {
            wake.notify();
        }
        // Original record cursors occupy every bit of the actual channel key.
        // Disabled neighbours must not affect delivery or its allocation cost.
        // The zero key then traverses the full fixed-width production index.
        for bit in 0..64 {
            let channel_id = 1_u64 << bit;
            shared.register(
                wire::SessionChannel {
                    channel_id,
                    generation: 2,
                },
                futures::stream::iter(records.clone()),
            );
            shared.mode(channel_id, None);
        }
        shared.register(
            wire::SessionChannel {
                channel_id: 0,
                generation: 1,
            },
            futures::stream::iter(records.clone()),
        );
        let mut cx = Context::from_waker(std::task::Waker::noop());
        // Warm allocator measurement before observing the production scheduler.
        acyclic_test_support::measure(|| {});
        for (sequence, expected) in (0_u64..).zip(records) {
            let mut current = Poll::Pending;
            let pending = shared.signal.pending();
            let allocation = acyclic_test_support::measure(|| {
                current = shared.poll_next(&mut cx);
            });
            let Poll::Ready(Ok(Some(current))) = current else {
                panic!("committed record must be ready");
            };
            assert_eq!(current, expected);
            assert_eq!(current.sequence, sequence);
            assert_eq!(
                shared.signal.pending(),
                pending.saturating_sub(1),
                "ready record delivery drained unrelated provider hints"
            );
            assert_eq!(
                allocation.count_total, 0,
                "ready record scheduling allocated physical heap storage"
            );
        }
        assert_eq!(shared.len(), 65);
        for bit in 0..64 {
            let channel_id = 1_u64 << bit;
            assert!(shared.take(channel_id).is_some());
            assert!(shared.get(channel_id).is_none());
        }
        assert_eq!(shared.len(), 1);
        // Each opening is known runnable; one owner turn polls one original
        // provider, even when every registered follow will initially park.
        let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut idle = ReadySources::new();
        for channel_id in 0..32 {
            let mut follow = crate::records_only(
                provider
                    .follow(crate::FollowRequest::new(path.clone(), 32))
                    .await
                    .unwrap(),
            );
            let polls = polls.clone();
            idle.register(
                wire::SessionChannel {
                    channel_id,
                    generation: 1,
                },
                futures::stream::poll_fn(move |cx| {
                    polls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    follow.poll_next_unpin(cx)
                }),
            );
        }
        for expected in 1..=32 {
            assert!(idle.poll_next(&mut cx).is_pending());
            assert_eq!(
                polls.load(std::sync::atomic::Ordering::Relaxed),
                expected,
                "owner turn polled unrelated pending providers"
            );
        }
        provider
            .append(AppendRequest {
                path,
                records: vec![b"published".as_slice().into()],
                if_tail: Some(32),
                idempotency_key: IdempotencyKey::new(b"published".to_vec()).unwrap(),
            })
            .await
            .unwrap();
        let record = std::future::poll_fn(|cx| idle.poll_next(cx))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(record.sequence, 32);
        assert_eq!(record.value.as_slice(), b"published");
    });
}
