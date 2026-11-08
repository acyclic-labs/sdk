use super::*;
use crate::{
    AppendRequest, IdempotencyKey, MemoryStream, ReadRequest, StreamPath, StreamProvider,
    StreamProviderExt as _,
};
use futures::StreamExt;

#[test]
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
        // Measure only transfer of the immutable records, excluding provider
        // reads and registration allocations from scheduler work.
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
        let mut cx = Context::from_waker(std::task::Waker::noop());
        // Warm allocator measurement before observing the production scheduler.
        acyclic_test_support::measure(|| {});
        for (sequence, expected) in (0_u64..).zip(records) {
            let mut current = Poll::Pending;
            let allocation = acyclic_test_support::measure(|| {
                current = shared.poll_next(&mut cx);
            });
            let Poll::Ready(Ok(Some(current))) = current else {
                panic!("committed record must be ready");
            };
            assert_eq!(current, expected);
            assert_eq!(current.sequence, sequence);
            assert_eq!(
                allocation.count_total, 0,
                "ready record scheduling allocated physical heap storage"
            );
        }
    });
}
