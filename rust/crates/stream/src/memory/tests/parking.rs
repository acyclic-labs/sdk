use super::*;
use futures::TryFutureExt as _;
use std::task::{Context, Waker};

#[tokio::test]
async fn cancelled_missing_follows_release_path_registrations() {
    let provider = MemoryStream::default();
    for progress in [false, true] {
        let path = StreamPath::new("not-created").unwrap();
        let mut request = crate::FollowRequest::new(path.clone(), 0);
        request.when_absent = crate::WhenAbsent::Wait;
        request.progress = progress;
        let follow = provider.follow(request).await.unwrap();
        assert!(
            provider
                .state
                .read()
                .await
                .signals
                .get(&path)
                .unwrap()
                .is_some()
        );
        drop(follow);
        assert!(
            provider
                .state
                .read()
                .await
                .signals
                .get(&path)
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn unpolled_streams_do_not_block_appends() {
    for checked in [false, true] {
        let provider = MemoryStream::default();
        let path = StreamPath::new("watched").unwrap();
        let AppendOutcome::Committed(seed) = provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![(Bytes::from_static(b"seed")).into()],
                if_tail: None,
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let mut follow = {
            let follow = if checked {
                provider
                    .follow(crate::FollowRequest {
                        bootstrap_head: false,
                        until: None,
                        when_absent: Default::default(),
                        path: path.clone(),
                        from: 1,
                        preceding: Some(seed.commit_id),
                        filter: crate::FollowFilter::default(),
                        progress: false,
                        at_least: Default::default(),
                    })
                    .map_ok(crate::records_only)
                    .await
                    .unwrap()
            } else {
                provider
                    .follow(crate::FollowRequest::new(path.clone(), 0))
                    .map_ok(crate::records_only)
                    .await
                    .unwrap()
            };
            follow.map(|item| item.map(|_| ())).boxed_local()
        };
        let held = provider.state.write().await;
        assert!(
            follow
                .as_mut()
                .poll_next(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(held);
        assert!(matches!(
            provider
                .append(AppendRequest {
                    path,
                    records: vec![(Bytes::from_static(b"next")).into()],
                    if_tail: Some(1),
                    idempotency_key: crate::IdempotencyKey::unique(),
                })
                .await
                .unwrap(),
            AppendOutcome::Committed(_)
        ));
        assert_eq!(follow.next().await, Some(Ok(())));
    }
}

// A dropped consumer cancels its snapshot job: once the job runs it neither
// enters nor stays queued on the lock, so a released writer leaves the lock
// free. The control keeps the consumer: its job does take the handed-off read
// reservation, which this same oracle observes until the consumer completes.
#[tokio::test]
async fn dropped_consumers_leave_no_queued_snapshot_reservation() {
    for keep in [false, true] {
        let provider = MemoryStream::default();
        let path = StreamPath::new("watched").unwrap();
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![(Bytes::from_static(b"seed")).into()],
                if_tail: None,
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await
            .unwrap();
        let mut follow = {
            provider
                .follow(crate::FollowRequest::new(path, 0))
                .map_ok(crate::records_only)
                .await
                .unwrap()
                .map(|item| item.map(|_| ()))
                .boxed_local()
        };
        let held = provider.state.write().await;
        let cut = if keep {
            crate::snapshot::Cut::Pending
        } else {
            crate::snapshot::Cut::Retired
        };
        let (pending, settled) = crate::snapshot::observe(cut, || {
            follow
                .as_mut()
                .poll_next(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        });
        assert!(pending);
        let kept = keep.then_some(follow);
        settled
            .await
            .expect("original snapshot job ended before its observed cut");
        drop(held);
        assert_eq!(provider.state.try_write().is_ok(), !keep, "keep={keep}");
        if let Some(mut follow) = kept {
            assert_eq!(follow.next().await, Some(Ok(())));
            drop(follow);
            assert!(provider.state.try_write().is_ok());
        }
    }
}

#[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
#[tokio::test]
async fn physical_snapshot_submission_keeps_the_original_executor() {
    use acyclic_native_runtime::testing::{Schedule, blocking_on};

    let provider = MemoryStream::default();
    let path = StreamPath::new("physical-snapshot").unwrap();
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"original").into()],
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await
        .unwrap();
    let mut follow = crate::records_only(
        provider
            .follow(crate::FollowRequest::new(path, 0))
            .await
            .unwrap(),
    )
    .boxed();
    let (schedule, mut events) = Schedule::seeded(71);
    let operation = schedule.scope(async {
        let held = provider.state.write().await;
        let (entered, waiting) = futures::channel::oneshot::channel();
        let completion = acyclic_native_runtime::run_blocking_io(
            move || {
                assert!(tokio::runtime::Handle::try_current().is_err());
                let (pending, observed) =
                    crate::snapshot::observe(crate::snapshot::Cut::Pending, || {
                        follow
                            .as_mut()
                            .poll_next(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    });
                assert!(pending, "the original writer must hold the snapshot lock");
                entered.send(observed).unwrap();
                blocking_on(follow.next())
            },
            None,
        )
        .unwrap();
        let (record, ()) = futures::future::join(completion, async {
            waiting
                .await
                .expect("physical producer ended before snapshot submission")
                .await
                .expect("snapshot producer ended before its original lock wait");
            drop(held);
        })
        .await;
        let record = record.unwrap().unwrap().unwrap();
        assert_eq!(record.sequence, 0);
        assert_eq!(record.value.as_slice(), b"original");
        schedule.drained().await;
    });
    tokio::pin!(operation);
    loop {
        tokio::select! {
            biased;
            event = events.recv() => event.expect("lost original submission boundary").release(),
            () = &mut operation => break,
        }
    }
    assert_eq!(
        schedule
            .trace()
            .iter()
            .filter(|event| event.starts_with("task:"))
            .count(),
        1,
        "the original snapshot worker escaped its seeded executor",
    );
}
