use super::*;
use std::task::{Context, Waker};

#[tokio::test]
async fn unpolled_streams_do_not_block_appends() {
    for (children, checked) in [(false, false), (false, true), (true, false)] {
        let provider = MemoryStream::default();
        let path = StreamPath::new("watched").unwrap();
        let AppendOutcome::Committed(seed) = provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![(Bytes::from_static(b"seed")).into()],
                if_tail: None,
                idempotency_key: None,
            })
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let mut follow = if children {
            provider
                .children(ChildrenRequest {
                    parent: None,
                    limit: 1,
                })
                .await
                .unwrap()
                .map(|item| item.map(|_| ()))
                .boxed_local()
        } else {
            let follow = if checked {
                provider
                    .follow_checked(
                        path.clone(),
                        1,
                        seed.commit_id,
                        crate::FollowFilter::default(),
                    )
                    .await
                    .unwrap()
            } else {
                provider
                    .follow(path.clone(), 0, crate::FollowFilter::default())
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
                    idempotency_key: None,
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
    for (keep, children) in [(false, false), (false, true), (true, false), (true, true)] {
        let provider = MemoryStream::default();
        let path = StreamPath::new("watched").unwrap();
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![(Bytes::from_static(b"seed")).into()],
                if_tail: None,
                idempotency_key: None,
            })
            .await
            .unwrap();
        let mut follow = if children {
            provider
                .children(ChildrenRequest {
                    parent: None,
                    limit: 1,
                })
                .await
                .unwrap()
                .map(|item| item.map(|_| ()))
                .boxed_local()
        } else {
            provider
                .follow(path, 0, crate::FollowFilter::default())
                .await
                .unwrap()
                .map(|item| item.map(|_| ()))
                .boxed_local()
        };
        let held = provider.state.write().await;
        assert!(
            follow
                .as_mut()
                .poll_next(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let kept = keep.then_some(follow);
        // Run the spawned job while the writer still holds the lock.
        tokio::task::yield_now().await;
        drop(held);
        assert_eq!(
            provider.state.try_write().is_ok(),
            !keep,
            "keep={keep} children={children}"
        );
        if let Some(mut follow) = kept {
            assert_eq!(follow.next().await, Some(Ok(())));
            drop(follow);
            assert!(provider.state.try_write().is_ok());
        }
    }
}
