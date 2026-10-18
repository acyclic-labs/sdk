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
                records: vec![Bytes::from_static(b"seed")],
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
                .boxed()
        } else {
            let follow = if checked {
                provider
                    .follow_checked(path.clone(), 1, seed.commit_id)
                    .await
                    .unwrap()
            } else {
                provider.follow(path.clone(), 0).await.unwrap()
            };
            follow.map(|item| item.map(|_| ())).boxed()
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
                    records: vec![Bytes::from_static(b"next")],
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
