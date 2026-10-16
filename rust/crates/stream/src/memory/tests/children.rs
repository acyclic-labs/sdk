use super::*;

#[cfg(not(target_arch = "wasm32"))]
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(128))]
    #[test]
    fn child_index_matches_creation_order_across_snapshot_and_follow(
        writes in proptest::collection::vec((0_u8..16, proptest::prelude::any::<bool>()), 0..64),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime").block_on(async {
            let provider = MemoryStream::default();
            let mut seen = std::collections::BTreeSet::new();
            let mut expected = Vec::new();
            let split = writes.len() / 2;
            for &(id, nested) in &writes[..split] {
                let child = format!("agents/{id}");
                let path = if nested { format!("{child}/leaf") } else { child.clone() };
                append(&provider, &path).await.expect("append");
                if seen.insert(child.clone()) { expected.push(child); }
            }
            let mut children = provider.children(ChildrenRequest {
                parent: Some(StreamPath::new("agents").expect("parent")), limit: 1,
            }).await.expect("children");
            for path in expected {
                assert_eq!(children.next().await.expect("ended").expect("child").path.as_str(), path);
            }
            for &(id, nested) in &writes[split..] {
                let child = format!("agents/{id}");
                let path = if nested { format!("{child}/leaf") } else { child.clone() };
                append(&provider, &path).await.expect("append");
                if seen.insert(child.clone()) {
                    assert_eq!(children.next().await.expect("ended").expect("child").path.as_str(), child);
                } else { assert!(futures::poll!(children.next()).is_pending()); }
            }
            assert!(futures::poll!(children.next()).is_pending());
        });
    }
}

#[tokio::test]
async fn children_replays_every_page_then_pushes_new_paths_once() -> Result<(), StreamError> {
    let provider = MemoryStream::default();
    for path in ["agents/m", "agents/z", "elsewhere/a"] {
        append(&provider, path).await?;
    }
    let mut children = provider
        .children(ChildrenRequest {
            parent: Some(StreamPath::new("agents")?),
            limit: 1,
        })
        .await?;
    assert_eq!(
        children.next().await.transpose()?.map(|child| child.path),
        Some(StreamPath::new("agents/m")?)
    );
    // Creation before the replay finishes must not be lost, even before the cursor.
    append(&provider, "agents/a/nested").await?;
    assert_eq!(
        children.next().await.transpose()?.map(|child| child.path),
        Some(StreamPath::new("agents/z")?)
    );
    assert_eq!(
        children.next().await.transpose()?.map(|child| child.path),
        Some(StreamPath::new("agents/a")?)
    );
    assert!(futures::poll!(children.next()).is_pending());
    append(&provider, "agents/m").await?;
    append(&provider, "elsewhere/b").await?;
    assert!(futures::poll!(children.next()).is_pending());
    append(&provider, "agents/0").await?;
    assert_eq!(
        children.next().await.transpose()?.map(|child| child.path),
        Some(StreamPath::new("agents/0")?)
    );
    assert!(futures::poll!(children.next()).is_pending());
    Ok(())
}

#[tokio::test]
async fn children_subscribes_before_first_poll_and_discovers_forks() -> Result<(), StreamError> {
    let provider = MemoryStream::default();
    let mut roots = provider
        .children(ChildrenRequest {
            parent: None,
            limit: 2,
        })
        .await?;
    let mut children = provider
        .children(ChildrenRequest {
            parent: Some(StreamPath::new("agents")?),
            limit: 2,
        })
        .await?;
    append(&provider, "source").await?;
    provider
        .fork(ForkRequest {
            source: StreamPath::new("source")?,
            destination: StreamPath::new("agents/new")?,
            at_tail: None,
            idempotency_key: None,
        })
        .await?;
    assert_eq!(
        roots.next().await.transpose()?.map(|child| child.path),
        Some(StreamPath::new("source")?)
    );
    assert_eq!(
        roots.next().await.transpose()?.map(|child| child.path),
        Some(StreamPath::new("agents")?)
    );
    assert_eq!(
        children.next().await.transpose()?.map(|child| child.path),
        Some(StreamPath::new("agents/new")?)
    );
    assert!(futures::poll!(children.next()).is_pending());
    assert!(matches!(
        provider
            .children(ChildrenRequest {
                parent: None,
                limit: 0
            })
            .await,
        Err(StreamError::LimitExceeded)
    ));
    Ok(())
}

async fn append(provider: &MemoryStream, path: &str) -> Result<(), StreamError> {
    provider
        .append(AppendRequest {
            path: StreamPath::new(path)?,
            records: vec![Bytes::from_static(b"value")],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn children_replay_visits_each_creation_once_with_limit_one() -> Result<(), StreamError> {
    let provider = MemoryStream::default();
    for index in (0..1_000).rev() {
        append(&provider, &format!("agents/{index}")).await?;
    }
    let mut children = provider
        .children(ChildrenRequest {
            parent: Some(StreamPath::new("agents")?),
            limit: 1,
        })
        .await?;
    for index in (0..1_000).rev() {
        assert_eq!(
            children.next().await.transpose()?.map(|child| child.path),
            Some(StreamPath::new(format!("agents/{index}"))?)
        );
    }
    assert_eq!(
        provider
            .state
            .read()
            .await
            .child_visits
            .load(std::sync::atomic::Ordering::Relaxed),
        1_000
    );
    assert!(futures::poll!(children.next()).is_pending());
    Ok(())
}

#[tokio::test]
async fn children_creation_only_wakes_the_affected_parent() -> Result<(), StreamError> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Wakes(AtomicUsize);
    impl futures::task::ArcWake for Wakes {
        fn wake_by_ref(value: &Arc<Self>) {
            value.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let provider = MemoryStream::default();
    append(&provider, "agents").await?;
    let mut children = provider
        .children(ChildrenRequest {
            parent: Some(StreamPath::new("agents")?),
            limit: 1,
        })
        .await?;
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let waker = futures::task::waker(Arc::clone(&wakes));
    let mut context = std::task::Context::from_waker(&waker);
    assert!(children.as_mut().poll_next(&mut context).is_pending());
    append(&provider, "elsewhere/new").await?;
    assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
    append(&provider, "agents/new").await?;
    assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
    assert_eq!(
        children.next().await.transpose()?.map(|child| child.path),
        Some(StreamPath::new("agents/new")?)
    );
    Ok(())
}

#[cfg(feature = "local")]
#[tokio::test]
async fn children_creation_cursor_rebuilds_from_snapshot() -> Result<(), StreamError> {
    let provider = MemoryStream::default();
    for path in ["agents/z", "agents/a/nested"] {
        append(&provider, path).await?;
    }
    let restored = MemoryStream::default();
    restored
        .install_state(&provider.encode_state().await)
        .await?;
    let mut children = restored
        .children(ChildrenRequest {
            parent: Some(StreamPath::new("agents")?),
            limit: 1,
        })
        .await?;
    for path in ["agents/z", "agents/a"] {
        assert_eq!(
            children.next().await.transpose()?.map(|child| child.path),
            Some(StreamPath::new(path)?)
        );
    }
    append(&restored, "agents/b").await?;
    assert_eq!(
        children.next().await.transpose()?.map(|child| child.path),
        Some(StreamPath::new("agents/b")?)
    );
    assert!(futures::poll!(children.next()).is_pending());
    Ok(())
}
