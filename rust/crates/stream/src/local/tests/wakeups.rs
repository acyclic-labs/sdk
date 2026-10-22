use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Wake, Waker};

#[derive(Default)]
struct Push(AtomicUsize);
impl Wake for Push {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
async fn append(provider: &LocalStream, path: StreamPath) -> Result<(), StreamError> {
    deferring_durability(provider.append(AppendRequest {
        path,
        records: vec![(Bytes::from_static(b"accepted")).into()],
        if_tail: None,
        idempotency_key: None,
    }))
    .await?;
    Ok(())
}
/// Poll `stream` once under a fresh probe; nothing may be ready yet.
fn probe<S: futures::Stream + ?Sized>(stream: std::pin::Pin<&mut S>) -> Arc<Push> {
    let probe = Arc::new(Push::default());
    let wake = Waker::from(probe.clone());
    assert!(futures::Stream::poll_next(stream, &mut Context::from_waker(&wake)).is_pending());
    probe
}
/// Every subscription ends with one `Unavailable` once the provider fails.
async fn assert_failed<S, T>(streams: &mut [S]) -> Result<(), Box<dyn std::error::Error>>
where
    S: futures::Stream<Item = Result<T, StreamError>> + Unpin,
{
    for stream in streams {
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), stream.next()).await?,
            Some(Err(StreamError::Unavailable))
        ));
        assert!(stream.next().await.is_none());
    }
    Ok(())
}
async fn qualify(broadcast: bool) -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let mut records = Vec::new();
    let mut children = Vec::new();
    let mut wakes = Vec::new();
    let mut child_wakes = Vec::new();
    for id in 0..32 {
        let parent = StreamPath::new(format!("agents/a{id}"))?;
        let path = StreamPath::new(format!("{}/data", parent.as_str()))?;
        append(&provider, path.clone()).await?;
        let mut follow = provider
            .follow(path, 1, crate::FollowFilter::default())
            .await?;
        wakes.push(probe(follow.as_mut()));
        records.push(follow);
        let mut discovery = provider
            .children(ChildrenRequest {
                parent: Some(parent),
                limit: 1,
            })
            .await?;
        assert!(discovery.next().await.is_some());
        child_wakes.push(probe(discovery.as_mut()));
        children.push(discovery);
    }
    // Seeding must not have woken earlier, unrelated subscriptions either.
    assert!(
        wakes
            .iter()
            .chain(&child_wakes)
            .all(|wake| wake.0.load(Ordering::SeqCst) == 0),
        "seeding woke unrelated subscriptions"
    );
    assert_eq!(provider.inner.revision.receiver_count(), 0);
    let revision = *provider.inner.revision.borrow();
    append(&provider, StreamPath::new("agents/a0/data")?).await?;
    assert_eq!(*provider.inner.revision.borrow(), revision + 1);
    if broadcast {
        // Restore the old provider-wide publication wake without poisoning.
        provider.inner.failed.send_modify(|_| {});
    }
    let ([woken, unrelated @ ..], Some(follow)) = (wakes.as_slice(), records.first_mut()) else {
        return Err("no subscriptions were seeded".into());
    };
    assert!(
        unrelated
            .iter()
            .chain(&child_wakes)
            .all(|wake| wake.0.load(Ordering::SeqCst) == 0),
        "append woke unrelated subscriptions"
    );
    assert_eq!(
        woken.0.load(Ordering::SeqCst),
        1,
        "accepted append must wake its follower once"
    );
    let record = tokio::time::timeout(std::time::Duration::from_secs(1), follow.next())
        .await?
        .ok_or("follow ended")??;
    assert_eq!(record.sequence, 1);
    let wake = Waker::from(woken.clone());
    assert!(
        follow
            .as_mut()
            .poll_next(&mut Context::from_waker(&wake))
            .is_pending()
    );
    provider.inner.poison();
    assert!(
        wakes
            .iter()
            .chain(&child_wakes)
            .all(|wake| wake.0.load(Ordering::SeqCst) > 0)
    );
    assert_failed(&mut records).await?;
    assert_failed(&mut children).await?;
    assert_eq!(provider.inner.failed.receiver_count(), 0);
    eprintln!(
        "local_push subscriptions=64 accepted_record_wakes=1 unrelated_wakes=0 failure_wakes=64"
    );
    Ok(())
}
#[tokio::test]
async fn accepted_append_wakes_only_its_path_and_failure_wakes_every_subscriber()
-> Result<(), Box<dyn std::error::Error>> {
    qualify(false).await
}
#[tokio::test]
#[should_panic(expected = "append woke unrelated subscriptions")]
async fn negative_control_broadcasts_every_accepted_append() {
    qualify(true).await.unwrap();
}

// A dropped consumer cancels its snapshot job, so a released publication writer
// leaves the lock free. The control keeps the consumer, whose job is handed the
// read reservation until it completes.
#[tokio::test]
async fn dropped_consumers_leave_no_queued_publication_reservation()
-> Result<(), Box<dyn std::error::Error>> {
    for (keep, children) in [(false, false), (false, true), (true, false), (true, true)] {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let path = StreamPath::new("watched")?;
        append(&provider, path.clone()).await?;
        let mut follow = if children {
            provider
                .children(ChildrenRequest {
                    parent: None,
                    limit: 1,
                })
                .await?
                .map(|item| item.map(|_| ()))
                .boxed()
        } else {
            provider
                .follow(path, 0, crate::FollowFilter::default())
                .await?
                .map(|item| item.map(|_| ()))
                .boxed()
        };
        let held = provider.inner.publication.write().await;
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
            provider.inner.publication.try_write().is_ok(),
            !keep,
            "keep={keep} children={children}"
        );
        if let Some(mut follow) = kept {
            assert!(matches!(follow.next().await, Some(Ok(()))));
            drop(follow);
            assert!(provider.inner.publication.try_write().is_ok());
        }
    }
    Ok(())
}
