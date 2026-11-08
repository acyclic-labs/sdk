use super::*;
use futures::Stream as _;
use futures::TryFutureExt as _;
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
        idempotency_key: crate::IdempotencyKey::unique(),
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
            stream.next().await,
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
    let mut wakes = Vec::new();
    for id in 0..32 {
        let parent = StreamPath::new(format!("agents/a{id}"))?;
        let path = StreamPath::new(format!("{}/data", parent.as_str()))?;
        append(&provider, path.clone()).await?;
        let mut follow = provider
            .follow(crate::FollowRequest::new(path, 1))
            .map_ok(crate::records_only)
            .await?;
        wakes.push(probe(std::pin::Pin::new(&mut follow)));
        records.push(follow);
    }
    // Seeding must not have woken earlier, unrelated subscriptions either.
    assert!(
        wakes.iter().all(|wake| wake.0.load(Ordering::SeqCst) == 0),
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
            .all(|wake| wake.0.load(Ordering::SeqCst) == 0),
        "append woke unrelated subscriptions"
    );
    assert_eq!(
        woken.0.load(Ordering::SeqCst),
        1,
        "accepted append must wake its follower once"
    );
    let record = follow.next().await.ok_or("follow ended")??;
    assert_eq!(record.sequence, 1);
    let wake = Waker::from(woken.clone());
    assert!(
        std::pin::Pin::new(&mut *follow)
            .poll_next(&mut Context::from_waker(&wake))
            .is_pending()
    );
    provider.inner.poison();
    assert!(wakes.iter().all(|wake| wake.0.load(Ordering::SeqCst) > 0));
    assert_failed(&mut records).await?;
    assert_eq!(provider.inner.failed.receiver_count(), 0);
    eprintln!(
        "local_push subscriptions=32 accepted_record_wakes=1 unrelated_wakes=0 failure_wakes=32"
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
    for keep in [false, true] {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let path = StreamPath::new("watched")?;
        append(&provider, path.clone()).await?;
        let mut follow = {
            provider
                .follow(crate::FollowRequest::new(path, 0))
                .map_ok(crate::records_only)
                .await?
                .map(|item| item.map(|_| ()))
                .boxed()
        };
        let held = provider.inner.publication.write().await;
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
        settled.await?;
        drop(held);
        assert_eq!(
            provider.inner.publication.try_write().is_ok(),
            !keep,
            "keep={keep}"
        );
        if let Some(mut follow) = kept {
            assert!(matches!(follow.next().await, Some(Ok(()))));
            drop(follow);
            assert!(provider.inner.publication.try_write().is_ok());
        }
    }
    Ok(())
}

#[tokio::test]
async fn a_follow_at_its_tail_is_a_reported_wait_until_its_record_arrives()
-> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    let waits = acyclic_native_runtime::obs::WaitReport::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(waits.clone()));
    let report = || -> Result<String, Box<dyn std::error::Error>> {
        let mut output = Vec::new();
        waits.report(&mut output)?;
        Ok(String::from_utf8(output)?)
    };
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let path = StreamPath::new("agents/report/data")?;
    append(&provider, path.clone()).await?;
    let mut follow = provider
        .follow(crate::FollowRequest::new(path.clone(), 1))
        .map_ok(crate::records_only)
        .await?;
    probe(std::pin::Pin::new(&mut follow));
    assert_eq!(report()?.matches("waiting: stream-follow ").count(), 1);
    append(&provider, path).await?;
    assert!(matches!(follow.next().await, Some(Ok(_))));
    assert_eq!(report()?, "");
    Ok(())
}

#[test]
fn executor_retirement_ends_forked_follows_before_publication_unlocks()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let setup = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let provider = setup.block_on(LocalStream::open(
        directory.path(),
        LocalStreamLimits::default(),
    ))?;
    let mut path = StreamPath::new("root")?;
    setup.block_on(provider.append(AppendRequest {
        path: path.clone(),
        records: vec![Bytes::from_static(b"inherited").into()],
        if_tail: Some(0),
        idempotency_key: crate::IdempotencyKey::unique(),
    }))?;
    for name in ["child", "grandchild"] {
        let destination = StreamPath::new(name)?;
        setup.block_on(provider.fork(ForkRequest {
            source: path,
            destination: destination.clone(),
            at_tail: Some(1),
            exclude: Vec::new(),
            idempotency_key: crate::IdempotencyKey::unique(),
        }))?;
        path = destination;
    }
    let AppendOutcome::Committed(seed) = setup.block_on(provider.append(AppendRequest {
        path: path.clone(),
        records: vec![Bytes::from_static(b"local").into()],
        if_tail: Some(1),
        idempotency_key: crate::IdempotencyKey::unique(),
    }))?
    else {
        return Err("forked seed append conflicted".into());
    };

    for (checked, progress) in [(false, false), (true, false), (false, true), (true, true)] {
        for queued in [false, true] {
            let runtime = tokio::runtime::Builder::new_current_thread().build()?;
            let mut request =
                crate::FollowRequest::new(path.clone(), if checked { seed.end } else { 0 });
            request.preceding = checked.then_some(seed.commit_id);
            request.progress = progress;
            let mut follow = setup.block_on(provider.follow(request))?;
            let held = setup.block_on(provider.inner.publication.write());
            let probe = Arc::new(Push::default());
            let wake = Waker::from(probe.clone());
            let (pending, settled) = {
                let _entered = runtime.enter();
                crate::snapshot::observe(
                    if queued {
                        crate::snapshot::Cut::Pending
                    } else {
                        crate::snapshot::Cut::Created
                    },
                    || {
                        std::pin::Pin::new(&mut follow)
                            .poll_next(&mut Context::from_waker(&wake))
                            .is_pending()
                    },
                )
            };
            assert!(pending);
            runtime.block_on(settled)?;
            drop(runtime);
            let woke = probe.0.load(Ordering::SeqCst) > 0;
            let result = {
                let _entered = setup.enter();
                std::pin::Pin::new(&mut follow).poll_next(&mut Context::from_waker(Waker::noop()))
            };
            drop(follow);
            drop(held);
            assert!(
                woke,
                "retired local snapshot did not wake its public follow"
            );
            assert!(
                matches!(
                    result,
                    std::task::Poll::Ready(Some(Err(StreamError::Unavailable)))
                ),
                "retired local snapshot left its public follow parked"
            );
            assert!(
                provider.inner.publication.try_write().is_ok(),
                "retired local snapshot retained its publication reservation"
            );
            assert!(provider.failure().is_none());
        }
    }
    drop(provider);
    let reopened = setup.block_on(LocalStream::open(
        directory.path(),
        LocalStreamLimits::default(),
    ))?;
    assert_eq!(setup.block_on(reopened.tail(path))?, seed.tail);
    Ok(())
}
