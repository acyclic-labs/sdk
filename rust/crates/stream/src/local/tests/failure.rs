use super::super::*;
use futures::FutureExt as _;
use std::{
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};

// The OS refuses a real journal write through a held read-only descriptor.
// Keep its lease so the descriptor cache cannot reopen it with write access.
#[tokio::test]
async fn failed_physical_write_keeps_its_cause_and_ends_followers()
-> Result<(), Box<dyn std::error::Error>> {
    struct Observer(AtomicBool);
    impl futures::task::ArcWake for Observer {
        fn wake_by_ref(observer: &Arc<Self>) {
            observer.0.store(true, Ordering::Release);
        }
    }
    fn observed<T>(result: crate::NativeStreamResult<T>) -> Result<(), StreamError> {
        match result {
            Ok(result) => result.map(|_| ()),
            Err(error) => panic!("observation took an unrelated worker's physical error: {error}"),
        }
    }
    for native in [false, true] {
        let (schedule, mut boundaries) = acyclic_native_runtime::testing::Schedule::new();
        let (root, path, meter) = super::journal_lifecycle(&schedule, &mut boundaries, async {
            let root = tempfile::tempdir()?;
            let stream = LocalStream::open(root.path(), LocalStreamLimits::default()).await?;
            let path = StreamPath::new("failure")?;
            stream.append(super::keyed_append("failure", 0)?).await?;
            // Scope the owned workers, but poll reads through ordinary dispatch:
            // scheduling instrumentation must not insert an observation wait.
            let mut follow = acyclic_native_runtime::testing::untracked(
                stream.follow(crate::FollowRequest::new(path.clone(), 1)),
            )
            .await?;
            {
                let next =
                    std::pin::pin!(acyclic_native_runtime::testing::untracked(follow.next()));
                assert!(futures::poll!(next).is_pending());
            }
            let meter = Arc::new(crate::test_meter::Meter::local());
            let request = super::prepared_fixture(&stream).await?;
            let packet = stream
                .native_prepare_commit(request.clone(), 64, meter.original())
                .await??;
            let (packet, allocation) = packet.into_parts();
            let mut revision = stream.wait_applied(&Default::default()).await?;
            revision.index += 1;
            let minimum = crate::RevisionCut::of(revision);
            let future_read = ReadRequest {
                path: path.clone(),
                from: 0,
                limit: 1,
                key: None,
                at_least: minimum.clone(),
            };
            let mut future_follow = crate::FollowRequest::new(path.clone(), 1);
            future_follow.at_least = minimum.clone();
            let future_heads = crate::HeadsRequest {
                paths: std::iter::once(path.clone()).collect(),
                retained: Default::default(),
                projection: crate::HeadProjection::Tail,
                durable_through: None,
                children: None,
                limit: 1,
                at_least: minimum.clone(),
            };
            let file_path = root.path().join(JOURNAL_FILE);
            let read_only = acyclic_native_runtime::CachedFile::new(
                std::fs::File::open(&file_path)?,
                &file_path,
                true,
                false,
            )?;
            let held = read_only.lease()?;
            stream
                .inner
                .journal
                .lock()
                .map_err(|_| "journal poisoned")?
                .file = read_only;
            let work = if native {
                Some(crate::native::Work::new(meter.original())?)
            } else {
                None
            };
            let frame = PreparedFrame::encode(
                &WriteRequest::Append(super::keyed_append("failure", 1)?),
                SystemUnixMillisClock.now_unix_millis(),
                work.clone(),
            )?;
            let visibility = stream.inner.visibility.write().await;
            let publication = stream.inner.publication.write().await;
            // These public operations are parked before they can observe memory.
            // Failure must wake them while both original fences remain held.
            let mut waits: [crate::BoxProviderFuture<'_, Result<(), StreamError>>; 13] = [
                Box::pin(
                    acyclic_native_runtime::testing::untracked(stream.tail(path.clone()))
                        .map(|result| result.map(|_| ())),
                ),
                Box::pin(
                    acyclic_native_runtime::testing::untracked(stream.read(ReadRequest {
                        path: path.clone(),
                        from: 0,
                        limit: 1,
                        key: None,
                        at_least: Default::default(),
                    }))
                    .map(|result| result.map(|_| ())),
                ),
                Box::pin(
                    acyclic_native_runtime::testing::untracked(
                        stream.follow(crate::FollowRequest::new(path.clone(), 1)),
                    )
                    .map(|result| result.map(|_| ())),
                ),
                Box::pin(
                    stream
                        .commit(CommitRequest {
                            conditions: vec![crate::CommitCondition::Tail {
                                path: path.clone(),
                                expected: 1,
                            }],
                            mutations: vec![crate::CommitMutation::Append {
                                path: path.clone(),
                                records: vec!["queued".into()],
                            }],
                            deadline_unix_millis: None,
                            idempotency_key: IdempotencyKey::unique(),
                        })
                        .map(|result| result.map(|_| ())),
                ),
                Box::pin(
                    stream
                        .native_commit_guarded(request.clone(), None, meter.original(), || {
                            std::future::ready(Ok::<_, StreamError>(()))
                        })
                        .map(observed),
                ),
                Box::pin(
                    stream
                        .native_prepare_commit(request, 64, meter.original())
                        .map(observed),
                ),
                Box::pin(
                    packet
                        .finish(crate::OwnedBytes::from_static(b"queued prepared record").into())
                        .map(observed),
                ),
                // Future cuts park before publication. Their original producer
                // must retire even while both publication fences stay held.
                Box::pin(
                    stream
                        .wait_applied(&minimum)
                        .map(|result| result.map(|_| ())),
                ),
                Box::pin(
                    acyclic_native_runtime::testing::untracked(stream.read(future_read.clone()))
                        .map(|result| result.map(|_| ())),
                ),
                Box::pin(
                    acyclic_native_runtime::testing::untracked(stream.follow(future_follow))
                        .map(|result| result.map(|_| ())),
                ),
                Box::pin(
                    acyclic_native_runtime::testing::untracked(stream.heads(future_heads.clone()))
                        .map(|result| result.map(|_| ())),
                ),
                Box::pin(
                    stream
                        .native_read(future_read, meter.original())
                        .map(observed),
                ),
                Box::pin(
                    stream
                        .native_heads(future_heads, meter.original())
                        .map(observed),
                ),
            ];
            let observers: [_; 13] =
                std::array::from_fn(|_| Arc::new(Observer(AtomicBool::new(false))));
            let wakers = observers
                .each_ref()
                .map(|observer| futures::task::waker(observer.clone()));
            for (waiting, waker) in waits.iter_mut().zip(&wakers) {
                assert!(
                    waiting
                        .as_mut()
                        .poll(&mut Context::from_waker(waker))
                        .is_pending()
                );
            }
            assert_eq!(stream.persist(frame).await, Err(StreamError::Unavailable));
            for (index, ((waiting, waker), observer)) in
                waits.iter_mut().zip(&wakers).zip(&observers).enumerate()
            {
                let diagnostic = match index {
                    3 => "failed journal left a conditional mutation parked",
                    4 => "failed journal left a native guarded commit parked",
                    5 => "failed journal left native preparation parked",
                    6 => "failed journal left a prepared completion parked",
                    7.. => "failed journal left a future revision parked",
                    _ => "failed journal left a public observation parked",
                };
                assert!(observer.0.load(Ordering::Acquire), "{diagnostic}");
                assert!(
                    matches!(
                        waiting.as_mut().poll(&mut Context::from_waker(waker)),
                        Poll::Ready(Err(StreamError::Unavailable))
                    ),
                    "{diagnostic}"
                );
            }
            drop(waits);
            drop(allocation);
            let cause = stream.failure().ok_or("missing original failure")?;
            assert_eq!(cause.operation, "write");
            assert_eq!(cause.cause, "io");
            assert!(cause.io_kind.is_some());
            assert!(cause.raw_os_error.is_some());
            if let Some(work) = work {
                assert_eq!(work.failure("write"), Some(cause));
                assert!(
                    work.finish::<(), StreamError>(Err(StreamError::Unavailable))
                        .is_err()
                );
            }
            stream.inner.poison();
            assert_eq!(
                stream.failure(),
                Some(cause),
                "a secondary failure replaced the OS cause"
            );
            drop(publication);
            drop(visibility);
            assert!(matches!(
                acyclic_native_runtime::testing::untracked(follow.next()).now_or_never(),
                Some(Some(Err(StreamError::Unavailable)))
            ));
            assert!(matches!(
                acyclic_native_runtime::testing::untracked(follow.next()).now_or_never(),
                Some(None)
            ));
            drop(follow);
            drop(held);
            drop(stream);
            Ok::<_, Box<dyn std::error::Error>>((root, path, meter))
        })
        .await??;
        meter.assert_refunded();
        let reopened = LocalStream::open(root.path(), LocalStreamLimits::default()).await?;
        assert_eq!(
            reopened.tail(path).await?,
            1,
            "failed physical write fabricated a commit"
        );
    }
    Ok(())
}
