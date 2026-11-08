use super::*;
use futures::Stream as _;
use futures::TryFutureExt as _;
use std::task::{Context, Waker};

#[tokio::test]
async fn snapshot_callbacks_retire_outside_the_original_job_mutex() {
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::task::Wake;

    struct Trace {
        retired: AtomicUsize,
        under_lock: AtomicBool,
    }
    struct ConsumerWake {
        probe: Arc<crate::snapshot::Probe>,
        trace: Arc<Trace>,
        published: Mutex<Option<futures::channel::oneshot::Sender<()>>>,
    }
    impl ConsumerWake {
        fn observe(&self) {
            if !self.probe.unlocked() {
                self.trace.under_lock.store(true, Ordering::SeqCst);
            }
        }
    }
    impl Wake for ConsumerWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.observe();
            if let Some(published) = self.published.lock().unwrap().take() {
                let _ = published.send(());
            }
        }
    }
    impl Drop for ConsumerWake {
        fn drop(&mut self) {
            self.observe();
            self.trace.retired.fetch_add(1, Ordering::SeqCst);
        }
    }

    for keep in [false, true] {
        let provider = MemoryStream::default();
        let path = StreamPath::new("snapshot-callback-custody").unwrap();
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![Bytes::from_static(b"record").into()],
                if_tail: None,
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await
            .unwrap();
        let mut follow = provider
            .follow(crate::FollowRequest::new(path, 0))
            .await
            .unwrap();
        let held = provider.state.write().await;
        let (pending, created) = crate::snapshot::observe(crate::snapshot::Cut::Created, || {
            std::pin::Pin::new(&mut follow)
                .poll_next(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        });
        assert!(pending);
        let probe = Arc::new(
            created
                .await
                .expect("the dispatched snapshot owns its mutex"),
        );
        let (published, publication) = futures::channel::oneshot::channel();
        let mut published = Some(published);
        let trace = Arc::new(Trace {
            retired: AtomicUsize::new(0),
            under_lock: AtomicBool::new(false),
        });
        // Each Waker's last owner is the real receiver registration. Replacing
        // it, cancelling it and publishing it must all permit callback re-entry.
        for index in 0..2 {
            let waker = Waker::from(Arc::new(ConsumerWake {
                probe: Arc::clone(&probe),
                trace: Arc::clone(&trace),
                published: Mutex::new(if index == 1 { published.take() } else { None }),
            }));
            assert!(
                std::pin::Pin::new(&mut follow)
                    .poll_next(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }
        assert_eq!(trace.retired.load(Ordering::SeqCst), 1);
        if keep {
            drop(held);
            // The job is this event's producer. Its final owner dropping also
            // closes the channel, so executor loss cannot leave this wait open.
            publication
                .await
                .expect("snapshot ended without publishing");
            assert!(matches!(
                follow.next().await,
                Some(Ok(crate::FollowUpdate::Record(_)))
            ));
            drop(follow);
        } else {
            drop(follow);
            drop(held);
        }
        assert_eq!(trace.retired.load(Ordering::SeqCst), 2);
        // A destructor records the violation; report only after all owners
        // retire so the negative control cannot poison cleanup while unwinding.
        assert!(
            !trace.under_lock.load(Ordering::SeqCst),
            "wake must run outside the publication lock"
        );
    }
}

#[tokio::test]
async fn publication_wakers_can_cancel_other_follows() {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use std::task::Wake;

    struct CancelFollow {
        index: Arc<crate::query_sync::SyncMutex<crate::follow_index::FollowIndex>>,
        victim: Mutex<Option<crate::FollowStream>>,
        wakes: AtomicUsize,
    }
    impl Wake for CancelFollow {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            // Fail before taking the registration's lock: the original source
            // would deadlock while synchronously dropping this real follow.
            assert!(
                self.index.try_lock().is_ok(),
                "follow wake retained the index lock"
            );
            let victim = self.victim.lock().unwrap().take();
            drop(victim);
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    for existing in [false, true] {
        let provider = MemoryStream::default();
        let path = StreamPath::new("cancel-during-publication").unwrap();
        let request = || AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"record").into()],
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        };
        if existing {
            provider.append(request()).await.unwrap();
        }
        let mut request_follow = crate::FollowRequest::new(path.clone(), u64::from(existing));
        request_follow.when_absent = crate::WhenAbsent::Wait;
        let victim = provider.follow(request_follow.clone()).await.unwrap();
        let mut followed = provider.follow(request_follow).await.unwrap();
        let index = provider
            .state
            .read()
            .await
            .signals
            .get(&path)
            .unwrap()
            .unwrap()
            .follows;
        let observer = Arc::new(CancelFollow {
            index: Arc::clone(&index),
            victim: Mutex::new(Some(victim)),
            wakes: AtomicUsize::new(0),
        });
        let wake = Waker::from(Arc::clone(&observer));
        assert!(
            std::pin::Pin::new(&mut followed)
                .poll_next(&mut Context::from_waker(&wake))
                .is_pending()
        );
        assert_eq!(observer.wakes.load(Ordering::SeqCst), 0);
        provider.append(request()).await.unwrap();
        assert!(
            observer.wakes.load(Ordering::SeqCst) > 0,
            "publication left its follow parked"
        );
        assert!(observer.victim.lock().unwrap().is_none());
        assert!(
            matches!(std::pin::Pin::new(&mut followed).poll_next(&mut Context::from_waker(Waker::noop())),
            std::task::Poll::Ready(Some(Ok(crate::FollowUpdate::Record(record)))) if record.sequence == u64::from(existing))
        );
        drop(followed);
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
