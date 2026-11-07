use super::*;
use futures::Stream as _;
use std::task::{Context, Waker};

#[test]
fn executor_loss_ends_public_follows_before_a_retained_writer_releases() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Observer(AtomicUsize);
    impl std::task::Wake for Observer {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    for (checked, progress) in [(false, false), (true, false), (false, true), (true, true)] {
        for queued in [false, true] {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let provider = MemoryStream::default();
            let path = StreamPath::new("retired-snapshot").unwrap();
            let AppendOutcome::Committed(seed) = runtime
                .block_on(provider.append(AppendRequest {
                    path: path.clone(),
                    records: vec![Bytes::from_static(b"seed").into()],
                    if_tail: None,
                    idempotency_key: crate::IdempotencyKey::unique(),
                }))
                .unwrap()
            else {
                unreachable!()
            };
            let mut request = crate::FollowRequest::new(path, u64::from(checked));
            request.preceding = checked.then_some(seed.commit_id);
            request.progress = progress;
            let mut follow = runtime.block_on(provider.follow(request)).unwrap();
            let held = runtime.block_on(provider.state.write());
            let observer = Arc::new(Observer(AtomicUsize::new(0)));
            let wake = Waker::from(Arc::clone(&observer));
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
            runtime
                .block_on(settled)
                .expect("original worker retired before its observed cut");
            drop(runtime);
            assert!(
                observer.0.load(Ordering::SeqCst) > 0,
                "lost snapshot producer did not wake its public follow"
            );
            // Even an available replacement executor cannot supply the lost
            // read while the original writer remains held.
            let replacement = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let _entered = replacement.enter();
            assert!(
                matches!(
                    std::pin::Pin::new(&mut follow)
                        .poll_next(&mut Context::from_waker(Waker::noop())),
                    std::task::Poll::Ready(Some(Err(StreamError::Unavailable)))
                ),
                "lost snapshot producer left its public follow parked"
            );
            drop(follow);
            drop(held);
            assert!(
                provider.state.try_write().is_ok(),
                "retired worker retained its reservation"
            );
        }
    }
}
