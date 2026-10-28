use super::*;
use futures::TryFutureExt as _;
use std::{
    sync::{
        Barrier,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Waker},
};

struct Clock {
    armed: AtomicBool,
    started: Barrier,
    release: Barrier,
}
impl UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.started.wait();
            self.release.wait();
        }
        42
    }
}

fn append(provider: &MemoryStream) -> crate::AppendReceipt {
    let AppendOutcome::Committed(receipt) =
        futures::executor::block_on(provider.append(AppendRequest {
            path: StreamPath::new("watched").unwrap(),
            records: vec![(Bytes::from_static(b"x")).into()],
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        }))
        .unwrap()
    else {
        unreachable!()
    };
    receipt
}

// Public-API reproduction from review949/runtime_shutdown_probe.rs, extended to
// checked records and children and workers cancelled before/after their first poll.
#[test]
fn pending_stream_survives_its_polling_runtime() {
    for (_children, checked) in [(false, false), (false, true)] {
        for poll_worker in [false, true] {
            let clock = Arc::new(Clock {
                armed: AtomicBool::new(false),
                started: Barrier::new(2),
                release: Barrier::new(2),
            });
            let provider = MemoryStream::new_with_clock(MemoryLimits::default(), clock.clone());
            let seed = append(&provider);
            let path = StreamPath::new("watched").unwrap();
            let mut follow = {
                let follow = if checked {
                    futures::executor::block_on(
                        provider
                            .follow(crate::FollowRequest {
                                when_absent: Default::default(),
                                path: path,
                                from: 1,
                                preceding: Some(seed.commit_id),
                                filter: crate::FollowFilter::default(),
                                progress: false,
                                at_least: Default::default(),
                            })
                            .map_ok(crate::records_only),
                    )
                } else {
                    futures::executor::block_on(
                        provider
                            .follow(crate::FollowRequest::new(path, 0))
                            .map_ok(crate::records_only),
                    )
                }
                .unwrap();
                follow
                    .map(move |record| {
                        record.map(|record| {
                            assert_eq!(record.sequence, u64::from(checked));
                            assert_eq!(record.value.as_slice(), b"x");
                        })
                    })
                    .boxed()
            };
            clock.armed.store(true, Ordering::SeqCst);
            let writer = std::thread::spawn({
                let provider = provider.clone();
                move || append(&provider)
            });
            clock.started.wait();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            runtime.block_on(async {
                assert!(
                    follow
                        .as_mut()
                        .poll_next(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                if poll_worker {
                    tokio::task::yield_now().await;
                }
            });
            clock.release.wait();
            writer.join().unwrap();
            drop(runtime);
            assert_eq!(futures::executor::block_on(follow.next()), Some(Ok(())));
            append(&provider);
        }
    }
}
