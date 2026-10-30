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

#[test]
fn pending_commit_reports_lost_executor_and_retains_exact_recovery() {
    use futures::FutureExt as _;

    let provider = Arc::new(MemoryStream::default());
    let path = StreamPath::new("pending-recovery").unwrap();
    let request = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Absent { path: path.clone() }],
        mutations: vec![CommitMutation::Append {
            path: path.clone(),
            records: vec!["original".into()],
        }],
        idempotency_key: IdempotencyKey::unique(),
    };
    // The original publication owner prevents this submission from accepting.
    // No substitute provider or task-completion producer is involved.
    let publication = futures::executor::block_on(provider.publication.lock());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let pending = {
        let _entered = runtime.enter();
        crate::Pending::submit(
            Arc::clone(&provider),
            request.clone(),
            &crate::runtime::PlatformExecutor,
        )
    };
    assert!(pending.wait().now_or_never().is_none());
    assert_eq!(
        futures::executor::block_on(provider.head(path.clone())),
        Err(StreamError::NotFound)
    );
    drop(runtime);
    assert_eq!(
        pending.wait().now_or_never(),
        Some(Err(StreamError::Unavailable)),
        "losing the original task producer must settle every waiter"
    );
    assert_eq!(
        pending.clone().now_or_never(),
        Some(Err(StreamError::Unavailable))
    );
    assert_eq!(pending.request(), &request);
    drop(publication);
    assert_eq!(
        futures::executor::block_on(provider.head(path)),
        Err(StreamError::NotFound),
        "the cancelled task must not publish after its owner is released"
    );
    let outcome = futures::executor::block_on(provider.commit(pending.request().clone())).unwrap();
    assert!(matches!(outcome, CommitOutcome::Committed(_)));

    // Negative control: an already completed exact replay survives shutdown;
    // executor loss cannot replace its authentic result with Unavailable.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let completed = {
        let _entered = runtime.enter();
        crate::Pending::submit(provider, request, &crate::runtime::PlatformExecutor)
    };
    drop(runtime);
    assert_eq!(completed.now_or_never(), Some(Ok(outcome)));
}
