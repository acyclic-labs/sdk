use super::*;

fn reentrant_waker_cleanup(entry: u8) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Reenter {
        executor: std::sync::Weak<Executor>,
        dropped: Arc<AtomicUsize>,
    }
    impl Wake for Reenter {
        fn wake(self: Arc<Self>) {}
    }
    impl Drop for Reenter {
        fn drop(&mut self) {
            let executor = self.executor.upgrade().unwrap();
            // Fail finitely in the counterfactual before actually reentering.
            assert!(
                executor.0.try_lock().is_ok(),
                "waker cleanup held executor lock"
            );
            executor.trace();
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
    let executor = Executor::new(71);
    let root = executor.register(None);
    let dropped = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(Reenter {
        executor: Arc::downgrade(&executor),
        dropped: dropped.clone(),
    }));
    executor.next(
        &PollContext::from_waker(&waker),
        (entry != 2).then_some(root.id),
    );
    drop(waker);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    if entry == 0 {
        executor.retire(root.id);
    } else {
        assert!(
            executor
                .next(
                    &PollContext::from_waker(Waker::noop()),
                    (entry == 1).then_some(root.id)
                )
                .is_none()
        );
    }
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    executor.shutdown();
}

#[test]
fn root_retirement_releases_waker_outside_executor_lock() {
    reentrant_waker_cleanup(0);
}

#[test]
fn root_replacement_releases_waker_outside_executor_lock() {
    reentrant_waker_cleanup(1);
}

#[test]
fn drain_replacement_releases_waker_outside_executor_lock() {
    reentrant_waker_cleanup(2);
}

struct PendingDrop {
    dropped: Arc<std::sync::atomic::AtomicBool>,
    polled: Arc<std::sync::atomic::AtomicBool>,
    wake: Option<Waker>,
}

impl Future for PendingDrop {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut PollContext<'_>) -> Poll<()> {
        self.polled.store(true, std::sync::atomic::Ordering::SeqCst);
        self.wake = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for PendingDrop {
    fn drop(&mut self) {
        self.dropped
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

fn abandoned_scope(polled: bool, panic: bool) -> io::Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let (schedule, mut events) = Schedule::seeded(17);
    let other_controller = schedule.clone();
    let weak = Arc::downgrade(&schedule.0);
    let dropped = Arc::new(AtomicBool::new(false));
    let was_polled = Arc::new(AtomicBool::new(false));
    let mut handle = None;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime.block_on(schedule.scope(async {
            handle = Some(crate::testing::spawn(PendingDrop {
                dropped: dropped.clone(),
                polled: was_polled.clone(),
                wake: None,
            }));
            assert!(!panic, "abandoned replay root");
        }));
    }));
    assert_eq!(result.is_err(), panic);
    if polled {
        let executor = schedule.0.executor.as_ref().unwrap();
        let cx = PollContext::from_waker(Waker::noop());
        executor.poll_owned(&cx);
        events.try_recv().unwrap().release();
        executor.poll_owned(&cx);
        assert!(was_polled.load(Ordering::SeqCst));
    }
    let handle = handle.unwrap();
    let abort = handle.abort_handle();
    drop(events);
    drop(schedule);
    assert!(
        !dropped.load(Ordering::SeqCst),
        "a live controller lost its work"
    );
    drop(other_controller);
    assert!(
        dropped.load(Ordering::SeqCst),
        "abandoned task retained its resources"
    );
    assert!(
        weak.upgrade().is_none(),
        "task context retained its schedule"
    );
    abort.abort();
    let error = runtime.block_on(handle).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    Ok(())
}

#[test]
fn abandoned_unpolled_task_releases_its_context_and_resources() -> io::Result<()> {
    abandoned_scope(false, false)
}

#[test]
fn abandoned_pending_task_releases_its_stored_waker_and_resources() -> io::Result<()> {
    abandoned_scope(true, false)
}

#[test]
fn panicking_replay_releases_unfinished_tasks_after_controller_drop() -> io::Result<()> {
    abandoned_scope(true, true)
}

async fn yield_once() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

fn script(seed: u64) -> io::Result<Vec<(u64, &'static str)>> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let executor = Executor::new(seed);
    runtime.block_on(executor.run(async {
        let tasks: Vec<_> = (0..4)
            .map(|id| {
                executor.spawn(async move {
                    yield_once().await;
                    id
                })
            })
            .collect();
        for (expected, task) in tasks.into_iter().enumerate() {
            assert_eq!(task.await.unwrap(), expected);
        }
    }));
    assert!(lock(&executor.0).live.is_empty());
    Ok(executor.trace())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn same_seed_replays_every_original_task_poll(seed in proptest::prelude::any::<u64>()) {
        proptest::prop_assert_eq!(script(seed).unwrap(), script(seed).unwrap());
    }

    #[test]
    fn every_continuously_ready_task_runs_in_each_round(seed in proptest::prelude::any::<u64>()) {
        fair_rounds(seed, false);
    }
}

fn fair_rounds(seed: u64, forget_round: bool) {
    let executor = Executor::new(seed);
    lock(&executor.0).forget_round = forget_round;
    let tasks: Vec<_> = (0..4)
        .map(|_| executor.register(Some(Box::pin(std::future::pending()))))
        .collect();
    let driver = Waker::noop();
    let cx = PollContext::from_waker(driver);
    for _ in 0..8 {
        let mut seen = BTreeSet::new();
        for _ in 0..tasks.len() {
            let (id, work, cancelled) = executor.next(&cx, None).unwrap();
            assert!(work.is_some() && !cancelled);
            assert!(seen.insert(id), "ready task starved within a round");
            executor.polled(id, work, "pending");
            executor.ready(id);
        }
        assert_eq!(seen.len(), tasks.len());
    }
    for task in tasks {
        executor.retire(task.id);
    }
}

#[test]
fn concurrent_scopes_keep_their_roots_and_wakes() -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()?;
    runtime.block_on(async {
        tokio::time::pause();
        for seed in 0..32 {
            let (schedule, _) = Schedule::seeded(seed);
            let (left_send, left_receive) = oneshot::channel();
            let (right_send, right_receive) = oneshot::channel();
            let left_schedule = schedule.clone();
            let left = tokio::spawn(async move {
                left_schedule
                    .scope(async {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                        left_send.send(13).unwrap();
                        right_receive.await.unwrap()
                    })
                    .await
            });
            let right_schedule = schedule.clone();
            let right = tokio::spawn(async move {
                right_schedule
                    .scope(async {
                        assert_eq!(left_receive.await.unwrap(), 13);
                        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                        right_send.send(37).unwrap();
                        53
                    })
                    .await
            });
            // Separate Tokio tasks supply different caller wakers. The channel
            // exchange requires concurrent progress; serializing scopes hangs.
            let (left, right) = tokio::time::timeout(std::time::Duration::from_secs(1), async {
                tokio::join!(left, right)
            })
            .await
            .expect("concurrent scope lost a root wake");
            assert_eq!(left.unwrap(), 37);
            assert_eq!(right.unwrap(), 53);
            let executor = schedule.0.executor.as_ref().unwrap();
            let state = lock(&executor.0);
            assert!(state.live.is_empty() && state.ready.is_empty());
        }
    });
    Ok(())
}

#[test]
fn borrowed_concurrent_scopes_and_nested_reentry_complete() -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()?;
    runtime.block_on(async {
        tokio::time::pause();
        for seed in 0..32 {
            let (schedule, _) = Schedule::seeded(seed);
            let mut left = 0;
            let mut right = 0;
            let work = async {
                tokio::join!(
                    schedule.scope(async {
                        schedule
                            .scope(async {
                                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                                left = 13;
                            })
                            .await;
                    }),
                    schedule.scope(async {
                        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                        right = 37;
                    }),
                );
            };
            tokio::time::timeout(std::time::Duration::from_secs(1), work)
                .await
                .expect("borrowed scope or nested reentry lost progress");
            assert_eq!((left, right), (13, 37));
            let executor = schedule.0.executor.as_ref().unwrap();
            assert!(lock(&executor.0).live.is_empty());
        }
    });
    Ok(())
}

#[test]
fn draining_inside_a_root_keeps_the_external_driver() -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let executor = Executor::new(71);
    runtime.block_on(executor.run(async {
        let task = executor.spawn(async {
            yield_once().await;
            37
        });
        let mut task = std::pin::pin!(task);
        let value = std::future::poll_fn(|cx| {
            executor.poll_owned(cx);
            task.as_mut().poll(cx)
        })
        .await
        .unwrap();
        assert_eq!(value, 37);
    }));
    assert!(lock(&executor.0).live.is_empty());
    Ok(())
}

#[test]
#[should_panic(expected = "ready task starved within a round")]
fn negative_control_removing_rounds_allows_a_ready_task_to_starve() {
    fair_rounds(0, true);
}

#[test]
fn different_seeds_explore_different_ready_orders() -> io::Result<()> {
    let traces: BTreeSet<_> = (0..8).map(script).collect::<io::Result<_>>()?;
    assert!(traces.len() > 1, "seed never changed the polling order");
    Ok(())
}

#[test]
fn dropped_handles_detach_and_aborted_futures_retire() -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let executor = Executor::new(73);
    runtime.block_on(executor.run(async {
        let (finished, waiting) = oneshot::channel();
        drop(executor.spawn(async move {
            yield_once().await;
            finished.send(91).unwrap();
        }));
        assert_eq!(waiting.await.unwrap(), 91);
        let task = executor.spawn(std::future::pending::<()>());
        task.abort();
        assert_eq!(task.await.unwrap_err().to_string(), "owned task cancelled");
    }));
    let state = lock(&executor.0);
    assert!(state.tasks.is_empty() && state.ready.is_empty() && state.live.is_empty());
    Ok(())
}

#[test]
fn owned_panics_keep_their_cause_and_retire() -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let executor = Executor::new(11);
    runtime.block_on(executor.run(async {
        let task = executor.spawn(async { panic!("original task cause") });
        let error = task.await.unwrap_err().to_string();
        assert_eq!(error, "owned task panicked: original task cause");
    }));
    assert!(lock(&executor.0).live.is_empty());
    Ok(())
}

#[test]
fn cancellation_of_the_root_keeps_owned_work_drivable() -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let executor = Executor::new(11);
    runtime.block_on(async {
        let (finished, mut waiting) = oneshot::channel();
        let mut root = Box::pin(executor.run(async {
            drop(executor.spawn(async {
                yield_once().await;
                finished.send(57).unwrap();
            }));
            std::future::pending::<()>().await
        }));
        // Poll the root until its real owned submission has been accepted.
        std::future::poll_fn(|cx| {
            assert!(root.as_mut().poll(cx).is_pending());
            if lock(&executor.0).next > 1 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        drop(root);
        let result = std::future::poll_fn(|cx| {
            executor.poll_owned(cx);
            Pin::new(&mut waiting).poll(cx)
        })
        .await
        .unwrap();
        assert_eq!(result, 57);
    });
    assert!(lock(&executor.0).live.is_empty());
    Ok(())
}
