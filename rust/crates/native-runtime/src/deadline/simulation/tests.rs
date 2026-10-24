use super::*;

async fn lazy_native_job(broken: bool) -> std::io::Result<()> {
    let clock = Clock::new();
    clock.0.state.lock().unwrap().native_runnable = broken;
    let unpolled = clock
        .enter(|| crate::run_blocking_io(|| panic!("an unpolled native job executed"), None))?;
    let timer = clock.register();
    timer.block(
        Some(Deadline::at(clock.now() + Duration::from_secs(3))),
        None,
        None,
    );
    assert!(
        clock.advance_if_idle(),
        "unsubmitted native work prevented a bounded wait expiring"
    );
    drop(timer);
    clock
        .scope(async {
            super::super::sleep(Duration::from_secs(2));
            drop(unpolled);
        })
        .await;
    assert_eq!(
        clock.advances(),
        [Duration::from_secs(3), Duration::from_secs(5)]
    );
    assert!(clock.0.state.lock().unwrap().actors.is_empty());
    Ok(())
}
#[tokio::test(flavor = "current_thread")]
async fn unpolled_native_futures_do_not_occupy_a_worker_or_prevent_deadlines() -> std::io::Result<()>
{
    lazy_native_job(false).await
}
#[tokio::test(flavor = "current_thread")]
#[should_panic(expected = "unsubmitted native work prevented a bounded wait expiring")]
async fn counting_unsubmitted_native_work_as_runnable_fails_the_same_oracle() {
    lazy_native_job(true).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn nested_scoped_futures_park_their_synchronous_parent() {
    let clock = Clock::new();
    clock
        .scope(async {
            clock
                .scope(async { super::super::sleep(Duration::from_secs(3)) })
                .await;
        })
        .await;
    assert_eq!(clock.advances(), [Duration::from_secs(3)]);
    assert!(clock.0.state.lock().unwrap().actors.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn original_worker_pool_expires_virtual_waits_before_running_queued_jobs()
-> std::io::Result<()> {
    if crate::tests::passed_in_exclusive_process(
        "deadline::simulation::tests::original_worker_pool_expires_virtual_waits_before_running_queued_jobs",
    )? {
        return Ok(());
    }
    let capacity = crate::workers(&crate::BLOCKING_WORKERS, false)?.exclusive_threads();
    let clock = Clock::new();
    clock
        .scope(async {
            let mut tasks = Vec::new();
            for _ in 0..=capacity {
                tasks.push(Some(crate::run_blocking_io(
                    || {
                        super::super::sleep(Duration::from_secs(5));
                    },
                    None,
                )?));
            }
            std::future::poll_fn(|cx| {
                let mut pending = false;
                for task in &mut tasks {
                    if let Some(future) = task {
                        match Pin::new(future).poll(cx) {
                            Poll::Ready(Ok(())) => {
                                task.take();
                            }
                            Poll::Ready(Err(cause)) => return Poll::Ready(Err(cause)),
                            Poll::Pending => pending = true,
                        }
                    }
                }
                if pending {
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(()))
                }
            })
            .await
        })
        .await?;
    assert_eq!(
        clock.advances(),
        [Duration::from_secs(5), Duration::from_secs(10)]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn captured_callbacks_park_and_read_the_same_clock_after_scope_exit() {
    let origin = Instant::now();
    let mut traces = Vec::new();
    for skew in [Duration::ZERO, Duration::from_secs(1_000_000)] {
        let clock = Clock::at(origin + skew);
        let captured = clock.scope(async { Clock::capture().unwrap() }).await;
        let elapsed = std::thread::spawn(move || {
            captured.enter(|| {
                let start = super::super::now();
                // A second entry must reuse this actor rather than leave a
                // runnable outer actor that prevents its own timer expiring.
                captured.enter(|| super::super::sleep(Duration::from_secs(60)));
                super::super::now().duration_since(start)
            })
        })
        .join()
        .unwrap();
        assert_eq!(elapsed, Duration::from_secs(60));
        assert!(clock.0.state.lock().unwrap().actors.is_empty());
        traces.push(clock.advances());
    }
    assert_eq!(traces[0], [Duration::from_secs(60)]);
    assert_eq!(traces[0], traces[1]);
}

#[tokio::test(flavor = "current_thread")]
async fn original_native_workers_park_on_virtual_deadlines_under_host_skew() -> std::io::Result<()>
{
    let origin = Instant::now();
    let mut traces = Vec::new();
    for skew in [Duration::ZERO, Duration::from_secs(1_000_000)] {
        let clock = Clock::at(origin + skew);
        let started = clock.now();
        let result = clock
            .scope(async {
                crate::run_blocking_io(
                    || {
                        super::super::sleep(Duration::from_secs(5));
                        super::super::sleep(Duration::from_secs(2));
                        super::super::now()
                    },
                    None,
                )?
                .await
            })
            .await?;
        assert_eq!(result.duration_since(started), Duration::from_secs(7));
        traces.push(clock.advances());
    }
    assert_eq!(traces[0], traces[1]);
    assert_eq!(traces[0], [Duration::from_secs(5), Duration::from_secs(7)]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn original_child_tasks_inherit_clock_and_retain_their_original_task_wake()
-> std::io::Result<()> {
    let clock = Clock::new();
    clock
        .scope(async {
            crate::testing::spawn(async {
                crate::run_blocking_io(|| super::super::sleep(Duration::from_secs(30)), None)?.await
            })
            .await
            .map_err(std::io::Error::other)?
        })
        .await?;
    assert_eq!(clock.advances(), [Duration::from_secs(30)]);
    Ok(())
}

fn cannot_advance_over_a_runnable_actor(broken: bool) {
    let clock = Clock::new();
    let blocked = clock.register();
    let _running = clock.register();
    blocked.block(Some(Deadline::after(Duration::from_secs(1))), None, None);
    clock.0.state.lock().unwrap().advance_while_running = broken;
    assert!(
        !clock.advance_if_idle(),
        "virtual time advanced over a runnable actor"
    );
    assert!(clock.advances().is_empty());
}
#[test]
fn accepted_work_prevents_premature_time_advancement() {
    cannot_advance_over_a_runnable_actor(false);
}
#[test]
#[should_panic(expected = "virtual time advanced over a runnable actor")]
fn ignoring_runnable_actors_is_caught_by_the_same_oracle() {
    cannot_advance_over_a_runnable_actor(true);
}

#[test]
fn earliest_deadline_wins_and_notifications_are_runnable_before_advancement() {
    let clock = Clock::new();
    let a = clock.register();
    let b = clock.register();
    let origin = clock.now();
    a.block(
        Some(Deadline::at(origin + Duration::from_secs(2))),
        None,
        None,
    );
    b.block(
        Some(Deadline::at(origin + Duration::from_secs(5))),
        None,
        None,
    );
    assert!(clock.advance_if_idle());
    assert_eq!(clock.now(), origin + Duration::from_secs(2));
    assert!(!clock.advance_if_idle());
    a.block(None, None, None);
    assert!(clock.advance_if_idle());
    assert_eq!(
        clock.advances(),
        [Duration::from_secs(2), Duration::from_secs(5)]
    );
    a.resume();
    assert!(!clock.advance_if_idle());
}

#[tokio::test(flavor = "current_thread")]
async fn synchronous_wait_can_advance_when_the_driver_itself_is_parked() {
    let clock = Clock::new();
    clock
        .scope(async {
            super::super::sleep(Duration::from_secs(60));
        })
        .await;
    assert_eq!(clock.advances(), [Duration::from_secs(60)]);
}

#[tokio::test(flavor = "current_thread")]
async fn retained_notification_does_not_consume_virtual_time() {
    let clock = Clock::new();
    let waiter = Arc::new(Waiter::new());
    clock
        .scope(async {
            waiter.notify();
            waiter.wait(Some(Deadline::after(Duration::from_secs(60))));
        })
        .await;
    assert!(clock.advances().is_empty());
}

async fn host_skew_oracle(broken: bool) {
    let origin = Instant::now();
    let mut trace = Vec::new();
    for skew in [Duration::ZERO, Duration::from_secs(1_000_000)] {
        let clock = Clock::at(origin + skew);
        clock.0.state.lock().unwrap().host_clock = broken;
        let observed = clock.scope(async { super::super::now() }).await;
        trace.push(observed.checked_duration_since(clock.now()));
    }
    assert_eq!(
        trace,
        [Some(Duration::ZERO); 2],
        "host clock leaked into seeded trace"
    );
}
#[tokio::test(flavor = "current_thread")]
async fn host_skew_cannot_change_the_monotonic_trace() {
    host_skew_oracle(false).await;
}
#[tokio::test(flavor = "current_thread")]
#[should_panic(expected = "host clock leaked into seeded trace")]
async fn restoring_the_host_clock_fails_the_same_oracle() {
    host_skew_oracle(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn original_file_completions_register_until_the_physical_terminal() -> std::io::Result<()> {
    let temporary = tempfile::NamedTempFile::new()?;
    std::fs::write(temporary.path(), b"virtual read")?;
    let file = std::fs::File::open(temporary.path())?;
    let clock = Clock::new();
    let result = clock
        .scope(async {
            crate::read_batch_async(
                file,
                vec![crate::OwnedRead {
                    offset: 0,
                    length: 12,
                }],
            )
            .await
        })
        .await?;
    assert_eq!(result[0].as_ref(), b"virtual read");
    assert!(clock.advances().is_empty());
    assert!(clock.0.state.lock().unwrap().actors.is_empty());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn original_spawn_blocking_inherits_the_clock_before_submission() -> std::io::Result<()> {
    let clock = Clock::new();
    clock
        .scope(async {
            crate::testing::spawn_blocking(|| super::super::sleep(Duration::from_secs(17))).await
        })
        .await?;
    assert_eq!(clock.advances(), [Duration::from_secs(17)]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retaining_a_completed_futures_waker_does_not_keep_an_actor_runnable() {
    let clock = Clock::new();
    let mut retained = None;
    clock
        .scope(std::future::poll_fn(|cx| {
            retained = Some(cx.waker().clone());
            Poll::Ready(())
        }))
        .await;
    assert!(clock.0.state.lock().unwrap().actors.is_empty());
    clock
        .scope(async {
            super::super::sleep(Duration::from_secs(3));
        })
        .await;
    assert_eq!(clock.advances(), [Duration::from_secs(3)]);
    retained.unwrap().wake();
    assert!(clock.0.state.lock().unwrap().actors.is_empty());
}
