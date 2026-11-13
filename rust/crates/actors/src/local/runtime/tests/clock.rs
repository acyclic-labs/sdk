use super::*;
use std::task::Wake;

#[derive(Default)]
struct Notifications(AtomicUsize);

impl Wake for Notifications {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Advance the SAME clock used by the continuous runtime, not a direct drain
/// or a guest timer. Refused time and lost producer keep future alarms unapplied.
#[tokio::test]
async fn controlled_clock_drives_original_alarms_and_reports_producer_loss() -> TestResult {
    for observed_before_advance in [false, true] {
        for detach_waiter in [false, true] {
            for replace_epoch in [false, true] {
                clock_schedule(observed_before_advance, detach_waiter, replace_epoch).await?;
            }
        }
    }
    Ok(())
}

async fn clock_schedule(
    observed_before_advance: bool,
    detach_waiter: bool,
    replace_epoch: bool,
) -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let first = spawn_with(&store, "clock-first", &[b"set 10"]).await?;
    let second = spawn_with(&store, "clock-second", &[b"set 20"]).await?;
    let future = spawn_with(&store, "clock-future", &[b"set 30"]).await?;
    let (clock, mut control) = ControlledClock::new(100);
    let clock = Arc::new(clock);
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::clone(&clock) as Arc<dyn Clock>,
    );
    let notifications = Arc::new(Notifications::default());
    let waker = Waker::from(Arc::clone(&notifications));
    let mut context = Context::from_waker(&waker);
    let conflicts = AtomicUsize::new(0);
    let (reported, mut failures) = futures::channel::mpsc::unbounded();
    let mut serving = Box::pin(runtime.run(|address, error| {
        assert!(
            replace_epoch && address == &first && matches!(error, Error::Conflict),
            "unexpected delivery failure: {error}"
        );
        assert_eq!(conflicts.fetch_add(1, Ordering::SeqCst), 0);
        reported
            .unbounded_send(())
            .expect("failure observer dropped");
    }));

    // A pending dispatcher poll may be a cooperative yield between actors.
    // Observe all three original committed inputs before advancing their clock.
    let armed = observe_while_running(
        serving.as_mut(),
        futures::future::try_join3(
            state_after(&store, &first, |state| state.cursor("inbox") == Some(1)),
            state_after(&store, &second, |state| state.cursor("inbox") == Some(1)),
            state_after(&store, &future, |state| state.cursor("inbox") == Some(1)),
        ),
    )
    .await?;
    for (state, deadline) in [(&armed.0, 110), (&armed.1, 120), (&armed.2, 130)] {
        assert_eq!(state.alarm().deadline_unix_ms, Some(deadline));
        assert_eq!(state.alarm().delivered_generation, None);
    }
    let mut early = clock.sleep_until(110);
    assert!(early.as_mut().poll(&mut context).is_pending());
    notifications.0.store(0, Ordering::SeqCst);
    control.advance_to(109)?;
    assert!(
        notifications.0.load(Ordering::SeqCst) > 0,
        "lost clock wake"
    );
    assert!(early.as_mut().poll(&mut context).is_pending());
    drop(early);
    assert!(futures::poll!(serving.as_mut()).is_pending());
    assert!(records(&*provider, "out/clock-first").await?.is_empty());

    let replacement = if replace_epoch {
        let snapshot = store.load(&first).await?;
        assert_eq!(snapshot.state().map(State::epoch), Some(1));
        Some(store.claim(&snapshot, 2, key("clock-replacement")?).await?)
    } else {
        None
    };
    // Both an installed waiter and a not-yet-observed future retain the same
    // original advance. Neither owns the clock producer.
    let mut reached = clock.sleep_until(110);
    if observed_before_advance {
        assert!(reached.as_mut().poll(&mut context).is_pending());
    }
    control.advance_to(110)?;
    reached.await?;
    if let Some(replacement) = replacement {
        // Placement alone appends no activation. The original due alarm wakes
        // this stale owner; the retained report is the refusal's event.
        observe_while_running(serving.as_mut(), async {
            failures
                .next()
                .await
                .ok_or_else(|| StreamError::Unavailable.into())
        })
        .await?;
        assert_eq!(conflicts.load(Ordering::SeqCst), 1);
        assert!(records(&*provider, "out/clock-first").await?.is_empty());
        let refused = store.load(&first).await?;
        assert_eq!(refused.anchor, replacement.anchor);
        assert_eq!(refused.tail, replacement.tail);
        let state = refused.state().ok_or("missing replacement state")?;
        assert_eq!(state.epoch(), 2);
        assert_eq!(state.cursor("inbox"), Some(1));
        assert_eq!(state.alarm().deadline_unix_ms, Some(110));
        assert_eq!(state.alarm().delivered_generation, None);
        // Only this explicit placement may claim epoch 3 and deliver the
        // still-committed alarm; the stale dispatcher may not publish it.
        runtime.place(first.clone(), 2).await?;
    } else {
        assert_eq!(conflicts.load(Ordering::SeqCst), 0);
    }
    observe_while_running(
        serving.as_mut(),
        state_after(&store, &first, |state| {
            state.alarm().delivered_generation == Some(1)
        }),
    )
    .await?;
    assert_eq!(
        records(&*provider, "out/clock-first").await?,
        vec![Bytes::from_static(b"a1")]
    );
    assert!(records(&*provider, "out/clock-second").await?.is_empty());
    assert_eq!(
        store
            .load(&second)
            .await?
            .state()
            .and_then(|state| state.alarm().pending()),
        Some(120)
    );
    let mut retained = clock.sleep_until(130);
    assert!(retained.as_mut().poll(&mut context).is_pending());
    notifications.0.store(0, Ordering::SeqCst);
    assert!(matches!(control.advance_to(109), Err(ClockError::Reversed)));
    control.advance_to(110)?;
    assert_eq!(clock.now_unix_ms(), 110);
    assert_eq!(
        notifications.0.load(Ordering::SeqCst),
        0,
        "refusal/equal time woke a waiter"
    );

    control.advance_to(120)?;
    observe_while_running(
        serving.as_mut(),
        state_after(&store, &second, |state| {
            state.alarm().delivered_generation == Some(1)
        }),
    )
    .await?;
    assert_eq!(
        records(&*provider, "out/clock-first").await?,
        vec![Bytes::from_static(b"a1")]
    );
    assert_eq!(
        records(&*provider, "out/clock-second").await?,
        vec![Bytes::from_static(b"a1")]
    );
    assert_eq!(
        store
            .load(&future)
            .await?
            .state()
            .and_then(|state| state.alarm().pending()),
        Some(130)
    );
    for address in [&first, &second] {
        let snapshot = store.load(address).await?;
        let state = snapshot.state().ok_or("missing alarm state")?;
        assert_eq!(state.cursor("inbox"), Some(1));
        assert_eq!(state.alarm().delivered_generation, Some(1));
        assert_eq!(
            state.epoch(),
            if replace_epoch && address == &first {
                3
            } else {
                1
            }
        );
    }

    let mut pending = clock.sleep_until(130);
    assert!(pending.as_mut().poll(&mut context).is_pending());
    // Drop only this original subscriber, after registration. The retained
    // serving future and late waiters still depend on the sole controller.
    let pending = if detach_waiter {
        drop(pending);
        None
    } else {
        Some(pending)
    };
    // Consume the 120 advance and rearm this same retained subscriber before
    // checking the producer-close notification.
    assert!(retained.as_mut().poll(&mut context).is_pending());
    notifications.0.store(0, Ordering::SeqCst);
    drop(control);
    assert!(
        notifications.0.load(Ordering::SeqCst) > 0,
        "lost producer-close wake"
    );
    if let Some(pending) = pending {
        assert!(matches!(pending.await, Err(ClockError::ProducerDropped)));
    }
    assert!(matches!(retained.await, Err(ClockError::ProducerDropped)));
    assert!(matches!(
        clock.sleep_until(130).await,
        Err(ClockError::ProducerDropped)
    ));
    clock.sleep_until(120).await?;
    assert!(matches!(
        serving.await,
        Err(Error::Clock(ClockError::ProducerDropped))
    ));
    assert!(records(&*provider, "out/clock-future").await?.is_empty());
    let snapshot = store.load(&future).await?;
    let state = snapshot.state().ok_or("missing future alarm")?;
    assert_eq!(state.cursor("inbox"), Some(1));
    assert_eq!(state.alarm().deadline_unix_ms, Some(130));
    assert_eq!(state.alarm().delivered_generation, None);
    Ok(())
}

#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
#[test]
fn system_clock_keeps_its_original_deadline_after_runtime_retirement() -> TestResult {
    for poll_before_retirement in [true, false] {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?;
        let mut wait = SystemClock.sleep_until(u64::MAX);
        let mut context = Context::from_waker(Waker::noop());
        if poll_before_retirement {
            let entered = runtime.enter();
            assert!(wait.as_mut().poll(&mut context).is_pending());
            drop(entered);
        }
        // The SAME future outlives the original Tokio timer producer. The
        // native absolute alarm owns its event independently of that driver.
        drop(runtime);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            wait.as_mut().poll(&mut context)
        }));
        assert!(
            result.is_ok(),
            "wall clock retained a retired Tokio timer driver"
        );
        assert!(
            result
                .map_err(|_| "wall clock panicked after runtime retirement")?
                .is_pending(),
            "a live native producer must retain the future absolute deadline"
        );
        // Exercise the same original native driver delivering an elapsed
        // deadline with no Tokio runtime, not a virtual time or mock callback.
        futures::executor::block_on(acyclic_native_runtime::WallAlarm::wait(0)?)?;
        futures::executor::block_on(SystemClock.sleep_until(0))?;
        assert!(wait.as_mut().poll(&mut context).is_pending());
    }
    for polled_before_loss in [false, true] {
        let runtime = acyclic_native_runtime::testing::seeded_runtime(71)?;
        let (schedule, _) = acyclic_native_runtime::testing::Schedule::seeded(71);
        let notifications = Arc::new(Notifications::default());
        let waker = Waker::from(notifications.clone());
        let mut wait = runtime.block_on(async {
            tokio::time::pause();
            schedule
                .scope(async {
                    let now = SystemClock.now_unix_ms();
                    let start = tokio::time::Instant::now();
                    let mut jumped = SystemClock.sleep_until(now + 100);
                    let mut context = Context::from_waker(&waker);
                    assert!(jumped.as_mut().poll(&mut context).is_pending());
                    schedule.set_unix_millis(now - 100);
                    assert!(
                        jumped.as_mut().poll(&mut context).is_pending(),
                        "a backwards wall jump completed a future deadline"
                    );
                    schedule.set_unix_millis(now + 100);
                    jumped.await?;
                    assert_eq!(
                        start.elapsed(),
                        std::time::Duration::ZERO,
                        "a forward wall jump retained a relative timeout"
                    );
                    let mut wait = SystemClock.sleep_until(u64::MAX);
                    if polled_before_loss {
                        assert!(wait.as_mut().poll(&mut context).is_pending());
                    }
                    Ok::<_, ClockError>(wait)
                })
                .await
        })?;
        notifications.0.store(0, Ordering::SeqCst);
        drop(schedule);
        if polled_before_loss {
            assert!(
                notifications.0.load(Ordering::SeqCst) > 0,
                "wall clock lost its registered producer-retirement wake"
            );
        }
        let mut context = Context::from_waker(&waker);
        let result = wait.as_mut().poll(&mut context);
        assert!(
            result.is_ready(),
            "wall clock escaped its original producer before first poll"
        );
        let std::task::Poll::Ready(Err(ClockError::Native(error))) = result else {
            panic!("wall clock did not report original producer retirement");
        };
        assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
        assert!(error.to_string().contains("step=wall-alarm-producer"));
    }
    Ok(())
}

/// The real dispatcher must return the original clock failure while both
/// actors retain committed alarms, not rearm an already retired producer.
#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
#[test]
fn running_dispatcher_reports_its_original_clock_retirement() -> TestResult {
    let executor = acyclic_native_runtime::testing::seeded_runtime(72)?;
    let (schedule, _) = acyclic_native_runtime::testing::Schedule::seeded(72);
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(provider.clone(), root()?);
    let runtime = Runtime::new(
        Store::new(provider.clone(), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    let mut serving = Box::pin(runtime.run(|_, error| panic!("delivery failed: {error}")));
    let notifications = Arc::new(Notifications::default());
    let waker = Waker::from(notifications.clone());
    let (first, second) = executor.block_on(async {
        tokio::time::pause();
        schedule
            .scope(async {
                let first = spawn_with(
                    &store,
                    "retired-clock-first",
                    &[b"set 18446744073709551615"],
                )
                .await?;
                let second = spawn_with(
                    &store,
                    "retired-clock-second",
                    &[b"set 18446744073709551615"],
                )
                .await?;
                let armed = observe_while_running(
                    serving.as_mut(),
                    futures::future::try_join(
                        state_after(&store, &first, |state| state.cursor("inbox") == Some(1)),
                        state_after(&store, &second, |state| state.cursor("inbox") == Some(1)),
                    ),
                )
                .await?;
                for state in [&armed.0, &armed.1] {
                    assert!(state.alarm().pending().is_some());
                    assert_eq!(state.alarm().delivered_generation, None);
                }
                let mut context = Context::from_waker(&waker);
                assert!(serving.as_mut().poll(&mut context).is_pending());
                Ok::<_, Box<dyn std::error::Error>>((first, second))
            })
            .await
    })?;
    notifications.0.store(0, Ordering::SeqCst);
    drop(schedule);
    assert!(
        notifications.0.load(Ordering::SeqCst) > 0,
        "original clock retirement did not wake the parked dispatcher"
    );
    let mut context = Context::from_waker(&waker);
    let result = serving.as_mut().poll(&mut context);
    let std::task::Poll::Ready(Err(Error::Clock(ClockError::Native(error)))) = result else {
        panic!("dispatcher did not return its original retired clock failure");
    };
    assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    assert!(error.to_string().contains("step=wall-alarm-producer"));
    // No actor alarm was delivered merely because its clock producer retired.
    executor.block_on(async {
        for address in [first, second] {
            let state = store.load(&address).await?;
            assert_eq!(
                state
                    .state()
                    .and_then(|state| state.alarm().delivered_generation),
                None
            );
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

/// A checkpoint past an alarm's activation must not lose it: the replacement
/// host finds the pending deadline in its shard's window, not by replaying
/// activations or reading actor states.
#[tokio::test]
async fn a_replacement_host_fires_alarms_armed_before_the_checkpoint() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let armed = spawn_with(&store, "bucket-armed", &[b"set 50"]).await?;
    let shard = store.scheduling_shard(&armed);
    let host = |now| {
        let (clock, control) = ControlledClock::new(now);
        let runtime = Runtime::new(
            Store::new(Arc::clone(&provider), root()?),
            Arc::new(Package::new()?),
            Arc::new(clock) as Arc<dyn Clock>,
        )
        .with_scheduling_shards([shard])?;
        Ok::<_, Box<dyn std::error::Error>>((runtime, control))
    };
    let (first, _control) = host(100)?;
    let tail = provider.tail(store.scheduling_path(shard)?).await?;
    let (checkpoint, dispatcher) = first.dispatcher_store(shard)?;
    {
        let mut serving = Box::pin(first.run(|_, error| panic!("delivery failed: {error}")));
        observe_while_running(
            serving.as_mut(),
            state_after(&checkpoint, &dispatcher, |state| {
                state.cursor("activation") == Some(tail)
            }),
        )
        .await?;
    }
    let pending = store.load(&armed).await?;
    assert_eq!(
        pending.state().and_then(|state| state.alarm().pending()),
        Some(150)
    );
    assert!(records(&*provider, "out/bucket-armed").await?.is_empty());

    // The replacement starts a window later, so the watermark may pass it.
    let (replacement, _control) = host(60_000)?;
    let mut serving = Box::pin(replacement.run(|_, error| panic!("delivery failed: {error}")));
    observe_while_running(
        serving.as_mut(),
        state_after(&store, &armed, |state| {
            state.alarm().delivered_generation == Some(1)
        }),
    )
    .await?;
    assert_eq!(
        records(&*provider, "out/bucket-armed").await?,
        vec![Bytes::from_static(b"a1")]
    );
    // The delivered, closed window moves the watermark, so a later restart
    // pages past it instead of waking the actor again.
    observe_while_running(
        serving.as_mut(),
        state_after(&checkpoint, &dispatcher, |state| {
            state.cursor("deadline") == Some(1)
        }),
    )
    .await?;
    Ok(())
}

struct Authority(AtomicU64);

impl acyclic_stream::UnixMillisClock for Authority {
    fn now_unix_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Reports each alarm consumption the authority decides, with its time.
struct Decisions(
    futures::channel::mpsc::UnboundedSender<(u64, u64)>,
    Arc<Authority>,
);

impl acyclic_stream::CommitAdmission for Decisions {
    fn not_before(&self, value: &[u8]) -> Option<u64> {
        let deadline = crate::local::alarm_admission().not_before(value)?;
        let _ = self
            .0
            .unbounded_send((deadline, self.1.0.load(Ordering::SeqCst)));
        Some(deadline)
    }
}

/// A host clock ahead of the authority cannot consume an alarm early: the
/// authority refuses at its own commit time, and the host waits out exactly
/// the remaining authority time on its own clock before the next attempt.
#[tokio::test]
async fn the_authority_refuses_early_alarm_consumption_and_the_host_rearms() -> TestResult {
    let authority = Arc::new(Authority(AtomicU64::new(100)));
    let provider = Arc::new(MemoryStream::new_with_clock(
        Default::default(),
        Arc::clone(&authority) as _,
    ));
    let (decided, mut decisions) = futures::channel::mpsc::unbounded();
    provider.install_commit_admission(Arc::new(Decisions(decided, Arc::clone(&authority))))?;
    let store = Store::new(Arc::clone(&provider), root()?);
    let early = spawn_with(&store, "authority-early", &[b"set 50"]).await?;
    let (clock, mut control) = ControlledClock::new(100);
    let clock = Arc::new(clock);
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::clone(&clock) as Arc<dyn Clock>,
    );
    let mut serving = Box::pin(runtime.run(|_, error| panic!("delivery failed: {error}")));
    let armed = observe_while_running(
        serving.as_mut(),
        state_after(&store, &early, |state| state.cursor("inbox") == Some(1)),
    )
    .await?;
    assert_eq!(armed.alarm().pending(), Some(150));
    // The host clock passes the deadline; the authority's is 30 ms short.
    authority.0.store(120, Ordering::SeqCst);
    control.advance_to(160)?;
    let refused = observe_while_running(serving.as_mut(), async {
        decisions.next().await.ok_or_else(|| "no decision".into())
    })
    .await?;
    assert_eq!(refused, (150, 120));
    assert!(records(&*provider, "out/authority-early").await?.is_empty());
    // The authority's time now suffices, but the host waits its own 30 ms.
    authority.0.store(150, Ordering::SeqCst);
    control.advance_to(190)?;
    observe_while_running(
        serving.as_mut(),
        state_after(&store, &early, |state| {
            state.alarm().delivered_generation == Some(1)
        }),
    )
    .await?;
    // No attempt between the refusal and the host's re-armed time.
    assert_eq!(decisions.try_recv()?, (150, 150));
    assert_eq!(
        records(&*provider, "out/authority-early").await?,
        vec![Bytes::from_static(b"a1")]
    );
    Ok(())
}

/// A deferred wake never moves the deadline watermark past the alarm's own
/// window. Window two holds a deadline at 150000; a replacement host at
/// 240000 opens it, the authority at 120000 refuses, and the wake is deferred
/// to 270000. The checkpoint stays at window two, so a host started after a
/// crash still finds and fires the alarm.
#[tokio::test]
async fn a_deferred_alarm_survives_a_crash_after_the_checkpoint() -> TestResult {
    let authority = Arc::new(Authority(AtomicU64::new(100_000)));
    let provider = Arc::new(MemoryStream::new_with_clock(
        Default::default(),
        Arc::clone(&authority) as _,
    ));
    let (decided, mut decisions) = futures::channel::mpsc::unbounded();
    provider.install_commit_admission(Arc::new(Decisions(decided, Arc::clone(&authority))))?;
    let store = Store::new(Arc::clone(&provider), root()?)
        .with_scheduling_shards(std::num::NonZeroU16::MIN);
    let actor = spawn_with(&store, "deferred", &[b"set 50000"]).await?;
    let host = |now| {
        let (clock, control) = ControlledClock::new(now);
        let runtime = Runtime::new(
            store.clone(),
            Arc::new(Package::new()?),
            Arc::new(clock) as Arc<dyn Clock>,
        );
        Ok::<_, Box<dyn std::error::Error>>((runtime, control))
    };
    let (earlier, _control) = host(100_000)?;
    earlier.track(actor.clone())?;
    earlier.drain_actor(&actor).await?;
    let armed = store.load(&actor).await?;
    assert_eq!(
        armed.state().and_then(|state| state.alarm().pending()),
        Some(150_000)
    );
    authority.0.store(120_000, Ordering::SeqCst);
    let tail = provider.tail(store.scheduling_path(0)?).await?;
    let (replacement, _control) = host(240_000)?;
    let (checkpoint, dispatcher) = replacement.dispatcher_store(0)?;
    {
        let mut serving = Box::pin(replacement.run(|_, error| panic!("delivery failed: {error}")));
        // Only a quiescent host past window two's consumption checkpoints a
        // deadline cursor of two or more, so this is the refusal's checkpoint.
        let checkpointed = observe_while_running(
            serving.as_mut(),
            state_after(&checkpoint, &dispatcher, |state| {
                state.cursor("activation") == Some(tail) && state.cursor("deadline") >= Some(2)
            }),
        )
        .await?;
        assert_eq!(decisions.try_recv()?, (150_000, 120_000));
        assert_eq!(checkpointed.cursor("deadline"), Some(2));
    }
    // The crash: the deferred wake dies with the host.
    assert!(records(&*provider, "out/deferred").await?.is_empty());
    authority.0.store(150_000, Ordering::SeqCst);
    let (restarted, _control) = host(240_000)?;
    let mut serving = Box::pin(restarted.run(|_, error| panic!("delivery failed: {error}")));
    observe_while_running(
        serving.as_mut(),
        state_after(&store, &actor, |state| {
            state.alarm().delivered_generation == Some(1)
        }),
    )
    .await?;
    assert_eq!(
        records(&*provider, "out/deferred").await?,
        vec![Bytes::from_static(b"a1")]
    );
    Ok(())
}

/// A malformed record or child in a runtime deadline stream refuses only
/// itself: a restarted host still wakes the window's actor and keeps serving,
/// rather than terminating on retained bytes.
#[tokio::test]
async fn a_malformed_window_record_refuses_only_itself() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?)
        .with_scheduling_shards(std::num::NonZeroU16::MIN);
    let actor = spawn_with(&store, "malformed", &[b"set 50000"]).await?;
    let host = |now| {
        let (clock, control) = ControlledClock::new(now);
        let runtime = Runtime::new(
            store.clone(),
            Arc::new(Package::new()?),
            Arc::new(clock) as Arc<dyn Clock>,
        );
        Ok::<_, Box<dyn std::error::Error>>((runtime, control))
    };
    let (earlier, _control) = host(100_000)?;
    earlier.track(actor.clone())?;
    earlier.drain_actor(&actor).await?;
    // Window one holds only junk, window two the actor's 150000 alarm plus
    // junk, and a non-window child sorts after both.
    let shard = store.scheduling_path(0)?;
    let mut corrupt = PreparedPublish::foreign(key("corrupt")?);
    corrupt.join(CommitEffect::hard(
        Vec::new(),
        [
            format!("{shard}/{:020}", 1),
            format!("{shard}/{:020}", 2),
            format!("{shard}/zz"),
        ]
        .into_iter()
        .map(|path| {
            Ok(CommitMutation::Append {
                path: StreamPath::new(path)?,
                records: vec![Bytes::from_static(b"junk").into()],
            })
        })
        .collect::<Result<_, StreamError>>()?,
    ))?;
    store.submit(corrupt).await?;
    let (restarted, _control) = host(240_000)?;
    let mut serving = Box::pin(restarted.run(|_, error| panic!("delivery failed: {error}")));
    observe_while_running(
        serving.as_mut(),
        state_after(&store, &actor, |state| {
            state.alarm().delivered_generation == Some(1)
        }),
    )
    .await?;
    assert_eq!(
        records(&*provider, "out/malformed").await?,
        vec![Bytes::from_static(b"a1")]
    );
    Ok(())
}
