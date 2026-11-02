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
    let activation_tail = provider.tail(root()?).await?;
    let (dispatch_store, dispatch_actor) = runtime.dispatcher_store()?;
    observe_while_running(
        serving.as_mut(),
        state_after(&dispatch_store, &dispatch_actor, |state| {
            state
                .cursor("activation")
                .is_some_and(|cursor| cursor >= activation_tail)
        }),
    )
    .await?;

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

    if let Some(replacement) = &replacement {
        // The replacement's original activation reports the stale epoch before
        // time advances. Older activation entries cannot re-place it afterwards.
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
    }

    // Both an installed waiter and a not-yet-observed future retain the same
    // original advance. Neither owns the clock producer.
    let mut reached = clock.sleep_until(110);
    if observed_before_advance {
        assert!(reached.as_mut().poll(&mut context).is_pending());
    }
    control.advance_to(110)?;
    reached.await?;
    if replacement.is_some() {
        // Only this explicit placement may claim epoch 3 and deliver the
        // still-committed alarm; the stale dispatcher may not publish it.
        runtime.track(first.clone())?;
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

    notifications.0.store(0, Ordering::SeqCst);
    assert_eq!(control.advance_to(109), Err(ClockError::Reversed));
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
    let mut retained = clock.sleep_until(130);
    assert!(retained.as_mut().poll(&mut context).is_pending());
    notifications.0.store(0, Ordering::SeqCst);
    drop(control);
    assert!(
        notifications.0.load(Ordering::SeqCst) > 0,
        "lost producer-close wake"
    );
    if let Some(pending) = pending {
        assert_eq!(pending.await, Err(ClockError::ProducerDropped));
    }
    assert_eq!(retained.await, Err(ClockError::ProducerDropped));
    assert_eq!(
        clock.sleep_until(130).await,
        Err(ClockError::ProducerDropped)
    );
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
