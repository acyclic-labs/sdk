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
    let mut serving = Box::pin(runtime.run(|_, error| panic!("delivery failed: {error}")));

    // The real MemoryStream and original Package execute synchronously until
    // their original event producers park. No timer, retry or readiness loop.
    assert!(serving.as_mut().poll(&mut context).is_pending());
    assert_eq!(runtime.ready.next_deadline(), Some(110));
    assert_eq!(runtime.ready.alarm_count(), 3);
    assert_eq!(runtime.counts().1, 0);

    notifications.0.store(0, Ordering::SeqCst);
    control.advance_to(109)?;
    assert!(
        notifications.0.load(Ordering::SeqCst) > 0,
        "lost clock wake"
    );
    assert!(serving.as_mut().poll(&mut context).is_pending());
    assert!(records(&*provider, "out/clock-first").await?.is_empty());

    // An advance before this future's first poll cannot be lost.
    let reached = clock.sleep_until(110);
    control.advance_to(110)?;
    reached.await?;
    assert!(serving.as_mut().poll(&mut context).is_pending());
    assert_eq!(
        records(&*provider, "out/clock-first").await?,
        vec![Bytes::from_static(b"a1")]
    );
    assert!(records(&*provider, "out/clock-second").await?.is_empty());
    assert_eq!(runtime.ready.next_deadline(), Some(120));

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
    assert!(serving.as_mut().poll(&mut context).is_pending());
    assert_eq!(
        records(&*provider, "out/clock-first").await?,
        vec![Bytes::from_static(b"a1")]
    );
    assert_eq!(
        records(&*provider, "out/clock-second").await?,
        vec![Bytes::from_static(b"a1")]
    );
    assert_eq!(runtime.ready.next_deadline(), Some(130));
    for address in [&first, &second] {
        let snapshot = store.load(address).await?;
        let state = snapshot.state().ok_or("missing alarm state")?;
        assert_eq!(state.cursor("inbox"), Some(1));
        assert_eq!(state.alarm().delivered_generation, Some(1));
    }

    let mut pending = clock.sleep_until(130);
    assert!(pending.as_mut().poll(&mut context).is_pending());
    notifications.0.store(0, Ordering::SeqCst);
    drop(control);
    assert!(
        notifications.0.load(Ordering::SeqCst) > 0,
        "lost producer-close wake"
    );
    assert_eq!(pending.await, Err(ClockError::ProducerDropped));
    assert_eq!(
        clock.sleep_until(130).await,
        Err(ClockError::ProducerDropped)
    );
    clock.sleep_until(120).await?;
    assert!(matches!(
        serving.as_mut().poll(&mut context),
        Poll::Ready(Err(Error::Clock(ClockError::ProducerDropped)))
    ));
    assert!(records(&*provider, "out/clock-future").await?.is_empty());
    let snapshot = store.load(&future).await?;
    let state = snapshot.state().ok_or("missing future alarm")?;
    assert_eq!(state.cursor("inbox"), Some(1));
    assert_eq!(state.alarm().deadline_unix_ms, Some(130));
    assert_eq!(state.alarm().delivered_generation, None);
    Ok(())
}
