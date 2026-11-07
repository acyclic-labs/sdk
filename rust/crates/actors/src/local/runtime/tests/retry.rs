use super::*;

/// Refuses the first delivery of its actor's alarm with a retained failure,
/// then delegates; counts every alarm delivery it is handed.
struct RefusesFirstAlarm {
    package: Package,
    alarms: AtomicUsize,
}

impl Handler for RefusesFirstAlarm {
    fn manifest(&self) -> &Manifest {
        self.package.manifest()
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        if matches!(delivery.trigger, Trigger::Alarm { .. })
            && self.alarms.fetch_add(1, Ordering::SeqCst) == 0
        {
            return futures::future::ready(Err(crate::guest::GuestError::Handler(
                "refused alarm".into(),
            )))
            .boxed();
        }
        self.package.on_record(delivery, checkpoint, cancellation)
    }
}

/// A failed delivery stays queued for the actor's NEXT event. Its own due
/// alarm is not one: the continuous host must not redeliver it in a loop, and
/// reaches its idle boundary with the failure reported once.
///
/// Negative control: installing the alarm being delivered into the due index
/// before its handler runs made the shared timer fire it again as soon as the
/// attempt failed, so the host redelivered it forever without a new event.
#[tokio::test]
async fn a_failed_alarm_delivery_waits_for_its_next_event() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = spawn_with(&store, "refused-alarm", &[b"set 10"]).await?;
    let (clock, mut control) = ControlledClock::new(100);
    let handler = Arc::new(RefusesFirstAlarm {
        package: Package::new()?,
        alarms: AtomicUsize::new(0),
    });
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::clone(&handler) as Arc<dyn Handler>,
        Arc::new(clock),
    );
    let reported = AtomicUsize::new(0);
    let mut serving = Box::pin(runtime.run(|actor, error| {
        assert_eq!(actor, &address);
        assert!(matches!(error, Error::Handler(_)), "{error}");
        reported.fetch_add(1, Ordering::SeqCst);
    }));
    observe_while_running(
        serving.as_mut(),
        state_after(&store, &address, |state| state.cursor("inbox") == Some(1)),
    )
    .await?;
    control.advance_to(200)?;
    let completed = observe_while_running(serving.as_mut(), async {
        Ok::<_, Box<dyn std::error::Error>>(runtime.complete().await)
    })
    .await?;
    assert!(
        matches!(completed, Err(Error::CompletionFailed(_))),
        "{completed:?}"
    );
    assert_eq!(
        handler.alarms.load(Ordering::SeqCst),
        1,
        "the failed alarm was redelivered without a new event"
    );
    assert_eq!(reported.load(Ordering::SeqCst), 1);
    assert!(records(&*provider, "out/refused-alarm").await?.is_empty());
    // The producer's wake is the next event: it delivers the retained alarm
    // before the new input.
    provider
        .append(AppendRequest {
            path: StreamPath::new("agents/refused-alarm")?,
            records: vec![Bytes::from_static(b"emit").into()],
            if_tail: Some(1),
            idempotency_key: key("refused-alarm-next")?,
        })
        .await?;
    runtime.wake(&address);
    observe_while_running(
        serving.as_mut(),
        state_after(&store, &address, |state| state.cursor("inbox") == Some(2)),
    )
    .await?;
    assert_eq!(handler.alarms.load(Ordering::SeqCst), 2);
    assert_eq!(
        records(&*provider, "out/refused-alarm").await?,
        vec![Bytes::from_static(b"a1"), Bytes::from_static(b"r1")]
    );
    Ok(())
}
