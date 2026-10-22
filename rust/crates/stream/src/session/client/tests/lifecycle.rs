use super::*;

fn waiting_client() -> (Client, mpsc::Receiver<Command>) {
    let (commands, receiver) = mpsc::channel(1);
    (
        Client {
            connections: super::super::super::Connections::new(),
            executor: std::sync::Arc::new(super::super::super::runtime::PlatformExecutor),
            commands,
            removed: Arc::new(signal::Signal::new()),
            next_id: Arc::new(AtomicU64::new(1)),
            active: Arc::new(AtomicU64::new(0)),
            admissions: Arc::new(Admissions::new()),
            attempt_epoch: Arc::new(AtomicU64::new(0)),
            abandoned: Arc::new(gate::Gate::new()),
        },
        receiver,
    )
}

#[tokio::test(start_paused = true)]
async fn registration_deadline_removes_only_the_stalled_interest() -> TestResult {
    let (client, mut commands) = waiting_client();
    let mut stalled = Box::pin(client.follow_checked(
        StreamPath::new("stalled")?,
        1,
        crate::CommitId::from_bytes([7; 32]),
        crate::FollowFilter::default(),
    ));
    assert!(futures::poll!(stalled.as_mut()).is_pending());
    let Some(Command::Open {
        id, ready: _held, ..
    }) = commands.recv().await
    else {
        return Err("missing registration".into());
    };
    let mut healthy = Box::pin(client.follow(
        StreamPath::new("healthy")?,
        0,
        crate::FollowFilter::default(),
    ));
    assert!(futures::poll!(healthy.as_mut()).is_pending());
    let Some(Command::Open {
        id: healthy_id,
        ready,
        ..
    }) = commands.recv().await
    else {
        return Err("missing sibling".into());
    };
    ready
        .send(Ok(healthy_id))
        .map_err(|_| "sibling cancelled")?;
    let healthy = healthy.await?;
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(matches!(stalled.await, Err(StreamError::DeadlineElapsed)));
    assert_eq!(client.removed.next().await?, id);
    assert_eq!(client.active.load(Ordering::Acquire), 1);
    drop(healthy);
    assert_eq!(client.removed.next().await?, healthy_id);
    assert_eq!(client.active.load(Ordering::Acquire), 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn registration_deadline_also_withdraws_blocked_queue_admission() -> TestResult {
    let (client, mut commands) = waiting_client();
    let mut first =
        Box::pin(client.follow(StreamPath::new("first")?, 0, crate::FollowFilter::default()));
    assert!(futures::poll!(first.as_mut()).is_pending());
    let mut blocked = Box::pin(client.follow_checked(
        StreamPath::new("blocked")?,
        1,
        crate::CommitId::from_bytes([7; 32]),
        crate::FollowFilter::default(),
    ));
    assert!(futures::poll!(blocked.as_mut()).is_pending());
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(matches!(blocked.await, Err(StreamError::DeadlineElapsed)));
    drop(first);
    assert_eq!(client.active.load(Ordering::Acquire), 0);
    assert!(commands.try_recv().is_ok());
    assert!(
        commands.try_recv().is_err(),
        "expired admission must not enqueue an Open"
    );
    Ok(())
}

/// Each channel span records how its follow ended, without leaking private paths.
fn assert_channel_traces(capture: &acyclic_grpc_observability::tests::Capture) -> TestResult {
    let channels = capture.spans("stream.session.channel");
    assert_physical_sessions(&capture.spans("stream.session"));
    let [first, second, third, ..] = channels.as_slice() else {
        panic!("expected three channel spans, found {}", channels.len());
    };
    assert_eq!(
        first.fields.get("error.kind").map(String::as_str),
        Some("deadline_elapsed")
    );
    assert_eq!(
        second.fields.get("registration").map(String::as_str),
        Some("opened")
    );
    assert_eq!(
        second.fields.get("generation").map(String::as_str),
        Some("2")
    );
    assert_eq!(
        second.fields.get("error.kind").map(String::as_str),
        Some("access_denied")
    );
    assert_eq!(
        third.fields.get("outcome").map(String::as_str),
        Some("cancelled")
    );
    let evidence = capture.0.lock().map_err(|_| "capture poisoned")?;
    assert!(evidence.events.iter().any(|(parent, fields)| {
        parent.as_deref() == Some("stream.session.channel")
            && fields
                .iter()
                .any(|(_, name, value)| name == "event" && value.contains("reconnect"))
    }));
    assert!(!format!("{:?}", evidence.spans).contains("private/"));
    Ok(())
}

#[test]
fn traces_cover_registration_stall_reconnect_terminal_error_and_caller_drop() -> TestResult {
    use crate::grpc::tests::retry_capture;
    use acyclic_grpc_observability::tests::Capture;
    use tracing::instrument::WithSubscriber as _;
    let test = format!(
        "{}::traces_cover_registration_stall_reconnect_terminal_error_and_caller_drop",
        module_path!()
    );
    if retry_capture::isolate(&test)? {
        return Ok(());
    }
    let capture = Capture::default();
    let (_registration, subscriber) = retry_capture::dispatch(capture.clone());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(
        async {
            tokio::time::pause();
            let (client, mut commands) = waiting_client();
            let mut stalled = Box::pin(client.follow_checked(
                StreamPath::new("private/stalled")?,
                1,
                crate::CommitId::from_bytes([7; 32]),
                crate::FollowFilter::default(),
            ));
            assert!(futures::poll!(stalled.as_mut()).is_pending());
            let _held = commands.recv().await.ok_or("missing Open")?;
            tokio::time::advance(Duration::from_secs(10)).await;
            assert!(matches!(stalled.await, Err(StreamError::DeadlineElapsed)));
            drop(_held);
            tokio::time::resume();
            let memory = Arc::new(MemoryStream::default());
            append(&memory, "private/events", 1).await?;
            let fixture = Fixture::open(Arc::clone(&memory), "node").await?;
            let client = fixture.client()?;
            let mut records = client
                .follow(
                    StreamPath::new("private/events")?,
                    1,
                    crate::FollowFilter::default(),
                )
                .await?;
            fixture
                .controls
                .send(Err(Status::unavailable("fixture reconnect")))?;
            loop {
                let changed = fixture.counts.changed.notified();
                if fixture
                    .counts
                    .resumes
                    .lock()
                    .map_err(|_| "resumes poisoned")?
                    .iter()
                    .any(|(_, generation, _)| *generation == 2)
                {
                    break;
                }
                changed.await;
            }
            append(&memory, "private/events", 1).await?;
            assert_eq!(records.next().await.ok_or("follow ended")??.sequence, 1);
            fixture
                .controls
                .send(Err(Status::permission_denied("terminal fixture failure")))?;
            assert_eq!(records.next().await, Some(Err(StreamError::AccessDenied)));
            drop(records);
            let cancelled = client
                .follow(
                    StreamPath::new("private/events")?,
                    2,
                    crate::FollowFilter::default(),
                )
                .await?;
            drop(cancelled);
            // Retirement is an observable completion, not a sleep or elapsed-time assertion.
            loop {
                let changed = capture.1.notified();
                if capture.spans("stream.session.channel").len() == 3
                    && capture.spans("stream.session").len() == 3
                {
                    break;
                }
                changed.await;
            }
            assert_channel_traces(&capture)?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }
        .with_subscriber(subscriber),
    )
}

fn assert_physical_sessions(sessions: &[acyclic_grpc_observability::tests::CapturedSpan]) {
    let outcomes = sessions
        .iter()
        .map(|session| {
            (
                session.fields.get("outcome").map(String::as_str),
                session.fields.get("error.kind").map(String::as_str),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        outcomes,
        [
            (Some("err"), Some("unavailable")),
            (Some("err"), Some("access_denied")),
            (Some("closed"), None),
        ],
        "physical session outcomes must distinguish failure from closure"
    );
}

#[test]
fn negative_control_reports_physical_failures_as_normal_closure() -> TestResult {
    use crate::grpc::tests::retry_capture;
    use acyclic_grpc_observability::tests::Capture;
    let test = format!(
        "{}::negative_control_reports_physical_failures_as_normal_closure",
        module_path!()
    );
    if retry_capture::isolate(&test)? {
        return Ok(());
    }
    let capture = Capture::default();
    let (_registration, subscriber) = retry_capture::dispatch(capture.clone());
    tracing::dispatcher::with_default(&subscriber, || {
        for attempt in 0..3 {
            let mut observation = Observation::session(attempt);
            // Deliberately omit the reconnect/terminal errors through the real
            // formatter. All three spans still exist and retire normally.
            observation.finish(None);
        }
    });
    let sessions = capture.spans("stream.session");
    assert_eq!(sessions.len(), 3);
    let rejected = std::panic::catch_unwind(|| assert_physical_sessions(&sessions));
    let failure = rejected.expect_err("the trace negative control passed");
    let message = failure
        .downcast_ref::<String>()
        .ok_or("unexpected trace failure")?;
    assert!(message.contains("physical session outcomes must distinguish failure from closure"));
    Ok(())
}
