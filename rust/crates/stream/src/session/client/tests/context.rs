use super::*;
use crate::grpc::tests::retry_capture;
use acyclic_grpc_observability::tests::Capture;

const CLOSE: &str = "the first request must close while its pool remains cached";

fn run_context_case(observed_first: bool, retain_first: bool) -> TestResult {
    let first = Capture::default();
    let second = Capture::default();
    let (_first_registration, first_dispatch) = retry_capture::dispatch(first.clone());
    let (_second_registration, second_dispatch) = retry_capture::dispatch(second.clone());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let memory = Arc::new(MemoryStream::default());
        append(&memory, "events", 1).await?;
        let fixture = Fixture::open(memory, "node").await?;
        let client = fixture.client()?;
        let origin = if observed_first {
            tracing::dispatcher::with_default(&first_dispatch, || {
                crate::obs::OperationSpan::new(tracing::info_span!("first.request"))
            })
        } else {
            crate::obs::OperationSpan::new(tracing::Span::none())
        };
        // Deliberately reproduce the former cached-pool ownership in the control.
        let retained = retain_first.then(|| origin.clone());
        let records = origin
            .scope(client.follow(StreamPath::new("events")?, 1))
            .await?;
        drop(records);
        drop(origin);
        tokio::time::timeout(
            Duration::from_secs(1),
            fixture.until(|counts| counts.closed_sessions.load(Ordering::Relaxed) == 1),
        )
        .await?;
        if observed_first {
            assert_eq!(first.closed("first.request"), 1, "{CLOSE}");
        }
        let origin = tracing::dispatcher::with_default(&second_dispatch, || {
            crate::obs::OperationSpan::new(tracing::info_span!("second.request"))
        });
        let records = origin
            .scope(client.follow(StreamPath::new("events")?, 1))
            .await?;
        drop(records);
        drop(origin);
        tokio::time::timeout(
            Duration::from_secs(1),
            fixture.until(|counts| counts.closed_sessions.load(Ordering::Relaxed) == 2),
        )
        .await?;
        assert_eq!(second.closed("second.request"), 1);
        assert_eq!(
            first.spans("stream.session").len(),
            usize::from(observed_first)
        );
        assert_eq!(
            second.spans("stream.session").len(),
            1,
            "a later physical Session must use its own subscriber"
        );
        if observed_first {
            assert_eq!(first.parent("stream.session"), None);
        }
        assert_eq!(second.parent("stream.session"), None);
        assert_eq!(fixture.counts.connections.load(Ordering::Relaxed), 1);
        assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 2);
        // The SDK client and its cached pool remain alive through all assertions.
        drop(client);
        drop(retained);
        Ok(())
    })
}

#[test]
fn cached_pool_releases_the_first_request_and_uses_each_session_origin() -> TestResult {
    let test = format!(
        "{}::cached_pool_releases_the_first_request_and_uses_each_session_origin",
        module_path!()
    );
    if retry_capture::isolate(&test)? {
        return Ok(());
    }
    run_context_case(true, false)
}

#[test]
fn unobserved_initialization_cannot_hide_a_later_session_subscriber() -> TestResult {
    let test = format!(
        "{}::unobserved_initialization_cannot_hide_a_later_session_subscriber",
        module_path!()
    );
    if retry_capture::isolate(&test)? {
        return Ok(());
    }
    run_context_case(false, false)
}

#[test]
fn negative_control_retains_the_first_request_context() -> TestResult {
    let test = format!(
        "{}::negative_control_retains_the_first_request_context",
        module_path!()
    );
    if retry_capture::isolate(&test)? {
        return Ok(());
    }
    let failure = std::panic::catch_unwind(|| run_context_case(true, true))
        .expect_err("retained caller context escaped the same closure assertion");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains(CLOSE), "unexpected failure: {message}");
    Ok(())
}

fn origin(dispatch: &tracing::Dispatch, first: bool) -> crate::obs::OperationSpan {
    tracing::dispatcher::with_default(dispatch, || {
        crate::obs::OperationSpan::new(if first {
            tracing::info_span!("first.request")
        } else {
            tracing::info_span!("second.request")
        })
    })
}

fn assert_overlap(capture: &Capture) {
    assert_eq!(
        capture.closed("first.request"),
        1,
        "A must close while B remains active"
    );
    assert_eq!(capture.closed("second.request"), 0);
    let history = capture.0.lock().unwrap().spans.clone();
    let sessions = history
        .iter()
        .filter(|span| span.name == "stream.session")
        .collect::<Vec<_>>();
    assert_eq!(sessions.len(), 1);
    let session = sessions[0];
    assert_eq!(session.parent_id, None, "a physical session must be a root");
    assert!(!session.closed, "B must retain the physical session");
    for name in ["first.request", "second.request"] {
        assert!(
            session.follows_from.iter().any(|index| {
                let cause = &history[*index];
                cause.name == "stream.session.channel"
                    && cause
                        .parent
                        .is_some_and(|parent| history[parent].name == name)
            }),
            "each joined interest must have a causal link"
        );
    }
}

fn overlapping(fault: u8) -> TestResult {
    let capture = Capture::default();
    let (_registration, subscriber) = retry_capture::dispatch(capture.clone());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let memory = Arc::new(MemoryStream::default());
        append(&memory, "events", 1).await?;
        let fixture = Fixture::open(Arc::clone(&memory), "node").await?;
        let client = fixture.client()?;
        let first = origin(&subscriber, true);
        let a = first
            .scope(client.follow(StreamPath::new("events")?, 1))
            .await?;
        let second = origin(&subscriber, false);
        let mut b = second
            .scope(client.follow(StreamPath::new("events")?, 1))
            .await?;
        // Lifetime mutant is independent of the root/causal-link controls below.
        let retained = (fault == 1).then(|| first.clone());
        drop(a);
        drop(first);
        fixture
            .until(|counts| counts.removals.load(Ordering::Relaxed) == 1)
            .await;
        append(&memory, "events", 1).await?;
        assert_eq!(next(&mut b).await?.sequence, 1);
        if fault >= 2 {
            let mut history = capture.0.lock().unwrap();
            let caller = history
                .spans
                .iter()
                .find(|span| span.name == "first.request")
                .unwrap()
                .id;
            let session = history
                .spans
                .iter_mut()
                .find(|span| span.name == "stream.session")
                .unwrap();
            if fault == 2 {
                session.parent_id = Some(caller);
            }
            if fault == 3 {
                session.follows_from.clear();
            }
        }
        assert_overlap(&capture);
        assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
        assert_eq!(fixture.counts.closed_sessions.load(Ordering::Relaxed), 0);
        drop(retained);
        drop(b);
        drop(second);
        Ok(())
    })
}

fn rejected(work: impl FnOnce() -> TestResult + std::panic::UnwindSafe, assertion: &str) {
    let error = std::panic::catch_unwind(work).expect_err("the broken variant passed");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains(assertion), "unexpected failure: {message}");
}

#[test]
fn root_session_links_both_interests_and_releases_a_while_b_is_active() -> TestResult {
    let name = format!(
        "{}::root_session_links_both_interests_and_releases_a_while_b_is_active",
        module_path!()
    );
    if retry_capture::isolate(&name)? {
        return Ok(());
    }
    overlapping(0)
}

#[test]
fn negative_controls_reject_retention_parenting_and_missing_causal_links() -> TestResult {
    let name = format!(
        "{}::negative_controls_reject_retention_parenting_and_missing_causal_links",
        module_path!()
    );
    if retry_capture::isolate(&name)? {
        return Ok(());
    }
    for (fault, assertion) in [
        (1, "A must close while B remains active"),
        (2, "a physical session must be a root"),
        (3, "each joined interest must have a causal link"),
    ] {
        rejected(|| overlapping(fault), assertion);
    }
    Ok(())
}

fn assert_reconnect_subscriber(first: &Capture, second: &Capture) {
    assert_eq!(
        first.all("stream.session").len(),
        0,
        "reconnect must exclude cancelled A"
    );
    assert_eq!(
        second.all("stream.session").len(),
        1,
        "reconnect must use live B"
    );
    assert_eq!(second.parent("stream.session"), None);
}

fn reconnect_before_removal(broken: bool) -> TestResult {
    let first = Capture::default();
    let second = Capture::default();
    let (_a_registration, a_dispatch) = retry_capture::dispatch(first.clone());
    let (_b_registration, b_dispatch) = retry_capture::dispatch(second.clone());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let fixture = Fixture::open(Arc::new(MemoryStream::default()), "good").await?;
        let (mut pool, healthy) = pool::fixture::fixture(fixture._channel.clone());
        pool.links.remove("bad");
        let a_origin = origin(&a_dispatch, true);
        let b_origin = origin(&b_dispatch, false);
        let (sender, receiver) = mpsc::channel(2);
        let a_progress = Arc::clone(&pool.routes[&1].progress);
        let a = pool.routes.get_mut(&1).unwrap();
        a.records = sender;
        a.node = "good".into();
        a.observation = a_origin.in_scope(|| Observation::channel(1).operation.clone());
        pool.routes.get_mut(&3).unwrap().observation =
            b_origin.in_scope(|| Observation::channel(3).operation.clone());
        // The real Cursor drop sets cancellation before its queued removal runs.
        let client = Client {
            connections: super::super::super::Connections::new(),
            executor: std::sync::Arc::new(super::super::super::runtime::PlatformExecutor),
            commands: mpsc::channel(2).0,
            removed: Arc::clone(&pool.removed),
            next_id: Arc::new(AtomicU64::new(5)),
            active: Arc::clone(&pool.active),
            admissions: Arc::clone(&pool.admissions),
            attempt_epoch: Arc::clone(&pool.attempt_epoch),
            abandoned: Arc::clone(&pool.abandoned),
        };
        let a = client.cursor(1, a_progress, receiver)?;
        let _b = client.cursor(3, Arc::clone(&pool.routes[&3].progress), healthy)?;
        drop(a);
        // Deliberately assign the stale origin to the successor to test the
        // subscriber oracle independently of span-retirement assertions.
        if broken {
            pool.routes.get_mut(&3).unwrap().observation = pool.routes[&1].observation.clone();
        }
        pool.reconnect("good")?;
        assert_eq!(pool.routes.len(), 2, "A's removal must still be queued");
        let (node, epoch, ready) = future::poll_fn(|cx| pool::poll_dials(&mut pool.dials, cx))
            .await
            .ok_or("dial ended")?;
        pool.dial_ready(node, epoch, ready)?;
        pool.subscribe(3)?;
        assert_reconnect_subscriber(&first, &second);
        Ok(())
    })
}

#[test]
fn reconnect_ignores_cancelled_route_before_queued_removal() -> TestResult {
    let name = format!(
        "{}::reconnect_ignores_cancelled_route_before_queued_removal",
        module_path!()
    );
    if retry_capture::isolate(&name)? {
        return Ok(());
    }
    reconnect_before_removal(false)
}

#[test]
fn negative_control_attributes_reconnect_to_the_cancelled_origin() -> TestResult {
    let name = format!(
        "{}::negative_control_attributes_reconnect_to_the_cancelled_origin",
        module_path!()
    );
    if retry_capture::isolate(&name)? {
        return Ok(());
    }
    rejected(
        || reconnect_before_removal(true),
        "reconnect must exclude cancelled A",
    );
    Ok(())
}

struct DialDrop(std::sync::Arc<tokio::sync::Notify>);
impl Drop for DialDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

async fn moved_owner_withdraws() -> TestResult {
    let capture = Capture::default();
    let (_registration, subscriber) = retry_capture::dispatch(capture.clone());
    let memory = Arc::new(MemoryStream::default());
    append(&memory, "move", 1).await?;
    append(&memory, "stay", 1).await?;
    let home = Fixture::open(Arc::clone(&memory), "home").await?;
    let destination = Fixture::open(Arc::clone(&memory), "away").await?;
    let (release, blocked) = oneshot::channel::<()>();
    // Lock order: this one-shot test gate is never nested or held while polling.
    let blocked = Arc::new(Mutex::new(Some(blocked)));
    let started = std::sync::Arc::new(tokio::sync::Notify::new());
    let cancelled = std::sync::Arc::new(tokio::sync::Notify::new());
    let begin = started.clone();
    let end = cancelled.clone();
    let target = destination._channel.clone();
    let connect: Connect = std::sync::Arc::new(move |node| {
        assert_eq!(node.node_id, "away");
        let gate = blocked.lock().unwrap().take().unwrap();
        let target = target.clone();
        let guard = DialDrop(end.clone());
        begin.notify_one();
        Box::pin(async move {
            let _guard = guard;
            gate.await.map_err(|_| StreamError::Unavailable)?;
            Ok(target)
        })
    });
    let client = Client::with_connector(
        home._channel.clone(),
        Some("Bearer fixture".parse()?),
        None,
        connect,
    );
    let a_origin = origin(&subscriber, true);
    let moving = a_origin
        .scope(client.follow(StreamPath::new("move")?, 1))
        .await?;
    let b_origin = origin(&subscriber, false);
    let mut staying = b_origin
        .scope(client.follow(StreamPath::new("stay")?, 1))
        .await?;
    home.controls.send(Ok(frame(
        1,
        1,
        Body::Moved(wire::SessionMoved {
            node: Some(wire::SessionNode {
                node_id: "away".into(),
                endpoint: "https://away.invalid".into(),
                placement_epoch: 1,
            }),
            resume: None,
        }),
    )))?;
    started.notified().await;
    drop(moving);
    drop(a_origin);
    // This notification comes from dropping the actual blocked connector future.
    // The sibling remains live on a different node throughout cancellation.
    cancelled.notified().await;
    assert!(
        release.send(()).is_err(),
        "ownerless connector must be dropped"
    );
    assert_eq!(
        destination.counts.sessions.load(Ordering::Relaxed),
        0,
        "ownerless dial must never install a session"
    );
    loop {
        let changed = capture.1.notified();
        if capture.closed("first.request") == 1 {
            break;
        }
        changed.await;
    }
    assert_eq!(
        capture.closed("first.request"),
        1,
        "cancelled moved caller must close"
    );
    append(&memory, "stay", 1).await?;
    assert_eq!(next(&mut staying).await?.sequence, 1);
    assert_eq!(home.counts.sessions.load(Ordering::Relaxed), 1);
    assert_eq!(home.counts.closed_sessions.load(Ordering::Relaxed), 0);
    Ok(())
}

#[test]
fn withdrawing_a_moved_interest_cancels_only_its_node_dial() -> TestResult {
    let name = format!(
        "{}::withdrawing_a_moved_interest_cancels_only_its_node_dial",
        module_path!()
    );
    if retry_capture::isolate(&name)? {
        return Ok(());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(moved_owner_withdraws())
}

#[test]
fn cancelling_the_dial_origin_releases_its_span_while_b_waits_for_welcome() -> TestResult {
    let name = format!(
        "{}::cancelling_the_dial_origin_releases_its_span_while_b_waits_for_welcome",
        module_path!()
    );
    if retry_capture::isolate(&name)? {
        return Ok(());
    }
    let capture = Capture::default();
    let (_registration, subscriber) = retry_capture::dispatch(capture.clone());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let memory = Arc::new(MemoryStream::default());
        append(&memory, "events", 1).await?;
        let fixture = Fixture::open(memory, "node").await?;
        let (release, held) = oneshot::channel();
        // No nested locks, and the receiver is taken before awaiting it.
        let held = std::sync::Arc::new(Mutex::new(Some(held)));
        let started = std::sync::Arc::new(tokio::sync::Notify::new());
        let ready = started.clone();
        let credential = grpc::CredentialProvider::new(move || {
            let held = held.lock().unwrap().take();
            ready.notify_one();
            async move {
                held.ok_or(grpc::CredentialError::Invalid)?
                    .await
                    .map_err(|_| grpc::CredentialError::Invalid)?;
                Ok("fixture".into())
            }
        });
        let client = Client::with_connector(
            fixture._channel.clone(),
            None,
            Some(credential),
            std::sync::Arc::new(|_| Box::pin(future::ready(Err(StreamError::Unavailable)))),
        );
        let a_origin = origin(&subscriber, true);
        let mut a = Box::pin(a_origin.scope(client.follow(StreamPath::new("events")?, 1)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        started.notified().await;
        let b_origin = origin(&subscriber, false);
        let mut b = Box::pin(b_origin.scope(client.follow(StreamPath::new("events")?, 1)));
        assert!(futures::poll!(b.as_mut()).is_pending());
        drop(a);
        drop(a_origin);
        loop {
            let changed = capture.1.notified();
            if capture.closed("first.request") == 1 {
                break;
            }
            changed.await;
        }
        assert_eq!(capture.closed("second.request"), 0);
        assert_eq!(
            capture.all("stream.session").len(),
            1,
            "pending B must preserve shared initialization"
        );
        assert_eq!(capture.parent("stream.session"), None);
        release
            .send(())
            .map_err(|_| "B lost shared initialization")?;
        let _b = b.await?;
        assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
        assert_eq!(fixture.counts.follows.load(Ordering::Relaxed), 1);
        Ok(())
    })
}

#[test]
fn overlapping_subscribers_cannot_alias_equal_channel_span_ids() -> TestResult {
    let name = format!(
        "{}::overlapping_subscribers_cannot_alias_equal_channel_span_ids",
        module_path!()
    );
    if retry_capture::isolate(&name)? {
        return Ok(());
    }
    let first = Capture::default();
    let second = Capture::default();
    let (_a_registration, a_dispatch) = retry_capture::dispatch(first.clone());
    let (_b_registration, b_dispatch) = retry_capture::dispatch(second.clone());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let memory = Arc::new(MemoryStream::default());
        append(&memory, "events", 1).await?;
        let fixture = Fixture::open(Arc::clone(&memory), "node").await?;
        let client = fixture.client()?;
        let a_origin = origin(&a_dispatch, true);
        let a = a_origin
            .scope(client.follow(StreamPath::new("events")?, 1))
            .await?;
        let b_origin = origin(&b_dispatch, false);
        let mut b = b_origin
            .scope(client.follow(StreamPath::new("events")?, 1))
            .await?;
        let a_history = first.0.lock().unwrap().spans.clone();
        let b_history = second.0.lock().unwrap().spans.clone();
        let a_channel = a_history
            .iter()
            .find(|span| span.name == "stream.session.channel")
            .unwrap();
        let b_channel = b_history
            .iter()
            .find(|span| span.name == "stream.session.channel")
            .unwrap();
        assert_eq!(
            a_channel.id, b_channel.id,
            "the probe must exercise subscriber-local ID aliasing"
        );
        let physical = a_history
            .iter()
            .find(|span| span.name == "stream.session")
            .unwrap();
        assert_eq!(physical.parent_id, None);
        assert_eq!(
            physical.follows_from.len(),
            1,
            "foreign interest must not add an aliased causal edge"
        );
        assert_eq!(a_history[physical.follows_from[0]].id, a_channel.id);
        assert_eq!(second.all("stream.session").len(), 0);
        drop(a);
        drop(a_origin);
        fixture
            .until(|counts| counts.removals.load(Ordering::Relaxed) == 1)
            .await;
        assert_eq!(first.closed("first.request"), 1);
        append(&memory, "events", 1).await?;
        assert_eq!(next(&mut b).await?.sequence, 1);
        assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
        Ok(())
    })
}
