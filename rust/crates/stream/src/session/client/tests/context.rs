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
        fixture
            .until(|counts| counts.closed_sessions.load(Ordering::Relaxed) == 1)
            .await;
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
        fixture
            .until(|counts| counts.closed_sessions.load(Ordering::Relaxed) == 2)
            .await;
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
            assert!(first.has_ancestor("stream.session", "first.request"));
        }
        assert!(second.has_ancestor("stream.session", "second.request"));
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
