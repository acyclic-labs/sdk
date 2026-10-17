use super::*;
use acyclic_grpc_observability::tests::Capture;
use tracing::{Dispatch, instrument::WithSubscriber as _};
use tracing_subscriber::layer::SubscriberExt as _;

pub(crate) fn dispatch(capture: Capture) -> (Dispatch, Dispatch) {
    // tracing-core's single-dispatcher fast path computes first-registration
    // interest using the registering thread's current subscriber. An unrelated
    // caller without a subscriber can therefore cache Never for our RPC callsite.
    // Keep two dispatchers alive so registration considers all live subscribers.
    let registration = Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let capture = Dispatch::new(tracing_subscriber::Registry::default().with(capture));
    (registration, capture)
}

pub(crate) fn isolate(test: &str) -> TestResult<bool> {
    // Another test can already be inside tracing's first-registration fast path
    // when this capture starts. Isolate that process-global cache, not just TLS.
    let parent = module_path!().rsplit_once("::").ok_or("test module")?.0;
    let path = if test.contains("::") {
        test.to_owned()
    } else {
        format!("{parent}::{test}")
    };
    Ok(crate::test_trace::isolate(&path)?)
}

#[test]
fn unobserved_first_registration_preserves_retry_capture() -> TestResult {
    isolated_registration(false)
}

#[test]
fn negative_control_single_dispatcher_loses_retry_capture() -> TestResult {
    isolated_registration(true)
}

fn isolated_registration(broken: bool) -> TestResult {
    const CHILD: &str = "ACYCLIC_RETRY_CAPTURE_REGISTRATION";
    const ASSERTION: &str = "unobserved first registration disabled retry capture";
    let mode = if broken { "single" } else { "paired" };
    if std::env::var(CHILD).as_deref() != Ok(mode) {
        // Fresh processes make the dependency's process-global callsite state
        // deterministic even when the parent libtest suite runs in parallel.
        let test = if broken {
            "negative_control_single_dispatcher_loses_retry_capture"
        } else {
            "unobserved_first_registration_preserves_retry_capture"
        };
        let output = crate::test_trace::command(&format!("{}::{test}", module_path!()))?
            .env(CHILD, mode)
            .output()?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        if broken {
            assert!(!output.status.success(), "negative control passed");
            assert!(stderr.contains(ASSERTION), "{stdout}\n{stderr}");
            assert!(stderr.contains("left: 0"), "{stdout}\n{stderr}");
        } else {
            assert!(output.status.success(), "{stdout}\n{stderr}");
            assert!(stdout.contains("1 passed; 0 failed"), "{stdout}\n{stderr}");
        }
        return Ok(());
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let captured = Capture::default();
    let (_registration, subscriber) = if broken {
        (
            None,
            Dispatch::new(tracing_subscriber::Registry::default().with(captured.clone())),
        )
    } else {
        let (registration, capture) = dispatch(captured.clone());
        (Some(registration), capture)
    };
    runtime.block_on(async {
        let client = unconnected()?;
        // Register the real unary RPC callsite without a thread-local subscriber
        // while the capture dispatcher exists, before any captured attempt.
        let response = client
            .unary(ATTEMPT_TIMEOUT, (), |_, _| {
                Box::pin(async { Ok(Response::new(())) })
            })
            .await?;
        decode_response(response, Ok)?;
        async {
            let mut first = true;
            let response = client
                .unary(ATTEMPT_TIMEOUT, (), |_, _| {
                    let result = if std::mem::take(&mut first) {
                        Err(Status::unavailable("retry"))
                    } else {
                        Ok(Response::new(()))
                    };
                    Box::pin(async move { result })
                })
                .await?;
            decode_response(response, Ok)
        }
        .with_subscriber(subscriber)
        .await?;
        let attempts = captured.all("acyclic.stream.grpc.call");
        assert_eq!(attempts.len(), 2, "{ASSERTION}");
        for (attempt, expected) in attempts.iter().zip(["1", "2"]) {
            assert_eq!(attempt.get("attempt").map(String::as_str), Some(expected));
        }
        Ok(())
    })
}
