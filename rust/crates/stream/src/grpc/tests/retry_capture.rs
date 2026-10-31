use super::*;
use acyclic_grpc_observability::tests::Capture;
use tracing::{Dispatch, instrument::WithSubscriber as _};
use tracing_subscriber::layer::SubscriberExt as _;

pub(crate) fn dispatch(capture: Capture) -> Dispatch {
    Dispatch::new(tracing_subscriber::Registry::default().with(capture))
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
fn capture_owns_registration_without_caller_guard() -> TestResult {
    isolated_registration(true)
}

fn isolated_registration(direct: bool) -> TestResult {
    const CHILD: &str = "ACYCLIC_RETRY_CAPTURE_REGISTRATION";
    const ASSERTION: &str = "unobserved first registration disabled retry capture";
    let mode = if direct { "direct" } else { "factory" };
    if std::env::var(CHILD).as_deref() != Ok(mode) {
        // Fresh processes make the dependency's process-global callsite state
        // deterministic even when the parent libtest suite runs in parallel.
        let test = if direct {
            "capture_owns_registration_without_caller_guard"
        } else {
            "unobserved_first_registration_preserves_retry_capture"
        };
        let output = crate::test_trace::command(&format!("{}::{test}", module_path!()))?
            .env(CHILD, mode)
            .output()?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(stdout.contains("1 passed; 0 failed"), "{stdout}\n{stderr}");
        return Ok(());
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let captured = Capture::default();
    let subscriber = if direct {
        Dispatch::new(tracing_subscriber::Registry::default().with(captured.clone()))
    } else {
        dispatch(captured.clone())
    };
    runtime.block_on(async {
        let client = unconnected()?;
        // Register the real unary RPC callsite without a thread-local subscriber
        // while the capture dispatcher exists, before any captured attempt.
        let response = client
            .unary((), |_, _| Box::pin(async { Ok(Response::new(())) }))
            .await?;
        decode_response(response, Ok)?;
        async {
            let mut first = true;
            let response = client
                .unary((), |_, _| {
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
