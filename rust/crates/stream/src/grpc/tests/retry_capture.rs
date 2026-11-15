use super::*;
use acyclic_grpc_observability::tests::Capture;
use tracing::Dispatch;
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
