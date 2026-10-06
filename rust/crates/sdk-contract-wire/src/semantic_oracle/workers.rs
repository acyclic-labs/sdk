//! Exact semantic observations from `sdk-examples` WorkersFixture.

const SELECT: &str = r#"{"identity_pairs":[],"cursor_trace":[],"revision_trace":[1],"status_trace":[],"transitions":[]}"#;
const SUBMIT: &str = r#"{"identity_pairs":[],"cursor_trace":[],"revision_trace":[],"status_trace":["SUCCEEDED"],"transitions":[]}"#;
const INSPECT: &str = r#"{"identity_pairs":[{"field":"job_id","request":"fixture-job-1","response":"fixture-job-1"}],"cursor_trace":[],"revision_trace":[],"status_trace":["SUCCEEDED"],"transitions":[]}"#;
const CANCEL: &str = r#"{"identity_pairs":[{"field":"job_id","request":"fixture-job-1","response":"fixture-job-1"}],"cursor_trace":[],"revision_trace":[],"status_trace":["CANCELLED"],"transitions":[{"kind":"cancellation","before":"SUCCEEDED","after":"CANCELLED"}]}"#;
const INVOKE_VERSION: &str = r#"{"identity_pairs":[],"cursor_trace":[],"revision_trace":[],"status_trace":["200"],"transitions":[]}"#;
const INVOKE_DEPLOYMENT: &str = r#"{"identity_pairs":[],"cursor_trace":[],"revision_trace":[1],"status_trace":["200"],"transitions":[]}"#;

pub(super) fn expectation_json(rpc: &str) -> Option<&'static str> {
    match rpc {
        "acyclic.workers.v1.WorkersService/SelectDeployment" => Some(SELECT),
        "acyclic.workers.v1.WorkersService/SubmitJob" => Some(SUBMIT),
        "acyclic.workers.v1.WorkersService/InspectJob" => Some(INSPECT),
        "acyclic.workers.v1.WorkersService/CancelJob" => Some(CANCEL),
        "acyclic.workers.v1.WorkersService/InvokeVersion" => Some(INVOKE_VERSION),
        "acyclic.workers.v1.WorkersService/InvokeDeployment" => Some(INVOKE_DEPLOYMENT),
        _ => None,
    }
}
