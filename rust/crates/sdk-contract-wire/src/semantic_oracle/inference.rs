//! Exact semantic observations from the Rust Inference Run watch fixture.

const WATCH: &str =
    r#"{"identity_pairs":[],"cursor_trace":[0,1],"revision_trace":[],"status_trace":["queued","COMPLETED"],"transitions":[]}"#;

pub(super) fn expectation_json(rpc: &str) -> Option<&'static str> {
    match rpc {
        "inference.customer.v1.RunsService/Watch" => Some(WATCH),
        _ => None,
    }
}
