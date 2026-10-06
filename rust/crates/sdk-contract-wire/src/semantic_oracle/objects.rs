//! Exact semantic observations from the Rust Objects fixture server.

const GET_OBJECT: &str = r#"{"identity_pairs":[],"cursor_trace":[],"revision_trace":[],"status_trace":["ok"],"transitions":[]}"#;

pub(super) fn expectation_json(rpc: &str) -> Option<&'static str> {
    match rpc {
        "acyclic.objects.v2.ObjectsService/GetObject" => Some(GET_OBJECT),
        _ => None,
    }
}
