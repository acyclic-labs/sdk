//! Exact semantic observations from the Rust MemoryStream append/read fixture.

const APPEND: &str = r#"{"identity_pairs":[],"cursor_trace":[0,1],"revision_trace":[],"status_trace":["committed"],"transitions":[]}"#;
const READ: &str = r#"{"identity_pairs":[],"cursor_trace":[0,1],"revision_trace":[],"status_trace":["ok"],"transitions":[]}"#;

pub(super) fn expectation_json(rpc: &str) -> Option<&'static str> {
    match rpc {
        "acyclic.stream.v2.StreamService/Append" => Some(APPEND),
        "acyclic.stream.v2.StreamService/Read" => Some(READ),
        _ => None,
    }
}
