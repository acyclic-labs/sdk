//! Exact semantic observations from the Rust process-local Machines fixture.

const CREATE: &str = r#"{"identity_pairs":[],"cursor_trace":[],"revision_trace":[],"status_trace":["RUNNING"],"transitions":[]}"#;

pub(super) fn expectation_json(rpc: &str) -> Option<&'static str> {
    match rpc {
        "acyclic.machines.v1.MachinesService/Create" => Some(CREATE),
        _ => None,
    }
}
