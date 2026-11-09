//! Native stdin and selected-task admission, before any workspace operation.
use acyclic_harness::agent::AgentPath;
use serde_json::{Value, json};
use std::io::{self, Read as _};

/// Admission result retaining the original parser or operating-system error.
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Parse a selected task through the shared agent path contract.
///
/// # Errors
/// Returns the original agent path admission error for an invalid task.
pub fn parse_task(value: &str) -> Result<AgentPath> {
    Ok(serde_json::from_value(json!(value))?)
}

/// Admit a selected task without treating invalid native encoding as absence.
///
/// # Errors
/// Refuses non-UTF-8 identities and preserves canonical task admission errors.
pub fn parse_native_task(value: &std::ffi::OsStr) -> Result<AgentPath> {
    parse_task(value.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "step=admit selected native task: native task identity is not UTF-8; recovery: set ACYCLIC_TASK to a UTF-8 agent identity or unset it",
        )
    })?)
}

/// Read native JSON before workspace acquisition, with an optional frame bound.
///
/// # Errors
/// Preserves read and JSON errors, and refuses oversized hook frames.
pub fn read_json(input: impl io::Read, maximum: Option<u64>) -> Result<Value> {
    let mut bytes = Vec::new();
    input
        .take(maximum.map_or(u64::MAX, |bound| bound.saturating_add(1)))
        .read_to_end(&mut bytes)?;
    if maximum.is_some_and(|bound| bytes.len() as u64 > bound) {
        return Err("native hook exceeds shared command file bound".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
