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

/// Read one bounded native hook input before workspace acquisition.
///
/// # Errors
/// Preserves read and JSON errors, and refuses oversized hook frames.
pub fn read_json(input: impl io::Read) -> Result<Value> {
    let mut bytes = Vec::new();
    input.take(65_537).read_to_end(&mut bytes)?;
    if bytes.len() > 65_536 {
        return Err("native hook exceeds shared command file bound".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
