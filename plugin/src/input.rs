//! Native stdin and selected-task admission, before any workspace operation.
use acyclic_harness::agent::AgentPath;
use serde::de::DeserializeOwned;
use serde_json::json;
use std::io::{self, Read as _};

pub(super) type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(super) fn parse_task(value: &str) -> Result<AgentPath> {
    Ok(serde_json::from_value(json!(value))?)
}

pub(super) fn read_json<T: DeserializeOwned>(
    input: impl io::Read,
    maximum: Option<u64>,
) -> Result<T> {
    let mut bytes = Vec::new();
    let mut input = input.take(maximum.map_or(u64::MAX, |bound| bound.saturating_add(1)));
    input.read_to_end(&mut bytes)?;
    if maximum.is_some_and(|bound| bytes.len() as u64 > bound) {
        return Err(
            "step=admit native hook: input exceeds its frame bound; recovery=acyclic --help".into(),
        );
    }
    Ok(serde_json::from_slice(&bytes)?)
}
