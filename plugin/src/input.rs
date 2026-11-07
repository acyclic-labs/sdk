//! Native stdin and selected-task admission, before any workspace operation.
use acyclic_harness::agent::AgentPath;
use serde_json::{Value, json};
use std::io::{self, Read as _};

pub(super) type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(super) fn parse_task(value: &str) -> Result<AgentPath> {
    Ok(serde_json::from_value(json!(value))?)
}

pub(super) fn read_json(input: impl io::Read) -> Result<Value> {
    let mut bytes = Vec::new();
    input.take(65_537).read_to_end(&mut bytes)?;
    if bytes.len() > 65_536 {
        return Err("native hook exceeds shared command file bound".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
