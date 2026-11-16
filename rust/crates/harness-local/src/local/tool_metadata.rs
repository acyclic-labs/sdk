//! Terminal metadata and captured output have one representation each.
use super::*;

pub(super) fn encode(value: &serde_json::Map<String, serde_json::Value>) -> Result<Vec<u8>> {
    let metadata: std::collections::BTreeMap<_, _> = value
        .iter()
        .filter(|(key, _)| !matches!(key.as_str(), "stdout" | "stderr"))
        .collect();
    serde_json::to_vec(&metadata).map_err(storage)
}
