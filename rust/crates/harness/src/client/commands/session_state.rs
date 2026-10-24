use super::*;
use std::{ops::Deref, sync::OnceLock};

/// The parsed value shares the accepted file's lifetime, including eviction.
/// Parsing stays lazy so non-JSON files and invalid optional state retain their
/// existing read-time semantics.
#[derive(Debug)]
pub(super) struct StateFile {
    pub value: files::FileValue,
    json: OnceLock<std::result::Result<Value, String>>,
}

impl StateFile {
    pub fn new(value: files::FileValue) -> Self {
        Self {
            value,
            json: OnceLock::new(),
        }
    }

    pub fn json(&self) -> Result<&Value> {
        self.json
            .get_or_init(|| {
                serde_json::from_slice(&self.value.bytes).map_err(|error| error.to_string())
            })
            .as_ref()
            .map_err(|message| Error::Invalid(message.clone()))
    }
}

impl Deref for StateFile {
    type Target = files::FileValue;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
#[path = "session_state/tests.rs"]
mod tests;
