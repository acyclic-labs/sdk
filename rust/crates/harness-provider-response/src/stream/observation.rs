//! Read-only parser observations for cross-crate conformance cases.

use serde_json::{Map, Value};

use super::State;

/// Coupled state observed after one frame.
pub struct Observation<'a> {
    /// Number of retained tool calls.
    pub calls: usize,
    /// Number of retained reasoning blocks.
    pub thinking: usize,
    /// Bytes charged to the retained state.
    pub retained_bytes: usize,
    /// Metadata admitted so far.
    pub metadata: &'a Map<String, Value>,
    /// The latest completed message, if one has been admitted.
    pub completed_message: Option<&'a (u64, String)>,
}

/// The admitted identity and argument bytes of one tool call.
pub struct CallObservation<'a> {
    /// Canonical provider call identifier.
    pub id: &'a str,
    /// Provider output item identifier.
    pub item_id: &'a str,
    /// Name carried by the provider.
    pub name: &'a str,
    /// Retained argument text.
    pub arguments: &'a str,
}

impl State<'_> {
    /// Observe the coupled state without admitting or allocating more input.
    pub fn observe(&self) -> Observation<'_> {
        Observation {
            calls: self.calls.len(),
            thinking: self.thinking.len(),
            retained_bytes: self.budget.used(),
            metadata: &self.metadata,
            completed_message: self.completed_message.as_ref(),
        }
    }

    /// Observe one retained call without admitting more input.
    pub fn observe_call(&self, index: u64) -> Option<CallObservation<'_>> {
        let call = self.calls.get(&index)?;
        Some(CallObservation {
            id: &call.id,
            item_id: &call.item_id,
            name: &call.name,
            arguments: &call.arguments,
        })
    }
}
