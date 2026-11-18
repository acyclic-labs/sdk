//! Shared provider wire shapes, names, and sequence admission.

pub mod content;
pub mod gemini;
pub mod name;
pub mod sequence;
pub mod shape;

/// Optional wire values use null and omission interchangeably. Apply before
/// alias selection or retained-state admission, not after allocating a block.
pub fn present(value: Option<&serde_json::Value>) -> Option<&serde_json::Value> {
    value.filter(|value| !value.is_null())
}
