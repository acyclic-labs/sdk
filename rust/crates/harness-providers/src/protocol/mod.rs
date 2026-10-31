pub(crate) mod content;
pub(crate) mod name;
pub(crate) mod sequence;
pub(crate) mod shape;
pub(crate) mod strict;

/// Optional wire values use null and omission interchangeably. Apply before
/// alias selection or retained-state admission, not after allocating a block.
pub(crate) fn present(value: Option<&serde_json::Value>) -> Option<&serde_json::Value> {
    value.filter(|value| !value.is_null())
}
