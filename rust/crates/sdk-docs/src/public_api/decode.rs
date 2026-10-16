use serde::Deserialize;

pub(super) fn from_value(
    value: &serde_json::Value,
) -> Result<rustdoc_types::Crate, serde_json::Error> {
    rustdoc_types::Crate::deserialize(value)
}
