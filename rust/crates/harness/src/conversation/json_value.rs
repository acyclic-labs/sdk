use crate::{Result, executor::json::JsonCodecCharge, store::history::HistoryIo};
use serde_json::Value;

/// The portable rule every recorded tool artifact satisfies: no reserved
/// serde marker key, and only numbers a JavaScript Number round-trips exactly.
/// Provider streams apply it to a decoded model tool call, so a call the stream
/// emits is never rejected later when the call is recorded.
pub fn validate_portable_json(value: &Value) -> Result<()> {
    validate_json_value(value, &mut None)
}

pub(crate) fn validate_json_value(
    value: &Value,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    if let Some(io) = work.as_deref_mut() {
        io.codec_charge(JsonCodecCharge::Work { bytes: 0, items: 1 })?;
    }
    match value {
        Value::Array(values) => values
            .iter()
            .try_for_each(|value| validate_json_value(value, work)),
        Value::Object(values) => {
            if values.contains_key("$serde_json::private::RawValue")
                || values.contains_key("$serde_json::private::Number")
            {
                return Err(crate::store::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "tool artifact contains a reserved JSON marker",
                ));
            }
            values
                .values()
                .try_for_each(|value| validate_json_value(value, work))
        }
        Value::Number(number)
            if !super::is_exact_js_integer(number)
                || number
                    .as_f64()
                    .is_some_and(|value| !super::is_exact_js_number(value)) =>
        {
            Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "tool artifact contains an inexact number",
            ))
        }
        _ => Ok(()),
    }
}
