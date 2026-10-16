use crate::{Result, executor::json::JsonCodecCharge, store::history::HistoryIo};
use serde_json::Value;

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
