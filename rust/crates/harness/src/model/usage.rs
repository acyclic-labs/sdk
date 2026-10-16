//! Shared provider-unit accumulation for live views and canonical turn metadata.

use crate::Result;
use serde_json::Value;

pub(crate) fn accumulate_usage(total: &mut Value, step: Value) -> Result<()> {
    let overflow = || crate::Error::Invalid("usage counter overflow".into());
    match (total, step) {
        // Providers may omit usage on a step; absence cannot erase prior units.
        (_, Value::Null) => {}
        (Value::Object(total), Value::Object(step)) => {
            for (key, value) in step {
                accumulate_usage(total.entry(key).or_insert(Value::Null), value)?;
            }
        }
        (Value::Number(total), Value::Number(step)) => {
            *total = if let (Some(left), Some(right)) = (total.as_u64(), step.as_u64()) {
                left.checked_add(right).ok_or_else(overflow)?.into()
            } else if let (Some(left), Some(right)) = (total.as_i64(), step.as_i64()) {
                left.checked_add(right).ok_or_else(overflow)?.into()
            } else {
                serde_json::Number::from_f64(
                    total.as_f64().ok_or_else(overflow)? + step.as_f64().ok_or_else(overflow)?,
                )
                .ok_or_else(overflow)?
            };
        }
        (total, step) => *total = step,
    }
    Ok(())
}
