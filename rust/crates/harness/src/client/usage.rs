//! Provisional per-turn usage reconciled with the canonical metadata file.

use crate::Result;
use serde_json::Value;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct Usage {
    committed: Value,
    pending: BTreeMap<Uuid, Value>,
}

impl Usage {
    pub(super) fn complete(&mut self, message: Uuid, metadata: Value) -> Result<Value> {
        accumulate(self.pending.entry(message).or_default(), metadata)?;
        self.total()
    }

    pub(super) fn commit(&mut self, message: Uuid, metadata: Value) -> Result<()> {
        accumulate(&mut self.committed, metadata)?;
        self.pending.remove(&message);
        Ok(())
    }

    pub(super) fn total(&self) -> Result<Value> {
        let mut total = self.committed.clone();
        for metadata in self.pending.values() {
            accumulate(&mut total, metadata.clone())?;
        }
        Ok(total)
    }
}

pub(super) fn accumulate(total: &mut Value, step: Value) -> Result<()> {
    let overflow = || crate::Error::Invalid("usage counter overflow".into());
    match (total, step) {
        (Value::Object(total), Value::Object(step)) => {
            for (key, value) in step {
                accumulate(total.entry(key).or_insert(Value::Null), value)?;
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
