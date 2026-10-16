//! Provisional per-turn usage reconciled with the canonical metadata file.

use crate::{Result, model::accumulate_usage};
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
        accumulate_usage(self.pending.entry(message).or_default(), metadata)?;
        self.total()
    }

    pub(super) fn commit(&mut self, message: Uuid, metadata: Value) -> Result<()> {
        accumulate_usage(&mut self.committed, metadata)?;
        self.pending.remove(&message);
        Ok(())
    }

    pub(super) fn total(&self) -> Result<Value> {
        let mut total = self.committed.clone();
        for metadata in self.pending.values() {
            accumulate_usage(&mut total, metadata.clone())?;
        }
        Ok(total)
    }
}
