//! Compact tool views from the original journal, never a second fact store.
use super::{ExecutionEvent, ExecutionRecord, OperationId, ToolErrorClass};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Disjoint original tool observations, either one page or an accumulated view.
/// Storage is independent of call count: failure keys are the finite typed
/// classes, and no invocation, result, error text or call history is retained.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ToolUsage {
    /// Original dispatch admissions. A pre-dispatch refusal has no admission.
    pub started: u64,
    /// Original successful settlements, excluding failed calls.
    pub completed: u64,
    /// Original settled failures grouped by their credential-free cause class.
    pub failures: BTreeMap<ToolErrorClass, u64>,
}

impl ToolUsage {
    /// Whether this source page contains no tool observations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.started == 0 && self.completed == 0 && self.failures.is_empty()
    }

    /// Number of disjoint original source records, or unknown on overflow or a
    /// noncanonical zero-count failure entry. Used to validate source-page bounds.
    pub fn observations(&self) -> Option<u64> {
        self.failures.values().try_fold(
            self.started.checked_add(self.completed)?,
            |total, count| {
                if *count == 0 {
                    None
                } else {
                    total.checked_add(*count)
                }
            },
        )
    }

    /// Project the next view from a disjoint page without changing this view.
    pub fn merged(&self, page: &Self) -> Result<Self> {
        let mut next = self.clone();
        next.started = next
            .started
            .checked_add(page.started)
            .ok_or_else(overflow)?;
        next.completed = next
            .completed
            .checked_add(page.completed)
            .ok_or_else(overflow)?;
        for (class, count) in &page.failures {
            let prior = next.failures.entry(*class).or_default();
            *prior = prior.checked_add(*count).ok_or_else(overflow)?;
        }
        Ok(next)
    }

    pub(super) fn observe(
        &mut self,
        operation: OperationId,
        record: &ExecutionRecord,
    ) -> Result<()> {
        if matches!(
            &record.event,
            ExecutionEvent::ToolStarted { .. }
                | ExecutionEvent::ToolCompleted { .. }
                | ExecutionEvent::ToolFailed { .. }
        ) && (record.operation_id != operation || record.sequence == 0)
        {
            return Err(Error::Storage(
                "tool inspection has a foreign operation or zero source sequence".into(),
            ));
        }
        match &record.event {
            ExecutionEvent::ToolStarted { .. } => {
                self.started = self.started.checked_add(1).ok_or_else(overflow)?;
            }
            ExecutionEvent::ToolCompleted { .. } => {
                self.completed = self.completed.checked_add(1).ok_or_else(overflow)?;
            }
            ExecutionEvent::ToolFailed { cause, .. } => {
                let count = self.failures.entry(cause.class).or_default();
                *count = count.checked_add(1).ok_or_else(overflow)?;
            }
            _ => {}
        }
        Ok(())
    }
}

fn overflow() -> Error {
    Error::Storage("tool inspection counters overflow".into())
}
