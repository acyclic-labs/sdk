//! Compact tool views from the original journal, never a second fact store.
use super::{
    ExecutionEvent, ExecutionJournal, ExecutionRecord, OperationId, ToolErrorClass, ToolRecordKind,
};
use crate::live::feedback::Kind as FeedbackKind;
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
    /// Authored report categories from the same successful completions. These
    /// are agent reports, not measured failures or estimates of actual usage.
    pub feedback: BTreeMap<FeedbackKind, u64>,
}

impl ToolUsage {
    /// Whether this source page contains no tool observations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.started == 0
            && self.completed == 0
            && self.failures.is_empty()
            && self.feedback.is_empty()
    }

    /// Number of disjoint original source records, or unknown on overflow or a
    /// noncanonical zero-count failure entry. Used to validate source-page bounds.
    pub fn observations(&self) -> Option<u64> {
        // A single successful write can retain any number of authored reports;
        // these category totals are not counts of source completion records.
        let reports = category_total(self.feedback.values())?;
        if reports != 0 && self.completed == 0 {
            return None;
        }
        self.started
            .checked_add(self.completed)?
            .checked_add(category_total(self.failures.values())?)
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
        merge_categories(&mut next.failures, &page.failures)?;
        merge_categories(&mut next.feedback, &page.feedback)?;
        Ok(next)
    }

    pub(super) async fn observe(
        &mut self,
        journal: &dyn ExecutionJournal,
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
            ExecutionEvent::ToolCompleted {
                result,
                record_kind,
                ..
            } => {
                self.completed = self.completed.checked_add(1).ok_or_else(overflow)?;
                if *record_kind == Some(ToolRecordKind::Feedback) {
                    // Classification comes from the trusted completion producer.
                    // Unrelated outputs never cause a payload or invocation read.
                    let bytes = journal.load(result, None)?.await?;
                    let result: crate::tool::ToolResult = crate::contract::json_from_slice(&bytes)
                        .map_err(|_| {
                            Error::Storage("invalid original feedback tool result".into())
                        })?;
                    let report: crate::tool::files::RecordResult =
                        serde_json::from_value(result.value).map_err(|_| {
                            Error::Storage("invalid original feedback result".into())
                        })?;
                    report
                        .validate()
                        .map_err(|_| Error::Storage("invalid original feedback report".into()))?;
                    for report in report.reports {
                        let count = self.feedback.entry(report.kind).or_default();
                        *count = count.checked_add(1).ok_or_else(overflow)?;
                    }
                }
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

fn category_total<'a>(mut counts: impl Iterator<Item = &'a u64>) -> Option<u64> {
    counts.try_fold(0u64, |total, count| {
        if *count == 0 {
            None
        } else {
            total.checked_add(*count)
        }
    })
}

fn merge_categories<K: Copy + Ord>(
    target: &mut BTreeMap<K, u64>,
    page: &BTreeMap<K, u64>,
) -> Result<()> {
    for (kind, count) in page {
        let prior = target.entry(*kind).or_default();
        *prior = prior.checked_add(*count).ok_or_else(overflow)?;
    }
    Ok(())
}

fn overflow() -> Error {
    Error::Storage("tool inspection counters overflow".into())
}
