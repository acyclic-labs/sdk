//! Fixed-size projection of original transport waits; no history or payload reads.
use super::{ExecutionEvent, ExecutionRecord, OperationId, UsageTotal};
use crate::{Error, Result, model::ModelWait};
use serde::{Deserialize, Serialize};

/// Disjoint wait observations from one source page or accumulated query.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct WaitUsage {
    /// Original timer starts, including timers without a successful finish.
    pub started: u64,
    /// Original successful timer finishes.
    pub completed: u64,
    /// Completed timers' measured elapsed nanoseconds. A value that cannot fit
    /// is unknown. When started differs from completed, this omits unfinished
    /// timers and must not be presented as the entire waiting time.
    pub completed_ns: UsageTotal,
}

impl WaitUsage {
    /// Entire waiting time only when every start finished and every duration
    /// fits the counter. Never present an unfinished timer as zero waiting time.
    pub fn elapsed_ns(self) -> Option<u64> {
        (self.started == self.completed)
            .then(|| self.completed_ns.value())
            .flatten()
    }
    /// Whether the source page contains no wait facts.
    pub fn is_empty(&self) -> bool {
        self.started == 0 && self.completed == 0
    }

    /// Disjoint source count, refusing overflow or noncanonical elapsed values.
    pub fn observations(&self) -> Option<u64> {
        if self.completed_ns.unknown > self.completed
            || (self.completed == self.completed_ns.unknown && self.completed_ns.known != 0)
        {
            return None;
        }
        self.started.checked_add(self.completed)
    }

    /// Combine disjoint pages atomically, retaining no call identities.
    pub fn merged(self, page: Self) -> Result<Self> {
        Ok(Self {
            started: self
                .started
                .checked_add(page.started)
                .ok_or_else(overflow)?,
            completed: self
                .completed
                .checked_add(page.completed)
                .ok_or_else(overflow)?,
            completed_ns: self
                .completed_ns
                .combine(page.completed_ns)
                .map_err(|_| overflow())?,
        })
    }

    pub(super) fn observe(
        &mut self,
        operation: OperationId,
        record: &ExecutionRecord,
    ) -> Result<()> {
        let ExecutionEvent::ModelWait { observation, .. } = &record.event else {
            return Ok(());
        };
        if record.operation_id != operation || record.sequence == 0 {
            return Err(Error::Storage(
                "wait inspection has a foreign operation or zero source sequence".into(),
            ));
        }
        match observation {
            ModelWait::Started { .. } => {
                self.started = self.started.checked_add(1).ok_or_else(overflow)?;
            }
            ModelWait::Finished { elapsed } => {
                self.completed = self.completed.checked_add(1).ok_or_else(overflow)?;
                self.completed_ns
                    .push(u64::try_from(elapsed.as_nanos()).ok())
                    .map_err(|_| overflow())?;
            }
        }
        Ok(())
    }
}

fn overflow() -> Error {
    Error::Storage("wait inspection counters overflow".into())
}
