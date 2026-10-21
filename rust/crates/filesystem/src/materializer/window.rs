use super::*;
use crate::sync::Mutex;
use crate::{WorkBudget, WorkCounters};
use std::sync::Arc;

/// Cumulative work one invocation charged to a shared ledger.
///
/// A long-lived ledger keeps custody, retention and live allocations for a whole
/// session, but each invocation (one run, one resume, one effect) is checked against
/// the full original budget from zero. Live allocation stays a property of the shared
/// ledger: a window never forgets bytes that are still held.
#[derive(Debug)]
pub(crate) struct NativeWorkWindow {
    // Locked only under the original leaf, which serializes every window of the ledger.
    consumed: Mutex<WorkCounters>,
    // Claimed outside every window, so charging through it never stacks two windows.
    metadata: NativeAllocation,
}

impl NativeWorkWindow {
    /// The shared leaf seen from this window, for native I/O metered by the plain ledger.
    pub(super) fn with_original_leaf<T>(
        &self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        self.metadata
            .work()
            .state(|state| state.within_window(Some(self), operation))
    }
    fn consumed(&self) -> WorkCounters {
        *self
            .consumed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn consume(&self, delta: &WorkCounters) {
        let mut consumed = self
            .consumed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *consumed = consumed.saturating_add_cumulative(delta);
    }
}

impl NativeMaterializationWork {
    /// Opens a fresh accounting window on the same ledger.
    ///
    /// Cumulative counters charged through the returned context are checked against the
    /// original budget from zero, and neither the enclosing window nor the ledger's
    /// unwindowed work is charged for them. Custody, retention and live allocation stay on
    /// the shared ledger, so peak allocation is still checked across every window.
    pub fn invocation_window(&self) -> Result<Self, NativeTreeMaterializationError> {
        let bytes = const {
            match crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<NativeWorkWindow>())
            {
                Ok(bytes) => bytes as u64,
                Err(_) => panic!("work window Arc layout overflow"),
            }
        };
        let mut base = self.allocation_work();
        base.window = None;
        let metadata = base.claim(bytes, 1, WorkCounters::UNCHARGED)?;
        let mut work = self.allocation_work();
        work.window = Some(Arc::new(NativeWorkWindow {
            consumed: Mutex::new(WorkCounters::UNCHARGED),
            metadata,
        }));
        Ok(work)
    }
}

impl NativeMaterializationWork {
    /// The same context outside any invocation window, for a ledger retained across
    /// invocations: work charged through it later is never attributed to this one.
    #[must_use]
    pub fn outside_window(&self) -> Self {
        let mut work = self.clone();
        work.window = None;
        work
    }
}

impl crate::demand::DemandWork {
    /// Runs `operation` with the budget seen from `window`.
    ///
    /// Inside a window the cumulative limit is `budget + unwindowed - consumed`: work
    /// charged outside this window is excluded, and this window's own work counts in
    /// full. Work charged inside a window is excluded from every other view by raising
    /// the stored budget by the same amount. Any budget change the operation makes
    /// itself, such as a reservation or refund, is carried back exactly.
    pub(crate) fn within_window<R>(
        &mut self,
        window: Option<&NativeWorkWindow>,
        operation: impl FnOnce(&mut Self) -> R,
    ) -> R {
        let Some(window) = window else {
            return operation(self);
        };
        let consumed = window.consumed();
        let before = self.work;
        let unwindowed = before.saturating_sub_cumulative(&self.windowed);
        let stored = self.budget;
        let viewed = stored
            .saturating_add_cumulative(&unwindowed)
            .saturating_sub_cumulative(&consumed);
        self.budget = viewed;
        let answer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(self)));
        let charged = self.work.saturating_sub_cumulative(&before);
        let changed = self.budget;
        self.budget = stored
            .shifted_cumulative(&viewed, &changed)
            .saturating_add_cumulative(&charged);
        self.windowed = self.windowed.saturating_add_cumulative(&charged);
        window.consume(&charged);
        answer.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    }
}

impl WorkBudget {
    /// `self + (to - from)` per cumulative counter, saturating at both ends; the peak
    /// allocation bound follows `to`.
    fn shifted_cumulative(&self, from: &Self, to: &Self) -> Self {
        let mut shifted = self
            .saturating_add_cumulative(&to.saturating_sub_cumulative(from))
            .saturating_sub_cumulative(&from.saturating_sub_cumulative(to));
        shifted.peak_allocation_bytes = to.peak_allocation_bytes;
        shifted
    }
}

#[cfg(test)]
mod tests;
