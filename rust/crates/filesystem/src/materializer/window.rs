use super::*;
use crate::sync::Mutex;
use crate::{WorkBudget, WorkCounters, WorkError};
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
    ledger: Mutex<WindowLedger>,
    // Claimed outside every window, so charging through it never stacks two windows.
    metadata: NativeAllocation,
}

/// The only cumulative state a window's budget is derived from.
#[derive(Clone, Copy, Debug, Default)]
struct WindowLedger {
    /// Work this window charged.
    consumed: WorkCounters,
    /// Reservations this window holds and has not yet started or refunded.
    held: WorkCounters,
}

impl WindowLedger {
    /// The ledger budget that leaves this window exactly `limit - held - consumed`
    /// against the shared `work`, which already includes everything this window charged.
    fn view(&self, limit: &WorkBudget, work: &WorkCounters) -> Result<WorkBudget, WorkError> {
        limit
            .checked_add_cumulative(&work.checked_sub_cumulative(&self.consumed)?)?
            .checked_sub_cumulative(&self.held)
    }
    /// This window after an operation moved the shared work and pending reservations,
    /// with the work it charged.
    fn settle(
        &self,
        work: (&WorkCounters, &WorkCounters),
        pending: (&WorkCounters, &WorkCounters),
    ) -> Result<(Self, WorkCounters), WorkError> {
        let charged = work.1.checked_sub_cumulative(work.0)?;
        Ok((
            Self {
                consumed: self.consumed.checked_add_cumulative(&charged)?,
                held: self
                    .held
                    .checked_add_cumulative(pending.1)?
                    .checked_sub_cumulative(pending.0)?,
            },
            charged,
        ))
    }
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
    fn ledger(&self) -> crate::sync::MutexGuard<'_, WindowLedger> {
        self.ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
            ledger: Mutex::new(WindowLedger::default()),
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
    /// Inside a window every cumulative counter has exactly `limit - held - consumed`
    /// left, from that window's own state alone: work and reservations of other windows
    /// and of the unwindowed ledger are never part of its view. The shared ledger keeps
    /// only live allocation and peak. The unwindowed budget is raised by exactly what
    /// the window charged, so no other view counts it.
    ///
    /// Arithmetic is exact. A view or settlement that cannot be represented is an
    /// accounting invariant failure, which fails the ledger closed; nothing is clamped.
    pub(crate) fn within_window<R>(
        &mut self,
        window: Option<&NativeWorkWindow>,
        operation: impl FnOnce(&mut Self) -> R,
    ) -> R {
        let Some(window) = window else {
            return operation(self);
        };
        let stored = self.budget;
        let work = self.work;
        let pending = self.pending_reservations();
        let ledger = *window.ledger();
        self.budget = match ledger.view(&self.original_budget(), &work) {
            Ok(view) => view,
            Err(error) => {
                self.fail_window_invariant(error);
                work
            }
        };
        self.budget.peak_allocation_bytes = stored.peak_allocation_bytes;
        let answer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(self)));
        let peak = self.budget.peak_allocation_bytes;
        let settled = ledger
            .settle(
                (&work, &self.work),
                (&pending, &self.pending_reservations()),
            )
            .and_then(|(ledger, charged)| Ok((ledger, stored.checked_add_cumulative(&charged)?)));
        self.budget = match settled {
            Ok((settled, budget)) => {
                *window.ledger() = settled;
                budget
            }
            Err(error) => {
                self.fail_window_invariant(error);
                stored
            }
        };
        self.budget.peak_allocation_bytes = peak;
        answer.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    }
    fn fail_window_invariant(&mut self, error: WorkError) {
        self.record_native_io_failure(error.into());
        self.poison_native_original_work();
    }
}

#[cfg(test)]
mod tests;
