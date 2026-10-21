// The expression and its trace form one operation, so omitting a preflight
// also omits its observed invocation. Production builds contain only the call.
macro_rules! operation {
    ($phase:ident, $delta:expr, $call:expr) => {{
        #[cfg(test)]
        #[cfg(all(debug_assertions, not(target_arch = "wasm32")))]
        crate::accounting_trace::before(crate::accounting_trace::Phase::$phase, &$delta);
        let result = $call;
        #[cfg(test)]
        #[cfg(all(debug_assertions, not(target_arch = "wasm32")))]
        crate::accounting_trace::after(crate::accounting_trace::Phase::$phase, &$delta, &result);
        result
    }};
}

use crate::sync::SyncMutex;
use acyclic_fs::kernel::{KernelAccounting, allocation::AllocationLedger};
use acyclic_fs::{CancellationToken, FsError, WorkBudget, WorkCounters, WorkError};
use std::collections::BTreeMap;

pub(super) fn conflict<V>(
    paths: &BTreeMap<String, V>,
    path: &str,
    budget: WorkBudget,
    work: &SyncMutex<(WorkCounters, Result<AllocationLedger, WorkError>)>,
    cancellation: &CancellationToken,
    conflicts: impl Fn(&str, &str) -> bool,
) -> Result<bool, FsError> {
    // Empty scans do not touch accounting or cancellation, including poison.
    if paths.is_empty() {
        return Ok(false);
    }
    // Lock order: this owned ledger is held alone. The borrowed kernel context
    // only checks the cancellation flag and updates this protected state; it
    // never enters another preparation or native ledger lock.
    let mut state = work
        .lock()
        .map_err(|_| FsError::PendingMutationAllocationFailed)?;
    let (spent, allocations) = &mut *state;
    let allocations = allocations
        .as_mut()
        .map_err(|error| FsError::from(*error))?;
    let live = allocations.live_bytes();
    let mut accounting =
        KernelAccounting::borrowed_standalone(allocations, spent, budget, cancellation);
    for existing in paths.keys() {
        let examine = WorkCounters {
            items_examined: 1,
            source_bytes_read: (existing.len() + path.len()) as u64,
            peak_allocation_bytes: live,
            ..Default::default()
        };
        operation!(Preflight, examine, accounting.admit(examine))?;
        operation!(Charge, examine, accounting.charge(examine))?;
        if conflicts(existing, path) {
            return Ok(true);
        }
    }
    Ok(false)
}
