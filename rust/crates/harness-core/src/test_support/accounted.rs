//! Native ledger and custody observations for differential runs.
use crate::filesystem::PreparationIo;
use {
    acyclic_fs::{CancellationToken, NativeMaterializationWork},
    acyclic_fs_work::performance::WorkCounters,
};

// Include the original native ledger and live custody on both sides of IO drop;
// copying returned bytes in `run` releases their producer custody before sampling.
pub(crate) fn accounted<R>(
    native: bool,
    maximum: u64,
    cancelled: bool,
    run: impl FnOnce(&PreparationIo) -> R,
) -> Result<(R, WorkCounters, u64, WorkCounters, u64, u64), (String, Option<WorkCounters>)> {
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs_work::performance::work_budget(1_000_000, maximum);
    let work = if native {
        Some(
            NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, receipt)| (cause.to_string(), Some(*receipt)))?,
        )
    } else {
        None
    };
    let observe_native = || {
        work.as_ref()
            .map(NativeMaterializationWork::live_allocation_bytes)
            .transpose()
            .map(Option::unwrap_or_default)
    };
    let io = match &work {
        Some(work) => PreparationIo::from_native_work(work),
        None => PreparationIo::from_budget(budget, &cancellation),
    }
    .map_err(|error| (error.to_string(), None))?;
    if cancelled {
        cancellation.cancel();
    }
    let result = run(&io);
    let receipt = io.receipt().map_err(|error| (error.to_string(), None))?;
    let observed = |error: crate::Error| (error.to_string(), Some(receipt));
    let owned = io.owned_bytes().map_err(observed)?;
    let held = observe_native().map_err(|error| observed(error.into()))?;
    drop(io);
    let native_receipt = work
        .as_ref()
        .map(NativeMaterializationWork::receipt)
        .transpose()
        .map_err(|error| observed(error.into()))?
        .unwrap_or_default();
    let retired = observe_native().map_err(|error| observed(error.into()))?;
    Ok((result, receipt, owned, native_receipt, held, retired))
}
