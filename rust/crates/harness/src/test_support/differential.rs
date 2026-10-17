//! Independent, current-thread differential runs over the real filesystem paths.
use crate::filesystem::PreparationIo;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkCounters};
pub(crate) use acyclic_test_support::differential::{allocation_counts, pair, record};

// Include the original native ledger and live custody on both sides of IO drop;
// copying returned bytes in `run` releases their producer custody before sampling.
pub(crate) fn accounted<R>(
    native: bool,
    maximum: u64,
    cancelled: bool,
    run: impl FnOnce(&PreparationIo) -> R,
) -> Result<(R, WorkCounters, u64, WorkCounters, u64, u64), (String, WorkCounters)> {
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000, maximum);
    let work = if native {
        Some(
            NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, receipt)| (cause.to_string(), *receipt))?,
        )
    } else {
        None
    };
    let io = match &work {
        Some(work) => PreparationIo::from_native_work(work),
        None => PreparationIo::from_budget(budget, &cancellation),
    }
    .unwrap();
    if cancelled {
        cancellation.cancel();
    }
    let result = run(&io);
    let receipt = io.receipt();
    let owned = io.owned_bytes().unwrap();
    let held = work
        .as_ref()
        .map_or(0, NativeMaterializationWork::live_allocation_bytes);
    drop(io);
    Ok((
        result,
        receipt,
        owned,
        work.as_ref()
            .map_or(WorkCounters::default(), NativeMaterializationWork::receipt),
        held,
        work.as_ref()
            .map_or(0, NativeMaterializationWork::live_allocation_bytes),
    ))
}
