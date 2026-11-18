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
) -> Result<(R, WorkCounters, u64, WorkCounters, u64, u64), (String, WorkCounters)> {
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs_work::performance::work_budget(1_000_000, maximum);
    let work = if native {
        Some(
            NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, receipt)| (cause.to_string(), *receipt))?,
        )
    } else {
        None
    };
    let native_receipt = || {
        work.as_ref()
            .map_or_else(WorkCounters::default, NativeMaterializationWork::receipt)
    };
    let live_native = || match &work {
        Some(work) => work
            .live_allocation_bytes()
            .map_err(|error| (error.to_string(), work.receipt())),
        None => Ok(0),
    };
    let io = match &work {
        Some(work) => PreparationIo::from_native_work(work),
        None => PreparationIo::from_budget(budget, &cancellation),
    }
    .map_err(|error| (error.to_string(), native_receipt()))?;
    if cancelled {
        cancellation.cancel();
    }
    let result = run(&io);
    let receipt = io.receipt();
    let owned = io
        .owned_bytes()
        .map_err(|error| (error.to_string(), receipt))?;
    let held = live_native()?;
    drop(io);
    Ok((
        result,
        receipt,
        owned,
        native_receipt(),
        held,
        live_native()?,
    ))
}
