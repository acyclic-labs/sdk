//! Independent, current-thread differential runs over the real filesystem paths.
use crate::filesystem::PreparationIo;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkCounters};
use std::cell::RefCell;

thread_local! {
    static TRACE: RefCell<Option<Vec<(&'static str, u64)>>> = const { RefCell::new(None) };
}

pub(crate) fn record(operation: &'static str, amount: u64) {
    TRACE.with_borrow_mut(|trace| {
        if let Some(trace) = trace {
            trace.push((operation, amount));
        }
    });
}

pub(crate) fn pair<S, R>(
    mut initialize: impl FnMut() -> S,
    mut run: impl FnMut(S, bool) -> R,
) -> [(R, Vec<(&'static str, u64)>); 2] {
    [true, false].map(|old| {
        let state = initialize();
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                TRACE.with_borrow_mut(|trace| *trace = None);
            }
        }
        TRACE.with_borrow_mut(|trace| {
            assert!(trace.is_none());
            *trace = Some(Vec::new());
        });
        let _reset = Reset;
        let result = run(state, old);
        (result, TRACE.with_borrow_mut(|trace| trace.take().unwrap()))
    })
}

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
