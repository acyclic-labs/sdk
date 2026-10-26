//! Owned outputs share one ledger through both preparation observer adapters.
use super::*;
use crate::store::history::{HistoryIo, HistoryWork};

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_recovery_keeps_custody_and_charges_the_current_window() -> Result<()> {
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(8, 1_000_000);
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let custody = work.claim(8, 1, WorkCounters::default())?;
    work.charge(WorkCounters {
        items_examined: 7,
        ..Default::default()
    })?;
    let window = work.invocation_window()?;
    let foreign = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let custody = PreparationIo::from_native_custody_with_work(custody, &foreign)
        .err()
        .ok_or_else(|| Error::Invalid("foreign ledger adopted recovery custody".into()))?;
    assert_eq!(custody.bytes(), 8);
    assert!(custody.work().shares_allocation_context(&work));
    let io = PreparationIo::from_native_custody_with_work(custody, &window).map_err(|custody| {
        Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            custody: Some(custody),
        }
    })?;
    io.charged(WorkCounters {
        items_examined: 2,
        ..Default::default()
    })?;
    assert_eq!(window.remaining()?.items_examined, 6);
    assert_eq!(work.remaining()?.items_examined, 1);
    assert_eq!(work.receipt().items_examined, 9);
    assert_eq!(io.owned_bytes()?, 8);
    cancellation.cancel();
    let before = window.receipt();
    assert!(io.charged(WorkCounters::default()).is_err());
    assert_eq!(window.receipt(), before);
    let retained = work.live_allocation_bytes();
    drop(io);
    assert_eq!(work.live_allocation_bytes(), retained - 8);
    Ok(())
}

#[test]
fn output_custody_outlives_preparation_observer() -> Result<()> {
    output_custody_outlives_observer(false)
}

#[test]
fn output_custody_outlives_measured_history_observer() -> Result<()> {
    output_custody_outlives_observer(true)
}

fn output_custody_outlives_observer(measured: bool) -> Result<()> {
    let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
    let cancellation = CancellationToken::new();
    let io = PreparationIo::from_budget(budget, &cancellation)?;
    let attempt = io.scoped()?;
    attempt.retain_owned(8)?;
    assert!(attempt.take_owned_custody(9).is_err());
    assert_eq!(attempt.owned_bytes()?, 8);
    let output = if measured {
        let mut observer = HistoryWork::new(attempt);
        HistoryIo::take_owned_custody(&mut observer, 8)?
    } else {
        let output = HistoryIo::take_owned_custody(&mut attempt.history_io(), 8)?;
        assert_eq!(attempt.owned_bytes()?, 0);
        output
    }
    .ok_or_else(|| io.invalid("preparation observer must transfer custody"))?;
    assert_eq!(io.owned_bytes()?, 0);
    assert_eq!(
        io.remaining()?.peak_allocation_bytes,
        budget.peak_allocation_bytes - 8
    );
    cancellation.cancel();
    drop(output);
    // Retirement must still work after cancellation and observer destruction.
    let live = match &io {
        PreparationIo::Owned { work, .. } => work
            .lock()
            .map_err(|_| io.invalid("fixture ledger is poisoned"))?
            .1
            .as_ref()
            .map_err(|error| acyclic_fs::FsError::from(*error))?
            .live_bytes(),
        #[cfg(not(target_arch = "wasm32"))]
        PreparationIo::Native { .. } => unreachable!("standalone fixture"),
    };
    assert_eq!(live, 0);
    Ok(())
}

#[test]
fn cancelled_custody_transfer_preserves_the_current_owner() -> Result<()> {
    let cancellation = CancellationToken::new();
    let io = PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(1_000_000, 1_000_000),
        &cancellation,
    )?;
    io.retain_owned(8)?;
    cancellation.cancel();
    assert!(io.take_owned_custody(8).is_err());
    assert_eq!(io.owned_bytes()?, 8);
    Ok(())
}
