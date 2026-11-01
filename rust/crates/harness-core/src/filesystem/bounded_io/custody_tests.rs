//! Owned outputs share one ledger through both preparation observer adapters.
use super::*;
use crate::store::history::{HistoryIo, HistoryWork};

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
    let mut reservation = io.reserve_decode(8)?;
    cancellation.cancel();
    assert!(reservation.take_custody().is_err());
    assert_eq!(reservation.bytes(), 8);
    assert_eq!(io.owned_bytes()?, 8);
    drop(reservation);
    assert_eq!(io.owned_bytes()?, 0);
    Ok(())
}

#[test]
fn original_index_custody_survives_issuer_drop_and_refused_mutation() -> Result<()> {
    use acyclic_collections::FixedIndex;

    let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
    let cancellation = CancellationToken::new();
    let io = PreparationIo::from_budget(budget, &cancellation)?;
    let PreparationIo::Owned { work, .. } = &io else {
        unreachable!("standalone constructor")
    };
    let original = Arc::clone(work);
    let mut index = FixedIndex::<u64, PreparationCustody>::default();
    index.get_or_try_insert_with([0; 16], &mut &io, |_| Ok(7))?;
    assert_eq!(io.owned_bytes()?, 0);
    let retained = original.lock().unwrap().1.as_ref().unwrap().live_bytes();
    assert!(retained > 0);
    drop(io);

    // Each attempted mutation uses its own actual caller frame. Refusal cannot
    // return a predecessor's storage or leave any of this frame's nodes behind.
    for allocations in 0..=2 {
        let attempt = PreparationIo::from_budget(
            WorkBudget {
                allocation_operations: allocations,
                ..budget
            },
            &cancellation,
        )?;
        let mut constructed = false;
        let refused = index.get_or_try_insert_with([255; 16], &mut &attempt, |_| {
            constructed = true;
            Err(Error::Invalid("original constructor refusal".into()))
        });
        assert!(refused.is_err());
        assert_eq!(constructed, allocations == 2);
        assert_eq!(attempt.owned_bytes()?, 0);
        assert_eq!(
            attempt.remaining()?.peak_allocation_bytes,
            budget.peak_allocation_bytes
        );
        assert_eq!(
            original.lock().unwrap().1.as_ref().unwrap().live_bytes(),
            retained
        );
    }

    let reader = PreparationIo::from_budget(budget, &cancellation)?;
    assert_eq!(index.get([0; 16], &mut &reader)?, Some(&7));
    assert_eq!(index.get([255; 16], &mut &reader)?, None);
    assert_eq!(reader.receipt().allocation_operations, 0);
    let exhausted = PreparationIo::from_budget(
        WorkBudget {
            items_examined: 0,
            ..budget
        },
        &cancellation,
    )?;
    assert!(index.get([0; 16], &mut &exhausted).is_err());
    assert_eq!(exhausted.receipt().items_examined, 0);
    cancellation.cancel();
    assert!(index.get([0; 16], &mut &reader).is_err());
    let before = reader.receipt();
    drop(index);
    assert_eq!(reader.receipt(), before);
    assert_eq!(original.lock().unwrap().1.as_ref().unwrap().live_bytes(), 0);

    #[cfg(not(target_arch = "wasm32"))]
    {
        let cancellation = CancellationToken::new();
        let native = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = native.live_allocation_bytes();
        let io = PreparationIo::from_native_work(&native)?;
        let before = native.receipt().allocation_operations;
        let mut index = FixedIndex::<u64, PreparationCustody>::default();
        for key in 0..3 {
            index.get_or_try_insert_with([key; 16], &mut &io, |_| Ok(u64::from(key)))?;
        }
        assert_eq!(native.receipt().allocation_operations - before, 5);
        drop(io);
        assert!(native.live_allocation_bytes() > baseline);
        let reader = PreparationIo::from_native_work(&native)?;
        cancellation.cancel();
        assert!(matches!(
            index.get([0; 16], &mut &reader),
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            })
        ));
        let before = native.receipt();
        assert!(index.retire_if([0; 16], |_| true));
        drop(index);
        assert_eq!(native.receipt(), before);
        assert_eq!(native.live_allocation_bytes(), baseline);
    }
    Ok(())
}
