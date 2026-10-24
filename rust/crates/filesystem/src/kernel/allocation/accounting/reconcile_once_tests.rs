use super::tests::{amount, counters, status};
use super::*;
use proptest::prelude::*;

/// Runs `operation`, returning its leaf locks and registry reconciliations.
fn probed<T>(
    root: &crate::NativeMaterializationWork,
    operation: impl FnOnce() -> T,
) -> (T, usize, usize) {
    let before = crate::demand::RECONCILES.with(std::cell::Cell::get);
    let mut observed = None;
    let heap = acyclic_test_support::measure(|| {
        observed = Some(root.state.count_locks(operation));
    });
    assert_eq!(heap.count_total, 0, "native accounting must not allocate");
    let (result, locks) = observed.unwrap();
    let reconciles = crate::demand::RECONCILES.with(std::cell::Cell::get) - before;
    (result, locks, reconciles)
}

/// The replaced native sequences: the independent baseline.
fn old_admit(
    work: &crate::NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
    delta: WorkCounters,
) -> Result<(), crate::FsError> {
    let native = crate::materializer::native_materialization_work_error;
    work.state(|state| {
        state.admit(WorkCounters::UNCHARGED).map_err(native)?;
        cancellation.check()?;
        state.admit(delta).map_err(native).map_err(Into::into)
    })
}

fn old_charge(
    work: &crate::NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
    delta: WorkCounters,
) -> Result<(), crate::FsError> {
    let native = crate::materializer::native_materialization_work_error;
    work.state(|state| {
        state.admit(WorkCounters::UNCHARGED).map_err(native)?;
        cancellation.check()?;
        state.charge(delta).map_err(native).map_err(Into::into)
    })
}

fn old_record(
    work: &crate::NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
    delta: WorkCounters,
) -> Result<(), crate::FsError> {
    work.record_admitted(delta)?;
    work.admit(WorkCounters::UNCHARGED)?;
    cancellation.check().map_err(Into::into)
}

fn new_call(
    operation: u8,
    work: &crate::NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
    delta: WorkCounters,
) -> Result<(), crate::FsError> {
    let mut accounting = KernelAccounting::native(work, cancellation);
    match operation % 3 {
        0 => accounting.admit(delta),
        1 => accounting.charge(delta),
        _ => accounting.record_completed(delta),
    }
}

fn old_call(
    operation: u8,
    work: &crate::NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
    delta: WorkCounters,
) -> Result<(), crate::FsError> {
    match operation % 3 {
        0 => old_admit(work, cancellation, delta),
        1 => old_charge(work, cancellation, delta),
        _ => old_record(work, cancellation, delta),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    /// Reconciling once per leaf gives the replaced sequences' exact results,
    /// receipts and live storage at every step, through cancellation, poison
    /// and every limit.
    #[test]
    fn one_reconciliation_matches_the_replaced_sequences(
        limits in prop::array::uniform24(amount(96)),
        steps in prop::collection::vec((0_u8..3, prop::array::uniform24(amount(8))), 0..16),
        resource in prop::array::uniform3(amount(64)),
        scoped in any::<bool>(), invocation in any::<bool>(), recheck in any::<bool>(),
        stop_at in 0_usize..16, failure in 0_u8..4,
    ) {
        let mut budget = WorkBudget::UNBOUNDED;
        budget.items_examined = limits[19].saturating_add(64);
        budget.bytes_copied = limits[13];
        budget.bytes_hashed = limits[12];
        budget.object_probes = limits[4];
        let [old, new] = acyclic_test_support::differential::pair::<_, _, (u8, WorkCounters)>(|| {
            let root_cancel = crate::CancellationToken::new();
            let caller_cancel = crate::CancellationToken::new();
            let root = crate::NativeMaterializationWork::new(budget, &root_cancel).unwrap();
            let mut work = root.clone();
            if scoped {
                work = work.native_volume_work(WorkBudget {
                    bytes_copied: resource[0], items_examined: resource[1].saturating_add(64),
                    bytes_hashed: resource[2], ..WorkBudget::UNBOUNDED
                }).unwrap();
            }
            if invocation { work = work.invocation_view().unwrap().0; }
            if recheck { work = work.recheck_admitted(); }
            (root, work, root_cancel, caller_cancel)
        }, |(root, work, root_cancel, caller_cancel), previous| {
            let mut outcomes = Vec::new();
            for (step, &(operation, delta)) in steps.iter().enumerate() {
                if step == stop_at {
                    match failure {
                        1 => caller_cancel.cancel(),
                        2 => root_cancel.cancel(),
                        3 => work.state(|state| state.poison_native_original_work()),
                        _ => (),
                    }
                }
                let delta = counters(delta);
                acyclic_test_support::differential::record((operation, delta));
                let result = if previous {
                    old_call(operation, &work, &caller_cancel, delta)
                } else {
                    new_call(operation, &work, &caller_cancel, delta)
                };
                outcomes.push((status(result), root.receipt(), root.live_allocation_bytes()));
            }
            outcomes
        });
        prop_assert_eq!(new, old);
    }
}

/// Each native kernel call takes the leaf once and reconciles the registry
/// once; the replaced sequences took two of either.
#[test]
fn native_kernel_calls_reconcile_once_under_one_lock() {
    let delta = WorkCounters {
        bytes_copied: 3,
        items_examined: 2,
        ..WorkCounters::UNCHARGED
    };
    for operation in 0..3 {
        let [
            ((old_locks, old_reconciles, old_receipt, old_live), old_trace),
            ((locks, reconciles, receipt, live), trace),
        ] = acyclic_test_support::differential::pair::<_, _, (u8, WorkCounters)>(
            || {
                let cancellation = crate::CancellationToken::new();
                let root =
                    crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                        .unwrap();
                // The initial receipt belongs to fixture setup, including the
                // debug lock-order observer's first per-thread allocation.
                let _initial = root.receipt();
                (root, cancellation)
            },
            |(root, cancellation), previous| {
                acyclic_test_support::differential::record((operation, delta));
                let (result, locks, reconciles) = probed(&root, || {
                    if previous {
                        old_call(operation, &root, &cancellation, delta)
                    } else {
                        new_call(operation, &root, &cancellation, delta)
                    }
                });
                result.unwrap();
                (
                    locks,
                    reconciles,
                    root.receipt(),
                    root.live_allocation_bytes(),
                )
            },
        );
        let minimal = |locks, reconciles| (locks, reconciles) == (1, 1);
        assert!(minimal(locks, reconciles));
        // Restoring the replaced sequence fails the same exact oracle.
        assert!(!minimal(old_locks, old_reconciles));
        assert_eq!(
            (operation, old_locks + old_reconciles),
            (operation, if operation == 2 { 4 } else { 3 })
        );
        assert_eq!((old_receipt, old_live, old_trace), (receipt, live, trace));
    }
}
