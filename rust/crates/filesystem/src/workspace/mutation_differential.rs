use super::*;
use acyclic_test_support::{AllocationInfo, measure};
use proptest::prelude::*;
use std::cell::Cell;

type Push = Transaction<crate::memory::MemoryAuthorityStore, crate::memory::MemoryObjectStore>;
type Value = Box<[u8; 8]>;
thread_local! {
    static REDUNDANT: Cell<bool> = const { Cell::new(false) };
    static COPIED: Cell<u64> = const { Cell::new(0) };
}
pub(super) fn before_charge(
    accounting: &KernelAccounting<'_>,
    delta: crate::WorkCounters,
) -> Result<(), WorkspaceError> {
    if REDUNDANT.with(Cell::get) {
        accounting.admit(delta)?;
    }
    Ok(())
}
pub(super) fn observed_copy(bytes: u64) {
    COPIED.with(|copied| copied.set(bytes));
}

// Frozen old sequence, on independently allocated values/list and a fresh ledger.
fn old_push(
    operations: &mut crate::kernel::Owned<Vec<Value>>,
    mut operation: crate::kernel::Owned<Value>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    let copied = copy_work(std::mem::size_of::<Value>() as u64);
    accounting.admit(copied)?;
    operations.allocation.absorb(&mut operation.allocation)?;
    operations.bytes = operations
        .bytes
        .checked_add(operation.bytes)
        .ok_or(crate::WorkError::Overflow)?;
    operations.value.push(operation.value);
    observed_copy(copied.bytes_copied);
    accounting.record_completed(copied)?;
    Ok(())
}

fn run(
    old: bool,
    mode: u8,
    copies: u64,
    failure: u8,
    foreign: bool,
    redundant: bool,
) -> (
    Result<(), String>,
    Vec<[u8; 8]>,
    u64,
    u64,
    crate::WorkCounters,
    crate::WorkCounters,
    u64,
    u64,
    AllocationInfo,
    (usize, usize),
) {
    let root_cancel = CancellationToken::new();
    let caller = CancellationToken::new();
    let root = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &root_cancel).unwrap();
    let other =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .unwrap();
    let mut work = root.clone();
    for _ in 0..mode.min(2) {
        work = work
            .native_volume_work(WorkBudget {
                bytes_copied: copies,
                ..WorkBudget::UNBOUNDED
            })
            .unwrap();
    }
    if mode == 3 {
        work = work.invocation_view().unwrap().0;
    }
    if mode == 0 {
        root.state(|state| state.budget.bytes_copied = copies);
    }
    let funding = CancellationToken::new();
    let mut accounting = KernelAccounting::native(&work, &funding);
    let mut operations = crate::kernel::Owned::<Vec<Value>>::allocate(1, &mut accounting).unwrap();
    let mut operation_accounting =
        KernelAccounting::native(if foreign { &other } else { &work }, &funding);
    let allocation = operation_accounting.claim_owned(8, 1).unwrap();
    let operation = crate::kernel::Owned::new(Box::new([7; 8]), allocation, 8);
    let before = root.receipt();
    match failure {
        1 => caller.cancel(),
        2 => root_cancel.cancel(),
        3 => root.state(|state| state.poison_native_original_work()),
        _ => (),
    }
    let mut accounting = KernelAccounting::native(&work, &caller);
    COPIED.with(|copied| copied.set(0));
    REDUNDANT.with(|flag| flag.set(redundant));
    let mut captured = None;
    let heap = measure(|| {
        captured = Some(crate::materializer::count_resource_locks(|| {
            root.state.count_locks(|| {
                if old {
                    old_push(&mut operations, operation, &mut accounting)
                } else {
                    Push::push_mutation(&mut operations, operation, &mut accounting)
                }
            })
        }))
    });
    REDUNDANT.with(|flag| flag.set(false));
    let ((result, leaf), resources) = captured.unwrap();
    let result = result.map_err(|error| format!("{error:?}"));
    let copied = COPIED.with(Cell::get);
    if copied != 0 {
        acyclic_test_support::differential::record(copied);
    }
    let output = operations.value.iter().map(|value| **value).collect();
    let held = match &operations.allocation {
        KernelOwnedAllocation::NativeOriginal(allocation) => allocation.bytes(),
        _ => unreachable!("native fixture"),
    };
    let bytes = operations.bytes;
    let home_work = root.receipt();
    if result.is_ok() {
        assert_eq!(home_work, before.checked_add(copy_work(8)).unwrap());
    }
    let data = (
        result,
        output,
        bytes,
        held,
        home_work,
        other.receipt(),
        root.live_allocation_bytes(),
        other.live_allocation_bytes(),
        heap,
        (leaf, resources),
    );
    operations
        .retire(&mut KernelAccounting::native(&work, &funding))
        .unwrap();
    // Cancellation can retire standing invocation metadata. The receipt/live
    // snapshot above has reconciled it; retiring the list must release exactly
    // all bytes its funding owns, independently of that metadata transition.
    assert_eq!(root.live_allocation_bytes(), data.6 - held);
    data
}

fn compare(
    mode: u8,
    copies: u64,
    failure: u8,
    foreign: bool,
    redundant: bool,
) -> Result<(), &'static str> {
    let [(old, old_trace), (new, new_trace)] = acyclic_test_support::differential::pair::<_, _, u64>(
        || (),
        |(), baseline| {
            run(
                baseline,
                mode,
                copies,
                failure,
                foreign,
                !baseline && redundant,
            )
        },
    );
    if old.0 != new.0
        || old.1 != new.1
        || old.2 != new.2
        || old.3 != new.3
        || old.4 != new.4
        || old.5 != new.5
        || old.6 != new.6
        || old.7 != new.7
        || old_trace != new_trace
    {
        return Err("full output/error/work/funding/storage/trace equality");
    }
    let expected = if new.0.is_ok() {
        AllocationInfo::default()
    } else {
        AllocationInfo {
            count_current: -1,
            bytes_current: -8,
            ..AllocationInfo::default()
        }
    };
    if old.8 != new.8 || new.8 != expected {
        return Err("exact allocation and retirement counts");
    }
    if new.0.is_ok()
        && (old.9 != (3, 4 * usize::from(mode.min(2)))
            || new.9 != (1, 2 * usize::from(mode.min(2))))
    {
        return Err("exact lock counts");
    }
    Ok(())
}

#[test]
fn pushes_match_full_independent_results_and_redundant_admission_breaks_the_lock_pin() {
    for mode in 0..4 {
        compare(mode, u64::MAX, 0, false, false).unwrap();
        assert_eq!(
            compare(mode, u64::MAX, 0, false, true),
            Err("exact lock counts")
        );
    }
    compare(3, 0, 2, false, false).unwrap();
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]
    #[test]
    fn failed_and_successful_pushes_preserve_exact_funding_receipts_errors_and_effects(mode in 0_u8..4, copies in 0_u64..17, failure in 0_u8..4) {
        // Reachable pushes move funding within one context. Foreign funding is
        // covered separately by the precheck/failure-before-charge control.
        compare(mode, copies, failure, false, false).map_err(proptest::test_runner::TestCaseError::fail)?;
    }
}
