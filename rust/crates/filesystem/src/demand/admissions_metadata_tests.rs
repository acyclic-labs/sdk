use super::*;
use proptest::prelude::*;
use std::cell::Cell;

std::thread_local! {
    static OMIT_ARC_HEADER: Cell<bool> = const { Cell::new(false) };
}

pub(super) fn admitted_bytes(bytes: u64) -> u64 {
    if OMIT_ARC_HEADER.with(Cell::get) {
        std::mem::size_of::<NativeProofAccounting>() as u64
    } else {
        bytes
    }
}

fn measured<T>(run: impl FnOnce() -> T) -> (T, acyclic_test_support::AllocationInfo) {
    let mut value = None;
    let heap = acyclic_test_support::measure(|| value = Some(run()));
    (value.unwrap(), heap)
}

fn allocation_matches_heap(broken: bool) -> Result<u64, &'static str> {
    let original =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .unwrap();
    let baseline = original.live_allocation_bytes();
    let before = original.receipt();
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            OMIT_ARC_HEADER.with(|flag| flag.set(self.0));
        }
    }
    let _restore = Restore(OMIT_ARC_HEADER.with(|flag| flag.replace(broken)));
    let (shared, allocated) = measured(|| original.state.lock().proof_accounting().unwrap());
    if allocated.count_total != 1 || allocated.count_current != 1 || allocated.count_max != 1 {
        return Err("one shared allocation");
    }
    if allocated.bytes_total != allocated.bytes_current as u64
        || allocated.bytes_total != allocated.bytes_max
    {
        return Err("exact shared allocation");
    }
    // Keep a publisher alive while checking the real heap delta: a terminal
    // reconciliation would otherwise discard the aggregate before comparison.
    if original.live_allocation_bytes() != baseline + allocated.bytes_total {
        return Err("shared metadata must fund its complete physical allocation");
    }
    let expected = before
        .checked_add(WorkCounters {
            allocation_operations: 1,
            peak_allocation_bytes: baseline + allocated.bytes_total,
            ..WorkCounters::UNCHARGED
        })
        .unwrap();
    if original.receipt() != expected {
        return Err("metadata receipt");
    }
    let ((), retired) = measured(|| {
        drop(shared);
        original.charge(WorkCounters::UNCHARGED).unwrap();
    });
    if retired.count_total != 0
        || retired.count_current != -1
        || retired.bytes_current != -(allocated.bytes_total as i64)
    {
        return Err("final owner releases the physical allocation once");
    }
    if original.live_allocation_bytes() != baseline || original.receipt() != expected {
        return Err("final owner restores standing bytes and keeps the receipt");
    }
    Ok(allocated.bytes_total)
}

#[test]
fn shared_proof_metadata_funds_the_actual_arc_until_final_reconciliation() {
    let physical = allocation_matches_heap(false).unwrap();
    eprintln!(
        "shared_proof_accounting physical={physical} legacy_charge={} admitted={}",
        std::mem::size_of::<NativeProofAccounting>(),
        NativeProofAccounting::ALLOCATION_BYTES
    );
}

#[test]
fn the_old_payload_only_charge_fails_the_same_physical_allocation_check() {
    assert_eq!(
        allocation_matches_heap(true),
        Err("shared metadata must fund its complete physical allocation")
    );
}

#[test]
fn an_exact_physical_metadata_budget_succeeds_and_one_byte_less_allocates_nothing() {
    let physical = allocation_matches_heap(false).unwrap();
    for short in [false, true] {
        let cancellation = CancellationToken::new();
        let baseline = crate::NativeMaterializationWork::accounting_state_bytes();
        let maximum = baseline + physical - u64::from(short);
        let original = crate::NativeMaterializationWork::new(
            WorkBudget {
                peak_allocation_bytes: maximum,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .unwrap();
        assert_eq!(original.live_allocation_bytes(), baseline);
        let before = original.receipt();
        let (result, heap) = measured(|| original.state.lock().proof_accounting());
        if short {
            assert!(
                matches!(result, Err(DemandError::Work(crate::WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes", observed, maximum: limit,
            })) if observed == baseline + physical && limit == maximum)
            );
            assert_eq!(heap, acyclic_test_support::AllocationInfo::default());
            assert_eq!(original.receipt(), before);
        } else {
            let shared = result.unwrap();
            assert_eq!(heap.count_total, 1);
            assert_eq!(heap.bytes_total, physical);
            assert_eq!(original.live_allocation_bytes(), maximum);
            drop(shared);
        }
        original.charge(WorkCounters::UNCHARGED).unwrap();
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Arbitrary publisher cloning and retirement never duplicates a charge;
    // the aggregate remains funded until its final original-ledger drain.
    #[test]
    fn every_publisher_trace_preserves_physical_storage_and_one_charge(
        publishers in 1_usize..9,
        actions in prop::collection::vec((any::<bool>(), 0_usize..8), 0..33),
    ) {
        let physical = allocation_matches_heap(false).unwrap();
        let original = crate::NativeMaterializationWork::new(
            WorkBudget::UNBOUNDED, &CancellationToken::new(),
        ).unwrap();
        let baseline = original.live_allocation_bytes();
        let initial = original.state.lock().proof_accounting().unwrap();
        let mut owners = vec![Some(initial); publishers];
        let charged = original.receipt();
        for (clone, index) in actions {
            let index = index % owners.len();
            if clone {
                if let Some(owner) = &owners[index] { owners.push(Some(owner.clone())); }
            } else {
                drop(owners[index].take());
            }
            original.charge(WorkCounters::UNCHARGED).unwrap();
            let live = u64::from(owners.iter().any(Option::is_some));
            prop_assert_eq!(original.live_allocation_bytes(), baseline + live * physical);
            prop_assert_eq!(original.receipt(), charged);
        }
        drop(owners);
        original.charge(WorkCounters::UNCHARGED).unwrap();
        prop_assert_eq!(original.live_allocation_bytes(), baseline);
        prop_assert_eq!(original.receipt(), charged);
    }
}
