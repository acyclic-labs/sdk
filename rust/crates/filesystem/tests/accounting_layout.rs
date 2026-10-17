//! Actual allocator requests and admission boundaries for shared ledger and name ownership.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_fs::{
    CancellationToken, NativeMaterializationWork, WorkBudget, WorkCounters, WorkspaceName,
};
fn measured<T>(operation: impl FnOnce() -> T) -> (T, acyclic_test_support::AllocationInfo) {
    let mut result = None;
    let allocations = acyclic_test_support::measure(|| result = Some(operation()));
    (result.unwrap(), allocations)
}

#[test]
fn original_ledger_records_the_actual_allocator_layout() {
    let cancellation = CancellationToken::new();
    let (work, allocations) = measured(|| {
        NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
            .unwrap()
    });
    assert_eq!(allocations.count_total, 1);
    assert_eq!(allocations.count_current, 1);
    assert_eq!(allocations.bytes_current as u64, allocations.bytes_total);
    assert_eq!(allocations.bytes_max, allocations.bytes_total);
    eprintln!("ledger allocator: {allocations:?}");
    assert_eq!(
        allocations.bytes_total,
        NativeMaterializationWork::accounting_state_bytes()
    );
    assert_eq!(work.live_allocation_bytes(), allocations.bytes_total);
    assert_eq!(
        work.receipt().peak_allocation_bytes,
        allocations.bytes_total
    );
    assert_eq!(work.receipt().allocation_operations, 1);
}

#[test]
fn denied_ledger_does_not_allocate_its_arc() {
    let cancellation = CancellationToken::new();
    let (probe, allocations) = measured(|| {
        NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
            .unwrap()
    });
    let required = allocations.bytes_total;
    drop(probe);
    for ceiling in 0..=required {
        let (result, allocations) = measured(|| {
            NativeMaterializationWork::new(
                WorkBudget {
                    peak_allocation_bytes: ceiling,
                    ..acyclic_fs::performance::BOUNDARY_BUDGET
                },
                &cancellation,
            )
        });
        if ceiling == required {
            let work = result.unwrap();
            assert_eq!(allocations.count_total, 1);
            assert_eq!(allocations.bytes_total, required);
            assert_eq!(work.live_allocation_bytes(), required);
            assert_eq!(work.receipt().peak_allocation_bytes, required);
            assert_eq!(work.receipt().allocation_operations, 1);
            continue;
        }
        let (_, work) = result.unwrap_err();
        assert_eq!(*work, WorkCounters::UNCHARGED);
        // The typed error owns one receipt; the admitted ledger Arc is absent.
        assert_eq!(allocations.count_total, 1);
        assert_eq!(
            allocations.bytes_total,
            std::mem::size_of::<WorkCounters>() as u64
        );
    }
}

#[test]
fn resource_allowances_fund_each_actual_arc_and_release_the_chain() {
    let cancellation = CancellationToken::new();
    let original = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let baseline = original.live_allocation_bytes();
    let mut parent = original.clone();
    for depth in 1..=4 {
        let before = original.live_allocation_bytes();
        let (child, allocations) = measured(|| {
            parent
                .native_volume_work(WorkBudget {
                    peak_allocation_bytes: 0,
                    ..WorkBudget::UNBOUNDED
                })
                .unwrap()
        });
        assert_eq!(allocations.count_total, 1);
        assert_eq!(allocations.count_current, 1);
        assert_eq!(allocations.bytes_current as u64, allocations.bytes_total);
        assert_eq!(allocations.bytes_max, allocations.bytes_total);
        assert_eq!(
            original.live_allocation_bytes() - before,
            allocations.bytes_total
        );
        assert_eq!(
            original.receipt().peak_allocation_bytes,
            before + allocations.bytes_total
        );
        assert_eq!(original.receipt().allocation_operations, 1 + depth);
        assert_eq!(child.remaining().unwrap().peak_allocation_bytes, 0);
        eprintln!("resource depth {depth} allocator: {allocations:?}");
        parent = child;
    }
    let receipt = original.receipt();
    drop(parent);
    assert_eq!(original.live_allocation_bytes(), baseline);
    assert_eq!(original.receipt(), receipt);
}

#[test]
fn denied_resource_does_not_allocate_its_arc_or_spend_work() {
    let cancellation = CancellationToken::new();
    let probe = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let baseline = probe.live_allocation_bytes();
    let (resource, allocations) =
        measured(|| probe.native_volume_work(WorkBudget::UNBOUNDED).unwrap());
    assert_eq!(allocations.count_total, 1);
    let required = allocations.bytes_total;
    drop(resource);
    assert_eq!(probe.live_allocation_bytes(), baseline);
    for ceiling in 0..=required {
        let root = NativeMaterializationWork::new(
            WorkBudget {
                peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes()
                    + ceiling,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .unwrap();
        let before = root.receipt();
        let (result, allocations) = measured(|| root.native_volume_work(WorkBudget::UNBOUNDED));
        assert_eq!(result.is_ok(), ceiling == required);
        if let Ok(resource) = result {
            assert_eq!(allocations.count_total, 1);
            assert_eq!(allocations.bytes_total, required);
            assert_eq!(
                root.live_allocation_bytes(),
                before.peak_allocation_bytes + required
            );
            assert_eq!(
                root.receipt().allocation_operations,
                before.allocation_operations + 1
            );
            drop(resource);
        } else {
            assert_eq!(allocations.count_total, 0);
            assert_eq!(allocations.bytes_total, 0);
            assert_eq!(root.receipt(), before);
        }
        assert_eq!(root.live_allocation_bytes(), before.peak_allocation_bytes);
    }
}

fn assert_name_layout(name: &str, native: bool) -> u64 {
    let cancellation = CancellationToken::new();
    let original = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let before = original.live_allocation_bytes();
    let (value, receipt, allocations) = if native {
        let (value, allocations) =
            measured(|| WorkspaceName::new(name, &&original, &cancellation).unwrap());
        let mut receipt = original.receipt();
        receipt.peak_allocation_bytes -= before;
        receipt.allocation_operations -= 1;
        assert_eq!(
            original.live_allocation_bytes() - before,
            allocations.bytes_total
        );
        (value, receipt, allocations)
    } else {
        let (receipt, allocations) =
            measured(|| WorkspaceName::new(name, &WorkBudget::UNBOUNDED, &cancellation).unwrap());
        (receipt.value, receipt.work, allocations)
    };
    assert_eq!(value.as_str(), name);
    assert_eq!(allocations.count_total, 2);
    assert_eq!(allocations.count_current, 2);
    assert_eq!(allocations.bytes_current as u64, allocations.bytes_total);
    assert_eq!(allocations.bytes_max, allocations.bytes_total);
    assert_eq!(receipt.peak_allocation_bytes, allocations.bytes_total);
    assert_eq!(receipt.allocation_operations, allocations.count_total);
    // The allocation observation is independent of the charged layout. The
    // former one-counter receipt must fail this exact same equality.
    assert_ne!(
        receipt.peak_allocation_bytes - std::mem::size_of::<usize>() as u64,
        allocations.bytes_total
    );
    drop(value);
    assert_eq!(original.live_allocation_bytes(), before);
    allocations.bytes_total
}

#[test]
fn workspace_names_fund_the_actual_arc_at_every_ascii_length() {
    for length in 1..=255 {
        let name = "x".repeat(length);
        let standalone = assert_name_layout(&name, false);
        assert_eq!(assert_name_layout(&name, true), standalone);
    }
}

#[test]
fn workspace_name_admission_uses_the_complete_arc_boundary() {
    let required = assert_name_layout("private", false);
    let cancellation = CancellationToken::new();
    for ceiling in 0..=required {
        let (result, allocations) = measured(|| {
            WorkspaceName::new(
                "private",
                &WorkBudget {
                    peak_allocation_bytes: ceiling,
                    ..WorkBudget::UNBOUNDED
                },
                &cancellation,
            )
        });
        assert_eq!(result.is_ok(), ceiling == required);
        if let Ok(receipt) = result {
            assert_eq!(receipt.work.peak_allocation_bytes, required);
            assert_eq!(allocations.bytes_total, required);
        } else {
            // A rejected name may allocate its string plus the typed error's
            // existing receipt. Its Arc is never allocated before admission.
            assert_eq!(allocations.count_total, 1 + u64::from(ceiling >= 7));
        }
        let root_bytes = NativeMaterializationWork::accounting_state_bytes();
        let original = NativeMaterializationWork::new(
            WorkBudget {
                peak_allocation_bytes: root_bytes + ceiling,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .unwrap();
        let result = WorkspaceName::new("private", &&original, &cancellation);
        assert_eq!(result.is_ok(), ceiling == required);
        drop(result);
        assert_eq!(original.live_allocation_bytes(), root_bytes);
    }
}
