//! Retired native allocation owners leave exact receipts and bounded registries.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_fs::demand::native::NativeDemandSource;
use acyclic_fs::model::{FilesystemProfile, VolumeLimits};
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget, WorkCounters};
use std::path::Path;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const BUDGET: WorkBudget = acyclic_fs::performance::BOUNDARY_BUDGET;

async fn open(path: &Path, work: &NativeMaterializationWork) -> TestResult<NativeDemandSource> {
    Ok(NativeDemandSource::open(
        std::borrow::Cow::Borrowed(path),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        work,
        work.cancellation(),
    )
    .await?)
}

#[tokio::test]
async fn repeated_retirement_reclaims_registry_without_losing_charges() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
    let baseline = work.live_allocation_bytes();
    for _ in 0..64 {
        let source = open(root.path(), &work).await?;
        assert!(work.live_allocation_bytes() > baseline);
        drop(source);
        let before = work.receipt();
        work.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
        let mut expected = before;
        expected.items_examined += 1;
        assert_eq!(work.receipt(), expected);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    work.receipt().verify(BUDGET)?;
    cancellation.cancel();
    let before = work.receipt();
    assert!(
        work.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })
        .is_err()
    );
    assert_eq!(work.receipt(), before);
    Ok(())
}

#[tokio::test]
async fn interior_retirement_compacts_once_and_defers_when_copy_budget_is_full() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let mut setup_copies = None;
    for compact in [true, false] {
        let mut budget = BUDGET;
        if let Some(copies) = setup_copies {
            budget.bytes_copied = copies;
        }
        let work = NativeMaterializationWork::new(budget, &cancellation).map_err(|(e, _)| e)?;
        let baseline = work.live_allocation_bytes();
        let first = open(root.path(), &work).await?;
        let second = open(root.path(), &work).await?;
        let third = open(root.path(), &work).await?;
        let before = work.receipt();
        setup_copies = Some(before.bytes_copied);
        drop(first);
        work.charge(WorkCounters::UNCHARGED)?;
        let after = work.receipt();
        if compact {
            assert_eq!(after.bytes_copied - before.bytes_copied, 16);
            let mut expected = before;
            expected.bytes_copied = after.bytes_copied;
            assert_eq!(after, expected);
        } else {
            assert_eq!(after, before);
        }
        for _ in 0..64 {
            work.charge(WorkCounters::UNCHARGED)?;
            assert_eq!(work.receipt(), after);
        }
        assert!(work.live_allocation_bytes() > baseline);
        drop((second, third));
        work.charge(WorkCounters::UNCHARGED)?;
        assert_eq!(work.live_allocation_bytes(), baseline);
        assert_eq!(work.receipt(), after);
        work.receipt().verify(budget)?;
    }
    Ok(())
}

#[tokio::test]
async fn failed_owner_admission_retires_its_pending_slot_and_preserves_spent_work() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let reference = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
    let source = open(root.path(), &reference).await?;
    let successful_allocations = reference.receipt().allocation_operations;
    drop(source);
    for allowed in 1..successful_allocations {
        let budget = WorkBudget {
            allocation_operations: allowed,
            ..BUDGET
        };
        let work = NativeMaterializationWork::new(budget, &cancellation).map_err(|(e, _)| e)?;
        let baseline = work.live_allocation_bytes();
        assert!(open(root.path(), &work).await.is_err());
        let spent = work.receipt();
        spent.verify(budget)?;
        work.charge(WorkCounters::UNCHARGED)?;
        assert_eq!(work.receipt(), spent);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

#[tokio::test]
async fn compaction_defers_at_resource_copy_limit_and_retries_on_original() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
    let first = open(root.path(), &work).await?;
    let second = open(root.path(), &work).await?;
    let limited = work.native_volume_work(WorkBudget {
        bytes_copied: 0,
        ..BUDGET
    })?;
    drop(first);
    let before = work.receipt();
    let retained = work.live_allocation_bytes();
    limited.charge(WorkCounters::UNCHARGED)?;
    assert_eq!(work.receipt(), before);
    assert_eq!(work.live_allocation_bytes(), retained);
    assert_eq!(limited.remaining()?.bytes_copied, 0);
    work.charge(WorkCounters::UNCHARGED)?;
    let mut expected = before;
    expected.bytes_copied += 16;
    assert_eq!(work.receipt(), expected);
    drop(second);
    work.charge(WorkCounters::UNCHARGED)?;
    Ok(())
}

#[tokio::test]
async fn compaction_charges_each_resource_ancestor_once() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
    let first = open(root.path(), &work).await?;
    let second = open(root.path(), &work).await?;
    let parent = work.native_volume_work(WorkBudget {
        bytes_copied: 16,
        ..BUDGET
    })?;
    let child = parent.native_volume_work(BUDGET)?;
    let before_parent = parent.remaining()?.bytes_copied;
    let before_child = child.remaining()?.bytes_copied;
    drop(first);
    let before = work.receipt();
    child.charge(WorkCounters::UNCHARGED)?;
    let mut expected = before;
    expected.bytes_copied += 16;
    assert_eq!(work.receipt(), expected);
    assert_eq!(parent.remaining()?.bytes_copied, before_parent - 16);
    assert_eq!(child.remaining()?.bytes_copied, before_child - 16);
    child.charge(WorkCounters::UNCHARGED)?;
    assert_eq!(work.receipt(), expected);
    drop(second);
    work.charge(WorkCounters::UNCHARGED)?;
    Ok(())
}

#[tokio::test]
async fn cancelled_compaction_preserves_retirement_without_copying() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    for cancel_original in [false, true] {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
        let first = open(root.path(), &work).await?;
        let second = open(root.path(), &work).await?;
        let (invocation, invocation_cancel) = work.invocation_view()?;
        if cancel_original {
            cancellation.cancel();
        } else {
            invocation_cancel.cancel();
        }
        drop(first);
        let before = work.receipt();
        let retained = work.live_allocation_bytes();
        assert!(invocation.charge(WorkCounters::UNCHARGED).is_err());
        assert_eq!(work.receipt(), before);
        assert_eq!(work.live_allocation_bytes(), retained);
        if !cancel_original {
            work.charge(WorkCounters::UNCHARGED)?;
            let mut expected = before;
            expected.bytes_copied += 16;
            assert_eq!(work.receipt(), expected);
        }
        drop(second);
        drop(invocation);
        let spent = work.receipt();
        let _ = work.charge(WorkCounters::UNCHARGED);
        assert_eq!(work.receipt(), spent);
    }
    Ok(())
}

#[tokio::test]
async fn oldest_owner_churn_moves_at_most_one_survivor_per_retirement() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
    let baseline = work.live_allocation_bytes();
    let mut sources = std::collections::VecDeque::with_capacity(64);
    for _ in 0..64 {
        sources.push_back(open(root.path(), &work).await?);
    }
    let before = work.receipt();
    let mut previous = before;
    while let Some(source) = sources.pop_front() {
        drop(source);
        work.charge(WorkCounters::UNCHARGED)?;
        let after = work.receipt();
        assert!(after.bytes_copied - previous.bytes_copied <= 16);
        let mut expected = previous;
        expected.bytes_copied = after.bytes_copied;
        assert_eq!(after, expected);
        previous = after;
    }
    assert_eq!(previous.bytes_copied - before.bytes_copied, 32 * 16);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn mixed_retirement_layouts_charge_only_moved_survivors() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    // Expected survivor moves for every subset of four retired owners.
    let moves = [0, 1, 1, 2, 1, 1, 1, 1, 0, 1, 1, 1, 0, 1, 0, 0];
    for (mask, moved) in moves.into_iter().enumerate() {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
        let baseline = work.live_allocation_bytes();
        let mut survivors = Vec::with_capacity(4);
        for _ in 0..4 {
            survivors.push(Some(open(root.path(), &work).await?));
        }
        let before = work.receipt();
        for (index, source) in survivors.iter_mut().enumerate() {
            if mask & (1 << index) != 0 {
                drop(source.take());
            }
        }
        work.charge(WorkCounters::UNCHARGED)?;
        let mut expected = before;
        expected.bytes_copied += moved * 16;
        assert_eq!(work.receipt(), expected, "retired mask {mask}");
        work.charge(WorkCounters::UNCHARGED)?;
        assert_eq!(work.receipt(), expected);
        drop(survivors);
        work.charge(WorkCounters::UNCHARGED)?;
        assert_eq!(work.receipt(), expected);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

#[tokio::test]
async fn rejected_admissions_with_a_live_owner_do_not_accumulate_slots_or_copies() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    for denied_budget in [
        WorkBudget {
            peak_allocation_bytes: 0,
            ..BUDGET
        },
        WorkBudget {
            allocation_operations: 0,
            ..BUDGET
        },
    ] {
        let work = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
        let owner = open(root.path(), &work).await?;
        let denied = work.native_volume_work(denied_budget)?;
        assert!(open(root.path(), &denied).await.is_err());
        work.charge(WorkCounters::UNCHARGED)?;
        let retained = work.live_allocation_bytes();
        let spent = work.receipt();
        for _ in 0..64 {
            assert!(open(root.path(), &denied).await.is_err());
            let failed_prefix = work.receipt();
            work.charge(WorkCounters::UNCHARGED)?;
            assert_eq!(work.receipt(), failed_prefix);
            assert_eq!(work.live_allocation_bytes(), retained);
            assert_eq!(work.receipt(), spent);
        }
        drop(owner);
        work.charge(WorkCounters::UNCHARGED)?;
        assert_eq!(work.receipt(), spent);
        drop(denied);
        assert_eq!(
            work.live_allocation_bytes(),
            NativeMaterializationWork::accounting_state_bytes()
        );
    }
    Ok(())
}
