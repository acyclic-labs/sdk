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
            assert!(after.bytes_copied > before.bytes_copied);
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
