//! Native owner changes reconcile once while unchanged charges retain exact receipts.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_fs::demand::{DemandSource, native::NativeDemandSource};
use acyclic_fs::facade::FsWork;
use acyclic_fs::kernel::NamespacePath;
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
async fn unchanged_charges_and_cross_thread_retirement_preserve_exact_receipts() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    for count in [0, 1, 16, 128] {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
        let baseline = work.live_allocation_bytes();
        let mut sources = Vec::with_capacity(count);
        for _ in 0..count {
            sources.push(open(root.path(), &work).await?);
        }
        let mut expected = work.receipt();
        let live = work.live_allocation_bytes();
        for _ in 0..64 {
            work.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            expected.items_examined += 1;
            assert_eq!(work.receipt(), expected);
            assert_eq!(work.live_allocation_bytes(), live);
        }
        std::thread::spawn(move || drop(sources))
            .join()
            .map_err(|_| "retirement panicked")?;
        let retired = work.live_allocation_bytes();
        assert_eq!(work.receipt(), expected);
        work.charge(WorkCounters::UNCHARGED)?;
        let settled = work.live_allocation_bytes();
        if count == 0 {
            assert_eq!(settled, retired);
        } else {
            assert!(settled == baseline || settled + 1 == retired);
        }
        let settled = work.live_allocation_bytes();
        for _ in 0..64 {
            work.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            expected.items_examined += 1;
            assert_eq!(work.receipt(), expected);
            assert_eq!(work.live_allocation_bytes(), settled);
        }
        cancellation.cancel();
        assert!(
            work.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })
            .is_err()
        );
        assert_eq!(work.receipt(), expected);
        expected.verify(BUDGET)?;
    }
    Ok(())
}

#[tokio::test]
async fn retirement_followed_by_new_admission_keeps_its_initial_snapshot() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
    for _ in 0..16 {
        let first = open(root.path(), &work).await?;
        let second = open(root.path(), &work).await?;
        let before = work.receipt();
        let live = work.live_allocation_bytes();
        drop(second);
        let retired = work.live_allocation_bytes();
        assert!(retired < live);
        work.charge(WorkCounters::UNCHARGED)?;
        // Pruning also destroys the retired owner's inert Arc gauge.
        let gauge_bytes = std::alloc::Layout::array::<usize>(2)?
            .extend(std::alloc::Layout::new::<std::sync::atomic::AtomicU64>())?
            .0
            .pad_to_align()
            .size() as u64;
        assert_eq!(work.live_allocation_bytes(), retired - gauge_bytes);
        assert_eq!(work.receipt(), before);
        drop(first);
        // Admission must consume pending retirement and initialize the new owner,
        // even though no later payload mutation will mark its snapshot dirty.
        let third = open(root.path(), &work).await?;
        let expected = work.receipt();
        let live = work.live_allocation_bytes();
        work.charge(WorkCounters::UNCHARGED)?;
        assert_eq!(work.live_allocation_bytes(), live);
        assert_eq!(work.receipt(), expected);
        drop(third);
        let retired = work.live_allocation_bytes();
        work.charge(WorkCounters::UNCHARGED)?;
        let settled = work.live_allocation_bytes();
        assert!(
            settled == NativeMaterializationWork::accounting_state_bytes()
                || settled + 1 == retired
        );
        assert_eq!(work.receipt(), expected);
    }
    Ok(())
}

#[tokio::test]
async fn directory_payload_retirement_preserves_a_live_owner_and_all_spent_work() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    for name in ["a", "b", "c", "d"] {
        std::fs::write(root.path().join(name), name)?;
    }
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
    let source = open(root.path(), &work).await?;
    let portable = acyclic_fs::path::PortablePath::parse("/", VolumeLimits::default())?;
    let path = NamespacePath::from_portable(&portable, VolumeLimits::default())?;
    let mut cursor = None;
    let mut seen = 0;
    loop {
        let mut page = source
            .list_page(source.reference(), &path, cursor, 1, &work, &cancellation)
            .await?;
        seen += page.entries.len();
        cursor = page.next.take();
        page.publication
            .as_mut()
            .ok_or("native publication missing")?
            .accept(&mut (&work).accounting(&cancellation))?;
        drop(page);
        let expected = work.receipt();
        let live = work.live_allocation_bytes();
        work.charge(WorkCounters::UNCHARGED)?;
        assert_eq!(work.receipt(), expected);
        assert!(work.live_allocation_bytes() <= live);
        let settled = work.live_allocation_bytes();
        work.charge(WorkCounters::UNCHARGED)?;
        assert_eq!(work.live_allocation_bytes(), settled);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(seen, 4);
    drop(source);
    let expected = work.receipt();
    work.charge(WorkCounters::UNCHARGED)?;
    assert_eq!(work.receipt(), expected);
    expected.verify(BUDGET)?;
    Ok(())
}

#[tokio::test]
async fn rejected_owner_admission_preserves_every_spent_prefix() -> TestResult {
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
        assert!(open(root.path(), &work).await.is_err());
        let spent = work.receipt();
        spent.verify(budget)?;
        work.charge(WorkCounters::UNCHARGED)?;
        assert_eq!(work.receipt(), spent);
        let live = work.live_allocation_bytes();
        work.charge(WorkCounters::UNCHARGED)?;
        assert_eq!(work.live_allocation_bytes(), live);
        assert_eq!(work.receipt(), spent);
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_charges_cannot_consume_a_later_owner_retirement() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let mut expected_live = None;
    for concurrent in [false, true] {
        let work = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
        let mut sources = Vec::with_capacity(64);
        for _ in 0..64 {
            sources.push(open(root.path(), &work).await?);
        }
        let mut expected = work.receipt();
        if concurrent {
            let start = std::sync::Barrier::new(4);
            std::thread::scope(|scope| {
                let mut charges = Vec::new();
                for _ in 0..3 {
                    let work = &work;
                    let start = &start;
                    charges.push(scope.spawn(move || {
                        start.wait();
                        for _ in 0..256 {
                            work.charge(WorkCounters {
                                items_examined: 1,
                                ..WorkCounters::UNCHARGED
                            })?;
                        }
                        Ok::<_, acyclic_fs::NativeTreeMaterializationError>(())
                    }));
                }
                start.wait();
                while let Some(source) = sources.pop() {
                    drop(source);
                }
                for worker in charges {
                    worker.join().map_err(|_| "charge worker panicked")??;
                }
                Ok::<_, Box<dyn std::error::Error>>(())
            })?;
            expected.items_examined += 3 * 256;
        } else {
            drop(sources);
        }
        work.charge(WorkCounters::UNCHARGED)?;
        assert_eq!(work.receipt(), expected);
        let live = work.live_allocation_bytes();
        if let Some(expected) = expected_live {
            assert_eq!(live, expected);
        }
        expected_live = Some(live);
        expected.verify(BUDGET)?;
    }
    Ok(())
}

#[tokio::test]
async fn shared_registry_notification_does_not_pin_a_retired_resource() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
    let resource_budget = WorkBudget {
        peak_allocation_bytes: 64 * 1024,
        ..BUDGET
    };
    let first = work.native_volume_work(resource_budget)?;
    let second = work.native_volume_work(resource_budget)?;
    let first_remaining = first.remaining()?.peak_allocation_bytes;
    let second_remaining = second.remaining()?.peak_allocation_bytes;
    let first_source = open(root.path(), &first).await?;
    let second_source = open(root.path(), &second).await?;
    assert!(first.remaining()?.peak_allocation_bytes < first_remaining);
    assert!(second.remaining()?.peak_allocation_bytes < second_remaining);
    let mut expected = work.receipt();
    drop(first_source);
    work.charge(WorkCounters::UNCHARGED)?;
    let reconciled = work.receipt();
    assert!(reconciled.bytes_copied >= expected.bytes_copied);
    expected.bytes_copied = reconciled.bytes_copied;
    assert_eq!(reconciled, expected);
    assert_eq!(first.remaining()?.peak_allocation_bytes, first_remaining);
    assert!(second.remaining()?.peak_allocation_bytes < second_remaining);
    drop(second_source);
    work.charge(WorkCounters::UNCHARGED)?;
    assert_eq!(second.remaining()?.peak_allocation_bytes, second_remaining);
    assert_eq!(work.receipt(), expected);
    expected.verify(BUDGET)?;
    Ok(())
}
