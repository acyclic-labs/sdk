use super::*;
use crate::NativeMaterializationWork;

type TestResult = Result<(), Box<dyn std::error::Error>>;
const BUDGET: WorkBudget = crate::performance::BOUNDARY_BUDGET;

#[test]
fn pending_admission_keeps_retirement_dirty_with_a_live_survivor() -> TestResult {
    let cancellation = CancellationToken::new();
    let mut original = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
    let first = NativeMountAllocationCustody::new(&mut original)?;
    let survivor = NativeMountAllocationCustody::new(&mut original)?;
    original.charge(WorkCounters::UNCHARGED)?;
    {
        let mut work = original.state.lock();
        let before = work.work;
        let retained = work.allocations.live_bytes();
        work.scope(
            |work| &mut work.roots.native_mount_admission_pending,
            true,
            |work| -> Result<(), crate::WorkError> {
                drop(first);
                work.reconcile_cancellation_registry()?;
                assert_eq!(work.work, before);
                assert_eq!(
                    work.allocations.live_bytes(),
                    retained - std::mem::size_of::<NativeMountAllocationCustody>() as u64
                );
                assert_eq!(work.roots.native_mount_admissions.0.len(), 2);
                assert!(
                    work.roots
                        .native_mount_dirty
                        .as_ref()
                        .is_some_and(|dirty| dirty.load(Ordering::Acquire))
                );
                Ok(())
            },
        )?;
        assert!(!work.roots.native_mount_admission_pending);
        work.charge(WorkCounters::UNCHARGED)?;
        let mut expected = before;
        expected.bytes_copied +=
            std::mem::size_of::<(Weak<NativeMountAllocationCustody>, u64)>() as u64;
        assert_eq!(work.work, expected);
        assert_eq!(work.roots.native_mount_admissions.0.len(), 1);
        assert!(std::ptr::eq(
            work.roots.native_mount_admissions.0[0].0.as_ptr(),
            Arc::as_ptr(&survivor)
        ));
        work.charge(WorkCounters::UNCHARGED)?;
        assert_eq!(work.work, expected);
    }
    drop(survivor);
    original.charge(WorkCounters::UNCHARGED)?;
    assert_eq!(
        original.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}
