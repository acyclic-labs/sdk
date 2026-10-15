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
        expected.bytes_copied += std::mem::size_of::<(Arc<AtomicU64>, u64)>() as u64;
        assert_eq!(work.work, expected);
        assert_eq!(work.roots.native_mount_admissions.0.len(), 1);
        assert!(survivor.accounts_for(&work.roots.native_mount_admissions.0[0].0));
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

proptest::proptest! {
    // Every live owner pays for its custody, inert gauge and payload exactly
    // once. Retirement never erases spent work; all registry storage is freed
    // when the final owner leaves, regardless of operation/drop order.
    #[test]
    fn custody_gauges_preserve_exact_retirement_accounting(
        actions in proptest::collection::vec((0u8..4, 0u8..3, 0u16..257), 1..65),
    ) {
        let cancellation = CancellationToken::new();
        let mut original = NativeMaterializationWork::new(BUDGET, &cancellation)
            .map_err(|(error, _)| error).unwrap();
        let baseline = original.live_allocation_bytes();
        let gauge_bytes = std::alloc::Layout::array::<usize>(2).unwrap()
            .extend(std::alloc::Layout::new::<AtomicU64>()).unwrap().0
            .pad_to_align().size() as u64;
        let mut owners: Vec<Option<(Arc<NativeMountAllocationCustody>, u64)>> = vec![None; 4];
        for (index, action, bytes) in actions {
            let slot = &mut owners[usize::from(index)];
            let bytes = u64::from(bytes);
            match (slot.as_mut(), action) {
                (None, _) => {
                    let owner = NativeMountAllocationCustody::new(&mut original).unwrap();
                    *slot = Some((owner, 0));
                }
                (Some((owner, retained)), 0) => {
                    original.with_original_leaf(|work| owner.claim(bytes, work)).unwrap();
                    *retained += bytes;
                }
                (Some((owner, retained)), 1) => {
                    let bytes = bytes.min(*retained);
                    owner.retire(bytes).unwrap();
                    *retained -= bytes;
                }
                (Some(_), _) => { *slot = None; }
            }
            original.charge(WorkCounters::UNCHARGED).unwrap();
            let live = owners.iter().flatten().collect::<Vec<_>>();
            let state = original.state.lock();
            let registry_bytes = state.roots.native_mount_admissions.1.logical_bytes();
            let expected = baseline + registry_bytes + u64::from(!live.is_empty())
                + live.iter().map(|(_, retained)| {
                    std::mem::size_of::<NativeMountAllocationCustody>() as u64
                        + gauge_bytes + retained
                }).sum::<u64>();
            proptest::prop_assert_eq!(state.allocations.live_bytes(), expected);
            proptest::prop_assert_eq!(state.roots.native_mount_admissions.0.len(), live.len());
            proptest::prop_assert!(state.work.verify(BUDGET).is_ok());
        }
        let spent = original.receipt();
        drop(owners);
        original.charge(WorkCounters::UNCHARGED).unwrap();
        proptest::prop_assert_eq!(original.live_allocation_bytes(), baseline);
        proptest::prop_assert_eq!(original.receipt(), spent);
    }
}
