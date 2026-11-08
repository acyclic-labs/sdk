use super::*;
use crate::NativeMaterializationWork;

type TestResult = Result<(), Box<dyn std::error::Error>>;
const BUDGET: WorkBudget = crate::performance::BOUNDARY_BUDGET;

#[test]
fn logical_growth_uses_retired_credit_before_admission() -> TestResult {
    let cancellation = CancellationToken::new();
    let mut original = NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
    let mut allocation = original.claim(0, 0, WorkCounters::UNCHARGED)?;
    let retired = NativeMountAllocationCustody::new(&mut original)?;
    let survivor = NativeMountAllocationCustody::new(&mut original)?;
    drop(retired);

    let remaining = BUDGET.peak_allocation_bytes - original.live_allocation_bytes();
    allocation.resize(remaining)?;
    assert_eq!(
        original.live_allocation_bytes(),
        BUDGET.peak_allocation_bytes
    );
    drop(allocation);
    drop(survivor);
    Ok(())
}

proptest::proptest! {
    // Exercise the real owner and ledger: every retirement immediately returns
    // original credit without copying/scanning survivors or admitting new work.
    #[test]
    fn custody_retirement_preserves_original_credit_and_receipts(
        actions in proptest::collection::vec((0u8..4, 0u8..3, 0u16..257), 1..65),
    ) {
        let cancellation = CancellationToken::new();
        let mut original = NativeMaterializationWork::new(BUDGET, &cancellation)
            .map_err(|(error, _)| error).unwrap();
        let mut foreign = NativeMaterializationWork::new(BUDGET, &CancellationToken::new()).unwrap();
        let baseline = original.live_allocation_bytes();
        let mut owners: Vec<Option<(Arc<NativeMountAllocationCustody>, u64)>> = vec![None; 4];
        for (index, action, bytes) in actions {
            let slot = &mut owners[usize::from(index)];
            let bytes = u64::from(bytes);
            match (slot.as_mut(), action) {
                (None, _) => {
                    let owner = NativeMountAllocationCustody::new(&mut original).unwrap();
                    // A different ledger cannot adopt or grow this live owner.
                    let before = foreign.receipt();
                    proptest::prop_assert!(matches!(
                        foreign.with_original_leaf(|work| owner.claim(1, work)),
                        Err(DemandError::InvalidRequest)
                    ));
                    proptest::prop_assert_eq!(foreign.receipt(), before);
                    *slot = Some((owner, 0));
                }
                (Some((owner, retained)), 0) => {
                    original.with_original_leaf(|work| owner.claim(bytes, work)).unwrap();
                    *retained += bytes;
                }
                (Some((owner, retained)), 1) => {
                    // Over-retirement must leave all original credit intact.
                    proptest::prop_assert!(owner.retire(*retained + 1).is_err());
                    let before = original.receipt();
                    let bytes = bytes.min(*retained);
                    owner.retire(bytes).unwrap();
                    *retained -= bytes;
                    proptest::prop_assert_eq!(original.receipt(), before);
                }
                (Some(_), _) => {
                    let before = original.receipt();
                    *slot = None;
                    proptest::prop_assert_eq!(original.receipt(), before);
                }
            }
            let live = owners.iter().flatten().collect::<Vec<_>>();
            let expected = baseline
                + u64::from(!live.is_empty()) * NativeAllocationAccounting::ALLOCATION_BYTES
                + live.iter().map(|(_, retained)| {
                    NativeMountAllocationCustody::ALLOCATION_BYTES + retained
                }).sum::<u64>();
            proptest::prop_assert_eq!(original.live_allocation_bytes(), expected);
            let projected = original.remaining().unwrap();
            let spent = original.receipt();
            original.charge(WorkCounters::UNCHARGED).unwrap();
            proptest::prop_assert_eq!(original.remaining().unwrap(), projected);
            proptest::prop_assert_eq!(original.receipt(), spent);
            proptest::prop_assert!(spent.verify(BUDGET).is_ok());
        }
        let spent = original.receipt();
        cancellation.cancel();
        drop(owners);
        // Cancellation cannot require an admitted operation to retire storage.
        proptest::prop_assert_eq!(original.live_allocation_bytes(), baseline);
        proptest::prop_assert_eq!(original.receipt(), spent);
    }
}
