use super::*;
use crate::{CancellationToken, WorkBudget, WorkCounters};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Zero retirement is an identity at every scope, including cancellation;
    // splitting and retiring real capacity still releases exactly those bytes.
    #[test]
    fn empty_retirement_preserves_capacity_receipts_and_resource_credit(
        bytes in 0_u64..256,
        split in 0_u64..256,
        depth in 0_u8..3,
        cancelled in any::<bool>(),
    ) {
        let cancel = CancellationToken::new();
        let root = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel).unwrap();
        let mut work = root.clone();
        for _ in 0..depth { work = work.native_volume_work(WorkBudget::UNBOUNDED).unwrap(); }
        let baseline = root.live_allocation_bytes();
        let resource_before = work.resource.as_ref().map(|r| r.remaining().unwrap().peak_allocation_bytes);
        let mut allocation = work.claim(bytes, 1, WorkCounters::UNCHARGED).unwrap();
        let mut part = allocation.split_off(split.min(bytes)).unwrap();
        let receipt = root.receipt();
        if cancelled { cancel.cancel(); }
        for _ in 0..4 {
            allocation.release(0).unwrap();
            part.release(0).unwrap();
        }
        prop_assert_eq!(root.receipt(), receipt);
        prop_assert_eq!(root.live_allocation_bytes(), baseline + bytes);
        let retained = allocation.bytes();
        prop_assert!(allocation.release(retained + 1).is_err());
        prop_assert_eq!(allocation.bytes(), retained);
        drop(part);
        prop_assert_eq!(root.live_allocation_bytes(), baseline + retained);
        drop(allocation);
        prop_assert_eq!(root.live_allocation_bytes(), baseline);
        prop_assert_eq!(root.receipt(), receipt);
        prop_assert_eq!(work.resource.as_ref().map(|r| r.remaining().unwrap().peak_allocation_bytes), resource_before);
    }
}
