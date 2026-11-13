use super::*;
use crate::{CancellationToken, WorkBudget, WorkCounters};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn ownership_transfer_is_atomic_and_preserves_original_debits(
        source_bytes in 0_u64..256,
        target_bytes in 0_u64..256,
        requested in 0_u64..512,
        relation in 0_u8..5,
        cancelled in any::<bool>(),
    ) {
        for bytes in [0, source_bytes, source_bytes + 1, requested] {
            let cancel = CancellationToken::new();
            let original = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel).unwrap();
            let foreign = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel).unwrap();
            let resource_budget = WorkBudget {
                peak_allocation_bytes: source_bytes + target_bytes,
                ..WorkBudget::UNBOUNDED
            };
            let source_work = if relation >= 3 {
                original.native_volume_work(resource_budget).unwrap()
            } else {
                original.clone()
            };
            let destination = match relation {
                0 => original.clone(),
                1 => original.native_volume_work(resource_budget).unwrap(),
                2 => foreign.clone(),
                3 => source_work.clone(),
                _ => original.native_volume_work(resource_budget).unwrap(),
            };
            let baseline = (original.live_allocation_bytes(), foreign.live_allocation_bytes());
            let resource_before = destination.resource.as_ref().map(|resource| resource.remaining().unwrap().peak_allocation_bytes);
            let source_resource_before = source_work.resource.as_ref().map(|resource| resource.remaining().unwrap().peak_allocation_bytes);
            let mut source = source_work.claim(source_bytes, 1, WorkCounters::UNCHARGED).unwrap();
            let mut target = destination.claim(target_bytes, 1, WorkCounters::UNCHARGED).unwrap();
            let receipts = (original.receipt(), foreign.receipt());
            let live = (original.live_allocation_bytes(), foreign.live_allocation_bytes());
            let resource = destination.resource.as_ref().map(|resource| resource.remaining().unwrap());
            if cancelled { cancel.cancel(); }
            let result = source.transfer_to(&mut target, bytes);
            let accepted = (relation == 0 || relation == 3) && bytes <= source_bytes;
            prop_assert_eq!(result.is_ok(), accepted);
            if relation == 1 || relation == 2 || relation == 4 {
                prop_assert!(matches!(result, Err(NativeTreeMaterializationError::InvalidLayout)));
            } else if bytes > source_bytes {
                prop_assert!(matches!(result, Err(NativeTreeMaterializationError::Work(crate::WorkError::Overflow))));
            }
            prop_assert_eq!((source.bytes(), target.bytes()), if accepted {
                (source_bytes - bytes, target_bytes + bytes)
            } else {
                (source_bytes, target_bytes)
            });
            prop_assert_eq!((original.receipt(), foreign.receipt()), receipts);
            prop_assert_eq!((original.live_allocation_bytes(), foreign.live_allocation_bytes()), live);
            prop_assert_eq!(destination.resource.as_ref().map(|resource| resource.remaining().unwrap()), resource);
            let source_retained = source.bytes();
            drop(source);
            let retained = target.bytes();
            prop_assert_eq!((original.live_allocation_bytes(), foreign.live_allocation_bytes()), if relation == 2 {
                (baseline.0, baseline.1 + retained)
            } else {
                (baseline.0 + retained, baseline.1)
            });
            prop_assert_eq!(destination.resource.as_ref().map(|resource| resource.remaining().unwrap().peak_allocation_bytes),
                resource.map(|remaining| remaining.peak_allocation_bytes + if relation == 3 { source_retained } else { 0 }));
            prop_assert_eq!(source_work.resource.as_ref().map(|resource| resource.remaining().unwrap().peak_allocation_bytes),
                source_resource_before.map(|capacity| capacity - if relation == 3 { retained } else { 0 }));
            drop(target);
            prop_assert_eq!((original.live_allocation_bytes(), foreign.live_allocation_bytes()), baseline);
            prop_assert_eq!(destination.resource.as_ref().map(|resource| resource.remaining().unwrap().peak_allocation_bytes), resource_before);
            prop_assert_eq!(source_work.resource.as_ref().map(|resource| resource.remaining().unwrap().peak_allocation_bytes), source_resource_before);
        }
    }

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
