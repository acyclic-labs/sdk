use super::*;
use proptest::prelude::*;

proptest! {
    // Property: every path buffer is funded in all enclosing volume scopes;
    // destroying it refunds that same custody without poisoning any scope.
    #[test]
    fn namespace_copies_fund_nested_volume_custody(
        names in prop::collection::vec("[a-z]{1,24}", 1..8),
        depth in 1..4_usize,
        append in any::<bool>(),
    ) {
        let budget = crate::performance::work_budget(1_000_000, 1_000_000);
        let original = crate::NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap();
        let mut volume = original.clone();
        for _ in 0..depth { volume = volume.native_volume_work(budget).unwrap(); }
        let limits = crate::model::VolumeLimits::default();
        let names = names.into_iter().map(|name| LogicalName::new(
            NameEncoding::Utf8, name.into_bytes(), limits.maximum_component_bytes,
        ).unwrap()).collect();
        let path = NamespacePath::new(names, limits).unwrap();
        let baseline = original.live_allocation_bytes();
        let available = volume.remaining().unwrap().peak_allocation_bytes;
        let mut receipt = GenerationReceipt::from_work(&volume).unwrap();
        let copied = clone_namespace(&path, append.then(|| path.components().first().unwrap()), limits, &mut receipt).unwrap();
        let bytes = receipt.custody.bytes();
        prop_assert!(bytes > 0);
        prop_assert_eq!(original.live_allocation_bytes(), baseline + bytes);
        prop_assert_eq!(volume.remaining().unwrap().peak_allocation_bytes, available - bytes);
        drop((copied, receipt));
        prop_assert_eq!(original.live_allocation_bytes(), baseline);
        prop_assert_eq!(volume.remaining().unwrap().peak_allocation_bytes, available);
        // Admission must fail before creating an unfunded path in a child.
        let denied = volume.native_volume_work(crate::WorkBudget { peak_allocation_bytes: 0, ..budget }).unwrap();
        let mut receipt = GenerationReceipt::from_work(&denied).unwrap();
        prop_assert!(clone_namespace(&path, None, limits, &mut receipt).is_err());
        prop_assert_eq!(receipt.custody.bytes(), 0);
        prop_assert!(original.remaining().is_ok());
    }
}
