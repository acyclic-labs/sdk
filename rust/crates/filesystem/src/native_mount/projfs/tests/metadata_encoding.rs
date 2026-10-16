use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn direct_metadata_encoding_keeps_every_charge_and_budget_failure(
        tails in proptest::collection::vec(proptest::collection::vec(any::<u16>().prop_filter("canonical unit", |u| !matches!(u, 0 | 47 | 58 | 92)), 0..12), 0..5),
        limit in 0_u64..128,
    ) {
        crate::initialize_native_mount_services().expect("native services");
        let mut path = MountPath::root();
        for tail in tails {
            let bytes = std::iter::once(120_u16).chain(tail).flat_map(u16::to_le_bytes).collect();
            path = path.child(bytes);
        }
        let host = host_relative_path(&path).expect("host path");
        let budgets = [
            crate::WorkBudget::UNBOUNDED,
            crate::WorkBudget { items_examined: limit, ..crate::WorkBudget::UNBOUNDED },
            crate::WorkBudget { bytes_encoded: limit, ..crate::WorkBudget::UNBOUNDED },
            crate::WorkBudget { peak_allocation_bytes: limit, ..crate::WorkBudget::UNBOUNDED },
            crate::WorkBudget { allocation_operations: 0, ..crate::WorkBudget::UNBOUNDED },
        ];
        for budget in budgets {
            let cancellation = crate::CancellationToken::new();
            let old = crate::NativeMaterializationWork::new(budget, &cancellation).map_err(|(e, _)| e).expect("old work");
            let direct = crate::NativeMaterializationWork::new(budget, &cancellation).map_err(|(e, _)| e).expect("direct work");
            let old_result = crate::native_host::with_wide_path(&host, &mut &old, |wide, _| Ok(wide.to_vec()));
            let units = super::super::callback_path::wide_components(path.components()).expect("canonical parts");
            let direct_result = crate::native_host::with_wide_units(units, &mut &direct, |wide, _| Ok(wide.to_vec()));
            prop_assert_eq!(old_result.map_err(|e| e.to_string()), direct_result.map_err(|e| e.to_string()));
            prop_assert_eq!(old.receipt(), direct.receipt());
            cancellation.cancel();
            let before = direct.receipt();
            let units = super::super::callback_path::wide_components(path.components()).expect("canonical parts");
            prop_assert!(crate::native_host::with_wide_units(units, &mut &direct, |_, _| Ok(())).is_err());
            prop_assert_eq!(direct.receipt(), before);
        }
    }
}
