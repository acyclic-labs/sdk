use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn direct_metadata_encoding_keeps_every_charge_and_budget_failure(
        tails in proptest::collection::vec(proptest::collection::vec(any::<u16>().prop_filter("canonical unit", |u| !matches!(u, 0 | 47 | 58 | 92)), 0..12), 0..5),
        limit in 0_u64..128,
    ) {
        crate::initialize_native_mount_services().map_err(|e| TestCaseError::fail(e.to_string()))?;
        let mut path = MountPath::root();
        for tail in tails {
            let bytes = std::iter::once(120_u16).chain(tail).flat_map(u16::to_le_bytes).collect();
            path = path.child(bytes);
        }
        let host = host_relative_path(&path).map_err(|e| TestCaseError::fail(e.to_string()))?;
        let overhead = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map_err(|(e, _)| TestCaseError::fail(e.to_string()))?.receipt();
        let budgets = [
            crate::WorkBudget::UNBOUNDED,
            crate::WorkBudget { items_examined: limit, ..crate::WorkBudget::UNBOUNDED },
            crate::WorkBudget { bytes_encoded: limit, ..crate::WorkBudget::UNBOUNDED },
            crate::WorkBudget { peak_allocation_bytes: overhead.peak_allocation_bytes + limit, ..crate::WorkBudget::UNBOUNDED },
            crate::WorkBudget { allocation_operations: overhead.allocation_operations, ..crate::WorkBudget::UNBOUNDED },
        ];
        for budget in budgets {
            let cancellation = crate::CancellationToken::new();
            let old = crate::NativeMaterializationWork::new(budget, &cancellation).map_err(|(e, _)| TestCaseError::fail(e.to_string()))?;
            let direct = crate::NativeMaterializationWork::new(budget, &cancellation).map_err(|(e, _)| TestCaseError::fail(e.to_string()))?;
            let before = direct.receipt();
            let old_result = crate::native_host::with_wide_path(&host, &mut &old, |wide, _| Ok(wide.to_vec()));
            let units = super::super::callback_path::wide_components(path.components()).ok_or_else(|| TestCaseError::fail("canonical parts"))?;
            let direct_result = crate::native_host::with_wide_units(units, &mut &direct, |wide, _| Ok(wide.to_vec()));
            prop_assert_eq!(old_result.map_err(|e| e.to_string()), direct_result.map_err(|e| e.to_string()));
            prop_assert_eq!(old.receipt(), direct.receipt());
            if budget == crate::WorkBudget::UNBOUNDED {
                let length = path.components().iter().map(|p| p.len() as u64 / 2).sum::<u64>()
                    + path.components().len().saturating_sub(1) as u64;
                let mut expected = before;
                expected.items_examined += length;
                expected.bytes_encoded += length * 2;
                expected.allocation_operations += u64::from(length != 0);
                expected.peak_allocation_bytes += length * 2;
                prop_assert_eq!(direct.receipt(), expected);
            }
            cancellation.cancel();
            let before = direct.receipt();
            let units = super::super::callback_path::wide_components(path.components()).ok_or_else(|| TestCaseError::fail("canonical parts"))?;
            prop_assert!(crate::native_host::with_wide_units(units, &mut &direct, |_, _| Ok(())).is_err());
            prop_assert_eq!(direct.receipt(), before);
        }
    }
}
