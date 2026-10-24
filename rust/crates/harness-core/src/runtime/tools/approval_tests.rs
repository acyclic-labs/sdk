use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn approval_subject_copy_charges_every_text_byte_and_releases_custody(
        strings in prop::collection::vec(".{0,24}", 0..6),
        count in any::<i32>(), enabled in any::<bool>(),
    ) {
        let invocation = ToolInvocation {
            operation_id: OperationId::from_bytes([1;16]),
            call_id: "call".into(), name: "tool".into(),
            arguments: serde_json::json!({"payload": strings, "count": count, "enabled": enabled}),
        };
        let expected = 8 + 7 + 5 + 7 + strings.iter().map(|text| text.len() as u64).sum::<u64>();
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(4096, 1 << 20), &cancellation,
        ).unwrap();
        let baseline = work.live_allocation_bytes();
        let paid = copy_approval_subject(&invocation, Some(&work)).unwrap();
        prop_assert_eq!((work.receipt().bytes_copied, paid.1.is_some()), (expected, true));
        prop_assert_eq!(&paid.0, &ApprovalSubject::Tool {
            tool: invocation.name.clone(), call_id: invocation.call_id.clone(), arguments: invocation.arguments.clone(),
        });
        prop_assert!(paid.1.as_ref().unwrap().work().shares_ledger(&work));
        prop_assert!(paid.1.as_ref().unwrap().bytes() > 0);
        drop(paid);
        prop_assert_eq!(work.live_allocation_bytes(), baseline);
        // Negative control: forgetting the original work still copies the same
        // value, but cannot satisfy the exact copy charge or custody assertion.
        let control = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(4096, 1 << 20), &cancellation,
        ).unwrap();
        let unmetered = copy_approval_subject(&invocation, None).unwrap();
        prop_assert_eq!(&unmetered.0, &ApprovalSubject::Tool {
            tool: invocation.name, call_id: invocation.call_id, arguments: invocation.arguments,
        });
        prop_assert_ne!((control.receipt().bytes_copied, unmetered.1.is_some()), (expected, true));
    }
}

#[test]
fn subject_copy_denies_exhausted_copy_work_before_allocating() {
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut budget = acyclic_fs::performance::work_budget(4096, 1 << 20);
    budget.bytes_copied = 0;
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let invocation = ToolInvocation {
        operation_id: OperationId::from_bytes([1; 16]),
        call_id: "call".into(),
        name: "tool".into(),
        arguments: serde_json::json!({"payload":"copy"}),
    };
    let baseline = work.live_allocation_bytes();
    let allocations = work.receipt().allocation_operations;
    assert!(copy_approval_subject(&invocation, Some(&work)).is_err());
    assert_eq!(work.receipt().allocation_operations, allocations);
    assert_eq!(work.live_allocation_bytes(), baseline);
}
