use super::*;
use proptest::prelude::*;

fn work() -> NativeMaterializationWork {
    let cancel = acyclic_fs::CancellationToken::new();
    NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 4_000_000),
        &cancel,
    )
    .expect("fixture native work")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn retained_memory_changes_admission_without_changing_spent_copy_work(
        bytes in 1u64..4096, nodes in 1u64..32, prior in 1u64..16384,
    ) {
        let fresh = work();
        let retained = work();
        let fresh_hold = reserve(&fresh, bytes, nodes, 0).unwrap();
        let retained_hold = reserve(&retained, bytes, nodes, prior).unwrap();
        prop_assert_eq!(fresh.receipt().bytes_copied, retained.receipt().bytes_copied);
        prop_assert!(retained_hold.bytes() > fresh_hold.bytes());
        drop((fresh_hold, retained_hold));
        prop_assert_eq!(fresh.live_allocation_bytes(), retained.live_allocation_bytes());
    }
}
