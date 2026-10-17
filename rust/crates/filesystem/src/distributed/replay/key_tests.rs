use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Authority, logical operation and immutable attempt ordinal uniquely bind a slot.
    #[test]
    fn operation_slots_are_stable_and_distinct(
        authority in any::<[u8;16]>(), operation in any::<[u8;16]>(), attempt in any::<u64>(),
    ) {
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
        let authority = AuthorityId::from_bytes(authority);
        let operation = OperationId::from_bytes(operation);
        let key = operation_key(authority, operation, attempt, &mut work).unwrap();
        prop_assert_eq!(&key, &operation_key(authority, operation, attempt, &mut work).unwrap());
        let mut changed_authority = authority.into_bytes();
        changed_authority[0] ^= 1;
        let mut changed_operation = operation.into_bytes();
        changed_operation[0] ^= 1;
        for (authority, operation, attempt) in [
            (AuthorityId::from_bytes(changed_authority), operation, attempt),
            (authority, OperationId::from_bytes(changed_operation), attempt),
            (authority, operation, attempt ^ 1),
        ] {
            prop_assert_ne!(&key, &operation_key(authority, operation, attempt, &mut work).unwrap());
        }
        prop_assert_ne!(operation_key(authority, operation, 0, &mut work).unwrap(), operation_key(authority, operation, 1, &mut work).unwrap());
    }
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn the_last_attempt_keeps_its_original_allocation_custody() {
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::BOUNDARY_BUDGET;
    let original = crate::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(error, _)| error)
        .unwrap();
    let mut native = AuthorityWork::native(&original, &cancellation);
    let mut ordinary = AuthorityWork::new(budget, &cancellation);
    let authority = AuthorityId::from_bytes([255; 16]);
    let operation = OperationId::from_bytes([255; 16]);
    let expected = operation_key(authority, operation, u64::MAX, &mut ordinary).unwrap();
    let before = original.receipt().bytes_copied;
    let actual = operation_key(authority, operation, u64::MAX, &mut native).unwrap();
    assert_eq!(actual, expected);
    original.receipt().verify(budget).unwrap();
    assert!(original.receipt().bytes_copied - before >= actual.as_bytes().len() as u64);
}
