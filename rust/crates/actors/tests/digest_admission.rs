//! Digest admission snapshots the caller without a temporary heap buffer.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_actors::domain::{CodeSha256, DomainError};

acyclic_test_support::counting_allocator!();

#[test]
fn digest_admission_owns_bytes_without_a_temporary_allocation() {
    for (length, byte, valid) in [
        (31, 1, false),
        (32, 0, false),
        (32, 1, true),
        (33, 1, false),
    ] {
        let mut input = vec![byte; length];
        let mut admitted = None;
        let cost = acyclic_test_support::measure(|| {
            admitted = Some(CodeSha256::new(input.as_slice()));
        });
        assert_eq!(cost.count_total, 0);
        assert_eq!(cost.bytes_total, 0);
        input.fill(0);
        match admitted.expect("admission completed synchronously") {
            Ok(digest) => {
                assert!(valid);
                assert_eq!(digest.as_bytes(), &[1; 32]);
            }
            Err(error) => {
                assert!(!valid);
                assert_eq!(error, DomainError::InvalidCodeSha256);
            }
        }
    }
}
