use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn reconciliation_keys_retain_both_complete_generation_references(
        operation in any::<[u8; 16]>(),
        digest in any::<[u8; 32]>(),
        locator in any::<[u8; 32]>(),
        root in any::<[u8; 16]>(),
        origin in any::<[u8; 32]>(),
    ) {
        let operation = OperationId::from_bytes(operation);
        let mut object = acyclic_fs::ObjectId {
            kind: acyclic_fs::ObjectKind::GenerationRoot,
            digest: acyclic_fs::Digest::from_bytes(digest),
            storage: Some(acyclic_fs::ObjectStorage {
                locator: acyclic_fs::Digest::from_bytes(locator),
                root_file_id: acyclic_fs::FileId::from_bytes(root),
                origin_generation: acyclic_fs::Digest::from_bytes(origin),
            }),
        };
        let sealed = acyclic_fs::GenerationId::from_root(object);
        let original = sealed.to_bytes();
        let key = reconciliation_operation(operation, b"before", sealed, sealed);

        // Keep the canonical tag and mutate every digest, locator, root and
        // origin byte. Equal plaintext digests must not share replay custody.
        for index in 1..original.len() {
            let mut changed = [0; acyclic_fs::ObjectId::ENCODED_LEN];
            changed.copy_from_slice(original.as_slice());
            changed[index] ^= 1;
            let changed = acyclic_fs::GenerationId::from_bytes(&changed)
                .ok_or_else(|| TestCaseError::fail("canonical reference mutation refused"))?;
            prop_assert_ne!(reconciliation_operation(operation, b"before", changed, sealed), key);
            prop_assert_ne!(reconciliation_operation(operation, b"before", sealed, changed), key);
        }

        object.storage = None;
        let plain = acyclic_fs::GenerationId::from_root(object);
        prop_assert_eq!(plain.digest(), sealed.digest());
        prop_assert_ne!(reconciliation_operation(operation, b"before", plain, sealed), key);
        prop_assert_ne!(reconciliation_operation(operation, b"before", sealed, plain), key);
    }
}
