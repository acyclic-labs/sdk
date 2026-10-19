use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    /// Checkout incarnation versioning preserves every configured volume's
    /// original state key, including its owner credentials and journals.
    #[test]
    fn configured_volumes_keep_their_existing_state(
        name in "[a-z0-9]{1,32}", owner in "[a-z0-9]{1,32}",
        nested in proptest::bool::ANY,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let path = if nested { directory.path().join("nested") } else { directory.path().to_path_buf() };
        fs::create_dir_all(&path).unwrap();
        let canonical = fs::canonicalize(&path).unwrap();
        let volume = VolumeRef::new(
            crate::resources::ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None).unwrap(),
            name, VolumeClass::Project, VolumeOwner::Project(owner), &mut None,
        ).unwrap();
        // Independent pre-PR key algorithm, also used to locate a persisted
        // owner. No checkout incarnation participates in a volume's key.
        let mut legacy = blake3::Hasher::new();
        legacy.update(b"acyclic.local.state/v1\0");
        legacy.update(b"volume\0");
        let bytes = canonical.as_os_str().as_encoded_bytes();
        legacy.update(&(bytes.len() as u64).to_le_bytes());
        legacy.update(bytes);
        legacy.update(volume.storage_name(&mut None).unwrap().as_bytes());
        let expected = directory.path().join(legacy.finalize().to_hex().as_str());
        let project = LocalProject::Volume { directory: path, volume };
        let state = state_directory_in(directory.path(), &project, None).unwrap();
        prop_assert_eq!(&state, &expected);
        let state = self::directory(&state).unwrap();
        let original = Owner::open(&state, &project, None).unwrap();
        let reopened = Owner::open(&state, &project, None).unwrap();
        prop_assert_eq!(original.key, reopened.key);
        prop_assert_eq!(original.task, reopened.task);

        // Restore the production defect: the same selection now misses that
        // persisted owner. Apply the same key-equality detector to the mutant.
        REKEY_VOLUMES.set(true);
        let mutant = state_directory_in(directory.path(), &project, None);
        REKEY_VOLUMES.set(false);
        prop_assert_ne!(mutant.unwrap(), expected);
    }
}
