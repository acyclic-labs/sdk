use super::*;

proptest::proptest! {
    #[test]
    fn pending_publication_files_stay_in_the_reserved_scratch_namespace(
        bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..512),
    ) {
        let directory = tempfile::tempdir().unwrap();
        let mut pending = publication_temp_file(directory.path()).unwrap();
        pending.write_all(&bytes).unwrap();
        let name = pending.path().file_name().unwrap().to_str().unwrap();
        // The generator's artifact inventory excludes this producer namespace.
        proptest::prop_assert!(name.starts_with(".tmp-"));
        proptest::prop_assert_eq!(fs::read(pending.path()).unwrap(), bytes);
        let path = pending.path().to_owned();
        drop(pending);
        proptest::prop_assert!(!path.exists());
    }
}

proptest::proptest! {
    #[test]
    fn native_digest_and_chunked_input_keep_sha256_identity(
        chunks in proptest::collection::vec(proptest::collection::vec(proptest::prelude::any::<u8>(), 0..512), 0..16),
    ) {
        let mut native = ring::digest::Context::new(&ring::digest::SHA256);
        let mut reference = Sha256::new();
        let bytes = chunks.concat();
        for chunk in &chunks {
            native.update(chunk);
            reference.update(chunk);
        }
        let expected = format!("{:x}", reference.finalize());
        let one_shot = sha256_hex(&bytes);
        proptest::prop_assert_eq!(one_shot.as_str(), expected.as_str());
        proptest::prop_assert_eq!(hex_digest(native.finish().as_ref()), expected);
    }
}
