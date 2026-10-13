use super::*;

#[cfg(all(test, unix))]
mod native_view_baseline_tests {
    use super::*;

    #[test]
    fn native_view_keeps_canonical_stamps_for_the_same_root()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("file"), b"body")?;
        let root = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let identity = root.identity();
        let baseline = NativeViewBaseline::new(identity, false);
        let snapshot = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("file"))?)?;
        let prior = FileMetadata {
            changed_ns: MetadataField::Value(-123),
            created_ns: MetadataField::Value(-456),
            ..FileMetadata::default()
        };
        let mut observed = snapshot.metadata;
        baseline.restore_canonical_stamps(identity, prior, &mut observed);
        assert_eq!(observed.changed_ns, prior.changed_ns);
        #[cfg(target_os = "linux")]
        assert_eq!(observed.created_ns, prior.created_ns);

        let wrong_identity = NativeRootIdentity {
            device: identity.device,
            object: identity.object.wrapping_add(1),
        };
        let mut observed = snapshot.metadata;
        baseline.restore_canonical_stamps(wrong_identity, prior, &mut observed);
        assert_eq!(observed.changed_ns, snapshot.metadata.changed_ns);
        Ok(())
    }
}
