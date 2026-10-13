use super::*;

#[cfg(test)]
mod host_file_race_tests {
    use super::*;

    #[test]
    fn rejects_a_replaced_file_between_path_metadata_and_open()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let current = temporary.path().join("current");
        let replacement = temporary.path().join("replacement");
        std::fs::write(&current, b"first")?;
        std::fs::write(&replacement, b"other")?;
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        std::fs::rename(&current, temporary.path().join("displaced"))?;
        std::fs::rename(&replacement, &current)?;
        let opened = root
            .open_file(
                Path::new("current"),
                &mut crate::native_host::ordinary_work(),
            )
            .map_err(crate::native_host::ordinary_error)?;
        assert!(matches!(
            ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?),
            Err(CaptureError::Kernel(crate::FsError::SourceSnapshotStale))
        ));
        Ok(())
    }

    #[test]
    fn rejects_a_changed_file_after_open() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let current = temporary.path().join("current");
        std::fs::write(&current, b"first")?;
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        let opened = root
            .open_file(
                Path::new("current"),
                &mut crate::native_host::ordinary_work(),
            )
            .map_err(crate::native_host::ordinary_error)?;
        ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?)?;
        std::fs::write(&current, b"changed body")?;
        assert!(matches!(
            ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?),
            Err(CaptureError::Kernel(crate::FsError::SourceSnapshotStale))
        ));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn rejects_a_permission_change_after_open() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::PermissionsExt;
        let temporary = tempfile::tempdir()?;
        let current = temporary.path().join("current");
        std::fs::write(&current, b"first")?;
        std::fs::set_permissions(&current, std::fs::Permissions::from_mode(0o644))?;
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        let opened = root
            .open_file(
                Path::new("current"),
                &mut crate::native_host::ordinary_work(),
            )
            .map_err(crate::native_host::ordinary_error)?;
        ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?)?;
        std::fs::set_permissions(&current, std::fs::Permissions::from_mode(0o600))?;
        assert!(matches!(
            ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?),
            Err(CaptureError::Kernel(crate::FsError::SourceSnapshotStale))
        ));
        Ok(())
    }

    #[test]
    fn creation_time_changes_require_two_observable_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("current"), b"first")?;
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let metadata = test_host_stat(&root, Path::new("current"))?;
        let mut expected = HostSnapshot::from_metadata(&metadata)?;
        expected.metadata.created_ns = MetadataField::Value(123);
        let mut observed = expected;
        observed.metadata.created_ns = MetadataField::Unavailable;
        ensure_same_host_snapshot(&expected, &observed)?;
        ensure_same_host_snapshot(&observed, &expected)?;
        observed.metadata.created_ns = MetadataField::Value(124);
        assert!(matches!(
            ensure_same_host_snapshot(&expected, &observed),
            Err(CaptureError::Kernel(crate::FsError::SourceSnapshotStale))
        ));
        Ok(())
    }

    #[test]
    fn rejects_a_replaced_directory_before_capture_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let current = temporary.path().join("current");
        let replacement = temporary.path().join("replacement");
        std::fs::create_dir(&current)?;
        std::fs::create_dir(&replacement)?;
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        std::fs::rename(&current, temporary.path().join("displaced"))?;
        std::fs::rename(&replacement, &current)?;
        let native =
            crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .map_err(|(error, _)| error)?;
        let scope = CaptureWorkScope::new(&native)?;
        assert!(matches!(
            ensure_current_host_node(
                &root,
                &test_host_path(Path::new("current")),
                &expected,
                &CancellationToken::new(),
                &scope,
            ),
            Err(CaptureError::Kernel(crate::FsError::SourceSnapshotStale))
        ));
        Ok(())
    }
}
