use super::*;

#[test]
fn rejects_a_replaced_file_between_path_metadata_and_open() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    let current = temporary.path().join("current");
    let replacement = temporary.path().join("replacement");
    std::fs::write(&current, b"first")?;
    std::fs::write(&replacement, b"other")?;
    let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let expected = test_host_stat(&root, Path::new("current"))?;
    std::fs::rename(&current, temporary.path().join("displaced"))?;
    std::fs::rename(&replacement, &current)?;
    let opened = test_open_file(&root, Path::new("current"))?;
    assert!(matches!(
        ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?),
        Err(CaptureError::Kernel(error)) if matches!(*error, crate::FsError::SourceSnapshotStale)
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
    let expected = test_host_stat(&root, Path::new("current"))?;
    let opened = test_open_file(&root, Path::new("current"))?;
    ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?)?;
    std::fs::write(&current, b"changed body")?;
    assert!(matches!(
        ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?),
        Err(CaptureError::Kernel(error)) if matches!(*error, crate::FsError::SourceSnapshotStale)
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
    let expected = test_host_stat(&root, Path::new("current"))?;
    let opened = test_open_file(&root, Path::new("current"))?;
    ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?)?;
    std::fs::set_permissions(&current, std::fs::Permissions::from_mode(0o600))?;
    assert!(matches!(
        ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?),
        Err(CaptureError::Kernel(error)) if matches!(*error, crate::FsError::SourceSnapshotStale)
    ));
    Ok(())
}

#[test]
fn rejects_a_replaced_directory_before_capture_admission() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    let current = temporary.path().join("current");
    let replacement = temporary.path().join("replacement");
    std::fs::create_dir(&current)?;
    std::fs::create_dir(&replacement)?;
    let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let expected = test_host_stat(&root, Path::new("current"))?;
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
            &scope.native,
        ),
        Err(CaptureError::Kernel(error)) if matches!(*error, crate::FsError::SourceSnapshotStale)
    ));
    Ok(())
}
