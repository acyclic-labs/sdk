use super::*;
#[test]
fn ordinary_held_root_has_no_reparse_tag() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let work = setup_work()?;
    let (directory, _) = crate::native_host::open_root_directory(root.path(), &mut &work)?;
    assert_eq!(super::reparse_tag_held(&directory, &mut &work)?, None);
    Ok(())
}

#[test]
fn staging_refuses_a_link_in_place_of_its_directory() -> Result<(), Box<dyn std::error::Error>> {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("mount");
    std::fs::create_dir(&root)?;
    let work = setup_work()?;
    assert!(super::staging_directory(&root, &work).is_ok());
    std::fs::remove_dir(super::staging_path(&root))?;
    let elsewhere = tempfile::tempdir()?;
    std::os::windows::fs::symlink_dir(elsewhere.path(), super::staging_path(&root))?;
    assert!(super::staging_directory(&root, &work).is_err());
    Ok(())
}

#[test]
#[ignore = "requires an enabled ProjFS filter"]
fn stale_projection_is_preserved_for_recovery() {
    let root = tempfile::tempdir().expect("root");
    let wide = HSTRING::from(root.path().as_os_str());
    let guid = GUID::from_values(0, 0, 0, [0; 8]);
    // SAFETY: pointers remain live for this synchronous call.
    unsafe {
        PrjMarkDirectoryAsPlaceholder(
            PCWSTR::from_raw(wide.as_ptr()),
            PCWSTR::null(),
            None,
            &raw const guid,
        )
    }
    .expect("mark projection root");
    let error = reject_stale_projection(root.path()).expect_err("preserve stale root");
    assert!(error.to_string().contains("preserved for recovery"));
    assert!(root.path().exists());
}

#[test]
fn projection_cleanup_rejects_a_replaced_root_identity() -> Result<(), Box<dyn std::error::Error>> {
    let original = tempfile::tempdir()?;
    let replacement = tempfile::tempdir()?;
    let original_identity = crate::capture_root_identity(original.path())?;
    let work = setup_work()?;
    assert!(
        super::remove_empty_recovery_destination(
            replacement.path(),
            Some(original_identity),
            &mut &work
        )
        .is_err()
    );
    assert!(replacement.path().exists());
    Ok(())
}
