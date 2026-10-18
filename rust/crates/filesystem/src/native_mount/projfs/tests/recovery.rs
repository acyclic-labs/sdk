use super::*;

#[tokio::test]
async fn detach_removes_projection_and_preserves_non_provider_content()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let folder = windows_path("nested");
    source.create_directory(&folder, FileMetadata::default(), &work)?;
    let seed = folder.child(windows_name("seed.txt"));
    source.create_file(&seed, FileMetadata::default(), None, &work)?;
    source.write_range(&seed, 0, Bytes::from_static(b"seed"))?;
    let (_root, destination, mut session) =
        mount_source(&source).map_err(|error| format!("mounting recovery fixture: {error}"))?;
    let projected = destination.join("nested/seed.txt");
    let mut permissions = std::fs::metadata(&projected)?.permissions();
    permissions.set_readonly(true);
    std::fs::set_permissions(&projected, permissions)?;
    let checkout = tempfile::tempdir()?;
    std::fs::write(checkout.path().join("keep.txt"), b"checkout")?;
    super::detach_projection(
        &destination,
        session
            .native_root_identity()
            .ok_or("missing root identity")?,
        source.as_ref(),
        &work,
        || {
            // Add unplanned entries after cleanup samples its held names,
            // while the provider still serves paths. Windows rejects new
            // path access after SDK stop until the root tag is removed.
            std::fs::write(destination.join("keep.txt"), b"unrelated")?;
            std::fs::write(destination.join("nested/keep.txt"), b"nested unrelated")?;
            std::os::windows::fs::symlink_dir(checkout.path(), destination.join("checkout"))?;
            session.stop().map(|_| ())
        },
    )
    .map_err(|error| format!("detaching recovery fixture: {error}"))?;
    assert!(!projected.exists());
    assert_eq!(std::fs::read(destination.join("keep.txt"))?, b"unrelated");
    assert_eq!(
        std::fs::read(destination.join("nested/keep.txt"))?,
        b"nested unrelated"
    );
    assert_eq!(
        std::fs::read(checkout.path().join("keep.txt"))?,
        b"checkout"
    );
    assert_eq!(super::reparse_tag(&destination)?.0, None);
    Ok(())
}
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
    reject_stale_projection(root.path()).expect_err("preserve stale root");
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
