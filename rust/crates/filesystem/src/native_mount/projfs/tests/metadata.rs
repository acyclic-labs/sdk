use super::*;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn metadata_edit_after_a_boundary_through_a_handle_opened_before_it_is_captured()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let (_root, destination, mut session) = mount_source(&source)?;
    let file = destination.join("late.txt");
    client::run("late", &file, || {
        session.flush_callbacks(&work)?;
        Ok(())
    })?;
    session.flush_callbacks(&work)?;
    let captured = source
        .lookup(&windows_path("late.txt"))?
        .ok_or("late.txt was not captured")?;
    assert_eq!(
        captured.metadata.modified_ns,
        crate::kernel::MetadataField::Value(978_307_200_000_000_000),
        "the edit made after the boundary was lost"
    );
    session.stop()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn a_time_set_through_a_writable_handle_stays() -> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    const FILES: usize = 120;
    let started = std::time::Instant::now();
    let source = windows_checkout_source_with_files(FILES).await?;
    let (_root, destination, mut session) = mount_source(&source)?;
    eprintln!("stress mounted {:?}", started.elapsed());
    client::run("stress", &destination, || Ok(()))?;
    eprintln!("stress client complete {:?}", started.elapsed());
    session.flush_callbacks(&work)?;
    session.revalidate(&work)?;
    eprintln!("stress revalidated {:?}", started.elapsed());
    let wanted = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
    let mut lost = Vec::new();
    for index in 0..FILES {
        let on_disk = std::fs::metadata(destination.join(format!("f{index}.txt")))?.modified()?;
        let recorded = source
            .lookup(&windows_path(&format!("f{index}.txt")))?
            .ok_or("file gone from the source")?
            .metadata
            .modified_ns;
        if on_disk != wanted
            || recorded != crate::kernel::MetadataField::Value(1_700_000_000_000_000_000)
        {
            lost.push((index, on_disk, recorded));
        }
    }
    assert!(lost.is_empty(), "times lost: {lost:?}");
    session.stop()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn external_metadata_only_change_survives_remount() -> Result<(), Box<dyn std::error::Error>>
{
    if let Some(mode) = std::env::var_os(client::MODE) {
        return client::execute(&mode);
    }
    let work = setup_work()?;
    let root = tempfile::tempdir()?;
    let workspace = local_workspace(root.path(), "metadata-projfs", &work).await?;
    workspace
        .write("/metadata.txt", Bytes::from_static(b"unchanged contents"))
        .await?;

    let first = root.path().join("first-mount");
    std::fs::create_dir(&first)?;
    let mount = workspace.mount(&first, manual_mount_options()).await?;
    // Nested paths retain the handle-opening validator and its callback guard.
    std::fs::create_dir(first.join("nested"))?;
    std::fs::write(first.join("nested/metadata.txt"), b"unchanged contents")?;
    mount.sync().await?;
    for relative in ["metadata.txt", "nested/metadata.txt"] {
        let projected = first.join(relative);
        for _ in 0..3 {
            assert_eq!(std::fs::read(&projected)?, b"unchanged contents");
        }
        client::run("readonly", &projected, || Ok(()))?;
    }
    mount.sync().await?;
    mount.unmount().await?;

    let second = root.path().join("second-mount");
    std::fs::create_dir(&second)?;
    let remount = workspace.mount(&second, manual_mount_options()).await?;
    for relative in ["metadata.txt", "nested/metadata.txt"] {
        assert!(
            std::fs::metadata(second.join(relative))?
                .permissions()
                .readonly(),
            "metadata-only edits must survive synchronization and remount: {relative}"
        );
    }
    remount.unmount().await?;
    Ok(())
}

#[path = "metadata_client.rs"]
mod client;
