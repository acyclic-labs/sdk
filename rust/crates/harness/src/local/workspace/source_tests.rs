use super::*;
use acyclic_fs::demand::DemandSource as _;

#[tokio::test]
async fn held_source_keeps_former_recovery_names_and_refuses_root_replacement() -> Result<()> {
    acyclic_native_runtime::initialize_native_io_services().map_err(storage)?;
    let directory = tempfile::tempdir().map_err(storage)?;
    let path = directory.path().join("checkout");
    std::fs::create_dir(&path).map_err(storage)?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let held = acyclic_fs::hold_root(&path).map_err(storage)?;
    let identity = held.identity();
    let name = format!(
        ".acyclic-r-{}",
        uuid::Uuid::from_bytes(identity.to_bytes()).simple()
    );
    std::fs::create_dir(path.join(&name)).map_err(storage)?;
    std::fs::write(path.join(&name).join("authored"), b"retained").map_err(storage)?;
    let config = acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable);
    let prefix = acyclic_fs::kernel::NamespacePath::from_text_in_profile(
        &format!("/{name}"),
        config.profile,
        config.limits,
    )
    .map_err(storage)?;
    let source = held_source(
        acyclic_fs::hold_root(&path).map_err(storage)?,
        config,
        None,
        &work,
        &cancellation,
    )
    .await?;
    assert!(
        source
            .lookup(source.reference(), &prefix, &work, &cancellation)
            .await
            .map_err(storage)?
            .is_some()
    );
    drop(source);

    let retained = directory.path().join("retained-checkout");
    std::fs::rename(&path, &retained).map_err(storage)?;
    std::fs::create_dir(&path).map_err(storage)?;
    assert_ne!(work.root_identity(&path)?, identity);
    assert!(
        held_source(held, config, None, &work, &cancellation)
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read(retained.join(&name).join("authored")).map_err(storage)?,
        b"retained"
    );
    assert_eq!(std::fs::read_dir(&path).map_err(storage)?.count(), 0);
    Ok(())
}
