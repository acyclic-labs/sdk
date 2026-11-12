use super::*;
use acyclic_fs::demand::DemandSource as _;

#[tokio::test]
async fn recovery_exclusion_is_bound_to_the_held_checkout_before_startup() -> Result<()> {
    acyclic_native_runtime::initialize_native_io_services().map_err(storage)?;
    let directory = tempfile::tempdir().map_err(storage)?;
    let path = directory.path().join("checkout");
    std::fs::create_dir(&path).map_err(storage)?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let held = acyclic_fs::hold_root(&path).map_err(storage)?;
    let identity = held.identity();
    let state = LocalCoreStateStore::new(directory.path().join("state"));
    state
        .bind_native_recovery_root(&path, identity, &work)
        .await
        .map_err(storage)?;
    let name = acyclic_fs::NativeRecoveryRoot::name(identity);
    let name = std::str::from_utf8(&name).map_err(storage)?;
    std::fs::write(path.join(name).join("recovery-copy"), b"retained").map_err(storage)?;
    let config = acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable);
    let prefix = acyclic_fs::NativeRecoveryRoot::prefix(identity, config.profile, config.limits)
        .map_err(storage)?;
    let source = held_source(
        acyclic_fs::hold_root(&path).map_err(storage)?,
        &state,
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
            .is_none()
    );
    drop(source);

    let retained = directory.path().join("retained-checkout");
    std::fs::rename(&path, &retained).map_err(storage)?;
    std::fs::create_dir(&path).map_err(storage)?;
    let replacement = work.root_identity(&path)?;
    assert_ne!(replacement, identity);
    // Reopening only the path accepts this replacement. Startup must retain
    // the held checkout's identity for both exclusion and inspection instead.
    state
        .inspect_native_recovery_root(&path, replacement, &work)
        .await
        .map_err(storage)?;
    assert!(
        held_source(held, &state, config, None, &work, &cancellation)
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read(retained.join(name).join("recovery-copy")).map_err(storage)?,
        b"retained"
    );
    assert_eq!(std::fs::read_dir(&path).map_err(storage)?.count(), 0);
    Ok(())
}
