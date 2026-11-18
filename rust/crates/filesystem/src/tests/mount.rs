use super::*;

#[path = "../../tests/support/mount_checkout.rs"]
mod fixture;
use fixture::checkout;

#[tokio::test]
async fn mounted_checkout_accessors_and_snapshot_are_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let root = checkout().await?;
    let expected_volume = root.volume_id();
    let expected_generation = root.generation_id();
    let mut view = MountedView::builder().mount("/", root)?.build()?;
    let binding = view
        .resolve(&PortablePath::parse(
            "/file",
            crate::model::VolumeLimits::default(),
        )?)
        .ok_or("root mount was not resolved")?;
    assert_eq!(binding.path().as_str(), "/");
    assert_eq!(binding.volume_id(), expected_volume);
    assert_eq!(binding.checkout().generation_id(), expected_generation);
    let mount_id = binding.mount_id();

    let routed = view.route_mut(&PortablePath::parse(
        "/file",
        crate::model::VolumeLimits::default(),
    )?)?;
    assert_eq!(routed.mount_id, mount_id);
    assert_eq!(routed.checkout.volume_id(), expected_volume);
    let _ = routed.checkout;
    let binding = view
        .bindings
        .first_mut()
        .ok_or("root binding disappeared")?;
    assert_eq!(binding.checkout_mut().generation_id(), expected_generation);

    assert_eq!(
        view.snapshot().bindings,
        vec![MountedGeneration {
            mount_id,
            path: PortablePath::parse("/", crate::model::VolumeLimits::default())?,
            volume_id: expected_volume,
            generation: expected_generation,
            access: AccessMode::ReadWrite,
            consistency: ConsistencyMode::Pinned,
        }]
    );
    Ok(())
}

#[tokio::test]
async fn mount_routing_failures_are_typed_and_non_mutating()
-> Result<(), Box<dyn std::error::Error>> {
    let path = PortablePath::parse("/unmapped", crate::model::VolumeLimits::default())?;
    let mut empty: MountedView<crate::MemoryAuthorityStore, crate::MemoryObjectStore> =
        MountedView {
            bindings: Vec::new(),
            by_path: PathPrefixIndex::default(),
        };
    assert!(matches!(
        empty.route_mut(&path),
        Err(MountError::UnmappedPath)
    ));
    assert_eq!(
        empty.validate_rename(&path, &path),
        Err(MountError::UnmappedPath)
    );
    assert_eq!(
        relative_path(
            &path,
            &PortablePath::parse("/other", crate::model::VolumeLimits::default())?,
        ),
        Err(MountError::UnmappedPath)
    );
    assert_eq!(
        relative_path(
            &path,
            &PortablePath::parse("/un", crate::model::VolumeLimits::default())?,
        ),
        Err(MountError::UnmappedPath)
    );
    assert!(matches!(
        MountedView::builder().mount("relative", checkout().await?),
        Err(MountError::InvalidPath(_))
    ));
    for error in [
        MountError::DuplicatePath,
        MountError::MissingRoot,
        MountError::UnmappedPath,
        MountError::CrossVolume,
    ] {
        assert!(!error.to_string().is_empty());
    }
    Ok(())
}
