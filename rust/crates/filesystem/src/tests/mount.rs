use super::*;
use crate::cancellation::CancellationToken;
use crate::facade::Fs;
use crate::model::{CheckoutMode, GenerationSelector, Lifecycle, MutationMode, VolumeConfig};
use crate::performance::WorkBudget;

async fn checkout() -> Result<crate::facade::MemoryCheckout, Box<dyn std::error::Error>> {
    checkout_with_config(VolumeConfig::portable(Lifecycle::Ephemeral)).await
}

async fn checkout_with_config(
    config: VolumeConfig,
) -> Result<crate::facade::MemoryCheckout, Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::default();
    let volume = (fs.create_volume(config, WorkBudget::UNBOUNDED, &cancellation))
        .await?
        .value;
    Ok((volume.checkout(
        GenerationSelector::Head,
        CheckoutMode {
            access: AccessMode::ReadWrite,
            consistency: ConsistencyMode::Pinned,
            mutations: MutationMode::PrivateOverlay,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value)
}

async fn same_volume_checkouts(
    count: usize,
) -> Result<Vec<crate::facade::MemoryCheckout>, Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::default();
    let volume = (fs.create_volume(
        VolumeConfig::portable(Lifecycle::Ephemeral),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let mut checkouts = Vec::new();
    for _ in 0..count {
        checkouts.push(
            volume
                .checkout(
                    GenerationSelector::Head,
                    CheckoutMode {
                        access: AccessMode::ReadWrite,
                        consistency: ConsistencyMode::Pinned,
                        mutations: MutationMode::PrivateOverlay,
                    },
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?
                .value,
        );
    }
    Ok(checkouts)
}

#[tokio::test]
async fn routing_owns_checkouts_and_translates_relative_paths()
-> Result<(), Box<dyn std::error::Error>> {
    let root = checkout().await?;
    let scratch = checkout().await?;
    let root_volume = root.volume_id();
    let scratch_volume = scratch.volume_id();
    let mut view = MountedView::builder()
        .mount("/", root)?
        .mount("/.scratch", scratch)?
        .build()?;
    let nested = PortablePath::parse("/.scratch/task/file", crate::model::VolumeLimits::default())?;
    let routed = view.route_mut(&nested)?;
    assert_eq!(routed.checkout.volume_id(), scratch_volume);
    assert_eq!(
        routed
            .path
            .components()
            .iter()
            .map(crate::kernel::LogicalName::as_bytes)
            .collect::<Vec<_>>(),
        vec![b"task".as_slice(), b"file".as_slice()]
    );
    let root_file = PortablePath::parse("/file", crate::model::VolumeLimits::default())?;
    assert_eq!(
        view.resolve(&root_file).map(MountedCheckout::volume_id),
        Some(root_volume)
    );
    assert_eq!(
        view.validate_rename(&root_file, &nested),
        Err(MountError::CrossVolume)
    );
    let sibling = PortablePath::parse("/other", crate::model::VolumeLimits::default())?;
    assert_eq!(view.validate_rename(&root_file, &sibling), Ok(()));
    let exact_mount = PortablePath::parse("/.scratch", crate::model::VolumeLimits::default())?;
    assert!(view.route_mut(&exact_mount)?.path.components().is_empty());
    Ok(())
}

#[tokio::test]
async fn borrowed_routes_enforce_selected_profile_and_limits_without_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
    config.profile = crate::model::FilesystemProfile::Windows;
    config.limits.maximum_path_bytes = 16;
    config.limits.maximum_component_bytes = 8;
    config.limits.maximum_path_depth = 2;
    let mut view = MountedView::builder()
        .mount("/", checkout().await?)?
        .mount("/w", checkout_with_config(config).await?)?
        .build()?;
    let before = view.snapshot();
    let limits = crate::model::VolumeLimits::default();
    let unicode = PortablePath::parse("/w/é/𝄞", limits)?;
    let routed = view.route_mut(&unicode)?;
    assert_eq!(
        routed
            .path
            .components()
            .iter()
            .map(crate::kernel::LogicalName::as_bytes)
            .collect::<Vec<_>>(),
        vec![&[0xe9, 0x00][..], &[0x34, 0xd8, 0x1e, 0xdd][..]]
    );
    assert!(matches!(
        view.route_mut(&PortablePath::parse("/w/abcde", limits)?),
        Err(MountError::InvalidNamespacePath(NamespacePathError::Name(
            _
        )))
    ));
    assert!(matches!(
        view.route_mut(&PortablePath::parse("/w/aabb/ccdd", limits)?),
        Err(MountError::InvalidNamespacePath(
            NamespacePathError::PathTooLong
        ))
    ));
    assert!(matches!(
        view.route_mut(&PortablePath::parse("/w/123456789", limits)?),
        Err(MountError::InvalidPath(PathError::ComponentTooLong))
    ));
    assert!(matches!(
        view.route_mut(&PortablePath::parse("/w/a/b/c", limits)?),
        Err(MountError::InvalidPath(PathError::TooDeep))
    ));
    let sibling = PortablePath::parse("/wide/abcde", limits)?;
    assert_eq!(view.route_mut(&sibling)?.path.depth(), 2);
    assert_eq!(view.snapshot(), before);
    Ok(())
}

#[tokio::test]
async fn duplicate_and_missing_root_paths_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let duplicate = MountedView::builder()
        .mount("/", checkout().await?)?
        .mount("/", checkout().await?);
    assert!(matches!(duplicate, Err(MountError::DuplicatePath)));
    let missing_root = MountedView::builder()
        .mount("/scratch", checkout().await?)?
        .build();
    assert!(matches!(missing_root, Err(MountError::MissingRoot)));
    Ok(())
}

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

#[tokio::test]
async fn every_nested_route_and_rename_pair_is_deterministic_even_for_one_volume()
-> Result<(), Box<dyn std::error::Error>> {
    let mount_paths = ["/", "/a", "/a/b", "/a/b/c", "/a/é", "/z"];
    let checkouts = same_volume_checkouts(mount_paths.len()).await?;
    let expected_volume = checkouts.first().ok_or("missing checkout")?.volume_id();
    let mut builder = MountedView::builder();
    for (path, checkout) in mount_paths.iter().rev().zip(checkouts) {
        builder = builder.mount(path, checkout)?;
    }
    let mut view = builder.build()?;
    let routed_cases = [
        ("/", "/"),
        ("/root", "/"),
        ("/a", "/a"),
        ("/a/file", "/a"),
        ("/a/b", "/a/b"),
        ("/a/b/file", "/a/b"),
        ("/a/b/c", "/a/b/c"),
        ("/a/b/c/deep", "/a/b/c"),
        ("/a/bb", "/a"),
        ("/a/b/cousin", "/a/b"),
        ("/aa/b/c", "/"),
        ("/a/é/漢", "/a/é"),
        ("/a/éclair", "/a"),
        ("/z/file", "/z"),
    ];
    let limits = crate::model::VolumeLimits::default();
    for (path, expected_mount) in routed_cases {
        let path = PortablePath::parse(path, limits)?;
        let binding = view.resolve(&path).ok_or("path was not routed")?;
        assert_eq!(binding.path().as_str(), expected_mount);
        assert_eq!(binding.volume_id(), expected_volume);
        let expected_mount_id = binding.mount_id();
        let routed = view.route_mut(&path)?;
        assert_eq!(routed.mount_id, expected_mount_id);
        assert_eq!(routed.checkout.volume_id(), expected_volume);
        let relative = if expected_mount == "/" {
            path.as_str()
        } else {
            path.as_str()
                .strip_prefix(expected_mount)
                .ok_or("mount prefix mismatch")?
        };
        let expected_components = relative
            .split('/')
            .filter(|component| !component.is_empty())
            .map(str::as_bytes)
            .collect::<Vec<_>>();
        assert_eq!(
            routed
                .path
                .components()
                .iter()
                .map(crate::kernel::LogicalName::as_bytes)
                .collect::<Vec<_>>(),
            expected_components
        );
    }

    let representatives = ["/root", "/a/file", "/a/b/file", "/a/b/c/deep", "/z/file"]
        .map(|path| PortablePath::parse(path, limits))
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    for source in &representatives {
        for destination in &representatives {
            let same_binding = view.resolve(source).map(MountedCheckout::mount_id)
                == view.resolve(destination).map(MountedCheckout::mount_id);
            assert_eq!(
                view.validate_rename(source, destination),
                if same_binding {
                    Ok(())
                } else {
                    Err(MountError::CrossVolume)
                }
            );
        }
    }
    assert_eq!(
        view.snapshot()
            .bindings
            .iter()
            .map(|binding| binding.path.as_str())
            .collect::<Vec<_>>(),
        mount_paths
    );
    Ok(())
}
