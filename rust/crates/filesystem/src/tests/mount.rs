use super::*;
use crate::async_storage::poll_ready;
use crate::cancellation::CancellationToken;
use crate::facade::Fs;
use crate::model::{CheckoutMode, GenerationSelector, Lifecycle, MutationMode, VolumeConfig};
use crate::performance::WorkBudget;

fn checkout() -> Result<
    Checkout<crate::MemoryAuthorityStore, crate::MemoryObjectStore>,
    Box<dyn std::error::Error>,
> {
    let fs = Fs::memory_bounded(crate::model::VolumeLimits::default().maximum_object_bytes)?;
    let cancellation = CancellationToken::default();
    let volume = poll_ready(fs.create_volume(
        VolumeConfig::portable(Lifecycle::Ephemeral),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    Ok(poll_ready(volume.checkout(
        GenerationSelector::Head,
        CheckoutMode {
            access: AccessMode::ReadWrite,
            consistency: ConsistencyMode::Pinned,
            mutations: MutationMode::PrivateOverlay,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("checkout blocked")??
    .value)
}

fn same_volume_checkouts(
    count: usize,
) -> Result<
    Vec<Checkout<crate::MemoryAuthorityStore, crate::MemoryObjectStore>>,
    Box<dyn std::error::Error>,
> {
    let fs = Fs::memory_bounded(crate::model::VolumeLimits::default().maximum_object_bytes)?;
    let cancellation = CancellationToken::default();
    let volume = poll_ready(fs.create_volume(
        VolumeConfig::portable(Lifecycle::Ephemeral),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("volume creation blocked")??
    .value;
    (0..count)
        .map(|_| {
            poll_ready(volume.checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::PrivateOverlay,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .ok_or_else(|| "checkout blocked".into())
            .and_then(|result| result.map(|receipt| receipt.value).map_err(Into::into))
        })
        .collect()
}

#[test]
fn routing_owns_checkouts_and_translates_relative_paths() -> Result<(), Box<dyn std::error::Error>>
{
    let root = checkout()?;
    let scratch = checkout()?;
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

#[test]
fn duplicate_and_missing_root_paths_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let duplicate = MountedView::builder()
        .mount("/", checkout()?)?
        .mount("/", checkout()?);
    assert!(matches!(duplicate, Err(MountError::DuplicatePath)));
    let missing_root = MountedView::builder()
        .mount("/scratch", checkout()?)?
        .build();
    assert!(matches!(missing_root, Err(MountError::MissingRoot)));
    Ok(())
}

#[test]
fn mounted_checkout_accessors_and_snapshot_are_exact() -> Result<(), Box<dyn std::error::Error>> {
    let root = checkout()?;
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
        .get_mut(&PortablePath::parse(
            "/",
            crate::model::VolumeLimits::default(),
        )?)
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

#[test]
fn mount_routing_failures_are_typed_and_non_mutating() -> Result<(), Box<dyn std::error::Error>> {
    let path = PortablePath::parse("/unmapped", crate::model::VolumeLimits::default())?;
    let mut empty: MountedView<crate::MemoryAuthorityStore, crate::MemoryObjectStore> =
        MountedView {
            bindings: BTreeMap::new(),
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
            crate::model::VolumeLimits::default(),
        ),
        Err(MountError::UnmappedPath)
    );
    assert!(matches!(
        MountedView::builder().mount("relative", checkout()?),
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

#[test]
fn every_nested_route_and_rename_pair_is_deterministic_even_for_one_volume()
-> Result<(), Box<dyn std::error::Error>> {
    let mount_paths = ["/", "/a", "/a/b", "/a/b/c", "/z"];
    let checkouts = same_volume_checkouts(mount_paths.len())?;
    let expected_volume = checkouts.first().ok_or("missing checkout")?.volume_id();
    let mut builder = MountedView::builder();
    for (path, checkout) in mount_paths.iter().zip(checkouts) {
        builder = builder.mount(path, checkout)?;
    }
    let view = builder.build()?;
    let routed_cases = [
        ("/root", "/"),
        ("/a", "/a"),
        ("/a/file", "/a"),
        ("/a/b", "/a/b"),
        ("/a/b/file", "/a/b"),
        ("/a/b/c", "/a/b/c"),
        ("/a/b/c/deep", "/a/b/c"),
        ("/a/bb", "/a"),
        ("/z/file", "/z"),
    ];
    let limits = crate::model::VolumeLimits::default();
    for (path, expected_mount) in routed_cases {
        let path = PortablePath::parse(path, limits)?;
        let binding = view.resolve(&path).ok_or("path was not routed")?;
        assert_eq!(binding.path().as_str(), expected_mount);
        assert_eq!(binding.volume_id(), expected_volume);
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
