use super::*;
use crate::native_mount::fuse::FuseProjection;

type TestResult = Result<(), Box<dyn std::error::Error>>;

pub(super) fn folded_projection(
    directories: &[MountPath],
) -> Result<(FuseProjection, Arc<MemorySource>), Box<dyn std::error::Error>> {
    let mut config = crate::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
    config.unicode = crate::model::UnicodePolicy::Preserve;
    config.case_sensitivity = crate::model::CaseSensitivity::ProfileFolded;
    let mut checkout = memory_checkout(
        config,
        crate::model::CheckoutMode {
            access: crate::model::AccessMode::ReadWrite,
            consistency: crate::model::ConsistencyMode::Pinned,
            mutations: crate::model::MutationMode::PrivateOverlay,
        },
        1,
    )?
    .pop()
    .ok_or("one checkout")?;
    if !directories.is_empty() {
        let mutations = directories
            .iter()
            .map(|path| {
                let names = path
                    .components()
                    .iter()
                    .map(|name| {
                        crate::kernel::LogicalName::new(
                            crate::kernel::NameEncoding::Utf8,
                            name.clone(),
                            config.limits.maximum_component_bytes,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<_, Box<dyn std::error::Error>>(
                    crate::facade::AuthoredMutation::CreateDirectory {
                        path: crate::kernel::NamespacePath::new(names, config.limits)?,
                        metadata: FileMetadata::default(),
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        runtime.block_on(checkout.apply_authored_transaction(
            mutations,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        ))?;
    }
    let source = Arc::new(
        crate::native_mount::adapter::tests::access::bind_test_access(
            MemorySource::new(
                Arc::new(crate::native_mount::SharedCheckout::new(checkout)),
                config,
            )?,
            vec![],
            None,
        )?,
    );
    let core = projection_core(
        source.clone(),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?;
    Ok((FuseProjection { core }, source))
}

pub(super) fn lookup(
    projection: &FuseProjection,
    parent: u64,
    value: &str,
    work: &crate::NativeMaterializationWork,
) -> Result<u64, NativeMountError> {
    Ok(projection
        .lookup_entry(parent, std::ffi::OsStr::new(value), work)?
        .attr
        .ino
        .0)
}

#[test]
fn folded_legacy_collisions_cannot_reuse_a_warm_inode() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let (checkout, [first, _]) =
        runtime.block_on(crate::facade::test_fixtures::legacy_folded_checkout())?;
    let config = checkout.volume_config();
    let source = Arc::new(
        crate::native_mount::adapter::tests::access::bind_test_access(
            MemorySource::new(
                Arc::new(crate::native_mount::SharedCheckout::new(checkout)),
                config,
            )?,
            vec![],
            None,
        )?,
    );
    let work = fuse_fixture_work();
    let core = projection_core(
        source.clone(),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?;
    let projection = FuseProjection { core };
    let stamp = source.view_stamp_with_meter(&work)?;
    // Model an already warm exact binding; no alias may inherit its identity.
    let inode = projection
        .core
        .state()
        .map_err(NativeMountError::from)?
        .intern_projected(
            crate::native_mount::fuse::FuseOwned::empty(name("strasse")),
            &regular(first, 0),
            stamp,
            true,
            &work,
        )?;
    for spelling in ["Straße", "STRASSE", "strasse"] {
        assert!(
            matches!(
                projection.lookup_entry(ROOT_INODE, std::ffi::OsStr::new(spelling), &work),
                Err(NativeMountError::Source(MountSourceError::Filesystem(
                    crate::FsError::NameCollision
                )))
            ),
            "{spelling}"
        );
    }
    assert_eq!(
        projection
            .core
            .state()
            .map_err(NativeMountError::from)?
            .by_inode
            .get(&inode)
            .ok_or("warm inode missing")?
            .lookup
            .node
            .file_id,
        first
    );
    let page = source.read_directory_with_meter(&MountPath::root(), None, 256, &work)?;
    assert!(!page.entries.is_empty());
    assert!(
        page.entries
            .iter()
            .all(|entry| entry.name.starts_with(b"Sa"))
    );
    Ok(())
}

#[test]
fn folded_nested_directory_stream_uses_parent_identity() -> TestResult {
    let (projection, source) = folded_projection(&[])?;
    let work = fuse_fixture_work();
    source.create_directory(&name("Dir"), FileMetadata::default(), &work)?;
    source.create_directory(
        &name("Dir").child(b"Child".to_vec()),
        FileMetadata::default(),
        &work,
    )?;
    let parent = lookup(&projection, ROOT_INODE, "Dir", &work)?;
    let child = lookup(&projection, parent, "Child", &work)?;
    let stream = projection
        .core
        .state()
        .map_err(NativeMountError::from)?
        .directory_stream(source.as_ref(), child, 0, &work)?;
    assert_eq!(stream.parent_inode, parent);
    assert_ne!(stream.parent_inode, ROOT_INODE);
    assert_eq!(*stream.path, name("Dir").child(b"Child".to_vec()));
    Ok(())
}

#[test]
fn folded_alias_rename_rebases_parked_native_streams() -> TestResult {
    let (projection, source) = folded_projection(&[])?;
    let work = fuse_fixture_work();
    source.create_directory(&name("Dir"), FileMetadata::default(), &work)?;
    source.create_directory(
        &name("Dir").child(b"Child".to_vec()),
        FileMetadata::default(),
        &work,
    )?;
    source.create_directory(&name("Other"), FileMetadata::default(), &work)?;
    let destination_parent = lookup(&projection, ROOT_INODE, "Other", &work)?;
    let parent = lookup(&projection, ROOT_INODE, "Dir", &work)?;
    let child = lookup(&projection, parent, "Child", &work)?;
    {
        let mut state = projection.core.state().map_err(NativeMountError::from)?;
        for inode in [parent, child] {
            let stream = state.directory_stream(source.as_ref(), inode, 0, &work)?;
            let parking = state.streams.prepare_park(&work)?;
            state.streams.park(inode, stream, parking);
        }
    }
    projection.rename_name(
        ROOT_INODE,
        std::ffi::OsStr::new("dir"),
        destination_parent,
        std::ffi::OsStr::new("Moved"),
        0,
        &work,
    )?;
    let mut state = projection.core.state().map_err(NativeMountError::from)?;
    for (inode, expected) in [
        (parent, name("Other").child(b"Moved".to_vec())),
        (
            child,
            name("Other")
                .child(b"Moved".to_vec())
                .child(b"Child".to_vec()),
        ),
    ] {
        let stream = state.directory_stream(source.as_ref(), inode, 0, &work)?;
        assert_eq!(*stream.path, expected);
        assert_eq!(
            stream.parent_inode,
            if inode == parent {
                destination_parent
            } else {
                parent
            }
        );
        source.read_directory_with_meter(&stream.path, None, 16, &work)?;
    }
    Ok(())
}

#[test]
fn folded_expanding_directory_facts_keep_native_spelling() -> TestResult {
    let (projection, source) = folded_projection(&[])?;
    let work = fuse_fixture_work();
    let long = "ΐ".repeat(100);
    source.create_directory(&name(&long), FileMetadata::default(), &work)?;
    let inode = lookup(&projection, ROOT_INODE, &long, &work)?;
    for _ in 0..2 {
        let (attributes, _) = projection.node_attributes(inode, None, &work)?;
        assert_eq!(attributes.ino.0, inode);
        assert_eq!(attributes.kind, fuser::FileType::Directory);
    }
    let stream = projection
        .core
        .state()
        .map_err(NativeMountError::from)?
        .directory_stream(source.as_ref(), inode, 0, &work)?;
    assert_eq!(*stream.path, name(&long));
    source.read_directory_with_meter(&stream.path, None, 16, &work)?;
    Ok(())
}

#[test]
fn folded_directory_validation_is_shared_between_pages() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let checkout = runtime.block_on(crate::facade::test_fixtures::folded_checkout(
        (0..1024).map(|index| format!("Entry{index:04}")),
    ))?;
    let config = checkout.volume_config();
    let source = crate::native_mount::adapter::tests::access::bind_test_access(
        MemorySource::new(
            Arc::new(crate::native_mount::SharedCheckout::new(checkout)),
            config,
        )?,
        vec![],
        None,
    )?;
    let work = fuse_fixture_work();
    let first = source.read_directory_with_meter(&MountPath::root(), None, 256, &work)?;
    assert_eq!(first.entries.len(), 256);
    assert!(work.receipt().bytes_encoded >= 1024 * 9);
    // Current-policy authentication is mandatory for every page. Fund exactly
    // that shared probe; cached namespace validation must add no encoding work.
    let probe = fuse_fixture_work();
    source.access_snapshot_with_meter(&probe)?;
    let policy_bytes = probe.receipt().bytes_encoded;
    let budget = crate::WorkBudget {
        bytes_encoded: policy_bytes,
        ..crate::WorkBudget::UNBOUNDED
    };
    let cached = cache_work(budget, &crate::CancellationToken::new());
    let next = source.read_directory_with_meter(
        &MountPath::root(),
        first.next_cursor.as_deref(),
        256,
        &cached,
    )?;
    assert_eq!(next.entries.len(), 256);
    assert!(first.entries.last().ok_or("first page missing")?.name < next.entries[0].name);
    assert_eq!(cached.receipt().bytes_encoded, policy_bytes);
    // An edit changes the tree identity: the old validation must not transfer.
    source.create_directory(&name("New"), FileMetadata::default(), &work)?;
    let stale = cache_work(budget, &crate::CancellationToken::new());
    assert!(
        source
            .read_directory_with_meter(&MountPath::root(), None, 256, &stale)
            .is_err(),
        "the same bound must reject an actual namespace-validation scan"
    );
    let before = work.receipt().bytes_encoded;
    source.read_directory_with_meter(&MountPath::root(), None, 256, &work)?;
    assert!(work.receipt().bytes_encoded >= before + 1024 * 9);
    Ok(())
}

#[test]
fn folded_collisions_leave_unrelated_files_and_descendants_accessible() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let mut checkout = runtime.block_on(crate::facade::test_fixtures::folded_checkout(
        ["Stra\u{df}e", "STRASSE", "notes"].map(str::to_owned),
    ))?;
    let config = checkout.volume_config();
    let path = |text: &str| -> Result<_, Box<dyn std::error::Error>> {
        Ok(crate::kernel::NamespacePath::from_portable(
            &crate::path::PortablePath::parse(text, config.limits)?,
            config.limits,
        )?)
    };
    runtime.block_on(checkout.apply_authored_transaction(
        vec![
            crate::AuthoredMutation::CreateDirectory {
                path: path("/Nested")?,
                metadata: FileMetadata::default(),
            },
            crate::AuthoredMutation::CreateFile {
                path: path("/Nested/inside")?,
                bytes: acyclic_stream::OwnedBytes::new(),
                metadata: FileMetadata::default(),
            },
        ],
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ))?;
    let source = crate::native_mount::adapter::tests::access::bind_test_access(
        MemorySource::new(
            Arc::new(crate::native_mount::SharedCheckout::new(checkout)),
            config,
        )?,
        vec![],
        None,
    )?;
    let work = fuse_fixture_work();
    for _ in 0..2 {
        for spelling in ["notes", "NOTES", "Nested/inside", "NESTED/INSIDE"] {
            let path = spelling.split('/').fold(MountPath::root(), |path, name| {
                path.child(name.as_bytes().to_vec())
            });
            assert!(
                source.lookup_with_meter(&path, &work)?.is_some(),
                "{spelling}"
            );
        }
        for spelling in ["Stra\u{df}e", "STRASSE", "strasse"] {
            assert!(matches!(
                source.lookup_with_meter(&name(spelling), &work),
                Err(MountSourceError::Filesystem(crate::FsError::NameCollision))
            ));
        }
    }
    let mut cursor = None;
    let mut listed = Vec::new();
    loop {
        let page =
            source.read_directory_with_meter(&MountPath::root(), cursor.as_deref(), 1, &work)?;
        listed.extend(page.entries.iter().map(|entry| entry.name.clone()));
        if page.entries.is_empty() {
            assert!(
                page.next_cursor().is_some(),
                "ambiguous page lost its source frontier"
            );
        }
        cursor = page.next_cursor().map(<[u8]>::to_vec);
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(listed, [b"Nested".to_vec(), b"notes".to_vec()]);
    assert!(
        source
            .read_directory_with_meter(&name("Nested"), None, 1, &work)?
            .entries
            .iter()
            .any(|entry| entry.name == b"inside")
    );
    Ok(())
}
