use super::*;
use crate::native_host::HostRoot;

// Main's promotion checks: preserve identical files and directories with unknown children.
pub(super) async fn host_holds_generation_file<
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
>(
    root: &HostRoot,
    path: &str,
    generation: &crate::Generation<A, O>,
    work: &NativeMaterializationWork,
) -> Result<bool, NativeWorkspacePublicationError> {
    let relative = Path::new(path);
    let Some(observed) = host_stat(root, relative, work)? else {
        return Ok(false);
    };
    if observed.file_type() != crate::demand::SourceNodeKind::RegularFile {
        return Ok(false);
    }
    let (absolute, _allocation) = work.display(&format_args!("/{path}"))?;
    let source = generation
        .stat(&absolute, work, work.cancellation())
        .await?;
    let authored = &source.metadata;
    if source.kind != crate::kernel::FileKind::Regular
        || source.logical_bytes != Some(observed.len())
        || authored.has_named_attributes
        || authored.has_acl
        || authored.has_security_descriptor
        || authored.posix_flags.is_some_and(|flags| flags != 0)
    {
        return Ok(false);
    }
    #[cfg(unix)]
    if authored
        .posix_mode
        .is_some_and(|mode| mode & 0o7777 != (observed.raw.st_mode as u32) & 0o7777)
        || authored
            .posix_uid
            .is_some_and(|uid| uid != observed.raw.st_uid as u32)
        || authored
            .posix_gid
            .is_some_and(|gid| gid != observed.raw.st_gid as u32)
    {
        return Ok(false);
    }
    #[cfg(windows)]
    if authored
        .windows_attributes
        .is_some_and(|attributes| attributes & 0x2127 != observed.raw.FileAttributes & 0x2127)
    {
        return Ok(false);
    }
    let parent = host_path(
        relative.parent().unwrap_or_else(|| Path::new("")),
        &mut &*work,
        |path, work| root.open_dir_held(path, work),
    )?;
    let mut file = crate::native_host::open_leaf(
        &parent,
        Path::new(
            relative
                .file_name()
                .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
        ),
        &crate::native_host::NativeOpenOptions::Read,
        &mut &*work,
    )?;
    if crate::native_host::stat_file(&file, &mut &*work)?.identity() != observed.identity() {
        return Ok(false);
    }
    let mut buffer = [0; 64 * 1024];
    let mut offset = 0;
    use std::io::Read as _;
    while offset < observed.len() {
        let length = (observed.len() - offset)
            .min(buffer.len() as u64)
            .min(work.remaining()?.source_bytes_read / 2);
        if length == 0 {
            work.charge(NativeResourceCredit::ReadBytes.delta(2))?;
        }
        let expected = generation
            .read_range(&absolute, offset, length, work, work.cancellation())
            .await?;
        if expected.bytes.len() as u64 != length {
            return Ok(false);
        }
        let mut count = 0;
        while count < length as usize {
            work.charge(NativeResourceCredit::ReadOperations.delta(1))?;
            let read = file.read(&mut buffer[count..length as usize])?;
            work.spent(NativeResourceCredit::ReadBytes.delta(read as u64))?;
            if read == 0 {
                return Ok(false);
            }
            count += read;
        }
        if native_compare(
            expected.bytes.iter().copied(),
            buffer[..count].iter().copied(),
            work,
        )? != std::cmp::Ordering::Equal
        {
            return Ok(false);
        }
        offset += length;
    }
    let finished = crate::native_host::stat_file(&file, &mut &*work)?;
    Ok(finished.identity() == observed.identity()
        && same_file_version(&observed, &finished)?
        && host_stat(root, relative, work)?
            .is_some_and(|selected| selected.identity() == observed.identity()))
}

pub(super) async fn host_directory_holds_unknown_entries<
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
>(
    root: &HostRoot,
    path: &str,
    generation: &crate::Generation<A, O>,
    work: &NativeMaterializationWork,
) -> Result<bool, NativeWorkspacePublicationError> {
    use crate::facade::FsWork as _;

    let directory = host_path(Path::new(path), &mut &*work, |path, work| {
        root.open_dir_held(path, work)
    })?;
    let identity = crate::native_host::directory_identity(&directory, &mut &*work)?;
    let (absolute, _allocation) = work.display(&format_args!("/{path}"))?;
    let cancellation = work.cancellation();
    let mut accounting = work.accounting(cancellation);
    let mut scope = accounting
        .allocation_scope()
        .map_err(crate::WorkspaceError::from)?;
    let workspace = &generation.workspace;
    let result: Result<bool, crate::WorkspaceError> = async {
        let (mut checkout, selected) = workspace
            .open_read_path(
                crate::model::GenerationSelector::Exact(generation.id()),
                &absolute,
                &mut scope,
                cancellation,
            )
            .await?;
        let record = {
            let (lookup, _allocation) = checkout
                .value
                .lookup_no_follow_with_accounting(&selected.value, &mut scope, cancellation, true)
                .await?;
            lookup.record.ok_or(crate::WorkspaceError::NotFound)?
        };
        if record.kind != crate::kernel::FileKind::Directory {
            return Err(crate::WorkspaceError::NotDirectory);
        }
        let config = checkout.value.volume_config();
        let (mut reader, _cursor) = crate::native_host::read_dir_held(&directory, &mut &*work)?;
        let unknown = loop {
            let Some(physical) = reader.next_name(&mut &*work)? else {
                break false;
            };
            if !host_name_has_binding(&record, checkout.value.objects(), config, &physical.0, work)
                .await?
            {
                break true;
            }
        };
        selected.retire(&mut scope)?;
        checkout.retire(&mut scope)?;
        Ok(unknown || crate::native_host::directory_identity(&directory, &mut &*work)? != identity)
    }
    .await;
    workspace
        .check_current_access(&mut scope, cancellation)
        .await?;
    result.map_err(Into::into)
}

async fn host_name_has_binding<O: crate::AsyncObjectStore>(
    record: &crate::kernel::FileRecord,
    objects: &O,
    config: crate::VolumeConfig,
    physical: &std::ffi::OsStr,
    work: &NativeMaterializationWork,
) -> Result<bool, crate::WorkspaceError> {
    use crate::facade::FsWork as _;

    let cancellation = work.cancellation();
    let mut accounting = work.accounting(cancellation);
    // Native profiles also admit explicit UTF-8 keys. Both encodings project
    // to the same host spelling; neither lookup may fold case or list siblings.
    let utf8 = (!matches!(
        config.profile,
        crate::FilesystemProfile::Portable | crate::FilesystemProfile::Browser
    ))
    .then_some(crate::FilesystemProfile::Portable);
    for profile in std::iter::once(config.profile).chain(utf8) {
        let encoded = match crate::native_name::host_name_bytes_with_native_work(
            physical,
            profile,
            config.limits.maximum_component_bytes,
            work,
        ) {
            Ok(encoded) => encoded,
            Err(NativeTreeMaterializationError::InvalidLayout) => continue,
            Err(error) => return Err(error.into()),
        };
        let Ok(name) = crate::kernel::LogicalName::new(
            encoded.0.0,
            encoded.0.1,
            config.limits.maximum_component_bytes,
        ) else {
            continue;
        };
        let name = (name, encoded.1);
        accounting.charge_items(1)?;
        if name.0.unicode_utf8_bytes().is_none() {
            return Ok(false);
        }
        if record
            .binding_with_accounting(
                objects,
                &name.0,
                crate::kernel::DecodeLimits::for_volume(config),
                &mut accounting,
                cancellation,
            )
            .await?
            .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn directory_preflight_keeps_name_projection_on_original_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = crate::CancellationToken::new();
        let budget = crate::WorkBudget::UNBOUNDED;
        let storage = tempfile::tempdir()?;
        let fs = crate::Fs::local(crate::LocalOptions::new(storage.path().join("fs"))).await?;
        let config = crate::VolumeConfig {
            profile: crate::FilesystemProfile::Windows,
            ..crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral)
        };
        let workspace = fs
            .create_workspace_with_config("preflight", config, budget, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value;
        let mut setup = workspace
            .begin_transaction(crate::IdempotencyKey::new(), budget, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value;
        setup.write_text("/known", "authored").await?;
        setup
            .write_text("/zzlater", "unrelated authored child")
            .await?;
        for index in 0..64 {
            setup
                .write_text(
                    &format!("/unrelated-{index:02}"),
                    "unrelated authored child",
                )
                .await?;
        }
        setup
            .checkout
            .apply_authored_transaction(
                vec![crate::AuthoredMutation::CreateFile {
                    path: crate::kernel::NamespacePath::new(
                        vec![crate::kernel::LogicalName::new(
                            crate::kernel::NameEncoding::Utf8,
                            b"explicit-utf8".to_vec(),
                            config.limits.maximum_component_bytes,
                        )?],
                        config.limits,
                    )?,
                    bytes: acyclic_stream::OwnedBytes::copy_from_slice(b"authored"),
                    metadata: crate::kernel::FileMetadata::default(),
                }],
                budget,
                &cancellation,
            )
            .await
            .map_err(|failure| failure.error)?;
        setup
            .commit(
                None,
                crate::PublicationPermit::Unrestricted,
                budget,
                &cancellation,
                |_| async {
                    Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(
                        crate::OperationReceipt {
                            value: (),
                            work: crate::WorkCounters::UNCHARGED,
                        },
                    )
                },
            )
            .await
            .map_err(|failure| failure.error)?;
        let generation = workspace
            .head(budget, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value;
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("known"), "physical")?;

        // Reading the root requires no path encoding. Its UTF-16 child must
        // spend the original invocation's encoding credit when projected.
        let original = NativeMaterializationWork::new(
            crate::WorkBudget {
                bytes_encoded: 0,
                ..budget
            },
            &cancellation,
        )
        .map_err(|(error, _)| error)?;
        let (page, allocation) = generation
            .list_directory("/", None, 1024, &original, &cancellation)
            .await?;
        assert_eq!(page.entries.len(), 67);
        assert!(
            page.entries
                .iter()
                .any(|entry| entry.name.encoding() == crate::kernel::NameEncoding::WindowsUtf16Le)
        );
        drop(page);
        drop(allocation);
        let root = HostRoot::open(directory.path(), &mut &original)?;
        let retained = original.live_allocation_bytes();
        let result = host_directory_holds_unknown_entries(&root, "", &generation, &original).await;
        assert_eq!(original.live_allocation_bytes(), retained);
        assert!(
            matches!(
                result,
                Err(NativeWorkspacePublicationError::Workspace(
                    crate::WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                        counter: "bytes_encoded",
                        maximum: 0,
                        ..
                    })
                ))
            ),
            "{result:?}"
        );
        assert_eq!(original.receipt().bytes_encoded, 0);
        assert_eq!(std::fs::read(directory.path().join("known"))?, b"physical");

        let original =
            NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
        let root = HostRoot::open(directory.path(), &mut &original)?;
        let retained = original.live_allocation_bytes();
        assert!(!host_directory_holds_unknown_entries(&root, "", &generation, &original).await?);
        assert_eq!(original.live_allocation_bytes(), retained);
        assert_eq!(
            original.receipt().items_returned,
            0,
            "membership must not enumerate unrelated authored children"
        );
        assert_eq!(original.receipt().bytes_encoded, 2 * "known".len() as u64);
        use std::os::unix::ffi::OsStrExt as _;
        for (name, unknown) in [
            (std::ffi::OsStr::new("explicit-utf8"), false),
            (std::ffi::OsStr::new("KNOWN"), true),
            (std::ffi::OsStr::from_bytes(b"raw-\xff"), true),
        ] {
            let path = directory.path().join(name);
            std::fs::write(&path, "must survive")?;
            assert_eq!(
                host_directory_holds_unknown_entries(&root, "", &generation, &original).await?,
                unknown
            );
            assert_eq!(original.live_allocation_bytes(), retained);
            assert_eq!(std::fs::read(&path)?, b"must survive");
            std::fs::remove_file(&path)?;
        }
        std::fs::write(directory.path().join("foreign"), "must survive")?;
        assert!(host_directory_holds_unknown_entries(&root, "", &generation, &original).await?);
        assert_eq!(
            std::fs::read(directory.path().join("foreign"))?,
            b"must survive"
        );

        // An empty physical directory still authenticates its original
        // generation, but spends no credit projecting unrelated child names.
        std::fs::remove_file(directory.path().join("known"))?;
        std::fs::remove_file(directory.path().join("foreign"))?;
        let original = NativeMaterializationWork::new(
            crate::WorkBudget {
                bytes_encoded: 0,
                ..budget
            },
            &cancellation,
        )
        .map_err(|(error, _)| error)?;
        let root = HostRoot::open(directory.path(), &mut &original)?;
        assert!(!host_directory_holds_unknown_entries(&root, "", &generation, &original).await?);
        assert_eq!(original.receipt().bytes_encoded, 0);

        let stopped = crate::CancellationToken::new();
        let stopped_work =
            NativeMaterializationWork::new(budget, &stopped).map_err(|(error, _)| error)?;
        let stopped_root = HostRoot::open(directory.path(), &mut &stopped_work)?;
        let retained = stopped_work.live_allocation_bytes();
        stopped.cancel();
        assert!(matches!(
            host_directory_holds_unknown_entries(&stopped_root, "", &generation, &stopped_work)
                .await,
            Err(NativeWorkspacePublicationError::Native(
                NativeTreeMaterializationError::Cancelled
            ))
        ));
        assert_eq!(stopped_work.live_allocation_bytes(), retained);

        // Physical emptiness must not turn an authored file into a directory.
        std::fs::create_dir(directory.path().join("known"))?;
        let original =
            NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
        let root = HostRoot::open(directory.path(), &mut &original)?;
        assert!(matches!(
            host_directory_holds_unknown_entries(&root, "known", &generation, &original).await,
            Err(NativeWorkspacePublicationError::Workspace(
                crate::WorkspaceError::NotDirectory
            ))
        ));

        // Drive the same decision through the durable publication entry point.
        // A removed directory may disappear only if every physical child was
        // part of the pinned source generation.
        let mut transaction = workspace
            .begin_transaction(crate::IdempotencyKey::new(), budget, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value;
        transaction
            .create_directory("/retired", budget, &cancellation)
            .await
            .map_err(|failure| failure.error)?;
        transaction.write_text("/retired/known", "authored").await?;
        transaction
            .commit(
                None,
                crate::PublicationPermit::Unrestricted,
                budget,
                &cancellation,
                |_| async {
                    Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(
                        crate::OperationReceipt {
                            value: (),
                            work: crate::WorkCounters::UNCHARGED,
                        },
                    )
                },
            )
            .await
            .map_err(|failure| failure.error)?;
        let from = workspace
            .head(budget, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value;
        workspace.remove("/retired/known").await?;
        workspace.remove("/retired").await?;
        let to = workspace
            .head(budget, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value;
        let state = crate::LocalCoreStateStore::new(storage.path().join("state"));
        for (label, known, foreign) in [
            ("empty", false, false),
            ("known", true, false),
            ("foreign", true, true),
        ] {
            let checkout = tempfile::tempdir()?;
            let retired = checkout.path().join("retired");
            std::fs::create_dir(&retired)?;
            if known {
                std::fs::write(retired.join("known"), "physical")?;
            }
            if foreign {
                std::fs::write(retired.join("foreign"), "must survive")?;
            }
            let operation = storage.path().join(label);
            let original = NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error)?;
            let identity = original.root_identity(checkout.path())?;
            let result = publish_native_generation_transition(
                &from,
                &to,
                &state,
                NativeWorkspacePublication {
                    root: checkout.path(),
                    source_root_identity: identity,
                    operation_directory: &operation,
                    operation_id: OperationId::new(),
                    from: from.id(),
                    to: to.id(),
                    excluded_names: &[],
                    options: &crate::MaterializeOptions::native(operation.join("target")),
                    budget,
                    cancellation: &cancellation,
                },
                original,
                || std::future::ready(Ok(())),
            )
            .await
            .map_err(|(error, _)| error)?;
            assert_eq!(result.value.phase, MaterializationPhase::Applied);
            assert!(!retired.join("known").exists());
            if foreign {
                assert_eq!(std::fs::read(retired.join("foreign"))?, b"must survive");
            } else {
                assert!(!retired.exists());
            }
        }
        Ok(())
    }
}
