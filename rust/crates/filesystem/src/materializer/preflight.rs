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
    let directory = host_path(Path::new(path), &mut &*work, |path, work| {
        root.open_dir_held(path, work)
    })?;
    let identity = crate::native_host::directory_identity(&directory, &mut &*work)?;
    let mut storage = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let mut names = Vec::new();
    let (mut reader, cursor) = crate::native_host::read_dir_held(&directory, &mut &*work)?;
    while let Some((entry, allocation)) = reader.next(&mut &*work)? {
        push_buffer(&mut names, (entry.name, allocation), &mut storage)?;
    }
    drop(reader);
    drop(cursor);
    let (absolute, _allocation) = work.display(&format_args!("/{path}"))?;
    let mut after = None;
    let mut after_allocation = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    loop {
        let (mut page, allocation) = generation
            .list_directory(&absolute, after.as_ref(), 1024, work, work.cancellation())
            .await?;
        let mut allocation = allocation.ok_or(NativeTreeMaterializationError::MissingCustody)?;
        for entry in &page.entries {
            let (logical, _logical_allocation) =
                match entry.name.unicode_text(work, work.cancellation()) {
                    Ok(projected) => projected,
                    Err(crate::FsError::Path(crate::kernel::PathLookupError::NamespacePath(
                        crate::kernel::NamespacePathError::NonUnicodeProjection,
                    ))) => continue,
                    Err(error) => return Err(crate::WorkspaceError::from(error).into()),
                };
            let mut found = None;
            for (index, (name, _)) in names.iter().enumerate() {
                #[cfg(unix)]
                let matched = {
                    use std::os::unix::ffi::OsStrExt as _;
                    native_compare(name.as_bytes().iter().copied(), logical.bytes(), work)?
                };
                #[cfg(windows)]
                let matched = native_compare(name.iter().copied(), logical.encode_utf16(), work)?;
                if matched == std::cmp::Ordering::Equal {
                    found = Some(index);
                    break;
                }
            }
            if let Some(index) = found {
                names.swap_remove(index);
            }
        }
        let next = if page.has_more {
            page.entries.pop().map(|entry| entry.name)
        } else {
            None
        };
        let bytes = next.as_ref().map_or(0, |name| name.retained_bytes() as u64);
        drop(page);
        allocation.resize(bytes)?;
        drop(after.take());
        drop(after_allocation);
        after = next;
        after_allocation = allocation;
        if after.is_none() {
            break;
        }
    }
    Ok(!names.is_empty()
        || crate::native_host::directory_identity(&directory, &mut &*work)? != identity)
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
        workspace.write_text("/known", "authored").await?;
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
        assert_eq!(page.entries.len(), 1);
        assert_eq!(
            page.entries[0].name.encoding(),
            crate::kernel::NameEncoding::WindowsUtf16Le
        );
        drop(page);
        drop(allocation);
        let root = HostRoot::open(directory.path(), &mut &original)?;
        let result = host_directory_holds_unknown_entries(&root, "", &generation, &original).await;
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
        assert!(!host_directory_holds_unknown_entries(&root, "", &generation, &original).await?);
        assert!(original.receipt().bytes_encoded > 0);
        std::fs::write(directory.path().join("foreign"), "must survive")?;
        assert!(host_directory_holds_unknown_entries(&root, "", &generation, &original).await?);
        assert_eq!(
            std::fs::read(directory.path().join("foreign"))?,
            b"must survive"
        );

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
        for foreign in [false, true] {
            let checkout = tempfile::tempdir()?;
            let retired = checkout.path().join("retired");
            std::fs::create_dir(&retired)?;
            std::fs::write(retired.join("known"), "physical")?;
            if foreign {
                std::fs::write(retired.join("foreign"), "must survive")?;
            }
            let operation = storage
                .path()
                .join(if foreign { "foreign" } else { "known" });
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
