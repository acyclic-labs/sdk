use super::*;

/// Materializes and publishes one authenticated workspace generation to a
/// physical root through the core journal. Recovery resumes an existing
/// operation instead of rebuilding or accepting a partial checkout.
pub struct NativeWorkspacePublication<'a> {
    /// Physical checkout root.
    pub root: &'a Path,
    /// Service-owned staging and recovery directory.
    pub operation_directory: &'a Path,
    /// Stable retry identity.
    pub operation_id: OperationId,
    /// Expected physical/logical baseline.
    pub from: GenerationId,
    /// Exact target generation.
    pub to: GenerationId,
    /// Root-level names never touched by publication.
    pub excluded_names: &'a [&'a str],
    /// Native staging policy.
    pub options: &'a crate::MaterializeOptions,
    /// Cumulative planning and staging budget.
    pub budget: crate::WorkBudget,
    /// Shared cancellation boundary.
    pub cancellation: &'a crate::CancellationToken,
}

/// Publishes a transition between two authenticated generations, including
/// generations owned by sibling workspaces in the same distributed filesystem.
///
/// This is the branch-switch primitive: workspace identity may change while
/// physical checkout work remains proportional only to changed paths.
#[allow(
    clippy::too_many_lines,
    reason = "one transition plans, journals, and applies every host change between two generations"
)]
pub async fn publish_native_generation_transition<A, O>(
    from_generation: &crate::Generation<A, O>,
    to_generation: &crate::Generation<A, O>,
    state: &crate::LocalCoreStateStore,
    publication: NativeWorkspacePublication<'_>,
) -> Result<MaterializationJournal, NativeWorkspacePublicationError>
where
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
{
    let NativeWorkspacePublication {
        root,
        operation_directory,
        operation_id,
        from,
        to,
        excluded_names,
        options,
        budget,
        cancellation,
    } = publication;
    if from_generation.id() != from || to_generation.id() != to {
        return Err(NativeWorkspacePublicationError::MismatchedJournal);
    }
    let root = root.to_path_buf();
    let operation_directory = operation_directory.to_path_buf();
    acyclic_native_runtime::run_blocking_io({
        let root = root.clone();
        let operation_directory = operation_directory.clone();
        move || validate_native_operation_location(&root, &operation_directory)
    })
    .await??;
    if let Some(existing) = MaterializationJournalStore::load(state, operation_id).await? {
        if existing.plan.from != from || existing.plan.to != to {
            return Err(NativeWorkspacePublicationError::MismatchedJournal);
        }
        let backend = acyclic_native_runtime::run_blocking_io(move || {
            NativeTreeMaterializationBackend::new(root, operation_directory)
        })
        .await??;
        return JournaledMaterializer::new(state.clone(), backend)
            .recover(operation_id, MaterializationRecovery::Complete)
            .await
            .map_err(NativeWorkspacePublicationError::from)?
            .ok_or(NativeWorkspacePublicationError::MismatchedJournal);
    }

    let changes = from_generation
        .diff_to_bounded(to_generation, u32::MAX, budget, cancellation)
        .await?;
    let changed = changes
        .changed_paths_bounded(u32::MAX, budget, cancellation)
        .await?;
    let remaining_budget = changed
        .work
        .remaining(budget)
        .map_err(NativeWorkspacePublicationError::Work)?;
    let mut paths = Vec::with_capacity(changed.value.len());
    let mut structural_directories = Vec::new();
    let mut metadata_edits = Vec::new();
    let excluded_names = excluded_names
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    for change in changed.value {
        let mut path = String::new();
        for component in change.path.components() {
            if !path.is_empty() {
                path.push('/');
            }
            path.push_str(&component.unicode_text().ok_or_else(|| {
                NativeTreeMaterializationError::InvalidPath("<non-Unicode>".to_owned())
            })?);
        }
        let top = path.split('/').next().unwrap_or_default();
        if excluded_names.contains(top) {
            continue;
        }
        let before_directory = change
            .before
            .is_some_and(|record| record.kind == crate::kernel::FileKind::Directory);
        let after_directory = change
            .after
            .is_some_and(|record| record.kind == crate::kernel::FileKind::Directory);
        // A directory with no earlier record that already exists on the host
        // is a lazily promoted source directory. Replacing it wholesale would
        // give it and every file under it new host identities; it is kept,
        // takes the generation's metadata, and each descendant is reconciled
        // on its own.
        let promoted_directory = change.before.is_none()
            && after_directory
            && std::fs::symlink_metadata(root.join(&path))
                .is_ok_and(|metadata| metadata.file_type().is_dir());
        // Symmetrically, a directory the target drops that still holds host
        // entries the source generation never held is a source directory
        // read through the source: removing it would take those along, so
        // it stays and only what the generation held beneath it goes.
        if before_directory
            && change.after.is_none()
            && host_directory_holds_unknown_entries(&root, &path, from_generation).await?
        {
            continue;
        }
        if (before_directory || promoted_directory) && after_directory {
            let stat = to_generation.stat(&format!("/{path}")).await?;
            metadata_edits.push(MaterializationEdit::SetMetadata {
                path,
                image: encode_native_metadata(&stat.metadata)?,
            });
            continue;
        }
        if before_directory || after_directory {
            structural_directories.push(path.clone());
        }
        let fresh_regular = change.before.is_none()
            && change
                .after
                .is_some_and(|record| record.kind == crate::kernel::FileKind::Regular);
        paths.push((path, change.after.is_some(), fresh_regular));
    }
    paths.retain(|(path, _, _)| {
        !structural_directories.iter().any(|directory| {
            path != directory
                && path
                    .strip_prefix(directory)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
    });
    // A regular file with no earlier record that already sits on the host
    // exactly as the generation holds it is a lazily promoted source file, not
    // new content. Rewriting it would give it a new host identity, and every
    // fork's promotion of that file is keyed by the identity it was read from.
    let mut retained = Vec::with_capacity(paths.len());
    for (path, install, fresh_regular) in paths {
        if install
            && fresh_regular
            && host_holds_generation_file(&root, &path, to_generation).await?
        {
            continue;
        }
        retained.push((path, install));
    }
    let mut paths = retained;
    paths.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let target = acyclic_native_runtime::run_blocking_io({
        let operation_directory = operation_directory.clone();
        move || {
            std::fs::create_dir_all(&operation_directory)?;
            let target = operation_directory.join("target");
            if target.exists() {
                std::fs::remove_dir_all(&target)?;
            }
            std::fs::create_dir_all(&target)?;
            Ok::<_, NativeTreeMaterializationError>(target)
        }
    })
    .await??;
    let mut path_options = options.clone();
    path_options.destination.clone_from(&target);
    let install_paths = paths
        .iter()
        .filter(|(_, install)| *install)
        .map(|(path, _)| format!("/{path}"))
        .collect::<Vec<_>>();
    if !install_paths.is_empty() {
        to_generation
            .materialize_paths(
                &install_paths,
                &path_options,
                remaining_budget,
                cancellation,
            )
            .await?;
    }
    let (backend, mut plan) = acyclic_native_runtime::run_blocking_io(move || {
        let backend = NativeTreeMaterializationBackend::new(root, operation_directory)?;
        let plan = backend.plan_paths(
            operation_id,
            from,
            to,
            paths.into_iter().map(|(path, _)| path),
        )?;
        Ok::<_, NativeTreeMaterializationError>((backend, plan))
    })
    .await??;
    plan.edits.extend(metadata_edits);
    JournaledMaterializer::new(state.clone(), backend)
        .apply(plan)
        .await
        .map_err(Into::into)
}

/// Largest host file compared against a generation before publication skips
/// rewriting it; larger files are always rewritten.
const MAXIMUM_PROMOTION_COMPARISON_BYTES: u64 = 64 * 1024 * 1024;

/// Whether the host directory at `path` holds an entry `generation` does
/// not have there.
pub(super) async fn host_directory_holds_unknown_entries<A, O>(
    root: &Path,
    path: &str,
    generation: &crate::Generation<A, O>,
) -> Result<bool, NativeWorkspacePublicationError>
where
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
{
    let Ok(entries) = std::fs::read_dir(root.join(path)) else {
        return Ok(false);
    };
    let mut host = std::collections::BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(NativeTreeMaterializationError::from)?;
        host.insert(entry.file_name().to_string_lossy().into_owned());
    }
    let directory = format!("/{path}");
    let mut after = None;
    loop {
        let page = generation
            .list_directory(&directory, after.as_ref(), 1_024)
            .await?;
        for entry in &page.entries {
            if let Some(name) = entry.name.unicode_text() {
                host.remove(name.as_ref());
            }
        }
        after = page.entries.last().map(|entry| entry.name.clone());
        if !page.has_more || after.is_none() {
            break;
        }
    }
    Ok(!host.is_empty())
}

/// Whether the host already holds `path` as a regular file with exactly the
/// generation's bytes (and, on Unix, its permission bits).
pub(super) async fn host_holds_generation_file<A, O>(
    root: &Path,
    path: &str,
    generation: &crate::Generation<A, O>,
) -> Result<bool, NativeWorkspacePublicationError>
where
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
{
    let host_path = root.join(path);
    let Ok(metadata) = std::fs::symlink_metadata(&host_path) else {
        return Ok(false);
    };
    if !metadata.is_file() || metadata.len() > MAXIMUM_PROMOTION_COMPARISON_BYTES {
        return Ok(false);
    }
    let stat = generation.stat(&format!("/{path}")).await?;
    if stat.kind != crate::kernel::FileKind::Regular || stat.logical_bytes != Some(metadata.len()) {
        return Ok(false);
    }
    // Every authored field must match too, or publication would drop an
    // edit of metadata alone. What cannot be compared cheaply here (named
    // attributes, ACLs, flags) is published.
    let authored = &stat.metadata;
    if authored.has_named_attributes
        || authored.has_acl
        || authored.has_security_descriptor
        || authored.posix_flags.is_some_and(|flags| flags != 0)
    {
        return Ok(false);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        if authored
            .posix_mode
            .is_some_and(|mode| mode & 0o7777 != metadata.permissions().mode() & 0o7777)
            || authored.posix_uid.is_some_and(|uid| uid != metadata.uid())
            || authored.posix_gid.is_some_and(|gid| gid != metadata.gid())
        {
            return Ok(false);
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        // The attributes publication sets: read-only, hidden, system,
        // archive, temporary and not-content-indexed.
        const AUTHORED_ATTRIBUTES: u32 = 0x1 | 0x2 | 0x4 | 0x20 | 0x100 | 0x2000;
        if authored.windows_attributes.is_some_and(|attributes| {
            attributes & AUTHORED_ATTRIBUTES != metadata.file_attributes() & AUTHORED_ATTRIBUTES
        }) {
            return Ok(false);
        }
    }
    let published = generation.read(&format!("/{path}"), metadata.len()).await?;
    let host = std::fs::read(&host_path)?;
    Ok(published.as_ref() == host.as_slice())
}

/// Core native workspace publication failure.
#[derive(Debug, Error)]
pub enum NativeWorkspacePublicationError {
    /// Authenticated workspace access or export failed.
    #[error(transparent)]
    Workspace(#[from] crate::WorkspaceError),
    /// Host tree planning or publication failed.
    #[error(transparent)]
    Native(#[from] NativeTreeMaterializationError),
    /// Durable state access failed.
    #[error(transparent)]
    State(#[from] crate::LocalCoreStateStoreError),
    /// Journaled publication failed.
    #[error(transparent)]
    Materialization(
        #[from]
        MaterializationError<crate::LocalCoreStateStoreError, NativeTreeMaterializationError>,
    ),
    /// An operation identity was reused for different generations.
    #[error("native workspace publication journal does not match the requested generations")]
    MismatchedJournal,
    /// Native staging failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Publication planning or materialization exceeded its cumulative budget.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
}
