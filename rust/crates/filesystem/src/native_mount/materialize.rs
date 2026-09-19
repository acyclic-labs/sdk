//! Explicit native materialization of one authenticated checkout.

#[cfg(any(target_os = "macos", windows))]
use crate::ObjectId;
use crate::kernel::{
    ExtentKind, FileKind, FileMetadata, FilePayload, LogicalName, NameEncoding, NamespacePath,
};
use crate::native_host::HostRoot;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, ByteRange, CancellationToken, Checkout, FileId,
    OperationFailure, OperationReceipt, PinnedReader, ResolvedFileRangeReadRequest, WorkBudget,
    WorkCounters, WorkError,
};
use bytes::Bytes;
use std::collections::HashMap;
#[cfg(unix)]
use std::ffi::OsStr;
use std::ffi::OsString;
#[cfg(windows)]
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

static RESTORE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Exact hard bounds and destination for one materialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializeOptions {
    /// Existing empty directory owned by the caller.
    pub destination: PathBuf,
    /// Maximum directory entries fetched per authenticated page.
    pub maximum_directory_entries: u32,
    /// Maximum sparse spans fetched per authenticated range plan.
    pub maximum_extent_spans: u32,
    /// Maximum bytes read or zero-filled in one allocation.
    pub transfer_bytes: u64,
}

/// Exact result and host-side movement facts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MaterializationReceipt {
    /// Materialized regular-file or hard-link bindings.
    pub files: u64,
    /// Materialized directory bindings, excluding the supplied root.
    pub directories: u64,
    /// Materialized symbolic-link bindings.
    pub symbolic_links: u64,
    /// Materialized FIFO, socket, character-device, or block-device bindings.
    pub special_files: u64,
    /// Logical regular-file bytes represented at the destination.
    pub logical_file_bytes: u64,
    /// Bytes physically written for content and allocated-zero spans.
    pub written_bytes: u64,
    /// Complete canonical-engine and host-movement work.
    pub work: WorkCounters,
}

/// How a materialized host path replaces its current destination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostPathReplacement {
    /// Exchange an existing entry atomically on the same volume.
    Atomic,
    /// Route ordinary renames through a live kernel mount.
    LiveMount,
}

/// Exact result of restoring one authenticated path into a host tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostPathRestore {
    /// The authenticated entry was materialized and published.
    Restored,
    /// The authenticated path was absent and the host entry was removed.
    Removed,
}

/// Fail-closed explicit materialization errors.
#[derive(Debug, Error)]
pub enum MaterializeError {
    /// Destination is absent, non-directory, a symlink, or non-empty.
    #[error("materialization destination must be an existing empty real directory")]
    InvalidDestination,
    /// A canonical name cannot be represented exactly on this host.
    #[error("canonical name cannot be represented exactly on this host")]
    UnrepresentableName,
    /// The requested authenticated path is absent from the checkout.
    #[error("requested path is absent from the checkout")]
    MissingPath,
    /// Host path is empty, absolute, escapes the root, or cannot be encoded.
    #[error("requested host path is not a valid relative path")]
    InvalidPath,
    /// This host adapter cannot recreate the authenticated kind exactly.
    #[error("file kind {0:?} cannot be materialized exactly on this host")]
    UnsupportedKind(FileKind),
    /// Supplied operation bounds are zero or exceed native addressability.
    #[error("materialization options are invalid")]
    InvalidOptions,
    /// Canonical engine operation failed.
    #[error("filesystem engine failed: {0}")]
    Engine(String),
    /// Host filesystem operation failed.
    #[error("host filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
    /// Exact work overflowed or exceeded the admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

/// Explicitly materializes one checkout without changing or publishing it.
///
/// Sparse files are pre-sized and only authenticated content or allocated-zero
/// spans are written. Hole spans issue no body I/O. Hard links are recreated by
/// stable file identity, and directory enumeration remains bounded and paged.
/// This function is only invoked by explicit materialization requests.
///
/// # Errors
///
/// Fails before traversal for an invalid destination or zero bounds. During
/// traversal it fails closed on unrepresentable names, unsupported special
/// kinds, canonical engine errors, host I/O, cancellation, or work exhaustion.
pub async fn materialize_checkout<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<MaterializationReceipt>, OperationFailure<MaterializeError>> {
    let host_root = validate_options(options).map_err(OperationFailure::before_work)?;
    let limits = checkout.volume_config().limits;
    let root = NamespacePath::new(Vec::new(), limits).map_err(|error| {
        OperationFailure::before_work(MaterializeError::Engine(error.to_string()))
    })?;
    let mut pending = vec![(root.clone(), PathBuf::new())];
    let mut deferred_directory_metadata = vec![(root, PathBuf::new())];
    let mut known_files = HashMap::<FileId, PathBuf>::new();
    #[cfg(any(target_os = "macos", windows))]
    let mut known_payloads = HashMap::<(u64, ObjectId, ObjectId), PathBuf>::new();
    #[cfg(any(target_os = "macos", windows))]
    let clone_available = crate::probe_native_storage_capabilities(&options.destination)
        .is_ok_and(|capabilities| capabilities.block_cloning);
    let mut receipt = MaterializationReceipt::default();

    while let Some((directory, host_directory)) = pending.pop() {
        cancellation.check().map_err(|error| {
            OperationFailure::new(MaterializeError::Engine(error.to_string()), receipt.work)
        })?;
        let mut after: Option<LogicalName> = None;
        loop {
            let remaining = receipt.work.remaining(budget).map_err(|error| {
                OperationFailure::new(MaterializeError::Work(error), receipt.work)
            })?;
            let page = checkout
                .list_directory_records(
                    &directory,
                    after.as_ref(),
                    options.maximum_directory_entries,
                    remaining,
                    cancellation,
                )
                .await
                .map_err(|failure| map_engine_failure(failure, receipt.work))?;
            receipt.work = add_work(receipt.work, page.work)?;
            if page.value.entries.is_empty() {
                break;
            }
            for entry in page.value.entries {
                let child = append_path(&directory, entry.name.clone(), limits)
                    .map_err(|error| OperationFailure::new(error, receipt.work))?;
                let host_child = host_directory.join(
                    host_name(&entry.name)
                        .map_err(|error| OperationFailure::new(error, receipt.work))?,
                );
                materialize_entry(
                    checkout,
                    &host_root,
                    &child,
                    &host_child,
                    entry.record.file_id,
                    entry.record.kind,
                    entry.record.payload,
                    #[cfg(any(target_os = "macos", windows))]
                    entry.record.metadata,
                    options,
                    budget,
                    cancellation,
                    &mut known_files,
                    #[cfg(any(target_os = "macos", windows))]
                    &mut known_payloads,
                    #[cfg(any(target_os = "macos", windows))]
                    clone_available,
                    &mut pending,
                    &mut receipt,
                )
                .await?;
                if entry.record.kind == FileKind::Directory {
                    deferred_directory_metadata.push((child, host_child));
                }
                after = Some(entry.name);
            }
            if !page.value.has_more {
                break;
            }
        }
    }
    for (directory, host_directory) in deferred_directory_metadata.into_iter().rev() {
        apply_metadata(
            checkout,
            &host_root,
            &directory,
            &host_directory,
            budget,
            cancellation,
            &mut receipt,
        )
        .await?;
    }
    Ok(OperationReceipt {
        value: receipt,
        work: receipt.work,
    })
}

/// Materializes one authenticated path into an empty destination directory.
///
/// The destination receives the same relative logical path below its root;
/// for example, `src/main.rs` is written to
/// `options.destination/src/main.rs`.  This lets callers stage a path in a
/// sibling directory and perform their own atomic host-side exchange without
/// reimplementing authenticated lookup, sparse-file handling, hard links, or
/// platform name conversion.  Parent directories created for the selected
/// path are not included in the receipt; the selected node and its subtree
/// are. Hard links within that subtree retain their shared identity; a link
/// to a node outside the selected subtree becomes an independent copy.
#[allow(clippy::too_many_lines)]
pub async fn materialize_checkout_path<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    path: &NamespacePath,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<MaterializationReceipt>, OperationFailure<MaterializeError>> {
    let host_root = validate_options(options).map_err(OperationFailure::before_work)?;
    if path.components().is_empty() {
        return Err(OperationFailure::before_work(
            MaterializeError::InvalidOptions,
        ));
    }
    let lookup = checkout
        .lookup_no_follow(path, budget, cancellation)
        .await
        .map_err(|failure| map_engine_failure(failure, WorkCounters::default()))?;
    let record = lookup
        .value
        .record
        .ok_or_else(|| OperationFailure::new(MaterializeError::MissingPath, lookup.work))?;
    let mut host_path = PathBuf::new();
    for component in path.components() {
        host_path
            .push(host_name(component).map_err(|error| OperationFailure::new(error, lookup.work))?);
    }
    let mut parent = PathBuf::new();
    for component in host_path.parent().into_iter().flat_map(Path::components) {
        parent.push(component.as_os_str());
        host_root
            .create_dir(&parent)
            .map_err(|error| OperationFailure::new(error.into(), lookup.work))?;
    }

    let mut receipt = MaterializationReceipt {
        work: lookup.work,
        ..MaterializationReceipt::default()
    };
    let mut known_files = HashMap::<FileId, PathBuf>::new();
    #[cfg(any(target_os = "macos", windows))]
    let mut known_payloads = HashMap::<(u64, ObjectId, ObjectId), PathBuf>::new();
    #[cfg(any(target_os = "macos", windows))]
    let clone_available = crate::probe_native_storage_capabilities(&options.destination)
        .is_ok_and(|capabilities| capabilities.block_cloning);
    let mut pending = Vec::new();
    let mut deferred_directory_metadata = Vec::new();
    if record.kind == FileKind::Directory {
        deferred_directory_metadata.push((path.clone(), host_path.clone()));
    }
    materialize_entry(
        checkout,
        &host_root,
        path,
        &host_path,
        record.file_id,
        record.kind,
        record.payload,
        #[cfg(any(target_os = "macos", windows))]
        record.metadata,
        options,
        budget,
        cancellation,
        &mut known_files,
        #[cfg(any(target_os = "macos", windows))]
        &mut known_payloads,
        #[cfg(any(target_os = "macos", windows))]
        clone_available,
        &mut pending,
        &mut receipt,
    )
    .await?;

    while let Some((directory, host_directory)) = pending.pop() {
        cancellation.check().map_err(|error| {
            OperationFailure::new(MaterializeError::Engine(error.to_string()), receipt.work)
        })?;
        let mut after = None;
        loop {
            let remaining = receipt.work.remaining(budget).map_err(|error| {
                OperationFailure::new(MaterializeError::Work(error), receipt.work)
            })?;
            let page = checkout
                .list_directory_records(
                    &directory,
                    after.as_ref(),
                    options.maximum_directory_entries,
                    remaining,
                    cancellation,
                )
                .await
                .map_err(|failure| map_engine_failure(failure, receipt.work))?;
            receipt.work = add_work(receipt.work, page.work)?;
            for entry in page.value.entries {
                let child = append_path(
                    &directory,
                    entry.name.clone(),
                    checkout.volume_config().limits,
                )
                .map_err(|error| OperationFailure::new(error, receipt.work))?;
                let child_host = host_directory.join(
                    host_name(&entry.name)
                        .map_err(|error| OperationFailure::new(error, receipt.work))?,
                );
                materialize_entry(
                    checkout,
                    &host_root,
                    &child,
                    &child_host,
                    entry.record.file_id,
                    entry.record.kind,
                    entry.record.payload,
                    #[cfg(any(target_os = "macos", windows))]
                    entry.record.metadata,
                    options,
                    budget,
                    cancellation,
                    &mut known_files,
                    #[cfg(any(target_os = "macos", windows))]
                    &mut known_payloads,
                    #[cfg(any(target_os = "macos", windows))]
                    clone_available,
                    &mut pending,
                    &mut receipt,
                )
                .await?;
                if entry.record.kind == FileKind::Directory {
                    deferred_directory_metadata.push((child, child_host));
                }
                after = Some(entry.name);
            }
            if !page.value.has_more {
                break;
            }
        }
    }
    for (directory, host_directory) in deferred_directory_metadata.into_iter().rev() {
        apply_metadata(
            checkout,
            &host_root,
            &directory,
            &host_directory,
            budget,
            cancellation,
            &mut receipt,
        )
        .await?;
    }
    Ok(OperationReceipt {
        value: receipt,
        work: receipt.work,
    })
}

/// Materializes a host-relative path using the checkout's configured name
/// profile. Native consumers do not need to construct logical names or know
/// whether this platform stores them as bytes or UTF-16 units.
pub async fn materialize_checkout_host_path<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    relative: &Path,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<MaterializationReceipt>, OperationFailure<MaterializeError>> {
    let limits = checkout.volume_config().limits;
    let profile = checkout.volume_config().profile;
    let mut names = Vec::new();
    for component in relative.components() {
        let std::path::Component::Normal(name) = component else {
            return Err(OperationFailure::before_work(MaterializeError::InvalidPath));
        };
        let (encoding, bytes) =
            crate::native_name::host_name_bytes(name, profile, limits.maximum_component_bytes)
                .map_err(|_| OperationFailure::before_work(MaterializeError::InvalidPath))?;
        let logical = LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
            .map_err(|_| OperationFailure::before_work(MaterializeError::InvalidPath))?;
        names.push(logical);
    }
    if names.is_empty() {
        return Err(OperationFailure::before_work(MaterializeError::InvalidPath));
    }
    let path = NamespacePath::new(names, limits)
        .map_err(|_| OperationFailure::before_work(MaterializeError::InvalidPath))?;
    materialize_checkout_path(checkout, &path, options, budget, cancellation).await
}

/// Restores one authenticated host-relative path without touching siblings.
///
/// Materialization occurs in a private same-volume stage. Existing entries are
/// exchanged atomically or replaced through ordinary mount-visible renames.
/// An absent authenticated path removes the current host entry.
pub async fn restore_checkout_host_path<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    relative: &Path,
    replacement: HostPathReplacement,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<HostPathRestore>, OperationFailure<MaterializeError>> {
    validate_host_relative(relative).map_err(OperationFailure::before_work)?;
    let destination_root = options.destination.clone();
    let relative = relative.to_path_buf();
    let (destination, stage_root) = tokio::task::spawn_blocking({
        let destination_root = destination_root.clone();
        let relative = relative.clone();
        move || prepare_restore(&destination_root, &relative, replacement)
    })
    .await
    .map_err(|error| OperationFailure::before_work(MaterializeError::Engine(error.to_string())))?
    .map_err(OperationFailure::before_work)?;
    let staged = stage_root.join(&relative);
    let materialize_options = MaterializeOptions {
        destination: stage_root.clone(),
        maximum_directory_entries: options.maximum_directory_entries,
        maximum_extent_spans: options.maximum_extent_spans,
        transfer_bytes: options.transfer_bytes,
    };
    let materialized = materialize_checkout_host_path(
        checkout,
        &relative,
        &materialize_options,
        budget,
        cancellation,
    )
    .await;
    let receipt = match materialized {
        Ok(receipt) => receipt,
        Err(failure) if matches!(failure.error, MaterializeError::MissingPath) => {
            let work = *failure.work;
            tokio::task::spawn_blocking(move || {
                ensure_real_parents(&destination_root, &relative).map_err(materialize_io_error)?;
                remove_any(&stage_root)?;
                remove_any(&destination)
            })
            .await
            .map_err(|error| {
                OperationFailure::new(MaterializeError::Engine(error.to_string()), work)
            })?
            .map_err(|error| OperationFailure::new(MaterializeError::Io(error), work))?;
            return Ok(OperationReceipt {
                value: HostPathRestore::Removed,
                work,
            });
        }
        Err(failure) => {
            let _ = tokio::task::spawn_blocking(move || remove_any(&stage_root)).await;
            return Err(failure);
        }
    };
    tokio::task::spawn_blocking(move || {
        publish_restore(
            &destination_root,
            &relative,
            &destination,
            &staged,
            &stage_root,
            replacement,
        )
    })
    .await
    .map_err(|error| {
        OperationFailure::new(MaterializeError::Engine(error.to_string()), receipt.work)
    })?
    .map_err(|error| OperationFailure::new(error, receipt.work))?;
    Ok(OperationReceipt {
        value: HostPathRestore::Restored,
        work: receipt.work,
    })
}

fn materialize_io_error(error: MaterializeError) -> std::io::Error {
    match error {
        MaterializeError::Io(error) => error,
        error => std::io::Error::other(error),
    }
}

fn prepare_restore(
    destination_root: &Path,
    relative: &Path,
    replacement: HostPathReplacement,
) -> Result<(PathBuf, PathBuf), MaterializeError> {
    let destination = destination_root.join(relative);
    ensure_real_parents(destination_root, relative)?;
    let stage_parent = match replacement {
        HostPathReplacement::Atomic => destination_root
            .parent()
            .ok_or(MaterializeError::InvalidPath)?
            .to_path_buf(),
        HostPathReplacement::LiveMount => destination
            .parent()
            .ok_or(MaterializeError::InvalidPath)?
            .to_path_buf(),
    };
    Ok((destination, create_restore_stage(&stage_parent)?))
}

fn publish_restore(
    destination_root: &Path,
    relative: &Path,
    destination: &Path,
    staged: &Path,
    stage_root: &Path,
    replacement: HostPathReplacement,
) -> Result<(), MaterializeError> {
    ensure_real_parents(destination_root, relative)?;
    match std::fs::symlink_metadata(destination) {
        Ok(_) => match replacement {
            HostPathReplacement::Atomic => crate::exchange_native_entries(destination, staged)
                .map_err(|error| MaterializeError::Engine(error.to_string()))?,
            HostPathReplacement::LiveMount => replace_live_mount(
                staged,
                destination,
                destination.parent().ok_or(MaterializeError::InvalidPath)?,
            )?,
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => match replacement {
            HostPathReplacement::Atomic => acyclic_native_runtime::durable_rename(
                staged,
                destination,
                acyclic_native_runtime::RenameMode::NoReplace,
            )?,
            HostPathReplacement::LiveMount => std::fs::rename(staged, destination)?,
        },
        Err(error) => return Err(error.into()),
    }
    remove_any(stage_root)?;
    Ok(())
}

fn validate_host_relative(relative: &Path) -> Result<(), MaterializeError> {
    let mut any = false;
    for component in relative.components() {
        let std::path::Component::Normal(_) = component else {
            return Err(MaterializeError::InvalidPath);
        };
        any = true;
    }
    if any {
        Ok(())
    } else {
        Err(MaterializeError::InvalidPath)
    }
}

fn ensure_real_parents(root: &Path, relative: &Path) -> Result<(), MaterializeError> {
    fn check(path: &Path) -> Result<(), MaterializeError> {
        let metadata = std::fs::symlink_metadata(path)?;
        #[cfg(windows)]
        let reparse = {
            use std::os::windows::fs::MetadataExt as _;
            metadata.file_attributes() & 0x400 != 0
        };
        #[cfg(not(windows))]
        let reparse = false;
        if !metadata.is_dir() || metadata.file_type().is_symlink() || reparse {
            return Err(MaterializeError::InvalidDestination);
        }
        Ok(())
    }

    check(root)?;
    let mut cursor = root.to_path_buf();
    if let Some(parent) = relative.parent() {
        for component in parent.components() {
            let std::path::Component::Normal(name) = component else {
                return Err(MaterializeError::InvalidPath);
            };
            cursor.push(name);
            match std::fs::symlink_metadata(&cursor) {
                Ok(_) => check(&cursor)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    std::fs::create_dir(&cursor)?;
                    check(&cursor)?;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(())
}

fn create_restore_stage(parent: &Path) -> Result<PathBuf, MaterializeError> {
    for _ in 0..16 {
        let sequence = RESTORE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let stage = parent.join(format!(
            ".acyclic-restore-{}-{sequence}",
            std::process::id()
        ));
        match std::fs::create_dir(&stage) {
            Ok(()) => return Ok(stage),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(MaterializeError::Io(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "restore stage names are exhausted",
    )))
}

fn remove_any(path: &Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(path),
        Ok(_) => std::fs::remove_file(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn replace_live_mount(
    staged: &Path,
    destination: &Path,
    parent: &Path,
) -> Result<(), MaterializeError> {
    let backup_root = create_restore_stage(parent)?;
    let backup = backup_root.join("old");
    if let Err(error) = std::fs::rename(destination, &backup) {
        let _ = std::fs::remove_dir(&backup_root);
        return Err(error.into());
    }
    if let Err(error) = std::fs::rename(staged, destination) {
        if let Err(rollback) = std::fs::rename(&backup, destination) {
            return Err(MaterializeError::Engine(format!(
                "replacement failed: {error}; displaced entry remains at {} after rollback failed: {rollback}",
                backup.display()
            )));
        }
        let _ = std::fs::remove_dir(&backup_root);
        return Err(error.into());
    }
    remove_any(&backup)?;
    std::fs::remove_dir(&backup_root)?;
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn materialize_entry<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    host_root: &HostRoot,
    path: &NamespacePath,
    host_path: &Path,
    file_id: FileId,
    kind: FileKind,
    payload: FilePayload,
    #[cfg(any(target_os = "macos", windows))] metadata_id: ObjectId,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    known_files: &mut HashMap<FileId, PathBuf>,
    #[cfg(any(target_os = "macos", windows))] known_payloads: &mut HashMap<
        (u64, ObjectId, ObjectId),
        PathBuf,
    >,
    #[cfg(any(target_os = "macos", windows))] clone_available: bool,
    pending: &mut Vec<(NamespacePath, PathBuf)>,
    receipt: &mut MaterializationReceipt,
) -> Result<(), OperationFailure<MaterializeError>> {
    if let Some(existing) = known_files.get(&file_id) {
        host_root
            .hard_link(existing, host_path)
            .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
        receipt.files = receipt.files.checked_add(1).ok_or_else(|| {
            OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
        })?;
        account_materialization(receipt, 0, budget)?;
        return Ok(());
    }

    let mut authenticated_metadata = None;
    match (kind, payload) {
        (FileKind::Directory, FilePayload::Directory { .. }) => {
            host_root
                .create_dir(host_path)
                .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
            known_files.insert(file_id, host_path.to_path_buf());
            pending.push((path.clone(), host_path.to_path_buf()));
            receipt.directories = receipt.directories.checked_add(1).ok_or_else(|| {
                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
            })?;
            account_materialization(receipt, 0, budget)?;
            return Ok(());
        }
        (FileKind::Regular, FilePayload::InlineRegular(data)) => {
            let file = create_file(host_root, host_path, receipt.work)?;
            acyclic_native_runtime::write_all_at(file.as_file(), 0, data.as_bytes())
                .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
            file.sync(acyclic_native_runtime::Durability::Full)
                .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
            known_files.insert(file_id, host_path.to_path_buf());
            account_file(
                receipt,
                data.as_bytes().len() as u64,
                data.as_bytes().len() as u64,
                budget,
            )?;
        }
        (
            FileKind::Regular,
            FilePayload::Regular {
                logical_bytes,
                extents,
            },
        ) => {
            #[cfg(not(any(target_os = "macos", windows)))]
            let _ = extents;
            #[cfg(any(target_os = "macos", windows))]
            let cloned = if clone_available
                && let Some(source) = known_payloads.get(&(logical_bytes, extents, metadata_id))
            {
                host_root
                    .clone_file(source, host_path)
                    .map_err(|error| OperationFailure::new(error.into(), receipt.work))?
            } else {
                false
            };
            #[cfg(not(any(target_os = "macos", windows)))]
            let cloned = false;
            if !cloned {
                let reader = checkout.pinned_reader().map_err(|error| {
                    OperationFailure::new(MaterializeError::Engine(error.to_string()), receipt.work)
                })?;
                let mut file = create_file(host_root, host_path, receipt.work)?;
                #[cfg(windows)]
                {
                    mark_sparse(file.as_file())
                        .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
                }
                file.set_len(logical_bytes)
                    .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
                authenticated_metadata = Some(
                    materialize_sparse_file(
                        &reader,
                        path,
                        &mut file,
                        logical_bytes,
                        options,
                        budget,
                        cancellation,
                        receipt,
                    )
                    .await?,
                );
                file.sync(acyclic_native_runtime::Durability::Full)
                    .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
            }
            #[cfg(target_os = "macos")]
            if cloned {
                host_root
                    .open_file(host_path)
                    .and_then(|file| file.sync_all())
                    .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
            }
            #[cfg(any(target_os = "macos", windows))]
            known_payloads
                .entry((logical_bytes, extents, metadata_id))
                .or_insert_with(|| host_path.to_path_buf());
            known_files.insert(file_id, host_path.to_path_buf());
            receipt.files = receipt.files.checked_add(1).ok_or_else(|| {
                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
            })?;
            receipt.logical_file_bytes = receipt
                .logical_file_bytes
                .checked_add(logical_bytes)
                .ok_or_else(|| {
                    OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
                })?;
            account_materialization(receipt, 0, budget)?;
        }
        (FileKind::SymbolicLink, FilePayload::SymbolicLink { target_bytes, .. }) => {
            let remaining = receipt.work.remaining(budget).map_err(|error| {
                OperationFailure::new(MaterializeError::Work(error), receipt.work)
            })?;
            let target = checkout
                .read_symbolic_link(path, remaining, cancellation)
                .await
                .map_err(|failure| map_engine_failure(failure, receipt.work))?;
            receipt.work = add_work(receipt.work, target.work)?;
            if u64::try_from(target.value.len()).unwrap_or(u64::MAX) != target_bytes {
                return Err(OperationFailure::new(
                    MaterializeError::Engine("symbolic-link length mismatch".to_owned()),
                    receipt.work,
                ));
            }
            create_symlink(host_root, &target.value, host_path)
                .map_err(|error| OperationFailure::new(error, receipt.work))?;
            known_files.insert(file_id, host_path.to_path_buf());
            receipt.symbolic_links = receipt.symbolic_links.checked_add(1).ok_or_else(|| {
                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
            })?;
            account_materialization(receipt, 0, budget)?;
        }
        (kind @ (FileKind::Fifo | FileKind::Socket), FilePayload::Empty) => {
            create_special(host_root, host_path, kind, None)
                .map_err(|error| OperationFailure::new(error, receipt.work))?;
            known_files.insert(file_id, host_path.to_path_buf());
            receipt.special_files = receipt.special_files.checked_add(1).ok_or_else(|| {
                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
            })?;
            account_materialization(receipt, 0, budget)?;
        }
        (
            kind @ (FileKind::CharacterDevice | FileKind::BlockDevice),
            FilePayload::Device { major, minor },
        ) => {
            create_special(host_root, host_path, kind, Some((major, minor)))
                .map_err(|error| OperationFailure::new(error, receipt.work))?;
            known_files.insert(file_id, host_path.to_path_buf());
            receipt.special_files = receipt.special_files.checked_add(1).ok_or_else(|| {
                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
            })?;
            account_materialization(receipt, 0, budget)?;
        }
        (unsupported, _) => {
            return Err(OperationFailure::new(
                MaterializeError::UnsupportedKind(unsupported),
                receipt.work,
            ));
        }
    }
    if let Some(metadata) = authenticated_metadata {
        apply_host_metadata(host_root, host_path, metadata)
            .map_err(|error| OperationFailure::new(error, receipt.work))
    } else {
        apply_metadata(
            checkout,
            host_root,
            path,
            host_path,
            budget,
            cancellation,
            receipt,
        )
        .await
    }
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn mark_sparse(file: &File) -> std::io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::FSCTL_SET_SPARSE;

    let handle = HANDLE(file.as_raw_handle());
    // SAFETY: `handle` is borrowed from the live `File` for the duration of
    // this synchronous call. FSCTL_SET_SPARSE takes no input/output buffers,
    // and every optional pointer is therefore null.
    unsafe {
        DeviceIoControl(handle, FSCTL_SET_SPARSE, None, 0, None, 0, None, None)
            .map_err(|error| std::io::Error::other(error.to_string()))
    }
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
async fn materialize_sparse_file<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &PinnedReader<A, O>,
    path: &NamespacePath,
    file: &mut acyclic_native_runtime::NativeFile,
    logical_bytes: u64,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    receipt: &mut MaterializationReceipt,
) -> Result<FileMetadata, OperationFailure<MaterializeError>> {
    let remaining = receipt
        .work
        .remaining(budget)
        .map_err(|error| OperationFailure::new(MaterializeError::Work(error), receipt.work))?;
    let resolved = reader
        .resolve_files(std::slice::from_ref(path), remaining, cancellation)
        .await
        .map_err(|failure| map_engine_failure(failure, receipt.work))?;
    receipt.work = add_work(receipt.work, resolved.work)?;
    let resolved = resolved
        .value
        .into_iter()
        .next()
        .flatten()
        .ok_or_else(|| OperationFailure::new(MaterializeError::MissingPath, receipt.work))?;
    let metadata = resolved.description().metadata;
    let mut offset = 0_u64;
    while offset < logical_bytes {
        let length = options.transfer_bytes.min(logical_bytes - offset);
        let remaining = receipt
            .work
            .remaining(budget)
            .map_err(|error| OperationFailure::new(MaterializeError::Work(error), receipt.work))?;
        let plan = resolved
            .plan_extents(
                ByteRange { offset, length },
                options.maximum_extent_spans,
                remaining,
                cancellation,
            )
            .await
            .map_err(|failure| map_engine_failure(failure, receipt.work))?;
        receipt.work = add_work(receipt.work, plan.work)?;
        let Some(plan) = plan.value else {
            return Err(OperationFailure::new(
                MaterializeError::Engine(
                    "non-inline file returned an inline extent plan".to_owned(),
                ),
                receipt.work,
            ));
        };
        let mut writes = Vec::new();
        writes
            .try_reserve_exact(plan.spans.len())
            .map_err(|_| OperationFailure::new(MaterializeError::InvalidOptions, receipt.work))?;
        let mut reads = Vec::new();
        reads
            .try_reserve_exact(plan.spans.len())
            .map_err(|_| OperationFailure::new(MaterializeError::InvalidOptions, receipt.work))?;
        for span in plan.spans {
            match span.kind {
                ExtentKind::Hole => {
                    #[cfg(target_os = "macos")]
                    crate::native_host::punch_hole(file.as_file(), span.offset, span.length)
                        .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
                }
                ExtentKind::AllocatedZero => {
                    queue_zero_writes(
                        &mut writes,
                        span.offset,
                        span.length,
                        options.transfer_bytes,
                        receipt.work,
                    )?;
                }
                ExtentKind::Content { .. } => {
                    reads.push(ResolvedFileRangeReadRequest {
                        file: &resolved,
                        range: ByteRange {
                            offset: span.offset,
                            length: span.length,
                        },
                    });
                }
            }
        }
        if !reads.is_empty() {
            read_content_spans(
                reader,
                reads,
                options.maximum_extent_spans,
                budget,
                cancellation,
                receipt,
                &mut writes,
            )
            .await?;
        }
        cancellation.check().map_err(|error| {
            OperationFailure::new(MaterializeError::Engine(error.to_string()), receipt.work)
        })?;
        let written_bytes = writes.iter().try_fold(0_u64, |total, write| {
            total
                .checked_add(u64::try_from(write.bytes.len()).unwrap_or(u64::MAX))
                .ok_or_else(|| {
                    OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
                })
        })?;
        let mut write = file
            .write_all_batch_async(writes)
            .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
        let mut cancelled = false;
        tokio::select! {
            result = &mut write => {
                result.map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
            }
            () = cancellation.cancelled() => {
                cancelled = true;
                write.await.map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
            }
        }
        account_written(receipt, written_bytes, budget)?;
        if cancelled {
            return Err(OperationFailure::new(
                MaterializeError::Engine("operation cancelled".to_owned()),
                receipt.work,
            ));
        }
        offset = offset.checked_add(length).ok_or_else(|| {
            OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
        })?;
    }
    Ok(metadata)
}

#[allow(clippy::too_many_arguments)]
async fn read_content_spans<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &PinnedReader<A, O>,
    reads: Vec<ResolvedFileRangeReadRequest<'_, A, O>>,
    maximum_extent_spans: u32,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    receipt: &mut MaterializationReceipt,
    writes: &mut Vec<acyclic_native_runtime::OwnedWrite>,
) -> Result<(), OperationFailure<MaterializeError>> {
    let remaining = receipt
        .work
        .remaining(budget)
        .map_err(|error| OperationFailure::new(MaterializeError::Work(error), receipt.work))?;
    let read = reader
        .read_resolved_ranges(
            &reads,
            usize::try_from(maximum_extent_spans).unwrap_or(usize::MAX),
            remaining,
            cancellation,
        )
        .await
        .map_err(|failure| map_engine_failure(failure, receipt.work))?;
    receipt.work = add_work(receipt.work, read.work)?;
    writes.extend(reads.into_iter().zip(read.value).map(|(request, result)| {
        acyclic_native_runtime::OwnedWrite {
            offset: request.range.offset,
            bytes: result.bytes,
        }
    }));
    Ok(())
}

fn queue_zero_writes(
    writes: &mut Vec<acyclic_native_runtime::OwnedWrite>,
    offset: u64,
    length: u64,
    transfer_bytes: u64,
    work: WorkCounters,
) -> Result<(), OperationFailure<MaterializeError>> {
    if transfer_bytes == 0 {
        return Err(OperationFailure::new(
            MaterializeError::InvalidOptions,
            work,
        ));
    }
    let capacity = usize::try_from(transfer_bytes.min(length))
        .map_err(|_| OperationFailure::new(MaterializeError::InvalidOptions, work))?;
    let zeros = bytes::Bytes::from(vec![0_u8; capacity]);
    let chunks = length.div_ceil(transfer_bytes);
    writes
        .try_reserve(usize::try_from(chunks).unwrap_or(usize::MAX))
        .map_err(|_| OperationFailure::new(MaterializeError::InvalidOptions, work))?;
    let mut remaining = length;
    let mut position = offset;
    while remaining != 0 {
        let count = usize::try_from(remaining.min(transfer_bytes))
            .map_err(|_| OperationFailure::new(MaterializeError::InvalidOptions, work))?;
        writes.push(acyclic_native_runtime::OwnedWrite {
            offset: position,
            bytes: zeros.slice(..count),
        });
        position = position.checked_add(count as u64).ok_or_else(|| {
            OperationFailure::new(MaterializeError::Work(WorkError::Overflow), work)
        })?;
        remaining -= count as u64;
    }
    Ok(())
}

async fn apply_metadata<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    host_root: &HostRoot,
    path: &NamespacePath,
    host_path: &Path,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    receipt: &mut MaterializationReceipt,
) -> Result<(), OperationFailure<MaterializeError>> {
    let remaining = receipt
        .work
        .remaining(budget)
        .map_err(|error| OperationFailure::new(MaterializeError::Work(error), receipt.work))?;
    let metadata = checkout
        .read_metadata(path, remaining, cancellation)
        .await
        .map_err(|failure| map_engine_failure(failure, receipt.work))?;
    receipt.work = add_work(receipt.work, metadata.work)?;
    apply_host_metadata(host_root, host_path, metadata.value)
        .map_err(|error| OperationFailure::new(error, receipt.work))
}

fn validate_options(options: &MaterializeOptions) -> Result<HostRoot, MaterializeError> {
    if options.maximum_directory_entries == 0
        || options.maximum_extent_spans == 0
        || options.transfer_bytes == 0
        || usize::try_from(options.transfer_bytes).is_err()
    {
        return Err(MaterializeError::InvalidOptions);
    }
    let root =
        HostRoot::open(&options.destination).map_err(|_| MaterializeError::InvalidDestination)?;
    if !root
        .is_empty()
        .map_err(|_| MaterializeError::InvalidDestination)?
    {
        return Err(MaterializeError::InvalidDestination);
    }
    Ok(root)
}

fn append_path(
    parent: &NamespacePath,
    name: LogicalName,
    limits: crate::model::VolumeLimits,
) -> Result<NamespacePath, MaterializeError> {
    let mut components = parent.components().to_vec();
    components.push(name);
    NamespacePath::new(components, limits)
        .map_err(|error| MaterializeError::Engine(error.to_string()))
}

fn create_file(
    host_root: &HostRoot,
    path: &Path,
    work: WorkCounters,
) -> Result<acyclic_native_runtime::NativeFile, OperationFailure<MaterializeError>> {
    host_root
        .create_file(path)
        .map(acyclic_native_runtime::NativeFile::from_file)
        .map_err(|error| OperationFailure::new(error.into(), work))
}

#[allow(clippy::needless_pass_by_value)]
fn map_engine_failure<E: std::fmt::Display>(
    failure: OperationFailure<E>,
    prior: WorkCounters,
) -> OperationFailure<MaterializeError> {
    match prior.checked_add(*failure.work) {
        Ok(work) => {
            OperationFailure::new(MaterializeError::Engine(failure.error.to_string()), work)
        }
        Err(error) => OperationFailure::new(MaterializeError::Work(error), prior),
    }
}

fn add_work(
    left: WorkCounters,
    right: WorkCounters,
) -> Result<WorkCounters, OperationFailure<MaterializeError>> {
    left.checked_add(right)
        .map_err(|error| OperationFailure::new(error.into(), left))
}

fn account_written(
    receipt: &mut MaterializationReceipt,
    bytes: u64,
    budget: WorkBudget,
) -> Result<(), OperationFailure<MaterializeError>> {
    receipt.written_bytes = receipt.written_bytes.checked_add(bytes).ok_or_else(|| {
        OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
    })?;
    receipt.work = add_work(
        receipt.work,
        WorkCounters {
            bytes_copied: bytes,
            output_bytes: bytes,
            ..WorkCounters::default()
        },
    )?;
    receipt
        .work
        .verify(budget)
        .map_err(|error| OperationFailure::new(MaterializeError::Work(error), receipt.work))
}

fn account_materialization(
    receipt: &mut MaterializationReceipt,
    bytes: u64,
    budget: WorkBudget,
) -> Result<(), OperationFailure<MaterializeError>> {
    account_written(receipt, bytes, budget)?;
    receipt.work = add_work(
        receipt.work,
        WorkCounters {
            materializations: 1,
            ..WorkCounters::default()
        },
    )?;
    receipt
        .work
        .verify(budget)
        .map_err(|error| OperationFailure::new(MaterializeError::Work(error), receipt.work))
}

fn account_file(
    receipt: &mut MaterializationReceipt,
    logical: u64,
    written: u64,
    budget: WorkBudget,
) -> Result<(), OperationFailure<MaterializeError>> {
    receipt.files = receipt.files.checked_add(1).ok_or_else(|| {
        OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
    })?;
    receipt.logical_file_bytes =
        receipt
            .logical_file_bytes
            .checked_add(logical)
            .ok_or_else(|| {
                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
            })?;
    account_materialization(receipt, written, budget)
}

#[cfg(unix)]
pub(crate) fn host_name(name: &LogicalName) -> Result<OsString, MaterializeError> {
    use std::os::unix::ffi::OsStringExt;
    match name.encoding() {
        NameEncoding::Utf8 | NameEncoding::PosixBytes => {
            Ok(OsString::from_vec(name.as_bytes().to_vec()))
        }
        NameEncoding::WindowsUtf16Le => Err(MaterializeError::UnrepresentableName),
    }
}

#[cfg(windows)]
pub(crate) fn host_name(name: &LogicalName) -> Result<OsString, MaterializeError> {
    use std::os::windows::ffi::OsStringExt;
    match name.encoding() {
        NameEncoding::Utf8 | NameEncoding::PosixBytes => std::str::from_utf8(name.as_bytes())
            .map(OsString::from)
            .map_err(|_| MaterializeError::UnrepresentableName),
        NameEncoding::WindowsUtf16Le => {
            let units = name
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| {
                    pair.try_into()
                        .map(u16::from_le_bytes)
                        .map_err(|_| MaterializeError::UnrepresentableName)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(OsString::from_wide(&units))
        }
    }
}

#[cfg(all(test, windows))]
mod windows_name_tests {
    use super::*;

    #[test]
    fn posix_profile_names_materialize_through_utf8_on_windows()
    -> Result<(), Box<dyn std::error::Error>> {
        let valid = LogicalName::new(
            NameEncoding::PosixBytes,
            "uni-é中.txt".as_bytes().to_vec(),
            255,
        )?;
        assert_eq!(host_name(&valid)?, OsString::from("uni-é中.txt"));

        let invalid = LogicalName::new(NameEncoding::PosixBytes, vec![0xff], 255)?;
        assert!(matches!(
            host_name(&invalid),
            Err(MaterializeError::UnrepresentableName)
        ));
        Ok(())
    }
}

#[cfg(unix)]
fn create_symlink(
    host_root: &HostRoot,
    target: &Bytes,
    destination: &Path,
) -> Result<(), MaterializeError> {
    use std::os::unix::ffi::OsStrExt;
    host_root
        .symlink(OsStr::from_bytes(target), destination)
        .map_err(Into::into)
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn create_special(
    host_root: &HostRoot,
    destination: &Path,
    kind: FileKind,
    device: Option<(u32, u32)>,
) -> Result<(), MaterializeError> {
    use std::os::unix::ffi::OsStrExt;
    let destination = std::ffi::CString::new(destination.as_os_str().as_bytes())
        .map_err(|_| MaterializeError::UnrepresentableName)?;
    let result = match kind {
        FileKind::Fifo => {
            // SAFETY: `destination` is a live NUL-terminated byte string and
            // the mode contains only a conventional permission mask.
            unsafe { libc::mkfifoat(host_root.raw_directory_fd(), destination.as_ptr(), 0o600) }
        }
        FileKind::CharacterDevice | FileKind::BlockDevice => {
            let Some((major, minor)) = device else {
                return Err(MaterializeError::UnsupportedKind(kind));
            };
            let file_type = if kind == FileKind::CharacterDevice {
                libc::S_IFCHR
            } else {
                libc::S_IFBLK
            };
            let device_number = match super::device::join_device(major, minor) {
                Ok(device_number) => device_number,
                Err(errno) => return Err(std::io::Error::from_raw_os_error(errno).into()),
            };
            // SAFETY: `destination` is a live NUL-terminated byte string and
            // the mode is a value-only libc operation.
            unsafe {
                libc::mknodat(
                    host_root.raw_directory_fd(),
                    destination.as_ptr(),
                    file_type | 0o600,
                    device_number,
                )
            }
        }
        FileKind::Socket => {
            host_root.bind_unix_socket(Path::new(OsStr::from_bytes(destination.as_bytes())))?;
            return Ok(());
        }
        _ => return Err(MaterializeError::UnsupportedKind(kind)),
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

#[cfg(not(unix))]
fn create_special(
    _: &HostRoot,
    _: &Path,
    kind: FileKind,
    _: Option<(u32, u32)>,
) -> Result<(), MaterializeError> {
    Err(MaterializeError::UnsupportedKind(kind))
}

#[cfg(windows)]
fn create_symlink(
    host_root: &HostRoot,
    target: &Bytes,
    destination: &Path,
) -> Result<(), MaterializeError> {
    use std::os::windows::ffi::OsStringExt;
    if !target.len().is_multiple_of(2) {
        return Err(MaterializeError::UnrepresentableName);
    }
    let units = target
        .chunks_exact(2)
        .map(|pair| {
            pair.try_into()
                .map(u16::from_le_bytes)
                .map_err(|_| MaterializeError::UnrepresentableName)
        })
        .collect::<Result<Vec<_>, _>>()?;
    host_root
        .symlink_file(Path::new(&OsString::from_wide(&units)), destination)
        .map_err(Into::into)
}

#[cfg(unix)]
fn apply_host_metadata(
    host_root: &HostRoot,
    path: &Path,
    metadata: FileMetadata,
) -> Result<(), MaterializeError> {
    use crate::kernel::MetadataField;
    use cap_std::fs::PermissionsExt;
    let file_type = host_root.symlink_metadata(path)?.file_type();
    if file_type.is_symlink() {
        return if metadata_is_unavailable(metadata) {
            Ok(())
        } else {
            Err(MaterializeError::UnsupportedKind(FileKind::SymbolicLink))
        };
    }
    if let MetadataField::Value(mode) = metadata.posix_mode {
        if file_type.is_file() || file_type.is_dir() {
            host_root.set_permissions(path, cap_std::fs::Permissions::from_mode(mode & 0o7777))?;
        } else {
            host_root.set_permissions_without_open(path, mode & 0o7777)?;
        }
    }
    Ok(())
}

#[cfg(windows)]
fn apply_host_metadata(
    host_root: &HostRoot,
    path: &Path,
    metadata: FileMetadata,
) -> Result<(), MaterializeError> {
    use crate::kernel::MetadataField;
    if host_root.symlink_metadata(path)?.file_type().is_symlink() {
        return if metadata_is_unavailable(metadata) {
            Ok(())
        } else {
            Err(MaterializeError::UnsupportedKind(FileKind::SymbolicLink))
        };
    }
    if let MetadataField::Value(attributes) = metadata.windows_attributes {
        let mut permissions = host_root.symlink_metadata(path)?.permissions();
        permissions.set_readonly(attributes & 1 != 0);
        host_root.set_permissions(path, permissions)?;
    }
    Ok(())
}

fn metadata_is_unavailable(metadata: FileMetadata) -> bool {
    use crate::kernel::MetadataField;
    matches!(metadata.posix_mode, MetadataField::Unavailable)
        && matches!(metadata.posix_uid, MetadataField::Unavailable)
        && matches!(metadata.posix_gid, MetadataField::Unavailable)
        && matches!(metadata.posix_flags, MetadataField::Unavailable)
        && matches!(metadata.windows_attributes, MetadataField::Unavailable)
        && matches!(metadata.created_ns, MetadataField::Unavailable)
        && matches!(metadata.modified_ns, MetadataField::Unavailable)
        && matches!(metadata.accessed_ns, MetadataField::Unavailable)
        && matches!(metadata.changed_ns, MetadataField::Unavailable)
        && matches!(metadata.named_attributes, MetadataField::Unavailable)
        && matches!(metadata.acl, MetadataField::Unavailable)
        && matches!(metadata.security_descriptor, MetadataField::Unavailable)
}

#[cfg(all(test, any(target_os = "macos", windows)))]
mod clone_tests {
    use super::*;
    use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
    use crate::model::{
        AccessMode, CaseSensitivity, CheckoutMode, ConcurrencyMode, ConsistencyMode,
        FilesystemProfile, GenerationSelector, Lifecycle, MutationMode, UnicodePolicy,
        VolumeConfig, VolumeLimits,
    };
    use crate::{Fs, VolumeId};

    #[tokio::test]
    async fn identical_regular_payloads_use_native_clone_without_aliasing()
    -> Result<(), Box<dyn std::error::Error>> {
        #[cfg(target_os = "macos")]
        let directory = tempfile::tempdir()?;
        #[cfg(windows)]
        let directory = {
            if let Some(parent) = std::env::var_os("ACYCLIC_TEST_BLOCK_CLONE_ROOT") {
                tempfile::Builder::new()
                    .prefix("acyclic-materialize-clone-")
                    .tempdir_in(parent)?
            } else {
                tempfile::tempdir()?
            }
        };
        let clone_available =
            crate::probe_native_storage_capabilities(directory.path())?.block_cloning;
        let limits = VolumeLimits::default();
        let fs = Fs::memory();
        let cancellation = CancellationToken::new();
        let volume = fs
            .create_volume_with_id(
                VolumeId::from_bytes([73; 16]),
                VolumeConfig {
                    profile: FilesystemProfile::Portable,
                    concurrency: ConcurrencyMode::Optimistic,
                    lifecycle: Lifecycle::Ephemeral,
                    case_sensitivity: CaseSensitivity::Sensitive,
                    unicode: UnicodePolicy::Preserve,
                    symbolic_links: true,
                    hard_links: true,
                    sparse_files: true,
                    limits,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let mut checkout = volume
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
            .value;
        // The payload deliberately ends between cluster boundaries: Windows
        // must clone the aligned body and copy the short tail.
        let bytes = Bytes::from(vec![0xA5; 128 * 1024 + 17]);
        for name in ["first.bin", "second.bin"] {
            let path = NamespacePath::new(
                vec![LogicalName::new(
                    NameEncoding::Utf8,
                    name.as_bytes().to_vec(),
                    limits.maximum_component_bytes,
                )?],
                limits,
            )?;
            checkout
                .create_file(path, bytes.clone(), WorkBudget::UNBOUNDED, &cancellation)
                .await?;
        }
        let destination = directory.path().join("view");
        std::fs::create_dir(&destination)?;
        let receipt = materialize_checkout(
            &mut checkout,
            &MaterializeOptions {
                destination: destination.clone(),
                maximum_directory_entries: 16,
                maximum_extent_spans: 16,
                transfer_bytes: 64 * 1024,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        assert_eq!(receipt.value.files, 2);
        assert_eq!(
            receipt.value.written_bytes,
            bytes.len() as u64 * if clone_available { 1 } else { 2 }
        );
        let first = destination.join("first.bin");
        let second = destination.join("second.bin");
        assert_eq!(std::fs::read(&first)?.as_slice(), bytes.as_ref());
        assert_eq!(std::fs::read(&second)?.as_slice(), bytes.as_ref());
        std::fs::write(&first, b"changed")?;
        assert_eq!(std::fs::read(&second)?.as_slice(), bytes.as_ref());
        Ok(())
    }
}
