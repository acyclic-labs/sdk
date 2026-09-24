//! Explicit native materialization of one authenticated checkout.

#[cfg(any(target_os = "macos", windows))]
use crate::ObjectId;
use crate::kernel::{
    ExtentKind, FileKind, FileMetadata, FilePayload, LogicalName, NameEncoding, NamespacePath,
};
use crate::native_host::{HostDirectory, HostRoot};
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
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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

/// Both outputs preserve canonical metadata in the SDK. Host-generated Unix
/// timestamps are view-local and are not required to match the physical view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaterializeMode {
    DurableOutput,
    ReconstructibleView,
}

impl MaterializeMode {
    fn host_metadata(metadata: FileMetadata) -> FileMetadata {
        #[cfg(unix)]
        let mut metadata = metadata;
        #[cfg(unix)]
        {
            metadata.changed_ns = crate::kernel::MetadataField::Unavailable;
        }
        #[cfg(target_os = "linux")]
        {
            metadata.created_ns = crate::kernel::MetadataField::Unavailable;
        }
        metadata
    }
}

impl MaterializeOptions {
    /// Creates the canonical bounded native materialization policy.
    #[must_use]
    pub fn native(destination: impl Into<PathBuf>) -> Self {
        Self {
            destination: destination.into(),
            maximum_directory_entries: 1_024,
            maximum_extent_spans: 65_536,
            transfer_bytes: 8 * 1024 * 1024,
        }
    }
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
    /// This host adapter cannot reproduce an authenticated metadata field.
    #[error("metadata field {0} cannot be materialized exactly on this host")]
    UnsupportedMetadata(&'static str),
    /// Supplied operation bounds are zero or exceed native addressability.
    #[error("materialization options are invalid")]
    InvalidOptions,
    /// Selected paths overlap, so pathwise staging would be order-dependent.
    #[error("materialization paths must be unique and non-overlapping")]
    OverlappingPaths,
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
    materialize_checkout_with_mode(
        checkout,
        options,
        budget,
        cancellation,
        MaterializeMode::DurableOutput,
    )
    .await
}

pub(crate) async fn materialize_checkout_with_mode<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    mode: MaterializeMode,
) -> Result<OperationReceipt<MaterializationReceipt>, OperationFailure<MaterializeError>> {
    let root =
        NamespacePath::new(Vec::new(), checkout.volume_config().limits).map_err(|error| {
            OperationFailure::before_work(MaterializeError::Engine(error.to_string()))
        })?;
    materialize_checkout_paths_with_mode(
        checkout,
        std::slice::from_ref(&root),
        options,
        budget,
        cancellation,
        mode,
    )
    .await
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
    materialize_checkout_paths(
        checkout,
        std::slice::from_ref(path),
        options,
        budget,
        cancellation,
    )
    .await
}

/// Materializes multiple non-overlapping authenticated paths in one traversal.
///
/// All selected paths share one destination capability, cumulative work
/// receipt, and file-identity table, preserving hard links across siblings.
#[allow(clippy::too_many_lines)]
pub async fn materialize_checkout_paths<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<MaterializationReceipt>, OperationFailure<MaterializeError>> {
    materialize_checkout_paths_with_mode(
        checkout,
        paths,
        options,
        budget,
        cancellation,
        MaterializeMode::DurableOutput,
    )
    .await
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn materialize_checkout_paths_with_mode<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    mode: MaterializeMode,
) -> Result<OperationReceipt<MaterializationReceipt>, OperationFailure<MaterializeError>> {
    let host_root = Arc::new(validate_options(options).map_err(OperationFailure::before_work)?);
    let reader = checkout.pinned_reader().map_err(|error| {
        OperationFailure::before_work(MaterializeError::Engine(error.to_string()))
    })?;
    if paths.is_empty()
        || (paths.len() != 1 && paths.iter().any(|path| path.components().is_empty()))
    {
        return Err(OperationFailure::before_work(
            MaterializeError::InvalidOptions,
        ));
    }
    let mut selected_paths = paths.to_vec();
    selected_paths.sort_unstable();
    if selected_paths
        .windows(2)
        .any(|pair| matches!(pair, [ancestor, path] if path.is_within(ancestor)))
    {
        return Err(OperationFailure::before_work(
            MaterializeError::OverlappingPaths,
        ));
    }
    let mut receipt = MaterializationReceipt::default();
    let mut known_files = HashMap::<FileId, PathBuf>::new();
    #[cfg(any(target_os = "macos", windows))]
    let mut known_payloads = HashMap::<(u64, ObjectId, ObjectId), PathBuf>::new();
    #[cfg(any(target_os = "macos", windows))]
    let clone_available = crate::probe_native_storage_capabilities(&options.destination)
        .is_ok_and(|capabilities| capabilities.block_cloning);
    let mut pending = Vec::new();
    let mut deferred_directory_metadata = Vec::new();
    for path in &selected_paths {
        if path.components().is_empty() {
            pending.push((path.clone(), PathBuf::new()));
            deferred_directory_metadata.push((path.clone(), PathBuf::new()));
            continue;
        }
        cancellation.check().map_err(|error| {
            OperationFailure::new(MaterializeError::Engine(error.to_string()), receipt.work)
        })?;
        let remaining = receipt
            .work
            .remaining(budget)
            .map_err(|error| OperationFailure::new(MaterializeError::Work(error), receipt.work))?;
        let lookup = checkout
            .lookup_no_follow(path, remaining, cancellation)
            .await
            .map_err(|failure| map_engine_failure(failure, receipt.work))?;
        receipt.work = add_work(receipt.work, lookup.work)?;
        let record = lookup
            .value
            .record
            .ok_or_else(|| OperationFailure::new(MaterializeError::MissingPath, receipt.work))?;
        let mut host_path = PathBuf::new();
        for component in path.components() {
            host_path.push(
                host_name(component).map_err(|error| OperationFailure::new(error, receipt.work))?,
            );
        }
        if let Some(parent) = host_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            host_root
                .create_dir_all_held(parent)
                .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
        }
        if record.kind == FileKind::Directory {
            deferred_directory_metadata.push((path.clone(), host_path.clone()));
        }
        materialize_entry(
            &reader,
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
            mode,
        )
        .await?;
    }

    while let Some((directory, host_directory)) = pending.pop() {
        cancellation.check().map_err(|error| {
            OperationFailure::new(MaterializeError::Engine(error.to_string()), receipt.work)
        })?;
        let mut after = None;
        loop {
            cancellation.check().map_err(|error| {
                OperationFailure::new(MaterializeError::Engine(error.to_string()), receipt.work)
            })?;
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
            if page.value.has_more && page.value.entries.is_empty() {
                return Err(OperationFailure::new(
                    MaterializeError::Engine("directory pagination made no progress".to_owned()),
                    receipt.work,
                ));
            }
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
                if entry.record.kind == FileKind::Directory {
                    deferred_directory_metadata.push((child.clone(), child_host.clone()));
                }
                materialize_entry(
                    &reader,
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
                    mode,
                )
                .await?;
                after = Some(entry.name);
            }
            if !page.value.has_more {
                break;
            }
        }
    }
    for (directory, host_directory) in deferred_directory_metadata.into_iter().rev() {
        apply_metadata(
            &reader,
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
    validate_bounds(options).map_err(OperationFailure::before_work)?;
    if cancellation.is_cancelled() {
        return Err(OperationFailure::before_work(MaterializeError::Engine(
            "materialization cancelled".into(),
        )));
    }
    let destination_root = options.destination.clone();
    let relative = relative.to_path_buf();
    let (destination, stage_root, _restore_lock) = tokio::task::spawn_blocking({
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
                let parent =
                    held_parent(&destination_root, &relative).map_err(materialize_io_error)?;
                let name = relative
                    .file_name()
                    .ok_or_else(|| std::io::Error::other("restore path has no leaf"))?;
                remove_any(&stage_root)?;
                remove_restored_path(&parent, Path::new(name), &destination_root, &relative)
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
) -> Result<(PathBuf, PathBuf, File), MaterializeError> {
    let destination = destination_root.join(relative);
    let destination_parent = held_parent(destination_root, relative)?;
    let destination_name = relative.file_name().ok_or(MaterializeError::InvalidPath)?;
    let restore_lock = acquire_restore_lock(relative, &destination, true)?;
    cleanup_removed_restore(&destination_parent, relative, &destination)?;
    match replacement {
        HostPathReplacement::Atomic => {
            crate::native_exchange::recover_native_entry_exchange(&destination)
                .map_err(|error| MaterializeError::Engine(error.to_string()))?;
        }
        HostPathReplacement::LiveMount => recover_live_mount_replacement(
            &destination_parent,
            Path::new(destination_name),
            relative,
            &destination,
        )?,
    }
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
    Ok((
        destination,
        create_restore_stage(&stage_parent)?,
        restore_lock,
    ))
}

fn publish_restore(
    destination_root: &Path,
    relative: &Path,
    destination: &Path,
    staged: &Path,
    stage_root: &Path,
    replacement: HostPathReplacement,
) -> Result<(), MaterializeError> {
    let destination_parent = held_parent(destination_root, relative)?;
    let destination_name = relative.file_name().ok_or(MaterializeError::InvalidPath)?;
    let stage_parent = held_parent(stage_root, relative)?;
    let staged_name = relative.file_name().ok_or(MaterializeError::InvalidPath)?;
    match destination_parent.symlink_metadata(Path::new(destination_name)) {
        Ok(_) => match replacement {
            HostPathReplacement::Atomic => crate::exchange_native_entries(destination, staged)
                .map_err(|error| MaterializeError::Engine(error.to_string()))?,
            HostPathReplacement::LiveMount => replace_live_mount(
                &stage_parent,
                Path::new(staged_name),
                &destination_parent,
                Path::new(destination_name),
                relative,
                destination,
                staged,
            )?,
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            stage_parent.rename_to(
                Path::new(staged_name),
                &destination_parent,
                Path::new(destination_name),
            )?;
            sync_restore_parent(destination)?;
            sync_restore_parent(staged)?;
        }
        Err(error) => return Err(error.into()),
    }
    stage_parent.close();
    drop(destination_parent);
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

fn held_parent(root: &Path, relative: &Path) -> Result<HostDirectory, MaterializeError> {
    let root = HostRoot::open(root).map_err(|_| MaterializeError::InvalidDestination)?;
    root.create_dir_all_held(relative.parent().unwrap_or_else(|| Path::new("")))
        .map_err(|_| MaterializeError::InvalidDestination)
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

fn restore_artifact_name(prefix: &str, relative: &Path) -> PathBuf {
    let mut hasher = blake3::Hasher::new();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        hasher.update(relative.as_os_str().as_bytes());
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;
        for unit in relative.as_os_str().encode_wide() {
            hasher.update(&unit.to_le_bytes());
        }
    }
    #[cfg(not(any(unix, windows)))]
    hasher.update(relative.to_string_lossy().as_bytes());
    PathBuf::from(format!("{prefix}{}", hasher.finalize().to_hex()))
}

fn acquire_restore_lock(
    relative: &Path,
    destination: &Path,
    blocking: bool,
) -> Result<File, MaterializeError> {
    let path = restore_witness_path(".acyclic-restore-lock-", relative, destination);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)?;
    let metadata = std::fs::symlink_metadata(&path)?;
    if metadata.file_type().is_symlink() || !file.metadata().is_ok_and(|value| value.is_file()) {
        return Err(MaterializeError::InvalidDestination);
    }
    if blocking {
        fs2::FileExt::lock_exclusive(&file)?;
    } else {
        fs2::FileExt::try_lock_exclusive(&file)?;
    }
    Ok(file)
}

fn sync_restore_parent(destination: &Path) -> Result<(), MaterializeError> {
    let parent = destination.parent().ok_or(MaterializeError::InvalidPath)?;
    acyclic_native_runtime::sync_parent(parent, acyclic_native_runtime::Durability::Full)
        .map_err(Into::into)
}

const REMOVE_RESTORE_WITNESS: &[u8] = b"acyclic-remove-restore-v1\n";
const LIVE_RESTORE_WITNESS: &[u8] = b"acyclic-live-restore-v1\n";

fn restore_witness_path(prefix: &str, relative: &Path, destination: &Path) -> PathBuf {
    destination
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .join(restore_artifact_name(prefix, relative))
}

fn create_restore_witness(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;

    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)?;
    file.write_all(contents)?;
    file.sync_all()?;
    acyclic_native_runtime::sync_parent(
        path.parent()
            .ok_or_else(|| std::io::Error::other("restore witness has no parent"))?,
        acyclic_native_runtime::Durability::Full,
    )
}

fn validate_restore_witness(path: &Path, expected: &[u8]) -> std::io::Result<bool> {
    match std::fs::read(path) {
        Ok(contents) if contents == expected => Ok(true),
        Ok(_) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "restore artifact ownership witness is invalid",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn remove_restore_witness(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => acyclic_native_runtime::sync_parent(
            path.parent()
                .ok_or_else(|| std::io::Error::other("restore witness has no parent"))?,
            acyclic_native_runtime::Durability::Full,
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn cleanup_removed_restore(
    destination_parent: &HostDirectory,
    relative: &Path,
    destination: &Path,
) -> Result<(), MaterializeError> {
    let removed = restore_artifact_name(".acyclic-restore-removed-", relative);
    let witness = restore_witness_path(".acyclic-restore-remove-witness-", relative, destination);
    match destination_parent.symlink_metadata(&removed) {
        Ok(_) => {
            if !validate_restore_witness(&witness, REMOVE_RESTORE_WITNESS)? {
                return Err(MaterializeError::Engine(
                    "unowned restore-removal artifact collision".into(),
                ));
            }
            destination_parent.remove(&removed)?;
            sync_restore_parent(destination)?;
            remove_restore_witness(&witness)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if validate_restore_witness(&witness, REMOVE_RESTORE_WITNESS)? {
                remove_restore_witness(&witness)?;
            }
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn remove_restored_path(
    destination_parent: &HostDirectory,
    destination_name: &Path,
    destination_root: &Path,
    relative: &Path,
) -> std::io::Result<()> {
    let destination = destination_root.join(relative);
    let removed = restore_artifact_name(".acyclic-restore-removed-", relative);
    let witness = restore_witness_path(".acyclic-restore-remove-witness-", relative, &destination);
    create_restore_witness(&witness, REMOVE_RESTORE_WITNESS)?;
    match destination_parent.symlink_metadata(destination_name) {
        Ok(_) => {
            destination_parent.rename_to(destination_name, destination_parent, &removed)?;
            acyclic_native_runtime::sync_parent(
                destination.parent().ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has no parent")
                })?,
                acyclic_native_runtime::Durability::Full,
            )?;
            destination_parent.remove(&removed)?;
            acyclic_native_runtime::sync_parent(
                destination.parent().ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has no parent")
                })?,
                acyclic_native_runtime::Durability::Full,
            )?;
            remove_restore_witness(&witness)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            remove_restore_witness(&witness)
        }
        Err(error) => Err(error),
    }
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
    staged_parent: &HostDirectory,
    staged_name: &Path,
    destination_parent: &HostDirectory,
    destination_name: &Path,
    relative: &Path,
    destination: &Path,
    staged: &Path,
) -> Result<(), MaterializeError> {
    let backup_name = restore_artifact_name(".acyclic-restore-backup-", relative);
    recover_live_mount_replacement(destination_parent, destination_name, relative, destination)?;
    let witness = restore_witness_path(".acyclic-restore-live-witness-", relative, destination);
    create_restore_witness(&witness, LIVE_RESTORE_WITNESS)?;
    destination_parent.rename_to(destination_name, destination_parent, &backup_name)?;
    sync_restore_parent(destination)?;
    if let Err(error) = staged_parent.rename_to(staged_name, destination_parent, destination_name) {
        if let Err(rollback) =
            destination_parent.rename_to(&backup_name, destination_parent, destination_name)
        {
            return Err(MaterializeError::Engine(format!(
                "replacement failed: {error}; displaced entry remains at {} after rollback failed: {rollback}",
                backup_name.display()
            )));
        }
        sync_restore_parent(destination)?;
        return Err(error.into());
    }
    sync_restore_parent(destination)?;
    sync_restore_parent(staged)?;
    destination_parent.remove(&backup_name)?;
    sync_restore_parent(destination)?;
    remove_restore_witness(&witness)?;
    Ok(())
}

fn recover_live_mount_replacement(
    destination_parent: &HostDirectory,
    destination_name: &Path,
    relative: &Path,
    destination: &Path,
) -> Result<(), MaterializeError> {
    let backup_name = restore_artifact_name(".acyclic-restore-backup-", relative);
    let witness = restore_witness_path(".acyclic-restore-live-witness-", relative, destination);
    let backup_exists = match destination_parent.symlink_metadata(&backup_name) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    if !backup_exists {
        if validate_restore_witness(&witness, LIVE_RESTORE_WITNESS)? {
            remove_restore_witness(&witness)?;
        }
        return Ok(());
    }
    if !validate_restore_witness(&witness, LIVE_RESTORE_WITNESS)? {
        return Err(MaterializeError::Engine(
            "unowned live-restore backup collision".into(),
        ));
    }
    match destination_parent.symlink_metadata(destination_name) {
        Ok(_) => destination_parent.remove(&backup_name)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            destination_parent.rename_to(&backup_name, destination_parent, destination_name)?;
        }
        Err(error) => return Err(error.into()),
    }
    sync_restore_parent(destination)?;
    remove_restore_witness(&witness)?;
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn materialize_entry<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &PinnedReader<A, O>,
    host_root: &Arc<HostRoot>,
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
    mode: MaterializeMode,
) -> Result<(), OperationFailure<MaterializeError>> {
    cancellation.check().map_err(|error| {
        OperationFailure::new(MaterializeError::Engine(error.to_string()), receipt.work)
    })?;
    if let Some(existing) = known_files.get(&file_id) {
        host_root
            .hard_link(existing, host_path)
            .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
        receipt.files = receipt.files.checked_add(1).ok_or_else(|| {
            OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
        })?;
        account_materialization(receipt, 0, budget)?;
        cancellation.check().map_err(|error| {
            OperationFailure::new(MaterializeError::Engine(error.to_string()), receipt.work)
        })?;
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
            cancellation.check().map_err(|error| {
                OperationFailure::new(MaterializeError::Engine(error.to_string()), receipt.work)
            })?;
            return Ok(());
        }
        (FileKind::Regular, FilePayload::InlineRegular(data)) => {
            let file = create_file(host_root, host_path, receipt.work)?;
            file.write_all_batch_async(vec![acyclic_native_runtime::OwnedWrite {
                offset: 0,
                bytes: Bytes::copy_from_slice(data.as_bytes()),
            }])
            .await
            .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
            if mode == MaterializeMode::DurableOutput {
                file.sync_async(acyclic_native_runtime::Durability::Full)
                    .await
                    .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
            }
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
                let mut file = create_file(host_root, host_path, receipt.work)?;
                #[cfg(windows)]
                {
                    file.control_async(mark_sparse)
                        .await
                        .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
                }
                file.set_len_async(logical_bytes)
                    .await
                    .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
                authenticated_metadata = Some(
                    materialize_sparse_file(
                        reader,
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
                if mode == MaterializeMode::DurableOutput {
                    file.sync_async(acyclic_native_runtime::Durability::Full)
                        .await
                        .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
                }
            }
            #[cfg(target_os = "macos")]
            if cloned && mode == MaterializeMode::DurableOutput {
                let file = host_root
                    .open_file(host_path)
                    .map(cap_std::fs::File::into_std)
                    .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
                acyclic_native_runtime::NativeFile::from_file(file)
                    .map_err(|error| OperationFailure::new(error.into(), receipt.work))?
                    .sync_async(acyclic_native_runtime::Durability::Full)
                    .await
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
            let target = reader
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
    // Directory metadata is applied after descendants have been populated.
    // Applying it here would do the same work twice and can make a directory
    // read-only before its children have been created.
    if kind != FileKind::Directory {
        if let Some(metadata) = authenticated_metadata {
            apply_host_metadata_offloaded(
                host_root,
                host_path,
                MaterializeMode::host_metadata(metadata),
            )
            .await
            .map_err(|error| OperationFailure::new(error, receipt.work))?;
        } else {
            apply_metadata(
                reader,
                host_root,
                path,
                host_path,
                budget,
                cancellation,
                receipt,
            )
            .await?;
        }
    }
    cancellation.check().map_err(|error| {
        OperationFailure::new(MaterializeError::Engine(error.to_string()), receipt.work)
    })?;
    Ok(())
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn mark_sparse(file: &File) -> std::io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::Foundation::{ERROR_IO_PENDING, HANDLE};
    use windows::Win32::System::IO::{DeviceIoControl, GetOverlappedResult, OVERLAPPED};
    use windows::Win32::System::Ioctl::FSCTL_SET_SPARSE;

    let handle = HANDLE(file.as_raw_handle());
    let mut overlapped = OVERLAPPED::default();
    // This runs as the first sequenced control operation, before the native
    // completion owner attaches the overlapped file handle to an IOCP. The
    // FSCTL has no payload buffers; its OVERLAPPED must nevertheless remain
    // live until terminal completion, including when DeviceIoControl pends.
    let submitted = unsafe {
        DeviceIoControl(
            handle,
            FSCTL_SET_SPARSE,
            None,
            0,
            None,
            0,
            None,
            Some(&mut overlapped),
        )
    };
    match submitted {
        Ok(()) => Ok(()),
        Err(error) if error.code() == windows::core::HRESULT::from_win32(ERROR_IO_PENDING.0) => {
            let mut transferred = 0;
            // SAFETY: the file and OVERLAPPED remain live through this wait.
            // With no other I/O on this newly created file, a null hEvent is
            // the file handle's own event. If waiting itself fails, completion
            // is uncertain and unwinding would free the stack OVERLAPPED.
            match unsafe { GetOverlappedResult(handle, &overlapped, &mut transferred, true) } {
                Ok(()) => Ok(()),
                Err(_) => std::process::abort(),
            }
        }
        Err(error) => Err(std::io::Error::other(error.to_string())),
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
                    file.control_async(move |handle| {
                        crate::native_host::punch_hole(handle, span.offset, span.length)
                    })
                    .await
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
        let mut write = file.write_all_batch_async(writes);
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

#[allow(clippy::too_many_arguments)]
async fn apply_metadata<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &PinnedReader<A, O>,
    host_root: &Arc<HostRoot>,
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
    let metadata = reader
        .read_metadata(path, remaining, cancellation)
        .await
        .map_err(|failure| map_engine_failure(failure, receipt.work))?;
    receipt.work = add_work(receipt.work, metadata.work)?;
    apply_host_metadata_offloaded(
        host_root,
        host_path,
        MaterializeMode::host_metadata(metadata.value),
    )
    .await
    .map_err(|error| OperationFailure::new(error, receipt.work))
}

#[cfg(target_os = "linux")]
async fn apply_host_metadata_offloaded(
    host_root: &Arc<HostRoot>,
    host_path: &Path,
    metadata: FileMetadata,
) -> Result<(), MaterializeError> {
    reject_unbaselined_unix_metadata(metadata)?;
    if metadata == FileMetadata::default() {
        return verify_host_path_offloaded(host_root, host_path).await;
    }
    let root = Arc::clone(host_root);
    let path = host_path.to_path_buf();
    // Opening is non-mutating. If its observer is dropped, the worker can
    // only release the newly held inode; it cannot alter a reused path.
    let target =
        acyclic_native_runtime::run_blocking_io(move || root.open_linux_metadata_target(&path))
            .await
            .map_err(MaterializeError::Io)?
            .map_err(linux_metadata_error)?;
    // All mutations use the pinned inode. A dropped observer cannot redirect
    // this operation to a replacement at the same relative path.
    acyclic_native_runtime::run_blocking_io(move || target.apply(metadata))
        .await
        .map_err(MaterializeError::Io)?
        .map_err(linux_metadata_error)
}

#[cfg(target_os = "macos")]
async fn apply_host_metadata_offloaded(
    host_root: &Arc<HostRoot>,
    host_path: &Path,
    metadata: FileMetadata,
) -> Result<(), MaterializeError> {
    reject_unbaselined_unix_metadata(metadata)?;
    if metadata == FileMetadata::default() {
        return verify_host_path_offloaded(host_root, host_path).await;
    }
    let root = Arc::clone(host_root);
    let path = host_path.to_path_buf();
    let target =
        acyclic_native_runtime::run_blocking_io(move || root.open_macos_metadata_target(&path))
            .await
            .map_err(MaterializeError::Io)?
            .map_err(mac_metadata_error)?;
    acyclic_native_runtime::run_blocking_io(move || target.apply(metadata))
        .await
        .map_err(MaterializeError::Io)?
        .map_err(mac_metadata_error)
}

#[cfg(windows)]
async fn apply_host_metadata_offloaded(
    host_root: &Arc<HostRoot>,
    host_path: &Path,
    metadata: FileMetadata,
) -> Result<(), MaterializeError> {
    if metadata == FileMetadata::default() {
        return verify_host_path_offloaded(host_root, host_path).await;
    }
    let root = Arc::clone(host_root);
    let path = host_path.to_path_buf();
    let target =
        acyclic_native_runtime::run_blocking_io(move || root.open_windows_metadata_target(&path))
            .await
            .map_err(MaterializeError::Io)?
            .map_err(windows_metadata_error)?;
    acyclic_native_runtime::run_blocking_io(move || target.apply(metadata))
        .await
        .map_err(MaterializeError::Io)?
        .map_err(windows_metadata_error)
}

async fn verify_host_path_offloaded(
    host_root: &Arc<HostRoot>,
    host_path: &Path,
) -> Result<(), MaterializeError> {
    let root = Arc::clone(host_root);
    let path = host_path.to_path_buf();
    acyclic_native_runtime::run_blocking_io(move || root.symlink_metadata_held(&path).map(|_| ()))
        .await
        .map_err(MaterializeError::Io)?
        .map_err(MaterializeError::Io)
}

#[cfg(windows)]
fn windows_metadata_error(error: crate::native_host::WindowsMetadataError) -> MaterializeError {
    match error {
        crate::native_host::WindowsMetadataError::Unsupported(field) => {
            MaterializeError::UnsupportedMetadata(field)
        }
        crate::native_host::WindowsMetadataError::Io(error) => MaterializeError::Io(error),
    }
}

#[cfg(unix)]
fn reject_unbaselined_unix_metadata(metadata: FileMetadata) -> Result<(), MaterializeError> {
    use crate::kernel::MetadataField;
    if matches!(metadata.changed_ns, MetadataField::Value(_)) {
        return Err(MaterializeError::UnsupportedMetadata(
            "changed_ns without native-view baseline",
        ));
    }
    #[cfg(target_os = "linux")]
    if matches!(metadata.created_ns, MetadataField::Value(_)) {
        return Err(MaterializeError::UnsupportedMetadata(
            "created_ns without native-view baseline",
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn linux_metadata_error(error: crate::native_host::LinuxMetadataError) -> MaterializeError {
    match error {
        crate::native_host::LinuxMetadataError::Unsupported(field) => {
            MaterializeError::UnsupportedMetadata(field)
        }
        crate::native_host::LinuxMetadataError::Io(error) => MaterializeError::Io(error),
    }
}

#[cfg(target_os = "macos")]
fn mac_metadata_error(error: crate::native_host::MacMetadataError) -> MaterializeError {
    match error {
        crate::native_host::MacMetadataError::Unsupported(field) => {
            MaterializeError::UnsupportedMetadata(field)
        }
        crate::native_host::MacMetadataError::Io(error) => MaterializeError::Io(error),
    }
}

fn validate_options(options: &MaterializeOptions) -> Result<HostRoot, MaterializeError> {
    validate_bounds(options)?;
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

fn validate_bounds(options: &MaterializeOptions) -> Result<(), MaterializeError> {
    if options.maximum_directory_entries == 0
        || options.maximum_extent_spans == 0
        || options.transfer_bytes == 0
        || usize::try_from(options.transfer_bytes).is_err()
    {
        return Err(MaterializeError::InvalidOptions);
    }
    Ok(())
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
    #[cfg(windows)]
    {
        let file = host_root
            .create_overlapped_file(path)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        // SAFETY: HostRoot created this handle with FILE_FLAG_OVERLAPPED,
        // capability-relative to the authorized root. It is newly created,
        // has no completion-port association, and is moved directly into the
        // sole native I/O owner without any independent file I/O.
        #[allow(unsafe_code)]
        unsafe { acyclic_native_runtime::NativeFile::from_overlapped_file_unchecked(file) }
            .map_err(|error| OperationFailure::new(error.into(), work))
    }
    #[cfg(not(windows))]
    {
        host_root
            .create_file(path)
            .and_then(acyclic_native_runtime::NativeFile::from_file)
            .map_err(|error| OperationFailure::new(error.into(), work))
    }
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

    #[tokio::test]
    async fn overlapped_sparse_control_finishes_before_native_writes()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let root = HostRoot::open(directory.path())?;
        let path = Path::new("sparse-control.bin");
        let file = root.create_overlapped_file(path)?;
        // SAFETY: the new capability-rooted handle is overlapped, unattached
        // to an IOCP, and moved directly to its sole native owner.
        #[allow(unsafe_code)]
        let native =
            unsafe { acyclic_native_runtime::NativeFile::from_overlapped_file_unchecked(file)? };
        native.control_async(mark_sparse).await?;
        native.set_len_async(1024 * 1024).await?;
        native
            .write_all_batch_async(vec![acyclic_native_runtime::OwnedWrite {
                offset: 1024 * 1024 - 4,
                bytes: Bytes::from_static(b"tail"),
            }])
            .await?;
        native
            .sync_async(acyclic_native_runtime::Durability::Full)
            .await?;
        let bytes = std::fs::read(directory.path().join(path))?;
        assert_eq!(bytes.len(), 1024 * 1024);
        assert_eq!(&bytes[bytes.len() - 4..], b"tail");
        Ok(())
    }

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
#[allow(
    clippy::useless_conversion,
    reason = "libc file-type constants have different widths on Linux and macOS"
)]
fn create_special(
    host_root: &HostRoot,
    destination: &Path,
    kind: FileKind,
    device: Option<(u32, u32)>,
) -> Result<(), MaterializeError> {
    match kind {
        FileKind::Fifo => host_root.create_fifo_held(destination, 0o600)?,
        FileKind::CharacterDevice | FileKind::BlockDevice => {
            let Some((major, minor)) = device else {
                return Err(MaterializeError::UnsupportedKind(kind));
            };
            let file_type = if kind == FileKind::CharacterDevice {
                u32::from(libc::S_IFCHR)
            } else {
                u32::from(libc::S_IFBLK)
            };
            let device_number = match super::device::join_device(major, minor) {
                Ok(device_number) => device_number,
                Err(errno) => return Err(std::io::Error::from_raw_os_error(errno).into()),
            };
            host_root.create_device_held(destination, file_type | 0o600, device_number)?;
        }
        FileKind::Socket => {
            host_root.bind_unix_socket(destination)?;
        }
        _ => return Err(MaterializeError::UnsupportedKind(kind)),
    }
    Ok(())
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

#[cfg(test)]
mod restore_recovery_tests {
    use super::*;

    #[tokio::test]
    async fn native_materialization_rejects_unrepresentable_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::MetadataField;

        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("file"), b"data")?;
        let root = Arc::new(HostRoot::open(temporary.path())?);
        #[cfg(unix)]
        for (metadata, expected) in [
            (
                FileMetadata {
                    posix_flags: MetadataField::Value(1),
                    ..FileMetadata::default()
                },
                "posix_flags",
            ),
            (
                FileMetadata {
                    changed_ns: MetadataField::Value(123),
                    ..FileMetadata::default()
                },
                "changed_ns without native-view baseline",
            ),
        ] {
            assert!(matches!(
                apply_host_metadata_offloaded(&root, Path::new("file"), metadata).await,
                Err(MaterializeError::UnsupportedMetadata(field)) if field == expected
            ));
        }
        #[cfg(windows)]
        let metadata = FileMetadata {
            posix_mode: MetadataField::Value(0o644),
            ..FileMetadata::default()
        };
        #[cfg(windows)]
        assert!(matches!(
            apply_host_metadata_offloaded(&root, Path::new("file"), metadata).await,
            Err(MaterializeError::UnsupportedMetadata(_))
        ));
        Ok(())
    }

    #[test]
    fn same_path_restores_are_serialized_by_a_process_crash_safe_lock()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("root");
        std::fs::create_dir(&root)?;
        let relative = Path::new("entry");
        let destination = root.join(relative);
        let first = acquire_restore_lock(relative, &destination, true)?;

        assert!(acquire_restore_lock(relative, &destination, false).is_err());
        drop(first);
        let _next = acquire_restore_lock(relative, &destination, false)?;
        Ok(())
    }

    #[test]
    fn live_mount_recovery_restores_a_displaced_entry() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("root");
        std::fs::create_dir(&root)?;
        let relative = Path::new("entry");
        let destination = root.join(relative);
        std::fs::write(&destination, b"before")?;
        let parent = held_parent(&root, relative)?;
        let backup = restore_artifact_name(".acyclic-restore-backup-", relative);
        let witness =
            restore_witness_path(".acyclic-restore-live-witness-", relative, &destination);
        create_restore_witness(&witness, LIVE_RESTORE_WITNESS)?;
        parent.rename_to(relative, &parent, &backup)?;

        recover_live_mount_replacement(&parent, relative, relative, &destination)?;

        assert_eq!(std::fs::read(&destination)?, b"before");
        assert!(!root.join(backup).exists());
        Ok(())
    }

    #[test]
    fn live_mount_recovery_keeps_a_published_entry() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("root");
        std::fs::create_dir(&root)?;
        let relative = Path::new("entry");
        let destination = root.join(relative);
        std::fs::write(&destination, b"after")?;
        let backup = restore_artifact_name(".acyclic-restore-backup-", relative);
        let witness =
            restore_witness_path(".acyclic-restore-live-witness-", relative, &destination);
        create_restore_witness(&witness, LIVE_RESTORE_WITNESS)?;
        std::fs::write(root.join(&backup), b"before")?;
        let parent = held_parent(&root, relative)?;

        recover_live_mount_replacement(&parent, relative, relative, &destination)?;

        assert_eq!(std::fs::read(&destination)?, b"after");
        assert!(!root.join(backup).exists());
        Ok(())
    }

    #[test]
    fn live_mount_recovery_preserves_an_unowned_backup_collision()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("root");
        std::fs::create_dir(&root)?;
        let relative = Path::new("entry");
        let destination = root.join(relative);
        std::fs::write(&destination, b"live")?;
        let backup = restore_artifact_name(".acyclic-restore-backup-", relative);
        std::fs::write(root.join(&backup), b"unrelated")?;
        let parent = held_parent(&root, relative)?;

        assert!(recover_live_mount_replacement(&parent, relative, relative, &destination).is_err());
        assert_eq!(std::fs::read(&destination)?, b"live");
        assert_eq!(std::fs::read(root.join(backup))?, b"unrelated");
        Ok(())
    }

    #[test]
    fn absent_restore_moves_then_reclaims_the_old_entry() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("root");
        std::fs::create_dir(&root)?;
        let relative = Path::new("entry");
        std::fs::write(root.join(relative), b"old")?;
        let parent = held_parent(&root, relative)?;

        remove_restored_path(&parent, relative, &root, relative)?;

        assert!(!root.join(relative).exists());
        assert!(
            !root
                .join(restore_artifact_name(".acyclic-restore-removed-", relative))
                .exists()
        );
        Ok(())
    }
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
