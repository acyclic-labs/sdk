//! Bounded host-state capture into one atomic sparse checkout transaction.

use crate::kernel::{
    FileKind, FileMetadata, FileRecord, LogicalName, MetadataField, NameEncoding, NamespacePath,
};
use crate::model::FilesystemProfile;
use crate::native_host::{HostDataRange, HostRoot, allocated_data_ranges};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, CancellationToken, Checkout,
    NativeRootIdentity, OperationFailure, OperationReceipt, StagedContent, WatchBatch, WatchChange,
    WatchEpoch, WatchInvalidationReason, WatchSequence, WorkBudget, WorkCounters, WorkError,
};

/// Returns the stable identity of a no-follow, capability-held capture root.
///
/// # Errors
///
/// Rejects symlink/reparse roots, non-directories, and platforms unable to
/// report a stable root identity.
pub fn capture_root_identity(path: &Path) -> Result<NativeRootIdentity, CaptureError> {
    HostRoot::open(path)
        .map(|root| root.identity())
        .map_err(|_| CaptureError::InvalidOptions)
}
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Exact native capture options.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureOptions {
    /// Materialized checkout root owned by the caller.
    pub source_root: PathBuf,
    /// Stable identity that the capability actually used for capture must own.
    pub expected_root_identity: NativeRootIdentity,
    /// Maximum changed paths admitted in one atomic transaction.
    pub maximum_paths: u32,
    /// Maximum physically allocated host ranges admitted per regular file.
    pub maximum_extent_spans: u32,
}

/// Successful authored host-state capture.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CaptureReceipt {
    /// Exact paths examined from the caller's bounded set.
    pub examined_paths: u64,
    /// Paths whose final host state required a checkout mutation.
    pub changed_paths: u64,
    /// Logical regular-file bytes streamed into immutable objects.
    pub staged_file_bytes: u64,
    /// Complete canonical-engine and source movement work.
    pub work: WorkCounters,
}

/// A watcher interval acknowledged only after its complete capture transaction
/// changes the checkout candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WatchCaptureReceipt {
    /// Watcher epoch against which every path was interpreted.
    pub epoch: WatchEpoch,
    /// First process-local sequence represented by the batch.
    pub first_sequence: WatchSequence,
    /// Sequence safe to request after this transaction succeeds.
    pub next_sequence: WatchSequence,
    /// Exact capture and movement facts.
    pub capture: CaptureReceipt,
}

/// Fail-closed native capture errors.
#[derive(Debug, Error)]
pub enum CaptureError {
    /// Capture bounds or source root are invalid.
    #[error("native capture options are invalid")]
    InvalidOptions,
    /// The exact capability opened for capture is not the admitted root.
    #[error("native capture root identity changed")]
    RootChanged,
    /// Watcher hints are invalid until the caller completes a fresh baseline.
    #[error("watcher epoch {epoch} requires a new baseline: {reason}")]
    RescanRequired {
        /// Invalidated watcher epoch.
        epoch: u64,
        /// Exact invalidation reason.
        reason: WatchInvalidationReason,
    },
    /// A complete baseline cannot replace an existing private mutation set.
    #[error("baseline capture requires a clean checkout")]
    DirtyCheckout,
    /// One requested path cannot be represented exactly on this host.
    #[error("native capture path is not exactly representable")]
    UnrepresentablePath,
    /// The host kind cannot be represented exactly by this volume adapter.
    #[error("host file kind is unsupported for exact capture")]
    UnsupportedKind,
    /// Canonical filesystem operation failed.
    #[error("filesystem engine failed: {0}")]
    Engine(String),
    /// Source filesystem operation failed.
    #[error("source filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
    /// Exact work overflowed or exceeded the admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

/// Reads a bounded set of final host states and applies one atomic checkout
/// transaction.
///
/// Regular files stream directly into immutable chunks; their complete bodies
/// are never retained in memory. Missing host paths become removals only when
/// the checkout currently contains the path. Existing paths are replaced only
/// when kind changes; same-kind regular files preserve stable file identity.
/// Watch notifications are suitable inputs because they are treated only as
/// hints selecting which host paths to authenticate and capture.
///
/// # Errors
///
/// Rejects zero/excessive bounds, source-root escape, unsupported host kinds,
/// source races or I/O, canonical engine failures, cancellation, and work
/// exhaustion. No checkout candidate changes unless the complete transaction
/// succeeds.
pub async fn capture_paths<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    options: &CaptureOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    validate_path_count(paths, options).map_err(OperationFailure::before_work)?;
    let source_root = open_source_root(options).map_err(OperationFailure::before_work)?;
    capture_paths_from_root(
        checkout,
        paths,
        options.maximum_extent_spans,
        &source_root,
        budget,
        cancellation,
    )
    .await
}

async fn capture_paths_from_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let mut ordered = paths.to_vec();
    ordered.sort_by(|left, right| {
        let left_present =
            relative_host_path(left).is_ok_and(|path| source_root.symlink_metadata(&path).is_ok());
        let right_present =
            relative_host_path(right).is_ok_and(|path| source_root.symlink_metadata(&path).is_ok());
        match (left_present, right_present) {
            (true, true) => left
                .depth()
                .cmp(&right.depth())
                .then_with(|| left.cmp(right)),
            (false, false) => right
                .depth()
                .cmp(&left.depth())
                .then_with(|| left.cmp(right)),
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
        }
    });
    ordered.dedup();
    if ordered.len() != paths.len() {
        return Err(OperationFailure::before_work(CaptureError::InvalidOptions));
    }
    let mut receipt = CaptureReceipt::default();
    let mut mutations = Vec::new();
    mutations
        .try_reserve(paths.len().saturating_mul(3))
        .map_err(|_| OperationFailure::before_work(CaptureError::InvalidOptions))?;

    for path in ordered {
        capture_final_path(
            checkout,
            path,
            CurrentRecord::Lookup,
            CaptureIntent::Complete,
            maximum_extent_spans,
            source_root,
            &mut mutations,
            &mut receipt,
            budget,
            cancellation,
        )
        .await?;
    }

    apply_capture_transaction(checkout, mutations, &mut receipt, budget, cancellation).await?;
    receipt
        .work
        .verify(budget)
        .map_err(|error| OperationFailure::new(CaptureError::Work(error), receipt.work))?;
    Ok(OperationReceipt {
        value: receipt,
        work: receipt.work,
    })
}

/// Authenticates one complete bounded host baseline into a checkout candidate.
///
/// The scan takes the exact union of host and checkout paths, so host additions,
/// changes, and deletions become one authored transaction. Directories are
/// admitted before descendants and removals are applied deepest-first. Native
/// links are never followed. Callers bracket this operation with
/// [`crate::NativeWatch::begin_rescan`] and `finish_rescan`; events arriving
/// during the scan then form the next exact delta interval.
///
/// # Errors
///
/// Rejects an unrepresentable host name, path/count/work bound, source race or
/// I/O error, unsupported exact file kind, cancellation, or canonical-engine
/// failure. The checkout remains unchanged unless the complete baseline
/// transaction succeeds.
pub async fn capture_baseline<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    options: &CaptureOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    if checkout.has_pending_mutations() {
        return Err(OperationFailure::before_work(CaptureError::DirtyCheckout));
    }
    if options.maximum_paths == 0 || options.maximum_extent_spans == 0 {
        return Err(OperationFailure::before_work(CaptureError::InvalidOptions));
    }
    let source_root = open_source_root(options).map_err(OperationFailure::before_work)?;
    let limits = checkout.volume_config().limits;
    let profile = checkout.volume_config().profile;
    let maximum = usize::try_from(options.maximum_paths)
        .map_err(|_| OperationFailure::before_work(CaptureError::InvalidOptions))?;
    let mut paths = BTreeSet::new();
    let mut work = WorkCounters::default();
    collect_host_paths(
        &source_root,
        profile,
        limits,
        maximum,
        &mut paths,
        &mut work,
        budget,
        cancellation,
    )?;
    Box::pin(collect_checkout_paths(
        checkout,
        limits,
        maximum,
        &mut paths,
        &mut work,
        budget,
        cancellation,
    ))
    .await?;
    let paths = paths.into_iter().collect::<Vec<_>>();
    let remaining = work
        .remaining(budget)
        .map_err(|error| OperationFailure::new(CaptureError::Work(error), work))?;
    let mut captured = Box::pin(capture_paths_from_root(
        checkout,
        &paths,
        options.maximum_extent_spans,
        &source_root,
        remaining,
        cancellation,
    ))
    .await
    .map_err(|failure| map_capture_failure(failure, work))?;
    let combined = add_work(work, captured.work)?;
    captured.value.work = combined;
    Ok(OperationReceipt {
        value: captured.value,
        work: combined,
    })
}

#[allow(clippy::too_many_arguments)]
fn collect_host_paths(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    paths: &mut BTreeSet<NamespacePath>,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), OperationFailure<CaptureError>> {
    let volume_root = NamespacePath::new(Vec::new(), limits)
        .map_err(|error| OperationFailure::before_work(CaptureError::Engine(error.to_string())))?;
    collect_host_subtree_paths(
        root,
        profile,
        limits,
        maximum,
        PathBuf::new(),
        volume_root,
        paths,
        work,
        budget,
        cancellation,
    )
}

#[allow(clippy::too_many_arguments)]
fn collect_host_subtree_paths(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    host_root: PathBuf,
    volume_root: NamespacePath,
    paths: &mut BTreeSet<NamespacePath>,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), OperationFailure<CaptureError>> {
    let mut pending = vec![(host_root, volume_root)];
    while let Some((host_parent, volume_parent)) = pending.pop() {
        cancellation.check().map_err(|error| {
            OperationFailure::new(CaptureError::Engine(error.to_string()), *work)
        })?;
        let entries = root
            .read_dir(&host_parent)
            .map_err(|error| OperationFailure::new(error.into(), *work))?;
        for entry in entries {
            let entry = entry.map_err(|error| OperationFailure::new(error.into(), *work))?;
            let name = logical_host_name(&entry.file_name(), profile, limits)
                .map_err(|error| OperationFailure::new(error, *work))?;
            let child = append_path(&volume_parent, name, limits)
                .map_err(|error| OperationFailure::new(error, *work))?;
            insert_scanned_path(paths, child.clone(), maximum, work, budget)?;
            let host_child = host_parent.join(entry.file_name());
            let metadata = root
                .symlink_metadata(&host_child)
                .map_err(|error| OperationFailure::new(error.into(), *work))?;
            if metadata.is_dir() && !metadata.file_type().is_symlink() {
                pending.push((host_child, child));
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn collect_checkout_paths<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    paths: &mut BTreeSet<NamespacePath>,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), OperationFailure<CaptureError>> {
    let root = NamespacePath::new(Vec::new(), limits)
        .map_err(|error| OperationFailure::before_work(CaptureError::Engine(error.to_string())))?;
    collect_checkout_subtree_paths(
        checkout,
        limits,
        maximum,
        root,
        paths,
        work,
        budget,
        cancellation,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn collect_checkout_subtree_paths<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    root: NamespacePath,
    paths: &mut BTreeSet<NamespacePath>,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), OperationFailure<CaptureError>> {
    const PAGE_ENTRIES: u32 = 1024;
    let mut pending = vec![root];
    while let Some(directory) = pending.pop() {
        let mut after = None;
        loop {
            let remaining = work
                .remaining(budget)
                .map_err(|error| OperationFailure::new(CaptureError::Work(error), *work))?;
            let page = checkout
                .list_directory_records(
                    &directory,
                    after.as_ref(),
                    PAGE_ENTRIES,
                    remaining,
                    cancellation,
                )
                .await
                .map_err(|failure| map_engine_failure(failure, *work))?;
            *work = add_work(*work, page.work)?;
            if page.value.has_more && page.value.entries.is_empty() {
                return Err(OperationFailure::new(
                    CaptureError::Engine("directory pagination made no progress".to_owned()),
                    *work,
                ));
            }
            for entry in &page.value.entries {
                let child = append_path(&directory, entry.name.clone(), limits)
                    .map_err(|error| OperationFailure::new(error, *work))?;
                insert_scanned_path(paths, child.clone(), maximum, work, budget)?;
                if entry.record.kind == FileKind::Directory {
                    pending.push(child);
                }
            }
            after = page.value.entries.last().map(|entry| entry.name.clone());
            if !page.value.has_more {
                break;
            }
        }
    }
    Ok(())
}

fn insert_scanned_path(
    paths: &mut BTreeSet<NamespacePath>,
    path: NamespacePath,
    maximum: usize,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), OperationFailure<CaptureError>> {
    let encoded_bytes = u64::from(path.encoded_bytes());
    if paths.insert(path) {
        if paths.len() > maximum {
            return Err(OperationFailure::new(CaptureError::InvalidOptions, *work));
        }
        *work = add_work(
            *work,
            WorkCounters {
                bytes_copied: encoded_bytes,
                items_examined: 1,
                allocation_operations: 1,
                peak_allocation_bytes: encoded_bytes,
                ..WorkCounters::default()
            },
        )?;
        work.verify(budget)
            .map_err(|error| OperationFailure::new(CaptureError::Work(error), *work))?;
    }
    Ok(())
}

fn append_path(
    parent: &NamespacePath,
    name: LogicalName,
    limits: crate::model::VolumeLimits,
) -> Result<NamespacePath, CaptureError> {
    let mut components = parent.components().to_vec();
    components.push(name);
    NamespacePath::new(components, limits).map_err(|error| CaptureError::Engine(error.to_string()))
}

fn logical_host_name(
    name: &std::ffi::OsStr,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
) -> Result<LogicalName, CaptureError> {
    let (encoding, bytes) = host_name_bytes(name, profile)?;
    LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
        .map_err(|error| CaptureError::Engine(error.to_string()))
}

#[cfg(unix)]
fn host_name_bytes(
    name: &std::ffi::OsStr,
    profile: FilesystemProfile,
) -> Result<(NameEncoding, Vec<u8>), CaptureError> {
    use std::os::unix::ffi::OsStrExt;
    let raw = name.as_bytes();
    match profile {
        FilesystemProfile::Posix => Ok((NameEncoding::PosixBytes, raw.to_vec())),
        FilesystemProfile::Windows => {
            let text = std::str::from_utf8(raw).map_err(|_| CaptureError::UnrepresentablePath)?;
            Ok((
                NameEncoding::WindowsUtf16Le,
                text.encode_utf16().flat_map(u16::to_le_bytes).collect(),
            ))
        }
        FilesystemProfile::Portable | FilesystemProfile::Browser => {
            std::str::from_utf8(raw).map_err(|_| CaptureError::UnrepresentablePath)?;
            Ok((NameEncoding::Utf8, raw.to_vec()))
        }
    }
}

#[cfg(windows)]
fn host_name_bytes(
    name: &std::ffi::OsStr,
    profile: FilesystemProfile,
) -> Result<(NameEncoding, Vec<u8>), CaptureError> {
    use std::os::windows::ffi::OsStrExt;
    match profile {
        FilesystemProfile::Windows => Ok((
            NameEncoding::WindowsUtf16Le,
            name.encode_wide().flat_map(u16::to_le_bytes).collect(),
        )),
        FilesystemProfile::Posix => Ok((
            NameEncoding::PosixBytes,
            name.to_str()
                .ok_or(CaptureError::UnrepresentablePath)?
                .as_bytes()
                .to_vec(),
        )),
        FilesystemProfile::Portable | FilesystemProfile::Browser => Ok((
            NameEncoding::Utf8,
            name.to_str()
                .ok_or(CaptureError::UnrepresentablePath)?
                .as_bytes()
                .to_vec(),
        )),
    }
}

#[cfg(not(any(unix, windows)))]
fn host_name_bytes(
    name: &std::ffi::OsStr,
    _profile: FilesystemProfile,
) -> Result<(NameEncoding, Vec<u8>), CaptureError> {
    Ok((
        NameEncoding::Utf8,
        name.to_str()
            .ok_or(CaptureError::UnrepresentablePath)?
            .as_bytes()
            .to_vec(),
    ))
}

/// Atomically captures one contiguous native-watcher batch.
///
/// Paired renames are replayed in watcher order and preserve the exact
/// path-independent file identity, including rename chains. Final destination
/// state is then read from the host and appended to the same authored
/// transaction. The returned `next_sequence` is therefore safe to acknowledge
/// only after this function succeeds.
///
/// # Errors
///
/// Returns [`CaptureError::RescanRequired`] without touching the checkout for
/// an invalidated watcher epoch. Other failures are the union of exact rename,
/// host-state capture, cancellation, engine, allocation, and bounded-work
/// failures from [`capture_paths`].
#[allow(clippy::too_many_lines)]
pub async fn capture_watch_batch<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    batch: WatchBatch,
    options: &CaptureOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<WatchCaptureReceipt>, OperationFailure<CaptureError>> {
    let WatchBatch::Changes {
        epoch,
        first_sequence,
        next_sequence,
        changes,
    } = batch
    else {
        let WatchBatch::RescanRequired { epoch, reason } = batch else {
            unreachable!("watch batch variants are exhaustive")
        };
        return Err(OperationFailure::before_work(
            CaptureError::RescanRequired {
                epoch: epoch.get(),
                reason,
            },
        ));
    };
    if options.maximum_paths == 0
        || options.maximum_extent_spans == 0
        || changes.len() > usize::try_from(options.maximum_paths).unwrap_or(usize::MAX)
    {
        return Err(OperationFailure::before_work(CaptureError::InvalidOptions));
    }
    let source_root = open_source_root(options).map_err(OperationFailure::before_work)?;

    let mut receipt = CaptureReceipt::default();
    let mut mutations = Vec::new();
    mutations
        .try_reserve(changes.len().saturating_mul(4))
        .map_err(|_| OperationFailure::before_work(CaptureError::InvalidOptions))?;
    let mut ordinary = BTreeMap::new();
    let mut rename_records = BTreeMap::<NamespacePath, FileRecord>::new();
    let mut moved_away = BTreeSet::new();

    for change in changes {
        match change {
            WatchChange::Created(path) => {
                let current = rename_records.get(&path).copied().map_or_else(
                    || {
                        if moved_away.contains(&path) {
                            CurrentRecord::Known(None)
                        } else {
                            CurrentRecord::Lookup
                        }
                    },
                    |record| CurrentRecord::Known(Some(record)),
                );
                ordinary.insert(path, (current, CaptureIntent::Replace));
            }
            WatchChange::Modified(path) | WatchChange::Removed(path) => {
                let current = rename_records.get(&path).copied().map_or_else(
                    || {
                        if moved_away.contains(&path) {
                            CurrentRecord::Known(None)
                        } else {
                            CurrentRecord::Lookup
                        }
                    },
                    |record| CurrentRecord::Known(Some(record)),
                );
                ordinary
                    .entry(path)
                    .or_insert((current, CaptureIntent::Complete));
            }
            WatchChange::MetadataChanged(path) => {
                let current = rename_records.get(&path).copied().map_or_else(
                    || {
                        if moved_away.contains(&path) {
                            CurrentRecord::Known(None)
                        } else {
                            CurrentRecord::Lookup
                        }
                    },
                    |record| CurrentRecord::Known(Some(record)),
                );
                ordinary
                    .entry(path)
                    .or_insert((current, CaptureIntent::MetadataOnly));
            }
            WatchChange::Renamed { from, to } => {
                cancellation.check().map_err(|error| {
                    OperationFailure::new(CaptureError::Engine(error.to_string()), receipt.work)
                })?;
                let prior_source = ordinary.remove(&from);
                ordinary.remove(&to);
                let record = if let Some(record) = rename_records.remove(&from) {
                    Some(record)
                } else if matches!(prior_source, Some((CurrentRecord::Known(None), _)))
                    || moved_away.contains(&from)
                {
                    None
                } else {
                    let remaining = receipt.work.remaining(budget).map_err(|error| {
                        OperationFailure::new(CaptureError::Work(error), receipt.work)
                    })?;
                    let source = checkout
                        .lookup_no_follow(&from, remaining, cancellation)
                        .await
                        .map_err(|failure| map_engine_failure(failure, receipt.work))?;
                    receipt.work = add_work(receipt.work, source.work)?;
                    match source.value.record {
                        Some(record) => Some(record),
                        None if prior_source.is_some() => None,
                        None => None,
                    }
                };
                if let Some(record) = record {
                    mutations.push(AuthoredMutation::Rename {
                        source: from.clone(),
                        destination: to.clone(),
                        replace: true,
                    });
                    rename_records.insert(to.clone(), record);
                } else {
                    ordinary.insert(to.clone(), (CurrentRecord::Lookup, CaptureIntent::Replace));
                }
                moved_away.insert(from);
                moved_away.remove(&to);
            }
        }
    }

    Box::pin(expand_directory_hints(
        checkout,
        &mut ordinary,
        &source_root,
        options.maximum_paths,
        &mut receipt,
        budget,
        cancellation,
    ))
    .await?;

    for (path, record) in rename_records {
        if ordinary.contains_key(&path) {
            continue;
        }
        capture_final_path(
            checkout,
            path,
            CurrentRecord::Known(Some(record)),
            CaptureIntent::Complete,
            options.maximum_extent_spans,
            &source_root,
            &mut mutations,
            &mut receipt,
            budget,
            cancellation,
        )
        .await?;
    }
    let mut ordinary = ordinary
        .into_iter()
        .map(|(path, (current, intent))| {
            let host_path = relative_host_path(&path)?;
            let exists = match source_root.symlink_metadata(&host_path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Ok(_) | Err(_) => true,
            };
            Ok((path, current, intent, exists))
        })
        .collect::<Result<Vec<_>, CaptureError>>()
        .map_err(|error| OperationFailure::new(error, receipt.work))?;
    ordinary.sort_by(|left, right| match (left.3, right.3) {
        (true, true) => left
            .0
            .depth()
            .cmp(&right.0.depth())
            .then_with(|| left.0.cmp(&right.0)),
        (false, false) => right
            .0
            .depth()
            .cmp(&left.0.depth())
            .then_with(|| left.0.cmp(&right.0)),
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
    });
    for (path, current, intent, _exists) in ordinary {
        capture_final_path(
            checkout,
            path,
            current,
            intent,
            options.maximum_extent_spans,
            &source_root,
            &mut mutations,
            &mut receipt,
            budget,
            cancellation,
        )
        .await?;
    }
    apply_capture_transaction(checkout, mutations, &mut receipt, budget, cancellation).await?;
    Ok(OperationReceipt {
        value: WatchCaptureReceipt {
            epoch,
            first_sequence,
            next_sequence,
            capture: receipt,
        },
        work: receipt.work,
    })
}

#[derive(Clone, Copy)]
enum CurrentRecord {
    Lookup,
    Known(Option<FileRecord>),
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CaptureIntent {
    MetadataOnly,
    Complete,
    Replace,
}

#[allow(clippy::too_many_arguments)]
async fn expand_directory_hints<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    ordinary: &mut BTreeMap<NamespacePath, (CurrentRecord, CaptureIntent)>,
    source_root: &HostRoot,
    maximum_paths: u32,
    receipt: &mut CaptureReceipt,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), OperationFailure<CaptureError>> {
    let maximum = usize::try_from(maximum_paths)
        .map_err(|_| OperationFailure::new(CaptureError::InvalidOptions, receipt.work))?;
    if ordinary.len() > maximum {
        return Err(OperationFailure::new(
            CaptureError::InvalidOptions,
            receipt.work,
        ));
    }
    let limits = checkout.volume_config().limits;
    let profile = checkout.volume_config().profile;
    let roots = ordinary
        .keys()
        .filter_map(|path| {
            let host_path = relative_host_path(path).ok()?;
            source_root
                .symlink_metadata(&host_path)
                .ok()
                .filter(cap_std::fs::Metadata::is_dir)
                .map(|_| (host_path, path.clone()))
        })
        .collect::<Vec<_>>();
    let mut paths = ordinary.keys().cloned().collect::<BTreeSet<_>>();
    for (host_path, volume_path) in roots {
        collect_host_subtree_paths(
            source_root,
            profile,
            limits,
            maximum,
            host_path,
            volume_path.clone(),
            &mut paths,
            &mut receipt.work,
            budget,
            cancellation,
        )?;
        let remaining = receipt
            .work
            .remaining(budget)
            .map_err(|error| OperationFailure::new(CaptureError::Work(error), receipt.work))?;
        let lookup = checkout
            .lookup_no_follow(&volume_path, remaining, cancellation)
            .await
            .map_err(|failure| map_engine_failure(failure, receipt.work))?;
        receipt.work = add_work(receipt.work, lookup.work)?;
        if lookup
            .value
            .record
            .is_some_and(|record| record.kind == FileKind::Directory)
        {
            collect_checkout_subtree_paths(
                checkout,
                limits,
                maximum,
                volume_path,
                &mut paths,
                &mut receipt.work,
                budget,
                cancellation,
            )
            .await?;
        }
    }
    for path in paths {
        ordinary
            .entry(path)
            .or_insert((CurrentRecord::Lookup, CaptureIntent::Complete));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn capture_final_path<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    path: NamespacePath,
    current: CurrentRecord,
    intent: CaptureIntent,
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), OperationFailure<CaptureError>> {
    cancellation.check().map_err(|error| {
        OperationFailure::new(CaptureError::Engine(error.to_string()), receipt.work)
    })?;
    let host_path =
        relative_host_path(&path).map_err(|error| OperationFailure::new(error, receipt.work))?;
    let current = match current {
        CurrentRecord::Known(record) => record,
        CurrentRecord::Lookup => {
            let remaining = receipt
                .work
                .remaining(budget)
                .map_err(|error| OperationFailure::new(CaptureError::Work(error), receipt.work))?;
            let lookup = checkout
                .lookup_no_follow(&path, remaining, cancellation)
                .await
                .map_err(|failure| map_engine_failure(failure, receipt.work))?;
            receipt.work = add_work(receipt.work, lookup.work)?;
            lookup.value.record
        }
    };
    match source_root.symlink_metadata(&host_path) {
        Ok(metadata) => {
            let host_kind = host_kind(&metadata)?;
            // A created hint replaces same-kind regular files with a fresh
            // identity, but a directory that is still a directory keeps its
            // identity: its replacement cannot be expressed as one remove
            // (the directory may be non-empty), its children are captured
            // through their own paths, and native watchers (FSEvents) may
            // replay creation hints for paths that already exist.
            let replace = intent == CaptureIntent::Replace
                && !(host_kind == FileKind::Directory
                    && current.is_some_and(|record| record.kind == FileKind::Directory));
            if let Some(record) = current
                && (record.kind != host_kind || replace)
            {
                mutations.push(AuthoredMutation::Remove {
                    path: path.clone(),
                    expected_file_id: Some(record.file_id),
                });
            }
            let exists_with_kind =
                !replace && current.is_some_and(|record| record.kind == host_kind);
            let mut canonical_metadata = if host_kind == FileKind::SymbolicLink {
                unrestorable_metadata()
            } else {
                capture_metadata(&metadata)
            };
            if let Some(record) = current.filter(|record| {
                exists_with_kind && record.kind == host_kind && host_kind != FileKind::SymbolicLink
            }) {
                let remaining = receipt.work.remaining(budget).map_err(|error| {
                    OperationFailure::new(CaptureError::Work(error), receipt.work)
                })?;
                let prior = checkout
                    .read_metadata_by_id(record.file_id, remaining, cancellation)
                    .await
                    .map_err(|failure| map_engine_failure(failure, receipt.work))?;
                receipt.work = add_work(receipt.work, prior.work)?;
                canonical_metadata = preserve_unobserved_metadata(canonical_metadata, prior.value);
            }
            if intent == CaptureIntent::MetadataOnly && exists_with_kind {
                mutations.push(AuthoredMutation::SetMetadata {
                    path,
                    metadata: canonical_metadata,
                });
            } else {
                append_final_state(
                    checkout,
                    path,
                    maximum_extent_spans,
                    source_root,
                    host_path,
                    metadata,
                    host_kind,
                    exists_with_kind,
                    canonical_metadata,
                    mutations,
                    receipt,
                    budget,
                    cancellation,
                )
                .await?;
            }
            receipt.changed_paths = checked_increment(receipt.changed_paths, receipt.work)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(record) = current {
                mutations.push(AuthoredMutation::Remove {
                    path,
                    expected_file_id: Some(record.file_id),
                });
                receipt.changed_paths = checked_increment(receipt.changed_paths, receipt.work)?;
            }
        }
        Err(error) => return Err(OperationFailure::new(error.into(), receipt.work)),
    }
    receipt.examined_paths = checked_increment(receipt.examined_paths, receipt.work)?;
    Ok(())
}

async fn apply_capture_transaction<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    mutations: Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), OperationFailure<CaptureError>> {
    if mutations.is_empty() {
        return Ok(());
    }
    let remaining = receipt
        .work
        .remaining(budget)
        .map_err(|error| OperationFailure::new(CaptureError::Work(error), receipt.work))?;
    let applied = checkout
        .apply_authored_transaction(mutations, remaining, cancellation)
        .await
        .map_err(|failure| map_engine_failure(failure, receipt.work))?;
    receipt.work = add_work(receipt.work, applied.work)?;
    Ok(())
}

fn checked_increment(
    value: u64,
    work: WorkCounters,
) -> Result<u64, OperationFailure<CaptureError>> {
    value
        .checked_add(1)
        .ok_or_else(|| OperationFailure::new(CaptureError::Work(WorkError::Overflow), work))
}

#[allow(clippy::too_many_arguments)]
async fn append_final_state<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &Checkout<A, O>,
    path: NamespacePath,
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    host_path: PathBuf,
    metadata: cap_std::fs::Metadata,
    kind: FileKind,
    exists_with_kind: bool,
    canonical_metadata: FileMetadata,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), OperationFailure<CaptureError>> {
    match kind {
        FileKind::Regular => {
            append_regular_state(
                checkout,
                path,
                maximum_extent_spans,
                source_root,
                &host_path,
                exists_with_kind,
                canonical_metadata,
                mutations,
                receipt,
                budget,
                cancellation,
            )
            .await?;
        }
        FileKind::Directory => {
            if exists_with_kind {
                mutations.push(AuthoredMutation::SetMetadata {
                    path,
                    metadata: canonical_metadata,
                });
            } else {
                mutations.push(AuthoredMutation::CreateDirectory {
                    path,
                    metadata: canonical_metadata,
                });
            }
        }
        FileKind::SymbolicLink => {
            let target = read_link_bytes(source_root, &host_path)
                .map_err(|error| OperationFailure::new(error, receipt.work))?;
            if exists_with_kind {
                mutations.push(AuthoredMutation::Remove {
                    path: path.clone(),
                    expected_file_id: None,
                });
            }
            mutations.push(AuthoredMutation::CreateSymbolicLink {
                path,
                target: target.into(),
                metadata: canonical_metadata,
            });
        }
        FileKind::Fifo | FileKind::Socket | FileKind::CharacterDevice | FileKind::BlockDevice => {
            append_special_state(
                path,
                &metadata,
                kind,
                exists_with_kind,
                canonical_metadata,
                mutations,
                receipt.work,
            )?;
        }
        FileKind::ReparsePoint | FileKind::MountBoundary => {
            return Err(OperationFailure::new(
                CaptureError::UnsupportedKind,
                receipt.work,
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn append_regular_state<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &Checkout<A, O>,
    path: NamespacePath,
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    host_path: &Path,
    exists_with_kind: bool,
    canonical_metadata: FileMetadata,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), OperationFailure<CaptureError>> {
    let mut file = source_root
        .open_file(host_path)
        .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
    let metadata = file
        .metadata()
        .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
    if !metadata.is_file() {
        return Err(OperationFailure::new(
            CaptureError::Io(std::io::Error::other(
                "host file kind changed during capture",
            )),
            receipt.work,
        ));
    }
    let logical_bytes = metadata.len();
    let ranges = allocated_data_ranges(&file, logical_bytes, maximum_extent_spans)
        .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
    let staged =
        stage_host_ranges(checkout, &mut file, &ranges, receipt, budget, cancellation).await?;
    if exists_with_kind {
        mutations.push(AuthoredMutation::Resize {
            path: path.clone(),
            logical_bytes: 0,
        });
    } else {
        mutations.push(AuthoredMutation::CreateFile {
            path: path.clone(),
            bytes: bytes::Bytes::new(),
            metadata: canonical_metadata,
        });
    }
    if logical_bytes != 0 {
        mutations.push(AuthoredMutation::Resize {
            path: path.clone(),
            logical_bytes,
        });
    }
    mutations.extend(staged.into_iter().map(|(range, content)| {
        AuthoredMutation::WriteFromContent {
            path: path.clone(),
            offset: range.offset,
            content,
        }
    }));
    mutations.push(AuthoredMutation::SetMetadata {
        path,
        metadata: canonical_metadata,
    });
    Ok(())
}

async fn stage_host_ranges<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &Checkout<A, O>,
    file: &mut cap_std::fs::File,
    ranges: &[HostDataRange],
    receipt: &mut CaptureReceipt,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<Vec<(HostDataRange, StagedContent)>, OperationFailure<CaptureError>> {
    let mut staged = Vec::new();
    staged
        .try_reserve(ranges.len())
        .map_err(|_| OperationFailure::new(CaptureError::InvalidOptions, receipt.work))?;
    for range in ranges {
        file.seek(SeekFrom::Start(range.offset))
            .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
        let mut bounded = (&mut *file).take(range.length);
        let remaining = receipt
            .work
            .remaining(budget)
            .map_err(|error| OperationFailure::new(CaptureError::Work(error), receipt.work))?;
        let content = checkout
            .stage_content(&mut bounded, range.length, remaining, cancellation)
            .await
            .map_err(|failure| map_engine_failure(failure, receipt.work))?;
        receipt.work = add_work(receipt.work, content.work)?;
        if content.value.logical_bytes() != range.length {
            return Err(OperationFailure::new(
                CaptureError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "host file changed while capturing a sparse range",
                )),
                receipt.work,
            ));
        }
        receipt.staged_file_bytes = receipt
            .staged_file_bytes
            .checked_add(range.length)
            .ok_or_else(|| {
                OperationFailure::new(CaptureError::Work(WorkError::Overflow), receipt.work)
            })?;
        staged.push((*range, content.value));
    }
    Ok(staged)
}

fn append_special_state(
    path: NamespacePath,
    metadata: &cap_std::fs::Metadata,
    kind: FileKind,
    exists_with_kind: bool,
    canonical_metadata: FileMetadata,
    mutations: &mut Vec<AuthoredMutation>,
    work: WorkCounters,
) -> Result<(), OperationFailure<CaptureError>> {
    if matches!(kind, FileKind::Fifo | FileKind::Socket) {
        mutations.push(if exists_with_kind {
            AuthoredMutation::SetMetadata {
                path,
                metadata: canonical_metadata,
            }
        } else {
            AuthoredMutation::CreateEmptySpecial {
                path,
                kind,
                metadata: canonical_metadata,
            }
        });
        return Ok(());
    }
    let (major, minor) =
        host_device_identity(metadata).map_err(|error| OperationFailure::new(error, work))?;
    if exists_with_kind {
        mutations.push(AuthoredMutation::Remove {
            path: path.clone(),
            expected_file_id: None,
        });
    }
    mutations.push(AuthoredMutation::CreateDevice {
        path,
        kind,
        major,
        minor,
        metadata: canonical_metadata,
    });
    Ok(())
}

fn validate_path_count(
    paths: &[NamespacePath],
    options: &CaptureOptions,
) -> Result<(), CaptureError> {
    if options.maximum_paths == 0
        || options.maximum_extent_spans == 0
        || paths.len() > usize::try_from(options.maximum_paths).unwrap_or(usize::MAX)
    {
        return Err(CaptureError::InvalidOptions);
    }
    Ok(())
}

fn open_source_root(options: &CaptureOptions) -> Result<HostRoot, CaptureError> {
    let root = HostRoot::open(&options.source_root).map_err(|_| CaptureError::InvalidOptions)?;
    if root.identity() != options.expected_root_identity {
        return Err(CaptureError::RootChanged);
    }
    Ok(root)
}

fn relative_host_path(path: &NamespacePath) -> Result<PathBuf, CaptureError> {
    let mut result = PathBuf::new();
    for component in path.components() {
        result.push(capture_host_name(component)?);
    }
    Ok(result)
}

/// Converts one canonical name to its exact host spelling.
///
/// Mirrors `host_name_bytes`: a Windows-profile volume is capturable on a
/// Unix host exactly when its names are UTF-8 representable, so UTF-16
/// canonical names decode back to the UTF-8 host names they were captured
/// from instead of failing as unrepresentable.
#[cfg(unix)]
fn capture_host_name(component: &LogicalName) -> Result<std::ffi::OsString, CaptureError> {
    use std::os::unix::ffi::OsStringExt;
    match component.encoding() {
        NameEncoding::Utf8 | NameEncoding::PosixBytes => {
            Ok(std::ffi::OsString::from_vec(component.as_bytes().to_vec()))
        }
        NameEncoding::WindowsUtf16Le => {
            let units = component
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
                .collect::<Vec<_>>();
            String::from_utf16(&units)
                .map(std::ffi::OsString::from)
                .map_err(|_| CaptureError::UnrepresentablePath)
        }
    }
}

#[cfg(not(unix))]
fn capture_host_name(component: &LogicalName) -> Result<std::ffi::OsString, CaptureError> {
    use std::os::windows::ffi::OsStringExt;
    match component.encoding() {
        NameEncoding::Utf8 => std::str::from_utf8(component.as_bytes())
            .map(std::ffi::OsString::from)
            .map_err(|_| CaptureError::UnrepresentablePath),
        NameEncoding::WindowsUtf16Le => {
            let units = component
                .as_bytes()
                .chunks_exact(2)
                .map(|unit| u16::from_le_bytes([unit[0], unit[1]]))
                .collect::<Vec<_>>();
            Ok(std::ffi::OsString::from_wide(&units))
        }
        NameEncoding::PosixBytes => Err(CaptureError::UnrepresentablePath),
    }
}

fn host_kind(metadata: &cap_std::fs::Metadata) -> Result<FileKind, OperationFailure<CaptureError>> {
    let kind = metadata.file_type();
    if kind.is_file() {
        return Ok(FileKind::Regular);
    }
    if kind.is_dir() {
        return Ok(FileKind::Directory);
    }
    if kind.is_symlink() {
        return Ok(FileKind::SymbolicLink);
    }
    #[cfg(unix)]
    {
        use cap_std::fs::FileTypeExt;
        if kind.is_fifo() {
            return Ok(FileKind::Fifo);
        }
        if kind.is_socket() {
            return Ok(FileKind::Socket);
        }
        if kind.is_char_device() {
            return Ok(FileKind::CharacterDevice);
        }
        if kind.is_block_device() {
            return Ok(FileKind::BlockDevice);
        }
    }
    Err(OperationFailure::before_work(CaptureError::UnsupportedKind))
}

#[cfg(unix)]
#[allow(clippy::unnecessary_wraps)]
fn host_device_identity(metadata: &cap_std::fs::Metadata) -> Result<(u32, u32), CaptureError> {
    use cap_std::fs::MetadataExt;
    Ok(split_device(metadata.rdev()))
}

#[cfg(target_os = "linux")]
fn split_device(device: u64) -> (u32, u32) {
    (libc::major(device), libc::minor(device))
}

#[cfg(target_os = "macos")]
fn split_device(device: u64) -> (u32, u32) {
    let low = u32::try_from(device & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    let native = i32::from_ne_bytes(low.to_ne_bytes());
    (
        u32::try_from(libc::major(native)).unwrap_or(u32::MAX),
        u32::try_from(libc::minor(native)).unwrap_or(u32::MAX),
    )
}

#[cfg(not(unix))]
fn host_device_identity(_: &cap_std::fs::Metadata) -> Result<(u32, u32), CaptureError> {
    Err(CaptureError::UnsupportedKind)
}

#[cfg(unix)]
fn read_link_bytes(root: &HostRoot, path: &Path) -> Result<Vec<u8>, CaptureError> {
    use std::os::unix::ffi::OsStringExt;
    Ok(root.read_link(path)?.into_os_string().into_vec())
}

#[cfg(windows)]
fn read_link_bytes(root: &HostRoot, path: &Path) -> Result<Vec<u8>, CaptureError> {
    use std::os::windows::ffi::OsStrExt;
    Ok(root
        .read_link(path)?
        .as_os_str()
        .encode_wide()
        .flat_map(u16::to_le_bytes)
        .collect())
}

/// Symbolic links own no restorable host metadata: mode, ownership, and
/// timestamps cannot be applied to a link during materialization, and
/// recording them makes every captured link fail restore fail-closed.
fn unrestorable_metadata() -> FileMetadata {
    FileMetadata {
        posix_mode: MetadataField::Unavailable,
        posix_uid: MetadataField::Unavailable,
        posix_gid: MetadataField::Unavailable,
        posix_flags: MetadataField::Unavailable,
        windows_attributes: MetadataField::Unavailable,
        created_ns: MetadataField::Unavailable,
        modified_ns: MetadataField::Unavailable,
        accessed_ns: MetadataField::Unavailable,
        changed_ns: MetadataField::Unavailable,
        named_attributes: MetadataField::Unavailable,
        acl: MetadataField::Unavailable,
        security_descriptor: MetadataField::Unavailable,
    }
}

fn capture_metadata(metadata: &cap_std::fs::Metadata) -> FileMetadata {
    let mut result = FileMetadata {
        posix_mode: MetadataField::Unavailable,
        posix_uid: MetadataField::Unavailable,
        posix_gid: MetadataField::Unavailable,
        posix_flags: MetadataField::Unavailable,
        windows_attributes: MetadataField::Unavailable,
        created_ns: system_time(metadata.created()),
        modified_ns: system_time(metadata.modified()),
        accessed_ns: system_time(metadata.accessed()),
        changed_ns: MetadataField::Unavailable,
        named_attributes: MetadataField::Unavailable,
        acl: MetadataField::Unavailable,
        security_descriptor: MetadataField::Unavailable,
    };
    populate_platform_metadata(metadata, &mut result);
    result
}

fn preserve_unobserved_metadata(observed: FileMetadata, prior: FileMetadata) -> FileMetadata {
    FileMetadata {
        posix_mode: preserve_field(observed.posix_mode, prior.posix_mode),
        posix_uid: preserve_field(observed.posix_uid, prior.posix_uid),
        posix_gid: preserve_field(observed.posix_gid, prior.posix_gid),
        posix_flags: preserve_field(observed.posix_flags, prior.posix_flags),
        windows_attributes: preserve_field(observed.windows_attributes, prior.windows_attributes),
        created_ns: preserve_field(observed.created_ns, prior.created_ns),
        modified_ns: preserve_field(observed.modified_ns, prior.modified_ns),
        accessed_ns: preserve_field(observed.accessed_ns, prior.accessed_ns),
        changed_ns: preserve_field(observed.changed_ns, prior.changed_ns),
        named_attributes: preserve_field(observed.named_attributes, prior.named_attributes),
        acl: preserve_field(observed.acl, prior.acl),
        security_descriptor: preserve_field(
            observed.security_descriptor,
            prior.security_descriptor,
        ),
    }
}

fn preserve_field<T: Copy>(
    observed: MetadataField<T>,
    prior: MetadataField<T>,
) -> MetadataField<T> {
    match observed {
        MetadataField::Unavailable => prior,
        MetadataField::Value(_) => observed,
    }
}

#[allow(clippy::needless_pass_by_value)]
fn system_time(value: std::io::Result<cap_std::time::SystemTime>) -> MetadataField<i64> {
    let Ok(value) = value else {
        return MetadataField::Unavailable;
    };
    let Ok(duration) = value.into_std().duration_since(std::time::UNIX_EPOCH) else {
        return MetadataField::Unavailable;
    };
    let nanos =
        i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos());
    i64::try_from(nanos).map_or(MetadataField::Unavailable, MetadataField::Value)
}

#[cfg(unix)]
fn populate_platform_metadata(metadata: &cap_std::fs::Metadata, result: &mut FileMetadata) {
    use cap_std::fs::MetadataExt;
    result.posix_mode = MetadataField::Value(metadata.mode());
    result.posix_uid = MetadataField::Value(metadata.uid());
    result.posix_gid = MetadataField::Value(metadata.gid());
    let nanos = i128::from(metadata.ctime()) * 1_000_000_000 + i128::from(metadata.ctime_nsec());
    result.changed_ns =
        i64::try_from(nanos).map_or(MetadataField::Unavailable, MetadataField::Value);
}

#[cfg(windows)]
fn populate_platform_metadata(metadata: &cap_std::fs::Metadata, result: &mut FileMetadata) {
    use cap_std::fs::MetadataExt;
    result.windows_attributes = MetadataField::Value(metadata.file_attributes());
}

#[allow(clippy::needless_pass_by_value)]
fn map_engine_failure<E: std::fmt::Display>(
    failure: OperationFailure<E>,
    prior: WorkCounters,
) -> OperationFailure<CaptureError> {
    match prior.checked_add(*failure.work) {
        Ok(work) => OperationFailure::new(CaptureError::Engine(failure.error.to_string()), work),
        Err(error) => OperationFailure::new(CaptureError::Work(error), prior),
    }
}

fn map_capture_failure(
    failure: OperationFailure<CaptureError>,
    prior: WorkCounters,
) -> OperationFailure<CaptureError> {
    match prior.checked_add(*failure.work) {
        Ok(work) => OperationFailure::new(failure.error, work),
        Err(error) => OperationFailure::new(CaptureError::Work(error), prior),
    }
}

fn add_work(
    left: WorkCounters,
    right: WorkCounters,
) -> Result<WorkCounters, OperationFailure<CaptureError>> {
    left.checked_add(right)
        .map_err(|error| OperationFailure::new(error.into(), left))
}
