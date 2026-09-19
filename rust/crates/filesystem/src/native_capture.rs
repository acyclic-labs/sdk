//! Bounded host-state capture into one atomic sparse checkout transaction.

use crate::kernel::{
    FileKind, FileMetadata, FileRecord, LogicalName, MetadataField, NameEncoding, NamespacePath,
};
use crate::model::FilesystemProfile;
use crate::native_host::{HostDataRange, HostRoot, allocated_data_ranges};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, CancellationToken, Checkout,
    ContentStager, NativeRootIdentity, OperationFailure, OperationReceipt, StagedContent,
    WatchBatch, WatchChange, WatchEpoch, WatchInvalidationReason, WatchSequence, WorkBudget,
    WorkCounters, WorkError,
};
use futures::{StreamExt as _, stream};

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

/// Canonical path eligibility for native capture.
///
/// Excluded prefixes are omitted symmetrically from host discovery, checkout
/// discovery, explicit capture, and watcher reconciliation. An excluded
/// checkout path is therefore never mistaken for a host-side deletion.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CapturePolicy {
    excluded_prefixes: Vec<NamespacePath>,
}

impl CapturePolicy {
    /// Admits every path.
    #[must_use]
    pub const fn allow_all() -> Self {
        Self {
            excluded_prefixes: Vec::new(),
        }
    }

    /// Creates a policy from canonical namespace prefixes.
    ///
    /// Redundant descendants are removed so equivalent policies have one
    /// stable fingerprint. The volume root cannot be excluded.
    ///
    /// # Errors
    ///
    /// Rejects an attempt to exclude the complete capture root.
    pub fn excluding(mut prefixes: Vec<NamespacePath>) -> Result<Self, CaptureError> {
        if prefixes.iter().any(NamespacePath::is_root) {
            return Err(CaptureError::InvalidOptions);
        }
        prefixes.sort();
        prefixes.dedup();
        let mut canonical = Vec::<NamespacePath>::new();
        for prefix in prefixes {
            if canonical
                .last()
                .is_some_and(|ancestor| prefix.is_within(ancestor))
            {
                continue;
            }
            canonical.push(prefix);
        }
        Ok(Self {
            excluded_prefixes: canonical,
        })
    }

    /// Returns whether a path is excluded by an exact prefix.
    #[must_use]
    pub fn excludes(&self, path: &NamespacePath) -> bool {
        let candidate = self
            .excluded_prefixes
            .partition_point(|prefix| prefix <= path)
            .checked_sub(1)
            .and_then(|index| self.excluded_prefixes.get(index));
        candidate.is_some_and(|prefix| path.is_within(prefix))
    }

    /// Stable semantic identity used by durable source binding.
    #[must_use]
    pub fn fingerprint(&self) -> crate::foundation::Digest {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"acyclic-fs-capture-policy-v1\0");
        for prefix in &self.excluded_prefixes {
            hasher.update(&(prefix.depth() as u64).to_le_bytes());
            for component in prefix.components() {
                hasher.update(&[match component.encoding() {
                    NameEncoding::Utf8 => 1,
                    NameEncoding::PosixBytes => 2,
                    NameEncoding::WindowsUtf16Le => 3,
                }]);
                hasher.update(&(component.as_bytes().len() as u64).to_le_bytes());
                hasher.update(component.as_bytes());
            }
        }
        crate::foundation::Digest::from_bytes(*hasher.finalize().as_bytes())
    }
}

#[cfg(test)]
mod capture_policy_tests {
    use super::*;

    fn path(value: &str) -> Result<NamespacePath, Box<dyn std::error::Error>> {
        let limits = crate::model::VolumeLimits::default();
        Ok(NamespacePath::from_portable(
            &crate::path::PortablePath::parse(value, limits)?,
            limits,
        )?)
    }

    #[test]
    fn canonical_policy_uses_the_nearest_sorted_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let policy = CapturePolicy::excluding(vec![
            path("/z/private/nested")?,
            path("/a/cache")?,
            path("/z/private")?,
            path("/a/cache/deeper")?,
            path("/m")?,
        ])?;

        assert_eq!(
            policy.excluded_prefixes,
            vec![path("/a/cache")?, path("/m")?, path("/z/private")?]
        );
        assert!(policy.excludes(&path("/a/cache/object")?));
        assert!(policy.excludes(&path("/m")?));
        assert!(policy.excludes(&path("/z/private/nested/object")?));
        assert!(!policy.excludes(&path("/a/cached")?));
        assert!(!policy.excludes(&path("/n")?));
        assert!(!policy.excludes(&path("/z/public")?));
        Ok(())
    }

    #[test]
    fn subtree_roots_collapse_duplicates_and_descendants() -> Result<(), Box<dyn std::error::Error>>
    {
        let policy = CapturePolicy::excluding(vec![path("/excluded")?])?;
        assert_eq!(
            canonical_subtree_roots(
                &[
                    path("/z/child")?,
                    path("/a")?,
                    path("/z")?,
                    path("/a")?,
                    path("/excluded/child")?,
                ],
                &policy,
            ),
            vec![path("/a")?, path("/z")?]
        );
        Ok(())
    }

    #[test]
    fn combined_host_and_checkout_paths_share_one_limit() {
        assert!(!combined_path_count_exceeds(2, 1, 3));
        assert!(combined_path_count_exceeds(2, 2, 3));
        assert!(combined_path_count_exceeds(usize::MAX, 1, usize::MAX));
    }
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
/// are never retained in memory. Before applying the transaction, capture
/// rechecks the path and open handle against the observed host metadata.
/// This is not an atomic host snapshot: callers that need one must quiesce
/// the source or reconcile changes through a native watcher. Missing host
/// paths become removals only when
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
    capture_paths_with_policy(
        checkout,
        paths,
        options,
        &CapturePolicy::allow_all(),
        budget,
        cancellation,
    )
    .await
}

/// Captures eligible paths under an exact, symmetric path policy.
pub async fn capture_paths_with_policy<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    options: &CaptureOptions,
    policy: &CapturePolicy,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    validate_path_count(paths, options).map_err(OperationFailure::before_work)?;
    let source_root = open_source_root(options).map_err(OperationFailure::before_work)?;
    capture_paths_from_root(
        checkout,
        paths,
        policy,
        options.maximum_extent_spans,
        &source_root,
        budget,
        cancellation,
    )
    .await
}

/// Reconciles one host directory and every descendant as one authored
/// transaction. Both host and checkout descendants are included so removals
/// inside a replaced imported tree are exact.
pub async fn capture_subtree<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    root: NamespacePath,
    options: &CaptureOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    capture_subtrees_with_policy(
        checkout,
        &[root],
        options,
        &CapturePolicy::allow_all(),
        budget,
        cancellation,
    )
    .await
}

/// Reconciles one eligible host subtree under an exact path policy.
pub async fn capture_subtree_with_policy<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    root: NamespacePath,
    options: &CaptureOptions,
    policy: &CapturePolicy,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    capture_subtrees_with_policy(checkout, &[root], options, policy, budget, cancellation).await
}

/// Reconciles eligible disjoint host subtrees as one authored transaction.
///
/// Redundant descendants and duplicate roots are removed before any host or
/// checkout work. The remaining roots share one capability root, one batched
/// checkout lookup, and one atomic mutation transaction.
pub async fn capture_subtrees_with_policy<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    roots: &[NamespacePath],
    options: &CaptureOptions,
    policy: &CapturePolicy,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    if options.maximum_paths == 0 || options.maximum_extent_spans == 0 {
        return Err(OperationFailure::before_work(CaptureError::InvalidOptions));
    }
    let source_root = open_source_root(options).map_err(OperationFailure::before_work)?;
    cancellation
        .check()
        .map_err(|error| OperationFailure::before_work(CaptureError::Engine(error.to_string())))?;
    let canonical = canonical_subtree_roots(roots, policy);
    if canonical.is_empty() {
        return Ok(OperationReceipt {
            value: CaptureReceipt::default(),
            work: WorkCounters::default(),
        });
    }
    let limits = checkout.volume_config().limits;
    let maximum = usize::try_from(options.maximum_paths)
        .map_err(|_| OperationFailure::before_work(CaptureError::InvalidOptions))?;
    let profile = checkout.volume_config().profile;
    let mut observed = BTreeMap::new();
    let mut paths = canonical.iter().cloned().collect::<BTreeSet<_>>();
    let mut work = WorkCounters::default();
    collect_host_subtree_roots(
        &source_root,
        profile,
        limits,
        maximum,
        &canonical,
        policy,
        &mut observed,
        &mut work,
        budget,
        cancellation,
    )?;
    let remaining = work
        .remaining(budget)
        .map_err(|error| OperationFailure::new(CaptureError::Work(error), work))?;
    let current = checkout
        .lookup_batch_no_follow(&canonical, remaining, cancellation)
        .await
        .map_err(|failure| map_engine_failure(failure, work))?;
    work = add_work(work, current.work)?;
    for (root, entry) in canonical.into_iter().zip(current.value.entries) {
        if entry
            .record
            .is_some_and(|record| record.kind == FileKind::Directory)
        {
            collect_checkout_subtree_paths(
                checkout,
                limits,
                maximum,
                root,
                policy,
                &mut paths,
                &mut work,
                budget,
                cancellation,
            )
            .await?;
        }
    }
    let ordered = order_host_checkout_union(observed, paths, maximum)
        .map_err(|error| OperationFailure::new(error, work))?;
    let remaining = work
        .remaining(budget)
        .map_err(|error| OperationFailure::new(CaptureError::Work(error), work))?;
    let mut captured = capture_observed_paths_from_root(
        checkout,
        ordered,
        options.maximum_extent_spans,
        &source_root,
        remaining,
        cancellation,
    )
    .await
    .map_err(|failure| map_capture_failure(failure, work))?;
    let combined = add_work(work, captured.work)?;
    captured.value.work = combined;
    Ok(OperationReceipt {
        value: captured.value,
        work: combined,
    })
}

fn canonical_subtree_roots(roots: &[NamespacePath], policy: &CapturePolicy) -> Vec<NamespacePath> {
    let mut sorted = roots
        .iter()
        .filter(|root| !policy.excludes(root))
        .cloned()
        .collect::<Vec<_>>();
    sorted.sort();
    sorted.dedup();
    let mut canonical = Vec::<NamespacePath>::new();
    for root in sorted {
        if canonical
            .last()
            .is_none_or(|ancestor| !root.is_within(ancestor))
        {
            canonical.push(root);
        }
    }
    canonical
}

async fn capture_paths_from_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    policy: &CapturePolicy,
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let mut unique = paths
        .iter()
        .filter(|path| !policy.excludes(path))
        .cloned()
        .collect::<Vec<_>>();
    unique.sort();
    if unique
        .windows(2)
        .any(|pair| matches!(pair, [left, right] if left == right))
    {
        return Err(OperationFailure::before_work(CaptureError::InvalidOptions));
    }
    capture_unique_paths_from_root(
        checkout,
        unique,
        maximum_extent_spans,
        source_root,
        budget,
        cancellation,
    )
    .await
}

async fn capture_unique_paths_from_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    unique: Vec<NamespacePath>,
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    // A comparator must not inspect a changing host tree: one path could
    // otherwise alternate between present and absent during the sort. This
    // also reduces host metadata probes from O(paths * log paths) to O(paths).
    let mut ordered = unique
        .into_iter()
        .map(|path| {
            let observation = namespace_to_host_path(&path)
                .ok()
                .and_then(|host_path| source_root.symlink_metadata(&host_path).ok())
                .map(HostObservation::from_metadata);
            (observation, path)
        })
        .collect::<Vec<_>>();
    ordered.sort_by(|(left_observation, left), (right_observation, right)| {
        match (left_observation.is_some(), right_observation.is_some()) {
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
    capture_observed_paths_from_root(
        checkout,
        ordered,
        maximum_extent_spans,
        source_root,
        budget,
        cancellation,
    )
    .await
}

async fn capture_observed_paths_from_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    ordered: Vec<(Option<HostObservation>, NamespacePath)>,
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let mut receipt = CaptureReceipt::default();
    let mut mutations = Vec::new();
    let mut host_links = BTreeMap::new();
    mutations
        .try_reserve(ordered.len().saturating_mul(3))
        .map_err(|_| OperationFailure::before_work(CaptureError::InvalidOptions))?;

    let mut observations = Vec::new();
    let mut paths = Vec::new();
    observations
        .try_reserve(ordered.len())
        .map_err(|_| OperationFailure::before_work(CaptureError::InvalidOptions))?;
    paths
        .try_reserve(ordered.len())
        .map_err(|_| OperationFailure::before_work(CaptureError::InvalidOptions))?;
    for (observation, path) in ordered {
        observations.push(observation);
        paths.push(path);
    }
    let current = if paths.is_empty() {
        Vec::new()
    } else {
        let remaining = receipt
            .work
            .remaining(budget)
            .map_err(|error| OperationFailure::new(CaptureError::Work(error), receipt.work))?;
        let lookup = checkout
            .lookup_batch_no_follow(&paths, remaining, cancellation)
            .await
            .map_err(|failure| map_engine_failure(failure, receipt.work))?;
        receipt.work = add_work(receipt.work, lookup.work)?;
        lookup
            .value
            .entries
            .into_iter()
            .map(|entry| entry.record)
            .collect()
    };

    let mut prepared = Vec::new();
    prepared
        .try_reserve(paths.len())
        .map_err(|_| OperationFailure::new(CaptureError::InvalidOptions, receipt.work))?;
    for ((observation, path), current) in observations.into_iter().zip(paths).zip(current) {
        let plan = prepare_final_path(
            checkout,
            path,
            CurrentRecord::Known(current),
            CaptureIntent::Complete,
            source_root,
            observation,
            &mut host_links,
            None,
            &mut mutations,
            &mut receipt,
            budget,
            cancellation,
        )
        .await?;
        if let Some(plan) = plan {
            prepared.push(plan);
        }
    }
    finish_prepared_paths(
        &checkout.content_stager(),
        source_root,
        prepared,
        maximum_extent_spans,
        &mut mutations,
        &mut receipt,
        budget,
        cancellation,
    )
    .await?;

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
    capture_baseline_with_policy(
        checkout,
        options,
        &CapturePolicy::allow_all(),
        budget,
        cancellation,
    )
    .await
}

/// Captures a complete eligible baseline under an exact path policy.
pub async fn capture_baseline_with_policy<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    options: &CaptureOptions,
    policy: &CapturePolicy,
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
    let observed = collect_host_observations(
        &source_root,
        profile,
        limits,
        maximum,
        policy,
        &mut work,
        budget,
        cancellation,
    )?;
    Box::pin(collect_checkout_paths(
        checkout,
        limits,
        maximum,
        policy,
        &mut paths,
        &mut work,
        budget,
        cancellation,
    ))
    .await?;
    let ordered = order_host_checkout_union(observed, paths, maximum)
        .map_err(|error| OperationFailure::new(error, work))?;
    let remaining = work
        .remaining(budget)
        .map_err(|error| OperationFailure::new(CaptureError::Work(error), work))?;
    let mut captured = Box::pin(capture_observed_paths_from_root(
        checkout,
        ordered,
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
fn collect_host_observations(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    policy: &CapturePolicy,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<BTreeMap<NamespacePath, HostObservation>, OperationFailure<CaptureError>> {
    let mut observed = BTreeMap::new();
    let volume_root = NamespacePath::new(Vec::new(), limits)
        .map_err(|error| OperationFailure::before_work(CaptureError::Engine(error.to_string())))?;
    let mut pending = vec![(PathBuf::new(), volume_root)];
    while let Some((host_parent, volume_parent)) = pending.pop() {
        cancellation.check().map_err(|error| {
            OperationFailure::new(CaptureError::Engine(error.to_string()), *work)
        })?;
        for entry in root
            .read_dir(&host_parent)
            .map_err(|error| OperationFailure::new(error.into(), *work))?
        {
            let entry = entry.map_err(|error| OperationFailure::new(error.into(), *work))?;
            let child = append_path(
                &volume_parent,
                logical_host_name(&entry.file_name(), profile, limits)
                    .map_err(|error| OperationFailure::new(error, *work))?,
                limits,
            )
            .map_err(|error| OperationFailure::new(error, *work))?;
            if policy.excludes(&child) {
                continue;
            }
            let host_child = host_parent.join(entry.file_name());
            let metadata = root
                .symlink_metadata(&host_child)
                .map_err(|error| OperationFailure::new(error.into(), *work))?;
            let file_type = metadata.file_type();
            insert_host_observation(
                &mut observed,
                child.clone(),
                metadata,
                maximum,
                work,
                budget,
            )?;
            if file_type.is_dir() && !file_type.is_symlink() {
                pending.push((host_child, child));
            }
        }
    }
    Ok(observed)
}

fn order_host_checkout_union(
    observed: BTreeMap<NamespacePath, HostObservation>,
    mut checkout_paths: BTreeSet<NamespacePath>,
    maximum: usize,
) -> Result<Vec<(Option<HostObservation>, NamespacePath)>, CaptureError> {
    for path in observed.keys() {
        checkout_paths.remove(path);
    }
    if combined_path_count_exceeds(observed.len(), checkout_paths.len(), maximum) {
        return Err(CaptureError::InvalidOptions);
    }
    let mut ordered = observed
        .into_iter()
        .map(|(path, observation)| (Some(observation), path))
        .collect::<Vec<_>>();
    ordered.sort_by(|(_, left), (_, right)| {
        left.depth()
            .cmp(&right.depth())
            .then_with(|| left.cmp(right))
    });
    let mut absent = checkout_paths.into_iter().collect::<Vec<_>>();
    absent.sort_by(|left, right| {
        right
            .depth()
            .cmp(&left.depth())
            .then_with(|| left.cmp(right))
    });
    ordered.extend(absent.into_iter().map(|path| (None, path)));
    Ok(ordered)
}

fn combined_path_count_exceeds(observed: usize, checkout_only: usize, maximum: usize) -> bool {
    observed
        .checked_add(checkout_only)
        .is_none_or(|combined| combined > maximum)
}

fn insert_host_observation(
    observed: &mut BTreeMap<NamespacePath, HostObservation>,
    path: NamespacePath,
    metadata: cap_std::fs::Metadata,
    maximum: usize,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), OperationFailure<CaptureError>> {
    let encoded_bytes = u64::from(path.encoded_bytes());
    observed.insert(path, HostObservation::from_metadata(metadata));
    if observed.len() > maximum {
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
        .map_err(|error| OperationFailure::new(CaptureError::Work(error), *work))
}

#[allow(clippy::too_many_arguments)]
fn collect_host_subtree_roots(
    source_root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    roots: &[NamespacePath],
    policy: &CapturePolicy,
    observed: &mut BTreeMap<NamespacePath, HostObservation>,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), OperationFailure<CaptureError>> {
    for root in roots {
        let host_root = namespace_to_host_path(root).map_err(OperationFailure::before_work)?;
        match source_root.symlink_metadata(&host_root) {
            Ok(metadata) => {
                let file_type = metadata.file_type();
                insert_host_observation(observed, root.clone(), metadata, maximum, work, budget)?;
                if file_type.is_dir() && !file_type.is_symlink() {
                    collect_host_subtree_observations(
                        source_root,
                        profile,
                        limits,
                        maximum,
                        host_root,
                        root.clone(),
                        policy,
                        observed,
                        work,
                        budget,
                        cancellation,
                    )?;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(OperationFailure::new(error.into(), *work)),
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn collect_host_subtree_observations(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    host_root: PathBuf,
    volume_root: NamespacePath,
    policy: &CapturePolicy,
    observed: &mut BTreeMap<NamespacePath, HostObservation>,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), OperationFailure<CaptureError>> {
    let mut pending = vec![(host_root, volume_root)];
    while let Some((host_parent, volume_parent)) = pending.pop() {
        cancellation.check().map_err(|error| {
            OperationFailure::new(CaptureError::Engine(error.to_string()), *work)
        })?;
        for entry in root
            .read_dir(&host_parent)
            .map_err(|error| OperationFailure::new(error.into(), *work))?
        {
            let entry = entry.map_err(|error| OperationFailure::new(error.into(), *work))?;
            let child = append_path(
                &volume_parent,
                logical_host_name(&entry.file_name(), profile, limits)
                    .map_err(|error| OperationFailure::new(error, *work))?,
                limits,
            )
            .map_err(|error| OperationFailure::new(error, *work))?;
            if policy.excludes(&child) {
                continue;
            }
            let host_child = host_parent.join(entry.file_name());
            let metadata = root
                .symlink_metadata(&host_child)
                .map_err(|error| OperationFailure::new(error.into(), *work))?;
            let file_type = metadata.file_type();
            insert_host_observation(observed, child.clone(), metadata, maximum, work, budget)?;
            if file_type.is_dir() && !file_type.is_symlink() {
                pending.push((host_child, child));
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn collect_host_subtree_paths(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    host_root: PathBuf,
    volume_root: NamespacePath,
    policy: &CapturePolicy,
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
            if policy.excludes(&child) {
                continue;
            }
            insert_scanned_path(paths, child.clone(), maximum, work, budget)?;
            let host_child = host_parent.join(entry.file_name());
            // Directory enumeration already supplies a no-follow file kind.
            // Capture later reopens and validates every selected path; this
            // probe only decides which descendants need enumeration.
            let file_type = entry
                .file_type()
                .map_err(|error| OperationFailure::new(error.into(), *work))?;
            if file_type.is_dir() && !file_type.is_symlink() {
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
    policy: &CapturePolicy,
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
        policy,
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
    policy: &CapturePolicy,
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
                if policy.excludes(&child) {
                    continue;
                }
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
    let (encoding, bytes) =
        crate::native_name::host_name_bytes(name, profile, limits.maximum_component_bytes)
            .map_err(|_| CaptureError::UnrepresentablePath)?;
    LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
        .map_err(|error| CaptureError::Engine(error.to_string()))
}

/// Converts one host-relative path to an exact bounded namespace path.
///
/// The supplied profile selects the canonical name encoding. Absolute paths,
/// parent traversal, platform prefixes, empty paths, and names that cannot be
/// represented exactly by that profile are rejected.
///
/// # Errors
///
/// Returns [`CaptureError::UnrepresentablePath`] unless every component can be
/// represented exactly within `limits`.
pub fn host_path_to_namespace(
    path: &std::path::Path,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
) -> Result<NamespacePath, CaptureError> {
    use std::path::Component;

    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(name) => components.push(logical_host_name(name, profile, limits)?),
            Component::CurDir => {}
            Component::Prefix(_) | Component::RootDir | Component::ParentDir => {
                return Err(CaptureError::UnrepresentablePath);
            }
        }
    }
    if components.is_empty() {
        return Err(CaptureError::UnrepresentablePath);
    }
    NamespacePath::new(components, limits).map_err(|_| CaptureError::UnrepresentablePath)
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
/// an invalidated epoch or a new hard link whose source lies outside the hint
/// batch. Other failures are the union of exact rename,
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
    capture_watch_batch_with_policy(
        checkout,
        batch,
        options,
        &CapturePolicy::allow_all(),
        budget,
        cancellation,
    )
    .await
}

/// Captures one watcher batch while omitting excluded paths symmetrically.
#[allow(clippy::too_many_lines)]
pub async fn capture_watch_batch_with_policy<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    batch: WatchBatch,
    options: &CaptureOptions,
    policy: &CapturePolicy,
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

    for change in changes.into_iter().filter_map(|change| match &change {
        WatchChange::Created(path)
        | WatchChange::Modified(path)
        | WatchChange::Removed(path)
        | WatchChange::MetadataChanged(path) => (!policy.excludes(path)).then_some(change),
        WatchChange::Renamed { from, to } => match (policy.excludes(from), policy.excludes(to)) {
            (false, false) => Some(change),
            (true, false) => Some(WatchChange::Created(to.clone())),
            (false, true) => Some(WatchChange::Removed(from.clone())),
            (true, true) => None,
        },
    }) {
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
        policy,
        &mut receipt,
        budget,
        cancellation,
    ))
    .await?;

    let lookup_paths = ordinary
        .iter()
        .filter(|(_, (current, _))| matches!(current, CurrentRecord::Lookup))
        .map(|(path, _)| path.clone())
        .collect::<Vec<_>>();
    if !lookup_paths.is_empty() {
        let remaining = receipt
            .work
            .remaining(budget)
            .map_err(|error| OperationFailure::new(CaptureError::Work(error), receipt.work))?;
        let lookup = checkout
            .lookup_batch_no_follow(&lookup_paths, remaining, cancellation)
            .await
            .map_err(|failure| map_engine_failure(failure, receipt.work))?;
        receipt.work = add_work(receipt.work, lookup.work)?;
        for (path, entry) in lookup_paths.into_iter().zip(lookup.value.entries) {
            let Some((current, _)) = ordinary.get_mut(&path) else {
                return Err(OperationFailure::new(
                    CaptureError::Engine("capture lookup path disappeared".into()),
                    receipt.work,
                ));
            };
            *current = CurrentRecord::Known(entry.record);
        }
    }

    let mut host_links = BTreeMap::new();
    let mut prepared = Vec::new();
    prepared
        .try_reserve(rename_records.len().saturating_add(ordinary.len()))
        .map_err(|_| OperationFailure::new(CaptureError::InvalidOptions, receipt.work))?;
    for (path, record) in rename_records {
        if ordinary.contains_key(&path) {
            continue;
        }
        let plan = prepare_final_path(
            checkout,
            path,
            CurrentRecord::Known(Some(record)),
            CaptureIntent::Complete,
            &source_root,
            None,
            &mut host_links,
            Some(epoch.get()),
            &mut mutations,
            &mut receipt,
            budget,
            cancellation,
        )
        .await?;
        if let Some(plan) = plan {
            prepared.push(plan);
        }
    }
    let mut ordinary = ordinary
        .into_iter()
        .map(|(path, (current, intent))| {
            let host_path = namespace_to_host_path(&path)?;
            let observation = match source_root.symlink_metadata(&host_path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(_) => None,
                Ok(metadata) => Some(HostObservation::from_metadata(metadata)),
            };
            Ok((path, current, intent, observation))
        })
        .collect::<Result<Vec<_>, CaptureError>>()
        .map_err(|error| OperationFailure::new(error, receipt.work))?;
    ordinary.sort_by(|left, right| match (left.3.is_some(), right.3.is_some()) {
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
    for (path, current, intent, observation) in ordinary {
        let plan = prepare_final_path(
            checkout,
            path,
            current,
            intent,
            &source_root,
            observation,
            &mut host_links,
            Some(epoch.get()),
            &mut mutations,
            &mut receipt,
            budget,
            cancellation,
        )
        .await?;
        if let Some(plan) = plan {
            prepared.push(plan);
        }
    }
    finish_prepared_paths(
        &checkout.content_stager(),
        &source_root,
        prepared,
        options.maximum_extent_spans,
        &mut mutations,
        &mut receipt,
        budget,
        cancellation,
    )
    .await?;
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

struct PreparedRegular {
    path: NamespacePath,
    host_path: PathBuf,
    snapshot: HostSnapshot,
    file: cap_std::fs::File,
    exists_with_kind: bool,
    canonical_metadata: FileMetadata,
}

enum PreparedPath {
    Regular(Box<PreparedRegular>),
    PendingHardLink {
        source: NamespacePath,
        destination: NamespacePath,
        current: Option<FileRecord>,
    },
}

#[derive(Clone)]
enum HostLinkSource {
    Pending(NamespacePath),
    Ready(NamespacePath),
}

#[allow(clippy::too_many_arguments)]
async fn expand_directory_hints<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    ordinary: &mut BTreeMap<NamespacePath, (CurrentRecord, CaptureIntent)>,
    source_root: &HostRoot,
    maximum_paths: u32,
    policy: &CapturePolicy,
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
            let host_path = namespace_to_host_path(path).ok()?;
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
            policy,
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
                policy,
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
#[allow(clippy::too_many_lines)]
async fn prepare_final_path<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    path: NamespacePath,
    current: CurrentRecord,
    intent: CaptureIntent,
    source_root: &HostRoot,
    observation: Option<HostObservation>,
    host_links: &mut BTreeMap<[u8; 16], HostLinkSource>,
    watch_epoch: Option<u64>,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<Option<PreparedPath>, OperationFailure<CaptureError>> {
    cancellation.check().map_err(|error| {
        OperationFailure::new(CaptureError::Engine(error.to_string()), receipt.work)
    })?;
    let host_path = namespace_to_host_path(&path)
        .map_err(|error| OperationFailure::new(error, receipt.work))?;
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
    let metadata = observation.map_or_else(
        || {
            source_root
                .symlink_metadata(&host_path)
                .map(HostObservation::from_metadata)
        },
        Ok,
    );
    match metadata {
        Ok(observation) => {
            let metadata = observation.metadata;
            let host_kind = host_kind(&metadata)?;
            let snapshot = HostSnapshot::from_metadata(&metadata)
                .map_err(|error| OperationFailure::new(error, receipt.work))?;
            let link_probe =
                if host_kind == FileKind::Regular && checkout.volume_config().hard_links {
                    host_link_count(source_root, &host_path, &snapshot, &metadata)
                        .map_err(|error| OperationFailure::new(error, receipt.work))?
                } else {
                    HostLinkProbe::unlinked()
                };
            let linked_regular = link_probe.count > 1;
            if linked_regular {
                let identity = snapshot.identity.to_bytes();
                if let Some(source) = host_links.get(&identity) {
                    ensure_current_host_node(source_root, &host_path, &snapshot)
                        .map_err(|error| OperationFailure::new(error, receipt.work))?;
                    receipt.changed_paths = checked_increment(receipt.changed_paths, receipt.work)?;
                    receipt.examined_paths =
                        checked_increment(receipt.examined_paths, receipt.work)?;
                    return Ok(match source {
                        HostLinkSource::Pending(source) => Some(PreparedPath::PendingHardLink {
                            source: source.clone(),
                            destination: path,
                            current,
                        }),
                        HostLinkSource::Ready(source) => {
                            append_hard_link(source.clone(), path, current, mutations);
                            None
                        }
                    });
                }
                if let Some(epoch) = watch_epoch {
                    // An existing alias may lie outside this hint batch.
                    // Rebaseline instead of recording a second FileId.
                    return Err(OperationFailure::new(
                        CaptureError::RescanRequired {
                            epoch,
                            reason: WatchInvalidationReason::NativeRescanRequired,
                        },
                        receipt.work,
                    ));
                }
            }
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
                snapshot.metadata
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
            let linked_path = path.clone();
            let prepared = if intent == CaptureIntent::MetadataOnly && exists_with_kind {
                ensure_current_host_node(source_root, &host_path, &snapshot)
                    .map_err(|error| OperationFailure::new(error, receipt.work))?;
                mutations.push(AuthoredMutation::SetMetadata {
                    path,
                    metadata: canonical_metadata,
                });
                None
            } else if host_kind == FileKind::Regular {
                let file = if let Some(file) = link_probe.file {
                    file
                } else {
                    source_root
                        .open_file(&host_path)
                        .map_err(|error| OperationFailure::new(error.into(), receipt.work))?
                };
                let opened = file
                    .metadata()
                    .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
                ensure_same_host_node(&snapshot, &opened)
                    .map_err(|error| OperationFailure::new(error, receipt.work))?;
                Some(PreparedPath::Regular(Box::new(PreparedRegular {
                    path,
                    host_path,
                    snapshot,
                    file,
                    exists_with_kind,
                    canonical_metadata,
                })))
            } else {
                append_final_state(
                    path,
                    source_root,
                    &host_path,
                    &metadata,
                    snapshot,
                    host_kind,
                    exists_with_kind,
                    canonical_metadata,
                    mutations,
                    receipt.work,
                )?;
                None
            };
            receipt.changed_paths = checked_increment(receipt.changed_paths, receipt.work)?;
            if linked_regular {
                host_links.insert(
                    snapshot.identity.to_bytes(),
                    if prepared.is_some() {
                        HostLinkSource::Pending(linked_path)
                    } else {
                        HostLinkSource::Ready(linked_path)
                    },
                );
            }
            receipt.examined_paths = checked_increment(receipt.examined_paths, receipt.work)?;
            return Ok(prepared);
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
    Ok(None)
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

fn append_hard_link(
    source: NamespacePath,
    destination: NamespacePath,
    current: Option<FileRecord>,
    mutations: &mut Vec<AuthoredMutation>,
) {
    if let Some(record) = current {
        mutations.push(AuthoredMutation::Remove {
            path: destination.clone(),
            expected_file_id: Some(record.file_id),
        });
    }
    mutations.push(AuthoredMutation::HardLink {
        source,
        destination,
    });
}

#[allow(clippy::too_many_arguments)]
fn append_final_state(
    path: NamespacePath,
    source_root: &HostRoot,
    host_path: &Path,
    metadata: &cap_std::fs::Metadata,
    snapshot: HostSnapshot,
    kind: FileKind,
    exists_with_kind: bool,
    canonical_metadata: FileMetadata,
    mutations: &mut Vec<AuthoredMutation>,
    work: WorkCounters,
) -> Result<(), OperationFailure<CaptureError>> {
    match kind {
        FileKind::Regular => unreachable!("regular files are staged after preparation"),
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
            let target = read_link_bytes(source_root, host_path)
                .map_err(|error| OperationFailure::new(error, work))?;
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
                metadata,
                kind,
                exists_with_kind,
                canonical_metadata,
                mutations,
                work,
            )?;
        }
        FileKind::ReparsePoint | FileKind::MountBoundary => {
            return Err(OperationFailure::new(CaptureError::UnsupportedKind, work));
        }
    }
    ensure_current_host_node(source_root, host_path, &snapshot)
        .map_err(|error| OperationFailure::new(error, work))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn finish_prepared_regular<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    stager: &ContentStager<A, O>,
    source_root: &HostRoot,
    prepared: PreparedRegular,
    maximum_extent_spans: u32,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), OperationFailure<CaptureError>> {
    let staged = stage_regular_body(
        stager,
        source_root,
        &prepared.host_path,
        &prepared.snapshot,
        Some(prepared.file),
        maximum_extent_spans,
        receipt.work,
        budget,
        cancellation,
    )
    .await?;
    receipt.work = add_work(receipt.work, staged.content.work)?;
    receipt.staged_file_bytes = receipt
        .staged_file_bytes
        .checked_add(staged.content.bytes)
        .ok_or_else(|| {
            OperationFailure::new(CaptureError::Work(WorkError::Overflow), receipt.work)
        })?;
    append_staged_regular_state(
        prepared.path,
        staged.logical_bytes,
        prepared.exists_with_kind,
        prepared.canonical_metadata,
        staged.content.ranges,
        mutations,
    );
    Ok(())
}

async fn finish_prepared_regular_batch<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    stager: &ContentStager<A, O>,
    source_root: &HostRoot,
    prepared: PreparedRegularBatch,
    maximum_extent_spans: u32,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    cancellation: &CancellationToken,
) -> Result<(), OperationFailure<CaptureError>> {
    let PreparedRegularBatch {
        regular,
        mut aliases,
    } = prepared;
    let concurrency = std::thread::available_parallelism()
        .map_or(1, std::num::NonZero::get)
        .min(8)
        .min(regular.len().max(1));
    let results = stream::iter(regular.into_iter().enumerate().map(|(index, prepared)| {
        let stager = stager.clone();
        async move {
            let PreparedRegular {
                path,
                host_path,
                snapshot,
                file,
                exists_with_kind,
                canonical_metadata,
            } = prepared;
            let result = stage_regular_body(
                &stager,
                source_root,
                &host_path,
                &snapshot,
                Some(file),
                maximum_extent_spans,
                WorkCounters::default(),
                WorkBudget::UNBOUNDED,
                cancellation,
            )
            .await;
            (index, (path, exists_with_kind, canonical_metadata), result)
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await;
    let mut results = results;
    results.sort_by_key(|(index, _, _)| *index);
    let mut staged_work = WorkCounters::default();
    for (_, _, result) in &results {
        let work = match result {
            Ok(staged) => staged.content.work,
            Err(failure) => *failure.work,
        };
        staged_work = staged_work
            .checked_add(work)
            .map_err(|error| OperationFailure::new(CaptureError::Work(error), receipt.work))?;
    }
    receipt.work = add_work(receipt.work, staged_work)?;
    for (_, (path, exists_with_kind, canonical_metadata), result) in results {
        let source = path.clone();
        let staged =
            result.map_err(|failure| OperationFailure::new(failure.error, receipt.work))?;
        receipt.staged_file_bytes = receipt
            .staged_file_bytes
            .checked_add(staged.content.bytes)
            .ok_or_else(|| {
                OperationFailure::new(CaptureError::Work(WorkError::Overflow), receipt.work)
            })?;
        append_staged_regular_state(
            path,
            staged.logical_bytes,
            exists_with_kind,
            canonical_metadata,
            staged.content.ranges,
            mutations,
        );
        if let Some(pending) = aliases.remove(&source) {
            for (destination, current) in pending {
                append_hard_link(source.clone(), destination, current, mutations);
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn finish_prepared_paths<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    stager: &ContentStager<A, O>,
    source_root: &HostRoot,
    prepared: Vec<PreparedPath>,
    maximum_extent_spans: u32,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), OperationFailure<CaptureError>> {
    let mut regular = Vec::new();
    let mut aliases = PendingHardLinks::new();
    regular
        .try_reserve(prepared.len())
        .map_err(|_| OperationFailure::new(CaptureError::InvalidOptions, receipt.work))?;
    for path in prepared {
        match path {
            PreparedPath::Regular(plan) => regular.push(*plan),
            PreparedPath::PendingHardLink {
                source,
                destination,
                current,
            } => aliases
                .entry(source)
                .or_default()
                .push((destination, current)),
        }
    }
    if budget == WorkBudget::UNBOUNDED {
        finish_prepared_regular_batch(
            stager,
            source_root,
            PreparedRegularBatch { regular, aliases },
            maximum_extent_spans,
            mutations,
            receipt,
            cancellation,
        )
        .await?;
        return Ok(());
    } else {
        for plan in regular {
            let source = plan.path.clone();
            finish_prepared_regular(
                stager,
                source_root,
                plan,
                maximum_extent_spans,
                mutations,
                receipt,
                budget,
                cancellation,
            )
            .await?;
            if let Some(pending) = aliases.remove(&source) {
                for (destination, current) in pending {
                    append_hard_link(source.clone(), destination, current, mutations);
                }
            }
        }
    }
    if !aliases.is_empty() {
        return Err(OperationFailure::new(
            CaptureError::Engine("pending hard-link source was not staged".into()),
            receipt.work,
        ));
    }
    Ok(())
}

struct StagedRegularBody {
    logical_bytes: u64,
    content: StagedHostRanges,
}

type PendingHardLinks = BTreeMap<NamespacePath, Vec<(NamespacePath, Option<FileRecord>)>>;

struct PreparedRegularBatch {
    regular: Vec<PreparedRegular>,
    aliases: PendingHardLinks,
}

#[allow(clippy::too_many_arguments)]
async fn stage_regular_body<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    stager: &ContentStager<A, O>,
    source_root: &HostRoot,
    host_path: &Path,
    snapshot: &HostSnapshot,
    opened_file: Option<cap_std::fs::File>,
    maximum_extent_spans: u32,
    prior_work: WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<StagedRegularBody, OperationFailure<CaptureError>> {
    let mut file = if let Some(file) = opened_file {
        file
    } else {
        source_root
            .open_file(host_path)
            .map_err(|error| OperationFailure::new(error.into(), prior_work))?
    };
    let metadata = file
        .metadata()
        .map_err(|error| OperationFailure::new(error.into(), prior_work))?;
    if !metadata.is_file() {
        return Err(OperationFailure::new(
            CaptureError::Io(std::io::Error::other(
                "host file kind changed during capture",
            )),
            prior_work,
        ));
    }
    ensure_same_host_node(snapshot, &metadata)
        .map_err(|error| OperationFailure::new(error, prior_work))?;
    let logical_bytes = metadata.len();
    let ranges = allocated_data_ranges(&file, logical_bytes, maximum_extent_spans)
        .map_err(|error| OperationFailure::new(error.into(), prior_work))?;
    let content =
        stage_host_ranges(stager, &mut file, &ranges, prior_work, budget, cancellation).await?;
    let accumulated = prior_work
        .checked_add(content.work)
        .map_err(|error| OperationFailure::new(CaptureError::Work(error), prior_work))?;
    let after = file
        .metadata()
        .map_err(|error| OperationFailure::new(error.into(), accumulated))?;
    ensure_same_host_node(snapshot, &after)
        .map_err(|error| OperationFailure::new(error, accumulated))?;
    ensure_current_host_node(source_root, host_path, snapshot)
        .map_err(|error| OperationFailure::new(error, accumulated))?;
    Ok(StagedRegularBody {
        logical_bytes,
        content,
    })
}

fn append_staged_regular_state(
    path: NamespacePath,
    logical_bytes: u64,
    exists_with_kind: bool,
    canonical_metadata: FileMetadata,
    ranges: Vec<(HostDataRange, StagedContent)>,
    mutations: &mut Vec<AuthoredMutation>,
) {
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
    mutations.extend(ranges.into_iter().map(|(range, content)| {
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
}

#[derive(Clone, Copy)]
struct HostSnapshot {
    identity: NativeRootIdentity,
    length: u64,
    metadata: FileMetadata,
}

struct HostObservation {
    metadata: cap_std::fs::Metadata,
}

impl HostObservation {
    fn from_metadata(metadata: cap_std::fs::Metadata) -> Self {
        Self { metadata }
    }
}

struct HostLinkProbe {
    count: u64,
    file: Option<cap_std::fs::File>,
}

impl HostLinkProbe {
    const fn unlinked() -> Self {
        Self {
            count: 1,
            file: None,
        }
    }
}

#[cfg(unix)]
fn host_link_count(
    _source_root: &HostRoot,
    _host_path: &Path,
    _snapshot: &HostSnapshot,
    metadata: &cap_std::fs::Metadata,
) -> Result<HostLinkProbe, CaptureError> {
    use cap_std::fs::MetadataExt;
    Ok(HostLinkProbe {
        count: metadata.nlink(),
        file: None,
    })
}

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "reads FILE_STANDARD_INFO through a held capability-opened file handle"
)]
fn host_link_count(
    source_root: &HostRoot,
    host_path: &Path,
    snapshot: &HostSnapshot,
    _metadata: &cap_std::fs::Metadata,
) -> Result<HostLinkProbe, CaptureError> {
    use std::mem::size_of;
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{
        FILE_STANDARD_INFO, FileStandardInfo, GetFileInformationByHandleEx,
    };

    let file = source_root.open_file(host_path)?;
    ensure_same_host_node(snapshot, &file.metadata()?)?;
    let mut information = FILE_STANDARD_INFO::default();
    // SAFETY: file stays open and information is a correctly sized output.
    unsafe {
        GetFileInformationByHandleEx(
            HANDLE(file.as_raw_handle()),
            FileStandardInfo,
            (&raw mut information).cast(),
            u32::try_from(size_of::<FILE_STANDARD_INFO>())
                .map_err(|_| CaptureError::InvalidOptions)?,
        )
        .map_err(|error| CaptureError::Io(std::io::Error::other(error)))?;
    }
    Ok(HostLinkProbe {
        count: u64::from(information.NumberOfLinks),
        file: Some(file),
    })
}

impl HostSnapshot {
    fn from_metadata(metadata: &cap_std::fs::Metadata) -> Result<Self, CaptureError> {
        Ok(Self {
            identity: NativeRootIdentity::from_metadata(metadata)?,
            length: metadata.len(),
            metadata: capture_metadata(metadata),
        })
    }
}

fn ensure_same_host_node(
    expected: &HostSnapshot,
    observed: &cap_std::fs::Metadata,
) -> Result<(), CaptureError> {
    let observed_identity = NativeRootIdentity::from_metadata(observed)?;
    let mut expected_metadata = expected.metadata;
    let mut observed_metadata = capture_metadata(observed);
    // Reading the file can legitimately update its access time.
    expected_metadata.accessed_ns = MetadataField::Unavailable;
    observed_metadata.accessed_ns = MetadataField::Unavailable;
    if expected.identity != observed_identity
        || expected.length != observed.len()
        || expected_metadata != observed_metadata
    {
        return Err(CaptureError::Io(std::io::Error::other(
            "host file changed during capture",
        )));
    }
    Ok(())
}

fn ensure_current_host_node(
    source_root: &HostRoot,
    host_path: &Path,
    expected: &HostSnapshot,
) -> Result<(), CaptureError> {
    let observed = source_root.symlink_metadata(host_path)?;
    ensure_same_host_node(expected, &observed)
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod host_file_race_tests {
    use super::*;

    #[test]
    fn rejects_a_replaced_file_between_path_metadata_and_open()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let current = temporary.path().join("current");
        let replacement = temporary.path().join("replacement");
        std::fs::write(&current, b"first")?;
        std::fs::write(&replacement, b"other")?;
        let root = HostRoot::open(temporary.path())?;
        let expected = HostSnapshot::from_metadata(&root.symlink_metadata(Path::new("current"))?)?;
        std::fs::rename(&current, temporary.path().join("displaced"))?;
        std::fs::rename(&replacement, &current)?;
        let opened = root.open_file(Path::new("current"))?;
        assert!(matches!(
            ensure_same_host_node(&expected, &opened.metadata()?),
            Err(CaptureError::Io(_))
        ));
        Ok(())
    }

    #[test]
    fn rejects_a_changed_file_after_open() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let current = temporary.path().join("current");
        std::fs::write(&current, b"first")?;
        let root = HostRoot::open(temporary.path())?;
        let expected = HostSnapshot::from_metadata(&root.symlink_metadata(Path::new("current"))?)?;
        let opened = root.open_file(Path::new("current"))?;
        ensure_same_host_node(&expected, &opened.metadata()?)?;
        std::fs::write(&current, b"changed body")?;
        assert!(matches!(
            ensure_same_host_node(&expected, &opened.metadata()?),
            Err(CaptureError::Io(_))
        ));
        Ok(())
    }

    #[test]
    fn rejects_a_path_replaced_after_open() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let current = temporary.path().join("current");
        let replacement = temporary.path().join("replacement");
        std::fs::write(&current, b"first")?;
        std::fs::write(&replacement, b"other")?;
        let root = HostRoot::open(temporary.path())?;
        let expected = HostSnapshot::from_metadata(&root.symlink_metadata(Path::new("current"))?)?;
        let opened = root.open_file(Path::new("current"))?;
        std::fs::rename(&current, temporary.path().join("displaced"))?;
        std::fs::rename(&replacement, &current)?;
        let _held_metadata = opened.metadata()?;
        let path_matches =
            ensure_same_host_node(&expected, &root.symlink_metadata(Path::new("current"))?).is_ok();
        assert!(!path_matches);
        Ok(())
    }

    #[test]
    fn rejects_a_replaced_directory_before_capture_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let current = temporary.path().join("current");
        let replacement = temporary.path().join("replacement");
        std::fs::create_dir(&current)?;
        std::fs::create_dir(&replacement)?;
        let root = HostRoot::open(temporary.path())?;
        let expected = HostSnapshot::from_metadata(&root.symlink_metadata(Path::new("current"))?)?;
        std::fs::rename(&current, temporary.path().join("displaced"))?;
        std::fs::rename(&replacement, &current)?;
        assert!(matches!(
            ensure_current_host_node(&root, Path::new("current"), &expected),
            Err(CaptureError::Io(_))
        ));
        Ok(())
    }
}

struct StagedHostRanges {
    ranges: Vec<(HostDataRange, StagedContent)>,
    work: WorkCounters,
    bytes: u64,
}

async fn stage_host_ranges<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    stager: &ContentStager<A, O>,
    file: &mut cap_std::fs::File,
    ranges: &[HostDataRange],
    prior_work: WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<StagedHostRanges, OperationFailure<CaptureError>> {
    let mut staged = Vec::new();
    staged
        .try_reserve(ranges.len())
        .map_err(|_| OperationFailure::new(CaptureError::InvalidOptions, prior_work))?;
    let mut work = WorkCounters::default();
    let mut bytes = 0_u64;
    for range in ranges {
        let accumulated = prior_work
            .checked_add(work)
            .map_err(|error| OperationFailure::new(CaptureError::Work(error), prior_work))?;
        file.seek(SeekFrom::Start(range.offset))
            .map_err(|error| OperationFailure::new(error.into(), accumulated))?;
        let mut bounded = (&mut *file).take(range.length);
        let remaining = accumulated
            .remaining(budget)
            .map_err(|error| OperationFailure::new(CaptureError::Work(error), accumulated))?;
        let content = stager
            .stage(&mut bounded, range.length, remaining, cancellation)
            .await
            .map_err(|failure| map_engine_failure(failure, accumulated))?;
        work = add_work(work, content.work)?;
        let accumulated = prior_work
            .checked_add(work)
            .map_err(|error| OperationFailure::new(CaptureError::Work(error), prior_work))?;
        if content.value.logical_bytes() != range.length {
            return Err(OperationFailure::new(
                CaptureError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "host file changed while capturing a sparse range",
                )),
                accumulated,
            ));
        }
        bytes = bytes.checked_add(range.length).ok_or_else(|| {
            OperationFailure::new(CaptureError::Work(WorkError::Overflow), accumulated)
        })?;
        staged.push((*range, content.value));
    }
    Ok(StagedHostRanges {
        ranges: staged,
        work,
        bytes,
    })
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

/// Converts one exact namespace path to a host-relative path without lossy text projection.
///
/// Native profile names retain their platform representation. A foreign profile is accepted only
/// when every component has an exact representation on this host.
///
/// # Errors
///
/// Returns [`CaptureError::UnrepresentablePath`] when any component cannot be represented exactly.
pub fn namespace_to_host_path(path: &NamespacePath) -> Result<PathBuf, CaptureError> {
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
                .map(|pair| {
                    let [high, low] = pair else {
                        unreachable!("chunks_exact(2) always yields exactly 2-byte chunks")
                    };
                    u16::from_le_bytes([*high, *low])
                })
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
        NameEncoding::Utf8 | NameEncoding::PosixBytes => {
            let name = std::str::from_utf8(component.as_bytes())
                .map_err(|_| CaptureError::UnrepresentablePath)?;
            if name.contains(['/', '\\']) {
                return Err(CaptureError::UnrepresentablePath);
            }
            Ok(std::ffi::OsString::from(name))
        }
        NameEncoding::WindowsUtf16Le => {
            let units = component
                .as_bytes()
                .chunks_exact(2)
                .map(|unit| {
                    let [high, low] = unit else {
                        unreachable!("chunks_exact(2) always yields exactly 2-byte chunks")
                    };
                    u16::from_le_bytes([*high, *low])
                })
                .collect::<Vec<_>>();
            Ok(std::ffi::OsString::from_wide(&units))
        }
    }
}

#[cfg(all(test, windows))]
mod windows_name_tests {
    use super::*;

    #[test]
    fn posix_profile_names_round_trip_through_utf8_on_windows()
    -> Result<(), Box<dyn std::error::Error>> {
        let valid = LogicalName::new(
            NameEncoding::PosixBytes,
            "uni-é中.txt".as_bytes().to_vec(),
            255,
        )?;
        assert_eq!(
            capture_host_name(&valid)?,
            std::ffi::OsString::from("uni-é中.txt")
        );

        let invalid = LogicalName::new(NameEncoding::PosixBytes, vec![0xff], 255)?;
        assert!(matches!(
            capture_host_name(&invalid),
            Err(CaptureError::UnrepresentablePath)
        ));
        let alias = LogicalName::new(NameEncoding::PosixBytes, b"a\\b".to_vec(), 255)?;
        assert!(matches!(
            capture_host_name(&alias),
            Err(CaptureError::UnrepresentablePath)
        ));
        Ok(())
    }

    #[test]
    fn host_path_round_trips_through_windows_profile() -> Result<(), Box<dyn std::error::Error>> {
        let limits = crate::model::VolumeLimits {
            maximum_component_bytes: 510,
            ..crate::model::VolumeLimits::default()
        };
        let host = std::path::Path::new("src/ünïcøde/日本語.rs");
        let namespace = host_path_to_namespace(host, FilesystemProfile::Windows, limits)?;
        assert_eq!(namespace_to_host_path(&namespace)?, host);
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod unix_name_tests {
    use super::*;
    use std::os::unix::ffi::OsStrExt;

    #[test]
    fn host_path_round_trips_arbitrary_posix_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let raw = std::ffi::OsStr::from_bytes(b"src/\xff.rs");
        let host = std::path::Path::new(raw);
        let namespace = host_path_to_namespace(
            host,
            FilesystemProfile::Posix,
            crate::model::VolumeLimits::default(),
        )?;
        assert_eq!(namespace_to_host_path(&namespace)?, host);
        Ok(())
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
