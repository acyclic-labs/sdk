//! Bounded host-state capture into one atomic sparse checkout transaction.
//!
//! Capture poll frames retain their short-frame heap boundaries. Each actual
//! future layout is admitted before construction and stays in the invocation's
//! allocation ledger until the completed box is dropped.

use crate::kernel::{
    FileKind, FileMetadata, FileRecord, LogicalName, MetadataField, NameEncoding, NamespacePath,
};
use crate::model::FilesystemProfile;
use crate::native_host::{HostDataRange, HostRoot, HostStat, allocated_data_ranges_with_work};
use crate::native_host::NativeHostWork as _;
use crate::demand::{
    DemandError, DemandWork, NativeDemandReadMeter,
    native_demand_error,
};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, CancellationToken, Checkout,
    NativeRootIdentity, OperationFailure, OperationReceipt, StagedContent,
    WatchBatch, WatchChange, WatchEpoch, WatchInvalidationReason, WatchSequence, WorkBudget,
    WorkCounters, WorkError,
};

/// Returns the stable identity of a no-follow, capability-held capture root.
///
/// # Errors
///
/// Rejects symlink/reparse roots, non-directories, and platforms unable to
/// report a stable root identity.
pub fn capture_root_identity(path: &Path) -> Result<NativeRootIdentity, CaptureError> {
    let span = crate::obs::span!(
        INFO,
        "acyclic.fs.native_capture.root_identity",
        outcome = crate::obs::Empty,
        error.kind = crate::obs::Empty,
    );
    let result = crate::obs::scope(&span, || {
        HostRoot::open(path, &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)
            .map(|root| root.identity())
            .map_err(|_| CaptureError::InvalidOptions)
    });
    crate::obs::outcome_on(&span, result)
}
use std::path::{Path, PathBuf};
use thiserror::Error;

#[cfg(feature = "native-mount")]
pub(crate) const MAX_NATIVE_EXACT_CAPTURE_PATHS: usize = 65_536;

/// A native view may have host-generated timestamps that are not SDK metadata.
/// The held root identity binds this policy to the prepared view without a
/// second full-tree scan after materialization.
pub(crate) struct NativeViewBaseline {
    root_identity: NativeRootIdentity,
    read_only: bool,
    _allocation: Option<crate::NativeAllocation>,
}

impl NativeViewBaseline {
    #[cfg(any(feature = "native-mount", all(test, unix)))]
    pub(crate) fn new(root_identity: NativeRootIdentity) -> Self {
        Self {
            root_identity,
            read_only: false,
            _allocation: None,
        }
    }

    #[cfg(feature = "native-mount")]
    pub(crate) fn read_only(root_identity: NativeRootIdentity) -> Self {
        Self {
            root_identity,
            read_only: true,
            _allocation: None,
        }
    }

    /// Creates the actual shared baseline control on the original invocation.
    /// Arc aliases retain its claim until this physical witness is destroyed.
    #[cfg(feature = "native-mount")]
    pub(crate) fn new_with_native_work(
        root_identity: NativeRootIdentity,
        read_only: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<std::sync::Arc<Self>, crate::NativeTreeMaterializationError> {
        let allocation = work.claim_allocation(std::mem::size_of::<Self>() as u64, 1, crate::WorkCounters::UNCHARGED)?;
        Ok(std::sync::Arc::new(Self {
            root_identity, read_only, _allocation: Some(allocation),
        }))
    }
    /// No caller-owned input is adopted: scalars are inline and a metered
    /// shared witness already carries its own intrinsic allocation claim.
    pub(crate) const fn logical_owned_bytes(&self) -> u64 {
        0
    }


    fn restore_canonical_stamps(
        &self,
        root_identity: NativeRootIdentity,
        prior: FileMetadata,
        observed: &mut FileMetadata,
    ) {
        if self.root_identity != root_identity {
            return;
        }
        // Fields absent from the selected SDK generation were not restored by
        // materialization. Host-created defaults cannot become an apparent
        // source edit merely because the view was created or read.
        macro_rules! retain_unavailable {
            ($($field:ident),+ $(,)?) => {$(
                if matches!(prior.$field, MetadataField::Unavailable) {
                    observed.$field = MetadataField::Unavailable;
                }
            )+};
        }
        retain_unavailable!(
            posix_mode,
            posix_uid,
            posix_gid,
            posix_flags,
            windows_attributes,
            created_ns,
            modified_ns,
            accessed_ns,
            changed_ns,
            named_attributes,
            acl,
            security_descriptor
        );
        if self.read_only {
            observed.accessed_ns = prior.accessed_ns;
        }
        #[cfg(unix)]
        {
            observed.changed_ns = prior.changed_ns;
        }
        #[cfg(target_os = "linux")]
        {
            observed.created_ns = prior.created_ns;
        }
    }
}

#[cfg(all(test, unix))]
mod native_view_baseline_tests {
    use super::*;

    #[test]
    fn native_view_keeps_canonical_stamps_for_the_same_root()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("file"), b"body")?;
        let root = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let identity = root.identity();
        let baseline = NativeViewBaseline::new(identity);
        let snapshot = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("file"))?)?;
        let prior = FileMetadata {
            changed_ns: MetadataField::Value(-123),
            created_ns: MetadataField::Value(-456),
            ..FileMetadata::default()
        };
        let mut observed = snapshot.metadata;
        baseline.restore_canonical_stamps(
            identity,
            prior,
            &mut observed,
        );
        assert_eq!(observed.changed_ns, prior.changed_ns);
        #[cfg(target_os = "linux")]
        assert_eq!(observed.created_ns, prior.created_ns);

        let mut changed = snapshot;
        changed.metadata.changed_ns = MetadataField::Value(-789);
        let mut observed = changed.metadata;
        baseline.restore_canonical_stamps(
            identity,
            prior,
            &mut observed,
        );
        assert_eq!(observed.changed_ns, prior.changed_ns);

        let wrong_identity = NativeRootIdentity {
            device: identity.device,
            object: identity.object.wrapping_add(1),
        };
        let mut observed = snapshot.metadata;
        baseline.restore_canonical_stamps(
            wrong_identity,
            prior,
            &mut observed,
        );
        assert_eq!(observed.changed_ns, snapshot.metadata.changed_ns);
        Ok(())
    }
}

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
        prefixes.sort_unstable();
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

    #[test]
    fn checkout_union_deduplicates_before_enforcing_the_path_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let duplicate = path("/same")?;
        let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        let duplicates = clone_capture_paths(&[duplicate.clone(), duplicate.clone(), duplicate.clone()], None, &mut meter)?;
        let ordered = order_host_checkout_union(CaptureMap::new(), duplicates, 1, &mut meter)?;
        assert_eq!(ordered.len(), 1);
        assert!(ordered[0].0.is_none());
        assert_eq!(ordered[0].1, duplicate);
        Ok(())
    }

    #[test]
    fn scanned_set_charges_only_unique_paths_at_the_limit() -> Result<(), Box<dyn std::error::Error>>
    {
        let duplicate = path("/same")?;
        let first = duplicate.clone();
        let other = path("/other")?;
        let mut paths = CaptureSet::new();
        let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        meter.adopt(namespace_owned_bytes(&first)? + namespace_owned_bytes(&duplicate)? + namespace_owned_bytes(&other)?)?;
        append_scanned_path(
            &mut paths,
            first,
            1,
            &mut meter,
        )?;
        let charged = meter.work;
        append_scanned_path(&mut paths, duplicate, 1, &mut meter)?;
        assert_eq!(meter.work, charged.checked_add(WorkCounters { items_examined: 2, ..WorkCounters::UNCHARGED })?);
        let failure = append_scanned_path(
            &mut paths,
            other,
            1,
            &mut meter,
        )
        .err()
        .ok_or("distinct path exceeded the limit")?;
        assert!(matches!(failure, CaptureError::InvalidOptions));
        assert_eq!(meter.work, charged.checked_add(WorkCounters { items_examined: 4, ..WorkCounters::UNCHARGED })?);
        assert_eq!(paths.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn watch_hint_observes_only_missing_ancestors_of_a_nested_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("sub"))?;
        std::fs::write(temporary.path().join("sub/changed.txt"), b"changed")?;
        std::fs::write(temporary.path().join("sub/unrelated.txt"), b"unrelated")?;
        let workspace = crate::Fs::memory()
            .create_workspace("watch-ancestor", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let mut checkout = workspace.checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let changed = path("/sub/changed.txt")?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                original: Default::default(),
                epoch: WatchEpoch::from_u64(1),
                first_sequence: WatchSequence::from_u64(1),
                next_sequence: WatchSequence::from_u64(2),
                changes: vec![WatchChange::Modified(changed.clone())],
            },
            &CaptureOptions {
                source_root: temporary.path().to_path_buf(),
                expected_root_identity: capture_root_identity(temporary.path())?,
                maximum_paths: 2,
                maximum_extent_spans: 8,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        let token = CancellationToken::new();
        let parent = checkout
            .lookup_no_follow(&path("/sub")?, WorkBudget::UNBOUNDED, &token)
            .await?;
        assert_eq!(
            parent.value.record.map(|record| record.kind),
            Some(FileKind::Directory)
        );
        let file = checkout
            .lookup_no_follow(&changed, WorkBudget::UNBOUNDED, &token)
            .await?;
        assert_eq!(
            file.value.record.map(|record| record.kind),
            Some(FileKind::Regular)
        );
        let unrelated = checkout
            .lookup_no_follow(&path("/sub/unrelated.txt")?, WorkBudget::UNBOUNDED, &token)
            .await?;
        assert!(unrelated.value.record.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn a_modified_directory_keeps_what_it_holds() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("pkg"))?;
        std::fs::write(temporary.path().join("pkg/shared.rs"), b"VALUE = 1")?;
        let workspace = crate::Fs::memory()
            .create_workspace("watch-directory", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let mut checkout = workspace.checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let options = CaptureOptions {
            source_root: temporary.path().to_path_buf(),
            expected_root_identity: capture_root_identity(temporary.path())?,
            maximum_paths: 8,
            maximum_extent_spans: 8,
        };
        capture_baseline(
            &mut checkout,
            &options,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                original: Default::default(),
                epoch: WatchEpoch::from_u64(1),
                first_sequence: WatchSequence::from_u64(1),
                next_sequence: WatchSequence::from_u64(2),
                changes: vec![WatchChange::Modified(path("/pkg")?)],
            },
            &options,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        let file = checkout
            .lookup_no_follow(
                &path("/pkg/shared.rs")?,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?;
        assert_eq!(
            file.value.record.map(|record| record.kind),
            Some(FileKind::Regular)
        );
        Ok(())
    }

    #[tokio::test]
    async fn watch_rename_creates_an_unobserved_destination_parent_first()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("sub"))?;
        std::fs::write(temporary.path().join("old.txt"), b"old")?;
        let workspace = crate::Fs::memory()
            .create_workspace("watch-rename-parent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        workspace.write_text("/old.txt", "old").await?;
        let mut checkout = workspace.checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        std::fs::rename(
            temporary.path().join("old.txt"),
            temporary.path().join("sub/new.txt"),
        )?;
        let old = path("/old.txt")?;
        let new = path("/sub/new.txt")?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                original: Default::default(),
                epoch: WatchEpoch::from_u64(1),
                first_sequence: WatchSequence::from_u64(1),
                next_sequence: WatchSequence::from_u64(2),
                changes: vec![WatchChange::Renamed {
                    from: old.clone(),
                    to: new.clone(),
                }],
            },
            &CaptureOptions {
                source_root: temporary.path().to_path_buf(),
                expected_root_identity: capture_root_identity(temporary.path())?,
                maximum_paths: 2,
                maximum_extent_spans: 8,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        let token = CancellationToken::new();
        assert!(
            checkout
                .lookup_no_follow(&old, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
                .record
                .is_none()
        );
        assert!(
            checkout
                .lookup_no_follow(&new, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
                .record
                .is_some()
        );
        Ok(())
    }

    #[tokio::test]
    async fn watch_directory_rename_then_child_change_keeps_the_renamed_directory()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("old"))?;
        std::fs::write(temporary.path().join("old/file.txt"), b"old")?;
        let workspace = crate::Fs::memory()
            .create_workspace("watch-renamed-directory", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let mut transaction = workspace.begin_transaction(crate::IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.create_dir_all("/old").await?;
        transaction.write_text("/old/file.txt", "old").await?;
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let mut checkout = workspace.checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        assert!(
            checkout
                .lookup_no_follow(
                    &path("/old")?,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new()
                )
                .await?
                .value
                .record
                .is_some()
        );
        std::fs::rename(temporary.path().join("old"), temporary.path().join("new"))?;
        std::fs::write(temporary.path().join("new/file.txt"), b"new")?;
        let old = path("/old")?;
        let new = path("/new")?;
        let child = path("/new/file.txt")?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                original: Default::default(),
                epoch: WatchEpoch::from_u64(1),
                first_sequence: WatchSequence::from_u64(1),
                next_sequence: WatchSequence::from_u64(3),
                changes: vec![
                    WatchChange::Renamed {
                        from: old.clone(),
                        to: new.clone(),
                    },
                    WatchChange::Modified(child.clone()),
                ],
            },
            &CaptureOptions {
                source_root: temporary.path().to_path_buf(),
                expected_root_identity: capture_root_identity(temporary.path())?,
                maximum_paths: 3,
                maximum_extent_spans: 8,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        let token = CancellationToken::new();
        assert!(
            checkout
                .lookup_no_follow(&old, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
                .record
                .is_none()
        );
        assert_eq!(
            checkout
                .lookup_no_follow(&new, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
                .record
                .map(|record| record.kind),
            Some(FileKind::Directory)
        );
        assert_eq!(
            checkout
                .lookup_no_follow(&child, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
                .record
                .map(|record| record.kind),
            Some(FileKind::Regular)
        );
        Ok(())
    }

    /// A directory modified in place is covered by its entries' own events:
    /// its unnamed entries (unobserved in a lazy checkout) stay out, while a
    /// directory that arrived by rename brings its whole subtree.
    #[cfg(not(target_os = "macos"))]
    #[tokio::test]
    async fn watch_expands_only_directories_that_appeared() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        for directory in ["modified", "arrived"] {
            std::fs::create_dir(temporary.path().join(directory))?;
            std::fs::write(temporary.path().join(directory).join("inner.txt"), b"x")?;
        }
        let workspace = crate::Fs::memory()
            .create_workspace("watch-expansion", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let mut checkout = workspace.checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        capture_watch_batch_with_policy(
            &mut checkout,
            WatchBatch::Changes {
                original: Default::default(),
                epoch: WatchEpoch::from_u64(1),
                first_sequence: WatchSequence::from_u64(1),
                next_sequence: WatchSequence::from_u64(3),
                changes: vec![
                    WatchChange::Modified(path("/modified")?),
                    WatchChange::Arrived(path("/arrived")?),
                ],
            },
            &CaptureOptions {
                source_root: temporary.path().to_path_buf(),
                expected_root_identity: capture_root_identity(temporary.path())?,
                maximum_paths: 8,
                maximum_extent_spans: 8,
            },
            &CapturePolicy::allow_all(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        let token = CancellationToken::new();
        let mut captured = Vec::new();
        for candidate in [
            "/modified",
            "/modified/inner.txt",
            "/arrived",
            "/arrived/inner.txt",
        ] {
            if checkout
                .lookup_no_follow(&path(candidate)?, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
                .record
                .is_some()
            {
                captured.push(candidate);
            }
        }
        assert_eq!(captured, ["/modified", "/arrived", "/arrived/inner.txt"]);
        Ok(())
    }

    #[tokio::test]
    async fn watch_nested_file_replaces_a_stale_non_directory_ancestor()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("sub"))?;
        std::fs::write(temporary.path().join("sub/new.txt"), b"new")?;
        let workspace = crate::Fs::memory()
            .create_workspace("watch-stale-ancestor", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        workspace.write_text("/sub", "old").await?;
        let mut checkout = workspace.checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let child = path("/sub/new.txt")?;
        capture_watch_batch_with_policy(
            &mut checkout,
            WatchBatch::Changes {
                original: Default::default(),
                epoch: WatchEpoch::from_u64(1),
                first_sequence: WatchSequence::from_u64(1),
                next_sequence: WatchSequence::from_u64(2),
                changes: vec![WatchChange::Created(child.clone())],
            },
            &CaptureOptions {
                source_root: temporary.path().to_path_buf(),
                expected_root_identity: capture_root_identity(temporary.path())?,
                maximum_paths: 2,
                maximum_extent_spans: 8,
            },
            &CapturePolicy::allow_all(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        let token = CancellationToken::new();
        assert_eq!(
            checkout
                .lookup_no_follow(&path("/sub")?, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
                .record
                .map(|record| record.kind),
            Some(FileKind::Directory)
        );
        assert!(
            checkout
                .lookup_no_follow(&child, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
                .record
                .is_some()
        );
        Ok(())
    }

    #[tokio::test]
    async fn watch_rename_chain_observes_intermediate_parent()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("sub"))?;
        std::fs::write(temporary.path().join("final.txt"), b"old")?;
        let workspace = crate::Fs::memory()
            .create_workspace("watch-rename-chain", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        workspace.write_text("/old.txt", "old").await?;
        let mut checkout = workspace.checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                original: Default::default(),
                epoch: WatchEpoch::from_u64(1),
                first_sequence: WatchSequence::from_u64(1),
                next_sequence: WatchSequence::from_u64(3),
                changes: vec![
                    WatchChange::Renamed {
                        from: path("/old.txt")?,
                        to: path("/sub/intermediate.txt")?,
                    },
                    WatchChange::Renamed {
                        from: path("/sub/intermediate.txt")?,
                        to: path("/final.txt")?,
                    },
                ],
            },
            &CaptureOptions {
                source_root: temporary.path().to_path_buf(),
                expected_root_identity: capture_root_identity(temporary.path())?,
                maximum_paths: 3,
                maximum_extent_spans: 8,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        let token = CancellationToken::new();
        assert!(
            checkout
                .lookup_no_follow(&path("/old.txt")?, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
                .record
                .is_none()
        );
        assert!(
            checkout
                .lookup_no_follow(&path("/final.txt")?, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
                .record
                .is_some()
        );
        Ok(())
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
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum CaptureError {
    /// Capture bounds or source root are invalid.
    #[error("native capture options are invalid")]
    InvalidOptions,
    /// The exact capability opened for capture is not the admitted root.
    #[error("native capture root identity changed")]
    RootChanged,
    /// A held capture source no longer matches the observed version.
    #[error("native capture source version changed")]
    StaleVersion,
    /// Cancellation stopped capture before the next admitted producer operation.
    #[error("native capture cancelled")]
    Cancelled,
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
    /// Exact canonical namespace construction retained its validation cause.
    #[error(transparent)]
    Path(#[from] crate::kernel::NamespacePathError),
    /// Exact canonical logical-name construction retained its validation cause.
    #[error(transparent)]
    Name(#[from] crate::kernel::TreePageError),
    /// The original native invocation was poisoned; no new work was admitted.
    #[error("original native capture work state is poisoned")]
    OriginalWorkPoisoned,
    /// Constructing the canonical original native work boundary failed.
    #[error(transparent)]
    NativeKernel(crate::NativeTreeMaterializationError),
    /// The canonical original filesystem operation retained its typed cause.
    #[error(transparent)]
    Kernel(crate::FsError),
    /// A fixed capture/SDK progress invariant failed without formatting data.
    #[error("{0}")]
    Invariant(&'static str),
    /// Source filesystem operation failed.
    #[error("source filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
    /// A concrete capture-owned container could not allocate its capacity.
    #[error("native capture allocation failed: {0}")]
    Allocation(#[from] std::collections::TryReserveError),
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
///
/// Funds one original native invocation. Its state remains shared with held
/// payloads after terminal task joins; receipts snapshot that same invocation.
pub async fn capture_paths_with_policy<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    options: &CaptureOptions,
    policy: &CapturePolicy,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let span = crate::obs::span!(
        INFO,
        "acyclic.fs.native_capture.paths_with_policy",
        outcome = crate::obs::Empty,
        error.kind = crate::obs::Empty,
        work.items = crate::obs::Empty,
        work.bytes = crate::obs::Empty,
        work.durability = crate::obs::Empty,
        path_count = paths.len(),
    );
    let result = crate::obs::in_span(&span, async {
        let original = capture_original_work(budget, cancellation)?;
        let result = async {
            validate_path_count(paths, options).map_err(OperationFailure::before_work)?;
            let native = capture_native_original(&original)?;
            let scope = CaptureWorkScope::new(&native)?;
            let source_root = open_source_root(options, &mut capture_meter(&scope))?;
            let result = capture_paths_from_root(
                checkout, paths, policy, options.maximum_extent_spans,
                &source_root, budget, cancellation, &scope,
            ).await;
            drop(source_root);
            capture_scope_result(scope, result)
        }.await;
        capture_original_result(&original, result).map(|mut receipt| {
            receipt.value.work = receipt.work;
            receipt
        })
    }).await;
    crate::obs::measured_on(&span, result, |receipt| &receipt.work)
}

/// Captures a large explicit path set as bounded transactions on one private
/// checkout candidate. The caller sees either every path or none of them;
/// hard-link identities are shared across batches. `budget` applies to the
/// complete path set, not independently to each batch.
#[cfg(feature = "native-mount")]
pub(crate) async fn capture_paths_batched_with_baseline<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    options: &CaptureOptions,
    batch_size: usize,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let original = capture_original_work(budget, cancellation)?;
    capture_paths_batched_with_work(
        checkout, paths, options, batch_size, cancellation, baseline, &original,
    ).await
}

#[cfg(feature = "native-mount")]
pub(crate) async fn capture_paths_batched_with_work<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    options: &CaptureOptions,
    batch_size: usize,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
    original: &crate::demand::OriginalDemandWork,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let native = capture_native_original(original)?;
    let result = capture_paths_batched_with_native_work(
        checkout, paths, options, batch_size, cancellation, baseline, &native,
    ).await;
    capture_original_result(original, result).map(|mut captured| {
        captured.value.work = captured.work;
        captured
    })
}

#[cfg(feature = "native-mount")]
pub(crate) async fn capture_paths_batched_with_native_work<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    options: &CaptureOptions,
    batch_size: usize,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
    work: &crate::NativeMaterializationWork,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let scope = CaptureWorkScope::new(work)?;
    let result = capture_paths_batched_in_scope(
        checkout, paths, options, batch_size, cancellation, baseline, &scope,
    ).await;
    capture_scope_result(scope, result).map(|mut captured| {
        captured.value.work = captured.work;
        captured
    })
}

#[cfg(feature = "native-mount")]
async fn capture_paths_batched_in_scope<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    options: &CaptureOptions,
    batch_size: usize,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
    meter: &CaptureWorkScope,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let (budget, cancellation) = {
        let mut original = capture_meter(meter);
        validate_path_count(paths, options)
            .map_err(|error| OperationFailure::new(error, original.completed()))?;
        if batch_size == 0 || paths.len() > MAX_NATIVE_EXACT_CAPTURE_PATHS {
            return Err(OperationFailure::new(CaptureError::InvalidOptions, original.completed()));
        }
        cancellation.check()
            .map_err(|_| OperationFailure::new(CaptureError::Cancelled, original.completed()))?;
        original.check()
            .map_err(|error| OperationFailure::new(capture_demand_error(error), original.completed()))?;
        if paths.is_empty() {
            let work = original.completed();
            return Ok(OperationReceipt { value: CaptureReceipt { work, ..CaptureReceipt::default() }, work });
        }
        (original.leaf(|work| work.budget), original.leaf(|work| work.cancellation().clone()))
    };
    let result = capture_in_heap_with_meter(|| async move {
        let mut host_work = CaptureNativeHostWork::new(meter);
        let source_root = open_source_root(options, &mut host_work)?;
        let (mut receipt, mut candidate, mut states, unique) = {
            let mut original = capture_meter(meter);
            let mut unique = clone_capture_paths(paths, None, &mut original)
                .map_err(|error| OperationFailure::new(error, original.completed()))?;
            capture_sort_by(&mut unique, Ord::cmp, &mut original)
                .map_err(|error| OperationFailure::new(error, original.completed()))?;
            if capture_has_duplicate_paths(&unique, &mut original)
                .map_err(|error| OperationFailure::new(error, original.completed()))? {
                return Err(OperationFailure::new(CaptureError::InvalidOptions, original.completed()));
            }
            let mut receipt = CaptureReceipt { work: original.completed(), ..CaptureReceipt::default() };
            let candidate = capture_candidate(checkout, &mut receipt.work, &cancellation, &mut original)?;
            let states = capture_vec(unique.len(), &mut original)
                .map_err(|error| OperationFailure::new(error, original.completed()))?;
            (receipt, candidate, states, unique)
        };
        let observed = observe_unique_paths(unique, &source_root, budget, &cancellation, &mut host_work)?.value;
        receipt.work = host_work.completed();
        let observations_bytes = observed.capacity() as u64 * std::mem::size_of::<ObservedPath>() as u64;
        let mut observations = observed.into_iter();
        loop {
            let (page, paths, lookup_cancellation) = {
                let mut original = capture_meter(meter);
                receipt.work = original.completed();
                let count = observations.len().min(batch_size);
                let mut page = capture_vec(count, &mut original)
                    .map_err(|error| OperationFailure::new(error, original.completed()))?;
                page.extend(observations.by_ref().take(count));
                if page.is_empty() {
                    break;
                }
                let mut paths = capture_vec(page.len(), &mut original)
                    .map_err(|error| OperationFailure::new(error, original.completed()))?;
                for (_, path) in &page {
                    paths.push(clone_capture_path(path, &mut original)
                        .map_err(|error| OperationFailure::new(error, original.completed()))?);
                }
                receipt.work = original.completed();
                (page, paths, original.leaf(|work| work.cancellation().clone()))
            };
            let lookup = capture_sdk_in_heap_with_meter(
                |native| candidate.lookup_batch_no_follow_with_native_work(&paths, native, &lookup_cancellation),
                meter, &lookup_cancellation,
            ).await?;
            {
                let mut original = capture_meter(meter);
                receipt.work = lookup.work;
                let lookup = lookup.value;
                let page_bytes = page.capacity() as u64 * std::mem::size_of::<ObservedPath>() as u64;
                states.extend(
                    page.into_iter().zip(lookup.0.entries)
                        .map(|((observation, path), entry)| (observation, path, entry.record)),
                );
                drop(lookup.1);
                capture_step(&mut receipt.work, &mut original, |meter| {
                    meter.release(page_bytes).map_err(capture_demand_error)?;
                    release_capture_paths(paths, meter)
                })?;
            }
        }
        drop(observations);
        {
            let mut original = capture_meter(meter);
            receipt.work = original.completed();
            capture_step(&mut receipt.work, &mut original, |meter| meter.release(observations_bytes).map_err(capture_demand_error))?;
            capture_step(&mut receipt.work, &mut original, |meter| sort_capture_states(&mut states, meter))?;
        }
        let mut host_links = CaptureMap::new();
        let states_bytes = states.capacity() as u64 * std::mem::size_of::<CapturePathState>() as u64;
        let mut states = states.into_iter();
        loop {
            let batch = {
                let mut original = capture_meter(meter);
                let count = states.len().min(batch_size);
                let mut batch = capture_vec(count, &mut original)
                    .map_err(|error| OperationFailure::new(error, original.completed()))?;
                batch.extend(states.by_ref().take(count));
                receipt.work = original.completed();
                if batch.is_empty() {
                    break;
                }
                batch
            };
            let captured = capture_ranked_paths_from_root(
                &mut candidate, batch, &mut host_links, options.maximum_extent_spans,
                &source_root, budget, &cancellation, baseline, meter,
            ).await?;
            for source in host_links.values_mut() {
                if let HostLinkSource::Pending(path) = source {
                    let ready = std::mem::replace(path, NamespacePath::new(Vec::new(), checkout.volume_config().limits)
                        .map_err(|error| OperationFailure::new(CaptureError::Path(error), captured.work))?);
                    *source = HostLinkSource::Ready(ready);
                }
            }
            receipt.work = captured.work;
            receipt.examined_paths = receipt.examined_paths.checked_add(captured.value.examined_paths)
                .ok_or_else(|| OperationFailure::new(CaptureError::Work(WorkError::Overflow), receipt.work))?;
            receipt.changed_paths = receipt.changed_paths.checked_add(captured.value.changed_paths)
                .ok_or_else(|| OperationFailure::new(CaptureError::Work(WorkError::Overflow), receipt.work))?;
            receipt.staged_file_bytes = receipt.staged_file_bytes.checked_add(captured.value.staged_file_bytes)
                .ok_or_else(|| OperationFailure::new(CaptureError::Work(WorkError::Overflow), receipt.work))?;
        }
        drop(states);
        {
            let mut original = capture_meter(meter);
            receipt.work = original.completed();
            capture_step(&mut receipt.work, &mut original, |meter| {
                meter.release(states_bytes).map_err(capture_demand_error)?;
                release_host_links(host_links, meter)
            })?;
            original.check()
                .map_err(|error| OperationFailure::new(capture_demand_error(error), original.completed()))?;
        }
        cancellation.commit_if_live(|| { *checkout = candidate; })
            .map_err(|_| OperationFailure::new(CaptureError::Cancelled, capture_meter(meter).completed()))?;
        receipt.work = capture_meter(meter).completed();
        Ok(OperationReceipt { value: receipt, work: receipt.work })
    }, meter.original()).await;
    let work = capture_meter(meter).completed();
    match result {
        Ok(mut captured) => {
            captured.work = work;
            captured.value.work = work;
            Ok(captured)
        }
        Err(mut failure) => {
            *failure.work = work;
            Err(failure)
        }
    }
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
    let span = crate::obs::span!(
        INFO,
        "acyclic.fs.native_capture.subtrees_with_policy",
        outcome = crate::obs::Empty,
        error.kind = crate::obs::Empty,
        work.items = crate::obs::Empty,
        work.bytes = crate::obs::Empty,
        work.durability = crate::obs::Empty,
        root_count = roots.len(),
    );
    let result = crate::obs::in_span(
        &span,
        capture_subtrees_with_policy_and_baseline(
            checkout,
            roots,
            options,
            policy,
            budget,
            cancellation,
            None,
        ),
    )
    .await;
    crate::obs::measured_on(&span, result, |receipt| &receipt.work)
}

fn capture_original_work(
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<crate::demand::OriginalDemandWork, OperationFailure<CaptureError>> {
    crate::NativeMaterializationWork::new(budget, cancellation)
        .map(|work| work.original_demand_work())
        .map_err(|(error, completed)| OperationFailure::new(CaptureError::NativeKernel(error), completed))
}

fn capture_original_result<T>(
    original: &crate::demand::OriginalDemandWork,
    result: Result<OperationReceipt<T>, OperationFailure<CaptureError>>,
) -> Result<OperationReceipt<T>, OperationFailure<CaptureError>> {
    match (result, original.finish_operation(original.check())) {
        (Err(failure), Ok(completed)) =>
            Err(OperationFailure::new(failure.error, completed.work)),
        (Err(failure), Err(completed)) =>
            Err(OperationFailure::new(failure.error, *completed.work)),
        (Ok(mut receipt), Ok(completed)) => {
            receipt.work = completed.work;
            Ok(receipt)
        }
        (Ok(_), Err(failure)) =>
            Err(OperationFailure::new(capture_demand_error(failure.error), *failure.work)),
    }
}

fn capture_native_original(
    original: &crate::demand::OriginalDemandWork,
) -> Result<crate::NativeMaterializationWork, OperationFailure<CaptureError>> {
    crate::NativeMaterializationWork::from_original_demand_work(original).map_err(|error| {
        let failure = original.finish_operation::<()>(Err(error))
            .expect_err("failed original restoration cannot succeed");
        OperationFailure::new(capture_demand_error(failure.error), *failure.work)
    })
}

fn capture_ledger_meter(
    meter: &std::sync::Arc<NativeDemandReadMeter>,
) -> std::sync::MutexGuard<'_, DemandWork> {
    meter.lock()
}

struct CaptureWorkScope {
    original: std::sync::Arc<NativeDemandReadMeter>,
    native: crate::NativeMaterializationWork,
    custody: Option<std::sync::Arc<crate::demand::CaptureTransientCustody>>,
    owns_custody: bool,
}

impl CaptureWorkScope {
    fn new(native: &crate::NativeMaterializationWork)
        -> Result<Self, OperationFailure<CaptureError>>
    {
        let original = native.native_read_meter();
        let custody = crate::demand::CaptureTransientCustody::new_native(native)
            .map_err(|error| OperationFailure::new(CaptureError::NativeKernel(error), native.receipt()))?;
        let view = native.with_capture_custody(&custody)
            .map_err(|error| OperationFailure::new(CaptureError::NativeKernel(error), native.receipt()))?;
        Ok(Self {
            original: std::sync::Arc::clone(original),
            native: view,
            custody: Some(custody),
            owns_custody: true,
        })
    }

    fn original(&self) -> &std::sync::Arc<NativeDemandReadMeter> { &self.original }
    fn native(&self) -> &crate::NativeMaterializationWork { &self.native }

    fn transfer_allocation(&self, bytes: u64) -> Result<crate::NativeAllocation, CaptureError> {
        self.native.transfer_capture_allocation(bytes,
            self.custody.as_ref().expect("capture scope is live"))
            .map_err(CaptureError::NativeKernel)
    }


    fn release(&mut self) -> Result<(), OperationFailure<CaptureError>> {
        // Aliases do not own retirement. Accepted workers and native source
        // packets retain the token until their physical payloads have retired.
        if !std::mem::replace(&mut self.owns_custody, false) { return Ok(()); }
        let custody = self.custody.take().expect("capture scope is live");
        while std::sync::Arc::strong_count(&custody) != 1 {
            std::thread::yield_now();
        }
        let bytes = custody.take_retained_bytes();
        drop(custody);
        let (released, completed) = self.native.with_original_leaf(|work| {
            (work.with_capture(None, |work| work.release(bytes)), work.work)
        });
        released.map_err(|error| OperationFailure::new(capture_demand_error(error), completed))
    }
}

impl Clone for CaptureWorkScope {
    fn clone(&self) -> Self {
        Self {
            original: std::sync::Arc::clone(&self.original),
            native: self.native.clone(),
            custody: self.custody.as_ref().map(std::sync::Arc::clone),
            owns_custody: false,
        }
    }
}

impl Drop for CaptureWorkScope {
    fn drop(&mut self) { let _ = self.release(); }
}

fn capture_owned_leaf<T>(
    meter: &CaptureWorkScope, operation: impl FnOnce(&mut DemandWork) -> T,
) -> T {
    meter.native().with_original_leaf(operation)
}

fn capture_meter(meter: &CaptureWorkScope) -> CaptureNativeHostWork<'_> {
    CaptureNativeHostWork::new(meter)
}

/// The same invocation's host event carrier. No original lock or active
/// scratch attribution survives entry into an operating-system operation.
struct CaptureNativeHostWork<'a> { scope: &'a CaptureWorkScope }

impl<'a> CaptureNativeHostWork<'a> {
    fn new(scope: &'a CaptureWorkScope) -> Self { Self { scope } }
    fn scope(&self) -> &'a CaptureWorkScope { self.scope }
}

trait CaptureNativeWork:
    crate::native_host::NativeHostWork<Error = DemandError, Allocation = u64, CleanupWrite = u64>
{
    /// Only fixed ledger, cancellation, and counter bookkeeping belongs here.
    /// Allocation, copies, container mutation/drop, and host calls stay outside.
    fn leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T;
    fn completed(&mut self) -> WorkCounters { self.leaf(|work| work.work) }
}

impl CaptureNativeWork for DemandWork {
    fn leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T { operation(self) }
}

impl CaptureNativeWork for CaptureNativeHostWork<'_> {
    fn leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        capture_owned_leaf(self.scope, operation)
    }
}

impl crate::native_host::NativeHostWork for CaptureNativeHostWork<'_> {
    type Error = DemandError;
    type Allocation = u64;
    type CleanupWrite = u64;
    fn io_error_kind(error: &DemandError) -> Option<std::io::ErrorKind> {
        match error { DemandError::Io(error) => Some(error.kind()), _ => None }
    }
    fn invalid_request() -> DemandError { DemandError::InvalidRequest }
    fn allocation_failed(error: std::collections::TryReserveError) -> DemandError {
        DemandError::Allocation(error)
    }
    fn check(&self) -> Result<(), DemandError> { capture_owned_leaf(self.scope, |work| work.check()) }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        capture_owned_leaf(self.scope, |work| work.admit(delta))
    }
    fn record(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        capture_owned_leaf(self.scope, |work| work.record(delta))
    }
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        capture_owned_leaf(self.scope, |work| work.charge(delta))
    }
    fn claim(&mut self, bytes: u64) -> Result<u64, DemandError> {
        capture_owned_leaf(self.scope, |work| work.allocate(bytes))?;
        Ok(bytes)
    }
    fn reserve_opaque_decode(&mut self, bytes: u64) -> Result<u64, DemandError> {
        capture_owned_leaf(self.scope, |work| work.adopt(bytes))?;
        Ok(bytes)
    }
    fn reconcile_allocation(&mut self, allocation: &mut u64, actual: u64) -> Result<(), DemandError> {
        capture_owned_leaf(self.scope, |work| work.reconcile_native_allocation(allocation, actual))
    }
    fn release(&mut self, bytes: u64) -> Result<(), DemandError> {
        capture_owned_leaf(self.scope, |work| work.release(bytes))
    }
    fn attempt_io<T>(
        &mut self, delta: WorkCounters, operation: impl FnOnce() -> std::io::Result<T>,
    ) -> Result<T, DemandError> {
        // Consume the real attempt's allowance before a reentrant host can
        // spend it. The operation immediately follows with the lock retired.
        capture_owned_leaf(self.scope, |work| work.charge(delta))?;
        operation().map_err(DemandError::from)
    }
    fn reserve_cleanup_write(&mut self) -> Result<u64, DemandError> {
        acyclic_native_runtime::NativeReadMeter::reserve_cleanup_write(self.scope.native().runtime_meter())
            .map_err(native_demand_error)
    }
    fn cancel_cleanup_write(&mut self, reservation: u64) {
        acyclic_native_runtime::NativeReadMeter::cancel_cleanup_write(
            self.scope.native().runtime_meter(), reservation,
        );
    }
    fn attempt_cleanup_write<T>(
        &mut self, reservation: u64, operation: impl FnOnce() -> T,
    ) -> (T, Result<(), DemandError>) {
        let recorded = acyclic_native_runtime::NativeReadMeter::record_cleanup_write(
            self.scope.native().runtime_meter(), reservation,
        ).map_err(native_demand_error);
        (operation(), recorded)
    }
}

/// A stack-only projection onto the existing native runtime meter. Only
/// concrete capture-owned allocation events enable transient attribution.
struct CaptureNativeReadMeter<'a>(&'a CaptureWorkScope);

impl acyclic_native_runtime::NativeReadMeter for CaptureNativeReadMeter<'_> {
    fn check(&self) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::check(self.0.native().runtime_meter())
    }
    fn reserve_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::reserve_operations(self.0.native().runtime_meter(), kind, count)
    }
    fn cancel_reserved_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) {
        acyclic_native_runtime::NativeReadMeter::cancel_reserved_operations(self.0.native().runtime_meter(), kind, count)
    }
    fn record_reserved_operation(&self, kind: acyclic_native_runtime::NativeOperationKind) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::record_reserved_operation(self.0.native().runtime_meter(), kind)
    }
    fn admit_write_bytes(&self, maximum: usize) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::admit_write_bytes(self.0.native().runtime_meter(), maximum)
    }
    fn reserve_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::reserve_bytes(self.0.native().runtime_meter(), kind, maximum)
    }
    fn cancel_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64) {
        acyclic_native_runtime::NativeReadMeter::cancel_reserved_bytes(self.0.native().runtime_meter(), kind, maximum)
    }
    fn record_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64, actual: u64) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::record_reserved_bytes(self.0.native().runtime_meter(), kind, maximum, actual)
    }
    fn allocate(&self, bytes: u64) -> std::io::Result<()> {
        let result = capture_owned_leaf(self.0, |work| work.allocate(bytes));
        result.map_err(|error| self.0.original().relay_io_failure(error))
    }
    fn reserve_allocation(&self, bytes: u64) -> std::io::Result<()> {
        let result = self.0.native().with_original_leaf(|work| work.reserve_native_allocation(bytes));
        result.map_err(|error| self.0.original().relay_io_failure(error))
    }
    fn cancel_reserved_allocation(&self, bytes: u64) {
        self.0.native().with_original_leaf(|work| {
            if let Err(error) = work.cancel_reserved_native_allocation(bytes) {
                work.record_native_io_failure(error);
                work.poison_native_original_work();
            }
        });
    }
    fn record_reserved_allocation(&self, bytes: u64) -> std::io::Result<()> {
        let result = self.0.native().with_original_leaf(|work| work.record_reserved_native_allocation(bytes));
        result.map_err(|error| self.0.original().relay_io_failure(error))
    }
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> std::io::Error {
        acyclic_native_runtime::NativeReadMeter::allocation_failed(self.0.native().runtime_meter(), error)
    }
    fn allocate_refcount_header(&self) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::allocate_refcount_header(self.0.native().runtime_meter())
    }
    fn adopt(&self, bytes: u64) -> std::io::Result<()> {
        let result = capture_owned_leaf(self.0, |work| work.adopt(bytes));
        result.map_err(|error| self.0.original().relay_io_failure(error))
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
        let (claimed, result) = capture_owned_leaf(self.0, |work| work.record_native_owned_completed(bytes, &work.capture_custody()));
        (claimed, result.map_err(|error| self.0.original().relay_io_failure(error)))
    }
    fn admit_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::admit_codec(self.0.native().runtime_meter(), encoded, copied, hashed)
    }
    fn record_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::record_codec(self.0.native().runtime_meter(), encoded, copied, hashed)
    }
    fn examine(&self, items: u64, copied: u64) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::examine(self.0.native().runtime_meter(), items, copied)
    }
    fn admit_examination(&self, items: u64, copied: u64) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::admit_examination(self.0.native().runtime_meter(), items, copied)
    }
    fn record_examination(&self, items: u64, copied: u64) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::record_examination(self.0.native().runtime_meter(), items, copied)
    }
    fn release(&self, bytes: u64) {
        capture_meter(self.0).release(bytes).expect("capture native allocation ownership balances");
    }
    fn admit_bytes(&self, maximum: usize) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::admit_bytes(self.0.native().runtime_meter(), maximum)
    }
    fn admit_read(&self, maximum: usize) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::admit_read(self.0.native().runtime_meter(), maximum)
    }
    fn admit_operations(&self, count: u64) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::admit_operations(self.0.native().runtime_meter(), count)
    }
    fn record_operations(&self, count: u64) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::record_operations(self.0.native().runtime_meter(), count)
    }
    fn record_read(&self, bytes: usize) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::record_read(self.0.native().runtime_meter(), bytes)
    }
    fn finish_read(&self, requested: usize, completed: usize) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::finish_read(self.0.native().runtime_meter(), requested, completed)
    }
    fn admit_write(&self, maximum: usize) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::admit_write(self.0.native().runtime_meter(), maximum)
    }
    fn record_write(&self, bytes: usize) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::record_write(self.0.native().runtime_meter(), bytes)
    }
    fn admit_write_operations(&self, count: u64) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::admit_write_operations(self.0.native().runtime_meter(), count)
    }
    fn record_write_operations(&self, count: u64) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::record_write_operations(self.0.native().runtime_meter(), count)
    }
    fn admit_durability(&self) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::admit_durability(self.0.native().runtime_meter())
    }
    fn reserve_cleanup_write(&self) -> std::io::Result<u64> {
        acyclic_native_runtime::NativeReadMeter::reserve_cleanup_write(self.0.native().runtime_meter())
    }
    fn cancel_cleanup_write(&self, reservation: u64) {
        acyclic_native_runtime::NativeReadMeter::cancel_cleanup_write(self.0.native().runtime_meter(), reservation);
    }
    fn record_cleanup_write(&self, reservation: u64) -> std::io::Result<()> {
        acyclic_native_runtime::NativeReadMeter::record_cleanup_write(self.0.native().runtime_meter(), reservation)
    }
}

struct CapturedRuntimeReadMeter {
    base: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    // The actual proxy body retires its claim before signalling scope
    // quiescence. Buffer, completion, and tail guards retain only `base`.
    _header: crate::NativeAllocation,
    _capture: std::sync::Arc<crate::demand::CaptureTransientCustody>,
}

pub(crate) fn capture_runtime_meter_owner(
    native: &crate::NativeMaterializationWork,
    capture: &std::sync::Arc<crate::demand::CaptureTransientCustody>,
) -> Result<std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>, crate::NativeTreeMaterializationError> {
    if !capture.owns_native_work(native) {
        return Err(crate::NativeTreeMaterializationError::InvalidLayout);
    }
    let header = native.claim(std::mem::size_of::<CapturedRuntimeReadMeter>() as u64)?;
    let base = native.runtime_meter_owner();
    Ok(std::sync::Arc::new(CapturedRuntimeReadMeter {
        base, _header: header, _capture: std::sync::Arc::clone(capture),
    }))
}

impl acyclic_native_runtime::NativeReadMeter for CapturedRuntimeReadMeter {
    fn allocation_owner(&self) -> Option<std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>> {
        Some(self.base.allocation_owner().unwrap_or_else(|| std::sync::Arc::clone(&self.base)))
    }
    fn check(&self) -> std::io::Result<()> { self.base.check() }
    fn reserve_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) -> std::io::Result<()> {
        self.base.reserve_operations(kind, count)
    }
    fn cancel_reserved_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) {
        self.base.cancel_reserved_operations(kind, count);
    }
    fn record_reserved_operation(&self, kind: acyclic_native_runtime::NativeOperationKind) -> std::io::Result<()> {
        self.base.record_reserved_operation(kind)
    }
    fn admit_write_bytes(&self, maximum: usize) -> std::io::Result<()> { self.base.admit_write_bytes(maximum) }
    fn reserve_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64) -> std::io::Result<()> {
        self.base.reserve_bytes(kind, maximum)
    }
    fn cancel_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64) {
        self.base.cancel_reserved_bytes(kind, maximum);
    }
    fn record_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64, actual: u64) -> std::io::Result<()> {
        self.base.record_reserved_bytes(kind, maximum, actual)
    }
    fn allocate(&self, bytes: u64) -> std::io::Result<()> { self.base.allocate(bytes) }
    fn reserve_allocation(&self, bytes: u64) -> std::io::Result<()> { self.base.reserve_allocation(bytes) }
    fn cancel_reserved_allocation(&self, bytes: u64) { self.base.cancel_reserved_allocation(bytes); }
    fn record_reserved_allocation(&self, bytes: u64) -> std::io::Result<()> { self.base.record_reserved_allocation(bytes) }
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> std::io::Error { self.base.allocation_failed(error) }
    fn allocate_refcount_header(&self) -> std::io::Result<()> { self.base.allocate_refcount_header() }
    fn adopt(&self, bytes: u64) -> std::io::Result<()> { self.base.adopt(bytes) }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) { self.base.record_completed_adoption(bytes) }
    fn admit_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
        self.base.admit_codec(encoded, copied, hashed)
    }
    fn record_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
        self.base.record_codec(encoded, copied, hashed)
    }
    fn examine(&self, items: u64, copied: u64) -> std::io::Result<()> { self.base.examine(items, copied) }
    fn admit_examination(&self, items: u64, copied: u64) -> std::io::Result<()> { self.base.admit_examination(items, copied) }
    fn record_examination(&self, items: u64, copied: u64) -> std::io::Result<()> { self.base.record_examination(items, copied) }
    fn release(&self, bytes: u64) { self.base.release(bytes); }
    fn admit_bytes(&self, maximum: usize) -> std::io::Result<()> { self.base.admit_bytes(maximum) }
    fn admit_read(&self, maximum: usize) -> std::io::Result<()> { self.base.admit_read(maximum) }
    fn admit_operations(&self, count: u64) -> std::io::Result<()> { self.base.admit_operations(count) }
    fn record_operations(&self, count: u64) -> std::io::Result<()> { self.base.record_operations(count) }
    fn record_read(&self, bytes: usize) -> std::io::Result<()> { self.base.record_read(bytes) }
    fn finish_read(&self, requested: usize, completed: usize) -> std::io::Result<()> {
        self.base.finish_read(requested, completed)
    }
    fn admit_write(&self, maximum: usize) -> std::io::Result<()> { self.base.admit_write(maximum) }
    fn record_write(&self, bytes: usize) -> std::io::Result<()> { self.base.record_write(bytes) }
    fn admit_write_operations(&self, count: u64) -> std::io::Result<()> { self.base.admit_write_operations(count) }
    fn record_write_operations(&self, count: u64) -> std::io::Result<()> { self.base.record_write_operations(count) }
    fn admit_durability(&self) -> std::io::Result<()> { self.base.admit_durability() }
    fn reserve_cleanup_write(&self) -> std::io::Result<u64> { self.base.reserve_cleanup_write() }
    fn cancel_cleanup_write(&self, reservation: u64) { self.base.cancel_cleanup_write(reservation); }
    fn record_cleanup_write(&self, reservation: u64) -> std::io::Result<()> { self.base.record_cleanup_write(reservation) }
}

#[cfg(windows)]
fn native_device_control_in_scope(
    file: &impl std::os::windows::io::AsRawHandle,
    code: u32, input: &[u8], output: &mut [u8], scope: &CaptureWorkScope,
) -> Result<usize, DemandError> {
    acyclic_native_runtime::device_control_in_place(file, code, input, output, Some((&CaptureNativeReadMeter(scope), acyclic_native_runtime::NativeOperationKind::Read))).map_err(|error| scope.original().take_native_io_failure()
        .unwrap_or_else(|| native_demand_error(error)))
}

fn capture_scope_result<T>(
    mut scope: CaptureWorkScope,
    result: Result<OperationReceipt<T>, OperationFailure<CaptureError>>,
) -> Result<OperationReceipt<T>, OperationFailure<CaptureError>> {
    let released = scope.release();
    let completed = capture_ledger_meter(scope.original()).work;
    match result {
        Err(failure) => Err(OperationFailure::new(failure.error, completed)),
        Ok(mut receipt) => {
            released?;
            receipt.work = completed;
            Ok(receipt)
        }
    }
}

struct CaptureHeapClaim {
    meter: std::sync::Arc<NativeDemandReadMeter>,
    bytes: Option<u64>,
}

impl CaptureHeapClaim {
    fn release(&mut self) -> Result<(), OperationFailure<CaptureError>> {
        let Some(bytes) = self.bytes.take() else { return Ok(()); };
        let (released, completed) = {
            let mut work = capture_ledger_meter(&self.meter);
            (work.release(bytes), work.work)
        };
        released.map_err(|error| OperationFailure::new(capture_demand_error(error), completed))
    }
}

impl Drop for CaptureHeapClaim {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

async fn capture_in_heap_with_meter<F, T>(
    create: impl FnOnce() -> F,
    meter: &std::sync::Arc<NativeDemandReadMeter>,
) -> Result<OperationReceipt<T>, OperationFailure<CaptureError>>
where
    F: std::future::Future<Output = Result<OperationReceipt<T>, OperationFailure<CaptureError>>>,
{
    // The claim precedes the box: cancellation, dropping, or unwinding destroys
    // the actual future before releasing its original-ledger ownership.
    let mut claim = CaptureHeapClaim { meter: std::sync::Arc::clone(meter), bytes: None };
    let (mut future, _) = crate::heap_future::in_heap_admitted(
        &mut claim,
        |claim, bytes| {
            let (allocated, completed) = {
                let mut work = capture_ledger_meter(&claim.meter);
                (if bytes != 0 { capture_allocate(&mut *work, bytes) } else { Ok(()) }, work.work)
            };
            allocated.map_err(|error| OperationFailure::new(error, completed))?;
            claim.bytes = Some(bytes);
            Ok::<(), OperationFailure<CaptureError>>(())
        },
        |_| create(),
    )?;
    let result = future.as_mut().await;
    drop(future);
    claim.release()?;
    let completed = capture_ledger_meter(meter).work;
    result
        .map(|mut receipt| { receipt.work = completed; receipt })
        .map_err(|failure| OperationFailure::new(failure.error, completed))
}

async fn capture_sdk_in_heap_with_meter<'a, F, T>(
    create: impl FnOnce(&'a crate::NativeMaterializationWork) -> F,
    meter: &'a CaptureWorkScope,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<T>, OperationFailure<CaptureError>>
where
    F: std::future::Future<Output = Result<T, crate::FsError>>,
{
    capture_in_heap_with_meter(
        || async {
            cancellation.check().map_err(|_| OperationFailure::new(
                CaptureError::Cancelled, capture_meter(meter).completed()))?;
            meter.native().admit(WorkCounters::UNCHARGED).map_err(|error| OperationFailure::new(
                CaptureError::NativeKernel(error), capture_meter(meter).completed()))?;
            let value = create(meter.native()).await.map_err(|error| OperationFailure::new(
                CaptureError::Kernel(error), capture_meter(meter).completed()))?;
            cancellation.check().map_err(|_| OperationFailure::new(
                CaptureError::Cancelled, capture_meter(meter).completed()))?;
            meter.native().admit(WorkCounters::UNCHARGED).map_err(|error| OperationFailure::new(
                CaptureError::NativeKernel(error), capture_meter(meter).completed()))?;
            Ok(OperationReceipt { value, work: capture_meter(meter).completed() })
        },
        meter.original(),
    ).await
}

pub(crate) async fn capture_subtrees_with_policy_and_baseline<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    checkout: &mut Checkout<A, O>,
    roots: &[NamespacePath],
    options: &CaptureOptions,
    policy: &CapturePolicy,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let original = capture_original_work(budget, cancellation)?;
    capture_subtrees_with_work(checkout, roots, options, policy, cancellation, baseline, &original).await
}

pub(crate) async fn capture_subtrees_with_work<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    checkout: &mut Checkout<A, O>,
    roots: &[NamespacePath],
    options: &CaptureOptions,
    policy: &CapturePolicy,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
    original: &crate::demand::OriginalDemandWork,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let native = capture_native_original(original)?;
    let result = capture_subtrees_with_native_work(
        checkout, roots, options, policy, cancellation, baseline, &native,
    ).await;
    capture_original_result(original, result).map(|mut captured| {
        captured.value.work = captured.work;
        captured
    })
}

pub(crate) async fn capture_subtrees_with_native_work<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    roots: &[NamespacePath],
    options: &CaptureOptions,
    policy: &CapturePolicy,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
    work: &crate::NativeMaterializationWork,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let scope = CaptureWorkScope::new(work)?;
    let result = capture_subtrees_in_scope(
        checkout, roots, options, policy, cancellation, baseline, &scope,
    ).await;
    capture_scope_result(scope, result).map(|mut captured| {
        captured.value.work = captured.work;
        captured
    })
}

async fn capture_subtrees_in_scope<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    roots: &[NamespacePath],
    options: &CaptureOptions,
    policy: &CapturePolicy,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
    meter: &CaptureWorkScope,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let (budget, cancellation) = {
        let mut original = capture_meter(meter);
        if options.maximum_paths == 0 || options.maximum_extent_spans == 0 {
            return Err(OperationFailure::new(CaptureError::InvalidOptions, original.completed()));
        }
        cancellation.check()
            .map_err(|_| OperationFailure::new(CaptureError::Cancelled, original.completed()))?;
        original.check()
            .map_err(|error| OperationFailure::new(capture_demand_error(error), original.completed()))?;
        if roots.is_empty() {
            let work = original.completed();
            return Ok(OperationReceipt { value: CaptureReceipt { work, ..CaptureReceipt::default() }, work });
        }
        (original.leaf(|work| work.budget), original.leaf(|work| work.cancellation().clone()))
    };
    let result = capture_in_heap_with_meter(|| async move {
        let limits = checkout.volume_config().limits;
        let profile = checkout.volume_config().profile;
        let (canonical, mut paths, host_roots, host_policy, source_path, source_path_bytes, maximum) = {
            let mut original = capture_meter(meter);
            let canonical = canonical_subtree_roots_measured(roots, policy, &mut original)
                .map_err(|error| OperationFailure::new(error, original.completed()))?;
            if canonical.is_empty() {
                let work = original.completed();
                return Ok(OperationReceipt { value: CaptureReceipt { work, ..CaptureReceipt::default() }, work });
            }
            let maximum = usize::try_from(options.maximum_paths)
                .map_err(|_| OperationFailure::new(CaptureError::InvalidOptions, original.completed()))?;
            let paths = clone_capture_paths(&canonical, None, &mut original)
                .map_err(|error| OperationFailure::new(error, original.completed()))?;
            let host_roots = clone_capture_paths(&canonical, None, &mut original)
                .map_err(|error| OperationFailure::new(error, original.completed()))?;
            let host_policy = CapturePolicy {
                excluded_prefixes: clone_capture_paths(&policy.excluded_prefixes, None, &mut original)
                    .map_err(|error| OperationFailure::new(error, original.completed()))?,
            };
            let (source_path, source_path_bytes) = crate::native_host::clone_path_with_work(&options.source_root, &mut original)
                .map_err(|error| OperationFailure::new(capture_demand_error(error), original.completed()))?;
            (canonical, paths, host_roots, host_policy, source_path, source_path_bytes, maximum)
        };
        let expected_identity = options.expected_root_identity;
        let host_cancellation = cancellation.clone();
        let worker = meter.clone();
        let task_meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter> = meter.original().clone();
        let task = acyclic_native_runtime::run_blocking_io(move || {
            let mut host_work = CaptureNativeHostWork::new(&worker);
            let result = (|| {
                let source_root = HostRoot::open(&source_path, &mut host_work)
                    .map_err(|error| OperationFailure::new(capture_demand_error(error), host_work.completed()))?;
                if source_root.identity() != expected_identity {
                    return Err(OperationFailure::new(CaptureError::RootChanged, host_work.completed()));
                }
                let mut observed = CaptureMap::new();
                let mut work = host_work.completed();
                collect_host_subtree_roots(
                    &source_root, profile, limits, maximum, &host_roots, &host_policy,
                    &mut observed, &mut work, budget, &host_cancellation, &mut host_work,
                )?;
                Ok((source_root, observed))
            })();
            let cleanup = (|| {
                release_capture_paths(host_roots, &mut host_work)?;
                release_capture_paths(host_policy.excluded_prefixes, &mut host_work)?;
                drop(source_path);
                host_work.release(source_path_bytes).map_err(capture_demand_error)
            })().map_err(|error| OperationFailure::new(error, host_work.completed()));
            match result {
                Ok(value) => {
                    cleanup?;
                    Ok(value)
                }
                Err(mut failure) => {
                    *failure.work = host_work.completed();
                    Err(failure)
                }
            }
        }, Some((task_meter, false)));
        let result = match task {
            Ok(task) => task.await.map_err(native_demand_error),
            Err(error) => Err(native_demand_error(error)),
        };
        let (source_root, observed) = {
            let mut original = capture_meter(meter);
            result.map_err(|error| OperationFailure::new(capture_demand_error(error), original.completed()))?
                .map_err(|mut failure| {
                    *failure.work = original.completed();
                    failure
                })?
        };
        let lookup_cancellation = capture_meter(meter).leaf(|work| work.cancellation().clone());
        let current = capture_sdk_in_heap_with_meter(
            |native| checkout.lookup_batch_no_follow_with_native_work(&canonical, native, &lookup_cancellation),
            meter, &lookup_cancellation,
        ).await?;
        let mut work = current.work;
        let current = current.value;
        let canonical_bytes = canonical.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
        for (root, entry) in canonical.into_iter().zip(current.0.entries) {
            if entry.record.is_some_and(|record| record.kind == FileKind::Directory) {
                collect_checkout_subtree_paths(
                    checkout, limits, root, policy, maximum, &mut paths, &mut work,
                    budget, &cancellation, meter,
                ).await?;
            } else {
                let mut original = capture_meter(meter);
                work = original.completed();
                capture_step(&mut work, &mut original, |meter| capture_drop_path(root, meter))?;
            }
        }
        drop(current.1);
        let ordered = {
            let mut original = capture_meter(meter);
            work = original.completed();
            capture_step(&mut work, &mut original, |meter| meter.release(canonical_bytes).map_err(capture_demand_error))?;
            order_host_checkout_union(observed, paths, maximum, &mut original)
                .map_err(|error| OperationFailure::new(error, original.completed()))?
        };
        let mut captured = capture_observed_paths_from_root(
            checkout, ordered, options.maximum_extent_spans, &source_root,
            budget, &cancellation, baseline, meter,
        ).await?;
        captured.value.work = captured.work;
        Ok(captured)
    }, meter.original()).await;
    let work = capture_meter(meter).completed();
    match result {
        Ok(mut captured) => {
            captured.work = work;
            captured.value.work = work;
            Ok(captured)
        }
        Err(mut failure) => {
            *failure.work = work;
            Err(failure)
        }
    }
}

fn capture_demand_error(error: DemandError) -> CaptureError {
    match error {
        DemandError::Work(error) => CaptureError::Work(error),
        DemandError::Io(error) => CaptureError::Io(error),
        DemandError::Allocation(error) => CaptureError::Allocation(error),
        DemandError::StaleVersion => CaptureError::StaleVersion,
        DemandError::Cancelled => CaptureError::Cancelled,
        DemandError::OriginalWorkPoisoned => CaptureError::OriginalWorkPoisoned,
        _ => CaptureError::InvalidOptions,
    }
}

trait CaptureKey: Ord {
    fn owned_bytes(&self) -> Result<u64, CaptureError>;
}

impl CaptureKey for NamespacePath {
    fn owned_bytes(&self) -> Result<u64, CaptureError> { namespace_owned_bytes(self) }
}

impl CaptureKey for [u8; 16] {
    fn owned_bytes(&self) -> Result<u64, CaptureError> { Ok(0) }
}

struct CaptureMap<K, V> {
    entries: Vec<(K, V)>,
}

impl<K: CaptureKey, V> CaptureMap<K, V> {
    const fn new() -> Self { Self { entries: Vec::new() } }
    fn len(&self) -> usize { self.entries.len() }
    fn is_empty(&self) -> bool { self.entries.is_empty() }
    fn storage_bytes(&self) -> u64 {
        self.entries.capacity() as u64 * std::mem::size_of::<(K, V)>() as u64
    }
    fn position_measured(&self, key: &K, meter: &mut impl CaptureNativeWork) -> Result<Result<usize, usize>, CaptureError> {
        let (mut low, mut high) = (0, self.entries.len());
        while low < high {
            capture_charge(meter, WorkCounters { items_examined: 2, ..WorkCounters::UNCHARGED })?;
            let middle = low + (high - low) / 2;
            match self.entries[middle].0.cmp(key) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(Ok(middle)),
            }
        }
        Ok(Err(low))
    }
    fn contains_key_measured(&self, key: &K, meter: &mut impl CaptureNativeWork) -> Result<bool, CaptureError> {
        Ok(self.position_measured(key, meter)?.is_ok())
    }
    fn get_measured(&self, key: &K, meter: &mut impl CaptureNativeWork) -> Result<Option<&V>, CaptureError> {
        Ok(self.position_measured(key, meter)?.ok().map(|index| &self.entries[index].1))
    }
    fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.entries.iter().map(|entry| (&entry.0, &entry.1))
    }
    fn keys(&self) -> impl Iterator<Item = &K> {
        self.entries.iter().map(|entry| &entry.0)
    }
    fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.entries.iter_mut().map(|entry| &mut entry.1)
    }
    fn insert_at(&mut self, index: usize, key: K, value: V, meter: &mut impl CaptureNativeWork) -> Result<(), CaptureError> {
        let copied = (self.entries.len() - index).checked_mul(std::mem::size_of::<(K, V)>())
            .ok_or(CaptureError::Work(WorkError::Overflow))? as u64;
        let delta = WorkCounters { bytes_copied: copied, ..WorkCounters::UNCHARGED };
        capture_reserve(&mut self.entries, 1, meter)?;
        capture_charge(meter, delta)?;
        self.entries.insert(index, (key, value));
        Ok(())
    }
    fn insert_measured(&mut self, key: K, value: V, meter: &mut impl CaptureNativeWork) -> Result<Option<V>, CaptureError> {
        match self.position_measured(&key, meter)? {
            Ok(index) => {
                let bytes = key.owned_bytes()?;
                drop(key);
                meter.leaf(|work| work.release(bytes)).map_err(capture_demand_error)?;
                Ok(Some(std::mem::replace(&mut self.entries[index].1, value)))
            }
            Err(index) => {
                self.insert_at(index, key, value, meter)?;
                Ok(None)
            }
        }
    }
    fn get_or_insert_measured(&mut self, key: K, value: V, meter: &mut impl CaptureNativeWork) -> Result<&mut V, CaptureError> {
        let index = match self.position_measured(&key, meter)? {
            Ok(index) => {
                let bytes = key.owned_bytes()?;
                drop(key);
                meter.leaf(|work| work.release(bytes)).map_err(capture_demand_error)?;
                index
            }
            Err(index) => {
                self.insert_at(index, key, value, meter)?;
                index
            }
        };
        Ok(&mut self.entries[index].1)
    }
    fn remove_measured(&mut self, key: &K, meter: &mut impl CaptureNativeWork) -> Result<Option<(K, V)>, CaptureError> {
        let Ok(index) = self.position_measured(key, meter)? else { return Ok(None) };
        let copied = (self.entries.len() - index - 1).checked_mul(std::mem::size_of::<(K, V)>())
            .ok_or(CaptureError::Work(WorkError::Overflow))? as u64;
        capture_charge(meter, WorkCounters { bytes_copied: copied, ..WorkCounters::UNCHARGED })?;
        Ok(Some(self.entries.remove(index)))
    }
}

impl<K, V> IntoIterator for CaptureMap<K, V> {
    type Item = (K, V);
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter { self.entries.into_iter() }
}
impl<'a, K, V> IntoIterator for &'a mut CaptureMap<K, V> {
    type Item = (&'a K, &'a mut V);
    type IntoIter = std::iter::Map<std::slice::IterMut<'a, (K, V)>, fn(&'a mut (K, V)) -> Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter_mut().map(|entry| (&entry.0, &mut entry.1))
    }
}


struct CaptureSet<K> {
    map: CaptureMap<K, ()>,
}

impl<K: CaptureKey> CaptureSet<K> {
    const fn new() -> Self { Self { map: CaptureMap::new() } }
    fn len(&self) -> usize { self.map.len() }
    fn contains_measured(&self, key: &K, meter: &mut impl CaptureNativeWork) -> Result<bool, CaptureError> {
        self.map.contains_key_measured(key, meter)
    }
    fn storage_bytes(&self) -> u64 { self.map.storage_bytes() }
    fn insert_measured(&mut self, key: K, meter: &mut impl CaptureNativeWork) -> Result<(), CaptureError> {
        self.map.insert_measured(key, (), meter).map(|_| ())
    }
    fn remove_measured(&mut self, key: &K, meter: &mut impl CaptureNativeWork) -> Result<Option<K>, CaptureError> {
        self.map.remove_measured(key, meter).map(|entry| entry.map(|(key, ())| key))
    }
}

impl<K> IntoIterator for CaptureSet<K> {
    type Item = K;
    type IntoIter = std::iter::Map<std::vec::IntoIter<(K, ())>, fn((K, ())) -> K>;
    fn into_iter(self) -> Self::IntoIter { self.map.into_iter().map(|(key, ())| key) }
}

fn capture_sort_by<T>(
    values: &mut [T],
    mut compare: impl FnMut(&T, &T) -> std::cmp::Ordering,
    meter: &mut impl CaptureNativeWork,
) -> Result<(), CaptureError> {
    fn sift<T>(
        values: &mut [T], mut root: usize, end: usize,
        compare: &mut impl FnMut(&T, &T) -> std::cmp::Ordering,
        meter: &mut impl CaptureNativeWork,
    ) -> Result<(), CaptureError> {
        while root < end / 2 {
            let mut child = root * 2 + 1;
            if child + 1 < end {
                capture_charge(meter, WorkCounters { items_examined: 2, ..WorkCounters::UNCHARGED })?;
                if compare(&values[child], &values[child + 1]).is_lt() { child += 1; }
            }
            capture_charge(meter, WorkCounters { items_examined: 2, ..WorkCounters::UNCHARGED })?;
            if !compare(&values[root], &values[child]).is_lt() { break; }
            values.swap(root, child);
            root = child;
        }
        Ok(())
    }
    meter.leaf(|work| work.check()).map_err(capture_demand_error)?;
    let length = values.len();
    for root in (0..length / 2).rev() {
        sift(values, root, length, &mut compare, meter)?;
    }
    for end in (1..length).rev() {
        meter.leaf(|work| work.check()).map_err(capture_demand_error)?;
        values.swap(0, end);
        sift(values, 0, end, &mut compare, meter)?;
    }
    Ok(())
}

fn with_capture_work<T, W: CaptureNativeWork>(
    work: &mut WorkCounters,
    operation: impl FnOnce(&mut W) -> Result<T, CaptureError>,
    meter: &mut W,
) -> Result<T, OperationFailure<CaptureError>> {
    let result = operation(meter);
    *work = meter.completed();
    result.map_err(|error| OperationFailure::new(error, *work))
}

fn capture_allocate(meter: &mut impl CaptureNativeWork, bytes: u64) -> Result<(), CaptureError> {
    meter.leaf(|work| work.allocate(bytes)).map_err(capture_demand_error)
}


fn capture_charge(meter: &mut impl CaptureNativeWork, work: WorkCounters) -> Result<(), CaptureError> {
    meter.leaf(|meter| meter.charge(work)).map_err(capture_demand_error)
}

fn capture_step<T, W: CaptureNativeWork>(
    work: &mut WorkCounters,
    meter: &mut W,
    operation: impl FnOnce(&mut W) -> Result<T, CaptureError>,
) -> Result<T, OperationFailure<CaptureError>> {
    let result = operation(meter);
    *work = meter.completed();
    result.map_err(|error| OperationFailure::new(error, *work))
}

fn capture_path_clone(path: &NamespacePath, work: &mut WorkCounters, meter: &mut impl CaptureNativeWork)
    -> Result<NamespacePath, OperationFailure<CaptureError>> {
    capture_step(work, meter, |meter| clone_capture_path(path, meter))
}


fn capture_candidate<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &Checkout<A, O>,
    work: &mut WorkCounters,
    cancellation: &CancellationToken,
    meter: &mut CaptureNativeHostWork<'_>,
) -> Result<Checkout<A, O>, OperationFailure<CaptureError>> {
    let candidate = checkout.private_candidate(meter.scope().native(), cancellation)
        .map_err(|error| OperationFailure::new(CaptureError::Kernel(error), meter.completed()))?;
    *work = meter.completed();
    Ok(candidate)
}

fn clone_capture_name(name: &LogicalName, meter: &mut impl CaptureNativeWork) -> Result<LogicalName, CaptureError> {
    capture_charge(meter, WorkCounters { source_path_components: 1, items_examined: 1,
        ..WorkCounters::UNCHARGED })?;
    let copied = WorkCounters { source_bytes_read: name.as_bytes().len() as u64,
        bytes_copied: name.as_bytes().len() as u64, ..WorkCounters::UNCHARGED };
    meter.admit(copied).map_err(capture_demand_error)?;
    let mut bytes = capture_vec(name.as_bytes().len(), meter)?;
    let retained = bytes.capacity() as u64;
    bytes.extend_from_slice(name.as_bytes());
    if let Err(error) = meter.record(copied) {
        drop(bytes);
        meter.release(retained).map_err(capture_demand_error)?;
        return Err(capture_demand_error(error));
    }
    match LogicalName::new(name.encoding(), bytes, u32::MAX) {
        Ok(name) => Ok(name),
        Err(error) => {
            meter.release(retained).map_err(capture_demand_error)?;
            Err(CaptureError::Name(error))
        }
    }
}

fn clone_capture_prefix(path: &NamespacePath, depth: usize, limits: crate::model::VolumeLimits,
    meter: &mut impl CaptureNativeWork) -> Result<NamespacePath, CaptureError> {
    let names = path.components().get(..depth)
        .ok_or(CaptureError::Path(crate::kernel::NamespacePathError::TooDeep))?;
    NamespacePath::validate_component_lengths(names.iter().map(|name| name.as_bytes().len()), limits)
        .map_err(CaptureError::Path)?;
    let mut components = capture_vec(depth, meter)?;
    for name in names {
        match clone_capture_name(name, meter) {
            Ok(name) => components.push(name),
            Err(error) => {
                capture_drop_components(components, meter)?;
                return Err(error);
            }
        }
    }
    let retained = capture_components_bytes(&components)?;
    match NamespacePath::new(components, limits) {
        Ok(path) => Ok(path),
        Err(error) => {
            meter.release(retained).map_err(capture_demand_error)?;
            Err(CaptureError::Path(error))
        }
    }
}

fn capture_components_bytes(components: &Vec<LogicalName>) -> Result<u64, CaptureError> {
    components.iter().try_fold(
        components.capacity() as u64 * std::mem::size_of::<LogicalName>() as u64,
        |bytes, name| bytes.checked_add(name.retained_bytes() as u64)
            .ok_or(CaptureError::Work(WorkError::Overflow)),
    )
}

fn capture_drop_components(components: Vec<LogicalName>, meter: &mut impl CaptureNativeWork)
    -> Result<(), CaptureError> {
    let bytes = capture_components_bytes(&components)?;
    drop(components);
    meter.release(bytes).map_err(capture_demand_error)
}

fn capture_remaining(meter: &mut impl CaptureNativeWork) -> Result<WorkBudget, DemandError> {
    meter.leaf(|work| work.remaining())
}


fn capture_vec<T>(count: usize, meter: &mut impl CaptureNativeWork) -> Result<Vec<T>, CaptureError> {
    let bytes = count.checked_mul(std::mem::size_of::<T>()).ok_or(CaptureError::Work(WorkError::Overflow))?;
    capture_allocate(meter, bytes as u64)?;
    let mut retained = bytes as u64;
    let mut values = Vec::new();
    let reserved = values.try_reserve_exact(count);
    let actual = match values.capacity().checked_mul(std::mem::size_of::<T>()) {
        Some(actual) => actual as u64,
        None => {
            drop(values);
            meter.leaf(|work| work.release(retained)).map_err(capture_demand_error)?;
            return Err(CaptureError::Work(WorkError::Overflow));
        }
    };
    // Completed capacity, including any capacity retained on allocator failure,
    // belongs to the original ledger before the late cancellation/budget check.
    let reconciled = meter.leaf(|work| work.reconcile_native_allocation(&mut retained, actual));
    if let Err(error) = reserved {
        drop(values);
        meter.leaf(|work| work.release(retained)).map_err(capture_demand_error)?;
        return Err(CaptureError::Allocation(error));
    }
    if let Err(error) = reconciled {
        drop(values);
        meter.leaf(|work| work.release(retained)).map_err(capture_demand_error)?;
        return Err(capture_demand_error(error));
    }
    Ok(values)
}

fn capture_reserve<T>(values: &mut Vec<T>, additional: usize, meter: &mut impl CaptureNativeWork) -> Result<(), CaptureError> {
    meter.leaf(|work| work.check()).map_err(capture_demand_error)?;
    let wanted = values.len().checked_add(additional).ok_or(CaptureError::Work(WorkError::Overflow))?;
    if wanted <= values.capacity() { return Ok(()); }
    let size = std::mem::size_of::<T>();
    let old = values.capacity().checked_mul(size).ok_or(CaptureError::Work(WorkError::Overflow))?;
    let copied = values.len().checked_mul(size).ok_or(CaptureError::Work(WorkError::Overflow))?;
    let grown = wanted.max(values.capacity().saturating_mul(2).max(4));
    let capacity = match grown.checked_mul(size) {
        Some(bytes) if bytes as u64 <= meter.leaf(|work| work.remaining())
            .map_err(capture_demand_error)?.peak_allocation_bytes => grown,
        _ => wanted,
    };
    let mut replacement = capture_vec(capacity, meter)?;
    if let Err(error) = capture_charge(meter, WorkCounters { bytes_copied: copied as u64, ..WorkCounters::UNCHARGED }) {
        let retained = replacement.capacity() as u64 * size as u64;
        drop(replacement);
        meter.leaf(|work| work.release(retained)).map_err(capture_demand_error)?;
        return Err(error);
    }
    replacement.append(values);
    let old_values = std::mem::replace(values, replacement);
    drop(old_values);
    meter.leaf(|work| work.release(old as u64)).map_err(capture_demand_error)
}

fn capture_excludes(policy: &CapturePolicy, path: &NamespacePath, meter: &mut impl CaptureNativeWork) -> Result<bool, CaptureError> {
    for prefix in &policy.excluded_prefixes {
        capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        for _ in prefix.components().iter().zip(path.components()) {
            capture_charge(meter, WorkCounters { source_path_components: 1, ..WorkCounters::UNCHARGED })?;
        }
        if path.is_within(prefix) { return Ok(true); }
    }
    Ok(false)
}

fn clone_capture_paths(
    paths: &[NamespacePath],
    policy: Option<&CapturePolicy>,
    meter: &mut impl CaptureNativeWork,
) -> Result<Vec<NamespacePath>, CaptureError> {
    let mut result = capture_vec(paths.len(), meter)?;
    let copied = (|| {
        for path in paths {
            capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            if let Some(policy) = policy
                && capture_excludes(policy, path, meter)? {
                continue;
            }
            result.push(clone_capture_path(path, meter)?);
        }
        Ok::<(), CaptureError>(())
    })();
    if let Err(error) = copied {
        release_capture_paths(result, meter)?;
        return Err(error);
    }
    Ok(result)
}

fn release_capture_paths(paths: Vec<NamespacePath>, meter: &mut impl CaptureNativeWork) -> Result<(), CaptureError> {
    let bytes = paths.iter().try_fold(
        paths.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64,
        |bytes, path| bytes.checked_add(path.retained_bytes())
            .ok_or(CaptureError::Work(WorkError::Overflow)),
    )?;
    drop(paths);
    meter.release(bytes).map_err(capture_demand_error)
}
fn capture_drop_host_path(path: CaptureHostPath, meter: &mut impl CaptureNativeWork) -> Result<(), CaptureError> {
    let bytes = capture_host_path_bytes(&path);
    drop(path);
    meter.release(bytes).map_err(capture_demand_error)
}

fn capture_has_duplicate_paths(
    paths: &[NamespacePath], meter: &mut impl CaptureNativeWork,
) -> Result<bool, CaptureError> {
    for pair in paths.windows(2) {
        capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if matches!(pair, [left, right] if left == right) { return Ok(true); }
    }
    Ok(false)
}


fn canonical_subtree_roots_measured(
    roots: &[NamespacePath], policy: &CapturePolicy, meter: &mut impl CaptureNativeWork,
) -> Result<Vec<NamespacePath>, CaptureError> {
    let mut sorted = clone_capture_paths(roots, Some(policy), meter)?;
    let sorted_bytes = sorted.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    if let Err(error) = capture_sort_by(&mut sorted, Ord::cmp, meter) {
        release_capture_paths(sorted, meter)?;
        return Err(error);
    }
    let mut canonical: Vec<NamespacePath> = match capture_vec(sorted.len(), meter) {
        Ok(paths) => paths,
        Err(error) => {
            release_capture_paths(sorted, meter)?;
            return Err(error);
        }
    };
    let mut pending = sorted.into_iter();
    let result = (|| {
        while let Some(root) = pending.next() {
            let examined = (|| {
                capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                for _ in root.components() {
                    capture_charge(meter, WorkCounters { source_path_components: 1, ..WorkCounters::UNCHARGED })?;
                }
                Ok::<(), CaptureError>(())
            })();
            if let Err(error) = examined {
                capture_drop_path(root, meter)?;
                return Err(error);
            }
            if canonical.last().is_none_or(|ancestor| !root.is_within(ancestor)) {
                canonical.push(root);
            } else {
                capture_drop_path(root, meter)?;
            }
        }
        Ok::<(), CaptureError>(())
    })();
    let remaining_bytes = pending.as_slice().iter().try_fold(sorted_bytes,
        |bytes, path| bytes.checked_add(path.retained_bytes())
            .ok_or(CaptureError::Work(WorkError::Overflow)))?;
    drop(pending);
    meter.release(remaining_bytes).map_err(capture_demand_error)?;
    if let Err(error) = result {
        release_capture_paths(canonical, meter)?;
        return Err(error);
    }
    Ok(canonical)
}

fn namespace_owned_bytes(path: &NamespacePath) -> Result<u64, CaptureError> {
    Ok(path.retained_bytes())
}

fn clone_capture_path(path: &NamespacePath, meter: &mut impl CaptureNativeWork) -> Result<NamespacePath, CaptureError> {
    use crate::kernel::{ReadAllocationStep, NamespacePathStep};
    let cancellation = meter.leaf(|work| work.cancellation().clone());
    let mut retained = 0_u64;
    let result = path.clone_with_steps(&cancellation, &mut |step| {
        let account = (|| -> Result<(), CaptureError> {
            match step {
                NamespacePathStep::Admit(delta) => {
                    let mut delta = *delta;
                    delta.source_bytes_read = delta.bytes_copied;
                    meter.admit(delta).map_err(capture_demand_error)
                }
                NamespacePathStep::Performed(delta) => {
                    let mut delta = *delta;
                    delta.source_bytes_read = delta.bytes_copied;
                    meter.record(delta).map_err(capture_demand_error)
                }
                NamespacePathStep::Charge(delta) => capture_charge(meter, *delta),
                NamespacePathStep::Allocation(ReadAllocationStep::Allocate { bytes, operations }) => {
                    let total = retained.checked_add(bytes).ok_or(CaptureError::Work(WorkError::Overflow))?;
                    meter.leaf(|work| work.claim_allocation(bytes, operations)).map_err(capture_demand_error)?;
                    retained = total;
                    Ok(())
                }
                NamespacePathStep::Allocation(ReadAllocationStep::Capacity { admitted_bytes, actual_bytes }) => {
                    let mut allocation = admitted_bytes;
                    let result = meter.leaf(|work| work.reconcile_native_allocation(&mut allocation, actual_bytes));
                    retained = retained.checked_sub(admitted_bytes)
                        .and_then(|bytes| bytes.checked_add(allocation))
                        .ok_or(CaptureError::Work(WorkError::Overflow))?;
                    result.map_err(capture_demand_error)
                }
                NamespacePathStep::Allocation(ReadAllocationStep::ReserveFailed(error)) =>
                    Err(CaptureError::Allocation(error)),
                NamespacePathStep::Allocation(ReadAllocationStep::BeforeCopy(bytes)) =>
                    meter.admit(WorkCounters { bytes_copied: bytes, source_bytes_read: bytes,
                        ..WorkCounters::UNCHARGED }).map_err(capture_demand_error),
                NamespacePathStep::Allocation(ReadAllocationStep::Copied(bytes)) =>
                    meter.record(WorkCounters { bytes_copied: bytes, source_bytes_read: bytes,
                        ..WorkCounters::UNCHARGED }).map_err(capture_demand_error),
                NamespacePathStep::Allocation(ReadAllocationStep::BeforeHash(bytes)) => {
                    meter.check().map_err(capture_demand_error)?;
                    meter.admit(WorkCounters { bytes_hashed: bytes, ..WorkCounters::UNCHARGED }).map_err(capture_demand_error)
                }
                NamespacePathStep::Allocation(ReadAllocationStep::Hashed(bytes)) => {
                    meter.record(WorkCounters { bytes_hashed: bytes, ..WorkCounters::UNCHARGED }).map_err(capture_demand_error)?;
                    meter.check().map_err(capture_demand_error)
                }
                NamespacePathStep::Allocation(ReadAllocationStep::BeforeItem) =>
                    meter.admit(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }).map_err(capture_demand_error),
                NamespacePathStep::Allocation(ReadAllocationStep::Item) =>
                    capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }),
            }
        })();
        account
    });
    match result {
        Ok(path) => Ok(path),
        Err(error) => {
            // The canonical producer has already dropped every partial buffer.
            meter.release(retained).map_err(capture_demand_error)?;
            Err(error)
        }
    }
}

#[cfg(unix)]
type CaptureHostPath = PathBuf;
#[cfg(windows)]
type CaptureHostPath = Vec<u16>;

fn namespace_to_host_path_measured(path: &NamespacePath, meter: &mut impl CaptureNativeWork) -> Result<CaptureHostPath, CaptureError> {
    #[cfg(unix)]
    let result = namespace_to_host_path_with_work(path, meter).map(|(path, _allocation)| path);
    #[cfg(windows)]
    let result = namespace_host_units_with_work(path, meter, u16::from(b'\\'), namespace_component_wide)
        .map(|(units, _allocation)| units);
    result.map_err(|error| match error {
        DemandError::InvalidRequest => CaptureError::UnrepresentablePath,
        error => capture_demand_error(error),
    })
}

/// Converts the canonical namespace with the same bounded component scanner
/// used by capture. The returned token owns actual native path capacity.
pub(crate) fn namespace_to_host_path_with_work<W: crate::native_host::NativeHostWork>(
    path: &NamespacePath,
    work: &mut W,
) -> Result<(PathBuf, W::Allocation), W::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt as _;
        let (bytes, allocation) = namespace_host_units_with_work(
            path, work, b'/', namespace_component_bytes,
        )?;
        Ok((std::ffi::OsString::from_vec(bytes).into(), allocation))
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt as _;
        let (wide, wide_allocation) = namespace_host_units_with_work(
            path, work, u16::from(b'\\'), namespace_component_wide,
        )?;
        let reserved = (wide.len() as u64).checked_mul(3).ok_or(WorkError::Overflow)?;
        if let Err(error) = work.admit(WorkCounters { bytes_encoded: reserved, ..WorkCounters::UNCHARGED }) {
            drop(wide);
            let _retirement = work.release(wide_allocation);
            return Err(error);
        }
        let allocation = work.reserve_opaque_decode(reserved);
        let mut allocation = match allocation {
            Ok(allocation) => allocation,
            Err(error) => {
                drop(wide);
                let _retirement = work.release(wide_allocation);
                return Err(error);
            }
        };
        // Safe std UTF-16 decoding preserves isolated surrogates. Its internal
        // allocator attempts and temporary RSS are opaque, not fabricated
        // fixed SDK allocation counts. The original logical reservation is
        // active before this synchronous call; actual owned output is observed
        // before any late cancellation/budget refusal.
        let native = std::ffi::OsString::from_wide(&wide);
        let reconciled = work.reconcile_allocation(&mut allocation, native.capacity() as u64);
        let encoded = work.record(WorkCounters {
            bytes_encoded: native.as_encoded_bytes().len() as u64, ..WorkCounters::UNCHARGED
        });
        drop(wide);
        let retired = work.release(wide_allocation);
        let completed = reconciled.and(encoded).and(retired).and_then(|()| work.check());
        if let Err(error) = completed {
            drop(native);
            let _retirement = work.release(allocation);
            return Err(error);
        }
        Ok((native.into(), allocation))
    }
}

fn namespace_host_units_with_work<W: crate::native_host::NativeHostWork, T: Copy>(
    path: &NamespacePath,
    work: &mut W,
    separator: T,
    encode: impl Fn(&LogicalName, &mut W, &mut dyn FnMut(T, &mut W) -> Result<(), W::Error>) -> Result<(), W::Error>,
) -> Result<(Vec<T>, W::Allocation), W::Error> {
    let mut count = 0usize;
    for (index, component) in path.components().iter().enumerate() {
        work.check()?;
        let examined = WorkCounters { items_examined: component.as_bytes().len() as u64,
            source_path_components: 1, ..WorkCounters::UNCHARGED };
        work.admit(examined)?;
        work.record(examined)?;
        if index != 0 { count = count.checked_add(1).ok_or(WorkError::Overflow)?; }
        encode(component, work, &mut |_, _| {
            count = count.checked_add(1).ok_or(WorkError::Overflow)?;
            Ok(())
        })?;
    }
    let width = std::mem::size_of::<T>();
    let bytes = count.checked_mul(width).ok_or(WorkError::Overflow)?;
    let copied = WorkCounters { bytes_copied: bytes as u64,
        source_path_components: path.depth() as u64, ..WorkCounters::UNCHARGED };
    work.admit(copied)?;
    let mut allocation = work.claim(bytes as u64)?;
    let mut buffer = Vec::new();
    if let Err(error) = buffer.try_reserve_exact(count) {
        drop(buffer);
        let _retirement = work.release(allocation);
        return Err(W::allocation_failed(error));
    }
    let result = (|| {
        let actual = buffer.capacity().checked_mul(width).ok_or(WorkError::Overflow)?;
        work.reconcile_allocation(&mut allocation, actual as u64)?;
        for (index, component) in path.components().iter().enumerate() {
            work.check()?;
            let examined = WorkCounters { items_examined: component.as_bytes().len() as u64,
                source_path_components: 1, ..WorkCounters::UNCHARGED };
            work.admit(examined)?;
            work.record(examined)?;
            if index != 0 {
                buffer.push(separator);
                work.record(WorkCounters { bytes_copied: width as u64, ..WorkCounters::UNCHARGED })?;
            }
            encode(component, work, &mut |unit, work| {
                buffer.push(unit);
                work.record(WorkCounters { bytes_copied: width as u64, ..WorkCounters::UNCHARGED })
            })?;
        }
        Ok(())
    })();
    if let Err(error) = result {
        drop(buffer);
        let _retirement = work.release(allocation);
        return Err(error);
    }
    Ok((buffer, allocation))
}

#[cfg(unix)]
fn namespace_component_bytes<W: crate::native_host::NativeHostWork>(
    component: &LogicalName, work: &mut W,
    emit: &mut dyn FnMut(u8, &mut W) -> Result<(), W::Error>,
) -> Result<(), W::Error> {
    match component.encoding() {
        NameEncoding::Utf8 | NameEncoding::PosixBytes => {
            for byte in component.as_bytes() { work.check()?; emit(*byte, work)?; }
        }
        NameEncoding::WindowsUtf16Le => {
            let units = crate::kernel::types::utf16le_units(component.as_bytes()).ok_or_else(W::invalid_request)?;
            for character in char::decode_utf16(units) {
                work.check()?;
                let character = character.map_err(|_| W::invalid_request())?;
                let mut bytes = [0; 4];
                for byte in character.encode_utf8(&mut bytes).as_bytes() { emit(*byte, work)?; }
            }
        }
    }
    Ok(())
}

#[cfg(windows)]
fn namespace_component_wide<W: crate::native_host::NativeHostWork>(
    component: &LogicalName, work: &mut W,
    emit: &mut dyn FnMut(u16, &mut W) -> Result<(), W::Error>,
) -> Result<(), W::Error> {
    match component.encoding() {
        NameEncoding::WindowsUtf16Le => {
            for unit in crate::kernel::types::utf16le_units(component.as_bytes()).ok_or_else(W::invalid_request)? {
                work.check()?;
                emit(unit, work)?;
            }
        }
        NameEncoding::Utf8 | NameEncoding::PosixBytes => {
            let text = std::str::from_utf8(component.as_bytes()).map_err(|_| W::invalid_request())?;
            if text.contains(['/', '\\']) { return Err(W::invalid_request()); }
            for unit in text.encode_utf16() { work.check()?; emit(unit, work)?; }
        }
    }
    Ok(())
}

fn capture_stat<W: CaptureNativeWork>(root: &HostRoot, path: &CaptureHostPath, meter: &mut W) -> Result<HostStat, DemandError> {
    #[cfg(unix)]
    { root.stat(path, meter) }
    #[cfg(windows)]
    { root.stat_wide_with_work(path, meter) }
}

fn collect_host_subtree_observations_measured<W: CaptureNativeWork>(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    host_root: CaptureHostPath,
    volume_root: NamespacePath,
    policy: &CapturePolicy,
    observed: &mut CaptureMap<NamespacePath, HostObservation>,
    meter: &mut W,
) -> Result<(), CaptureError> {
    collect_host_walk_measured(root, profile, limits, host_root, volume_root, policy, meter,
        |path, stat, meter| insert_host_observation(observed, path, stat, maximum, meter))
}

fn collect_host_walk_measured<W: CaptureNativeWork>(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    host_root: CaptureHostPath,
    volume_root: NamespacePath,
    policy: &CapturePolicy,
    meter: &mut W,
    mut insert: impl FnMut(NamespacePath, HostStat, &mut W) -> Result<(), CaptureError>,
) -> Result<(), CaptureError> {
    let slot = std::mem::size_of::<(CaptureHostPath, NamespacePath)>() as u64;
    let mut pending = capture_vec(1, meter)?;
    pending.push((host_root, volume_root));
    while let Some((host_parent, volume_parent)) = pending.pop() {
        meter.check().map_err(capture_demand_error)?;
        #[cfg(unix)]
        let (mut reader, reader_allocation) = root.read_dir_stats(&host_parent, meter).map_err(capture_demand_error)?;
        #[cfg(windows)]
        let (mut reader, reader_allocation) = root.read_dir_stats_wide_with_work(&host_parent, meter).map_err(capture_demand_error)?;
        loop {
            #[cfg(unix)]
            let entry = reader.next_with_work(meter).map_err(capture_demand_error)?;
            #[cfg(windows)]
            let entry = reader.next_wide_with_work(meter).map_err(capture_demand_error)?;
            let Some((entry, name_allocation)) = entry else { break };
            let name = (|| {
                #[cfg(unix)]
                let name = {
                    let raw = entry.name.as_encoded_bytes();
                    capture_charge(meter, WorkCounters { items_examined: raw.len() as u64, ..WorkCounters::UNCHARGED })?;
                    let (encoding, bytes) = if profile == FilesystemProfile::Windows {
                        let text = std::str::from_utf8(raw).map_err(|_| CaptureError::UnrepresentablePath)?;
                        let length = text.encode_utf16().count().checked_mul(2)
                            .ok_or(CaptureError::Work(WorkError::Overflow))?;
                        let mut bytes = capture_vec(length, meter)?;
                        for unit in text.encode_utf16() {
                            capture_charge(meter, WorkCounters { bytes_copied: 2, ..WorkCounters::UNCHARGED })?;
                            bytes.extend_from_slice(&unit.to_le_bytes());
                        }
                        (NameEncoding::WindowsUtf16Le, bytes)
                    } else {
                        let mut bytes = capture_vec(raw.len(), meter)?;
                        capture_charge(meter, WorkCounters { bytes_copied: raw.len() as u64, ..WorkCounters::UNCHARGED })?;
                        bytes.extend_from_slice(raw);
                        (NameEncoding::PosixBytes, bytes)
                    };
                    LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
                        .map_err(CaptureError::Name)?
                };
                #[cfg(windows)]
                let name = {
                    let (encoding, bytes) = if profile == FilesystemProfile::Windows {
                        let mut bytes = capture_vec(entry.name.len().checked_mul(2)
                            .ok_or(CaptureError::Work(WorkError::Overflow))?, meter)?;
                        for unit in &entry.name {
                            capture_charge(meter, WorkCounters { bytes_copied: 2, ..WorkCounters::UNCHARGED })?;
                            bytes.extend_from_slice(&unit.to_le_bytes());
                        }
                        (NameEncoding::WindowsUtf16Le, bytes)
                    } else {
                        let mut length = 0usize;
                        for character in char::decode_utf16(entry.name.iter().copied()) {
                            meter.check().map_err(capture_demand_error)?;
                            length = length.checked_add(character.map_err(|_| CaptureError::UnrepresentablePath)?.len_utf8())
                                .ok_or(CaptureError::Work(WorkError::Overflow))?;
                        }
                        let mut bytes = capture_vec(length, meter)?;
                        for character in char::decode_utf16(entry.name.iter().copied()) {
                            let character = character.map_err(|_| CaptureError::UnrepresentablePath)?;
                            capture_charge(meter, WorkCounters { bytes_copied: character.len_utf8() as u64, ..WorkCounters::UNCHARGED })?;
                            let mut buffer = [0; 4];
                            bytes.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
                        }
                        (NameEncoding::PosixBytes, bytes)
                    };
                    LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
                        .map_err(CaptureError::Name)?
                };
                Ok::<_, CaptureError>(name)
            })()?;
            drop(entry.name);
            meter.release(name_allocation).map_err(capture_demand_error)?;
            let child = append_path_measured(&volume_parent, name, limits, meter)?;
            if capture_excludes(policy, &child, meter)? {
                capture_drop_path(child, meter)?;
                continue;
            }
            let host_child = namespace_to_host_path_measured(&child, meter)?;
            let stat = match entry.stat {
                Some(stat) => stat,
                None => capture_stat(root, &host_child, meter).map_err(capture_demand_error)?,
            };
            let recurse = stat.is_dir() && !stat.file_type().is_symlink();
            let observed_path = clone_capture_path(&child, meter)?;
            insert(observed_path, stat, meter)?;
            if recurse {
                capture_reserve(&mut pending, 1, meter)?;
                pending.push((host_child, child));
            } else {
                capture_drop_host_path(host_child, meter)?;
                capture_drop_path(child, meter)?;
            }
        }
        drop(reader);
        meter.release(reader_allocation).map_err(capture_demand_error)?;
        capture_drop_host_path(host_parent, meter)?;
        capture_drop_path(volume_parent, meter)?;
    }
    let pending_bytes = pending.capacity() as u64 * slot;
    drop(pending);
    meter.release(pending_bytes).map_err(capture_demand_error)?;
    Ok(())
}

fn capture_host_path_bytes(path: &CaptureHostPath) -> u64 {
    #[cfg(unix)]
    { path.capacity() as u64 }
    #[cfg(windows)]
    { path.capacity() as u64 * 2 }
}

fn append_path_measured(
    parent: &NamespacePath,
    name: LogicalName,
    limits: crate::model::VolumeLimits,
    meter: &mut impl CaptureNativeWork,
) -> Result<NamespacePath, CaptureError> {
    let name_bytes = name.retained_bytes() as u64;
    let result = (|| {
        meter.check().map_err(capture_demand_error)?;
        NamespacePath::validate_component_lengths(
            parent.components().iter().chain(std::iter::once(&name)).map(|name| name.as_bytes().len()), limits,
        ).map_err(CaptureError::Path)?;
        let depth = parent.depth().checked_add(1).ok_or(CaptureError::Work(WorkError::Overflow))?;
        capture_charge(meter, WorkCounters { source_path_components: 1, items_examined: 1,
            ..WorkCounters::UNCHARGED })?;
        let mut components = capture_vec(depth, meter)?;
        for component in parent.components() {
            match clone_capture_name(component, meter) {
                Ok(component) => components.push(component),
                Err(error) => {
                    capture_drop_components(components, meter)?;
                    return Err(error);
                }
            }
        }
        Ok(components)
    })();
    let mut components = match result {
        Ok(components) => components,
        Err(error) => {
            drop(name);
            meter.release(name_bytes).map_err(capture_demand_error)?;
            return Err(error);
        }
    };
    components.push(name);
    let retained = capture_components_bytes(&components)?;
    match NamespacePath::new(components, limits) {
        Ok(path) => Ok(path),
        Err(error) => {
            meter.release(retained).map_err(capture_demand_error)?;
            Err(CaptureError::Path(error))
        }
    }
}

#[cfg(test)]
fn canonical_subtree_roots(roots: &[NamespacePath], policy: &CapturePolicy) -> Vec<NamespacePath> {
    let mut sorted = roots
        .iter()
        .filter(|root| !policy.excludes(root))
        .cloned()
        .collect::<Vec<_>>();
    sorted.sort_unstable();
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
    meter: &CaptureWorkScope,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let unique = {
        let mut original = capture_meter(meter);
        let mut unique = clone_capture_paths(paths, Some(policy), &mut original)
            .map_err(|error| OperationFailure::new(error, original.completed()))?;
        capture_sort_by(&mut unique, Ord::cmp, &mut original)
            .map_err(|error| OperationFailure::new(error, original.completed()))?;
        if capture_has_duplicate_paths(&unique, &mut original)
            .map_err(|error| OperationFailure::new(error, original.completed()))? {
            return Err(OperationFailure::new(CaptureError::InvalidOptions, original.completed()));
        }
        unique
    };
    capture_unique_paths_from_root(
        checkout, unique, maximum_extent_spans, source_root, budget, cancellation, meter,
    ).await
}

async fn capture_unique_paths_from_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    unique: Vec<NamespacePath>,
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    // A comparator must not inspect a changing host tree: one path could
    // otherwise alternate between present and absent during the sort. This
    // also reduces host metadata probes from O(paths * log paths) to O(paths).
    let observed = observe_unique_paths(
        unique, source_root, budget, cancellation, &mut CaptureNativeHostWork::new(meter),
    )?;
    let mut captured = capture_observed_paths_from_root(
        checkout, observed.value, maximum_extent_spans, source_root, budget,
        cancellation, None, meter,
    ).await?;
    captured.value.work = captured.work;
    Ok(captured)
}

type ObservedPath = (Option<HostObservation>, NamespacePath);

fn observe_unique_paths<W: CaptureNativeWork>(
    unique: Vec<NamespacePath>,
    source_root: &HostRoot,
    _budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &mut W,
) -> Result<OperationReceipt<Vec<ObservedPath>>, OperationFailure<CaptureError>> {
    let mut ordered = capture_vec(unique.len(), meter)
        .map_err(|error| OperationFailure::new(error, meter.completed()))?;
    let unique_bytes = unique.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    let result = (|| {
        for path in unique {
            capture_charge(meter, WorkCounters { source_entries_visited: 1, ..WorkCounters::UNCHARGED })?;
            let host_path = namespace_to_host_path_measured(&path, meter)?;
            let observation = observe_host_path(source_root, &host_path, meter)?;
            capture_drop_host_path(host_path, meter)?;
            ordered.push((observation, path));
        }
        meter.release(unique_bytes).map_err(capture_demand_error)?;
        Ok(())
    })();
    result.map_err(|error| OperationFailure::new(error, meter.completed()))?;
    let work = meter.completed();
    cancellation
        .check()
        .map_err(|_| OperationFailure::new(CaptureError::Cancelled, work))?;
    capture_sort_by(&mut ordered, |(left_observation, left), (right_observation, right)| {
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
    }, meter).map_err(|error| OperationFailure::new(error, meter.completed()))?;
    let work = meter.completed();
    cancellation
        .check()
        .map_err(|_| OperationFailure::new(CaptureError::Cancelled, work))?;
    Ok(OperationReceipt {
        value: ordered,
        work,
    })
}

fn observe_host_path<W: CaptureNativeWork>(
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    meter: &mut W,
) -> Result<Option<HostObservation>, CaptureError> {
    classify_host_observation(capture_stat(source_root, host_path, meter))
}

fn classify_host_observation(
    metadata: Result<HostStat, DemandError>,
) -> Result<Option<HostObservation>, CaptureError> {
    match metadata {
        Ok(metadata) => Ok(Some(HostObservation::from_metadata(metadata))),
        Err(DemandError::Io(error)) if host_path_is_absent(&error) => Ok(None),
        Err(DemandError::Absent | DemandError::NotDirectory) => Ok(None),
        Err(error) => Err(capture_demand_error(error)),
    }
}

#[cfg(test)]
mod host_observation_tests {
    use super::*;

    #[test]
    fn original_capture_input_transfer_keeps_physical_custody_without_readoption()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let cancellation = CancellationToken::new();
        let native = crate::NativeMaterializationWork::new(
            native_range_tests::original_budget(), &cancellation,
        ).map_err(|(error, _)| error)?;
        let baseline = native.live_allocation_bytes();
        let mut scope = CaptureWorkScope::new(&native)?;
        let mut bytes = capture_vec::<u8>(3, &mut capture_meter(&scope))?;
        bytes.extend_from_slice(b"abc");
        let capacity = bytes.capacity() as u64;
        let completed = native.receipt();
        let held = native.live_allocation_bytes();
        let input = (bytes, scope.transfer_allocation(capacity)?);
        assert_eq!(native.receipt(), completed);
        assert_eq!(native.live_allocation_bytes(), held);
        assert_eq!(scope.custody.as_ref().expect("live scope").retained_bytes(), 0);
        scope.release()?;
        assert_eq!(native.live_allocation_bytes(), baseline + capacity);
        assert_eq!(input.0.as_slice(), b"abc");
        drop(input);
        assert_eq!(native.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn capture_allocation_credit_reports_only_accepted_physical_backing()
        -> Result<(), Box<dyn std::error::Error>>
    {
        use acyclic_native_runtime::NativeReadMeter;
        let cancellation = CancellationToken::new();
        let native = crate::NativeMaterializationWork::new(
            native_range_tests::original_budget(), &cancellation,
        ).map_err(|(error, _)| error)?;
        let baseline = native.live_allocation_bytes();
        let mut scope = CaptureWorkScope::new(&native)?;
        let held = native.live_allocation_bytes();
        let before = native.receipt();
        let meter = CaptureNativeReadMeter(&scope);
        meter.reserve_allocation(8)?;
        assert_eq!(native.receipt(), before);
        assert_eq!(native.live_allocation_bytes(), held);
        assert_eq!(scope.custody.as_ref().expect("live scope").retained_bytes(), 0);
        meter.cancel_reserved_allocation(8);
        assert_eq!(native.receipt(), before);
        assert_eq!(native.live_allocation_bytes(), held);
        meter.reserve_allocation(8)?;
        cancellation.cancel();
        meter.record_reserved_allocation(8)?;
        let bytes = Box::new([0_u8; 8]);
        assert_eq!(native.receipt().allocation_operations, before.allocation_operations + 1);
        assert_eq!(native.live_allocation_bytes(), held + 8);
        assert_eq!(scope.custody.as_ref().expect("live scope").retained_bytes(), 8);
        drop(bytes);
        meter.release(8);
        assert_eq!(native.live_allocation_bytes(), held);
        assert_eq!(scope.custody.as_ref().expect("live scope").retained_bytes(), 0);
        scope.release()?;
        assert_eq!(native.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[test]
    fn original_capture_input_transfer_rejects_a_different_root()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let cancellation = CancellationToken::new();
        let native = crate::NativeMaterializationWork::new(
            native_range_tests::original_budget(), &cancellation,
        ).map_err(|(error, _)| error)?;
        let foreign = crate::NativeMaterializationWork::new(
            native_range_tests::original_budget(), &cancellation,
        ).map_err(|(error, _)| error)?;
        let mut scope = CaptureWorkScope::new(&native)?;
        let bytes = capture_vec::<u8>(3, &mut capture_meter(&scope))?;
        let capacity = bytes.capacity() as u64;
        let custody = scope.custody.as_ref().expect("live scope");
        let completed = native.receipt();
        let held = native.live_allocation_bytes();
        assert!(foreign.transfer_capture_allocation(capacity, custody).is_err());
        assert_eq!(custody.retained_bytes(), capacity);
        assert_eq!(native.receipt(), completed);
        assert_eq!(native.live_allocation_bytes(), held);
        let input = (bytes, scope.transfer_allocation(capacity)?);
        drop(input);
        scope.release()?;
        assert_eq!(native.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes());
        Ok(())
    }


    #[test]
    fn capture_heap_refusal_precedes_future_construction() {
        let constructed = std::cell::Cell::new(false);
        let mut budget = WorkBudget::UNBOUNDED;
        budget.allocation_operations = 0;
        let mut meter = DemandWork::new(budget, &CancellationToken::new());
        let failure = match crate::heap_future::in_heap_admitted(
            &mut meter,
            |meter, bytes| capture_allocate(*meter, bytes)
                .map_err(|error| OperationFailure::new(error, meter.work)),
            |meter| {
                constructed.set(true);
                async move { meter }
            },
        ) {
            Err(failure) => failure,
            Ok(_) => panic!("refused heap boundary constructed a future"),
        };
        assert!(!constructed.get());
        assert!(matches!(failure.error, CaptureError::Work(
            WorkError::BudgetExceeded { counter: "allocation_operations", .. })));
        assert_eq!(*failure.work, WorkCounters::UNCHARGED);
        assert_eq!(meter.work, WorkCounters::UNCHARGED);
    }

    #[test]
    fn capture_heap_refusal_accounts_live_caller_buffer() -> Result<(), Box<dyn std::error::Error>> {
        let caller = vec![0_u8; 32];
        let caller_bytes = caller.capacity() as u64;
        let constructed = std::cell::Cell::new(false);
        let mut budget = WorkBudget::UNBOUNDED;
        budget.peak_allocation_bytes = caller_bytes;
        let mut meter = DemandWork::new(budget, &CancellationToken::new());
        meter.adopt(caller_bytes)?;
        let failure = match crate::heap_future::in_heap_admitted(
            &mut meter,
            |meter, bytes| capture_allocate(*meter, bytes)
                .map_err(|error| OperationFailure::new(error, meter.work)),
            |meter| {
                constructed.set(true);
                async move { meter }
            },
        ) {
            Err(failure) => failure,
            Ok(_) => panic!("heap boundary exceeded the live caller buffer budget"),
        };
        assert!(!constructed.get());
        assert!(matches!(failure.error, CaptureError::Work(
            WorkError::BudgetExceeded { counter: "peak_allocation_bytes", .. })));
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.peak_allocation_bytes, caller_bytes);
        assert_eq!(meter.remaining()?.peak_allocation_bytes, 0);
        drop(caller);
        meter.release(caller_bytes)?;
        assert_eq!(meter.remaining()?.peak_allocation_bytes, caller_bytes);
        Ok(())
    }

    #[tokio::test]
    async fn capture_heap_cancelled_body_retains_real_metadata_prefix()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("file"), b"body")?;
        let root = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let path = test_host_path(Path::new("file"));
        let cancellation = CancellationToken::new();
        let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let (mut future, heap_bytes) = crate::heap_future::in_heap_admitted(
            &mut meter,
            |meter, bytes| capture_allocate(*meter, bytes)
                .map_err(|error| OperationFailure::new(error, meter.work)),
            |meter| async move {
                let observed = capture_stat(&root, &path, meter);
                cancellation.cancel();
                let cancelled = meter.check();
                (observed, cancelled, meter)
            },
        )?;
        let (observed, cancelled, meter) = future.as_mut().await;
        drop(future);
        observed?;
        assert!(matches!(cancelled, Err(DemandError::Cancelled)));
        assert!(heap_bytes > 0);
        assert!(meter.work.backend_read_operations > 0);
        assert!(meter.work.allocation_operations > 0);
        let completed = meter.work;
        meter.release(heap_bytes)?;
        assert_eq!(meter.work, completed);
        Ok(())
    }
    #[test]
    fn only_absent_paths_may_become_deletions() -> Result<(), Box<dyn std::error::Error>> {
        for kind in [
            std::io::ErrorKind::NotFound,
            std::io::ErrorKind::NotADirectory,
        ] {
            assert!(classify_host_observation(Err(DemandError::Io(kind.into())))?.is_none());
        }
        for kind in [
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::InvalidInput,
        ] {
            assert!(matches!(
                classify_host_observation(Err(DemandError::Io(kind.into()))),
                Err(CaptureError::Io(_))
            ));
        }
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("file"), b"body")?;
        let source_root = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        assert!(observe_host_path(&source_root, &test_host_path(Path::new("file/child")),
            &mut DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()))?.is_none());
        Ok(())
    }

    #[test]
    fn bounded_ordering_admits_comparisons_before_examining_items() {
        let mut values = [2, 1];
        let mut budget = WorkBudget::UNBOUNDED;
        budget.items_examined = 0;
        let mut meter = DemandWork::new(budget, &CancellationToken::new());
        assert!(matches!(capture_sort_by(&mut values, Ord::cmp, &mut meter),
            Err(CaptureError::Work(WorkError::BudgetExceeded { counter: "items_examined", .. }))));
        assert_eq!(values, [2, 1]);
        assert_eq!(meter.work, WorkCounters::UNCHARGED);
    }

    #[test]
    fn bounded_ordering_observes_cancellation_between_comparisons() {
        let cancellation = CancellationToken::new();
        let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let mut values = [4, 3, 2, 1];
        let mut comparisons = 0;
        assert!(capture_sort_by(&mut values, |left, right| {
            comparisons += 1;
            cancellation.cancel();
            left.cmp(right)
        }, &mut meter).is_err());
        assert_eq!(comparisons, 1);
        assert_eq!(meter.work.items_examined, 2);
        assert_eq!(meter.work.allocation_operations, 0);
        assert_eq!(meter.work.backend_read_operations, 0);
    }

    #[test]
    fn bounded_ordering_handles_duplicates_without_allocating() -> Result<(), CaptureError> {
        let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        let mut values = [4, 1, 3, 2, 2];
        capture_sort_by(&mut values, Ord::cmp, &mut meter)?;
        assert_eq!(values, [1, 2, 2, 3, 4]);
        assert_eq!(meter.work.allocation_operations, 0);
        Ok(())
    }

    #[test]
    fn map_buffer_uses_actual_capacity_without_per_node_estimates() -> Result<(), CaptureError> {
        let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        let mut map = CaptureMap::new();
        map.insert_measured([1u8; 16], 1u64, &mut meter)?;
        assert_eq!(meter.work.allocation_operations, 1);
        assert_eq!(meter.work.peak_allocation_bytes, map.storage_bytes());
        assert_eq!(map.len(), 1);
        Ok(())
    }

    #[test]
    fn map_shift_is_admitted_before_inserting_or_copying() -> Result<(), CaptureError> {
        let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        let mut map = CaptureMap::new();
        map.insert_measured([2u8; 16], 2u64, &mut meter)?;
        meter.budget.bytes_copied = 0;
        assert!(matches!(map.insert_measured([1u8; 16], 1u64, &mut meter),
            Err(CaptureError::Work(WorkError::BudgetExceeded { counter: "bytes_copied", .. }))));
        assert_eq!(map.len(), 1);
        assert_eq!(meter.work.bytes_copied, 0);
        assert_eq!(map.entries[0].0, [2u8; 16]);
        Ok(())
    }

    #[test]
    fn cancelled_map_does_not_allocate_its_first_buffer() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let mut map = CaptureMap::new();
        assert!(map.insert_measured([1u8; 16], 1u64, &mut meter).is_err());
        assert_eq!(meter.work, WorkCounters::UNCHARGED);
        assert!(map.is_empty());
    }

    #[test]
    fn buffer_growth_uses_exact_fit_when_geometric_growth_would_exceed_peak() -> Result<(), CaptureError> {
        let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        let mut values = capture_vec(1, &mut meter)?;
        values.push(7u8);
        meter.budget.peak_allocation_bytes = 3;
        capture_reserve(&mut values, 1, &mut meter)?;
        assert_eq!(values, [7]);
        assert_eq!(values.capacity(), 2);
        assert_eq!(meter.work.peak_allocation_bytes, 3);
        assert_eq!(meter.work.allocation_operations, 2);
        assert_eq!(meter.work.bytes_copied, 1);
        Ok(())
    }

    #[test]
    fn link_spare_capacity_admits_only_the_shared_header_without_copying() -> Result<(), CaptureError> {
        let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        let mut target = capture_vec(8, &mut meter)?;
        target.extend_from_slice(b"link");
        let pointer = target.as_ptr();
        let before = meter.work;
        let target = capture_link_target(target, &mut meter)?;
        assert_eq!(target.as_ref(), b"link");
        assert_eq!(target.as_ptr(), pointer);
        assert_eq!(meter.work.allocation_operations, before.allocation_operations + 1);
        assert_eq!(meter.work.peak_allocation_bytes, before.peak_allocation_bytes);
        assert_eq!(meter.work.bytes_copied, before.bytes_copied);
        Ok(())
    }

    #[test]
    fn link_header_allocation_is_admitted_before_byte_ownership_conversion() -> Result<(), CaptureError> {
        let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
        let mut target = capture_vec(8, &mut meter)?;
        target.extend_from_slice(b"link");
        let before = meter.work;
        meter.budget.allocation_operations = before.allocation_operations;
        assert!(matches!(capture_link_target(target, &mut meter),
            Err(CaptureError::Work(WorkError::BudgetExceeded { counter: "allocation_operations", .. }))));
        assert_eq!(meter.work, before);
        assert_eq!(meter.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn native_root_failure_retains_completed_probe_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let options = CaptureOptions {
            source_root: directory.path().to_path_buf(),
            expected_root_identity: capture_root_identity(directory.path())?,
            maximum_paths: 1,
            maximum_extent_spans: 1,
        };
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_read_operations = 1;
        let mut meter = DemandWork::new(budget, &CancellationToken::new());
        let failure = open_source_root(&options, &mut meter).err().ok_or("root probe unexpectedly fit")?;
        assert!(matches!(failure.error, CaptureError::Work(WorkError::BudgetExceeded { counter: "backend_read_operations", .. })));
        assert_eq!(failure.work.backend_read_operations, 1);
        assert_eq!(*failure.work, meter.work);
        let cancellation = CancellationToken::new();
        let native = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let original = native.original_demand_work();
        let baseline = original.meter().lock().live_allocation_bytes();
        let scope = CaptureWorkScope::new(&native)?;
        let failure = open_source_root(&options, &mut CaptureNativeHostWork::new(&scope))
            .err().ok_or("shared root probe unexpectedly fit")?;
        assert!(matches!(failure.error, CaptureError::Work(WorkError::BudgetExceeded { counter: "backend_read_operations", .. })));
        assert_eq!(failure.work.backend_read_operations, 1);
        assert_eq!(*failure.work, original.counters()?);
        cancellation.cancel();
        let cancelled = open_source_root(&options, &mut CaptureNativeHostWork::new(&scope))
            .err().ok_or("shared root probe ignored original cancellation")?;
        assert!(matches!(cancelled.error, CaptureError::Cancelled));
        assert_eq!(*cancelled.work, *failure.work);
        let retained = capture_original_result::<()>(&original, Err(failure))
            .expect_err("late cancellation must not replace the completed probe failure");
        assert!(matches!(retained.error, CaptureError::Work(WorkError::BudgetExceeded {
            counter: "backend_read_operations", ..
        })));
        assert_eq!(*retained.work, *cancelled.work);
        drop(scope);
        let live = original.meter().lock().live_allocation_bytes();
        assert_eq!(live, baseline);
        Ok(())
    }

    #[test]
    fn path_copy_is_admitted_before_buffer_allocation() -> Result<(), Box<dyn std::error::Error>> {
        let path = NamespacePath::new(vec![LogicalName::new(NameEncoding::Utf8, b"file".to_vec(), 255)?],
            crate::model::VolumeLimits::default())?;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.bytes_copied = 0;
        let mut meter = DemandWork::new(budget, &CancellationToken::new());
        assert!(matches!(namespace_to_host_path_measured(&path, &mut meter),
            Err(CaptureError::Work(WorkError::BudgetExceeded { counter: "bytes_copied", .. }))));
        assert_eq!(meter.work.allocation_operations, 0);
        assert_eq!(meter.work.bytes_copied, 0);
        assert_eq!(meter.work.backend_read_operations, 0);
        Ok(())
    }

    #[test]
    fn cancelled_path_conversion_performs_no_work() -> Result<(), Box<dyn std::error::Error>> {
        let path = NamespacePath::new(vec![LogicalName::new(NameEncoding::Utf8, b"file".to_vec(), 255)?],
            crate::model::VolumeLimits::default())?;
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        assert!(namespace_to_host_path_measured(&path, &mut meter).is_err());
        assert_eq!(meter.work, WorkCounters::UNCHARGED);
        Ok(())
    }

    #[test]
    fn multipath_failure_keeps_first_observation_work() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("first"), b"one")?;
        std::fs::write(directory.path().join("second"), b"two")?;
        let root = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let limits = crate::model::VolumeLimits::default();
        let path = |name: &[u8]| -> Result<NamespacePath, Box<dyn std::error::Error>> {
            Ok(NamespacePath::new(vec![LogicalName::new(NameEncoding::Utf8, name.to_vec(), 255)?], limits)?)
        };
        let paths = [path(b"first")?, path(b"second")?];
        let cancellation = CancellationToken::new();
        let mut probe = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        root.stat(Path::new("first"), &mut probe)?;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.backend_read_operations = probe.work.backend_read_operations;
        let mut meter = DemandWork::new(budget, &cancellation);
        let unique = clone_capture_paths(&paths, None, &mut meter)?;
        let cloned = meter.work;
        let failure = observe_unique_paths(unique, &root, budget, &cancellation, &mut meter)
            .err().ok_or("second observation unexpectedly fit")?;
        assert!(matches!(failure.error, CaptureError::Work(WorkError::BudgetExceeded { counter: "backend_read_operations", .. })));
        assert_eq!(failure.work.backend_read_operations, probe.work.backend_read_operations);
        assert_eq!(failure.work.source_entries_visited, 2);
        assert!(failure.work.allocation_operations > cloned.allocation_operations);
        assert_eq!(*failure.work, meter.work);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn subtree_walk_rejects_intermediate_symlinks() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir()?;
        let outside = tempfile::tempdir()?;
        std::fs::write(outside.path().join("private"), b"outside")?;
        symlink(outside.path(), directory.path().join("alias"))?;
        let source_root = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        assert!(source_root.open_dir_held(Path::new("alias"), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error).is_err());
        assert!(
            source_root
                .stat(Path::new("alias/private"), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)
                .is_err()
        );
        Ok(())
    }
}

async fn capture_observed_paths_from_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    ordered: Vec<(Option<HostObservation>, NamespacePath)>,
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
    meter: &CaptureWorkScope,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let result = capture_in_heap_with_meter(|| async move {
        let (mut receipt, observations, paths) = {
            let mut original = capture_meter(meter);
            let mut receipt = CaptureReceipt { work: original.completed(), ..CaptureReceipt::default() };
            let ordered_bytes = ordered.capacity() as u64 * std::mem::size_of::<ObservedPath>() as u64;
            let mut observations = capture_vec(ordered.len(), &mut original)
                .map_err(|error| OperationFailure::new(error, original.completed()))?;
            let mut paths = capture_vec(ordered.len(), &mut original)
                .map_err(|error| OperationFailure::new(error, original.completed()))?;
            for (observation, path) in ordered {
                observations.push(observation);
                paths.push(path);
            }
            capture_step(&mut receipt.work, &mut original, |meter| meter.release(ordered_bytes).map_err(capture_demand_error))?;
            (receipt, observations, paths)
        };
        let current = if paths.is_empty() {
            Vec::new()
        } else {
            let lookup_cancellation = capture_meter(meter).leaf(|work| work.cancellation().clone());
            let lookup = capture_sdk_in_heap_with_meter(
                |native| checkout.lookup_batch_no_follow_with_native_work(&paths, native, &lookup_cancellation),
                meter, &lookup_cancellation,
            ).await?;
            receipt.work = lookup.work;
            let lookup = lookup.value;
            let mut original = capture_meter(meter);
            let mut records = capture_vec(lookup.0.entries.len(), &mut original)
                .map_err(|error| OperationFailure::new(error, original.completed()))?;
            for entry in lookup.0.entries {
                records.push(entry.record);
            }
            drop(lookup.1);
            records
        };
        let states = {
            let mut original = capture_meter(meter);
            let states = order_capture_states(observations, paths, current, &mut original)
                .map_err(|error| OperationFailure::new(error, original.completed()))?;
            receipt.work = original.completed();
            states
        };
        let mut host_links = CaptureMap::new();
        let captured = capture_ranked_paths_from_root(
            checkout, states, &mut host_links, maximum_extent_spans, source_root,
            budget, cancellation, baseline, meter,
        ).await;
        {
            let mut original = capture_meter(meter);
            receipt.work = original.completed();
            let cleanup = capture_step(&mut receipt.work, &mut original, |meter| release_host_links(host_links, meter));
            let captured = match captured {
                Ok(captured) => {
                    cleanup?;
                    captured
                }
                Err(mut failure) => {
                    *failure.work = original.completed();
                    return Err(failure);
                }
            };
            receipt.examined_paths = captured.value.examined_paths;
            receipt.changed_paths = captured.value.changed_paths;
            receipt.staged_file_bytes = captured.value.staged_file_bytes;
            receipt.work = original.completed();
        }
        Ok(OperationReceipt { value: receipt, work: receipt.work })
    }, meter.original()).await;
    let work = capture_meter(meter).completed();
    match result {
        Ok(mut captured) => {
            captured.work = work;
            captured.value.work = work;
            Ok(captured)
        }
        Err(mut failure) => {
            *failure.work = work;
            Err(failure)
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the checkout, ranked states, link map, span bound, source root, budget, cancellation, and baseline are all inputs of one capture"
)]
async fn capture_ranked_paths_from_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    states: Vec<CapturePathState>,
    host_links: &mut CaptureMap<[u8; 16], HostLinkSource>,
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    _budget: WorkBudget,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
    meter: &CaptureWorkScope,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let states_bytes = states.capacity() as u64 * std::mem::size_of::<CapturePathState>() as u64;
    let (mut receipt, mut prepared, budget, cancellation) = {
        let mut original = capture_meter(meter);
        cancellation.check()
            .map_err(|_| OperationFailure::new(CaptureError::Cancelled, original.completed()))?;
        original.check()
            .map_err(|error| OperationFailure::new(capture_demand_error(error), original.completed()))?;
        let prepared = capture_vec(states.len(), &mut original)
            .map_err(|error| OperationFailure::new(error, original.completed()))?;
        (
            CaptureReceipt { work: original.completed(), ..CaptureReceipt::default() },
            prepared, original.leaf(|work| work.budget), original.leaf(|work| work.cancellation().clone()),
        )
    };
    let mut mutations = Vec::new();
    for (observation, path, current) in states {
        let plan = prepare_final_path(
            checkout, path, CurrentRecord::Known(current), CaptureIntent::Complete,
            source_root, observation, host_links, None, baseline, &mut mutations,
            &mut receipt, budget, &cancellation, meter,
        ).await?;
        if let Some(plan) = plan {
            prepared.push(plan);
        }
    }
    {
        let mut original = capture_meter(meter);
        capture_step(&mut receipt.work, &mut original, |meter| meter.release(states_bytes).map_err(capture_demand_error))?;
    }
    finish_prepared_paths(
        checkout, source_root, prepared, maximum_extent_spans,
        &mut mutations, &mut receipt, budget, &cancellation, meter,
    ).await?;
    apply_capture_transaction(checkout, mutations, &mut receipt, budget, &cancellation, meter).await?;
    {
        let mut original = capture_meter(meter);
        receipt.work = original.completed();
        receipt.work.verify(original.leaf(|work| work.budget))
            .map_err(|error| OperationFailure::new(CaptureError::Work(error), original.completed()))?;
    }
    Ok(OperationReceipt { value: receipt, work: receipt.work })
}

type CapturePathState = (Option<HostObservation>, NamespacePath, Option<FileRecord>);

fn order_capture_states(
    observations: Vec<Option<HostObservation>>,
    paths: Vec<NamespacePath>,
    current: Vec<Option<FileRecord>>,
    meter: &mut impl CaptureNativeWork,
) -> Result<Vec<CapturePathState>, CaptureError> {
    let input_bytes = observations.capacity() as u64 * std::mem::size_of::<Option<HostObservation>>() as u64
        + paths.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64
        + current.capacity() as u64 * std::mem::size_of::<Option<FileRecord>>() as u64;
    let mut states = capture_vec(paths.len(), meter)?;
    for ((observation, path), current) in observations.into_iter().zip(paths).zip(current) {
        meter.check().map_err(capture_demand_error)?;
        states.push((observation, path, current));
    }
    meter.release(input_bytes).map_err(capture_demand_error)?;
    // Remove checkout-only descendants first, before an observed ancestor
    // changes kind. Existing bindings then lead newly observed hard-link
    // aliases so an earlier-sorting alias cannot replace a stable SDK FileId.
    sort_capture_states(&mut states, meter)?;
    Ok(states)
}

fn sort_capture_states(states: &mut [CapturePathState], meter: &mut impl CaptureNativeWork) -> Result<(), CaptureError> {
    capture_sort_by(states, |(left, left_path, left_current), (right, right_path, right_current)| {
        let rank = |observation: &Option<HostObservation>, current: &Option<FileRecord>| {
            match observation {
                None => 0,
                Some(observation) if observation.metadata.is_dir() => 1,
                Some(_) if current.is_some() => 2,
                Some(_) => 3,
            }
        };
        let left_rank = rank(left, left_current);
        let right_rank = rank(right, right_current);
        left_rank.cmp(&right_rank).then_with(|| {
            if left_rank == 0 {
                right_path
                    .depth()
                    .cmp(&left_path.depth())
                    .then_with(|| left_path.cmp(right_path))
            } else {
                left_path
                    .depth()
                    .cmp(&right_path.depth())
                    .then_with(|| left_path.cmp(right_path))
            }
        })
    }, meter)
}

/// Authenticates one complete bounded host baseline into a checkout candidate.
///
/// The scan takes the exact union of host and checkout paths, so host additions,
/// changes, and deletions become one authored transaction. Directories are
/// admitted before descendants and removals are applied deepest-first. Native
/// links are never followed. Callers bracket this operation with
/// [`crate::NativeWatch::begin_rescan`] and `finish_rescan`; events arriving
/// during the scan remain change hints for the next observation interval.
/// Native notifications alone do not prove that a later tool boundary has
/// captured every write, so callers without complete change evidence must
/// reconcile the subtree before publication.
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
///
/// The numeric budget funds one original native accounting state, shared by
/// the complete baseline and any still-live intrinsic source payloads.
pub async fn capture_baseline_with_policy<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    options: &CaptureOptions,
    policy: &CapturePolicy,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let span = crate::obs::span!(
        INFO,
        "acyclic.fs.native_capture.baseline_with_policy",
        outcome = crate::obs::Empty,
        error.kind = crate::obs::Empty,
        work.items = crate::obs::Empty,
        work.bytes = crate::obs::Empty,
        work.durability = crate::obs::Empty,
    );
    let result = crate::obs::in_span(&span, async {
        let original = capture_original_work(budget, cancellation)?;
        let native = capture_native_original(&original)?;
        let result = capture_baseline_with_native_work(
            checkout, options, policy, cancellation, &native,
        ).await;
        capture_original_result(&original, result).map(|mut receipt| {
            receipt.value.work = receipt.work;
            receipt
        })
    }).await;
    crate::obs::measured_on(&span, result, |receipt| &receipt.work)
}

pub(crate) async fn capture_baseline_with_native_work<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    options: &CaptureOptions,
    policy: &CapturePolicy,
    cancellation: &CancellationToken,
    work: &crate::NativeMaterializationWork,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let meter = work.native_read_meter();
    let completed = { capture_ledger_meter(meter).work };
    if checkout.has_pending_mutations() {
        return Err(OperationFailure::new(CaptureError::DirtyCheckout, completed));
    }
    if options.maximum_paths == 0 || options.maximum_extent_spans == 0 {
        return Err(OperationFailure::new(CaptureError::InvalidOptions, completed));
    }
    cancellation.check()
        .map_err(|_| OperationFailure::new(CaptureError::Cancelled, completed))?;
    let (checked, completed) = {
        let held = capture_ledger_meter(meter);
        (held.check(), held.work)
    };
    checked.map_err(|error| OperationFailure::new(capture_demand_error(error), completed))?;
    let scope = CaptureWorkScope::new(work)?;
    let meter = &scope;
    let result = capture_in_heap_with_meter(|| async {
        let (budget, maximum) = {
            let mut held = capture_meter(meter);
            let maximum = usize::try_from(options.maximum_paths)
                .map_err(|_| OperationFailure::new(CaptureError::InvalidOptions, held.completed()))?;
            (held.leaf(|work| work.budget), maximum)
        };
        let mut host_work = CaptureNativeHostWork::new(meter);
        let source_root = open_source_root(options, &mut host_work)?;
        let limits = checkout.volume_config().limits;
        let profile = checkout.volume_config().profile;
        let mut paths = Vec::new();
        let mut work = host_work.completed();
        let observed = collect_host_observations(
            &source_root, profile, limits, maximum, policy, &mut work,
            budget, cancellation, &mut host_work,
        )?;
        collect_checkout_paths(
            checkout, limits, policy, maximum, &mut paths, &mut work,
            budget, cancellation, meter,
        ).await?;
        let ordered = {
            let mut held = capture_meter(meter);
            order_host_checkout_union(observed, paths, maximum, &mut held)
                .map_err(|error| OperationFailure::new(error, held.completed()))?
        };
        capture_observed_paths_from_root(
            checkout, ordered, options.maximum_extent_spans, &source_root,
            budget, cancellation, None, meter,
        ).await
    }, meter.original()).await;
    capture_scope_result(scope, result).map(|mut receipt| {
        receipt.value.work = receipt.work;
        receipt
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the host walk needs the root, profile, limits, bound, and policy plus work, budget, and cancellation"
)]
fn collect_host_observations<W: CaptureNativeWork>(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    policy: &CapturePolicy,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &mut W,
) -> Result<CaptureMap<NamespacePath, HostObservation>, OperationFailure<CaptureError>> {
    let mut observed = CaptureMap::new();
    let volume_root = NamespacePath::new(Vec::new(), limits)
        .map_err(|error| OperationFailure::new(CaptureError::Path(error), meter.completed()))?;
    collect_host_subtree_observations(root,
    profile,
    limits,
    maximum,
    CaptureHostPath::new(),
    volume_root,
    policy,
    &mut observed,
    work,
    budget,
    cancellation, meter)?;
    Ok(observed)
}

fn order_host_checkout_union(
    observed: CaptureMap<NamespacePath, HostObservation>,
    checkout_paths: Vec<NamespacePath>,
    maximum: usize,
    meter: &mut impl CaptureNativeWork,
) -> Result<Vec<(Option<HostObservation>, NamespacePath)>, CaptureError> {
    let checkout_bytes = checkout_paths.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    let observed_bytes = observed.storage_bytes();
    let mut checkout_paths = checkout_paths;
    capture_sort_by(&mut checkout_paths, Ord::cmp, meter)?;
    let mut absent: Vec<NamespacePath> = capture_vec(checkout_paths.len(), meter)?;
    for path in checkout_paths {
        capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if observed.contains_key_measured(&path, meter)? || absent.last().is_some_and(|prior| prior == &path) {
            capture_drop_path(path, meter)?;
        } else {
            absent.push(path);
        }
    }
    meter.release(checkout_bytes).map_err(capture_demand_error)?;
    if combined_path_count_exceeds(observed.len(), absent.len(), maximum) {
        return Err(CaptureError::InvalidOptions);
    }
    let mut ordered = capture_vec(observed.len() + absent.len(), meter)?;
    for (path, observation) in observed {
        meter.check().map_err(capture_demand_error)?;
        ordered.push((Some(observation), path));
    }
    meter.release(observed_bytes).map_err(capture_demand_error)?;
    capture_sort_by(&mut ordered, |(_, left), (_, right)| {
        left.depth()
            .cmp(&right.depth())
            .then_with(|| left.cmp(right))
    }, meter)?;
    let absent_bytes = absent.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    capture_sort_by(&mut absent, |left, right| {
        right
            .depth()
            .cmp(&left.depth())
            .then_with(|| left.cmp(right))
    }, meter)?;
    ordered.extend(absent.into_iter().map(|path| (None, path)));
    meter.release(absent_bytes).map_err(capture_demand_error)?;
    Ok(ordered)
}

fn combined_path_count_exceeds(observed: usize, checkout_only: usize, maximum: usize) -> bool {
    observed
        .checked_add(checkout_only)
        .is_none_or(|combined| combined > maximum)
}

fn insert_host_observation(
    observed: &mut CaptureMap<NamespacePath, HostObservation>,
    path: NamespacePath,
    metadata: HostStat,
    maximum: usize,
    meter: &mut impl CaptureNativeWork,
) -> Result<(), CaptureError> {
    if !observed.contains_key_measured(&path, meter)? && observed.len() >= maximum {
        return Err(CaptureError::InvalidOptions);
    }
    capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
    observed.insert_measured(path, HostObservation::from_metadata(metadata), meter)?;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the host walk needs the root, profile, limits, bound, roots, and policy plus the shared map, work, budget, and cancellation"
)]
fn collect_host_subtree_roots<W: CaptureNativeWork>(
    source_root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    roots: &[NamespacePath],
    policy: &CapturePolicy,
    observed: &mut CaptureMap<NamespacePath, HostObservation>,
    work: &mut WorkCounters,
    _budget: WorkBudget,
    _cancellation: &CancellationToken,
    meter: &mut W,
) -> Result<(), OperationFailure<CaptureError>> {
    let result = (|| {
        for root in roots {
            let host_root = namespace_to_host_path_measured(root, meter)?;
            if let Some(observation) = observe_host_path(source_root, &host_root, meter)? {
                let recurse = observation.metadata.is_dir() && !observation.metadata.file_type().is_symlink();
                let path = clone_capture_path(root, meter)?;
                insert_host_observation(observed, path, observation.metadata, maximum, meter)?;
                if recurse {
                    let path = clone_capture_path(root, meter)?;
                    collect_host_subtree_observations_measured(
                        source_root, profile, limits, maximum, host_root, path, policy, observed, meter,
                    )?;
                } else {
                    capture_drop_host_path(host_root, meter)?;
                }
            }
            else {
                capture_drop_host_path(host_root, meter)?;
            }
        }
        Ok(())
    })();
    *work = meter.completed();
    result.map_err(|error| OperationFailure::new(error, *work))
}

#[allow(
    clippy::too_many_arguments,
    reason = "the host walk needs the root, profile, limits, bound, subtree, and policy plus the shared map, work, budget, and cancellation"
)]
fn collect_host_subtree_observations<W: CaptureNativeWork>(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    host_root: CaptureHostPath,
    volume_root: NamespacePath,
    policy: &CapturePolicy,
    observed: &mut CaptureMap<NamespacePath, HostObservation>,
    work: &mut WorkCounters,
    _budget: WorkBudget,
    _cancellation: &CancellationToken,
    meter: &mut W,
) -> Result<(), OperationFailure<CaptureError>> {
    let result = collect_host_subtree_observations_measured(
        root, profile, limits, maximum, host_root, volume_root, policy, observed, meter,
    );
    *work = meter.completed();
    result.map_err(|error| OperationFailure::new(error, *work))
}

#[allow(
    clippy::too_many_arguments,
    reason = "the host walk needs the root, profile, limits, subtree, policy, and bound plus the path sink, work, budget, and cancellation"
)]
fn collect_host_subtree_paths<P: ScannedPaths, W: CaptureNativeWork>(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    host_root: CaptureHostPath,
    volume_root: NamespacePath,
    policy: &CapturePolicy,
    maximum: usize,
    paths: &mut P,
    work: &mut WorkCounters,
    _budget: WorkBudget,
    _cancellation: &CancellationToken,
    meter: &mut W,
) -> Result<(), OperationFailure<CaptureError>> {
    let result = collect_host_walk_measured(root, profile, limits, host_root, volume_root, policy, meter,
        |path, _, meter| append_scanned_path(paths, path, maximum, meter));
    *work = meter.completed();
    result.map_err(|error| OperationFailure::new(error, *work))
}

#[allow(
    clippy::too_many_arguments,
    reason = "the checkout walk needs limits, policy, and bound plus the path sink, work, budget, and cancellation"
)]
async fn collect_checkout_paths<A: AsyncAuthorityStore, O: AsyncObjectStore, P: ScannedPaths>(
    checkout: &mut Checkout<A, O>,
    limits: crate::model::VolumeLimits,
    policy: &CapturePolicy,
    maximum: usize,
    paths: &mut P,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(), OperationFailure<CaptureError>> {
    let result = capture_in_heap_with_meter(|| {
        let work = &mut *work;
        async move {
            let root = NamespacePath::new(Vec::new(), limits).map_err(|error| {
                OperationFailure::new(CaptureError::Path(error), capture_meter(meter).completed())
            })?;
            collect_checkout_subtree_paths(
                checkout, limits, root, policy, maximum, paths, work, budget, cancellation, meter,
            ).await?;
            Ok(OperationReceipt { value: (), work: capture_meter(meter).completed() })
        }
    }, meter.original()).await;
    *work = capture_meter(meter).completed();
    result.map(|_| ())
}


#[allow(
    clippy::too_many_arguments,
    reason = "the checkout walk needs limits, subtree, policy, and bound plus the path sink, work, budget, and cancellation"
)]
async fn collect_checkout_subtree_paths<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    P: ScannedPaths,
>(
    checkout: &mut Checkout<A, O>,
    limits: crate::model::VolumeLimits,
    root: NamespacePath,
    policy: &CapturePolicy,
    maximum: usize,
    paths: &mut P,
    work: &mut WorkCounters,
    _budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(), OperationFailure<CaptureError>> {
    const PAGE_ENTRIES: u32 = 1024;
    let mut pending = {
        let mut original = capture_meter(meter);
        cancellation.check()
            .map_err(|_| OperationFailure::new(CaptureError::Cancelled, original.completed()))?;
        capture_step(work, &mut original, |meter| capture_vec(1, meter))?
    };
    pending.push(root);
    while let Some(directory) = pending.pop() {
        let mut after = None;
        loop {
            let page_cancellation = capture_meter(meter).leaf(|work| work.cancellation().clone());
            let page = capture_sdk_in_heap_with_meter(
                |native| checkout.list_directory_records_with_native_work(
                    &directory, after.as_ref(), PAGE_ENTRIES, native, &page_cancellation,
                ),
                meter, &page_cancellation,
            ).await?;
            *work = page.work;
            let page = page.value;
            let has_more = {
                let mut original = capture_meter(meter);
                capture_step(work, &mut original, |meter| capture_charge(meter,
                    WorkCounters { items_examined: page.0.entries.len() as u64, ..WorkCounters::UNCHARGED }))?;
                if page.0.has_more && page.0.entries.is_empty() {
                    return Err(OperationFailure::new(
                        CaptureError::Invariant("directory pagination made no progress"), original.completed(),
                    ));
                }
                for entry in &page.0.entries {
                    let child = capture_step(work, &mut original, |meter| {
                        capture_charge(meter, WorkCounters { source_entries_visited: 1, ..WorkCounters::UNCHARGED })?;
                        let name = clone_capture_name(&entry.name, meter)?;
                        append_path_measured(&directory, name, limits, meter)
                    })?;
                    if capture_step(work, &mut original, |meter| capture_excludes(policy, &child, meter))? {
                        capture_step(work, &mut original, |meter| capture_drop_path(child, meter))?;
                        continue;
                    }
                    let scanned = capture_path_clone(&child, work, &mut original)?;
                    capture_step(work, &mut original, |meter| append_scanned_path(paths, scanned, maximum, meter))?;
                    if entry.record.kind == FileKind::Directory {
                        capture_step(work, &mut original, |meter| capture_reserve(&mut pending, 1, meter))?;
                        pending.push(child);
                    } else {
                        capture_step(work, &mut original, |meter| capture_drop_path(child, meter))?;
                    }
                }
                if let Some(previous) = after.take() {
                    let bytes = previous.as_bytes().len() as u64;
                    drop(previous);
                    capture_step(work, &mut original, |meter| meter.release(bytes).map_err(capture_demand_error))?;
                }
                after = match page.0.entries.last() {
                    Some(entry) => Some(capture_step(work, &mut original, |meter| clone_capture_name(&entry.name, meter))?),
                    None => None,
                };
                let has_more = page.0.has_more;
                drop(page);
                has_more
            };
            if !has_more {
                break;
            }
        }
        {
            let mut original = capture_meter(meter);
            if let Some(cursor) = after {
                let bytes = cursor.as_bytes().len() as u64;
                drop(cursor);
                capture_step(work, &mut original, |meter| meter.release(bytes).map_err(capture_demand_error))?;
            }
            capture_step(work, &mut original, |meter| capture_drop_path(directory, meter))?;
        }
    }
    let pending_bytes = pending.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    drop(pending);
    let mut original = capture_meter(meter);
    capture_step(work, &mut original, |meter| meter.release(pending_bytes).map_err(capture_demand_error))?;
    Ok(())
}

trait ScannedPaths {
    fn len(&self) -> usize;
    fn contains_measured(&self, path: &NamespacePath, meter: &mut impl CaptureNativeWork) -> Result<bool, CaptureError>;
    fn insert_measured(&mut self, path: NamespacePath, meter: &mut impl CaptureNativeWork) -> Result<(), CaptureError>;
}

impl ScannedPaths for Vec<NamespacePath> {
    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn contains_measured(&self, _path: &NamespacePath, meter: &mut impl CaptureNativeWork) -> Result<bool, CaptureError> {
        meter.check().map_err(capture_demand_error)?;
        Ok(false)
    }

    fn insert_measured(&mut self, path: NamespacePath, meter: &mut impl CaptureNativeWork) -> Result<(), CaptureError> {
        capture_reserve(self, 1, meter)?;
        self.push(path);
        Ok(())
    }
}

impl ScannedPaths for CaptureSet<NamespacePath> {
    fn len(&self) -> usize {
        CaptureSet::len(self)
    }

    fn contains_measured(&self, path: &NamespacePath, meter: &mut impl CaptureNativeWork) -> Result<bool, CaptureError> {
        CaptureSet::contains_measured(self, path, meter)
    }

    fn insert_measured(&mut self, path: NamespacePath, meter: &mut impl CaptureNativeWork) -> Result<(), CaptureError> {
        CaptureSet::insert_measured(self, path, meter)
    }
}

fn append_scanned_path<P: ScannedPaths>(
    paths: &mut P,
    path: NamespacePath,
    maximum: usize,
    meter: &mut impl CaptureNativeWork,
) -> Result<(), CaptureError> {
    if paths.contains_measured(&path, meter)? {
        return capture_drop_path(path, meter);
    }
    if paths.len() >= maximum { return Err(CaptureError::InvalidOptions); }
    capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
    paths.insert_measured(path, meter)
}

fn capture_drop_path(path: NamespacePath, meter: &mut impl CaptureNativeWork) -> Result<(), CaptureError> {
    let bytes = namespace_owned_bytes(&path)?;
    drop(path);
    meter.release(bytes).map_err(capture_demand_error)
}

fn capture_watch_owned_bytes(changes: &[WatchChange], incoming: &crate::watch::WatchBatchOriginal,
    storage_bytes: u64, meter: &mut impl CaptureNativeWork) -> Result<u64, CaptureError> {
    let mut bytes = storage_bytes;
    for (index, change) in changes.iter().enumerate() {
        if incoming.change_has_native_payload_custody(index) { continue; }
        let (first, second) = match change {
            WatchChange::Created(path) | WatchChange::Modified(path) | WatchChange::Arrived(path)
            | WatchChange::Removed(path) | WatchChange::MetadataChanged(path) => (path, None),
            WatchChange::Renamed { from, to } => (from, Some(to)),
        };
        for path in std::iter::once(first).chain(second) {
            capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
            for _ in path.components() {
                capture_charge(meter, WorkCounters { source_path_components: 1, items_examined: 1,
                    ..WorkCounters::UNCHARGED })?;
            }
            bytes = bytes.checked_add(namespace_owned_bytes(path)?).ok_or(CaptureError::Work(WorkError::Overflow))?;
        }
    }
    Ok(bytes)
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
        .map_err(CaptureError::Name)
}

/// Converts one host-relative path to an exact bounded namespace path.
///
/// The supplied profile selects the canonical name encoding. Absolute paths,
/// parent traversal, platform prefixes, empty paths, and names that cannot be
/// represented exactly by that profile are rejected.
///
/// # Errors
///
/// Returns [`CaptureError::UnrepresentablePath`] for an inexact host spelling,
/// or preserves canonical validation failures as [`CaptureError::Name`] and
/// [`CaptureError::Path`].
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
    NamespacePath::new(components, limits).map_err(CaptureError::Path)
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
///
/// The budget facade funds one original native state shared with the watch
/// engine. SDK waits never retain its mutex; terminal joins never wait for
/// unique ownership. All receipts retain the original invocation's prefix.
pub async fn capture_watch_batch_with_policy<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    batch: WatchBatch,
    options: &CaptureOptions,
    policy: &CapturePolicy,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<WatchCaptureReceipt>, OperationFailure<CaptureError>> {
    let original = capture_original_work(budget, cancellation)?;
    let native = capture_native_original(&original)?;
    let result = capture_watch_batch_with_native_work(
        checkout, batch, options, policy, cancellation, &native,
    ).await;
    capture_original_result(&original, result).map(|mut captured| {
        captured.value.capture.work = captured.work;
        captured
    })
}

/// Replays one watcher batch with the caller's full original native resource.
pub(crate) async fn capture_watch_batch_with_native_work<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    batch: WatchBatch,
    options: &CaptureOptions,
    policy: &CapturePolicy,
    cancellation: &CancellationToken,
    work: &crate::NativeMaterializationWork,
) -> Result<OperationReceipt<WatchCaptureReceipt>, OperationFailure<CaptureError>> {
    let meter = work.native_read_meter();
    let mut scope = CaptureWorkScope::new(work)?;
    let result = capture_watch_batch_in_scope(
        checkout, batch, options, policy, cancellation, &scope,
    ).await;
    scope.release()?;
    let completed = meter.lock().work;
    match result {
        Ok(mut captured) => {
            captured.work = completed;
            captured.value.capture.work = completed;
            Ok(captured)
        }
        Err(failure) => Err(OperationFailure::new(failure.error, completed)),
    }
}

/// Replays one watcher batch against the original shared invocation ledger.
#[allow(
    clippy::too_many_lines,
    reason = "one watcher batch is classified, staged, and applied as a single atomic capture"
)]
async fn capture_watch_batch_in_scope<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    batch: WatchBatch,
    options: &CaptureOptions,
    policy: &CapturePolicy,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<OperationReceipt<WatchCaptureReceipt>, OperationFailure<CaptureError>> {
    // The facade opens fixed accounting leaves only; owned helpers and drops
    // remain outside those short original-meter loans.
    fn capture_step<T>(
        work: &mut WorkCounters,
        meter: &CaptureWorkScope,
        operation: impl FnOnce(&mut CaptureNativeHostWork<'_>) -> Result<T, CaptureError>,
    ) -> Result<T, OperationFailure<CaptureError>> {
        self::capture_step(work, &mut capture_meter(meter), operation)
    }

    fn capture_path_clone(
        path: &NamespacePath,
        work: &mut WorkCounters,
        meter: &CaptureWorkScope,
    ) -> Result<NamespacePath, OperationFailure<CaptureError>> {
        self::capture_path_clone(path, work, &mut capture_meter(meter))
    }

    let span = crate::obs::span!(
        INFO,
        "acyclic.fs.native_capture.watch_batch_with_policy",
        outcome = crate::obs::Empty,
        error.kind = crate::obs::Empty,
        work.items = crate::obs::Empty,
        work.bytes = crate::obs::Empty,
        work.durability = crate::obs::Empty,
    );
    let (budget, cancellation) = {
        let mut original = capture_meter(meter);
        cancellation.check()
            .map_err(|_| OperationFailure::new(CaptureError::Cancelled, original.completed()))?;
        original.check()
            .map_err(|error| OperationFailure::new(capture_demand_error(error), original.completed()))?;
        if let WatchBatch::Changes { changes, original: incoming, .. } = &batch {
            let storage_bytes = if incoming.has_native_changes_storage_custody() { 0 } else {
                (changes.capacity() as u64).checked_mul(std::mem::size_of::<WatchChange>() as u64)
                    .ok_or_else(|| OperationFailure::new(CaptureError::Work(WorkError::Overflow), original.completed()))?
            };
            let bytes = capture_watch_owned_bytes(changes, incoming, storage_bytes, &mut original)
                .map_err(|error| OperationFailure::new(error, original.completed()))?;
            original.leaf(|work| work.adopt(bytes))
                .map_err(|error| OperationFailure::new(capture_demand_error(error), original.completed()))?;
        }
        (original.leaf(|work| work.budget), original.leaf(|work| work.cancellation().clone()))
    };
    let result = crate::obs::in_span(&span, capture_in_heap_with_meter(|| async move {
        let WatchBatch::Changes {
            original: incoming,
            epoch,
            first_sequence,
            next_sequence,
            changes,
        } = batch
        else {
            let WatchBatch::RescanRequired { epoch, reason } = batch else {
                unreachable!("watch batch variants are exhaustive")
            };
            return Err(OperationFailure::new(
                CaptureError::RescanRequired {
                    epoch: epoch.get(),
                    reason,
                },
                capture_meter(meter).completed(),
            ));
        };
        let changes_storage_native = incoming.has_native_changes_storage_custody();
        // Keep original service-backed custody after the physical change Vec.
        // The loop moves this Vec into its iterator; on early return that
        // iterator and all later capture-owned locals retire before the guard.
        let incoming = (changes, incoming);
        if options.maximum_paths == 0
            || options.maximum_extent_spans == 0
            || incoming.0.len() > usize::try_from(options.maximum_paths).unwrap_or(usize::MAX)
        {
            return Err(OperationFailure::new(CaptureError::InvalidOptions, capture_meter(meter).completed()));
        }
        let source_root = open_source_root(options, &mut CaptureNativeHostWork::new(meter))?;
        let mut receipt = CaptureReceipt { work: capture_meter(meter).completed(), ..CaptureReceipt::default() };
        let changes_bytes = incoming.0.capacity() as u64 * std::mem::size_of::<WatchChange>() as u64;
        let mut mutations = Vec::new();
        let mut ordinary = CaptureMap::new();
        let mut rename_records = CaptureMap::<NamespacePath, FileRecord>::new();
        let mut renamed_directories = Vec::new();
        let mut rename_destinations = Vec::new();
        let mut moved_away = CaptureSet::new();
        
        for (index, change) in incoming.0.into_iter().enumerate() {
            let change = if incoming.1.change_has_native_payload_custody(index) {
                let copied = match &change {
                    WatchChange::Created(path) => WatchChange::Created(capture_path_clone(path, &mut receipt.work, meter)?),
                    WatchChange::Modified(path) => WatchChange::Modified(capture_path_clone(path, &mut receipt.work, meter)?),
                    WatchChange::Arrived(path) => WatchChange::Arrived(capture_path_clone(path, &mut receipt.work, meter)?),
                    WatchChange::Removed(path) => WatchChange::Removed(capture_path_clone(path, &mut receipt.work, meter)?),
                    WatchChange::MetadataChanged(path) => WatchChange::MetadataChanged(capture_path_clone(path, &mut receipt.work, meter)?),
                    WatchChange::Renamed { from, to } => WatchChange::Renamed {
                        from: capture_path_clone(from, &mut receipt.work, meter)?,
                        to: capture_path_clone(to, &mut receipt.work, meter)?,
                    },
                };
                drop(change);
                copied
            } else { change };
            capture_step(&mut receipt.work, meter, |meter| capture_charge(meter,
                WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
            let change = match change {
                WatchChange::Renamed { from, to } => {
                    let excluded_from = capture_step(&mut receipt.work, meter, |meter| capture_excludes(policy, &from, meter))?;
                    let excluded_to = capture_step(&mut receipt.work, meter, |meter| capture_excludes(policy, &to, meter))?;
                    match (excluded_from, excluded_to) {
                        (false, false) => WatchChange::Renamed { from, to },
                        (true, false) => {
                            capture_step(&mut receipt.work, meter, |meter| capture_drop_path(from, meter))?;
                            WatchChange::Created(to)
                        }
                        (false, true) => {
                            capture_step(&mut receipt.work, meter, |meter| capture_drop_path(to, meter))?;
                            WatchChange::Removed(from)
                        }
                        (true, true) => {
                            capture_step(&mut receipt.work, meter, |meter| capture_drop_path(from, meter))?;
                            capture_step(&mut receipt.work, meter, |meter| capture_drop_path(to, meter))?;
                            continue;
                        }
                    }
                }
                change => {
                    let path = match &change {
                        WatchChange::Created(path) | WatchChange::Modified(path) | WatchChange::Arrived(path)
                        | WatchChange::Removed(path) | WatchChange::MetadataChanged(path) => path,
                        WatchChange::Renamed { .. } => unreachable!("renames were classified above"),
                    };
                    if capture_step(&mut receipt.work, meter, |meter| capture_excludes(policy, path, meter))? {
                        let bytes = capture_step(&mut receipt.work, meter, |_| namespace_owned_bytes(path))?;
                        drop(change);
                        capture_step(&mut receipt.work, meter, |meter| {
                            meter.release(bytes).map_err(capture_demand_error)
                        })?;
                        continue;
                    }
                    change
                }
            };
            match change {
                WatchChange::Created(path) => {
                    let current = capture_step(&mut receipt.work, meter, |meter| current_record(&rename_records, &moved_away, &path, meter))?;
                    capture_step(&mut receipt.work, meter, |meter| ordinary.insert_measured(path, (current, CaptureIntent::Replace), meter))?;
                }
                WatchChange::Modified(path) | WatchChange::Removed(path) => {
                    let current = capture_step(&mut receipt.work, meter, |meter| current_record(&rename_records, &moved_away, &path, meter))?;
                    capture_step(&mut receipt.work, meter, |meter| ordinary.get_or_insert_measured(path, (current, CaptureIntent::Complete), meter).map(|_| ()))?;
                }
                WatchChange::Arrived(path) => {
                    let current = capture_step(&mut receipt.work, meter, |meter| current_record(&rename_records, &moved_away, &path, meter))?;
                    capture_step(&mut receipt.work, meter, |meter| {
                        let entry = ordinary.get_or_insert_measured(path, (current, CaptureIntent::Arrived), meter)?;
                        if entry.1 != CaptureIntent::Replace { entry.1 = CaptureIntent::Arrived; }
                        Ok(())
                    })?;
                }
                WatchChange::MetadataChanged(path) => {
                    let current = capture_step(&mut receipt.work, meter, |meter| current_record(&rename_records, &moved_away, &path, meter))?;
                    capture_step(&mut receipt.work, meter, |meter| ordinary.get_or_insert_measured(path, (current, CaptureIntent::MetadataOnly), meter).map(|_| ()))?;
                }
                WatchChange::Renamed { from, to } => {
                    cancellation.check().map_err(|_| {
                        OperationFailure::new(
                            CaptureError::Cancelled,
                            receipt.work,
                        )
                    })?;
                    let prior_source = capture_step(&mut receipt.work, meter, |meter| ordinary.remove_measured(&from, meter))?;
                    let prior_source = if let Some((key, value)) = prior_source {
                        capture_step(&mut receipt.work, meter, |meter| capture_drop_path(key, meter))?;
                        Some(value)
                    } else { None };
                    if let Some((key, _)) = capture_step(&mut receipt.work, meter, |meter| ordinary.remove_measured(&to, meter))? {
                        capture_step(&mut receipt.work, meter, |meter| capture_drop_path(key, meter))?;
                    }
                    let record = if let Some((key, record)) = capture_step(&mut receipt.work, meter, |meter| rename_records.remove_measured(&from, meter))? {
                        capture_step(&mut receipt.work, meter, |meter| capture_drop_path(key, meter))?;
                        Some(record)
                    } else if matches!(prior_source, Some((CurrentRecord::Known(None), _)))
                        || capture_step(&mut receipt.work, meter, |meter| moved_away.contains_measured(&from, meter))?
                    {
                        None
                    } else {
                        let source = capture_sdk_in_heap_with_meter(
                            |native| checkout.lookup_no_follow(&from, native, &cancellation),
                            meter, &cancellation,
                        ).await?;
                        receipt.work = source.work;
                        match source.value.record {
                            Some(record) => Some(record),
                            None if prior_source.is_some() => None,
                            None => None,
                        }
                    };
                    if let Some(record) = record {
                        if record.kind == FileKind::Directory {
                            capture_step(&mut receipt.work, meter, |meter| capture_reserve(&mut renamed_directories, 1, meter))?;
                            renamed_directories.push((capture_path_clone(&from, &mut receipt.work, meter)?,
                                capture_path_clone(&to, &mut receipt.work, meter)?));
                        }
                        capture_step(&mut receipt.work, meter, |meter| capture_reserve(&mut rename_destinations, 1, meter))?;
                        rename_destinations.push(capture_path_clone(&to, &mut receipt.work, meter)?);
                        capture_step(&mut receipt.work, meter, |meter| capture_reserve(&mut mutations, 1, meter))?;
                        mutations.push(AuthoredMutation::Rename {
                            source: capture_path_clone(&from, &mut receipt.work, meter)?,
                            destination: capture_path_clone(&to, &mut receipt.work, meter)?,
                            replace: true,
                        });
                        let destination = capture_path_clone(&to, &mut receipt.work, meter)?;
                        capture_step(&mut receipt.work, meter, |meter| rename_records.insert_measured(destination, record, meter))?;
                    } else {
                        let destination = capture_path_clone(&to, &mut receipt.work, meter)?;
                        capture_step(&mut receipt.work, meter, |meter| ordinary.insert_measured(
                            destination, (CurrentRecord::Lookup, CaptureIntent::Replace), meter))?;
                    }
                    capture_step(&mut receipt.work, meter, |meter| moved_away.insert_measured(from, meter))?;
                    if let Some(key) = capture_step(&mut receipt.work, meter, |meter| moved_away.remove_measured(&to, meter))? {
                        capture_step(&mut receipt.work, meter, |meter| capture_drop_path(key, meter))?;
                    }
                    capture_step(&mut receipt.work, meter, |meter| capture_drop_path(to, meter))?;
                }
            }
        }
        if !changes_storage_native {
            capture_step(&mut receipt.work, meter, |meter| meter.release(changes_bytes).map_err(capture_demand_error))?;
        }
        drop(incoming.1);
        capture_step(&mut receipt.work, meter, |meter| release_capture_set(moved_away, meter))?;
        
        expand_directory_hints(checkout,
        &mut ordinary,
        &source_root,
        options.maximum_paths,
        policy,
        &mut receipt,
        budget,
        &cancellation, meter)
        .await?;
        
        // A watcher may report a nested file before any ancestor has been
        // observed in this lazy checkout. Capture only the missing ancestors,
        // rather than expanding their entire host subtrees as event roots.
        let maximum_paths = usize::try_from(options.maximum_paths).unwrap_or(usize::MAX);
        let ancestors = watch_ancestor_candidates(
            ordinary
                .keys()
                .chain(rename_records.keys())
                .chain(rename_destinations.iter()),
            &ordinary,
            &rename_records,
            policy,
            maximum_paths,
            &mut receipt.work,
            &cancellation,
            checkout.volume_config().limits,
            &mut capture_meter(meter),
        )?;
        capture_step(&mut receipt.work, meter, |meter| release_capture_paths(rename_destinations, meter))?;
        let mut missing_ancestors = hydrate_watch_ancestors(checkout,
        &mut ordinary,
        ancestors,
        WatchAncestorScope {
            source_root: &source_root,
            epoch,
            maximum_paths: maximum_paths.saturating_sub(rename_records.len()),
            budget,
            cancellation: &cancellation,
        },
        &mut receipt, meter)
        .await?;
        
        let mut lookup_paths = capture_step(&mut receipt.work, meter, |meter| capture_vec(ordinary.len(), meter))?;
        for (path, (current, _)) in ordinary.iter() {
            if matches!(current, CurrentRecord::Lookup) {
                lookup_paths.push(capture_path_clone(path, &mut receipt.work, meter)?);
            }
        }
        if !lookup_paths.is_empty() {
            let lookup = capture_sdk_in_heap_with_meter(
                |native| checkout.lookup_batch_no_follow_with_native_work(&lookup_paths, native, &cancellation),
                meter, &cancellation,
            ).await?;
            receipt.work = lookup.work;
            let lookup = lookup.value;
            let lookup_paths_bytes = lookup_paths.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
            for (path, entry) in lookup_paths.into_iter().zip(lookup.0.entries) {
                let mut record = entry.record;
                if record.is_none()
                    && let Some(prior_path) = capture_step(&mut receipt.work, meter, |meter| remap_renamed_directory_path(
                        &path, &renamed_directories, checkout.volume_config().limits, meter,
                    ))?
                {
                    let prior = capture_sdk_in_heap_with_meter(
                        |native| checkout.lookup_no_follow(&prior_path, native, &cancellation),
                        meter, &cancellation,
                    ).await?;
                    receipt.work = prior.work;
                    record = prior.value.record;
                    capture_step(&mut receipt.work, meter, |meter| capture_drop_path(prior_path, meter))?;
                }
                let Ok(index) = capture_step(&mut receipt.work, meter, |meter| ordinary.position_measured(&path, meter))? else {
                    return Err(OperationFailure::new(
                        CaptureError::Invariant("capture lookup path disappeared"),
                        receipt.work,
                    ));
                };
                let (current, _) = &mut ordinary.entries[index].1;
                *current = CurrentRecord::Known(record);
                capture_step(&mut receipt.work, meter, |meter| capture_drop_path(path, meter))?;
            }
            drop(lookup.1);
            capture_step(&mut receipt.work, meter, |meter| meter.release(lookup_paths_bytes).map_err(capture_demand_error))?;
        } else {
            capture_step(&mut receipt.work, meter, |meter| release_capture_paths(lookup_paths, meter))?;
        }
        capture_step(&mut receipt.work, meter, |meter| {
            let storage_bytes = renamed_directories.capacity() as u64 * std::mem::size_of::<(NamespacePath, NamespacePath)>() as u64;
            for (from, to) in renamed_directories {
                capture_drop_path(from, meter)?;
                capture_drop_path(to, meter)?;
            }
            meter.release(storage_bytes).map_err(capture_demand_error)
        })?;
        
        let mut host_links = CaptureMap::new();
        let mut prepared = capture_step(&mut receipt.work, meter, |meter| capture_vec(
            rename_records.len().checked_add(ordinary.len()).ok_or(CaptureError::Work(WorkError::Overflow))?, meter))?;
        // A rename can target a directory that has never been observed in this
        // lazy checkout. Materialize only those host ancestors before the rename,
        // preserving the event order for all remaining mutations.
        capture_step(&mut receipt.work, meter, |meter| capture_sort_by(&mut missing_ancestors, |left, right| {
            left.depth()
                .cmp(&right.depth())
                .then_with(|| left.cmp(right))
        }, meter))?;
        let rename_mutations = std::mem::take(&mut mutations);
        let missing_bytes = missing_ancestors.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
        for path in missing_ancestors {
            let Some((key, (current, intent))) = capture_step(&mut receipt.work, meter, |meter| ordinary.remove_measured(&path, meter))? else {
                return Err(OperationFailure::new(
                    CaptureError::Invariant("capture ancestor path disappeared"),
                    receipt.work,
                ));
            };
            capture_step(&mut receipt.work, meter, |meter| capture_drop_path(key, meter))?;
            let plan = prepare_final_path(checkout,
            path,
            current,
            intent,
            &source_root,
            None,
            &mut host_links,
            Some(epoch.get()),
            None,
            &mut mutations,
            &mut receipt,
            budget,
            &cancellation, meter)
            .await?;
            if let Some(plan) = plan {
                prepared.push(plan);
            }
        }
        capture_step(&mut receipt.work, meter, |meter| meter.release(missing_bytes).map_err(capture_demand_error))?;
        let rename_mutations_bytes = rename_mutations.capacity() as u64 * std::mem::size_of::<AuthoredMutation>() as u64;
        capture_step(&mut receipt.work, meter, |meter| capture_reserve(&mut mutations, rename_mutations.len(), meter))?;
        capture_step(&mut receipt.work, meter, |meter| capture_charge(meter, WorkCounters {
            bytes_copied: rename_mutations.len() as u64 * std::mem::size_of::<AuthoredMutation>() as u64,
            ..WorkCounters::UNCHARGED
        }))?;
        mutations.extend(rename_mutations);
        capture_step(&mut receipt.work, meter, |meter| meter.release(rename_mutations_bytes).map_err(capture_demand_error))?;
        let namespace_boundary = mutations.len();
        let rename_records_bytes = rename_records.storage_bytes();
        for (path, record) in rename_records {
            if capture_step(&mut receipt.work, meter, |meter| ordinary.contains_key_measured(&path, meter))? {
                capture_step(&mut receipt.work, meter, |meter| capture_drop_path(path, meter))?;
                continue;
            }
            let plan = prepare_final_path(checkout,
            path,
            CurrentRecord::Known(Some(record)),
            CaptureIntent::Complete,
            &source_root,
            None,
            &mut host_links,
            Some(epoch.get()),
            None,
            &mut mutations,
            &mut receipt,
            budget,
            &cancellation, meter)
            .await?;
            if let Some(plan) = plan {
                prepared.push(plan);
            }
        }
        capture_step(&mut receipt.work, meter, |meter| meter.release(rename_records_bytes).map_err(capture_demand_error))?;
        let mut observed_ordinary = capture_step(&mut receipt.work, meter, |meter| capture_vec(ordinary.len(), meter))?;
        let ordinary_bytes = ordinary.storage_bytes();
        for (path, (current, intent)) in ordinary {
            let mut host_work = CaptureNativeHostWork::new(meter);
            let host_path = namespace_to_host_path_measured(&path, &mut host_work)
                .map_err(|error| OperationFailure::new(error, host_work.completed()))?;
            let observation = observe_host_path(&source_root, &host_path, &mut host_work)
                .map_err(|error| OperationFailure::new(error, host_work.completed()))?;
            capture_drop_host_path(host_path, &mut host_work)
                .map_err(|error| OperationFailure::new(error, host_work.completed()))?;
            receipt.work = host_work.completed();
            observed_ordinary.push((path, current, intent, observation));
        }
        capture_step(&mut receipt.work, meter, |meter| meter.release(ordinary_bytes).map_err(capture_demand_error))?;
        let mut ordinary = observed_ordinary;
        capture_step(&mut receipt.work, meter, |meter| capture_sort_by(&mut ordinary, |left, right| match (left.3.is_some(), right.3.is_some()) {
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
        }, meter))?;
        let observed_bytes = ordinary.capacity() as u64 * std::mem::size_of::<(NamespacePath, CurrentRecord, CaptureIntent, Option<HostObservation>)>() as u64;
        for (path, current, intent, observation) in ordinary {
            let plan = prepare_final_path(checkout,
            path,
            current,
            intent,
            &source_root,
            observation,
            &mut host_links,
            Some(epoch.get()),
            None,
            &mut mutations,
            &mut receipt,
            budget,
            &cancellation, meter)
            .await?;
            if let Some(plan) = plan {
                prepared.push(plan);
            }
        }
        capture_step(&mut receipt.work, meter, |meter| meter.release(observed_bytes).map_err(capture_demand_error))?;
        finish_prepared_paths(checkout,
        &source_root,
        prepared,
        options.maximum_extent_spans,
        &mut mutations,
        &mut receipt,
        budget,
        &cancellation, meter)
        .await?;
        capture_step(&mut receipt.work, meter, |meter| release_host_links(host_links, meter))?;
        if namespace_boundary == 0 || namespace_boundary == mutations.len() {
            apply_capture_transaction(checkout, mutations, &mut receipt, budget, &cancellation, meter)
                .await?;
        } else {
            // Keep the watch batch atomic to callers, but compile the path-shape
            // changes before mutations that address their resulting bindings.
            // The authored compiler resolves those bindings against its input
            // checkout, even though the kernel executes each batch in order.
            let mut candidate = capture_candidate(checkout, &mut receipt.work, &cancellation, &mut capture_meter(meter))?;
            let final_state = capture_step(&mut receipt.work, meter, |meter| {
                let count = mutations.len() - namespace_boundary;
                let bytes = count.checked_mul(std::mem::size_of::<AuthoredMutation>())
                    .ok_or(CaptureError::Work(WorkError::Overflow))? as u64;
                let copied = WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED };
                meter.admit(copied).map_err(capture_demand_error)?;
                let mut final_state = capture_vec(count, meter)?;
                final_state.extend(mutations.drain(namespace_boundary..));
                meter.record(copied).map_err(capture_demand_error)?;
                Ok(final_state)
            })?;
            apply_capture_transaction(&mut candidate,
            mutations,
            &mut receipt,
            budget,
            &cancellation, meter)
            .await?;
            apply_capture_transaction(&mut candidate,
            final_state,
            &mut receipt,
            budget,
            &cancellation, meter)
            .await?;
            capture_step(&mut receipt.work, meter, |meter| {
                cancellation.check().map_err(|_| CaptureError::Cancelled)?;
                meter.check().map_err(capture_demand_error)
            })?;
            cancellation.commit_if_live(|| {
                *checkout = candidate;
                Ok::<(), OperationFailure<CaptureError>>(())
            }).map_err(|_| OperationFailure::new(CaptureError::Cancelled, capture_meter(meter).completed()))??;
        }
        Ok(OperationReceipt {
            value: WatchCaptureReceipt {
                epoch,
                first_sequence,
                next_sequence,
                capture: receipt,
            },
            work: receipt.work,
        })
    }, meter.original())).await;
    let completed = capture_meter(meter).completed();
    let result = result.map(|mut receipt| {
        receipt.work = completed;
        receipt.value.capture.work = receipt.work;
        receipt
    }).map_err(|mut failure| {
        *failure.work = completed;
        failure
    });
    crate::obs::measured_on(&span, result, |receipt| &receipt.work)
}

#[cfg(test)]
mod shared_watch_capture_tests {
    use super::*;
    use crate::storage::{
        ObjectId, ObjectRead, ObjectReadRequest, ObjectResult, ObjectWrite,
    };
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct ReenteringObjects {
        inner: crate::memory::MemoryObjectStore,
        meter: Arc<Mutex<Option<Arc<NativeDemandReadMeter>>>>,
        calls: Arc<AtomicUsize>,
    }

    impl ReenteringObjects {
        async fn reenter(&self) {
            for _ in 0..2 {
                {
                    let binding = self.meter.lock().expect("fixture meter binding");
                    if let Some(meter) = binding.as_ref() {
                        let original = meter.0.try_lock()
                            .expect("watch capture must release its original meter before backend entry");
                        original.check().expect("fixture invocation remains active");
                        self.calls.fetch_add(1, Ordering::Relaxed);
                    }
                }
                tokio::task::yield_now().await;
            }
        }
    }

    impl AsyncObjectStore for ReenteringObjects {
        async fn put(
            &self, object_id: ObjectId, bytes: acyclic_stream::OwnedBytes, budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> ObjectResult<()> {
            self.reenter().await;
            AsyncObjectStore::put(&self.inner, object_id, bytes, budget, cancellation).await
        }

        async fn put_many(
            &self, writes: &[ObjectWrite], budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> ObjectResult<()> {
            self.reenter().await;
            AsyncObjectStore::put_many(&self.inner, writes, budget, cancellation).await
        }

        async fn read(
            &self, object_id: ObjectId, maximum_bytes: u64, budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> ObjectResult<ObjectRead> {
            self.reenter().await;
            AsyncObjectStore::read(&self.inner, object_id, maximum_bytes, budget, cancellation).await
        }

        async fn read_many(
            &self, requests: &[ObjectReadRequest], budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> ObjectResult<Vec<ObjectRead>> {
            self.reenter().await;
            AsyncObjectStore::read_many(&self.inner, requests, budget, cancellation).await
        }

        async fn contains(
            &self, object_id: ObjectId, budget: WorkBudget,
            cancellation: &CancellationToken,
        ) -> ObjectResult<bool> {
            self.reenter().await;
            AsyncObjectStore::contains(&self.inner, object_id, budget, cancellation).await
        }
    }

    #[tokio::test]
    async fn shared_watch_reenters_original_meter_and_preserves_failure_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("file"), vec![b'x'; 256 * 1024])?;
        let binding = Arc::new(Mutex::new(None));
        let calls = Arc::new(AtomicUsize::new(0));
        let fs = crate::Fs::new(
            crate::memory::MemoryAuthorityStore::default(),
            ReenteringObjects {
                inner: crate::memory::MemoryObjectStore::default(),
                meter: Arc::clone(&binding),
                calls: Arc::clone(&calls),
            },
            crate::EmbeddedCapabilities::MEMORY,
        );
        let workspace = fs.create_workspace("shared-watch-reentry", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        workspace.write_text("/file", "prior watch value").await?;
        let mut checkout = workspace.checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let cancellation = CancellationToken::new();
        let budget = WorkBudget::UNBOUNDED;
        let native = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let original = native.original_demand_work();
        original.with_original_leaf(|work| work.charge(WorkCounters {
            items_examined: 7, ..WorkCounters::UNCHARGED
        }))??;
        let retained = Vec::from(b"original caller allocation".as_slice());
        original.with_original_leaf(|work| work.adopt(retained.capacity() as u64))??;
        let meter = Arc::clone(original.meter());
        *binding.lock().expect("fixture binding") = Some(Arc::clone(&meter));
        let limits = checkout.volume_config().limits;
        let path = NamespacePath::from_portable(
            &crate::path::PortablePath::parse("/file", limits)?, limits,
        )?;
        let options = CaptureOptions {
            source_root: directory.path().to_path_buf(),
            expected_root_identity: capture_root_identity(directory.path())?,
            maximum_paths: 8,
            maximum_extent_spans: 8,
        };
        let captured = capture_watch_batch_with_native_work(
            &mut checkout,
            WatchBatch::Changes {
                original: Default::default(),
                epoch: WatchEpoch::from_u64(9),
                first_sequence: WatchSequence::from_u64(3),
                next_sequence: WatchSequence::from_u64(4),
                changes: vec![WatchChange::Created(path)],
            },
            &options, &CapturePolicy::allow_all(), &cancellation, &native,
        ).await?;
        assert!(calls.load(Ordering::Relaxed) > 0);
        assert_eq!(captured.value.epoch, WatchEpoch::from_u64(9));
        assert_eq!(captured.value.next_sequence, WatchSequence::from_u64(4));
        assert_eq!(captured.work, original.counters()?);
        assert_eq!(captured.value.capture.work, captured.work);
        assert!(captured.work.items_examined > 7);
        let live_before_failure = meter.lock().live_allocation_bytes();
        let failed_path = NamespacePath::from_portable(
            &crate::path::PortablePath::parse("/file", limits)?, limits,
        )?;
        let failure = capture_watch_batch_with_native_work(
            &mut checkout,
            WatchBatch::Changes {
                original: Default::default(),
                epoch: WatchEpoch::from_u64(9),
                first_sequence: WatchSequence::from_u64(4),
                next_sequence: WatchSequence::from_u64(4),
                changes: vec![WatchChange::Modified(failed_path)],
            },
            &CaptureOptions { maximum_paths: 0, ..options },
            &CapturePolicy::allow_all(), &cancellation, &native,
        ).await.expect_err("invalid options retain original invocation work");
        assert!(matches!(failure.error, CaptureError::InvalidOptions));
        let (completed, live) = {
            let work = meter.lock();
            (work.work, work.live_allocation_bytes())
        };
        assert_eq!(*failure.work, completed);
        assert!(failure.work.items_examined >= captured.work.items_examined);
        assert_eq!(original.full_budget()?, budget);
        assert_eq!(live, live_before_failure);
        assert!(live >= retained.capacity() as u64);
        let retained_bytes = retained.capacity() as u64;
        drop(retained);
        original.with_original_leaf(|work| work.release(retained_bytes))??;
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum CurrentRecord {
    Lookup,
    Known(Option<FileRecord>),
}

/// What a hinted path held before this batch: a renamed record, nothing if
/// it was moved away, or the checkout's current binding.
fn current_record(
    rename_records: &CaptureMap<NamespacePath, FileRecord>,
    moved_away: &CaptureSet<NamespacePath>,
    path: &NamespacePath,
    meter: &mut impl CaptureNativeWork,
) -> Result<CurrentRecord, CaptureError> {
    Ok(match rename_records.get_measured(path, meter)? {
        Some(record) => CurrentRecord::Known(Some(*record)),
        None if moved_away.contains_measured(path, meter)? => CurrentRecord::Known(None),
        None => CurrentRecord::Lookup,
    })
}

fn watch_ancestor_candidates<'a>(
    paths: impl Iterator<Item = &'a NamespacePath>,
    ordinary: &CaptureMap<NamespacePath, (CurrentRecord, CaptureIntent)>,
    rename_records: &CaptureMap<NamespacePath, FileRecord>,
    policy: &CapturePolicy,
    maximum_paths: usize,
    work: &mut WorkCounters,
    cancellation: &CancellationToken,
    limits: crate::model::VolumeLimits,
    meter: &mut impl CaptureNativeWork,
) -> Result<Vec<NamespacePath>, OperationFailure<CaptureError>> {
    let _ = cancellation;
    let mut ancestors = CaptureSet::new();
    for path in paths {
        for depth in (1..path.depth()).rev() {
            let parent = capture_step(work, meter, |meter| clone_capture_prefix(path, depth, limits, meter))?;
            if !capture_step(work, meter, |meter| ordinary.contains_key_measured(&parent, meter))?
                && !capture_step(work, meter, |meter| rename_records.contains_key_measured(&parent, meter))?
                && !capture_step(work, meter, |meter| capture_excludes(policy, &parent, meter))?
                && !capture_step(work, meter, |meter| ancestors.contains_measured(&parent, meter))? {
                if combined_path_count_exceeds(ancestors.len() + ordinary.len(), rename_records.len() + 1, maximum_paths) {
                    return Err(OperationFailure::new(CaptureError::InvalidOptions, *work));
                }
                capture_step(work, meter, |meter| ancestors.insert_measured(parent, meter))?;
            } else {
                capture_step(work, meter, |meter| capture_drop_path(parent, meter))?;
            }
        }
    }
    let storage_bytes = ancestors.storage_bytes();
    let mut ordered = capture_step(work, meter, |meter| capture_vec(ancestors.len(), meter))?;
    ordered.extend(ancestors);
    capture_step(work, meter, |meter| meter.release(storage_bytes).map_err(capture_demand_error))?;
    let mut ancestors = ordered;
    capture_step(work, meter, |meter| capture_sort_by(&mut ancestors, |left, right| {
        left.depth()
            .cmp(&right.depth())
            .then_with(|| left.cmp(right))
    }, meter))?;
    Ok(ancestors)
}

struct WatchAncestorScope<'a> {
    source_root: &'a HostRoot,
    epoch: WatchEpoch,
    maximum_paths: usize,
    budget: WorkBudget,
    cancellation: &'a CancellationToken,
}

fn release_capture_set(paths: CaptureSet<NamespacePath>, meter: &mut impl CaptureNativeWork) -> Result<(), CaptureError> {
    let bytes = paths.storage_bytes();
    for path in paths {
        capture_drop_path(path, meter)?;
    }
    meter.release(bytes).map_err(capture_demand_error)
}

fn release_host_links(links: CaptureMap<[u8; 16], HostLinkSource>, meter: &mut impl CaptureNativeWork) -> Result<(), CaptureError> {
    let bytes = links.storage_bytes();
    for (_, source) in links {
        let path = match source { HostLinkSource::Pending(path) | HostLinkSource::Ready(path) => path };
        capture_drop_path(path, meter)?;
    }
    meter.release(bytes).map_err(capture_demand_error)
}

fn capture_within_any(path: &NamespacePath, ancestors: &[NamespacePath], exclude_self: bool, meter: &mut impl CaptureNativeWork) -> Result<bool, CaptureError> {
    for ancestor in ancestors {
        capture_charge(meter, WorkCounters { items_examined: 2,
            source_path_components: path.depth().min(ancestor.depth()) as u64, ..WorkCounters::UNCHARGED })?;
        if (!exclude_self || path != ancestor) && path.is_within(ancestor) { return Ok(true); }
    }
    Ok(false)
}

async fn hydrate_watch_ancestors<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    ordinary: &mut CaptureMap<NamespacePath, (CurrentRecord, CaptureIntent)>,
    ancestors: Vec<NamespacePath>,
    scope: WatchAncestorScope<'_>,
    receipt: &mut CaptureReceipt,
    meter: &CaptureWorkScope,
) -> Result<Vec<NamespacePath>, OperationFailure<CaptureError>> {
    let result = capture_in_heap_with_meter(|| async {
    let mut missing = Vec::new();
    let mut replaced = Vec::new();
    let ancestors_bytes = ancestors.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    for path in ancestors {
        let below_replaced = capture_step(&mut receipt.work, &mut capture_meter(meter),
            |meter| capture_within_any(&path, &replaced, false, meter))?;
        let current = if below_replaced {
            None
        } else {
            let lookup = capture_sdk_in_heap_with_meter(
                |native| checkout.lookup_no_follow(&path, native, scope.cancellation),
                meter, scope.cancellation,
            ).await?;
            receipt.work = lookup.work;
            lookup.value.record
        };
        let mut host_work = CaptureNativeHostWork::new(meter);
        let host_path = namespace_to_host_path_measured(&path, &mut host_work)
            .map_err(|error| OperationFailure::new(error, host_work.completed()))?;
        let observation = observe_host_path(scope.source_root, &host_path, &mut host_work)
            .map_err(|error| OperationFailure::new(error, host_work.completed()))?;
        capture_drop_host_path(host_path, &mut host_work)
            .map_err(|error| OperationFailure::new(error, host_work.completed()))?;
        receipt.work = host_work.completed();
        match observation {
            Some(observation) if observation.metadata.is_dir() => {
                if current.is_none_or(|record| record.kind != FileKind::Directory) {
                    if current.is_some() {
                        capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| {
                            capture_reserve(&mut replaced, 1, meter)?;
                            replaced.push(clone_capture_path(&path, meter)?);
                            Ok(())
                        })?;
                    }
                    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| {
                        capture_reserve(&mut missing, 1, meter)?;
                        missing.push(clone_capture_path(&path, meter)?);
                        Ok(())
                    })?;
                    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| ordinary.insert_measured(
                        path, (CurrentRecord::Known(current), CaptureIntent::Replace), meter))?;
                } else {
                    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_drop_path(path, meter))?;
                }
            }
            _ => {
                return Err(OperationFailure::new(
                    CaptureError::RescanRequired {
                        epoch: scope.epoch.get(),
                        reason: WatchInvalidationReason::NativeRescanRequired,
                    },
                    receipt.work,
                ));
            }
        }
    }
    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| meter.release(ancestors_bytes).map_err(capture_demand_error))?;
    if ordinary.len() > scope.maximum_paths {
        return Err(OperationFailure::new(
            CaptureError::InvalidOptions,
            receipt.work,
        ));
    }

    // Descendants of a replaced file or symlink were absent in the old view.
    for (path, (current, _)) in ordinary {
        if matches!(current, CurrentRecord::Lookup)
            && capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_within_any(path, &replaced, true, meter))?
        {
            *current = CurrentRecord::Known(None);
        }
    }
    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| release_capture_paths(replaced, meter))?;
    Ok(OperationReceipt { value: missing, work: capture_meter(meter).completed() })
    }, meter.original()).await;
    receipt.work = capture_meter(meter).completed();
    result.map(|completed| completed.value).map_err(|mut failure| {
        *failure.work = receipt.work;
        failure
    })
}

fn remap_renamed_directory_path(
    path: &NamespacePath,
    renames: &[(NamespacePath, NamespacePath)],
    limits: crate::model::VolumeLimits,
    meter: &mut impl CaptureNativeWork,
) -> Result<Option<NamespacePath>, CaptureError> {
    let mut prior = clone_capture_path(path, meter)?;
    for (from, to) in renames.iter().rev() {
        capture_charge(meter, WorkCounters { items_examined: 1, source_path_components: prior.depth() as u64, ..WorkCounters::UNCHARGED })?;
        if prior == *to || !prior.is_within(to) {
            continue;
        }
        let suffix = prior
            .components()
            .get(to.depth()..)
            .ok_or(CaptureError::InvalidOptions)?;
        let mut components = capture_vec(from.depth() + suffix.len(), meter)?;
        for name in from.components().iter().chain(suffix) {
            components.push(clone_capture_name(name, meter)?);
        }
        let replacement = NamespacePath::new(components, limits).map_err(CaptureError::Path)?;
        let previous = std::mem::replace(&mut prior, replacement);
        capture_drop_path(previous, meter)?;
    }
    if prior == *path {
        capture_drop_path(prior, meter)?;
        Ok(None)
    } else {
        Ok(Some(prior))
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CaptureIntent {
    MetadataOnly,
    Complete,
    /// As `Complete`, for a name a rename bound: a directory's subtree is new.
    Arrived,
    Replace,
}

struct PreparedRegular {
    path: NamespacePath,
    host_path: CaptureHostPath,
    snapshot: HostSnapshot,
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

#[allow(
    clippy::too_many_arguments,
    reason = "expands hints into the shared ordinary map and receipt under the capture's bound, policy, budget, and cancellation"
)]
async fn expand_directory_hints<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    ordinary: &mut CaptureMap<NamespacePath, (CurrentRecord, CaptureIntent)>,
    source_root: &HostRoot,
    maximum_paths: u32,
    policy: &CapturePolicy,
    receipt: &mut CaptureReceipt,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(), OperationFailure<CaptureError>> {
    let result = capture_in_heap_with_meter(|| async {
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
    let mut roots = capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_vec(ordinary.len(), meter))?;
    for (path, (_, intent)) in ordinary.iter() {
        // Only a directory that appeared at its path (created, or renamed
        // in) holds entries no event names. A directory modified in place
        // is covered by its entries' own reports, and expanding it would
        // capture every entry beneath it, most of them unchanged and
        // unobserved: a lazy checkout would take each as new. FSEvents
        // reports appearance only as modification, so there every hint
        // expands.
        if !cfg!(target_os = "macos")
            && !matches!(intent, CaptureIntent::Replace | CaptureIntent::Arrived)
        {
            continue;
        }
        let mut host_work = CaptureNativeHostWork::new(meter);
        let host_path = namespace_to_host_path_measured(path, &mut host_work)
            .map_err(|error| OperationFailure::new(error, host_work.completed()))?;
        let observation = observe_host_path(source_root, &host_path, &mut host_work)
            .map_err(|error| OperationFailure::new(error, host_work.completed()))?;
        receipt.work = host_work.completed();
        if observation.is_some_and(|observation| observation.metadata.is_dir()) {
            capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| {
                roots.push((host_path, clone_capture_path(path, meter)?));
                Ok(())
            })?;
        } else {
            capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_drop_host_path(host_path, meter))?;
        }
    }
    let mut paths = CaptureSet::new();
    for path in ordinary.keys() {
        capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| {
            let path = clone_capture_path(path, meter)?;
            paths.insert_measured(path, meter)
        })?;
    }
    let roots_bytes = roots.capacity() as u64 * std::mem::size_of::<(CaptureHostPath, NamespacePath)>() as u64;
    for (host_path, volume_path) in roots {
        let host_volume_path = capture_path_clone(&volume_path, &mut receipt.work, &mut capture_meter(meter))?;
        collect_host_subtree_paths(source_root,
            profile,
            limits,
            host_path,
            host_volume_path,
            policy,
            maximum,
            &mut paths,
            &mut receipt.work,
            budget,
            cancellation, &mut CaptureNativeHostWork::new(meter))?;
        let lookup = capture_sdk_in_heap_with_meter(
            |native| checkout.lookup_no_follow(&volume_path, native, cancellation),
            meter, cancellation,
        ).await?;
        receipt.work = lookup.work;
        if lookup
            .value
            .record
            .is_some_and(|record| record.kind == FileKind::Directory)
        {
            collect_checkout_subtree_paths(checkout,
            limits,
            volume_path,
            policy,
            maximum,
            &mut paths,
            &mut receipt.work,
            budget,
            cancellation, meter)
            .await?;
        } else {
            capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_drop_path(volume_path, meter))?;
        }
    }
    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| meter.release(roots_bytes).map_err(capture_demand_error))?;
    let paths_bytes = paths.storage_bytes();
    for path in paths {
        capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| ordinary.get_or_insert_measured(
            path, (CurrentRecord::Lookup, CaptureIntent::Complete), meter).map(|_| ()))?;
    }
    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| meter.release(paths_bytes).map_err(capture_demand_error))?;
    Ok(OperationReceipt { value: (), work: capture_meter(meter).completed() })
    }, meter.original()).await;
    receipt.work = capture_meter(meter).completed();
    result.map(|completed| completed.value).map_err(|mut failure| {
        *failure.work = receipt.work;
        failure
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "one path's record, intent, observation, link map, epoch, and baseline all decide its final state"
)]
#[allow(
    clippy::too_many_lines,
    reason = "classifies one path against every host and record kind in a single decision"
)]
async fn prepare_final_path<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    path: NamespacePath,
    current: CurrentRecord,
    intent: CaptureIntent,
    source_root: &HostRoot,
    observation: Option<HostObservation>,
    host_links: &mut CaptureMap<[u8; 16], HostLinkSource>,
    watch_epoch: Option<u64>,
    baseline: Option<&NativeViewBaseline>,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<Option<PreparedPath>, OperationFailure<CaptureError>> {
    cancellation.check().map_err(|_| {
        OperationFailure::new(CaptureError::Cancelled, receipt.work)
    })?;
    let host_path = capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| namespace_to_host_path_measured(&path, meter))?;
    let current = match current {
        CurrentRecord::Known(record) => record,
        CurrentRecord::Lookup => {
            let lookup_cancellation = capture_meter(meter).leaf(|work| work.cancellation().clone());
            let lookup = capture_sdk_in_heap_with_meter(
                |native| checkout.lookup_no_follow(&path, native, &lookup_cancellation),
                meter, &lookup_cancellation,
            ).await?;
            receipt.work = lookup.work;
            lookup.value.record
        }
    };
    let observation = match observation {
        Some(observation) => Some(observation),
        None => {
            let observed = observe_host_path(
                source_root, &host_path, &mut CaptureNativeHostWork::new(meter),
            );
            receipt.work = capture_meter(meter).completed();
            observed.map_err(|error| OperationFailure::new(error, receipt.work))?
        }
    };
    match observation {
        Some(observation) => {
            let metadata = observation.metadata;
            let host_kind = host_kind(&metadata).map_err(|error| OperationFailure::new(error, receipt.work))?;
            let snapshot = HostSnapshot::from_metadata(&metadata)
                .map_err(|error| OperationFailure::new(error, receipt.work))?;
            let linked_regular = host_kind == FileKind::Regular
                && checkout.volume_config().hard_links
                && host_link_count(&metadata)
                    .map_err(|error| OperationFailure::new(error, receipt.work))?
                    > 1;
            if linked_regular {
                let identity = snapshot.identity.to_bytes();
                let source = capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| host_links.get_measured(&identity, meter))?;
                if let Some(source) = source {
                    ensure_current_host_node(source_root, &host_path, &snapshot, &mut receipt.work, budget, cancellation, &mut CaptureNativeHostWork::new(meter))?;
                    receipt.changed_paths = checked_increment(receipt.changed_paths, receipt.work)?;
                    receipt.examined_paths =
                        checked_increment(receipt.examined_paths, receipt.work)?;
                    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_drop_host_path(host_path, meter))?;
                    return Ok(match source {
                        HostLinkSource::Pending(source) => Some(PreparedPath::PendingHardLink {
                            source: capture_path_clone(source, &mut receipt.work, &mut capture_meter(meter))?,
                            destination: path,
                            current,
                        }),
                        HostLinkSource::Ready(source) => {
                            let source = capture_path_clone(source, &mut receipt.work, &mut capture_meter(meter))?;
                            append_hard_link(source, path, current, mutations, &mut receipt.work, &mut capture_meter(meter))?;
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
                if record.kind == FileKind::Directory {
                    push_subtree_removals(checkout,
                    &path,
                    mutations,
                    receipt,
                    budget,
                    cancellation, meter)
                    .await?;
                }
                capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_reserve(mutations, 1, meter))?;
                mutations.push(AuthoredMutation::Remove {
                    path: capture_path_clone(&path, &mut receipt.work, &mut capture_meter(meter))?,
                    expected_file_id: Some(record.file_id),
                });
            }
            let exists_with_kind =
                !replace && current.is_some_and(|record| record.kind == host_kind);
            let mut canonical_metadata = if host_kind == FileKind::SymbolicLink && cfg!(unix) {
                unrestorable_metadata()
            } else {
                snapshot.metadata
            };
            let mut prior_metadata = None;
            if let Some(record) = current.filter(|record| {
                exists_with_kind
                    && record.kind == host_kind
                    && (host_kind != FileKind::SymbolicLink || cfg!(windows))
            }) {
                let lookup_cancellation = capture_meter(meter).leaf(|work| work.cancellation().clone());
                let prior = capture_sdk_in_heap_with_meter(
                    |native| checkout.read_metadata_by_id(record.file_id, native, &lookup_cancellation),
                    meter, &lookup_cancellation,
                ).await?;
                receipt.work = prior.work;
                prior_metadata = Some(prior.value);
                canonical_metadata = preserve_unobserved_metadata(canonical_metadata, prior.value);
                if let Some(baseline) = baseline {
                    baseline.restore_canonical_stamps(
                        source_root.identity(),
                        prior.value,
                        &mut canonical_metadata,
                    );
                }
            }
            // A directory's timestamps follow what happens to its entries,
            // each captured through its own path, and to reads of it: a
            // change to them alone is no change of the directory's.
            if host_kind == FileKind::Directory
                && prior_metadata.is_some_and(|prior| {
                    prior.without_timestamps() == canonical_metadata.without_timestamps()
                })
            {
                receipt.examined_paths = checked_increment(receipt.examined_paths, receipt.work)?;
                capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| {
                    capture_drop_path(path, meter)?;
                    capture_drop_host_path(host_path, meter)
                })?;
                return Ok(None);
            }
            let linked_path = if linked_regular {
                Some(capture_path_clone(&path, &mut receipt.work, &mut capture_meter(meter))?)
            } else { None };
            let prepared = if intent == CaptureIntent::MetadataOnly && exists_with_kind {
                ensure_current_host_node(source_root, &host_path, &snapshot, &mut receipt.work, budget, cancellation, &mut CaptureNativeHostWork::new(meter))?;
                capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_reserve(mutations, 1, meter))?;
                mutations.push(AuthoredMutation::SetMetadata {
                    path,
                    metadata: canonical_metadata,
                });
                capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_drop_host_path(host_path, meter))?;
                None
            } else if host_kind == FileKind::Regular {
                // Staging opens the file once and proves it is still the
                // observed node before reading a byte.
                capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_allocate(meter, std::mem::size_of::<PreparedRegular>() as u64))?;
                Some(PreparedPath::Regular(Box::new(PreparedRegular {
                    path,
                    host_path,
                    snapshot,
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
                    &mut receipt.work,
                    &mut CaptureNativeHostWork::new(meter),
                )?;
                capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_drop_host_path(host_path, meter))?;
                None
            };
            receipt.changed_paths = checked_increment(receipt.changed_paths, receipt.work)?;
            if let Some(linked_path) = linked_path {
                let source = if prepared.is_some() {
                    HostLinkSource::Pending(linked_path)
                } else {
                    HostLinkSource::Ready(linked_path)
                };
                capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| host_links.insert_measured(
                    snapshot.identity.to_bytes(), source, meter))?;
            }
            receipt.examined_paths = checked_increment(receipt.examined_paths, receipt.work)?;
            return Ok(prepared);
        }
        None => {
            if let Some(record) = current {
                // A directory that vanished takes its descendants with it.
                // A watcher may say so with one hint on the directory alone
                // (inotify on a directory moved out of the root, a rename
                // half on any platform), and a namespace remove of a
                // non-empty directory is refused, so remove the subtree
                // the checkout still holds, deepest first.
                if record.kind == FileKind::Directory {
                    push_subtree_removals(checkout,
                    &path,
                    mutations,
                    receipt,
                    budget,
                    cancellation, meter)
                    .await?;
                }
                capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_reserve(mutations, 1, meter))?;
                mutations.push(AuthoredMutation::Remove {
                    path,
                    expected_file_id: Some(record.file_id),
                });
                receipt.changed_paths = checked_increment(receipt.changed_paths, receipt.work)?;
            } else {
                capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_drop_path(path, meter))?;
            }
        }
    }
    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_drop_host_path(host_path, meter))?;
    receipt.examined_paths = checked_increment(receipt.examined_paths, receipt.work)?;
    Ok(None)
}

fn host_path_is_absent(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
    ) || cfg!(windows) && error.raw_os_error() == Some(267)
}

/// Queues a remove for every descendant of `root` that the checkout holds,
/// deepest first, so that a following remove of `root` itself finds it
/// empty. The host has already lost the subtree; only the checkout's view
/// is walked.
async fn push_subtree_removals<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    root: &NamespacePath,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    _budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(), OperationFailure<CaptureError>> {
    const PAGE: u32 = 256;
    let limits = checkout.volume_config().limits;
    // Descendants that carried their own hint are already queued (absent
    // paths are captured deepest first); a second remove of the same path
    // would find its source missing.
    let mut queued = CaptureSet::new();
    for mutation in mutations.iter() {
        capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_charge(meter,
            WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
        if let AuthoredMutation::Remove { path, .. } = mutation
            && !capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| queued.contains_measured(path, meter))? {
            let path = capture_path_clone(path, &mut receipt.work, &mut capture_meter(meter))?;
            capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| queued.insert_measured(path, meter))?;
        }
    }
    let mut pending = capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_vec(1, meter))?;
    pending.push(capture_path_clone(root, &mut receipt.work, &mut capture_meter(meter))?);
    let mut found: Vec<(NamespacePath, FileRecord)> = Vec::new();
    while let Some(directory) = pending.pop() {
        let mut after: Option<LogicalName> = None;
        loop {
            let page_cancellation = capture_meter(meter).leaf(|work| work.cancellation().clone());
            let page = capture_sdk_in_heap_with_meter(
                |native| checkout.list_directory_records_with_native_work(&directory, after.as_ref(), PAGE, native, &page_cancellation),
                meter, &page_cancellation,
            ).await?;
            receipt.work = page.work;
            let page = page.value;
            capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_charge(meter,
                WorkCounters { items_examined: page.0.entries.len() as u64, ..WorkCounters::UNCHARGED }))?;
            if page.0.has_more && page.0.entries.is_empty() {
                return Err(OperationFailure::new(CaptureError::Invariant("directory pagination made no progress"), receipt.work));
            }
            for entry in &page.0.entries {
                let child = capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| {
                    let name = clone_capture_name(&entry.name, meter)?;
                    append_path_measured(&directory, name, limits, meter)
                })?;
                if entry.record.kind == FileKind::Directory {
                    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_reserve(&mut pending, 1, meter))?;
                    pending.push(capture_path_clone(&child, &mut receipt.work, &mut capture_meter(meter))?);
                }
                if !capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| queued.contains_measured(&child, meter))? {
                    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_reserve(&mut found, 1, meter))?;
                    found.push((child, entry.record));
                } else {
                    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_drop_path(child, meter))?;
                }
            }
            let next = if page.0.has_more {
                let last = page.0.entries.last().ok_or_else(|| OperationFailure::new(
                    CaptureError::Invariant("directory pagination made no progress"), receipt.work))?;
                Some(capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| clone_capture_name(&last.name, meter))?)
            } else { None };
            if let Some(previous) = after.take() {
                let bytes = previous.as_bytes().len() as u64;
                drop(previous);
                capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| meter.release(bytes).map_err(capture_demand_error))?;
            }
            let has_more = page.0.has_more;
            drop(page);
            after = next;
            if !has_more { break; }
        }
        capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_drop_path(directory, meter))?;
    }
    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| {
        let pending_bytes = pending.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
        drop(pending);
        meter.release(pending_bytes).map_err(capture_demand_error)?;
        let queued_bytes = queued.storage_bytes();
        for path in queued {
            capture_drop_path(path, meter)?;
        }
        meter.release(queued_bytes).map_err(capture_demand_error)?;
        Ok(())
    })?;
    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_sort_by(&mut found,
        |left, right| right.0.depth().cmp(&left.0.depth()), meter))?;
    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_reserve(mutations, found.len(), meter))?;
    let found_bytes = found.capacity() as u64 * std::mem::size_of::<(NamespacePath, FileRecord)>() as u64;
    for (path, record) in found {
        mutations.push(AuthoredMutation::Remove {
            path,
            expected_file_id: Some(record.file_id),
        });
        receipt.changed_paths = checked_increment(receipt.changed_paths, receipt.work)?;
    }
    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| meter.release(found_bytes).map_err(capture_demand_error))?;
    Ok(())
}

fn capture_mutation_owned_bytes(
    mutations: &[AuthoredMutation], storage_bytes: u64, meter: &mut impl CaptureNativeWork,
) -> Result<u64, CaptureError> {
    let mut bytes = storage_bytes;
    for mutation in mutations {
        capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        let (path, second) = match mutation {
            AuthoredMutation::Rename { source, destination, .. }
            | AuthoredMutation::HardLink { source, destination } => (source, Some(destination)),
            AuthoredMutation::CloneRange(request) => (&request.source, Some(&request.destination)),
            AuthoredMutation::Reidentify { path, .. }
            | AuthoredMutation::CreateFile { path, .. }
            | AuthoredMutation::Write { path, .. }
            | AuthoredMutation::CreateSymbolicLink { path, .. }
            | AuthoredMutation::CreateReparsePoint { path, .. }
            | AuthoredMutation::CreateFileFromContent { path, .. }
            | AuthoredMutation::CreateDirectory { path, .. }
            | AuthoredMutation::CreateEmptySpecial { path, .. }
            | AuthoredMutation::CreateDevice { path, .. }
            | AuthoredMutation::Remove { path, .. }
            | AuthoredMutation::WriteFromContent { path, .. }
            | AuthoredMutation::SetMetadata { path, .. }
            | AuthoredMutation::Resize { path, .. }
            | AuthoredMutation::ZeroRange { path, .. }
            | AuthoredMutation::Preallocate { path, .. } => (path, None),
        };
        capture_charge(meter, WorkCounters { source_path_components: path.depth() as u64
            + second.map_or(0, |path| path.depth() as u64), ..WorkCounters::UNCHARGED })?;
        bytes = bytes.checked_add(mutation.logical_owned_bytes())
            .ok_or(CaptureError::Work(WorkError::Overflow))?;
    }
    Ok(bytes)
}

async fn apply_capture_transaction<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    mutations: Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    _budget: WorkBudget,
    _cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(), OperationFailure<CaptureError>> {
    let mutation_bytes = capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_mutation_owned_bytes(
        &mutations, mutations.capacity() as u64 * std::mem::size_of::<AuthoredMutation>() as u64, meter))?;
    if mutations.is_empty() {
        drop(mutations);
        capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| meter.release(mutation_bytes).map_err(capture_demand_error))?;
        return Ok(());
    }
    let input = (mutations, meter.transfer_allocation(mutation_bytes)
        .map_err(|error| OperationFailure::new(error, capture_meter(meter).completed()))?);
    let apply_cancellation = capture_meter(meter).leaf(|work| work.cancellation().clone());
    let applied = capture_sdk_in_heap_with_meter(
        |native| {
            let input = input;
            checkout.apply_authored_bulk_transaction_with_native_work(
                input.0, input.1, native, &apply_cancellation,
            )
        },
        meter, &apply_cancellation,
    ).await?;
    receipt.work = applied.work;
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
    work: &mut WorkCounters,
    meter: &mut impl CaptureNativeWork,
) -> Result<(), OperationFailure<CaptureError>> {
    capture_step(work, meter, |meter| {
        capture_reserve(mutations, 1 + usize::from(current.is_some()), meter)?;
        if let Some(record) = current {
            mutations.push(AuthoredMutation::Remove {
                path: clone_capture_path(&destination, meter)?,
                expected_file_id: Some(record.file_id),
            });
        }
        mutations.push(AuthoredMutation::HardLink { source, destination });
        Ok(())
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "appends the final state from the observed host facts of one path"
)]
fn append_final_state<W: CaptureNativeWork>(
    path: NamespacePath,
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    metadata: &HostStat,
    snapshot: HostSnapshot,
    kind: FileKind,
    exists_with_kind: bool,
    canonical_metadata: FileMetadata,
    mutations: &mut Vec<AuthoredMutation>,
    work: &mut WorkCounters,
    meter: &mut W,
) -> Result<(), OperationFailure<CaptureError>> {
    let appended = (|| {
        capture_reserve(mutations, if exists_with_kind { 2 } else { 1 }, meter)?;
        let target = if kind == FileKind::SymbolicLink {
            Some(read_link_bytes(source_root, host_path, meter)?)
        } else {
            None
        };
        {
            match kind {
                FileKind::Regular => unreachable!("regular files are staged after preparation"),
                FileKind::Directory => mutations.push(if exists_with_kind {
                    AuthoredMutation::SetMetadata { path, metadata: canonical_metadata }
                } else {
                    AuthoredMutation::CreateDirectory { path, metadata: canonical_metadata }
                }),
                FileKind::SymbolicLink => {
                    let target = target.expect("the symbolic link target was read before mutation");
                    if exists_with_kind {
                        mutations.push(AuthoredMutation::Remove { path: clone_capture_path(&path, meter)?, expected_file_id: None });
                    }
                    let target = capture_link_target(target, meter)?;
                    mutations.push(AuthoredMutation::CreateSymbolicLink { path, target: target.into(), metadata: canonical_metadata });
                }
                FileKind::Fifo | FileKind::Socket => mutations.push(if exists_with_kind {
                    AuthoredMutation::SetMetadata { path, metadata: canonical_metadata }
                } else {
                    AuthoredMutation::CreateEmptySpecial { path, kind, metadata: canonical_metadata }
                }),
                FileKind::CharacterDevice | FileKind::BlockDevice => {
                    let (major, minor) = host_device_identity(metadata)?;
                    if exists_with_kind {
                        mutations.push(AuthoredMutation::Remove { path: clone_capture_path(&path, meter)?, expected_file_id: None });
                    }
                    mutations.push(AuthoredMutation::CreateDevice { path, kind, major, minor, metadata: canonical_metadata });
                }
                FileKind::ReparsePoint | FileKind::MountBoundary => return Err(CaptureError::UnsupportedKind),
            }
        }
        let observed = capture_stat(source_root, host_path, meter).map_err(capture_demand_error)?;
        ensure_same_host_node(&snapshot, &observed)?;
        meter.check().map_err(capture_demand_error)
    })();
    *work = meter.completed();
    appended.map_err(|error| OperationFailure::new(error, *work))
}

#[allow(
    clippy::too_many_arguments,
    reason = "the stager, root, prepared file, span bound, mutation list, receipt, budget, and cancellation are all used per file"
)]
async fn finish_prepared_regular<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    stager: &Checkout<A, O>,
    source_root: &HostRoot,
    prepared: PreparedRegular,
    maximum_extent_spans: u32,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(), OperationFailure<CaptureError>> {
    let staged = stage_regular_body(
        stager,
        source_root,
        &prepared.host_path,
        &prepared.snapshot,
        maximum_extent_spans,
        receipt.work,
        budget,
        cancellation,
        meter,
    )
    .await?;
    receipt.work = staged.content.work;
    receipt.staged_file_bytes = receipt
        .staged_file_bytes
        .checked_add(staged.content.bytes)
        .ok_or_else(|| {
            OperationFailure::new(CaptureError::Work(WorkError::Overflow), receipt.work)
        })?;
    let host_path_bytes = capture_host_path_bytes(&prepared.host_path);
    drop(prepared.host_path);
    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| meter.release(host_path_bytes)
        .map_err(capture_demand_error))?;
    append_staged_regular_state(
        prepared.path,
        staged.logical_bytes,
        prepared.exists_with_kind,
        prepared.canonical_metadata,
        staged.content.ranges,
        mutations,
        &mut receipt.work,
        &mut capture_meter(meter),
    )?;
    Ok(())
}


#[allow(
    clippy::too_many_arguments,
    reason = "the stager, root, prepared paths, span bound, mutation list, receipt, budget, and cancellation are all used per batch"
)]
async fn finish_prepared_paths<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    stager: &Checkout<A, O>,
    source_root: &HostRoot,
    prepared: Vec<PreparedPath>,
    maximum_extent_spans: u32,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(), OperationFailure<CaptureError>> {
    let mut regular = capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_vec(prepared.len(), meter))?;
    let mut aliases = PendingHardLinks::new();
    let prepared_bytes = prepared.capacity() as u64 * std::mem::size_of::<PreparedPath>() as u64;
    for path in prepared {
        match path {
            PreparedPath::Regular(plan) => {
                regular.push(*plan);
                capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| meter.release(std::mem::size_of::<PreparedRegular>() as u64)
                    .map_err(capture_demand_error))?;
            }
            PreparedPath::PendingHardLink {
                source,
                destination,
                current,
            } => {
                capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| {
                    let pending = aliases.get_or_insert_measured(source, Vec::new(), meter)?;
                    capture_reserve(pending, 1, meter)?;
                    pending.push((destination, current));
                    Ok(())
                })?;
            }
        }
    }
    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| meter.release(prepared_bytes).map_err(capture_demand_error))?;
    let regular_bytes = regular.capacity() as u64 * std::mem::size_of::<PreparedRegular>() as u64;
    for plan in regular {
        let pending = capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| aliases.remove_measured(&plan.path, meter))?;
        finish_prepared_regular(stager,
        source_root,
        plan,
        maximum_extent_spans,
        mutations,
        receipt,
        budget,
        cancellation, meter)
        .await?;
        if let Some((key, pending)) = pending {
            let pending_bytes = pending.capacity() as u64 * std::mem::size_of::<(NamespacePath, Option<FileRecord>)>() as u64;
            let count = pending.len();
            let mut key = Some(key);
            for (index, (destination, current)) in pending.into_iter().enumerate() {
                let source = if index + 1 == count {
                    key.take().expect("final hard link owns its source")
                } else {
                    capture_path_clone(key.as_ref().expect("preceding hard link retains its source"), &mut receipt.work, &mut capture_meter(meter))?
                };
                append_hard_link(source, destination, current, mutations, &mut receipt.work, &mut capture_meter(meter))?;
            }
            if let Some(key) = key {
                capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| capture_drop_path(key, meter))?;
            }
            capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| meter.release(pending_bytes).map_err(capture_demand_error))?;
        }
    }
    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| meter.release(regular_bytes).map_err(capture_demand_error))?;
    if !aliases.is_empty() {
        return Err(OperationFailure::new(
            CaptureError::Invariant("pending hard-link source was not staged"),
            receipt.work,
        ));
    }
    let aliases_bytes = aliases.storage_bytes();
    drop(aliases);
    capture_step(&mut receipt.work, &mut capture_meter(meter), |meter| meter.release(aliases_bytes).map_err(capture_demand_error))?;
    Ok(())
}

struct StagedRegularBody {
    logical_bytes: u64,
    content: StagedHostRanges,
}

type PendingHardLinks = CaptureMap<NamespacePath, Vec<(NamespacePath, Option<FileRecord>)>>;


#[allow(
    clippy::too_many_arguments,
    reason = "stages one body from its host path and snapshot under the span bound, prior work, budget, and cancellation"
)]
async fn stage_regular_body<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    stager: &Checkout<A, O>,
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    snapshot: &HostSnapshot,
    maximum_extent_spans: u32,
    _prior_work: WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<StagedRegularBody, OperationFailure<CaptureError>> {
    let mut host_work = CaptureNativeHostWork::new(meter);
    let opened = (|| {
        cancellation.check().map_err(|_| CaptureError::Cancelled)?;
        #[cfg(unix)]
        let file = source_root.open_file_positional_with_work(host_path, &mut host_work)
            .map_err(capture_demand_error)?;
        #[cfg(windows)]
        let file = source_root.open_file_positional_wide_with_work(host_path, &mut host_work)
            .map_err(capture_demand_error)?;
        let file = file.into_std();
        let metadata = source_root.stat_file_with_work(&file, &mut host_work)
            .map_err(capture_demand_error)?;
        if !metadata.file_type().is_file() {
            return Err(CaptureError::StaleVersion);
        }
        ensure_same_host_node(snapshot, &metadata)?;
        let logical_bytes = metadata.len();
        let ranges = allocated_data_ranges_with_work(
            &file, logical_bytes, maximum_extent_spans, &mut host_work,
            |work| capture_ledger_meter(work.scope().original()).remaining(),
            #[cfg(windows)]
            |file, code, input, output, work| {
                native_device_control_in_scope(file, code, input, output, work.scope())
            },
        ).map_err(capture_demand_error)?;
        let positional = native_positional_file_measured(file, meter)?;
        host_work.leaf(|meter| {
            capture_allocate(meter, std::mem::size_of::<acyclic_native_runtime::NativeFile>() as u64)
        })?;
        let native = std::sync::Arc::new(positional);
        Ok((metadata, logical_bytes, ranges, native))
    })();
    let mut work = host_work.completed();
    let (metadata, logical_bytes, ranges, native) = opened
        .map_err(|error| OperationFailure::new(error, work))?;
    #[cfg(windows)]
    let ranges = {
        let (ranges, scanned) = settled_sparse_ranges(&native, &metadata, ranges,
            maximum_extent_spans, work, budget, cancellation, meter).await?;
        work = scanned;
        ranges
    };
    #[cfg(unix)]
    let _ = metadata;
    let staged = stage_host_ranges(stager, &native, &ranges,
        work, budget, cancellation, meter).await;
    let mut content = match staged {
        Ok(content) => content,
        Err(failure) => {
            let ranges_bytes = ranges.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64;
            let native_bytes = std::mem::size_of::<acyclic_native_runtime::NativeFile>() as u64;
            drop(ranges);
            drop(native);
            capture_step(&mut work, &mut capture_meter(meter), |meter| {
                meter.release(ranges_bytes).map_err(capture_demand_error)?;
                meter.release(native_bytes).map_err(capture_demand_error)
            })?;
            return Err(OperationFailure::new(failure.error, work));
        }
    };
    let verified = (|| {
        cancellation.check().map_err(|_| CaptureError::Cancelled)?;
        let after = source_root.stat_file_with_work(native.as_file(), &mut host_work)
            .map_err(capture_demand_error)?;
        ensure_same_host_node(snapshot, &after)?;
        let current = capture_stat(source_root, host_path, &mut host_work)
            .map_err(capture_demand_error)?;
        ensure_same_host_node(snapshot, &current)?;
        cancellation.check().map_err(|_| CaptureError::Cancelled)?;
        crate::native_host::NativeHostWork::check(&host_work).map_err(capture_demand_error)
    })();
    let ranges_bytes = ranges.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64;
    let native_bytes = std::mem::size_of::<acyclic_native_runtime::NativeFile>() as u64;
    drop(ranges);
    drop(native);
    capture_step(&mut work, &mut capture_meter(meter), |meter| {
        meter.release(ranges_bytes).map_err(capture_demand_error)?;
        meter.release(native_bytes).map_err(capture_demand_error)
    })?;
    if let Err(error) = verified {
        let bytes = content.ranges.capacity() as u64 * std::mem::size_of::<(HostDataRange, StagedContent)>() as u64;
        let ranges = std::mem::take(&mut content.ranges);
        drop(ranges);
        capture_step(&mut work, &mut capture_meter(meter), |meter| {
            meter.release(bytes).map_err(capture_demand_error)
        })?;
        return Err(OperationFailure::new(error, work));
    }
    content.work = work;
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
    work: &mut WorkCounters,
    meter: &mut impl CaptureNativeWork,
) -> Result<(), OperationFailure<CaptureError>> {
    capture_step(work, meter, |meter| {
        let ranges_bytes = ranges.capacity() as u64 * std::mem::size_of::<(HostDataRange, StagedContent)>() as u64;
        let count = ranges.len().checked_add(2 + usize::from(logical_bytes != 0))
            .ok_or(CaptureError::Work(WorkError::Overflow))?;
        capture_reserve(mutations, count, meter)?;
        if exists_with_kind {
            mutations.push(AuthoredMutation::Resize { path: clone_capture_path(&path, meter)?, logical_bytes: 0 });
        } else {
            mutations.push(AuthoredMutation::CreateFile { path: clone_capture_path(&path, meter)?,
                bytes: acyclic_stream::OwnedBytes::new(), metadata: canonical_metadata });
        }
        if logical_bytes != 0 {
            mutations.push(AuthoredMutation::Resize { path: clone_capture_path(&path, meter)?, logical_bytes });
        }
        for (range, content) in ranges {
            mutations.push(AuthoredMutation::WriteFromContent { path: clone_capture_path(&path, meter)?,
                offset: range.offset, content });
        }
        meter.release(ranges_bytes).map_err(capture_demand_error)?;
        mutations.push(AuthoredMutation::SetMetadata { path, metadata: canonical_metadata });
        Ok(())
    })
}

#[derive(Clone, Copy)]
struct HostSnapshot {
    identity: NativeRootIdentity,
    length: u64,
    metadata: FileMetadata,
}

struct HostObservation {
    metadata: HostStat,
}

impl HostObservation {
    fn from_metadata(metadata: HostStat) -> Self {
        Self { metadata }
    }
}

/// The link count of the observed node, read by the same query as its
/// identity, so it describes exactly the observed node.
#[cfg(unix)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "keeps the signature of the fallible Windows handle query"
)]
fn host_link_count(metadata: &HostStat) -> Result<u64, CaptureError> {
    Ok(metadata.nlink())
}

/// The link count of the observed node, read by the same handle query as
/// its identity, so it describes exactly the observed node.
#[cfg(windows)]
fn host_link_count(metadata: &HostStat) -> Result<u64, CaptureError> {
    metadata.number_of_links()
        .map(u64::from)
        .ok_or_else(|| CaptureError::Io(std::io::Error::other("host link count is unavailable")))
}

impl HostSnapshot {
    fn from_metadata(metadata: &HostStat) -> Result<Self, CaptureError> {
        Ok(Self {
            identity: metadata.identity().map_err(capture_demand_error)?,
            length: metadata.len(),
            metadata: capture_metadata(metadata),
        })
    }
}

fn ensure_same_host_node(
    expected: &HostSnapshot,
    observed: &HostStat,
) -> Result<(), CaptureError> {
    ensure_same_host_snapshot(expected, &HostSnapshot::from_metadata(observed)?)
}

fn ensure_same_host_snapshot(
    expected: &HostSnapshot,
    observed: &HostSnapshot,
) -> Result<(), CaptureError> {
    let mut expected_metadata = expected.metadata;
    let mut observed_metadata = observed.metadata;
    // Reading the file can legitimately update its access time.
    expected_metadata.accessed_ns = MetadataField::Unavailable;
    observed_metadata.accessed_ns = MetadataField::Unavailable;
    // Linux statx can expose birth time through a path while fstat on a held
    // descriptor (notably with musl) cannot. Compare it only when both queries
    // can observe it; identity, length, mtime, ctime and other metadata still
    // detect mutation, and the path is revalidated after reading.
    if matches!(expected_metadata.created_ns, MetadataField::Unavailable)
        || matches!(observed_metadata.created_ns, MetadataField::Unavailable)
    {
        expected_metadata.created_ns = MetadataField::Unavailable;
        observed_metadata.created_ns = MetadataField::Unavailable;
    }
    // Reading a projected placeholder hydrates it.
    #[cfg(windows)]
    if let (MetadataField::Value(before), MetadataField::Value(after)) = (
        expected_metadata.windows_attributes,
        observed_metadata.windows_attributes,
    ) && crate::native_host::same_windows_attributes(before, after)
    {
        observed_metadata.windows_attributes = expected_metadata.windows_attributes;
    }
    if expected.identity != observed.identity
        || expected.length != observed.length
        || expected_metadata != observed_metadata
    {
        return Err(CaptureError::StaleVersion);
    }
    Ok(())
}

fn ensure_current_host_node<W: CaptureNativeWork>(
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    expected: &HostSnapshot,
    work: &mut WorkCounters,
    _budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &mut W,
) -> Result<(), OperationFailure<CaptureError>> {
    let verified = (|| {
        cancellation.check().map_err(|_| CaptureError::Cancelled)?;
        meter.check().map_err(capture_demand_error)?;
        let observed = capture_stat(source_root, host_path, meter).map_err(capture_demand_error)?;
        ensure_same_host_node(expected, &observed)?;
        cancellation.check().map_err(|_| CaptureError::Cancelled)?;
        meter.check().map_err(capture_demand_error)
    })();
    *work = meter.completed();
    verified.map_err(|error| OperationFailure::new(error, *work))
}

#[cfg(test)]
fn test_host_stat(root: &HostRoot, path: &Path) -> Result<HostStat, CaptureError> {
    let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
    root.stat(path, &mut meter).map_err(capture_demand_error)
}

#[cfg(test)]
fn test_held_stat(root: &HostRoot, file: &cap_std::fs::File) -> Result<HostStat, CaptureError> {
    let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
    root.stat_file_with_work(&file.try_clone()?.into_std(), &mut meter).map_err(capture_demand_error)
}

#[cfg(test)]
fn test_host_path(path: &Path) -> CaptureHostPath {
    #[cfg(unix)]
    { path.to_path_buf() }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;
        path.as_os_str().encode_wide().collect()
    }
}

#[cfg(test)]
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
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        std::fs::rename(&current, temporary.path().join("displaced"))?;
        std::fs::rename(&replacement, &current)?;
        let opened = root.open_file(Path::new("current"), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        assert!(matches!(
            ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?),
            Err(CaptureError::StaleVersion)
        ));
        Ok(())
    }

    #[test]
    fn rejects_a_changed_file_after_open() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let current = temporary.path().join("current");
        std::fs::write(&current, b"first")?;
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        let opened = root.open_file(Path::new("current"), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?)?;
        std::fs::write(&current, b"changed body")?;
        assert!(matches!(
            ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?),
            Err(CaptureError::StaleVersion)
        ));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn rejects_a_permission_change_after_open() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::PermissionsExt;
        let temporary = tempfile::tempdir()?;
        let current = temporary.path().join("current");
        std::fs::write(&current, b"first")?;
        std::fs::set_permissions(&current, std::fs::Permissions::from_mode(0o644))?;
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        let opened = root.open_file(Path::new("current"), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?)?;
        std::fs::set_permissions(&current, std::fs::Permissions::from_mode(0o600))?;
        assert!(matches!(
            ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?),
            Err(CaptureError::StaleVersion)
        ));
        Ok(())
    }

    #[test]
    fn accepts_an_unobservable_creation_time_in_either_direction()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("current"), b"first")?;
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let metadata = test_host_stat(&root, Path::new("current"))?;
        let mut expected = HostSnapshot::from_metadata(&metadata)?;
        expected.metadata.created_ns = MetadataField::Value(123);
        let mut observed = expected;
        observed.metadata.created_ns = MetadataField::Unavailable;
        // The exact musl case: path statx sees birth time, held fstat cannot.
        ensure_same_host_snapshot(&expected, &observed)?;
        // Also allow the reverse observation order.
        ensure_same_host_snapshot(&observed, &expected)?;
        Ok(())
    }

    #[test]
    fn rejects_an_observable_creation_time_change() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("current"), b"first")?;
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let metadata = test_host_stat(&root, Path::new("current"))?;
        let mut expected = HostSnapshot::from_metadata(&metadata)?;
        expected.metadata.created_ns = MetadataField::Value(123);
        let mut observed = expected;
        observed.metadata.created_ns = MetadataField::Value(124);
        assert!(matches!(
            ensure_same_host_snapshot(&expected, &observed),
            Err(CaptureError::StaleVersion)
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
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        let opened = root.open_file(Path::new("current"), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        std::fs::rename(&current, temporary.path().join("displaced"))?;
        std::fs::rename(&replacement, &current)?;
        let _held_metadata = test_held_stat(&root, &opened)?;
        let path_matches =
            ensure_same_host_node(&expected, &test_host_stat(&root, Path::new("current"))?).is_ok();
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
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        std::fs::rename(&current, temporary.path().join("displaced"))?;
        std::fs::rename(&replacement, &current)?;
        assert!(matches!(
            ensure_current_host_node(&root, &test_host_path(Path::new("current")), &expected,
                &mut WorkCounters::UNCHARGED, WorkBudget::UNBOUNDED, &CancellationToken::new(),
                &mut DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())),
            Err(OperationFailure { error: CaptureError::StaleVersion, .. })
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
    stager: &Checkout<A, O>,
    native: &std::sync::Arc<acyclic_native_runtime::NativeFile>,
    ranges: &[HostDataRange],
    prior_work: WorkCounters,
    _budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<StagedHostRanges, OperationFailure<CaptureError>> {
    let mut work = prior_work;
    let mut staged = capture_step(&mut work, &mut capture_meter(meter), |meter| capture_vec(ranges.len(), meter))?;
    let mut bytes = 0_u64;
    for range in ranges {
        let mut bounded = NativeRangeSource::new_captured(
            acyclic_native_runtime::AsyncRangeReader::new(
                std::sync::Arc::clone(native), range.offset, range.length,
            ),
            meter,
        );
        let stage_cancellation = meter.native().cancellation().clone();
        let content = capture_sdk_in_heap_with_meter(
            |original| stager.stage(&mut bounded, range.length, original, &stage_cancellation),
            meter, &stage_cancellation,
        ).await;
        drop(bounded);
        let accumulated = capture_meter(meter).completed();
        let content = content.map_err(|failure| OperationFailure::new(failure.error, accumulated));
        let content = match content {
            Ok(content) => content,
            Err(failure) => {
                capture_step(&mut work, &mut capture_meter(meter), |meter| {
                    let bytes = staged.capacity() as u64 * std::mem::size_of::<(HostDataRange, StagedContent)>() as u64;
                    drop(staged);
                    meter.release(bytes).map_err(capture_demand_error)
                })?;
                return Err(OperationFailure::new(failure.error, work));
            }
        };
        if content.value.logical_bytes() != range.length {
            capture_step(&mut work, &mut capture_meter(meter), |meter| {
                let bytes = staged.capacity() as u64 * std::mem::size_of::<(HostDataRange, StagedContent)>() as u64;
                drop(staged);
                meter.release(bytes).map_err(capture_demand_error)
            })?;
            return Err(OperationFailure::new(CaptureError::StaleVersion, work));
        }
        bytes = bytes.checked_add(range.length).ok_or_else(|| {
            OperationFailure::new(CaptureError::Work(WorkError::Overflow), accumulated)
        })?;
        staged.push((*range, content.value));
        work = accumulated;
    }
    Ok(StagedHostRanges {
        ranges: staged,
        work: capture_meter(meter).completed(),
        bytes,
    })
}

/// The data ranges of a file marked sparse, settled against its content.
///
/// `ReFS` reports what was just written to a sparse file as allocated for a
/// while, up to the whole file, flushed or not, and only later the ranges
/// that hold data. Within what it reports, each block of zeros is taken as
/// the hole it reads as, so a capture holds the same content whenever it
/// runs. The report stands where settling it would take more than
/// `maximum_ranges`. Every byte read counts against `budget`, after
/// `prior_work`, and the scan stops once cancelled; returns the work it did.
#[cfg(windows)]
async fn settled_sparse_ranges(
    native: &std::sync::Arc<acyclic_native_runtime::NativeFile>,
    metadata: &HostStat,
    reported: Vec<HostDataRange>,
    maximum_ranges: u32,
    prior_work: WorkCounters,
    _budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(Vec<HostDataRange>, WorkCounters), OperationFailure<CaptureError>> {
    use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_SPARSE_FILE;

    const BLOCK: u64 = 4096;
    const CHUNK: u64 = 1 << 20;
    if metadata.file_attributes() & FILE_ATTRIBUTE_SPARSE_FILE.0 == 0 {
        return Ok((reported, capture_meter(meter).completed()));
    }
    let mut work = prior_work;
    let mut settled = Vec::new();
    for range in &reported {
        let mut reader = acyclic_native_runtime::AsyncRangeReader::new(
            std::sync::Arc::clone(native), range.offset, range.length,
        );
        let end = range.offset.checked_add(range.length).ok_or_else(|| {
            OperationFailure::new(CaptureError::Work(WorkError::Overflow), work)
        })?;
        let mut position = range.offset;
        let mut report_stands = false;
        while position < end {
            let wanted = (CHUNK - position % CHUNK).min(end - position);
            let read = capture_in_heap_with_meter(|| async {
                cancellation.check().map_err(|_| OperationFailure::new(
                    CaptureError::Cancelled, capture_meter(meter).completed()))?;
                meter.native().admit(WorkCounters { source_bytes_read: wanted,
                    ..WorkCounters::UNCHARGED })
                    .map_err(|error| OperationFailure::new(CaptureError::NativeKernel(error), capture_meter(meter).completed()))?;
                let runtime = crate::native_capture::capture_runtime_meter_owner(&meter.native(),
                    meter.custody.as_ref().expect("capture scope is live"))
                    .map_err(|error| OperationFailure::new(CaptureError::NativeKernel(error), capture_meter(meter).completed()))?;
                let value = reader.read(wanted as usize, runtime, true)
                    .await.map_err(|error| {
                        let cause = if let Some(cause) = meter.original().take_native_io_failure() {
                            capture_demand_error(cause)
                        } else if error.kind() == std::io::ErrorKind::UnexpectedEof {
                            CaptureError::StaleVersion
                        } else {
                            CaptureError::Io(error)
                        };
                        OperationFailure::new(cause, capture_meter(meter).completed())
                    })?;
                Ok(OperationReceipt { value, work: capture_meter(meter).completed() })
            }, meter.original()).await?;
            work = read.work;
            let chunk = read.value;
            let read_end = position + chunk.bytes.len() as u64;
            report_stands = capture_step(&mut work, &mut capture_meter(meter), |meter| {
                let scanned = (|| {
                    let mut start = position;
                    while start < read_end {
                        meter.check().map_err(capture_demand_error)?;
                        let block_end = start.saturating_add(BLOCK - start % BLOCK).min(read_end);
                        let block = chunk.bytes.get((start - position) as usize..(block_end - position) as usize)
                            .ok_or(CaptureError::InvalidOptions)?;
                        let mut nonzero = false;
                        for byte in block {
                            capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                            if *byte != 0 { nonzero = true; break; }
                        }
                        if nonzero && !keep_sparse_range(&mut settled, maximum_ranges as usize,
                            start, block_end - start, meter)? {
                            return Ok(true);
                        }
                        start = block_end;
                    }
                    Ok(false)
                })();
                // The original native guard remains behind the bytes through
                // every scan and retires only after their physical drop.
                drop(chunk);
                scanned
            })?;
            position = read_end;
            if report_stands { break; }
        }
        if report_stands {
            capture_step(&mut work, &mut capture_meter(meter), |meter| {
                let bytes = settled.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64;
                drop(settled);
                meter.release(bytes).map_err(capture_demand_error)
            })?;
            return Ok((reported, work));
        }
    }
    capture_step(&mut work, &mut capture_meter(meter), |meter| {
        let bytes = reported.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64;
        drop(reported);
        meter.release(bytes).map_err(capture_demand_error)
    })?;
    Ok((settled, work))
}

#[cfg(windows)]
fn keep_sparse_range(
    ranges: &mut Vec<HostDataRange>,
    maximum: usize,
    offset: u64,
    length: u64,
    meter: &mut impl CaptureNativeWork,
) -> Result<bool, CaptureError> {
    meter.check().map_err(capture_demand_error)?;
    if let Some(last) = ranges.last_mut() {
        if last.offset.checked_add(last.length) == Some(offset) {
            last.length = last.length.checked_add(length).ok_or(CaptureError::Work(WorkError::Overflow))?;
            return Ok(true);
        }
    }
    if ranges.len() == maximum { return Ok(false); }
    if ranges.len() == ranges.capacity() {
        let size = std::mem::size_of::<HostDataRange>();
        let preferred = ranges.capacity().saturating_mul(2).max(1).min(maximum);
        let available = meter.leaf(|work| work.remaining()).map_err(capture_demand_error)?
            .peak_allocation_bytes / size as u64;
        let next = preferred.min(usize::try_from(available).unwrap_or(usize::MAX))
            .max(ranges.len() + 1);
        let copied = ranges.len() * size;
        let delta = WorkCounters { bytes_copied: copied as u64, ..WorkCounters::UNCHARGED };
        meter.admit(delta).map_err(capture_demand_error)?;
        let mut replacement = capture_vec(next, meter)?;
        replacement.extend_from_slice(ranges);
        meter.record(delta).map_err(capture_demand_error)?;
        let old_bytes = ranges.capacity() * size;
        let old = std::mem::replace(ranges, replacement);
        drop(old);
        meter.release(old_bytes as u64).map_err(capture_demand_error)?;
    }
    ranges.push(HostDataRange { offset, length });
    Ok(true)
}

/// The native file a handle from [`HostRoot::open_file_positional`] reads
/// through.
#[cfg(all(test, windows))]
#[allow(
    unsafe_code,
    reason = "adopts the overlapped handle into the native runtime"
)]
fn native_positional_file(
    file: std::fs::File,
) -> std::io::Result<acyclic_native_runtime::NativeFile> {
    // SAFETY: a positional handle is opened overlapped; this fresh file
    // object is attached to no port, and this sole duplicate carries all of
    // its I/O.
    unsafe { acyclic_native_runtime::NativeFile::from_overlapped_file_unchecked(file, None).map_err(|(error, _)| error) }
}

/// The native file a handle from [`HostRoot::open_file_positional`] reads
/// through.
#[cfg(all(test, not(windows)))]
fn native_positional_file(
    file: std::fs::File,
) -> std::io::Result<acyclic_native_runtime::NativeFile> {
    acyclic_native_runtime::NativeFile::from_file(file, None).map_err(|(error, _)| error)
}

struct NativeRangeSource {
    reader: acyclic_native_runtime::AsyncRangeReader,
    capture_custody: Option<std::sync::Arc<crate::demand::CaptureTransientCustody>>,
}


#[allow(unsafe_code, reason = "the measured host opener transfers its fresh positional handle")]
fn native_positional_file_measured(
    file: std::fs::File,
    scope: &CaptureWorkScope,
) -> Result<acyclic_native_runtime::NativeFile, CaptureError> {
    let meter = scope.native().runtime_meter_owner();
    #[cfg(windows)]
    let result = unsafe {
        acyclic_native_runtime::NativeFile::from_overlapped_file_unchecked(file, Some(acyclic_native_runtime::NativeFileMeter::Owned(meter)))
    };
    #[cfg(not(windows))]
    let result = acyclic_native_runtime::NativeFile::from_file(file, Some(acyclic_native_runtime::NativeFileMeter::Owned(meter)));
    result.map_err(|(error, file)| {
        // This is an ordinary std file, not an explicit SDK close operation.
        // Retire its physical handle before projecting the original failure.
        drop(file);
        capture_demand_error(scope.original().take_native_io_failure()
            .unwrap_or_else(|| native_demand_error(error)))
    })
}


impl NativeRangeSource {
    #[cfg(test)]
    fn new_original(reader: acyclic_native_runtime::AsyncRangeReader) -> Self {
        Self { reader, capture_custody: None }
    }

    fn new_captured(reader: acyclic_native_runtime::AsyncRangeReader, scope: &CaptureWorkScope) -> Self {
        Self { reader, capture_custody: Some(std::sync::Arc::clone(
            scope.custody.as_ref().expect("capture scope is live"))) }
    }

}


impl crate::kernel::AsyncBlobSource for NativeRangeSource {
    async fn read<'a>(
        &'a mut self,
        _destination: &'a mut [u8],
        _budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        cancellation.check()
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::Interrupted))?;
        // This internal producer requires the original native invocation,
        // rather than constructing another counter/budget bank for a caller.
        Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
    }


    async fn read_with_native_work<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        original: &'a crate::NativeMaterializationWork,
        cancellation: &'a CancellationToken,
    ) -> Result<usize, crate::FsError> {
        cancellation.check()?;
        original.admit(WorkCounters::UNCHARGED)?;
        let reader = &mut self.reader;
        let maximum = destination.len().min(usize::try_from(reader.remaining()).unwrap_or(usize::MAX));
        if maximum == 0 { return Ok(0); }
        original.admit(WorkCounters { source_bytes_read: maximum as u64,
            ..WorkCounters::UNCHARGED })?;
        let copy = original.reserve_copy(maximum as u64)?;
        let runtime = match &self.capture_custody {
            Some(custody) => crate::native_capture::capture_runtime_meter_owner(&original, custody)?,
            None => original.runtime_meter_owner(),
        };
        let result = reader.read(maximum, runtime, true).await
            .map_err(|error| {
                original.native_read_meter().take_native_io_failure()
                    .map(crate::materializer::native_materialization_work_error)
                    .map(crate::FsError::NativeKernel)
                    .unwrap_or_else(|| if error.kind() == std::io::ErrorKind::UnexpectedEof {
                        crate::FsError::SourceSnapshotStale
                    } else {
                        crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Io(error))
                    })
            })?;
        if result.bytes.len() != maximum { return Err(crate::FsError::SourceSnapshotStale); }
        destination[..maximum].copy_from_slice(&result.bytes);
        copy.record(maximum as u64)?;
        // The native packet owns the actual buffer through the copy and any
        // late failure; bytes physically retire before its allocation guard.
        drop(result);
        cancellation.check()?;
        original.admit(WorkCounters::UNCHARGED)?;
        Ok(maximum)
    }

}


#[cfg(test)]
mod native_range_tests {
    use super::*;
    use crate::kernel::AsyncBlobSource as _;

    fn budget() -> WorkBudget {
        WorkCounters {
            backend_read_operations: 16, source_bytes_read: 32,
            allocation_operations: 16, peak_allocation_bytes: 16 * 1024,
            items_examined: 32, bytes_copied: 32,
            ..WorkCounters::UNCHARGED
        }
    }

    fn fixture_file(path: &Path) -> std::io::Result<std::fs::File> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt as _;
            options.custom_flags(windows::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED.0);
        }
        options.open(path)
    }

    fn reader(path: &Path, expected: u64) -> std::io::Result<acyclic_native_runtime::AsyncRangeReader> {
        Ok(acyclic_native_runtime::AsyncRangeReader::new(
            std::sync::Arc::new(native_positional_file(fixture_file(path)?)?), 0, expected,
        ))
    }

    pub(super) fn original_budget() -> WorkBudget {
        WorkCounters {
            allocation_operations: 128,
            peak_allocation_bytes: 1024 * 1024,
            ..budget()
        }
    }

    #[cfg(unix)]
    #[test]
    #[allow(unsafe_code, reason = "advisory locks observe the real imported descriptor's physical retirement")]
    fn cancelled_native_import_retires_std_file_before_preserving_original_failure()
        -> Result<(), Box<dyn std::error::Error>>
    {
        use std::os::fd::AsRawFd as _;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("cancelled-import");
        std::fs::write(&path, b"actual")?;
        let file = fixture_file(&path)?;
        let contender = fixture_file(&path)?;
        assert_eq!(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) }, 0);
        assert_ne!(unsafe { libc::flock(contender.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) }, 0);
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(original_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        let mut scope = CaptureWorkScope::new(&original)?;
        let before = original.receipt();
        cancellation.cancel();
        assert!(matches!(native_positional_file_measured(file, &scope), Err(CaptureError::Cancelled)));
        assert_eq!(unsafe { libc::flock(contender.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) }, 0);
        assert_eq!(original.receipt(), before);
        assert_eq!(original.receipt().backend_write_operations, 0);
        scope.release()?;
        Ok(())
    }

    #[tokio::test]
    async fn captured_read_guard_and_tail_do_not_retain_scope_custody()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("captured-guard");
        std::fs::write(&path, b"abc")?;
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(original_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        let mut scope = CaptureWorkScope::new(&original)?;
        let native = std::sync::Arc::new(native_positional_file_measured(fixture_file(&path)?, &scope)?);
        let meter = crate::native_capture::capture_runtime_meter_owner(&original,
            scope.custody.as_ref().expect("live scope"))?;
        let base = meter.allocation_owner().expect("capture allocations use the original base");
        assert!(std::sync::Arc::ptr_eq(&base, &original.runtime_meter_owner()));
        let packet = acyclic_native_runtime::AsyncRangeReader::new(
            std::sync::Arc::clone(&native), 0, 3,
        ).read(3, meter, true).await?;
        let prefix = original.receipt();
        scope.release()?;
        // The real returned data and native file tail remain alive here.
        // Their allocation owners must not retain the terminal scope anchor.
        assert_eq!(packet.bytes.as_ref(), b"abc");
        assert_eq!(original.receipt(), prefix);
        assert!(original.live_allocation_bytes() >= packet.buffer_capacity as u64);
        drop(packet);
        drop(native);
        assert_eq!(original.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes());
        Ok(())
    }

    #[tokio::test]
    async fn captured_source_stale_and_drop_retire_only_terminal_custody()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("captured-stale");
        std::fs::write(&path, b"abcdefgh")?;
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(original_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        let mut scope = CaptureWorkScope::new(&original)?;
        let native = std::sync::Arc::new(native_positional_file_measured(fixture_file(&path)?, &scope)?);
        let reader = acyclic_native_runtime::AsyncRangeReader::new(std::sync::Arc::clone(&native), 0, 8);
        let mut source = NativeRangeSource::new_captured(reader, &scope);
        std::fs::write(&path, b"abc")?;
        let before = original.receipt();
        let mut destination = [0xa5; 8];
        assert!(matches!(source.read_with_native_work(
            &mut destination, &original, &cancellation,
        ).await, Err(crate::FsError::SourceSnapshotStale)));
        let prefix = original.receipt();
        assert_eq!(prefix.source_bytes_read - before.source_bytes_read, 3);
        assert_eq!(prefix.bytes_copied, before.bytes_copied);
        assert_eq!(destination, [0xa5; 8]);
        drop(source);
        scope.release()?;
        assert_eq!(original.receipt(), prefix);
        drop(native);
        assert_eq!(original.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes());
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn captured_accepted_job_survives_observer_drop_then_retires_scope()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("captured-accepted");
        std::fs::write(&path, b"abc")?;
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(original_budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        let mut scope = CaptureWorkScope::new(&original)?;
        let native = std::sync::Arc::new(native_positional_file_measured(fixture_file(&path)?, &scope)?);
        let meter = crate::native_capture::capture_runtime_meter_owner(&original,
            scope.custody.as_ref().expect("live scope"))?;
        let (started, start) = std::sync::mpsc::channel();
        let (resume, resumed) = std::sync::mpsc::channel();
        let operation = native.control_async(move |file, meter| { let meter = meter.expect("metered control");
            started.send(()).map_err(std::io::Error::other)?;
            resumed.recv().map_err(std::io::Error::other)?;
            let mut data = [0; 3];
            assert_eq!(acyclic_native_runtime::read_declared_source_bytes(file, 0, &mut data, meter)?, 3);
            assert_eq!(&data, b"abc");
            Ok(())
        }, Some(meter))?;
        let operation = tokio::spawn(operation);
        tokio::task::yield_now().await;
        start.recv_timeout(std::time::Duration::from_secs(10))?;
        operation.abort();
        assert!(operation.await.expect_err("observer must be dropped").is_cancelled());
        let before = original.receipt();
        let (retired, retirement) = std::sync::mpsc::channel();
        let cleanup = std::thread::spawn(move || {
            let result = scope.release();
            let _ = retired.send(());
            result
        });
        assert!(matches!(retirement.recv_timeout(std::time::Duration::from_millis(20)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)));
        resume.send(())?;
        retirement.recv_timeout(std::time::Duration::from_secs(10))?;
        cleanup.join().expect("scope retirement must not panic")?;
        let completed = original.receipt();
        assert_eq!(completed.source_bytes_read - before.source_bytes_read, 3);
        assert!(completed.backend_read_operations > before.backend_read_operations);
        // A still-live shared file tail has no terminal capture anchor.
        assert!(native.as_file().metadata()?.len() == 3);
        drop(native);
        assert_eq!(original.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes());
        Ok(())
    }

    #[tokio::test]
    async fn original_native_eof_and_owned_decline_have_no_read_work()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("original-empty");
        std::fs::write(&path, [])?;
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(original_budget(), &cancellation)
            .expect("fund original accounting state");
        let baseline = original.receipt();
        assert_eq!(baseline.allocation_operations, 1);
        assert_eq!(original.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes());
        let mut source = NativeRangeSource::new_original(reader(&path, 0)?);
        let mut destination = [0xa5; 8];
        assert!(source.read_owned_with_native_work(8, &original, &cancellation).await?.is_none());
        assert_eq!(source.read_with_native_work(&mut destination, &original, &cancellation).await?, 0);
        assert_eq!(destination, [0xa5; 8]);
        assert_eq!(original.receipt(), baseline);
        assert_eq!(source.take_work(), None);
        Ok(())
    }

    #[tokio::test]
    async fn original_native_byte_and_copy_refusals_precede_io()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("original-denied");
        std::fs::write(&path, b"abc")?;
        for counter in ["source_bytes_read", "bytes_copied"] {
            let cancellation = CancellationToken::new();
            let mut limit = original_budget();
            if counter == "source_bytes_read" { limit.source_bytes_read = 2; }
            else { limit.bytes_copied = 2; }
            let original = crate::NativeMaterializationWork::new(limit, &cancellation)
                .expect("fund original accounting state");
            let baseline = original.receipt();
            let live = original.live_allocation_bytes();
            let mut source = NativeRangeSource::new_original(reader(&path, 3)?);
            let mut destination = [0xa5; 8];
            let error = source.read_with_native_work(&mut destination, &original, &cancellation)
                .await.expect_err("a denied full range must not become truncated success");
            assert!(matches!(error,
                crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(
                    crate::WorkError::BudgetExceeded { counter: refused, .. }
                )) if refused == counter));
            assert_eq!(destination, [0xa5; 8]);
            assert_eq!(source.reader.remaining(), 3);
            assert_eq!(original.receipt(), baseline);
            assert_eq!(original.live_allocation_bytes(), live);
        }
        Ok(())
    }

    #[tokio::test]
    async fn original_native_stale_retains_partial_io_without_shared_header()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("original-short");
        std::fs::write(&path, b"abcdefgh")?;
        let opened = reader(&path, 8)?;
        std::fs::write(&path, b"abc")?;
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(original_budget(), &cancellation)
            .expect("fund original accounting state");
        let baseline = original.receipt();
        let live = original.live_allocation_bytes();
        let mut source = NativeRangeSource::new_original(opened);
        let mut destination = [0xa5; 8];
        assert!(matches!(
            source.read_with_native_work(&mut destination, &original, &cancellation).await,
            Err(crate::FsError::SourceSnapshotStale)
        ));
        let partial = original.receipt();
        assert_eq!(partial.source_bytes_read - baseline.source_bytes_read, 3);
        assert_eq!(partial.backend_read_operations - baseline.backend_read_operations,
            if cfg!(windows) { 3 } else { 2 });
        assert!(partial.allocation_operations > baseline.allocation_operations);
        assert!(partial.peak_allocation_bytes >= live + 8);
        assert_eq!(partial.bytes_copied, baseline.bytes_copied);
        assert_eq!(destination, [0xa5; 8]);
        assert_eq!(original.live_allocation_bytes(), live);

        // The same physical short read in ordinary mode additionally builds
        // the shared-byte header; exact failure must stop before that step.
        let ordinary = crate::NativeMaterializationWork::new(original_budget(), &cancellation)
            .expect("fund ordinary accounting state");
        let ordinary_baseline = ordinary.receipt();
        let packet = reader(&path, 8)?.read(8, ordinary.runtime_meter_owner(), false).await?;
        assert_eq!(packet.bytes.as_ref(), b"abc");
        assert_eq!(ordinary.receipt().allocation_operations - ordinary_baseline.allocation_operations,
            partial.allocation_operations - baseline.allocation_operations + 1);
        drop(packet);
        assert_eq!(ordinary.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes());
        Ok(())
    }

    #[tokio::test]
    async fn original_native_multipath_stale_retains_success_and_partial_prefix()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let first_path = temporary.path().join("first");
        let second_path = temporary.path().join("second");
        std::fs::write(&first_path, b"abc")?;
        std::fs::write(&second_path, b"abcdefgh")?;
        let mut first = NativeRangeSource::new_original(reader(&first_path, 3)?);
        let mut second = NativeRangeSource::new_original(reader(&second_path, 8)?);
        std::fs::write(&second_path, b"xy")?;
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(original_budget(), &cancellation)
            .expect("fund original multipath accounting state");
        let baseline = original.receipt();
        let live = original.live_allocation_bytes();
        let mut first_destination = [0; 3];
        assert_eq!(first.read_with_native_work(
            &mut first_destination, &original, &cancellation,
        ).await?, 3);
        let first_prefix = original.receipt();
        assert_eq!(&first_destination, b"abc");
        let mut second_destination = [0xa5; 8];
        assert!(matches!(second.read_with_native_work(
            &mut second_destination, &original, &cancellation,
        ).await, Err(crate::FsError::SourceSnapshotStale)));
        let completed = original.receipt();
        assert_eq!(completed.source_bytes_read - baseline.source_bytes_read, 5);
        assert_eq!(completed.bytes_copied - baseline.bytes_copied, 3);
        assert!(completed.backend_read_operations >= first_prefix.backend_read_operations + 2);
        assert!(completed.allocation_operations > first_prefix.allocation_operations);
        assert_eq!(second_destination, [0xa5; 8]);
        assert_eq!(original.live_allocation_bytes(), live);
        assert_eq!(first.take_work(), None);
        assert_eq!(second.take_work(), None);
        drop((first, second));
        assert_eq!(original.receipt(), completed);
        Ok(())
    }

    #[tokio::test]
    async fn original_native_success_retires_packet_and_cancel_keeps_prefix()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("original-copy");
        std::fs::write(&path, b"abcdef")?;
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(original_budget(), &cancellation)
            .expect("fund original accounting state");
        let baseline = original.receipt();
        let live = original.live_allocation_bytes();
        let remaining = original.remaining()?.peak_allocation_bytes;
        let mut source = NativeRangeSource::new_original(reader(&path, 6)?);
        assert!(source.read_owned_with_native_work(3, &original, &cancellation).await?.is_none());
        assert_eq!(original.receipt(), baseline);
        let mut destination = [0; 3];
        assert_eq!(source.read_with_native_work(&mut destination, &original, &cancellation).await?, 3);
        assert_eq!(&destination, b"abc");
        let prefix = original.receipt();
        assert_eq!(prefix.source_bytes_read - baseline.source_bytes_read, 3);
        assert_eq!(prefix.bytes_copied - baseline.bytes_copied, 3);
        assert!(prefix.backend_read_operations > baseline.backend_read_operations);
        assert!(prefix.allocation_operations > baseline.allocation_operations);
        assert_eq!(original.live_allocation_bytes(), live);
        assert_eq!(original.remaining()?.peak_allocation_bytes, remaining);
        cancellation.cancel();
        assert!(matches!(
            source.read_with_native_work(&mut destination, &original, &cancellation).await,
            Err(crate::FsError::Cancelled(_)) |
                Err(crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Cancelled))
        ));
        assert_eq!(original.receipt(), prefix);
        assert_eq!(&destination, b"abc");
        assert_eq!(original.live_allocation_bytes(), live);
        Ok(())
    }

    #[tokio::test]
    async fn original_native_packet_unwind_retires_physical_capacity()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("original-unwind");
        std::fs::write(&path, b"abc")?;
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(original_budget(), &cancellation)
            .expect("fund original accounting state");
        let live = original.live_allocation_bytes();
        let remaining = original.remaining()?.peak_allocation_bytes;
        let packet = reader(&path, 3)?.read(3, original.runtime_meter_owner(), true).await?;
        let prefix = original.receipt();
        assert_eq!(original.live_allocation_bytes(), live + packet.buffer_capacity as u64);
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let packet = packet;
            assert_eq!(packet.bytes.as_ref(), b"abc");
            panic!("exercise physical native packet retirement");
        }));
        assert!(unwound.is_err());
        assert_eq!(original.live_allocation_bytes(), live);
        assert_eq!(original.remaining()?.peak_allocation_bytes, remaining);
        assert_eq!(original.receipt(), prefix);
        Ok(())
    }


    #[tokio::test]
    async fn native_source_drop_keeps_original_prefix_without_receipt_merging()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("original-prefix");
        std::fs::write(&path, b"abc")?;
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(original_budget(), &cancellation)
            .expect("fund original accounting state");
        let live = original.live_allocation_bytes();
        let mut source = NativeRangeSource::new_original(reader(&path, 3)?);
        let mut destination = [0_u8; 3];
        assert_eq!(source.read_with_native_work(&mut destination, &original, &cancellation).await?, 3);
        assert_eq!(&destination, b"abc");
        let prefix = original.receipt();
        assert_eq!(prefix.source_bytes_read, 3);
        assert!(prefix.backend_read_operations > 0);
        assert_eq!(source.take_work(), None);
        assert_eq!(source.take_work(), None);
        assert_eq!(original.receipt(), prefix);
        drop(source);
        assert_eq!(original.receipt(), prefix);
        assert_eq!(original.live_allocation_bytes(), live);
        Ok(())
    }

    #[tokio::test]
    async fn neutral_demand_meter_preserves_ordinary_eof_prefix()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("ordinary-eof");
        std::fs::write(&path, b"abc")?;
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(original_budget(), &cancellation)
            .expect("fund original accounting state");
        let baseline = original.receipt();
        let live = original.live_allocation_bytes();
        let remaining = original.remaining()?.peak_allocation_bytes;
        let mut reader = reader(&path, 8)?;
        let result = reader.read(8, original.runtime_meter_owner(), false).await?;
        assert_eq!(result.bytes.as_ref(), b"abc");
        assert!(result.buffer_capacity >= 8);
        let prefix = original.receipt();
        assert_eq!(prefix.source_bytes_read - baseline.source_bytes_read, 3);
        assert_eq!(prefix.backend_read_operations - baseline.backend_read_operations,
            if cfg!(windows) { 3 } else { 2 });
        assert_eq!(original.live_allocation_bytes(), live + result.buffer_capacity as u64);
        let copy = original.reserve_copy(result.bytes.len() as u64)?;
        let mut destination = [0; 3];
        destination.copy_from_slice(&result.bytes);
        copy.record(destination.len() as u64)?;
        assert_eq!(&destination, b"abc");
        assert_eq!(original.live_allocation_bytes(), live + result.buffer_capacity as u64);
        drop(result);
        assert_eq!(original.live_allocation_bytes(), live);
        assert_eq!(original.remaining()?.peak_allocation_bytes, remaining);
        assert_eq!(original.receipt().source_bytes_read, prefix.source_bytes_read);
        assert_eq!(original.receipt().backend_read_operations, prefix.backend_read_operations);
        assert_eq!(original.receipt().bytes_copied - baseline.bytes_copied, 3);
        Ok(())
    }

    #[tokio::test]
    async fn queued_capture_keeps_original_cancelled_probe_prefix()
        -> Result<(), Box<dyn std::error::Error>>
    {
        acyclic_native_runtime::initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().to_path_buf();
        let cancellation = CancellationToken::new();
        let mut original_budget = budget();
        original_budget.allocation_operations = 128;
        original_budget.peak_allocation_bytes = 1024 * 1024;
        original_budget.items_examined = 16 * 1024;
        original_budget.bytes_copied = 16 * 1024;
        original_budget.bytes_encoded = 16 * 1024;
        let original = capture_original_work(original_budget, &cancellation)
            .map_err(|failure| failure.error)?;
        let metadata_bytes = crate::NativeMaterializationWork::accounting_state_bytes();
        let path_bytes = path.capacity() as u64;
        original.with_original_leaf(|work| work.adopt(path_bytes))??;
        let caller_buffer = Vec::from(b"held".as_slice());
        let caller_bytes = caller_buffer.capacity() as u64;
        original.with_original_leaf(|work| work.adopt(caller_bytes))??;
        let worker = original.clone();
        let task_meter = std::sync::Arc::clone(original.meter());
        let (started, pending) = tokio::sync::oneshot::channel();
        let (resume, resumed) = std::sync::mpsc::channel();
        let task = acyclic_native_runtime::run_blocking_io(move || {
            let native = capture_native_original(&worker).map_err(|failure| failure.error)?;
            let mut scope = CaptureWorkScope::new(&native).map_err(|failure| failure.error)?;
            let root = HostRoot::open(&path, &mut capture_meter(&scope))
                .map_err(capture_demand_error)?;
            drop(path);
            worker.with_original_leaf(|work| work.release(path_bytes))
                .map_err(capture_demand_error)?.map_err(capture_demand_error)?;
            started.send(()).expect("caller must observe the pending worker");
            resumed.recv().expect("caller must resume the worker");
            let checked = worker.check();
            scope.release().map_err(|failure| failure.error)?;
            Ok::<_, CaptureError>((root, checked))
        }, Some((task_meter, false)))?;
        let task = tokio::spawn(task);
        pending.await?;
        assert_eq!(caller_buffer, b"held");
        assert_eq!(original.full_budget()?, original_budget);
        let prefix = original.counters()?;
        assert!(prefix.backend_read_operations > 0);
        assert!(original.with_original_leaf(|work| work.live_allocation_bytes())?
            >= metadata_bytes + caller_bytes);
        cancellation.cancel();
        resume.send(())?;
        let (root, checked) = task.await???;
        assert!(matches!(checked, Err(DemandError::Cancelled)));
        assert!(matches!(original.check(), Err(DemandError::Cancelled)));
        assert_eq!(original.full_budget()?, original_budget);
        let completed = original.finish_operation(original.check())
            .expect_err("original cancellation must remain authoritative");
        assert!(matches!(completed.error, DemandError::Cancelled));
        assert!(completed.work.backend_read_operations >= prefix.backend_read_operations);
        assert!(completed.work.allocation_operations >= prefix.allocation_operations);
        assert!(completed.work.peak_allocation_bytes >= prefix.peak_allocation_bytes);
        assert!(completed.work.backend_read_operations > 0);
        assert!(completed.work.allocation_operations > 0);
        assert_eq!(caller_buffer, b"held");
        // Joining the worker does not retire the original state or held file.
        drop(root);
        assert_eq!(original.with_original_leaf(|work| work.live_allocation_bytes())?,
            metadata_bytes + caller_bytes);
        drop(caller_buffer);
        original.with_original_leaf(|work| work.release(caller_bytes))??;
        assert_eq!(original.with_original_leaf(|work| work.live_allocation_bytes())?, metadata_bytes);
        Ok(())
    }

    #[tokio::test]
    async fn queued_capture_keeps_original_after_worker_fault()
        -> Result<(), Box<dyn std::error::Error>>
    {
        acyclic_native_runtime::initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().to_path_buf();
        let cancellation = CancellationToken::new();
        let mut original_budget = budget();
        original_budget.allocation_operations = 128;
        original_budget.peak_allocation_bytes = 1024 * 1024;
        original_budget.items_examined = 16 * 1024;
        original_budget.bytes_copied = 16 * 1024;
        original_budget.bytes_encoded = 16 * 1024;
        let original = capture_original_work(original_budget, &cancellation)
            .map_err(|failure| failure.error)?;
        let metadata_bytes = crate::NativeMaterializationWork::accounting_state_bytes();
        let path_bytes = path.capacity() as u64;
        original.with_original_leaf(|work| work.adopt(path_bytes))??;
        let caller_buffer = Vec::from(b"held".as_slice());
        let caller_bytes = caller_buffer.capacity() as u64;
        original.with_original_leaf(|work| work.adopt(caller_bytes))??;
        let worker = original.clone();
        let task_meter = std::sync::Arc::clone(original.meter());
        let (started, pending) = tokio::sync::oneshot::channel();
        let (resume, resumed) = std::sync::mpsc::channel();
        let task = acyclic_native_runtime::run_blocking_io(move || -> () {
            let native = capture_native_original(&worker)
                .expect("fault fixture must preserve its complete native resource");
            let scope = CaptureWorkScope::new(&native)
                .expect("fault fixture capture scope must be funded");
            let root = HostRoot::open(&path, &mut capture_meter(&scope))
                .expect("fault fixture root must open");
            drop(path);
            worker.with_original_leaf(|work| work.release(path_bytes))
                .expect("original work must remain available")
                .expect("transferred path claim must retire");
            started.send(()).expect("caller must observe the pending worker");
            resumed.recv().expect("caller must resume the worker");
            drop(root);
            panic!("native capture fault after the completed held-root probe");
        }, Some((task_meter, false)))?;
        let task = tokio::spawn(task);
        pending.await?;
        let prefix = original.counters()?;
        assert!(prefix.backend_read_operations > 0);
        assert!(prefix.allocation_operations > 0);
        assert_eq!(caller_buffer, b"held");
        assert!(original.with_original_leaf(|work| work.live_allocation_bytes())?
            >= metadata_bytes + caller_bytes);
        resume.send(())?;
        let fault = task.await.expect_err("observer must resume the worker panic");
        assert!(fault.is_panic());
        let payload = fault.into_panic();
        assert_eq!(payload.downcast_ref::<&'static str>(),
            Some(&"native capture fault after the completed held-root probe"));
        assert_eq!(original.full_budget()?, original_budget);
        assert!(!original.with_original_leaf(|work| work.cancellation().is_cancelled())?);
        original.check()?;
        assert_eq!(original.counters()?, prefix);
        assert_eq!(original.with_original_leaf(|work| work.live_allocation_bytes())?,
            metadata_bytes + caller_bytes);
        assert_eq!(caller_buffer, b"held");
        drop(caller_buffer);
        original.with_original_leaf(|work| work.release(caller_bytes))??;
        assert_eq!(original.with_original_leaf(|work| work.live_allocation_bytes())?, metadata_bytes);
        Ok(())
    }

    #[test]
    fn original_capture_failure_keeps_poisoned_prefix()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let cancellation = CancellationToken::new();
        let original = capture_original_work(budget(), &cancellation)
            .map_err(|failure| failure.error)?;
        original.with_original_leaf(|work| work.charge(WorkCounters {
            backend_read_operations: 1, ..WorkCounters::UNCHARGED
        }))??;
        let prefix = original.counters()?;
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = original.with_original_leaf(|_| {
                panic!("poison the actual original capture accounting state");
            });
        }));
        assert!(poisoned.is_err());
        let failure = capture_original_result(&original, Ok(OperationReceipt {
            value: (), work: WorkCounters::UNCHARGED,
        })).expect_err("a poisoned original must never become a new invocation");
        assert!(matches!(failure.error, CaptureError::OriginalWorkPoisoned));
        assert_eq!(*failure.work, prefix);
        assert!(matches!(original.check(), Err(DemandError::OriginalWorkPoisoned)));
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn existing_native_alias_borrows_shared_control_without_owned_duplication()
        -> Result<(), Box<dyn std::error::Error>>
    {
        acyclic_native_runtime::initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("alias");
        std::fs::write(&path, b"body")?;
        let held = acyclic_native_runtime::NativeFile::from_file(std::fs::File::open(&path)?, None).map_err(|(error, _)| error)?;
        let transferred = std::fs::File::open(&path)?;
        let cancellation = CancellationToken::new();
        let mut permitted = budget();
        permitted.peak_allocation_bytes = std::mem::size_of::<std::fs::File>() as u64;
        permitted.items_examined = 16 * 1024;
        let mut work = DemandWork::new(permitted, &cancellation);
        let meter = NativeDemandReadMeter(std::sync::Mutex::new(&mut work));
        let alias = acyclic_native_runtime::NativeFile::from_file(transferred, Some(acyclic_native_runtime::NativeFileMeter::Borrowed(&meter)))
            .map_err(|(error, file)| { drop(file); error })?;
        let owned_bytes = alias.logical_owned_bytes();
        assert_eq!(owned_bytes, std::mem::size_of::<std::fs::File>() as u64);
        {
            let work = meter.lock();
            assert_eq!(work.work.backend_read_operations, 1);
            assert_eq!(work.work.allocation_operations, 1);
            assert_eq!(work.live_allocation_bytes(), owned_bytes);
            assert_eq!(work.work.peak_allocation_bytes, owned_bytes);
        }
        drop(alias);
        meter.lock().release(owned_bytes)?;
        drop(meter);
        assert_eq!(work.live_allocation_bytes(), 0);
        drop(held);
        Ok(())
    }
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

fn open_source_root<W: CaptureNativeWork>(options: &CaptureOptions, meter: &mut W)
    -> Result<HostRoot, OperationFailure<CaptureError>> {
    let root = HostRoot::open(&options.source_root, meter)
        .map_err(|error| OperationFailure::new(capture_demand_error(error), meter.completed()))?;
    if root.identity() != options.expected_root_identity {
        return Err(OperationFailure::new(CaptureError::RootChanged, meter.completed()));
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
            let units = crate::kernel::types::utf16le_units(component.as_bytes())
                .ok_or(CaptureError::UnrepresentablePath)?
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
            let units = crate::kernel::types::utf16le_units(component.as_bytes())
                .ok_or(CaptureError::UnrepresentablePath)?
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

#[cfg(all(test, windows))]
mod windows_sparse_tests {
    use super::*;

    /// Settles `reported` for the file at `name` under `directory`.
    async fn settle(
        directory: &Path,
        name: &str,
        reported: Vec<HostDataRange>,
        maximum_ranges: u32,
    ) -> std::io::Result<Vec<(u64, u64)>> {
        let root = HostRoot::open(directory, &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let file = root.open_file_positional(Path::new(name))?;
        let metadata = test_host_stat(&root, Path::new(name)).map_err(std::io::Error::other)?;
        let native = std::sync::Arc::new(native_positional_file(file.into_std())?);
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| std::io::Error::other(error))?;
        let mut scope = CaptureWorkScope::new(&original)
            .map_err(|failure| std::io::Error::other(failure.error))?;
        capture_meter(&scope).leaf(|work| work.adopt(reported.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64))
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let (ranges, _) = settled_sparse_ranges(
            &native,
            &metadata,
            reported,
            maximum_ranges,
            WorkCounters::UNCHARGED,
            WorkBudget::UNBOUNDED,
            &cancellation,
            &scope,
        )
        .await
        .map_err(|failure| std::io::Error::other(failure.error.to_string()))?;
        let converted = ranges.into_iter()
            .map(|range| (range.offset, range.length)).collect();
        scope.release().map_err(|failure| std::io::Error::other(failure.error.to_string()))?;
        Ok(converted)
    }

    #[tokio::test]
    #[allow(
        unsafe_code,
        reason = "queries the file's allocated ranges through the Win32 API"
    )]
    async fn written_zeros_in_a_sparse_file_capture_as_holes() -> std::io::Result<()> {
        use std::io::{Seek as _, SeekFrom, Write as _};
        use std::os::windows::io::AsRawHandle as _;
        use windows::Win32::Foundation::HANDLE;
        use windows::Win32::System::IO::DeviceIoControl;
        use windows::Win32::System::Ioctl::FSCTL_SET_SPARSE;

        let directory = tempfile::tempdir()?;
        let length = 64 * 1024 + 100;
        let whole = || vec![HostDataRange { offset: 0, length }];
        let plain = std::fs::File::create(directory.path().join("plain.bin"))?;
        plain.set_len(length)?;
        drop(plain);
        // Not marked sparse: its report stands.
        assert_eq!(
            settle(directory.path(), "plain.bin", whole(), 16).await?,
            [(0, length)]
        );

        let mut file = std::fs::File::create(directory.path().join("sparse.bin"))?;
        // SAFETY: no buffers are passed and the handle stays live.
        unsafe {
            DeviceIoControl(
                HANDLE(file.as_raw_handle()),
                FSCTL_SET_SPARSE,
                None,
                0,
                None,
                0,
                None,
                None,
            )
            .map_err(std::io::Error::other)?;
        }
        file.set_len(length)?;
        file.write_all(&[1; 10])?;
        // Zeros written, so allocated, around the data block at 40 KiB.
        file.seek(SeekFrom::Start(4096))?;
        file.write_all(&[0; 36 * 1024])?;
        file.write_all(&[2; 5])?;
        file.seek(SeekFrom::Start(length - 3))?;
        file.write_all(&[3; 3])?;
        drop(file);
        assert_eq!(
            settle(directory.path(), "sparse.bin", whole(), 16).await?,
            [(0, 4096), (40 * 1024, 4096), (64 * 1024, 100)]
        );
        // Settling it would take more ranges than admitted: the report stands.
        assert_eq!(
            settle(directory.path(), "sparse.bin", whole(), 2).await?,
            [(0, length)]
        );

        // Every byte read is charged, and the scan stops at the budget or
        // once cancelled.
        let root = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        let file = root.open_file_positional(Path::new("sparse.bin"))?;
        let metadata = test_host_stat(&root, Path::new("sparse.bin")).map_err(std::io::Error::other)?;
        let native = std::sync::Arc::new(native_positional_file(file.into_std())?);
        let scan = |budget: WorkBudget, cancellation: CancellationToken| {
            let (native, metadata) = (std::sync::Arc::clone(&native), metadata.clone());
            async move {
                let reported = whole();
                let original = crate::NativeMaterializationWork::new(budget, &cancellation)
                    .map_err(|(error, work)| OperationFailure::new(CaptureError::NativeKernel(error), work))?;
                let mut scope = CaptureWorkScope::new(&original)?;
                {
                    let mut work = capture_meter(&scope);
                    work.leaf(|work| work.adopt(reported.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64))
                        .map_err(|error| OperationFailure::new(capture_demand_error(error), work.completed()))?;
                }
                let result = settled_sparse_ranges(
                    &native,
                    &metadata,
                    reported,
                    16,
                    WorkCounters::UNCHARGED,
                    budget,
                    &cancellation,
                    &scope,
                )
                .await.map(|(ranges, _)| drop(ranges));
                scope.release()?;
                let work = original.receipt();
                result.map(|()| ((), work))
                    .map_err(|failure| OperationFailure::new(failure.error, work))
            }
        };
        let (_, work) = scan(WorkBudget::UNBOUNDED, CancellationToken::new())
            .await
            .map_err(|failure| std::io::Error::other(failure.error.to_string()))?;
        assert_eq!(work.source_bytes_read, length);
        let short = WorkBudget {
            source_bytes_read: 4096,
            ..WorkBudget::UNBOUNDED
        };
        assert!(matches!(
            scan(short, CancellationToken::new()).await,
            Err(failure) if matches!(failure.error, CaptureError::Work(_))
        ));
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(scan(WorkBudget::UNBOUNDED, cancelled).await.is_err());
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

fn host_kind(metadata: &HostStat) -> Result<FileKind, CaptureError> {
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
    Err(CaptureError::UnsupportedKind)
}

#[cfg(unix)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "keeps the signature of the platforms whose device split can fail"
)]
fn host_device_identity(metadata: &HostStat) -> Result<(u32, u32), CaptureError> {
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
fn host_device_identity(_: &HostStat) -> Result<(u32, u32), CaptureError> {
    Err(CaptureError::UnsupportedKind)
}

fn capture_link_target(target: Vec<u8>, meter: &mut impl CaptureNativeWork) -> Result<bytes::Bytes, CaptureError> {
    let admitted = (|| {
        meter.check().map_err(capture_demand_error)?;
        if target.len() != target.capacity() {
            meter.leaf(|work| work.claim_allocation(0, 1)).map_err(capture_demand_error)?;
        }
        Ok(())
    })();
    if let Err(error) = admitted {
        let bytes = target.capacity() as u64;
        drop(target);
        let _ = meter.release(bytes);
        return Err(error);
    }
    Ok(bytes::Bytes::from(target))
}

fn read_link_bytes<W: CaptureNativeWork>(root: &HostRoot, path: &CaptureHostPath, meter: &mut W) -> Result<Vec<u8>, CaptureError> {
    let maximum = meter.leaf(|meter| meter.remaining()).map_err(capture_demand_error)?.source_bytes_read;
    #[cfg(unix)]
    {
        // The returned byte buffer keeps the driver's existing capture claim.
        let (bytes, _allocation) = root.read_link(path, maximum, meter).map_err(capture_demand_error)?;
        Ok(bytes)
    }
    #[cfg(windows)]
    {
        let (units, allocation) = root.read_link_wide_with_work(path, maximum, meter).map_err(capture_demand_error)?;
        let bytes = (|| {
            let length = units.len().checked_mul(2).ok_or(CaptureError::Work(WorkError::Overflow))?;
            let mut bytes = capture_vec(length, meter)?;
            for unit in &units {
                capture_charge(meter, WorkCounters { bytes_copied: 2, ..WorkCounters::UNCHARGED })?;
                bytes.extend_from_slice(&unit.to_le_bytes());
            }
            Ok::<_, CaptureError>(bytes)
        })()?;
        drop(units);
        meter.release(allocation).map_err(capture_demand_error)?;
        Ok(bytes)
    }
}

/// Unix symbolic-link metadata is not restored by the current native view.
/// Windows link-leaf attributes and timestamps are applied by `HostRoot` and
/// must remain in the canonical capture.
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

fn capture_metadata(metadata: &HostStat) -> FileMetadata {
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

#[allow(
    clippy::needless_pass_by_value,
    reason = "consumes the io::Result that the metadata accessor returns"
)]
pub(crate) fn system_time(value: std::io::Result<cap_std::time::SystemTime>) -> MetadataField<i64> {
    let Ok(value) = value else {
        return MetadataField::Unavailable;
    };
    let nanos = match value.into_std().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => {
            i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos())
        }
        Err(error) => {
            let duration = error.duration();
            -(i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos()))
        }
    };
    i64::try_from(nanos).map_or(MetadataField::Unavailable, MetadataField::Value)
}

#[cfg(unix)]
fn populate_platform_metadata(metadata: &HostStat, result: &mut FileMetadata) {
    result.posix_mode = MetadataField::Value(metadata.mode());
    result.posix_uid = MetadataField::Value(metadata.uid());
    result.posix_gid = MetadataField::Value(metadata.gid());
    let nanos = i128::from(metadata.ctime()) * 1_000_000_000 + i128::from(metadata.ctime_nsec());
    result.changed_ns =
        i64::try_from(nanos).map_or(MetadataField::Unavailable, MetadataField::Value);
}

#[cfg(windows)]
fn populate_platform_metadata(metadata: &HostStat, result: &mut FileMetadata) {
    result.windows_attributes = MetadataField::Value(metadata.file_attributes());
}



#[cfg(all(test, unix, feature = "native-mount"))]
mod root_capture_regression {
    use super::*;

    #[tokio::test]
    async fn root_capture_publishes_metadata_and_children() -> Result<(), Box<dyn std::error::Error>>
    {
        use std::os::unix::fs::PermissionsExt;
        let source = tempfile::tempdir()?;
        std::fs::set_permissions(source.path(), std::fs::Permissions::from_mode(0o750))?;
        std::fs::write(source.path().join("binary"), [0, 255, 10])?;
        let workspace = crate::Fs::memory()
            .create_workspace("root-capture-regression", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let mut checkout = workspace.checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let root = NamespacePath::new(Vec::new(), checkout.volume_config().limits)?;
        let cancellation = CancellationToken::new();
        capture_subtree(
            &mut checkout,
            root.clone(),
            &CaptureOptions {
                source_root: source.path().to_path_buf(),
                expected_root_identity: capture_root_identity(source.path())?,
                maximum_paths: 100,
                maximum_extent_spans: 100,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        crate::seal_checkout(
            &mut checkout,
            crate::OperationId::new(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let output = tempfile::tempdir()?;
        let mut reader = workspace.checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::read_only_pinned(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        crate::materialize_checkout(
            &mut reader,
            &crate::MaterializeOptions {
                destination: output.path().to_path_buf(),
                expected_root_identity: None,
                maximum_directory_entries: 2,
                maximum_extent_spans: 100,
                transfer_bytes: 1024,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        assert_eq!(std::fs::read(output.path().join("binary"))?, [0, 255, 10]);
        assert_eq!(
            std::fs::metadata(output.path())?.permissions().mode() & 0o777,
            0o750
        );
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod capture_span_tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, PoisonError};
    use tracing::Instrument as _;
    use tracing::field::{Field, Visit};
    use tracing_subscriber::layer::SubscriberExt as _;

    #[derive(Default, Debug)]
    struct Captured {
        spans: Vec<(&'static str, Vec<(&'static str, String)>)>,
        indexes: HashMap<u64, usize>,
        events: Vec<(&'static str, Vec<(&'static str, String)>)>,
        closed: Vec<usize>,
    }

    struct Capture(Arc<Mutex<Captured>>);
    struct Fields<'a>(&'a mut Vec<(&'static str, String)>);

    impl Visit for Fields<'_> {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.push((field.name(), value.to_owned()));
        }
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0.push((field.name(), format!("{value:?}")));
        }
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
        fn on_new_span(
            &self,
            attributes: &tracing::span::Attributes<'_>,
            id: &tracing::span::Id,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut fields = Vec::new();
            attributes.record(&mut Fields(&mut fields));
            let mut captured = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            let index = captured.spans.len();
            captured.indexes.insert(id.into_u64(), index);
            captured.spans.push((attributes.metadata().name(), fields));
        }
        fn on_record(
            &self,
            id: &tracing::span::Id,
            values: &tracing::span::Record<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut captured = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(index) = captured.indexes.get(&id.into_u64()).copied()
                && let Some((_, fields)) = captured.spans.get_mut(index)
            {
                values.record(&mut Fields(fields));
            }
        }
        fn on_close(&self, id: tracing::span::Id, _: tracing_subscriber::layer::Context<'_, S>) {
            let mut captured = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(index) = captured.indexes.remove(&id.into_u64()) {
                captured.closed.push(index);
            }
        }

        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut fields = Vec::new();
            event.record(&mut Fields(&mut fields));
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .events
                .push((event.metadata().target(), fields));
        }
    }

    #[tokio::test]
    async fn direct_capture_summaries_preserve_parent_and_hide_host_data()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("private-name"), b"private-body")?;
        let workspace = crate::Fs::memory()
            .create_workspace("capture-spans", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let mut checkout = workspace.checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let limits = crate::model::VolumeLimits::default();
        let path = NamespacePath::from_portable(
            &crate::path::PortablePath::parse("/private-name", limits)?,
            limits,
        )?;
        let options = CaptureOptions {
            source_root: directory.path().to_path_buf(),
            expected_root_identity: capture_root_identity(directory.path())?,
            maximum_paths: 8,
            maximum_extent_spans: 8,
        };
        // Two live dispatchers avoid thread-local callsite interest races.
        let _second = tracing::Dispatch::new(tracing_subscriber::Registry::default());
        let captured = Arc::new(Mutex::new(Captured::default()));
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(Capture(Arc::clone(&captured))),
        );
        let parent = tracing::info_span!("capture-parent", outcome = "parent");
        let receipt = async {
            assert!(capture_root_identity(&directory.path().join("private-name")).is_err());
            let receipt = capture_paths(
                &mut checkout,
                std::slice::from_ref(&path),
                &options,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?;
            assert!(receipt.work.items_examined > 0);
            let invalid = CaptureOptions {
                maximum_paths: 0,
                ..options.clone()
            };
            assert!(
                capture_paths_with_policy(
                    &mut checkout,
                    std::slice::from_ref(&path),
                    &invalid,
                    &CapturePolicy::allow_all(),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .is_err()
            );
            let cancelled = CancellationToken::new();
            cancelled.cancel();
            assert!(
                capture_paths_with_policy(
                    &mut checkout,
                    std::slice::from_ref(&path),
                    &options,
                    &CapturePolicy::allow_all(),
                    WorkBudget::UNBOUNDED,
                    &cancelled,
                )
                .await
                .is_err()
            );
            Ok::<_, Box<dyn std::error::Error>>(receipt)
        }
        .instrument(parent)
        .await?;
        let captured = captured.lock().map_err(|_| "capture poisoned")?;
        let fields = |name| {
            captured
                .spans
                .iter()
                .filter(|(found, _)| *found == name)
                .map(|(_, fields)| fields)
                .collect::<Vec<_>>()
        };
        let has = |fields: &[(&str, String)], key: &str, value: &str| {
            fields.contains(&(key, value.to_owned()))
        };
        assert_eq!(fields("capture-parent").len(), 1);
        assert_eq!(
            fields("capture-parent")[0],
            &vec![("outcome", "parent".to_owned())]
        );
        let paths = fields("acyclic.fs.native_capture.paths_with_policy");
        assert_eq!(paths.len(), 3);
        assert_eq!(
            paths
                .iter()
                .filter(|fields| has(fields, "outcome", "ok"))
                .count(),
            1
        );
        assert_eq!(
            paths
                .iter()
                .filter(|fields| has(fields, "outcome", "err"))
                .count(),
            2
        );
        assert!(
            paths
                .iter()
                .any(|fields| has(fields, "error.kind", "InvalidOptions"))
        );
        assert!(paths.iter().any(|fields| has(
            fields,
            "work.items",
            &receipt.work.items_examined.to_string()
        )));
        assert!(
            fields("acyclic.fs.native_capture.root_identity")
                .iter()
                .any(|fields| has(fields, "error.kind", "InvalidOptions"))
        );
        for (index, (name, _)) in captured.spans.iter().enumerate() {
            if name.starts_with("acyclic.fs.native_capture.") {
                assert!(
                    captured.closed.contains(&index),
                    "capture outlived completed work"
                );
            }
        }
        assert!(captured.events.iter().all(|(target, fields)| {
            *target != "acyclic.work"
                || fields
                    .iter()
                    .all(|(key, value)| *key != "op" || !value.starts_with("capture_"))
        }));
        let rendered = format!("{captured:?}");
        assert!(!rendered.contains("private-name"));
        assert!(!rendered.contains("private-body"));
        assert!(!rendered.contains(&directory.path().display().to_string()));
        Ok(())
    }
    #[tokio::test]
    async fn filtered_capture_spans_do_not_record_outcomes_on_the_caller()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let workspace = crate::Fs::memory()
            .create_workspace("filtered-capture", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let mut checkout = workspace.checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let options = CaptureOptions {
            source_root: directory.path().to_path_buf(),
            expected_root_identity: capture_root_identity(directory.path())?,
            maximum_paths: 0,
            maximum_extent_spans: 0,
        };
        let _second = tracing::Dispatch::new(tracing_subscriber::Registry::default());
        let captured = Arc::new(Mutex::new(Captured::default()));
        let capture = Capture(Arc::clone(&captured));
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(tracing_subscriber::Layer::with_filter(
                capture,
                tracing_subscriber::filter::filter_fn(|metadata| {
                    !metadata.name().starts_with("acyclic.fs.native_capture.")
                }),
            )),
        );
        let parent = tracing::info_span!("filtered-parent", outcome = "parent", work.items = 7);
        let token = CancellationToken::new();
        async {
            assert!(capture_root_identity(&directory.path().join("missing")).is_err());
            // Root identity success must not overwrite the caller either.
            assert!(capture_root_identity(directory.path()).is_ok());
            assert!(
                capture_paths_with_policy(
                    &mut checkout,
                    &[],
                    &options,
                    &CapturePolicy::allow_all(),
                    WorkBudget::UNBOUNDED,
                    &token,
                )
                .await
                .is_err()
            );
            assert!(
                capture_subtrees_with_policy(
                    &mut checkout,
                    &[],
                    &options,
                    &CapturePolicy::allow_all(),
                    WorkBudget::UNBOUNDED,
                    &token,
                )
                .await
                .is_err()
            );
            assert!(
                capture_baseline_with_policy(
                    &mut checkout,
                    &options,
                    &CapturePolicy::allow_all(),
                    WorkBudget::UNBOUNDED,
                    &token,
                )
                .await
                .is_err()
            );
            assert!(
                capture_watch_batch_with_policy(
                    &mut checkout,
                    WatchBatch::Changes {
                        original: Default::default(),
                        epoch: WatchEpoch::from_u64(1),
                        first_sequence: WatchSequence::from_u64(1),
                        next_sequence: WatchSequence::from_u64(1),
                        changes: Vec::new(),
                    },
                    &options,
                    &CapturePolicy::allow_all(),
                    WorkBudget::UNBOUNDED,
                    &token,
                )
                .await
                .is_err()
            );
        }
        .instrument(parent)
        .await;
        let captured = captured.lock().map_err(|_| "capture poisoned")?;
        assert_eq!(captured.spans.len(), 1);
        assert_eq!(captured.spans[0].0, "filtered-parent");
        assert_eq!(
            captured.spans[0].1,
            vec![
                ("outcome", "parent".to_owned()),
                ("work.items", "7".to_owned()),
            ]
        );
        assert!(captured.events.is_empty());
        Ok(())
    }
}
