//! Bounded host-state capture into one atomic sparse checkout transaction.
//!
//! Capture poll frames retain their short-frame heap boundaries. Each actual
//! future layout is admitted before construction and stays in the invocation's
//! allocation ledger until the completed box is dropped.

use crate::kernel::{
    FileKind, FileMetadata, FileRecord, LogicalName, MetadataField, NameEncoding, NamespacePath,
};
use crate::model::FilesystemProfile;
use crate::native_host::{HostDataRange, HostRoot, HostStat, allocated_data_ranges_measured};
use crate::demand::{
    DemandError, DemandWork, NativeDemandReadMeter, NativeDemandReadWork,
    native_demand_error, native_demand_read_error,
};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, CancellationToken, Checkout,
    ContentStager, NativeRootIdentity, OperationFailure, OperationReceipt, StagedContent,
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
        HostRoot::open(path)
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
}

impl NativeViewBaseline {
    #[cfg(any(feature = "native-mount", all(test, unix)))]
    pub(crate) fn new(root_identity: NativeRootIdentity) -> Self {
        Self {
            root_identity,
            read_only: false,
        }
    }

    #[cfg(feature = "native-mount")]
    pub(crate) fn read_only(root_identity: NativeRootIdentity) -> Self {
        Self {
            root_identity,
            read_only: true,
        }
    }
    /// The witness contains only inline identity and policy scalars.
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
        let root = HostRoot::open(directory.path())?;
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
            .create_workspace("watch-ancestor")
            .await?;
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await?;
        let changed = path("/sub/changed.txt")?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
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
            .create_workspace("watch-directory")
            .await?;
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await?;
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
            .create_workspace("watch-rename-parent")
            .await?;
        workspace.write_text("/old.txt", "old").await?;
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await?;
        std::fs::rename(
            temporary.path().join("old.txt"),
            temporary.path().join("sub/new.txt"),
        )?;
        let old = path("/old.txt")?;
        let new = path("/sub/new.txt")?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
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
            .create_workspace("watch-renamed-directory")
            .await?;
        let mut transaction = workspace
            .begin_transaction(crate::IdempotencyKey::new())
            .await?;
        transaction.create_dir_all("/old").await?;
        transaction.write_text("/old/file.txt", "old").await?;
        transaction.commit().await?;
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await?;
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
            .create_workspace("watch-expansion")
            .await?;
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await?;
        capture_watch_batch_with_policy(
            &mut checkout,
            WatchBatch::Changes {
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
            .create_workspace("watch-stale-ancestor")
            .await?;
        workspace.write_text("/sub", "old").await?;
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await?;
        let child = path("/sub/new.txt")?;
        capture_watch_batch_with_policy(
            &mut checkout,
            WatchBatch::Changes {
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
            .create_workspace("watch-rename-chain")
            .await?;
        workspace.write_text("/old.txt", "old").await?;
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await?;
        capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
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
        validate_path_count(paths, options).map_err(OperationFailure::before_work)?;
        let mut capture_meter = DemandWork::new(budget, cancellation);
        let meter = &mut capture_meter;
        let source_root = open_source_root(options, meter)?;
        capture_paths_from_root(checkout,
        paths,
        policy,
        options.maximum_extent_spans,
        &source_root,
        budget,
        cancellation, meter)
        .await
    })
    .await;
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
    let mut meter = DemandWork::new(budget, cancellation);
    capture_paths_batched_with_work(
        checkout, paths, options, batch_size, cancellation, baseline, &mut meter,
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
    meter: &mut DemandWork,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    validate_path_count(paths, options).map_err(|error| OperationFailure::new(error, meter.work))?;
    if batch_size == 0 || paths.len() > MAX_NATIVE_EXACT_CAPTURE_PATHS {
        return Err(OperationFailure::new(CaptureError::InvalidOptions, meter.work));
    }
    cancellation.check()
        .map_err(|_| OperationFailure::new(CaptureError::Cancelled, meter.work))?;
    meter.check()
        .map_err(|error| OperationFailure::new(capture_demand_error(error), meter.work))?;
    if paths.is_empty() {
        let work = meter.work;
        return Ok(OperationReceipt { value: CaptureReceipt { work, ..CaptureReceipt::default() }, work });
    }
    let budget = meter.budget;
    let (mut future, heap_bytes) = crate::heap_future::in_heap_admitted(
        meter,
        |meter, bytes| capture_allocate(meter, bytes)
            .map_err(|error| OperationFailure::new(error, meter.work)),
        move |meter| async move {
        let result = async {
    let source_root = open_source_root(options, meter)?;
    let mut unique = clone_capture_paths(paths, None, meter)
        .map_err(|error| OperationFailure::new(error, meter.work))?;
    capture_sort_by(&mut unique, Ord::cmp, meter)
        .map_err(|error| OperationFailure::new(error, meter.work))?;
    if unique.windows(2).any(|pair| matches!(pair, [left, right] if left == right)) {
        return Err(OperationFailure::new(CaptureError::InvalidOptions, meter.work));
    }
    let mut receipt = CaptureReceipt { work: meter.work, ..CaptureReceipt::default() };
    let mut candidate = capture_candidate(checkout, &mut receipt.work, budget, cancellation, meter)?;
    let mut states = capture_vec(unique.len(), meter)
        .map_err(|error| OperationFailure::new(error, meter.work))?;
    let observed = observe_unique_paths(unique, &source_root, budget, cancellation, meter)?;
    receipt.work = observed.work;
    let mut observations = observed.value.into_iter();
    loop {
        let count = observations.len().min(batch_size);
        let mut page = capture_vec(count, meter).map_err(|error| OperationFailure::new(error, meter.work))?;
        page.extend(observations.by_ref().take(count));
        receipt.work = meter.work;
        if page.is_empty() {
            break;
        }
        let mut paths = capture_vec(page.len(), meter).map_err(|error| OperationFailure::new(error, meter.work))?;
        for (_, path) in &page {
            paths.push(clone_capture_path(path, meter).map_err(|error| OperationFailure::new(error, meter.work))?);
        }
        receipt.work = meter.work;
        let remaining = capture_remaining(receipt.work, budget, meter)
            .map_err(|error| OperationFailure::new(CaptureError::Work(error), receipt.work))?;
        let lookup = candidate
            .lookup_batch_no_follow(&paths, remaining, cancellation)
            .await
            .map_err(|failure| map_engine_failure(failure, receipt.work, meter))?;
        receipt.work = merge_capture_work(receipt.work, lookup.work, meter)?;
        let lookup_bytes = lookup.value.retained_allocation_bytes;
        let page_bytes = page.capacity() as u64 * std::mem::size_of::<ObservedPath>() as u64;
        capture_step(&mut receipt.work, meter, |meter| meter.adopt(lookup_bytes).map_err(capture_demand_error))?;
        states.extend(
            page.into_iter()
                .zip(lookup.value.entries)
                .map(|((observation, path), entry)| (observation, path, entry.record)),
        );
        capture_step(&mut receipt.work, meter, |meter| {
            meter.release(lookup_bytes + page_bytes).map_err(capture_demand_error)?;
            release_capture_paths(paths, meter)
        })?;
    }
    capture_step(&mut receipt.work, meter, |meter| sort_capture_states(&mut states, meter))?;
    let mut host_links = CaptureMap::new();
    let states_bytes = states.capacity() as u64 * std::mem::size_of::<CapturePathState>() as u64;
    let mut states = states.into_iter();
    loop {
        let count = states.len().min(batch_size);
        let mut batch = capture_vec(count, meter).map_err(|error| OperationFailure::new(error, meter.work))?;
        batch.extend(states.by_ref().take(count));
        receipt.work = meter.work;
        if batch.is_empty() {
            break;
        }
        let captured = capture_ranked_paths_from_root(&mut candidate,
        batch,
        &mut host_links,
        options.maximum_extent_spans,
        &source_root,
        budget,
        cancellation,
        baseline, meter)
        .await?;
        for source in host_links.values_mut() {
            if let HostLinkSource::Pending(path) = source {
                let ready = std::mem::replace(path, NamespacePath::new(Vec::new(), checkout.volume_config().limits)
                    .map_err(|error| OperationFailure::new(CaptureError::Engine(error.to_string()), receipt.work))?);
                *source = HostLinkSource::Ready(ready);
            }
        }
        receipt.examined_paths = receipt
            .examined_paths
            .checked_add(captured.value.examined_paths)
            .ok_or_else(|| {
                OperationFailure::new(CaptureError::Work(WorkError::Overflow), receipt.work)
            })?;
        receipt.changed_paths = receipt
            .changed_paths
            .checked_add(captured.value.changed_paths)
            .ok_or_else(|| {
                OperationFailure::new(CaptureError::Work(WorkError::Overflow), receipt.work)
            })?;
        receipt.staged_file_bytes = receipt
            .staged_file_bytes
            .checked_add(captured.value.staged_file_bytes)
            .ok_or_else(|| {
                OperationFailure::new(CaptureError::Work(WorkError::Overflow), receipt.work)
            })?;
        receipt.work = captured.work;
    }
    drop(states);
    capture_step(&mut receipt.work, meter, |meter| {
        meter.release(states_bytes).map_err(capture_demand_error)?;
        release_host_links(host_links, meter)
    })?;
    *checkout = candidate;
    Ok(OperationReceipt {
        value: receipt,
        work: receipt.work,
    })
        }.await;
        (result, meter)
        },
    )?;
    let (result, meter) = future.as_mut().await;
    drop(future);
    meter.release(heap_bytes)
        .map_err(|error| OperationFailure::new(capture_demand_error(error), meter.work))?;
    result
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

struct CaptureQueuedWork<'a> {
    original: &'a mut DemandWork,
    shared: Option<std::sync::Arc<NativeDemandReadMeter>>,
    metadata_admitted: bool,
}

impl<'a> CaptureQueuedWork<'a> {
    fn new(original: &'a mut DemandWork) -> Result<Self, OperationFailure<CaptureError>> {
        let mut queued = Self { original, shared: None, metadata_admitted: false };
        capture_allocate(queued.original, std::mem::size_of::<NativeDemandReadMeter>() as u64)
            .map_err(|error| OperationFailure::new(error, queued.original.work))?;
        queued.metadata_admitted = true;
        let mut shared = std::sync::Arc::new(NativeDemandReadMeter(std::sync::Mutex::new(
            queued.original.native_queue_shell(),
        )));
        let target = std::sync::Arc::get_mut(&mut shared)
            .expect("new capture work has no aliases")
            .0.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner);
        *target = queued.original.take_for_native_queue();
        queued.shared = Some(shared);
        Ok(queued)
    }

    fn shared(&self) -> &std::sync::Arc<NativeDemandReadMeter> {
        self.shared.as_ref().expect("capture work is transferred")
    }

    async fn retire(mut self) -> Result<(), CaptureError> {
        while std::sync::Arc::strong_count(self.shared()) != 1 {
            tokio::task::yield_now().await;
        }
        self.restore()
    }

    fn restore(&mut self) -> Result<(), CaptureError> {
        let Some(shared) = self.shared.take() else {
            return self.release_metadata();
        };
        // Accepted native work must finish before the original invocation can
        // resume, including unwinding or dropping its asynchronous caller.
        let mut shared = shared;
        let transferred = loop {
            match std::sync::Arc::try_unwrap(shared) {
                Ok(meter) => break meter.0.into_inner()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
                Err(retained) => {
                    shared = retained;
                    std::thread::yield_now();
                }
            }
        };
        *self.original = transferred;
        self.release_metadata()
    }

    fn release_metadata(&mut self) -> Result<(), CaptureError> {
        if !std::mem::replace(&mut self.metadata_admitted, false) {
            return Ok(());
        }
        self.original.release(std::mem::size_of::<NativeDemandReadMeter>() as u64)
            .map_err(capture_demand_error)
    }
}

impl Drop for CaptureQueuedWork<'_> {
    fn drop(&mut self) {
        let _ = self.restore();
    }
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
    let mut meter = DemandWork::new(budget, cancellation);
    capture_subtrees_with_work(checkout, roots, options, policy, cancellation, baseline, &mut meter).await
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
    meter: &mut DemandWork,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    if options.maximum_paths == 0 || options.maximum_extent_spans == 0 {
        return Err(OperationFailure::new(CaptureError::InvalidOptions, meter.work));
    }
    cancellation.check()
        .map_err(|_| OperationFailure::new(CaptureError::Cancelled, meter.work))?;
    meter.check()
        .map_err(|error| OperationFailure::new(capture_demand_error(error), meter.work))?;
    if roots.is_empty() {
        let work = meter.work;
        return Ok(OperationReceipt { value: CaptureReceipt { work, ..CaptureReceipt::default() }, work });
    }
    let budget = meter.budget;
    let (mut future, heap_bytes) = crate::heap_future::in_heap_admitted(
        meter,
        |meter, bytes| capture_allocate(meter, bytes)
            .map_err(|error| OperationFailure::new(error, meter.work)),
        move |meter| async move {
            let result = async {
    let capture_meter = &mut *meter;
    let canonical = canonical_subtree_roots_measured(roots, policy, capture_meter)
        .map_err(|error| OperationFailure::new(error, capture_meter.work))?;
    if canonical.is_empty() {
        let work = capture_meter.work;
        return Ok(OperationReceipt { value: CaptureReceipt { work, ..CaptureReceipt::default() }, work });
    }
    let limits = checkout.volume_config().limits;
    let maximum = usize::try_from(options.maximum_paths)
        .map_err(|_| OperationFailure::new(CaptureError::InvalidOptions, capture_meter.work))?;
    let profile = checkout.volume_config().profile;
    let mut paths = clone_capture_paths(&canonical, None, capture_meter)
        .map_err(|error| OperationFailure::new(error, capture_meter.work))?;
    let host_roots = clone_capture_paths(&canonical, None, capture_meter)
        .map_err(|error| OperationFailure::new(error, capture_meter.work))?;
    let host_policy = CapturePolicy {
        excluded_prefixes: clone_capture_paths(&policy.excluded_prefixes, None, capture_meter)
            .map_err(|error| OperationFailure::new(error, capture_meter.work))?,
    };
    let source_path_bytes = options.source_root.as_os_str().as_encoded_bytes().len() as u64;
    let source_path = crate::native_host::clone_path_measured(&options.source_root, capture_meter)
        .map_err(|error| OperationFailure::new(capture_demand_error(error), capture_meter.work))?;
    let expected_identity = options.expected_root_identity;
    let host_cancellation = cancellation.clone();
    let queued = CaptureQueuedWork::new(capture_meter)?;
    let worker = std::sync::Arc::clone(queued.shared());
    let task_meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter> = queued.shared().clone();
    let task = acyclic_native_runtime::run_blocking_io_measured(move || {
        let mut guard = worker.lock();
        let meter = &mut *guard;
        let source_root = HostRoot::open_measured(&source_path, meter)
            .map_err(|error| OperationFailure::new(capture_demand_error(error), meter.work))?;
        if source_root.identity() != expected_identity {
            return Err(OperationFailure::new(CaptureError::RootChanged, meter.work));
        }
        let mut observed = CaptureMap::new();
        let mut work = meter.work;
        collect_host_subtree_roots(&source_root, profile, limits, maximum, &host_roots,
            &host_policy, &mut observed, &mut work, budget, &host_cancellation, meter)?;
        capture_step(&mut work, meter, |meter| {
            release_capture_paths(host_roots, meter)?;
            release_capture_paths(host_policy.excluded_prefixes, meter)?;
            drop(source_path);
            meter.release(source_path_bytes).map_err(capture_demand_error)
        })?;
        Ok((source_root, observed))
    }, task_meter);
    let result = match task {
        Ok(task) => task.await.map_err(native_demand_error),
        Err(error) => Err(native_demand_error(error)),
    };
    queued.retire().await
        .map_err(|error| OperationFailure::new(error, capture_meter.work))?;
    let (source_root, observed) = result
        .map_err(|error| OperationFailure::new(capture_demand_error(error), capture_meter.work))??;
    let meter = capture_meter;
    let mut work = meter.work;
    let remaining = capture_remaining(work, budget, meter)
        .map_err(|error| OperationFailure::new(CaptureError::Work(error), work))?;
    let current = checkout.lookup_batch_no_follow(&canonical, remaining, cancellation)
        .await.map_err(|failure| map_engine_failure(failure, work, meter))?;
    work = merge_capture_work(work, current.work, meter)?;
    let lookup_bytes = current.value.retained_allocation_bytes;
    let canonical_bytes = canonical.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    capture_step(&mut work, meter, |meter| meter.adopt(lookup_bytes).map_err(capture_demand_error))?;
    for (root, entry) in canonical.into_iter().zip(current.value.entries) {
        if entry
            .record
            .is_some_and(|record| record.kind == FileKind::Directory)
        {
            collect_checkout_subtree_paths(checkout,
            limits,
            root,
            policy,
            maximum,
            &mut paths,
            &mut work,
            budget,
            cancellation, meter)
            .await?;
        } else {
            capture_step(&mut work, meter, |meter| meter.release(namespace_owned_bytes(&root)?).map_err(capture_demand_error))?;
        }
    }
    capture_step(&mut work, meter, |meter| meter.release(lookup_bytes + canonical_bytes).map_err(capture_demand_error))?;
    let ordered = order_host_checkout_union(observed, paths, maximum, meter)
        .map_err(|error| OperationFailure::new(error, meter.work))?;
    let mut captured = capture_observed_paths_from_root(checkout,
    ordered,
    options.maximum_extent_spans,
    &source_root,
    budget,
    cancellation,
    baseline, meter)
    .await?;
    captured.value.work = captured.work;
    Ok(captured)
            }.await;
            (result, meter)
        },
    )?;
    let (result, meter) = future.as_mut().await;
    drop(future);
    meter.release(heap_bytes)
        .map_err(|error| OperationFailure::new(capture_demand_error(error), meter.work))?;
    result
}

fn capture_demand_error(error: DemandError) -> CaptureError {
    match error {
        DemandError::Work(error) => CaptureError::Work(error),
        DemandError::Io(error) => CaptureError::Io(error),
        DemandError::StaleVersion => CaptureError::StaleVersion,
        DemandError::Cancelled => CaptureError::Cancelled,
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
    fn position_measured(&self, key: &K, meter: &mut DemandWork) -> Result<Result<usize, usize>, CaptureError> {
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
    fn contains_key_measured(&self, key: &K, meter: &mut DemandWork) -> Result<bool, CaptureError> {
        Ok(self.position_measured(key, meter)?.is_ok())
    }
    fn get_measured(&self, key: &K, meter: &mut DemandWork) -> Result<Option<&V>, CaptureError> {
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
    fn insert_at(&mut self, index: usize, key: K, value: V, meter: &mut DemandWork) -> Result<(), CaptureError> {
        let copied = (self.entries.len() - index).checked_mul(std::mem::size_of::<(K, V)>())
            .ok_or(CaptureError::Work(WorkError::Overflow))? as u64;
        let delta = WorkCounters { bytes_copied: copied, ..WorkCounters::UNCHARGED };
        capture_reserve(&mut self.entries, 1, meter)?;
        capture_charge(meter, delta)?;
        self.entries.insert(index, (key, value));
        Ok(())
    }
    fn insert_measured(&mut self, key: K, value: V, meter: &mut DemandWork) -> Result<Option<V>, CaptureError> {
        match self.position_measured(&key, meter)? {
            Ok(index) => {
                let bytes = key.owned_bytes()?;
                drop(key);
                meter.release(bytes).map_err(capture_demand_error)?;
                Ok(Some(std::mem::replace(&mut self.entries[index].1, value)))
            }
            Err(index) => {
                self.insert_at(index, key, value, meter)?;
                Ok(None)
            }
        }
    }
    fn get_or_insert_measured(&mut self, key: K, value: V, meter: &mut DemandWork) -> Result<&mut V, CaptureError> {
        let index = match self.position_measured(&key, meter)? {
            Ok(index) => {
                let bytes = key.owned_bytes()?;
                drop(key);
                meter.release(bytes).map_err(capture_demand_error)?;
                index
            }
            Err(index) => {
                self.insert_at(index, key, value, meter)?;
                index
            }
        };
        Ok(&mut self.entries[index].1)
    }
    fn remove_measured(&mut self, key: &K, meter: &mut DemandWork) -> Result<Option<(K, V)>, CaptureError> {
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
    fn contains_measured(&self, key: &K, meter: &mut DemandWork) -> Result<bool, CaptureError> {
        self.map.contains_key_measured(key, meter)
    }
    fn storage_bytes(&self) -> u64 { self.map.storage_bytes() }
    fn insert_measured(&mut self, key: K, meter: &mut DemandWork) -> Result<(), CaptureError> {
        self.map.insert_measured(key, (), meter).map(|_| ())
    }
    fn remove_measured(&mut self, key: &K, meter: &mut DemandWork) -> Result<Option<K>, CaptureError> {
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
    meter: &mut DemandWork,
) -> Result<(), CaptureError> {
    fn sift<T>(
        values: &mut [T], mut root: usize, end: usize,
        compare: &mut impl FnMut(&T, &T) -> std::cmp::Ordering,
        meter: &mut DemandWork,
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
    meter.check().map_err(capture_demand_error)?;
    let length = values.len();
    for root in (0..length / 2).rev() {
        sift(values, root, length, &mut compare, meter)?;
    }
    for end in (1..length).rev() {
        meter.check().map_err(capture_demand_error)?;
        values.swap(0, end);
        sift(values, 0, end, &mut compare, meter)?;
    }
    Ok(())
}

fn with_capture_work<T>(
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    operation: impl FnOnce(&mut DemandWork) -> Result<T, CaptureError>,
    meter: &mut DemandWork,
) -> Result<T, OperationFailure<CaptureError>> {
    let _ = cancellation;
    meter.work = *work;
    meter.budget = budget;
    let result = operation(meter);
    *work = meter.work;
    result.map_err(|error| OperationFailure::new(error, *work))
}

fn capture_allocate(meter: &mut DemandWork, bytes: u64) -> Result<(), CaptureError> {
    meter.allocate(bytes).map_err(capture_demand_error)
}


fn capture_charge(meter: &mut DemandWork, work: WorkCounters) -> Result<(), CaptureError> {
    meter.charge(work).map_err(capture_demand_error)
}

fn capture_step<T>(
    work: &mut WorkCounters,
    meter: &mut DemandWork,
    operation: impl FnOnce(&mut DemandWork) -> Result<T, CaptureError>,
) -> Result<T, OperationFailure<CaptureError>> {
    meter.work = *work;
    let result = operation(meter);
    *work = meter.work;
    result.map_err(|error| OperationFailure::new(error, *work))
}

fn capture_path_clone(path: &NamespacePath, work: &mut WorkCounters, meter: &mut DemandWork)
    -> Result<NamespacePath, OperationFailure<CaptureError>> {
    capture_step(work, meter, |meter| clone_capture_path(path, meter))
}


fn capture_candidate<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &Checkout<A, O>,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &mut DemandWork,
) -> Result<Checkout<A, O>, OperationFailure<CaptureError>> {
    let remaining = capture_remaining(*work, budget, meter)
        .map_err(|error| OperationFailure::new(CaptureError::Work(error), *work))?;
    let candidate = checkout.private_candidate_measured(remaining, cancellation)
        .map_err(|failure| map_engine_failure(failure, *work, meter))?;
    *work = merge_capture_work(*work, candidate.work, meter)?;
    meter.adopt(candidate.value.logical_owned_bytes())
        .map_err(|error| OperationFailure::new(capture_demand_error(error), meter.work))?;
    *work = meter.work;
    Ok(candidate.value)
}

fn clone_capture_name(name: &LogicalName, meter: &mut DemandWork) -> Result<LogicalName, CaptureError> {
    let copied = WorkCounters { bytes_copied: name.as_bytes().len() as u64, ..WorkCounters::UNCHARGED };
    meter.work.admit(&copied, &meter.budget).map_err(CaptureError::Work)?;
    capture_allocate(meter, name.as_bytes().len() as u64)?;
    meter.record(copied).map_err(capture_demand_error)?;
    Ok(name.clone())
}

fn clone_capture_prefix(path: &NamespacePath, depth: usize, limits: crate::model::VolumeLimits,
    meter: &mut DemandWork) -> Result<NamespacePath, CaptureError> {
    let mut components = capture_vec(depth, meter)?;
    for name in path.components().iter().take(depth) {
        components.push(clone_capture_name(name, meter)?);
    }
    NamespacePath::new(components, limits).map_err(|_| CaptureError::InvalidOptions)
}

fn capture_remaining(work: WorkCounters, budget: WorkBudget, meter: &mut DemandWork) -> Result<WorkBudget, WorkError> {
    meter.work = work;
    meter.budget = budget;
    meter.remaining().map_err(|error| match error {
        DemandError::Work(error) => error,
        _ => WorkError::Overflow,
    })
}

fn merge_capture_work(prior: WorkCounters, delta: WorkCounters, meter: &mut DemandWork)
    -> Result<WorkCounters, OperationFailure<CaptureError>> {
    meter.work = prior;
    meter.merge(delta).map_err(|error| OperationFailure::new(capture_demand_error(error), meter.work))?;
    Ok(meter.work)
}

fn capture_vec<T>(count: usize, meter: &mut DemandWork) -> Result<Vec<T>, CaptureError> {
    let bytes = count.checked_mul(std::mem::size_of::<T>()).ok_or(CaptureError::Work(WorkError::Overflow))?;
    capture_allocate(meter, bytes as u64)?;
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|_| CaptureError::InvalidOptions)?;
    Ok(values)
}

fn capture_reserve<T>(values: &mut Vec<T>, additional: usize, meter: &mut DemandWork) -> Result<(), CaptureError> {
    meter.check().map_err(capture_demand_error)?;
    let wanted = values.len().checked_add(additional).ok_or(CaptureError::Work(WorkError::Overflow))?;
    if wanted <= values.capacity() { return Ok(()); }
    let size = std::mem::size_of::<T>();
    let old = values.capacity().checked_mul(size).ok_or(CaptureError::Work(WorkError::Overflow))?;
    let grown = wanted.max(values.capacity().saturating_mul(2).max(4));
    let capacity = match grown.checked_mul(size) {
        Some(bytes) if bytes as u64 <= meter.remaining().map_err(capture_demand_error)?.peak_allocation_bytes => grown,
        _ => wanted,
    };
    let mut replacement = capture_vec(capacity, meter)?;
    let copied = values.len().checked_mul(size).ok_or(CaptureError::Work(WorkError::Overflow))?;
    capture_charge(meter, WorkCounters { bytes_copied: copied as u64, ..WorkCounters::UNCHARGED })?;
    replacement.append(values);
    let old_values = std::mem::replace(values, replacement);
    drop(old_values);
    meter.release(old as u64).map_err(capture_demand_error)
}

fn capture_excludes(policy: &CapturePolicy, path: &NamespacePath, meter: &mut DemandWork) -> Result<bool, CaptureError> {
    for prefix in &policy.excluded_prefixes {
        capture_charge(meter, WorkCounters { items_examined: 1,
            source_path_components: prefix.depth().min(path.depth()) as u64, ..WorkCounters::UNCHARGED })?;
        if path.is_within(prefix) { return Ok(true); }
    }
    Ok(false)
}

fn clone_capture_paths(
    paths: &[NamespacePath],
    policy: Option<&CapturePolicy>,
    meter: &mut DemandWork,
) -> Result<Vec<NamespacePath>, CaptureError> {
    let mut result = capture_vec(paths.len(), meter)?;
    for path in paths {
        capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if let Some(policy) = policy
            && capture_excludes(policy, path, meter)? {
            continue;
        }
        result.push(clone_capture_path(path, meter)?);
    }
    Ok(result)
}

fn release_capture_paths(paths: Vec<NamespacePath>, meter: &mut DemandWork) -> Result<(), CaptureError> {
    let bytes = paths.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    for path in paths {
        meter.release(namespace_owned_bytes(&path)?).map_err(capture_demand_error)?;
    }
    meter.release(bytes).map_err(capture_demand_error)
}
fn capture_drop_host_path(path: CaptureHostPath, meter: &mut DemandWork) -> Result<(), CaptureError> {
    let bytes = capture_host_path_bytes(&path);
    drop(path);
    meter.release(bytes).map_err(capture_demand_error)
}


fn canonical_subtree_roots_measured(
    roots: &[NamespacePath], policy: &CapturePolicy, meter: &mut DemandWork,
) -> Result<Vec<NamespacePath>, CaptureError> {
    let mut sorted = clone_capture_paths(roots, Some(policy), meter)?;
    let sorted_bytes = sorted.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    capture_sort_by(&mut sorted, Ord::cmp, meter)?;
    let mut canonical: Vec<NamespacePath> = capture_vec(sorted.len(), meter)?;
    for root in sorted {
        capture_charge(meter, WorkCounters { items_examined: 1,
            source_path_components: root.depth() as u64, ..WorkCounters::UNCHARGED })?;
        if canonical.last().is_none_or(|ancestor| !root.is_within(ancestor)) {
            canonical.push(root);
        } else {
            meter.release(namespace_owned_bytes(&root)?).map_err(capture_demand_error)?;
        }
    }
    meter.release(sorted_bytes).map_err(capture_demand_error)?;
    Ok(canonical)
}

fn namespace_owned_bytes(path: &NamespacePath) -> Result<u64, CaptureError> {
    Ok(path.retained_bytes())
}

fn clone_capture_path(path: &NamespacePath, meter: &mut DemandWork) -> Result<NamespacePath, CaptureError> {
    capture_charge(meter, WorkCounters { source_path_components: path.depth() as u64,
        items_examined: path.depth() as u64, ..WorkCounters::UNCHARGED })?;
    let mut bytes = (path.depth() as u64).checked_mul(std::mem::size_of::<LogicalName>() as u64)
        .ok_or(CaptureError::Work(WorkError::Overflow))?;
    for name in path.components() {
        meter.check().map_err(capture_demand_error)?;
        bytes = bytes.checked_add(name.as_bytes().len() as u64)
            .ok_or(CaptureError::Work(WorkError::Overflow))?;
    }
    let copied = WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED };
    meter.work.admit(&copied, &meter.budget).map_err(CaptureError::Work)?;
    meter.allocate_counted(bytes, if path.depth() == 0 { 0 } else { path.depth() as u64 + 1 })
        .map_err(capture_demand_error)?;
    meter.record(copied).map_err(capture_demand_error)?;
    let cloned = path.clone();
    meter.check().map_err(capture_demand_error)?;
    Ok(cloned)
}

#[cfg(unix)]
type CaptureHostPath = PathBuf;
#[cfg(windows)]
type CaptureHostPath = Vec<u16>;

fn namespace_to_host_path_measured(path: &NamespacePath, meter: &mut DemandWork) -> Result<CaptureHostPath, CaptureError> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt as _;
        let mut length = 0usize;
        for component in path.components() {
            meter.check().map_err(capture_demand_error)?;
            capture_charge(meter, WorkCounters { items_examined: component.as_bytes().len() as u64,
                ..WorkCounters::UNCHARGED })?;
            let bytes = match component.encoding() {
                NameEncoding::Utf8 | NameEncoding::PosixBytes => component.as_bytes().len(),
                NameEncoding::WindowsUtf16Le => {
                    let units = crate::kernel::types::utf16le_units(component.as_bytes())
                        .ok_or(CaptureError::UnrepresentablePath)?;
                    let mut bytes = 0usize;
                    for character in char::decode_utf16(units) {
                        meter.check().map_err(capture_demand_error)?;
                        bytes = bytes.checked_add(character.map_err(|_| CaptureError::UnrepresentablePath)?.len_utf8())
                            .ok_or(CaptureError::Work(WorkError::Overflow))?;
                    }
                    bytes
                }
            };
            length = length.checked_add(bytes).and_then(|value| value.checked_add(usize::from(length != 0)))
                .ok_or(CaptureError::Work(WorkError::Overflow))?;
        }
        meter.work.admit(&WorkCounters { bytes_copied: length as u64,
            source_path_components: path.depth() as u64, ..WorkCounters::UNCHARGED }, &meter.budget)
            .map_err(CaptureError::Work)?;
        let mut buffer = capture_vec(length, meter)?;
        for component in path.components() {
            capture_charge(meter, WorkCounters { source_path_components: 1, ..WorkCounters::UNCHARGED })?;
            if !buffer.is_empty() {
                capture_charge(meter, WorkCounters { bytes_copied: 1, ..WorkCounters::UNCHARGED })?;
                buffer.push(b'/');
            }
            match component.encoding() {
                NameEncoding::Utf8 | NameEncoding::PosixBytes => {
                    capture_charge(meter, WorkCounters { bytes_copied: component.as_bytes().len() as u64, ..WorkCounters::UNCHARGED })?;
                    buffer.extend_from_slice(component.as_bytes());
                }
                NameEncoding::WindowsUtf16Le => {
                    let units = crate::kernel::types::utf16le_units(component.as_bytes())
                        .ok_or(CaptureError::UnrepresentablePath)?;
                    for character in char::decode_utf16(units) {
                        let character = character.map_err(|_| CaptureError::UnrepresentablePath)?;
                        capture_charge(meter, WorkCounters { bytes_copied: character.len_utf8() as u64, ..WorkCounters::UNCHARGED })?;
                        let mut bytes = [0; 4];
                        buffer.extend_from_slice(character.encode_utf8(&mut bytes).as_bytes());
                    }
                }
            }
        }
        Ok(std::ffi::OsString::from_vec(buffer).into())
    }
    #[cfg(windows)]
    {
        let mut length = 0usize;
        for component in path.components() {
            meter.check().map_err(capture_demand_error)?;
            capture_charge(meter, WorkCounters { items_examined: component.as_bytes().len() as u64,
                ..WorkCounters::UNCHARGED })?;
            let units = match component.encoding() {
                NameEncoding::WindowsUtf16Le => component.as_bytes().len() / 2,
                NameEncoding::Utf8 | NameEncoding::PosixBytes => {
                    let text = std::str::from_utf8(component.as_bytes()).map_err(|_| CaptureError::UnrepresentablePath)?;
                    if text.contains(['/', '\\']) { return Err(CaptureError::UnrepresentablePath); }
                    text.encode_utf16().count()
                }
            };
            length = length.checked_add(units).and_then(|value| value.checked_add(usize::from(length != 0)))
                .ok_or(CaptureError::Work(WorkError::Overflow))?;
        }
        let bytes = (length as u64).checked_mul(2).ok_or(CaptureError::Work(WorkError::Overflow))?;
        meter.work.admit(&WorkCounters { bytes_copied: bytes,
            source_path_components: path.depth() as u64, ..WorkCounters::UNCHARGED }, &meter.budget)
            .map_err(CaptureError::Work)?;
        let mut result = capture_vec(length, meter)?;
        for component in path.components() {
            capture_charge(meter, WorkCounters { source_path_components: 1, ..WorkCounters::UNCHARGED })?;
            if !result.is_empty() {
                capture_charge(meter, WorkCounters { bytes_copied: 2, ..WorkCounters::UNCHARGED })?;
                result.push(u16::from(b'\\'));
            }
            match component.encoding() {
                NameEncoding::WindowsUtf16Le => {
                    for unit in crate::kernel::types::utf16le_units(component.as_bytes())
                        .ok_or(CaptureError::UnrepresentablePath)? {
                        capture_charge(meter, WorkCounters { bytes_copied: 2, ..WorkCounters::UNCHARGED })?;
                        result.push(unit);
                    }
                }
                NameEncoding::Utf8 | NameEncoding::PosixBytes => {
                    for unit in std::str::from_utf8(component.as_bytes())
                        .map_err(|_| CaptureError::UnrepresentablePath)?.encode_utf16() {
                        capture_charge(meter, WorkCounters { bytes_copied: 2, ..WorkCounters::UNCHARGED })?;
                        result.push(unit);
                    }
                }
            }
        }
        Ok(result)
    }
}

fn capture_stat(root: &HostRoot, path: &CaptureHostPath, meter: &mut DemandWork) -> Result<HostStat, DemandError> {
    #[cfg(unix)]
    { root.stat_measured(path, meter) }
    #[cfg(windows)]
    { root.stat_wide_measured(path, meter) }
}

fn collect_host_subtree_observations_measured(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    host_root: CaptureHostPath,
    volume_root: NamespacePath,
    policy: &CapturePolicy,
    observed: &mut CaptureMap<NamespacePath, HostObservation>,
    meter: &mut DemandWork,
) -> Result<(), CaptureError> {
    collect_host_walk_measured(root, profile, limits, host_root, volume_root, policy, meter,
        |path, stat, meter| insert_host_observation(observed, path, stat, maximum, meter))
}

fn collect_host_walk_measured(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    host_root: CaptureHostPath,
    volume_root: NamespacePath,
    policy: &CapturePolicy,
    meter: &mut DemandWork,
    mut insert: impl FnMut(NamespacePath, HostStat, &mut DemandWork) -> Result<(), CaptureError>,
) -> Result<(), CaptureError> {
    let slot = std::mem::size_of::<(CaptureHostPath, NamespacePath)>() as u64;
    capture_allocate(meter, slot)?;
    let mut pending = Vec::new();
    pending.try_reserve_exact(1).map_err(|_| CaptureError::InvalidOptions)?;
    pending.push((host_root, volume_root));
    while let Some((host_parent, volume_parent)) = pending.pop() {
        meter.check().map_err(capture_demand_error)?;
        #[cfg(unix)]
        let mut reader = root.read_dir_stats_measured(&host_parent, meter).map_err(capture_demand_error)?;
        #[cfg(windows)]
        let mut reader = root.read_dir_stats_wide_measured(&host_parent, meter).map_err(capture_demand_error)?;
        loop {
            #[cfg(unix)]
            let entry = reader.next_measured(meter).map_err(capture_demand_error)?;
            #[cfg(windows)]
            let entry = reader.next_wide_measured(meter).map_err(capture_demand_error)?;
            let Some(entry) = entry else { break };
            #[cfg(unix)]
            let native_name_bytes = entry.name.as_encoded_bytes().len() as u64;
            #[cfg(windows)]
            let native_name_bytes = entry.name.len() as u64 * 2;
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
                    .map_err(|_| CaptureError::UnrepresentablePath)?
            };
            #[cfg(windows)]
            let name = {
                let (encoding, bytes) = if profile == FilesystemProfile::Windows {
                    capture_allocate(meter, native_name_bytes)?;
                    let mut bytes = Vec::new();
                    bytes.try_reserve_exact(entry.name.len() * 2).map_err(|_| CaptureError::InvalidOptions)?;
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
                    capture_allocate(meter, length as u64)?;
                    let mut bytes = Vec::new();
                    bytes.try_reserve_exact(length).map_err(|_| CaptureError::InvalidOptions)?;
                    for character in char::decode_utf16(entry.name.iter().copied()) {
                        let character = character.map_err(|_| CaptureError::UnrepresentablePath)?;
                        capture_charge(meter, WorkCounters { bytes_copied: character.len_utf8() as u64, ..WorkCounters::UNCHARGED })?;
                        let mut buffer = [0; 4];
                        bytes.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
                    }
                    (NameEncoding::PosixBytes, bytes)
                };
                LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
                    .map_err(|_| CaptureError::UnrepresentablePath)?
            };
            drop(entry.name);
            meter.release(native_name_bytes).map_err(capture_demand_error)?;
            let child = append_path_measured(&volume_parent, name, limits, meter)?;
            if capture_excludes(policy, &child, meter)? {
                meter.release(namespace_owned_bytes(&child)?).map_err(capture_demand_error)?;
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
                meter.release(capture_host_path_bytes(&host_child)).map_err(capture_demand_error)?;
                meter.release(namespace_owned_bytes(&child)?).map_err(capture_demand_error)?;
            }
        }
        let reader_bytes = reader.logical_owned_bytes();
        drop(reader);
        meter.release(reader_bytes).map_err(capture_demand_error)?;
        meter.release(capture_host_path_bytes(&host_parent)).map_err(capture_demand_error)?;
        meter.release(namespace_owned_bytes(&volume_parent)?).map_err(capture_demand_error)?;
    }
    meter.release(pending.capacity() as u64 * slot).map_err(capture_demand_error)?;
    Ok(())
}

fn capture_host_path_bytes(path: &CaptureHostPath) -> u64 {
    #[cfg(unix)]
    { path.as_os_str().as_encoded_bytes().len() as u64 }
    #[cfg(windows)]
    { path.len() as u64 * 2 }
}

fn append_path_measured(
    parent: &NamespacePath,
    name: LogicalName,
    limits: crate::model::VolumeLimits,
    meter: &mut DemandWork,
) -> Result<NamespacePath, CaptureError> {
    capture_charge(meter, WorkCounters {
        source_path_components: parent.depth() as u64 + 1,
        items_examined: parent.depth() as u64,
        ..WorkCounters::UNCHARGED
    })?;
    let mut components = capture_vec(parent.depth() + 1, meter)?;
    for component in parent.components() {
        components.push(clone_capture_name(component, meter)?);
    }
    components.push(name);
    NamespacePath::new(components, limits).map_err(|error| CaptureError::Engine(error.to_string()))
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
    meter: &mut DemandWork,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let mut unique = clone_capture_paths(paths, Some(policy), meter)
        .map_err(|error| OperationFailure::new(error, meter.work))?;
    capture_sort_by(&mut unique, Ord::cmp, meter)
        .map_err(|error| OperationFailure::new(error, meter.work))?;
    if unique
        .windows(2)
        .any(|pair| matches!(pair, [left, right] if left == right))
    {
        return Err(OperationFailure::new(CaptureError::InvalidOptions, meter.work));
    }
    capture_unique_paths_from_root(checkout,
    unique,
    maximum_extent_spans,
    source_root,
    budget,
    cancellation, meter)
    .await
}

async fn capture_unique_paths_from_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    unique: Vec<NamespacePath>,
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &mut DemandWork,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    // A comparator must not inspect a changing host tree: one path could
    // otherwise alternate between present and absent during the sort. This
    // also reduces host metadata probes from O(paths * log paths) to O(paths).
    let observed = observe_unique_paths(unique, source_root, budget, cancellation, meter)?;
    let mut captured = capture_observed_paths_from_root(checkout,
    observed.value,
    maximum_extent_spans,
    source_root,
    budget,
    cancellation,
    None, meter)
    .await?;
    captured.value.work = captured.work;
    Ok(captured)
}

type ObservedPath = (Option<HostObservation>, NamespacePath);

fn observe_unique_paths(
    unique: Vec<NamespacePath>,
    source_root: &HostRoot,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &mut DemandWork,
) -> Result<OperationReceipt<Vec<ObservedPath>>, OperationFailure<CaptureError>> {
    let mut ordered = Vec::new();
    let unique_bytes = unique.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    let result = (|| {
        capture_allocate(meter, unique.len() as u64 * std::mem::size_of::<ObservedPath>() as u64)?;
        ordered.try_reserve_exact(unique.len()).map_err(|_| CaptureError::InvalidOptions)?;
        for path in unique {
            capture_charge(meter, WorkCounters { source_entries_visited: 1, ..WorkCounters::UNCHARGED })?;
            let host_path = namespace_to_host_path_measured(&path, meter)?;
            let observation = observe_host_path(source_root, &host_path, meter)?;
            meter.release(capture_host_path_bytes(&host_path)).map_err(capture_demand_error)?;
            ordered.push((observation, path));
        }
        meter.release(unique_bytes).map_err(capture_demand_error)?;
        Ok(())
    })();
    result.map_err(|error| OperationFailure::new(error, meter.work))?;
    let work = meter.work;
    cancellation
        .check()
        .map_err(|error| OperationFailure::new(CaptureError::Engine(error.to_string()), work))?;
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
    }, meter).map_err(|error| OperationFailure::new(error, meter.work))?;
    let work = meter.work;
    cancellation
        .check()
        .map_err(|error| OperationFailure::new(CaptureError::Engine(error.to_string()), work))?;
    Ok(OperationReceipt {
        value: ordered,
        work,
    })
}

fn observe_host_path(
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    meter: &mut DemandWork,
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
    fn capture_heap_refusal_precedes_future_construction() {
        let constructed = std::cell::Cell::new(false);
        let mut budget = WorkBudget::UNBOUNDED;
        budget.allocation_operations = 0;
        let mut meter = DemandWork::new(budget, &CancellationToken::new());
        let failure = match crate::heap_future::in_heap_admitted(
            &mut meter,
            |meter, bytes| capture_allocate(meter, bytes)
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
            |meter, bytes| capture_allocate(meter, bytes)
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
        let root = HostRoot::open(directory.path())?;
        let path = test_host_path(Path::new("file"));
        let cancellation = CancellationToken::new();
        let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let (mut future, heap_bytes) = crate::heap_future::in_heap_admitted(
            &mut meter,
            |meter, bytes| capture_allocate(meter, bytes)
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
        let source_root = HostRoot::open(directory.path())?;
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
        let root = HostRoot::open(directory.path())?;
        let limits = crate::model::VolumeLimits::default();
        let path = |name: &[u8]| -> Result<NamespacePath, Box<dyn std::error::Error>> {
            Ok(NamespacePath::new(vec![LogicalName::new(NameEncoding::Utf8, name.to_vec(), 255)?], limits)?)
        };
        let paths = [path(b"first")?, path(b"second")?];
        let cancellation = CancellationToken::new();
        let mut probe = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        root.stat_measured(Path::new("first"), &mut probe)?;
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
        let source_root = HostRoot::open(directory.path())?;
        assert!(source_root.open_dir_held(Path::new("alias")).is_err());
        assert!(
            source_root
                .symlink_metadata_held(Path::new("alias/private"))
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
    meter: &mut DemandWork,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let (mut future, heap_bytes) = crate::heap_future::in_heap_admitted(
        meter,
        |meter, bytes| {
            meter.budget = budget;
            capture_allocate(meter, bytes)
                .map_err(|error| OperationFailure::new(error, meter.work))
        },
        move |meter| async move {
            let result = async {
    let mut receipt = CaptureReceipt { work: meter.work, ..CaptureReceipt::default() };
    let ordered_bytes = ordered.capacity() as u64 * std::mem::size_of::<ObservedPath>() as u64;
    let mut observations = capture_vec(ordered.len(), meter)
        .map_err(|error| OperationFailure::new(error, meter.work))?;
    let mut paths = capture_vec(ordered.len(), meter)
        .map_err(|error| OperationFailure::new(error, meter.work))?;
    receipt.work = meter.work;
    for (observation, path) in ordered {
        observations.push(observation);
        paths.push(path);
    }
    capture_step(&mut receipt.work, meter, |meter| meter.release(ordered_bytes).map_err(capture_demand_error))?;
    let current = if paths.is_empty() {
        Vec::new()
    } else {
        let remaining = capture_remaining(receipt.work, budget, meter)
            .map_err(|error| OperationFailure::new(CaptureError::Work(error), receipt.work))?;
        let lookup = checkout
            .lookup_batch_no_follow(&paths, remaining, cancellation)
            .await
            .map_err(|failure| map_engine_failure(failure, receipt.work, meter))?;
        receipt.work = merge_capture_work(receipt.work, lookup.work, meter)?;
        let lookup_bytes = lookup.value.retained_allocation_bytes;
        capture_step(&mut receipt.work, meter, |meter| meter.adopt(lookup_bytes).map_err(capture_demand_error))?;
        let mut records = capture_vec(lookup.value.entries.len(), meter)
            .map_err(|error| OperationFailure::new(error, meter.work))?;
        for entry in lookup.value.entries {
            records.push(entry.record);
        }
        capture_step(&mut receipt.work, meter, |meter| meter.release(lookup_bytes).map_err(capture_demand_error))?;
        records
    };
    
    let states = order_capture_states(observations, paths, current, meter)
        .map_err(|error| OperationFailure::new(error, meter.work))?;
    receipt.work = meter.work;
    let mut host_links = CaptureMap::new();
    let captured = capture_ranked_paths_from_root(checkout,
    states,
    &mut host_links,
    maximum_extent_spans,
    source_root,
    budget,
    cancellation,
    baseline, meter)
    .await?;
    receipt.examined_paths = captured.value.examined_paths;
    receipt.changed_paths = captured.value.changed_paths;
    receipt.staged_file_bytes = captured.value.staged_file_bytes;
    receipt.work = captured.work;
    capture_step(&mut receipt.work, meter, |meter| release_host_links(host_links, meter))?;
    Ok(OperationReceipt {
        value: receipt,
        work: receipt.work,
    })
            }.await;
            (result, meter)
        },
    )?;
    let (result, meter) = future.as_mut().await;
    drop(future);
    meter.release(heap_bytes)
        .map_err(|error| OperationFailure::new(capture_demand_error(error), meter.work))?;
    result
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
    budget: WorkBudget,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
    meter: &mut DemandWork,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let mut receipt = CaptureReceipt { work: meter.work, ..CaptureReceipt::default() };
    let mut mutations = Vec::new();
    let states_bytes = states.capacity() as u64 * std::mem::size_of::<CapturePathState>() as u64;
    let mut prepared = capture_vec(states.len(), meter)
        .map_err(|error| OperationFailure::new(error, meter.work))?;
    receipt.work = meter.work;
    for (observation, path, current) in states {
        let plan = prepare_final_path(checkout,
        path,
        CurrentRecord::Known(current),
        CaptureIntent::Complete,
        source_root,
        observation,
        host_links,
        None,
        baseline,
        &mut mutations,
        &mut receipt,
        budget,
        cancellation, meter)
        .await?;
        if let Some(plan) = plan {
            prepared.push(plan);
        }
    }
    capture_step(&mut receipt.work, meter, |meter| meter.release(states_bytes).map_err(capture_demand_error))?;
    finish_prepared_paths(&checkout.content_stager(),
    source_root,
    prepared,
    maximum_extent_spans,
    &mut mutations,
    &mut receipt,
    budget,
    cancellation, meter)
    .await?;

    apply_capture_transaction(checkout, mutations, &mut receipt, budget, cancellation, meter).await?;
    receipt
        .work
        .verify(budget)
        .map_err(|error| OperationFailure::new(CaptureError::Work(error), receipt.work))?;
    Ok(OperationReceipt {
        value: receipt,
        work: receipt.work,
    })
}

type CapturePathState = (Option<HostObservation>, NamespacePath, Option<FileRecord>);

fn order_capture_states(
    observations: Vec<Option<HostObservation>>,
    paths: Vec<NamespacePath>,
    current: Vec<Option<FileRecord>>,
    meter: &mut DemandWork,
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

fn sort_capture_states(states: &mut [CapturePathState], meter: &mut DemandWork) -> Result<(), CaptureError> {
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
    let result = crate::obs::in_span(
        &span,
        crate::heap_future::in_heap_measured(move |budget| async move {
        if checkout.has_pending_mutations() {
            return Err(OperationFailure::before_work(CaptureError::DirtyCheckout));
        }
        if options.maximum_paths == 0 || options.maximum_extent_spans == 0 {
            return Err(OperationFailure::before_work(CaptureError::InvalidOptions));
        }
        let mut capture_meter = DemandWork::new(budget, cancellation);
        let meter = &mut capture_meter;
        let source_root = open_source_root(options, meter)?;
        let limits = checkout.volume_config().limits;
        let profile = checkout.volume_config().profile;
        let maximum = usize::try_from(options.maximum_paths)
            .map_err(|_| OperationFailure::new(CaptureError::InvalidOptions, meter.work))?;
        let mut paths = Vec::new();
        let mut work = meter.work;
        let observed = collect_host_observations(&source_root,
        profile,
        limits,
        maximum,
        policy,
        &mut work,
        budget,
        cancellation, meter)?;
        collect_checkout_paths(checkout,
        limits,
        policy,
        maximum,
        &mut paths,
        &mut work,
        budget,
        cancellation, meter)
        .await?;
        let ordered = order_host_checkout_union(observed, paths, maximum, meter)
            .map_err(|error| OperationFailure::new(error, meter.work))?;
        let mut captured = capture_observed_paths_from_root(checkout,
        ordered,
        options.maximum_extent_spans,
        &source_root,
        budget,
        cancellation,
        None, meter)
        .await?;
        captured.value.work = captured.work;
        Ok(captured)
        }, budget, cancellation, CaptureError::Cancelled),
    )
    .await;
    let result = result.map(|mut receipt| {
        receipt.value.work = receipt.work;
        receipt
    });
    crate::obs::measured_on(&span, result, |receipt| &receipt.work)
}

#[allow(
    clippy::too_many_arguments,
    reason = "the host walk needs the root, profile, limits, bound, and policy plus work, budget, and cancellation"
)]
fn collect_host_observations(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    policy: &CapturePolicy,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &mut DemandWork,
) -> Result<CaptureMap<NamespacePath, HostObservation>, OperationFailure<CaptureError>> {
    let mut observed = CaptureMap::new();
    let volume_root = NamespacePath::new(Vec::new(), limits)
        .map_err(|error| OperationFailure::new(CaptureError::Engine(error.to_string()), *work))?;
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
    meter: &mut DemandWork,
) -> Result<Vec<(Option<HostObservation>, NamespacePath)>, CaptureError> {
    let checkout_bytes = checkout_paths.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    let observed_bytes = observed.storage_bytes();
    let mut checkout_paths = checkout_paths;
    capture_sort_by(&mut checkout_paths, Ord::cmp, meter)?;
    let mut absent: Vec<NamespacePath> = capture_vec(checkout_paths.len(), meter)?;
    for path in checkout_paths {
        capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if observed.contains_key_measured(&path, meter)? || absent.last().is_some_and(|prior| prior == &path) {
            meter.release(namespace_owned_bytes(&path)?).map_err(capture_demand_error)?;
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
    meter: &mut DemandWork,
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
fn collect_host_subtree_roots(
    source_root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    roots: &[NamespacePath],
    policy: &CapturePolicy,
    observed: &mut CaptureMap<NamespacePath, HostObservation>,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &mut DemandWork,
) -> Result<(), OperationFailure<CaptureError>> {
    with_capture_work(work, budget, cancellation, |meter| {
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
                    meter.release(capture_host_path_bytes(&host_root)).map_err(capture_demand_error)?;
                }
            }
            else {
                meter.release(capture_host_path_bytes(&host_root)).map_err(capture_demand_error)?;
            }
        }
        Ok(())
    }, meter)
}

#[allow(
    clippy::too_many_arguments,
    reason = "the host walk needs the root, profile, limits, bound, subtree, and policy plus the shared map, work, budget, and cancellation"
)]
fn collect_host_subtree_observations(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    host_root: CaptureHostPath,
    volume_root: NamespacePath,
    policy: &CapturePolicy,
    observed: &mut CaptureMap<NamespacePath, HostObservation>,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &mut DemandWork,
) -> Result<(), OperationFailure<CaptureError>> {
    with_capture_work(work, budget, cancellation, |meter| {
        collect_host_subtree_observations_measured(
            root, profile, limits, maximum, host_root, volume_root, policy, observed, meter,
        )
    }, meter)
}

#[allow(
    clippy::too_many_arguments,
    reason = "the host walk needs the root, profile, limits, subtree, policy, and bound plus the path sink, work, budget, and cancellation"
)]
fn collect_host_subtree_paths<P: ScannedPaths>(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    host_root: CaptureHostPath,
    volume_root: NamespacePath,
    policy: &CapturePolicy,
    maximum: usize,
    paths: &mut P,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &mut DemandWork,
) -> Result<(), OperationFailure<CaptureError>> {
    with_capture_work(work, budget, cancellation, |meter| {
        collect_host_walk_measured(root, profile, limits, host_root, volume_root, policy, meter,
            |path, _, meter| {
                append_scanned_path(paths, path, maximum, meter)
            })
    }, meter)
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
    meter: &mut DemandWork,
) -> Result<(), OperationFailure<CaptureError>> {
    let (mut future, heap_bytes) = crate::heap_future::in_heap_admitted(
        (meter, work),
        |(meter, work), bytes| {
            meter.budget = budget;
            capture_step(work, meter, |meter| capture_allocate(meter, bytes))
        },
        move |(meter, work)| async move {
            let result = async {
    let root = NamespacePath::new(Vec::new(), limits).map_err(|error| {
        OperationFailure::new(CaptureError::Engine(error.to_string()), *work)
    })?;
    collect_checkout_subtree_paths(checkout,
    limits,
    root,
    policy,
    maximum,
    paths,
    work,
    budget,
    cancellation, meter)
    .await
            }.await;
            (result, meter, work)
        },
    )?;
    let (result, meter, work) = future.as_mut().await;
    drop(future);
    let released = meter.release(heap_bytes).map_err(capture_demand_error);
    *work = meter.work;
    released.map_err(|error| OperationFailure::new(error, *work))?;
    result
}

fn capture_adopt_page(page: &crate::DirectoryRecordPage, meter: &mut DemandWork) -> Result<u64, CaptureError> {
    capture_charge(meter, WorkCounters { items_examined: page.entries.len() as u64, ..WorkCounters::UNCHARGED })?;
    let mut bytes = page.entries.capacity() as u64 * std::mem::size_of::<crate::DirectoryRecordEntry>() as u64;
    for entry in &page.entries {
        bytes = bytes.checked_add(entry.name.as_bytes().len() as u64)
            .ok_or(CaptureError::Work(WorkError::Overflow))?;
    }
    meter.adopt(bytes).map_err(capture_demand_error)?;
    Ok(bytes)
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
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &mut DemandWork,
) -> Result<(), OperationFailure<CaptureError>> {
    const PAGE_ENTRIES: u32 = 1024;
    let mut pending = capture_step(work, meter, |meter| capture_vec(1, meter))?;
    pending.push(root);
    while let Some(directory) = pending.pop() {
        let mut after = None;
        loop {
            let remaining = capture_remaining(*work, budget, meter)
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
                .map_err(|failure| map_engine_failure(failure, *work, meter))?;
            *work = merge_capture_work(*work, page.work, meter)?;
            let page_bytes = capture_step(work, meter, |meter| capture_adopt_page(&page.value, meter))?;
            if page.value.has_more && page.value.entries.is_empty() {
                return Err(OperationFailure::new(
                    CaptureError::Engine("directory pagination made no progress".to_owned()),
                    *work,
                ));
            }
            for entry in &page.value.entries {
                let child = capture_step(work, meter, |meter| {
                    capture_charge(meter, WorkCounters { source_entries_visited: 1, ..WorkCounters::UNCHARGED })?;
                    let name = clone_capture_name(&entry.name, meter)?;
                    append_path_measured(&directory, name, limits, meter)
                })?;
                let child_bytes = namespace_owned_bytes(&child)
                    .map_err(|error| OperationFailure::new(error, *work))?;
                if capture_step(work, meter, |meter| capture_excludes(policy, &child, meter))? {
                    capture_step(work, meter, |meter| meter.release(child_bytes).map_err(capture_demand_error))?;
                    continue;
                }
                let scanned = capture_path_clone(&child, work, meter)?;
                capture_step(work, meter, |meter| append_scanned_path(paths, scanned, maximum, meter))?;
                if entry.record.kind == FileKind::Directory {
                    capture_step(work, meter, |meter| capture_reserve(&mut pending, 1, meter))?;
                    pending.push(child);
                } else {
                    capture_step(work, meter, |meter| meter.release(child_bytes).map_err(capture_demand_error))?;
                }
            }
            if let Some(previous) = after.take() {
                let bytes = previous.as_bytes().len() as u64;
                drop(previous);
                capture_step(work, meter, |meter| meter.release(bytes).map_err(capture_demand_error))?;
            }
            after = match page.value.entries.last() {
                Some(entry) => Some(capture_step(work, meter, |meter| clone_capture_name(&entry.name, meter))?),
                None => None,
            };
            let has_more = page.value.has_more;
            drop(page);
            capture_step(work, meter, |meter| meter.release(page_bytes).map_err(capture_demand_error))?;
            if !has_more {
                break;
            }
        }
        if let Some(cursor) = after {
            capture_step(work, meter, |meter| meter.release(cursor.as_bytes().len() as u64).map_err(capture_demand_error))?;
        }
        capture_step(work, meter, |meter| meter.release(namespace_owned_bytes(&directory)?).map_err(capture_demand_error))?;
    }
    let pending_bytes = pending.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    drop(pending);
    capture_step(work, meter, |meter| meter.release(pending_bytes).map_err(capture_demand_error))?;
    Ok(())
}

trait ScannedPaths {
    fn len(&self) -> usize;
    fn contains_measured(&self, path: &NamespacePath, meter: &mut DemandWork) -> Result<bool, CaptureError>;
    fn insert_measured(&mut self, path: NamespacePath, meter: &mut DemandWork) -> Result<(), CaptureError>;
}

impl ScannedPaths for Vec<NamespacePath> {
    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn contains_measured(&self, _path: &NamespacePath, meter: &mut DemandWork) -> Result<bool, CaptureError> {
        meter.check().map_err(capture_demand_error)?;
        Ok(false)
    }

    fn insert_measured(&mut self, path: NamespacePath, meter: &mut DemandWork) -> Result<(), CaptureError> {
        capture_reserve(self, 1, meter)?;
        self.push(path);
        Ok(())
    }
}

impl ScannedPaths for CaptureSet<NamespacePath> {
    fn len(&self) -> usize {
        CaptureSet::len(self)
    }

    fn contains_measured(&self, path: &NamespacePath, meter: &mut DemandWork) -> Result<bool, CaptureError> {
        CaptureSet::contains_measured(self, path, meter)
    }

    fn insert_measured(&mut self, path: NamespacePath, meter: &mut DemandWork) -> Result<(), CaptureError> {
        CaptureSet::insert_measured(self, path, meter)
    }
}

fn append_scanned_path<P: ScannedPaths>(
    paths: &mut P,
    path: NamespacePath,
    maximum: usize,
    meter: &mut DemandWork,
) -> Result<(), CaptureError> {
    if paths.contains_measured(&path, meter)? {
        return capture_drop_path(path, meter);
    }
    if paths.len() >= maximum { return Err(CaptureError::InvalidOptions); }
    capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
    paths.insert_measured(path, meter)
}

fn capture_drop_path(path: NamespacePath, meter: &mut DemandWork) -> Result<(), CaptureError> {
    let bytes = namespace_owned_bytes(&path)?;
    drop(path);
    meter.release(bytes).map_err(capture_demand_error)
}

fn capture_watch_owned_bytes(changes: &[WatchChange], storage_bytes: u64, meter: &mut DemandWork) -> Result<u64, CaptureError> {
    let mut bytes = storage_bytes;
    for change in changes {
        let (first, second) = match change {
            WatchChange::Created(path) | WatchChange::Modified(path) | WatchChange::Arrived(path)
            | WatchChange::Removed(path) | WatchChange::MetadataChanged(path) => (path, None),
            WatchChange::Renamed { from, to } => (from, Some(to)),
        };
        for path in std::iter::once(first).chain(second) {
            capture_charge(meter, WorkCounters { items_examined: 1,
                source_path_components: path.depth() as u64, ..WorkCounters::UNCHARGED })?;
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
#[allow(
    clippy::too_many_lines,
    reason = "one watcher batch is classified, staged, and applied as a single atomic capture"
)]
pub async fn capture_watch_batch_with_policy<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    batch: WatchBatch,
    options: &CaptureOptions,
    policy: &CapturePolicy,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<WatchCaptureReceipt>, OperationFailure<CaptureError>> {
    let span = crate::obs::span!(
        INFO,
        "acyclic.fs.native_capture.watch_batch_with_policy",
        outcome = crate::obs::Empty,
        error.kind = crate::obs::Empty,
        work.items = crate::obs::Empty,
        work.bytes = crate::obs::Empty,
        work.durability = crate::obs::Empty,
    );
    let mut capture_meter = DemandWork::new(budget, cancellation);
    let owned_changes = if let WatchBatch::Changes { changes, .. } = &batch {
        let bytes = changes.capacity() as u64 * std::mem::size_of::<WatchChange>() as u64;
        let bytes = capture_watch_owned_bytes(changes, bytes, &mut capture_meter)
            .map_err(|error| OperationFailure::new(error, capture_meter.work))?;
        capture_meter.adopt(bytes)
            .map_err(|error| OperationFailure::new(capture_demand_error(error), capture_meter.work))?;
        bytes
    } else {
        0
    };
    let (mut future, heap_bytes) = crate::heap_future::in_heap_admitted(
        capture_meter,
        |meter, bytes| capture_allocate(meter, bytes)
            .map_err(|error| OperationFailure::new(error, meter.work)),
        move |mut capture_meter| async move {
        let meter = &mut capture_meter;
        let result = async {
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
        let source_root = open_source_root(options, meter)?;
        let mut receipt = CaptureReceipt { work: meter.work, ..CaptureReceipt::default() };
        let mut mutations = Vec::new();
        let mut ordinary = CaptureMap::new();
        let mut rename_records = CaptureMap::<NamespacePath, FileRecord>::new();
        let mut renamed_directories = Vec::new();
        let mut rename_destinations = Vec::new();
        let mut moved_away = CaptureSet::new();
        
        for change in changes {
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
                        capture_step(&mut receipt.work, meter, |meter| {
                            meter.release(namespace_owned_bytes(path)?).map_err(capture_demand_error)
                        })?;
                        drop(change);
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
                        let remaining = capture_remaining(receipt.work, budget, meter).map_err(|error| {
                            OperationFailure::new(CaptureError::Work(error), receipt.work)
                        })?;
                        let source = checkout
                            .lookup_no_follow(&from, remaining, cancellation)
                            .await
                            .map_err(|failure| map_engine_failure(failure, receipt.work, meter))?;
                        receipt.work = merge_capture_work(receipt.work, source.work, meter)?;
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
                        capture_step(&mut receipt.work, meter, |meter| meter.release(namespace_owned_bytes(&key)?).map_err(capture_demand_error))?;
                    }
                    capture_step(&mut receipt.work, meter, |meter| meter.release(namespace_owned_bytes(&to)?).map_err(capture_demand_error))?;
                }
            }
        }
        capture_step(&mut receipt.work, meter, |meter| meter.release(changes_bytes).map_err(capture_demand_error))?;
        capture_step(&mut receipt.work, meter, |meter| release_capture_set(moved_away, meter))?;
        
        expand_directory_hints(checkout,
        &mut ordinary,
        &source_root,
        options.maximum_paths,
        policy,
        &mut receipt,
        budget,
        cancellation, meter)
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
            cancellation,
            checkout.volume_config().limits,
            meter,
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
            cancellation,
        },
        &mut receipt, meter)
        .await?;
        
        let mut lookup_paths = capture_step(&mut receipt.work, meter, |meter| capture_vec(ordinary.len(), meter))?;
        for (path, (current, _)) in &ordinary {
            if matches!(current, CurrentRecord::Lookup) {
                lookup_paths.push(capture_path_clone(path, &mut receipt.work, meter)?);
            }
        }
        if !lookup_paths.is_empty() {
            let remaining = capture_remaining(receipt.work, budget, meter).map_err(|error| {
                OperationFailure::new(CaptureError::Work(error), receipt.work)
            })?;
            let lookup = checkout
                .lookup_batch_no_follow(&lookup_paths, remaining, cancellation)
                .await
                .map_err(|failure| map_engine_failure(failure, receipt.work, meter))?;
            receipt.work = merge_capture_work(receipt.work, lookup.work, meter)?;
            let lookup_bytes = lookup.value.retained_allocation_bytes;
            let lookup_paths_bytes = lookup_paths.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
            capture_step(&mut receipt.work, meter, |meter| meter.adopt(lookup_bytes).map_err(capture_demand_error))?;
            for (path, entry) in lookup_paths.into_iter().zip(lookup.value.entries) {
                let mut record = entry.record;
                if record.is_none()
                    && let Some(prior_path) = capture_step(&mut receipt.work, meter, |meter| remap_renamed_directory_path(
                        &path, &renamed_directories, checkout.volume_config().limits, meter,
                    ))?
                {
                    let remaining = capture_remaining(receipt.work, budget, meter).map_err(|error| {
                        OperationFailure::new(CaptureError::Work(error), receipt.work)
                    })?;
                    let prior = checkout
                        .lookup_no_follow(&prior_path, remaining, cancellation)
                        .await
                        .map_err(|failure| map_engine_failure(failure, receipt.work, meter))?;
                    receipt.work = merge_capture_work(receipt.work, prior.work, meter)?;
                    record = prior.value.record;
                    capture_step(&mut receipt.work, meter, |meter| meter.release(namespace_owned_bytes(&prior_path)?).map_err(capture_demand_error))?;
                }
                let Ok(index) = capture_step(&mut receipt.work, meter, |meter| ordinary.position_measured(&path, meter))? else {
                    return Err(OperationFailure::new(
                        CaptureError::Engine("capture lookup path disappeared".into()),
                        receipt.work,
                    ));
                };
                let (current, _) = &mut ordinary.entries[index].1;
                *current = CurrentRecord::Known(record);
                capture_step(&mut receipt.work, meter, |meter| meter.release(namespace_owned_bytes(&path)?).map_err(capture_demand_error))?;
            }
            capture_step(&mut receipt.work, meter, |meter| meter.release(lookup_bytes + lookup_paths_bytes).map_err(capture_demand_error))?;
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
                    CaptureError::Engine("capture ancestor path disappeared".into()),
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
            cancellation, meter)
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
            cancellation, meter)
            .await?;
            if let Some(plan) = plan {
                prepared.push(plan);
            }
        }
        capture_step(&mut receipt.work, meter, |meter| meter.release(rename_records_bytes).map_err(capture_demand_error))?;
        let mut observed_ordinary = capture_step(&mut receipt.work, meter, |meter| capture_vec(ordinary.len(), meter))?;
        let ordinary_bytes = ordinary.storage_bytes();
        for (path, (current, intent)) in ordinary {
            let observation = capture_step(&mut receipt.work, meter, |meter| {
                let host_path = namespace_to_host_path_measured(&path, meter)?;
                let observation = observe_host_path(&source_root, &host_path, meter)?;
                meter.release(capture_host_path_bytes(&host_path)).map_err(capture_demand_error)?;
                Ok(observation)
            })?;
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
            cancellation, meter)
            .await?;
            if let Some(plan) = plan {
                prepared.push(plan);
            }
        }
        capture_step(&mut receipt.work, meter, |meter| meter.release(observed_bytes).map_err(capture_demand_error))?;
        finish_prepared_paths(&checkout.content_stager(),
        &source_root,
        prepared,
        options.maximum_extent_spans,
        &mut mutations,
        &mut receipt,
        budget,
        cancellation, meter)
        .await?;
        capture_step(&mut receipt.work, meter, |meter| release_host_links(host_links, meter))?;
        if namespace_boundary == 0 || namespace_boundary == mutations.len() {
            apply_capture_transaction(checkout, mutations, &mut receipt, budget, cancellation, meter)
                .await?;
        } else {
            // Keep the watch batch atomic to callers, but compile the path-shape
            // changes before mutations that address their resulting bindings.
            // The authored compiler resolves those bindings against its input
            // checkout, even though the kernel executes each batch in order.
            let mut candidate = capture_candidate(checkout, &mut receipt.work, budget, cancellation, meter)?;
            let final_state = capture_step(&mut receipt.work, meter, |meter| {
                let bytes = (mutations.len() - namespace_boundary) as u64 * std::mem::size_of::<AuthoredMutation>() as u64;
                let copied = WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED };
                meter.work.admit(&copied, &meter.budget).map_err(CaptureError::Work)?;
                capture_allocate(meter, bytes)?;
                meter.record(copied).map_err(capture_demand_error)?;
                Ok(mutations.split_off(namespace_boundary))
            })?;
            apply_capture_transaction(&mut candidate,
            mutations,
            &mut receipt,
            budget,
            cancellation, meter)
            .await?;
            apply_capture_transaction(&mut candidate,
            final_state,
            &mut receipt,
            budget,
            cancellation, meter)
            .await?;
            *checkout = candidate;
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
        }.await;
        (result, capture_meter)
        },
    )?;
    let (result, mut capture_meter) = crate::obs::in_span(&span, future.as_mut()).await;
    drop(future);
    capture_meter.release(heap_bytes)
        .map_err(|error| OperationFailure::new(capture_demand_error(error), capture_meter.work))?;
    let result = result.map(|mut receipt| {
        receipt.work = capture_meter.work;
        receipt.value.capture.work = receipt.work;
        receipt
    }).map_err(|mut failure| {
        *failure.work = capture_meter.work;
        failure
    });
    crate::obs::measured_on(&span, result, |receipt| &receipt.work)
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
    meter: &mut DemandWork,
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
    meter: &mut DemandWork,
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
                capture_step(work, meter, |meter| meter.release(namespace_owned_bytes(&parent)?).map_err(capture_demand_error))?;
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

fn release_capture_set(paths: CaptureSet<NamespacePath>, meter: &mut DemandWork) -> Result<(), CaptureError> {
    let bytes = paths.storage_bytes();
    for path in paths {
        capture_drop_path(path, meter)?;
    }
    meter.release(bytes).map_err(capture_demand_error)
}

fn release_host_links(links: CaptureMap<[u8; 16], HostLinkSource>, meter: &mut DemandWork) -> Result<(), CaptureError> {
    let bytes = links.storage_bytes();
    for (_, source) in links {
        let path = match source { HostLinkSource::Pending(path) | HostLinkSource::Ready(path) => path };
        capture_drop_path(path, meter)?;
    }
    meter.release(bytes).map_err(capture_demand_error)
}

fn capture_within_any(path: &NamespacePath, ancestors: &[NamespacePath], exclude_self: bool, meter: &mut DemandWork) -> Result<bool, CaptureError> {
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
    meter: &mut DemandWork,
) -> Result<Vec<NamespacePath>, OperationFailure<CaptureError>> {
    let mut missing = Vec::new();
    let mut replaced = Vec::new();
    let ancestors_bytes = ancestors.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    for path in ancestors {
        let current = if capture_step(&mut receipt.work, meter, |meter| capture_within_any(&path, &replaced, false, meter))? {
            None
        } else {
            let remaining = capture_remaining(receipt.work, scope.budget, meter)
                .map_err(|error| OperationFailure::new(CaptureError::Work(error), receipt.work))?;
            let lookup = checkout
                .lookup_no_follow(&path, remaining, scope.cancellation)
                .await
                .map_err(|failure| map_engine_failure(failure, receipt.work, meter))?;
            receipt.work = merge_capture_work(receipt.work, lookup.work, meter)?;
            lookup.value.record
        };
        let observation = capture_step(&mut receipt.work, meter, |meter| {
            let host_path = namespace_to_host_path_measured(&path, meter)?;
            let observed = observe_host_path(scope.source_root, &host_path, meter)?;
            meter.release(capture_host_path_bytes(&host_path)).map_err(capture_demand_error)?;
            Ok(observed)
        })?;
        match observation {
            Some(observation) if observation.metadata.is_dir() => {
                if current.is_none_or(|record| record.kind != FileKind::Directory) {
                    if current.is_some() {
                        capture_step(&mut receipt.work, meter, |meter| capture_reserve(&mut replaced, 1, meter))?;
                        replaced.push(capture_path_clone(&path, &mut receipt.work, meter)?);
                    }
                    capture_step(&mut receipt.work, meter, |meter| capture_reserve(&mut missing, 1, meter))?;
                    missing.push(capture_path_clone(&path, &mut receipt.work, meter)?);
                    capture_step(&mut receipt.work, meter, |meter| ordinary.insert_measured(
                        path, (CurrentRecord::Known(current), CaptureIntent::Replace), meter))?;
                } else {
                    capture_step(&mut receipt.work, meter, |meter| capture_drop_path(path, meter))?;
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
    capture_step(&mut receipt.work, meter, |meter| meter.release(ancestors_bytes).map_err(capture_demand_error))?;
    if ordinary.len() > scope.maximum_paths {
        return Err(OperationFailure::new(
            CaptureError::InvalidOptions,
            receipt.work,
        ));
    }

    // Descendants of a replaced file or symlink were absent in the old view.
    for (path, (current, _)) in ordinary {
        if matches!(current, CurrentRecord::Lookup)
            && capture_step(&mut receipt.work, meter, |meter| capture_within_any(path, &replaced, true, meter))?
        {
            *current = CurrentRecord::Known(None);
        }
    }
    capture_step(&mut receipt.work, meter, |meter| release_capture_paths(replaced, meter))?;
    Ok(missing)
}

fn remap_renamed_directory_path(
    path: &NamespacePath,
    renames: &[(NamespacePath, NamespacePath)],
    limits: crate::model::VolumeLimits,
    meter: &mut DemandWork,
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
        let replacement = NamespacePath::new(components, limits).map_err(|_| CaptureError::InvalidOptions)?;
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
    meter: &mut DemandWork,
) -> Result<(), OperationFailure<CaptureError>> {
    let (mut future, heap_bytes) = crate::heap_future::in_heap_admitted(
        (meter, receipt),
        |(meter, receipt), bytes| {
            meter.budget = budget;
            capture_step(&mut receipt.work, meter, |meter| capture_allocate(meter, bytes))
        },
        move |(meter, receipt)| async move {
            let result = async {
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
    let mut roots = capture_step(&mut receipt.work, meter, |meter| capture_vec(ordinary.len(), meter))?;
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
        let (host_path, observation) = capture_step(&mut receipt.work, meter, |meter| {
            let host_path = namespace_to_host_path_measured(path, meter)?;
            let observation = observe_host_path(source_root, &host_path, meter)?;
            Ok((host_path, observation))
        })?;
        if observation.is_some_and(|observation| observation.metadata.is_dir()) {
            roots.push((host_path, capture_path_clone(path, &mut receipt.work, meter)?));
        } else {
            capture_step(&mut receipt.work, meter, |meter| capture_drop_host_path(host_path, meter))?;
        }
    }
    let mut paths = CaptureSet::new();
    for path in ordinary.keys() {
        let path = capture_path_clone(path, &mut receipt.work, meter)?;
        capture_step(&mut receipt.work, meter, |meter| paths.insert_measured(path, meter))?;
    }
    let roots_bytes = roots.capacity() as u64 * std::mem::size_of::<(CaptureHostPath, NamespacePath)>() as u64;
    for (host_path, volume_path) in roots {
        collect_host_subtree_paths(source_root,
        profile,
        limits,
        host_path,
        capture_path_clone(&volume_path, &mut receipt.work, meter)?,
        policy,
        maximum,
        &mut paths,
        &mut receipt.work,
        budget,
        cancellation, meter)?;
        let remaining = capture_remaining(receipt.work, budget, meter)
            .map_err(|error| OperationFailure::new(CaptureError::Work(error), receipt.work))?;
        let lookup = checkout
            .lookup_no_follow(&volume_path, remaining, cancellation)
            .await
            .map_err(|failure| map_engine_failure(failure, receipt.work, meter))?;
        receipt.work = merge_capture_work(receipt.work, lookup.work, meter)?;
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
            capture_step(&mut receipt.work, meter, |meter| capture_drop_path(volume_path, meter))?;
        }
    }
    capture_step(&mut receipt.work, meter, |meter| meter.release(roots_bytes).map_err(capture_demand_error))?;
    let paths_bytes = paths.storage_bytes();
    for path in paths {
        capture_step(&mut receipt.work, meter, |meter| ordinary.get_or_insert_measured(
            path, (CurrentRecord::Lookup, CaptureIntent::Complete), meter).map(|_| ()))?;
    }
    capture_step(&mut receipt.work, meter, |meter| meter.release(paths_bytes).map_err(capture_demand_error))?;
    Ok(())
            }.await;
            (result, meter, receipt)
        },
    )?;
    let (result, meter, receipt) = future.as_mut().await;
    drop(future);
    let released = meter.release(heap_bytes).map_err(capture_demand_error);
    receipt.work = meter.work;
    released.map_err(|error| OperationFailure::new(error, receipt.work))?;
    result
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
    meter: &mut DemandWork,
) -> Result<Option<PreparedPath>, OperationFailure<CaptureError>> {
    cancellation.check().map_err(|error| {
        OperationFailure::new(CaptureError::Engine(error.to_string()), receipt.work)
    })?;
    let host_path = capture_step(&mut receipt.work, meter, |meter| namespace_to_host_path_measured(&path, meter))?;
    let current = match current {
        CurrentRecord::Known(record) => record,
        CurrentRecord::Lookup => {
            let remaining = capture_remaining(receipt.work, budget, meter)
                .map_err(|error| OperationFailure::new(CaptureError::Work(error), receipt.work))?;
            let lookup = checkout
                .lookup_no_follow(&path, remaining, cancellation)
                .await
                .map_err(|failure| map_engine_failure(failure, receipt.work, meter))?;
            receipt.work = merge_capture_work(receipt.work, lookup.work, meter)?;
            lookup.value.record
        }
    };
    let observation = match observation {
        Some(observation) => Some(observation),
        None => capture_step(&mut receipt.work, meter, |meter| observe_host_path(source_root, &host_path, meter))?,
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
                if let Some(source) = capture_step(&mut receipt.work, meter, |meter| host_links.get_measured(&identity, meter))? {
                    ensure_current_host_node(source_root, &host_path, &snapshot, &mut receipt.work, budget, cancellation, meter)?;
                    receipt.changed_paths = checked_increment(receipt.changed_paths, receipt.work)?;
                    receipt.examined_paths =
                        checked_increment(receipt.examined_paths, receipt.work)?;
                    capture_step(&mut receipt.work, meter, |meter| capture_drop_host_path(host_path, meter))?;
                    return Ok(match source {
                        HostLinkSource::Pending(source) => Some(PreparedPath::PendingHardLink {
                            source: capture_path_clone(source, &mut receipt.work, meter)?,
                            destination: path,
                            current,
                        }),
                        HostLinkSource::Ready(source) => {
                            let source = capture_path_clone(source, &mut receipt.work, meter)?;
                            append_hard_link(source, path, current, mutations, &mut receipt.work, meter)?;
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
                capture_step(&mut receipt.work, meter, |meter| capture_reserve(mutations, 1, meter))?;
                mutations.push(AuthoredMutation::Remove {
                    path: capture_path_clone(&path, &mut receipt.work, meter)?,
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
                let remaining = capture_remaining(receipt.work, budget, meter).map_err(|error| {
                    OperationFailure::new(CaptureError::Work(error), receipt.work)
                })?;
                let prior = checkout
                    .read_metadata_by_id(record.file_id, remaining, cancellation)
                    .await
                    .map_err(|failure| map_engine_failure(failure, receipt.work, meter))?;
                receipt.work = merge_capture_work(receipt.work, prior.work, meter)?;
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
                capture_step(&mut receipt.work, meter, |meter| {
                    meter.release(namespace_owned_bytes(&path)?).map_err(capture_demand_error)?;
                    capture_drop_host_path(host_path, meter)
                })?;
                return Ok(None);
            }
            let linked_path = if linked_regular {
                Some(capture_path_clone(&path, &mut receipt.work, meter)?)
            } else { None };
            let prepared = if intent == CaptureIntent::MetadataOnly && exists_with_kind {
                ensure_current_host_node(source_root, &host_path, &snapshot, &mut receipt.work, budget, cancellation, meter)?;
                capture_step(&mut receipt.work, meter, |meter| capture_reserve(mutations, 1, meter))?;
                mutations.push(AuthoredMutation::SetMetadata {
                    path,
                    metadata: canonical_metadata,
                });
                capture_step(&mut receipt.work, meter, |meter| capture_drop_host_path(host_path, meter))?;
                None
            } else if host_kind == FileKind::Regular {
                // Staging opens the file once and proves it is still the
                // observed node before reading a byte.
                capture_step(&mut receipt.work, meter, |meter| capture_allocate(meter, std::mem::size_of::<PreparedRegular>() as u64))?;
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
                    meter,
                )?;
                capture_step(&mut receipt.work, meter, |meter| capture_drop_host_path(host_path, meter))?;
                None
            };
            receipt.changed_paths = checked_increment(receipt.changed_paths, receipt.work)?;
            if let Some(linked_path) = linked_path {
                let source = if prepared.is_some() {
                    HostLinkSource::Pending(linked_path)
                } else {
                    HostLinkSource::Ready(linked_path)
                };
                capture_step(&mut receipt.work, meter, |meter| host_links.insert_measured(
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
                capture_step(&mut receipt.work, meter, |meter| capture_reserve(mutations, 1, meter))?;
                mutations.push(AuthoredMutation::Remove {
                    path,
                    expected_file_id: Some(record.file_id),
                });
                receipt.changed_paths = checked_increment(receipt.changed_paths, receipt.work)?;
            } else {
                capture_step(&mut receipt.work, meter, |meter| meter.release(namespace_owned_bytes(&path)?).map_err(capture_demand_error))?;
            }
        }
    }
    capture_step(&mut receipt.work, meter, |meter| capture_drop_host_path(host_path, meter))?;
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
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &mut DemandWork,
) -> Result<(), OperationFailure<CaptureError>> {
    const PAGE: u32 = 256;
    let limits = checkout.volume_config().limits;
    // Descendants that carried their own hint are already queued (absent
    // paths are captured deepest first); a second remove of the same path
    // would find its source missing.
    let mut queued = CaptureSet::new();
    for mutation in mutations.iter() {
        capture_step(&mut receipt.work, meter, |meter| capture_charge(meter,
            WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
        if let AuthoredMutation::Remove { path, .. } = mutation
            && !capture_step(&mut receipt.work, meter, |meter| queued.contains_measured(path, meter))? {
            let path = capture_path_clone(path, &mut receipt.work, meter)?;
            capture_step(&mut receipt.work, meter, |meter| queued.insert_measured(path, meter))?;
        }
    }
    let mut pending = capture_step(&mut receipt.work, meter, |meter| capture_vec(1, meter))?;
    pending.push(capture_path_clone(root, &mut receipt.work, meter)?);
    let mut found: Vec<(NamespacePath, FileRecord)> = Vec::new();
    while let Some(directory) = pending.pop() {
        let mut after: Option<LogicalName> = None;
        loop {
            let remaining = capture_remaining(receipt.work, budget, meter)
                .map_err(|error| OperationFailure::new(CaptureError::Work(error), receipt.work))?;
            let page = checkout
                .list_directory_records(&directory, after.as_ref(), PAGE, remaining, cancellation)
                .await
                .map_err(|failure| map_engine_failure(failure, receipt.work, meter))?;
            receipt.work = merge_capture_work(receipt.work, page.work, meter)?;
            let page = page.value;
            let page_bytes = capture_step(&mut receipt.work, meter, |meter| capture_adopt_page(&page, meter))?;
            if page.has_more && page.entries.is_empty() {
                return Err(OperationFailure::new(CaptureError::Engine("directory pagination made no progress".into()), receipt.work));
            }
            for entry in &page.entries {
                let child = capture_step(&mut receipt.work, meter, |meter| {
                    let name = clone_capture_name(&entry.name, meter)?;
                    append_path_measured(&directory, name, limits, meter)
                })?;
                if entry.record.kind == FileKind::Directory {
                    capture_step(&mut receipt.work, meter, |meter| capture_reserve(&mut pending, 1, meter))?;
                    pending.push(capture_path_clone(&child, &mut receipt.work, meter)?);
                }
                if !capture_step(&mut receipt.work, meter, |meter| queued.contains_measured(&child, meter))? {
                    capture_step(&mut receipt.work, meter, |meter| capture_reserve(&mut found, 1, meter))?;
                    found.push((child, entry.record));
                } else {
                    capture_step(&mut receipt.work, meter, |meter| meter.release(namespace_owned_bytes(&child)?).map_err(capture_demand_error))?;
                }
            }
            let next = if page.has_more {
                let last = page.entries.last().ok_or_else(|| OperationFailure::new(
                    CaptureError::Engine("directory pagination made no progress".into()), receipt.work))?;
                Some(capture_step(&mut receipt.work, meter, |meter| clone_capture_name(&last.name, meter))?)
            } else { None };
            if let Some(previous) = after.take() {
                let bytes = previous.as_bytes().len() as u64;
                drop(previous);
                capture_step(&mut receipt.work, meter, |meter| meter.release(bytes).map_err(capture_demand_error))?;
            }
            let has_more = page.has_more;
            drop(page);
            capture_step(&mut receipt.work, meter, |meter| meter.release(page_bytes).map_err(capture_demand_error))?;
            after = next;
            if !has_more { break; }
        }
        capture_step(&mut receipt.work, meter, |meter| meter.release(namespace_owned_bytes(&directory)?).map_err(capture_demand_error))?;
    }
    capture_step(&mut receipt.work, meter, |meter| {
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
    capture_step(&mut receipt.work, meter, |meter| capture_sort_by(&mut found,
        |left, right| right.0.depth().cmp(&left.0.depth()), meter))?;
    capture_step(&mut receipt.work, meter, |meter| capture_reserve(mutations, found.len(), meter))?;
    let found_bytes = found.capacity() as u64 * std::mem::size_of::<(NamespacePath, FileRecord)>() as u64;
    for (path, record) in found {
        mutations.push(AuthoredMutation::Remove {
            path,
            expected_file_id: Some(record.file_id),
        });
        receipt.changed_paths = checked_increment(receipt.changed_paths, receipt.work)?;
    }
    capture_step(&mut receipt.work, meter, |meter| meter.release(found_bytes).map_err(capture_demand_error))?;
    Ok(())
}

fn capture_mutation_owned_bytes(
    mutations: &[AuthoredMutation], storage_bytes: u64, meter: &mut DemandWork,
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
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &mut DemandWork,
) -> Result<(), OperationFailure<CaptureError>> {
    let mutation_bytes = capture_step(&mut receipt.work, meter, |meter| capture_mutation_owned_bytes(
        &mutations, mutations.capacity() as u64 * std::mem::size_of::<AuthoredMutation>() as u64, meter))?;
    if mutations.is_empty() {
        drop(mutations);
        capture_step(&mut receipt.work, meter, |meter| meter.release(mutation_bytes).map_err(capture_demand_error))?;
        return Ok(());
    }
    capture_step(&mut receipt.work, meter, |meter| meter.release(mutation_bytes).map_err(capture_demand_error))?;
    let remaining = capture_remaining(receipt.work, budget, meter)
        .map_err(|error| OperationFailure::new(CaptureError::Work(error), receipt.work))?;
    let applied = checkout
        .apply_authored_bulk_transaction(mutations, remaining, cancellation)
        .await
        .map_err(|failure| map_engine_failure(failure, receipt.work, meter))?;
    receipt.work = merge_capture_work(receipt.work, applied.work, meter)?;
    drop(applied);
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
    meter: &mut DemandWork,
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
fn append_final_state(
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
    meter: &mut DemandWork,
) -> Result<(), OperationFailure<CaptureError>> {
    capture_step(work, meter, |meter| {
        capture_reserve(mutations, if exists_with_kind { 2 } else { 1 }, meter)?;
        match kind {
            FileKind::Regular => unreachable!("regular files are staged after preparation"),
            FileKind::Directory => mutations.push(if exists_with_kind {
                AuthoredMutation::SetMetadata { path, metadata: canonical_metadata }
            } else {
                AuthoredMutation::CreateDirectory { path, metadata: canonical_metadata }
            }),
            FileKind::SymbolicLink => {
                let target = read_link_bytes(source_root, host_path, meter)?;
                if exists_with_kind {
                    mutations.push(AuthoredMutation::Remove { path: clone_capture_path(&path, meter)?, expected_file_id: None });
                }
                let target = capture_link_target(target, meter)?;
                mutations.push(AuthoredMutation::CreateSymbolicLink { path, target, metadata: canonical_metadata });
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
        let observed = capture_stat(source_root, host_path, meter).map_err(capture_demand_error)?;
        ensure_same_host_node(&snapshot, &observed)
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the stager, root, prepared file, span bound, mutation list, receipt, budget, and cancellation are all used per file"
)]
async fn finish_prepared_regular<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    stager: &ContentStager<A, O>,
    source_root: &HostRoot,
    prepared: PreparedRegular,
    maximum_extent_spans: u32,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &mut DemandWork,
) -> Result<(), OperationFailure<CaptureError>> {
    let remaining = capture_remaining(receipt.work, budget, meter)
        .map_err(|error| OperationFailure::new(CaptureError::Work(error), receipt.work))?;
    let staged = stage_regular_body(
        stager,
        source_root,
        &prepared.host_path,
        &prepared.snapshot,
        maximum_extent_spans,
        WorkCounters::UNCHARGED,
        remaining,
        cancellation,
    )
    .await.map_err(|failure| map_capture_failure(failure, receipt.work, meter))?;
    receipt.work = merge_capture_work(receipt.work, staged.content.work, meter)?;
    capture_step(&mut receipt.work, meter, |meter| meter.adopt(staged.content.ranges.capacity() as u64
        * std::mem::size_of::<(HostDataRange, StagedContent)>() as u64).map_err(capture_demand_error))?;
    receipt.staged_file_bytes = receipt
        .staged_file_bytes
        .checked_add(staged.content.bytes)
        .ok_or_else(|| {
            OperationFailure::new(CaptureError::Work(WorkError::Overflow), receipt.work)
        })?;
    let host_path_bytes = capture_host_path_bytes(&prepared.host_path);
    drop(prepared.host_path);
    capture_step(&mut receipt.work, meter, |meter| meter.release(host_path_bytes)
        .map_err(capture_demand_error))?;
    append_staged_regular_state(
        prepared.path,
        staged.logical_bytes,
        prepared.exists_with_kind,
        prepared.canonical_metadata,
        staged.content.ranges,
        mutations,
        &mut receipt.work,
        meter,
    )?;
    Ok(())
}


#[allow(
    clippy::too_many_arguments,
    reason = "the stager, root, prepared paths, span bound, mutation list, receipt, budget, and cancellation are all used per batch"
)]
async fn finish_prepared_paths<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    stager: &ContentStager<A, O>,
    source_root: &HostRoot,
    prepared: Vec<PreparedPath>,
    maximum_extent_spans: u32,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &mut DemandWork,
) -> Result<(), OperationFailure<CaptureError>> {
    let mut regular = capture_step(&mut receipt.work, meter, |meter| capture_vec(prepared.len(), meter))?;
    let mut aliases = PendingHardLinks::new();
    let prepared_bytes = prepared.capacity() as u64 * std::mem::size_of::<PreparedPath>() as u64;
    for path in prepared {
        match path {
            PreparedPath::Regular(plan) => {
                regular.push(*plan);
                capture_step(&mut receipt.work, meter, |meter| meter.release(std::mem::size_of::<PreparedRegular>() as u64)
                    .map_err(capture_demand_error))?;
            }
            PreparedPath::PendingHardLink {
                source,
                destination,
                current,
            } => {
                capture_step(&mut receipt.work, meter, |meter| {
                    let pending = aliases.get_or_insert_measured(source, Vec::new(), meter)?;
                    capture_reserve(pending, 1, meter)?;
                    pending.push((destination, current));
                    Ok(())
                })?;
            }
        }
    }
    capture_step(&mut receipt.work, meter, |meter| meter.release(prepared_bytes).map_err(capture_demand_error))?;
    let regular_bytes = regular.capacity() as u64 * std::mem::size_of::<PreparedRegular>() as u64;
    for plan in regular {
        let pending = capture_step(&mut receipt.work, meter, |meter| aliases.remove_measured(&plan.path, meter))?;
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
                    capture_path_clone(key.as_ref().expect("preceding hard link retains its source"), &mut receipt.work, meter)?
                };
                append_hard_link(source, destination, current, mutations, &mut receipt.work, meter)?;
            }
            if let Some(key) = key {
                capture_step(&mut receipt.work, meter, |meter| capture_drop_path(key, meter))?;
            }
            capture_step(&mut receipt.work, meter, |meter| meter.release(pending_bytes).map_err(capture_demand_error))?;
        }
    }
    capture_step(&mut receipt.work, meter, |meter| meter.release(regular_bytes).map_err(capture_demand_error))?;
    if !aliases.is_empty() {
        return Err(OperationFailure::new(
            CaptureError::Engine("pending hard-link source was not staged".into()),
            receipt.work,
        ));
    }
    let aliases_bytes = aliases.storage_bytes();
    drop(aliases);
    capture_step(&mut receipt.work, meter, |meter| meter.release(aliases_bytes).map_err(capture_demand_error))?;
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
    stager: &ContentStager<A, O>,
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    snapshot: &HostSnapshot,
    maximum_extent_spans: u32,
    prior_work: WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<StagedRegularBody, OperationFailure<CaptureError>> {
    let remaining = prior_work.remaining(budget)
        .map_err(|error| OperationFailure::new(CaptureError::Work(error), prior_work))?;
    let mut meter = DemandWork::new(remaining, cancellation);
    let prepared = (|| {
        #[cfg(unix)]
        let file = source_root.open_file_positional_measured(host_path, &mut meter).map_err(capture_demand_error)?;
        #[cfg(windows)]
        let file = source_root.open_file_positional_wide_measured(host_path, &mut meter).map_err(capture_demand_error)?;
        let file = file.into_std();
        let metadata = source_root.stat_file_measured(&file, &mut meter).map_err(capture_demand_error)?;
        if !metadata.file_type().is_file() {
            return Err(CaptureError::Io(std::io::Error::other("host file kind changed during capture")));
        }
        ensure_same_host_node(snapshot, &metadata)?;
        let logical_bytes = metadata.len();
        let ranges = allocated_data_ranges_measured(&file, logical_bytes, maximum_extent_spans, &mut meter)
            .map_err(capture_demand_error)?;
        let positional = native_positional_file_measured(file, &mut meter)?;
        capture_allocate(&mut meter, std::mem::size_of::<acyclic_native_runtime::NativeFile>() as u64)?;
        let native = std::sync::Arc::new(positional);
        Ok((metadata, logical_bytes, ranges, native))
    })();
    let prepared_work = add_work(prior_work, meter.work)?;
    let (metadata, logical_bytes, ranges, native) = prepared
        .map_err(|error| OperationFailure::new(error, prepared_work))?;
    #[cfg(windows)]
    let ranges = {
        let old_bytes = ranges.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64;
        meter.release(old_bytes).map_err(|error| OperationFailure::new(capture_demand_error(error), prepared_work))?;
        let scan_budget = meter.remaining()
            .map_err(|error| OperationFailure::new(capture_demand_error(error), prepared_work))?;
        let local_prior = meter.work;
        let (ranges, scanned) = settled_sparse_ranges(&native, &metadata, ranges,
            maximum_extent_spans, WorkCounters::UNCHARGED, scan_budget, cancellation)
            .await.map_err(|failure| map_capture_failure(failure, local_prior, &mut meter)
                .map_with_prior_work(prior_work, |error| error))?;
        meter.merge(scanned).map_err(|error| OperationFailure::new(capture_demand_error(error), meter.work))?;
        meter.adopt(ranges.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64)
            .map_err(|error| OperationFailure::new(capture_demand_error(error), meter.work))?;
        ranges
    };
    #[cfg(unix)]
    let _ = metadata;
    let before_staging = add_work(prior_work, meter.work)?;
    let staging_budget = meter.remaining()
        .map_err(|error| OperationFailure::new(capture_demand_error(error), before_staging))?;
    let local_prior = meter.work;
    let mut content = stage_host_ranges(stager, &native, &ranges,
        WorkCounters::UNCHARGED, staging_budget, cancellation)
        .await.map_err(|failure| map_capture_failure(failure, local_prior, &mut meter)
            .map_with_prior_work(prior_work, |error| error))?;
    meter.merge(content.work)
        .map_err(|error| OperationFailure::new(capture_demand_error(error), meter.work))?;
    meter.adopt(content.ranges.capacity() as u64 * std::mem::size_of::<(HostDataRange, StagedContent)>() as u64)
        .map_err(|error| OperationFailure::new(capture_demand_error(error), meter.work))?;
    let verified = (|| {
        let file = native.as_file();
        let after = source_root.stat_file_measured(file, &mut meter).map_err(capture_demand_error)?;
        ensure_same_host_node(snapshot, &after)?;
        let current = capture_stat(source_root, host_path, &mut meter).map_err(capture_demand_error)?;
        ensure_same_host_node(snapshot, &current)
    })();
    let accumulated = add_work(prior_work, meter.work)?;
    verified.map_err(|error| OperationFailure::new(error, accumulated))?;
    content.work = meter.work;
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
    meter: &mut DemandWork,
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
                bytes: bytes::Bytes::new(), metadata: canonical_metadata });
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
        return Err(CaptureError::Io(std::io::Error::other(
            "host file changed during capture",
        )));
    }
    Ok(())
}

fn ensure_current_host_node(
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    expected: &HostSnapshot,
    work: &mut WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    meter: &mut DemandWork,
) -> Result<(), OperationFailure<CaptureError>> {
    with_capture_work(work, budget, cancellation, |meter| {
        let observed = capture_stat(source_root, host_path, meter).map_err(capture_demand_error)?;
        ensure_same_host_node(expected, &observed)
    }, meter)
}

#[cfg(test)]
fn test_host_stat(root: &HostRoot, path: &Path) -> Result<HostStat, CaptureError> {
    let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
    root.stat_measured(path, &mut meter).map_err(capture_demand_error)
}

#[cfg(test)]
fn test_held_stat(root: &HostRoot, file: &cap_std::fs::File) -> Result<HostStat, CaptureError> {
    let mut meter = DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
    root.stat_file_measured(&file.try_clone()?.into_std(), &mut meter).map_err(capture_demand_error)
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
        let root = HostRoot::open(temporary.path())?;
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        std::fs::rename(&current, temporary.path().join("displaced"))?;
        std::fs::rename(&replacement, &current)?;
        let opened = root.open_file(Path::new("current"))?;
        assert!(matches!(
            ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?),
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
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        let opened = root.open_file(Path::new("current"))?;
        ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?)?;
        std::fs::write(&current, b"changed body")?;
        assert!(matches!(
            ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?),
            Err(CaptureError::Io(_))
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
        let root = HostRoot::open(temporary.path())?;
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        let opened = root.open_file(Path::new("current"))?;
        ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?)?;
        std::fs::set_permissions(&current, std::fs::Permissions::from_mode(0o600))?;
        assert!(matches!(
            ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?),
            Err(CaptureError::Io(_))
        ));
        Ok(())
    }

    #[test]
    fn accepts_an_unobservable_creation_time_in_either_direction()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("current"), b"first")?;
        let root = HostRoot::open(temporary.path())?;
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
        let root = HostRoot::open(temporary.path())?;
        let metadata = test_host_stat(&root, Path::new("current"))?;
        let mut expected = HostSnapshot::from_metadata(&metadata)?;
        expected.metadata.created_ns = MetadataField::Value(123);
        let mut observed = expected;
        observed.metadata.created_ns = MetadataField::Value(124);
        assert!(matches!(
            ensure_same_host_snapshot(&expected, &observed),
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
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        let opened = root.open_file(Path::new("current"))?;
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
        let root = HostRoot::open(temporary.path())?;
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        std::fs::rename(&current, temporary.path().join("displaced"))?;
        std::fs::rename(&replacement, &current)?;
        assert!(matches!(
            ensure_current_host_node(&root, &test_host_path(Path::new("current")), &expected,
                &mut WorkCounters::UNCHARGED, WorkBudget::UNBOUNDED, &CancellationToken::new(),
                &mut DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())),
            Err(OperationFailure { error: CaptureError::Io(_), .. })
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
    native: &std::sync::Arc<acyclic_native_runtime::NativeFile>,
    ranges: &[HostDataRange],
    prior_work: WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<StagedHostRanges, OperationFailure<CaptureError>> {
    let remaining = prior_work.remaining(budget)
        .map_err(|error| OperationFailure::new(CaptureError::Work(error), prior_work))?;
    let mut meter = crate::demand::DemandWork::new(remaining, cancellation);
    let storage_bytes = ranges.len().checked_mul(std::mem::size_of::<(HostDataRange, StagedContent)>())
        .ok_or_else(|| OperationFailure::new(CaptureError::Work(WorkError::Overflow), prior_work))?;
    meter.allocate(storage_bytes as u64)
        .map_err(|error| OperationFailure::new(capture_demand_error(error), prior_work))?;
    let mut staged = Vec::new();
    let attempted = add_work(prior_work, meter.work)?;
    staged.try_reserve_exact(ranges.len())
        .map_err(|_| OperationFailure::new(CaptureError::InvalidOptions, attempted))?;
    let mut bytes = 0_u64;
    for range in ranges {
        let created = NativeRangeSource::new_measured(acyclic_native_runtime::AsyncRangeReader::new(
            std::sync::Arc::clone(native), range.offset, range.length,
        ), &mut meter, cancellation);
        let accumulated = add_work(prior_work, meter.work)?;
        let mut bounded = created.map_err(|error| OperationFailure::new(error, accumulated))?;
        let remaining = meter.remaining()
            .map_err(|error| OperationFailure::new(capture_demand_error(error), accumulated))?;
        let content = stager.stage(&mut bounded, range.length, remaining, cancellation).await;
        let measured = match &content { Ok(content) => content.work, Err(failure) => *failure.work };
        meter.merge(measured).map_err(|error| OperationFailure::new(
            capture_demand_error(error), accumulated,
        ))?;
        let stale = bounded.stale;
        let cancelled = bounded.cancelled;
        let budget_failure = bounded.budget_failure;
        let finished = bounded.finish(&mut meter).await;
        let accumulated = add_work(prior_work, meter.work)?;
        finished.map_err(|error| OperationFailure::new(error, accumulated))?;
        let content = content.map_err(|failure| OperationFailure::new(
            if let Some(error) = budget_failure { CaptureError::Work(error) }
                else if stale { CaptureError::StaleVersion }
                else if cancelled || cancellation.is_cancelled() { CaptureError::Cancelled }
                else { CaptureError::Engine(failure.error.to_string()) }, accumulated,
        ))?;
        if content.value.logical_bytes() != range.length {
            return Err(OperationFailure::new(
                CaptureError::StaleVersion,
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
        work: meter.work,
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
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(Vec<HostDataRange>, WorkCounters), OperationFailure<CaptureError>> {
    use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_SPARSE_FILE;

    const BLOCK: u64 = 4096;
    const CHUNK: u64 = 1 << 20;
    if metadata.file_attributes() & FILE_ATTRIBUTE_SPARSE_FILE.0 == 0 {
        return Ok((reported, WorkCounters::UNCHARGED));
    }
    let remaining = prior_work.remaining(budget)
        .map_err(|error| OperationFailure::new(CaptureError::Work(error), prior_work))?;
    let mut meter = crate::demand::DemandWork::new(remaining, cancellation);
    meter.adopt((reported.capacity() * std::mem::size_of::<HostDataRange>()) as u64)
        .map_err(|error| OperationFailure::new(capture_demand_error(error), prior_work))?;
    let mut settled = Vec::new();
    for range in &reported {
        let created = NativeRangeSource::new_measured(acyclic_native_runtime::AsyncRangeReader::new(
            std::sync::Arc::clone(native), range.offset, range.length,
        ), &mut meter, cancellation);
        let spent = add_work(prior_work, meter.work)?;
        let mut source = created.map_err(|error| OperationFailure::new(error, spent))?;
        let end = range.offset.checked_add(range.length).ok_or_else(|| {
            OperationFailure::new(CaptureError::Work(WorkError::Overflow), prior_work)
        })?;
        let mut position = range.offset;
        while position < end {
            let spent = add_work(prior_work, meter.work)?;
            let remaining = meter.remaining().map_err(|error| {
                OperationFailure::new(capture_demand_error(error), spent)
            })?;
            let wanted = (CHUNK - position % CHUNK).min(end - position);
            let read = source.read_chunk(wanted as usize, remaining, cancellation).await;
            let measured = crate::kernel::AsyncBlobSource::take_work(&mut source)
                .expect("native source always reports work");
            meter.merge(measured).map_err(|error| {
                OperationFailure::new(capture_demand_error(error), spent)
            })?;
            let spent = add_work(prior_work, meter.work)?;
            let chunk = read.map_err(|error| OperationFailure::new(
                capture_demand_error(native_demand_error(error)), spent,
            ))?;
            let owned = chunk.logical_owned_bytes;
            let bytes = chunk.bytes;
            meter.adopt(owned).map_err(|error| {
                OperationFailure::new(capture_demand_error(error), spent)
            })?;
            let read_end = position + bytes.len() as u64;
            let mut start = position;
            while start < read_end {
                let spent = add_work(prior_work, meter.work)?;
                meter.check().map_err(|error| OperationFailure::new(
                    capture_demand_error(error), spent,
                ))?;
                let block_end = start.saturating_add(BLOCK - start % BLOCK).min(read_end);
                let block = bytes.get((start - position) as usize..(block_end - position) as usize)
                    .ok_or_else(|| OperationFailure::new(CaptureError::InvalidOptions, spent))?;
                let mut nonzero = false;
                for byte in block {
                    let charge = meter.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED });
                    let spent = add_work(prior_work, meter.work)?;
                    charge.map_err(|error| OperationFailure::new(capture_demand_error(error), spent))?;
                    if *byte != 0 { nonzero = true; break; }
                }
                if nonzero {
                    let kept = keep_sparse_range(&mut settled, maximum_ranges as usize,
                        start, block_end - start, &mut meter);
                    let spent = add_work(prior_work, meter.work)?;
                    if !kept.map_err(|error| OperationFailure::new(error, spent))? {
                        return Ok((reported, meter.work));
                    }
                }
                start = block_end;
            }
            drop(bytes);
            let release = meter.release(owned);
            let spent = add_work(prior_work, meter.work)?;
            release.map_err(|error| OperationFailure::new(capture_demand_error(error), spent))?;
            position = read_end;
        }
        let finished = source.finish(&mut meter).await;
        let spent = add_work(prior_work, meter.work)?;
        finished.map_err(|error| OperationFailure::new(error, spent))?;
    }
    Ok((settled, meter.work))
}

#[cfg(windows)]
fn keep_sparse_range(
    ranges: &mut Vec<HostDataRange>,
    maximum: usize,
    offset: u64,
    length: u64,
    meter: &mut crate::demand::DemandWork,
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
        let available = meter.remaining().map_err(capture_demand_error)?
            .peak_allocation_bytes / size as u64;
        let next = preferred.min(usize::try_from(available).unwrap_or(usize::MAX))
            .max(ranges.len() + 1);
        let copied = ranges.len() * size;
        let delta = WorkCounters { bytes_copied: copied as u64, ..WorkCounters::UNCHARGED };
        meter.work.admit(&delta, &meter.budget).map_err(CaptureError::Work)?;
        meter.allocate((next * size) as u64).map_err(capture_demand_error)?;
        let mut replacement = Vec::new();
        replacement.try_reserve_exact(next).map_err(|_| CaptureError::InvalidOptions)?;
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
    unsafe { acyclic_native_runtime::NativeFile::from_overlapped_file_unchecked(file) }
}

/// The native file a handle from [`HostRoot::open_file_positional`] reads
/// through.
#[cfg(all(test, not(windows)))]
fn native_positional_file(
    file: std::fs::File,
) -> std::io::Result<acyclic_native_runtime::NativeFile> {
    acyclic_native_runtime::NativeFile::from_file(file)
}

struct CapturedReadWork(DemandWork);

impl std::ops::Deref for CapturedReadWork {
    type Target = DemandWork;
    fn deref(&self) -> &DemandWork { &self.0 }
}

impl std::ops::DerefMut for CapturedReadWork {
    fn deref_mut(&mut self) -> &mut DemandWork { &mut self.0 }
}

impl NativeDemandReadWork for CapturedReadWork {
    fn as_work(&mut self) -> &mut DemandWork { &mut self.0 }

    fn finish_read(&mut self, requested: usize, completed: usize) -> Result<(), DemandError> {
        self.0.check()?;
        if completed != requested { return Err(DemandError::StaleVersion); }
        Ok(())
    }
}

struct NativeRangeSource {
    reader: acyclic_native_runtime::AsyncRangeReader,
    meter: Option<std::sync::Arc<NativeDemandReadMeter<CapturedReadWork>>>,
    stale: bool,
    cancelled: bool,
    budget_failure: Option<WorkError>,
}


#[allow(unsafe_code, reason = "the measured host opener transfers its fresh positional handle")]
fn native_positional_file_measured(
    file: std::fs::File,
    work: &mut crate::demand::DemandWork,
) -> Result<acyclic_native_runtime::NativeFile, CaptureError> {
    let meter = NativeDemandReadMeter(std::sync::Mutex::new(work));
    #[cfg(windows)]
    let result = unsafe {
        acyclic_native_runtime::NativeFile::from_overlapped_file_measured_unchecked(file, &meter)
    };
    #[cfg(not(windows))]
    let result = acyclic_native_runtime::NativeFile::from_file_measured(file, &meter);
    result.map_err(|error| capture_demand_error(native_demand_error(error)))
}

#[cfg(windows)]
pub(crate) fn native_device_control_measured(
    file: &impl std::os::windows::io::AsRawHandle,
    code: u32,
    input: &[u8],
    output: &mut [u8],
    work: &mut crate::demand::DemandWork,
) -> Result<usize, crate::DemandError> {
    let meter = NativeDemandReadMeter(std::sync::Mutex::new(work));
    acyclic_native_runtime::device_control_in_place_measured(file, code, input, output, &meter)
        .map_err(native_demand_error)
}

impl NativeRangeSource {
    fn new_measured(
        reader: acyclic_native_runtime::AsyncRangeReader,
        caller: &mut crate::demand::DemandWork,
        cancellation: &CancellationToken,
    ) -> Result<Self, CaptureError> {
        caller.check().map_err(capture_demand_error)?;
        let meter = if reader.remaining() == 0 {
            None
        } else {
            caller.allocate(std::mem::size_of::<NativeDemandReadMeter<CapturedReadWork>>() as u64)
                .map_err(capture_demand_error)?;
            let budget = caller.remaining().map_err(capture_demand_error)?;
            Some(std::sync::Arc::new(NativeDemandReadMeter(std::sync::Mutex::new(
                CapturedReadWork(crate::demand::DemandWork::new(budget, cancellation)),
            ))))
        };
        Ok(Self { reader, meter, stale: false, cancelled: false, budget_failure: None })
    }

    async fn finish(self, caller: &mut crate::demand::DemandWork) -> Result<(), CaptureError> {
        if let Some(meter) = self.meter {
            // Retired wake snapshots can still own the metered node. Keep its
            // actual payload admitted until the existing callbacks retire.
            while std::sync::Arc::strong_count(&meter) != 1 {
                tokio::task::yield_now().await;
            }
            drop(meter);
            caller.release(std::mem::size_of::<NativeDemandReadMeter<CapturedReadWork>>() as u64)
                .map_err(capture_demand_error)?;
        }
        Ok(())
    }

    async fn read_chunk(
        &mut self,
        maximum: usize,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> std::io::Result<crate::kernel::OwnedSourceChunk> {
        cancellation.check().map_err(|error| {
            self.cancelled = true;
            std::io::Error::new(std::io::ErrorKind::Interrupted, error)
        })?;
        let maximum = maximum.min(usize::try_from(self.reader.remaining()).unwrap_or(usize::MAX));
        if maximum == 0 {
            return Ok(crate::kernel::OwnedSourceChunk {
                bytes: bytes::Bytes::new(), logical_owned_bytes: 0,
            });
        }
        // A smaller byte allowance is rejection, never a shortened successful
        // file. Caller-owned live buffers have already been deducted upstream.
        WorkCounters { source_bytes_read: maximum as u64, ..WorkCounters::UNCHARGED }
            .verify(budget).map_err(|error| {
                self.budget_failure = Some(error);
                std::io::Error::other(error)
            })?;
        let meter = self.meter.as_ref().expect("native meter initialized");
        meter.lock().budget = budget;
        let observer: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter> = meter.clone();
        match self.reader.read_measured(maximum, observer).await {
            Ok(result) => Ok(crate::kernel::OwnedSourceChunk {
                logical_owned_bytes: result.buffer_capacity as u64, bytes: result.bytes,
            }),
            Err(error) => {
                if let Some(cause) = error.get_ref().and_then(|error|
                    error.downcast_ref::<crate::DemandError>()) {
                    match cause {
                        crate::DemandError::StaleVersion => self.stale = true,
                        crate::DemandError::Cancelled => self.cancelled = true,
                        crate::DemandError::Work(error) => self.budget_failure = Some(*error),
                        _ => {}
                    }
                }
                Err(error)
            }
        }
    }
}

impl crate::kernel::AsyncBlobSource for NativeRangeSource {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        let maximum = destination.len().min(usize::try_from(self.reader.remaining()).unwrap_or(usize::MAX));
        WorkCounters { bytes_copied: maximum as u64, ..WorkCounters::UNCHARGED }
            .verify(budget).map_err(|error| {
                self.budget_failure = Some(error);
                std::io::Error::other(error)
            })?;
        let chunk = self.read_chunk(maximum, budget, cancellation).await?;
        let bytes = &chunk.bytes;
        destination.get_mut(..bytes.len())
            .ok_or_else(|| std::io::Error::other("native read exceeded destination"))?
            .copy_from_slice(bytes);
        if let Some(meter) = &self.meter {
            meter.lock().record(WorkCounters {
                bytes_copied: bytes.len() as u64,
                ..WorkCounters::UNCHARGED
            }).map_err(native_demand_read_error)?;
        }
        Ok(bytes.len())
    }

    async fn read_owned(
        &mut self,
        maximum: usize,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> std::io::Result<Option<crate::kernel::OwnedSourceChunk>> {
        self.read_chunk(maximum, budget, cancellation).await.map(Some)
    }

    fn take_work(&mut self) -> Option<WorkCounters> {
        if let Some(meter) = &self.meter {
            Some(std::mem::take(&mut meter.lock().work))
        } else {
            Some(WorkCounters::UNCHARGED)
        }
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

    fn reader(path: &Path, expected: u64) -> std::io::Result<acyclic_native_runtime::AsyncRangeReader> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt as _;
            options.custom_flags(windows::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED.0);
        }
        Ok(acyclic_native_runtime::AsyncRangeReader::new(
            std::sync::Arc::new(native_positional_file(options.open(path)?)?), 0, expected,
        ))
    }

    #[tokio::test]
    async fn empty_native_range_has_no_callback_work() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("empty");
        std::fs::write(&path, [])?;
        let cancellation = CancellationToken::new();
        let mut caller = DemandWork::new(WorkCounters::UNCHARGED, &cancellation);
        let mut source = NativeRangeSource::new_measured(reader(&path, 0)?, &mut caller, &cancellation)?;
        let chunk = source.read_owned(8, WorkCounters::UNCHARGED, &cancellation).await?
            .ok_or("native source did not return its owned packet")?;
        assert!(chunk.bytes.is_empty());
        assert_eq!(chunk.logical_owned_bytes, 0);
        assert_eq!(source.take_work(), Some(WorkCounters::UNCHARGED));
        source.finish(&mut caller).await?;
        assert_eq!(caller.work, WorkCounters::UNCHARGED);
        Ok(())
    }

    #[tokio::test]
    async fn native_byte_refusal_does_not_shorten_the_file() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("bytes");
        std::fs::write(&path, b"abc")?;
        let cancellation = CancellationToken::new();
        let mut caller = DemandWork::new(budget(), &cancellation);
        let mut source = NativeRangeSource::new_measured(reader(&path, 3)?, &mut caller, &cancellation)?;
        let limited = WorkCounters { source_bytes_read: 2, ..caller.remaining()? };
        assert!(source.read_owned(8, limited, &cancellation).await.is_err());
        let work = source.take_work().ok_or("native source omitted refusal work")?;
        assert_eq!(work.source_bytes_read, 0);
        assert_eq!(work.backend_read_operations, 0);
        assert_eq!(source.reader.remaining(), 3);
        source.finish(&mut caller).await?;
        Ok(())
    }

    #[tokio::test]
    async fn native_truncation_is_typed_stale_with_actual_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("short");
        std::fs::write(&path, b"abcdefgh")?;
        let opened = reader(&path, 8)?;
        std::fs::write(&path, b"abc")?;
        let cancellation = CancellationToken::new();
        let mut caller = DemandWork::new(budget(), &cancellation);
        let mut source = NativeRangeSource::new_measured(opened, &mut caller, &cancellation)?;
        let error = match source.read_owned(8, caller.remaining()?, &cancellation).await {
            Err(error) => error,
            Ok(_) => return Err("truncated pinned range succeeded".into()),
        };
        assert!(matches!(native_demand_error(error), DemandError::StaleVersion));
        assert!(source.stale);
        let work = source.take_work().ok_or("native source omitted partial work")?;
        assert_eq!(work.source_bytes_read, 3);
        assert_eq!(work.backend_read_operations, if cfg!(windows) { 3 } else { 2 });
        assert!(work.allocation_operations >= 3);
        assert!(work.peak_allocation_bytes >= 8);
        caller.merge(work)?;
        source.finish(&mut caller).await?;
        assert_eq!(caller.work.source_bytes_read, 3);
        Ok(())
    }

    #[tokio::test]
    async fn native_owned_packet_transfers_capacity_and_drains_work() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("owned");
        std::fs::write(&path, b"abc")?;
        let cancellation = CancellationToken::new();
        let mut caller = DemandWork::new(budget(), &cancellation);
        let mut source = NativeRangeSource::new_measured(reader(&path, 3)?, &mut caller, &cancellation)?;
        let chunk = source.read_owned(8, caller.remaining()?, &cancellation).await?
            .ok_or("native source did not return its owned packet")?;
        assert_eq!(chunk.bytes.as_ref(), b"abc");
        assert!(chunk.logical_owned_bytes >= chunk.bytes.len() as u64);
        let work = source.take_work().ok_or("native source omitted actual work")?;
        assert_eq!(work.source_bytes_read, 3);
        assert_eq!(source.take_work(), Some(WorkCounters::UNCHARGED));
        caller.merge(work)?;
        caller.adopt(chunk.logical_owned_bytes)?;
        let eof = source.read_owned(8, caller.remaining()?, &cancellation).await?
            .ok_or("native source did not return EOF")?;
        assert!(eof.bytes.is_empty());
        assert_eq!(eof.logical_owned_bytes, 0);
        assert_eq!(source.take_work(), Some(WorkCounters::UNCHARGED));
        source.finish(&mut caller).await?;
        let owned = chunk.logical_owned_bytes;
        drop(chunk);
        caller.release(owned)?;
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
        let mut work = DemandWork::new(budget(), &cancellation);
        let metadata_bytes = std::mem::size_of::<NativeDemandReadMeter>() as u64;
        work.allocate(metadata_bytes)?;
        let shared = std::sync::Arc::new(NativeDemandReadMeter(std::sync::Mutex::new(work)));
        let mut reader = reader(&path, 8)?;
        let result = reader.read_measured(8, shared.clone()).await?;
        assert_eq!(result.bytes.as_ref(), b"abc");
        assert_eq!(result.buffer_capacity, 8);
        assert_eq!(shared.lock().work.source_bytes_read, 3);
        assert_eq!(shared.lock().work.backend_read_operations, if cfg!(windows) { 3 } else { 2 });
        drop(result);
        while std::sync::Arc::strong_count(&shared) != 1 {
            tokio::task::yield_now().await;
        }
        let mut work = std::sync::Arc::try_unwrap(shared)
            .map_err(|_| "ordinary reader retained its completed meter")?
            .0.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
        work.release(metadata_bytes)?;
        assert_eq!(work.work.source_bytes_read, 3);
        Ok(())
    }

    #[tokio::test]
    async fn queued_capture_restores_original_cancelled_probe_prefix()
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
        let mut work = DemandWork::new(original_budget, &cancellation);
        let path_bytes = path.capacity() as u64;
        work.adopt(path_bytes)?;
        let caller_buffer = Vec::from(b"held".as_slice());
        let caller_bytes = caller_buffer.capacity() as u64;
        work.adopt(caller_bytes)?;
        let queued = CaptureQueuedWork::new(&mut work)
            .map_err(|failure| failure.error)?;
        let worker = queued.shared().clone();
        let task_meter = queued.shared().clone();
        let stop = cancellation.clone();
        let task = acyclic_native_runtime::run_blocking_io_measured(move || {
            let mut work = worker.lock();
            let root = HostRoot::open_measured(&path, &mut work);
            drop(path);
            work.release(path_bytes)?;
            drop(root?);
            stop.cancel();
            work.check()
        }, task_meter)?;
        assert!(matches!(task.await?, Err(DemandError::Cancelled)));
        queued.retire().await?;
        assert_eq!(work.budget, original_budget);
        assert!(work.cancellation().is_cancelled());
        assert!(work.work.backend_read_operations > 0);
        assert!(work.work.allocation_operations > 0);
        assert_eq!(work.live_allocation_bytes(), caller_bytes);
        drop(caller_buffer);
        work.release(caller_bytes)?;
        Ok(())
    }

    #[tokio::test]
    async fn queued_capture_restores_original_after_worker_fault()
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
        let mut work = DemandWork::new(original_budget, &cancellation);
        let path_bytes = path.capacity() as u64;
        work.adopt(path_bytes)?;
        let queued = CaptureQueuedWork::new(&mut work)
            .map_err(|failure| failure.error)?;
        let worker = queued.shared().clone();
        let task_meter = queued.shared().clone();
        let task = acyclic_native_runtime::run_blocking_io_measured(move || {
            let mut work = worker.lock();
            let root = HostRoot::open_measured(&path, &mut work);
            drop(path);
            work.release(path_bytes).expect("transferred path claim must retire");
            drop(root.expect("fault fixture root must open"));
            panic!("native capture fault after the completed held-root probe");
        }, task_meter)?;
        assert_eq!(task.await.expect_err("worker fault must fail").kind(), std::io::ErrorKind::Other);
        drop(queued);
        assert_eq!(work.budget, original_budget);
        assert!(!work.cancellation().is_cancelled());
        assert!(work.work.backend_read_operations > 0);
        assert!(work.work.allocation_operations > 0);
        assert_eq!(work.live_allocation_bytes(), 0);
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
        let held = acyclic_native_runtime::NativeFile::from_file(std::fs::File::open(&path)?)?;
        let transferred = std::fs::File::open(&path)?;
        let cancellation = CancellationToken::new();
        let mut permitted = budget();
        permitted.peak_allocation_bytes = std::mem::size_of::<std::fs::File>() as u64;
        permitted.items_examined = 16 * 1024;
        let mut work = DemandWork::new(permitted, &cancellation);
        let meter = NativeDemandReadMeter(std::sync::Mutex::new(&mut work));
        let alias = acyclic_native_runtime::NativeFile::from_file_measured(transferred, &meter)?;
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

fn open_source_root(options: &CaptureOptions, meter: &mut DemandWork)
    -> Result<HostRoot, OperationFailure<CaptureError>> {
    let root = HostRoot::open_measured(&options.source_root, meter)
        .map_err(|error| OperationFailure::new(capture_demand_error(error), meter.work))?;
    if root.identity() != options.expected_root_identity {
        return Err(OperationFailure::new(CaptureError::RootChanged, meter.work));
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
        let root = HostRoot::open(directory)?;
        let file = root.open_file_positional(Path::new(name))?;
        let metadata = test_host_stat(&root, Path::new(name))?;
        let native = std::sync::Arc::new(native_positional_file(file.into_std())?);
        let (ranges, _) = settled_sparse_ranges(
            &native,
            &metadata,
            reported,
            maximum_ranges,
            WorkCounters::default(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| std::io::Error::other(failure.error.to_string()))?;
        Ok(ranges
            .into_iter()
            .map(|range| (range.offset, range.length))
            .collect())
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
        let root = HostRoot::open(directory.path())?;
        let file = root.open_file_positional(Path::new("sparse.bin"))?;
        let metadata = test_host_stat(&root, Path::new("sparse.bin"))?;
        let native = std::sync::Arc::new(native_positional_file(file.into_std())?);
        let scan = |budget: WorkBudget, cancellation: CancellationToken| {
            let (native, metadata) = (std::sync::Arc::clone(&native), metadata.clone());
            async move {
                settled_sparse_ranges(
                    &native,
                    &metadata,
                    whole(),
                    16,
                    WorkCounters::default(),
                    budget,
                    &cancellation,
                )
                .await
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

fn capture_link_target(target: Vec<u8>, meter: &mut DemandWork) -> Result<bytes::Bytes, CaptureError> {
    meter.check().map_err(capture_demand_error)?;
    if target.len() != target.capacity() {
        meter.allocate_counted(0, 1).map_err(capture_demand_error)?;
    }
    Ok(bytes::Bytes::from(target))
}

fn read_link_bytes(root: &HostRoot, path: &CaptureHostPath, meter: &mut DemandWork) -> Result<Vec<u8>, CaptureError> {
    let maximum = meter.remaining().map_err(capture_demand_error)?.source_bytes_read;
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt as _;
        Ok(root.read_link_measured(path, maximum, meter).map_err(capture_demand_error)?.into_os_string().into_vec())
    }
    #[cfg(windows)]
    {
        let units = root.read_link_wide_measured(path, maximum, meter).map_err(capture_demand_error)?;
        let length = units.len().checked_mul(2).ok_or(CaptureError::Work(WorkError::Overflow))?;
        let mut bytes = capture_vec(length, meter)?;
        for unit in &units {
            capture_charge(meter, WorkCounters { bytes_copied: 2, ..WorkCounters::UNCHARGED })?;
            bytes.extend_from_slice(&unit.to_le_bytes());
        }
        let units_bytes = units.capacity() as u64 * 2;
        drop(units);
        meter.release(units_bytes).map_err(capture_demand_error)?;
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

#[allow(
    clippy::needless_pass_by_value,
    reason = "used as a map_err adapter, which hands over the failure by value"
)]
fn map_engine_failure<E: std::fmt::Display>(
    failure: OperationFailure<E>,
    prior: WorkCounters,
    meter: &mut DemandWork,
) -> OperationFailure<CaptureError> {
    match merge_capture_work(prior, *failure.work, meter) {
        Ok(work) => OperationFailure::new(CaptureError::Engine(failure.error.to_string()), work),
        Err(failure) => failure,
    }
}

fn map_capture_failure(
    failure: OperationFailure<CaptureError>,
    prior: WorkCounters,
    meter: &mut DemandWork,
) -> OperationFailure<CaptureError> {
    match merge_capture_work(prior, *failure.work, meter) {
        Ok(work) => OperationFailure::new(failure.error, work),
        Err(failure) => failure,
    }
}

fn add_work(
    left: WorkCounters,
    right: WorkCounters,
) -> Result<WorkCounters, OperationFailure<CaptureError>> {
    left.checked_add(right)
        .map_err(|error| OperationFailure::new(error.into(), left))
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
            .create_workspace("root-capture-regression")
            .await?;
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await?;
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
        let mut reader = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::read_only_pinned(),
            )
            .await?;
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
            .create_workspace("capture-spans")
            .await?;
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await?;
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
            .create_workspace("filtered-capture")
            .await?;
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
            )
            .await?;
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
