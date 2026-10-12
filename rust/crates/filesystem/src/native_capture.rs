//! Bounded host-state capture into one atomic sparse checkout transaction.

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
    ContentStager, NativeRootIdentity, OperationFailure, OperationReceipt, StagedContent,
    WatchBatch, WatchChange, WatchEpoch, WatchInvalidationReason, WatchSequence, WorkBudget,
    WorkCounters, WorkError,
};

/// Returns the stable identity of a no-follow, capability-held capture root.
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
    #[cfg(feature = "native-mount")]
    pub(crate) fn new_with_native_work(
        root_identity: NativeRootIdentity,
        read_only: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<std::sync::Arc<Self>, crate::NativeTreeMaterializationError> {
        let allocation = work.claim_allocation(std::mem::size_of::<Self>() as u64, 1)?;
        Ok(std::sync::Arc::new(Self {
            root_identity, read_only, _allocation: Some(allocation),
        }))
    }
    /// No caller-owned input is adopted: scalars are inline and a metered shared witness already carries its own intrinsic allocation claim.
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
        // Fields absent from the selected SDK generation were not restored by materialization. Host-created defaults cannot become an apparent source edit merely because the view was created or read.
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

    /// A directory modified in place is covered by its entries' own events: its unnamed entries (unobserved in a lazy checkout) stay out, while a directory that arrived by rename brings its whole subtree.
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
}

/// A watcher interval acknowledged only after its complete capture transaction changes the checkout candidate.
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

/// Reads a bounded set of final host states and applies one atomic checkout transaction.
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
        let original = capture_original_work(budget, cancellation,
)?;
        let result = async {
            validate_path_count(paths, options).map_err(OperationFailure::before_work)?;
            let native = capture_native_original(&original)?;
            let scope = CaptureWorkScope::new(&native)?;
            let result = async {
            let source_root = open_source_root(options, &mut &scope)?;
            let result = capture_paths_from_root(checkout,
    paths,
    policy,
    options.maximum_extent_spans,
    &source_root,
    cancellation,
    &scope,
).await;
            drop(source_root);
            result
            }.await;
            capture_scope_result(scope, result)
        }.await;
        capture_original_result(&original, result)
    }).await;
    crate::obs::measured_on(&span, result, |receipt| &receipt.work)
}

/// Captures a large explicit path set as bounded transactions on one private checkout candidate.
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
    let original = capture_original_work(budget, cancellation,
)?;
    let native = capture_native_original(&original)?;
    let result = capture_paths_batched_with_native_work(
        checkout, paths, options, batch_size, cancellation, baseline, &native,
    ).await;
    capture_original_result(&original, result)
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
    capture_scope_result(scope, result)
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
) -> Result<CaptureReceipt, CaptureError> {
    let cancellation = {
        let mut original = meter;
        validate_path_count(paths, options)
            ?;
        if batch_size == 0 || paths.len() > MAX_NATIVE_EXACT_CAPTURE_PATHS {
            return Err(CaptureError::InvalidOptions);
        }
        cancellation.check()
            .map_err(|_| CaptureError::Cancelled)?;
        original.check()
            .map_err(capture_demand_error)?;
        if paths.is_empty() {

            return Ok(CaptureReceipt::default());
        }
        original.leaf(|work| work.cancellation().clone())
    };
    capture_in_heap_with_meter(|| async move {
        let mut host_work = meter;
        let source_root = open_source_root(options, &mut host_work)?;
        let (mut receipt, mut candidate, mut states, unique) = {
            let mut original = meter;
            let mut unique = clone_capture_paths(paths, None, &mut original)
                ?;
            capture_sort_by(&mut unique, Ord::cmp, &mut original)
                ?;
            if capture_has_duplicate_paths(&unique, &mut original)
                ? {
                return Err(CaptureError::InvalidOptions);
            }
            let mut receipt = CaptureReceipt::default();
            let candidate = capture_candidate(checkout, &cancellation, &mut original)?;
            let states = capture_vec(unique.len(), &mut original)
                ?;
            (receipt, candidate, states, unique)
        };
        let observed = observe_unique_paths(unique,
    &source_root,
    &cancellation,
    &mut host_work,
)?;
        let observations_bytes = observed.capacity() as u64 * std::mem::size_of::<ObservedPath>() as u64;
        let mut observations = observed.into_iter();
        loop {
            let (page, paths, lookup_cancellation) = {
                let mut original = meter;
                let count = observations.len().min(batch_size);
                let mut page = capture_vec(count, &mut original)
                    ?;
                page.extend(observations.by_ref().take(count));
                if page.is_empty() {
                    break;
                }
                let mut paths = capture_vec(page.len(), &mut original)
                    ?;
                for (_, path) in &page {
                    paths.push(clone_capture_path(path, &mut original)
                        ?);
                }
                (page, paths, original.leaf(|work| work.cancellation().clone()))
            };
            let lookup = capture_sdk_in_heap_with_meter(
                |native| candidate.lookup_batch_no_follow_with_native_work(&paths, native, &lookup_cancellation),
                meter, &lookup_cancellation,
            ).await?;
            {
                let mut original = meter;

                let page_bytes = page.capacity() as u64 * std::mem::size_of::<ObservedPath>() as u64;
                states.extend(
                    page.into_iter().zip(lookup.0.entries)
                        .map(|((observation, path), entry)| (observation, path, entry.record)),
                );
                drop(lookup.1);
                { let meter = &mut original; meter.release(page_bytes).map_err(capture_demand_error)?;
                    release_capture_paths(paths, meter) }?;
            }
        }
        drop(observations);
        {
            let mut original = meter;
            (&mut original).release(observations_bytes).map_err(capture_demand_error)?;
            sort_capture_states(&mut states, (&mut original))?;
        }
        let mut host_links = CaptureMap::new();
        let states_bytes = states.capacity() as u64 * std::mem::size_of::<CapturePathState>() as u64;
        let mut states = states.into_iter();
        loop {
            let batch = {
                let mut original = meter;
                let count = states.len().min(batch_size);
                let mut batch = capture_vec(count, &mut original)
                    ?;
                batch.extend(states.by_ref().take(count));
                if batch.is_empty() {
                    break;
                }
                batch
            };
            let captured = capture_ranked_paths_from_root(&mut candidate,
    batch,
    &mut host_links,
    options.maximum_extent_spans,
    &source_root,
    &cancellation,
    baseline,
    meter,
).await?;
            for source in host_links.values_mut() {
                if let HostLinkSource::Pending(path) = source {
                    let ready = std::mem::replace(path, NamespacePath::new(Vec::new(), checkout.volume_config().limits)
                        .map_err(CaptureError::Path)?);
                    *source = HostLinkSource::Ready(ready);
                }
            }
            receipt.examined_paths = receipt.examined_paths.checked_add(captured.examined_paths)
                .ok_or(CaptureError::Work(WorkError::Overflow))?;
            receipt.changed_paths = receipt.changed_paths.checked_add(captured.changed_paths)
                .ok_or(CaptureError::Work(WorkError::Overflow))?;
            receipt.staged_file_bytes = receipt.staged_file_bytes.checked_add(captured.staged_file_bytes)
                .ok_or(CaptureError::Work(WorkError::Overflow))?;
        }
        drop(states);
        {
            let mut original = meter;
            { let meter = &mut original; meter.release(states_bytes).map_err(capture_demand_error)?;
                release_host_links(host_links, meter) }?;
            original.check()
                .map_err(capture_demand_error)?;
        }
        cancellation.commit_if_live(|| { *checkout = candidate; })
            .map_err(|_| CaptureError::Cancelled)?;
        Ok(receipt)
    }, meter.native()).await
}

/// Reconciles one host directory and every descendant as one authored transaction.
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

fn capture_original_work(budget: WorkBudget, cancellation: &CancellationToken,
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

struct CaptureWorkScope {
    native: crate::NativeMaterializationWork,
    custody: Option<std::sync::Arc<crate::demand::CaptureTransientCustody>>,
    owns_custody: bool,
}

impl CaptureWorkScope {
    fn new(native: &crate::NativeMaterializationWork)
        -> Result<Self, OperationFailure<CaptureError>>
    {
        let custody = crate::demand::CaptureTransientCustody::new_native(native)
            .map_err(|error| OperationFailure::new(CaptureError::NativeKernel(error), native.receipt()))?;
        let view = native.with_capture_custody(&custody)
            .map_err(|error| OperationFailure::new(CaptureError::NativeKernel(error), native.receipt()))?;
        Ok(Self {
            native: view,
            custody: Some(custody),
            owns_custody: true,
        })
    }

    fn original(&self) -> &std::sync::Arc<NativeDemandReadMeter> { self.native.native_read_meter() }
    fn native(&self) -> &crate::NativeMaterializationWork { &self.native }

    fn transfer_allocation(&self, bytes: u64) -> Result<crate::NativeAllocation, CaptureError> {
        self.native.transfer_capture_allocation(bytes,
            self.custody.as_ref().expect("capture scope is live"))
            .map_err(CaptureError::NativeKernel)
    }

    fn release(&mut self) -> Result<(), OperationFailure<CaptureError>> {
        // Aliases do not own retirement. Accepted workers and native source packets retain the token until their physical payloads have retired.
        if !std::mem::replace(&mut self.owns_custody, false) { return Ok(()); }
        let custody = self.custody.take().expect("capture scope is live");
        while std::sync::Arc::strong_count(&custody) != 1 {
            std::thread::yield_now();
        }
        let bytes = custody.take_retained_bytes();
        drop(custody);
        let (released, completed) = self.native.with_original_leaf(|work| {
            (work.without_capture_custody(|work| work.release(bytes)), work.work)
        });
        released.map_err(|error| OperationFailure::new(capture_demand_error(error), completed))
    }
}

impl Clone for CaptureWorkScope {
    fn clone(&self) -> Self {
        Self {
            native: self.native.clone(),
            custody: self.custody.as_ref().map(std::sync::Arc::clone),
            owns_custody: false,
        }
    }
}

impl Drop for CaptureWorkScope {
    fn drop(&mut self) { let _ = self.release(); }
}

trait CaptureNativeWork:
    crate::native_host::NativeHostWork<Error = DemandError, Allocation = u64, CleanupWrite = u64>
{
    /// Only fixed ledger, cancellation, and counter bookkeeping belongs here.
    fn leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T;
    fn completed(&mut self) -> WorkCounters { self.leaf(|work| work.work) }
}

impl CaptureNativeWork for DemandWork {
    fn leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T { operation(self) }
}

impl CaptureNativeWork for &CaptureWorkScope {
    fn leaf<T>(&mut self, operation: impl FnOnce(&mut DemandWork) -> T) -> T {
        self.native().with_original_leaf(operation)
    }
}

impl crate::native_host::NativeHostWork for &CaptureWorkScope {
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
    fn check(&self) -> Result<(), DemandError> { self.native().with_original_leaf(|work| work.check()) }
    fn admit(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.native().with_original_leaf(|work| work.admit(delta))
    }
    fn record(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.native().with_original_leaf(|work| work.record(delta))
    }
    fn started_io(&mut self, delta: WorkCounters) -> Result<(), DemandError> {
        self.native().with_original_leaf(|work| work.charge(delta))
    }
    fn claim(&mut self, bytes: u64) -> Result<u64, DemandError> {
        self.native().with_original_leaf(|work| work.allocate(bytes))?;
        Ok(bytes)
    }
    fn reserve_opaque_decode(&mut self, bytes: u64) -> Result<u64, DemandError> {
        self.native().with_original_leaf(|work| work.adopt(bytes))?;
        Ok(bytes)
    }
    fn reconcile_allocation(&mut self, allocation: &mut u64, actual: u64) -> Result<(), DemandError> {
        self.native().with_original_leaf(|work| work.reconcile_native_allocation(allocation, actual))
    }
    fn release(&mut self, bytes: u64) -> Result<(), DemandError> {
        self.native().with_original_leaf(|work| work.release(bytes))
    }
    fn attempt_io<T>(
        &mut self, delta: WorkCounters, operation: impl FnOnce() -> std::io::Result<T>,
    ) -> Result<T, DemandError> {
        // Consume the real attempt's allowance before a reentrant host can spend it. The operation immediately follows with the lock retired.
        self.native().with_original_leaf(|work| work.charge(delta))?;
        operation().map_err(DemandError::from)
    }
    fn reserve_cleanup_write(&mut self) -> Result<u64, DemandError> {
        acyclic_native_runtime::NativeReadMeter::reserve_cleanup_write(self.native().runtime_meter())
            .map_err(native_demand_error)
    }
    fn cancel_cleanup_write(&mut self, reservation: u64) {
        acyclic_native_runtime::NativeReadMeter::cancel_cleanup_write(
            self.native().runtime_meter(), reservation,
        );
    }
    fn attempt_cleanup_write<T>(
        &mut self, reservation: u64, operation: impl FnOnce() -> T,
    ) -> (T, Result<(), DemandError>) {
        let recorded = acyclic_native_runtime::NativeReadMeter::record_cleanup_write(
            self.native().runtime_meter(), reservation,
        ).map_err(native_demand_error);
        (operation(), recorded)
    }
}

struct CapturedRuntimeReadMeter {
    base: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    // The actual proxy body retires its claim before signalling scope quiescence. Buffer, completion, and tail guards retain only `base`.
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
    acyclic_native_runtime::device_control_in_place_measured(
        file, code, input, output, scope.native().runtime_meter(),
    ).map_err(|error| scope.original().take_native_io_failure()
        .unwrap_or_else(|| native_demand_error(error)))
}

fn capture_scope_result<T>(
    mut scope: CaptureWorkScope,
    result: Result<T, CaptureError>,
) -> Result<OperationReceipt<T>, OperationFailure<CaptureError>> {
    let released = scope.release();
    let completed = scope.original().lock().work;
    match result {
        Err(error) => Err(OperationFailure::new(error, completed)),
        Ok(value) => {
            released?;
            Ok(OperationReceipt { value, work: completed })
        }
    }
}

async fn capture_in_heap_with_meter<F, T>(
    create: impl FnOnce() -> F,
    meter: &crate::NativeMaterializationWork,
) -> Result<T, CaptureError>
where
    F: std::future::Future<Output = Result<T, CaptureError>>,
{
    let mut claim = None;
    let (mut future, _) = crate::heap_future::in_heap_admitted(
        &mut claim,
        |claim, bytes| {
            if bytes != 0 {
                **claim = Some(meter.claim(bytes).map_err(|error| {
                    let error = match error {
                        crate::NativeTreeMaterializationError::Work(error) => CaptureError::Work(error),
                        crate::NativeTreeMaterializationError::Cancelled => CaptureError::Cancelled,
                        error => CaptureError::NativeKernel(error),
                    };
                    error
                })?);
            }
            Ok::<(), CaptureError>(())
        },
        |_| create(),
    )?;
    future.as_mut().await;
    drop(future);
    drop(claim)
}

async fn capture_sdk_in_heap_with_meter<'a, F, T>(
    create: impl FnOnce(&'a crate::NativeMaterializationWork) -> F,
    meter: &'a CaptureWorkScope,
    cancellation: &CancellationToken,
) -> Result<T, CaptureError>
where
    F: std::future::Future<Output = Result<T, crate::FsError>>,
{
    capture_in_heap_with_meter(
        || async {
            cancellation.check().map_err(|_| CaptureError::Cancelled)?;
            meter.native().admit(WorkCounters::UNCHARGED).map_err(CaptureError::NativeKernel)?;
            let value = create(meter.native()).await.map_err(CaptureError::Kernel)?;
            cancellation.check().map_err(|_| CaptureError::Cancelled)?;
            meter.native().admit(WorkCounters::UNCHARGED).map_err(CaptureError::NativeKernel)?;
            Ok(value)
        },
        meter.native(),
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
    let original = capture_original_work(budget, cancellation,
)?;
    let native = capture_native_original(&original)?;
    let result = capture_subtrees_with_native_work(
        checkout, roots, options, policy, cancellation, baseline, &native,
    ).await;
    capture_original_result(&original, result)
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
    capture_scope_result(scope, result)
}

async fn capture_subtrees_in_scope<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    roots: &[NamespacePath],
    options: &CaptureOptions,
    policy: &CapturePolicy,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
    meter: &CaptureWorkScope,
) -> Result<CaptureReceipt, CaptureError> {
    let cancellation = {
        let mut original = meter;
        if options.maximum_paths == 0 || options.maximum_extent_spans == 0 {
            return Err(CaptureError::InvalidOptions);
        }
        cancellation.check()
            .map_err(|_| CaptureError::Cancelled)?;
        original.check()
            .map_err(capture_demand_error)?;
        if roots.is_empty() {

            return Ok(CaptureReceipt::default());
        }
        original.leaf(|work| work.cancellation().clone())
    };
    capture_in_heap_with_meter(|| async move {
        let limits = checkout.volume_config().limits;
        let profile = checkout.volume_config().profile;
        let (canonical, mut paths, host_roots, host_policy, source_path, source_path_bytes, maximum) = {
            let mut original = meter;
            let canonical = canonical_subtree_roots_measured(roots, policy, &mut original)
                ?;
            if canonical.is_empty() {

                return Ok(CaptureReceipt::default());
            }
            let maximum = usize::try_from(options.maximum_paths)
                .map_err(|_| CaptureError::InvalidOptions)?;
            let paths = clone_capture_paths(&canonical, None, &mut original)
                ?;
            let host_roots = clone_capture_paths(&canonical, None, &mut original)
                ?;
            let host_policy = CapturePolicy {
                excluded_prefixes: clone_capture_paths(&policy.excluded_prefixes, None, &mut original)
                    ?,
            };
            let (source_path, source_path_bytes) = crate::native_host::clone_path_with_work(&options.source_root, &mut original)
                .map_err(capture_demand_error)?;
            (canonical, paths, host_roots, host_policy, source_path, source_path_bytes, maximum)
        };
        let expected_identity = options.expected_root_identity;
        let host_cancellation = cancellation.clone();
        let worker = meter.clone();
        let task_meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter> = meter.original().clone();
        let task = acyclic_native_runtime::run_blocking_io_measured(move || {
            let mut host_work = &worker;
            let result = (|| {
                let source_root = HostRoot::open_with_work(&source_path, &mut host_work)
                    .map_err(capture_demand_error)?;
                if source_root.identity() != expected_identity {
                    return Err(CaptureError::RootChanged);
                }
                let mut observed = CaptureMap::new();

                collect_host_subtree_roots(&source_root,
    profile,
    limits,
    maximum,
    &host_roots,
    &host_policy,
    &mut observed,
    &mut host_work,
)?;
                Ok((source_root, observed))
            })();
            let cleanup = (|| {
                release_capture_paths(host_roots, &mut host_work)?;
                release_capture_paths(host_policy.excluded_prefixes, &mut host_work)?;
                drop(source_path);
                host_work.release(source_path_bytes).map_err(capture_demand_error)
            })();
            match result {
                Ok(value) => {
                    cleanup?;
                    Ok(value)
                }
                Err(failure) => {

                    Err(failure)
                }
            }
        }, task_meter);
        let result = match task {
            Ok(task) => task.await.map_err(native_demand_error),
            Err(error) => Err(native_demand_error(error)),
        };
        let (source_root, observed) = {
            let mut original = meter;
            result.map_err(capture_demand_error)?
                .map_err(|mut failure| {

                    failure
                })?
        };
        let lookup_cancellation = meter.native().with_original_leaf(|work| work.cancellation().clone());
        let current = capture_sdk_in_heap_with_meter(
            |native| checkout.lookup_batch_no_follow_with_native_work(&canonical, native, &lookup_cancellation),
            meter, &lookup_cancellation,
        ).await?;

        let canonical_bytes = canonical.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
        for (root, entry) in canonical.into_iter().zip(current.0.entries) {
            if entry.record.is_some_and(|record| record.kind == FileKind::Directory) {
                collect_checkout_subtree_paths(checkout,
    limits,
    root,
    policy,
    maximum,
    &mut paths,
    &cancellation,
    meter,
).await?;
            } else {
                let mut original = meter;

                capture_drop_path(root, (&mut original))?;
            }
        }
        drop(current.1);
        let ordered = {
            let mut original = meter;

            (&mut original).release(canonical_bytes).map_err(capture_demand_error)?;
            order_host_checkout_union(observed, paths, maximum, &mut original)
                ?
        };
        let mut captured = capture_observed_paths_from_root(checkout,
    ordered,
    options.maximum_extent_spans,
    &source_root,
    &cancellation,
    baseline,
    meter,
).await?;
        Ok(captured)
    }, meter.native()).await
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

fn capture_allocate(meter: &mut impl CaptureNativeWork, bytes: u64) -> Result<(), CaptureError> {
    meter.leaf(|work| work.allocate(bytes)).map_err(capture_demand_error)
}

fn capture_charge(meter: &mut impl CaptureNativeWork, work: WorkCounters) -> Result<(), CaptureError> {
    meter.leaf(|meter| meter.charge(work)).map_err(capture_demand_error)
}

fn capture_candidate<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &Checkout<A, O>,
    cancellation: &CancellationToken,
    meter: &mut &CaptureWorkScope,
) -> Result<Checkout<A, O>, CaptureError> {
    let candidate = checkout.private_candidate_with_native_work(meter.native(), cancellation)
        .map_err(CaptureError::Kernel)?;
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
    // Completed capacity, including any capacity retained on allocator failure, belongs to the original ledger before the late cancellation/budget check.
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

/// Converts the canonical namespace with the same bounded component scanner used by capture.
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
        // Reserve before opaque UTF-16 decoding, then reconcile the owned output.
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
    { root.stat_with_work(path, meter) }
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
        let (mut reader, reader_allocation) = root.read_dir_stats_with_work(&host_parent, meter).map_err(capture_demand_error)?;
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

async fn capture_paths_from_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    policy: &CapturePolicy,
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<CaptureReceipt, CaptureError> {
    let unique = {
        let mut original = meter;
        let mut unique = clone_capture_paths(paths, Some(policy), &mut original)
            ?;
        capture_sort_by(&mut unique, Ord::cmp, &mut original)
            ?;
        if capture_has_duplicate_paths(&unique, &mut original)
            ? {
            return Err(CaptureError::InvalidOptions);
        }
        unique
    };
    let observed = observe_unique_paths(unique, source_root, cancellation, &mut &*meter)?;
    capture_observed_paths_from_root(
        checkout, observed, maximum_extent_spans, source_root, cancellation, None, meter,
    ).await
}

type ObservedPath = (Option<HostObservation>, NamespacePath);

fn observe_unique_paths<W: CaptureNativeWork>(unique: Vec<NamespacePath>,
        source_root: &HostRoot,
        cancellation: &CancellationToken,
        meter: &mut W,
) -> Result<Vec<ObservedPath>, CaptureError> {
    let mut ordered = capture_vec(unique.len(), meter)
        ?;
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
    result?;

    cancellation
        .check()
        .map_err(|_| CaptureError::Cancelled)?;
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
    }, meter)?;

    cancellation
        .check()
        .map_err(|_| CaptureError::Cancelled)?;
    Ok(ordered)
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

async fn capture_observed_paths_from_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(checkout: &mut Checkout<A, O>,
    ordered: Vec<(Option<HostObservation>, NamespacePath)>,
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
    meter: &CaptureWorkScope,
) -> Result<CaptureReceipt, CaptureError> {
    capture_in_heap_with_meter(|| async move {
        let (observations, paths) = {
            let mut original = meter;
            let ordered_bytes = ordered.capacity() as u64 * std::mem::size_of::<ObservedPath>() as u64;
            let mut observations = capture_vec(ordered.len(), &mut original)
                ?;
            let mut paths = capture_vec(ordered.len(), &mut original)
                ?;
            for (observation, path) in ordered {
                observations.push(observation);
                paths.push(path);
            }
            (&mut original).release(ordered_bytes).map_err(capture_demand_error)?;
            (observations, paths)
        };
        let current = if paths.is_empty() {
            Vec::new()
        } else {
            let lookup_cancellation = meter.native().with_original_leaf(|work| work.cancellation().clone());
            let lookup = capture_sdk_in_heap_with_meter(
                |native| checkout.lookup_batch_no_follow_with_native_work(&paths, native, &lookup_cancellation),
                meter, &lookup_cancellation,
            ).await?;

            let mut original = meter;
            let mut records = capture_vec(lookup.0.entries.len(), &mut original)
                ?;
            for entry in lookup.0.entries {
                records.push(entry.record);
            }
            drop(lookup.1);
            records
        };
        let states = {
            let mut original = meter;
            let states = order_capture_states(observations, paths, current, &mut original)
                ?;
            states
        };
        let mut host_links = CaptureMap::new();
        let captured = capture_ranked_paths_from_root(checkout,
    states,
    &mut host_links,
    maximum_extent_spans,
    source_root,
    cancellation,
    baseline,
    meter,
).await;
        let cleanup = release_host_links(host_links, &mut &*meter);
        match captured {
            Ok(captured) => { cleanup?; Ok(captured) }
            Err(error) => Err(error),
        }
    }, meter.native()).await
}

#[allow(clippy::too_many_arguments)]
async fn capture_ranked_paths_from_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(checkout: &mut Checkout<A, O>,
        states: Vec<CapturePathState>,
        host_links: &mut CaptureMap<[u8; 16], HostLinkSource>,
        maximum_extent_spans: u32,
        source_root: &HostRoot,
        cancellation: &CancellationToken,
        baseline: Option<&NativeViewBaseline>,
        meter: &CaptureWorkScope,
) -> Result<CaptureReceipt, CaptureError> {
    let states_bytes = states.capacity() as u64 * std::mem::size_of::<CapturePathState>() as u64;
    let (mut receipt, mut prepared, cancellation) = {
        let mut original = meter;
        cancellation.check()
            .map_err(|_| CaptureError::Cancelled)?;
        original.check()
            .map_err(capture_demand_error)?;
        let prepared = capture_vec(states.len(), &mut original)
            ?;
        (
            CaptureReceipt::default(),
            prepared, original.leaf(|work| work.cancellation().clone()),
        )
    };
    let mut mutations = Vec::new();
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
    &cancellation,
    meter,
).await?;
        if let Some(plan) = plan {
            prepared.push(plan);
        }
    }
    {
        let mut original = meter;
        (&mut original).release(states_bytes).map_err(capture_demand_error)?;
    }
    finish_prepared_paths(&checkout.content_stager(),
    source_root,
    prepared,
    maximum_extent_spans,
    &mut mutations,
    &mut receipt,
    &cancellation,
    meter,
).await?;
    apply_capture_transaction(checkout,
    mutations,
    &mut receipt,
    meter,
).await?;
    {
        let mut original = meter;
        meter.native().receipt().verify(original.leaf(|work| work.budget))
            .map_err(CaptureError::Work)?;
    }
    Ok(receipt)
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
    // Remove descendants before kind changes; existing hard-link bindings preserve stable FileIds.
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
    let result = crate::obs::in_span(&span, async {
        let original = capture_original_work(budget, cancellation,
)?;
        let native = capture_native_original(&original)?;
        let result = capture_baseline_with_native_work(
            checkout, options, policy, cancellation, &native,
        ).await;
        capture_original_result(&original, result)
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
    let completed = { meter.lock().work };
    if checkout.has_pending_mutations() {
        return Err(OperationFailure::new(CaptureError::DirtyCheckout, completed));
    }
    if options.maximum_paths == 0 || options.maximum_extent_spans == 0 {
        return Err(OperationFailure::new(CaptureError::InvalidOptions, completed));
    }
    cancellation.check()
        .map_err(|_| OperationFailure::new(CaptureError::Cancelled, completed))?;
    let (checked, completed) = {
        let held = meter.lock();
        (held.check(), held.work)
    };
    checked.map_err(|error| OperationFailure::new(capture_demand_error(error), completed))?;
    let scope = CaptureWorkScope::new(work)?;
    let meter = &scope;
    let result = capture_in_heap_with_meter(|| async {
        let maximum = {
            let mut held = meter;
            let maximum = usize::try_from(options.maximum_paths)
                .map_err(|_| CaptureError::InvalidOptions)?;
            maximum
        };
        let mut host_work = meter;
        let source_root = open_source_root(options, &mut host_work)?;
        let limits = checkout.volume_config().limits;
        let profile = checkout.volume_config().profile;
        let mut paths = Vec::new();

        let observed = collect_host_observations(&source_root,
    profile,
    limits,
    maximum,
    policy,
    cancellation,
    &mut host_work,
)?;
        collect_checkout_paths(checkout,
    limits,
    policy,
    maximum,
    &mut paths,
    cancellation,
    meter,
).await?;
        let ordered = {
            let mut held = meter;
            order_host_checkout_union(observed, paths, maximum, &mut held)
                .map_err(|error| error)?
        };
        capture_observed_paths_from_root(checkout,
    ordered,
    options.maximum_extent_spans,
    &source_root,
    cancellation,
    None,
    meter,
).await
    }, meter.native()).await;
    capture_scope_result(scope, result)
}

#[allow(clippy::too_many_arguments)]
fn collect_host_observations<W: CaptureNativeWork>(root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    policy: &CapturePolicy,
    cancellation: &CancellationToken,
    meter: &mut W,
) -> Result<CaptureMap<NamespacePath, HostObservation>, CaptureError> {
    let mut observed = CaptureMap::new();
    let volume_root = NamespacePath::new(Vec::new(), limits)
        .map_err(CaptureError::Path)?;
    collect_host_subtree_observations_measured(root,
    profile,
    limits,
    maximum,
    CaptureHostPath::new(),
    volume_root,
    policy,
    &mut observed,
    meter,
)?;
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

#[allow(clippy::too_many_arguments)]
fn collect_host_subtree_roots<W: CaptureNativeWork>(source_root: &HostRoot,
        profile: FilesystemProfile,
        limits: crate::model::VolumeLimits,
        maximum: usize,
        roots: &[NamespacePath],
        policy: &CapturePolicy,
        observed: &mut CaptureMap<NamespacePath, HostObservation>,
        meter: &mut W,
) -> Result<(), CaptureError> {
    (|| {
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
    })()
}

#[allow(clippy::too_many_arguments)]
fn collect_host_subtree_paths<P: ScannedPaths, W: CaptureNativeWork>(root: &HostRoot,
        profile: FilesystemProfile,
        limits: crate::model::VolumeLimits,
        host_root: CaptureHostPath,
        volume_root: NamespacePath,
        policy: &CapturePolicy,
        maximum: usize,
        paths: &mut P,
        meter: &mut W,
) -> Result<(), CaptureError> {
    collect_host_walk_measured(root, profile, limits, host_root, volume_root, policy, meter,
        |path, _, meter| append_scanned_path(paths, path, maximum, meter))
}

#[allow(clippy::too_many_arguments)]
async fn collect_checkout_paths<A: AsyncAuthorityStore, O: AsyncObjectStore, P: ScannedPaths>(checkout: &mut Checkout<A, O>,
    limits: crate::model::VolumeLimits,
    policy: &CapturePolicy,
    maximum: usize,
    paths: &mut P,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    let result = capture_in_heap_with_meter(|| {

        async move {
            let root = NamespacePath::new(Vec::new(), limits).map_err(|error| {
                CaptureError::Path(error)
            })?;
            collect_checkout_subtree_paths(checkout,
    limits,
    root,
    policy,
    maximum,
    paths,
    cancellation,
    meter,
).await?;
            Ok(())
        }
    }, meter.native()).await;

    result.map(|_| ())
}

#[allow(clippy::too_many_arguments)]
async fn collect_checkout_subtree_paths<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    P: ScannedPaths,
>(checkout: &mut Checkout<A, O>,
        limits: crate::model::VolumeLimits,
        root: NamespacePath,
        policy: &CapturePolicy,
        maximum: usize,
        paths: &mut P,
        cancellation: &CancellationToken,
        meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    const PAGE_ENTRIES: u32 = 1024;
    let mut pending = {
        let mut original = meter;
        cancellation.check()
            .map_err(|_| CaptureError::Cancelled)?;
        capture_vec(1, (&mut original))?
    };
    pending.push(root);
    while let Some(directory) = pending.pop() {
        let mut after = None;
        loop {
            let page_cancellation = meter.native().with_original_leaf(|work| work.cancellation().clone());
            let page = capture_sdk_in_heap_with_meter(
                |native| checkout.list_directory_records_with_native_work(
                    &directory, after.as_ref(), PAGE_ENTRIES, native, &page_cancellation,
                ),
                meter, &page_cancellation,
            ).await?;

            let has_more = {
                let mut original = meter;
                { let meter = &mut original; capture_charge(meter,
                    WorkCounters { items_examined: page.0.entries.len() as u64, ..WorkCounters::UNCHARGED }) }?;
                if page.0.has_more && page.0.entries.is_empty() {
                    return Err(CaptureError::Invariant("directory pagination made no progress"));
                }
                for entry in &page.0.entries {
                    let child = { let meter = &mut original; capture_charge(meter, WorkCounters { source_entries_visited: 1, ..WorkCounters::UNCHARGED })?;
                        let name = clone_capture_name(&entry.name, meter)?;
                        append_path_measured(&directory, name, limits, meter) }?;
                    if capture_excludes(policy, &child, (&mut original))? {
                        capture_drop_path(child, (&mut original))?;
                        continue;
                    }
                    let scanned = clone_capture_path(&child, &mut original)?;
                    append_scanned_path(paths, scanned, maximum, (&mut original))?;
                    if entry.record.kind == FileKind::Directory {
                        capture_reserve(&mut pending, 1, (&mut original))?;
                        pending.push(child);
                    } else {
                        capture_drop_path(child, (&mut original))?;
                    }
                }
                if let Some(previous) = after.take() {
                    let bytes = previous.as_bytes().len() as u64;
                    drop(previous);
                    (&mut original).release(bytes).map_err(capture_demand_error)?;
                }
                after = match page.0.entries.last() {
                    Some(entry) => Some(clone_capture_name(&entry.name, (&mut original))?),
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
            let mut original = meter;
            if let Some(cursor) = after {
                let bytes = cursor.as_bytes().len() as u64;
                drop(cursor);
                (&mut original).release(bytes).map_err(capture_demand_error)?;
            }
            capture_drop_path(directory, (&mut original))?;
        }
    }
    let pending_bytes = pending.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    drop(pending);
    let mut original = meter;
    (&mut original).release(pending_bytes).map_err(capture_demand_error)?;
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

impl ScannedPaths for CaptureMap<NamespacePath, ()> {
    fn len(&self) -> usize {
        CaptureMap::len(self)
    }

    fn contains_measured(&self, path: &NamespacePath, meter: &mut impl CaptureNativeWork) -> Result<bool, CaptureError> {
        CaptureMap::contains_key_measured(self, path, meter)
    }

    fn insert_measured(&mut self, path: NamespacePath, meter: &mut impl CaptureNativeWork) -> Result<(), CaptureError> {
        CaptureMap::insert_measured(self, path, (), meter).map(|_| ())
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
pub async fn capture_watch_batch_with_policy<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    batch: WatchBatch,
    options: &CaptureOptions,
    policy: &CapturePolicy,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<WatchCaptureReceipt>, OperationFailure<CaptureError>> {
    let original = capture_original_work(budget, cancellation,
)?;
    let native = capture_native_original(&original)?;
    let result = capture_watch_batch_with_native_work(
        checkout, batch, options, policy, cancellation, &native,
    ).await;
    capture_original_result(&original, result)
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
    let scope = CaptureWorkScope::new(work)?;
    let result = capture_watch_batch_in_scope(
        checkout, batch, options, policy, cancellation, &scope,
    ).await;
    capture_scope_result(scope, result)
}

/// Replays one watcher batch against the original shared invocation ledger.
#[allow(clippy::too_many_lines)]
async fn capture_watch_batch_in_scope<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    batch: WatchBatch,
    options: &CaptureOptions,
    policy: &CapturePolicy,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<WatchCaptureReceipt, CaptureError> {
    // The facade opens fixed accounting leaves only; owned helpers and drops remain outside those short original-meter loans.
    let cancellation = {
        let mut original = meter;
        cancellation.check()
            .map_err(|_| CaptureError::Cancelled)?;
        original.check()
            .map_err(capture_demand_error)?;
        if let WatchBatch::Changes { changes, original: incoming, .. } = &batch {
            let storage_bytes = if incoming.has_native_changes_storage_custody() { 0 } else {
                (changes.capacity() as u64).checked_mul(std::mem::size_of::<WatchChange>() as u64)
                    .ok_or(CaptureError::Work(WorkError::Overflow))?
            };
            let bytes = capture_watch_owned_bytes(changes, incoming, storage_bytes, &mut original)
                ?;
            original.leaf(|work| work.adopt(bytes))
                .map_err(capture_demand_error)?;
        }
        original.leaf(|work| work.cancellation().clone())
    };
    capture_in_heap_with_meter(|| async move {
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
            return Err(CaptureError::RescanRequired {
                    epoch: epoch.get(),
                    reason,
                });
        };
        let changes_storage_native = incoming.has_native_changes_storage_custody();
        // Drop moved hints before their original allocation guards, including on early failure.
        let incoming = (changes, incoming);
        if options.maximum_paths == 0
            || options.maximum_extent_spans == 0
            || incoming.0.len() > usize::try_from(options.maximum_paths).unwrap_or(usize::MAX)
        {
            return Err(CaptureError::InvalidOptions);
        }
        let source_root = open_source_root(options, &mut &*meter)?;
        let mut receipt = CaptureReceipt::default();
        let changes_bytes = incoming.0.capacity() as u64 * std::mem::size_of::<WatchChange>() as u64;
        let mut mutations = Vec::new();
        let mut ordinary = CaptureMap::new();
        let mut rename_records = CaptureMap::<NamespacePath, FileRecord>::new();
        let mut renamed_directories = Vec::new();
        let mut rename_destinations = Vec::new();
        let mut moved_away = CaptureMap::new();
        
        for (index, change) in incoming.0.into_iter().enumerate() {
            let change = if incoming.1.change_has_native_payload_custody(index) {
                let copied = match &change {
                    WatchChange::Created(path) => WatchChange::Created(clone_capture_path(path, &mut &*meter)?),
                    WatchChange::Modified(path) => WatchChange::Modified(clone_capture_path(path, &mut &*meter)?),
                    WatchChange::Arrived(path) => WatchChange::Arrived(clone_capture_path(path, &mut &*meter)?),
                    WatchChange::Removed(path) => WatchChange::Removed(clone_capture_path(path, &mut &*meter)?),
                    WatchChange::MetadataChanged(path) => WatchChange::MetadataChanged(clone_capture_path(path, &mut &*meter)?),
                    WatchChange::Renamed { from, to } => WatchChange::Renamed {
                        from: clone_capture_path(from, &mut &*meter)?,
                        to: clone_capture_path(to, &mut &*meter)?,
                    },
                };
                drop(change);
                copied
            } else { change };
            { let meter = &mut &*meter; capture_charge(meter,
                WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }) }?;
            let change = match change {
                WatchChange::Renamed { from, to } => {
                    let excluded_from = capture_excludes(policy, &from, (&mut &*meter))?;
                    let excluded_to = capture_excludes(policy, &to, (&mut &*meter))?;
                    match (excluded_from, excluded_to) {
                        (false, false) => WatchChange::Renamed { from, to },
                        (true, false) => {
                            capture_drop_path(from, (&mut &*meter))?;
                            WatchChange::Created(to)
                        }
                        (false, true) => {
                            capture_drop_path(to, (&mut &*meter))?;
                            WatchChange::Removed(from)
                        }
                        (true, true) => {
                            capture_drop_path(from, (&mut &*meter))?;
                            capture_drop_path(to, (&mut &*meter))?;
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
                    if capture_excludes(policy, path, (&mut &*meter))? {
                        let bytes = namespace_owned_bytes(path)?;
                        drop(change);
                        (&mut &*meter).release(bytes).map_err(capture_demand_error)?;
                        continue;
                    }
                    change
                }
            };
            match change {
                WatchChange::Created(path) => {
                    let current = current_record(&rename_records, &moved_away, &path, (&mut &*meter))?;
                    ordinary.insert_measured(path, (current, CaptureIntent::Replace), (&mut &*meter))?;
                }
                WatchChange::Modified(path) | WatchChange::Removed(path) => {
                    let current = current_record(&rename_records, &moved_away, &path, (&mut &*meter))?;
                    ordinary.get_or_insert_measured(path, (current, CaptureIntent::Complete), (&mut &*meter)).map(|_| ())?;
                }
                WatchChange::Arrived(path) => {
                    let current = current_record(&rename_records, &moved_away, &path, (&mut &*meter))?;
                    { let meter = &mut &*meter; let entry = ordinary.get_or_insert_measured(path, (current, CaptureIntent::Arrived), meter)?;
                        if entry.1 != CaptureIntent::Replace { entry.1 = CaptureIntent::Arrived; }
                        ()};
                }
                WatchChange::MetadataChanged(path) => {
                    let current = current_record(&rename_records, &moved_away, &path, (&mut &*meter))?;
                    ordinary.get_or_insert_measured(path, (current, CaptureIntent::MetadataOnly), (&mut &*meter)).map(|_| ())?;
                }
                WatchChange::Renamed { from, to } => {
                    cancellation.check().map_err(|_| {
                        CaptureError::Cancelled
                    })?;
                    let prior_source = ordinary.remove_measured(&from, (&mut &*meter))?;
                    let prior_source = if let Some((key, value)) = prior_source {
                        capture_drop_path(key, (&mut &*meter))?;
                        Some(value)
                    } else { None };
                    if let Some((key, _)) = ordinary.remove_measured(&to, (&mut &*meter))? {
                        capture_drop_path(key, (&mut &*meter))?;
                    }
                    let record = if let Some((key, record)) = rename_records.remove_measured(&from, (&mut &*meter))? {
                        capture_drop_path(key, (&mut &*meter))?;
                        Some(record)
                    } else if matches!(prior_source, Some((CurrentRecord::Known(None), _)))
                        || moved_away.contains_key_measured(&from, (&mut &*meter))?
                    {
                        None
                    } else {
                        let source = capture_sdk_in_heap_with_meter(
                            |native| checkout.lookup_no_follow_with_native_work(&from, native, &cancellation),
                            meter, &cancellation,
                        ).await?;
                        match source.record {
                            Some(record) => Some(record),
                            None if prior_source.is_some() => None,
                            None => None,
                        }
                    };
                    if let Some(record) = record {
                        if record.kind == FileKind::Directory {
                            capture_reserve(&mut renamed_directories, 1, (&mut &*meter))?;
                            renamed_directories.push((clone_capture_path(&from, &mut &*meter)?,
                                clone_capture_path(&to, &mut &*meter)?));
                        }
                        capture_reserve(&mut rename_destinations, 1, (&mut &*meter))?;
                        rename_destinations.push(clone_capture_path(&to, &mut &*meter)?);
                        capture_reserve(&mut mutations, 1, (&mut &*meter))?;
                        mutations.push(AuthoredMutation::Rename {
                            source: clone_capture_path(&from, &mut &*meter)?,
                            destination: clone_capture_path(&to, &mut &*meter)?,
                            replace: true,
                        });
                        let destination = clone_capture_path(&to, &mut &*meter)?;
                        rename_records.insert_measured(destination, record, (&mut &*meter))?;
                    } else {
                        let destination = clone_capture_path(&to, &mut &*meter)?;
                        { let meter = &mut &*meter; ordinary.insert_measured(
                            destination, (CurrentRecord::Lookup, CaptureIntent::Replace), meter) }?;
                    }
                    moved_away.insert_measured(from, (), (&mut &*meter))?;
                    if let Some((key, ())) = moved_away.remove_measured(&to, (&mut &*meter))? {
                        capture_drop_path(key, (&mut &*meter))?;
                    }
                    capture_drop_path(to, (&mut &*meter))?;
                }
            }
        }
        if !changes_storage_native {
            (&mut &*meter).release(changes_bytes).map_err(capture_demand_error)?;
        }
        drop(incoming.1);
        release_capture_set(moved_away, (&mut &*meter))?;
        
        expand_directory_hints(checkout,
    &mut ordinary,
    &source_root,
    options.maximum_paths,
    policy,
    &mut receipt,
    &cancellation,
    meter,
)
        .await?;
        
        // A watcher may report a nested file before any ancestor has been observed in this lazy checkout. Capture only the missing ancestors, rather than expanding their entire host subtrees as event roots.
        let maximum_paths = usize::try_from(options.maximum_paths).unwrap_or(usize::MAX);
        let ancestors = watch_ancestor_candidates(ordinary
                .keys()
                .chain(rename_records.keys())
                .chain(rename_destinations.iter()),
    &ordinary,
    &rename_records,
    policy,
    maximum_paths,
    &cancellation,
    checkout.volume_config().limits,
    &mut &*meter,
)?;
        release_capture_paths(rename_destinations, (&mut &*meter))?;
        let mut missing_ancestors = hydrate_watch_ancestors(checkout,
        &mut ordinary,
        ancestors,
        WatchAncestorScope {
            source_root: &source_root,
            epoch,
            maximum_paths: maximum_paths.saturating_sub(rename_records.len()),
            cancellation: &cancellation,
        },
        &mut receipt, meter)
        .await?;
        
        let mut lookup_paths = capture_vec(ordinary.len(), (&mut &*meter))?;
        for (path, (current, _)) in ordinary.iter() {
            if matches!(current, CurrentRecord::Lookup) {
                lookup_paths.push(clone_capture_path(path, &mut &*meter)?);
            }
        }
        if !lookup_paths.is_empty() {
            let lookup = capture_sdk_in_heap_with_meter(
                |native| checkout.lookup_batch_no_follow_with_native_work(&lookup_paths, native, &cancellation),
                meter, &cancellation,
            ).await?;

            let lookup_paths_bytes = lookup_paths.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
            for (path, entry) in lookup_paths.into_iter().zip(lookup.0.entries) {
                let mut record = entry.record;
                if record.is_none()
                    && let Some(prior_path) = { let meter = &mut &*meter; remap_renamed_directory_path(
                        &path, &renamed_directories, checkout.volume_config().limits, meter,
                    ) }?
                {
                    let prior = capture_sdk_in_heap_with_meter(
                        |native| checkout.lookup_no_follow_with_native_work(&prior_path, native, &cancellation),
                        meter, &cancellation,
                    ).await?;
                    record = prior.record;
                    capture_drop_path(prior_path, (&mut &*meter))?;
                }
                let Ok(index) = ordinary.position_measured(&path, (&mut &*meter))? else {
                    return Err(CaptureError::Invariant("capture lookup path disappeared"));
                };
                let (current, _) = &mut ordinary.entries[index].1;
                *current = CurrentRecord::Known(record);
                capture_drop_path(path, (&mut &*meter))?;
            }
            drop(lookup.1);
            (&mut &*meter).release(lookup_paths_bytes).map_err(capture_demand_error)?;
        } else {
            release_capture_paths(lookup_paths, (&mut &*meter))?;
        }
        { let meter = &mut &*meter; let storage_bytes = renamed_directories.capacity() as u64 * std::mem::size_of::<(NamespacePath, NamespacePath)>() as u64;
            for (from, to) in renamed_directories {
                capture_drop_path(from, meter)?;
                capture_drop_path(to, meter)?;
            }
            meter.release(storage_bytes).map_err(capture_demand_error) }?;
        
        let mut host_links = CaptureMap::new();
        let mut prepared = { let meter = &mut &*meter; capture_vec(
            rename_records.len().checked_add(ordinary.len()).ok_or(CaptureError::Work(WorkError::Overflow))?, meter) }?;
        // A rename can target a directory that has never been observed in this lazy checkout. Materialize only those host ancestors before the rename, preserving the event order for all remaining mutations.
        { let meter = &mut &*meter; capture_sort_by(&mut missing_ancestors, |left, right| {
            left.depth()
                .cmp(&right.depth())
                .then_with(|| left.cmp(right))
        }, meter) }?;
        let rename_mutations = std::mem::take(&mut mutations);
        let missing_bytes = missing_ancestors.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
        for path in missing_ancestors {
            let Some((key, (current, intent))) = ordinary.remove_measured(&path, (&mut &*meter))? else {
                return Err(CaptureError::Invariant("capture ancestor path disappeared"));
            };
            capture_drop_path(key, (&mut &*meter))?;
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
    &cancellation,
    meter,
)
            .await?;
            if let Some(plan) = plan {
                prepared.push(plan);
            }
        }
        (&mut &*meter).release(missing_bytes).map_err(capture_demand_error)?;
        let rename_mutations_bytes = rename_mutations.capacity() as u64 * std::mem::size_of::<AuthoredMutation>() as u64;
        capture_reserve(&mut mutations, rename_mutations.len(), (&mut &*meter))?;
        { let meter = &mut &*meter; capture_charge(meter, WorkCounters {
            bytes_copied: rename_mutations.len() as u64 * std::mem::size_of::<AuthoredMutation>() as u64,
            ..WorkCounters::UNCHARGED
        }) }?;
        mutations.extend(rename_mutations);
        (&mut &*meter).release(rename_mutations_bytes).map_err(capture_demand_error)?;
        let namespace_boundary = mutations.len();
        let rename_records_bytes = rename_records.storage_bytes();
        for (path, record) in rename_records {
            if ordinary.contains_key_measured(&path, (&mut &*meter))? {
                capture_drop_path(path, (&mut &*meter))?;
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
    &cancellation,
    meter,
)
            .await?;
            if let Some(plan) = plan {
                prepared.push(plan);
            }
        }
        (&mut &*meter).release(rename_records_bytes).map_err(capture_demand_error)?;
        let mut observed_ordinary = capture_vec(ordinary.len(), (&mut &*meter))?;
        let ordinary_bytes = ordinary.storage_bytes();
        for (path, (current, intent)) in ordinary {
            let mut host_work = meter;
            let host_path = namespace_to_host_path_measured(&path, &mut host_work)
                ?;
            let observation = observe_host_path(&source_root, &host_path, &mut host_work)
                ?;
            capture_drop_host_path(host_path, &mut host_work)
                ?;
            observed_ordinary.push((path, current, intent, observation));
        }
        (&mut &*meter).release(ordinary_bytes).map_err(capture_demand_error)?;
        let mut ordinary = observed_ordinary;
        { let meter = &mut &*meter; capture_sort_by(&mut ordinary, |left, right| match (left.3.is_some(), right.3.is_some()) {
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
        }, meter) }?;
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
    &cancellation,
    meter,
)
            .await?;
            if let Some(plan) = plan {
                prepared.push(plan);
            }
        }
        (&mut &*meter).release(observed_bytes).map_err(capture_demand_error)?;
        finish_prepared_paths(&checkout.content_stager(),
    &source_root,
    prepared,
    options.maximum_extent_spans,
    &mut mutations,
    &mut receipt,
    &cancellation,
    meter,
)
        .await?;
        release_host_links(host_links, (&mut &*meter))?;
        if namespace_boundary == 0 || namespace_boundary == mutations.len() {
            apply_capture_transaction(checkout,
    mutations,
    &mut receipt,
    meter,
)
                .await?;
        } else {
            // Compile binding changes before final-state mutations on one atomic candidate.
            let mut candidate = capture_candidate(checkout, &cancellation, &mut &*meter)?;
            let final_state = { let meter = &mut &*meter; let count = mutations.len() - namespace_boundary;
                let bytes = count.checked_mul(std::mem::size_of::<AuthoredMutation>())
                    .ok_or(CaptureError::Work(WorkError::Overflow))? as u64;
                let copied = WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED };
                meter.admit(copied).map_err(capture_demand_error)?;
                let mut final_state = capture_vec(count, meter)?;
                final_state.extend(mutations.drain(namespace_boundary..));
                meter.record(copied).map_err(capture_demand_error)?;
                final_state};
            apply_capture_transaction(&mut candidate,
    mutations,
    &mut receipt,
    meter,
)
            .await?;
            apply_capture_transaction(&mut candidate,
    final_state,
    &mut receipt,
    meter,
)
            .await?;
            { let meter = &mut &*meter; cancellation.check().map_err(|_| CaptureError::Cancelled)?;
                meter.check().map_err(capture_demand_error) }?;
            cancellation.commit_if_live(|| {
                *checkout = candidate;
                Ok::<(), CaptureError>(())
            }).map_err(|_| CaptureError::Cancelled)??;
        }
        Ok(WatchCaptureReceipt {
                epoch,
                first_sequence,
                next_sequence,
                capture: receipt,
            })
    }, meter.native()).await
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
        let workspace = fs.create_workspace("shared-watch-reentry").await?;
        workspace.write_text("/file", "prior watch value").await?;
        let mut checkout = workspace.checkout(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::tracking_transaction(),
        ).await?;
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

/// What a hinted path held before this batch: a renamed record, nothing if it was moved away, or the checkout's current binding.
fn current_record(
    rename_records: &CaptureMap<NamespacePath, FileRecord>,
    moved_away: &CaptureMap<NamespacePath, ()>,
    path: &NamespacePath,
    meter: &mut impl CaptureNativeWork,
) -> Result<CurrentRecord, CaptureError> {
    Ok(match rename_records.get_measured(path, meter)? {
        Some(record) => CurrentRecord::Known(Some(*record)),
        None if moved_away.contains_key_measured(path, meter)? => CurrentRecord::Known(None),
        None => CurrentRecord::Lookup,
    })
}

fn watch_ancestor_candidates<'a>(paths: impl Iterator<Item = &'a NamespacePath>,
        ordinary: &CaptureMap<NamespacePath, (CurrentRecord, CaptureIntent)>,
        rename_records: &CaptureMap<NamespacePath, FileRecord>,
        policy: &CapturePolicy,
        maximum_paths: usize,
        cancellation: &CancellationToken,
        limits: crate::model::VolumeLimits,
        meter: &mut impl CaptureNativeWork,
) -> Result<Vec<NamespacePath>, CaptureError> {
    let _ = cancellation;
    let mut ancestors = CaptureMap::new();
    for path in paths {
        for depth in (1..path.depth()).rev() {
            let parent = clone_capture_prefix(path, depth, limits, (&mut *meter))?;
            if !ordinary.contains_key_measured(&parent, (&mut *meter))?
                && !rename_records.contains_key_measured(&parent, (&mut *meter))?
                && !capture_excludes(policy, &parent, (&mut *meter))?
                && !ancestors.contains_key_measured(&parent, (&mut *meter))? {
                if combined_path_count_exceeds(ancestors.len() + ordinary.len(), rename_records.len() + 1, maximum_paths) {
                    return Err(CaptureError::InvalidOptions);
                }
                ancestors.insert_measured(parent, (), (&mut *meter))?;
            } else {
                capture_drop_path(parent, (&mut *meter))?;
            }
        }
    }
    let storage_bytes = ancestors.storage_bytes();
    let mut ordered = capture_vec(ancestors.len(), (&mut *meter))?;
    ordered.extend(ancestors.into_iter().map(|(path, ())| path));
    (&mut *meter).release(storage_bytes).map_err(capture_demand_error)?;
    let mut ancestors = ordered;
    { let meter = &mut *meter; capture_sort_by(&mut ancestors, |left, right| {
        left.depth()
            .cmp(&right.depth())
            .then_with(|| left.cmp(right))
    }, meter) }?;
    Ok(ancestors)
}

struct WatchAncestorScope<'a> {
    source_root: &'a HostRoot,
    epoch: WatchEpoch,
    maximum_paths: usize,
    cancellation: &'a CancellationToken,
}

fn release_capture_set(paths: CaptureMap<NamespacePath, ()>, meter: &mut impl CaptureNativeWork) -> Result<(), CaptureError> {
    let bytes = paths.storage_bytes();
    for (path, ()) in paths {
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
) -> Result<Vec<NamespacePath>, CaptureError> {
    capture_in_heap_with_meter(|| async {
    let mut missing = Vec::new();
    let mut replaced = Vec::new();
    let ancestors_bytes = ancestors.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    for path in ancestors {
        let below_replaced = capture_within_any(&path, &replaced, false, (&mut &*meter))?;
        let current = if below_replaced {
            None
        } else {
            let lookup = capture_sdk_in_heap_with_meter(
                |native| checkout.lookup_no_follow_with_native_work(&path, native, scope.cancellation),
                meter, scope.cancellation,
            ).await?;
            lookup.record
        };
        let mut host_work = meter;
        let host_path = namespace_to_host_path_measured(&path, &mut host_work)
            ?;
        let observation = observe_host_path(scope.source_root, &host_path, &mut host_work)
            ?;
        capture_drop_host_path(host_path, &mut host_work)
            ?;
        match observation {
            Some(observation) if observation.metadata.is_dir() => {
                if current.is_none_or(|record| record.kind != FileKind::Directory) {
                    if current.is_some() {
                        { let meter = &mut &*meter; capture_reserve(&mut replaced, 1, meter)?;
                            replaced.push(clone_capture_path(&path, meter)?);
                            ()};
                    }
                    { let meter = &mut &*meter; capture_reserve(&mut missing, 1, meter)?;
                        missing.push(clone_capture_path(&path, meter)?);
                        ()};
                    { let meter = &mut &*meter; ordinary.insert_measured(
                        path, (CurrentRecord::Known(current), CaptureIntent::Replace), meter) }?;
                } else {
                    capture_drop_path(path, (&mut &*meter))?;
                }
            }
            _ => {
                return Err(CaptureError::RescanRequired {
                        epoch: scope.epoch.get(),
                        reason: WatchInvalidationReason::NativeRescanRequired,
                    });
            }
        }
    }
    (&mut &*meter).release(ancestors_bytes).map_err(capture_demand_error)?;
    if ordinary.len() > scope.maximum_paths {
        return Err(CaptureError::InvalidOptions);
    }

    // Descendants of a replaced file or symlink were absent in the old view.
    for (path, (current, _)) in ordinary {
        if matches!(current, CurrentRecord::Lookup)
            && capture_within_any(path, &replaced, true, (&mut &*meter))?
        {
            *current = CurrentRecord::Known(None);
        }
    }
    release_capture_paths(replaced, (&mut &*meter))?;
    Ok(missing)
    }, meter.native()).await
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

#[allow(clippy::too_many_arguments)]
async fn expand_directory_hints<A: AsyncAuthorityStore, O: AsyncObjectStore>(checkout: &mut Checkout<A, O>,
    ordinary: &mut CaptureMap<NamespacePath, (CurrentRecord, CaptureIntent)>,
    source_root: &HostRoot,
    maximum_paths: u32,
    policy: &CapturePolicy,
    receipt: &mut CaptureReceipt,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    capture_in_heap_with_meter(|| async {
    let maximum = usize::try_from(maximum_paths)
        .map_err(|_| CaptureError::InvalidOptions)?;
    if ordinary.len() > maximum {
        return Err(CaptureError::InvalidOptions);
    }
    let limits = checkout.volume_config().limits;
    let profile = checkout.volume_config().profile;
    let mut roots = capture_vec(ordinary.len(), (&mut &*meter))?;
    for (path, (_, intent)) in ordinary.iter() {
        // Expand newly arrived directories; ordinary directory hints already name changed children.
        if !cfg!(target_os = "macos")
            && !matches!(intent, CaptureIntent::Replace | CaptureIntent::Arrived)
        {
            continue;
        }
        let mut host_work = meter;
        let host_path = namespace_to_host_path_measured(path, &mut host_work)
            ?;
        let observation = observe_host_path(source_root, &host_path, &mut host_work)
            ?;
        if observation.is_some_and(|observation| observation.metadata.is_dir()) {
            { let meter = &mut &*meter; roots.push((host_path, clone_capture_path(path, meter)?));
                ()};
        } else {
            capture_drop_host_path(host_path, (&mut &*meter))?;
        }
    }
    let mut paths = CaptureMap::new();
    for path in ordinary.keys() {
        { let meter = &mut &*meter; let path = clone_capture_path(path, meter)?;
            paths.insert_measured(path, (), meter).map(|_| ()) }?;
    }
    let roots_bytes = roots.capacity() as u64 * std::mem::size_of::<(CaptureHostPath, NamespacePath)>() as u64;
    for (host_path, volume_path) in roots {
        let host_volume_path = clone_capture_path(&volume_path, &mut &*meter)?;
        collect_host_subtree_paths(source_root,
    profile,
    limits,
    host_path,
    host_volume_path,
    policy,
    maximum,
    &mut paths,
    &mut meter,
)?;
        let lookup = capture_sdk_in_heap_with_meter(
            |native| checkout.lookup_no_follow_with_native_work(&volume_path, native, cancellation),
            meter, cancellation,
        ).await?;
        if lookup
            .record
            .is_some_and(|record| record.kind == FileKind::Directory)
        {
            collect_checkout_subtree_paths(checkout,
    limits,
    volume_path,
    policy,
    maximum,
    &mut paths,
    cancellation,
    meter,
)
            .await?;
        } else {
            capture_drop_path(volume_path, (&mut &*meter))?;
        }
    }
    (&mut &*meter).release(roots_bytes).map_err(capture_demand_error)?;
    let paths_bytes = paths.storage_bytes();
    for (path, ()) in paths {
        { let meter = &mut &*meter; ordinary.get_or_insert_measured(
            path, (CurrentRecord::Lookup, CaptureIntent::Complete), meter).map(|_| ()) }?;
    }
    (&mut &*meter).release(paths_bytes).map_err(capture_demand_error)?;
    Ok(())
    }, meter.native()).await
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
async fn prepare_final_path<A: AsyncAuthorityStore, O: AsyncObjectStore>(checkout: &mut Checkout<A, O>,
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
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<Option<PreparedPath>, CaptureError> {
    cancellation.check().map_err(|_| {
        CaptureError::Cancelled
    })?;
    let host_path = namespace_to_host_path_measured(&path, (&mut &*meter))?;
    let current = match current {
        CurrentRecord::Known(record) => record,
        CurrentRecord::Lookup => {
            let lookup_cancellation = meter.native().with_original_leaf(|work| work.cancellation().clone());
            let lookup = capture_sdk_in_heap_with_meter(
                |native| checkout.lookup_no_follow_with_native_work(&path, native, &lookup_cancellation),
                meter, &lookup_cancellation,
            ).await?;
            lookup.record
        }
    };
    let observation = match observation {
        Some(observation) => Some(observation),
        None => {
            let observed = observe_host_path(
                source_root, &host_path, &mut &*meter,
            );
            observed?
        }
    };
    match observation {
        Some(observation) => {
            let metadata = observation.metadata;
            let host_kind = host_kind(&metadata)?;
            let snapshot = HostSnapshot::from_metadata(&metadata)
                ?;
            let linked_regular = host_kind == FileKind::Regular
                && checkout.volume_config().hard_links
                && host_link_count(&metadata)
                    ?
                    > 1;
            if linked_regular {
                let identity = snapshot.identity.to_bytes();
                let source = host_links.get_measured(&identity, (&mut &*meter))?;
                if let Some(source) = source {
                    ensure_current_host_node(source_root,
    &host_path,
    &snapshot,
    cancellation,
    &mut &*meter,
)?;
                    receipt.changed_paths = checked_increment(receipt.changed_paths,
)?;
                    receipt.examined_paths =
                        checked_increment(receipt.examined_paths,
)?;
                    capture_drop_host_path(host_path, (&mut &*meter))?;
                    return Ok(match source {
                        HostLinkSource::Pending(source) => Some(PreparedPath::PendingHardLink {
                            source: clone_capture_path(source, &mut &*meter)?,
                            destination: path,
                            current,
                        }),
                        HostLinkSource::Ready(source) => {
                            let source = clone_capture_path(source, &mut &*meter)?;
                            append_hard_link(source,
    path,
    current,
    mutations,
    &mut &*meter,
)?;
                            None
                        }
                    });
                }
                if let Some(epoch) = watch_epoch {
                    // An existing alias may lie outside this hint batch. Rebaseline instead of recording a second FileId.
                    return Err(CaptureError::RescanRequired {
                            epoch,
                            reason: WatchInvalidationReason::NativeRescanRequired,
                        });
                }
            }
            // Created files replace identity; same-kind directories retain identity while reconciling children.
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
    cancellation,
    meter,
)
                    .await?;
                }
                capture_reserve(mutations, 1, (&mut &*meter))?;
                mutations.push(AuthoredMutation::Remove {
                    path: clone_capture_path(&path, &mut &*meter)?,
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
                let lookup_cancellation = meter.native().with_original_leaf(|work| work.cancellation().clone());
                let prior = capture_sdk_in_heap_with_meter(
                    |native| checkout.read_metadata_by_id_with_native_work(record.file_id, native, &lookup_cancellation),
                    meter, &lookup_cancellation,
                ).await?;
                prior_metadata = Some(prior);
                canonical_metadata = preserve_unobserved_metadata(canonical_metadata, prior);
                if let Some(baseline) = baseline {
                    baseline.restore_canonical_stamps(
                        source_root.identity(),
                        prior,
                        &mut canonical_metadata,
                    );
                }
            }
            // A directory's timestamps follow what happens to its entries, each captured through its own path, and to reads of it: a change to them alone is no change of the directory's.
            if host_kind == FileKind::Directory
                && prior_metadata.is_some_and(|prior| {
                    prior.without_timestamps() == canonical_metadata.without_timestamps()
                })
            {
                receipt.examined_paths = checked_increment(receipt.examined_paths,
)?;
                { let meter = &mut &*meter; capture_drop_path(path, meter)?;
                    capture_drop_host_path(host_path, meter) }?;
                return Ok(None);
            }
            let linked_path = if linked_regular {
                Some(clone_capture_path(&path, &mut &*meter)?)
            } else { None };
            let prepared = if intent == CaptureIntent::MetadataOnly && exists_with_kind {
                ensure_current_host_node(source_root,
    &host_path,
    &snapshot,
    cancellation,
    &mut &*meter,
)?;
                capture_reserve(mutations, 1, (&mut &*meter))?;
                mutations.push(AuthoredMutation::SetMetadata {
                    path,
                    metadata: canonical_metadata,
                });
                capture_drop_host_path(host_path, (&mut &*meter))?;
                None
            } else if host_kind == FileKind::Regular {
                // Staging opens the file once and proves it is still the observed node before reading a byte.
                capture_allocate((&mut &*meter), std::mem::size_of::<PreparedRegular>() as u64)?;
                Some(PreparedPath::Regular(Box::new(PreparedRegular {
                    path,
                    host_path,
                    snapshot,
                    exists_with_kind,
                    canonical_metadata,
                })))
            } else {
                append_final_state(path,
    source_root,
    &host_path,
    &metadata,
    snapshot,
    host_kind,
    exists_with_kind,
    canonical_metadata,
    mutations,
    &mut &*meter,
)?;
                capture_drop_host_path(host_path, (&mut &*meter))?;
                None
            };
            receipt.changed_paths = checked_increment(receipt.changed_paths,
)?;
            if let Some(linked_path) = linked_path {
                let source = if prepared.is_some() {
                    HostLinkSource::Pending(linked_path)
                } else {
                    HostLinkSource::Ready(linked_path)
                };
                { let meter = &mut &*meter; host_links.insert_measured(
                    snapshot.identity.to_bytes(), source, meter) }?;
            }
            receipt.examined_paths = checked_increment(receipt.examined_paths,
)?;
            return Ok(prepared);
        }
        None => {
            if let Some(record) = current {
                // A vanished directory requires descendant removals before removing the directory itself.
                if record.kind == FileKind::Directory {
                    push_subtree_removals(checkout,
    &path,
    mutations,
    receipt,
    cancellation,
    meter,
)
                    .await?;
                }
                capture_reserve(mutations, 1, (&mut &*meter))?;
                mutations.push(AuthoredMutation::Remove {
                    path,
                    expected_file_id: Some(record.file_id),
                });
                receipt.changed_paths = checked_increment(receipt.changed_paths,
)?;
            } else {
                capture_drop_path(path, (&mut &*meter))?;
            }
        }
    }
    capture_drop_host_path(host_path, (&mut &*meter))?;
    receipt.examined_paths = checked_increment(receipt.examined_paths,
)?;
    Ok(None)
}

fn host_path_is_absent(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
    ) || cfg!(windows) && error.raw_os_error() == Some(267)
}

/// Queues a remove for every descendant of `root` that the checkout holds, deepest first, so that a following remove of `root` itself finds it empty.
async fn push_subtree_removals<A: AsyncAuthorityStore, O: AsyncObjectStore>(checkout: &mut Checkout<A, O>,
        root: &NamespacePath,
        mutations: &mut Vec<AuthoredMutation>,
        receipt: &mut CaptureReceipt,
        cancellation: &CancellationToken,
        meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    const PAGE: u32 = 256;
    let limits = checkout.volume_config().limits;
    // Descendants that carried their own hint are already queued (absent paths are captured deepest first); a second remove of the same path would find its source missing.
    let mut queued = CaptureMap::new();
    for mutation in mutations.iter() {
        { let meter = &mut &*meter; capture_charge(meter,
            WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }) }?;
        if let AuthoredMutation::Remove { path, .. } = mutation
            && !queued.contains_key_measured(path, (&mut &*meter))? {
            let path = clone_capture_path(path, &mut &*meter)?;
            queued.insert_measured(path, (), (&mut &*meter))?;
        }
    }
    let mut pending = capture_vec(1, (&mut &*meter))?;
    pending.push(clone_capture_path(root, &mut &*meter)?);
    let mut found: Vec<(NamespacePath, FileRecord)> = Vec::new();
    while let Some(directory) = pending.pop() {
        let mut after: Option<LogicalName> = None;
        loop {
            let page_cancellation = meter.native().with_original_leaf(|work| work.cancellation().clone());
            let page = capture_sdk_in_heap_with_meter(
                |native| checkout.list_directory_records_with_native_work(&directory, after.as_ref(), PAGE, native, &page_cancellation),
                meter, &page_cancellation,
            ).await?;

            { let meter = &mut &*meter; capture_charge(meter,
                WorkCounters { items_examined: page.0.entries.len() as u64, ..WorkCounters::UNCHARGED }) }?;
            if page.0.has_more && page.0.entries.is_empty() {
                return Err(CaptureError::Invariant("directory pagination made no progress"));
            }
            for entry in &page.0.entries {
                let child = { let meter = &mut &*meter; let name = clone_capture_name(&entry.name, meter)?;
                    append_path_measured(&directory, name, limits, meter) }?;
                if entry.record.kind == FileKind::Directory {
                    capture_reserve(&mut pending, 1, (&mut &*meter))?;
                    pending.push(clone_capture_path(&child, &mut &*meter)?);
                }
                if !queued.contains_key_measured(&child, (&mut &*meter))? {
                    capture_reserve(&mut found, 1, (&mut &*meter))?;
                    found.push((child, entry.record));
                } else {
                    capture_drop_path(child, (&mut &*meter))?;
                }
            }
            let next = if page.0.has_more {
                let last = page.0.entries.last().ok_or_else(|| CaptureError::Invariant("directory pagination made no progress"))?;
                Some(clone_capture_name(&last.name, (&mut &*meter))?)
            } else { None };
            if let Some(previous) = after.take() {
                let bytes = previous.as_bytes().len() as u64;
                drop(previous);
                (&mut &*meter).release(bytes).map_err(capture_demand_error)?;
            }
            let has_more = page.0.has_more;
            drop(page);
            after = next;
            if !has_more { break; }
        }
        capture_drop_path(directory, (&mut &*meter))?;
    }
    { let meter = &mut &*meter; let pending_bytes = pending.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
        drop(pending);
        meter.release(pending_bytes).map_err(capture_demand_error)?;
        let queued_bytes = queued.storage_bytes();
        for (path, ()) in queued {
            capture_drop_path(path, meter)?;
        }
        meter.release(queued_bytes).map_err(capture_demand_error)?;
        ()};
    { let meter = &mut &*meter; capture_sort_by(&mut found,
        |left, right| right.0.depth().cmp(&left.0.depth()), meter) }?;
    capture_reserve(mutations, found.len(), (&mut &*meter))?;
    let found_bytes = found.capacity() as u64 * std::mem::size_of::<(NamespacePath, FileRecord)>() as u64;
    for (path, record) in found {
        mutations.push(AuthoredMutation::Remove {
            path,
            expected_file_id: Some(record.file_id),
        });
        receipt.changed_paths = checked_increment(receipt.changed_paths,
)?;
    }
    (&mut &*meter).release(found_bytes).map_err(capture_demand_error)?;
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

async fn apply_capture_transaction<A: AsyncAuthorityStore, O: AsyncObjectStore>(checkout: &mut Checkout<A, O>,
        mutations: Vec<AuthoredMutation>,
        receipt: &mut CaptureReceipt,
        meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    let mutation_bytes = { let meter = &mut &*meter; capture_mutation_owned_bytes(
        &mutations, mutations.capacity() as u64 * std::mem::size_of::<AuthoredMutation>() as u64, meter) }?;
    if mutations.is_empty() {
        drop(mutations);
        (&mut &*meter).release(mutation_bytes).map_err(capture_demand_error)?;
        return Ok(());
    }
    let input = (mutations, meter.transfer_allocation(mutation_bytes)
        ?);
    let apply_cancellation = meter.native().with_original_leaf(|work| work.cancellation().clone());
    let applied = capture_sdk_in_heap_with_meter(
        |native| {

            checkout.apply_authored_bulk_transaction_with_native_work(
                input.0, input.1, native, &apply_cancellation,
            )
        },
        meter, &apply_cancellation,
    ).await?;
    Ok(())
}

fn checked_increment(value: u64,
) -> Result<u64, CaptureError> {
    value
        .checked_add(1)
        .ok_or(CaptureError::Work(WorkError::Overflow))
}

fn append_hard_link(source: NamespacePath,
        destination: NamespacePath,
        current: Option<FileRecord>,
        mutations: &mut Vec<AuthoredMutation>,
        meter: &mut impl CaptureNativeWork,
) -> Result<(), CaptureError> {
    { let meter = &mut *meter; capture_reserve(mutations, 1 + usize::from(current.is_some()), meter)?;
        if let Some(record) = current {
            mutations.push(AuthoredMutation::Remove {
                path: clone_capture_path(&destination, meter)?,
                expected_file_id: Some(record.file_id),
            });
        }
        mutations.push(AuthoredMutation::HardLink { source, destination });
        Ok(()) }
}

#[allow(clippy::too_many_arguments)]
fn append_final_state<W: CaptureNativeWork>(path: NamespacePath,
        source_root: &HostRoot,
        host_path: &CaptureHostPath,
        metadata: &HostStat,
        snapshot: HostSnapshot,
        kind: FileKind,
        exists_with_kind: bool,
        canonical_metadata: FileMetadata,
        mutations: &mut Vec<AuthoredMutation>,
        meter: &mut W,
) -> Result<(), CaptureError> {
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

    appended
}

#[allow(clippy::too_many_arguments)]
async fn finish_prepared_regular<A: AsyncAuthorityStore, O: AsyncObjectStore>(stager: &ContentStager<A, O>,
    source_root: &HostRoot,
    prepared: PreparedRegular,
    maximum_extent_spans: u32,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    let staged = stage_regular_body(stager,
    source_root,
    &prepared.host_path,
    &prepared.snapshot,
    maximum_extent_spans,
    cancellation,
    meter,
)
    .await?;
    receipt.staged_file_bytes = receipt
        .staged_file_bytes
        .checked_add(staged.content.bytes)
        .ok_or_else(|| {
            CaptureError::Work(WorkError::Overflow)
        })?;
    let host_path_bytes = capture_host_path_bytes(&prepared.host_path);
    drop(prepared.host_path);
    { let meter = &mut &*meter; meter.release(host_path_bytes)
        .map_err(capture_demand_error) }?;
    append_staged_regular_state(prepared.path,
    staged.logical_bytes,
    prepared.exists_with_kind,
    prepared.canonical_metadata,
    staged.content.ranges,
    mutations,
    &mut &*meter,
)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn finish_prepared_paths<A: AsyncAuthorityStore, O: AsyncObjectStore>(stager: &ContentStager<A, O>,
    source_root: &HostRoot,
    prepared: Vec<PreparedPath>,
    maximum_extent_spans: u32,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    let mut regular = capture_vec(prepared.len(), (&mut &*meter))?;
    let mut aliases = PendingHardLinks::new();
    let prepared_bytes = prepared.capacity() as u64 * std::mem::size_of::<PreparedPath>() as u64;
    for path in prepared {
        match path {
            PreparedPath::Regular(plan) => {
                regular.push(*plan);
                { let meter = &mut &*meter; meter.release(std::mem::size_of::<PreparedRegular>() as u64)
                    .map_err(capture_demand_error) }?;
            }
            PreparedPath::PendingHardLink {
                source,
                destination,
                current,
            } => {
                { let meter = &mut &*meter; let pending = aliases.get_or_insert_measured(source, Vec::new(), meter)?;
                    capture_reserve(pending, 1, meter)?;
                    pending.push((destination, current));
                    ()};
            }
        }
    }
    (&mut &*meter).release(prepared_bytes).map_err(capture_demand_error)?;
    let regular_bytes = regular.capacity() as u64 * std::mem::size_of::<PreparedRegular>() as u64;
    for plan in regular {
        let pending = aliases.remove_measured(&plan.path, (&mut &*meter))?;
        finish_prepared_regular(stager,
    source_root,
    plan,
    maximum_extent_spans,
    mutations,
    receipt,
    cancellation,
    meter,
)
        .await?;
        if let Some((key, pending)) = pending {
            let pending_bytes = pending.capacity() as u64 * std::mem::size_of::<(NamespacePath, Option<FileRecord>)>() as u64;
            let count = pending.len();
            let mut key = Some(key);
            for (index, (destination, current)) in pending.into_iter().enumerate() {
                let source = if index + 1 == count {
                    key.take().expect("final hard link owns its source")
                } else {
                    clone_capture_path(key.as_ref().expect("preceding hard link retains its source"), &mut &*meter)?
                };
                append_hard_link(source,
    destination,
    current,
    mutations,
    &mut &*meter,
)?;
            }
            if let Some(key) = key {
                capture_drop_path(key, (&mut &*meter))?;
            }
            (&mut &*meter).release(pending_bytes).map_err(capture_demand_error)?;
        }
    }
    (&mut &*meter).release(regular_bytes).map_err(capture_demand_error)?;
    if !aliases.is_empty() {
        return Err(CaptureError::Invariant("pending hard-link source was not staged"));
    }
    let aliases_bytes = aliases.storage_bytes();
    drop(aliases);
    (&mut &*meter).release(aliases_bytes).map_err(capture_demand_error)?;
    Ok(())
}

struct StagedRegularBody {
    logical_bytes: u64,
    content: StagedHostRanges,
}

type PendingHardLinks = CaptureMap<NamespacePath, Vec<(NamespacePath, Option<FileRecord>)>>;

#[allow(clippy::too_many_arguments)]
async fn stage_regular_body<A: AsyncAuthorityStore, O: AsyncObjectStore>(stager: &ContentStager<A, O>,
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    snapshot: &HostSnapshot,
    maximum_extent_spans: u32,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<StagedRegularBody, CaptureError> {
    let mut host_work = meter;
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
            |work| work.original().lock().remaining(),
            #[cfg(windows)]
            |file, code, input, output, work| {
                native_device_control_in_scope(file, code, input, output, work)
            },
        ).map_err(capture_demand_error)?;
        let positional = native_positional_file_measured(file, meter)?;
        host_work.leaf(|meter| {
            capture_allocate(meter, std::mem::size_of::<acyclic_native_runtime::NativeFile>() as u64)
        })?;
        let native = std::sync::Arc::new(positional);
        Ok((metadata, logical_bytes, ranges, native))
    })();

    let (metadata, logical_bytes, ranges, native) = opened
        ?;
    #[cfg(windows)]
    let ranges = {
        let ranges = settled_sparse_ranges(&native,
    &metadata,
    ranges,
    maximum_extent_spans,
    cancellation,
    meter,
).await?;

        ranges
    };
    #[cfg(unix)]
    let _ = metadata;
    let staged = stage_host_ranges(stager,
    &native,
    &ranges,
    cancellation,
    meter,
).await;
    let mut content = match staged {
        Ok(content) => content,
        Err(failure) => {
            let ranges_bytes = ranges.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64;
            let native_bytes = std::mem::size_of::<acyclic_native_runtime::NativeFile>() as u64;
            drop(ranges);
            drop(native);
            { let meter = &mut &*meter; meter.release(ranges_bytes).map_err(capture_demand_error)?;
                meter.release(native_bytes).map_err(capture_demand_error) }?;
            return Err(failure);
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
    { let meter = &mut &*meter; meter.release(ranges_bytes).map_err(capture_demand_error)?;
        meter.release(native_bytes).map_err(capture_demand_error) }?;
    if let Err(error) = verified {
        let bytes = content.ranges.capacity() as u64 * std::mem::size_of::<(HostDataRange, StagedContent)>() as u64;
        let ranges = std::mem::take(&mut content.ranges);
        drop(ranges);
        (&mut &*meter).release(bytes).map_err(capture_demand_error)?;
        return Err(error);
    }

    Ok(StagedRegularBody {
        logical_bytes,
        content,
    })
}

fn append_staged_regular_state(path: NamespacePath,
        logical_bytes: u64,
        exists_with_kind: bool,
        canonical_metadata: FileMetadata,
        ranges: Vec<(HostDataRange, StagedContent)>,
        mutations: &mut Vec<AuthoredMutation>,
        meter: &mut impl CaptureNativeWork,
) -> Result<(), CaptureError> {
    { let meter = &mut *meter; let ranges_bytes = ranges.capacity() as u64 * std::mem::size_of::<(HostDataRange, StagedContent)>() as u64;
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
        Ok(()) }
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

/// The link count of the observed node, read by the same handle query as its identity, so it describes exactly the observed node.
#[cfg(unix)]
#[allow(clippy::unnecessary_wraps)]
fn host_link_count(metadata: &HostStat) -> Result<u64, CaptureError> {
    Ok(metadata.nlink())
}

/// The link count of the observed node, read by the same handle query as its identity, so it describes exactly the observed node.
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
    // Compare birth time only when both stat paths expose it; revalidate the final path snapshot.
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

fn ensure_current_host_node<W: CaptureNativeWork>(source_root: &HostRoot,
        host_path: &CaptureHostPath,
        expected: &HostSnapshot,
        cancellation: &CancellationToken,
        meter: &mut W,
) -> Result<(), CaptureError> {
    let verified = (|| {
        cancellation.check().map_err(|_| CaptureError::Cancelled)?;
        meter.check().map_err(capture_demand_error)?;
        let observed = capture_stat(source_root, host_path, meter).map_err(capture_demand_error)?;
        ensure_same_host_node(expected, &observed)?;
        cancellation.check().map_err(|_| CaptureError::Cancelled)?;
        meter.check().map_err(capture_demand_error)
    })();

    verified
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
            Err(CaptureError::StaleVersion)
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
        let root = HostRoot::open(temporary.path())?;
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        let opened = root.open_file(Path::new("current"))?;
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
            ensure_current_host_node(&root,
    &test_host_path(Path::new("current")),
    &expected,
    &CancellationToken::new(),
    &mut DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
),
            Err(OperationFailure { error: CaptureError::StaleVersion, .. })
        ));
        Ok(())
    }
}

struct StagedHostRanges {
    ranges: Vec<(HostDataRange, StagedContent)>,
    bytes: u64,
}

async fn stage_host_ranges<A: AsyncAuthorityStore, O: AsyncObjectStore>(stager: &ContentStager<A, O>,
        native: &std::sync::Arc<acyclic_native_runtime::NativeFile>,
        ranges: &[HostDataRange],
        cancellation: &CancellationToken,
        meter: &CaptureWorkScope,
) -> Result<StagedHostRanges, CaptureError> {

    let mut staged = capture_vec(ranges.len(), (&mut &*meter))?;
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
            |original| stager.stage_with_native_work(&mut bounded, range.length, original, &stage_cancellation),
            meter, &stage_cancellation,
        ).await;
        drop(bounded);

        let content = match content {
            Ok(content) => content,
            Err(failure) => {
                { let meter = &mut &*meter; let bytes = staged.capacity() as u64 * std::mem::size_of::<(HostDataRange, StagedContent)>() as u64;
                    drop(staged);
                    meter.release(bytes).map_err(capture_demand_error) }?;
                return Err(failure);
            }
        };
        if content.logical_bytes() != range.length {
            { let meter = &mut &*meter; let bytes = staged.capacity() as u64 * std::mem::size_of::<(HostDataRange, StagedContent)>() as u64;
                drop(staged);
                meter.release(bytes).map_err(capture_demand_error) }?;
            return Err(CaptureError::StaleVersion);
        }
        bytes = bytes.checked_add(range.length).ok_or_else(|| {
            CaptureError::Work(WorkError::Overflow)
        })?;
        staged.push((*range, content));

    }
    Ok(StagedHostRanges {
        ranges: staged,
            bytes,
    })
}

/// The data ranges of a file marked sparse, settled against its content.
#[cfg(windows)]
async fn settled_sparse_ranges(native: &std::sync::Arc<acyclic_native_runtime::NativeFile>,
        metadata: &HostStat,
        reported: Vec<HostDataRange>,
        maximum_ranges: u32,
        cancellation: &CancellationToken,
        meter: &CaptureWorkScope,
) -> Result<Vec<HostDataRange>, CaptureError> {
    use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_SPARSE_FILE;

    const BLOCK: u64 = 4096;
    const CHUNK: u64 = 1 << 20;
    if metadata.file_attributes() & FILE_ATTRIBUTE_SPARSE_FILE.0 == 0 {
        return Ok(reported);
    }

    let mut settled = Vec::new();
    for range in &reported {
        let mut reader = acyclic_native_runtime::AsyncRangeReader::new(
            std::sync::Arc::clone(native), range.offset, range.length,
        );
        let end = range.offset.checked_add(range.length).ok_or_else(|| {
            CaptureError::Work(WorkError::Overflow)
        })?;
        let mut position = range.offset;
        let mut report_stands = false;
        while position < end {
            let wanted = (CHUNK - position % CHUNK).min(end - position);
            let read = capture_in_heap_with_meter(|| async {
                cancellation.check().map_err(|_| CaptureError::Cancelled)?;
                meter.native().admit(WorkCounters { source_bytes_read: wanted,
                    ..WorkCounters::UNCHARGED })
                    .map_err(CaptureError::NativeKernel)?;
                let runtime = meter.native().runtime_meter_owner_with_capture_custody(
                    meter.custody.as_ref().expect("capture scope is live"))
                    .map_err(CaptureError::NativeKernel)?;
                let value = reader.read_exact_measured(wanted as usize, runtime)
                    .await.map_err(|error| {
                        let cause = if let Some(cause) = meter.original().take_native_io_failure() {
                            capture_demand_error(cause)
                        } else if error.kind() == std::io::ErrorKind::UnexpectedEof {
                            CaptureError::StaleVersion
                        } else {
                            CaptureError::Io(error)
                        };
                        cause
                    })?;
                Ok(value)
            }, meter.native()).await?;

            let chunk = read;
            let read_end = position + chunk.bytes.len() as u64;
            report_stands = { let meter = &mut &*meter; let scanned: Result<bool, CaptureError> = (|| {
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
                // The original native guard remains behind the bytes through every scan and retires only after their physical drop.
                drop(chunk);
                scanned }?;
            position = read_end;
            if report_stands { break; }
        }
        if report_stands {
            { let meter = &mut &*meter; let bytes = settled.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64;
                drop(settled);
                meter.release(bytes).map_err(capture_demand_error) }?;
            return Ok(reported);
        }
    }
    { let meter = &mut &*meter; let bytes = reported.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64;
        drop(reported);
        meter.release(bytes).map_err(capture_demand_error) }?;
    Ok(settled)
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

/// The native file a handle from [`HostRoot::open_file_positional`] reads through.
#[cfg(all(test, windows))]
#[allow(
    unsafe_code,
    reason = "adopts the overlapped handle into the native runtime"
)]
fn native_positional_file(
    file: std::fs::File,
) -> std::io::Result<acyclic_native_runtime::NativeFile> {
    // SAFETY: a positional handle is opened overlapped; this fresh file object is attached to no port, and this sole duplicate carries all of its I/O.
    unsafe { acyclic_native_runtime::NativeFile::from_overlapped_file_unchecked(file) }
}

/// The native file a handle from [`HostRoot::open_file_positional`] reads through.
#[cfg(all(test, not(windows)))]
fn native_positional_file(
    file: std::fs::File,
) -> std::io::Result<acyclic_native_runtime::NativeFile> {
    acyclic_native_runtime::NativeFile::from_file(file)
}

struct NativeRangeSource {
    reader: acyclic_native_runtime::AsyncRangeReader,
    capture_custody: std::sync::Arc<crate::demand::CaptureTransientCustody>,
}

#[allow(unsafe_code, reason = "the measured host opener transfers its fresh positional handle")]
fn native_positional_file_measured(
    file: std::fs::File,
    scope: &CaptureWorkScope,
) -> Result<acyclic_native_runtime::NativeFile, CaptureError> {
    let meter = scope.native().runtime_meter_owner();
    #[cfg(windows)]
    let result = unsafe {
        acyclic_native_runtime::NativeFile::from_overlapped_file_measured_owned_unchecked(file, meter)
    };
    #[cfg(not(windows))]
    let result = acyclic_native_runtime::NativeFile::from_file_measured_owned(file, meter);
    result.map_err(|(error, file)| {
        // This is an ordinary std file, not an explicit SDK close operation. Retire its physical handle before projecting the original failure.
        drop(file);
        capture_demand_error(scope.original().take_native_io_failure()
            .unwrap_or_else(|| native_demand_error(error)))
    })
}

impl NativeRangeSource {
    fn new_captured(reader: acyclic_native_runtime::AsyncRangeReader, scope: &CaptureWorkScope) -> Self {
        Self { reader, capture_custody: std::sync::Arc::clone(
            scope.custody.as_ref().expect("capture scope is live")) }
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
        // This internal producer requires the original native invocation, rather than constructing another counter/budget bank for a caller.
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
        let runtime = original.runtime_meter_owner_with_capture_custody(&self.capture_custody)?;
        let result = reader.read_exact_measured(maximum, runtime).await
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
        // The native packet owns the actual buffer through the copy and any late failure; bytes physically retire before its allocation guard.
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
        let meter = original.runtime_meter_owner_with_capture_custody(
            scope.custody.as_ref().expect("live scope"))?;
        let (started, start) = std::sync::mpsc::channel();
        let (resume, resumed) = std::sync::mpsc::channel();
        let operation = native.control_bounded_async(move |file, meter| {
            started.send(()).map_err(std::io::Error::other)?;
            resumed.recv().map_err(std::io::Error::other)?;
            let mut data = [0; 3];
            assert_eq!(acyclic_native_runtime::read_declared_source_bytes(file, 0, &mut data, meter)?, 3);
            assert_eq!(&data, b"abc");
            Ok(())
        }, meter)?;
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
            let scope = CaptureWorkScope::new(&original)?;
            let native = std::sync::Arc::new(native_positional_file_measured(fixture_file(&path)?, &scope)?);
            let mut source = NativeRangeSource::new_captured(
                acyclic_native_runtime::AsyncRangeReader::new(native, 0, 3), &scope,
            );
            let baseline = original.receipt();
            let live = original.live_allocation_bytes();
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
        let original = capture_original_work(original_budget, &cancellation,
)
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
        let task = acyclic_native_runtime::run_blocking_io_measured(move || {
            let native = capture_native_original(&worker).map_err(|failure| failure.error)?;
            let mut scope = CaptureWorkScope::new(&native).map_err(|failure| failure.error)?;
            let root = HostRoot::open_with_work(&path, &mut &scope)
                .map_err(capture_demand_error)?;
            drop(path);
            worker.with_original_leaf(|work| work.release(path_bytes))
                .map_err(capture_demand_error)?.map_err(capture_demand_error)?;
            started.send(()).expect("caller must observe the pending worker");
            resumed.recv().expect("caller must resume the worker");
            let checked = worker.check();
            scope.release().map_err(|failure| failure.error)?;
            Ok::<_, CaptureError>((root, checked))
        }, task_meter)?;
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
        let original = capture_original_work(original_budget, &cancellation,
)
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
        let task = acyclic_native_runtime::run_blocking_io_measured(move || -> () {
            let native = capture_native_original(&worker)
                .expect("fault fixture must preserve its complete native resource");
            let scope = CaptureWorkScope::new(&native)
                .expect("fault fixture capture scope must be funded");
            let root = HostRoot::open_with_work(&path, &mut &scope)
                .expect("fault fixture root must open");
            drop(path);
            worker.with_original_leaf(|work| work.release(path_bytes))
                .expect("original work must remain available")
                .expect("transferred path claim must retire");
            started.send(()).expect("caller must observe the pending worker");
            resumed.recv().expect("caller must resume the worker");
            drop(root);
            panic!("native capture fault after the completed held-root probe");
        }, task_meter)?;
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
    -> Result<HostRoot, CaptureError> {
    let root = HostRoot::open_with_work(&options.source_root, meter)
        .map_err(capture_demand_error)?;
    if root.identity() != options.expected_root_identity {
        return Err(CaptureError::RootChanged);
    }
    Ok(root)
}

/// Converts one exact namespace path to a host-relative path without lossy text projection.
pub fn namespace_to_host_path(path: &NamespacePath) -> Result<PathBuf, CaptureError> {
    let mut result = PathBuf::new();
    for component in path.components() {
        result.push(capture_host_name(component)?);
    }
    Ok(result)
}

/// Converts one canonical name to its exact host spelling.
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

/// Converts one canonical name to its exact host spelling.
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
        let metadata = test_host_stat(&root, Path::new(name)).map_err(std::io::Error::other)?;
        let native = std::sync::Arc::new(native_positional_file(file.into_std())?);
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| std::io::Error::other(error))?;
        let mut scope = CaptureWorkScope::new(&original)
            .map_err(|failure| std::io::Error::other(failure.error))?;
        scope.native().with_original_leaf(|work| work.adopt(reported.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64))
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let ranges = settled_sparse_ranges(&native,
    &metadata,
    reported,
    maximum_ranges,
    &cancellation,
    &scope,
)
        .await
        .map_err(std::io::Error::other)?;
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

        // Every byte read is charged, and the scan stops at the budget or once cancelled.
        let root = HostRoot::open(directory.path())?;
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
                    let mut work = &scope;
                    work.leaf(|work| work.adopt(reported.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64))
                        .map_err(|error| OperationFailure::new(capture_demand_error(error), work.completed()))?;
                }
                let result = settled_sparse_ranges(&native,
    &metadata,
    reported,
    16,
    &cancellation,
    &scope,
)
                .await.map(drop);
                scope.release()?;
                let work = original.receipt();
                result.map(|()| ((), work))
                    .map_err(|error| OperationFailure::new(error, work))
            }
        };
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
#[allow(clippy::unnecessary_wraps)]
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
        let (bytes, _allocation) = root.read_link_with_work(path, maximum, meter).map_err(capture_demand_error)?;
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

#[allow(clippy::needless_pass_by_value)]
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
