//! Bounded host-state capture into one atomic sparse checkout transaction.

use crate::kernel::{
    FileKind, FileMetadata, FileRecord, LogicalName, MetadataField, NameEncoding, NamespacePath,
};
use crate::model::FilesystemProfile;
use crate::native_host::NativeHostWork as _;
use crate::native_host::{HostDataRange, HostRoot, HostStat, allocated_data_ranges};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, CancellationToken, Checkout,
    NativeRootIdentity, OperationFailure, OperationReceipt, StagedContent, WatchBatch, WatchChange,
    WatchInvalidationReason, WorkBudget, WorkCounters, WorkError,
};

/// Returns the stable identity of a no-follow, capability-held capture root.
pub fn capture_root_identity(path: &Path) -> Result<NativeRootIdentity, CaptureError> {
    HostRoot::open(path, &mut crate::native_host::ordinary_work())
        .map(|root| root.identity())
        .map_err(|_| CaptureError::InvalidOptions)
}
use std::path::{Path, PathBuf};
use thiserror::Error;

#[cfg(feature = "native-mount")]
pub(crate) const MAX_NATIVE_EXACT_CAPTURE_PATHS: usize = 65_536;

/// A native view may have host-generated timestamps that are not SDK metadata.
#[derive(Clone, Copy)]
pub(crate) struct NativeViewBaseline {
    root_identity: NativeRootIdentity,
    read_only: bool,
}

impl NativeViewBaseline {
    /// Records the identity and access mode of the materialized view.
    #[cfg(any(feature = "native-mount", all(test, unix)))]
    pub(crate) const fn new(root_identity: NativeRootIdentity, read_only: bool) -> Self {
        Self {
            root_identity,
            read_only,
        }
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
        let root = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let identity = root.identity();
        let baseline = NativeViewBaseline::new(identity, false);
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
    /// Creates a policy from canonical namespace prefixes.
    pub fn excluding(mut prefixes: Vec<NamespacePath>) -> Result<Self, CaptureError> {
        if prefixes.iter().any(NamespacePath::is_root) {
            return Err(CaptureError::InvalidOptions);
        }
        prefixes.sort_unstable();
        prefixes.dedup_by(|prefix, ancestor| prefix.is_within(ancestor));
        Ok(Self { excluded_prefixes: prefixes })
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

    fn batch(changes: Vec<WatchChange>) -> WatchBatch {
        WatchBatch::Changes {
            original: Default::default(), epoch: 1, first_sequence: 1,
            next_sequence: 1 + changes.len() as u64, changes,
        }
    }

    fn options(root: &Path, maximum_paths: u32) -> Result<CaptureOptions, CaptureError> {
        Ok(CaptureOptions {
            source_root: root.to_path_buf(), expected_root_identity: capture_root_identity(root)?,
            maximum_paths, maximum_extent_spans: 8,
        })
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

        assert!(policy.excludes(&path("/a/cache/object")?));
        assert!(policy.excludes(&path("/m")?));
        assert!(policy.excludes(&path("/z/private/nested/object")?));
        assert!(!policy.excludes(&path("/a/cached")?));
        assert!(!policy.excludes(&path("/n")?));
        assert!(!policy.excludes(&path("/z/public")?));
        Ok(())
    }

    #[tokio::test]
    async fn nested_hints_hydrate_missing_or_stale_ancestors_without_expanding_siblings()
    -> Result<(), Box<dyn std::error::Error>> {
        for (workspace_name, child_name, contents, prior) in [
            ("watch-ancestor", "/sub/changed.txt", "changed", None),
            ("watch-stale-ancestor", "/sub/new.txt", "new", Some("old")),
        ] {
            let temporary = tempfile::tempdir()?;
            std::fs::create_dir(temporary.path().join("sub"))?;
            std::fs::write(temporary.path().join(&child_name[1..]), contents.as_bytes())?;
            if prior.is_none() {
                std::fs::write(temporary.path().join("sub/unrelated.txt"), b"unrelated")?;
            }
            let workspace = crate::Fs::memory()
                .create_workspace(workspace_name, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            if let Some(contents) = prior { workspace.write_text("/sub", contents).await?; }
            let mut checkout = workspace.checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            let child = path(child_name)?;
            let hint = if prior.is_some() { WatchChange::Created(child.clone()) }
                else { WatchChange::Modified(child.clone()) };
            capture_watch_batch(&mut checkout, batch(vec![hint]), &options(temporary.path(), 2)?,
                &CapturePolicy::default(), WorkBudget::UNBOUNDED, &CancellationToken::new()).await?;
            let token = CancellationToken::new();
            let parent = checkout.lookup_no_follow(&path("/sub")?, WorkBudget::UNBOUNDED, &token).await?;
            assert_eq!(parent.value.record.map(|record| record.kind), Some(FileKind::Directory));
            let file = checkout.lookup_no_follow(&child, WorkBudget::UNBOUNDED, &token).await?;
            assert_eq!(file.value.record.map(|record| record.kind), Some(FileKind::Regular));
            if prior.is_none() {
                let unrelated = checkout.lookup_no_follow(&path("/sub/unrelated.txt")?, WorkBudget::UNBOUNDED, &token).await?;
                assert!(unrelated.value.record.is_none());
            }
        }
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
        let options = options(temporary.path(), 8)?;
        capture_baseline(
            &mut checkout,
            &options,
            &crate::CapturePolicy::default(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        capture_watch_batch(
            &mut checkout,
            batch(vec![WatchChange::Modified(path("/pkg")?)]),
            &options,
            &crate::CapturePolicy::default(),
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
    async fn watch_directory_rename_then_child_change_keeps_the_renamed_directory()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("old"))?;
        std::fs::write(temporary.path().join("old/file.txt"), b"old")?;
        let workspace = crate::Fs::memory()
            .create_workspace("watch-renamed-directory", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let mut transaction = workspace.begin_transaction(crate::IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.create_dir_all("/old", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
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
            batch(vec![
                    WatchChange::Renamed {
                        from: old.clone(),
                        to: new.clone(),
                    },
                    WatchChange::Modified(child.clone()),
                ]),
            &options(temporary.path(), 3)?,
            &crate::CapturePolicy::default(),
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
            .create_workspace("watch-expansion", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let mut checkout = workspace.checkout(crate::model::GenerationSelector::Head, crate::model::CheckoutMode::tracking_transaction(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        capture_watch_batch(
            &mut checkout,
            batch(vec![
                    WatchChange::Modified(path("/modified")?),
                    WatchChange::Arrived(path("/arrived")?),
                ]),
            &options(temporary.path(), 8)?,
            &CapturePolicy::default(),
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
            batch(vec![
                    WatchChange::Renamed {
                        from: path("/old.txt")?,
                        to: path("/sub/intermediate.txt")?,
                    },
                    WatchChange::Renamed {
                        from: path("/sub/intermediate.txt")?,
                        to: path("/final.txt")?,
                    },
                ]),
            &options(temporary.path(), 3)?,
            &crate::CapturePolicy::default(),
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
    pub epoch: u64,
    /// First process-local sequence represented by the batch.
    pub first_sequence: u64,
    /// Sequence safe to request after this transaction succeeds.
    pub next_sequence: u64,
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
    /// The canonical original filesystem operation retained its typed cause.
    #[error(transparent)]
    Kernel(#[from] crate::FsError),
    /// A fixed capture/SDK progress invariant failed without formatting data.
    #[error("{0}")]
    Invariant(&'static str),

}

impl From<crate::CancellationError> for CaptureError {
    fn from(error: crate::CancellationError) -> Self { Self::Kernel(error.into()) }
}
impl From<crate::NativeTreeMaterializationError> for CaptureError {
    fn from(error: crate::NativeTreeMaterializationError) -> Self { Self::Kernel(crate::FsError::NativeKernel(error)) }
}
impl From<WorkError> for CaptureError {
    fn from(error: WorkError) -> Self { Self::Kernel(crate::FsError::Work(error)) }
}
impl From<std::io::Error> for CaptureError {
    fn from(error: std::io::Error) -> Self { crate::NativeTreeMaterializationError::Io(error).into() }
}
impl From<std::collections::TryReserveError> for CaptureError {
    fn from(error: std::collections::TryReserveError) -> Self { crate::NativeTreeMaterializationError::AllocationFailed(error).into() }
}

/// Reads a bounded set of final host states and applies one atomic checkout transaction.
pub async fn capture_paths<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    options: &CaptureOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let native = capture_work(budget, cancellation)?;
    let original = native.original_demand_work();
    let result = async {
        validate_path_count(paths, options).map_err(OperationFailure::before_work)?;
        let scope = CaptureWorkScope::new(&native)?;
        let result = async {
            let source_root = open_source_root(options, &scope)?;
            let mut unique = clone_capture_paths(paths, None, &scope)?;
            capture_sort_by(&mut unique, Ord::cmp, &scope)?;
            if capture_has_duplicate_paths(&unique, &scope)? {
                return Err(CaptureError::InvalidOptions);
            }
            let observed = observe_unique_paths(unique, &source_root, cancellation, &scope)?;
            capture_observed_paths_from_root(checkout, observed, options.maximum_extent_spans,
                &source_root, cancellation, None, &scope).await
        }.await;
        capture_scope_result(scope, result)
    }.await;
    capture_original_result(&original, result)
}

#[cfg(feature = "native-mount")]
pub(crate) async fn capture_paths_batched<A: AsyncAuthorityStore, O: AsyncObjectStore>(
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
        validate_path_count(paths, options)
        ?;
        if batch_size == 0 || paths.len() > MAX_NATIVE_EXACT_CAPTURE_PATHS {
            return Err(CaptureError::InvalidOptions);
        }
        cancellation.check()?;
        meter.source().check()
        .map_err(capture_demand_error)?;
        if paths.is_empty() {
            return Ok(CaptureReceipt::default());
        }
        meter.native.with_original_leaf(|work| work.cancellation().clone())
    };
    let source_root = open_source_root(options, meter)?;
    let (mut receipt, mut candidate, unique) = {
        let mut unique = clone_capture_paths(paths, None, meter)
        ?;
        capture_sort_by(&mut unique, Ord::cmp, meter)
        ?;
        if capture_has_duplicate_paths(&unique, meter)
        ? {
            return Err(CaptureError::InvalidOptions);
        }
        let receipt = CaptureReceipt::default();
        let candidate = checkout.private_candidate(&meter.native, &cancellation)?;
        (receipt, candidate, unique)
    };
    let observed = observe_unique_paths(unique, &source_root, &cancellation, meter)?;
    let states = lookup_capture_states(&mut candidate, observed, batch_size, meter).await?;
    let mut host_links = CaptureMap::new();
    let states_bytes = states.capacity() as u64 * std::mem::size_of::<CapturePathState>() as u64;
    let mut states = states.into_iter();
    loop {
        let batch = {
            let count = states.len().min(batch_size);
            let mut batch = capture_vec(count, meter)
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
        receipt.examined_paths = receipt.examined_paths.checked_add(captured.examined_paths)
        .ok_or(CaptureError::from(WorkError::Overflow))?;
        receipt.changed_paths = receipt.changed_paths.checked_add(captured.changed_paths)
        .ok_or(CaptureError::from(WorkError::Overflow))?;
        receipt.staged_file_bytes = receipt.staged_file_bytes.checked_add(captured.staged_file_bytes)
        .ok_or(CaptureError::from(WorkError::Overflow))?;
    }
    drop(states);
    {
        { meter.source().release(states_bytes).map_err(capture_demand_error)?;
            release_host_links(host_links, meter) }?;
        meter.source().check()
        .map_err(capture_demand_error)?;
    }
    cancellation.commit_if_live(|| { *checkout = candidate; })?;
    Ok(receipt)

}

/// Reconciles one host directory and every descendant as one authored transaction.
pub async fn capture_subtree<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    root: NamespacePath,
    options: &CaptureOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let native = capture_work(budget, cancellation)?;
    let original = native.original_demand_work();
    let result = capture_subtrees_with_native_work(
        checkout, &[root], options, &CapturePolicy::default(), cancellation, None, &native,
    ).await;
    capture_original_result(&original, result)
}

fn capture_work(budget: WorkBudget, cancellation: &CancellationToken,
) -> Result<crate::NativeMaterializationWork, OperationFailure<CaptureError>> {
    crate::NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(error, completed)| OperationFailure::new(CaptureError::from(error), completed))
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

struct CaptureWorkScope {
    native: crate::NativeMaterializationWork,
    custody: Option<std::sync::Arc<crate::demand::CaptureTransientCustody>>,
}

impl CaptureWorkScope {
    fn new(native: &crate::NativeMaterializationWork)
        -> Result<Self, OperationFailure<CaptureError>>
    {
        let custody = crate::demand::CaptureTransientCustody::new_native(native)
            .map_err(|error| OperationFailure::new(CaptureError::from(error), native.receipt()))?;
        let view = native.with_capture_custody(&custody)
            .map_err(|error| OperationFailure::new(CaptureError::from(error), native.receipt()))?;
        Ok(Self {
            native: view,
            custody: Some(custody),
        })
    }

    fn source(&self) -> crate::demand::NativeSourceWork<'_> {
        crate::demand::NativeSourceWork::from_native_materialization_work(&self.native)
    }

    fn release(&mut self) -> Result<(), CaptureError> {
        // Accepted workers and source packets keep the token until their payloads retire.
        let Some(custody) = self.custody.take() else { return Ok(()) };
        while std::sync::Arc::strong_count(&custody) != 1 {
            std::thread::yield_now();
        }
        let bytes = custody.take_retained_bytes();
        drop(custody);
        self.native.with_original_leaf(|work| work.with_capture(None, |work| work.release(bytes)))
            .map_err(capture_demand_error)
    }
}

impl Drop for CaptureWorkScope {
    fn drop(&mut self) { let _ = self.release(); }
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
    let header = native.claim(std::mem::size_of::<CapturedRuntimeReadMeter>() as u64, u64::from((std::mem::size_of::<CapturedRuntimeReadMeter>() as u64) != 0), crate::WorkCounters::UNCHARGED)?;
    let base = native.runtime_meter();
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
    acyclic_native_runtime::device_control_in_place(
        file, code, input, output, Some((scope.native.runtime_meter().as_ref(), acyclic_native_runtime::NativeOperationKind::Read)),
    ).map_err(|error| scope.native.native_read_meter().take_native_io_failure()
        .unwrap_or_else(|| native_demand_error(error)))
}

fn capture_scope_result<T>(
    mut scope: CaptureWorkScope,
    result: Result<T, CaptureError>,
) -> Result<OperationReceipt<T>, OperationFailure<CaptureError>> {
    let released = scope.release();
    let completed = scope.native.native_read_meter().lock().work;
    result.and_then(|value| released.map(|()| value))
        .map(|value| OperationReceipt { value, work: completed })
        .map_err(|error| OperationFailure::new(error, completed))
}

async fn capture_sdk<F, T>(
    future: F,
    meter: &CaptureWorkScope,
    cancellation: &CancellationToken,
) -> Result<T, CaptureError>
where
    F: std::future::Future<Output = Result<T, crate::FsError>>,
{
    cancellation.check()?;
    meter.native.admit(WorkCounters::UNCHARGED).map_err(CaptureError::from)?;
    let value = future.await?;
    cancellation.check()?;
    meter.native.admit(WorkCounters::UNCHARGED).map_err(CaptureError::from)?;
    Ok(value)
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
        if options.maximum_paths == 0 || options.maximum_extent_spans == 0 {
            return Err(CaptureError::InvalidOptions);
        }
        cancellation.check()?;
        meter.source().check()
        .map_err(capture_demand_error)?;
        if roots.is_empty() {
            return Ok(CaptureReceipt::default());
        }
        meter.native.with_original_leaf(|work| work.cancellation().clone())
    };
    let limits = checkout.volume_config().limits;
    let profile = checkout.volume_config().profile;
    let (canonical, mut paths, host_roots, host_policy, source_path, source_path_bytes, maximum) = {
        let canonical = canonical_subtree_roots_measured(roots, policy, meter)
        ?;
        if canonical.is_empty() {
            return Ok(CaptureReceipt::default());
        }
        let maximum = usize::try_from(options.maximum_paths)
        .map_err(|_| CaptureError::InvalidOptions)?;
        let mut paths = CaptureMap::new();
        for path in &canonical {
            let path = clone_capture_path(path, meter)?;
            append_scanned_path(&mut paths, path, maximum, meter)?;
        }
        let host_roots = clone_capture_paths(&canonical, None, meter)
        ?;
        let host_policy = CapturePolicy {
            excluded_prefixes: clone_capture_paths(&policy.excluded_prefixes, None, meter)?,
        };
        let (source_path, source_path_bytes) =
            crate::native_host::clone_path(&options.source_root, &mut meter.source())
                .map_err(capture_demand_error)?;
        (
            canonical,
            paths,
            host_roots,
            host_policy,
            source_path,
            source_path_bytes,
            maximum,
        )
    };
    let expected_identity = options.expected_root_identity;
    let worker = meter.native.clone();
    let capture = std::sync::Arc::clone(meter.custody.as_ref().expect("capture scope is live"));
    let task_meter = meter.native.runtime_meter();
    let task = acyclic_native_runtime::run_blocking_io(
        move || {
            let worker = CaptureWorkScope {
                native: worker,
                custody: None,
            };
            let result = (|| {
                let source_root = HostRoot::open(&source_path, &mut worker.source())
                    .map_err(capture_demand_error)?;
                if source_root.identity() != expected_identity {
                    return Err(CaptureError::RootChanged);
                }
                let mut observed = CaptureMap::new();
                collect_host_subtree_roots(
                    &source_root,
                    profile,
                    limits,
                    maximum,
                    &host_roots,
                    &host_policy,
                    &mut observed,
                    &worker,
                )?;
                Ok((source_root, observed))
            })();
            let cleanup = (|| {
                release_capture_paths(host_roots, &worker)?;
                release_capture_paths(host_policy.excluded_prefixes, &worker)?;
                drop(source_path);
                worker
                    .source()
                    .release(source_path_bytes)
                    .map_err(capture_demand_error)
            })();
            let result = result.and_then(|value| cleanup.map(|()| value));
            // Completion storage drops the payload before its accounting token.
            (result, capture)
        },
        Some((task_meter, false)),
    );
    let result = match task {
        Ok(task) => task.await.map_err(native_demand_error),
        Err(error) => Err(native_demand_error(error)),
    };
    let (result, _capture) = result.map_err(capture_demand_error)?;
    let (source_root, observed) = result?;
    let lookup_cancellation = meter.native.with_original_leaf(|work| work.cancellation().clone());
    let current = capture_sdk(
        checkout.lookup_batch_no_follow_with_native_work(&canonical, &meter.native, &lookup_cancellation),
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
            capture_drop_path(root, meter)?;
        }
    }
    drop(current.1);
    let ordered = {
        meter.source().release(canonical_bytes).map_err(capture_demand_error)?;
        order_host_checkout_union(observed, paths, maximum, meter)
        ?
    };
    capture_observed_paths_from_root(checkout,
        ordered,
        options.maximum_extent_spans,
        &source_root,
        &cancellation,
        baseline,
        meter,
    ).await

}

fn capture_demand_error(error: DemandError) -> CaptureError {
    match error {
        DemandError::Work(error) => CaptureError::from(error),
        DemandError::Io(error) => CaptureError::from(error),
        DemandError::Allocation(error) => CaptureError::from(error),
        DemandError::StaleVersion => CaptureError::Kernel(crate::FsError::SourceSnapshotStale),
        DemandError::Cancelled => crate::CancellationError.into(),
        DemandError::OriginalWorkPoisoned => crate::NativeTreeMaterializationError::OriginalWorkPoisoned.into(),
        _ => CaptureError::InvalidOptions,
    }
}

trait CaptureKey: Ord {
    fn owned_bytes(&self) -> u64;
}

impl CaptureKey for NamespacePath {
    fn owned_bytes(&self) -> u64 { self.retained_bytes() }
}

impl CaptureKey for [u8; 16] {
    fn owned_bytes(&self) -> u64 { 0 }
}

struct CaptureMap<K, V> {
    entries: Vec<(K, V)>,
}

impl<K: CaptureKey, V> CaptureMap<K, V> {
    const fn new() -> Self { Self { entries: Vec::new() } }
    fn len(&self) -> usize { self.entries.len() }
    fn storage_bytes(&self) -> u64 {
        self.entries.capacity() as u64 * std::mem::size_of::<(K, V)>() as u64
    }
    fn position_measured(&self, key: &K, meter: &CaptureWorkScope) -> Result<Result<usize, usize>, CaptureError> {
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
    fn contains_key_measured(&self, key: &K, meter: &CaptureWorkScope) -> Result<bool, CaptureError> {
        Ok(self.position_measured(key, meter)?.is_ok())
    }
    fn get_measured(&self, key: &K, meter: &CaptureWorkScope) -> Result<Option<&V>, CaptureError> {
        Ok(self.position_measured(key, meter)?.ok().map(|index| &self.entries[index].1))
    }
    fn insert_at(&mut self, index: usize, key: K, value: V, meter: &CaptureWorkScope) -> Result<(), CaptureError> {
        let copied = (self.entries.len() - index).checked_mul(std::mem::size_of::<(K, V)>())
            .ok_or(CaptureError::from(WorkError::Overflow))? as u64;
        let delta = WorkCounters { bytes_copied: copied, ..WorkCounters::UNCHARGED };
        capture_reserve(&mut self.entries, 1, meter)?;
        capture_charge(meter, delta)?;
        self.entries.insert(index, (key, value));
        Ok(())
    }
    fn insert_measured(&mut self, key: K, value: V, meter: &CaptureWorkScope) -> Result<Option<V>, CaptureError> {
        match self.position_measured(&key, meter)? {
            Ok(index) => {
                let bytes = key.owned_bytes();
                drop(key);
                meter.native.with_original_leaf(|work| work.release(bytes)).map_err(capture_demand_error)?;
                Ok(Some(std::mem::replace(&mut self.entries[index].1, value)))
            }
            Err(index) => {
                self.insert_at(index, key, value, meter)?;
                Ok(None)
            }
        }
    }
    fn get_or_insert_measured(&mut self, key: K, value: V, meter: &CaptureWorkScope) -> Result<&mut V, CaptureError> {
        let index = match self.position_measured(&key, meter)? {
            Ok(index) => {
                let bytes = key.owned_bytes();
                drop(key);
                meter.native.with_original_leaf(|work| work.release(bytes)).map_err(capture_demand_error)?;
                index
            }
            Err(index) => {
                self.insert_at(index, key, value, meter)?;
                index
            }
        };
        Ok(&mut self.entries[index].1)
    }
    fn remove_measured(&mut self, key: &K, meter: &CaptureWorkScope) -> Result<Option<(K, V)>, CaptureError> {
        let Ok(index) = self.position_measured(key, meter)? else { return Ok(None) };
        let copied = (self.entries.len() - index - 1).checked_mul(std::mem::size_of::<(K, V)>())
            .ok_or(CaptureError::from(WorkError::Overflow))? as u64;
        capture_charge(meter, WorkCounters { bytes_copied: copied, ..WorkCounters::UNCHARGED })?;
        Ok(Some(self.entries.remove(index)))
    }
}

fn capture_sort_by<T>(
    values: &mut [T],
    mut compare: impl FnMut(&T, &T) -> std::cmp::Ordering,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    fn sift<T>(
        values: &mut [T], mut root: usize, end: usize,
        compare: &mut impl FnMut(&T, &T) -> std::cmp::Ordering,
        meter: &CaptureWorkScope,
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
    meter.native.with_original_leaf(|work| work.check()).map_err(capture_demand_error)?;
    let length = values.len();
    for root in (0..length / 2).rev() {
        sift(values, root, length, &mut compare, meter)?;
    }
    for end in (1..length).rev() {
        meter.native.with_original_leaf(|work| work.check()).map_err(capture_demand_error)?;
        values.swap(0, end);
        sift(values, 0, end, &mut compare, meter)?;
    }
    Ok(())
}

fn capture_allocate(meter: &CaptureWorkScope, bytes: u64) -> Result<(), CaptureError> {
    meter.native.with_original_leaf(|work| work.allocate(bytes)).map_err(capture_demand_error)
}

fn capture_charge(meter: &CaptureWorkScope, work: WorkCounters) -> Result<(), CaptureError> {
    meter.native.with_original_leaf(|meter| meter.charge(work)).map_err(capture_demand_error)
}

fn clone_capture_name(name: &LogicalName, meter: &CaptureWorkScope) -> Result<LogicalName, CaptureError> {
    capture_charge(meter, WorkCounters { source_path_components: 1, items_examined: 1,
        ..WorkCounters::UNCHARGED })?;
    let copied = WorkCounters { source_bytes_read: name.as_bytes().len() as u64,
        bytes_copied: name.as_bytes().len() as u64, ..WorkCounters::UNCHARGED };
    meter.source().admit(copied).map_err(capture_demand_error)?;
    let mut bytes = capture_vec(name.as_bytes().len(), meter)?;
    bytes.extend_from_slice(name.as_bytes());
    meter.source().record(copied).map_err(capture_demand_error)?;
    LogicalName::new(name.encoding(), bytes, u32::MAX).map_err(CaptureError::Name)
}

fn clone_capture_prefix(path: &NamespacePath, depth: usize, limits: crate::model::VolumeLimits,
    meter: &CaptureWorkScope) -> Result<NamespacePath, CaptureError> {
    let names = path.components().get(..depth)
        .ok_or(CaptureError::Path(crate::kernel::NamespacePathError::TooDeep))?;
    NamespacePath::validate_component_lengths(names.iter().map(|name| name.as_bytes().len()), limits)
        .map_err(CaptureError::Path)?;
    let mut components = capture_vec(depth, meter)?;
    for name in names { components.push(clone_capture_name(name, meter)?); }
    NamespacePath::new(components, limits).map_err(CaptureError::Path)
}

fn capture_vec<T>(count: usize, meter: &CaptureWorkScope) -> Result<Vec<T>, CaptureError> {
    let bytes = count.checked_mul(std::mem::size_of::<T>()).ok_or(CaptureError::from(WorkError::Overflow))?;
    capture_allocate(meter, bytes as u64)?;
    let mut retained = bytes as u64;
    let mut values = Vec::new();
    let reserved = values.try_reserve_exact(count);
    let actual = values.capacity().checked_mul(std::mem::size_of::<T>())
        .ok_or(CaptureError::from(WorkError::Overflow))? as u64;
    // Reconcile completed capacity before any late refusal; the scope retires failed payloads.
    let reconciled = meter.native.with_original_leaf(|work| work.reconcile_native_allocation(&mut retained, actual));
    reserved.map_err(CaptureError::from)?;
    reconciled.map_err(capture_demand_error)?;
    Ok(values)
}

fn capture_reserve<T>(values: &mut Vec<T>, additional: usize, meter: &CaptureWorkScope) -> Result<(), CaptureError> {
    meter.native.with_original_leaf(|work| work.check()).map_err(capture_demand_error)?;
    let wanted = values.len().checked_add(additional).ok_or(CaptureError::from(WorkError::Overflow))?;
    if wanted <= values.capacity() { return Ok(()); }
    let size = std::mem::size_of::<T>();
    let old = values.capacity().checked_mul(size).ok_or(CaptureError::from(WorkError::Overflow))?;
    let copied = values.len().checked_mul(size).ok_or(CaptureError::from(WorkError::Overflow))?;
    let grown = wanted.max(values.capacity().saturating_mul(2).max(4));
    let capacity = match grown.checked_mul(size) {
        Some(bytes) if bytes as u64 <= meter.native.with_original_leaf(|work| work.remaining())
            .map_err(capture_demand_error)?.peak_allocation_bytes => grown,
        _ => wanted,
    };
    let mut replacement = capture_vec(capacity, meter)?;
    capture_charge(meter, WorkCounters { bytes_copied: copied as u64, ..WorkCounters::UNCHARGED })?;
    replacement.append(values);
    let old_values = std::mem::replace(values, replacement);
    drop(old_values);
    meter.native.with_original_leaf(|work| work.release(old as u64)).map_err(capture_demand_error)
}

fn capture_excludes(policy: &CapturePolicy, path: &NamespacePath, meter: &CaptureWorkScope) -> Result<bool, CaptureError> {
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
    meter: &CaptureWorkScope,
) -> Result<Vec<NamespacePath>, CaptureError> {
    let mut result = capture_vec(paths.len(), meter)?;
    for path in paths {
        capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if let Some(policy) = policy && capture_excludes(policy, path, meter)? { continue; }
        result.push(clone_capture_path(path, meter)?);
    }
    Ok(result)
}

fn release_capture_paths(paths: Vec<NamespacePath>, meter: &CaptureWorkScope) -> Result<(), CaptureError> {
    let bytes = paths.iter().try_fold(
        paths.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64,
        |bytes, path| bytes.checked_add(path.retained_bytes())
            .ok_or(CaptureError::from(WorkError::Overflow)),
    )?;
    drop(paths);
    meter.source().release(bytes).map_err(capture_demand_error)
}
fn capture_drop_host_path(path: CaptureHostPath, meter: &CaptureWorkScope) -> Result<(), CaptureError> {
    #[cfg(unix)]
    let bytes = path.capacity() as u64;
    #[cfg(windows)]
    let bytes = path.capacity() as u64 * 2;
    drop(path);
    meter.source().release(bytes).map_err(capture_demand_error)
}

fn capture_has_duplicate_paths(
    paths: &[NamespacePath], meter: &CaptureWorkScope,
) -> Result<bool, CaptureError> {
    for pair in paths.windows(2) {
        capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if matches!(pair, [left, right] if left == right) { return Ok(true); }
    }
    Ok(false)
}

fn canonical_subtree_roots_measured(
    roots: &[NamespacePath], policy: &CapturePolicy, meter: &CaptureWorkScope,
) -> Result<Vec<NamespacePath>, CaptureError> {
    let mut sorted = clone_capture_paths(roots, Some(policy), meter)?;
    capture_sort_by(&mut sorted, Ord::cmp, meter)?;
    for root in &sorted {
        capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        for _ in root.components() {
            capture_charge(meter, WorkCounters { source_path_components: 1, ..WorkCounters::UNCHARGED })?;
        }
    }
    let mut failure = None;
    sorted.dedup_by(|root, ancestor| {
        if failure.is_some() || !root.is_within(ancestor) { return false; }
        let removed = std::mem::replace(root,
            NamespacePath::new(Vec::new(), crate::model::VolumeLimits::default())
                .expect("an empty namespace path is valid"));
        failure = capture_drop_path(removed, meter).err();
        true
    });
    if let Some(error) = failure { return Err(error); }
    Ok(sorted)
}

fn clone_capture_path(path: &NamespacePath, meter: &CaptureWorkScope) -> Result<NamespacePath, CaptureError> {
    use crate::kernel::{ReadAllocationStep, NamespacePathStep};
    let cancellation = meter.native.with_original_leaf(|work| work.cancellation().clone());
    path.clone_with_steps(&cancellation, &mut |step| -> Result<(), CaptureError> {
        match step {
                NamespacePathStep::Admit(delta) => {
                    let mut delta = *delta;
                    delta.source_bytes_read = delta.bytes_copied;
                    meter.source().admit(delta).map_err(capture_demand_error)
                }
                NamespacePathStep::Performed(delta) => {
                    let mut delta = *delta;
                    delta.source_bytes_read = delta.bytes_copied;
                    meter.source().record(delta).map_err(capture_demand_error)
                }
                NamespacePathStep::Charge(delta) => capture_charge(meter, *delta),
                NamespacePathStep::Allocation(ReadAllocationStep::Allocate { bytes, operations }) => {
                    meter.native.with_original_leaf(|work| work.claim_allocation(bytes, operations)).map_err(capture_demand_error)
                }
                NamespacePathStep::Allocation(ReadAllocationStep::Capacity { admitted_bytes, actual_bytes }) => {
                    let mut allocation = admitted_bytes;
                    meter.native.with_original_leaf(|work| work.reconcile_native_allocation(&mut allocation, actual_bytes))
                        .map_err(capture_demand_error)
                }
                NamespacePathStep::Allocation(ReadAllocationStep::ReserveFailed(error)) =>
                    Err(CaptureError::from(error)),
                NamespacePathStep::Allocation(ReadAllocationStep::BeforeCopy(bytes)) =>
                    meter.source().admit(WorkCounters { bytes_copied: bytes, source_bytes_read: bytes,
                        ..WorkCounters::UNCHARGED }).map_err(capture_demand_error),
                NamespacePathStep::Allocation(ReadAllocationStep::Copied(bytes)) =>
                    meter.source().record(WorkCounters { bytes_copied: bytes, source_bytes_read: bytes,
                        ..WorkCounters::UNCHARGED }).map_err(capture_demand_error),
                NamespacePathStep::Allocation(ReadAllocationStep::BeforeHash(bytes)) => {
                    meter.source().check().map_err(capture_demand_error)?;
                    meter.source().admit(WorkCounters { bytes_hashed: bytes, ..WorkCounters::UNCHARGED }).map_err(capture_demand_error)
                }
                NamespacePathStep::Allocation(ReadAllocationStep::Hashed(bytes)) => {
                    meter.source().record(WorkCounters { bytes_hashed: bytes, ..WorkCounters::UNCHARGED }).map_err(capture_demand_error)?;
                    meter.source().check().map_err(capture_demand_error)
                }
                NamespacePathStep::Allocation(ReadAllocationStep::BeforeItem) =>
                    meter.source().admit(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }).map_err(capture_demand_error),
                NamespacePathStep::Allocation(ReadAllocationStep::Item) =>
                    capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }),
        }
    })
}

#[cfg(unix)]
type CaptureHostPath = PathBuf;
#[cfg(windows)]
type CaptureHostPath = Vec<u16>;

fn namespace_to_host_path_measured(path: &NamespacePath, meter: &CaptureWorkScope) -> Result<CaptureHostPath, CaptureError> {
    #[cfg(unix)]
    let result = namespace_to_host_path_with_work(path, &mut meter.source()).map(|(path, _allocation)| path);
    #[cfg(windows)]
    let result = namespace_host_units_with_work(path, &mut meter.source(), u16::from(b'\\'), namespace_component_wide)
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

fn capture_stat(
    root: &HostRoot,
    path: &CaptureHostPath,
    meter: &CaptureWorkScope,
) -> Result<HostStat, DemandError> {
    #[cfg(unix)]
    {
        root.stat(path, &mut meter.source())
    }
    #[cfg(windows)]
    {
        root.stat_wide(path, &mut meter.source())
    }
}

fn collect_host_walk_measured(
    root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    host_root: CaptureHostPath,
    volume_root: NamespacePath,
    policy: &CapturePolicy,
    meter: &CaptureWorkScope,
    mut insert: impl FnMut(NamespacePath, HostStat, &CaptureWorkScope) -> Result<(), CaptureError>,
) -> Result<(), CaptureError> {
    let slot = std::mem::size_of::<(CaptureHostPath, NamespacePath)>() as u64;
    let mut pending = capture_vec(1, meter)?;
    pending.push((host_root, volume_root));
    while let Some((host_parent, volume_parent)) = pending.pop() {
        meter.source().check().map_err(capture_demand_error)?;
        #[cfg(unix)]
        let (mut reader, reader_allocation) = root
            .read_dir_stats(&host_parent, &mut meter.source())
            .map_err(capture_demand_error)?;
        #[cfg(windows)]
        let (mut reader, reader_allocation) = root
            .read_dir_stats_wide(&host_parent, &mut meter.source())
            .map_err(capture_demand_error)?;
        loop {
            #[cfg(unix)]
            let entry = reader
                .next(&mut meter.source())
                .map_err(capture_demand_error)?;
            #[cfg(windows)]
            let entry = reader
                .next_wide(&mut meter.source())
                .map_err(capture_demand_error)?;
            let Some((entry, name_allocation)) = entry else {
                break;
            };
            let name = (|| {
                #[cfg(unix)]
                let name = {
                    let raw = entry.name.as_encoded_bytes();
                    capture_charge(meter, WorkCounters { items_examined: raw.len() as u64, ..WorkCounters::UNCHARGED })?;
                    let (encoding, bytes) = if profile == FilesystemProfile::Windows {
                        let text = std::str::from_utf8(raw).map_err(|_| CaptureError::UnrepresentablePath)?;
                        let length = text.encode_utf16().count().checked_mul(2)
                            .ok_or(CaptureError::from(WorkError::Overflow))?;
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
                            .ok_or(CaptureError::from(WorkError::Overflow))?, meter)?;
                        for unit in &entry.name {
                            capture_charge(meter, WorkCounters { bytes_copied: 2, ..WorkCounters::UNCHARGED })?;
                            bytes.extend_from_slice(&unit.to_le_bytes());
                        }
                        (NameEncoding::WindowsUtf16Le, bytes)
                    } else {
                        let mut length = 0usize;
                        for character in char::decode_utf16(entry.name.iter().copied()) {
                            meter.source().check().map_err(capture_demand_error)?;
                            length = length.checked_add(character.map_err(|_| CaptureError::UnrepresentablePath)?.len_utf8())
                                .ok_or(CaptureError::from(WorkError::Overflow))?;
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
            meter.source().release(name_allocation).map_err(capture_demand_error)?;
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
        meter.source().release(reader_allocation).map_err(capture_demand_error)?;
        capture_drop_host_path(host_parent, meter)?;
        capture_drop_path(volume_parent, meter)?;
    }
    let pending_bytes = pending.capacity() as u64 * slot;
    drop(pending);
    meter.source().release(pending_bytes).map_err(capture_demand_error)?;
    Ok(())
}

fn append_path_measured(
    parent: &NamespacePath,
    name: LogicalName,
    limits: crate::model::VolumeLimits,
    meter: &CaptureWorkScope,
) -> Result<NamespacePath, CaptureError> {
    meter.source().check().map_err(capture_demand_error)?;
    NamespacePath::validate_component_lengths(
        parent.components().iter().chain(std::iter::once(&name)).map(|name| name.as_bytes().len()), limits,
    ).map_err(CaptureError::Path)?;
    let depth = parent.depth().checked_add(1).ok_or(CaptureError::from(WorkError::Overflow))?;
    capture_charge(meter, WorkCounters { source_path_components: 1, items_examined: 1,
        ..WorkCounters::UNCHARGED })?;
    let mut components = capture_vec(depth, meter)?;
    for component in parent.components() {
        components.push(clone_capture_name(component, meter)?);
    }
    components.push(name);
    NamespacePath::new(components, limits).map_err(CaptureError::Path)
}

fn observe_unique_paths(
    paths: Vec<NamespacePath>,
    source_root: &HostRoot,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(Vec<Option<HostStat>>, Vec<NamespacePath>), CaptureError> {
    let mut observations = capture_vec(paths.len(), meter)?;
    for path in &paths {
        capture_charge(meter, WorkCounters { source_entries_visited: 1, ..WorkCounters::UNCHARGED })?;
        let host_path = namespace_to_host_path_measured(path, meter)?;
        observations.push(observe_host_path(source_root, &host_path, meter)?);
        capture_drop_host_path(host_path, meter)?;
    }
    cancellation.check()?;
    Ok((observations, paths))
}

fn observe_host_path(
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    meter: &CaptureWorkScope,
) -> Result<Option<HostStat>, CaptureError> {
    classify_host_observation(capture_stat(source_root, host_path, meter))
}

fn classify_host_observation(
    metadata: Result<HostStat, DemandError>,
) -> Result<Option<HostStat>, CaptureError> {
    match metadata {
        Ok(metadata) => Ok(Some(metadata)),
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
                Err(CaptureError::Kernel(crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Io(_))))
            ));
        }
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("file"), b"body")?;
        let source_root =
            HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work())
                .map_err(crate::native_host::ordinary_error)?;
        let native =
            crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .map_err(|(error, _)| error)?;
        let scope = CaptureWorkScope::new(&native)?;
        assert!(observe_host_path(&source_root, &test_host_path(Path::new("file/child")), &scope)?.is_none());
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
        let source_root =
            HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work())
                .map_err(crate::native_host::ordinary_error)?;
        assert!(
            source_root
                .stat(Path::new("alias/private"), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)
                .is_err()
        );
        Ok(())
    }
}

async fn capture_observed_paths_from_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(checkout: &mut Checkout<A, O>,
    observed: (Vec<Option<HostStat>>, Vec<NamespacePath>),
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
    meter: &CaptureWorkScope,
) -> Result<CaptureReceipt, CaptureError> {
    let states = lookup_capture_states(checkout, observed, usize::MAX, meter).await?;
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
    let cleanup = release_host_links(host_links, meter);
    match captured {
        Ok(captured) => { cleanup?; Ok(captured) }
        Err(error) => Err(error),
    }

}

#[allow(clippy::too_many_arguments)]
async fn capture_ranked_paths_from_root<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    states: Vec<CapturePathState>,
    host_links: &mut CaptureMap<[u8; 16], NamespacePath>,
    maximum_extent_spans: u32,
    source_root: &HostRoot,
    cancellation: &CancellationToken,
    baseline: Option<&NativeViewBaseline>,
    meter: &CaptureWorkScope,
) -> Result<CaptureReceipt, CaptureError> {
    let states_bytes = states.capacity() as u64 * std::mem::size_of::<CapturePathState>() as u64;
    let (mut receipt, mut prepared, cancellation) = {
        cancellation.check()?;
        meter.source().check()
            .map_err(capture_demand_error)?;
        let prepared = capture_vec(states.len(), meter)
            ?;
        (
            CaptureReceipt::default(),
            prepared, meter.native.with_original_leaf(|work| work.cancellation().clone()),
        )
    };
    let mut mutations = Vec::new();
    for (observation, path, current) in states {
        let plan = prepare_final_path(checkout,
    path,
    current,
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
        meter.source().release(states_bytes).map_err(capture_demand_error)?;
    }
    finish_prepared_paths(checkout,
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
    meter,
).await?;
    {
        meter.native.receipt().verify(meter.native.with_original_leaf(|work| work.budget))
            .map_err(CaptureError::from)?;
    }
    Ok(receipt)
}

type CapturePathState = (Option<HostStat>, NamespacePath, Option<FileRecord>);

async fn lookup_capture_states<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    (observations, paths): (Vec<Option<HostStat>>, Vec<NamespacePath>),
    page_size: usize, meter: &CaptureWorkScope,
) -> Result<Vec<CapturePathState>, CaptureError> {
    let input_bytes = observations.capacity() as u64 * std::mem::size_of::<Option<HostStat>>() as u64
        + paths.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    let mut states = capture_vec(paths.len(), meter)?;
    let mut paths = paths.into_iter();
    let mut observations = observations.into_iter();
    let cancellation = meter.native.cancellation().clone();
    while paths.len() != 0 {
        let count = paths.len().min(page_size);
        let page = &paths.as_slice()[..count];
        let lookup = capture_sdk(
            checkout.lookup_batch_no_follow_with_native_work(page, &meter.native, &cancellation),
            meter, &cancellation,
        ).await?;
        for ((path, observation), entry) in paths.by_ref().take(count)
            .zip(observations.by_ref()).zip(lookup.0.entries)
        {
            meter.source().check().map_err(capture_demand_error)?;
            states.push((observation, path, entry.record));
        }
        drop(lookup.1);
    }
    drop(paths);
    drop(observations);
    meter.source().release(input_bytes).map_err(capture_demand_error)?;
    sort_capture_states(&mut states, meter)?;
    Ok(states)
}

fn sort_capture_states(states: &mut [CapturePathState], meter: &CaptureWorkScope) -> Result<(), CaptureError> {
    capture_sort_by(states, |(left, left_path, left_current), (right, right_path, right_current)| {
        let rank = |observation: &Option<HostStat>, current: &Option<FileRecord>| {
            match observation {
                None => 0,
                Some(observation) if observation.is_dir() => 1,
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

/// Captures a complete eligible baseline under an exact path policy.
pub async fn capture_baseline<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    options: &CaptureOptions,
    policy: &CapturePolicy,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<CaptureReceipt>, OperationFailure<CaptureError>> {
    let native = capture_work(budget, cancellation)?;
    let original = native.original_demand_work();
    let result = capture_baseline_with_native_work(checkout, options, policy, cancellation, &native).await;
    capture_original_result(&original, result)
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
        .map_err(|error| OperationFailure::new(error.into(), completed))?;
    let (checked, completed) = {
        let held = meter.lock();
        (held.check(), held.work)
    };
    checked.map_err(|error| OperationFailure::new(capture_demand_error(error), completed))?;
    let scope = CaptureWorkScope::new(work)?;
    let meter = &scope;
    let result = (async {
        let maximum = usize::try_from(options.maximum_paths)
            .map_err(|_| CaptureError::InvalidOptions)?;
        let source_root = open_source_root(options, meter)?;
        let limits = checkout.volume_config().limits;
        let profile = checkout.volume_config().profile;
        let mut paths = CaptureMap::new();

        let mut observed = CaptureMap::new();
        let root = NamespacePath::new(Vec::new(), limits).map_err(CaptureError::Path)?;
        collect_host_walk_measured(
            &source_root, profile, limits, CaptureHostPath::new(), root, policy, meter,
            |path, stat, meter| insert_host_observation(&mut observed, path, stat, maximum, meter),
        )?;
        let root = NamespacePath::new(Vec::new(), limits).map_err(CaptureError::Path)?;
        collect_checkout_subtree_paths(
            checkout, limits, root, policy, maximum, &mut paths, cancellation, meter,
        ).await?;
        let ordered = order_host_checkout_union(observed, paths, maximum, meter)?;
        capture_observed_paths_from_root(checkout,
    ordered,
    options.maximum_extent_spans,
    &source_root,
    cancellation,
    None,
    meter,
).await
    }).await;
    capture_scope_result(scope, result)
}

fn order_host_checkout_union(
    mut observed: CaptureMap<NamespacePath, Option<HostStat>>,
    checkout_paths: CaptureMap<NamespacePath, ()>,
    maximum: usize,
    meter: &CaptureWorkScope,
) -> Result<(Vec<Option<HostStat>>, Vec<NamespacePath>), CaptureError> {
    let checkout_bytes = checkout_paths.storage_bytes();
    for (path, ()) in checkout_paths.entries {
        capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        match observed.position_measured(&path, meter)? {
            Ok(_) => capture_drop_path(path, meter)?,
            Err(index) => {
                if observed.len() >= maximum { return Err(CaptureError::InvalidOptions); }
                observed.insert_at(index, path, None, meter)?;
            }
        }
    }
    meter.source().release(checkout_bytes).map_err(capture_demand_error)?;
    let observed_bytes = observed.storage_bytes();
    let mut observations = capture_vec(observed.len(), meter)?;
    let mut paths = capture_vec(observed.len(), meter)?;
    for (path, observation) in observed.entries {
        meter.source().check().map_err(capture_demand_error)?;
        observations.push(observation);
        paths.push(path);
    }
    meter.source().release(observed_bytes).map_err(capture_demand_error)?;
    Ok((observations, paths))
}

fn combined_path_count_exceeds(observed: usize, checkout_only: usize, maximum: usize) -> bool {
    observed.checked_add(checkout_only).is_none_or(|combined| combined > maximum)
}

fn insert_host_observation(
    observed: &mut CaptureMap<NamespacePath, Option<HostStat>>,
    path: NamespacePath,
    metadata: HostStat,
    maximum: usize,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    if !observed.contains_key_measured(&path, meter)? && observed.len() >= maximum {
        return Err(CaptureError::InvalidOptions);
    }
    capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
    observed.insert_measured(path, Some(metadata), meter)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn collect_host_subtree_roots(
    source_root: &HostRoot,
    profile: FilesystemProfile,
    limits: crate::model::VolumeLimits,
    maximum: usize,
    roots: &[NamespacePath],
    policy: &CapturePolicy,
    observed: &mut CaptureMap<NamespacePath, Option<HostStat>>,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    for root in roots {
        let host_root = namespace_to_host_path_measured(root, meter)?;
        if let Some(observation) = observe_host_path(source_root, &host_root, meter)? {
            let recurse = observation.is_dir() && !observation.file_type().is_symlink();
            let path = clone_capture_path(root, meter)?;
            insert_host_observation(observed, path, observation, maximum, meter)?;
            if recurse {
                let path = clone_capture_path(root, meter)?;
                collect_host_walk_measured(
                    source_root, profile, limits, host_root, path, policy, meter,
                    |path, stat, meter| insert_host_observation(observed, path, stat, maximum, meter),
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
}

async fn walk_checkout_subtree<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>, root: NamespacePath, page_size: u32,
    meter: &CaptureWorkScope,
    mut visit: impl FnMut(&NamespacePath, &crate::DirectoryRecordEntry, &mut Vec<NamespacePath>)
        -> Result<(), CaptureError>,
) -> Result<(), CaptureError> {
    let mut pending = capture_vec(1, meter)?;
    pending.push(root);
    while let Some(directory) = pending.pop() {
        let mut after = None;
        loop {
            let cancellation = meter.native.cancellation().clone();
            let page = capture_sdk(
                checkout.list_directory_records_with_native_work(
                    &directory, after.as_ref(), page_size, &meter.native, &cancellation,
                ),
                meter, &cancellation,
            ).await?;
            capture_charge(meter, WorkCounters {
                items_examined: page.0.entries.len() as u64, ..WorkCounters::UNCHARGED
            })?;
            if page.0.has_more && page.0.entries.is_empty() {
                return Err(CaptureError::Invariant("directory pagination made no progress"));
            }
            for entry in &page.0.entries {
                visit(&directory, entry, &mut pending)?;
            }
            let next = if page.0.has_more {
                let last = page.0.entries.last().ok_or(CaptureError::Invariant(
                    "directory pagination made no progress",
                ))?;
                Some(clone_capture_name(&last.name, meter)?)
            } else { None };
            if let Some(previous) = after.take() {
                let bytes = previous.retained_bytes() as u64;
                drop(previous);
                meter.source().release(bytes).map_err(capture_demand_error)?;
            }
            let has_more = page.0.has_more;
            drop(page);
            after = next;
            if !has_more {
                break;
            }
        }
        capture_drop_path(directory, meter)?;
    }
    release_capture_paths(pending, meter)
}

#[allow(clippy::too_many_arguments)]
async fn collect_checkout_subtree_paths<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>, limits: crate::model::VolumeLimits,
    root: NamespacePath, policy: &CapturePolicy, maximum: usize,
    paths: &mut CaptureMap<NamespacePath, ()>, cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    cancellation.check()?;
    walk_checkout_subtree(checkout, root, 1024, meter, |directory, entry, pending| {
        capture_charge(meter, WorkCounters { source_entries_visited: 1, ..WorkCounters::UNCHARGED })?;
        let name = clone_capture_name(&entry.name, meter)?;
        let child = append_path_measured(directory, name, limits, meter)?;
        if capture_excludes(policy, &child, meter)? {
            return capture_drop_path(child, meter);
        }
        let scanned = clone_capture_path(&child, meter)?;
        append_scanned_path(paths, scanned, maximum, meter)?;
        if entry.record.kind == FileKind::Directory {
            capture_reserve(pending, 1, meter)?;
            pending.push(child);
        } else {
            capture_drop_path(child, meter)?;
        }
        Ok(())
    }).await
}

fn append_scanned_path(
    paths: &mut CaptureMap<NamespacePath, ()>,
    path: NamespacePath,
    maximum: usize,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    let index = match paths.position_measured(&path, meter)? {
        Ok(_) => return capture_drop_path(path, meter),
        Err(index) => index,
    };
    if paths.len() >= maximum { return Err(CaptureError::InvalidOptions); }
    capture_charge(meter, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
    paths.insert_at(index, path, (), meter)
}

fn capture_drop_path(path: NamespacePath, meter: &CaptureWorkScope) -> Result<(), CaptureError> {
    let bytes = path.retained_bytes();
    drop(path);
    meter.source().release(bytes).map_err(capture_demand_error)
}

fn capture_watch_owned_bytes(changes: &[WatchChange], incoming: &crate::watch::WatchBatchOriginal,
    storage_bytes: u64, meter: &CaptureWorkScope) -> Result<u64, CaptureError> {
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
            bytes = bytes.checked_add(path.retained_bytes()).ok_or(CaptureError::from(WorkError::Overflow))?;
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

/// Captures one watcher batch while omitting excluded paths symmetrically.
pub async fn capture_watch_batch<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    batch: WatchBatch,
    options: &CaptureOptions,
    policy: &CapturePolicy,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<WatchCaptureReceipt>, OperationFailure<CaptureError>> {
    let native = capture_work(budget, cancellation)?;
        let original = native.original_demand_work();
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
    // The facade opens fixed accounting leaves only; owned helpers and drops remain outside those short meter-meter loans.
    let cancellation = {
        cancellation.check()?;
        meter.source().check()
        .map_err(capture_demand_error)?;
        if let WatchBatch::Changes { changes, original: incoming, .. } = &batch {
            let storage_bytes = if incoming.has_native_changes_storage_custody() { 0 } else {
                (changes.capacity() as u64).checked_mul(std::mem::size_of::<WatchChange>() as u64)
                .ok_or(CaptureError::from(WorkError::Overflow))?
            };
            let bytes = capture_watch_owned_bytes(changes, incoming, storage_bytes, meter)
            ?;
            meter.native.with_original_leaf(|work| work.adopt(bytes))
            .map_err(capture_demand_error)?;
        }
        meter.native.with_original_leaf(|work| work.cancellation().clone())
    };
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
                epoch: epoch,
                reason,
        });
    };
    let changes_storage_native = incoming.has_native_changes_storage_custody();
    // Drop moved hints before their meter allocation guards, including on early failure.
    let incoming = (changes, incoming);
    if options.maximum_paths == 0
    || options.maximum_extent_spans == 0
    || incoming.0.len() > usize::try_from(options.maximum_paths).unwrap_or(usize::MAX)
    {
        return Err(CaptureError::InvalidOptions);
    }
    let source_root = open_source_root(options, meter)?;
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
                WatchChange::Created(path) => WatchChange::Created(clone_capture_path(path, meter)?),
                WatchChange::Modified(path) => WatchChange::Modified(clone_capture_path(path, meter)?),
                WatchChange::Arrived(path) => WatchChange::Arrived(clone_capture_path(path, meter)?),
                WatchChange::Removed(path) => WatchChange::Removed(clone_capture_path(path, meter)?),
                WatchChange::MetadataChanged(path) => WatchChange::MetadataChanged(clone_capture_path(path, meter)?),
                WatchChange::Renamed { from, to } => WatchChange::Renamed {
                    from: clone_capture_path(from, meter)?,
                    to: clone_capture_path(to, meter)?,
                },
            };
            drop(change);
            copied
        } else { change };
        { capture_charge(meter,
                WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }) }?;
        let change = match change {
            WatchChange::Renamed { from, to } => {
                let excluded_from = capture_excludes(policy, &from, meter)?;
                let excluded_to = capture_excludes(policy, &to, meter)?;
                match (excluded_from, excluded_to) {
                    (false, false) => WatchChange::Renamed { from, to },
                    (true, false) => {
                        capture_drop_path(from, meter)?;
                        WatchChange::Created(to)
                    }
                    (false, true) => {
                        capture_drop_path(to, meter)?;
                        WatchChange::Removed(from)
                    }
                    (true, true) => {
                        capture_drop_path(from, meter)?;
                        capture_drop_path(to, meter)?;
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
                if capture_excludes(policy, path, meter)? {
                    let bytes = path.retained_bytes();
                    drop(change);
                    meter.source().release(bytes).map_err(capture_demand_error)?;
                    continue;
                }
                change
            }
        };
        match change {
            WatchChange::Created(path) => {
                let current = current_record(&rename_records, &moved_away, &path, meter)?;
                ordinary.insert_measured(path, (current, CaptureIntent::Replace), meter)?;
            }
            WatchChange::Modified(path) | WatchChange::Removed(path) => {
                let current = current_record(&rename_records, &moved_away, &path, meter)?;
                ordinary.get_or_insert_measured(path, (current, CaptureIntent::Complete), meter).map(|_| ())?;
            }
            WatchChange::Arrived(path) => {
                let current = current_record(&rename_records, &moved_away, &path, meter)?;
                { let entry = ordinary.get_or_insert_measured(path, (current, CaptureIntent::Arrived), meter)?;
                    if entry.1 != CaptureIntent::Replace { entry.1 = CaptureIntent::Arrived; }
                    ()};
            }
            WatchChange::MetadataChanged(path) => {
                let current = current_record(&rename_records, &moved_away, &path, meter)?;
                ordinary.get_or_insert_measured(path, (current, CaptureIntent::MetadataOnly), meter).map(|_| ())?;
            }
            WatchChange::Renamed { from, to } => {
                cancellation.check()?;
                let prior_source = ordinary.remove_measured(&from, meter)?;
                let prior_source = if let Some((key, value)) = prior_source {
                    capture_drop_path(key, meter)?;
                    Some(value)
                } else { None };
                if let Some((key, _)) = ordinary.remove_measured(&to, meter)? {
                    capture_drop_path(key, meter)?;
                }
                let record = if let Some((key, record)) = rename_records.remove_measured(&from, meter)? {
                    capture_drop_path(key, meter)?;
                    Some(record)
                } else if matches!(prior_source, Some((CurrentRecord::Known(None), _)))
                || moved_away.contains_key_measured(&from, meter)?
                {
                    None
                } else {
                    let source = capture_sdk(
                        checkout.lookup_no_follow(&from, &meter.native, &cancellation),
                        meter, &cancellation,
                    ).await?;
                    source.record
                };
                if let Some(record) = record {
                    if record.kind == FileKind::Directory {
                        capture_reserve(&mut renamed_directories, 1, meter)?;
                        renamed_directories.push((clone_capture_path(&from, meter)?,
                                clone_capture_path(&to, meter)?));
                    }
                    capture_reserve(&mut rename_destinations, 1, meter)?;
                    rename_destinations.push(clone_capture_path(&to, meter)?);
                    capture_reserve(&mut mutations, 1, meter)?;
                    mutations.push(AuthoredMutation::Rename {
                            source: clone_capture_path(&from, meter)?,
                            destination: clone_capture_path(&to, meter)?,
                            replace: true,
                    });
                    let destination = clone_capture_path(&to, meter)?;
                    rename_records.insert_measured(destination, record, meter)?;
                } else {
                    let destination = clone_capture_path(&to, meter)?;
                    { ordinary.insert_measured(
                            destination, (CurrentRecord::Lookup, CaptureIntent::Replace), meter) }?;
                }
                moved_away.insert_measured(from, (), meter)?;
                if let Some((key, ())) = moved_away.remove_measured(&to, meter)? {
                    capture_drop_path(key, meter)?;
                }
                capture_drop_path(to, meter)?;
            }
        }
    }
    if !changes_storage_native {
        meter.source().release(changes_bytes).map_err(capture_demand_error)?;
    }
    drop(incoming.1);
    release_capture_set(moved_away, meter)?;

    expand_directory_hints(checkout,
        &mut ordinary,
        &source_root,
        options.maximum_paths,
        policy,
        &cancellation,
        meter,
    )
    .await?;

    // A watcher may report a nested file before any ancestor has been observed in this lazy checkout. Capture only the missing ancestors, rather than expanding their entire host subtrees as event roots.
    let maximum_paths = usize::try_from(options.maximum_paths).unwrap_or(usize::MAX);
    let ancestors = watch_ancestor_candidates(ordinary.entries.iter().map(|(path, _)| path)
        .chain(rename_records.entries.iter().map(|(path, _)| path))
        .chain(rename_destinations.iter()),
        &ordinary,
        &rename_records,
        policy,
        maximum_paths,
        checkout.volume_config().limits,
        meter,
    )?;
    release_capture_paths(rename_destinations, meter)?;
    let mut missing_ancestors = hydrate_watch_ancestors(checkout,
        &mut ordinary,
        ancestors,
        &source_root,
        epoch,
        maximum_paths.saturating_sub(rename_records.len()),
        &cancellation,
        meter,
    )
    .await?;

    let mut lookup_paths = capture_vec(ordinary.len(), meter)?;
    for (path, (current, _)) in &ordinary.entries {
        if matches!(current, CurrentRecord::Lookup) {
            lookup_paths.push(clone_capture_path(path, meter)?);
        }
    }
    if !lookup_paths.is_empty() {
        let lookup = capture_sdk(
            checkout.lookup_batch_no_follow_with_native_work(&lookup_paths, &meter.native, &cancellation),
            meter, &cancellation,
        ).await?;

        let lookup_paths_bytes = lookup_paths.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
        for (path, entry) in lookup_paths.into_iter().zip(lookup.0.entries) {
            let mut record = entry.record;
            if record.is_none()
            && let Some(prior_path) = { remap_renamed_directory_path(
                    &path, &renamed_directories, checkout.volume_config().limits, meter,
                ) }?
            {
                let prior = capture_sdk(
                    checkout.lookup_no_follow(&prior_path, &meter.native, &cancellation),
                    meter, &cancellation,
                ).await?;
                record = prior.record;
                capture_drop_path(prior_path, meter)?;
            }
            let Ok(index) = ordinary.position_measured(&path, meter)? else {
                return Err(CaptureError::Invariant("capture lookup path disappeared"));
            };
            let (current, _) = &mut ordinary.entries[index].1;
            *current = CurrentRecord::Known(record);
            capture_drop_path(path, meter)?;
        }
        drop(lookup.1);
        meter.source().release(lookup_paths_bytes).map_err(capture_demand_error)?;
    } else {
        release_capture_paths(lookup_paths, meter)?;
    }
    { let storage_bytes = renamed_directories.capacity() as u64 * std::mem::size_of::<(NamespacePath, NamespacePath)>() as u64;
        for (from, to) in renamed_directories {
            capture_drop_path(from, meter)?;
            capture_drop_path(to, meter)?;
        }
        meter.source().release(storage_bytes).map_err(capture_demand_error) }?;

    let mut host_links = CaptureMap::new();
    let mut prepared = { capture_vec(
            rename_records.len().checked_add(ordinary.len()).ok_or(CaptureError::from(WorkError::Overflow))?, meter) }?;
    // A rename can target a directory that has never been observed in this lazy checkout. Materialize only those host ancestors before the rename, preserving the event order for all remaining mutations.
    { capture_sort_by(&mut missing_ancestors, |left, right| {
                left.depth()
                .cmp(&right.depth())
                .then_with(|| left.cmp(right))
            }, meter) }?;
    let rename_mutations = std::mem::take(&mut mutations);
    let missing_bytes = missing_ancestors.capacity() as u64 * std::mem::size_of::<NamespacePath>() as u64;
    for path in missing_ancestors {
        let Some((key, (CurrentRecord::Known(current), intent))) = ordinary.remove_measured(&path, meter)? else {
            return Err(CaptureError::Invariant("capture ancestor path disappeared"));
        };
        capture_drop_path(key, meter)?;
        let plan = prepare_final_path(checkout,
            path,
            current,
            intent,
            &source_root,
            None,
            &mut host_links,
            Some(epoch),
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
    meter.source().release(missing_bytes).map_err(capture_demand_error)?;
    let rename_mutations_bytes = rename_mutations.capacity() as u64 * std::mem::size_of::<AuthoredMutation>() as u64;
    capture_reserve(&mut mutations, rename_mutations.len(), meter)?;
    { capture_charge(meter, WorkCounters {
                bytes_copied: rename_mutations.len() as u64 * std::mem::size_of::<AuthoredMutation>() as u64,
                ..WorkCounters::UNCHARGED
        }) }?;
    mutations.extend(rename_mutations);
    meter.source().release(rename_mutations_bytes).map_err(capture_demand_error)?;
    let namespace_boundary = mutations.len();
    let rename_records_bytes = rename_records.storage_bytes();
    for (path, record) in rename_records.entries {
        if ordinary.contains_key_measured(&path, meter)? {
            capture_drop_path(path, meter)?;
            continue;
        }
        let plan = prepare_final_path(checkout,
            path,
            Some(record),
            CaptureIntent::Complete,
            &source_root,
            None,
            &mut host_links,
            Some(epoch),
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
    meter.source().release(rename_records_bytes).map_err(capture_demand_error)?;
    let mut observed_ordinary = capture_vec(ordinary.len(), meter)?;
    let ordinary_bytes = ordinary.storage_bytes();
    for (path, (current, intent)) in ordinary.entries {
        let CurrentRecord::Known(current) = current else {
            return Err(CaptureError::Invariant("incomplete capture lookup"));
        };
        let host_path = namespace_to_host_path_measured(&path, meter)
        ?;
        let observation = observe_host_path(&source_root, &host_path, meter)
        ?;
        capture_drop_host_path(host_path, meter)
        ?;
        observed_ordinary.push((path, current, intent, observation));
    }
    meter.source().release(ordinary_bytes).map_err(capture_demand_error)?;
    let mut ordinary = observed_ordinary;
    { capture_sort_by(&mut ordinary, |left, right| match (left.3.is_some(), right.3.is_some()) {
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
    let observed_bytes = ordinary.capacity() as u64 * std::mem::size_of::<(NamespacePath, Option<FileRecord>, CaptureIntent, Option<HostStat>)>() as u64;
    for (path, current, intent, observation) in ordinary {
        let plan = prepare_final_path(checkout,
            path,
            current,
            intent,
            &source_root,
            observation,
            &mut host_links,
            Some(epoch),
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
    meter.source().release(observed_bytes).map_err(capture_demand_error)?;
    finish_prepared_paths(checkout,
        &source_root,
        prepared,
        options.maximum_extent_spans,
        &mut mutations,
        &mut receipt,
        &cancellation,
        meter,
    )
    .await?;
    release_host_links(host_links, meter)?;
    if namespace_boundary == 0 || namespace_boundary == mutations.len() {
        apply_capture_transaction(checkout,
            mutations,
            meter,
        )
        .await?;
    } else {
        // Compile binding changes before final-state mutations on one atomic candidate.
        let mut candidate = checkout.private_candidate(&meter.native, &cancellation)?;
        let final_state = { let count = mutations.len() - namespace_boundary;
            let bytes = count.checked_mul(std::mem::size_of::<AuthoredMutation>())
            .ok_or(CaptureError::from(WorkError::Overflow))? as u64;
            let copied = WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED };
            meter.source().admit(copied).map_err(capture_demand_error)?;
            let mut final_state = capture_vec(count, meter)?;
            final_state.extend(mutations.drain(namespace_boundary..));
            meter.source().record(copied).map_err(capture_demand_error)?;
            final_state};
        apply_capture_transaction(&mut candidate,
            mutations,
            meter,
        )
        .await?;
        apply_capture_transaction(&mut candidate,
            final_state,
            meter,
        )
        .await?;
        { cancellation.check()?;
            meter.source().check().map_err(capture_demand_error) }?;
        cancellation.commit_if_live(|| { *checkout = candidate; })?;
    }
    Ok(WatchCaptureReceipt {
            epoch,
            first_sequence,
            next_sequence,
            capture: receipt,
    })

}

#[cfg(test)]
mod shared_watch_capture_tests {
    use super::*;
    use crate::demand::NativeDemandReadMeter;
    use crate::storage::{
        ObjectId, ObjectRead, ObjectReadRequest, ObjectResult, ObjectWrite,
    };
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct ReenteringObjects {
        inner: crate::memory::MemoryObjectStore,
        meter: Arc<Mutex<Option<Arc<crate::demand::NativeDemandReadMeter>>>>,
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
    async fn put(&self, object_id: ObjectId, bytes: acyclic_stream::OwnedBytes, context: &mut crate::AuthorityWork<'_>) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() { return Err(crate::ObjectStoreError::NativeUnavailable); }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            self.reenter().await;
            ({ let __object_args = (object_id, bytes, budget, cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = AsyncObjectStore::put(&self.inner, __object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) })
         }.await;
        context.accept(result)
    }

    async fn put_many(&self, writes: &[ObjectWrite], context: &mut crate::AuthorityWork<'_>) -> Result<(), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() { return Err(crate::ObjectStoreError::NativeUnavailable); }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            self.reenter().await;
            ({ let __object_args = (writes, budget, cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2); let __object_result = AsyncObjectStore::put_many(&self.inner, __object_args.0, &mut __object_work).await; __object_work.receipt(__object_result) })
         }.await;
        context.accept(result)
    }

    async fn read(&self, object_id: ObjectId, maximum_bytes: u64, context: &mut crate::AuthorityWork<'_>) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() { return Err(crate::ObjectStoreError::NativeUnavailable); }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            self.reenter().await;
            ({ let __object_args = (object_id, maximum_bytes, budget, cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3); let __object_result = AsyncObjectStore::read(&self.inner, __object_args.0, __object_args.1, &mut __object_work).await; __object_work.receipt(__object_result) })
         }.await;
        context.accept(result)
    }

    async fn read_many(&self, requests: &[ObjectReadRequest], context: &mut crate::AuthorityWork<'_>) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() { return Err(crate::ObjectStoreError::NativeUnavailable); }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            self.reenter().await;
            ({ let __object_args = (requests, budget, cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2); let __object_result = AsyncObjectStore::read_many(&self.inner, __object_args.0, &mut __object_work).await.map(|(values, _allocation)| values); __object_work.receipt(__object_result) })
         }.await;
        context.accept(result).map(|values| (values, None))
    }

    async fn contains(&self, object_id: ObjectId, context: &mut crate::AuthorityWork<'_>) -> Result<bool, crate::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() { return Err(crate::ObjectStoreError::NativeUnavailable); }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            self.reenter().await;
            ({ let __object_args = (object_id, budget, cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2); let __object_result = AsyncObjectStore::contains(&self.inner, __object_args.0, &mut __object_work).await; __object_work.receipt(__object_result) })
         }.await;
        context.accept(result)
    }
    }

    #[tokio::test]
    async fn shared_watch_reenters_original_meter()
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
        let meter = Arc::clone(&original.state);
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
                epoch: 9,
                first_sequence: 3,
                next_sequence: 4,
                changes: vec![WatchChange::Created(path)],
            },
            &options, &CapturePolicy::default(), &cancellation, &native,
        ).await?;
        assert!(calls.load(Ordering::Relaxed) > 0);
        assert_eq!(captured.value.epoch, 9);
        assert_eq!(captured.value.next_sequence, 4);
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
    meter: &CaptureWorkScope,
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
        limits: crate::model::VolumeLimits,
        meter: &CaptureWorkScope,
) -> Result<Vec<(NamespacePath, ())>, CaptureError> {
    let mut ancestors = CaptureMap::new();
    for path in paths {
        for depth in (1..path.depth()).rev() {
            let parent = clone_capture_prefix(path, depth, limits, meter)?;
            if !ordinary.contains_key_measured(&parent, meter)?
                && !rename_records.contains_key_measured(&parent, meter)?
                && !capture_excludes(policy, &parent, meter)?
                && !ancestors.contains_key_measured(&parent, meter)? {
                if combined_path_count_exceeds(ancestors.len() + ordinary.len(), rename_records.len() + 1, maximum_paths) {
                    return Err(CaptureError::InvalidOptions);
                }
                ancestors.insert_measured(parent, (), meter)?;
            } else {
                capture_drop_path(parent, meter)?;
            }
        }
    }
    let mut ancestors = ancestors.entries;
    capture_sort_by(&mut ancestors, |left, right| {
        left.0.depth().cmp(&right.0.depth()).then_with(|| left.0.cmp(&right.0))
    }, meter)?;
    Ok(ancestors)
}

fn release_capture_set(paths: CaptureMap<NamespacePath, ()>, meter: &CaptureWorkScope) -> Result<(), CaptureError> {
    let bytes = paths.storage_bytes();
    for (path, ()) in paths.entries {
        capture_drop_path(path, meter)?;
    }
    meter.source().release(bytes).map_err(capture_demand_error)
}

fn release_host_links(links: CaptureMap<[u8; 16], NamespacePath>, meter: &CaptureWorkScope) -> Result<(), CaptureError> {
    let bytes = links.storage_bytes();
    for (_, source) in links.entries {
        capture_drop_path(source, meter)?;
    }
    meter.source().release(bytes).map_err(capture_demand_error)
}

fn capture_within_any(path: &NamespacePath, ancestors: &[NamespacePath], exclude_self: bool, meter: &CaptureWorkScope) -> Result<bool, CaptureError> {
    for ancestor in ancestors {
        capture_charge(meter, WorkCounters { items_examined: 2,
            source_path_components: path.depth().min(ancestor.depth()) as u64, ..WorkCounters::UNCHARGED })?;
        if (!exclude_self || path != ancestor) && path.is_within(ancestor) { return Ok(true); }
    }
    Ok(false)
}

async fn hydrate_watch_ancestors<A: AsyncAuthorityStore, O: AsyncObjectStore>(checkout: &mut Checkout<A, O>,
    ordinary: &mut CaptureMap<NamespacePath, (CurrentRecord, CaptureIntent)>,
    ancestors: Vec<(NamespacePath, ())>,
    source_root: &HostRoot,
    epoch: u64,
    maximum_paths: usize,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<Vec<NamespacePath>, CaptureError> {
    let mut missing = Vec::new();
    let mut replaced = Vec::new();
    let ancestors_bytes = ancestors.capacity() as u64 * std::mem::size_of::<(NamespacePath, ())>() as u64;
    for (path, ()) in ancestors {
        let below_replaced = capture_within_any(&path, &replaced, false, meter)?;
        let current = if below_replaced {
            None
        } else {
            let lookup = capture_sdk(
                checkout.lookup_no_follow(&path, &meter.native, cancellation),
                meter, cancellation,
            ).await?;
            lookup.record
        };

        let host_path = namespace_to_host_path_measured(&path, meter)
        ?;
        let observation = observe_host_path(source_root, &host_path, meter)
        ?;
        capture_drop_host_path(host_path, meter)
        ?;
        match observation {
            Some(observation) if observation.is_dir() => {
                if current.is_none_or(|record| record.kind != FileKind::Directory) {
                    if current.is_some() {
                        { capture_reserve(&mut replaced, 1, meter)?;
                            replaced.push(clone_capture_path(&path, meter)?);
                            ()};
                    }
                    { capture_reserve(&mut missing, 1, meter)?;
                        missing.push(clone_capture_path(&path, meter)?);
                        ()};
                    { ordinary.insert_measured(
                            path, (CurrentRecord::Known(current), CaptureIntent::Replace), meter) }?;
                } else {
                    capture_drop_path(path, meter)?;
                }
            }
            _ => {
                return Err(CaptureError::RescanRequired {
                        epoch: epoch,
                        reason: WatchInvalidationReason::NativeRescanRequired,
                });
            }
        }
    }
    meter.source().release(ancestors_bytes).map_err(capture_demand_error)?;
    if ordinary.len() > maximum_paths {
        return Err(CaptureError::InvalidOptions);
    }

    // Descendants of a replaced file or symlink were absent in the old view.
    for (path, (current, _)) in &mut ordinary.entries {
        if matches!(current, CurrentRecord::Lookup)
        && capture_within_any(path, &replaced, true, meter)?
        {
            *current = CurrentRecord::Known(None);
        }
    }
    release_capture_paths(replaced, meter)?;
    Ok(missing)

}

fn remap_renamed_directory_path(
    path: &NamespacePath,
    renames: &[(NamespacePath, NamespacePath)],
    limits: crate::model::VolumeLimits,
    meter: &CaptureWorkScope,
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

enum PreparedPath {
    Regular {
        path: NamespacePath,
        host_path: CaptureHostPath,
        snapshot: HostSnapshot,
        exists_with_kind: bool,
        canonical_metadata: FileMetadata,
    },
    HardLink {
        source: NamespacePath,
        destination: NamespacePath,
        current: Option<FileRecord>,
    },
}

#[allow(clippy::too_many_arguments)]
async fn expand_directory_hints<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    ordinary: &mut CaptureMap<NamespacePath, (CurrentRecord, CaptureIntent)>,
    source_root: &HostRoot,
    maximum_paths: u32,
    policy: &CapturePolicy,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    let maximum = usize::try_from(maximum_paths)
    .map_err(|_| CaptureError::InvalidOptions)?;
    if ordinary.len() > maximum {
        return Err(CaptureError::InvalidOptions);
    }
    let limits = checkout.volume_config().limits;
    let profile = checkout.volume_config().profile;
    let mut roots = capture_vec(ordinary.len(), meter)?;
    for (path, (_, intent)) in &ordinary.entries {
        // Expand newly arrived directories; ordinary directory hints already name changed children.
        if !cfg!(target_os = "macos")
        && !matches!(intent, CaptureIntent::Replace | CaptureIntent::Arrived)
        {
            continue;
        }

        let host_path = namespace_to_host_path_measured(path, meter)
        ?;
        let observation = observe_host_path(source_root, &host_path, meter)
        ?;
        if observation.is_some_and(|observation| observation.is_dir()) {
            { roots.push((host_path, clone_capture_path(path, meter)?));
                ()};
        } else {
            capture_drop_host_path(host_path, meter)?;
        }
    }
    let mut paths = CaptureMap::new();
    for (path, _) in &ordinary.entries {
        { let path = clone_capture_path(path, meter)?;
            paths.insert_measured(path, (), meter).map(|_| ()) }?;
    }
    let roots_bytes = roots.capacity() as u64 * std::mem::size_of::<(CaptureHostPath, NamespacePath)>() as u64;
    for (host_path, volume_path) in roots {
        let host_volume_path = clone_capture_path(&volume_path, meter)?;
        collect_host_walk_measured(
            source_root, profile, limits, host_path, host_volume_path, policy, meter,
            |path, _, meter| append_scanned_path(&mut paths, path, maximum, meter),
        )?;
        let lookup = capture_sdk(
            checkout.lookup_no_follow(&volume_path, &meter.native, cancellation),
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
            capture_drop_path(volume_path, meter)?;
        }
    }
    meter.source().release(roots_bytes).map_err(capture_demand_error)?;
    let paths_bytes = paths.storage_bytes();
    for (path, ()) in paths.entries {
        { ordinary.get_or_insert_measured(
                path, (CurrentRecord::Lookup, CaptureIntent::Complete), meter).map(|_| ()) }?;
    }
    meter.source().release(paths_bytes).map_err(capture_demand_error)?;
    Ok(())

}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
async fn prepare_final_path<A: AsyncAuthorityStore, O: AsyncObjectStore>(checkout: &mut Checkout<A, O>,
    path: NamespacePath,
    current: Option<FileRecord>,
    intent: CaptureIntent,
    source_root: &HostRoot,
    observation: Option<HostStat>,
    host_links: &mut CaptureMap<[u8; 16], NamespacePath>,
    watch_epoch: Option<u64>,
    baseline: Option<&NativeViewBaseline>,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<Option<PreparedPath>, CaptureError> {
    cancellation.check()?;
    let host_path = namespace_to_host_path_measured(&path, meter)?;
    let observation = match observation {
        Some(observation) => Some(observation),
        None => {
            let observed = observe_host_path(
                source_root, &host_path, meter,
            );
            observed?
        }
    };
    match observation {
        Some(observation) => {
            let metadata = observation;
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
                let source = host_links.get_measured(&identity, meter)?;
                if let Some(source) = source {
                    ensure_current_host_node(source_root,
    &host_path,
    &snapshot,
    cancellation,
    meter,
)?;
                    receipt.changed_paths = checked_increment(receipt.changed_paths,
)?;
                    receipt.examined_paths =
                        checked_increment(receipt.examined_paths,
)?;
                    capture_drop_host_path(host_path, meter)?;
                    return Ok(Some(PreparedPath::HardLink {
                        source: clone_capture_path(source, meter)?,
                        destination: path,
                        current,
                    }));
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
    meter,
)
                    .await?;
                }
                capture_reserve(mutations, 1, meter)?;
                mutations.push(AuthoredMutation::Remove {
                    path: clone_capture_path(&path, meter)?,
                    expected_file_id: Some(record.file_id),
                });
            }
            let exists_with_kind =
                !replace && current.is_some_and(|record| record.kind == host_kind);
            let mut canonical_metadata = if host_kind == FileKind::SymbolicLink && cfg!(unix) {
                FileMetadata::default()
            } else {
                snapshot.metadata
            };
            let mut prior_metadata = None;
            if let Some(record) = current.filter(|record| {
                exists_with_kind
                    && record.kind == host_kind
                    && (host_kind != FileKind::SymbolicLink || cfg!(windows))
            }) {
                let lookup_cancellation = meter.native.with_original_leaf(|work| work.cancellation().clone());
                let prior = capture_sdk(
                    checkout.read_metadata_by_id(record.file_id, &meter.native, &lookup_cancellation),
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
                { capture_drop_path(path, meter)?;
                    capture_drop_host_path(host_path, meter) }?;
                return Ok(None);
            }
            let linked_path = if linked_regular {
                Some(clone_capture_path(&path, meter)?)
            } else { None };
            let prepared = if intent == CaptureIntent::MetadataOnly && exists_with_kind {
                ensure_current_host_node(source_root,
    &host_path,
    &snapshot,
    cancellation,
    meter,
)?;
                capture_reserve(mutations, 1, meter)?;
                mutations.push(AuthoredMutation::SetMetadata {
                    path,
                    metadata: canonical_metadata,
                });
                capture_drop_host_path(host_path, meter)?;
                None
            } else if host_kind == FileKind::Regular {
                // Staging opens the file once and proves it is still the observed node before reading a byte.
                Some(PreparedPath::Regular {
                    path,
                    host_path,
                    snapshot,
                    exists_with_kind,
                    canonical_metadata,
                })
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
    meter,
)?;
                capture_drop_host_path(host_path, meter)?;
                None
            };
            receipt.changed_paths = checked_increment(receipt.changed_paths,
)?;
            if let Some(linked_path) = linked_path {
                host_links.insert_measured(snapshot.identity.to_bytes(), linked_path, meter)?;
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
    meter,
)
                    .await?;
                }
                capture_reserve(mutations, 1, meter)?;
                mutations.push(AuthoredMutation::Remove {
                    path,
                    expected_file_id: Some(record.file_id),
                });
                receipt.changed_paths = checked_increment(receipt.changed_paths,
)?;
            } else {
                capture_drop_path(path, meter)?;
            }
        }
    }
    capture_drop_host_path(host_path, meter)?;
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
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    const PAGE: u32 = 256;
    let limits = checkout.volume_config().limits;
    // Descendants that carried their own hint are already queued (absent paths are captured deepest first); a second remove of the same path would find its source missing.
    let mut queued = CaptureMap::new();
    for mutation in mutations.iter() {
        { capture_charge(meter,
            WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }) }?;
        if let AuthoredMutation::Remove { path, .. } = mutation
            && !queued.contains_key_measured(path, meter)? {
            let path = clone_capture_path(path, meter)?;
            queued.insert_measured(path, (), meter)?;
        }
    }
    let mut found: Vec<(NamespacePath, FileRecord)> = Vec::new();
    let root = clone_capture_path(root, meter)?;
    walk_checkout_subtree(checkout, root, PAGE, meter, |directory, entry, pending| {
        let name = clone_capture_name(&entry.name, meter)?;
        let child = append_path_measured(directory, name, limits, meter)?;
        if entry.record.kind == FileKind::Directory {
            capture_reserve(pending, 1, meter)?;
            pending.push(clone_capture_path(&child, meter)?);
        }
        if !queued.contains_key_measured(&child, meter)? {
            capture_reserve(&mut found, 1, meter)?;
            found.push((child, entry.record));
        } else {
            capture_drop_path(child, meter)?;
        }
        Ok(())
    }).await?;
    release_capture_set(queued, meter)?;
    { capture_sort_by(&mut found,
        |left, right| right.0.depth().cmp(&left.0.depth()), meter) }?;
    capture_reserve(mutations, found.len(), meter)?;
    let found_bytes = found.capacity() as u64 * std::mem::size_of::<(NamespacePath, FileRecord)>() as u64;
    for (path, record) in found {
        mutations.push(AuthoredMutation::Remove {
            path,
            expected_file_id: Some(record.file_id),
        });
        receipt.changed_paths = checked_increment(receipt.changed_paths,
)?;
    }
    meter.source().release(found_bytes).map_err(capture_demand_error)?;
    Ok(())
}

fn capture_mutation_owned_bytes(
    mutations: &[AuthoredMutation], storage_bytes: u64, meter: &CaptureWorkScope,
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
            .ok_or(CaptureError::from(WorkError::Overflow))?;
    }
    Ok(bytes)
}

async fn apply_capture_transaction<A: AsyncAuthorityStore, O: AsyncObjectStore>(checkout: &mut Checkout<A, O>,
    mutations: Vec<AuthoredMutation>,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    let mutation_bytes = { capture_mutation_owned_bytes(
        &mutations, mutations.capacity() as u64 * std::mem::size_of::<AuthoredMutation>() as u64, meter) }?;
    if mutations.is_empty() {
        drop(mutations);
        meter.source().release(mutation_bytes).map_err(capture_demand_error)?;
        return Ok(());
    }
    let input = (mutations, meter.native.transfer_capture_allocation(
        mutation_bytes, meter.custody.as_ref().expect("capture scope is live"),
    ).map_err(CaptureError::from)?);
    let apply_cancellation = meter.native.with_original_leaf(|work| work.cancellation().clone());
    capture_sdk(
        async {
            // Preflight abandonment drops mutation data before its guard.
            let input = input;
            let mut accounting = crate::kernel::allocation::KernelAccounting::native(&meter.native, &apply_cancellation);
            checkout.apply_authored_bulk_transaction_with_accounting(
                crate::kernel::Owned { value: input.0, bytes: input.1.bytes(), allocation: crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(input.1) },
                &mut accounting, &apply_cancellation,
            ).await
        },
        meter, &apply_cancellation,
    ).await?;
    Ok(())
}

fn checked_increment(value: u64,
) -> Result<u64, CaptureError> {
    value
        .checked_add(1)
        .ok_or(CaptureError::from(WorkError::Overflow))
}

fn append_hard_link(source: NamespacePath,
        destination: NamespacePath,
        current: Option<FileRecord>,
        mutations: &mut Vec<AuthoredMutation>,
        meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    { capture_reserve(mutations, 1 + usize::from(current.is_some()), meter)?;
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
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
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
    let observed = capture_stat(source_root, host_path, meter).map_err(capture_demand_error)?;
    ensure_same_host_node(&snapshot, &observed)?;
    meter.source().check().map_err(capture_demand_error)
}

#[allow(clippy::too_many_arguments)]
async fn finish_prepared_paths<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    stager: &Checkout<A, O>,
    source_root: &HostRoot,
    prepared: Vec<PreparedPath>,
    maximum_extent_spans: u32,
    mutations: &mut Vec<AuthoredMutation>,
    receipt: &mut CaptureReceipt,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    let prepared_bytes = prepared.capacity() as u64 * std::mem::size_of::<PreparedPath>() as u64;
    // A pending alias is planned only after its source, so staging preserves their dependency order.
    for path in prepared {
        match path {
            PreparedPath::Regular { path, host_path, snapshot, exists_with_kind, canonical_metadata } => {
                let staged = stage_regular_body(stager, source_root, &host_path,
                    &snapshot, maximum_extent_spans, cancellation, meter,
                    &mut receipt.staged_file_bytes).await?;
                capture_drop_host_path(host_path, meter)?;
                append_staged_regular_state(path, snapshot.length,
                    exists_with_kind, canonical_metadata, staged, mutations, meter)?;
            }
            PreparedPath::HardLink { source, destination, current } => {
                append_hard_link(source, destination, current, mutations, meter)?;
            }
        }
    }
    meter.source().release(prepared_bytes).map_err(capture_demand_error)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn stage_regular_body<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    stager: &Checkout<A, O>,
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    snapshot: &HostSnapshot,
    maximum_extent_spans: u32,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
    staged_file_bytes: &mut u64,
) -> Result<Vec<(HostDataRange, StagedContent)>, CaptureError> {
    let opened = (|| {
        cancellation.check()?;
        #[cfg(unix)]
        let file = source_root
            .open_file_positional(host_path, &mut meter.source())
            .map_err(capture_demand_error)?;
        #[cfg(windows)]
        let file = source_root
            .open_file_positional_wide(host_path, &mut meter.source())
            .map_err(capture_demand_error)?;
        let file = file.into_std();
        let metadata = source_root
            .stat_file(&file, &mut meter.source())
            .map_err(capture_demand_error)?;
        if !metadata.file_type().is_file() {
            return Err(CaptureError::Kernel(crate::FsError::SourceSnapshotStale));
        }
        ensure_same_host_node(snapshot, &metadata)?;
        let logical_bytes = metadata.len();
        let ranges = allocated_data_ranges(
            &file,
            0,
            logical_bytes,
            maximum_extent_spans,
            &mut meter.source(),
        )
        .map_err(capture_demand_error)?;
        let positional = native_positional_file(file, &meter.native)?;
        capture_allocate(meter, std::mem::size_of::<acyclic_native_runtime::NativeFile>() as u64)?;
        let native = std::sync::Arc::new(positional);
        Ok((metadata, ranges, native))
    })();

    let (metadata, ranges, native) = opened
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
    meter,
    staged_file_bytes,
).await;
    let content = staged?;
    let verified = (|| {
        cancellation.check()?;
        let after = source_root
            .stat_file(native.as_file(), &mut meter.source())
            .map_err(capture_demand_error)?;
        ensure_same_host_node(snapshot, &after)?;
        let current = capture_stat(source_root, host_path, meter)
            .map_err(capture_demand_error)?;
        ensure_same_host_node(snapshot, &current)?;
        cancellation.check()?;
        meter.source().check().map_err(capture_demand_error)
    })();
    let ranges_bytes = ranges.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64;
    let native_bytes = std::mem::size_of::<acyclic_native_runtime::NativeFile>() as u64;
    drop(ranges);
    drop(native);
    { meter.source().release(ranges_bytes).map_err(capture_demand_error)?;
        meter.source().release(native_bytes).map_err(capture_demand_error) }?;
    if let Err(error) = verified {
        let bytes = content.capacity() as u64 * std::mem::size_of::<(HostDataRange, StagedContent)>() as u64;
        drop(content);
        meter.source().release(bytes).map_err(capture_demand_error)?;
        return Err(error);
    }

    Ok(content)
}

fn append_staged_regular_state(path: NamespacePath,
        logical_bytes: u64,
        exists_with_kind: bool,
        canonical_metadata: FileMetadata,
        ranges: Vec<(HostDataRange, StagedContent)>,
        mutations: &mut Vec<AuthoredMutation>,
        meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    { let ranges_bytes = ranges.capacity() as u64 * std::mem::size_of::<(HostDataRange, StagedContent)>() as u64;
        let count = ranges.len().checked_add(2 + usize::from(logical_bytes != 0))
            .ok_or(CaptureError::from(WorkError::Overflow))?;
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
        meter.source().release(ranges_bytes).map_err(capture_demand_error)?;
        mutations.push(AuthoredMutation::SetMetadata { path, metadata: canonical_metadata });
        Ok(()) }
}

#[derive(Clone, Copy)]
struct HostSnapshot {
    identity: NativeRootIdentity,
    length: u64,
    metadata: FileMetadata,
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
        .ok_or_else(|| CaptureError::from(std::io::Error::other("host link count is unavailable")))
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
        return Err(CaptureError::Kernel(crate::FsError::SourceSnapshotStale));
    }
    Ok(())
}

fn ensure_current_host_node(
    source_root: &HostRoot,
    host_path: &CaptureHostPath,
    expected: &HostSnapshot,
    cancellation: &CancellationToken,
    meter: &CaptureWorkScope,
) -> Result<(), CaptureError> {
    cancellation.check()?;
    meter.source().check().map_err(capture_demand_error)?;
    let observed = capture_stat(source_root, host_path, meter).map_err(capture_demand_error)?;
    ensure_same_host_node(expected, &observed)?;
    cancellation.check()?;
    meter.source().check().map_err(capture_demand_error)
}

#[cfg(test)]
fn test_host_stat(root: &HostRoot, path: &Path) -> Result<HostStat, CaptureError> {
    let mut meter =
        crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
    root.stat(path, &mut meter).map_err(capture_demand_error)
}

#[cfg(test)]
fn test_held_stat(root: &HostRoot, file: &cap_std::fs::File) -> Result<HostStat, CaptureError> {
    let mut meter =
        crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
    root.stat_file(&file.try_clone()?.into_std(), &mut meter)
        .map_err(capture_demand_error)
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
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        std::fs::rename(&current, temporary.path().join("displaced"))?;
        std::fs::rename(&replacement, &current)?;
        let opened = root.open_file(Path::new("current"), &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?;
        assert!(matches!(
            ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?),
            Err(CaptureError::Kernel(crate::FsError::SourceSnapshotStale))
        ));
        Ok(())
    }

    #[test]
    fn rejects_a_changed_file_after_open() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let current = temporary.path().join("current");
        std::fs::write(&current, b"first")?;
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        let opened = root
            .open_file(
                Path::new("current"),
                &mut crate::native_host::ordinary_work(),
            )
            .map_err(crate::native_host::ordinary_error)?;
        ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?)?;
        std::fs::write(&current, b"changed body")?;
        assert!(matches!(
            ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?),
            Err(CaptureError::Kernel(crate::FsError::SourceSnapshotStale))
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
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        let opened = root
            .open_file(
                Path::new("current"),
                &mut crate::native_host::ordinary_work(),
            )
            .map_err(crate::native_host::ordinary_error)?;
        ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?)?;
        std::fs::set_permissions(&current, std::fs::Permissions::from_mode(0o600))?;
        assert!(matches!(
            ensure_same_host_node(&expected, &test_held_stat(&root, &opened)?),
            Err(CaptureError::Kernel(crate::FsError::SourceSnapshotStale))
        ));
        Ok(())
    }

    #[test]
    fn creation_time_changes_require_two_observable_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("current"), b"first")?;
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let metadata = test_host_stat(&root, Path::new("current"))?;
        let mut expected = HostSnapshot::from_metadata(&metadata)?;
        expected.metadata.created_ns = MetadataField::Value(123);
        let mut observed = expected;
        observed.metadata.created_ns = MetadataField::Unavailable;
        ensure_same_host_snapshot(&expected, &observed)?;
        ensure_same_host_snapshot(&observed, &expected)?;
        observed.metadata.created_ns = MetadataField::Value(124);
        assert!(matches!(
            ensure_same_host_snapshot(&expected, &observed),
            Err(CaptureError::Kernel(crate::FsError::SourceSnapshotStale))
        ));
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
        let root = HostRoot::open(temporary.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let expected = HostSnapshot::from_metadata(&test_host_stat(&root, Path::new("current"))?)?;
        std::fs::rename(&current, temporary.path().join("displaced"))?;
        std::fs::rename(&replacement, &current)?;
        let native = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
        let scope = CaptureWorkScope::new(&native)?;
        assert!(matches!(
            ensure_current_host_node(&root,
    &test_host_path(Path::new("current")),
    &expected,
    &CancellationToken::new(),
    &scope,
),
            Err(CaptureError::Kernel(crate::FsError::SourceSnapshotStale))
        ));
        Ok(())
    }
}

async fn stage_host_ranges<A: AsyncAuthorityStore, O: AsyncObjectStore>(stager: &Checkout<A, O>,
    native: &std::sync::Arc<acyclic_native_runtime::NativeFile>,
    ranges: &[HostDataRange],
    meter: &CaptureWorkScope,
    staged_file_bytes: &mut u64,
) -> Result<Vec<(HostDataRange, StagedContent)>, CaptureError> {
    let mut staged = capture_vec(ranges.len(), meter)?;
    for range in ranges {
        let mut bounded = NativeRangeSource {
            reader: acyclic_native_runtime::AsyncRangeReader::new(
                std::sync::Arc::clone(native), range.offset, range.length,
            ),
            capture_custody: std::sync::Arc::clone(meter.custody.as_ref().expect("capture scope is live")),
        };
        let stage_cancellation = meter.native.cancellation().clone();
        let content = capture_sdk(
            stager.stage(&mut bounded, range.length, &meter.native, &stage_cancellation),
            meter, &stage_cancellation,
        ).await;
        drop(bounded);

        let content = content?;
        if content.logical_bytes() != range.length { return Err(CaptureError::Kernel(crate::FsError::SourceSnapshotStale)); }
        *staged_file_bytes = staged_file_bytes.checked_add(range.length)
            .ok_or(CaptureError::from(WorkError::Overflow))?;
        staged.push((*range, content));
    }
    Ok(staged)
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
            CaptureError::from(WorkError::Overflow)
        })?;
        let mut position = range.offset;
        let mut report_stands = false;
        while position < end {
            let wanted = (CHUNK - position % CHUNK).min(end - position);
            let read = (async {
                cancellation.check()?;
                meter.native.admit(WorkCounters { source_bytes_read: wanted,
                    ..WorkCounters::UNCHARGED })
                    .map_err(CaptureError::from)?;
                let runtime = capture_runtime_meter_owner(&meter.native,
                    meter.custody.as_ref().expect("capture scope is live"))
                    .map_err(CaptureError::from)?;
                let value = reader.read(wanted as usize, runtime, true)
                    .await.map_err(|error| {
                        let cause = if let Some(cause) = meter.native.native_read_meter().take_native_io_failure() {
                            capture_demand_error(cause)
                        } else if error.kind() == std::io::ErrorKind::UnexpectedEof {
                            CaptureError::Kernel(crate::FsError::SourceSnapshotStale)
                        } else {
                            CaptureError::from(error)
                        };
                        cause
                    })?;
                Ok::<_, CaptureError>(value)
            }).await?;

            let chunk = read;
            let read_end = position + chunk.bytes.len() as u64;
            report_stands = { let scanned: Result<bool, CaptureError> = (|| {
                    let mut start = position;
                    while start < read_end {
                        meter.source().check().map_err(capture_demand_error)?;
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
            { let bytes = settled.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64;
                drop(settled);
                meter.source().release(bytes).map_err(capture_demand_error) }?;
            return Ok(reported);
        }
    }
    { let bytes = reported.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64;
        drop(reported);
        meter.source().release(bytes).map_err(capture_demand_error) }?;
    Ok(settled)
}

#[cfg(windows)]
fn keep_sparse_range(
    ranges: &mut Vec<HostDataRange>,
    maximum: usize,
    offset: u64,
    length: u64,
    meter: &CaptureWorkScope,
) -> Result<bool, CaptureError> {
    meter.source().check().map_err(capture_demand_error)?;
    if let Some(last) = ranges.last_mut() {
        if last.offset.checked_add(last.length) == Some(offset) {
            last.length = last.length.checked_add(length).ok_or(CaptureError::from(WorkError::Overflow))?;
            return Ok(true);
        }
    }
    if ranges.len() == maximum { return Ok(false); }
    if ranges.len() == ranges.capacity() {
        let size = std::mem::size_of::<HostDataRange>();
        let preferred = ranges.capacity().saturating_mul(2).max(1).min(maximum);
        let available = meter
            .native
            .with_original_leaf(|work| work.remaining())
            .map_err(capture_demand_error)?
            .peak_allocation_bytes
            / size as u64;
        let next = preferred
            .min(usize::try_from(available).unwrap_or(usize::MAX))
            .max(ranges.len() + 1);
        let copied = ranges.len() * size;
        let delta = WorkCounters {
            bytes_copied: copied as u64,
            ..WorkCounters::UNCHARGED
        };
        meter.source().admit(delta).map_err(capture_demand_error)?;
        let mut replacement = capture_vec(next, meter)?;
        replacement.extend_from_slice(ranges);
        meter.source().record(delta).map_err(capture_demand_error)?;
        let old_bytes = ranges.capacity() * size;
        let old = std::mem::replace(ranges, replacement);
        drop(old);
        meter.source().release(old_bytes as u64).map_err(capture_demand_error)?;
    }
    ranges.push(HostDataRange { offset, length });
    Ok(true)
}

struct NativeRangeSource {
    reader: acyclic_native_runtime::AsyncRangeReader,
    capture_custody: std::sync::Arc<crate::demand::CaptureTransientCustody>,
}

#[allow(unsafe_code, reason = "the measured host opener transfers its fresh positional handle")]
fn native_positional_file(
    file: std::fs::File,
    original: &crate::NativeMaterializationWork,
) -> Result<acyclic_native_runtime::NativeFile, CaptureError> {
    let meter = original.runtime_meter();
    #[cfg(windows)]
    let result = unsafe {
        // SAFETY: the fresh positional handle is overlapped and has no prior completion port.
        acyclic_native_runtime::NativeFile::from_overlapped_file_unchecked(file, Some(acyclic_native_runtime::NativeFileMeter::Owned(meter)))
    };
    #[cfg(not(windows))]
    let result = acyclic_native_runtime::NativeFile::from_file(file, Some(acyclic_native_runtime::NativeFileMeter::Owned(meter)));
    result.map_err(|(error, file)| {
        // This is an ordinary std file, not an explicit SDK close operation. Retire its physical handle before projecting the original failure.
        drop(file);
        capture_demand_error(original.native_read_meter().take_native_io_failure()
            .unwrap_or_else(|| native_demand_error(error)))
    })
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
        let mut copy = original.reserve_work(WorkCounters { bytes_copied: maximum as u64, ..WorkCounters::UNCHARGED })?;
        let runtime = capture_runtime_meter_owner(original, &self.capture_custody)?;
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
        copy.record_completed(WorkCounters { bytes_copied: maximum as u64, ..WorkCounters::UNCHARGED })?;
        // The packet retains its buffer until the copy and late checks finish.
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
            allocation_operations: 128, peak_allocation_bytes: 1024 * 1024,
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
        let original = crate::NativeMaterializationWork::new(budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        let mut scope = CaptureWorkScope::new(&original)?;
        cancellation.cancel();
        assert!(matches!(native_positional_file(file, &scope.native), Err(CaptureError::Kernel(crate::FsError::Cancelled(_)))));
        assert_eq!(unsafe { libc::flock(contender.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) }, 0);
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
        let original = crate::NativeMaterializationWork::new(budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        let mut scope = CaptureWorkScope::new(&original)?;
        let native = std::sync::Arc::new(native_positional_file(fixture_file(&path)?, &scope.native)?);
        let reader = acyclic_native_runtime::AsyncRangeReader::new(std::sync::Arc::clone(&native), 0, 8);
        let mut source = NativeRangeSource {
            reader, capture_custody: std::sync::Arc::clone(scope.custody.as_ref().expect("capture scope is live")),
        };
        std::fs::write(&path, b"abc")?;
        let mut destination = [0xa5; 8];
        assert!(matches!(source.read_with_native_work(
            &mut destination, &original, &cancellation,
        ).await, Err(crate::FsError::SourceSnapshotStale)));
        assert_eq!(destination, [0xa5; 8]);
        drop(source);
        scope.release()?;
        drop(native);
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
        let original = crate::NativeMaterializationWork::new(budget(), &cancellation)
            .map_err(|(error, _)| error)?;
        let mut scope = CaptureWorkScope::new(&original)?;
        let native = std::sync::Arc::new(native_positional_file(fixture_file(&path)?, &scope.native)?);
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
        // A still-live shared file tail has no terminal capture anchor.
        assert!(native.as_file().metadata()?.len() == 3);
        drop(native);
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
            let mut limit = budget();
            if counter == "source_bytes_read" { limit.source_bytes_read = 2; }
            else { limit.bytes_copied = 2; }
            let original = crate::NativeMaterializationWork::new(limit, &cancellation)
                .expect("fund original accounting state");
            let scope = CaptureWorkScope::new(&original)?;
            let native = std::sync::Arc::new(native_positional_file(fixture_file(&path)?, &scope.native)?);
            let mut source = NativeRangeSource {
                reader: acyclic_native_runtime::AsyncRangeReader::new(native, 0, 3),
                capture_custody: std::sync::Arc::clone(scope.custody.as_ref().expect("capture scope is live")),
            };
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

fn open_source_root(
    options: &CaptureOptions,
    meter: &CaptureWorkScope,
) -> Result<HostRoot, CaptureError> {
    let root =
        HostRoot::open(&options.source_root, &mut meter.source()).map_err(capture_demand_error)?;
    if root.identity() != options.expected_root_identity {
        return Err(CaptureError::RootChanged);
    }
    Ok(root)
}

/// Converts one exact namespace path to a host-relative path without lossy text projection.
pub fn namespace_to_host_path(path: &NamespacePath) -> Result<PathBuf, CaptureError> {
    let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
    namespace_to_host_path_with_work(path, &mut work)
        .map(|(path, _allocation)| path)
        .map_err(|error| match error {
            DemandError::InvalidRequest => CaptureError::UnrepresentablePath,
            error => capture_demand_error(error),
        })
}

#[cfg(all(test, windows))]
mod windows_name_tests {
    use super::*;

    #[test]
    fn posix_profile_names_round_trip_through_utf8_on_windows()
    -> Result<(), Box<dyn std::error::Error>> {
        let host_path = |name| {
            let path = NamespacePath::new(vec![name], crate::model::VolumeLimits::default())
                .map_err(CaptureError::Path)?;
            namespace_to_host_path(&path)
        };
        let valid = LogicalName::new(
            NameEncoding::PosixBytes,
            "uni-Ã©ä¸­.txt".as_bytes().to_vec(),
            255,
        )?;
        assert_eq!(
            host_path(valid)?,
            std::ffi::OsString::from("uni-Ã©ä¸­.txt")
        );

        let invalid = LogicalName::new(NameEncoding::PosixBytes, vec![0xff], 255)?;
        assert!(matches!(
            host_path(invalid),
            Err(CaptureError::UnrepresentablePath)
        ));
        let alias = LogicalName::new(NameEncoding::PosixBytes, b"a\\b".to_vec(), 255)?;
        assert!(matches!(
            host_path(alias),
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
        let host = std::path::Path::new("src/Ã¼nÃ¯cÃ¸de/æ—¥æœ¬èªž.rs");
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
        let root = HostRoot::open(directory, &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let file = root
            .open_file_positional(Path::new(name), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let metadata = test_host_stat(&root, Path::new(name)).map_err(std::io::Error::other)?;
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| std::io::Error::other(error))?;
        let native = std::sync::Arc::new(native_positional_file(file.into_std(), &original).map_err(std::io::Error::other)?);
        let mut scope = CaptureWorkScope::new(&original)
            .map_err(|failure| std::io::Error::other(failure.error))?;
        scope.native.with_original_leaf(|work| work.adopt(reported.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64))
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
        scope.release().map_err(std::io::Error::other)?;
        Ok(converted)
    }

    #[tokio::test]
    async fn written_zeros_in_a_sparse_file_capture_as_holes() -> std::io::Result<()> {
        use std::io::{Seek as _, SeekFrom, Write as _};
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
        acyclic_native_runtime::device_control_in_place(&file, FSCTL_SET_SPARSE, &[], &mut [], None)?;
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
        let root = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work())
            .map_err(crate::native_host::ordinary_error)?;
        let file = root
            .open_file_positional(
                Path::new("sparse.bin"),
                &mut crate::native_host::ordinary_work(),
            )
            .map_err(crate::native_host::ordinary_error)?;
        let metadata =
            test_host_stat(&root, Path::new("sparse.bin")).map_err(std::io::Error::other)?;
        let file_work =
            crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .map_err(|(error, _)| std::io::Error::other(error))?;
        let native = std::sync::Arc::new(
            native_positional_file(file.into_std(), &file_work).map_err(std::io::Error::other)?,
        );
        let scan = |budget: WorkBudget, cancellation: CancellationToken| {
            let (native, metadata) = (std::sync::Arc::clone(&native), metadata.clone());
            async move {
                let reported = whole();
                let original = crate::NativeMaterializationWork::new(budget, &cancellation)
                    .map_err(|(error, work)| OperationFailure::new(CaptureError::from(error), work))?;
                let mut scope = CaptureWorkScope::new(&original)?;
                {
                    let mut work = &scope;
                    work.native.with_original_leaf(|work| work.adopt(reported.capacity() as u64 * std::mem::size_of::<HostDataRange>() as u64))
                        .map_err(|error| OperationFailure::new(capture_demand_error(error), work.native.receipt()))?;
                }
                let result = settled_sparse_ranges(&native,
    &metadata,
    reported,
    16,
    &cancellation,
    &scope,
)
                .await.map(drop);
                scope.release().map_err(|error| OperationFailure::new(error, original.receipt()))?;
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
            Err(failure) if matches!(failure.error, CaptureError::Kernel(crate::FsError::Work(_) | crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(_))))
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

fn capture_link_target(target: Vec<u8>, meter: &CaptureWorkScope) -> Result<bytes::Bytes, CaptureError> {
    meter.source().check().map_err(capture_demand_error)?;
    if target.len() != target.capacity() {
        meter.native.with_original_leaf(|work| work.claim_allocation(0, 1)).map_err(capture_demand_error)?;
    }
    Ok(bytes::Bytes::from(target))
}

fn read_link_bytes(
    root: &HostRoot,
    path: &CaptureHostPath,
    meter: &CaptureWorkScope,
) -> Result<Vec<u8>, CaptureError> {
    let maximum = meter
        .native
        .with_original_leaf(|meter| meter.remaining())
        .map_err(capture_demand_error)?
        .source_bytes_read;
    #[cfg(unix)]
    {
        // The returned byte buffer keeps the driver's existing capture claim.
        let (bytes, _allocation) = root.read_link(path, maximum, &mut meter.source()).map_err(capture_demand_error)?;
        Ok(bytes)
    }
    #[cfg(windows)]
    {
        let (units, allocation) = root
            .read_link_wide(path, maximum, &mut meter.source())
            .map_err(capture_demand_error)?;
        let length = units
            .len()
            .checked_mul(2)
            .ok_or(CaptureError::from(WorkError::Overflow))?;
        let mut bytes = capture_vec(length, meter)?;
        for unit in &units {
            capture_charge(meter, WorkCounters { bytes_copied: 2, ..WorkCounters::UNCHARGED })?;
            bytes.extend_from_slice(&unit.to_le_bytes());
        }
        drop(units);
        meter.source().release(allocation).map_err(capture_demand_error)?;
        Ok(bytes)
    }
}

fn capture_metadata(metadata: &HostStat) -> FileMetadata {
    let mut result = FileMetadata {
        created_ns: system_time(metadata.created()),
        modified_ns: system_time(metadata.modified()),
        accessed_ns: system_time(metadata.accessed()),
        ..FileMetadata::default()
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
