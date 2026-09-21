//! Journaled pathwise application and crash recovery.
//!
//! Merge publication changes an authenticated workspace head atomically. A
//! native checkout still needs pathwise host operations, which cannot be made
//! atomic by an operating system. This module centralizes the durable protocol:
//! capture every preimage before the first mutation, record progress after each
//! operation, and complete or roll back deterministically after interruption.

use crate::{GenerationId, OperationId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::future::Future;
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use thiserror::Error;

const JOURNAL_VERSION: u32 = 2;
const MAXIMUM_CAS_ATTEMPTS: u8 = 32;

/// One declarative pathwise checkout edit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum MaterializationEdit {
    /// Install a complete backend-defined entry image at one portable path.
    Install {
        /// Canonical relative path.
        path: String,
        /// Opaque entry image interpreted by the backend.
        image: Vec<u8>,
    },
    /// Remove one entry or subtree.
    Remove {
        /// Canonical relative path.
        path: String,
    },
    /// Rename one entry without flattening its metadata or link identity.
    Rename {
        /// Existing canonical relative path.
        from: String,
        /// Destination canonical relative path.
        to: String,
    },
    /// Change representable metadata without replacing a directory subtree.
    SetMetadata {
        /// Canonical relative path.
        path: String,
        /// Backend-defined declarative metadata image.
        image: Vec<u8>,
    },
}

/// Immutable materialization plan bound to exact SDK generations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPlan {
    /// Stable retry and recovery identity.
    pub operation_id: OperationId,
    /// Generation expected to be represented before application.
    pub from: GenerationId,
    /// Published generation represented after application.
    pub to: GenerationId,
    /// Ordered path edits. Destructive edits do not overlap; metadata edits may
    /// follow edits to descendants so directory identity is preserved.
    pub edits: Vec<MaterializationEdit>,
}

/// Opaque complete preimage for one edit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPreimage {
    /// Backend-defined bytes sufficient to restore every representable fact.
    pub image: Vec<u8>,
}

/// Exact physical state observed while resuming one journaled edit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializationObservation {
    /// Backend delegates idempotency to its apply/restore implementations.
    Unverified,
    /// The edit has not yet been applied.
    Preimage,
    /// The edit was applied before its progress record became durable.
    Postimage,
    /// An operation-owned rename sequence stopped between its durable endpoints.
    Interrupted,
    /// Neither durable endpoint matches; an external writer intervened.
    Diverged,
}

/// Durable application phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationPhase {
    /// Every preimage is durable and no edit has been applied.
    Prepared,
    /// Forward application is in progress.
    Applying,
    /// Every edit is durable in the target checkout.
    Applied,
    /// Reverse preimage restoration is in progress.
    RollingBack,
    /// The original checkout has been restored.
    RolledBack,
}

/// Complete versioned recovery journal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationJournal {
    /// Serialization contract version.
    pub version: u32,
    /// Monotonic optimistic-concurrency revision.
    pub revision: u64,
    /// Immutable plan.
    pub plan: MaterializationPlan,
    /// Preimages in exact edit order.
    pub preimages: Vec<MaterializationPreimage>,
    /// Current recovery phase.
    pub phase: MaterializationPhase,
    /// Number of forward edits known durable.
    pub applied: u32,
    /// Number of applied edits restored from the end.
    pub restored: u32,
}

/// Durable optimistic-concurrency adapter for materialization journals.
pub trait MaterializationJournalStore: Send + Sync {
    /// Adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Loads one operation journal.
    fn load(
        &self,
        operation_id: OperationId,
    ) -> impl Future<Output = Result<Option<MaterializationJournal>, Self::Error>> + Send;

    /// Atomically replaces `expected_revision`; zero creates a journal.
    fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MaterializationJournal,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;
}

/// Backend that can capture, apply, and restore every representable host fact.
pub trait MaterializationBackend: Send + Sync {
    /// Backend error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Verifies that the checkout still represents the plan's exact source.
    /// Backends without an independently observable generation may retain the
    /// conservative default and provide equivalent fencing externally.
    fn verify_source(
        &self,
        _from: GenerationId,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send {
        async { Ok(true) }
    }

    /// Captures a complete preimage without mutating the checkout.
    fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> impl Future<Output = Result<MaterializationPreimage, Self::Error>> + Send;

    /// Classifies the current path against both durable endpoints.
    ///
    /// Backends that cannot independently witness physical state may retain
    /// the default only when equivalent exclusion is provided externally.
    fn observe(
        &self,
        _edit: &MaterializationEdit,
        _preimage: &MaterializationPreimage,
    ) -> impl Future<Output = Result<MaterializationObservation, Self::Error>> + Send {
        async { Ok(MaterializationObservation::Unverified) }
    }

    /// Applies one declarative edit idempotently.
    fn apply(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Restores the exact preimage for one edit idempotently.
    fn restore(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Recovery decision for a nonterminal journal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializationRecovery {
    /// Continue applying the unpublished checkout candidate.
    Complete,
    /// Restore every path changed before interruption.
    RollBack,
}

/// Materialization failure.
#[derive(Debug, Error)]
pub enum MaterializationError<S: std::error::Error + 'static, B: std::error::Error + 'static> {
    /// Durable journal access failed.
    #[error("materialization journal failed: {0}")]
    Store(S),
    /// Host checkout access failed.
    #[error("materialization backend failed: {0}")]
    Backend(B),
    /// A retry supplied different immutable plan input.
    #[error("materialization retry conflicts with its durable plan")]
    PlanConflict,
    /// Persisted progress is malformed or uses an unsupported version.
    #[error("materialization journal is incompatible")]
    IncompatibleJournal,
    /// Optimistic state contention exceeded the bounded retry policy.
    #[error("materialization journal remained contended")]
    Contended,
    /// Physical state no longer matches either durable endpoint.
    #[error("materialization paused because an external path mutation was observed")]
    ExternalMutation,
}

/// Core materializer coordinating one journal store and one host backend.
pub struct JournaledMaterializer<S, B> {
    store: S,
    backend: B,
}

impl<S, B> JournaledMaterializer<S, B> {
    /// Creates a journaled materializer.
    #[must_use]
    pub const fn new(store: S, backend: B) -> Self {
        Self { store, backend }
    }

    /// Borrows the journal adapter.
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// Borrows the host backend.
    #[must_use]
    pub const fn backend(&self) -> &B {
        &self.backend
    }
}

impl<S: MaterializationJournalStore, B: MaterializationBackend> JournaledMaterializer<S, B> {
    /// Captures preimages, applies the plan, and records every durable boundary.
    pub async fn apply(
        &self,
        plan: MaterializationPlan,
    ) -> Result<MaterializationJournal, MaterializationError<S::Error, B::Error>> {
        validate_plan(&plan).map_err(|()| MaterializationError::IncompatibleJournal)?;
        let mut journal = match self.load(plan.operation_id).await? {
            Some(existing) => {
                if existing.plan != plan {
                    return Err(MaterializationError::PlanConflict);
                }
                existing
            }
            None => self.prepare(plan).await?,
        };
        if matches!(journal.phase, MaterializationPhase::RolledBack) {
            return Err(MaterializationError::PlanConflict);
        }
        if journal.phase == MaterializationPhase::Applied {
            self.repair_applied(&journal).await?;
            return Ok(journal);
        }
        journal.phase = MaterializationPhase::Applying;
        journal = self.persist(journal).await?;
        while usize::try_from(journal.applied).unwrap_or(usize::MAX) < journal.plan.edits.len() {
            let index = usize::try_from(journal.applied)
                .map_err(|_| MaterializationError::IncompatibleJournal)?;
            let edit = journal
                .plan
                .edits
                .get(index)
                .ok_or(MaterializationError::IncompatibleJournal)?;
            let preimage = journal
                .preimages
                .get(index)
                .ok_or(MaterializationError::IncompatibleJournal)?;
            match self
                .backend
                .observe(edit, preimage)
                .await
                .map_err(MaterializationError::Backend)?
            {
                MaterializationObservation::Unverified
                | MaterializationObservation::Preimage
                | MaterializationObservation::Interrupted => {
                    self.backend
                        .apply(edit, preimage)
                        .await
                        .map_err(MaterializationError::Backend)?;
                }
                MaterializationObservation::Postimage => {}
                MaterializationObservation::Diverged => {
                    return Err(MaterializationError::ExternalMutation);
                }
            }
            journal.applied = journal.applied.saturating_add(1);
            journal = self.persist(journal).await?;
        }
        journal.phase = MaterializationPhase::Applied;
        self.persist(journal).await
    }

    /// Recovers an interrupted operation by completing it or restoring preimages.
    pub async fn recover(
        &self,
        operation_id: OperationId,
        recovery: MaterializationRecovery,
    ) -> Result<Option<MaterializationJournal>, MaterializationError<S::Error, B::Error>> {
        let Some(journal) = self.load(operation_id).await? else {
            return Ok(None);
        };
        match recovery {
            MaterializationRecovery::Complete => {
                if journal.phase == MaterializationPhase::RollingBack || journal.restored != 0 {
                    return Err(MaterializationError::IncompatibleJournal);
                }
                self.apply(journal.plan.clone()).await.map(Some)
            }
            MaterializationRecovery::RollBack => self.rollback(journal).await.map(Some),
        }
    }

    async fn prepare(
        &self,
        plan: MaterializationPlan,
    ) -> Result<MaterializationJournal, MaterializationError<S::Error, B::Error>> {
        validate_plan(&plan).map_err(|()| MaterializationError::IncompatibleJournal)?;
        if !self
            .backend
            .verify_source(plan.from)
            .await
            .map_err(MaterializationError::Backend)?
        {
            return Err(MaterializationError::PlanConflict);
        }
        let mut preimages = Vec::with_capacity(plan.edits.len());
        for edit in &plan.edits {
            preimages.push(
                self.backend
                    .capture(edit)
                    .await
                    .map_err(MaterializationError::Backend)?,
            );
        }
        let journal = MaterializationJournal {
            version: JOURNAL_VERSION,
            revision: 1,
            plan,
            preimages,
            phase: MaterializationPhase::Prepared,
            applied: 0,
            restored: 0,
        };
        if self
            .store
            .compare_and_swap(journal.plan.operation_id, 0, journal.clone())
            .await
            .map_err(MaterializationError::Store)?
        {
            Ok(journal)
        } else {
            self.load(journal.plan.operation_id)
                .await?
                .ok_or(MaterializationError::Contended)
        }
    }

    async fn rollback(
        &self,
        mut journal: MaterializationJournal,
    ) -> Result<MaterializationJournal, MaterializationError<S::Error, B::Error>> {
        if journal.phase == MaterializationPhase::RolledBack {
            return Ok(journal);
        }
        journal.phase = MaterializationPhase::RollingBack;
        journal = self.persist(journal).await?;
        while journal.restored < journal.applied {
            let index = journal
                .applied
                .checked_sub(journal.restored.saturating_add(1))
                .and_then(|value| usize::try_from(value).ok())
                .ok_or(MaterializationError::IncompatibleJournal)?;
            let edit = journal
                .plan
                .edits
                .get(index)
                .ok_or(MaterializationError::IncompatibleJournal)?;
            let preimage = journal
                .preimages
                .get(index)
                .ok_or(MaterializationError::IncompatibleJournal)?;
            match self
                .backend
                .observe(edit, preimage)
                .await
                .map_err(MaterializationError::Backend)?
            {
                MaterializationObservation::Unverified
                | MaterializationObservation::Postimage
                | MaterializationObservation::Interrupted => {
                    self.backend
                        .restore(edit, preimage)
                        .await
                        .map_err(MaterializationError::Backend)?;
                }
                MaterializationObservation::Preimage => {}
                MaterializationObservation::Diverged => {
                    return Err(MaterializationError::ExternalMutation);
                }
            }
            journal.restored = journal.restored.saturating_add(1);
            journal = self.persist(journal).await?;
        }
        journal.phase = MaterializationPhase::RolledBack;
        self.persist(journal).await
    }

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MaterializationJournal>, MaterializationError<S::Error, B::Error>> {
        let journal = self
            .store
            .load(operation_id)
            .await
            .map_err(MaterializationError::Store)?;
        if journal.as_ref().is_some_and(|journal| {
            !valid_journal(journal, operation_id) || validate_plan(&journal.plan).is_err()
        }) {
            return Err(MaterializationError::IncompatibleJournal);
        }
        Ok(journal)
    }

    async fn repair_applied(
        &self,
        journal: &MaterializationJournal,
    ) -> Result<(), MaterializationError<S::Error, B::Error>> {
        for (edit, preimage) in journal.plan.edits.iter().zip(&journal.preimages) {
            match self
                .backend
                .observe(edit, preimage)
                .await
                .map_err(MaterializationError::Backend)?
            {
                MaterializationObservation::Unverified | MaterializationObservation::Postimage => {}
                MaterializationObservation::Preimage | MaterializationObservation::Interrupted => {
                    self.backend
                        .apply(edit, preimage)
                        .await
                        .map_err(MaterializationError::Backend)?;
                }
                MaterializationObservation::Diverged => {
                    return Err(MaterializationError::ExternalMutation);
                }
            }
        }
        Ok(())
    }

    async fn persist(
        &self,
        mut journal: MaterializationJournal,
    ) -> Result<MaterializationJournal, MaterializationError<S::Error, B::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let expected = journal.revision;
            journal.revision = expected.saturating_add(1);
            if self
                .store
                .compare_and_swap(journal.plan.operation_id, expected, journal.clone())
                .await
                .map_err(MaterializationError::Store)?
            {
                return Ok(journal);
            }
            let Some(current) = self.load(journal.plan.operation_id).await? else {
                return Err(MaterializationError::IncompatibleJournal);
            };
            if current.plan != journal.plan {
                return Err(MaterializationError::PlanConflict);
            }
            journal = current;
        }
        Err(MaterializationError::Contended)
    }
}

fn valid_journal(journal: &MaterializationJournal, operation_id: OperationId) -> bool {
    let edit_count = journal.plan.edits.len();
    let applied = usize::try_from(journal.applied).unwrap_or(usize::MAX);
    let restored = usize::try_from(journal.restored).unwrap_or(usize::MAX);
    journal.version == JOURNAL_VERSION
        && journal.plan.operation_id == operation_id
        && journal.preimages.len() == edit_count
        && applied <= edit_count
        && restored <= applied
        && match journal.phase {
            MaterializationPhase::Prepared => applied == 0 && restored == 0,
            MaterializationPhase::Applying => restored == 0,
            MaterializationPhase::Applied => applied == edit_count && restored == 0,
            MaterializationPhase::RollingBack => true,
            MaterializationPhase::RolledBack => restored == applied,
        }
}

fn validate_plan(plan: &MaterializationPlan) -> Result<(), ()> {
    let mut paths = Vec::with_capacity(plan.edits.len().saturating_mul(2));
    for edit in &plan.edits {
        match edit {
            MaterializationEdit::Install { path, .. } | MaterializationEdit::Remove { path } => {
                validate_materialization_path(path)?;
                paths.push(path.as_str());
            }
            MaterializationEdit::SetMetadata { path, .. } => {
                validate_materialization_path(path)?;
            }
            MaterializationEdit::Rename { from, to } => {
                validate_materialization_path(from)?;
                validate_materialization_path(to)?;
                if from == to {
                    return Err(());
                }
                paths.push(from.as_str());
                paths.push(to.as_str());
            }
        }
    }
    paths.sort_unstable();
    for pair in paths.windows(2) {
        let [left, right] = pair else { continue };
        if left == right
            || right
                .strip_prefix(*left)
                .is_some_and(|suffix| suffix.starts_with('/'))
        {
            return Err(());
        }
    }
    Ok(())
}

fn validate_materialization_path(path: &str) -> Result<(), ()> {
    if path.is_empty()
        || path.starts_with('/')
        || path.starts_with('\\')
        || path.contains('\\')
        || path
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(());
    }
    Ok(())
}

/// Native same-volume tree publisher used for root checkout application.
///
/// Callers first materialize the exact target generation into `target`. The
/// operation directory may be a sibling of the checkout so no repository
/// marker is required, but it must not be the checkout, an ancestor, or a
/// descendant. Each
/// top-level binding is then exchanged with the live root independently. The
/// displaced live binding remains in `backup` until the journal is terminal,
/// preserving metadata, sparse allocation, links, and hard-link identity for
/// rollback without re-encoding host state.
#[cfg(not(target_arch = "wasm32"))]
pub struct NativeTreeMaterializationBackend {
    root: PathBuf,
    target: PathBuf,
    backup: PathBuf,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeTreeMaterializationBackend {
    /// Binds an existing checkout root and same-volume operation directory.
    /// `operation_directory/target` must contain the fully materialized target
    /// tree; `operation_directory/backup` is owned by this backend.
    pub fn new(
        root: impl Into<PathBuf>,
        operation_directory: impl Into<PathBuf>,
    ) -> Result<Self, NativeTreeMaterializationError> {
        let root = root.into().canonicalize()?;
        let operation_directory = operation_directory.into().canonicalize()?;
        let target = operation_directory.join("target");
        if !target.is_dir()
            || operation_directory == root
            || root.starts_with(&operation_directory)
            || operation_directory.starts_with(&root)
            || !same_native_volume(&root, &operation_directory)?
        {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let backup = operation_directory.join("backup");
        std::fs::create_dir_all(&backup)?;
        Ok(Self {
            root,
            target,
            backup,
        })
    }

    /// Builds one non-overlapping edit per top-level binding. Excluded names
    /// remain untouched in the live root and must also be excluded from source
    /// capture.
    pub fn plan(
        &self,
        operation_id: OperationId,
        from: GenerationId,
        to: GenerationId,
        excluded_names: &[&str],
    ) -> Result<MaterializationPlan, NativeTreeMaterializationError> {
        let mut names = std::collections::BTreeSet::new();
        collect_utf8_names(&self.root, &mut names)?;
        collect_utf8_names(&self.target, &mut names)?;
        for excluded in excluded_names {
            names.remove(*excluded);
        }
        let edits = names
            .into_iter()
            .map(|path| {
                let edit = if native_entry_exists(&self.target.join(&path))? {
                    MaterializationEdit::Install {
                        path,
                        image: vec![1],
                    }
                } else {
                    MaterializationEdit::Remove { path }
                };
                Ok(edit)
            })
            .collect::<Result<Vec<_>, std::io::Error>>()?;
        Ok(MaterializationPlan {
            operation_id,
            from,
            to,
            edits,
        })
    }

    #[cfg(any(feature = "native-mount", test))]
    fn plan_paths(
        &self,
        operation_id: OperationId,
        from: GenerationId,
        to: GenerationId,
        paths: impl IntoIterator<Item = String>,
    ) -> Result<MaterializationPlan, NativeTreeMaterializationError> {
        let edits = paths
            .into_iter()
            .map(|path| {
                validate_materialization_path(&path)
                    .map_err(|()| NativeTreeMaterializationError::InvalidPath(path.clone()))?;
                if native_entry_exists(&self.target.join(&path))? {
                    Ok(MaterializationEdit::Install {
                        path,
                        image: vec![1],
                    })
                } else {
                    Ok(MaterializationEdit::Remove { path })
                }
            })
            .collect::<Result<Vec<_>, NativeTreeMaterializationError>>()?;
        let plan = MaterializationPlan {
            operation_id,
            from,
            to,
            edits,
        };
        validate_plan(&plan).map_err(|()| NativeTreeMaterializationError::OverlappingPaths)?;
        Ok(plan)
    }

    fn paths(&self, path: &str) -> (PathBuf, PathBuf, PathBuf) {
        (
            self.root.join(path),
            self.target.join(path),
            self.backup.join(path),
        )
    }
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
fn validate_native_operation_location(
    root: &Path,
    operation_directory: &Path,
) -> Result<(), NativeTreeMaterializationError> {
    let root = root.canonicalize()?;
    let operation = prospective_canonical_path(operation_directory)?;
    if operation == root || operation.starts_with(&root) || root.starts_with(&operation) {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    }
    let existing = nearest_existing_ancestor(operation_directory)?;
    if !same_native_volume(&root, &existing.canonicalize()?)? {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    }
    Ok(())
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
fn nearest_existing_ancestor(path: &Path) -> Result<PathBuf, std::io::Error> {
    let mut cursor = path;
    loop {
        match std::fs::symlink_metadata(cursor) {
            Ok(_) => return Ok(cursor.to_path_buf()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                cursor = cursor.parent().ok_or(error)?;
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
fn prospective_canonical_path(path: &Path) -> Result<PathBuf, std::io::Error> {
    let existing = nearest_existing_ancestor(path)?;
    let mut canonical = existing.canonicalize()?;
    let suffix = path
        .strip_prefix(&existing)
        .map_err(|_| std::io::Error::other("invalid prospective path"))?;
    for component in suffix.components() {
        match component {
            std::path::Component::Normal(name) => canonical.push(name),
            _ => return Err(std::io::Error::other("invalid prospective path component")),
        }
    }
    Ok(canonical)
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
fn same_native_volume(left: &Path, right: &Path) -> Result<bool, std::io::Error> {
    use std::os::unix::fs::MetadataExt as _;
    Ok(std::fs::metadata(left)?.dev() == std::fs::metadata(right)?.dev())
}

#[cfg(all(windows, not(target_arch = "wasm32")))]
fn same_native_volume(left: &Path, right: &Path) -> Result<bool, std::io::Error> {
    use std::path::Component;
    let prefix = |path: &Path| {
        path.components().find_map(|component| match component {
            Component::Prefix(prefix) => Some(prefix.as_os_str().to_os_string()),
            _ => None,
        })
    };
    Ok(prefix(left) == prefix(right))
}

#[cfg(all(not(unix), not(windows), not(target_arch = "wasm32")))]
fn same_native_volume(_left: &Path, _right: &Path) -> Result<bool, std::io::Error> {
    Ok(true)
}

#[cfg(not(target_arch = "wasm32"))]
impl MaterializationBackend for NativeTreeMaterializationBackend {
    type Error = NativeTreeMaterializationError;

    async fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> Result<MaterializationPreimage, Self::Error> {
        let path = edit_path(edit);
        let (live, target, backup) = self.paths(path);
        if native_entry_exists(&backup)? {
            return Err(NativeTreeMaterializationError::UnexpectedBackup(
                path.to_owned(),
            ));
        }
        let before = native_entry_fingerprint(&live)?;
        let after = match edit {
            MaterializationEdit::Install { .. } => native_entry_fingerprint(&target)?
                .ok_or_else(|| NativeTreeMaterializationError::MissingTarget(path.to_owned()))?
                .into(),
            MaterializationEdit::Remove { .. } => None,
            MaterializationEdit::SetMetadata { image, .. } => {
                let desired = decode_native_metadata(image)?;
                native_entry_fingerprint_with_metadata(&live, &desired)?
            }
            MaterializationEdit::Rename { .. } => {
                return Err(NativeTreeMaterializationError::UnsupportedRename);
            }
        };
        let metadata = if matches!(edit, MaterializationEdit::SetMetadata { .. }) {
            Some(native_metadata(&std::fs::symlink_metadata(&live)?))
        } else {
            None
        };
        Ok(MaterializationPreimage {
            image: encode_native_witness(before, after, metadata)?,
        })
    }

    async fn observe(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<MaterializationObservation, Self::Error> {
        let (before, after) = decode_native_witness(&preimage.image)?;
        let path = edit_path(edit);
        let (live, target, backup) = self.paths(path);
        let current = native_entry_fingerprint(&live)?;
        let target_current = if matches!(edit, MaterializationEdit::Install { .. }) {
            native_entry_fingerprint(&target)?
        } else {
            None
        };
        let backup_current = native_entry_fingerprint(&backup)?;
        let backup_exists = backup_current.is_some();
        if current == before && current == after && target_current.is_none() && backup_exists {
            return Ok(MaterializationObservation::Postimage);
        }
        if current == before {
            if matches!(edit, MaterializationEdit::Install { .. }) && target_current != after {
                return Ok(MaterializationObservation::Diverged);
            }
            return Ok(MaterializationObservation::Preimage);
        }
        if current == after {
            if target_current.is_some() && target_current != after {
                return Ok(MaterializationObservation::Diverged);
            }
            return Ok(MaterializationObservation::Postimage);
        }

        // A crash may land between the two renames used for an install. The
        // durable backup proves the old binding was moved by this operation;
        // the still-present staged target means forward application can resume.
        if current.is_none()
            && backup_current == before
            && matches!(edit, MaterializationEdit::Install { .. })
        {
            return if target_current.is_none() || target_current == after {
                Ok(MaterializationObservation::Interrupted)
            } else {
                Ok(MaterializationObservation::Diverged)
            };
        }
        Ok(MaterializationObservation::Diverged)
    }

    async fn apply(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        let path = edit_path(edit);
        let (live, target, backup) = self.paths(path);
        let (before, after) = decode_native_witness(&preimage.image)?;
        if let MaterializationEdit::SetMetadata { image, .. } = edit {
            let current = native_entry_fingerprint(&live)?;
            if current == after {
                return Ok(());
            }
            if current != before {
                return Err(NativeTreeMaterializationError::ExternalMutation(
                    path.to_owned(),
                ));
            }
            return apply_native_metadata(&live, &decode_native_metadata(image)?);
        }
        let target_expected = matches!(edit, MaterializationEdit::Install { .. });
        let backup_exists = native_entry_exists(&backup)?;
        let target_fingerprint = native_entry_fingerprint(&target)?;
        let target_exists = target_fingerprint.is_some();
        let live_fingerprint = native_entry_fingerprint(&live)?;
        let live_exists = live_fingerprint.is_some();
        if !backup_exists && !target_exists && live_fingerprint == after {
            return Ok(());
        }
        if backup_exists && !target_exists && live_fingerprint == after {
            return Ok(());
        }
        if backup_exists && live_fingerprint.is_some() && live_fingerprint != after {
            return Err(NativeTreeMaterializationError::ExternalMutation(
                path.to_owned(),
            ));
        }
        if !backup_exists && live_fingerprint != before {
            return Err(NativeTreeMaterializationError::ExternalMutation(
                path.to_owned(),
            ));
        }
        if target_expected && target_fingerprint != after {
            return Err(NativeTreeMaterializationError::ExternalMutation(
                path.to_owned(),
            ));
        }
        if backup_exists {
            if target_expected && target_exists {
                remove_native_entry_durable(&live)?;
                durable_native_rename(&target, &live)?;
            } else if target_expected && !live_exists {
                return Err(NativeTreeMaterializationError::MissingTarget(
                    path.to_owned(),
                ));
            } else if !target_expected {
                remove_native_entry_durable(&live)?;
            }
            return Ok(());
        }
        if live_exists {
            if let Some(parent) = backup.parent() {
                std::fs::create_dir_all(parent)?;
            }
            durable_native_rename(&live, &backup)?;
        }
        if target_expected {
            if !target_exists {
                return Err(NativeTreeMaterializationError::MissingTarget(
                    path.to_owned(),
                ));
            }
            if let Some(parent) = live.parent() {
                std::fs::create_dir_all(parent)?;
            }
            if let Err(error) = durable_native_rename(&target, &live) {
                if native_entry_exists(&backup)? && !native_entry_exists(&live)? {
                    durable_native_rename(&backup, &live)?;
                }
                return Err(error);
            }
        }
        Ok(())
    }

    async fn restore(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        let path = edit_path(edit);
        let (live, _, backup) = self.paths(path);
        let (before, after) = decode_native_witness(&preimage.image)?;
        let current = native_entry_fingerprint(&live)?;
        if matches!(edit, MaterializationEdit::SetMetadata { .. }) {
            if current == before {
                return Ok(());
            }
            if current != after {
                return Err(NativeTreeMaterializationError::ExternalMutation(
                    path.to_owned(),
                ));
            }
            let metadata = decode_native_preimage_metadata(&preimage.image)?
                .ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
            return apply_native_metadata(&live, &metadata);
        }
        let backup_current = native_entry_fingerprint(&backup)?;
        let backup_exists = backup_current.is_some();
        if backup_exists && backup_current != before {
            return Err(NativeTreeMaterializationError::ExternalMutation(
                path.to_owned(),
            ));
        }
        if current == before && !backup_exists {
            return Ok(());
        }
        if current != after && !(current.is_none() && backup_exists) {
            return Err(NativeTreeMaterializationError::ExternalMutation(
                path.to_owned(),
            ));
        }
        match before {
            None => remove_native_entry_durable(&live),
            Some(_) if backup_exists => {
                remove_native_entry_durable(&live)?;
                durable_native_rename(&backup, &live)
            }
            Some(expected) if current == Some(expected) => Ok(()),
            Some(_) => Err(NativeTreeMaterializationError::MissingPreimage(
                path.to_owned(),
            )),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn durable_native_rename(from: &Path, to: &Path) -> Result<(), NativeTreeMaterializationError> {
    acyclic_native_runtime::durable_rename(from, to, acyclic_native_runtime::RenameMode::NoReplace)
        .map_err(Into::into)
}

#[cfg(not(target_arch = "wasm32"))]
fn remove_native_entry_durable(path: &Path) -> Result<(), NativeTreeMaterializationError> {
    let existed = native_entry_exists(path)?;
    remove_native_entry(path)?;
    if existed {
        let parent = path
            .parent()
            .ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
        acyclic_native_runtime::sync_parent(parent, acyclic_native_runtime::Durability::Full)?;
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn edit_path(edit: &MaterializationEdit) -> &str {
    match edit {
        MaterializationEdit::Install { path, .. }
        | MaterializationEdit::Remove { path }
        | MaterializationEdit::SetMetadata { path, .. } => path,
        MaterializationEdit::Rename { from, .. } => from,
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn collect_utf8_names(
    directory: &Path,
    names: &mut std::collections::BTreeSet<String>,
) -> Result<(), NativeTreeMaterializationError> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| NativeTreeMaterializationError::UnrepresentableName)?;
        names.insert(name);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn native_entry_exists(path: &Path) -> Result<bool, std::io::Error> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(not(target_arch = "wasm32"))]
const NATIVE_WITNESS_VERSION: u8 = 1;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, Deserialize, Serialize)]
struct NativeMetadataImage {
    readonly: bool,
    posix_mode: Option<u32>,
    windows_attributes: Option<u32>,
}

#[cfg(not(target_arch = "wasm32"))]
type NativeFingerprint = [u8; 32];

#[cfg(not(target_arch = "wasm32"))]
type NativeWitness = (Option<NativeFingerprint>, Option<NativeFingerprint>);

#[cfg(not(target_arch = "wasm32"))]
fn encode_native_witness(
    before: Option<[u8; 32]>,
    after: Option<[u8; 32]>,
    metadata: Option<NativeMetadataImage>,
) -> Result<Vec<u8>, NativeTreeMaterializationError> {
    let mut image = Vec::with_capacity(67);
    image.push(NATIVE_WITNESS_VERSION);
    for fingerprint in [before, after] {
        image.push(u8::from(fingerprint.is_some()));
        image.extend_from_slice(&fingerprint.unwrap_or([0; 32]));
    }
    if let Some(metadata) = metadata {
        image.extend_from_slice(
            &serde_json::to_vec(&metadata)
                .map_err(|_| NativeTreeMaterializationError::InvalidPreimage)?,
        );
    }
    Ok(image)
}

#[cfg(not(target_arch = "wasm32"))]
fn decode_native_witness(image: &[u8]) -> Result<NativeWitness, NativeTreeMaterializationError> {
    let (witness, _) = image
        .split_at_checked(67)
        .ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
    let image: &[u8; 67] = witness
        .try_into()
        .map_err(|_| NativeTreeMaterializationError::InvalidPreimage)?;
    let (version, fields) = image
        .split_first()
        .ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
    if *version != NATIVE_WITNESS_VERSION {
        return Err(NativeTreeMaterializationError::InvalidPreimage);
    }
    let decode = |present: u8, bytes: &[u8]| {
        let fingerprint: [u8; 32] = bytes
            .try_into()
            .map_err(|_| NativeTreeMaterializationError::InvalidPreimage)?;
        match present {
            0 if fingerprint == [0; 32] => Ok(None),
            1 => Ok(Some(fingerprint)),
            _ => Err(NativeTreeMaterializationError::InvalidPreimage),
        }
    };
    let (before, after) = fields
        .split_at_checked(33)
        .ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
    let (before_present, before_bytes) = before
        .split_first()
        .ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
    let (after_present, after_bytes) = after
        .split_first()
        .ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
    Ok((
        decode(*before_present, before_bytes)?,
        decode(*after_present, after_bytes)?,
    ))
}

#[cfg(not(target_arch = "wasm32"))]
fn decode_native_preimage_metadata(
    image: &[u8],
) -> Result<Option<NativeMetadataImage>, NativeTreeMaterializationError> {
    let (_, metadata) = image
        .split_at_checked(67)
        .ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
    if metadata.is_empty() {
        Ok(None)
    } else {
        serde_json::from_slice(metadata)
            .map(Some)
            .map_err(|_| NativeTreeMaterializationError::InvalidPreimage)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn decode_native_metadata(
    image: &[u8],
) -> Result<NativeMetadataImage, NativeTreeMaterializationError> {
    serde_json::from_slice(image).map_err(|_| NativeTreeMaterializationError::InvalidPreimage)
}

#[cfg(not(target_arch = "wasm32"))]
fn native_entry_fingerprint(path: &Path) -> Result<Option<[u8; 32]>, std::io::Error> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut hasher = blake3::Hasher::new();
    hash_native_entry(path, &metadata, &mut hasher, None)?;
    Ok(Some(*hasher.finalize().as_bytes()))
}

#[cfg(not(target_arch = "wasm32"))]
fn native_entry_fingerprint_with_metadata(
    path: &Path,
    desired: &NativeMetadataImage,
) -> Result<Option<[u8; 32]>, NativeTreeMaterializationError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut hasher = blake3::Hasher::new();
    hash_native_entry(path, &metadata, &mut hasher, Some(desired))?;
    Ok(Some(*hasher.finalize().as_bytes()))
}

#[cfg(not(target_arch = "wasm32"))]
fn hash_native_entry(
    path: &Path,
    metadata: &std::fs::Metadata,
    hasher: &mut blake3::Hasher,
    override_metadata: Option<&NativeMetadataImage>,
) -> Result<(), std::io::Error> {
    use std::io::Read as _;

    let file_type = metadata.file_type();
    hasher.update(if file_type.is_symlink() {
        b"link"
    } else if file_type.is_dir() {
        b"directory"
    } else if file_type.is_file() {
        b"file"
    } else {
        b"special"
    });
    hasher.update(&metadata.len().to_le_bytes());
    hasher.update(&[u8::from(override_metadata.map_or_else(
        || metadata.permissions().readonly(),
        |value| value.readonly,
    ))]);
    hash_native_metadata(metadata, override_metadata, hasher);

    if file_type.is_symlink() {
        hash_native_os_str(std::fs::read_link(path)?.as_os_str(), hasher);
    } else if file_type.is_dir() {
        let mut entries = std::fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            hash_native_os_str(&entry.file_name(), hasher);
            let metadata = std::fs::symlink_metadata(entry.path())?;
            hash_native_entry(&entry.path(), &metadata, hasher, None)?;
        }
    } else if file_type.is_file() {
        let mut file = std::fs::File::open(path)?;
        let mut buffer = vec![0; 1024 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            let bytes = buffer
                .get(..read)
                .ok_or_else(|| std::io::Error::other("invalid read length"))?;
            hasher.update(bytes);
        }
    }
    Ok(())
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
fn hash_native_metadata(
    metadata: &std::fs::Metadata,
    desired: Option<&NativeMetadataImage>,
    hasher: &mut blake3::Hasher,
) {
    use std::os::unix::fs::MetadataExt as _;
    for value in [
        metadata.dev(),
        metadata.ino(),
        u64::from(
            desired
                .and_then(|value| value.posix_mode)
                .unwrap_or_else(|| metadata.mode()),
        ),
        metadata.nlink(),
        u64::from(metadata.uid()),
        u64::from(metadata.gid()),
        metadata.rdev(),
    ] {
        hasher.update(&value.to_le_bytes());
    }
    hasher.update(&metadata.mtime().to_le_bytes());
    hasher.update(&metadata.mtime_nsec().to_le_bytes());
}

#[cfg(all(windows, not(target_arch = "wasm32")))]
fn hash_native_metadata(
    metadata: &std::fs::Metadata,
    desired: Option<&NativeMetadataImage>,
    hasher: &mut blake3::Hasher,
) {
    use std::os::windows::fs::MetadataExt as _;
    let attributes = desired.map_or_else(
        || metadata.file_attributes(),
        |value| {
            if let Some(attributes) = value.windows_attributes {
                attributes
            } else if value.readonly {
                metadata.file_attributes() | 1
            } else {
                metadata.file_attributes() & !1
            }
        },
    );
    for value in [
        u64::from(attributes),
        metadata.last_write_time(),
        metadata.file_size(),
    ] {
        hasher.update(&value.to_le_bytes());
    }
}

#[cfg(all(not(unix), not(windows), not(target_arch = "wasm32")))]
fn hash_native_metadata(
    _metadata: &std::fs::Metadata,
    _desired: Option<&NativeMetadataImage>,
    _hasher: &mut blake3::Hasher,
) {
}

#[cfg(not(target_arch = "wasm32"))]
fn native_metadata(metadata: &std::fs::Metadata) -> NativeMetadataImage {
    NativeMetadataImage {
        readonly: metadata.permissions().readonly(),
        #[cfg(unix)]
        posix_mode: {
            use std::os::unix::fs::MetadataExt as _;
            Some(metadata.mode())
        },
        #[cfg(not(unix))]
        posix_mode: None,
        #[cfg(windows)]
        windows_attributes: {
            use std::os::windows::fs::MetadataExt as _;
            Some(metadata.file_attributes())
        },
        #[cfg(not(windows))]
        windows_attributes: None,
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn apply_native_metadata(
    path: &Path,
    desired: &NativeMetadataImage,
) -> Result<(), NativeTreeMaterializationError> {
    #[cfg(windows)]
    if let Some(attributes) = desired.windows_attributes {
        acyclic_native_runtime::set_file_attributes(path, attributes)?;
        return Ok(());
    }
    let mut permissions = std::fs::symlink_metadata(path)?.permissions();
    #[cfg(unix)]
    if let Some(mode) = desired.posix_mode {
        use std::os::unix::fs::PermissionsExt as _;
        permissions.set_mode(mode);
    }
    #[cfg(not(unix))]
    permissions.set_readonly(desired.readonly);
    std::fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
fn encode_native_metadata(
    metadata: &crate::WorkspaceMetadata,
) -> Result<Vec<u8>, NativeTreeMaterializationError> {
    let readonly = metadata
        .windows_attributes
        .is_some_and(|attributes| attributes & 1 != 0)
        || metadata.posix_mode.is_some_and(|mode| mode & 0o222 == 0);
    serde_json::to_vec(&NativeMetadataImage {
        readonly,
        posix_mode: metadata.posix_mode,
        windows_attributes: metadata.windows_attributes,
    })
    .map_err(|_| NativeTreeMaterializationError::InvalidPreimage)
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
fn hash_native_os_str(value: &std::ffi::OsStr, hasher: &mut blake3::Hasher) {
    use std::os::unix::ffi::OsStrExt as _;
    hasher.update(value.as_bytes());
}

#[cfg(all(windows, not(target_arch = "wasm32")))]
fn hash_native_os_str(value: &std::ffi::OsStr, hasher: &mut blake3::Hasher) {
    use std::os::windows::ffi::OsStrExt as _;
    for code_unit in value.encode_wide() {
        hasher.update(&code_unit.to_le_bytes());
    }
}

#[cfg(all(not(unix), not(windows), not(target_arch = "wasm32")))]
fn hash_native_os_str(value: &std::ffi::OsStr, hasher: &mut blake3::Hasher) {
    hasher.update(value.to_string_lossy().as_bytes());
}

#[cfg(not(target_arch = "wasm32"))]
fn remove_native_entry(path: &Path) -> Result<(), std::io::Error> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    }
}

/// Native tree publication failure.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Error)]
pub enum NativeTreeMaterializationError {
    /// Root, target, or backup layout is not same-root and operation-scoped.
    #[error("native materialization layout is invalid")]
    InvalidLayout,
    /// A host top-level name cannot be represented by the portable journal.
    #[error("native materialization name is not UTF-8 representable")]
    UnrepresentableName,
    /// The exact target binding disappeared before application.
    #[error("native materialization target is missing for '{0}'")]
    MissingTarget(String),
    /// A physical endpoint changed after its durable witness was captured.
    #[error("native materialization observed an external mutation at '{0}'")]
    ExternalMutation(String),
    /// A durable preimage references a missing backup binding.
    #[error("native materialization preimage is missing for '{0}'")]
    MissingPreimage(String),
    /// Persisted preimage bytes are malformed.
    #[error("native materialization preimage is invalid")]
    InvalidPreimage,
    /// A backup exists before its operation journal has been created.
    #[error("native materialization found an unexpected backup for '{0}'")]
    UnexpectedBackup(String),
    /// Native path exchange does not yet expose a rename edit directly.
    #[error("native materialization rename edits are unsupported")]
    UnsupportedRename,
    /// Native filesystem operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A changed namespace path cannot be represented by this host publisher.
    #[error("native materialization path is invalid: '{0}'")]
    InvalidPath(String),
    /// Changed paths overlap and cannot be exchanged independently.
    #[error("native materialization paths overlap")]
    OverlappingPaths,
}

/// Publishes one fully prepared native tree through the durable local state
/// store.
///
/// The terminal journal is deliberately retained as the durable idempotency
/// receipt. Removing it would make a lost response indistinguishable from a
/// new request and could turn a retry of an install into a removal after the
/// staged target has been consumed.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub async fn publish_native_tree(
    state: &crate::LocalCoreStateStore,
    root: impl Into<PathBuf>,
    operation_directory: impl Into<PathBuf>,
    operation_id: OperationId,
    from: GenerationId,
    to: GenerationId,
    excluded_names: &[&str],
) -> Result<MaterializationJournal, NativeTreePublicationError> {
    let backend = NativeTreeMaterializationBackend::new(root, operation_directory)?;
    if let Some(existing) = MaterializationJournalStore::load(state, operation_id).await? {
        if existing.plan.from != from || existing.plan.to != to {
            return Err(MaterializationError::PlanConflict.into());
        }
        return JournaledMaterializer::new(state.clone(), backend)
            .recover(operation_id, MaterializationRecovery::Complete)
            .await?
            .ok_or(MaterializationError::IncompatibleJournal.into());
    }
    let plan = backend.plan(operation_id, from, to, excluded_names)?;
    let materializer = JournaledMaterializer::new(state.clone(), backend);
    materializer.apply(plan).await.map_err(Into::into)
}

/// Materializes and publishes one authenticated workspace generation to a
/// physical root through the core journal. Recovery resumes an existing
/// operation instead of rebuilding or accepting a partial checkout.
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
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

#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
#[allow(clippy::too_many_lines)]
/// Executes or resumes one bounded, journaled physical-root publication.
pub async fn publish_native_workspace_generation<A, O>(
    workspace: &crate::Workspace<A, O>,
    state: &crate::LocalCoreStateStore,
    publication: NativeWorkspacePublication<'_>,
) -> Result<MaterializationJournal, NativeWorkspacePublicationError>
where
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
{
    let from_generation = workspace.generation(publication.from).await?;
    let to_generation = workspace.generation(publication.to).await?;
    publish_native_generation_transition(&from_generation, &to_generation, state, publication).await
}

/// Publishes a transition between two authenticated generations, including
/// generations owned by sibling workspaces in the same distributed filesystem.
///
/// This is the branch-switch primitive: workspace identity may change while
/// physical checkout work remains proportional only to changed paths.
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
#[allow(clippy::too_many_lines)]
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
    validate_native_operation_location(&root, &operation_directory)?;
    if let Some(existing) = MaterializationJournalStore::load(state, operation_id).await? {
        if existing.plan.from != from || existing.plan.to != to {
            return Err(NativeWorkspacePublicationError::MismatchedJournal);
        }
        let backend = NativeTreeMaterializationBackend::new(&root, &operation_directory)?;
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
            match component.encoding() {
                crate::kernel::NameEncoding::Utf8 => {
                    path.push_str(std::str::from_utf8(component.as_bytes()).map_err(|_| {
                        NativeTreeMaterializationError::InvalidPath("<non-UTF-8>".to_owned())
                    })?);
                }
                crate::kernel::NameEncoding::PosixBytes
                | crate::kernel::NameEncoding::WindowsUtf16Le => {
                    return Err(NativeTreeMaterializationError::InvalidPath(
                        "<non-portable>".to_owned(),
                    )
                    .into());
                }
            }
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
        if before_directory && after_directory {
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
        paths.push((path, change.after.is_some()));
    }
    paths.retain(|(path, _)| {
        !structural_directories.iter().any(|directory| {
            path != directory
                && path
                    .strip_prefix(directory)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
    });
    paths.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    std::fs::create_dir_all(&operation_directory)?;
    let target = operation_directory.join("target");
    if target.exists() {
        std::fs::remove_dir_all(&target)?;
    }
    std::fs::create_dir_all(&target)?;
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
    let backend = NativeTreeMaterializationBackend::new(root, operation_directory)?;
    let mut plan = backend.plan_paths(
        operation_id,
        from,
        to,
        paths.into_iter().map(|(path, _)| path),
    )?;
    plan.edits.extend(metadata_edits);
    JournaledMaterializer::new(state.clone(), backend)
        .apply(plan)
        .await
        .map_err(Into::into)
}

/// Core native workspace publication failure.
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
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

/// Native tree publication failure.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[derive(Debug, Error)]
pub enum NativeTreePublicationError {
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
}

/// Process-local materialization journal adapter.
#[derive(Default)]
pub struct MemoryMaterializationJournalStore {
    journals: Mutex<BTreeMap<OperationId, MaterializationJournal>>,
}

impl MaterializationJournalStore for MemoryMaterializationJournalStore {
    type Error = MemoryMaterializationJournalStoreError;

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MaterializationJournal>, Self::Error> {
        self.journals
            .lock()
            .map_err(|_| MemoryMaterializationJournalStoreError)
            .map(|journals| journals.get(&operation_id).cloned())
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MaterializationJournal,
    ) -> Result<bool, Self::Error> {
        let mut journals = self
            .journals
            .lock()
            .map_err(|_| MemoryMaterializationJournalStoreError)?;
        let revision = journals
            .get(&operation_id)
            .map_or(0, |journal| journal.revision);
        if revision != expected_revision {
            return Ok(false);
        }
        journals.insert(operation_id, replacement);
        Ok(true)
    }
}

/// Process-local materialization journal synchronization failure.
#[derive(Debug, Error)]
#[error("materialization memory store is unavailable")]
pub struct MemoryMaterializationJournalStoreError;

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::Digest;

    #[derive(Default)]
    struct Backend(Mutex<Vec<Vec<u8>>>);

    #[derive(Debug, Error)]
    #[error("test backend unavailable")]
    struct BackendError;

    impl MaterializationBackend for Backend {
        type Error = BackendError;

        async fn capture(
            &self,
            _edit: &MaterializationEdit,
        ) -> Result<MaterializationPreimage, Self::Error> {
            Ok(MaterializationPreimage { image: vec![0] })
        }

        async fn apply(
            &self,
            edit: &MaterializationEdit,
            _preimage: &MaterializationPreimage,
        ) -> Result<(), Self::Error> {
            let MaterializationEdit::Install { image, .. } = edit else {
                return Err(BackendError);
            };
            self.0.lock().map_err(|_| BackendError)?.push(image.clone());
            Ok(())
        }

        async fn restore(
            &self,
            _edit: &MaterializationEdit,
            preimage: &MaterializationPreimage,
        ) -> Result<(), Self::Error> {
            self.0
                .lock()
                .map_err(|_| BackendError)?
                .push(preimage.image.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn apply_and_rollback_are_journaled_and_idempotent() {
        let operation_id = OperationId::new();
        let materializer = JournaledMaterializer::new(
            MemoryMaterializationJournalStore::default(),
            Backend::default(),
        );
        let plan = MaterializationPlan {
            operation_id,
            from: GenerationId::new(Digest::from_bytes([1; 32])),
            to: GenerationId::new(Digest::from_bytes([2; 32])),
            edits: vec![MaterializationEdit::Install {
                path: "file".to_owned(),
                image: vec![9],
            }],
        };
        let applied = materializer.apply(plan.clone()).await.expect("apply");
        assert_eq!(applied.phase, MaterializationPhase::Applied);
        assert_eq!(
            materializer.apply(plan).await.expect("retry").phase,
            MaterializationPhase::Applied
        );
        assert_eq!(
            materializer
                .backend()
                .0
                .lock()
                .expect("backend lock")
                .as_slice(),
            &[vec![9]]
        );
        let rolled_back = materializer
            .recover(operation_id, MaterializationRecovery::RollBack)
            .await
            .expect("recover")
            .expect("journal");
        assert_eq!(rolled_back.phase, MaterializationPhase::RolledBack);
    }

    #[tokio::test]
    async fn interrupted_rollback_cannot_be_completed_forward() {
        let operation_id = OperationId::new();
        let store = MemoryMaterializationJournalStore::default();
        let plan = MaterializationPlan {
            operation_id,
            from: GenerationId::new(Digest::from_bytes([1; 32])),
            to: GenerationId::new(Digest::from_bytes([2; 32])),
            edits: vec![MaterializationEdit::Install {
                path: "file".to_owned(),
                image: vec![9],
            }],
        };
        store
            .compare_and_swap(
                operation_id,
                0,
                MaterializationJournal {
                    version: JOURNAL_VERSION,
                    revision: 1,
                    plan,
                    preimages: vec![MaterializationPreimage { image: vec![0] }],
                    phase: MaterializationPhase::RollingBack,
                    applied: 1,
                    restored: 1,
                },
            )
            .await
            .expect("store journal");
        let materializer = JournaledMaterializer::new(store, Backend::default());
        assert!(matches!(
            materializer
                .recover(operation_id, MaterializationRecovery::Complete)
                .await,
            Err(MaterializationError::IncompatibleJournal)
        ));
    }

    #[tokio::test]
    async fn overlapping_or_noncanonical_plans_are_rejected_before_capture() {
        let materializer = JournaledMaterializer::new(
            MemoryMaterializationJournalStore::default(),
            Backend::default(),
        );
        let plan = MaterializationPlan {
            operation_id: OperationId::new(),
            from: GenerationId::new(Digest::from_bytes([1; 32])),
            to: GenerationId::new(Digest::from_bytes([2; 32])),
            edits: vec![
                MaterializationEdit::Remove {
                    path: "directory".to_owned(),
                },
                MaterializationEdit::Install {
                    path: "directory/file".to_owned(),
                    image: vec![1],
                },
            ],
        };
        assert!(matches!(
            materializer.apply(plan).await,
            Err(MaterializationError::IncompatibleJournal)
        ));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_applies_and_rolls_back_without_touching_exclusions() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let target = operation.join("target");
        std::fs::create_dir_all(root.join(".git")).expect("git directory");
        std::fs::create_dir_all(&target).expect("target directory");
        std::fs::write(root.join("old.txt"), b"old").expect("old file");
        std::fs::write(root.join(".git/config"), b"git").expect("git file");
        std::fs::write(target.join("new.txt"), b"new").expect("new file");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let plan = backend
            .plan(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                &[".git"],
            )
            .expect("plan");
        let operation_id = plan.operation_id;
        let materializer =
            JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), backend);
        materializer.apply(plan).await.expect("apply tree");
        assert!(!root.join("old.txt").exists());
        assert_eq!(std::fs::read(root.join("new.txt")).expect("new"), b"new");
        assert_eq!(
            std::fs::read(root.join(".git/config")).expect("git"),
            b"git"
        );
        materializer
            .recover(operation_id, MaterializationRecovery::RollBack)
            .await
            .expect("rollback");
        assert_eq!(std::fs::read(root.join("old.txt")).expect("old"), b"old");
        assert!(!root.join("new.txt").exists());
    }

    #[cfg(feature = "local")]
    #[tokio::test]
    async fn native_tree_publication_retains_receipt_for_lost_response_retry() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation_directory = temporary.path().join("operation");
        let target = operation_directory.join("target");
        let state = crate::LocalCoreStateStore::new(temporary.path().join("state"));
        std::fs::create_dir_all(&root).expect("checkout root");
        std::fs::create_dir_all(&target).expect("target root");
        std::fs::write(root.join("file"), b"before").expect("source file");
        std::fs::write(target.join("file"), b"after").expect("target file");
        let operation_id = OperationId::from_bytes([0x51; 16]);
        let from = GenerationId::new(Digest::from_bytes([0x11; 32]));
        let to = GenerationId::new(Digest::from_bytes([0x22; 32]));

        let first = publish_native_tree(
            &state,
            &root,
            &operation_directory,
            operation_id,
            from,
            to,
            &[],
        )
        .await
        .expect("initial publication");
        let retried = publish_native_tree(
            &state,
            &root,
            &operation_directory,
            operation_id,
            from,
            to,
            &[],
        )
        .await
        .expect("lost response retry");

        assert_eq!(retried, first);
        assert_eq!(
            std::fs::read(root.join("file")).expect("live file"),
            b"after"
        );
        assert_eq!(
            state.materialization_operations().expect("operations"),
            vec![operation_id]
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_pauses_before_overwriting_an_external_mutation() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let target = operation.join("target");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&target).expect("target");
        std::fs::write(root.join("file.txt"), b"before").expect("before");
        std::fs::write(target.join("file.txt"), b"after").expect("after");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let plan = backend
            .plan_paths(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                ["file.txt".to_owned()],
            )
            .expect("plan");
        let preimage = backend.capture(&plan.edits[0]).await.expect("capture");
        let store = MemoryMaterializationJournalStore::default();
        store
            .compare_and_swap(
                plan.operation_id,
                0,
                MaterializationJournal {
                    version: JOURNAL_VERSION,
                    revision: 1,
                    plan: plan.clone(),
                    preimages: vec![preimage.clone()],
                    phase: MaterializationPhase::Prepared,
                    applied: 0,
                    restored: 0,
                },
            )
            .await
            .expect("journal");
        std::fs::write(root.join("file.txt"), b"external").expect("external mutation");

        assert!(matches!(
            JournaledMaterializer::new(store, backend).apply(plan).await,
            Err(MaterializationError::ExternalMutation)
        ));
        assert_eq!(
            std::fs::read(root.join("file.txt")).expect("external remains"),
            b"external"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_rejects_tampered_or_missing_staged_targets() {
        for replacement in [Some(b"tampered".as_slice()), None] {
            let temporary = tempfile::tempdir().expect("temporary root");
            let root = temporary.path().join("checkout");
            let operation = temporary.path().join("operation");
            let target = operation.join("target");
            std::fs::create_dir_all(&root).expect("root");
            std::fs::create_dir_all(&target).expect("target");
            std::fs::write(root.join("file.txt"), b"before").expect("before");
            std::fs::write(target.join("file.txt"), b"after").expect("after");
            let backend =
                NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
            let plan = backend
                .plan_paths(
                    OperationId::new(),
                    GenerationId::new(Digest::from_bytes([1; 32])),
                    GenerationId::new(Digest::from_bytes([2; 32])),
                    ["file.txt".to_owned()],
                )
                .expect("plan");
            let edit = plan.edits.first().expect("install edit");
            let preimage = backend.capture(edit).await.expect("capture");
            match replacement {
                Some(bytes) => std::fs::write(target.join("file.txt"), bytes).expect("tamper"),
                None => std::fs::remove_file(target.join("file.txt")).expect("delete target"),
            }
            assert_eq!(
                backend.observe(edit, &preimage).await.expect("observe"),
                MaterializationObservation::Diverged
            );
            assert_eq!(
                std::fs::read(root.join("file.txt")).expect("live remains"),
                b"before"
            );
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_rejects_missing_staged_target_during_capture() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(operation.join("target")).expect("target");
        std::fs::write(root.join("file.txt"), b"before").expect("before");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let edit = MaterializationEdit::Install {
            path: "file.txt".to_owned(),
            image: Vec::new(),
        };

        assert!(matches!(
            backend.capture(&edit).await,
            Err(NativeTreeMaterializationError::MissingTarget(path)) if path == "file.txt"
        ));
        assert_eq!(
            std::fs::read(root.join("file.txt")).expect("live remains"),
            b"before"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_rolls_back_interrupted_install_from_durable_backup() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let target = operation.join("target");
        let backup = operation.join("backup");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&target).expect("target");
        std::fs::create_dir_all(&backup).expect("backup");
        std::fs::write(root.join("file.txt"), b"before").expect("before");
        std::fs::write(target.join("file.txt"), b"after").expect("after");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let plan = backend
            .plan_paths(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                ["file.txt".to_owned()],
            )
            .expect("plan");
        let edit = &plan.edits[0];
        let preimage = backend.capture(edit).await.expect("capture");

        std::fs::rename(root.join("file.txt"), backup.join("file.txt"))
            .expect("simulate crash after backup rename");
        assert_eq!(
            backend.observe(edit, &preimage).await.expect("observe"),
            MaterializationObservation::Interrupted
        );
        backend
            .restore(edit, &preimage)
            .await
            .expect("restore interrupted install");
        assert_eq!(
            std::fs::read(root.join("file.txt")).expect("restored live"),
            b"before"
        );
        assert!(!backup.join("file.txt").exists());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_revalidates_witnesses_inside_apply() {
        for mutate_target in [false, true] {
            let temporary = tempfile::tempdir().expect("temporary root");
            let root = temporary.path().join("checkout");
            let operation = temporary.path().join("operation");
            let target = operation.join("target");
            std::fs::create_dir_all(&root).expect("root");
            std::fs::create_dir_all(&target).expect("target");
            std::fs::write(root.join("file.txt"), b"before").expect("before");
            std::fs::write(target.join("file.txt"), b"after").expect("after");
            let backend =
                NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
            let plan = backend
                .plan_paths(
                    OperationId::new(),
                    GenerationId::new(Digest::from_bytes([1; 32])),
                    GenerationId::new(Digest::from_bytes([2; 32])),
                    ["file.txt".to_owned()],
                )
                .expect("plan");
            let edit = plan.edits.first().expect("install edit");
            let preimage = backend.capture(edit).await.expect("capture");
            assert_eq!(
                backend.observe(edit, &preimage).await.expect("observe"),
                MaterializationObservation::Preimage
            );
            let changed = if mutate_target {
                target.join("file.txt")
            } else {
                root.join("file.txt")
            };
            std::fs::write(&changed, b"external").expect("mutate after observe");
            assert!(matches!(
                backend.apply(edit, &preimage).await,
                Err(NativeTreeMaterializationError::ExternalMutation(path)) if path == "file.txt"
            ));
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_refuses_to_roll_back_over_an_external_write() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let target = operation.join("target");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&target).expect("target");
        std::fs::write(root.join("file.txt"), b"before").expect("before");
        std::fs::write(target.join("file.txt"), b"after").expect("after");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let plan = backend
            .plan_paths(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                ["file.txt".to_owned()],
            )
            .expect("plan");
        let edit = plan.edits.first().expect("install edit");
        let preimage = backend.capture(edit).await.expect("capture");
        backend.apply(edit, &preimage).await.expect("apply");
        std::fs::write(root.join("file.txt"), b"external").expect("external write");

        assert!(matches!(
            backend.restore(edit, &preimage).await,
            Err(NativeTreeMaterializationError::ExternalMutation(path)) if path == "file.txt"
        ));
        assert_eq!(
            std::fs::read(root.join("file.txt")).expect("external content survives"),
            b"external"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_journals_directory_metadata_without_replacing_children() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        std::fs::create_dir_all(root.join("directory")).expect("directory");
        std::fs::create_dir_all(operation.join("target")).expect("target");
        std::fs::write(root.join("directory/child.txt"), b"child").expect("child");
        let before = native_metadata(
            &std::fs::symlink_metadata(root.join("directory")).expect("before metadata"),
        );
        let mut desired = before.clone();
        desired.readonly = !before.readonly;
        #[cfg(windows)]
        {
            const FILE_ATTRIBUTE_HIDDEN: u32 = 0x2;
            desired.windows_attributes = before
                .windows_attributes
                .map(|attributes| attributes ^ FILE_ATTRIBUTE_HIDDEN);
            desired.readonly = desired
                .windows_attributes
                .is_some_and(|attributes| attributes & 1 != 0);
        }
        #[cfg(unix)]
        {
            desired.posix_mode = before.posix_mode.map(|mode| mode ^ 0o200);
        }
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let operation_id = OperationId::new();
        let plan = MaterializationPlan {
            operation_id,
            from: GenerationId::new(Digest::from_bytes([1; 32])),
            to: GenerationId::new(Digest::from_bytes([2; 32])),
            edits: vec![MaterializationEdit::SetMetadata {
                path: "directory".to_owned(),
                image: serde_json::to_vec(&desired).expect("metadata image"),
            }],
        };
        let materializer =
            JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), backend);
        materializer.apply(plan).await.expect("apply metadata");
        assert_eq!(
            std::fs::read(root.join("directory/child.txt")).expect("child remains"),
            b"child"
        );
        let applied = native_metadata(
            &std::fs::symlink_metadata(root.join("directory")).expect("applied metadata"),
        );
        assert_eq!(applied.readonly, desired.readonly);
        #[cfg(windows)]
        assert_eq!(applied.windows_attributes, desired.windows_attributes);
        #[cfg(unix)]
        assert_eq!(applied.posix_mode, desired.posix_mode);

        materializer
            .recover(operation_id, MaterializationRecovery::RollBack)
            .await
            .expect("rollback metadata");
        let restored = native_metadata(
            &std::fs::symlink_metadata(root.join("directory")).expect("restored metadata"),
        );
        assert_eq!(restored.readonly, before.readonly);
        #[cfg(windows)]
        assert_eq!(restored.windows_attributes, before.windows_attributes);
        #[cfg(unix)]
        assert_eq!(restored.posix_mode, before.posix_mode);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_recognizes_apply_before_progress_was_persisted() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let target = operation.join("target");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&target).expect("target");
        std::fs::write(root.join("file.txt"), b"before").expect("before");
        std::fs::write(target.join("file.txt"), b"after").expect("after");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let plan = backend
            .plan_paths(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                ["file.txt".to_owned()],
            )
            .expect("plan");
        let preimage = backend.capture(&plan.edits[0]).await.expect("capture");
        let store = MemoryMaterializationJournalStore::default();
        store
            .compare_and_swap(
                plan.operation_id,
                0,
                MaterializationJournal {
                    version: JOURNAL_VERSION,
                    revision: 1,
                    plan: plan.clone(),
                    preimages: vec![preimage.clone()],
                    phase: MaterializationPhase::Applying,
                    applied: 0,
                    restored: 0,
                },
            )
            .await
            .expect("journal");
        backend
            .apply(&plan.edits[0], &preimage)
            .await
            .expect("physical apply");

        let recovered = JournaledMaterializer::new(store, backend)
            .recover(plan.operation_id, MaterializationRecovery::Complete)
            .await
            .expect("recover")
            .expect("journal");
        assert_eq!(recovered.phase, MaterializationPhase::Applied);
        assert_eq!(
            std::fs::read(root.join("file.txt")).expect("after remains"),
            b"after"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_pauses_rollback_after_external_mutation() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let target = operation.join("target");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&target).expect("target");
        std::fs::write(root.join("file.txt"), b"before").expect("before");
        std::fs::write(target.join("file.txt"), b"after").expect("after");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let plan = backend
            .plan_paths(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                ["file.txt".to_owned()],
            )
            .expect("plan");
        let operation_id = plan.operation_id;
        let materializer =
            JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), backend);
        materializer.apply(plan).await.expect("apply");
        std::fs::write(root.join("file.txt"), b"external").expect("external mutation");

        assert!(matches!(
            materializer
                .recover(operation_id, MaterializationRecovery::RollBack)
                .await,
            Err(MaterializationError::ExternalMutation)
        ));
        assert_eq!(
            std::fs::read(root.join("file.txt")).expect("external remains"),
            b"external"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn native_tree_backend_preserves_dangling_symlinks_on_rollback() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let target = operation.join("target");
        std::fs::create_dir_all(&target).expect("target directory");
        symlink("missing-target", root.join("link")).expect("dangling symlink");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let plan = backend
            .plan(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                &[],
            )
            .expect("plan");
        let operation_id = plan.operation_id;
        let materializer =
            JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), backend);
        materializer.apply(plan).await.expect("remove link");
        assert!(std::fs::symlink_metadata(root.join("link")).is_err());
        materializer
            .recover(operation_id, MaterializationRecovery::RollBack)
            .await
            .expect("restore link");
        assert_eq!(
            std::fs::read_link(root.join("link")).expect("restored symlink"),
            PathBuf::from("missing-target")
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_tree_backend_rejects_operation_directory_inside_checkout() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = root.join("operation");
        let sentinel = operation.join("target/sentinel");
        std::fs::create_dir_all(sentinel.parent().expect("sentinel parent"))
            .expect("target directory");
        std::fs::write(&sentinel, b"keep").expect("sentinel");

        assert!(matches!(
            NativeTreeMaterializationBackend::new(&root, &operation),
            Err(NativeTreeMaterializationError::InvalidLayout)
        ));
        assert_eq!(std::fs::read(sentinel).expect("sentinel remains"), b"keep");
    }
}
