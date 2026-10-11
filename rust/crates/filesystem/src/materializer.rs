//! Journaled pathwise application and crash recovery.
//!
//! Merge publication changes an authenticated workspace head atomically. A
//! native checkout still needs pathwise host operations, which cannot be made
//! atomic by an operating system. This module centralizes the durable protocol:
//! capture every preimage before the first mutation, record progress after each
//! operation, and complete or roll back deterministically after interruption.

use crate::record_store::{MAXIMUM_CAS_ATTEMPTS, MemoryRecords, next_revision};
use crate::{GenerationId, OperationId};
use serde::{Deserialize, Serialize};
use std::future::Future;
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};
#[cfg(not(target_arch = "wasm32"))]
use std::{collections::BTreeMap, sync::Arc};
#[cfg(not(target_arch = "wasm32"))]
use crate::{kernel::NamespacePath, native_host::HostRoot};
#[cfg(not(target_arch = "wasm32"))]
#[path = "materializer_native.rs"]
mod native_entry;
use thiserror::Error;

const JOURNAL_VERSION: u32 = 1;

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
#[cfg_attr(feature = "napi-types", napi_derive::napi(string_enum))]
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
#[derive(Debug, Error, strum::IntoStaticStr)]
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
        // An interrupted forward pass may have applied the edit after the
        // last durable one before recording it. Include that edit; the
        // observation below skips it when it is still at its preimage.
        if journal.phase == MaterializationPhase::Applying
            && usize::try_from(journal.applied)
                .is_ok_and(|applied| applied < journal.plan.edits.len())
        {
            journal.applied = journal.applied.saturating_add(1);
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
        let span = crate::obs::span!(
            DEBUG,
            "acyclic.fs.materializer.persist",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty
        );
        crate::obs::outcome_on(
            &span,
            crate::obs::in_span(&span, async move {
                for _ in 0..MAXIMUM_CAS_ATTEMPTS {
                    let expected = next_revision(&mut journal.revision);
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
            })
            .await,
        )
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
    crate::path::is_canonical_relative(path)
        .then_some(())
        .ok_or(())
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
#[derive(Clone)]
pub struct NativeTreeMaterializationBackend {
    root_path: Arc<PathBuf>,
    root: Arc<HostRoot>,
    target: Arc<HostRoot>,
    backup: Arc<HostRoot>,
    captured: Option<Arc<CapturedNativeSource>>,
}

#[cfg(not(target_arch = "wasm32"))]
struct CapturedNativeSource {
    generation: GenerationId,
    preimages: Arc<BTreeMap<NamespacePath, MaterializationPreimage>>,
    config: crate::model::VolumeConfig,
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
        let root_path = root.into().canonicalize()?;
        let root = Arc::new(HostRoot::open(&root_path)?);
        Self::with_root(root_path, root, operation_directory.into())
    }

    #[cfg(any(all(feature = "local", feature = "native-mount"), test))]
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
                if native_entry_exists_in(&self.target, Path::new(&path))? {
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

    /// Uses the producer's original held root and immutable native preimages.
    pub(crate) fn from_captured_root(
        root_path: &Path,
        root: Arc<HostRoot>,
        generation: GenerationId,
        preimages: Arc<BTreeMap<NamespacePath, MaterializationPreimage>>,
        config: crate::model::VolumeConfig,
        operation_directory: &Path,
    ) -> Result<Self, NativeTreeMaterializationError> {
        let mut backend = Self::with_root(root_path.to_path_buf(), root, operation_directory.to_path_buf())?;
        backend.captured = Some(Arc::new(CapturedNativeSource { generation, preimages, config }));
        Ok(backend)
    }

    fn with_root(
        root_path: PathBuf,
        root: Arc<HostRoot>,
        operation_directory: PathBuf,
    ) -> Result<Self, NativeTreeMaterializationError> {
        if HostRoot::open(&root_path)?.identity() != root.identity() {
            return Err(NativeTreeMaterializationError::ExternalMutation(String::new()));
        }
        let operation_directory = operation_directory.canonicalize()?;
        if operation_directory == root_path || root_path.starts_with(&operation_directory) || operation_directory.starts_with(&root_path) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let operation = HostRoot::open(&operation_directory)?;
        operation.create_dir_all_held(Path::new("backup"))?;
        let target = Arc::new(HostRoot::open(&operation_directory.join("target"))?);
        let backup = Arc::new(HostRoot::open(&operation_directory.join("backup"))?);
        if root.identity().device != target.identity().device || root.identity().device != backup.identity().device {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        if HostRoot::open(&operation_directory)?.identity() != operation.identity() {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        Ok(Self { root_path: Arc::new(root_path), root, target, backup, captured: None })
    }

    fn verify_root_binding(&self) -> Result<(), NativeTreeMaterializationError> {
        if HostRoot::open(&self.root_path)?.identity() != self.root.identity() {
            return Err(NativeTreeMaterializationError::ExternalMutation(String::new()));
        }
        Ok(())
    }

    fn verify_parent_bindings(&self, path: &Path) -> Result<(), NativeTreeMaterializationError> {
        let Some(source) = &self.captured else { return Ok(()); };
        for parent in path.ancestors().skip(1).filter(|parent| !parent.as_os_str().is_empty()) {
            let original = source.original(parent)?.ok_or_else(|| native_external_mutation(parent))?;
            let metadata = decode_native_preimage_metadata(&original.image)?.ok_or_else(|| native_external_mutation(parent))?;
            let (before, _) = decode_native_witness(&original.image)?;
            // Parent mode may legitimately change without changing its binding.
            if native_entry_fingerprint_scoped(&self.root, parent, false, Some(&metadata))? != before {
                return Err(native_external_mutation(parent));
            }
        }
        Ok(())
    }

    fn verify_held_parent_binding(&self, path: &Path, parent: &cap_std::fs::Dir) -> Result<(), NativeTreeMaterializationError> {
        let parent_path = path.parent().unwrap_or_else(|| Path::new(""));
        let observed = parent.dir_metadata()?;
        if parent_path.as_os_str().is_empty() {
            if crate::NativeRootIdentity::from_metadata(&observed)? != self.root.identity() {
                return Err(native_external_mutation(path));
            }
        } else if let Some(source) = &self.captured {
            let original = source.original(parent_path)?.ok_or_else(|| native_external_mutation(parent_path))?;
            let metadata = decode_native_preimage_metadata(&original.image)?.ok_or_else(|| native_external_mutation(parent_path))?;
            let (expected, _) = decode_native_witness(&original.image)?;
            if native_metadata_fingerprint(&observed, Some(&metadata))? != expected.ok_or(NativeTreeMaterializationError::InvalidPreimage)? {
                return Err(native_external_mutation(parent_path));
            }
        }
        Ok(())
    }

    fn apply_metadata(&self, path: &Path, desired: &NativeMetadataImage, expected: Option<NativeFingerprint>) -> Result<(), NativeTreeMaterializationError> {
        let entry = native_entry::Entry::open(&self.root, path)?;
        self.verify_root_binding()?;
        self.verify_parent_bindings(path)?;
        self.verify_held_parent_binding(path, &entry.parent)?;
        let metadata = entry.metadata()?.ok_or_else(|| native_external_mutation(path))?;
        if Some(native_metadata_fingerprint(&metadata, None)?) != expected {
            return Err(native_external_mutation(path));
        }
        let previous = native_metadata_held(&metadata);
        entry.set_metadata(desired.readonly, desired.posix_mode, desired.windows_attributes)?;
        let binding = self.verify_root_binding()
            .and_then(|()| self.verify_parent_bindings(path))
            .and_then(|()| self.verify_held_parent_binding(path, &entry.parent));
        if let Err(error) = binding {
            // Undo only our metadata on this exact held entry, never a replacement or later edit.
            if let Some(current) = entry.metadata()?
                && native_metadata_fingerprint(&current, None)? == native_metadata_fingerprint(&metadata, Some(desired))?
            {
                entry.set_metadata(previous.readonly, previous.posix_mode, previous.windows_attributes)?;
            }
            return Err(error);
        }
        Ok(())
    }

    fn verify_original_entry(&self, path: &Path, descendants: bool) -> Result<(), NativeTreeMaterializationError> {
        self.verify_original_entry_in(&self.root, path, descendants)
    }

    fn verify_original_entry_in(&self, root: &HostRoot, path: &Path, descendants: bool) -> Result<(), NativeTreeMaterializationError> {
        let Some(source) = &self.captured else { return Ok(()); };
        let Some(original) = source.original(path)? else {
            return if native_entry_exists_in(root, path)? { Err(native_external_mutation(path)) } else { Ok(()) };
        };
        let metadata = decode_native_preimage_metadata(&original.image)?;
        let (before, _) = decode_native_witness(&original.image)?;
        if native_entry_fingerprint_scoped(root, path, metadata.is_none(), None)? != before {
            return Err(native_external_mutation(path));
        }
        if metadata.is_some() && descendants {
            let directory = root.open_dir_held(path)?;
            let names = directory.entries()?.map(|entry| entry.map(|entry| entry.file_name())).collect::<Result<Vec<_>, _>>()?;
            let namespace = source.namespace(path)?;
            let expected_children = source.preimages
                .range((std::ops::Bound::Included(&namespace), std::ops::Bound::Unbounded))
                .take_while(|(candidate, _)| candidate.is_within(&namespace))
                .filter(|(candidate, _)| candidate.depth() == namespace.depth() + 1)
                .count();
            if names.len() != expected_children { return Err(native_external_mutation(path)); }
            for name in names {
                let child = path.join(name);
                // Unknown children are user data, not part of this structural replacement.
                if source.original(&child)?.is_none() { return Err(native_external_mutation(&child)); }
                self.verify_original_entry_in(root, &child, true)?;
            }
        }
        Ok(())
    }

    fn original_before_for_edit(&self, path: &Path, edit: &MaterializationEdit, current: Option<NativeFingerprint>) -> Result<Option<NativeFingerprint>, NativeTreeMaterializationError> {
        let Some(source) = &self.captured else { return Ok(current); };
        let Some(original) = source.original(path)? else { return Ok(None); };
        let metadata_only = decode_native_preimage_metadata(&original.image)?.is_some();
        if !metadata_only || matches!(edit, MaterializationEdit::SetMetadata { .. }) {
            return Ok(decode_native_witness(&original.image)?.0);
        }
        // A structural directory journal carries its complete physical image. Its
        // descendants remain independently bound to the original retained preimages,
        // including after displacement into backup, rather than being adopted here.
        Ok(current)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl CapturedNativeSource {
    fn namespace(&self, path: &Path) -> Result<NamespacePath, NativeTreeMaterializationError> {
        let path = path.to_str().ok_or_else(|| NativeTreeMaterializationError::InvalidPath("<non-Unicode>".to_owned()))?;
        let portable = crate::path::PortablePath::parse(&format!("/{path}"), self.config.limits)
            .map_err(|_| NativeTreeMaterializationError::InvalidPath(path.to_owned()))?;
        NamespacePath::from_portable_in_profile(&portable, self.config.profile, self.config.limits)
            .map_err(|_| NativeTreeMaterializationError::InvalidPath(path.to_owned()))
    }

    fn original(&self, path: &Path) -> Result<Option<&MaterializationPreimage>, NativeTreeMaterializationError> {
        Ok(self.preimages.get(&self.namespace(path)?))
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn namespace_materialization_path(path: &NamespacePath) -> Result<PathBuf, NativeTreeMaterializationError> {
    let mut host = PathBuf::new();
    for component in path.components() {
        let text = component.unicode_text().ok_or_else(|| NativeTreeMaterializationError::InvalidPath("<non-Unicode>".to_owned()))?;
        host.push(text.as_ref());
    }
    Ok(host)
}

#[cfg(not(target_arch = "wasm32"))]
fn native_external_mutation(path: &Path) -> NativeTreeMaterializationError {
    NativeTreeMaterializationError::ExternalMutation(path.to_string_lossy().into_owned())
}

#[cfg(not(target_arch = "wasm32"))]
fn native_entry_exists_in(root: &HostRoot, path: &Path) -> Result<bool, std::io::Error> {
    match root.symlink_metadata_held(path) {
        Ok(_) => Ok(true),
        Err(error) if matches!(error.kind(), std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Retains original physical identity and content before any conditional host application.
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub(crate) fn capture_native_preimages(
    root: &HostRoot,
    paths: &[NamespacePath],
) -> Result<BTreeMap<NamespacePath, MaterializationPreimage>, NativeTreeMaterializationError> {
    paths.iter().map(|namespace| {
        let path = crate::native_capture::namespace_to_host_path(namespace)
            .map_err(|_| NativeTreeMaterializationError::InvalidPath("<unrepresentable>".to_owned()))?;
        let metadata = root.symlink_metadata_held(&path)?;
        let directory = metadata.is_dir();
        let before = native_entry_fingerprint_scoped(root, &path, !directory, None)?;
        let metadata = directory.then(|| native_metadata_held(&metadata));
        Ok((namespace.clone(), MaterializationPreimage { image: encode_native_witness(before, None, metadata)? }))
    }).collect()
}

#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
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

#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
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

#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
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

    async fn verify_source(&self, from: GenerationId) -> Result<bool, Self::Error> {
        let backend = self.clone();
        acyclic_native_runtime::run_blocking_io(move || {
            backend.verify_root_binding()?;
            Ok(backend.captured.as_ref().is_none_or(|source| source.generation == from))
        }).await?
    }

    async fn capture(&self, edit: &MaterializationEdit) -> Result<MaterializationPreimage, Self::Error> {
        let backend = self.clone();
        let edit = edit.clone();
        acyclic_native_runtime::run_blocking_io(move || {
            backend.verify_root_binding()?;
            let path = Path::new(edit_path(&edit));
            backend.verify_parent_bindings(path)?;
            backend.verify_original_entry(path, !matches!(edit, MaterializationEdit::SetMetadata { .. }))?;
            if native_entry_exists_in(&backend.backup, path)? {
                return Err(NativeTreeMaterializationError::UnexpectedBackup(edit_path(&edit).to_owned()));
            }
            let current = native_entry_fingerprint_for_edit(&backend.root, path, &edit)?;
            let before = backend.original_before_for_edit(path, &edit, current)?;
            let after = match &edit {
                MaterializationEdit::Install { .. } => Some(native_entry_fingerprint(&backend.target, path)?
                    .ok_or_else(|| NativeTreeMaterializationError::MissingTarget(edit_path(&edit).to_owned()))?),
                MaterializationEdit::Remove { .. } => None,
                MaterializationEdit::SetMetadata { image, .. } => {
                    native_entry_fingerprint_scoped(&backend.root, path, false, Some(&decode_native_metadata(image)?))?
                }
                MaterializationEdit::Rename { .. } => return Err(NativeTreeMaterializationError::UnsupportedRename),
            };
            let metadata = if matches!(edit, MaterializationEdit::SetMetadata { .. }) {
                Some(native_metadata_held(&backend.root.symlink_metadata_held(path)?))
            } else { None };
            backend.verify_root_binding()?;
            Ok(MaterializationPreimage { image: encode_native_witness(before, after, metadata)? })
        }).await?
    }

    async fn observe(&self, edit: &MaterializationEdit, preimage: &MaterializationPreimage) -> Result<MaterializationObservation, Self::Error> {
        let backend = self.clone();
        let edit = edit.clone();
        let preimage = preimage.clone();
        acyclic_native_runtime::run_blocking_io(move || {
            backend.verify_root_binding()?;
            let path = Path::new(edit_path(&edit));
            backend.verify_parent_bindings(path)?;
            let (before, after) = decode_native_witness(&preimage.image)?;
            let current = native_entry_fingerprint_for_edit(&backend.root, path, &edit)?;
            let target = if matches!(edit, MaterializationEdit::Install { .. }) { native_entry_fingerprint(&backend.target, path)? } else { None };
            let backup = native_entry_fingerprint(&backend.backup, path)?;
            backend.verify_root_binding()?;
            if current == before && current == after && target.is_none() && backup.is_some() {
                return Ok(MaterializationObservation::Postimage);
            }
            if current == before {
                return Ok(if matches!(edit, MaterializationEdit::Install { .. }) && target != after {
                    MaterializationObservation::Diverged
                } else { MaterializationObservation::Preimage });
            }
            if current == after {
                return Ok(if target.is_some() && target != after {
                    MaterializationObservation::Diverged
                } else { MaterializationObservation::Postimage });
            }
            if current.is_none() && backup == before && matches!(edit, MaterializationEdit::Install { .. }) {
                return Ok(if target.is_none() || target == after { MaterializationObservation::Interrupted } else { MaterializationObservation::Diverged });
            }
            Ok(MaterializationObservation::Diverged)
        }).await?
    }

    async fn apply(&self, edit: &MaterializationEdit, preimage: &MaterializationPreimage) -> Result<(), Self::Error> {
        let backend = self.clone();
        let edit = edit.clone();
        let preimage = preimage.clone();
        acyclic_native_runtime::run_blocking_io(move || {
            backend.verify_root_binding()?;
            let path = Path::new(edit_path(&edit));
            backend.verify_parent_bindings(path)?;
            let (before, after) = decode_native_witness(&preimage.image)?;
            if let MaterializationEdit::SetMetadata { image, .. } = &edit {
                let current = native_entry_fingerprint_for_edit(&backend.root, path, &edit)?;
                if current == after { return Ok(()); }
                if current != before { return Err(native_external_mutation(path)); }
                backend.apply_metadata(path, &decode_native_metadata(image)?, before)?;
                backend.verify_root_binding()?;
                if native_entry_fingerprint_for_edit(&backend.root, path, &edit)? != after {
                    return Err(native_external_mutation(path));
                }
                return Ok(());
            }
            let target_expected = matches!(edit, MaterializationEdit::Install { .. });
            let current = native_entry_fingerprint(&backend.root, path)?;
            let target = native_entry_fingerprint(&backend.target, path)?;
            let backup = native_entry_fingerprint(&backend.backup, path)?;
            if current == after && target.is_none() { return Ok(()); }
            if (backup.is_some() && backup != before) || (backup.is_none() && current != before)
                || (backup.is_some() && current.is_some()) || (target_expected && target != after)
            {
                return Err(native_external_mutation(path));
            }
            if backup.is_none() { backend.verify_original_entry(path, true)?; }
            backend.backup.create_dir_all_held(path.parent().unwrap_or_else(|| Path::new("")))?;
            backend.root.create_dir_all_held(path.parent().unwrap_or_else(|| Path::new("")))?;
            // Keep this exact parent capability through both renames. Reopening
            // the live path here would admit a replaced parent between steps.
            let live = native_entry::Entry::open(&backend.root, path)?;
            let saved = native_entry::Entry::open(&backend.backup, path)?;
            backend.verify_held_parent_binding(path, &live.parent)?;
            if backup.is_none() && current.is_some() {
                backend.verify_root_binding()?;
                backend.verify_parent_bindings(path)?;
                backend.verify_held_parent_binding(path, &live.parent)?;
                live.rename_to(&saved)?;
                let original = backend.verify_original_entry_in(&backend.backup, path, true);
                let binding = backend.verify_root_binding().and_then(|()| backend.verify_parent_bindings(path));
                if native_entry_fingerprint(&backend.backup, path)? != before || original.is_err() || binding.is_err() {
                    if !live.is_present()? { saved.rename_to(&live)?; }
                    return Err(native_external_mutation(path));
                }
            }
            if target_expected {
                if target.is_none() { return Err(NativeTreeMaterializationError::MissingTarget(edit_path(&edit).to_owned())); }
                let staged = native_entry::Entry::open(&backend.target, path)?;
                backend.verify_root_binding()?;
                backend.verify_parent_bindings(path)?;
                backend.verify_held_parent_binding(path, &live.parent)?;
                if let Err(error) = staged.rename_to(&live) {
                    if saved.is_present()? && !live.is_present()? { saved.rename_to(&live)?; }
                    return Err(error.into());
                }
            }
            let binding = backend.verify_root_binding()
                .and_then(|()| backend.verify_parent_bindings(path))
                .and_then(|()| backend.verify_held_parent_binding(path, &live.parent));
            if let Err(error) = binding {
                // A root/parent move during commit is not a successful publication.
                // Recover through the same original held parent, without following its new name.
                if after.is_some() {
                    let installed = live.repin()?;
                    if native_held_entry_fingerprint(&installed)? != after { return Err(native_external_mutation(path)); }
                    let staged = native_entry::Entry::open(&backend.target, path)?;
                    installed.rename_to(&staged)?;
                    if native_entry_fingerprint(&backend.target, path)? != after {
                        if !live.is_present()? { staged.rename_to(&live)?; }
                        return Err(native_external_mutation(path));
                    }
                }
                if before.is_some() && saved.is_present()? && !live.is_present()? { saved.rename_to(&live)?; }
                return Err(error);
            }
            if native_held_entry_fingerprint(&live.repin()?)? != after { return Err(native_external_mutation(path)); }
            Ok(())
        }).await?
    }

    async fn restore(&self, edit: &MaterializationEdit, preimage: &MaterializationPreimage) -> Result<(), Self::Error> {
        let backend = self.clone();
        let edit = edit.clone();
        let preimage = preimage.clone();
        acyclic_native_runtime::run_blocking_io(move || {
            backend.verify_root_binding()?;
            let path = Path::new(edit_path(&edit));
            backend.verify_parent_bindings(path)?;
            let (before, after) = decode_native_witness(&preimage.image)?;
            let current = native_entry_fingerprint_for_edit(&backend.root, path, &edit)?;
            if matches!(edit, MaterializationEdit::SetMetadata { .. }) {
                if current == before { return Ok(()); }
                if current != after { return Err(native_external_mutation(path)); }
                let metadata = decode_native_preimage_metadata(&preimage.image)?.ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
                backend.apply_metadata(path, &metadata, after)?;
                backend.verify_root_binding()?;
                return Ok(());
            }
            let backup = native_entry_fingerprint(&backend.backup, path)?;
            if backup.is_some() && backup != before { return Err(native_external_mutation(path)); }
            if current == before && backup.is_none() { return Ok(()); }
            if current != after && !(current.is_none() && backup.is_some()) { return Err(native_external_mutation(path)); }
            backend.target.create_dir_all_held(path.parent().unwrap_or_else(|| Path::new("")))?;
            let live = native_entry::Entry::open(&backend.root, path)?;
            let staged = native_entry::Entry::open(&backend.target, path)?;
            let saved = native_entry::Entry::open(&backend.backup, path)?;
            backend.verify_held_parent_binding(path, &live.parent)?;
            if current.is_some() {
                // Carry rather than delete; retain this same original parent through rollback.
                live.rename_to(&staged)?;
                let binding = backend.verify_root_binding().and_then(|()| backend.verify_parent_bindings(path));
                if native_entry_fingerprint(&backend.target, path)? != after || binding.is_err() {
                    if !live.is_present()? { staged.rename_to(&live)?; }
                    return Err(native_external_mutation(path));
                }
            }
            match before {
                None => {},
                Some(_) if backup.is_some() => {
                    backend.verify_root_binding()?;
                    backend.verify_parent_bindings(path)?;
                    backend.verify_held_parent_binding(path, &live.parent)?;
                    saved.rename_to(&live)?;
                }
                Some(_) => return Err(NativeTreeMaterializationError::MissingPreimage(edit_path(&edit).to_owned())),
            }
            let binding = backend.verify_root_binding()
                .and_then(|()| backend.verify_parent_bindings(path))
                .and_then(|()| backend.verify_held_parent_binding(path, &live.parent));
            if let Err(error) = binding {
                if before.is_some() {
                    let restored = live.repin()?;
                    if native_held_entry_fingerprint(&restored)? != before { return Err(native_external_mutation(path)); }
                    restored.rename_to(&saved)?;
                    if native_entry_fingerprint(&backend.backup, path)? != before {
                        if !live.is_present()? { saved.rename_to(&live)?; }
                        return Err(native_external_mutation(path));
                    }
                }
                if after.is_some() && staged.is_present()? && !live.is_present()? { staged.rename_to(&live)?; }
                return Err(error);
            }
            if native_held_entry_fingerprint(&live.repin()?)? != before { return Err(native_external_mutation(path)); }
            Ok(())
        }).await?
    }
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
fn native_entry_fingerprint(root: &HostRoot, path: &Path) -> Result<Option<[u8; 32]>, std::io::Error> {
    native_entry_fingerprint_scoped(root, path, true, None)
}

#[cfg(not(target_arch = "wasm32"))]
fn native_entry_fingerprint_for_edit(root: &HostRoot, path: &Path, edit: &MaterializationEdit) -> Result<Option<[u8; 32]>, std::io::Error> {
    native_entry_fingerprint_scoped(root, path, !matches!(edit, MaterializationEdit::SetMetadata { .. }), None)
}

#[cfg(not(target_arch = "wasm32"))]
fn native_entry_fingerprint_scoped(root: &HostRoot, path: &Path, include_contents: bool, override_metadata: Option<&NativeMetadataImage>) -> Result<Option<[u8; 32]>, std::io::Error> {
    let metadata = match root.symlink_metadata_held(path) {
        Ok(metadata) => metadata,
        Err(error) if matches!(error.kind(), std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory) => return Ok(None),
        Err(error) => return Err(error),
    };
    if !include_contents {
        return Ok(Some(native_metadata_fingerprint(&metadata, override_metadata)?));
    }
    let entry = native_entry::Entry::open(root, path)?;
    native_held_entry_fingerprint(&entry)
}

#[cfg(not(target_arch = "wasm32"))]
fn native_held_entry_fingerprint(entry: &native_entry::Entry<'_>) -> Result<Option<NativeFingerprint>, std::io::Error> {
    let Some(metadata) = entry.metadata()? else { return Ok(None); };
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    hash_native_entry(&entry.parent, entry.name, &metadata, &mut hasher, None, &mut buffer)?;
    Ok(Some(*hasher.finalize().as_bytes()))
}

#[cfg(not(target_arch = "wasm32"))]
fn native_metadata_fingerprint(metadata: &cap_std::fs::Metadata, desired: Option<&NativeMetadataImage>) -> Result<NativeFingerprint, std::io::Error> {
    let mut hasher = blake3::Hasher::new();
    hash_native_metadata_edit(metadata, desired, &mut hasher);
    #[cfg(windows)]
    if metadata.is_dir() || metadata.is_file() {
        hasher.update(&crate::NativeRootIdentity::from_metadata(metadata)?.to_bytes());
    }
    Ok(*hasher.finalize().as_bytes())
}

#[cfg(not(target_arch = "wasm32"))]
fn hash_native_metadata_edit(
    metadata: &cap_std::fs::Metadata,
    desired: Option<&NativeMetadataImage>,
    hasher: &mut blake3::Hasher,
) {
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
    #[cfg(unix)]
    {
        use cap_std::fs::MetadataExt as _;
        for value in [metadata.dev(), metadata.ino()] {
            hasher.update(&value.to_le_bytes());
        }
        hasher.update(
            &u64::from(
                desired
                    .and_then(|value| value.posix_mode)
                    .unwrap_or_else(|| metadata.mode()),
            )
            .to_le_bytes(),
        );
    }
    #[cfg(windows)]
    {
        use cap_std::fs::MetadataExt as _;
        let attributes = desired
            .and_then(|value| value.windows_attributes)
            .unwrap_or_else(|| {
                let attributes = metadata.file_attributes();
                match desired {
                    Some(value) if value.readonly => attributes | 1,
                    Some(_) => attributes & !1,
                    None => attributes,
                }
            });
        hasher.update(&attributes.to_le_bytes());
    }
    #[cfg(not(any(unix, windows)))]
    hasher.update(&[u8::from(desired.map_or_else(
        || metadata.permissions().readonly(),
        |value| value.readonly,
    ))]);
}

#[cfg(not(target_arch = "wasm32"))]
fn hash_native_entry(
    parent: &cap_std::fs::Dir,
    name: &std::ffi::OsStr,
    metadata: &cap_std::fs::Metadata,
    hasher: &mut blake3::Hasher,
    override_metadata: Option<&NativeMetadataImage>,
    buffer: &mut [u8],
) -> Result<(), std::io::Error> {
    use cap_fs_ext::{DirExt as _, OpenOptionsFollowExt as _};
    use std::io::Read as _;
    let file_type = metadata.file_type();
    hasher.update(if file_type.is_symlink() { b"link" } else if file_type.is_dir() { b"directory" } else if file_type.is_file() { b"file" } else { b"special" });
    hasher.update(&metadata.len().to_le_bytes());
    hasher.update(&[u8::from(override_metadata.map_or_else(|| metadata.permissions().readonly(), |value| value.readonly))]);
    hash_native_metadata(metadata, override_metadata, true, hasher);
    if file_type.is_symlink() {
        hash_native_os_str(parent.read_link_contents(name)?.as_os_str(), hasher);
    } else if file_type.is_dir() {
        let directory = parent.open_dir_nofollow(name)?;
        ensure_native_metadata_stable(metadata, &directory.dir_metadata()?)?;
        let mut names = directory.entries()?.map(|entry| entry.map(|entry| entry.file_name())).collect::<Result<Vec<_>, _>>()?;
        names.sort_unstable();
        for name in names {
            hash_native_os_str(&name, hasher);
            let metadata = directory.symlink_metadata(&name)?;
            hash_native_entry(&directory, &name, &metadata, hasher, None, buffer)?;
        }
        ensure_native_metadata_stable(metadata, &directory.dir_metadata()?)?;
    } else if file_type.is_file() {
        let mut options = cap_std::fs::OpenOptions::new();
        options.read(true).follow(cap_primitives::fs::FollowSymlinks::No);
        #[cfg(target_os = "linux")]
        {
            use cap_std::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_NOATIME);
        }
        let mut file = match parent.open_with(name, &options) {
            #[cfg(target_os = "linux")]
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                use cap_std::fs::OpenOptionsExt as _;
                options.custom_flags(0);
                parent.open_with(name, &options)?
            }
            result => result?,
        };
        ensure_native_metadata_stable(metadata, &file.metadata()?)?;
        loop {
            let read = file.read(buffer)?;
            if read == 0 { break; }
            hasher.update(buffer.get(..read).ok_or_else(|| std::io::Error::other("invalid read length"))?);
        }
        ensure_native_metadata_stable(metadata, &file.metadata()?)?;
    }
    ensure_native_metadata_stable(metadata, &parent.symlink_metadata(name)?)
}

#[cfg(not(target_arch = "wasm32"))]
fn ensure_native_metadata_stable(before: &cap_std::fs::Metadata, after: &cap_std::fs::Metadata) -> Result<(), std::io::Error> {
    let mut old = blake3::Hasher::new();
    let mut new = blake3::Hasher::new();
    hash_native_metadata(before, None, true, &mut old);
    hash_native_metadata(after, None, true, &mut new);
    if before.file_type() != after.file_type() || before.len() != after.len()
        || before.permissions().readonly() != after.permissions().readonly()
        || crate::NativeRootIdentity::from_metadata(before)? != crate::NativeRootIdentity::from_metadata(after)?
        || old.finalize() != new.finalize()
    {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "entry changed during fingerprint capture"));
    }
    Ok(())
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
fn hash_native_metadata(
    metadata: &cap_std::fs::Metadata,
    desired: Option<&NativeMetadataImage>,
    include_contents: bool,
    hasher: &mut blake3::Hasher,
) {
    use cap_std::fs::MetadataExt as _;
    for value in [
        metadata.dev(),
        metadata.ino(),
        u64::from(
            desired
                .and_then(|value| value.posix_mode)
                .unwrap_or_else(|| metadata.mode()),
        ),
    ] {
        hasher.update(&value.to_le_bytes());
    }
    if include_contents {
        for value in [
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
}

#[cfg(all(windows, not(target_arch = "wasm32")))]
fn hash_native_metadata(
    metadata: &cap_std::fs::Metadata,
    desired: Option<&NativeMetadataImage>,
    include_contents: bool,
    hasher: &mut blake3::Hasher,
) {
    use cap_std::fs::MetadataExt as _;
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
    hasher.update(&u64::from(attributes).to_le_bytes());
    if include_contents {
        for value in [metadata.last_write_time(), metadata.file_size()] {
            hasher.update(&value.to_le_bytes());
        }
    }
}

#[cfg(all(not(unix), not(windows), not(target_arch = "wasm32")))]
fn hash_native_metadata(
    _metadata: &cap_std::fs::Metadata,
    _desired: Option<&NativeMetadataImage>,
    _include_contents: bool,
    _hasher: &mut blake3::Hasher,
) {
}

#[cfg(not(target_arch = "wasm32"))]
fn native_metadata_held(metadata: &cap_std::fs::Metadata) -> NativeMetadataImage {
    NativeMetadataImage {
        readonly: metadata.permissions().readonly(),
        #[cfg(unix)]
        posix_mode: {
            use cap_std::fs::MetadataExt as _;
            Some(metadata.mode())
        },
        #[cfg(not(unix))]
        posix_mode: None,
        #[cfg(windows)]
        windows_attributes: {
            use cap_std::fs::MetadataExt as _;
            Some(metadata.file_attributes())
        },
        #[cfg(not(windows))]
        windows_attributes: None,
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
fn native_metadata(metadata: &std::fs::Metadata) -> NativeMetadataImage {
    native_metadata_held(&cap_std::fs::Metadata::from_just_metadata(metadata.clone()))
}


#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
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

/// Native tree publication failure.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Error)]
pub enum NativeTreeMaterializationError {
    /// Root, target, or backup layout is not same-root and operation-scoped.
    #[error("native materialization layout is invalid")]
    InvalidLayout,
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
    publish_native_generation_transition_owned(from_generation, to_generation, state, publication, None).await
}

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
pub(crate) async fn publish_captured_native_generation_transition<A, O>(
    capture: &crate::native_archive::NativeDirectoryCapture,
    from_generation: &crate::Generation<A, O>,
    to_generation: &crate::Generation<A, O>,
    state: &crate::LocalCoreStateStore,
    publication: NativeWorkspacePublication<'_>,
) -> Result<MaterializationJournal, NativeWorkspacePublicationError>
where
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
{
    publish_native_generation_transition_owned(from_generation, to_generation, state, publication, Some(capture.clone())).await
}

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
#[allow(clippy::too_many_lines, reason = "one canonical planner stages and journals both ordinary and retained-capture publications")]
async fn publish_native_generation_transition_owned<A, O>(
    from_generation: &crate::Generation<A, O>,
    to_generation: &crate::Generation<A, O>,
    state: &crate::LocalCoreStateStore,
    publication: NativeWorkspacePublication<'_>,
    capture: Option<crate::native_archive::NativeDirectoryCapture>,
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
    if let Some(capture) = &capture
        && (capture.original_generation() != from || capture.root_path() != root
            || from_generation.workspace_id().volume_id() != capture.original_volume()
            || to_generation.workspace_id().volume_id() != capture.original_volume())
    {
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
            native_publication_backend(root, operation_directory, capture.as_ref())
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
    if let Some(capture) = &capture {
        for change in &changed.value {
            if change.path.components().is_empty() || capture.capture_policy().excludes(&change.path) {
                return Err(NativeTreeMaterializationError::InvalidPath(namespace_materialization_path(&change.path)?.to_string_lossy().into_owned()).into());
            }
        }
    }
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
        let promoted_directory = capture.is_none() && change.before.is_none()
            && after_directory
            && std::fs::symlink_metadata(root.join(&path))
                .is_ok_and(|metadata| metadata.file_type().is_dir());
        // Symmetrically, a directory the target drops that still holds host
        // entries the source generation never held is a source directory
        // read through the source: removing it would take those along, so
        // it stays and only what the generation held beneath it goes.
        if capture.is_none() && before_directory
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
        if capture.is_none() && install
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
        let backend = native_publication_backend(root, operation_directory, capture.as_ref())?;
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

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
fn native_publication_backend(
    root: PathBuf,
    operation_directory: PathBuf,
    capture: Option<&crate::native_archive::NativeDirectoryCapture>,
) -> Result<NativeTreeMaterializationBackend, NativeTreeMaterializationError> {
    match capture {
        Some(capture) => NativeTreeMaterializationBackend::from_captured_root(
            capture.root_path(),
            Arc::clone(capture.held_root()),
            capture.original_generation(),
            capture.retained_preimages(),
            capture.volume_config(),
            &operation_directory,
        ),
        None => NativeTreeMaterializationBackend::new(root, operation_directory),
    }
}

/// Largest host file compared against a generation before publication skips
/// rewriting it; larger files are always rewritten.
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
const MAXIMUM_PROMOTION_COMPARISON_BYTES: u64 = 64 * 1024 * 1024;

/// Whether the host directory at `path` holds an entry `generation` does
/// not have there.
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
async fn host_directory_holds_unknown_entries<A, O>(
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
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
async fn host_holds_generation_file<A, O>(
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

/// Process-local materialization journal adapter.
#[derive(Default)]
pub struct MemoryMaterializationJournalStore {
    journals: MemoryRecords<OperationId, MaterializationJournal>,
}

impl MaterializationJournalStore for MemoryMaterializationJournalStore {
    type Error = MemoryMaterializationJournalStoreError;

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MaterializationJournal>, Self::Error> {
        self.journals
            .load(&operation_id)
            .map_err(|_| MemoryMaterializationJournalStoreError)
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MaterializationJournal,
    ) -> Result<bool, Self::Error> {
        self.journals
            .compare_and_swap(operation_id, expected_revision, replacement)
            .map_err(|_| MemoryMaterializationJournalStoreError)
    }
}

/// Process-local materialization journal synchronization failure.
#[derive(Debug, Error)]
#[error("materialization memory store is unavailable")]
pub struct MemoryMaterializationJournalStoreError;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Digest;
    use proptest::collection::btree_map;
    use proptest::option;
    use proptest::prelude::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

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

    /// Flat path model whose writes, like every journal CAS, are durable
    /// steps; the step numbered `crash_at` takes effect and then "crashes".
    #[derive(Default)]
    struct Crashing {
        tree: Mutex<BTreeMap<String, u8>>,
        steps: AtomicU64,
        crash_at: AtomicU64,
    }

    impl Crashing {
        fn step(&self) -> Result<(), BackendError> {
            let step = self.steps.fetch_add(1, SeqCst) + 1;
            if step == self.crash_at.load(SeqCst) {
                return Err(BackendError);
            }
            Ok(())
        }

        fn tree(&self) -> BTreeMap<String, u8> {
            self.tree.lock().expect("tree lock").clone()
        }

        fn current(&self, path: &str) -> Result<Option<u8>, BackendError> {
            Ok(self
                .tree
                .lock()
                .map_err(|_| BackendError)?
                .get(path)
                .copied())
        }

        fn set(&self, path: &str, value: Option<u8>) -> Result<(), BackendError> {
            let mut tree = self.tree.lock().map_err(|_| BackendError)?;
            match value {
                Some(value) => tree.insert(path.to_owned(), value),
                None => tree.remove(path),
            };
            drop(tree);
            self.step()
        }
    }

    fn endpoints(edit: &MaterializationEdit) -> (&str, Option<u8>) {
        match edit {
            MaterializationEdit::Install { path, image } => (path, image.first().copied()),
            MaterializationEdit::Remove { path } => (path, None),
            _ => ("", None),
        }
    }

    impl MaterializationBackend for &Crashing {
        type Error = BackendError;

        async fn capture(
            &self,
            edit: &MaterializationEdit,
        ) -> Result<MaterializationPreimage, Self::Error> {
            let value = self.current(endpoints(edit).0)?;
            Ok(MaterializationPreimage {
                image: value.into_iter().collect(),
            })
        }

        async fn observe(
            &self,
            edit: &MaterializationEdit,
            preimage: &MaterializationPreimage,
        ) -> Result<MaterializationObservation, Self::Error> {
            let (path, postimage) = endpoints(edit);
            let current = self.current(path)?;
            Ok(if current == postimage {
                MaterializationObservation::Postimage
            } else if current == preimage.image.first().copied() {
                MaterializationObservation::Preimage
            } else {
                MaterializationObservation::Diverged
            })
        }

        async fn apply(
            &self,
            edit: &MaterializationEdit,
            _preimage: &MaterializationPreimage,
        ) -> Result<(), Self::Error> {
            let (path, postimage) = endpoints(edit);
            self.set(path, postimage)
        }

        async fn restore(
            &self,
            edit: &MaterializationEdit,
            preimage: &MaterializationPreimage,
        ) -> Result<(), Self::Error> {
            self.set(endpoints(edit).0, preimage.image.first().copied())
        }
    }

    /// Journal store whose compare-and-swaps are durable steps of `.1`.
    struct CrashingStore<'a>(&'a MemoryMaterializationJournalStore, &'a Crashing);

    impl MaterializationJournalStore for CrashingStore<'_> {
        type Error = BackendError;

        async fn load(
            &self,
            operation_id: OperationId,
        ) -> Result<Option<MaterializationJournal>, Self::Error> {
            self.0.load(operation_id).await.map_err(|_| BackendError)
        }

        async fn compare_and_swap(
            &self,
            operation_id: OperationId,
            expected_revision: u64,
            replacement: MaterializationJournal,
        ) -> Result<bool, Self::Error> {
            let swapped = self
                .0
                .compare_and_swap(operation_id, expected_revision, replacement)
                .await
                .map_err(|_| BackendError)?;
            self.1.step()?;
            Ok(swapped)
        }
    }

    /// Applies `plan` to `original`, crashing after durable step `crash_at`
    /// (zero never crashes).
    fn crashing_apply(
        plan: &MaterializationPlan,
        original: &BTreeMap<String, u8>,
        crash_at: u64,
    ) -> (Crashing, MemoryMaterializationJournalStore, bool) {
        let backend = Crashing::default();
        original.clone_into(&mut backend.tree.lock().expect("tree lock"));
        backend.crash_at.store(crash_at, SeqCst);
        let journals = MemoryMaterializationJournalStore::default();
        let crashed = futures::executor::block_on(
            JournaledMaterializer::new(CrashingStore(&journals, &backend), &backend)
                .apply(plan.clone()),
        )
        .is_err();
        (backend, journals, crashed)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// A crash after any durable step, followed by either recovery,
        /// leaves exactly the planned tree or exactly the original one.
        #[test]
        fn recovery_after_a_crash_at_any_durable_step_is_all_or_nothing(
            original in btree_map("[a-e]", any::<u8>(), 0..5),
            targets in btree_map("[a-e]", option::of(any::<u8>()), 1..5),
            crash in any::<u64>(),
            forward in any::<bool>(),
        ) {
            let plan = MaterializationPlan {
                operation_id: OperationId::from_bytes([7; 16]),
                from: GenerationId::new(Digest::from_bytes([1; 32])),
                to: GenerationId::new(Digest::from_bytes([2; 32])),
                edits: targets
                    .iter()
                    .map(|(path, value)| match value {
                        Some(value) => MaterializationEdit::Install {
                            path: path.clone(),
                            image: vec![*value],
                        },
                        None => MaterializationEdit::Remove { path: path.clone() },
                    })
                    .collect(),
            };
            let mut planned = original.clone();
            for (path, value) in &targets {
                match value {
                    Some(value) => planned.insert(path.clone(), *value),
                    None => planned.remove(path),
                };
            }
            let (clean, _, crashed) = crashing_apply(&plan, &original, 0);
            prop_assert!(!crashed);
            prop_assert_eq!(clean.tree(), planned.clone());
            let steps = clean.steps.load(SeqCst);

            let (backend, journals, crashed) = crashing_apply(&plan, &original, 1 + crash % steps);
            prop_assert!(crashed);
            backend.crash_at.store(0, SeqCst);
            let restarted = JournaledMaterializer::new(CrashingStore(&journals, &backend), &backend);
            let recovery = if forward {
                MaterializationRecovery::Complete
            } else {
                MaterializationRecovery::RollBack
            };
            let journal = futures::executor::block_on(restarted.recover(plan.operation_id, recovery))
                .expect("recover")
                .expect("journal");
            if forward {
                prop_assert_eq!(journal.phase, MaterializationPhase::Applied);
                prop_assert_eq!(backend.tree(), planned);
            } else {
                prop_assert_eq!(journal.phase, MaterializationPhase::RolledBack);
                prop_assert_eq!(backend.tree(), original);
            }
        }
    }

    #[tokio::test]
    async fn obsolete_journal_versions_reject_recovery_without_effects() {
        for version in [0, 2, u32::MAX] {
            let operation_id = OperationId::new();
            let journal = MaterializationJournal {
                version,
                revision: 1,
                plan: MaterializationPlan {
                    operation_id,
                    from: GenerationId::new(Digest::from_bytes([1; 32])),
                    to: GenerationId::new(Digest::from_bytes([2; 32])),
                    edits: vec![MaterializationEdit::Install {
                        path: "file".into(),
                        image: vec![9],
                    }],
                },
                preimages: vec![MaterializationPreimage { image: vec![0] }],
                phase: MaterializationPhase::Prepared,
                applied: 0,
                restored: 0,
            };
            let store = MemoryMaterializationJournalStore::default();
            store
                .compare_and_swap(operation_id, 0, journal.clone())
                .await
                .expect("seed obsolete journal");
            let materializer = JournaledMaterializer::new(store, Backend::default());
            assert!(matches!(
                materializer
                    .recover(operation_id, MaterializationRecovery::Complete)
                    .await,
                Err(MaterializationError::IncompatibleJournal)
            ));
            assert!(materializer.backend.0.lock().expect("backend").is_empty());
            assert_eq!(
                materializer
                    .store
                    .load(operation_id)
                    .await
                    .expect("retained journal"),
                Some(journal)
            );
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
    async fn native_tree_backend_applies_and_rolls_back_selected_paths() {
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
            .plan_paths(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                ["old.txt".to_owned(), "new.txt".to_owned()],
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
    async fn metadata_edit_preserves_concurrent_child_content() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        std::fs::create_dir_all(root.join("directory")).expect("checkout directory");
        std::fs::create_dir_all(operation.join("target")).expect("target directory");
        let child = root.join("directory/child.txt");
        std::fs::write(&child, b"before").expect("child file");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let mut desired = native_metadata(
            &std::fs::symlink_metadata(root.join("directory")).expect("directory metadata"),
        );
        #[cfg(unix)]
        {
            desired.posix_mode = desired.posix_mode.map(|mode| mode ^ 0o200);
        }
        #[cfg(windows)]
        {
            desired.windows_attributes =
                desired.windows_attributes.map(|attributes| attributes ^ 2);
        }
        let edit = MaterializationEdit::SetMetadata {
            path: "directory".to_owned(),
            image: serde_json::to_vec(&desired).expect("metadata image"),
        };
        let preimage = backend.capture(&edit).await.expect("capture metadata");
        std::fs::write(&child, b"concurrent child write").expect("update child");
        assert_eq!(
            backend.observe(&edit, &preimage).await.expect("observe"),
            MaterializationObservation::Preimage
        );
        backend
            .apply(&edit, &preimage)
            .await
            .expect("apply metadata");
        assert_eq!(
            std::fs::read(&child).expect("read child"),
            b"concurrent child write"
        );
        backend
            .restore(&edit, &preimage)
            .await
            .expect("restore metadata");
        assert_eq!(
            std::fs::read(&child).expect("read restored child"),
            b"concurrent child write"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn metadata_edit_rejects_a_replaced_directory() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let directory = root.join("directory");
        std::fs::create_dir_all(&directory).expect("checkout directory");
        std::fs::create_dir_all(operation.join("target")).expect("target directory");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let edit = MaterializationEdit::SetMetadata {
            path: "directory".to_owned(),
            image: serde_json::to_vec(&native_metadata(
                &std::fs::symlink_metadata(&directory).expect("directory metadata"),
            ))
            .expect("metadata image"),
        };
        let preimage = backend.capture(&edit).await.expect("capture metadata");
        std::fs::rename(&directory, root.join("displaced")).expect("replace binding");
        std::fs::create_dir(&directory).expect("replacement directory");
        assert_eq!(
            backend.observe(&edit, &preimage).await.expect("observe"),
            MaterializationObservation::Diverged
        );
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
        std::fs::create_dir_all(&root).expect("checkout directory");
        std::fs::create_dir_all(&target).expect("target directory");
        symlink("missing-target", root.join("link")).expect("dangling symlink");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let plan = backend
            .plan_paths(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                ["link".to_owned()],
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
