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

const JOURNAL_VERSION: u32 = 1;
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
    /// Ordered, non-overlapping path edits.
    pub edits: Vec<MaterializationEdit>,
}

/// Opaque complete preimage for one edit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPreimage {
    /// Backend-defined bytes sufficient to restore every representable fact.
    pub image: Vec<u8>,
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

    /// Applies one declarative edit idempotently.
    fn apply(
        &self,
        edit: &MaterializationEdit,
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
            self.backend
                .apply(edit)
                .await
                .map_err(MaterializationError::Backend)?;
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
            self.backend
                .restore(edit, preimage)
                .await
                .map_err(MaterializationError::Backend)?;
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
/// Callers first materialize the exact target generation into `target`. Each
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
            || !operation_directory.starts_with(&root)
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

    fn paths(&self, path: &str) -> (PathBuf, PathBuf, PathBuf) {
        (
            self.root.join(path),
            self.target.join(path),
            self.backup.join(path),
        )
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl MaterializationBackend for NativeTreeMaterializationBackend {
    type Error = NativeTreeMaterializationError;

    async fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> Result<MaterializationPreimage, Self::Error> {
        let path = edit_path(edit);
        let (live, _, backup) = self.paths(path);
        if native_entry_exists(&backup)? {
            return Ok(MaterializationPreimage { image: vec![1] });
        }
        Ok(MaterializationPreimage {
            image: vec![u8::from(native_entry_exists(&live)?)],
        })
    }

    async fn apply(&self, edit: &MaterializationEdit) -> Result<(), Self::Error> {
        let path = edit_path(edit);
        let (live, target, backup) = self.paths(path);
        let target_expected = matches!(edit, MaterializationEdit::Install { .. });
        let backup_exists = native_entry_exists(&backup)?;
        let target_exists = native_entry_exists(&target)?;
        let live_exists = native_entry_exists(&live)?;
        if backup_exists {
            if target_expected && target_exists {
                remove_native_entry(&live)?;
                std::fs::rename(target, live)?;
            } else if target_expected && !live_exists {
                return Err(NativeTreeMaterializationError::MissingTarget(
                    path.to_owned(),
                ));
            } else if !target_expected {
                remove_native_entry(&live)?;
            }
            return Ok(());
        }
        if target_expected && !target_exists && live_exists {
            return Ok(());
        }
        if live_exists {
            std::fs::rename(&live, &backup)?;
        }
        if target_expected {
            if !target_exists {
                return Err(NativeTreeMaterializationError::MissingTarget(
                    path.to_owned(),
                ));
            }
            std::fs::rename(target, live)?;
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
        match preimage.image.as_slice() {
            [0] => remove_native_entry(&live).map_err(Into::into),
            [1] if native_entry_exists(&backup)? => {
                remove_native_entry(&live)?;
                std::fs::rename(backup, live).map_err(Into::into)
            }
            [1] if native_entry_exists(&live)? => Ok(()),
            [1] => Err(NativeTreeMaterializationError::MissingPreimage(
                path.to_owned(),
            )),
            _ => Err(NativeTreeMaterializationError::InvalidPreimage),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn edit_path(edit: &MaterializationEdit) -> &str {
    match edit {
        MaterializationEdit::Install { path, .. } | MaterializationEdit::Remove { path } => path,
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
    /// A durable preimage references a missing backup binding.
    #[error("native materialization preimage is missing for '{0}'")]
    MissingPreimage(String),
    /// Persisted preimage bytes are malformed.
    #[error("native materialization preimage is invalid")]
    InvalidPreimage,
    /// Native filesystem operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Publishes one fully prepared native tree through the durable local state
/// store and removes its terminal journal after success.
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
    let plan = backend.plan(operation_id, from, to, excluded_names)?;
    let materializer = JournaledMaterializer::new(state.clone(), backend);
    let journal = materializer.apply(plan).await?;
    state.remove_materialization(operation_id)?;
    Ok(journal)
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

        async fn apply(&self, edit: &MaterializationEdit) -> Result<(), Self::Error> {
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
        let operation = root.join(".acyclic-sdk/materializations/op");
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
                &[".git", ".acyclic-sdk"],
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

    #[cfg(unix)]
    #[tokio::test]
    async fn native_tree_backend_preserves_dangling_symlinks_on_rollback() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = root.join(".acyclic-sdk/materializations/op");
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
                &[".acyclic-sdk"],
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
}
