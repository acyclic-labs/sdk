//! Conditional working-tree application from one original native capture.
//!
//! The retained capture supplies authority, original physical preimages, and
//! the immutable logical baseline. The existing materializer owns application,
//! journal CAS, and recovery; caller paths and caller-made receipts never do.

use crate::kernel::FileKind;
use crate::materializer::{
    JournaledMaterializer, MaterializationError, MaterializationJournal,
    MaterializationJournalStore, MaterializationPhase, MaterializationRecovery,
    NativeTreeMaterializationBackend, NativeTreeMaterializationError,
    NativeWorkspacePublication, NativeWorkspacePublicationError,
    namespace_materialization_path, publish_captured_native_generation_transition,
};
use crate::native_archive::{NativeArchiveError, NativeDirectoryCapture};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Generation,
    GenerationId, LocalCoreStateStore, LocalCoreStateStoreError, MaterializeOptions,
    OperationId, OperationReceipt, WorkBudget, WorkspaceError,
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

/// One exact changed path, without private host paths or physical identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeDirectoryApplyChange {
    /// Portable path relative to the original working-tree root.
    pub path: String,
    /// Original entry kind, or authenticated absence.
    pub before_kind: Option<FileKind>,
    /// Desired entry kind, or authenticated removal.
    pub after_kind: Option<FileKind>,
    /// Kind-specific content changed between the pinned generations.
    pub content_changed: bool,
    /// Authenticated metadata changed between the pinned generations.
    pub metadata_changed: bool,
    /// Namespace binding identity changed between the pinned generations.
    pub binding_changed: bool,
}

/// Read-only preview of an exact desired generation against the original capture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeDirectoryApplyPreview {
    /// Immutable generation committed by the original capture producer.
    pub from: GenerationId,
    /// Exact desired generation, not a mutable head observation.
    pub to: GenerationId,
    /// Every changed path, never a truncated preview.
    pub changes: Vec<NativeDirectoryApplyChange>,
}

/// Actual durable materialization outcome, with private host data redacted.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NativeDirectoryApplyReceipt {
    /// Original apply identity used by the canonical journal and cold recovery.
    pub operation_id: OperationId,
    /// Exact original logical generation.
    pub from: GenerationId,
    /// Exact requested logical generation.
    pub to: GenerationId,
    /// Observed durable journal phase.
    pub phase: MaterializationPhase,
    /// Number of declared host edits in this exact journal.
    pub changed_paths: u32,
}

/// Conditional native capture, preview, publication, or recovery failure.
#[derive(Debug, Error)]
pub enum NativeDirectoryApplyError {
    /// The original capture binding no longer exists at its original path.
    #[error(transparent)]
    Capture(#[from] NativeArchiveError),
    /// An input generation does not belong to the retained original capture.
    #[error("native apply generation does not match the original capture")]
    CaptureGenerationMismatch,
    /// Root metadata cannot be represented by the non-root archive format.
    #[error("native apply requires an explicit root metadata edit, which the archive format does not carry")]
    RootMetadataChange,
    /// A target path lies outside the original capture policy.
    #[error("native apply path is outside the original capture policy: '{0}'")]
    OutsideCaptureScope(String),
    /// Authenticated filesystem access failed.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// The canonical bounded publisher failed.
    #[error(transparent)]
    Publication(#[from] NativeWorkspacePublicationError),
    /// Durable journal access failed.
    #[error(transparent)]
    State(#[from] LocalCoreStateStoreError),
    /// Held native entry access failed.
    #[error(transparent)]
    Native(#[from] NativeTreeMaterializationError),
    /// Canonical journal application or recovery failed.
    #[error(transparent)]
    Materialization(#[from] MaterializationError<LocalCoreStateStoreError, NativeTreeMaterializationError>),
    /// The journal's declared path count cannot be represented exactly.
    #[error("native apply journal has too many paths")]
    PathCountOverflow,
    /// The original operation key was reused for a different captured baseline.
    #[error("native apply journal does not belong to the original capture")]
    JournalCaptureMismatch,
}

fn verify_generation<A, O>(capture: &NativeDirectoryCapture, generation: &Generation<A, O>) -> Result<(), NativeDirectoryApplyError>
where A: AsyncAuthorityStore, O: AsyncObjectStore,
{
    if generation.id() != capture.original_generation()
        || generation.workspace_id().volume_id() != capture.original_volume()
    {
        return Err(NativeDirectoryApplyError::CaptureGenerationMismatch);
    }
    Ok(())
}

fn verify_target<A, O>(capture: &NativeDirectoryCapture, generation: &Generation<A, O>) -> Result<(), NativeDirectoryApplyError>
where A: AsyncAuthorityStore, O: AsyncObjectStore,
{
    if generation.workspace_id().volume_id() != capture.original_volume() {
        return Err(NativeDirectoryApplyError::CaptureGenerationMismatch);
    }
    Ok(())
}

/// Previews every actual changed path without writing to the customer working tree.
pub async fn preview_native_directory_apply<A, O>(
    capture: &NativeDirectoryCapture,
    from: &Generation<A, O>,
    to: &Generation<A, O>,
    maximum_changes: u32,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<NativeDirectoryApplyPreview>, NativeDirectoryApplyError>
where A: AsyncAuthorityStore, O: AsyncObjectStore,
{
    capture.verify_current_binding()?;
    verify_generation(capture, from)?;
    verify_target(capture, to)?;
    let changes = from.diff_to_bounded(to, maximum_changes, budget, cancellation).await?;
    let changed = changes.changed_paths_bounded(maximum_changes, budget, cancellation).await?;
    let mut preview = Vec::with_capacity(changed.value.len());
    for change in changed.value {
        if change.path.components().is_empty() { return Err(NativeDirectoryApplyError::RootMetadataChange); }
        let path = namespace_materialization_path(&change.path)?.into_os_string().into_string()
            .map_err(|_| NativeTreeMaterializationError::InvalidPath("<non-Unicode>".to_owned()))?;
        if capture.capture_policy().excludes(&change.path) {
            return Err(NativeDirectoryApplyError::OutsideCaptureScope(path));
        }
        preview.push(NativeDirectoryApplyChange {
            path,
            before_kind: change.before.map(|record| record.kind),
            after_kind: change.after.map(|record| record.kind),
            content_changed: change.before.map(|record| record.payload) != change.after.map(|record| record.payload),
            metadata_changed: change.before.map(|record| record.metadata) != change.after.map(|record| record.metadata),
            binding_changed: change.before.map(|record| record.file_id) != change.after.map(|record| record.file_id),
        });
    }
    capture.verify_current_binding()?;
    Ok(OperationReceipt { value: NativeDirectoryApplyPreview { from: from.id(), to: to.id(), changes: preview }, work: changed.work })
}

/// Applies only the original capture's paths under its retained native preimages.
/// The same operation key resumes the existing journal after a lost acknowledgement.
#[allow(clippy::too_many_arguments, reason = "one exact captured publication retains its existing native budget, cancellation, and materialization contracts")]
pub async fn apply_native_directory_generation<A, O>(
    capture: &NativeDirectoryCapture,
    from: &Generation<A, O>,
    to: &Generation<A, O>,
    state: &LocalCoreStateStore,
    operation_directory: &Path,
    operation_id: OperationId,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<NativeDirectoryApplyReceipt, NativeDirectoryApplyError>
where A: AsyncAuthorityStore, O: AsyncObjectStore,
{
    capture.verify_current_binding()?;
    verify_generation(capture, from)?;
    verify_target(capture, to)?;
    let journal = publish_captured_native_generation_transition(capture, from, to, state, NativeWorkspacePublication {
        root: capture.root_path(), operation_directory, operation_id,
        from: from.id(), to: to.id(), excluded_names: &[], options, budget, cancellation,
    }).await?;
    receipt(journal)
}

/// Recovers only an already admitted journal belonging to the original capture.
/// Restoring the capture from its original private record never recaptures current files.
pub async fn recover_native_directory_apply(
    capture: &NativeDirectoryCapture,
    state: &LocalCoreStateStore,
    operation_directory: &Path,
    operation_id: OperationId,
    recovery: MaterializationRecovery,
) -> Result<Option<NativeDirectoryApplyReceipt>, NativeDirectoryApplyError> {
    capture.verify_current_binding()?;
    let Some(journal) = MaterializationJournalStore::load(state, operation_id).await? else { return Ok(None); };
    if journal.plan.from != capture.original_generation() {
        return Err(NativeDirectoryApplyError::JournalCaptureMismatch);
    }
    let capture = capture.clone();
    let operation_directory = operation_directory.to_path_buf();
    let backend = acyclic_native_runtime::run_blocking_io(move || NativeTreeMaterializationBackend::from_captured_root(
        capture.root_path(), Arc::clone(capture.held_root()), capture.original_generation(),
        capture.retained_preimages(), capture.volume_config(), &operation_directory,
    )).await.map_err(NativeTreeMaterializationError::from)??;
    JournaledMaterializer::new(state.clone(), backend).recover(operation_id, recovery).await?
        .map(receipt).transpose()
}

fn receipt(journal: MaterializationJournal) -> Result<NativeDirectoryApplyReceipt, NativeDirectoryApplyError> {
    Ok(NativeDirectoryApplyReceipt {
        operation_id: journal.plan.operation_id,
        from: journal.plan.from,
        to: journal.plan.to,
        phase: journal.phase,
        changed_paths: u32::try_from(journal.plan.edits.len()).map_err(|_| NativeDirectoryApplyError::PathCountOverflow)?,
    })
}
