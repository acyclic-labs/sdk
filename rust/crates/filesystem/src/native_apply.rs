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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_archive::{capture_native_directory_archive, restore_native_directory_capture};
    use crate::native_capture::{CaptureOptions, CapturePolicy};
    use crate::native_host::HostRoot;
    use crate::{CheckoutMode, GenerationSelector, IdempotencyKey, LocalAuthorityBackend, LocalObjectBackend, LocalOptions, PublicationPermit, TransactionCommit};
    use std::path::PathBuf;

    struct Fixture {
        directory: tempfile::TempDir,
        source: PathBuf,
        state: LocalCoreStateStore,
        capture: NativeDirectoryCapture,
        from: Generation<LocalAuthorityBackend, LocalObjectBackend>,
        to: Generation<LocalAuthorityBackend, LocalObjectBackend>,
    }

    impl Fixture {
        async fn new(nested: bool) -> Result<Self, Box<dyn std::error::Error>> {
            let directory = tempfile::tempdir()?;
            let source = directory.path().join("customer-parent/working-tree");
            std::fs::create_dir_all(&source)?;
            std::fs::write(source.join("keep.txt"), b"original unrelated bytes")?;
            let relative = if nested {
                std::fs::create_dir(source.join("directory"))?;
                "directory/file.txt"
            } else { "file.txt" };
            std::fs::write(source.join(relative), b"original captured bytes")?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                std::fs::set_permissions(source.join(relative), std::fs::Permissions::from_mode(0o750))?;
                std::os::unix::fs::symlink(relative, source.join("retained-link"))?;
            }
            let fs = crate::Fs::local(LocalOptions::new(directory.path().join("sdk-storage"))).await?;
            let workspace = fs.create_workspace("actual-native-apply").await?;
            let mut checkout = workspace.checkout(GenerationSelector::Head, CheckoutMode::tracking_transaction()).await?;
            let root = Arc::new(HostRoot::open(&source)?);
            let options = CaptureOptions { source_root: source.clone(), expected_root_identity: root.identity(), maximum_paths: 16, maximum_extent_spans: 16 };
            let state = LocalCoreStateStore::new(directory.path().join("native-state"));
            let cancellation = CancellationToken::new();
            let captured = capture_native_directory_archive(&mut checkout, root, &options, &CapturePolicy::allow_all(), OperationId::new(), PublicationPermit::Unrestricted, &state, WorkBudget::UNBOUNDED, &cancellation).await?;
            let from = workspace.generation(captured.capture.original_generation()).await?;
            let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
            transaction.write_text(&format!("/{relative}"), "actual desired bytes").await?;
            let to = match transaction.commit_with_permit(PublicationPermit::Unrestricted).await? {
                TransactionCommit::Committed(generation) | TransactionCommit::AlreadyCommitted(generation) => generation,
                _ => return Err("actual desired SDK generation did not commit".into()),
            };
            Ok(Self { directory, source, state, capture: captured.capture, from, to })
        }

        fn options(&self) -> MaterializeOptions {
            MaterializeOptions { destination: self.directory.path().join("unused-caller-destination"), maximum_directory_entries: 16, maximum_extent_spans: 16, transfer_bytes: 64 * 1024 }
        }

        async fn apply(&self, operation: OperationId) -> Result<NativeDirectoryApplyReceipt, NativeDirectoryApplyError> {
            apply_native_directory_generation(&self.capture, &self.from, &self.to, &self.state, &self.directory.path().join("apply"), operation, &self.options(), WorkBudget::UNBOUNDED, &CancellationToken::new()).await
        }
    }

    #[tokio::test]
    async fn actual_local_generation_apply_preserves_unrelated_later_writer_modes_and_links() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new(false).await?;
        std::fs::write(fixture.source.join("keep.txt"), b"user wrote unrelated bytes after capture")?;
        let preview = preview_native_directory_apply(&fixture.capture, &fixture.from, &fixture.to, 16, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?;
        assert_eq!(preview.value.changes.iter().map(|change| change.path.as_str()).collect::<Vec<_>>(), ["file.txt"]);
        let result = fixture.apply(OperationId::new()).await?;
        assert_eq!(result.phase, MaterializationPhase::Applied);
        assert_eq!(std::fs::read(fixture.source.join("file.txt"))?, b"actual desired bytes");
        assert_eq!(std::fs::read(fixture.source.join("keep.txt"))?, b"user wrote unrelated bytes after capture");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(std::fs::metadata(fixture.source.join("file.txt"))?.permissions().mode() & 0o777, 0o750);
            assert_eq!(std::fs::read_link(fixture.source.join("retained-link"))?, PathBuf::from("file.txt"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn touched_later_writer_is_refused_without_adopting_current_preimages() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new(false).await?;
        std::fs::write(fixture.source.join("file.txt"), b"actual later writer owns this content")?;
        let operation = OperationId::new();
        assert!(fixture.apply(operation).await.is_err());
        assert_eq!(std::fs::read(fixture.source.join("file.txt"))?, b"actual later writer owns this content");
        assert_eq!(std::fs::read(fixture.source.join("keep.txt"))?, b"original unrelated bytes");
        assert!(MaterializationJournalStore::load(&fixture.state, operation).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn a_later_writer_matching_the_desired_bytes_is_not_adopted_as_a_completed_apply() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new(false).await?;
        std::fs::write(fixture.source.join("file.txt"), b"actual desired bytes")?;
        let writer = crate::NativeRootIdentity::from_file(&std::fs::File::open(fixture.source.join("file.txt"))?)?;
        assert!(fixture.apply(OperationId::new()).await.is_err());
        assert_eq!(std::fs::read(fixture.source.join("file.txt"))?, b"actual desired bytes");
        assert_eq!(crate::NativeRootIdentity::from_file(&std::fs::File::open(fixture.source.join("file.txt"))?)?, writer);
        Ok(())
    }

    #[tokio::test]
    async fn moved_original_root_never_mutates_a_foreign_replacement() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new(false).await?;
        let moved = fixture.directory.path().join("moved-original");
        std::fs::rename(&fixture.source, &moved)?;
        std::fs::create_dir(&fixture.source)?;
        std::fs::write(fixture.source.join("file.txt"), b"foreign replacement working tree")?;
        assert!(fixture.apply(OperationId::new()).await.is_err());
        assert_eq!(std::fs::read(moved.join("file.txt"))?, b"original captured bytes");
        assert_eq!(std::fs::read(fixture.source.join("file.txt"))?, b"foreign replacement working tree");
        assert!(restore_native_directory_capture(&LocalCoreStateStore::new(fixture.state.root()), fixture.capture.operation_id()).await.is_err());
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn moved_ancestor_symlink_back_to_the_original_inode_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new(false).await?;
        let parent = fixture.source.parent().ok_or("missing captured root parent")?;
        let moved = fixture.directory.path().join("moved-customer-parent");
        std::fs::rename(parent, &moved)?;
        std::os::unix::fs::symlink(&moved, parent)?;
        // The rebound ambient path still resolves to the original root inode.
        // Identity alone must not grant publication through this new ancestor.
        let reopened = std::fs::File::open(&fixture.source)?;
        assert_eq!(crate::NativeRootIdentity::from_file(&reopened)?, fixture.capture.root_identity());
        let operation = OperationId::new();
        assert!(fixture.apply(operation).await.is_err());
        assert_eq!(std::fs::read(moved.join("working-tree/file.txt"))?, b"original captured bytes");
        assert!(MaterializationJournalStore::load(&fixture.state, operation).await?.is_none());
        assert!(restore_native_directory_capture(&LocalCoreStateStore::new(fixture.state.root()), fixture.capture.operation_id()).await.is_err());
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn replaced_parent_symlink_cannot_escape_the_original_working_tree() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new(true).await?;
        let outside = fixture.directory.path().join("foreign-directory");
        let original = fixture.directory.path().join("retained-original-directory");
        std::fs::create_dir(&outside)?;
        std::fs::write(outside.join("file.txt"), b"foreign file must remain unchanged")?;
        std::fs::rename(fixture.source.join("directory"), &original)?;
        std::os::unix::fs::symlink(&outside, fixture.source.join("directory"))?;
        assert!(fixture.apply(OperationId::new()).await.is_err());
        assert_eq!(std::fs::read(outside.join("file.txt"))?, b"foreign file must remain unchanged");
        assert_eq!(std::fs::read(original.join("file.txt"))?, b"original captured bytes");
        Ok(())
    }

    #[tokio::test]
    async fn lost_acknowledgement_cold_recovery_keeps_the_original_operation_and_postimage_identity() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new(false).await?;
        let operation = OperationId::new();
        let capture_operation = fixture.capture.operation_id();
        let state_path = fixture.state.root().to_path_buf();
        let operation_directory = fixture.directory.path().join("apply");
        // Complete the real mutation, but discard its acknowledgement before reopening.
        fixture.apply(operation).await?;
        let before = crate::NativeRootIdentity::from_file(&std::fs::File::open(fixture.source.join("file.txt"))?)?;
        let revision = MaterializationJournalStore::load(&fixture.state, operation).await?.ok_or("missing actual journal")?.revision;
        drop(fixture.capture);
        drop(fixture.state);
        let cold_state = LocalCoreStateStore::new(state_path);
        let restored = restore_native_directory_capture(&cold_state, capture_operation).await?;
        let recovered = recover_native_directory_apply(&restored, &cold_state, &operation_directory, operation, MaterializationRecovery::Complete).await?.ok_or("original journal was not recovered")?;
        assert_eq!(recovered.operation_id, operation);
        assert_eq!(recovered.from, fixture.from.id());
        assert_eq!(recovered.to, fixture.to.id());
        assert_eq!(recovered.phase, MaterializationPhase::Applied);
        assert_eq!(crate::NativeRootIdentity::from_file(&std::fs::File::open(fixture.source.join("file.txt"))?)?, before);
        assert_eq!(MaterializationJournalStore::load(&cold_state, operation).await?.ok_or("cold journal disappeared")?.revision, revision);
        assert_eq!(std::fs::read(fixture.source.join("file.txt"))?, b"actual desired bytes");
        Ok(())
    }
}
