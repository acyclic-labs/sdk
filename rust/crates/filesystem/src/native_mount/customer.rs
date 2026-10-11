//! Customer-facing native mount composition over the canonical checkout.

use super::adapter::HostCaptureScope;
use super::{
    CheckoutMountSource, LazyMountSource, MountPath, NativeMountError, NativeMountRequest,
    NativeMountSession, SharedCheckout, mount_native,
};
use crate::demand::{DemandSource, SourceNode, SourceNodeKind};
use crate::kernel::FileKind;
use crate::kernel::NamespacePath;
use crate::model::{CheckoutMode, GenerationSelector};
use crate::native_capture::NativeViewBaseline;
use crate::workspace::{Workspace, WorkspaceError, customer_path};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, GenerationId, IdempotencyKey, LazyLookup, LazyWorkspace,
    LazyWorkspaceError, LazyWorkspaceStore, MountId, MountSourceError, NativeRootIdentity,
    VolumeId,
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use thiserror::Error;

/// When authored native mutations become a durable workspace generation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MountPublication {
    /// Publish on native close/fsync/rename boundaries and explicit sync.
    #[default]
    CloseAndSync,
    /// Publish every independently admitted native mutation.
    PerMutation,
    /// Publish only through [`Mount::sync`] or orderly [`Mount::unmount`].
    Manual,
}

/// Explicit customer mount configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountOptions {
    writable: bool,
    subdirectory: std::borrow::Cow<'static, str>,
    publication: MountPublication,
}

impl MountOptions {
    /// Creates a writable tracking-safe sparse mount with close/fsync publication.
    #[must_use]
    pub fn read_write() -> Self {
        Self {
            writable: true,
            subdirectory: std::borrow::Cow::Borrowed("/"),
            publication: MountPublication::CloseAndSync,
        }
    }

    /// Creates a pinned immutable mount.
    #[must_use]
    pub fn read_only() -> Self {
        Self {
            writable: false,
            subdirectory: std::borrow::Cow::Borrowed("/"),
            publication: MountPublication::Manual,
        }
    }

    /// Projects one exact workspace directory as the native mount root.
    #[must_use]
    pub fn subdirectory(mut self, path: impl Into<String>) -> Self {
        self.subdirectory = std::borrow::Cow::Owned(path.into());
        self
    }

    /// Selects the exact publication policy for writable authored effects.
    #[must_use]
    pub const fn publication(mut self, publication: MountPublication) -> Self {
        self.publication = publication;
        self
    }
}

/// One process-owned customer mount over a canonical workspace checkout.
pub struct Mount<A, O> {
    source: Arc<CheckoutMountSource<A, O>>,
    session: Mutex<Option<NativeMountSession>>,
    destination: PathBuf,
}

/// One process-owned native mount over a demand-backed sparse workspace.
pub struct LazyMount<A, O, D, S> {
    source: Arc<LazyMountSource<A, O, D, S>>,
    session: Mutex<Option<NativeMountSession>>,
    destination: PathBuf,
    _destination_custody: crate::NativeAllocation,
    // Caller-owned container capacity retires only after the mounted payload.
    payload_custody: Option<crate::NativeAllocation>,
}
enum MountedSnapshotCapture {
    Original(crate::LazySnapshotRef),
    Qualified(crate::NativePublicationSnapshot),
}


/// SDK-owned capture and publication boundary for a pinned native directory.
///
/// The caller owns the physical volume-root directory and supplies exact
/// changed paths. A selected subdirectory retains its namespace path below
/// that physical root; its contents are not remapped to the root itself.
/// Filesystem semantics and fenced publication use an exact-generation
/// checkout. This handle never rebinds to a live source or starts a
/// mount driver.
pub struct NativeWorkingSet<A, O> {
    source: Arc<CheckoutMountSource<A, O>>,
    workspace: Workspace<A, O>,
    source_root: PathBuf,
    source_identity: NativeRootIdentity,
    selected_root: PathBuf,
    expected_generation: GenerationId,
    baseline: Arc<NativeViewBaseline>,
    capture_budget: crate::WorkBudget,
    writable: bool,
    // Owned host-path containers retire after all working-set payload fields.
    _custody: crate::NativeAllocation,
}
/// Retains an SDK source after its view-dependent physical work has retired.
///
/// Only the SDK retirement engines can create this proof. It retains the
/// original source and allocation custody without allocating another owner.
/// It is not namespace-deletion authority or proof of process termination,
/// and does not certify unrelated immutable base-provider reads.
pub struct NativeMountDrainProof {
    _source: Arc<dyn super::MountFilesystem>,
}




impl<A, O> NativeWorkingSet<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Exact generation admitted into the physical view.
    #[must_use]
    pub const fn generation_id(&self) -> GenerationId {
        self.expected_generation
    }

    /// Physical volume-root directory owned by this prepared view.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.source_root
    }

    /// Accepts physical view-dependent I/O on this source's paid retirement gate.
    ///
    /// The caller must authenticate its owner, volume and path before issuance.
    /// This lease is cleanup custody, not a read or write grant.
    pub fn native_source_io_lease(
        &self,
    ) -> Result<super::adapter::NativeSourceIoLease, NativeMountError> {
        self.source.native_source_io_lease().map_err(NativeMountError::from)
    }

    /// Revalidates a prepared native view immediately before a quiescent
    /// presentation switch. This is optimistic admission, not a lock on the
    /// workspace head: publication remains fenced by the operation permit.
    /// It reads only the selected directory metadata and the current head.
    pub async fn validate_for_presentation(&self) -> Result<(), MountLifecycleError> {
        self.validate_for_presentation_measured(self.capture_budget, self.source.cancellation())
            .await.map(|receipt| receipt.value).map_err(|(error, _)| error)
    }

    /// Revalidates the current fence and physical root under one exact allowance.
    pub async fn validate_for_presentation_measured(
        &self,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<()>, (MountLifecycleError, crate::WorkCounters)> {
        let work = crate::NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(error, spent)| (MountLifecycleError::Materialization(error), spent))?;
        let result = self.validate_for_presentation_with_work(&work.native_mount_work(), cancellation).await;
        result.map(|value| crate::OperationReceipt { value, work: work.receipt() })
            .map_err(|error| (error, work.receipt()))
    }

    /// Revalidates the same selected generation and held-root metadata on the
    /// complete original native ledger, without constructing another meter.
    pub async fn validate_for_presentation_with_work(
        &self, work: &super::NativeMountMeterWork, cancellation: &crate::CancellationToken,
    ) -> Result<(), MountLifecycleError> {
        cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        if self.writable {
            let head = self.workspace.head_with_native_work(&work.native_materialization_work(), cancellation).await?;
            if head.id() != self.expected_generation {
                return Err(MountLifecycleError::Workspace(WorkspaceError::StaleGeneration));
            }
        }
        prepare_root_identity_with_work(
            &self.source_root, &self.selected_root, work, Some(self.source_identity),
        )?;
        if self.writable {
            let head = self.workspace.head_with_native_work(&work.native_materialization_work(), cancellation).await?;
            if head.id() != self.expected_generation {
                return Err(MountLifecycleError::Workspace(WorkspaceError::StaleGeneration));
            }
        }
        cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        Ok(())
    }

    /// Captures only the supplied changed paths from a native working directory.
    ///
    /// The source root is authenticated by the core capture path. This is a
    /// path hint, not proof that other native writes did not occur. Call this
    /// at an operation boundary, then publish with [`Self::sync_with_permit`].
    pub async fn capture_host_paths(&self, paths: &[MountPath]) -> Result<(), MountLifecycleError> {
        self.capture_host_paths_measured(paths, self.capture_budget, self.source.cancellation())
            .await.map(|receipt| receipt.value).map_err(|(error, _)| error)
    }

    /// Captures selected paths through the original capture gate and budget.
    pub async fn capture_host_paths_measured(
        &self,
        paths: &[MountPath],
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<()>, (MountLifecycleError, crate::WorkCounters)> {
        let work = crate::NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(error, work)| (MountLifecycleError::Materialization(error), work))?;
        let result = self.capture_host_paths_with_work(
            paths, &work.native_mount_work(), cancellation,
        ).await;
        result.map(|value| crate::OperationReceipt { value, work: work.receipt() })
            .map_err(|error| (error, work.receipt()))
    }

    /// Captures selected paths using the caller's complete original ledger.
    pub async fn capture_host_paths_with_work(
        &self,
        paths: &[MountPath],
        work: &super::NativeMountMeterWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), MountLifecycleError> {
        self.capture_with_work(paths, HostCaptureScope::ExactPaths, work, cancellation).await
    }

    /// Reconciles a complete selected subtree after a watcher overflow or
    /// missed event. Both host and checkout descendants participate, so
    /// creations and deletions are captured without scanning sibling trees.
    pub async fn capture_host_subtree(&self, root: &MountPath) -> Result<(), MountLifecycleError> {
        self.capture_host_subtree_measured(root, self.capture_budget, self.source.cancellation())
            .await.map(|receipt| receipt.value).map_err(|(error, _)| error)
    }

    /// Reconciles the complete physical subtree, retaining failed capture work.
    pub async fn capture_host_subtree_measured(
        &self,
        root: &MountPath,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<()>, (MountLifecycleError, crate::WorkCounters)> {
        let work = crate::NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(error, work)| (MountLifecycleError::Materialization(error), work))?;
        let result = self.capture_host_subtree_with_work(
            root, &work.native_mount_work(), cancellation,
        ).await;
        result.map(|value| crate::OperationReceipt { value, work: work.receipt() })
            .map_err(|error| (error, work.receipt()))
    }

    /// Captures one complete subtree through the original capture meter.
    pub async fn capture_host_subtree_with_work(
        &self,
        root: &MountPath,
        work: &super::NativeMountMeterWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), MountLifecycleError> {
        self.capture_with_work(
            std::slice::from_ref(root), HostCaptureScope::Subtree, work, cancellation,
        ).await
    }

    async fn capture_with_work(
        &self,
        paths: &[MountPath],
        scope: HostCaptureScope,
        work: &super::NativeMountMeterWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), MountLifecycleError> {
        cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        self.source.capture_host_with_identity_with_native_work(
            &self.source_root, paths, self.source_identity, scope,
            Some(Arc::clone(&self.baseline)), work,
        ).await?;
        cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        Ok(())
    }

    /// Reconciles the selected subtree and publishes under an active, fenced
    /// operation permit. Path hints alone cannot establish completeness: a
    /// missing or delayed notification must not silently omit a native write.
    pub async fn sync_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountLifecycleError> {
        self.sync_with_permit_measured(permit, self.capture_budget, self.source.cancellation())
            .await.map(|receipt| receipt.value).map_err(|(error, _)| error)
    }

    /// Captures and publishes one candidate without resetting its work budget.
    pub async fn sync_with_permit_measured(
        &self,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<()>, (MountLifecycleError, crate::WorkCounters)> {
        let work = crate::NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(error, spent)| (MountLifecycleError::Materialization(error), spent))?;
        let result = self.sync_with_permit_with_work(
            permit, &work.native_mount_work(), cancellation,
            |_| std::future::ready(Ok::<(), MountLifecycleError>(())),
        ).await;
        result.map(|_| crate::OperationReceipt { value: (), work: work.receipt() })
            .map_err(|error| (error.cause, work.receipt()))
    }

    /// Captures and publishes through the canonical source, retaining the
    /// caller's typed pre-publication guard error without formatting or boxing.
    /// Returns the source's actual operation and authored namespace qualification.
    pub async fn sync_with_permit_with_work<F, Fut, E>(
        &self, permit: crate::PublicationPermit, work: &super::NativeMountMeterWork,
        cancellation: &crate::CancellationToken, before_publication: F,
    ) -> Result<crate::NativePublicationSnapshot, NativeMountPublicationFailure<E>>
    where
        E: From<MountLifecycleError> + From<MountSourceError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: std::future::Future<Output = Result<(), E>> + Send + 'static,
    {
        if !self.writable {
            return Err(NativeMountPublicationFailure {
                cause: E::from(MountLifecycleError::NativePolicy("read-only native view cannot publish")),
                accounting_failure: None,
                source_acknowledged: false,
                publication: None,
            });
        }
        let result = async {
            cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
            let current = self.source.shared_checkout()
                .lock_with_carrier(
                    &mut super::adapter::NativeDemandCarrier::Native(work),
                    cancellation, Some(self.source.cancellation()),
                )
                .await?.generation_id();
            let head = self.workspace.head_with_native_work(&work.native_materialization_work(), cancellation).await?;
            if head.id() != current { return Err(MountLifecycleError::Source(MountSourceError::Stale)); }
            self.capture_host_subtree_with_work(&MountPath::root(), work, cancellation).await?;
            Ok::<(), MountLifecycleError>(())
        }.await;
        result.map_err(|cause| NativeMountPublicationFailure {
            cause: E::from(cause), accounting_failure: None, source_acknowledged: false, publication: None,
        })?;
        let context = crate::lazy_workspace::NativeSnapshotContext::Authored {
            workspace_id: self.workspace.id(), config: self.workspace.volume_config(),
        };
        let mut acknowledged = None;
        // This is the final SDK operation. Its own ACK slot survives errors
        // after a real DATA CAS; absent metadata remains unconfirmed.
        self.source.sync_async_with_permit_qualified_native_work_guarded_with_ack(
            permit, work, cancellation, context, &mut acknowledged, before_publication,
        ).await.map_err(|(cause, accounting_failure)| NativeMountPublicationFailure {
            cause, accounting_failure, source_acknowledged: acknowledged.is_some(), publication: acknowledged,
        })
    }
    /// Retires this view's accepted captures using its retained birth credit.
    ///
    /// `wait` controls the caller's wait, not the source's original authority.
    /// An error retains the source and its cleanup credit for a later retry.
    pub fn retire_with_work(
        &self,
        wait: &crate::CancellationToken,
    ) -> Result<NativeMountDrainProof, MountLifecycleError> {
        self.source.cancel_with_cleanup_wait(wait)?;
        if !self.source.capture_retired() {
            return Err(MountLifecycleError::Native(NativeMountError::ControlPending));
        }
        Ok(NativeMountDrainProof { _source: self.source.clone() })
    }


    /// Checks the complete selected physical subtree against its admitted
    /// generation without publishing. Host permissions are not a sandbox:
    /// an approved process may mutate the view, but cannot publish source edits.
    pub async fn verify_unchanged(&self) -> Result<(), MountLifecycleError> {
        self.verify_unchanged_measured(self.capture_budget, self.source.cancellation())
            .await.map(|receipt| receipt.value).map_err(|(error, _)| error)
    }

    /// Captures the admitted source without allowing publication of its edits.
    pub async fn verify_unchanged_measured(
        &self,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<()>, (MountLifecycleError, crate::WorkCounters)> {
        let work = crate::NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(error, work)| (MountLifecycleError::Materialization(error), work))?;
        let result = self.verify_unchanged_with_work(&work.native_mount_work(), cancellation).await;
        result.map(|_| crate::OperationReceipt { value: (), work: work.receipt() })
            .map_err(|error| (error, work.receipt()))
    }

    /// Verifies the complete admitted baseline and returns its genuine immutable SDK qualification.
    pub async fn verify_unchanged_with_work(
        &self,
        work: &super::NativeMountMeterWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::NativePublicationSnapshot, MountLifecycleError> {
        self.capture_host_subtree_with_work(&MountPath::root(), work, cancellation).await?;
        let pending = self.source.shared_checkout().lock_with_carrier(
            &mut super::adapter::NativeDemandCarrier::Native(work),
            cancellation, Some(self.source.cancellation()),
        ).await?.has_pending_mutations();
        if pending {
            return Err(MountLifecycleError::NativePolicy(
                "native source view changed after admission",
            ));
        }
        cancellation.check().map_err(|_| MountSourceError::Cancelled)?;
        Ok(crate::NativePublicationSnapshot::unchanged(
            crate::lazy_workspace::NativeSnapshotContext::Authored {
                workspace_id: self.workspace.id(), config: self.workspace.volume_config(),
            },
            self.expected_generation,
        ))
    }
}

// Customer namespace roots originate in a Unicode MountOptions string, including
// Windows-profile names. Build their host spelling without a UTF-16 temporary.
fn preparation_selected_root_measured(
    path: &NamespacePath,
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut crate::WorkCounters,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<PathBuf, crate::demand::DemandError> {
    use crate::demand::DemandError;
    use crate::kernel::NameEncoding;
    let allocation_error = |error: crate::kernel::allocation::AllocationError|
        error.into_work_or(DemandError::WorkerUnavailable);
    let mut length = 0usize;
    for (index, name) in path.components().iter().enumerate() {
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        work.charge(&crate::WorkCounters {
            items_examined: name.as_bytes().len() as u64,
            source_path_components: 1, ..crate::WorkCounters::UNCHARGED
        }, &budget)?;
        let bytes = match name.encoding() {
            NameEncoding::Utf8 | NameEncoding::PosixBytes => {
                let text = std::str::from_utf8(name.as_bytes()).map_err(|_| DemandError::InvalidRequest)?;
                #[cfg(windows)]
                if text.contains(['/', '\\']) { return Err(DemandError::InvalidRequest); }
                text.len()
            }
            NameEncoding::WindowsUtf16Le => {
                let units = crate::kernel::types::utf16le_units(name.as_bytes())
                    .ok_or(DemandError::InvalidRequest)?;
                let mut bytes = 0usize;
                for character in char::decode_utf16(units) {
                    cancellation.check().map_err(|_| DemandError::Cancelled)?;
                    bytes = bytes.checked_add(character.map_err(|_| DemandError::InvalidRequest)?.len_utf8())
                        .ok_or(crate::WorkError::Overflow)?;
                }
                bytes
            }
        };
        length = length.checked_add(bytes).and_then(|bytes| bytes.checked_add(usize::from(index != 0)))
            .ok_or(crate::WorkError::Overflow)?;
    }
    let copies = crate::WorkCounters { bytes_copied: length as u64, ..crate::WorkCounters::UNCHARGED };
    work.admit(&copies, &budget)?;
    cancellation.check().map_err(|_| DemandError::Cancelled)?;
    allocations.claim_bytes(length as u64, u64::from(length != 0), work, budget).map_err(allocation_error)?;
    let mut host = std::ffi::OsString::new();
    if host.try_reserve_exact(length).is_err() {
        drop(host);
        allocations.release(length as u64).map_err(allocation_error)?;
        return Err(DemandError::WorkerUnavailable);
    }
    let retained = host.capacity() as u64;
    if retained > length as u64 {
        if let Err(error) = allocations.claim_bytes(retained - length as u64, 0, work, budget) {
            drop(host);
            allocations.release(length as u64).map_err(allocation_error)?;
            return Err(allocation_error(error));
        }
    }
    let result = (|| {
        for (index, name) in path.components().iter().enumerate() {
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            if index != 0 {
                work.charge(&crate::WorkCounters { bytes_copied: 1, ..crate::WorkCounters::UNCHARGED }, &budget)?;
                host.push(std::path::MAIN_SEPARATOR_STR);
            }
            match name.encoding() {
                NameEncoding::Utf8 | NameEncoding::PosixBytes => {
                    let text = std::str::from_utf8(name.as_bytes()).map_err(|_| DemandError::InvalidRequest)?;
                    work.charge(&crate::WorkCounters { bytes_copied: text.len() as u64, ..crate::WorkCounters::UNCHARGED }, &budget)?;
                    host.push(text);
                }
                NameEncoding::WindowsUtf16Le => {
                    let units = crate::kernel::types::utf16le_units(name.as_bytes()).ok_or(DemandError::InvalidRequest)?;
                    for character in char::decode_utf16(units) {
                        cancellation.check().map_err(|_| DemandError::Cancelled)?;
                        let character = character.map_err(|_| DemandError::InvalidRequest)?;
                        work.charge(&crate::WorkCounters { bytes_copied: character.len_utf8() as u64,
                            ..crate::WorkCounters::UNCHARGED }, &budget)?;
                        let mut encoded = [0; 4];
                        host.push(character.encode_utf8(&mut encoded));
                    }
                }
            }
        }
        Ok(())
    })();
    if let Err(error) = result {
        drop(host);
        allocations.release(retained).map_err(allocation_error)?;
        return Err(error);
    }
    Ok(host.into())
}

fn flush_session_callbacks(
    session: &Mutex<Option<NativeMountSession>>,
) -> Result<(), MountLifecycleError> {
    with_live_session(session, NativeMountSession::flush_callbacks)
}

/// Waits until the kernel caches nothing a change to the source superseded.
fn revalidate_session(
    session: &Mutex<Option<NativeMountSession>>,
) -> Result<(), MountLifecycleError> {
    with_live_session(session, NativeMountSession::revalidate)
}

fn with_live_session(
    session: &Mutex<Option<NativeMountSession>>,
    operation: impl FnOnce(&NativeMountSession) -> Result<(), NativeMountError>,
) -> Result<(), MountLifecycleError> {
    let owner = match session.lock() {
        Ok(owner) => owner,
        Err(poisoned) => poisoned.into_inner(),
    };
    owner
        .as_ref()
        .map_or(Ok(()), operation)
        .map_err(MountLifecycleError::Native)
}

impl<A, O, D, S> LazyMount<A, O, D, S> {
    /// Exact mounted host path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.destination
    }
}

impl<A, O> Mount<A, O> {
    /// Exact mounted host path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.destination
    }
}

impl<A, O> Mount<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Publishes all pending authored effects with one fenced generation.
    ///
    /// # Errors
    ///
    /// Returns a typed publication failure without discarding pending state.
    pub async fn sync(&self) -> Result<(), MountLifecycleError> {
        flush_session_callbacks(&self.session)?;
        self.source
            .sync_async()
            .await
            .map_err(MountLifecycleError::Source)?;
        Ok(())
    }

    /// Publishes all pending effects under one active operation lease.
    pub async fn sync_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountLifecycleError> {
        flush_session_callbacks(&self.session)?;
        self.source
            .sync_async_with_permit(permit)
            .await
            .map_err(MountLifecycleError::Source)
    }

    /// Publishes pending writes and advances the live checkout to workspace head.
    ///
    /// This keeps a direct-parent agent mount coherent after a child join
    /// without exposing remount orchestration to adapters.
    pub async fn refresh(&self) -> Result<(), MountLifecycleError> {
        self.sync().await?;
        self.advance_to_head().await
    }

    /// Advances an already-clean live checkout to the workspace head.
    ///
    /// Use this after an external, fenced workspace publication when the
    /// caller already synchronized the mount before that publication. Unlike
    /// [`Self::refresh`], this does not try to republish against the newer head.
    ///
    /// # Errors
    ///
    /// Returns a typed conflict or storage failure without changing the
    /// mounted generation.
    pub async fn advance_to_head(&self) -> Result<(), MountLifecycleError> {
        let advanced = self.source.advance_to_head_async().await;
        // Even a failed advance may have moved part of the view.
        revalidate_session(&self.session)?;
        advanced.map_err(MountLifecycleError::Source)
    }

    /// Publishes all pending effects on the source's dedicated callback runtime.
    ///
    /// # Errors
    ///
    /// Returns a typed publication failure without discarding pending state.
    pub fn sync_blocking(&self) -> Result<(), MountLifecycleError> {
        flush_session_callbacks(&self.session)?;
        self.source.sync().map_err(MountLifecycleError::Source)
    }

    /// Publishes pending effects and detaches the native namespace exactly once.
    ///
    /// # Errors
    ///
    /// Publication failure leaves the live mount owned by this handle. A detach
    /// failure retains driver state and its destination fence for drop retry.
    pub async fn unmount(&self) -> Result<(), MountLifecycleError> {
        self.sync().await?;
        self.detach()
    }

    /// Detaches and cancels this mount without publishing pending callbacks.
    ///
    /// This is the fail-closed recovery path after a fenced publication. The
    /// caller may remount from the last durable generation.
    pub fn abandon(&self) -> Result<(), MountLifecycleError> {
        self.detach()
    }

    /// Synchronously publishes and detaches for foreign-runtime worker threads.
    ///
    /// # Errors
    ///
    /// Publication or detach failure retains the complete owner for retry.
    pub fn unmount_blocking(&self) -> Result<(), MountLifecycleError> {
        self.sync_blocking()?;
        self.detach()
    }

    fn detach(&self) -> Result<(), MountLifecycleError> {
        let mut owner = match self.session.lock() {
            Ok(owner) => owner,
            Err(poisoned) => poisoned.into_inner(),
        };
        let Some(session) = owner.as_mut() else {
            return Ok(());
        };
        session.stop().map_err(MountLifecycleError::Native)?;
        owner.take();
        self.source.cancel();
        Ok(())
    }
}

impl<A, O, D, S> LazyMount<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Accepts physical view-dependent I/O on the actual mounted source gate.
    ///
    /// The caller must retain its independent owner, volume and path guards.
    /// The lease follows accepted I/O through physical completion, not wait drop.
    pub fn native_source_io_lease(
        &self,
    ) -> Result<super::adapter::NativeSourceIoLease, NativeMountError> {
        self.source.native_source_io_lease().map_err(NativeMountError::from)
    }

    /// Transfers already-paid container capacity into this payload's lifetime.
    ///
    /// No allocation, admission or cancellation check occurs. The first guard
    /// retains its independent accounting view without granting mount authority.
    /// Extending custody requires the same ledger and resource view; failure
    /// leaves both guards unchanged. Keep the input until its container is freed.
    pub fn retain_payload_allocation(
        &mut self,
        incoming: &mut crate::NativeAllocation,
    ) -> Result<(), MountLifecycleError> {
        let result = if let Some(custody) = self.payload_custody.as_mut() {
            custody.absorb(incoming)
        } else {
            incoming.split_off(incoming.bytes()).map(|custody| self.payload_custody = Some(custody))
        };
        result.map_err(|error| MountLifecycleError::Native(NativeMountError::Materialization(error)))
    }

    /// Returns only the driver's authenticated post-mount root witness.
    pub fn native_root_identity(&self) -> Option<NativeRootIdentity> {
        self.session.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref().and_then(NativeMountSession::native_root_identity)
    }

    /// Acquires real callback-meter custody for a retained native I/O owner.
    ///
    /// Only the actual session's synchronous scope admission holds its mutex.
    /// This accounting scope grants no task, volume or path authority.
    pub fn begin_original_work(
        &self,
        native_work: &super::NativeMountMeterWork,
    ) -> Result<super::OwnedNativeMountOriginalWorkScope, MountLifecycleError> {
        let mut owner = self.session.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let session = owner.as_mut()
            .ok_or(MountLifecycleError::Source(MountSourceError::Stale))?;
        session.retain_measured_cleanup_owner();
        session.begin_original_work(native_work).map_err(MountLifecycleError::Native)
    }

    /// Binds actual mounted I/O callbacks to this unchanged original meter.
    ///
    /// This is a driver work scope, not a task grant or caller identity.
    /// Every callback reaching this physical root during the scope shares
    /// its work admission. The caller must retain its independent owner,
    /// volume and path guards, fork the supplied scope into each accepted
    /// native I/O payload before queueing, and explicitly settle cleanup custody.
    pub async fn with_original_work<T, F, Fut>(
        &self,
        native_work: &super::NativeMountMeterWork,
        operation: F,
    ) -> Result<T, MountLifecycleError>
    where
        T: Send,
        F: FnOnce(&super::OwnedNativeMountOriginalWorkScope) -> Fut + Send,
        Fut: Future<Output = T> + Send,
    {
        let scope = self.begin_original_work(native_work)?;
        // Accepted native I/O owns its own scope fork through physical
        // completion, independently of this caller future's cancellation.
        let value = operation(&scope).await;
        drop(scope);
        Ok(value)
    }

    /// Publishes all pending authored effects with one fenced generation.
    pub async fn sync(&self) -> Result<(), MountLifecycleError> {
        self.sync_with_permit(crate::PublicationPermit::Unrestricted).await
    }

    /// Publishes all pending effects under one active operation lease.
    pub async fn sync_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountLifecycleError> {
        self.sync_with_permit_measured_guarded(
            permit, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
            |_original| std::future::ready(Ok::<(), MountLifecycleError>(())),
        ).await.map(|receipt| receipt.value).map_err(|(error, _)| error.cause)
    }

    /// Flushes actual callbacks and checks the original owner before every seal.
    ///
    /// A failed flush, settlement or guard retains the original publication
    /// owner and the complete work prefix. The callback supplies freshness,
    /// never a replacement permit or a synthetic lazy snapshot.
    pub async fn sync_with_permit_measured_guarded<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        before_publication: F,
    ) -> Result<crate::OperationReceipt<()>, (NativeMountPublicationFailure<E>, crate::WorkCounters)>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountLifecycleError> + From<MountSourceError> + Send + 'static,
    {
        let work = customer_native_work(budget, cancellation)
            .map_err(|(error, spent)| {
                self.retain_failed_session();
                (NativeMountPublicationFailure {
                    cause: E::from(MountLifecycleError::Native(error)), source_acknowledged: false,
                    accounting_failure: None,
                    publication: None,
                }, spent)
            })?;
        let result = self.sync_with_permit_work_guarded(
            permit, &work.native_mount_work(), cancellation, before_publication,
        ).await;
        result.map(|_| crate::OperationReceipt { value: (), work: work.receipt() })
            .map_err(|error| (error, work.receipt()))
    }

    /// Publishes on the enclosing operation's unchanged original native meter.
    /// Failure retains whether the real source completed before native visibility
    /// validation failed. An unconfirmed source is not proof of absent publication.
    /// Success returns the real SDK operation and immutable lazy source cut;
    /// failure retains qualification only from a real source result or DATA ACK.
    pub async fn sync_with_permit_work_guarded<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        native_work: &super::NativeMountMeterWork,
        cancellation: &crate::CancellationToken,
        before_publication: F,
    ) -> Result<crate::NativePublicationSnapshot, NativeMountPublicationFailure<E>>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountLifecycleError> + From<MountSourceError> + Send + 'static,
    {
        let mut acknowledged = None;
        let result = async {
            native_work.meter().with_original_leaf(|work| work.check())
                .map_err(native_demand_lifecycle_error).map_err(|cause| (E::from(cause), None))?;
            with_live_session(&self.session, |session| session.flush_callbacks_with_meter(native_work))
                .map_err(|cause| (E::from(cause), None))?;
            self.source.sync_async_with_permit_qualified_native_work_guarded_with_ack(
                permit, native_work, cancellation, &mut acknowledged, before_publication,
            ).await
        }.await;
        match result {
            Ok(publication) => self.revalidate_acknowledged_publication(native_work, publication),
            Err((cause, accounting_failure)) => {
                self.retain_failed_session();
                Err(NativeMountPublicationFailure {
                    cause, accounting_failure, source_acknowledged: acknowledged.is_some(), publication: acknowledged,
                })
            }
        }
    }

    fn revalidate_acknowledged_publication<E>(
        &self,
        native_work: &super::NativeMountMeterWork,
        publication: crate::NativePublicationSnapshot,
    ) -> Result<crate::NativePublicationSnapshot, NativeMountPublicationFailure<E>>
    where E: From<MountLifecycleError> {
        let result = with_live_session(&self.session, |session| session.revalidate_with_meter(native_work));
        if result.is_err() { self.retain_failed_session(); }
        result.map(|()| publication).map_err(|cause| NativeMountPublicationFailure {
            cause: E::from(cause), accounting_failure: None, source_acknowledged: true, publication: Some(publication),
        })
    }

    /// Imports an original selected mutation through this mount's SDK owner.
    ///
    /// The source authenticates the supplied destination and sealed plan
    /// and guards every publication with the original permit. No private
    /// source handle or alternate publication engine is exposed.
    #[allow(clippy::too_many_arguments, reason = "retains the original selection, operation, permit, work bounds and owner guard")]
    pub async fn import_selected_measured_guarded<F, Fut, E>(
        &self,
        destination: crate::LazySnapshotRef,
        plan: crate::SelectedImportPlan,
        operation: crate::OperationId,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        before_publication: F,
    ) -> Result<crate::OperationReceipt<crate::LazySnapshotRef>, (E, crate::WorkCounters)>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountLifecycleError> + From<MountSourceError> + Send + 'static,
    {
        let work = customer_native_work(budget, cancellation)
            .map_err(|(error, spent)| {
                self.retain_failed_session();
                (E::from(MountLifecycleError::Native(error)), spent)
            })?;
        let result = async {
            let plan_allocation = work.retain_owned(plan.logical_owned_bytes())
                .map_err(|error| E::from(MountLifecycleError::Native(NativeMountError::Materialization(error))))?;
            self.import_selected_work_guarded(
                destination, plan, plan_allocation, operation, permit, &work.native_mount_work(),
                cancellation, before_publication,
            ).await
        }.await;
        if result.is_err() { self.retain_failed_session(); }
        result.map(|value| crate::OperationReceipt { value, work: work.receipt() })
            .map_err(|error| (error, work.receipt()))
    }

    /// Imports the sealed selection using the caller's original native meter.
    /// The already-paid input guard must cover the plan on this exact ledger
    /// and resource view. It grants no source or publication authority.
    /// Input custody is assembled before the returned future can be dropped.
    #[allow(clippy::too_many_arguments, reason = "retains the original selection, operation, permit, carrier and owner guard")]
    pub fn import_selected_work_guarded<'a, F, Fut, E>(
        &'a self,
        destination: crate::LazySnapshotRef,
        plan: crate::SelectedImportPlan,
        plan_allocation: crate::NativeAllocation,
        operation: crate::OperationId,
        permit: crate::PublicationPermit,
        native_work: &'a super::NativeMountMeterWork,
        cancellation: &'a crate::CancellationToken,
        before_publication: F,
    ) -> impl Future<Output = Result<crate::LazySnapshotRef, E>> + Send + 'a
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountLifecycleError> + From<MountSourceError> + Send + 'static,
    {
        // Capture the whole paid tuple: an unpolled future drops data first.
        let input = (plan, plan_allocation);
        async move {
            let input = input;
            let plan_custody = input.1;
            let custody = &plan_custody;
            let plan = input.0;
            // Once polled, this later frame owns the plan and only borrows its
            // outside guard, including every pre-dispatch error and cancellation.
            let result = async move {
                let original = native_work.native_materialization_work();
                original.admit(crate::WorkCounters::UNCHARGED)
                    .map_err(|error| E::from(MountLifecycleError::Native(NativeMountError::Materialization(error))))?;
                if !custody.work().shares_ledger(&original)
                    || !custody.work().shares_resource_allowance(&original)
                    || custody.bytes() < plan.logical_owned_bytes()
                {
                    return Err(E::from(MountLifecycleError::Native(NativeMountError::Materialization(
                        crate::NativeTreeMaterializationError::InvalidLayout,
                    ))));
                }
                with_live_session(&self.session, |session| session.flush_callbacks_with_meter(native_work))
                    .map_err(E::from)?;
                let snapshot = self.source.import_selected_meter_guarded(
                    destination, plan, operation, permit, native_work,
                    cancellation, before_publication,
                ).await?;
                with_live_session(&self.session, |session| session.revalidate_with_meter(native_work))
                    .map_err(E::from)?;
                Ok(snapshot)
            }.await;
            drop(plan_custody);
            if result.is_err() { self.retain_failed_session(); }
            result
        }
    }

    /// Finishes native visibility and reads this operation's original SDK ACK.
    ///
    /// `None` means no original SDK ACK exists. An ACK without completed
    /// native custody or retained original snapshot evidence is a typed error.
    /// This finalizer has no permit: it never seals fresh dirty state or
    /// substitutes the current head for the original completion snapshot.
    pub async fn selected_import_snapshot_measured(
        &self,
        operation: crate::OperationId,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<Option<crate::LazySnapshotRef>>, (NativeMountError, crate::WorkCounters)> {
        let work = customer_native_work(budget, cancellation).map_err(|failure| {
            self.retain_failed_session();
            failure
        })?;
        let result = self.selected_import_snapshot_with_work(operation, &work.native_mount_work(), cancellation).await;
        result.map(|value| crate::OperationReceipt { value, work: work.receipt() })
            .map_err(|error| (error, work.receipt()))
    }

    /// Reads only the original SDK ACK after actual native visibility settles.
    pub async fn selected_import_snapshot_with_work(
        &self,
        operation: crate::OperationId,
        native_work: &super::NativeMountMeterWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Option<crate::LazySnapshotRef>, NativeMountError> {
        let result = async {
            native_work.meter().with_original_leaf(|work| work.check()).map_err(NativeMountError::from)?;
            {
                let owner = self.session.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(session) = owner.as_ref() {
                    session.flush_callbacks_with_meter(native_work)?;
                    session.revalidate_with_meter(native_work)?;
                }
            }
            self.source.selected_import_snapshot_with_meter(operation, native_work, cancellation)
                .await.map_err(NativeMountError::from)
        }.await;
        if result.is_err() { self.retain_failed_session(); }
        result
    }

    /// Publishes pending writes and advances the live checkout to workspace head.
    pub async fn refresh(&self) -> Result<(), MountLifecycleError> {
        self.sync().await?;
        self.advance_to_head().await
    }

    /// Advances an already-clean checkout to workspace head.
    pub async fn advance_to_head(&self) -> Result<(), MountLifecycleError> {
        let advanced = self.source.advance_to_head_async().await;
        // Even a failed advance may have moved part of the view.
        revalidate_session(&self.session)?;
        advanced.map_err(MountLifecycleError::Source)
    }

    /// Makes changes another handle on the mounted workspace made at
    /// `paths` (workspace paths), and everything beneath them, visible
    /// through the mount once this returns.
    ///
    /// # Errors
    ///
    /// Returns a source failure for an invalid path, or a driver failure
    /// when the kernel refuses an invalidation.
    pub fn changed_outside(&self, paths: &[String]) -> Result<(), MountLifecycleError> {
        self.source
            .changed_outside(paths)
            .map_err(MountLifecycleError::Source)?;
        revalidate_session(&self.session)
    }

    /// Makes every change made to the source outside the mount, and
    /// completed before the call, visible through it once this returns.
    ///
    /// A watched source's changes become visible on their own as the host
    /// reports them; this waits for exactly those reported so far, and for
    /// the kernel caches to drop what they superseded. A process that learns
    /// of a change some other way calls this before it looks.
    ///
    /// # Errors
    ///
    /// Returns a driver failure when the kernel refuses an invalidation.
    pub fn revalidate(&self) -> Result<(), MountLifecycleError> {
        revalidate_session(&self.session)
    }

    /// Reads the actual lazy namespace through its existing publication owner.
    pub async fn snapshot_measured(
        &self,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<crate::LazySnapshotRef>, (MountLifecycleError, crate::WorkCounters)> {
        let work = customer_native_work(budget, cancellation)
            .map_err(|(error, spent)| {
                self.retain_failed_session();
                (MountLifecycleError::Native(error), spent)
            })?;
        let result = self.snapshot_with_work(&work.native_mount_work(), cancellation).await;
        result.map(|value| crate::OperationReceipt { value, work: work.receipt() })
            .map_err(|error| (error, work.receipt()))
    }

    /// Reads the original lazy namespace on the enclosing operation's meter.
    pub async fn snapshot_with_work(
        &self,
        native_work: &super::NativeMountMeterWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::LazySnapshotRef, MountLifecycleError> {
        self.snapshot_with_work_policy(native_work, cancellation, false).await
            .map(|capture| match capture {
                MountedSnapshotCapture::Original(snapshot) => snapshot,
                MountedSnapshotCapture::Qualified(..) => unreachable!("ordinary snapshot policy"),
            }).map_err(|(cause, _)| cause)
    }

    /// Verifies the existing mounted namespace and captures genuine readonly
    /// SDK qualification without source rebinding or publication.
    ///
    /// Fixed snapshot data escapes; decoded state and its custody retire together.
    /// Both an original source failure and a reconciliation failure are retained.
    pub async fn publication_snapshot_with_work(
        &self,
        native_work: &super::NativeMountMeterWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::NativePublicationSnapshot,
        (MountLifecycleError, Option<MountSourceError>)> {
        self.snapshot_with_work_policy(native_work, cancellation, true).await
            .map(|capture| match capture {
                MountedSnapshotCapture::Qualified(snapshot) => snapshot,
                MountedSnapshotCapture::Original(_) => unreachable!("qualified snapshot policy"),
            })
    }

    async fn snapshot_with_work_policy(
        &self,
        native_work: &super::NativeMountMeterWork,
        cancellation: &crate::CancellationToken,
        qualify_readonly: bool,
    ) -> Result<MountedSnapshotCapture, (MountLifecycleError, Option<MountSourceError>)> {
        let result = async {
            native_work.meter().with_original_leaf(|work| work.check())
                .map_err(|cause| (native_demand_lifecycle_error(cause), None))?;
            with_live_session(&self.session, |session| session.flush_callbacks_with_meter(native_work))
                .map_err(|cause| (cause, None))?;
            with_live_session(&self.session, |session| session.revalidate_with_meter(native_work))
                .map_err(|cause| (cause, None))?;
            if qualify_readonly {
                let snapshot = self.source.publication_snapshot_with_native_work(native_work, cancellation)
                    .await.map_err(|(cause, accounting)| (MountLifecycleError::Source(cause), accounting))?;
                Ok(MountedSnapshotCapture::Qualified(snapshot))
            } else {
                self.source.snapshot_with_native_work(native_work, cancellation)
                    .await.map(MountedSnapshotCapture::Original)
                    .map_err(|cause| (MountLifecycleError::Source(cause), None))
            }
        }.await;
        if result.is_err() { self.retain_failed_session(); }
        result
    }

    /// Validates the supplied immutable view on the current caller's work.
    ///
    /// This does not flush the driver, invoke the source owner's callback guard,
    /// enter its native scope, publish writes, or grant a caller read authority.
    pub async fn validate_snapshot_with_work(
        &self,
        frozen: crate::LazySnapshotRef,
        native_work: &super::NativeMountMeterWork,
    ) -> Result<(), MountLifecycleError> {
        self.source.validate_snapshot_with_native_work(
            frozen, &native_work.native_materialization_work(),
        ).await.map_err(MountLifecycleError::Lazy)
    }

    /// Looks up exactly the supplied frozen view on the current caller's work.
    ///
    /// Drop the returned lookup before its allocation guard. The source's held
    /// lifetime custody remains separate from this query's actual work.
    pub async fn lookup_at_snapshot_with_work(
        &self,
        frozen: crate::LazySnapshotRef,
        path: &str,
        native_work: &super::NativeMountMeterWork,
    ) -> Result<(crate::LazyLookup, crate::NativeAllocation), MountLifecycleError> {
        self.source.lookup_at_snapshot_with_work(
            frozen, path, &native_work.native_materialization_work(),
        ).await.map_err(MountLifecycleError::from)
    }

    /// Publishes pending effects on the source callback runtime.
    pub fn sync_blocking(&self) -> Result<(), MountLifecycleError> {
        flush_session_callbacks(&self.session)?;
        self.source.sync().map_err(MountLifecycleError::Source)
    }

    /// Publishes pending effects and detaches the native namespace exactly once.
    pub async fn unmount(&self) -> Result<(), MountLifecycleError> {
        self.sync().await?;
        self.detach()
    }

    /// Detaches and cancels this lazy mount without publishing pending state.
    pub fn abandon(&self) -> Result<(), MountLifecycleError> {
        self.detach()
    }

    /// Cancels callback custody and detaches through the original work ledger.
    ///
    /// Initialize native I/O services before calling. Any failure retains the
    /// actual session and its destination fence for an explicit bounded retry;
    /// dropping a failed measured owner must not run unbounded cleanup.
    pub fn abandon_measured(
        &self,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<()>, (MountLifecycleError, crate::WorkCounters)> {
        let work = customer_native_work(budget, cancellation)
            .map_err(|(error, spent)| {
                let mut owner = self.session.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(session) = owner.as_mut() {
                    session.record_measured_cleanup_failure();
                }
                (MountLifecycleError::Native(error), spent)
            })?;
        let result = self.abandon_with_work(&work.native_mount_work());
        result.map(|value| crate::OperationReceipt { value, work: work.receipt() })
            .map_err(|error| (error, work.receipt()))
    }

    /// Stops actual callback, driver and fence custody on the original meter.
    pub fn abandon_with_work(
        &self,
        native_work: &super::NativeMountMeterWork,
    ) -> Result<(), MountLifecycleError> {
        let mut owner = self.session.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = (|| {
            let Some(session) = owner.as_mut() else { return Ok(()); };
            // Mark finite cleanup custody before any callback admission can fail.
            session.retain_measured_cleanup_owner();
            native_work.meter().with_original_leaf(|work| work.check())
                .map_err(native_demand_lifecycle_error)?;
            session.stop_with_meter(native_work)
                .map_err(MountLifecycleError::Native)?;
            if !session.cleanup_state().cleanup_complete {
                return Err(MountLifecycleError::Native(NativeMountError::ControlPending));
            }
            owner.take();
            Ok(())
        })();
        if result.is_err() {
            if let Some(session) = owner.as_mut() {
                session.record_measured_cleanup_failure();
            }
        }
        result
    }
    /// Retires the actual driver, source, watch and fence on birth-time credit.
    ///
    /// The token controls waiting only. A failure preserves the remaining
    /// physical owners; no new meter or unbounded drop cleanup is substituted.
    /// The returned proof covers this mounted view, not unrelated base reads.
    pub fn retire_with_work(
        &self,
        wait: &crate::CancellationToken,
    ) -> Result<NativeMountDrainProof, MountLifecycleError> {
        let mut owner = self.session.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = (|| {
            if let Some(session) = owner.as_mut() {
                session.retain_measured_cleanup_owner();
                session.stop_with_cleanup_wait(wait).map_err(MountLifecycleError::Native)?;
                if !session.cleanup_state().cleanup_complete {
                    return Err(MountLifecycleError::Native(NativeMountError::ControlPending));
                }
            } else {
                // A previous detach retired the session. Use the source's
                // same idempotent engine, never status alone, for this proof.
                self.source.cancel_with_cleanup_wait(wait)?;
            }
            if !self.source.physically_retired()? {
                return Err(MountLifecycleError::Native(NativeMountError::ControlPending));
            }
            let proof = NativeMountDrainProof { _source: self.source.clone() };
            drop(owner.take());
            Ok(proof)
        })();
        if result.is_err() {
            if let Some(session) = owner.as_mut() {
                session.record_measured_cleanup_failure();
            }
        }
        result
    }


    /// Synchronously publishes and detaches for foreign-runtime worker threads.
    pub fn unmount_blocking(&self) -> Result<(), MountLifecycleError> {
        self.sync_blocking()?;
        self.detach()
    }

    fn retain_failed_session(&self) {
        let mut owner = self.session.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(session) = owner.as_mut() {
            session.retain_measured_cleanup_owner();
        }
    }

    fn detach(&self) -> Result<(), MountLifecycleError> {
        #[cfg(target_os = "macos")]
        acyclic_native_runtime::initialize_native_io_services()
            .map_err(NativeMountError::from)?;
        self.abandon_measured(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map(|receipt| receipt.value).map_err(|(error, _)| error)
    }
}

/// A typed publication failure retaining the SDK source's observed completion.
///
/// `false` means unconfirmed, not absent publication. `true` records only an
/// actual source result or observed DATA ACK, never namespace or cleanup authority.
#[derive(Debug, Error)]
#[error("{cause}")]
pub struct NativeMountPublicationFailure<E> {
    /// The original cause and all its intrinsic allocation custody.
    #[source]
    pub cause: E,
    /// A distinct accounting failure retained alongside the original cause.
    pub accounting_failure: Option<MountSourceError>,
    /// A real source result or DATA ACK was observed before this failure.
    pub source_acknowledged: bool,
    /// Actual qualified source metadata retained through finalization failure.
    pub publication: Option<crate::NativePublicationSnapshot>,
}
/// Customer mount admission, publication, and lifecycle failures.
#[derive(Debug, Error)]
pub enum MountLifecycleError {
    /// Workspace path, state, or storage failed admission.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// Original native workspace cause drops before its retained allocation custody.
    #[error("{source}")]
    NativeWorkspace {
        #[source]
        source: WorkspaceError,
        custody: Option<crate::NativeAllocation>,
    },
    /// Lazy source or sparse-overlay admission failed.
    #[error(transparent)]
    Lazy(#[from] LazyWorkspaceError),
    /// Original immutable lazy-query cause drops before its retained custody.
    #[error("{source}")]
    NativeLazy {
        #[source]
        source: LazyWorkspaceError,
        custody: Option<crate::NativeAllocation>,
    },
    /// Native driver admission or teardown failed.
    #[error(transparent)]
    Native(#[from] NativeMountError),
    /// Native callback publication failed.
    #[error(transparent)]
    Source(#[from] MountSourceError),
    /// The physical preparation engine retains its exact original cause.
    #[error(transparent)]
    Materialization(#[from] crate::NativeTreeMaterializationError),
    /// Canonical generation materialization retains its original typed failure.
    #[error(transparent)]
    Materialize(#[from] crate::MaterializeError),
    /// An original demand boundary failed without flattening its typed cause.
    #[error(transparent)]
    Demand(#[from] crate::DemandError),
    /// A fixed native-view policy decision requires no diagnostic allocation.
    #[error("{0}")]
    NativePolicy(&'static str),
    /// Combined exactification and materialization exceeded its work budget.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
}

impl From<(WorkspaceError, Option<crate::NativeAllocation>)> for MountLifecycleError {
    fn from((source, custody): (WorkspaceError, Option<crate::NativeAllocation>)) -> Self {
        Self::NativeWorkspace { source, custody }
    }
}

impl From<(LazyWorkspaceError, Option<crate::NativeAllocation>)> for MountLifecycleError {
    fn from((source, custody): (LazyWorkspaceError, Option<crate::NativeAllocation>)) -> Self {
        Self::NativeLazy { source, custody }
    }
}

impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Prepares an authenticated native working set from one exact generation.
    ///
    /// The destination must be an existing empty real directory. The SDK first
    /// exactifies only the selected subtree, materializes that exact generation,
    /// then admits the resulting directory. Preparation never starts a mount
    /// driver and fails if the workspace head advances before admission. The
    /// supplied work budget also caps each later capture or sync on the handle;
    /// those operations do not inherit the kernel-callback deadline or budget.
    pub async fn prepare_native_working_set(
        &self,
        subdirectory: &str,
        options: &crate::MaterializeOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        permit: crate::PublicationPermit,
    ) -> Result<crate::OperationReceipt<NativeWorkingSet<A, O>>, (MountLifecycleError, crate::WorkCounters)> {
        let work = crate::NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(error, spent)| (MountLifecycleError::Materialization(error), spent))?;
        let result = self.prepare_native_working_set_with_work(
            subdirectory, options, budget, &work.native_mount_work(), cancellation, permit, None,
        ).await;
        result.map(|value| crate::OperationReceipt { value, work: work.receipt() })
            .map_err(|error| (error, work.receipt()))
    }

    /// Exactifies and prepares through one original ledger and resource view.
    /// The capture allowance remains separate for each later invocation.
    /// `native_original` fixes the source-I/O accounting origin before birth;
    /// ordinary SDK callers use `None`. This choice is not namespace authority.
    pub async fn prepare_native_working_set_with_work(
        &self, subdirectory: &str, options: &crate::MaterializeOptions,
        capture_budget: crate::WorkBudget, meter: &super::NativeMountMeterWork,
        cancellation: &crate::CancellationToken, permit: crate::PublicationPermit,
        native_original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<NativeWorkingSet<A, O>, MountLifecycleError> {
        let original = meter.native_materialization_work();
        super::adapter::validate_native_source_io_binding(&original, native_original)?;
        let mut frame_custody = None;
        let (mut future, _) = crate::heap_future::in_heap_admitted(
            (&original, &mut frame_custody),
            |(work, custody), bytes| {
                **custody = Some(work.claim(bytes)?);
                Ok::<(), crate::NativeTreeMaterializationError>(())
            },
            |(work, _)| async move {
                if !options.destination.is_absolute() {
                    return Err(MountLifecycleError::NativePolicy(
                        "native working-set root must be absolute",
                    ));
                }
                let exact = self.exactify_subtree_with_native_work(
                    subdirectory, permit, work, cancellation,
                ).await?;
                let mut mount = (
                    MountOptions::read_write().publication(MountPublication::Manual),
                    work.retain_owned(0)?,
                );
                if subdirectory != "/" {
                    let (subdirectory, mut custody) = work.display(&subdirectory)?;
                    mount.0 = mount.0.subdirectory(subdirectory);
                    mount.1.absorb(&mut custody)?;
                }
                exact.prepare_native_working_set_with_work(
                    &mount.0, options, capture_budget, meter, cancellation, None, native_original,
                ).await
            },
        )?;
        let result = future.as_mut().await;
        drop(future);
        drop(frame_custody);
        result
    }
    async fn prepare_authored_mount_source(
        &self,
        options: &MountOptions,
        expected_generation: Option<GenerationId>,
        callback_guard: Option<&Arc<dyn super::NativeMountCallbackGuard>>,
        native_work: &super::NativeMountMeterWork,
        cancellation: &crate::CancellationToken,
        native_original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<
        (
            Arc<CheckoutMountSource<A, O>>,
            VolumeId,
            (NamespacePath, crate::kernel::allocation::KernelOwnedAllocation),
            (String, crate::kernel::allocation::KernelOwnedAllocation),
        ),
        MountLifecycleError,
    > {
        let original = native_work.native_materialization_work();
        super::adapter::validate_native_source_io_binding(&original, native_original)?;
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, cancellation);
        if let Some(expected) = expected_generation {
            let head = self.workspace().head_with_native_work(&original, cancellation)
                .await.map_err(MountLifecycleError::from)?;
            if head.id() != expected {
                return Err(MountLifecycleError::Workspace(WorkspaceError::StaleGeneration));
            }
        }
        let probe = self.workspace().engine_checkout_with_native_work(
            GenerationSelector::Head, CheckoutMode::read_only_pinned(),
            &original, cancellation,
        ).await.map_err(MountLifecycleError::Workspace)?;
        let config = probe.volume_config();
        drop(probe);
        // Its original SDK proof claims retire through their real custody.
        let root = crate::workspace::customer_path_with_accounting(
            &options.subdirectory, config, &mut accounting, cancellation,
        ).map_err(MountLifecycleError::Workspace)?;
        let root_text = root.0.unicode_text_with_accounting(&mut accounting, cancellation)
            .map_err(|error| MountLifecycleError::Workspace(error.into()))?;
        let selected = self.lookup_with_native_work(
            &root_text.0, &original, cancellation,
        ).await.map_err(MountLifecycleError::Lazy)?;
        // Declare custody before its value so every early return frees the
        // actual lookup buffer before releasing its original ledger claim.
        let selected_custody = selected.1;
        let selected = selected.0;
        if !matches!(
            selected,
            LazyLookup::Authored {
                stat: crate::WorkspaceStat {
                    kind: FileKind::Directory,
                    ..
                },
                ..
            } | LazyLookup::Source(SourceNode {
                kind: SourceNodeKind::Directory,
                ..
            }) | LazyLookup::Shadow {
                record: crate::kernel::FileRecord {
                    kind: FileKind::Directory,
                    ..
                },
                ..
            }
        ) {
            return Err(MountLifecycleError::Workspace(WorkspaceError::NotDirectory));
        }
        if let LazyLookup::Source(node) = &selected {
            if expected_generation.is_some() {
                return Err(MountLifecycleError::Workspace(
                    WorkspaceError::StaleGeneration,
                ));
            }
            let expected_source = self.source_file_id_with_work(
                node, &original, cancellation,
            ).map_err(MountLifecycleError::Lazy)?;
            original.charge(crate::WorkCounters {
                bytes_hashed: (b"acyclic-fs-lazy-mount-root-v1\0".len() + 16 + root_text.0.len() + node.version.0.len()) as u64,
                ..crate::WorkCounters::UNCHARGED
            }).map_err(|error| MountLifecycleError::Native(NativeMountError::Materialization(error)))?;
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"acyclic-fs-lazy-mount-root-v1\0");
            hasher.update(&self.workspace().id().into_bytes());
            hasher.update(root_text.0.as_bytes());
            hasher.update(&node.version.0);
            let mut key = [0_u8; 16];
            key.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
            self.promote_exact_with_native_work(
                &root_text.0, expected_source, 0, IdempotencyKey::from_bytes(key),
                &original, cancellation,
            ).await.map_err(MountLifecycleError::Lazy)?;
        }
        drop(selected);
        drop(selected_custody);

        let mode = if options.writable {
            CheckoutMode::tracking_transaction()
        } else {
            CheckoutMode::read_only_pinned()
        };
        let checkout = self.workspace().engine_checkout_with_native_work(
            expected_generation.map_or(GenerationSelector::Head, GenerationSelector::Exact),
            mode, &original, cancellation,
        ).await.map_err(MountLifecycleError::Workspace)?;
        let shared = SharedCheckout::with_publication_meter(checkout, options.publication, native_work)
            .map_err(MountLifecycleError::Source)?;
        let authored_root = root.0.clone_with_accounting(&mut accounting, cancellation)
            .map_err(|error| MountLifecycleError::Workspace(error.into()))?;
        let authored_root_custody = match authored_root.1 {
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(custody) => custody,
            crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
                return Err(MountLifecycleError::Native(NativeMountError::Materialization(
                    crate::NativeTreeMaterializationError::InvalidLayout,
                )));
            }
        };
        let authored = CheckoutMountSource::new_at_meter(
            shared, config, authored_root.0, authored_root_custody, native_work, native_original,
        )?;
        let authored = match callback_guard {
            Some(guard) => authored.with_native_callback_guard(Arc::clone(guard)),
            None => authored,
        };
        let authored = authored.into_arc_meter(native_work)?;
        if let Some(expected) = expected_generation {
            let head = self.workspace().head_with_native_work(&original, cancellation)
                .await.map_err(MountLifecycleError::from)?;
            if head.id() != expected {
                return Err(MountLifecycleError::Workspace(WorkspaceError::StaleGeneration));
            }
        }
        Ok((authored, self.workspace().id().volume_id(), root, root_text))
    }

    /// Mounts a demand-backed workspace without scanning its source tree.
    ///
    /// Admission observes only the selected root node. Unresolved descendants
    /// remain source-backed until a native callback addresses them.
    pub async fn mount(
        &self,
        destination: impl Into<PathBuf>,
        options: MountOptions,
    ) -> Result<LazyMount<A, O, D, S>, MountLifecycleError> {
        acyclic_native_runtime::initialize_native_io_services().map_err(NativeMountError::from)?;
        super::initialize_native_mount_services()?;
        let destination = destination.into();
        self.mount_measured(destination.as_path(), options, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|(error, _)| error)
    }

    /// Mounts the same real lazy source under one original operation allowance.
    ///
    /// Initialize native I/O and native mount services before invocation. This
    /// operation never hides process-wide service startup in a finite receipt.
    /// Admission reads only the selected root, leaving descendants lazy.
    pub async fn mount_measured(
        &self,
        destination: impl AsRef<Path>,
        options: MountOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<LazyMount<A, O, D, S>>, (MountLifecycleError, crate::WorkCounters)> {
        let work = customer_native_work(budget, cancellation)
            .map_err(|(error, spent)| (MountLifecycleError::Native(error), spent))?;
        let result = self.mount_with_work(destination.as_ref(), options, None, &work.native_mount_work(), cancellation, None).await;
        result.map(|value| crate::OperationReceipt { value, work: work.receipt() })
            .map_err(|error| (error, work.receipt()))
    }

    /// Mounts through an enclosing native operation's original carrier.
    ///
    /// The work ledger, retained allocation custody and cancellation roots are
    /// borrowed unchanged. Service startup remains outside this operation.
    /// Task-operated mounts must supply their genuine owner/fence/purpose
    /// callback guard; an absent guard is only for a host-configured source.
    /// `native_original` separately fixes native source-I/O accounting before
    /// birth. Ordinary SDK callers pass `None`; neither value grants authority.
    pub async fn mount_with_work(
        &self,
        destination: &Path,
        options: MountOptions,
        callback_guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
        native_work: &super::NativeMountMeterWork,
        cancellation: &crate::CancellationToken,
        native_original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<LazyMount<A, O, D, S>, MountLifecycleError> {
        async {
            let original = native_work.native_materialization_work();
            super::adapter::validate_native_source_io_binding(&original, native_original)?;
            let destination = original.copy_path(destination)
                .map_err(|error| MountLifecycleError::Native(NativeMountError::Materialization(error)))?;
            let destination_custody = destination.1;
            let destination = destination.0;
            let options_bytes = match &options.subdirectory {
                std::borrow::Cow::Borrowed(_) => 0,
                std::borrow::Cow::Owned(value) => value.capacity() as u64,
            };
            let _options_custody = original.retain_owned(options_bytes)
                .map_err(|error| MountLifecycleError::Native(NativeMountError::Materialization(error)))?;
            let options = options;
            if options.writable && options.publication == MountPublication::PerMutation {
                return Err(MountLifecycleError::Source(MountSourceError::NativeUnsupported));
            }
            let (authored, volume_id, root, root_text) =
                self.prepare_authored_mount_source(
                    &options, None, callback_guard.as_ref(), native_work, cancellation, native_original,
                ).await?;
            drop(root);
            let root_allocation = match root_text.1 {
                crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(custody) => custody,
                crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
                    return Err(MountLifecycleError::Native(NativeMountError::Materialization(
                        crate::NativeTreeMaterializationError::InvalidLayout,
                    )));
                }
            };
            let root_text = root_text.0;
            let session_destination = original.copy_path(&destination)
                .map_err(|error| MountLifecycleError::Native(NativeMountError::Materialization(error)))?;
            let lazy = self.clone().into_arc_with_native_work(&original)
                .map_err(MountLifecycleError::Lazy)?;
            // Admit the actual existing payload slot before starting its watch.
            let mut source_allocation = original.claim_allocation(
                std::mem::size_of::<LazyMountSource<A, O, D, S>>() as u64, 1,
            ).map_err(|error| MountLifecycleError::Native(NativeMountError::Materialization(error)))?;
            let mut source = Arc::<LazyMountSource<A, O, D, S>>::new_uninit();
            // The real error-owner slot must exist before its source starts a
            // watcher, including for zero remaining-budget startup failures.
            let prepared = super::prepare_native_mount_start_with_meter(native_work)?;
            let request = NativeMountRequest {
                mount_id: MountId::new(), volume_id, destination: session_destination.0,
                writable: options.writable,
            };
            let initialized = match LazyMountSource::new_with_native_work(
                lazy, authored, root_text, root_allocation, &mut source_allocation, native_work,
            ) {
                Ok(source) => source,
                Err(error) => return Err(MountLifecycleError::Native(
                    prepared.retain_source_startup_failure(request, session_destination.1, error),
                )),
            };
            Arc::get_mut(&mut source).expect("the preallocated source is uniquely owned").write(initialized);
            // SAFETY: the unique payload was fully initialized above and was
            // never published while uninitialized.
            #[allow(unsafe_code, reason = "initializes the real source payload before publishing its Arc")]
            let source = unsafe { source.assume_init() };
            let session = prepared.start_with_native_work(
                request, session_destination.1,
                Arc::clone(&source) as Arc<dyn super::MountFilesystem>,
                native_work,
            )?;
            // No fallible work follows acquisition of the live session owner.
            Ok(LazyMount {
                source, session: Mutex::new(Some(session)), destination,
                _destination_custody: destination_custody, payload_custody: None,
            })
        }.await
    }
}

fn customer_native_work(
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<crate::NativeMaterializationWork, (NativeMountError, crate::WorkCounters)> {
    crate::NativeMaterializationWork::new(budget, cancellation).map_err(|(error, work)| {
        let error = match error {
            crate::NativeTreeMaterializationError::Work(error) => NativeMountError::Work(error),
            crate::NativeTreeMaterializationError::Cancelled => crate::DemandError::Cancelled.into(),
            crate::NativeTreeMaterializationError::Io(error) => NativeMountError::Io(error),
            error => NativeMountError::Materialization(error),
        };
        (error, work)
    })
}

fn prepare_root_identity_with_work(
    source_root: &Path, selected_root: &Path,
    work: &super::NativeMountMeterWork, expected_identity: Option<NativeRootIdentity>,
) -> Result<NativeRootIdentity, MountLifecycleError> {
    let original = work.native_materialization_work();
    let root = crate::native_host::HostRoot::open_materialization_measured(source_root, &original)?;
    if expected_identity.is_some_and(|expected| expected != root.identity()) {
        return Err(MountLifecycleError::Source(MountSourceError::Stale));
    }
    if !root.stat_with_work(selected_root, &mut &original)?.is_dir() {
        return Err(MountLifecycleError::NativePolicy("selected native working-set directory is absent"));
    }
    Ok(root.identity())
}

fn native_demand_lifecycle_error(error: crate::demand::DemandError) -> MountLifecycleError {
    let source = match error {
        crate::demand::DemandError::Cancelled => MountSourceError::Cancelled,
        crate::demand::DemandError::Work(error) => MountSourceError::Work(error),
        crate::demand::DemandError::StaleVersion
        | crate::demand::DemandError::SourceUnavailable => MountSourceError::Stale,
        error => return MountLifecycleError::Demand(error),
    };
    MountLifecycleError::Source(source)
}


fn portable_namespace_path(
    path: &crate::kernel::NamespacePath,
) -> Result<String, MountLifecycleError> {
    let mut value = String::new();
    for component in path.components() {
        value.push('/');
        value.push_str(std::str::from_utf8(component.as_bytes()).map_err(|_| {
            MountLifecycleError::Source(MountSourceError::Unsupported(
                "lazy portable mounts cannot root at a non-UTF-8 path".to_owned(),
            ))
        })?);
    }
    if value.is_empty() {
        value.push('/');
    }
    Ok(value)
}

impl<A, O> Workspace<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Mounts the complete workspace or one exact subtree as a native directory.
    ///
    /// Admission authenticates only the selected root frontier; it never scans
    /// the workspace or materializes file bodies.
    ///
    /// # Errors
    ///
    /// Rejects invalid or non-directory roots, unavailable host capabilities,
    /// busy/nonempty destinations, and storage/authentication failures.
    pub async fn mount(
        &self,
        destination: impl Into<PathBuf>,
        options: MountOptions,
    ) -> Result<Mount<A, O>, MountLifecycleError> {
        let mode = if options.writable {
            CheckoutMode::tracking_transaction()
        } else {
            CheckoutMode::read_only_pinned()
        };
        let mut checkout = self.engine_checkout(GenerationSelector::Head, mode).await?;
        let config = checkout.volume_config();
        let root = customer_path(&options.subdirectory, config)?;
        let selected = checkout
            .lookup_no_follow(
                &root,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?
            .value
            .record
            .ok_or(WorkspaceError::NotFound)?;
        if selected.kind != FileKind::Directory {
            return Err(MountLifecycleError::Workspace(WorkspaceError::NotDirectory));
        }
        let shared = Arc::new(SharedCheckout::with_publication(
            checkout,
            options.publication,
        ));
        let source = Arc::new(CheckoutMountSource::new_at(shared, config, root)?);
        let destination = destination.into();
        let session = mount_native(
            NativeMountRequest {
                mount_id: MountId::new(),
                volume_id: self.id().volume_id(),
                destination: destination.clone(),
                writable: options.writable,
            },
            Arc::clone(&source) as Arc<dyn super::MountFilesystem>,
        )?;
        Ok(Mount {
            source,
            session: Mutex::new(Some(session)),
            destination,
        })
    }
}

impl<A, O> crate::Generation<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Materializes this exact generation through the original native ledger.
    /// The canonical durable-output traversal owns all work; its snapshot is
    /// never recharged as a new SDK operation or used as generation authority.
    pub async fn materialize_with_native_work(
        &self, options: &crate::MaterializeOptions,
        work: &super::NativeMountMeterWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::MaterializationReceipt, MountLifecycleError> {
        let original = work.native_materialization_work();
        let mut frame_custody = None;
        let (mut future, _) = crate::heap_future::in_heap_admitted(
            (&original, &mut frame_custody),
            |(original, custody), bytes| {
                **custody = Some(original.claim(bytes)?);
                Ok::<(), crate::NativeTreeMaterializationError>(())
            },
            |(original, _)| async move {
                let workspace = self.workspace();
                let reader = workspace.readonly_pinned_reader_with_native_work(
                    self.id(), original, cancellation,
                ).await?;
                super::materialize_checkout_with_work(&reader, options, work).await
                    .map_err(MountLifecycleError::Materialize)
            },
        )?;
        let result = future.as_mut().await;
        drop(future);
        drop(frame_custody);
        result
    }

    /// Materializes this exact generation and binds its native capture owner.
    /// Read-only views can be verified but never published. Writable views
    /// require the admitted generation to remain the workspace head; later
    /// publication uses the existing conflict and operation-permit mechanism.
    /// The destination must be an existing empty absolute directory. No live
    /// OS mount is started, and independent working sets are not jointly atomic.
    pub async fn prepare_native_working_set(
        &self,
        mount: &MountOptions,
        options: &crate::MaterializeOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<NativeWorkingSet<A, O>>, (MountLifecycleError, crate::WorkCounters)>
    {
        self.prepare_native_working_set_with_budgets(mount, options, budget, budget, cancellation)
            .await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one bounded selected-generation materialization and root-identity admission"
    )]
    /// Prepares the same working set while independently admitting later capture.
    /// Owned host paths and retained adapter/identity payloads share preparation's
    /// allocation ledger; borrowed caller inputs and Arc reference counts do not.
    /// Witnessed attachment's initial capture consumes the remaining preparation
    /// allowance as well as its capture allowance. Later invocations retain the
    /// original capture budget; every failure preserves the cumulative work.
    pub async fn prepare_native_working_set_with_budgets(
        &self,
        mount: &MountOptions,
        options: &crate::MaterializeOptions,
        preparation_budget: crate::WorkBudget,
        capture_budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<NativeWorkingSet<A, O>>, (MountLifecycleError, crate::WorkCounters)>
    {
        self.prepare_native_working_set_from(
            mount, options, None, preparation_budget, capture_budget, cancellation,
        ).await
    }

    /// Attaches an already journaled exact native generation without creating,
    /// adopting, or rewriting its host allocation.
    #[allow(clippy::too_many_arguments, reason = "explicit witnessed allocation and separate admission budgets")]
    pub async fn attach_native_working_set_with_budgets(
        &self,
        mount: &MountOptions,
        options: &crate::MaterializeOptions,
        expected_root_identity: NativeRootIdentity,
        preparation_budget: crate::WorkBudget,
        capture_budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<NativeWorkingSet<A, O>>, (MountLifecycleError, crate::WorkCounters)> {
        self.prepare_native_working_set_from(
            mount, options, Some(expected_root_identity),
            preparation_budget, capture_budget, cancellation,
        ).await
    }

    #[allow(clippy::too_many_arguments, reason = "one preparation and witnessed attachment engine")]
    async fn prepare_native_working_set_from(
        &self, mount: &MountOptions, options: &crate::MaterializeOptions,
        expected_root_identity: Option<NativeRootIdentity>, preparation_budget: crate::WorkBudget,
        capture_budget: crate::WorkBudget, cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<NativeWorkingSet<A, O>>, (MountLifecycleError, crate::WorkCounters)> {
        let original = crate::NativeMaterializationWork::new(preparation_budget, cancellation)
            .map_err(|(cause, work)| (MountLifecycleError::Materialization(cause), work))?;
        let result = self.prepare_native_working_set_from_work(
            mount, options, expected_root_identity, capture_budget,
            &original.native_mount_work(), cancellation, None, None,
        ).await;
        result.map(|value| crate::OperationReceipt { value, work: original.receipt() })
            .map_err(|error| (error, original.receipt()))
    }

    /// Prepares the exact native generation on the caller's original resource view.
    /// `native_original` binds later operations to the issued original resource;
    /// `None` preserves standalone operation accounting, not namespace authority.
    pub async fn prepare_native_working_set_with_work(
        &self, mount: &MountOptions, options: &crate::MaterializeOptions,
        capture_budget: crate::WorkBudget, work: &super::NativeMountMeterWork,
        cancellation: &crate::CancellationToken,
        callback_guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
        native_original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<NativeWorkingSet<A, O>, MountLifecycleError> {
        self.prepare_native_working_set_from_work(
            mount, options, None, capture_budget, work, cancellation, callback_guard, native_original,
        ).await
    }

    /// Reattaches only the witnessed original root without materialization.
    pub async fn attach_native_working_set_with_work(
        &self, mount: &MountOptions, options: &crate::MaterializeOptions,
        expected_root_identity: NativeRootIdentity, capture_budget: crate::WorkBudget,
        work: &super::NativeMountMeterWork, cancellation: &crate::CancellationToken,
        callback_guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
        native_original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<NativeWorkingSet<A, O>, MountLifecycleError> {
        self.prepare_native_working_set_from_work(
            mount, options, Some(expected_root_identity), capture_budget, work, cancellation, callback_guard, native_original,
        ).await
    }

    #[allow(clippy::too_many_arguments, reason = "one original-ledger generation preparation engine")]
    async fn prepare_native_working_set_from_work(
        &self, mount: &MountOptions, options: &crate::MaterializeOptions,
        expected_root_identity: Option<NativeRootIdentity>, capture_budget: crate::WorkBudget,
        meter: &super::NativeMountMeterWork, cancellation: &crate::CancellationToken,
        callback_guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
        native_original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<NativeWorkingSet<A, O>, MountLifecycleError> {
        let original = meter.native_materialization_work();
        super::adapter::validate_native_source_io_binding(&original, native_original)?;
        if let Some(guard) = callback_guard.as_ref() {
            let mut authorization = guard.check(meter)?;
            let result = (&mut authorization).await;
            drop(authorization);
            result?;
        }
        let mut frame_custody = None;
        let (mut future, _) = crate::heap_future::in_heap_admitted(
            (&original, &mut frame_custody),
            |(original, custody), bytes| {
                **custody = Some(original.claim(bytes)?);
                Ok::<(), crate::NativeTreeMaterializationError>(())
            },
            |(original, _)| async move {
                if !options.destination.is_absolute() || mount.publication != MountPublication::Manual {
                    return Err(MountLifecycleError::NativePolicy(
                        "native working set requires an absolute root and manual publication",
                    ));
                }
                let workspace = self.workspace();
                let expected_generation = self.id();
                if mount.writable
                    && workspace.head_with_native_work(original, cancellation).await?.id() != expected_generation
                {
                    return Err(MountLifecycleError::Workspace(WorkspaceError::StaleGeneration));
                }
                let mode = if mount.writable {
                    CheckoutMode::tracking_transaction()
                } else {
                    CheckoutMode::read_only_pinned()
                };
                let checkout = workspace.engine_checkout_with_native_work(
                    GenerationSelector::Exact(expected_generation), mode, original, cancellation,
                ).await?;
                let mut checkout = if mount.writable {
                    checkout
                } else {
                    checkout.into_unpublishable_capture_candidate()
                };
                let config = checkout.volume_config();
                let root = original.with_owned_allocations(
                    |allocations, work, budget, cancellation| {
                        crate::workspace::customer_path_measured(
                            &mount.subdirectory, config, allocations, work, budget, cancellation,
                        ).map_err(MountLifecycleError::Workspace)
                    },
                )?;
                if checkout.lookup_no_follow_with_native_work(&root.0, original, cancellation)
                    .await.map_err(WorkspaceError::from)?.record
                    .is_none_or(|record| record.kind != FileKind::Directory)
                {
                    return Err(MountLifecycleError::Workspace(WorkspaceError::NotDirectory));
                }
                if expected_root_identity.is_none() {
                    let candidate = checkout.private_candidate_with_native_work(original, cancellation)
                        .map_err(WorkspaceError::from)?;
                    let reader = candidate.into_pinned_reader_with_native_work(original, cancellation)
                        .map_err(WorkspaceError::from)?;
                    super::materialize_checkout_paths_with_mode_and_work(
                        &reader, std::slice::from_ref(&root.0), options,
                        super::MaterializeMode::ReconstructibleView, meter,
                    ).await.map_err(MountLifecycleError::Materialize)?;
                }
                let mut source_root = original.copy_path(&options.destination)?;
                let mut selected_root = original.with_owned_allocations(
                    |allocations, work, budget, cancellation| {
                        preparation_selected_root_measured(
                            &root.0, allocations, work, budget, cancellation,
                        ).map_err(native_demand_lifecycle_error)
                    },
                )?;
                source_root.1.absorb(&mut selected_root.1)?;
                let source_identity = prepare_root_identity_with_work(
                    &source_root.0, &selected_root.0, meter, expected_root_identity,
                )?;
                let baseline = NativeViewBaseline::new_with_native_work(
                    source_identity, !mount.writable, original,
                )?;
                let shared = SharedCheckout::with_publication_meter(
                    checkout, MountPublication::Manual, meter,
                )?;
                let source = CheckoutMountSource::new_at_meter(
                    shared, config, root.0, root.1, meter, native_original,
                )?;
                let source = match callback_guard {
                    Some(guard) => source.with_native_callback_guard(guard),
                    None => source,
                }.into_arc_meter(meter)?;
                if expected_root_identity.is_some() {
                    let capture = original.native_volume_work(capture_budget)?;
                    let capture_meter = capture.native_mount_work();
                    source.capture_host_with_identity_with_native_work(
                        &source_root.0, &[MountPath::root()], source_identity,
                        HostCaptureScope::Subtree, Some(Arc::clone(&baseline)), &capture_meter,
                    ).await?;
                    let pending = source.shared_checkout().lock_with_carrier(
                        &mut super::adapter::NativeDemandCarrier::Native(&capture_meter),
                        cancellation, Some(source.cancellation()),
                    ).await?.has_pending_mutations();
                    if pending {
                        return Err(MountLifecycleError::NativePolicy(
                            "journaled native allocation diverged from its original generation",
                        ));
                    }
                }
                if mount.writable
                    && workspace.head_with_native_work(original, cancellation).await?.id() != expected_generation
                {
                    return Err(MountLifecycleError::Workspace(WorkspaceError::StaleGeneration));
                }
                Ok(NativeWorkingSet {
                    source, workspace, source_root: source_root.0, source_identity, selected_root: selected_root.0,
                    expected_generation, baseline, capture_budget, writable: mount.writable,
                    _custody: source_root.1,
                })
            },
        )?;
        let result = future.as_mut().await;
        drop(future);
        drop(frame_custody);
        result
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::demand::native::NativeDemandSource;
    use crate::model::{FilesystemProfile, VolumeLimits};
    use crate::{Fs, MemoryLazyWorkspaceStore, PublicationPermit};

    #[test]
    fn selected_root_copies_preadmit_native_encoded_storage()
    -> Result<(), Box<dyn std::error::Error>> {
        for profile in [FilesystemProfile::Portable, FilesystemProfile::Windows] {
            let cancellation = crate::CancellationToken::new();
            let path = NamespacePath::parse_in_profile_measured(
                "/selected-é/leaf", profile, VolumeLimits::default(),
                crate::WorkBudget::UNBOUNDED, &cancellation, 0,
            )?.value;
            let mut allocations = crate::kernel::allocation::AllocationLedger::default();
            let mut work = crate::WorkCounters::UNCHARGED;
            let mut budget = crate::WorkBudget::UNBOUNDED;
            budget.bytes_copied = 0;
            assert!(matches!(
                preparation_selected_root_measured(&path, &mut allocations, &mut work, budget, &cancellation),
                Err(crate::demand::DemandError::Work(crate::WorkError::BudgetExceeded {
                    counter: "bytes_copied", ..
                }))
            ));
            assert_eq!(work.allocation_operations, 0);
            assert_eq!(work.bytes_copied, 0);
            assert_eq!(allocations.live_bytes(), 0);
            work = crate::WorkCounters::UNCHARGED;
            let host = preparation_selected_root_measured(
                &path, &mut allocations, &mut work, crate::WorkBudget::UNBOUNDED, &cancellation,
            )?;
            assert_eq!(host, Path::new("selected-é").join("leaf"));
            assert_eq!(work.bytes_copied, host.as_os_str().len() as u64);
            assert_eq!(work.allocation_operations, 1);
            assert_eq!(allocations.live_bytes(), host.capacity() as u64);
            let retained = host.capacity() as u64;
            drop(host);
            allocations.release(retained)?;
            assert_eq!(allocations.live_bytes(), 0);
            cancellation.cancel();
            work = crate::WorkCounters::UNCHARGED;
            assert!(matches!(
                preparation_selected_root_measured(
                    &path, &mut allocations, &mut work, crate::WorkBudget::UNBOUNDED, &cancellation,
                ),
                Err(crate::demand::DemandError::Cancelled)
            ));
            assert_eq!(work, crate::WorkCounters::UNCHARGED);
        }
        Ok(())
    }

    #[tokio::test]
    async fn measured_customer_root_admission_keeps_unselected_source_cold()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        crate::initialize_native_mount_services()?;
        let physical = tempfile::tempdir()?;
        std::fs::create_dir(physical.path().join("selected"))?;
        std::fs::write(physical.path().join("selected/cold.txt"), b"unread descendant")?;
        std::fs::create_dir(physical.path().join("unselected"))?;
        std::fs::write(physical.path().join("unselected/cold.txt"), b"unread sibling")?;
        let fs = Fs::memory();
        let demand = Arc::new(NativeDemandSource::open(
            physical.path(), FilesystemProfile::Portable, VolumeLimits::default(),
        ).await?);
        let lazy = LazyWorkspace::attach(
            &fs, "measured-customer-root", demand, MemoryLazyWorkspaceStore::default(),
        ).await?;
        let cancellation = crate::CancellationToken::new();
        let budget = crate::WorkBudget {
            source_bytes_read: 0,
            source_directory_pages: 0,
            materializations: 0,
            ..crate::WorkBudget::UNBOUNDED
        };
        let options = MountOptions::read_write().subdirectory("/selected")
            .publication(MountPublication::Manual);
        let work = customer_native_work(budget, &cancellation).map_err(|(error, _)| error)?;
        let (ordinary_source, volume, _, root) = lazy.prepare_authored_mount_source(
            &options, None, None, &work.native_mount_work(), &cancellation, None,
        ).await?;
        assert_eq!(volume, lazy.workspace().id().volume_id());
        assert_eq!(root.0, "/selected");
        work.receipt().verify(budget)?;
        assert_eq!(work.receipt().source_bytes_read, 0);
        assert_eq!(work.receipt().source_directory_pages, 0);
        assert_eq!(work.receipt().materializations, 0);
        assert!(work.receipt().backend_read_operations > 0);
        assert!(matches!(lazy.lookup("/selected").await?, LazyLookup::Authored { .. }));
        assert!(matches!(lazy.lookup("/selected/cold.txt").await?, LazyLookup::Source(_)));
        assert!(matches!(lazy.lookup("/unselected/cold.txt").await?, LazyLookup::Source(_)));
        let pinned = lazy.workspace().head().await?.id();
        let denied_budget = crate::WorkBudget {
            backend_read_operations: 0, ..crate::WorkBudget::UNBOUNDED
        };
        let denied = customer_native_work(denied_budget, &cancellation).map_err(|(error, _)| error)?;
        assert!(lazy.prepare_authored_mount_source(
            &options, Some(pinned), None, &denied.native_mount_work(), &cancellation, None,
        ).await.is_err());
        denied.receipt().verify(denied_budget)?;
        assert_eq!(denied.receipt().backend_read_operations, 0);
        assert_eq!(lazy.workspace().head().await?.id(), pinned);
        let job = customer_native_work(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        assert!(!job.shares_ledger(&work));
        let rejected_root = tempfile::tempdir()?;
        let before_binding = work.receipt();
        assert!(matches!(
            lazy.prepare_native_working_set_with_work(
                "/selected", &crate::MaterializeOptions::native(rejected_root.path()),
                budget, &work.native_mount_work(), &cancellation,
                PublicationPermit::Unrestricted, Some(&job),
            ).await,
            Err(MountLifecycleError::Source(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            )))
        ));
        assert_eq!(work.receipt(), before_binding);
        assert!(matches!(
            lazy.mount_with_work(
                rejected_root.path(), options.clone(), None, &work.native_mount_work(),
                &cancellation, Some(&job),
            ).await,
            Err(MountLifecycleError::Source(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            )))
        ));
        assert_eq!(work.receipt(), before_binding);
        assert_eq!(lazy.workspace().head().await?.id(), pinned);
        assert_eq!(std::fs::read_dir(rejected_root.path())?.count(), 0);
        let ordinary_job = job.with_source_io_lease(ordinary_source.native_source_io_lease()?)?;
        assert!(ordinary_job.shares_ledger(&job));
        assert!(ordinary_job.shares_resource_allowance(&job));
        assert!(!ordinary_job.shares_ledger(&work));
        drop(ordinary_job);
        ordinary_source.cancel_with_cleanup_wait(&crate::CancellationToken::new())?;
        let (native_source, _, _, _) = lazy.prepare_authored_mount_source(
            &options, Some(pinned), None, &work.native_mount_work(), &cancellation, Some(&work),
        ).await?;
        let before_foreign_job = job.receipt();
        assert!(matches!(
            job.with_source_io_lease(native_source.native_source_io_lease()?),
            Err(crate::NativeTreeMaterializationError::InvalidLayout)
        ));
        assert_eq!(job.receipt(), before_foreign_job);
        let native_job = work.with_source_io_lease(native_source.native_source_io_lease()?)?;
        assert!(native_job.shares_ledger(&work));
        assert!(native_job.shares_resource_allowance(&work));
        drop(native_job);
        native_source.cancel_with_cleanup_wait(&crate::CancellationToken::new())?;
        assert!(ordinary_source.capture_retired());
        assert!(native_source.capture_retired());
        work.receipt().verify(budget)?;
        assert_eq!(work.receipt().source_bytes_read, 0);
        assert_eq!(work.receipt().source_directory_pages, 0);
        assert_eq!(work.receipt().materializations, 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "mounts a live native session; requires the host's native mount capability"]
    async fn measured_lazy_customer_preserves_mixed_identity_guard_and_cleanup_custody()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        crate::initialize_native_mount_services()?;
        let physical = tempfile::tempdir()?;
        std::fs::write(physical.path().join("source.txt"), b"source")?;
        let destination = tempfile::tempdir()?;
        let fs = Fs::memory();
        let demand = Arc::new(NativeDemandSource::open(
            physical.path(), FilesystemProfile::Portable, VolumeLimits::default(),
        ).await?);
        let lazy = LazyWorkspace::attach(
            &fs, "measured-native-customer", demand, MemoryLazyWorkspaceStore::default(),
        ).await?;
        lazy.workspace().write_text("/authored.txt", "authored").await?;
        let generation = lazy.workspace().head().await?.id();
        let cancellation = crate::CancellationToken::new();
        let options = MountOptions::read_write().publication(MountPublication::Manual);
        let denied = lazy.mount_measured(
            destination.path(), options.clone(), crate::WorkBudget::default(), &cancellation,
        ).await.err().ok_or("zero mount allowance admitted a real provider")?;
        denied.1.verify(crate::WorkBudget::default())?;
        assert_eq!(denied.1, crate::WorkCounters::UNCHARGED);
        assert!(matches!(denied.0, MountLifecycleError::Native(NativeMountError::Work(_))));
        assert_eq!(lazy.workspace().head().await?.id(), generation);
        assert_eq!(std::fs::read_dir(destination.path())?.count(), 0);
        let path_budget = crate::WorkBudget { bytes_copied: 0, ..crate::WorkBudget::UNBOUNDED };
        let path_work = customer_native_work(path_budget, &cancellation).map_err(|(error, _)| error)?;
        let before_path_copy = path_work.receipt();
        let denied_path = lazy.mount_with_work(
            destination.path(), options.clone(), None, &path_work.native_mount_work(), &cancellation, None,
        ).await.err().ok_or("unfunded borrowed destination was copied before admission")?;
        assert!(matches!(denied_path, MountLifecycleError::Native(NativeMountError::Materialization(_))));
        assert_eq!(path_work.receipt(), before_path_copy);
        path_work.receipt().verify(path_budget)?;
        assert_eq!(lazy.workspace().head().await?.id(), generation);
        assert_eq!(std::fs::read_dir(destination.path())?.count(), 0);
        let startup = lazy.mount_measured(
            "relative-invalid-mount", options.clone(), crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await.err().ok_or("invalid destination admitted a real provider")?;
        assert_ne!(startup.1, crate::WorkCounters::UNCHARGED);
        let mut startup = match startup.0 {
            MountLifecycleError::Native(NativeMountError::StartupFailed(startup)) => startup,
            error => return Err(Box::new(error)),
        };
        // The source watcher is real cleanup custody even before a driver or
        // destination fence exists.
        assert!(!startup.owner().cleanup_state().cleanup_complete);
        assert!(startup.owner().cleanup_state().source_retained);
        let cancelled_startup_cleanup = crate::CancellationToken::new();
        cancelled_startup_cleanup.cancel();
        let retained = startup.owner_mut().stop_measured(
            crate::WorkBudget::UNBOUNDED, &cancelled_startup_cleanup,
        ).err().ok_or("cancelled startup cleanup discarded the source watcher")?;
        assert_eq!(retained.1, crate::WorkCounters::UNCHARGED);
        assert!(!startup.owner().cleanup_state().cleanup_complete);
        assert!(startup.owner().cleanup_state().source_retained);
        startup.owner_mut().stop_measured(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        assert!(startup.owner().cleanup_state().cleanup_complete);
        assert!(!startup.owner().cleanup_state().source_retained);
        let mounted = lazy.mount_measured(
            destination.path(), options, crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await.map_err(|(error, _)| error)?;
        assert!(mounted.work.backend_read_operations > 0);
        let mut mount = mounted.value;
        let payload_work = customer_native_work(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        assert!(!payload_work.shares_ledger(mount._destination_custody.work()));
        let payload_bytes = std::mem::size_of_val(&mount) as u64;
        let mut payload_custody = payload_work.claim(payload_bytes)?;
        let mut slot = Arc::new_uninit();
        let before_transfer = payload_work.receipt();
        mount.retain_payload_allocation(&mut payload_custody)?;
        assert_eq!(payload_custody.bytes(), 0);
        assert_eq!(payload_work.receipt(), before_transfer);
        assert_eq!(mount.payload_custody.as_ref().map(crate::NativeAllocation::bytes), Some(payload_bytes));
        let foreign_work = customer_native_work(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let mut foreign_custody = foreign_work.claim(8)?;
        let foreign_data = Box::new([0u8; 8]);
        let foreign_prefix = foreign_work.receipt();
        assert!(matches!(
            mount.retain_payload_allocation(&mut foreign_custody),
            Err(MountLifecycleError::Native(NativeMountError::Materialization(
                crate::NativeTreeMaterializationError::InvalidLayout,
            )))
        ));
        assert_eq!(foreign_custody.bytes(), 8);
        assert_eq!(foreign_work.receipt(), foreign_prefix);
        assert_eq!(payload_work.receipt(), before_transfer);
        assert_eq!(mount.payload_custody.as_ref().map(crate::NativeAllocation::bytes), Some(payload_bytes));
        drop(foreign_data);
        drop(foreign_custody);
        Arc::get_mut(&mut slot).ok_or("mount payload slot was shared before initialization")?.write(mount);
        // SAFETY: the unique slot contains the complete real mount above.
        #[allow(unsafe_code, reason = "tests the actual pre-admitted mounted Arc payload transfer")]
        let mount = unsafe { slot.assume_init() };
        let before = mount.snapshot_measured(crate::WorkBudget::UNBOUNDED, &cancellation)
            .await.map_err(|(error, _)| error)?;
        assert_eq!(before.value.authored_generation, generation);
        let query_work = customer_native_work(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        assert!(!query_work.shares_ledger(mount._destination_custody.work()));
        let query_native = query_work.native_mount_work();
        mount.validate_snapshot_with_work(before.value, &query_native).await?;
        let (lookup, lookup_custody) = mount.lookup_at_snapshot_with_work(
            before.value, "/source.txt", &query_native,
        ).await?;
        assert!(matches!(&lookup, crate::LazyLookup::Source(node) if node.kind == crate::SourceNodeKind::RegularFile));
        assert!(lookup_custody.work().shares_ledger(&query_work));
        assert!(query_work.receipt().items_examined > 0);
        drop(lookup);
        drop(lookup_custody);
        let denied_query_cancellation = crate::CancellationToken::new();
        let denied_query = customer_native_work(crate::WorkBudget::UNBOUNDED, &denied_query_cancellation)
            .map_err(|(error, _)| error)?;
        let denied_query_prefix = denied_query.receipt();
        denied_query_cancellation.cancel();
        assert!(mount.validate_snapshot_with_work(before.value, &denied_query.native_mount_work()).await.is_err());
        assert_eq!(denied_query.receipt(), denied_query_prefix);
        mount.validate_snapshot_with_work(before.value, &query_native).await?;
        assert_eq!(lazy.workspace().head().await?.id(), generation);
        let identity = mount.native_root_identity().ok_or("mounted driver lost its original root witness")?;
        let io_work = customer_native_work(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let unrelated = customer_native_work(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let (bytes, custody) = mount.with_original_work(&io_work.native_mount_work(), |_scope| async {
            let conflict = mount.with_original_work(
                &unrelated.native_mount_work(), |_scope| std::future::ready(()),
            ).await;
            assert!(matches!(conflict, Err(MountLifecycleError::Native(NativeMountError::ControlPending))));
            mount.with_original_work(&io_work.native_mount_work(), |scope| {
                let scope = scope.try_clone_for_io();
                let work = &io_work;
                let mounted = &mount;
                async move {
                    let scope = scope.map_err(MountLifecycleError::Native)?;
                    work.read_working_file_guarded_with_scope::<MountLifecycleError, _, _>(
                        scope, mounted.path(), identity, Path::new("source.txt"), 32,
                        || std::future::ready(Ok(())),
                    ).await
                }
            }).await?
        }).await??;
        assert_eq!(bytes, b"source");
        assert!(io_work.receipt().source_bytes_read >= 6);
        assert_eq!(unrelated.receipt().source_bytes_read, 0);
        drop(bytes);
        drop(custody);
        assert_eq!(std::fs::read(destination.path().join("source.txt"))?, b"source");
        assert_eq!(std::fs::read(destination.path().join("authored.txt"))?, b"authored");
        std::fs::write(destination.path().join("authored.txt"), b"native edit")?;
        let guarded = mount.sync_with_permit_measured_guarded(
            PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &cancellation,
            |_original| std::future::ready(Err::<(), _>(MountLifecycleError::Source(MountSourceError::Stale))),
        ).await.err().ok_or("failed original-owner guard published")?;
        assert!(!guarded.0.source_acknowledged);
        assert!(guarded.0.accounting_failure.is_none());
        assert!(guarded.0.publication.is_none());
        assert!(guarded.1.items_examined > 0);
        assert_eq!(lazy.workspace().head().await?.id(), generation);
        assert!(mount.snapshot_measured(crate::WorkBudget::UNBOUNDED, &cancellation).await.is_err());
        let original = customer_native_work(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let callback_original = original.clone();
        let published = mount.sync_with_permit_work_guarded(
            PublicationPermit::Unrestricted, &original.native_mount_work(), &cancellation,
            move |meter: &crate::NativeMaterializationWork| {
                assert!(meter.shares_ledger(&callback_original));
                assert!(meter.shares_resource_allowance(&callback_original));
                assert_eq!(meter.receipt(), callback_original.receipt());
                std::future::ready(Ok::<(), MountLifecycleError>(()))
            },
        ).await?;
        assert!(published.operation().is_some());
        assert!(published.snapshot().is_lazy());
        assert_eq!(published.snapshot().workspace_id(), lazy.workspace().id());
        assert_eq!(published.snapshot().volume_config(), lazy.workspace().volume_config());
        assert!(original.receipt().items_examined > 0);
        assert_eq!(lazy.read_range("/authored.txt", 0, 32).await?.as_ref(), b"native edit");
        std::fs::write(destination.path().join("authored.txt"), b"late source acknowledgement")?;
        let publication_cancellation = crate::CancellationToken::new();
        let publication_work = customer_native_work(crate::WorkBudget::UNBOUNDED, &publication_cancellation)
            .map_err(|(error, _)| error)?;
        let publication_native = publication_work.native_mount_work();
        with_live_session(&mount.session, |session| session.flush_callbacks_with_meter(&publication_native))?;
        let before_publication = |_actual: &crate::NativeMaterializationWork| std::future::ready(Ok::<(), MountSourceError>(()));
        let mut source_ack = None;
        let qualified_ack = mount.source.sync_async_with_permit_qualified_native_work_guarded_with_ack(
            PublicationPermit::Unrestricted, &publication_native, &publication_cancellation,
            &mut source_ack, before_publication,
        ).await.map_err(|(cause, accounting_failure)| NativeMountPublicationFailure {
            cause, accounting_failure, source_acknowledged: source_ack.is_some(), publication: source_ack,
        })?;
        assert_eq!(source_ack, Some(qualified_ack));
        assert!(qualified_ack.operation().is_some());
        let acknowledged = mount.source.snapshot_with_native_work(
            &publication_native, &publication_cancellation,
        ).await?;
        assert_eq!(
            qualified_ack.snapshot().lazy_snapshot_with_native_work(&publication_work, &publication_cancellation)?,
            Some(acknowledged),
        );
        assert_eq!(lazy.read_range("/authored.txt", 0, 32).await?.as_ref(), b"late source acknowledgement");
        let acknowledged_prefix = publication_work.receipt();
        publication_cancellation.cancel();
        let late_driver = mount.revalidate_acknowledged_publication::<MountLifecycleError>(
            &publication_native, qualified_ack,
        )
            .err().ok_or("cancelled native revalidation erased a real source acknowledgement")?;
        assert!(late_driver.source_acknowledged);
        assert!(late_driver.accounting_failure.is_none());
        assert_eq!(late_driver.publication, Some(qualified_ack));
        assert!(matches!(late_driver.cause, MountLifecycleError::Native(_)));
        acknowledged_prefix.verify(publication_work.receipt())?;
        assert!(mount.session.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_some());
        assert_eq!(
            mount.snapshot_measured(crate::WorkBudget::UNBOUNDED, &cancellation)
                .await.map_err(|(error, _)| error)?.value,
            acknowledged,
        );
        let actual = mount.snapshot_measured(crate::WorkBudget::UNBOUNDED, &cancellation)
            .await.map_err(|(error, _)| error)?;
        mount.snapshot_measured(actual.work, &cancellation)
            .await.map_err(|(error, _)| error)?.work.verify(actual.work)?;
        let imported_path = NamespacePath::new(vec![crate::kernel::LogicalName::new(
            crate::kernel::NameEncoding::Utf8, b"selected-import.txt".to_vec(),
            VolumeLimits::default().maximum_component_bytes,
        )?], VolumeLimits::default())?;
        let operation = crate::OperationId::new();
        let child = lazy.fork("selected-import-child", IdempotencyKey::new()).await?;
        child.workspace().write_text("/selected-import.txt", "selected SDK import").await?;
        let child_snapshot = child.snapshot_measured(crate::WorkBudget::UNBOUNDED, &cancellation).await?.value;
        let plan = child.prepare_selected_import_measured(
            child_snapshot, std::slice::from_ref(&imported_path), crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await?.value;
        let actual = mount.snapshot_measured(crate::WorkBudget::UNBOUNDED, &cancellation)
            .await.map_err(|(error, _)| error)?;
        let imported_head = lazy.workspace().head().await?.id();
        let pending = mount.selected_import_snapshot_measured(
            operation, crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await.map_err(|(error, _)| error)?;
        assert!(pending.value.is_none());
        assert_eq!(lazy.workspace().head().await?.id(), imported_head);
        let denied_import = mount.import_selected_measured_guarded(
            actual.value, plan, operation,
            PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &cancellation,
            |_original| std::future::ready(Err::<(), _>(MountLifecycleError::Source(MountSourceError::Stale))),
        ).await.err().ok_or("selected import bypassed its original owner guard")?;
        assert_ne!(denied_import.1, crate::WorkCounters::UNCHARGED);
        assert_eq!(lazy.workspace().head().await?.id(), imported_head);
        let selected_work = customer_native_work(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let (plan, plan_custody) = child.prepare_selected_import_with_native_work(
            child_snapshot, std::slice::from_ref(&imported_path), &selected_work,
        ).await.map_err(|(error, _)| error)?;
        assert!(plan_custody.work().shares_ledger(&selected_work));
        assert!(plan_custody.bytes() >= plan.logical_owned_bytes());
        let selected_native = selected_work.native_mount_work();
        let before_unpolled = selected_work.receipt();
        let unpolled = mount.import_selected_work_guarded(
            actual.value, plan, plan_custody, operation,
            PublicationPermit::Unrestricted, &selected_native, &cancellation,
            |_original| std::future::ready(Ok::<(), MountLifecycleError>(())),
        );
        drop(unpolled);
        assert_eq!(selected_work.receipt(), before_unpolled);
        assert_eq!(lazy.workspace().head().await?.id(), imported_head);
        let (plan, plan_custody) = child.prepare_selected_import_with_native_work(
            child_snapshot, std::slice::from_ref(&imported_path), &selected_work,
        ).await.map_err(|(error, _)| error)?;
        let foreign_native = foreign_work.native_mount_work();
        let before_foreign_import = foreign_work.receipt();
        assert!(matches!(
            mount.import_selected_work_guarded(
                actual.value, plan, plan_custody, operation,
                PublicationPermit::Unrestricted, &foreign_native, &cancellation,
                |_original| std::future::ready(Ok::<(), MountLifecycleError>(())),
            ).await,
            Err(MountLifecycleError::Native(NativeMountError::Materialization(
                crate::NativeTreeMaterializationError::InvalidLayout,
            )))
        ));
        assert_eq!(foreign_work.receipt(), before_foreign_import);
        assert_eq!(lazy.workspace().head().await?.id(), imported_head);
        let (plan, plan_custody) = child.prepare_selected_import_with_native_work(
            child_snapshot, std::slice::from_ref(&imported_path), &selected_work,
        ).await.map_err(|(error, _)| error)?;
        let imported = mount.import_selected_work_guarded(
            actual.value, plan, plan_custody, operation,
            PublicationPermit::Unrestricted, &selected_native, &cancellation,
            |_original| std::future::ready(Ok::<(), MountLifecycleError>(())),
        ).await?;
        selected_work.receipt().verify(crate::WorkBudget::UNBOUNDED)?;
        assert_eq!(imported.authored_generation, lazy.workspace().head().await?.id());
        let completed = mount.selected_import_snapshot_measured(
            operation, crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await.map_err(|(error, _)| error)?;
        assert_eq!(completed.value, Some(imported));
        let repeated = mount.selected_import_snapshot_measured(operation, completed.work, &cancellation)
            .await.map_err(|(error, _)| error)?;
        repeated.work.verify(completed.work)?;
        assert_eq!(repeated.value, completed.value);
        assert_eq!(lazy.read_range("/selected-import.txt", 0, 32).await?.as_ref(), b"selected SDK import");
        assert_eq!(lazy.read_range("/source.txt", 0, 32).await?.as_ref(), b"source");
        let no_detach = crate::WorkBudget {
            backend_write_operations: 0, ..crate::WorkBudget::UNBOUNDED
        };
        let denied_detach = mount.abandon_measured(no_detach, &cancellation)
            .err().ok_or("zero native-write allowance detached a live namespace")?;
        denied_detach.1.verify(no_detach)?;
        assert_eq!(denied_detach.1.backend_write_operations, 0);
        {
            let owner = mount.session.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let state = owner.as_ref().ok_or("denied detach lost cleanup custody")?.cleanup_state();
            assert!(state.driver_retained);
            assert!(state.destination_fence_retained);
            assert!(!state.namespace_detach_confirmed);
        }
        let cancelled = crate::CancellationToken::new();
        cancelled.cancel();
        let denied_cleanup = mount.abandon_measured(crate::WorkBudget::UNBOUNDED, &cancelled)
            .err().ok_or("cancelled cleanup discarded the live session")?;
        assert_eq!(denied_cleanup.1, crate::WorkCounters::UNCHARGED);
        {
            let owner = mount.session.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let state = owner.as_ref().ok_or("cleanup failure lost its session")?.cleanup_state();
            assert!(state.driver_retained);
            assert!(state.destination_fence_retained);
            assert!(state.measured_cleanup_failed);
            assert!(!state.cleanup_complete);
        }
        let original_mount_work = mount._destination_custody.work().clone();
        let denied_wait = mount.retire_with_work(&cancelled)
            .err().ok_or("cancelled wait minted a physical drain proof")?;
        assert!(matches!(denied_wait, MountLifecycleError::Native(_)));
        {
            let owner = mount.session.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let state = owner.as_ref().ok_or("cancelled wait lost physical cleanup ownership")?.cleanup_state();
            assert!(state.driver_retained);
            assert!(state.destination_fence_retained);
            assert!(!state.cleanup_complete);
        }
        original_mount_work.cancellation().cancel();
        let before_retire = original_mount_work.receipt();
        let drained = mount.retire_with_work(&crate::CancellationToken::new())?;
        assert!(original_mount_work.receipt().backend_write_operations > before_retire.backend_write_operations);
        assert!(mount.source.physically_retired()?);
        assert!(mount.session.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_none());
        assert!(mount.native_source_io_lease().is_err());
        let owners = Arc::strong_count(&mount.source);
        let before_replay = original_mount_work.receipt();
        let repeated = mount.retire_with_work(&cancelled)?;
        assert_eq!(Arc::strong_count(&mount.source), owners + 1);
        assert_eq!(original_mount_work.receipt(), before_replay);
        drop(repeated);
        drop(drained);
        Ok(())
    }

    #[tokio::test]
    async fn witnessed_attachment_preadmits_carriers_and_bounds_initial_capture_cumulatively()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        crate::initialize_native_mount_services()?;
        let fs = Fs::memory();
        let workspace = fs.create_workspace("native-carrier-admission").await?;
        let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
        transaction.create_directory("/selected-é").await?;
        transaction.commit().await?;
        let generation = workspace.head().await?;
        let directory = tempfile::tempdir()?;
        let options = crate::MaterializeOptions::native(directory.path());
        let mount = MountOptions::read_only().subdirectory("/selected-é");
        let cancellation = crate::CancellationToken::new();
        let prepared = generation.prepare_native_working_set_with_budgets(
            &mount, &options, crate::WorkBudget::UNBOUNDED,
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await.map_err(|(error, _)| error)?;
        let identity = prepared.value.source_identity;
        let rejected_capture = generation.attach_native_working_set_with_budgets(
            &mount, &options, identity, crate::WorkBudget::UNBOUNDED,
            crate::WorkBudget::default(), &cancellation,
        ).await.err().ok_or("zero-budget attachment capture succeeded")?;
        assert!(matches!(
            rejected_capture.0,
            MountLifecycleError::Source(MountSourceError::Work(crate::WorkError::BudgetExceeded { .. }))
                | MountLifecycleError::Work(crate::WorkError::BudgetExceeded { .. })
        ));
        let preparation = rejected_capture.1;
        assert_eq!(preparation.materializations, 0);
        assert!(preparation.allocation_operations >= 7);
        assert!(preparation.peak_allocation_bytes >=
            (std::mem::size_of_val(prepared.value.source.as_ref())
                + std::mem::size_of_val(prepared.value.baseline.as_ref())) as u64);
        let mut finite_preparation = crate::WorkBudget::UNBOUNDED;
        finite_preparation.allocation_operations = preparation.allocation_operations
            .checked_sub(1).ok_or("attachment omitted carrier allocations")?;
        let rejected_carrier = generation.attach_native_working_set_with_budgets(
            &mount, &options, identity, finite_preparation,
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await.err().ok_or("underfunded attachment carriers succeeded")?;
        assert!(matches!(
            rejected_carrier.0,
            MountLifecycleError::Workspace(WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                counter: "allocation_operations", ..
            }))
                | MountLifecycleError::Work(crate::WorkError::BudgetExceeded {
                    counter: "allocation_operations", ..
                })
        ));
        assert_eq!(rejected_carrier.1.allocation_operations, finite_preparation.allocation_operations);
        assert_eq!(rejected_carrier.1.materializations, 0);
        let rejected_initial_capture = generation.attach_native_working_set_with_budgets(
            &mount, &options, identity, preparation,
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await.err().ok_or("initial capture reset its preparation allowance")?;
        assert!(matches!(
            rejected_initial_capture.0,
            MountLifecycleError::Source(MountSourceError::Work(crate::WorkError::BudgetExceeded { .. }))
                | MountLifecycleError::Work(crate::WorkError::BudgetExceeded { .. })
        ));
        rejected_initial_capture.1.verify(preparation)?;
        assert_eq!(rejected_initial_capture.1.materializations, 0);
        let admitted = generation.attach_native_working_set_with_budgets(
            &mount, &options, identity, crate::WorkBudget::UNBOUNDED,
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await.map_err(|(error, _)| error)?;
        let exact = admitted.work;
        drop(admitted.value);
        let attached = generation.attach_native_working_set_with_budgets(
            &mount, &options, identity, exact,
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await.map_err(|(error, _)| error)?;
        attached.work.verify(exact)?;
        assert_eq!(attached.value.capture_budget, crate::WorkBudget::UNBOUNDED);
        assert_eq!(attached.value.source_identity, identity);
        assert!(attached.work.allocation_operations > preparation.allocation_operations);
        assert_eq!(attached.value.source_root, options.destination);
        assert_eq!(attached.value.selected_root, Path::new("selected-é"));
        assert_eq!(std::fs::read_dir(directory.path().join("selected-é"))?.count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn pinned_native_sources_cannot_publish_and_destinations_preserve_concurrent_head()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        crate::initialize_native_mount_services()?;
        let fs = Fs::memory();
        let workspace = fs.create_workspace("pinned-native-view").await?;
        workspace.write_text("/file.txt", "original").await?;
        let admitted = workspace.head().await?;
        workspace.write_text("/file.txt", "concurrent").await?;
        let concurrent = workspace.head().await?.id();
        let source_root = tempfile::tempdir()?;
        let source = admitted
            .prepare_native_working_set(
                &MountOptions::read_only(),
                &crate::MaterializeOptions::native(source_root.path()),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await.map_err(|(error, _)| error)?
            .value;
        source.validate_for_presentation().await?;
        source.verify_unchanged().await?;
        let capture_token = crate::CancellationToken::new();
        let capture_work = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &capture_token,
        ).map_err(|(error, _)| error)?;
        let caller_payload = capture_work.claim(64)?;
        let mut caller_buffer = Vec::<u8>::new();
        caller_buffer.try_reserve_exact(64)?;
        assert_eq!(caller_payload.bytes(), caller_buffer.capacity() as u64);
        let before_capture = capture_work.receipt();
        source.verify_unchanged_with_work(
            &capture_work.native_mount_work(), &capture_token,
        ).await?;
        assert!(capture_work.receipt().source_bytes_read > before_capture.source_bytes_read);
        assert!(capture_work.live_allocation_bytes() >= caller_payload.bytes());
        assert_eq!(
            capture_work.native_mount_work().native_materialization_work().receipt(),
            capture_work.receipt(),
        );
        let mut denied_budget = crate::WorkBudget::UNBOUNDED;
        denied_budget.items_examined = 0;
        let denied_work = crate::NativeMaterializationWork::new(
            denied_budget, &capture_token,
        ).map_err(|(error, _)| error)?;
        assert!(source.verify_unchanged_with_work(
            &denied_work.native_mount_work(), &capture_token,
        ).await.is_err());
        assert_eq!(denied_work.receipt().source_bytes_read, 0);
        assert_eq!(
            std::fs::read(source_root.path().join("file.txt"))?,
            b"original",
        );
        assert_eq!(
            std::fs::read_to_string(source_root.path().join("file.txt"))?,
            "original"
        );
        assert_eq!(source.generation_id(), admitted.id());
        assert!(
            source
                .sync_with_permit(PublicationPermit::Unrestricted)
                .await
                .is_err()
        );
        std::fs::write(source_root.path().join("file.txt"), "source mutation")?;
        assert!(source.verify_unchanged().await.is_err());
        assert_eq!(workspace.head().await?.id(), concurrent);

        let destination_root = tempfile::tempdir()?;
        let write = MountOptions::read_write().publication(MountPublication::Manual);
        let rejected = admitted
            .prepare_native_working_set(
                &write,
                &crate::MaterializeOptions::native(destination_root.path()),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .err()
            .ok_or("stale native preparation succeeded")?;
        assert!(matches!(
            rejected.0,
            MountLifecycleError::Workspace(WorkspaceError::StaleGeneration)
        ));
        assert!(rejected.1.backend_read_operations > 0);
        let destination = workspace
            .head()
            .await?
            .prepare_native_working_set(
                &write,
                &crate::MaterializeOptions::native(destination_root.path()),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await.map_err(|(error, _)| error)?
            .value;
        std::fs::write(destination_root.path().join("file.txt"), "native edit")?;
        workspace
            .write_text("/file.txt", "newer concurrent")
            .await?;
        assert!(destination.validate_for_presentation().await.is_err());
        assert!(
            destination
                .sync_with_permit(PublicationPermit::Unrestricted)
                .await
                .is_err()
        );
        assert_eq!(
            workspace.read("/file.txt", 64).await?.as_ref(),
            b"newer concurrent"
        );
        Ok(())
    }

    #[tokio::test]
    async fn prepared_subtree_captures_native_edit_and_rejects_stale_head()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        crate::initialize_native_mount_services()?;
        let source = tempfile::tempdir()?;
        std::fs::create_dir(source.path().join("hot"))?;
        std::fs::write(source.path().join("hot/file.txt"), b"base")?;
        std::fs::create_dir(source.path().join("hot/sub"))?;
        std::fs::write(source.path().join("hot/sub/child.txt"), b"old child")?;
        let native_view = tempfile::tempdir()?;
        let fs = Fs::memory();
        let demand = Arc::new(
            NativeDemandSource::open(
                source.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await?,
        );
        let lazy = LazyWorkspace::attach(
            &fs,
            "prepared-native-working-set",
            demand,
            MemoryLazyWorkspaceStore::default(),
        )
        .await?;
        let before_invalid = lazy.workspace().head().await?.id();
        assert!(matches!(
            lazy.prepare_native_working_set(
                "/hot",
                &crate::MaterializeOptions::native("relative-native-view"),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                PublicationPermit::Unrestricted,
            )
            .await.map_err(|(error, _)| error),
            Err(MountLifecycleError::NativePolicy("native working-set root must be absolute"))
        ));
        assert_eq!(lazy.workspace().head().await?.id(), before_invalid);
        let mut working_set = lazy
            .prepare_native_working_set(
                "/hot",
                &crate::MaterializeOptions::native(native_view.path()),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                PublicationPermit::Unrestricted,
            )
            .await.map_err(|(error, _)| error)?.value;
        let generation = working_set.expected_generation;
        let working = native_view.path();
        working_set.validate_for_presentation().await?;
        working_set.capture_budget = crate::WorkBudget::default();
        assert!(
            working_set
                .capture_host_subtree(&MountPath::root())
                .await
                .is_err()
        );
        assert_eq!(lazy.workspace().head().await?.id(), generation);
        working_set.capture_budget = crate::WorkBudget::UNBOUNDED;
        let name = if cfg!(windows) {
            "file.txt"
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect()
        } else {
            b"file.txt".to_vec()
        };
        let unchanged_head = lazy.workspace().head().await?.id();
        working_set
            .sync_with_permit(PublicationPermit::Unrestricted)
            .await?;
        assert_eq!(lazy.workspace().head().await?.id(), unchanged_head);
        #[cfg(unix)]
        let canonical_before = lazy
            .workspace()
            .head()
            .await?
            .stat("/hot/file.txt")
            .await?
            .metadata;
        #[cfg(unix)]
        {
            working_set
                .capture_host_paths(&[MountPath::root().child(name.clone())])
                .await?;
            working_set
                .sync_with_permit(PublicationPermit::Unrestricted)
                .await?;
            let canonical_after = lazy
                .workspace()
                .head()
                .await?
                .stat("/hot/file.txt")
                .await?
                .metadata;
            assert_eq!(canonical_after.changed_ns, canonical_before.changed_ns);
            #[cfg(target_os = "linux")]
            assert_eq!(canonical_after.created_ns, canonical_before.created_ns);
        }
        #[cfg(target_os = "linux")]
        {
            let accessed = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
            std::fs::File::open(working.join("hot/file.txt"))?
                .set_times(std::fs::FileTimes::new().set_accessed(accessed))?;
            working_set
                .sync_with_permit(PublicationPermit::Unrestricted)
                .await?;
            let canonical = lazy
                .workspace()
                .head()
                .await?
                .stat("/hot/file.txt")
                .await?
                .metadata;
            assert_eq!(canonical.accessed_ns, Some(1_700_000_000_000_000_000));
        }
        std::fs::write(working.join("hot/file.txt"), b"native-edit")?;
        let before_guard = lazy.workspace().head().await?.id();
        let denied_publication = customer_native_work(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let denied = working_set.sync_with_permit_with_work(
            PublicationPermit::Unrestricted, &denied_publication.native_mount_work(), &cancellation,
            |_original: &crate::NativeMaterializationWork| std::future::ready(Err::<(), _>(
                MountLifecycleError::NativePolicy("original materialized owner denied publication"),
            )),
        ).await.err().ok_or("materialized publication ignored its original owner guard")?;
        assert!(!denied.source_acknowledged);
        assert!(denied.accounting_failure.is_none());
        assert!(denied.publication.is_none());
        assert!(matches!(denied.cause, MountLifecycleError::NativePolicy(
            "original materialized owner denied publication",
        )));
        assert!(denied_publication.receipt().items_examined > 0);
        denied_publication.receipt().verify(crate::WorkBudget::UNBOUNDED)?;
        assert_eq!(lazy.workspace().head().await?.id(), before_guard);
        working_set
            .capture_host_paths(&[MountPath::root().child(name)])
            .await?;
        let publication = customer_native_work(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let guard_original = publication.clone();
        let published = working_set.sync_with_permit_with_work(
            PublicationPermit::Unrestricted, &publication.native_mount_work(), &cancellation,
            move |actual: &crate::NativeMaterializationWork| {
                assert!(actual.shares_ledger(&guard_original));
                assert!(actual.shares_resource_allowance(&guard_original));
                std::future::ready(Ok::<(), MountLifecycleError>(()))
            },
        ).await?;
        assert!(published.operation().is_some());
        assert!(!published.snapshot().is_lazy());
        assert_eq!(published.snapshot().workspace_id(), working_set.workspace.id());
        assert_eq!(published.snapshot().volume_config(), working_set.workspace.volume_config());
        assert_eq!(published.snapshot().generation(), working_set.workspace.head().await?.id());
        assert!(publication.receipt().items_examined > 0);
        assert_eq!(
            lazy.workspace().read("/hot/file.txt", 32).await?.as_ref(),
            b"native-edit"
        );
        std::fs::write(working.join("hot/created.txt"), b"new")?;
        working_set.capture_host_subtree(&MountPath::root()).await?;
        working_set
            .sync_with_permit(PublicationPermit::Unrestricted)
            .await?;
        assert_eq!(
            lazy.workspace()
                .read("/hot/created.txt", 16)
                .await?
                .as_ref(),
            b"new"
        );
        // The publication boundary must capture writes even when the caller
        // receives no change notification and supplies no path hint.
        std::fs::write(working.join("hot/unreported.txt"), b"unreported")?;
        working_set
            .sync_with_permit(PublicationPermit::Unrestricted)
            .await?;
        assert_eq!(
            lazy.workspace()
                .read("/hot/unreported.txt", 32)
                .await?
                .as_ref(),
            b"unreported"
        );
        std::fs::remove_file(working.join("hot/unreported.txt"))?;
        working_set
            .sync_with_permit(PublicationPermit::Unrestricted)
            .await?;
        assert!(
            lazy.workspace()
                .read("/hot/unreported.txt", 32)
                .await
                .is_err()
        );
        std::fs::remove_dir_all(working.join("hot/sub"))?;
        std::fs::write(working.join("hot/sub"), b"replacement")?;
        working_set.capture_host_subtree(&MountPath::root()).await?;
        working_set
            .sync_with_permit(PublicationPermit::Unrestricted)
            .await?;
        assert_eq!(
            lazy.workspace().read("/hot/sub", 32).await?.as_ref(),
            b"replacement"
        );
        assert!(
            lazy.workspace()
                .read("/hot/sub/child.txt", 32)
                .await
                .is_err()
        );
        lazy.write("/later.txt", bytes::Bytes::from_static(b"later"))
            .await?;
        lazy.write("/hot/conflict.txt", bytes::Bytes::from_static(b"parent"))
            .await?;
        std::fs::write(working.join("hot/conflict.txt"), b"child")?;
        working_set.capture_host_subtree(&MountPath::root()).await?;
        assert!(matches!(
            working_set
                .sync_with_permit(PublicationPermit::Unrestricted)
                .await,
            Err(MountLifecycleError::Source(MountSourceError::Stale))
        ));
        assert_eq!(
            lazy.workspace()
                .read("/hot/conflict.txt", 16)
                .await?
                .as_ref(),
            b"parent"
        );
        assert!(matches!(
            working_set.validate_for_presentation().await,
            Err(MountLifecycleError::Workspace(
                WorkspaceError::StaleGeneration
            ))
        ));
        assert_ne!(lazy.workspace().head().await?.id(), generation);
        let latest_view = tempfile::tempdir()?;
        let ready = lazy
            .prepare_native_working_set(
                "/hot",
                &crate::MaterializeOptions::native(latest_view.path()),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                PublicationPermit::Unrestricted,
            )
            .await.map_err(|(error, _)| error)?.value;
        ready.validate_for_presentation().await?;
        assert_eq!(
            std::fs::read(latest_view.path().join("hot/file.txt"))?,
            b"native-edit"
        );
        std::fs::rename(
            latest_view.path(),
            source.path().join("retired-native-view"),
        )?;
        std::fs::create_dir(latest_view.path())?;
        std::fs::create_dir(latest_view.path().join("hot"))?;
        assert!(matches!(
            ready.validate_for_presentation().await,
            Err(MountLifecycleError::Source(MountSourceError::Stale))
        ));
        let cancelled_wait = crate::CancellationToken::new();
        cancelled_wait.cancel();
        assert!(matches!(
            working_set.retire_with_work(&cancelled_wait),
            Err(MountLifecycleError::Source(MountSourceError::Cancelled))
        ));
        assert!(!working_set.source.capture_retired());
        let lease_owners = Arc::strong_count(&working_set.source);
        let source_io = working_set.native_source_io_lease()?;
        assert_eq!(Arc::strong_count(&working_set.source), lease_owners + 1);
        let source_io_tail = source_io.clone();
        assert_eq!(Arc::strong_count(&working_set.source), lease_owners + 1);
        drop(source_io);
        assert_eq!(Arc::strong_count(&working_set.source), lease_owners + 1);
        drop(source_io_tail);
        assert_eq!(Arc::strong_count(&working_set.source), lease_owners);
        // Revocation is not a new allowance: cleanup spends the source's
        // already accepted birth credit, independently of the wait token.
        working_set.source.cancellation().cancel();
        let owners = Arc::strong_count(&working_set.source);
        let drained = working_set.retire_with_work(&crate::CancellationToken::new())?;
        assert!(working_set.source.capture_retired());
        assert!(working_set.native_source_io_lease().is_err());
        assert_eq!(Arc::strong_count(&working_set.source), owners + 1);
        let repeated = working_set.retire_with_work(&cancelled_wait)?;
        assert_eq!(Arc::strong_count(&working_set.source), owners + 2);
        drop(repeated);
        drop(drained);
        assert_eq!(Arc::strong_count(&working_set.source), owners);
        let ready_drained = ready.retire_with_work(&crate::CancellationToken::new())?;
        assert!(ready.source.capture_retired());
        drop(ready_drained);
        Ok(())
    }
}
