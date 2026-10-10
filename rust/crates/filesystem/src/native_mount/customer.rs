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
    subdirectory: String,
    publication: MountPublication,
}

impl MountOptions {
    /// Creates a writable tracking-safe sparse mount with close/fsync publication.
    #[must_use]
    pub fn read_write() -> Self {
        Self {
            writable: true,
            subdirectory: "/".to_owned(),
            publication: MountPublication::CloseAndSync,
        }
    }

    /// Creates a pinned immutable mount.
    #[must_use]
    pub fn read_only() -> Self {
        Self {
            writable: false,
            subdirectory: "/".to_owned(),
            publication: MountPublication::Manual,
        }
    }

    /// Projects one exact workspace directory as the native mount root.
    #[must_use]
    pub fn subdirectory(mut self, path: impl Into<String>) -> Self {
        self.subdirectory = path.into();
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
    preparation_control_bytes: u64,
    preparation_captured_data_bytes: u64,
}

struct AbortTaskOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortTaskOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
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

    /// Actual retained storage recorded by the preparation constructor's ledger.
    /// This excludes newly captured proof payloads; those require the canonical
    /// capture output's separate ownership proof, not a guessed map footprint.
    #[must_use]
    pub const fn preparation_control_bytes(&self) -> u64 {
        self.preparation_control_bytes
    }

    /// Actual retained proof path/name buffers from the original capture.
    /// Immutable content bodies are already stored, not owned by this view;
    /// opaque BTree node storage is not inferred from entry counts.
    #[must_use]
    pub const fn preparation_captured_data_bytes(&self) -> u64 {
        self.preparation_captured_data_bytes
    }

    /// Physical volume-root directory owned by this prepared view.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.source_root
    }

    /// Revalidates a prepared native view immediately before a quiescent
    /// presentation switch. This is optimistic admission, not a lock on the
    /// workspace head: publication remains fenced by the operation permit.
    /// It reads only the selected directory metadata and the current head.
    pub async fn validate_for_presentation(&self) -> Result<(), MountLifecycleError> {
        self.validate_for_presentation_measured(self.capture_budget, self.source.cancellation())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Revalidates the current fence and physical root under one exact allowance.
    pub async fn validate_for_presentation_measured(
        &self,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<()>, MountLifecycleError> {
        let mut work = crate::WorkCounters::default();
        let result = async {
            cancellation.check().map_err(|_| MountLifecycleError::Source(MountSourceError::Cancelled))?;
            if self.writable {
                let head = self.workspace.head_measured(work.remaining(budget)?, cancellation).await
                    .map_err(|failure| preparation_failure(failure, &mut work, MountLifecycleError::Workspace))?;
                work = work.checked_add(head.work)?;
                if head.value.id() != self.expected_generation {
                    return Err(MountLifecycleError::Workspace(WorkspaceError::StaleGeneration));
                }
            }
            let checked = prepare_root_identity(
                &self.source_root,
                &self.selected_root,
                work.remaining(budget)?,
                cancellation,
                Some(self.source_identity),
            ).map_err(|failure| preparation_failure(
                failure, &mut work, std::convert::identity,
            ))?;
            work = work.checked_add(checked.work)?;
            if self.writable {
                let head = self.workspace.head_measured(work.remaining(budget)?, cancellation).await
                    .map_err(|failure| preparation_failure(failure, &mut work, MountLifecycleError::Workspace))?;
                work = work.checked_add(head.work)?;
                if head.value.id() != self.expected_generation {
                    return Err(MountLifecycleError::Workspace(WorkspaceError::StaleGeneration));
                }
            }
            cancellation.check().map_err(|_| MountLifecycleError::Source(MountSourceError::Cancelled))?;
            Ok(())
        }.await;
        result.map(|value| crate::OperationReceipt { value, work })
            .map_err(|error| crate::OperationFailure::new(error, work))
    }

    /// Captures only the supplied changed paths from a native working directory.
    ///
    /// The source root is authenticated by the core capture path. This is a
    /// path hint, not proof that other native writes did not occur. Call this
    /// at an operation boundary, then publish with [`Self::sync_with_permit`].
    pub async fn capture_host_paths(&self, paths: &[MountPath]) -> Result<(), MountLifecycleError> {
        self.capture_host_paths_measured(paths, self.capture_budget, self.source.cancellation())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Captures selected paths through the original capture gate and budget.
    pub async fn capture_host_paths_measured(
        &self,
        paths: &[MountPath],
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<()>, MountLifecycleError> {
        self.source.capture_host_with_identity_budgeted(
            &self.source_root, paths, self.source_identity, HostCaptureScope::ExactPaths,
            Arc::clone(&self.baseline), budget, cancellation,
        ).await.map(|receipt| crate::OperationReceipt { value: (), work: receipt.work })
            .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::default(), MountLifecycleError::Source))
    }

    /// Reconciles a complete selected subtree after a watcher overflow or
    /// missed event. Both host and checkout descendants participate, so
    /// creations and deletions are captured without scanning sibling trees.
    pub async fn capture_host_subtree(&self, root: &MountPath) -> Result<(), MountLifecycleError> {
        self.capture_host_subtree_measured(root, self.capture_budget, self.source.cancellation())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Reconciles the complete physical subtree, retaining failed capture work.
    pub async fn capture_host_subtree_measured(
        &self,
        root: &MountPath,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<()>, MountLifecycleError> {
        self.source.capture_host_with_identity_budgeted(
            &self.source_root, std::slice::from_ref(root), self.source_identity,
            HostCaptureScope::Subtree, Arc::clone(&self.baseline), budget, cancellation,
        ).await.map(|receipt| crate::OperationReceipt { value: (), work: receipt.work })
            .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::default(), MountLifecycleError::Source))
    }

    /// Reconciles the selected subtree and publishes under an active, fenced
    /// operation permit. Path hints alone cannot establish completeness: a
    /// missing or delayed notification must not silently omit a native write.
    pub async fn sync_with_permit(
        &self,
        permit: crate::PublicationPermit,
    ) -> Result<(), MountLifecycleError> {
        self.sync_with_permit_measured(permit, self.capture_budget, self.source.cancellation())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Captures and publishes one candidate without resetting its work budget.
    pub async fn sync_with_permit_measured(
        &self,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<()>, MountLifecycleError> {
        let mut work = crate::WorkCounters::default();
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let mut cancellation_admission = crate::demand::DemandCancellationRoots::default();
        let result = async {
            if !self.writable {
                return Err(MountLifecycleError::Source(MountSourceError::Invalid(
                    "read-only native view cannot publish".into(),
                )));
            }
            cancellation.check().map_err(|_| MountLifecycleError::Source(MountSourceError::Cancelled))?;
            let current = preparation_wait_measured(
                self.source.shared_checkout(), cancellation, Some(self.source.cancellation()),
                &mut allocations, &mut work, &mut cancellation_admission, budget,
                |checkout| checkout.generation_id(),
            ).await?;
            let head = self.workspace.head_measured(
                preparation_remaining_with_live(work, budget, allocations.live_bytes())?, cancellation,
            ).await.map_err(|failure| preparation_failure_with_live(
                failure, &mut work, allocations.live_bytes(), MountLifecycleError::Workspace,
            ))?;
            work = work.with_backend(head.work, allocations.live_bytes())?;
            if head.value.id() != current {
                return Err(MountLifecycleError::Source(MountSourceError::Stale));
            }
            drop(head.value);
            let captured = self.capture_host_subtree_measured(
                &MountPath::root(),
                preparation_remaining_with_live(work, budget, allocations.live_bytes())?, cancellation,
            ).await.map_err(|failure| preparation_failure_with_live(
                failure, &mut work, allocations.live_bytes(), std::convert::identity,
            ))?;
            work = work.with_backend(captured.work, allocations.live_bytes())?;
            let published = self.source.sync_async_with_permit_measured(
                permit,
                preparation_remaining_with_live(work, budget, allocations.live_bytes())?, cancellation,
            ).await.map_err(|failure| preparation_failure_with_live(
                failure, &mut work, allocations.live_bytes(), MountLifecycleError::Source,
            ))?;
            work = work.with_backend(published.work, allocations.live_bytes())?;
            Ok(())
        }.await;
        result.map(|value| crate::OperationReceipt { value, work })
            .map_err(|error| crate::OperationFailure::new(error, work))
    }

    /// Checks the complete selected physical subtree against its admitted
    /// generation without publishing. Host permissions are not a sandbox:
    /// an approved process may mutate the view, but cannot publish source edits.
    pub async fn verify_unchanged(&self) -> Result<(), MountLifecycleError> {
        self.verify_unchanged_measured(self.capture_budget, self.source.cancellation())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Captures the admitted source without allowing publication of its edits.
    pub async fn verify_unchanged_measured(
        &self,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<()>, MountLifecycleError> {
        let captured = self.capture_host_subtree_measured(&MountPath::root(), budget, cancellation).await?;
        let mut work = captured.work;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let mut cancellation_admission = crate::demand::DemandCancellationRoots::default();
        let pending = preparation_wait_measured(
            self.source.shared_checkout(), cancellation, Some(self.source.cancellation()),
            &mut allocations, &mut work, &mut cancellation_admission, budget,
            |checkout| checkout.has_pending_mutations(),
        ).await.map_err(|error| crate::OperationFailure::new(error, work))?;
        if pending {
            return Err(crate::OperationFailure::new(MountLifecycleError::Source(MountSourceError::Invalid(
                "native source view changed after admission".into(),
            )), work));
        }
        Ok(crate::OperationReceipt { value: (), work })
    }
}

#[allow(clippy::too_many_arguments, reason = "one original work/ledger/admission and distinct cancellation authorities")]
async fn preparation_wait_measured<A: AsyncAuthorityStore, O: AsyncObjectStore, T>(
    checkout: &SharedCheckout<A, O>,
    cancellation: &crate::CancellationToken,
    lifetime: Option<&crate::CancellationToken>,
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut crate::WorkCounters,
    cancellation_admission: &mut crate::demand::DemandCancellationRoots,
    budget: crate::WorkBudget,
    select: impl FnOnce(&super::SharedCheckoutState<A, O>) -> T,
) -> Result<T, MountLifecycleError> {
    // Move the existing meter into the shared lock engine; never create an
    // independent ledger or lose the original token's registry-backing proof.
    let mut meter = crate::demand::DemandWork::from_current(
        budget, cancellation, std::mem::take(work), std::mem::take(allocations),
        std::mem::take(cancellation_admission),
    );
    let mut view = super::view_ledger::ViewLedgerAdmission::default();
    let result = checkout.lock_measured(&mut meter, &mut view, cancellation, lifetime)
        .await.map(|checkout| select(&checkout));
    let (current_work, current_allocations, current_admission) = meter.into_current();
    *work = current_work;
    *allocations = current_allocations;
    *cancellation_admission = current_admission;
    result.map_err(MountLifecycleError::Source)
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
            || std::future::ready(Ok(())),
        ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Flushes actual callbacks and checks the original owner before every seal.
    ///
    /// A failed flush, settlement or guard retains the original publication
    /// owner and the complete work prefix. The callback supplies freshness,
    /// never a replacement permit or a synthetic lazy snapshot.
    pub async fn sync_with_permit_measured_guarded<F, Fut>(
        &self,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        mut before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<()>, MountLifecycleError>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), MountSourceError>> + Send,
    {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        let result = async {
            work.check().map_err(native_demand_lifecycle_error)?;
            with_live_session(&self.session, |session| {
                session.flush_callbacks_with_work(&mut super::NativeMountWork { work: &mut work })
            })?;
            self.source.sync_async_with_permit_work_guarded(
                permit, &mut work, cancellation, &mut before_publication,
            ).await.map_err(MountLifecycleError::Source)?;
            with_live_session(&self.session, |session| {
                session.revalidate_with_work(&mut super::NativeMountWork { work: &mut work })
            })
        }.await;
        result.map(|value| crate::OperationReceipt { value, work: work.work })
            .map_err(|error| crate::OperationFailure::new(error, work.work))
    }

    /// Imports an original selected mutation through this mount's SDK owner.
    ///
    /// The source authenticates the supplied destination and requested paths
    /// and guards every publication with the original permit. No private
    /// source handle or alternate publication engine is exposed.
    #[allow(clippy::too_many_arguments, reason = "retains the original selection, operation, permit, work bounds and owner guard")]
    pub async fn import_selected_measured_guarded<F, Fut>(
        &self,
        destination: crate::LazySnapshotRef,
        requested: &[NamespacePath],
        selected: Vec<crate::AuthoredMutation>,
        operation: crate::OperationId,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        mut before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::LazySnapshotRef>, MountLifecycleError>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), MountSourceError>> + Send,
    {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        let result = async {
            work.check().map_err(native_demand_lifecycle_error)?;
            let mut input_bytes = selected.capacity().checked_mul(std::mem::size_of::<crate::AuthoredMutation>())
                .ok_or(crate::WorkError::Overflow)? as u64;
            for mutation in &selected {
                work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })
                    .map_err(native_demand_lifecycle_error)?;
                input_bytes = input_bytes.checked_add(mutation.logical_owned_bytes())
                    .ok_or(crate::WorkError::Overflow)?;
            }
            work.adopt(input_bytes).map_err(native_demand_lifecycle_error)?;
            with_live_session(&self.session, |session| {
                session.flush_callbacks_with_work(&mut super::NativeMountWork { work: &mut work })
            })?;
            let snapshot = self.source.import_selected_work_guarded(
                destination, requested, selected, operation, permit, &mut work,
                cancellation, &mut before_publication,
            ).await.map_err(MountLifecycleError::Source)?;
            with_live_session(&self.session, |session| {
                session.revalidate_with_work(&mut super::NativeMountWork { work: &mut work })
            })?;
            Ok(snapshot)
        }.await;
        result.map(|value| crate::OperationReceipt { value, work: work.work })
            .map_err(|error| crate::OperationFailure::new(error, work.work))
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
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::LazySnapshotRef>, MountLifecycleError> {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        let result = async {
            work.check().map_err(native_demand_lifecycle_error)?;
            with_live_session(&self.session, |session| {
                session.flush_callbacks_with_work(&mut super::NativeMountWork { work: &mut work })
            })?;
            with_live_session(&self.session, |session| {
                session.revalidate_with_work(&mut super::NativeMountWork { work: &mut work })
            })?;
            self.source.snapshot_with_work(&mut work, cancellation)
                .await.map_err(MountLifecycleError::Source)
        }.await;
        result.map(|value| crate::OperationReceipt { value, work: work.work })
            .map_err(|error| crate::OperationFailure::new(error, work.work))
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
    ) -> crate::MeasuredResult<crate::OperationReceipt<()>, MountLifecycleError> {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        let mut owner = self.session.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = (|| {
            let Some(session) = owner.as_mut() else { return Ok(()); };
            // Mark finite cleanup custody before any callback admission can fail.
            session.retain_measured_cleanup_owner();
            work.check().map_err(native_demand_lifecycle_error)?;
            session.stop_with_work(&mut super::NativeMountWork { work: &mut work })
                .map_err(MountLifecycleError::Native)?;
            self.source.cancel_with_work(&mut work).map_err(MountLifecycleError::Source)?;
            debug_assert!(session.cleanup_state().cleanup_complete);
            owner.take();
            Ok(())
        })();
        if result.is_err() {
            if let Some(session) = owner.as_mut() {
                session.record_measured_cleanup_failure();
            }
        }
        result.map(|value| crate::OperationReceipt { value, work: work.work })
            .map_err(|error| crate::OperationFailure::new(error, work.work))
    }

    /// Synchronously publishes and detaches for foreign-runtime worker threads.
    pub fn unmount_blocking(&self) -> Result<(), MountLifecycleError> {
        self.sync_blocking()?;
        self.detach()
    }

    fn detach(&self) -> Result<(), MountLifecycleError> {
        #[cfg(target_os = "macos")]
        acyclic_native_runtime::initialize_native_io_services()
            .map_err(NativeMountError::from)?;
        self.abandon_measured(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .map(|receipt| receipt.value).map_err(|failure| failure.error)
    }
}

/// Customer mount admission, publication, and lifecycle failures.
#[derive(Debug, Error)]
pub enum MountLifecycleError {
    /// Workspace path, state, or storage failed admission.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// Lazy source or sparse-overlay admission failed.
    #[error(transparent)]
    Lazy(#[from] LazyWorkspaceError),
    /// Native driver admission or teardown failed.
    #[error(transparent)]
    Native(#[from] NativeMountError),
    /// Native callback publication failed.
    #[error(transparent)]
    Source(#[from] MountSourceError),
    /// Combined exactification and materialization exceeded its work budget.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
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
    ) -> crate::MeasuredResult<crate::OperationReceipt<NativeWorkingSet<A, O>>, MountLifecycleError> {
        if !options.destination.is_absolute() {
            return Err(crate::OperationFailure::before_work(MountLifecycleError::Source(MountSourceError::Invalid(
                "native working-set root must be absolute".to_owned(),
            ))));
        }
        // Keep the cold exactification frame independent of materialization:
        // both are large state machines, and nesting them exhausts the small
        // default stacks used by some host test/runtime threads.
        let lazy = self.clone();
        let selected = subdirectory.to_owned();
        let token = cancellation.clone();
        let context = crate::obs::caller_context();
        let mut task = AbortTaskOnDrop(tokio::spawn(async move {
            crate::obs::in_span(&context, async move {
                lazy.exactify_subtree_with_permit(&selected, budget, &token, permit)
                    .await
            })
            .await
        }));
        let exact = (&mut task.0)
            .await
            .map_err(|error| crate::OperationFailure::before_work(MountLifecycleError::Source(MountSourceError::Engine(error.to_string()))))?
            .map_err(|error| error.into_measured_failure().map_with_prior_work(crate::WorkCounters::default(), MountLifecycleError::Lazy))?;
        let remaining = exact.work.remaining(budget)
            .map_err(|error| crate::OperationFailure::new(MountLifecycleError::Work(error), exact.work))?;
        let prepared = exact
            .value
            .prepare_native_working_set_with_budgets(
                &MountOptions::read_write()
                    .subdirectory(subdirectory)
                    .publication(MountPublication::Manual),
                options,
                remaining,
                budget,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(exact.work, |error| error))?;
        let work = exact.work.checked_add(prepared.work)
            .map_err(|error| crate::OperationFailure::new(MountLifecycleError::Work(error), exact.work))?;
        Ok(crate::OperationReceipt { value: prepared.value, work })
    }
    async fn prepare_authored_mount_source(
        &self,
        options: &MountOptions,
        expected_generation: Option<GenerationId>,
        work: &mut crate::demand::DemandWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<
        (
            Arc<CheckoutMountSource<A, O>>,
            VolumeId,
            NamespacePath,
            String,
        ),
        MountLifecycleError,
    > {
        if let Some(expected) = expected_generation {
            let head = self.workspace().head_measured(
                work.remaining().map_err(native_demand_lifecycle_error)?, cancellation,
            ).await;
            let head = accept_customer_work(work, head, MountLifecycleError::Workspace)?;
            if head.id() != expected {
                return Err(MountLifecycleError::Workspace(WorkspaceError::StaleGeneration));
            }
        }
        let probe = self.workspace().engine_checkout_measured(
            GenerationSelector::Head, CheckoutMode::read_only_pinned(),
            work.remaining().map_err(native_demand_lifecycle_error)?, cancellation,
        ).await;
        let probe = accept_customer_work(work, probe, MountLifecycleError::Workspace)?;
        let probe_bytes = probe.logical_owned_bytes();
        work.adopt(probe_bytes).map_err(native_demand_lifecycle_error)?;
        let config = probe.volume_config();
        drop(probe);
        work.release(probe_bytes).map_err(native_demand_lifecycle_error)?;
        work.check().map_err(native_demand_lifecycle_error)?;
        let root = {
            let (_, allocations, counters, budget) = work.view_gate_admission_parts()
                .map_err(native_demand_lifecycle_error)?;
            crate::workspace::customer_path_measured(
                &options.subdirectory, config, allocations, counters, budget, cancellation,
            )?
        };
        let mut root_length = usize::from(root.is_root());
        for component in root.components() {
            work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })
                .map_err(native_demand_lifecycle_error)?;
            std::str::from_utf8(component.as_bytes()).map_err(|_| {
                MountLifecycleError::Source(MountSourceError::Unsupported(
                    "lazy portable mounts cannot root at a non-UTF-8 path".to_owned(),
                ))
            })?;
            root_length = root_length.checked_add(1).and_then(|length| length.checked_add(component.as_bytes().len()))
                .ok_or(crate::WorkError::Overflow)?;
        }
        let copied = crate::WorkCounters { bytes_copied: root_length as u64, ..crate::WorkCounters::UNCHARGED };
        work.work.admit(&copied, &work.budget)?;
        work.allocate(root_length as u64).map_err(native_demand_lifecycle_error)?;
        let mut root_text = String::with_capacity(root_length);
        for component in root.components() {
            root_text.push('/');
            root_text.push_str(std::str::from_utf8(component.as_bytes()).expect("validated root name"));
        }
        if root.is_root() { root_text.push('/'); }
        work.record(copied).map_err(native_demand_lifecycle_error)?;
        let selected = self.lookup_with_budget(
            &root_text, work.remaining().map_err(native_demand_lifecycle_error)?, cancellation,
        ).await;
        let selected = accept_customer_work(work, selected, MountLifecycleError::Lazy)?;
        let selected_bytes = match &selected {
            LazyLookup::Authored { path, .. } => path.capacity() as u64,
            LazyLookup::Shadow { .. } | LazyLookup::Source(_) => 0,
        };
        work.adopt(selected_bytes).map_err(native_demand_lifecycle_error)?;
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
        if let LazyLookup::Source(node) = selected {
            if expected_generation.is_some() {
                return Err(MountLifecycleError::Workspace(
                    WorkspaceError::StaleGeneration,
                ));
            }
            let expected_source = self.source_file_id(&node);
            work.charge(crate::WorkCounters {
                bytes_hashed: (b"acyclic-fs-lazy-mount-root-v1\0".len() + 16 + root_text.len() + node.version.0.len()) as u64,
                ..crate::WorkCounters::UNCHARGED
            }).map_err(native_demand_lifecycle_error)?;
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"acyclic-fs-lazy-mount-root-v1\0");
            hasher.update(&self.workspace().id().into_bytes());
            hasher.update(root_text.as_bytes());
            hasher.update(&node.version.0);
            let mut key = [0_u8; 16];
            key.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
            let promoted = self.promote_exact_measured(
                &root_text, expected_source, 0, IdempotencyKey::from_bytes(key),
                work.remaining().map_err(native_demand_lifecycle_error)?, cancellation,
            ).await;
            accept_customer_work(work, promoted, MountLifecycleError::Lazy)?;
        }
        drop(selected);
        work.release(selected_bytes).map_err(native_demand_lifecycle_error)?;

        let mode = if options.writable {
            CheckoutMode::tracking_transaction()
        } else {
            CheckoutMode::read_only_pinned()
        };
        let checkout = self.workspace().engine_checkout_measured(
            expected_generation.map_or(GenerationSelector::Head, GenerationSelector::Exact),
            mode, work.remaining().map_err(native_demand_lifecycle_error)?, cancellation,
        ).await;
        let checkout = accept_customer_work(work, checkout, MountLifecycleError::Workspace)?;
        work.adopt(checkout.logical_owned_bytes()).map_err(native_demand_lifecycle_error)?;
        work.allocate(std::mem::size_of::<SharedCheckout<A, O>>() as u64)
            .map_err(native_demand_lifecycle_error)?;
        let shared = Arc::new(SharedCheckout::with_publication(checkout, options.publication));
        let authored_root = super::lazy::mount_clone_namespace(root.components(), config.limits, work)
            .map_err(MountLifecycleError::Source)?;
        let authored = {
            let (_, allocations, counters, budget) = work.view_gate_admission_parts()
                .map_err(native_demand_lifecycle_error)?;
            CheckoutMountSource::new_at_measured(shared, config, authored_root, allocations, counters, budget)?
        };
        work.allocate(std::mem::size_of::<CheckoutMountSource<A, O>>() as u64)
            .map_err(native_demand_lifecycle_error)?;
        let authored = Arc::new(authored);
        if let Some(expected) = expected_generation {
            let head = self.workspace().head_measured(
                work.remaining().map_err(native_demand_lifecycle_error)?, cancellation,
            ).await;
            let head = accept_customer_work(work, head, MountLifecycleError::Workspace)?;
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
        self.mount_measured(destination, options, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Mounts the same real lazy source under one original operation allowance.
    ///
    /// Initialize native I/O and native mount services before invocation. This
    /// operation never hides process-wide service startup in a finite receipt.
    /// Admission reads only the selected root, leaving descendants lazy.
    pub async fn mount_measured(
        &self,
        destination: impl Into<PathBuf>,
        options: MountOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<LazyMount<A, O, D, S>>, MountLifecycleError> {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        let destination = destination.into();
        let result = async {
            work.check().map_err(native_demand_lifecycle_error)?;
            work.adopt(destination.capacity() as u64).map_err(native_demand_lifecycle_error)?;
            work.adopt(options.subdirectory.capacity() as u64).map_err(native_demand_lifecycle_error)?;
            if options.writable && options.publication == MountPublication::PerMutation {
                return Err(MountLifecycleError::Source(MountSourceError::Unsupported(
                    "lazy source whiteouts publish with a checkout boundary, not per callback".to_owned(),
                )));
            }
            let (authored, volume_id, root, root_text) =
                self.prepare_authored_mount_source(&options, None, &mut work, cancellation).await?;
            let root_bytes = super::lazy::mount_namespace_bytes(&root);
            drop(root);
            work.release(root_bytes).map_err(native_demand_lifecycle_error)?;
            work.allocate(std::mem::size_of::<LazyWorkspace<A, O, D, S>>() as u64)
                .map_err(native_demand_lifecycle_error)?;
            let lazy = Arc::new(self.clone());
            let source = LazyMountSource::new_with_work(lazy, authored, root_text, &mut work)?;
            work.allocate(std::mem::size_of::<LazyMountSource<A, O, D, S>>() as u64)
                .map_err(native_demand_lifecycle_error)?;
            let source = Arc::new(source);
            let copied = crate::WorkCounters {
                bytes_copied: destination.as_os_str().as_encoded_bytes().len() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            work.work.admit(&copied, &work.budget)?;
            work.allocate(copied.bytes_copied).map_err(native_demand_lifecycle_error)?;
            let session_destination = destination.clone();
            work.record(copied).map_err(native_demand_lifecycle_error)?;
            let session = super::mount_native_with_work(
                NativeMountRequest {
                    mount_id: MountId::new(), volume_id, destination: session_destination,
                    writable: options.writable,
                },
                Arc::clone(&source) as Arc<dyn super::MountFilesystem>,
                &mut super::NativeMountWork { work: &mut work },
            )?;
            // No fallible work follows acquisition of the live session owner.
            Ok(LazyMount { source, session: Mutex::new(Some(session)), destination })
        }.await;
        result.map(|value| crate::OperationReceipt { value, work: work.work })
            .map_err(|error| crate::OperationFailure::new(error, work.work))
    }
}

fn accept_customer_work<T, E>(
    work: &mut crate::demand::DemandWork,
    result: crate::MeasuredResult<crate::OperationReceipt<T>, E>,
    map: impl FnOnce(E) -> MountLifecycleError,
) -> Result<T, MountLifecycleError> {
    match result {
        Ok(receipt) => {
            work.merge(receipt.work).map_err(native_demand_lifecycle_error)?;
            Ok(receipt.value)
        }
        Err(failure) => {
            work.merge(*failure.work).map_err(native_demand_lifecycle_error)?;
            Err(map(failure.error))
        }
    }
}

fn preparation_failure<E>(
    failure: crate::OperationFailure<E>,
    work: &mut crate::WorkCounters,
    map: impl FnOnce(E) -> MountLifecycleError,
) -> MountLifecycleError {
    let combined = failure.map_with_prior_work(*work, map);
    *work = *combined.work;
    combined.error
}

fn preparation_failure_with_live<E>(
    failure: crate::OperationFailure<E>,
    work: &mut crate::WorkCounters,
    live_bytes: u64,
    map: impl FnOnce(E) -> MountLifecycleError,
) -> MountLifecycleError {
    match work.with_backend(*failure.work, live_bytes) {
        Ok(combined) => {
            *work = combined;
            map(failure.error)
        }
        Err(error) => MountLifecycleError::Work(error),
    }
}

fn preparation_remaining_with_live(
    work: crate::WorkCounters,
    budget: crate::WorkBudget,
    live_bytes: u64,
) -> Result<crate::WorkBudget, crate::WorkError> {
    let mut remaining = work.remaining(budget)?;
    remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
        .checked_sub(live_bytes).ok_or(crate::WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed: live_bytes,
            maximum: budget.peak_allocation_bytes,
        })?;
    Ok(remaining)
}

fn prepare_root_identity(
    source_root: &Path,
    selected_root: &Path,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
    expected_identity: Option<NativeRootIdentity>,
) -> crate::MeasuredResult<crate::OperationReceipt<NativeRootIdentity>, MountLifecycleError> {
    let mut meter = crate::demand::DemandWork::new(budget, cancellation);
    let result = (|| {
        let root = crate::native_host::HostRoot::open_measured(source_root, &mut meter)
            .map_err(native_demand_lifecycle_error)?;
        if expected_identity.is_some_and(|expected| expected != root.identity()) {
            return Err(MountLifecycleError::Source(MountSourceError::Stale));
        }
        let metadata = root.stat_measured(selected_root, &mut meter)
            .map_err(native_demand_lifecycle_error)?;
        if !metadata.is_dir() {
            return Err(MountLifecycleError::Source(MountSourceError::Invalid(
                "selected native working-set directory is absent".into(),
            )));
        }
        Ok(root.identity())
    })();
    let work = meter.work;
    result
        .map(|value| crate::OperationReceipt { value, work })
        .map_err(|error| crate::OperationFailure::new(error, work))
}

fn native_demand_lifecycle_error(error: crate::demand::DemandError) -> MountLifecycleError {
    let source = match error {
        crate::demand::DemandError::Cancelled => MountSourceError::Cancelled,
        crate::demand::DemandError::Work(error) => MountSourceError::Work(error),
        crate::demand::DemandError::StaleVersion
        | crate::demand::DemandError::SourceUnavailable => MountSourceError::Stale,
        error => MountSourceError::Engine(error.to_string()),
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
    ) -> crate::MeasuredResult<crate::OperationReceipt<NativeWorkingSet<A, O>>, MountLifecycleError>
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
    ) -> crate::MeasuredResult<crate::OperationReceipt<NativeWorkingSet<A, O>>, MountLifecycleError>
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
    ) -> crate::MeasuredResult<crate::OperationReceipt<NativeWorkingSet<A, O>>, MountLifecycleError> {
        self.prepare_native_working_set_from(
            mount, options, Some(expected_root_identity),
            preparation_budget, capture_budget, cancellation,
        ).await
    }

    #[allow(clippy::too_many_arguments, reason = "one preparation and witnessed attachment engine")]
    async fn prepare_native_working_set_from(
        &self,
        mount: &MountOptions,
        options: &crate::MaterializeOptions,
        expected_root_identity: Option<NativeRootIdentity>,
        preparation_budget: crate::WorkBudget,
        capture_budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<NativeWorkingSet<A, O>>, MountLifecycleError> {
        let mut work = crate::WorkCounters::default();
        let mut capture_work = crate::WorkCounters::default();
        let mut preparation_captured_data_bytes = 0_u64;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let mut cancellation_admission = crate::demand::DemandCancellationRoots::default();
        let result = async {
            if !options.destination.is_absolute() || mount.publication != MountPublication::Manual {
                return Err(MountLifecycleError::Source(MountSourceError::Invalid(
                    "native working set requires an absolute root and manual publication".into(),
                )));
            }
            let workspace = self.workspace();
            let expected_generation = self.id();
            if mount.writable {
                let head = workspace
                    .head_measured(work.remaining(preparation_budget)?, cancellation)
                    .await
                    .map_err(|failure| {
                        preparation_failure(failure, &mut work, MountLifecycleError::from)
                    })?;
                work = work.checked_add(head.work)?;
                work.verify(preparation_budget)?;
                if head.value.id() != expected_generation {
                    return Err(MountLifecycleError::Workspace(
                        WorkspaceError::StaleGeneration,
                    ));
                }
            }
            let mode = if mount.writable {
                CheckoutMode::tracking_transaction()
            } else {
                CheckoutMode::read_only_pinned()
            };
            let receipt = workspace
                .volume
                .checkout(
                    GenerationSelector::Exact(expected_generation),
                    mode,
                    work.remaining(preparation_budget)?,
                    cancellation,
                )
                .await
                .map_err(|failure| {
                    preparation_failure(failure, &mut work, |error| {
                        MountLifecycleError::Workspace(WorkspaceError::from(error))
                    })
                })?;
            work = work.checked_add(receipt.work)?;
            let mut checkout = receipt.value;
            let original_handle_bytes = checkout.logical_owned_bytes();
            allocations.claim_bytes(
                original_handle_bytes, 0, &mut work, preparation_budget,
            ).map_err(|error| WorkspaceError::from(crate::collection::handle_allocation_error(error)))?;
            if !mount.writable {
                let mut remaining = work.remaining(preparation_budget)?;
                remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
                    .checked_sub(allocations.live_bytes()).ok_or(crate::WorkError::Overflow)?;
                let candidate = checkout.unpublishable_capture_candidate_measured(remaining, cancellation)
                    .map_err(|failure| preparation_failure_with_live(
                        failure, &mut work, allocations.live_bytes(), |error| MountLifecycleError::Workspace(WorkspaceError::from(error)),
                    ))?;
                work = work.with_backend(candidate.work, allocations.live_bytes())?;
                drop(checkout);
                allocations.release(original_handle_bytes)
                    .map_err(|error| WorkspaceError::from(crate::collection::handle_allocation_error(error)))?;
                checkout = candidate.value;
                allocations.claim_bytes(
                    checkout.logical_owned_bytes(), 0, &mut work, preparation_budget,
                ).map_err(|error| WorkspaceError::from(crate::collection::handle_allocation_error(error)))?;
            }
            let config = checkout.volume_config();
            let root = crate::workspace::customer_path_measured(
                &mount.subdirectory, config, &mut allocations, &mut work,
                preparation_budget, cancellation,
            )?;
            let mut remaining = work.remaining(preparation_budget)?;
            remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
                .checked_sub(allocations.live_bytes()).ok_or(crate::WorkError::Overflow)?;
            let selected = checkout
                .lookup_no_follow(&root, remaining, cancellation)
                .await
                .map_err(|failure| {
                    preparation_failure_with_live(failure, &mut work, allocations.live_bytes(), |error| {
                        MountLifecycleError::Workspace(WorkspaceError::from(error))
                    })
                })?;
            work = work.with_backend(selected.work, allocations.live_bytes())?;
            if selected
                .value
                .record
                .is_none_or(|record| record.kind != FileKind::Directory)
            {
                return Err(MountLifecycleError::Workspace(WorkspaceError::NotDirectory));
            }
            if expected_root_identity.is_none() {
            let remaining = preparation_remaining_with_live(
                work, preparation_budget, allocations.live_bytes(),
            )?;
            let materialized = if mount.subdirectory == "/" {
                self.materialize_with_mode(
                    options,
                    remaining,
                    cancellation,
                    super::MaterializeMode::ReconstructibleView,
                )
                .await
                .map_err(|failure| {
                    preparation_failure_with_live(failure, &mut work, allocations.live_bytes(), MountLifecycleError::from)
                })?
            } else {
                self.materialize_path_with_mode(
                    &mount.subdirectory,
                    options,
                    remaining,
                    cancellation,
                    super::MaterializeMode::ReconstructibleView,
                )
                .await
                .map_err(|failure| {
                    preparation_failure_with_live(failure, &mut work, allocations.live_bytes(), MountLifecycleError::from)
                })?
            };
            work = work.with_backend(materialized.work, allocations.live_bytes())?;
            work.verify(preparation_budget)?;
            }
            let mut host_work = crate::demand::DemandWork::from_current(
                preparation_budget, cancellation, std::mem::take(&mut work),
                std::mem::take(&mut allocations), std::mem::take(&mut cancellation_admission),
            );
            let source_root = crate::native_host::clone_path_measured(&options.destination, &mut host_work);
            (work, allocations, cancellation_admission) = host_work.into_current();
            let source_root = source_root.map_err(super::lazy::mount_demand_error)?;
            let selected_root = preparation_selected_root_measured(
                &root, &mut allocations, &mut work, preparation_budget, cancellation,
            ).map_err(super::lazy::mount_demand_error)?;
            let identity = prepare_root_identity(
                &source_root,
                &selected_root,
                preparation_remaining_with_live(work, preparation_budget, allocations.live_bytes())?,
                cancellation,
                expected_root_identity,
            )
            .map_err(|failure| preparation_failure_with_live(
                failure, &mut work, allocations.live_bytes(), std::convert::identity,
            ))?;
            work = work.with_backend(identity.work, allocations.live_bytes())?;
            let source_identity = identity.value;
            let baseline = if mount.writable {
                NativeViewBaseline::new(source_identity)
            } else {
                NativeViewBaseline::read_only(source_identity)
            };
            let shared = SharedCheckout::with_publication_measured(
                checkout,
                MountPublication::Manual,
                &mut allocations, &mut work, preparation_budget,
            )?;
            allocations.claim_elements::<SharedCheckout<A, O>>(
                1, &mut work, preparation_budget,
            ).map_err(|error| WorkspaceError::from(crate::collection::handle_allocation_error(error)))?;
            let shared = Arc::new(shared);
            let source = CheckoutMountSource::new_at_measured(
                shared, config, root, &mut allocations, &mut work, preparation_budget,
            )?;
            allocations.claim_elements::<CheckoutMountSource<A, O>>(
                1, &mut work, preparation_budget,
            ).map_err(|error| WorkspaceError::from(crate::collection::handle_allocation_error(error)))?;
            let source = Arc::new(source);
            allocations.claim_bytes(
                baseline.logical_owned_bytes(), 0, &mut work, preparation_budget,
            ).map_err(|error| WorkspaceError::from(crate::collection::handle_allocation_error(error)))?;
            allocations.claim_elements::<NativeViewBaseline>(
                1, &mut work, preparation_budget,
            ).map_err(|error| WorkspaceError::from(crate::collection::handle_allocation_error(error)))?;
            let baseline = Arc::new(baseline);
            if expected_root_identity.is_some() {
                let preparation_live = allocations.live_bytes();
                let initial_capture_budget = capture_budget.intersect_budget(work.remaining(preparation_budget)?);
                let mut remaining_capture = initial_capture_budget;
                remaining_capture.peak_allocation_bytes = remaining_capture.peak_allocation_bytes
                    .checked_sub(preparation_live)
                    .ok_or(crate::WorkError::BudgetExceeded {
                        counter: "peak_allocation_bytes",
                        observed: preparation_live,
                        maximum: initial_capture_budget.peak_allocation_bytes,
                    })?;
                let captured = source.capture_host_with_identity_budgeted(
                    &source_root, &[MountPath::root()], source_identity,
                    HostCaptureScope::Subtree, Arc::clone(&baseline),
                    remaining_capture, cancellation,
                ).await;
                match captured {
                    Ok(receipt) => {
                        capture_work = crate::WorkCounters::default()
                            .with_backend(receipt.work, preparation_live)?;
                        preparation_captured_data_bytes = receipt.value;
                        allocations.claim_bytes(
                            preparation_captured_data_bytes, 0, &mut work, preparation_budget,
                        ).map_err(|error| WorkspaceError::from(crate::collection::handle_allocation_error(error)))?;
                    }
                    Err(failure) => {
                        capture_work = *failure.work;
                        capture_work = crate::WorkCounters::default()
                            .with_backend(capture_work, preparation_live)?;
                        return Err(MountLifecycleError::Source(failure.error));
                    }
                }
                capture_work.verify(initial_capture_budget)?;
                work = work.checked_add(capture_work)?;
                capture_work = crate::WorkCounters::default();
                work.verify(preparation_budget)?;
                let pending_result = preparation_wait_measured(
                    source.shared_checkout(), cancellation, Some(source.cancellation()),
                    &mut allocations, &mut work, &mut cancellation_admission, preparation_budget,
                    |checkout| checkout.has_pending_mutations(),
                ).await;
                let pending = pending_result?;
                if pending {
                    return Err(MountLifecycleError::Source(MountSourceError::Invalid(
                        "journaled native allocation diverged from its original generation".into(),
                    )));
                }
            }
            if mount.writable {
                let head = workspace
                    .head_measured(preparation_remaining_with_live(
                        work, preparation_budget, allocations.live_bytes(),
                    )?, cancellation)
                    .await
                    .map_err(|failure| {
                        preparation_failure_with_live(failure, &mut work, allocations.live_bytes(), MountLifecycleError::from)
                    })?;
                work = work.with_backend(head.work, allocations.live_bytes())?;
                work.verify(preparation_budget)?;
                if head.value.id() != expected_generation {
                    return Err(MountLifecycleError::Workspace(
                        WorkspaceError::StaleGeneration,
                    ));
                }
            }
            Ok(NativeWorkingSet {
                source,
                workspace,
                source_root,
                source_identity,
                selected_root,
                expected_generation,
                baseline,
                capture_budget,
                writable: mount.writable,
                preparation_control_bytes: allocations.live_bytes()
                    .checked_sub(preparation_captured_data_bytes).ok_or(crate::WorkError::Overflow)?,
                preparation_captured_data_bytes,
            })
        }
        .await;
        let work = work.checked_add(capture_work)
            .map_err(|error| crate::OperationFailure::new(MountLifecycleError::from(error), work))?;
        result
            .map(|value| crate::OperationReceipt { value, work })
            .map_err(|error| crate::OperationFailure::new(error, work))
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
        let mut work = crate::demand::DemandWork::new(budget, &cancellation);
        let (_, volume, _, root) = lazy.prepare_authored_mount_source(
            &options, None, &mut work, &cancellation,
        ).await?;
        assert_eq!(volume, lazy.workspace().id().volume_id());
        assert_eq!(root, "/selected");
        work.work.verify(budget)?;
        assert_eq!(work.work.source_bytes_read, 0);
        assert_eq!(work.work.source_directory_pages, 0);
        assert_eq!(work.work.materializations, 0);
        assert!(work.work.backend_read_operations > 0);
        assert!(matches!(lazy.lookup("/selected").await?, LazyLookup::Authored { .. }));
        assert!(matches!(lazy.lookup("/selected/cold.txt").await?, LazyLookup::Source(_)));
        assert!(matches!(lazy.lookup("/unselected/cold.txt").await?, LazyLookup::Source(_)));
        let pinned = lazy.workspace().head().await?.id();
        let mut denied = crate::demand::DemandWork::new(crate::WorkBudget::default(), &cancellation);
        assert!(lazy.prepare_authored_mount_source(
            &options, Some(pinned), &mut denied, &cancellation,
        ).await.is_err());
        assert_eq!(denied.work, crate::WorkCounters::UNCHARGED);
        assert_eq!(lazy.workspace().head().await?.id(), pinned);
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
        denied.work.verify(crate::WorkBudget::default())?;
        assert_eq!(lazy.workspace().head().await?.id(), generation);
        assert_eq!(std::fs::read_dir(destination.path())?.count(), 0);
        let mounted = lazy.mount_measured(
            destination.path(), options, crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await?;
        assert!(mounted.work.backend_read_operations > 0);
        let mount = mounted.value;
        let before = mount.snapshot_measured(crate::WorkBudget::UNBOUNDED, &cancellation).await?;
        assert_eq!(before.value.authored_generation, generation);
        assert_eq!(std::fs::read(destination.path().join("source.txt"))?, b"source");
        assert_eq!(std::fs::read(destination.path().join("authored.txt"))?, b"authored");
        std::fs::write(destination.path().join("authored.txt"), b"native edit")?;
        let guarded = mount.sync_with_permit_measured_guarded(
            PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &cancellation,
            || std::future::ready(Err(MountSourceError::Stale)),
        ).await.err().ok_or("failed original-owner guard published")?;
        assert!(matches!(guarded.error, MountLifecycleError::Source(MountSourceError::Stale)));
        assert!(guarded.work.items_examined > 0);
        assert_eq!(lazy.workspace().head().await?.id(), generation);
        assert!(mount.snapshot_measured(crate::WorkBudget::UNBOUNDED, &cancellation).await.is_err());
        mount.sync_with_permit_measured_guarded(
            PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &cancellation,
            || std::future::ready(Ok(())),
        ).await?;
        assert_eq!(lazy.read_range("/authored.txt", 0, 32).await?.as_ref(), b"native edit");
        let actual = mount.snapshot_measured(crate::WorkBudget::UNBOUNDED, &cancellation).await?;
        mount.snapshot_measured(actual.work, &cancellation).await?.work.verify(actual.work)?;
        let imported_path = NamespacePath::new(vec![crate::kernel::LogicalName::new(
            crate::kernel::NameEncoding::Utf8, b"selected-import.txt".to_vec(),
            VolumeLimits::default().maximum_component_bytes,
        )?], VolumeLimits::default())?;
        let operation = crate::OperationId::new();
        let selection = || vec![crate::AuthoredMutation::CreateFile {
            path: imported_path.clone(),
            bytes: bytes::Bytes::from_static(b"selected SDK import"),
            metadata: crate::kernel::FileMetadata::default(),
        }];
        let imported_head = lazy.workspace().head().await?.id();
        let denied_import = mount.import_selected_measured_guarded(
            actual.value, std::slice::from_ref(&imported_path), selection(), operation,
            PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &cancellation,
            || std::future::ready(Err(MountSourceError::Stale)),
        ).await.err().ok_or("selected import bypassed its original owner guard")?;
        assert!(matches!(denied_import.error, MountLifecycleError::Source(MountSourceError::Stale)));
        assert_eq!(lazy.workspace().head().await?.id(), imported_head);
        let imported = mount.import_selected_measured_guarded(
            actual.value, std::slice::from_ref(&imported_path), selection(), operation,
            PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &cancellation,
            || std::future::ready(Ok(())),
        ).await?;
        assert_eq!(imported.value.authored_generation, lazy.workspace().head().await?.id());
        assert_eq!(lazy.read_range("/selected-import.txt", 0, 32).await?.as_ref(), b"selected SDK import");
        assert_eq!(lazy.read_range("/source.txt", 0, 32).await?.as_ref(), b"source");
        let no_detach = crate::WorkBudget {
            backend_write_operations: 0, ..crate::WorkBudget::UNBOUNDED
        };
        let denied_detach = mount.abandon_measured(no_detach, &cancellation)
            .err().ok_or("zero native-write allowance detached a live namespace")?;
        denied_detach.work.verify(no_detach)?;
        assert_eq!(denied_detach.work.backend_write_operations, 0);
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
        assert_eq!(*denied_cleanup.work, crate::WorkCounters::UNCHARGED);
        {
            let owner = mount.session.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let state = owner.as_ref().ok_or("cleanup failure lost its session")?.cleanup_state();
            assert!(state.driver_retained);
            assert!(state.destination_fence_retained);
            assert!(state.measured_cleanup_failed);
            assert!(!state.cleanup_complete);
        }
        let cleanup = mount.abandon_measured(crate::WorkBudget::UNBOUNDED, &cancellation)?;
        assert!(cleanup.work.backend_write_operations > 0);
        assert!(mount.session.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_none());
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
        ).await?;
        let identity = prepared.value.source_identity;
        let rejected_capture = generation.attach_native_working_set_with_budgets(
            &mount, &options, identity, crate::WorkBudget::UNBOUNDED,
            crate::WorkBudget::default(), &cancellation,
        ).await.err().ok_or("zero-budget attachment capture succeeded")?;
        assert!(matches!(
            rejected_capture.error,
            MountLifecycleError::Source(MountSourceError::Work(crate::WorkError::BudgetExceeded { .. }))
                | MountLifecycleError::Work(crate::WorkError::BudgetExceeded { .. })
        ));
        let preparation = *rejected_capture.work;
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
            rejected_carrier.error,
            MountLifecycleError::Workspace(WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                counter: "allocation_operations", ..
            }))
                | MountLifecycleError::Work(crate::WorkError::BudgetExceeded {
                    counter: "allocation_operations", ..
                })
        ));
        assert_eq!(rejected_carrier.work.allocation_operations, finite_preparation.allocation_operations);
        assert_eq!(rejected_carrier.work.materializations, 0);
        let rejected_initial_capture = generation.attach_native_working_set_with_budgets(
            &mount, &options, identity, preparation,
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await.err().ok_or("initial capture reset its preparation allowance")?;
        assert!(matches!(
            rejected_initial_capture.error,
            MountLifecycleError::Source(MountSourceError::Work(crate::WorkError::BudgetExceeded { .. }))
                | MountLifecycleError::Work(crate::WorkError::BudgetExceeded { .. })
        ));
        rejected_initial_capture.work.verify(preparation)?;
        assert_eq!(rejected_initial_capture.work.materializations, 0);
        let admitted = generation.attach_native_working_set_with_budgets(
            &mount, &options, identity, crate::WorkBudget::UNBOUNDED,
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await?;
        let exact = admitted.work;
        drop(admitted.value);
        let attached = generation.attach_native_working_set_with_budgets(
            &mount, &options, identity, exact,
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).await?;
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
            .await?
            .value;
        source.validate_for_presentation().await?;
        source.verify_unchanged().await?;
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
            rejected.error,
            MountLifecycleError::Workspace(WorkspaceError::StaleGeneration)
        ));
        assert!(rejected.work.backend_read_operations > 0);
        let destination = workspace
            .head()
            .await?
            .prepare_native_working_set(
                &write,
                &crate::MaterializeOptions::native(destination_root.path()),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await?
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
            .await.map_err(|failure| failure.error),
            Err(MountLifecycleError::Source(MountSourceError::Invalid(_)))
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
            .await?.value;
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
        working_set
            .capture_host_paths(&[MountPath::root().child(name)])
            .await?;
        working_set
            .sync_with_permit(PublicationPermit::Unrestricted)
            .await?;
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
            .await?.value;
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
        Ok(())
    }
}
