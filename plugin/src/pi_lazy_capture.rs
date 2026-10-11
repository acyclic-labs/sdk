//! Native lazy selections retain the original authored/overlay/shadow cut.
//! The canonical Filesystem preparer remains the sole resource creator.

use crate::{LocalLazyWorkspace, SharedPhysicalRoot, LocalAuthorityBackend, LocalObjectBackend,
    LocalLazySource, LocalCoreStateStore, pi_native::PiNativeView};
use acyclic_fs::{CancellationToken, LazySnapshotRef, WorkBudget, WorkCounters,
    OperationReceipt, OperationFailure};
use acyclic_harness::{Error, Result,
    fork::{Capture, CapturedResource, ForkCaptureProvider, ForkRequest, ForkSeed, ForkSeedVerifier, ForkSelection, ResourceRevision},
    resources::{ArtifactRef, ProviderRef}};
use acyclic_stream::BoxProviderFuture;
use std::{future::Future, sync::Arc};

pub(crate) struct PiLazyCapture {
    provider: ProviderRef,
    source: Arc<LocalLazyWorkspace>,
    frozen: LazySnapshotRef,
    revision: ResourceRevision,
    budget: WorkBudget,
    cancellation: CancellationToken,
    work: Option<acyclic_fs::NativeMaterializationWork>,
}

impl PiLazyCapture {
    pub(crate) async fn new(
        source: Arc<LocalLazyWorkspace>, frozen: LazySnapshotRef,
        budget: WorkBudget, cancellation: CancellationToken, work: Option<acyclic_fs::NativeMaterializationWork>,
    ) -> Result<Self> {
        if !budget.has_finite_allowances() || frozen.workspace_id != source.workspace().id() {
            return Err(Error::Unauthorized("Pi native cut does not belong to its actual source workspace".into()));
        }
        let provider = ProviderRef::new("acyclic.pi", "native-lazy", "1")?;
        let revision = ResourceRevision::Artifact(ArtifactRef::new(
            provider.clone(), frozen.id.into_bytes(), None,
        )?);
        let capture = Self { provider, source, frozen, revision, budget, cancellation, work };
        capture.authenticate().await?;
        Ok(capture)
    }

    pub(crate) const fn frozen(&self) -> LazySnapshotRef { self.frozen }
    pub(crate) fn selection(&self) -> ForkSelection {
        ForkSelection { required: true, revision: self.revision.clone() }
    }

    async fn authenticate(&self) -> Result<()> {
        let budget = match &self.work { Some(work) => work.remaining()
            .map(|remaining| remaining.intersect_budget(self.budget)).map_err(|cause| Error::NativeWork { cause, custody: None })?,
            None => self.budget };
        let result = self.source.validate_snapshot_measured(self.frozen, budget, &self.cancellation).await;
        let spent = match &result { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        if let Some(work) = &self.work {
            work.spent(spent).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
        }
        result.map(|_| ()).map_err(|failure| Error::Storage(format!("{}; work={:?}", failure.error, failure.work)))
    }

    async fn selected(&self, request: &ForkRequest, selection: &ForkSelection) -> Result<Capture> {
        request.validate()?;
        if !selection.required || selection.revision != self.revision
            || !request.selections.contains(selection)
        {
            return Err(Error::Unauthorized("Pi lazy capture differs from the original immutable selection".into()));
        }
        self.authenticate().await?;
        Ok(Capture::Captured(CapturedResource {
            source: self.revision.clone(), revision: self.revision.clone(),
        }))
    }
}

impl ForkCaptureProvider for PiLazyCapture {
    fn provider(&self) -> &ProviderRef { &self.provider }
    fn capture<'a>(&'a self, request: &'a ForkRequest, selection: &'a ForkSelection)
        -> BoxProviderFuture<'a, Result<Capture>> {
        Box::pin(self.selected(request, selection))
    }
    fn reconcile<'a>(&'a self, request: &'a ForkRequest, selection: &'a ForkSelection)
        -> BoxProviderFuture<'a, Result<Option<Capture>>> {
        // This is immutable provider observation, never another allocation.
        Box::pin(async move { self.selected(request, selection).await.map(Some) })
    }
}

impl ForkSeedVerifier for PiLazyCapture {
    fn provider(&self) -> &ProviderRef { &self.provider }
    fn verify<'a>(&'a self, seed: &'a ForkSeed) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            seed.validate()?;
            let mut found = false;
            for capture in &seed.resources {
                if capture.source.provider() == &self.provider || capture.revision.provider() == &self.provider {
                    if found || capture.source != self.revision || capture.revision != self.revision {
                        return Err(Error::Unauthorized("published Pi lazy capture changed its original source cut".into()));
                    }
                    found = true;
                }
            }
            if !found { return Err(Error::Unauthorized("published Pi fork lost its required native cut".into())); }
            self.authenticate().await
        })
    }
}

/// Root-native observation is owned by the original root SDK task, not the
/// child selecting that project's data. The observer publishes its actual
/// watcher epoch before freezing any unresolved path references.
pub(crate) async fn freeze_root_source<P: acyclic_stream::StreamProvider>(
    original_root: acyclic_harness::TaskId,
    context: &acyclic_harness::runtime::TaskContext,
    owner: &acyclic_harness::durable_host::TaskJournalOwner<P>,
    physical: &SharedPhysicalRoot, source: &LocalLazyWorkspace,
    budget: WorkBudget, cancel: &CancellationToken,
) -> crate::pi_root_provider::PiMeasuredResult<OperationReceipt<LazySnapshotRef>> {
    let mut work = WorkCounters::default();
    if owner.task_id() != original_root || !budget.has_finite_allowances() {
        return Err((Error::Unauthorized("Pi root snapshot requires its original root owner and finite budget".into()), work));
    }
    crate::pi_inspection::authenticate(context, owner).await
        .map_err(|error| (error, work))?;
    let observed = physical.observe_measured(budget, cancel)
        .map_err(|failure| nested_failure(failure, work))?;
    work = observed.work;
    let remaining = work.remaining(budget).map_err(|cause| (Error::NativeWork { cause: cause.into(), custody: None }, work))?;
    let rebound = source.rebind_source_measured(remaining, cancel).await
        .map_err(|failure| nested_failure(failure, work))?;
    work = work.checked_add(rebound.work).map_err(|cause| (Error::NativeWork { cause: cause.into(), custody: None }, work))?;
    let remaining = work.remaining(budget).map_err(|cause| (Error::NativeWork { cause: cause.into(), custody: None }, work))?;
    let snapshot = source.snapshot_measured(remaining, cancel).await
        .map_err(|failure| nested_failure(failure, work))?;
    work = work.checked_add(snapshot.work).map_err(|cause| (Error::NativeWork { cause: cause.into(), custody: None }, work))?;
    if snapshot.value.source != observed.value.source {
        return Err((Error::Conflict("Pi root watcher changed before its original source freeze".into()), work));
    }
    owner.verify_dispatch().await.map_err(|error| (error, work))?;
    Ok(OperationReceipt { value: snapshot.value, work })
}

/// The Root's actual created-source guard spans the watcher, rebind and freeze
/// on the command's existing ledger. The SDK owns every returned source guard.
pub(crate) async fn freeze_root_source_with_native_work<P, F, G>(
    original_root: acyclic_harness::TaskId,
    context: &acyclic_harness::runtime::TaskContext,
    owner: &acyclic_harness::durable_host::TaskJournalOwner<P>,
    physical: &SharedPhysicalRoot, source: &LocalLazyWorkspace,
    work: &acyclic_fs::NativeMaterializationWork, mut guard: G,
) -> Result<(LazySnapshotRef, acyclic_fs::NativeAllocation)>
where P: acyclic_stream::StreamProvider,
      G: FnMut() -> F + Send,
      F: Future<Output = Result<()>> + Send,
{
    if owner.task_id() != original_root || context.durable_task_id() != Some(original_root) {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Root snapshot requires its actual original Root context and owner" });
    }
    owner.verify_with_native_work(false, work).await?;
    let observed = physical.observe_with_native_work(work, &mut guard).await?;
    guard().await?;
    let rebound = source.rebind_source_with_native_work(work).await.map_err(native_lazy_error)?;
    guard().await?;
    if rebound.0.source != observed.0.source {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "Root source rebind changed its guarded watcher observation" });
    }
    let mut frozen = source.snapshot_with_native_work(work).await.map_err(native_lazy_error)?;
    guard().await?;
    if frozen.0.source != observed.0.source {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "Root source freeze changed its guarded watcher observation" });
    }
    owner.verify_with_native_work(false, work).await?;
    drop(rebound.0);
    drop(rebound.1);
    drop(observed.0);
    let mut observed_custody = observed.1;
    frozen.1.absorb(&mut observed_custody).map_err(|cause| Error::NativeWork { cause, custody: None })?;
    Ok(frozen)
}

pub(crate) fn native_lazy_error(source: acyclic_fs::LazyWorkspaceError) -> Error {
    let (source, custody) = source.into_native_failure();
    match source {
        acyclic_fs::LazyWorkspaceError::NativeWork(cause) => Error::NativeWork { cause, custody },
        source => Error::NativeSource { source, custody },
    }
}

/// Only an active source's ORIGINAL native view owner may checkpoint its dirty
/// mounts. A selected caller's authority can never replace this source owner.
/// `work` is that view's retained original source-operation work, not a
/// selecting caller's query. Its returned custody remains separate from the
/// caller's later sealed-read/preparation work.
pub(crate) async fn freeze_child_source<P: acyclic_stream::StreamProvider>(
    context: &acyclic_harness::runtime::TaskContext,
    owner: &acyclic_harness::durable_host::TaskJournalOwner<P>,
    view: &PiNativeView<LocalAuthorityBackend, LocalObjectBackend, LocalLazySource, LocalCoreStateStore>,
    source: &LocalLazyWorkspace, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(LazySnapshotRef, acyclic_fs::NativeAllocation)> {
    let checkpoint = view.checkpoint_projects_with_work(owner, context, work).await?;
    let mut selected = checkpoint.0.iter().filter(|(_, snapshot)| snapshot.workspace_id == source.workspace().id());
    let snapshot = selected.next().ok_or_else(|| Error::Unauthorized(
        "Pi checkpoint has no actual selected source workspace".into()))?.1;
    if selected.next().is_some() {
        return Err(Error::Unauthorized("Pi checkpoint repeats a selected source workspace".into()));
    }
    drop(checkpoint.0);
    Ok((snapshot, checkpoint.1))
}

fn nested_failure<E: std::fmt::Display>(failure: OperationFailure<E>, prior: WorkCounters) -> (Error, WorkCounters) {
    match prior.checked_add(*failure.work) {
        Ok(work) => (Error::Storage(failure.error.to_string()), work),
        Err(cause) => (Error::NativeWork { cause: cause.into(), custody: None }, prior),
    }
}
