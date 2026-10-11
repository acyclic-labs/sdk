//! Native lazy selections retain the original authored/overlay/shadow cut.
//! The canonical Filesystem preparer remains the sole resource creator.

use crate::{LocalLazyWorkspace, SharedPhysicalRoot, LocalAuthorityBackend, LocalObjectBackend};
use acyclic_fs::{CancellationToken, LazySnapshotRef, WorkBudget, WorkCounters};
use acyclic_harness::{Error, Result,
    fork::{Capture, CapturedResource, ForkCaptureProvider, ForkRequest, ForkSeed, ForkSeedVerifier, ForkSelection, ResourceRevision},
    resources::{ArtifactRef, ProviderRef}};
use acyclic_stream::BoxProviderFuture;
use std::{future::Future, sync::Arc};

const LAZY_DIAGNOSTIC_LABEL_BYTES: u64 = crate::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES
    + crate::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "ResourceRevision", "History", "Project", "PrivateVolume", "Artifact", "Sandbox", "Checkpoint", "Context",
        "history", "project", "private_volume", "artifact", "sandbox", "checkpoint", "context", "kind", "reference",
        "ResourceRef", "ProviderRef", "namespace", "family", "version", "key", "provider", "volume", "generation", "paths",
        "Capture", "Captured", "Unsupported", "InFlight", "Indeterminate", "captured", "unsupported", "in_flight", "indeterminate",
        "CapturedResource", "source", "revision", "value",
        "ForkSelection", "required",
    ]);

pub(crate) struct PiLazyCapture {
    source: Arc<LocalLazyWorkspace>,
    frozen: LazySnapshotRef,
    revision: ResourceRevision,
    budget: WorkBudget,
    cancellation: CancellationToken,
    work: Option<acyclic_fs::NativeMaterializationWork>,
    _metadata_custody: Option<acyclic_fs::NativeAllocation>,
}

impl PiLazyCapture {
    pub(crate) async fn new(
        source: Arc<LocalLazyWorkspace>, frozen: LazySnapshotRef,
        budget: WorkBudget, cancellation: CancellationToken, work: Option<acyclic_fs::NativeMaterializationWork>,
    ) -> Result<Self> {
        if !budget.has_finite_allowances() || frozen.workspace_id != source.workspace().id() {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi native cut does not belong to its actual source workspace" });
        }
        let (revision, metadata) = if let Some(original) = &work {
            #[derive(serde::Serialize)]
            struct Provider { namespace: &'static str, family: &'static str, version: &'static str }
            #[derive(serde::Serialize)]
            struct Reference<'a> {
                kind: acyclic_harness::resources::ResourceKind, provider: Provider,
                key: &'a [u8], version: Option<&'static str>,
            }
            #[derive(serde::Serialize)]
            #[serde(tag = "kind", content = "reference", rename_all = "snake_case")]
            enum Revision<'a> { Artifact(Reference<'a>) }
            let bytes = crate::pi_root_provider::canonical_pi_value_with_native_work(
                &Revision::Artifact(Reference {
                    kind: acyclic_harness::resources::ResourceKind::Artifact,
                    provider: Provider { namespace: "acyclic.pi", family: "native-lazy", version: "1" },
                    key: &frozen.id.into_bytes(), version: None,
                }), crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64, original)?;
            let (revision, custody) = crate::pi_root_provider::guarded_pi_decode::<ResourceRevision>(
                &bytes.0, crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64, crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
                acyclic_harness::JsonValueSchema::None, LAZY_DIAGNOSTIC_LABEL_BYTES, original)?;
            (revision, Some(custody))
        } else {
            (ResourceRevision::Artifact(ArtifactRef::new(ProviderRef::new("acyclic.pi", "native-lazy", "1")?,
                frozen.id.into_bytes(), None)?), None)
        };
        let capture = Self { source, frozen, revision, budget, cancellation, work, _metadata_custody: metadata };
        capture.authenticate().await?;
        Ok(capture)
    }

    pub(crate) fn into_arc_with_native_work(mut self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        if !self._metadata_custody.as_ref().is_some_and(|custody| custody.matches_work(work)) {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi lazy capture sharing requires its original native descriptor custody" });
        }
        let mut slot = work.claim((std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>()) as u64)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        self._metadata_custody.as_mut().expect("native capture custody checked")
            .absorb(&mut slot).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(Arc::new(self))
    }

    pub(crate) const fn frozen(&self) -> LazySnapshotRef { self.frozen }
    pub(crate) fn selection_with_native_work(&self, work: &acyclic_fs::NativeMaterializationWork)
        -> Result<(ForkSelection, acyclic_fs::NativeAllocation)> {
        #[derive(serde::Serialize)]
        struct Selection<'a> { required: bool, revision: &'a ResourceRevision }
        let maximum = crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64;
        let bytes = crate::pi_root_provider::canonical_pi_value_with_native_work(
            &Selection { required: true, revision: &self.revision }, maximum, work)?;
        crate::pi_root_provider::guarded_pi_decode::<ForkSelection>(&bytes.0, maximum, maximum,
            acyclic_harness::JsonValueSchema::None, LAZY_DIAGNOSTIC_LABEL_BYTES, work)
    }

    async fn authenticate(&self) -> Result<()> {
        if let Some(work) = &self.work {
            return self.source.validate_snapshot_with_native_work(self.frozen, work).await.map_err(native_lazy_error);
        }
        let result = self.source.validate_snapshot_measured(self.frozen, self.budget, &self.cancellation).await;
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

    async fn selected_with_native_work(&self, request: &ForkRequest, selection: &ForkSelection,
        work: &acyclic_fs::NativeMaterializationWork) -> Result<(Capture, acyclic_fs::NativeAllocation)> {
        if !selection.required || selection.revision != self.revision {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi lazy capture differs from its original required immutable selection" });
        }
        let mut member = false;
        for selected in &request.selections {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            if selected == selection { member = true; }
        }
        if !member {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi lazy capture is not a member of the original fork request" });
        }
        self.source.validate_snapshot_with_native_work(self.frozen, work).await.map_err(native_lazy_error)?;
        #[derive(serde::Serialize)]
        struct Resource<'a> { source: &'a ResourceRevision, revision: &'a ResourceRevision }
        #[derive(serde::Serialize)]
        #[serde(tag = "kind", content = "value", rename_all = "snake_case")]
        enum ResultValue<'a> { Captured(Resource<'a>) }
        let maximum = crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64;
        let bytes = crate::pi_root_provider::canonical_pi_value_with_native_work(
            &ResultValue::Captured(Resource { source: &self.revision, revision: &self.revision }), maximum, work)?;
        crate::pi_root_provider::guarded_pi_decode::<Capture>(&bytes.0, maximum, maximum,
            acyclic_harness::JsonValueSchema::None, LAZY_DIAGNOSTIC_LABEL_BYTES, work)
    }
}

impl ForkCaptureProvider for PiLazyCapture {
    fn provider(&self) -> &ProviderRef { self.revision.provider() }
    fn capture<'a>(&'a self, request: &'a ForkRequest, selection: &'a ForkSelection)
        -> BoxProviderFuture<'a, Result<Capture>> {
        Box::pin(self.selected(request, selection))
    }
    fn reconcile<'a>(&'a self, request: &'a ForkRequest, selection: &'a ForkSelection)
        -> BoxProviderFuture<'a, Result<Option<Capture>>> {
        // This is immutable provider observation, never another allocation.
        Box::pin(async move { self.selected(request, selection).await.map(Some) })
    }

    fn capture_with_native_work<'a>(&'a self, request: &'a ForkRequest, selection: &'a ForkSelection,
        work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<acyclic_harness::NativeFutureBundle<'a, (Capture, acyclic_fs::NativeAllocation)>> {
        acyclic_harness::NativeFutureBundle::new(work, || self.selected_with_native_work(request, selection, work))
    }

    fn reconcile_with_native_work<'a>(&'a self, request: &'a ForkRequest, selection: &'a ForkSelection,
        work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<acyclic_harness::NativeFutureBundle<'a, (Option<Capture>, acyclic_fs::NativeAllocation)>> {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            let (capture, custody) = self.selected_with_native_work(request, selection, work).await?;
            Ok((Some(capture), custody))
        })
    }
}

impl ForkSeedVerifier for PiLazyCapture {
    fn provider(&self) -> &ProviderRef { self.revision.provider() }
    fn verify<'a>(&'a self, seed: &'a ForkSeed) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            seed.validate()?;
            let mut found = false;
            for capture in &seed.resources {
                if capture.source.provider() == self.revision.provider() || capture.revision.provider() == self.revision.provider() {
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

    fn verify_with_native_work<'a>(&'a self, seed: &'a ForkSeed, work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<acyclic_harness::NativeFutureBundle<'a, ()>> {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            let mut found = false;
            for capture in &seed.resources {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                if capture.source.provider() == self.revision.provider() || capture.revision.provider() == self.revision.provider() {
                    if found || capture.source != self.revision || capture.revision != self.revision {
                        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                            reason: "published Pi lazy capture changed its original native cut" });
                    }
                    found = true;
                }
            }
            if !found { return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "published Pi fork lost its required immutable native cut" }); }
            self.source.validate_snapshot_with_native_work(self.frozen, work).await.map_err(native_lazy_error)
        })
    }
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
    view: &acyclic_harness::filesystem::NativeVolumeView<LocalAuthorityBackend, LocalObjectBackend>,
    source: &LocalLazyWorkspace, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(LazySnapshotRef, acyclic_fs::NativeAllocation)> {
    if context.durable_task_id() != Some(owner.task_id()) {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi source checkpoint requires its actual source task context" });
    }
    owner.verify_with_native_work(false, work).await?;
    let checkpoint = view.checkpoint_projects_with_native_work(owner.native_view_owner(), work)?.await?;
    owner.verify_with_native_work(false, work).await?;
    let mut selected = checkpoint.0.iter().filter(|(_, snapshot)| snapshot.workspace_id == source.workspace().id());
    let snapshot = selected.next().ok_or_else(|| Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Unauthorized,
        reason: "Pi checkpoint has no actual selected source workspace" })?.1;
    if selected.next().is_some() {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi checkpoint repeats a selected source workspace" });
    }
    drop(checkpoint.0);
    Ok((snapshot, checkpoint.1))
}

