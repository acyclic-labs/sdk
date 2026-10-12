//! Provider-owned fork preparation journal and exact Filesystem captures.

use super::{FilesystemHost, ParentProjectController, WorkspaceMutation, map_error};
use crate::contract::capability;
use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{ContentGrant, ContentResidencyVerifier, VolumeOperation, VolumeRef},
    core::{Authority, AuthorityVerifier, Reducer, Scope},
    fork::{
        Capture, CapturedResource, ForkCaptureProvider, ForkPreparer, ForkReport, ForkRequest,
        ForkSeed, ForkSelection, InheritedConversationPrefix, PreparedForkHistory,
        PreparedSummaryFork, ReferenceGrant, ResourceRevision, SharedGrant, SummaryForkSelection,
    },
    resource_preparation::{ResourceCreationOrigin, TaskResourcePreparation},
    resources::{ProviderRef, ResourceRef, WorkspaceRef},
    source_authorization::ProjectSourceReadHandle,
    runtime::RuntimeScope,
    store::{HistoryCursor, HistoryReader},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::{BoxProviderFuture as BoxFuture, StreamProvider};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, sync::Arc};
use uuid::Uuid;

mod created_access;
pub(crate) mod bounded_io;
pub(crate) use bounded_io::PreparationIo;
pub use created_access::{FilesystemResourceCreationProvider, FilesystemRootBootstrapAllocation};

const MAX_REQUEST_BYTES: u64 = 4 * 1024 * 1024;
const MAX_REPORT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_CAPTURE_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ForkAllocationRole { Scratch, Project, Result }

fn creation_volumes(request: &ForkRequest) -> impl Iterator<Item=(&VolumeRef,ForkAllocationRole)> {
    [
        (Some(&request.preparation.child_private_volume), ForkAllocationRole::Scratch),
        (Some(&request.preparation.child_project_volume), ForkAllocationRole::Project),
        (request.preparation.child_result_volume.as_ref(), ForkAllocationRole::Result),
    ].into_iter().filter_map(|(volume, role)| volume.map(|volume| (volume,role)))
}

fn seed_creation_volumes(seed: &ForkSeed) -> Result<impl Iterator<Item=(&VolumeRef,ForkAllocationRole)>> {
    let project = seed.resources.iter().find_map(|resource| match &resource.revision {
        ResourceRevision::Project { volume, .. } => Some(volume),
        _ => None,
    }).ok_or_else(|| Error::Invalid("fork has no child project".into()))?;
    Ok([
        (Some(&seed.child_private_volume), ForkAllocationRole::Scratch),
        (Some(project), ForkAllocationRole::Project),
        (seed.child_result_volume.as_ref(), ForkAllocationRole::Result),
    ].into_iter().filter_map(|(volume,role)| volume.map(|volume| (volume,role))))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AllocationClaim {
    operation_id: OperationId,
    preparation_digest: [u8; 32],
    pub(crate) task_origin: Option<ResourceCreationOrigin>,
    parent: Authority,
    child: Authority,
    volume: VolumeRef,
    role: ForkAllocationRole,
}

impl bounded_io::UploadRecord for [u8; 32] {
    fn logical_owned_bytes(&self) -> Result<u64> { Ok(0) }
}

impl bounded_io::UploadRecord for AllocationClaim {
    fn logical_owned_bytes(&self) -> Result<u64> {
        let provider = self.volume.provider();
        let owner = match self.volume.owner() {
            crate::conversation::VolumeOwner::Project(owner)
            | crate::conversation::VolumeOwner::Session(owner) => owner.len(),
            crate::conversation::VolumeOwner::Agent(_) => 0,
        };
        let mut bytes = [
            self.parent.id.len(), self.child.id.len(), provider.namespace().len(),
            provider.family().len(), provider.version().len(), self.volume.id().len(), owner,
        ].into_iter().try_fold(0_u64, |total,length| total.checked_add(length as u64)
            .ok_or_else(|| Error::Invalid("allocation decode ownership overflows".into())))?;
        if let Some(origin) = &self.task_origin {
            for length in [origin.owner.id.len(), origin.creation_fence.reservation_id.len(),
                origin.creation_fence.placement.len()] {
                bytes = bytes.checked_add(length as u64)
                    .ok_or_else(|| Error::Invalid("allocation origin ownership overflows".into()))?;
            }
            for grant in origin.source_grants.iter() {
                bytes = bytes.checked_add(std::mem::size_of::<String>() as u64)
                    .and_then(|bytes| bytes.checked_add(grant.len() as u64))
                    .ok_or_else(|| Error::Invalid("allocation source grant ownership overflows".into()))?;
            }
        }
        Ok(bytes)
    }
}

impl bounded_io::PreparationRecord for AllocationClaim {}
impl bounded_io::PreparationRecord for [u8; 32] {}
impl bounded_io::PreparationRecord for Uuid {}
impl bounded_io::PreparationRecord for Capture {}

macro_rules! fork_preparation_records {
    ($($record:ident),+ $(,)?) => {$(
        impl bounded_io::PreparationRecord for $record {
            fn maximum_slot_bytes() -> u64 { super::default_tools::recursive_decode_slot_bytes::<Self>() }
            fn json_value_schema() -> crate::contract::JsonValueSchema { crate::contract::JsonValueSchema::$record }
        }
    )+};
}
fork_preparation_records!(ForkRequest, ForkReport);

/// Prepares idempotent captures of parent history and filesystem resources.
pub struct FilesystemForkPreparer<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    parent: Reducer,
    verifier: AuthorityVerifier,
    scope: Scope,
    project: VolumeRef,
    stream_provider: ProviderRef,
    resolver: Arc<dyn ContentResidencyVerifier>,
    capture_providers: Vec<Arc<dyn ForkCaptureProvider>>,
    summary: Option<BoundSummary>,
    history: Option<PreparedForkHistory>,
    task_preparation: Option<Arc<TaskResourcePreparation>>,
    project_source: Option<ProjectSourceReadHandle>,
    #[cfg(not(target_arch = "wasm32"))]
    native: Option<(acyclic_fs::NativeMaterializationWork, acyclic_fs::NativeAllocation)>,
}

struct BoundSummary {
    selection: SummaryForkSelection,
    journal: Arc<dyn crate::executor::ExecutionJournal>,
    source: Box<dyn SummaryHistorySource>,
}

trait SummaryHistorySource: acyclic_stream::ProviderPlatform {
    fn prepare<'a>(
        &'a self, parent: &'a Reducer, selection: SummaryForkSelection,
        journal: &'a dyn crate::executor::ExecutionJournal,
        resolver: &'a dyn ContentResidencyVerifier,
        io: Option<&'a super::PreparationIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, PreparedSummaryFork>>;

}

struct OriginalSummaryHistory<P> {
    scope: RuntimeScope,
    history: HistoryReader<P>,
    cursor: HistoryCursor,
}

impl<P: StreamProvider> SummaryHistorySource for OriginalSummaryHistory<P> {
    fn prepare<'a>(
        &'a self, parent: &'a Reducer, selection: SummaryForkSelection,
        journal: &'a dyn crate::executor::ExecutionJournal,
        resolver: &'a dyn ContentResidencyVerifier,
        io: Option<&'a super::PreparationIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, PreparedSummaryFork>> {
        let build = || crate::fork::prepare_summary_fork_context_with_io(parent, selection, journal,
            &self.scope, &self.history, &self.cursor, resolver, io);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            return Ok(crate::store::history::HistoryContentFuture::Native(
                crate::NativeFutureBundle::new(work, build)?));
        }
        match io {
            Some(io) => io.history_future(build),
            None => Ok(crate::store::history::HistoryContentFuture::Ordinary {
                future: Box::pin(build()), frame_bytes: 0,
            }),
        }
    }

}

struct PreparationAttempt<'a, A, O> {
    preparer: &'a FilesystemForkPreparer<A, O>,
    io: Option<PreparationIo>,
    task_preparation: Option<&'a TaskResourcePreparation>,
    #[cfg(not(target_arch = "wasm32"))]
    native_creation_guard: Option<Arc<crate::resource_preparation::TaskResourcePreparation>>,
}

impl<A, O> FilesystemForkPreparer<A, O> {
    /// Binds the authenticated parent, history and optional original native custody.
    #[allow(clippy::too_many_arguments, reason = "providers and authority are explicit")]
    pub fn new(
        host: Arc<FilesystemHost<A, O>>, parent: Reducer, verifier: AuthorityVerifier,
        scope: Scope, project: VolumeRef, stream_provider: ProviderRef,
        resolver: Arc<dyn ContentResidencyVerifier>, history: Option<PreparedForkHistory>,
        project_source: Option<ProjectSourceReadHandle>,
        #[cfg(not(target_arch = "wasm32"))]
        native: Option<(acyclic_fs::NativeAllocation, &acyclic_fs::NativeMaterializationWork)>,
    ) -> Result<Self> {
        let this = Self { host, parent, verifier, scope, project, stream_provider, resolver,
            capture_providers: Vec::new(), summary: None, history, project_source, task_preparation: None,
            #[cfg(not(target_arch = "wasm32"))] native: native.map(|(custody, work)| (work.clone(), custody)),
        };
        let reject = |code, reason: &'static str| {
            #[cfg(not(target_arch = "wasm32"))]
            if this.native.is_some() { return Error::NativeBoundary { code, reason }; }
            match code {
                crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
                crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
                _ => Error::Invalid(reason.into()),
            }
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some((work, custody)) = &this.native {
            if !custody.matches_work(work) || this.history.is_none() {
                return Err(reject(crate::wire::ErrorCode::Invalid, "native fork lacks matching custody or prepared history"));
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        let mut observer = this.native.as_ref().map(|(work, _)| crate::store::history::native_work_io(work)).transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut work = observer.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(target_arch = "wasm32")]
        let mut work = None;
        this.stream_provider.validate(&mut work)?;
        if this.stream_provider.family() != "stream" || this.parent.authority().kind != crate::core::AggregateKind::Conversation {
            return Err(reject(crate::wire::ErrorCode::Invalid, "fork requires Stream history and a parent conversation"));
        }
        this.verifier.verify_audience(this.parent.authority(), &mut work)?;
        this.verifier.verify(&this.scope, &mut work)?;
        if this.scope.agent().is_none() || this.parent.conversation().and_then(|state| state.agent) != this.scope.agent() {
            return Err(reject(crate::wire::ErrorCode::Unauthorized, "fork scope is not bound to the parent agent"));
        }
        this.project.validate(&mut work)?;
        if this.project.provider() != &this.host.provider {
            return Err(reject(crate::wire::ErrorCode::Invalid, "fork project belongs to another provider"));
        }
        if let Some(history) = &this.history {
            history.verify(&this.verifier, &mut work)?;
            if history.parent() != this.parent.authority() || history.parent_revision() != this.parent.revision() {
                return Err(reject(crate::wire::ErrorCode::Conflict, "fork history differs from the bound parent"));
            }
        }
        if this.project_source.as_ref().is_some_and(|handle| handle.authorization().project.provider() != &this.host.provider) {
            return Err(reject(crate::wire::ErrorCode::Unauthorized, "fork source belongs to another provider"));
        }
        drop(work);
        #[cfg(not(target_arch = "wasm32"))]
        drop(observer);
        Ok(this)
    }

    /// Shares this preparer on its native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(mut self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        if !self.native.as_ref().is_some_and(|(_, guard)| guard.matches_work(work)) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "fork preparer belongs to another original resource" });
        }
        let mut header = work.claim(std::mem::size_of::<Self>() as u64, u64::from((std::mem::size_of::<Self>() as u64) != 0), acyclic_fs::WorkCounters::default())
            .map_err(Error::from)?;
        self.native.as_mut().expect("native custody was checked").1.absorb(&mut header)
            .map_err(|cause| Error::NativeWork { cause, custody: Some(header) })?;
        Ok(Arc::new(self))
    }

    /// Binds the original Summary history, retaining its input custody on native work.
    pub fn with_summary_history<P: StreamProvider + 'static>(
        mut self,
        selection: SummaryForkSelection,
        journal: Arc<dyn crate::executor::ExecutionJournal>,
        scope: RuntimeScope,
        history: HistoryReader<P>,
        cursor: HistoryCursor,
        #[cfg(not(target_arch = "wasm32"))]
        custody: Option<acyclic_fs::NativeAllocation>,
    ) -> Result<Self> {
        let mut io = None;
        #[cfg(not(target_arch = "wasm32"))]
        match (self.native.as_mut(), custody) {
            (Some((work, retained)), Some(mut custody)) if custody.matches_work(work) => {
                retained.absorb(&mut custody).map_err(|cause| Error::NativeWork { cause, custody: Some(custody) })?;
                let binding = PreparationIo::from_native_work(work)?;
                binding.retain_owned(std::mem::size_of::<OriginalSummaryHistory<P>>() as u64)?;
                binding.allocation_attempt(std::mem::size_of::<OriginalSummaryHistory<P>>() as u64)?;
                io = Some(binding);
            }
            (None, None) => {}
            (_, Some(custody)) => return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout, custody: Some(custody),
            }),
            _ => return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "summary inputs lack their original native custody" }),
        }
        selection.validate_with_work(&mut io.as_ref().map(PreparationIo::history_io)
            .as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo))?;
        if cursor.authority != *self.parent.authority()
            || cursor.through_revision != self.parent.revision()
            || journal.canonical_authority() != Some(self.parent.authority()) {
            return Err(bounded_io::failure(io.as_ref(), crate::wire::ErrorCode::Conflict,
                "summary history differs from this original parent"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let retained = io.owned_bytes()?;
            let mut custody = io.into_native_custody(retained)?;
            self.native.as_mut().expect("native preparer retains its input custody").1
                .absorb(&mut custody).map_err(|cause| Error::NativeWork { cause, custody: Some(custody) })?;
        }
        self.summary = Some(BoundSummary {
            selection, journal, source: Box::new(OriginalSummaryHistory { scope, history, cursor }),
        });
        Ok(self)
    }

    /// Pins an authenticated ordinary preparation task to this provider.
    pub fn with_task_preparation(
        mut self,
        preparation: Arc<TaskResourcePreparation>,
    ) -> Result<Self> {
        if preparation.request().provider != self.host.provider
            || preparation.request().creation_grant != capability::FORK_PUBLISH
        {
            return Err(Error::Unauthorized(
                "fork preparation task belongs to another provider or creation authority".into(),
            ));
        }
        self.task_preparation = Some(preparation);
        Ok(self)
    }

    /// Registers an exact provider-owned resource capture.
    pub fn with_capture_provider(mut self, provider: Arc<dyn ForkCaptureProvider>) -> Result<Self> {
        provider.provider().validate(&mut None)?;
        if self
            .capture_providers
            .iter()
            .any(|bound| bound.provider() == provider.provider())
        {
            return Err(Error::Invalid(
                "fork capture provider is registered twice".into(),
            ));
        }
        self.capture_providers.push(provider);
        Ok(self)
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemForkPreparer<A, O> {
    fn controller(&self, io: Option<&PreparationIo>) -> Result<ParentProjectController<'_, A, O>> {
        ParentProjectController::new(&self.host, &self.parent, &self.verifier, &self.scope, &self.project, io)
    }

    /// Projects the exact foreign reader grants for child admission.
    pub(crate) async fn required_reference_grants(
        &self, request: &ForkRequest, io: Option<&PreparationIo>,
    ) -> Result<Vec<ReferenceGrant>> {
        let attempt;
        let io = match io {
            Some(io) => Some(io),
            None => { attempt = self.attempt(self.task_preparation.as_deref())?; attempt.io.as_ref() }
        };
        self.check_request(request, io, self.task_preparation.as_deref())?;
        let mut grants = self.reference_grants(request, io).await?;
        if request.preparation.summary.is_some() {
            let summary = self.prepare_bound_summary(request, io).await?
                .ok_or_else(|| bounded_io::failure(io, crate::wire::ErrorCode::Invalid, "summary source was not authenticated"))?;
            let (_, references) = super::summary_fork::split_references(&summary, io)?;
            let mut selected = BTreeSet::new();
            for grant in &grants {
                reference_selection(&mut selected, &grant.file, grant.reader, io)?;
            }
            let candidates = references.len().checked_mul(request.attached_agents.len() + 1)
                .and_then(|count| count.checked_add(grants.len()))
                .ok_or_else(|| bounded_io::failure(io, crate::wire::ErrorCode::Invalid, "summary reader projection overflows"))?;
            bounded_io::reserve(&mut grants,
                candidates.min(request.preparation.maximum_inherited_references as usize), io)?;
            for reader in std::iter::once(request.child_agent).chain(request.attached_agents.iter().copied()) {
                for file in references.values() {
                    if file.volume().owner() != &crate::conversation::VolumeOwner::Agent(reader)
                        && reference_selection(&mut selected, file, reader, io)? {
                        if selected.len() > request.preparation.maximum_inherited_references as usize {
                            return Err(bounded_io::failure(io, crate::wire::ErrorCode::Invalid, "summary reader grants exceed capture allowance"));
                        }
                        grants.push(ReferenceGrant { file: crate::executor::with_history_io(io, |work| file.copy(work))?,
                            reader, attachment_manifest: None });
                    }
                }
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work)
            && let Some(task) = &self.task_preparation {
            task.verify_creation_with_native_work(work)?.await?;
        }
        Ok(grants)
    }

    async fn reference_grants(&self, request: &ForkRequest, io: Option<&PreparationIo>) -> Result<Vec<ReferenceGrant>> {
        let maximum_messages = usize::try_from(request.preparation.maximum_inherited_messages)
            .map_err(|_| bounded_io::failure(io, crate::wire::ErrorCode::Invalid, "fork message bound is too large"))?;
        #[cfg(not(target_arch = "wasm32"))]
        let native = io.and_then(PreparationIo::original_native_work);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = native && let Some(task) = &self.task_preparation {
            task.verify_creation_with_native_work(work)?.await?;
        }
        let controller = self.controller(io.filter(|io| io.original_native_work().is_some()))?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = native {
            let (grants, custody) = controller.inherited_reference_grants_with_native_work(
                &self.parent, request.child_agent, &request.attached_agents,
                request.preparation.inherited_through_sequence, maximum_messages,
                request.preparation.maximum_inherited_bytes,
                request.preparation.maximum_inherited_references as usize,
                self.resolver.as_ref(), self.history.as_ref(), work,
            ).await?;
            io.expect("native projection IO").absorb_native_custody(custody)?;
            return Ok(grants);
        }
        controller.inherited_reference_grants(
            &self.parent, request.child_agent, &request.attached_agents,
            request.preparation.inherited_through_sequence, maximum_messages,
            request.preparation.maximum_inherited_bytes,
            request.preparation.maximum_inherited_references as usize,
            self.resolver.as_ref(), self.history.as_ref(),
        ).await
    }

    /// Builds the exact created identities and their allocation journal proofs.
    pub(crate) fn creation_allocations(&self, request: &ForkRequest, preparation: crate::TaskId,
        io: Option<&PreparationIo>) -> Result<(Vec<ResourceRef>, Vec<crate::resource_preparation::ResourceCreationProofRef>)> {
        if io.is_some_and(|io| io.original_native_work().is_some()) {
            self.check_request(request, io, self.task_preparation.as_deref())?;
        } else {
            let mut history = io.map(PreparationIo::history_io);
            request.validate(&mut history.as_mut().map(|history| history as &mut dyn crate::store::history::HistoryIo))?;
            for (volume, _) in creation_volumes(request) {
                if volume.provider() != &self.host.provider {
                    return Err(bounded_io::failure(io, crate::wire::ErrorCode::Invalid, "fork resource belongs to another provider"));
                }
            }
        }
        let count = creation_volumes(request).count();
        let mut resources = bounded_io::vector(count, io)?;
        let mut proofs = bounded_io::vector(count, io)?;
        for (volume, _) in creation_volumes(request) {
            let resource = bounded_io::content_workspace(&self.host.provider, volume, io)?.into_resource();
            let copied = crate::executor::with_history_io(io, |work| {
                let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
                let provider = crate::core::copy_provider(resource.provider(), &mut quota)?;
                let key = crate::core::copy_copy_vector(resource.key(), &mut quota)?;
                let version = resource.version().map(|value| crate::core::copy_text(value, &mut quota)).transpose()?;
                let copied = ResourceRef::new(resource.kind(), provider, key, version, quota.work())?;
                quota.transfer();
                Ok::<_, Error>(copied)
            })?;
            resources.push(copied);
            proofs.push(crate::resource_preparation::ResourceCreationProofRef {
                provider: match io { Some(io) => self.host.provider.clone_with_io(io)?, None => self.host.provider.clone() },
                preparation, resource, evidence: allocation_ref(&self.host.provider, volume, io)?.into_resource(),
            });
        }
        Ok((resources, proofs))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn require_native_projection_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        work.remaining().map_err(Error::from)?;
        if !self.native.as_ref().is_some_and(|(_, custody)| custody.matches_work(work)) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "fork projection belongs to another original resource" });
        }
        Ok(())
    }

    fn sealed_project_read<'a>(
        &'a self,
        request: &ForkRequest,
        volume: &VolumeRef,
        generation: &crate::resources::GenerationRef, io: Option<&PreparationIo>,
        task: Option<&'a TaskResourcePreparation>,
    ) -> Result<&'a ContentGrant> {
        let authorization = request.authorized_source.as_ref().ok_or_else(||
            bounded_io::failure(io, crate::wire::ErrorCode::Unauthorized, "fork source has no sealed project read"))?;
        let handle = task.and_then(TaskResourcePreparation::project_source)
            .or(self.project_source.as_ref()).ok_or_else(|| bounded_io::failure(io, crate::wire::ErrorCode::Unauthorized, "fork source descriptor has no sealed project read"))?;
        if handle.authorization() != authorization {
            return Err(bounded_io::failure(io, crate::wire::ErrorCode::Unauthorized, "fork source descriptor differs from its sealed project read"));
        }
        if authorization.source_task != authorization.caller_task
            && (request.preparation.inherited_through_sequence != 0 || request.preparation.summary.is_some())
        {
            return Err(bounded_io::failure(io, crate::wire::ErrorCode::Unsupported, "foreign workspace source cannot inherit conversation history"));
        }
        handle.require_selection(request.operation_id, authorization.source_task, volume, generation,
            authorization.native_snapshot.as_ref(), io.and_then(PreparationIo::original_native_work))?;
        Ok(handle.grant())
    }

    fn check_request(
        &self, request: &ForkRequest, io: Option<&PreparationIo>,
        task: Option<&TaskResourcePreparation>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            self.require_native_projection_work(work)?;
        }
        let reject = |code, reason| bounded_io::failure(io, code, reason);
        let read_granted = |volume: &VolumeRef| -> Result<bool> {
            let parent = match io {
                Some(io) => io.contains_capability(self.scope.capabilities(), volume, "volume:read:")?,
                None => self.scope.capabilities().contains(&volume.capability(VolumeOperation::Read, &mut None)?),
            };
            if !parent { return Ok(false); }
            match task {
                Some(task) => io.ok_or_else(|| reject(crate::wire::ErrorCode::Invalid, "task preparation lacks its IO ledger"))?
                    .contains_capability(task.grants(), volume, "volume:read:"),
                None => Ok(true),
            }
        };
        let mut history = io.map(PreparationIo::history_io);
        let mut observer = history.as_mut().map(|history| history as &mut dyn crate::store::history::HistoryIo);
        request.validate(&mut observer)?;
        if !self.scope.capabilities().contains(capability::FORK_PUBLISH) {
            return Err(reject(crate::wire::ErrorCode::Unauthorized, "parent project operation is not granted"));
        }
        if request.parent != *self.parent.authority() || request.parent_revision != self.parent.revision() {
            return Err(reject(crate::wire::ErrorCode::Conflict, "fork request differs from its original parent revision"));
        }
        if request.boundary.is_some() {
            return Err(reject(crate::wire::ErrorCode::Unsupported, "this preparer cannot attest a cross-provider boundary"));
        }
        if self.scope.agent() == Some(request.child_agent) {
            return Err(reject(crate::wire::ErrorCode::Invalid, "fork child must have a distinct agent identity"));
        }
        for (volume, _) in creation_volumes(request) {
            if volume.provider() != &self.host.provider {
                return Err(reject(crate::wire::ErrorCode::Invalid, "fork resource belongs to another provider"));
            }
        }
        for selection in &request.selections {
            match &selection.revision {
                ResourceRevision::History(reference) if reference.as_resource().provider() != &self.stream_provider => {
                    return Err(reject(crate::wire::ErrorCode::Invalid, "fork history belongs to another Stream provider"));
                }
                ResourceRevision::Project { volume, generation } => {
                    if request.authorized_source.is_some() {
                        self.sealed_project_read(request, volume, generation, io, task)?
                            .require(volume, VolumeOperation::Read, io)?;
                    } else if !read_granted(volume)? {
                        return Err(reject(crate::wire::ErrorCode::Unauthorized, "selected project source-read is not granted"));
                    }
                }
                ResourceRevision::PrivateVolume { volume, .. } => {
                    let agent = self.scope.agent().expect("fork scope is bound to its parent agent");
                    if volume.owner() != &crate::conversation::VolumeOwner::Agent(agent)
                        || !read_granted(volume)?
                    {
                        return Err(reject(crate::wire::ErrorCode::Unauthorized, "scratch source-read is outside the direct parent"));
                    }
                }
                _ => {}
            }
        }
        drop(observer);
        drop(history);
        if let Some(task) = task {
            let io = io.ok_or_else(|| reject(crate::wire::ErrorCode::Invalid, "task preparation lacks its IO ledger"))?;
            let bound = task.request();
            if task.origin().intended_child != bound.child.operation_id
                || bound.child.parent != Some(task.origin().parent_task)
                || task.grants() != &task.origin().source_grants
            {
                return Err(reject(crate::wire::ErrorCode::Unauthorized,
                    "fork preparation differs from its genuine immutable owner admission"));
            }
            if let Some(original) = self.task_preparation.as_deref() {
                if original.request() != bound
                    || !same_task_origin(Some(original.origin()), Some(task.origin()))
                {
                    return Err(reject(crate::wire::ErrorCode::Unauthorized,
                        "fork preparer belongs to another genuine preparation task"));
                }
            }
            if bound.provider != self.host.provider || bound.creation_grant != capability::FORK_PUBLISH
                || bound.read_authorization != request.authorized_source
                || request.preparation.maximum_inherited_bytes > bound.maximum_bytes
                || bound.resources.len() != creation_volumes(request).count()
            {
                return Err(reject(crate::wire::ErrorCode::Unauthorized, "fork differs from its admitted preparation boundary"));
            }
            let request_hold;
            let encoded = io.encode(request, bound.maximum_bytes, true)?;
            request_hold = io.guard_bytes(&encoded)?;
            let payload_hold;
            let payload = io.encode(&bound.payload, bound.maximum_bytes, true)?;
            payload_hold = io.guard_bytes(&payload)?;
            if encoded != payload {
                return Err(reject(crate::wire::ErrorCode::Unauthorized, "fork request differs from its admitted preparation"));
            }
            drop(payload);
            drop(payload_hold);
            drop(encoded);
            drop(request_hold);
            for (resource, (volume, _)) in bound.resources.iter().zip(creation_volumes(request)) {
                let before = io.owned_bytes()?;
                let resource_hold;
                let actual = bounded_io::content_workspace(&self.host.provider, volume, Some(io))?.into_resource();
                resource_hold = io.guard_owned(io.owned_bytes()? - before)?;
                if resource != &actual {
                    return Err(reject(crate::wire::ErrorCode::Unauthorized, "fork allocations differ from their admitted preparation"));
                }
                drop(actual);
                drop(resource_hold);
            }
            match (&request.authorized_source, task.project_source()) {
                (Some(authorization), Some(handle)) if handle.authorization() == authorization
                    && authorization.owner == task.origin().owner
                    && authorization.caller_task == task.origin().parent_task => {}
                (None, None) => {}
                _ => return Err(reject(crate::wire::ErrorCode::Unauthorized,
                    "fork preparation lacks its original owner's project source")),
            }
        }
        if let Some(history) = &self.history {
            if history.through_sequence() != request.preparation.inherited_through_sequence {
                return Err(reject(crate::wire::ErrorCode::Conflict, "fork archive differs from its selected logical cut"));
            }
        } else if self.parent.conversation().is_some_and(|state|
            state.resident_after_sequence() > 0 && request.preparation.inherited_through_sequence > 0)
        {
            return Err(reject(crate::wire::ErrorCode::Invalid, "archived fork requires its authenticated history capture"));
        }
        if let Some(selection) = &request.preparation.summary {
            let summary = self.summary.as_ref().ok_or(reject(crate::wire::ErrorCode::Unsupported,
                "summary fork owner-bound checkpoint provider is not installed"))?;
            if &summary.selection != selection
                || self.parent.conversation().map_or(0, |state| state.logical_revision())
                    != request.preparation.inherited_through_sequence
            {
                return Err(reject(crate::wire::ErrorCode::Conflict, "summary fork differs from its original capture"));
            }
        }
        Ok(())
    }

    pub(super) async fn prepare_bound_summary(
        &self, request: &ForkRequest, io: Option<&super::PreparationIo>,
    ) -> Result<Option<PreparedSummaryFork>> {
        let Some(selection) = &request.preparation.summary else { return Ok(None) };
        #[cfg(not(target_arch = "wasm32"))]
        match (&self.native, io.and_then(PreparationIo::original_native_work)) {
            (Some(_), Some(work)) => self.require_native_projection_work(work)?,
            (None, None) => {}
            _ => return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "summary preparation requires its original native work" }),
        }
        let bound = self.summary.as_ref().ok_or_else(|| bounded_io::failure(io, crate::wire::ErrorCode::Unsupported,
            "summary fork owner-bound checkpoint provider is not installed"))?;
        if selection != &bound.selection {
            return Err(bounded_io::failure(io, crate::wire::ErrorCode::Conflict,
                "summary fork request differs from its original capture"));
        }
        let future = bound.source.prepare(&self.parent, super::summary_fork::clone_selection(selection, io)?,
            bound.journal.as_ref(), self.resolver.as_ref(), io)?;
        match io {
            Some(io) => io.finish_future(future).await,
            None => future.await,
        }.map(Some)
    }

    fn attempt<'a>(
        &'a self,
        task: Option<&'a TaskResourcePreparation>,
    ) -> Result<PreparationAttempt<'a, A, O>> {
        let mut io = None;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some((work, _)) = &self.native { io = Some(PreparationIo::from_native_work(work)?); }
        if io.is_none() {
            io = task.map(|task| {
                let cancellation = task.cancellation().ok_or_else(|| Error::Unauthorized(
                    "task-bound preparation has no original invocation cancellation token".into()))?;
                PreparationIo::new(task.request(), cancellation)
            }).transpose()?;
        }
        if let (Some(io), Some(task)) = (&io, task) {
            io.encode(task.request(), task.request().maximum_bytes, false)?;
        }
        Ok(PreparationAttempt { preparer: self, io, task_preparation: task,
            #[cfg(not(target_arch = "wasm32"))]
            native_creation_guard: None })
    }

    /// Prepares captures under the authenticated preparation task.
    pub async fn prepare_for_task(
        &self,
        request: ForkRequest,
        preparation: &TaskResourcePreparation,
    ) -> Result<ForkReport> {
        if preparation.request().provider != self.host.provider
            || preparation.request().creation_grant != capability::FORK_PUBLISH
        {
            return Err(Error::Unauthorized(
                "fork preparation task belongs to another provider or creation authority".into(),
            ));
        }
        let attempt = self.attempt(Some(preparation))?;
        self.check_request(&request, attempt.io.as_ref(), Some(preparation))?;
        attempt.prepare(request).await
    }

    /// Borrows the exact admitted request and genuine current preparation owner.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn prepare_for_task_with_native_work<'a>(
        &'a self, preparation: &'a Arc<TaskResourcePreparation>, request: &'a ForkRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ForkReport, acyclic_fs::NativeAllocation)>> {
        self.require_native_projection_work(work)?;
        let mut attempt = self.attempt(Some(preparation.as_ref()))?;
        self.check_request(request, attempt.io.as_ref(), Some(preparation.as_ref()))?;
        attempt.native_creation_guard = Some(Arc::clone(preparation));
        crate::NativeFutureBundle::new(work, || async move {
            preparation.verify_creation_with_native_work(work)?.await?;
            let io = attempt.io.as_ref().expect("original native preparer owns its IO");
            io.retain_owned(std::mem::size_of::<ForkReport>() as u64)?;
            let owned = request.copy(&mut Some(&mut io.history_io()))?.0;
            let report = attempt.prepare(owned).await?;
            let retained = io.owned_bytes()?;
            let custody = io.take_native_custody(retained)?;
            Ok((report, custody))
        })
    }

    /// Reconciles the retained report under its settlement owner.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn reconcile_for_task_with_native_work<'a>(
        &'a self, preparation: &'a Arc<TaskResourcePreparation>, request: &'a ForkRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ForkReport>, acyclic_fs::NativeAllocation)>> {
        self.require_native_projection_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            preparation.verify_reconciliation_with_native_work(work)?.await?;
            let attempt = self.attempt(Some(preparation.as_ref()))?;
            let io = attempt.io.as_ref().expect("original native preparer owns its IO");
            self.check_request(request, Some(io), Some(preparation.as_ref()))?;
            let report = attempt.reconcile(request).await?;
            preparation.verify_reconciliation_with_native_work(work)?.await?;
            let retained = io.owned_bytes()?;
            let custody = io.take_native_custody(retained)?;
            Ok((report, custody))
        })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> PreparationAttempt<'_, A, O> {

    fn journal_limit(&self, default: u64) -> u64 {
        self.task_preparation.as_ref().map_or(default, |task| {
            default.min(task.request().maximum_bytes)
        })
    }

    fn clone_authority(&self, value: &Authority) -> Result<Authority> {
        use crate::store::history::HistoryIo as _;
        self.io.as_ref().map_or_else(|| Ok(value.clone()),
            |io| io.history_io().clone_authority(value))
    }

    fn clone_paths(&self, paths: &[String]) -> Result<Vec<String>> {
        let mut values = bounded_io::vector(paths.len(), self.io.as_ref())?;
        for path in paths { values.push(bounded_io::clone_text(path, self.io.as_ref())?); }
        Ok(values)
    }

    fn clone_origin(&self, origin: &ResourceCreationOrigin) -> Result<ResourceCreationOrigin> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = self.io.as_ref() && let Some(work) = io.original_native_work() {
            let (source_grants, custody) = origin.source_grants.clone_with_native_work(work)?;
            io.absorb_native_custody(custody)?;
            return Ok(ResourceCreationOrigin {
                owner: self.clone_authority(&origin.owner)?,
                parent_task: origin.parent_task, preparation_task: origin.preparation_task,
                intended_child: origin.intended_child, admission_digest: origin.admission_digest,
                source_grants, request_digest: origin.request_digest,
                creation_fence: crate::scheduler::LeaseFence {
                    reservation_id: bounded_io::clone_text(&origin.creation_fence.reservation_id, Some(io))?,
                    placement: bounded_io::clone_text(&origin.creation_fence.placement, Some(io))?,
                },
            });
        }
        Ok(origin.clone())
    }

    fn read_grant(&self, volume: &VolumeRef) -> Result<ContentGrant> {
        crate::executor::with_history_io(self.io.as_ref(), |work|
            ContentGrant::verify(&self.preparer.verifier, &self.preparer.scope, volume,
                VolumeOperation::Read, MAX_CAPTURE_BYTES, work))
    }

    async fn verify_task_lease(&self, settlement: bool) -> Result<()> {
        if let Some(preparation) = &self.task_preparation {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = self.io.as_ref().and_then(PreparationIo::original_native_work) {
                return if settlement {
                    preparation.verify_reconciliation_with_native_work(work)?.await
                } else {
                    preparation.verify_creation_with_native_work(work)?.await
                };
            }
            if settlement {
                preparation.verify_reconciliation().await?;
            } else {
                preparation.verify_creation().await?;
            }
        }
        Ok(())
    }

    async fn verify_task_allocations(&self, request: &ForkRequest) -> Result<()> {
        let Some(preparation) = &self.task_preparation else {
            return Ok(());
        };
        let encoded_hold;
        let encoded = encode(request, self.journal_limit(MAX_REQUEST_BYTES), self.io.as_ref())?;
        encoded_hold = self.io.as_ref().map(|io| io.guard_bytes(&encoded)).transpose()?.flatten();
        let digest = *bounded_io::digest(&encoded, self.io.as_ref())?.as_bytes();
        drop(encoded);
        drop(encoded_hold);
        for (volume, role) in creation_volumes(request) {
            let journal = allocation_ref(&self.preparer.host.provider, volume, self.io.as_ref())?;
            let retained = read_record::<A, O, AllocationClaim>(&self.preparer.host, &journal, "/claim.json", self.journal_limit(MAX_REQUEST_BYTES), self.io.as_ref())
            .await?
            .ok_or_else(|| bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Unauthorized, "task-bound allocation is missing"))?;
            if retained.operation_id != request.operation_id || retained.preparation_digest != digest
                || retained.parent != request.parent || retained.child != request.child
                || &retained.volume != volume || retained.role != role
                || !same_task_origin(retained.task_origin.as_ref(), Some(preparation.origin()))
            {
                return Err(bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Unauthorized, "allocation belongs to another preparation task"));
            }
        }
        Ok(())
    }

    async fn reconcile(&self, request: &ForkRequest) -> Result<Option<ForkReport>> {
        let journal = preparation_journal(&self.preparer.host.provider, request.operation_id, self.io.as_ref())?;
        match bounded_io::resolve(&self.preparer.host, &journal, self.io.as_ref()).await {
            Ok(_) => {}
            Err(error) if journal_absent(&error) => return Ok(None),
            Err(error) => return Err(error),
        }
        if !read_claim(&self.preparer.host, &journal, request, self.journal_limit(MAX_REQUEST_BYTES), self.io.as_ref()).await? {
            return Ok(None);
        }
        let report = read_report(&self.preparer.host, &journal, request, self.journal_limit(MAX_REPORT_BYTES), self.io.as_ref()).await?;
        if report.is_some() { self.verify_task_allocations(request).await?; }
        Ok(report)
    }

    async fn claim(&self, request: &ForkRequest) -> Result<WorkspaceRef> {
        let journal = preparation_journal(&self.preparer.host.provider, request.operation_id, self.io.as_ref())?;
        let name = std::str::from_utf8(journal.as_resource().key())
            .map_err(|_| bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Invalid, "fork journal name is not UTF-8"))?;
        let creation_key = IdempotencyKey::new(crate::contract::preparation_format(format_args!("fork:{}:journal", request.operation_id), self.io.as_ref())?)?;
        bounded_io::create(&self.preparer.host, name, &creation_key, self.io.as_ref(), self.task_preparation,
            #[cfg(not(target_arch = "wasm32"))]
            self.native_creation_guard.as_ref(),
        ).await?;
        if read_claim(&self.preparer.host, &journal, request, self.journal_limit(MAX_REQUEST_BYTES), self.io.as_ref()).await? {
            return Ok(journal);
        }
        let key = IdempotencyKey::new(crate::contract::preparation_format(format_args!("fork:{}:claim", request.operation_id), self.io.as_ref())?)?;
        write_record(&self.preparer.host, &journal, "/request.json", request,
            self.journal_limit(MAX_REQUEST_BYTES), &key, self.io.as_ref(), self.task_preparation,
            #[cfg(not(target_arch = "wasm32"))]
            self.native_creation_guard.as_ref(),
        ).await?;
        if !read_claim(&self.preparer.host, &journal, request, self.journal_limit(MAX_REQUEST_BYTES), self.io.as_ref()).await? {
            return Err(Error::Indeterminate(request.operation_id));
        }
        Ok(journal)
    }

    async fn commit_report(
        &self,
        journal: &WorkspaceRef,
        report: &ForkReport,
    ) -> Result<ForkReport> {
        let key = IdempotencyKey::new(crate::contract::preparation_format(format_args!("fork:{}:report", report.request.operation_id), self.io.as_ref())?)?;
        write_record(&self.preparer.host, journal, "/report.json", report,
            self.journal_limit(MAX_REPORT_BYTES), &key, self.io.as_ref(), self.task_preparation,
            #[cfg(not(target_arch = "wasm32"))]
            self.native_creation_guard.as_ref(),
        ).await?;
        match read_report(&self.preparer.host, journal, &report.request, self.journal_limit(MAX_REPORT_BYTES), self.io.as_ref()).await? {
            Some(committed) if committed == *report => Ok(committed),
            Some(_) => Err(bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Conflict, "fork operation has a different committed report")),
            None => Err(Error::Indeterminate(report.request.operation_id)),
        }
    }

    async fn capture_result(
        &self, journal: &WorkspaceRef, index: usize, selection: &ForkSelection,
    ) -> Result<Option<Capture>> {
        let path = crate::contract::preparation_format(format_args!("/capture-{index}-result.json"), self.io.as_ref())?;
        let Some(capture) = read_record::<A, O, Capture>(&self.preparer.host, journal, &path,
            self.journal_limit(MAX_CAPTURE_BYTES), self.io.as_ref()).await? else { return Ok(None) };
        let attempt_path = crate::contract::preparation_format(format_args!("/capture-{index}-attempt.json"), self.io.as_ref())?;
        read_record::<A, O, Uuid>(&self.preparer.host, journal, &attempt_path,
            self.journal_limit(MAX_CAPTURE_BYTES), self.io.as_ref()).await?
            .ok_or_else(|| bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Storage,
                "fork capture result has no durable start"))?;
        validate_capture(selection, &capture)?;
        Ok(Some(capture))
    }

    /// Returns true only to the writer that durably won this attempt.
    async fn start_capture(
        &self, journal: &WorkspaceRef, request: &ForkRequest, index: usize,
    ) -> Result<bool> {
        let path = crate::contract::preparation_format(format_args!("/capture-{index}-attempt.json"), self.io.as_ref())?;
        if read_record::<A, O, Uuid>(&self.preparer.host, journal, &path,
            self.journal_limit(MAX_CAPTURE_BYTES), self.io.as_ref()).await?.is_some() { return Ok(false); }
        let attempt = Uuid::new_v4();
        let key = IdempotencyKey::new(crate::contract::preparation_format(format_args!("fork:{}:capture-attempt:{index}:{attempt}", request.operation_id), self.io.as_ref())?)?;
        write_record(&self.preparer.host, journal, &path, &attempt,
            self.journal_limit(MAX_CAPTURE_BYTES), &key, self.io.as_ref(), self.task_preparation,
            #[cfg(not(target_arch = "wasm32"))]
            self.native_creation_guard.as_ref(),
        ).await?;
        match read_record::<A, O, Uuid>(&self.preparer.host, journal, &path,
            self.journal_limit(MAX_CAPTURE_BYTES), self.io.as_ref()).await? {
            Some(prior) => Ok(prior == attempt),
            None => Err(Error::Indeterminate(request.operation_id)),
        }
    }

    async fn commit_capture_result(
        &self,
        journal: &WorkspaceRef,
        request: &ForkRequest,
        index: usize,
        selection: &ForkSelection,
        capture: Capture,
    ) -> Result<Capture> {
        validate_capture(selection, &capture)?;
        if let Some(prior) = self
            .capture_result(journal, index, selection)
            .await?
        {
            return if prior == capture {
                Ok(prior)
            } else {
                Err(bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Conflict, "fork capture has another committed result"))
            };
        }
        let path = crate::contract::preparation_format(format_args!("/capture-{index}-result.json"), self.io.as_ref())?;
        let key = IdempotencyKey::new(crate::contract::preparation_format(format_args!("fork:{}:capture-result:{index}", request.operation_id), self.io.as_ref())?)?;
        write_record(&self.preparer.host, journal, &path, &capture,
            self.journal_limit(MAX_CAPTURE_BYTES), &key, self.io.as_ref(), self.task_preparation,
            #[cfg(not(target_arch = "wasm32"))]
            self.native_creation_guard.as_ref(),
        ).await?;
        match self
            .capture_result(journal, index, selection)
            .await?
        {
            Some(committed) if committed == capture => Ok(committed),
            Some(_) => Err(bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Conflict, "fork capture has another committed result")),
            None => Err(Error::Indeterminate(request.operation_id)),
        }
    }

    #[allow(
        clippy::too_many_lines,
        clippy::cognitive_complexity,
        reason = "one journaled fork preparation must bind each side effect to the same request"
    )]
    async fn prepare(&self, request: ForkRequest) -> Result<ForkReport> {
        if request.authorized_source.is_some() && self.task_preparation.is_none() {
            return Err(bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Unauthorized, "authorized workspace capture requires its original preparation lease"));
        }
        self.verify_task_lease(false).await?;
        let summary_before = self.io.as_ref().map(|io| io.owned_bytes()).transpose()?.unwrap_or(0);
        let _summary_hold;
        let summary_proof = self.preparer.prepare_bound_summary(&request, self.io.as_ref()).await?;
        _summary_hold = self.io.as_ref().map(|io| io.guard_owned(io.owned_bytes()? - summary_before)).transpose()?;
        let journal = self.claim(&request).await?;
        if let Some(report) = read_report(&self.preparer.host, &journal, &request, self.journal_limit(MAX_REPORT_BYTES), self.io.as_ref()).await? {
            self.verify_task_allocations(&request).await?;
            return Ok(report);
        }
        let (source_project, source_generation) = request
            .selections
            .iter()
            .find_map(|selection| match &selection.revision {
                ResourceRevision::Project { volume, generation } => Some((volume, generation)),
                _ => None,
            })
            .ok_or_else(|| bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Invalid, "fork has no project selection"))?;
        let request_digest = blake3::Hash::from(record_digest(&request, self.journal_limit(MAX_REQUEST_BYTES), self.io.as_ref())?);
        for (volume, role) in creation_volumes(&request) {
            self.verify_task_lease(false).await?;
            self.preparer.host
                .claim_fork_allocation(AllocationClaim {
                    operation_id: request.operation_id,
                    preparation_digest: *request_digest.as_bytes(),
                    task_origin: self.task_preparation.map(|task| self.clone_origin(task.origin())).transpose()?,
                    parent: self.clone_authority(&request.parent)?,
                    child: self.clone_authority(&request.child)?,
                    volume: crate::executor::with_history_io(self.io.as_ref(), |work| volume.copy(work))?,
                    role,
                }, self.journal_limit(MAX_REQUEST_BYTES), self.io.as_ref(), self.task_preparation,
                    #[cfg(not(target_arch = "wasm32"))]
                    self.native_creation_guard.as_ref(),
                )
                .await?;
        }
        let private = crate::executor::with_history_io(self.io.as_ref(), |work| request.preparation.child_private_volume.copy(work))?;
        let (source_private, private_generation, paths) = request
            .selections
            .iter()
            .find_map(|selection| match &selection.revision {
                ResourceRevision::PrivateVolume { volume, generation, paths } => Some((volume, generation, paths)),
                _ => None,
            })
            .ok_or_else(|| bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Invalid, "fork has no parent scratch selection"))?;
        let private_key = IdempotencyKey::new(crate::contract::preparation_format(format_args!("fork:{}:{request_digest}:scratch", request.operation_id), self.io.as_ref())?)?;
        let private_grant = self.read_grant(source_private)?;
        let project_grant = if request.authorized_source.is_some() {
            std::borrow::Cow::Borrowed(self.preparer.sealed_project_read(
                &request, source_project, source_generation, self.io.as_ref(), self.task_preparation,
            )?)
        } else { std::borrow::Cow::Owned(self.read_grant(source_project)?) };
        self.verify_task_lease(false).await?;
        let private_observation = bounded_io::fork(
            &self.preparer.host, source_private, &private_grant, private_generation, &private,
            Some(self.clone_paths(paths)?), &private_key, self.io.as_ref(), self.task_preparation,
            #[cfg(not(target_arch = "wasm32"))]
            self.native_creation_guard.as_ref(),
        ).await?;
        let controller = self.preparer.controller(self.io.as_ref())?;
        let project_key = IdempotencyKey::new(crate::contract::preparation_format(
            format_args!("fork:{}:{request_digest}:project", request.operation_id), self.io.as_ref())?)?;
        self.verify_task_lease(false).await?;
        let child_project = bounded_io::fork(
            &self.preparer.host, source_project, &project_grant, source_generation,
            &request.preparation.child_project_volume, None, &project_key, self.io.as_ref(), self.task_preparation,
            #[cfg(not(target_arch = "wasm32"))]
            self.native_creation_guard.as_ref(),
        ).await?;
        let (child_result_volume, child_result_generation) = if let Some(volume) = &request.preparation.child_result_volume {
            self.verify_task_lease(false).await?;
            let key = IdempotencyKey::new(crate::contract::preparation_format(format_args!("fork:{}:{request_digest}:result",request.operation_id), self.io.as_ref())?)?;
            let result = bounded_io::fork(
                &self.preparer.host, source_private, &private_grant, private_generation, volume,
                Some(Vec::new()), &key, self.io.as_ref(), self.task_preparation,
                #[cfg(not(target_arch = "wasm32"))]
                self.native_creation_guard.as_ref(),
            ).await?;
            (Some(crate::executor::with_history_io(self.io.as_ref(), |work| volume.copy(work))?), Some(result.generation))
        } else { (None,None) };
        let (
            mut child_private_generation,
            mut inherited_context,
            mut reference_grants,
            attachment_manifests,
        ) = if request.preparation.inherited_through_sequence == 0 {
            (
                self.io.as_ref().map_or_else(|| Ok(private_observation.generation.clone()), |io| private_observation.generation.clone_with_io(io))?,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
        } else {
            let context_key = IdempotencyKey::new(crate::contract::preparation_format(format_args!("fork:{}:{request_digest}:context", request.operation_id), self.io.as_ref())?)?;
            let continuation_keys = if request.preparation.summary.is_some() {
                let keys = super::summary_fork::summary_keys(&context_key, self.io.as_ref())?;
                let mut values = bounded_io::vector(keys.len(), self.io.as_ref())?;
                values.extend(keys);
                values
            } else {
                Vec::new()
            };
            let inherited = controller
                .materialize_inherited_conversation(
                    &self.preparer.parent,
                    &private,
                    &private_observation.generation,
                    request.child_agent,
                    &request.attached_agents,
                    request.preparation.inherited_through_sequence,
                    usize::try_from(request.preparation.maximum_inherited_messages)
                        .map_err(|_| bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Invalid, "fork message bound is too large"))?,
                    request.preparation.maximum_inherited_bytes,
                    request.preparation.maximum_inherited_references as usize,
                    self.preparer.resolver.as_ref(),
                    &context_key,
                    &continuation_keys,
                    self.preparer.history.as_ref().map(PreparedForkHistory::conversation),
                    self.io.as_ref(),
                    self.task_preparation,
                    #[cfg(not(target_arch = "wasm32"))]
                    self.native_creation_guard.as_ref(),
                )
                .await?;
            (
                inherited.generation,
                {
                    let mut files = bounded_io::vector(1, self.io.as_ref())?;
                    files.push(inherited.file);
                    files
                },
                inherited.reference_grants,
                inherited.attachment_manifests,
            )
        };
        let summary = if request.preparation.summary.is_some() {
            let bound = self.preparer.summary.as_ref().ok_or_else(|| {
                bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Unsupported, "summary fork provider is not installed")
            })?;
            let prepared = summary_proof.as_ref().ok_or_else(||
                bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Storage, "summary source proof is missing"))?;
            let key = IdempotencyKey::new(crate::contract::preparation_format(format_args!("fork:{}:{request_digest}:context", request.operation_id), self.io.as_ref())?)?;
            let prefix = inherited_context
                .first()
                .ok_or_else(|| bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Invalid, "summary fork omitted its logical prefix"))?;
            let (_, summary_references) = super::summary_fork::split_references(prepared, self.io.as_ref())?;
            let mut granted = BTreeSet::new();
            for grant in &reference_grants {
                reference_selection(&mut granted, &grant.file, grant.reader, self.io.as_ref())?;
            }
            for reader in
                std::iter::once(request.child_agent).chain(request.attached_agents.iter().copied())
            {
                for file in summary_references.values() {
                    if file.volume().owner() != &crate::conversation::VolumeOwner::Agent(reader) {
                        reference_selection(&mut granted, file, reader, self.io.as_ref())?;
                    }
                }
            }
            if granted.len() > request.preparation.maximum_inherited_references as usize {
                return Err(bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Invalid, "summary fork reader grants exceed capture allowance"));
            }
            let (capture, generation) = controller
                .materialize_summary_context(
                    &self.preparer.parent,
                    prepared,
                    bound.journal.as_ref(),
                    &private,
                    &child_private_generation,
                    prefix,
                    request.preparation.maximum_inherited_bytes,
                    request.preparation.maximum_inherited_references as usize,
                    &key,
                    self.preparer.resolver.as_ref(),
                    self.io.as_ref(),
                    self.task_preparation,
                    #[cfg(not(target_arch = "wasm32"))]
                    self.native_creation_guard.as_ref(),
                )
                .await?;
            granted.clear();
            for grant in &reference_grants {
                reference_selection(&mut granted, &grant.file, grant.reader, self.io.as_ref())?;
            }
            let maximum_grants = reference_grants.len().checked_add(capture.references.len()
                .checked_mul(request.attached_agents.len() + 1)
                .ok_or_else(|| bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Invalid, "summary readers overflow"))?)
                .ok_or_else(|| bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Invalid, "summary grants overflow"))?;
            bounded_io::reserve(&mut reference_grants, maximum_grants, self.io.as_ref())?;
            for reader in
                std::iter::once(request.child_agent).chain(request.attached_agents.iter().copied())
            {
                for file in &capture.references {
                    if file.volume().owner() != &crate::conversation::VolumeOwner::Agent(reader)
                        && reference_selection(&mut granted, file, reader, self.io.as_ref())?
                    {
                        reference_grants.push(ReferenceGrant {
                            file: crate::executor::with_history_io(self.io.as_ref(), |work| file.copy(work))?,
                            reader,
                            attachment_manifest: None,
                        });
                    }
                }
            }
            let capacity = inherited_context.len().checked_add(capture.inherited_files().count())
                .ok_or_else(|| bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Invalid, "summary context count overflows"))?;
            bounded_io::reserve(&mut inherited_context, capacity, self.io.as_ref())?;
            for file in capture.inherited_files() {
                inherited_context.push(crate::executor::with_history_io(self.io.as_ref(), |work| file.copy(work))?);
            }
            child_private_generation = generation;
            Some(capture)
        } else {
            None
        };
        let mut captures = bounded_io::vector(request.selections.len(), self.io.as_ref())?;
        let shared_count = request.selections.iter().filter(|selection|
            matches!(&selection.revision, ResourceRevision::SharedVolume(volume) if volume.provider() == &self.preparer.host.provider))
            .count().checked_mul(request.attached_agents.len() + 1)
            .ok_or_else(|| bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Invalid, "shared reader count overflows"))?;
        let mut shared_grants = bounded_io::vector(shared_count, self.io.as_ref())?;
        for (index, selection) in request.selections.iter().enumerate() {
            self.verify_task_lease(false).await?;
            let revision = match &selection.revision {
                ResourceRevision::History(_) => crate::executor::with_history_io(self.io.as_ref(), |work| selection.revision.copy(work).map(|(value, _)| value))?,
                ResourceRevision::Project { .. } => ResourceRevision::Project {
                    volume: crate::executor::with_history_io(self.io.as_ref(), |work| request.preparation.child_project_volume.copy(work))?,
                    generation: self.io.as_ref().map_or_else(|| Ok(child_project.generation.clone()), |io| child_project.generation.clone_with_io(io))?,
                },
                ResourceRevision::PrivateVolume { paths, .. } => ResourceRevision::PrivateVolume {
                    volume: crate::executor::with_history_io(self.io.as_ref(), |work| private.copy(work))?,
                    generation: self.io.as_ref().map_or_else(|| Ok(private_observation.generation.clone()), |io| private_observation.generation.clone_with_io(io))?,
                    paths: self.clone_paths(paths)?,
                },
                ResourceRevision::SharedVolume(volume) if volume.provider() == &self.preparer.host.provider => {
                    self.read_grant(volume)?;
                    let reference = bounded_io::content_workspace(&self.preparer.host.provider, volume, self.io.as_ref())?;
                    bounded_io::resolve(&self.preparer.host, &reference, self.io.as_ref()).await?;
                    for reader in std::iter::once(request.child_agent).chain(request.attached_agents.iter().copied()) {
                        bounded_io::item(self.io.as_ref(), std::mem::size_of::<VolumeOperation>() as u64)?;
                        shared_grants.push(SharedGrant {
                            volume: crate::executor::with_history_io(self.io.as_ref(), |work| volume.copy(work))?, child_agent: reader,
                            operations: BTreeSet::from([VolumeOperation::Read]),
                        });
                    }
                    crate::executor::with_history_io(self.io.as_ref(), |work| selection.revision.copy(work).map(|(value, _)| value))?
                }
                _ => {
                    let capture = if let Some(provider) = self.preparer.capture_providers.iter()
                        .find(|provider| provider.provider() == selection.revision.provider())
                    {
                        if let Some(committed) = self.capture_result(&journal, index, selection).await? {
                            committed
                        } else {
                            let captured = if self.start_capture(&journal, &request, index).await? {
                                self.verify_task_lease(false).await?;
                                provider.capture(&request, selection).await?
                            } else {
                                provider.reconcile(&request, selection).await?
                                    .ok_or(Error::Indeterminate(request.operation_id))?
                            };
                            self.commit_capture_result(&journal, &request, index, selection, captured).await?
                        }
                    } else {
                        Capture::Unsupported(bounded_io::clone_text("resource capture provider is not installed", self.io.as_ref())?)
                    };
                    captures.push(capture);
                    continue;
                }
            };
            captures.push(Capture::Captured(CapturedResource {
                source: crate::executor::with_history_io(self.io.as_ref(), |work| selection.revision.copy(work).map(|(value, _)| value))?, revision,
            }));
        }
        let inherited_through_sequence = request.preparation.inherited_through_sequence;
        let mut report = ForkReport {
            request,
            captures,
            child_private_volume: private,
            child_private_generation,
            child_result_volume,
            child_result_generation,
            inherited_context,
            summary,
            inherited_through_sequence,
            shared_grants,
            reference_grants,
            history_witness: None,
            attachment_manifests,
        };
        let mut history = self.io.as_ref().map(PreparationIo::history_io);
        report.validate(&mut history.as_mut().map(|history| history as &mut dyn crate::store::history::HistoryIo))?;
        if let Some(preparation) = &self.task_preparation {
            for grant in &report.reference_grants {
                require_reference_donor(preparation.grants(), grant, self.io.as_ref())?;
                if grant.reader == report.request.child_agent
                    && !{
                        let key = bounded_io::reference_key(&grant.file, self.io.as_ref())?;
                        preparation.request().child.grants.contains(&key)
                    }
                {
                    return Err(bounded_io::failure(self.io.as_ref(), crate::wire::ErrorCode::Unauthorized, "prepared child omitted an exact inherited-file read grant"));
                }
            }
        }
        let required_captures_ready =
            report
                .request
                .selections
                .iter()
                .zip(&report.captures)
                .all(|(selection, capture)| {
                    !selection.required || matches!(capture, Capture::Captured(_))
                });
        if required_captures_ready {
            self.verify_task_lease(false).await?;
            let mut seed = if let Some(io) = &self.io {
                let mut observer = io.history_io();
                report.copy(&mut Some(&mut observer))?.0
                    .into_seed(&mut Some(&mut observer))?
            } else {
                report.clone().into_seed(&mut None)?
            };
            if let Some(history) = &self.preparer.history {
                let mut observer = self.io.as_ref().map(PreparationIo::history_io);
                self.preparer.parent.attest_fork_history_with_work(&mut seed, history.conversation(),
                    &mut observer.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo))?;
                report.history_witness = seed.history_witness;
            }
            self.preparer.host.bind_fork_seed(&seed, self.journal_limit(MAX_REQUEST_BYTES), self.io.as_ref(), self.task_preparation,
                #[cfg(not(target_arch = "wasm32"))]
                self.native_creation_guard.as_ref(),
            ).await?;
        }
        self.verify_task_lease(false).await?;
        self.commit_report(&journal, &report).await
    }
}

/// Requires the donor to read the inherited file or its authenticated manifest.
pub(super) fn require_reference_donor(grants: &crate::Capabilities, reference: &ReferenceGrant,
    io: Option<&PreparationIo>) -> Result<()> {
    if io.is_none() { reference.validate(&mut None)?; }
    let mut history = io.map(PreparationIo::history_io);
    let mut observer = history.as_mut().map(|history| history as &mut dyn crate::store::history::HistoryIo);
    if crate::runtime::read_granted_with_work(grants, &reference.file, &mut observer)? {
        return Ok(());
    }
    if let Some(manifest) = &reference.attachment_manifest
        && crate::runtime::read_granted_with_work(grants, manifest, &mut observer)?
    {
        return Ok(());
    }
    Err(bounded_io::failure(io, crate::wire::ErrorCode::Unauthorized, "preparation task cannot delegate an inherited reference it cannot read"))
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Claims exact child-volume identities for a custom parent-authorized preparation path.
    pub async fn claim_fork_seed(
        &self,
        seed: &ForkSeed,
        parent: &Reducer,
        verifier: &AuthorityVerifier,
        scope: &Scope,
    ) -> Result<()> {
        if seed.summary.is_some() {
            return Err(Error::Unsupported(
                "summary fork allocation requires its owner-bound checkpoint preparer".into(),
            ));
        }
        seed.validate(&mut None)?;
        if parent.authority() != &seed.parent || parent.revision() != seed.parent_revision {
            return Err(Error::Conflict(
                "fork allocation is not at the selected parent revision".into(),
            ));
        }
        let source_project = seed
            .resources
            .iter()
            .find_map(|resource| match &resource.source {
                ResourceRevision::Project { volume, .. } => Some(volume),
                _ => None,
            })
            .ok_or_else(|| Error::Invalid("fork has no source project".into()))?;
        let controller =
            ParentProjectController::new(self, parent, verifier, scope, source_project, None)?;
        controller.require(capability::FORK_PUBLISH, VolumeOperation::Read)?;
        let parent_agent = scope
            .agent()
            .ok_or_else(|| Error::Unauthorized("fork allocation requires a parent agent".into()))?;
        if parent_agent == seed.child_agent {
            return Err(Error::Unauthorized(
                "fork child must have a distinct agent identity".into(),
            ));
        }
        let conversation = parent
            .conversation()
            .ok_or_else(|| Error::Invalid("fork parent has no conversation state".into()))?;
        if let Some(file) = seed.inherited_context.first() {
            let bytes = self
                .read_pinned(file, file.descriptor().byte_length())
                .await?;
            let actual: InheritedConversationPrefix = crate::contract::json_from_slice(&bytes)
                .map_err(|_| Error::Invalid("inherited conversation is malformed".into()))?;
            let selected = if seed.inherited_through_sequence < conversation.resident_after_sequence() {
                if seed.history_witness != Some(verifier.fork_history_proof(seed, &mut None)?) {
                    return Err(Error::Unauthorized("archived fork activation requires its original witness".into()));
                }
                actual.format_version == 3
                    && actual.parent == seed.parent
                    && actual.parent_revision == seed.parent_revision
                    && actual.parent_agent == parent_agent
                    && actual.through_sequence == seed.inherited_through_sequence
                    && actual.attached_agents == seed.attached_agents
            } else {
                actual == InheritedConversationPrefix::select(
                    seed.parent.clone(), seed.parent_revision, seed.inherited_through_sequence,
                    &seed.attached_agents, conversation, &mut None,
                )?
            };
            if !selected || crate::contract::canonical_json_bytes(&actual)?.as_slice() != bytes.as_ref() {
                return Err(Error::Conflict(
                    "inherited conversation differs from the selected parent prefix".into(),
                ));
            }
        }
        let digest = seed_digest(seed, MAX_REPORT_BYTES, None)?;
        if source_project.provider() != &self.provider
            || seed_creation_volumes(seed)?.any(|(volume, _)| volume.provider() != &self.provider)
        {
            return Err(Error::Invalid(
                "fork allocation belongs to another Filesystem provider".into(),
            ));
        }
        for (volume, role) in seed_creation_volumes(seed)? {
            self.claim_fork_allocation(AllocationClaim {
                operation_id: seed.operation_id,
                preparation_digest: digest,
                task_origin: None,
                parent: seed.parent.clone(),
                child: seed.child.clone(),
                volume: volume.clone(),
                role,
            }, MAX_REQUEST_BYTES, None, None,
                #[cfg(not(target_arch = "wasm32"))]
                None,
            ).await?;
        }
        self.bind_fork_seed(seed, MAX_REQUEST_BYTES, None, None,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        ).await
    }

    /// Reads the original protected claim through the same bounded codec as publication verification.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn fork_allocation<'a, P: StreamProvider>(
        &'a self,
        witness: &'a crate::durable_host::TaskAdmissionReadonlyWitness<P>,
        query: &'a Arc<crate::runtime::NativeTaskContext>,
        admitted: &'a crate::resource_preparation::ResourcePreparationRequest,
        volume: &'a VolumeRef,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Option<(AllocationClaim, crate::resource_preparation::ResourcePreparationRequest, acyclic_fs::NativeAllocation)>>> {
        let original = witness.original_preparation_request_with_native_work(query, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let (request, request_custody) = original.await?;
            io.absorb_native_custody(request_custody)?;
            let reject = |reason| Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized, reason,
            };
            if &request != admitted || request.provider != self.provider
                || request.creation_grant != capability::FORK_PUBLISH
            {
                return Err(reject("fork claim request differs from its original witnessed admission"));
            }
            request.validate(&mut Some(&mut io.history_io()))?;
            let fork = io.value_decode_preparation::<ForkRequest>(
                &request.payload, crate::wire::ErrorCode::Invalid,
            )?;
            fork.0.validate(&mut Some(&mut io.history_io()))?;
            if fork.0.authorized_source != request.read_authorization
                || request.resources.len() != creation_volumes(&fork.0).count()
                || fork.0.preparation.maximum_inherited_bytes > request.maximum_bytes
            {
                return Err(reject("fork claim source or resources differ from their immutable request"));
            }
            let role = creation_volumes(&fork.0).find_map(|(selected, role)|
                (selected == volume).then_some(role))
                .ok_or_else(|| reject("fork claim volume is outside the witnessed request"))?;
            for (resource, (selected, _)) in request.resources.iter().zip(creation_volumes(&fork.0)) {
                if selected.provider() != &self.provider {
                    return Err(reject("fork claim resource belongs to another original provider"));
                }
                let before = io.owned_bytes()?;
                let resource_hold;
                let actual = bounded_io::content_workspace(&self.provider, selected, Some(&io))?.into_resource();
                resource_hold = io.guard_owned(io.owned_bytes()? - before)?;
                if resource != &actual {
                    return Err(reject("fork claim resources differ from their original exact identities"));
                }
                drop(actual);
                drop(resource_hold);
            }
            let request_digest = crate::contract::canonical_json_digest_with_work(
                &request, request.maximum_bytes, &mut Some(&mut io.history_io()),
            )?;
            let preparation_digest = record_digest(&fork.0, MAX_REQUEST_BYTES.min(request.maximum_bytes), Some(&io))?;

            let journal_hold;
            let before = io.owned_bytes()?;
            let journal = allocation_ref(&self.provider, volume, Some(&io))?;
            journal_hold = io.guard_owned(io.owned_bytes()? - before)?;
            witness.verify_query_with_native_work(query, work).await?;
            let claim = read_allocation_claim(
                self, &journal, MAX_REQUEST_BYTES.min(request.maximum_bytes), Some(&io),
            ).await?;
            witness.verify_query_with_native_work(query, work).await?;
            let Some(claim) = claim else { return Ok(None) };
            let origin = claim.task_origin.as_ref()
                .ok_or_else(|| reject("fork claim has no genuine original preparation origin"))?;
            witness.verify_preparation_origin_with_native_work(query, &request, origin, work)?;
            if claim.operation_id != fork.0.operation_id || claim.parent != fork.0.parent
                || claim.child != fork.0.child || claim.volume != *volume || claim.role != role
                || claim.preparation_digest != preparation_digest
                || origin.request_digest != request_digest
                || origin.preparation_task != witness.task()
                || origin.intended_child != request.child.operation_id
                || request.child.parent != Some(origin.parent_task)
            {
                return Err(reject("fork claim differs from its complete original witnessed request"));
            }
            drop(journal);
            drop(journal_hold);
            drop(fork);
            let retained = io.owned_bytes()?;
            let custody = io.take_native_custody(retained)?;
            Ok(Some((claim, request, custody)))
        })
    }

    pub(crate) async fn verify_fork_allocation_io(
        &self, seed: &ForkSeed, volume: &VolumeRef, io: Option<&PreparationIo>,
    ) -> Result<()> {
        let journal_hold;
        let journal = allocation_ref(&self.provider, volume, io)?;
        journal_hold = io.map(|io| io.guard_owned(journal.logical_owned_bytes()?)).transpose()?;
        let claim = read_allocation_claim(
            self, &journal, MAX_REQUEST_BYTES, io,
        ).await?
            .ok_or_else(|| Error::Unauthorized("fork child volume was not allocated".into()))?;
        let validation = (|| {
        if claim.operation_id != seed.operation_id
            || claim.parent != seed.parent
            || claim.child != seed.child
            || claim.volume != *volume
            || seed_creation_volumes(seed)?.find(|(candidate,_)| *candidate == volume).map(|(_,role)| role) != Some(claim.role)
        {
            return Err(Error::Conflict(
                "fork child volume belongs to another preparation".into(),
            ));
        }
        Ok(())
        })();
        let retained = bounded_io::UploadRecord::logical_owned_bytes(&claim)?;
        drop(claim);
        if let Some(io) = io { io.release(retained)?; }
        validation?;
        let binding = read_record_decoded_bounded::<A,O,[u8; 32]>(
            self,&journal,"/seed.json",MAX_REQUEST_BYTES,io,
            |bytes, io| match io {
                Some(io) => io.decode_json(bytes),
                None => crate::contract::json_from_slice(bytes)
                    .map_err(|error| Error::Invalid(format!("fork seed binding is invalid: {error}"))),
            },
        )
            .await?
            .ok_or_else(|| {
                Error::Unauthorized("fork seed was not bound to its allocation".into())
            })?;
        if binding != seed_digest(seed, MAX_REPORT_BYTES, io)? {
            return Err(Error::Conflict(
                "fork seed differs from its allocated publication".into(),
            ));
        }
        drop(journal);
        drop(journal_hold);
        Ok(())
    }

    async fn bind_fork_seed(
        &self, seed: &ForkSeed, maximum_bytes: u64, io: Option<&PreparationIo>,
        preparation: Option<&crate::resource_preparation::TaskResourcePreparation>,
        #[cfg(not(target_arch = "wasm32"))]
        native_creation_guard: Option<&std::sync::Arc<crate::resource_preparation::TaskResourcePreparation>>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            native_creation_guard.ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "native seed binding requires its original issued creation guard" })?.verify_creation_with_native_work(work)?.await?;
        }
        let binding = seed_digest(seed, MAX_REPORT_BYTES, io)?;
        for (volume, role) in seed_creation_volumes(seed)? {
            let journal = allocation_ref(&self.provider, volume, io)?;
            let claim =
                read_record::<A, O, AllocationClaim>(self, &journal, "/claim.json", maximum_bytes, io)
                    .await?
                    .ok_or_else(|| bounded_io::failure(
                        io, crate::wire::ErrorCode::Unauthorized, "fork child volume was not allocated",
                    ))?;
            if claim.operation_id != seed.operation_id
                || claim.parent != seed.parent
                || claim.child != seed.child
                || claim.volume != *volume
                || claim.role != role
            {
                return Err(bounded_io::failure(
                    io, crate::wire::ErrorCode::Conflict, "fork child volume belongs to another preparation",
                ));
            }
            match read_record::<A, O, [u8; 32]>(self, &journal, "/seed.json", maximum_bytes, io).await? {
                Some(prior) if prior == binding => continue,
                Some(_) => return Err(bounded_io::failure(io, crate::wire::ErrorCode::Conflict, "fork allocation has another seed")),
                None => {}
            }
            let key = IdempotencyKey::new(crate::contract::preparation_format(
                format_args!("fork:{}:seed", seed.operation_id), io,
            )?)?;
            write_record(self, &journal, "/seed.json", &binding, maximum_bytes, &key, io, preparation,
                #[cfg(not(target_arch = "wasm32"))]
                native_creation_guard,
            ).await?;
            match read_record::<A, O, [u8; 32]>(self, &journal, "/seed.json", maximum_bytes, io).await? {
                Some(prior) if prior == binding => {}
                Some(_) => return Err(bounded_io::failure(io, crate::wire::ErrorCode::Conflict, "fork allocation has another seed")),
                None => return Err(Error::Indeterminate(seed.operation_id)),
            }
        }
        Ok(())
    }

    async fn claim_fork_allocation(
        &self, claim: AllocationClaim, maximum_bytes: u64, io: Option<&PreparationIo>,
        preparation: Option<&crate::resource_preparation::TaskResourcePreparation>,
        #[cfg(not(target_arch = "wasm32"))]
        native_creation_guard: Option<&std::sync::Arc<crate::resource_preparation::TaskResourcePreparation>>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            native_creation_guard.ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "native allocation claim requires its original issued creation guard" })?.verify_creation_with_native_work(work)?.await?;
        }
        crate::executor::with_history_io(io.filter(|io| io.original_native_work().is_some()),
            |work| claim.volume.validate(work))?;
        if claim.volume.provider() != &self.provider {
            return Err(bounded_io::failure(
                io, crate::wire::ErrorCode::Invalid, "fork allocation belongs to another Filesystem provider",
            ));
        }
        let journal = allocation_ref(&self.provider, &claim.volume, io)?;
        let name = std::str::from_utf8(journal.as_resource().key())
            .map_err(|_| bounded_io::failure(io, crate::wire::ErrorCode::Invalid, "allocation journal name is not UTF-8"))?;
        let creation_key = IdempotencyKey::new(crate::contract::preparation_format(
            format_args!("fork:{}:allocation-journal", claim.operation_id), io,
        )?)?;
        bounded_io::create(self, name, &creation_key, io, preparation,
            #[cfg(not(target_arch = "wasm32"))]
            native_creation_guard,
        ).await?;
        match read_record::<A, O, AllocationClaim>(self, &journal, "/claim.json", maximum_bytes, io).await? {
            Some(prior) if same_allocation_claim(&prior, &claim) => return Ok(()),
            Some(_) => {
                return Err(bounded_io::failure(
                    io, crate::wire::ErrorCode::Conflict, "fork child volume is already allocated",
                ));
            }
            None => {}
        }
        let key = IdempotencyKey::new(crate::contract::preparation_format(
            format_args!("fork:{}:allocation", claim.operation_id), io,
        )?)?;
        write_record(self, &journal, "/claim.json", &claim, maximum_bytes, &key, io, preparation,
            #[cfg(not(target_arch = "wasm32"))]
            native_creation_guard,
        ).await?;
        match read_record::<A, O, AllocationClaim>(self, &journal, "/claim.json", maximum_bytes, io).await? {
            Some(prior) if same_allocation_claim(&prior, &claim) => Ok(()),
            Some(_) => Err(bounded_io::failure(io, crate::wire::ErrorCode::Conflict, "fork child volume is already allocated")),
            None => Err(Error::Indeterminate(claim.operation_id)),
        }
    }
}

fn same_allocation_claim(retained: &AllocationClaim, requested: &AllocationClaim) -> bool {
    if retained.operation_id != requested.operation_id
        || retained.preparation_digest != requested.preparation_digest
        || retained.parent != requested.parent || retained.child != requested.child
        || retained.volume != requested.volume || retained.role != requested.role {
        return false;
    }
    same_task_origin(retained.task_origin.as_ref(), requested.task_origin.as_ref())
}

fn same_task_origin(retained: Option<&ResourceCreationOrigin>, requested: Option<&ResourceCreationOrigin>) -> bool {
    match (retained, requested) {
        (None, None) => true,
        (Some(prior), Some(current)) =>
            prior.owner == current.owner && prior.parent_task == current.parent_task
                && prior.preparation_task == current.preparation_task
                && prior.intended_child == current.intended_child
                && prior.admission_digest == current.admission_digest
                && prior.request_digest == current.request_digest
                && prior.source_grants == current.source_grants,
        _ => false,
    }
}

fn reference_selection(
    selected: &mut BTreeSet<(String, crate::AgentId)>, file: &crate::conversation::FileRef,
    reader: crate::AgentId, io: Option<&PreparationIo>,
) -> Result<bool> {
    let key = bounded_io::reference_key(file, io)?;
    let bytes = key.capacity() as u64;
    let selection = (key, reader);
    if selected.contains(&selection) {
        drop(selection);
        if let Some(io) = io { io.release(bytes)?; }
        return Ok(false);
    }
    if let Some(io) = io { io.retain_owned(std::mem::size_of::<(String, crate::AgentId)>() as u64)?; }
    Ok(selected.insert(selection))
}

fn preparation_journal(provider: &ProviderRef, operation: OperationId, io: Option<&PreparationIo>) -> Result<WorkspaceRef> {
    let name = crate::contract::preparation_format(format_args!("harness-fork-preparation-{operation}"), io)?;
    let provider = match io { Some(io) => provider.clone_with_io(io)?, None => provider.clone() };
    WorkspaceRef::new(provider, name.into_bytes(), None)
}

fn allocation_ref(provider: &ProviderRef, volume: &VolumeRef, io: Option<&PreparationIo>) -> Result<WorkspaceRef> {
    let volume_name = bounded_io::volume_storage_name(volume, io)?;
    let hold = io.map(|io| io.guard_owned(volume_name.capacity() as u64)).transpose()?;
    let name = crate::contract::preparation_format(format_args!("fork-allocation-{volume_name}"), io)?;
    drop(volume_name);
    drop(hold);
    let provider = match io {
        Some(io) => provider.clone_with_io(io)?,
        None => provider.clone(),
    };
    WorkspaceRef::new(provider, name.into_bytes(), None)
}

fn seed_digest(seed: &ForkSeed, maximum_bytes: u64, io: Option<&PreparationIo>) -> Result<[u8; 32]> {
    let mut history = io.map(PreparationIo::history_io);
    seed.validate(&mut history.as_mut().map(|history| history as &mut dyn crate::store::history::HistoryIo))?;
    record_digest(seed, maximum_bytes, io)
}

fn record_digest<T: Serialize>(value: &T, maximum_bytes: u64, io: Option<&PreparationIo>) -> Result<[u8; 32]> {
    let encoded_hold;
    let encoded = encode(value, maximum_bytes, io)?;
    encoded_hold = io.map(|io| io.guard_bytes(&encoded)).transpose()?.flatten();
    let digest = *bounded_io::digest(&encoded, io)?.as_bytes();
    drop(encoded);
    drop(encoded_hold);
    Ok(digest)
}

async fn write_record<A: AsyncAuthorityStore, O: AsyncObjectStore, T: Serialize>(
    host: &FilesystemHost<A, O>, journal: &WorkspaceRef, path: &str, value: &T,
    maximum_bytes: u64, key: &IdempotencyKey, io: Option<&PreparationIo>,
    preparation: Option<&TaskResourcePreparation>,
    #[cfg(not(target_arch = "wasm32"))]
    guard: Option<&Arc<crate::resource_preparation::TaskResourcePreparation>>,
) -> Result<()> {
    let bytes = encode(value, maximum_bytes, io)?;
    let hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
    let observed = bounded_io::resolve(host, journal, io).await?;
    let result = bounded_io::apply_generation_task_guarded(host, journal, Some(&observed.generation),
        &[WorkspaceMutation::PutFile { path: bounded_io::clone_text(path, io)?, bytes }],
        key, io, preparation,
        #[cfg(not(target_arch = "wasm32"))]
        guard,
    ).await;
    drop(hold);
    match result {
        Ok(_) => Ok(()),
        Err(error) if error.code() == crate::wire::ErrorCode::Conflict => Ok(()),
        Err(error) => Err(error),
    }
}

async fn read_claim<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, journal: &WorkspaceRef, request: &ForkRequest, maximum_bytes: u64, io: Option<&PreparationIo>,
) -> Result<bool> {
    match read_record::<A, O, ForkRequest>(host, journal, "/request.json", maximum_bytes, io).await? {
        Some(claim) if claim == *request => Ok(true),
        Some(_) => Err(bounded_io::failure(io, crate::wire::ErrorCode::Conflict, "fork operation is claimed by another request")),
        None => Ok(false),
    }
}

async fn read_report<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, journal: &WorkspaceRef, request: &ForkRequest, maximum_bytes: u64, io: Option<&PreparationIo>,
) -> Result<Option<ForkReport>> {
    let report = read_record::<A, O, ForkReport>(host, journal, "/report.json", maximum_bytes, io).await?;
    if let Some(report) = &report {
        crate::executor::with_history_io(io, |work| report.validate(work))?;
        if report.request != *request {
            return Err(bounded_io::failure(io, crate::wire::ErrorCode::Conflict, "fork report does not match its claimed request"));
        }
    }
    Ok(report)
}

fn validate_capture(selection: &ForkSelection, capture: &Capture) -> Result<()> {
    match capture {
        Capture::Captured(resource) if resource.source == selection.revision => resource.validate(&mut None),
        Capture::Captured(_) => Err(Error::Invalid(
            "capture provider substituted another source revision".into(),
        )),
        Capture::Unsupported(reason) if !reason.is_empty() => Ok(()),
        Capture::Unsupported(_) => Err(Error::Invalid("fork capture reason is invalid".into())),
        Capture::InFlight(operation) | Capture::Indeterminate(operation) => {
            Err(Error::Indeterminate(*operation))
        }
    }
}

impl<A, O> ForkPreparer for FilesystemForkPreparer<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn parent_snapshot(&self) -> (&Authority, u64) {
        (self.parent.authority(), self.parent.revision())
    }

    fn prepare<'a>(&'a self, request: ForkRequest) -> BoxFuture<'a, Result<ForkReport>> {
        Box::pin(async move {
            let attempt = self.attempt(self.task_preparation.as_deref())?;
            self.check_request(&request, attempt.io.as_ref(), attempt.task_preparation)?;
            attempt.prepare(request).await
        })
    }

    fn reconcile<'a>(&'a self, request: ForkRequest) -> BoxFuture<'a, Result<Option<ForkReport>>> {
        Box::pin(async move {
            let this = self.attempt(self.task_preparation.as_deref())?;
            self.check_request(&request, this.io.as_ref(), this.task_preparation)?;
            if request.authorized_source.is_some() && this.task_preparation.is_none() {
                return Err(Error::Unauthorized("authorized workspace reconciliation requires its original preparation lease".into()));
            }
            this.verify_task_lease(true).await?;
            let summary_before = this.io.as_ref().map(|io| io.owned_bytes()).transpose()?.unwrap_or(0);
            let _summary_hold;
            let _summary_proof = this.preparer.prepare_bound_summary(&request, this.io.as_ref()).await?;
            _summary_hold = this.io.as_ref().map(|io| io.guard_owned(io.owned_bytes()? - summary_before)).transpose()?;
            this.reconcile(&request).await
        })
    }
}

fn encode<T: Serialize>(value: &T, maximum_bytes: u64, io: Option<&PreparationIo>) -> Result<acyclic_stream::OwnedBytes> {
    if let Some(io) = io { return io.encode(value, maximum_bytes, false); }
    struct Record { bytes: Vec<u8>, maximum: u64 }
    impl std::io::Write for Record {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let next = (self.bytes.len() as u64).checked_add(bytes.len() as u64)
                .ok_or_else(|| std::io::Error::other("fork journal byte count overflows"))?;
            if next > self.maximum {
                return Err(std::io::Error::other("fork journal record exceeds byte limit"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    let mut record = Record { bytes: Vec::new(), maximum: maximum_bytes };
    serde_json::to_writer(&mut record, value).map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(record.bytes.into())
}

async fn read_allocation_claim<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, journal: &WorkspaceRef, maximum_bytes: u64,
    io: Option<&PreparationIo>,
) -> Result<Option<AllocationClaim>> {
    read_record_decoded_bounded(host, journal, "/claim.json", maximum_bytes, io,
        |bytes, io| match io {
            Some(io) => io.decode_json(bytes),
            None => crate::contract::json_from_slice(bytes)
                .map_err(|error| Error::Invalid(format!("fork allocation is invalid: {error}"))),
        },
    ).await
}

async fn read_record<A: AsyncAuthorityStore, O: AsyncObjectStore, T: bounded_io::PreparationRecord>(
    host: &FilesystemHost<A, O>, journal: &WorkspaceRef, path: &str, maximum_bytes: u64,
    io: Option<&PreparationIo>,
) -> Result<Option<T>> {
    read_record_decoded_bounded(host,journal,path,maximum_bytes,io, |bytes,io| {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            return Ok(crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<T>, bytes, crate::wire::ErrorCode::Storage)?));
        }
        crate::contract::json_from_slice(bytes)
            .map_err(|error| Error::Invalid(format!("fork journal record is invalid: {error}")))
    }).await
}

async fn read_record_decoded_bounded<A: AsyncAuthorityStore, O: AsyncObjectStore,T>(
    host: &FilesystemHost<A,O>, journal: &WorkspaceRef, path: &str, maximum_bytes:u64,
    io: Option<&PreparationIo>,
    decode: impl FnOnce(&acyclic_stream::OwnedBytes, Option<&PreparationIo>)->Result<T>,
) -> Result<Option<T>> {
    let raw;
    match bounded_io::read(host, journal, path, maximum_bytes, io).await {
        Ok(bytes) => {
            raw = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
            let value = decode(&bytes,io);
            drop(bytes);
            drop(raw);
            value.map(Some)
        }
        Err(error) if journal_absent(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

fn journal_absent(error: &Error) -> bool {
    match error {
        Error::NotFound(_) => true,
        #[cfg(not(target_arch = "wasm32"))]
        Error::NativeWorkspace { source: acyclic_fs::WorkspaceError::NotFound, .. } => true,
        _ => false,
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod task_origin_tests {
    use super::*;
    use crate::conversation::{VolumeClass, VolumeOwner};
    use crate::core::AggregateKind;
    use crate::{AgentId, TaskId};

    #[tokio::test]
    async fn allocation_retains_creation_fence_and_rejects_foreign_task_origins() -> Result<()> {
        let provider = ProviderRef::new("task-origin-test".into(), "filesystem".into(), "2".into(), &mut None)?;
        let host = FilesystemHost::new(acyclic_fs::Fs::memory(), provider.clone())?;
        let parent = Authority {
            kind: AggregateKind::Task,
            id: "parent".into(),
        };
        let child = Authority {
            kind: AggregateKind::Task,
            id: "child".into(),
        };
        let volume = VolumeRef::new(
            provider,
            "child".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([71; 16])),
         &mut None)?;
        let origin = ResourceCreationOrigin {
            owner: parent.clone(),
            parent_task: TaskId::from_bytes([72; 16]),
            preparation_task: TaskId::from_bytes([73; 16]),
            intended_child: OperationId::from_bytes([74; 16]),
            admission_digest: [75; 32],
            source_grants: crate::Capabilities::new(["file:read:original-source"]),
            request_digest: [76; 32],
            creation_fence: crate::scheduler::LeaseFence {
                reservation_id: "original".into(),
                placement: "first-worker".into(),
            },
        };
        let claim = AllocationClaim {
            operation_id: OperationId::from_bytes([77; 16]),
            preparation_digest: [78; 32],
            task_origin: Some(origin),
            parent,
            child,
            volume: volume.clone(),
            role: ForkAllocationRole::Scratch,
        };
        host.claim_fork_allocation(claim.clone(), MAX_REQUEST_BYTES, None, None, None).await?;
        let mut recovery = claim.clone();
        if let Some(origin) = &mut recovery.task_origin {
            origin.creation_fence = crate::scheduler::LeaseFence {
                reservation_id: "recovered".into(),
                placement: "later-worker".into(),
            };
        }
        host.claim_fork_allocation(recovery, MAX_REQUEST_BYTES, None, None, None).await?;
        let journal = allocation_ref(&host.provider, &volume, None)?;
        let retained = read_record::<_, _, AllocationClaim>(&host, &journal, "/claim.json", 65_536, None)
            .await?
            .ok_or_else(|| Error::NotFound("retained allocation".into()))?;
        assert_eq!(retained, claim);
        for role in [ForkAllocationRole::Project,ForkAllocationRole::Result] {
            let mut changed = claim.clone();
            changed.role = role;
            assert!(matches!(host.claim_fork_allocation(changed, MAX_REQUEST_BYTES, None, None, None).await,
                Err(Error::Conflict(_))));
        }
        for field in 0..7 {
            let mut changed = claim.clone();
            let origin = changed
                .task_origin
                .as_mut()
                .ok_or_else(|| Error::NotFound("test task origin".into()))?;
            match field {
                0 => origin.owner.id = "foreign".into(),
                1 => origin.parent_task = TaskId::from_bytes([79; 16]),
                2 => origin.preparation_task = TaskId::from_bytes([80; 16]),
                3 => origin.intended_child = OperationId::from_bytes([81; 16]),
                4 => origin.admission_digest = [82; 32],
                5 => origin.request_digest = [83; 32],
                _ => origin.source_grants = crate::Capabilities::new(["file:read:foreign-source"]),
            }
            assert!(matches!(
                host.claim_fork_allocation(changed, MAX_REQUEST_BYTES, None, None, None).await,
                Err(Error::Conflict(_))
            ));
        }
        let mut raw = claim.clone();
        raw.task_origin = None;
        assert!(matches!(
            host.claim_fork_allocation(raw, MAX_REQUEST_BYTES, None, None, None).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(
            read_record::<_, _, AllocationClaim>(&host, &journal, "/claim.json", 65_536, None)
                .await?
                .ok_or_else(|| Error::NotFound("retained allocation".into()))?,
            claim
        );
        Ok(())
    }
}
