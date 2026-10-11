//! Provider-owned fork preparation journal and exact Filesystem captures.
//!
//! The journal is a separate workspace: putting it in the child private or
//! project volume would leak implementation metadata into the published fork.
//!
//! Task-bound allocations retain the original creation fence in those same
//! journals. A configured `FilesystemResourceCreationProvider` authenticates
//! the complete request, seed and original generations before deriving exact
//! created-volume IO; each resulting handle reauthenticates on every use.
//! Ordinary preparation attempts aggregate measured Filesystem counter receipts
//! under their admitted finite work/byte envelope, including failed boundaries.
//! Native task preparation borrows the immutable admitted request and invokes
//! this same journal/capture engine on its original owner-bound ledger. Its
//! `(ForkReport, NativeAllocation)` result retains the actual funded copies
//! beyond the provider/request lifetime; drop report data before that guard.
//! Read-only recovery observes the actual protected allocation claim for every
//! fork role using a genuine immutable admission witness and issued READ query.
//! Its opaque receipt moves the funded original request and decoded claim with
//! original custody last; absence never falls back to process intents or a
//! current Started fence, and observation cannot create or publish resources.

use super::{FilesystemHost, ParentProjectController, WorkspaceMutation, map_error, workspace_ref};
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
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, WorkCounters};
use acyclic_stream::{BoxProviderFuture as BoxFuture, StreamProvider};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, sync::Arc};
use uuid::Uuid;

mod created_access;
pub(crate) mod bounded_io;
pub(crate) use bounded_io::PreparationIo;
pub use created_access::FilesystemResourceCreationProvider;

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
struct AllocationClaim {
    operation_id: OperationId,
    preparation_digest: [u8; 32],
    task_origin: Option<ResourceCreationOrigin>,
    parent: Authority,
    child: Authority,
    volume: VolumeRef,
    role: ForkAllocationRole,
}

/// A read-only observation of the actual protected fork allocation journal.
/// Only the authenticated witness observer constructs this receipt. Original
/// fences remain crate-private data, never a public authority constructor.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct OriginalForkAllocationClaim {
    claim: AllocationClaim,
    request: crate::resource_preparation::ResourcePreparationRequest,
    // All decoded origin/request-resource fields drop before their original guard.
    custody: acyclic_fs::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl OriginalForkAllocationClaim {
    pub(crate) fn origin(&self) -> &ResourceCreationOrigin {
        self.claim.task_origin.as_ref().expect("authenticated fork claim retains its original task")
    }

    pub(crate) const fn operation_id(&self) -> OperationId { self.claim.operation_id }

    pub(crate) fn volume(&self) -> &VolumeRef { &self.claim.volume }

    pub(crate) fn request(&self) -> &crate::resource_preparation::ResourcePreparationRequest {
        &self.request
    }

    pub(crate) const fn preparation_digest(&self) -> &[u8; 32] {
        &self.claim.preparation_digest
    }

    pub(crate) fn matches_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> bool {
        self.custody.matches_work(work)
    }

    pub(crate) fn into_request(
        self,
    ) -> Result<(crate::resource_preparation::ResourcePreparationRequest, acyclic_fs::NativeAllocation)> {
        let released = bounded_io::UploadRecord::logical_owned_bytes(&self.claim)?;
        let Self { claim, request, mut custody } = self;
        // The record decoder retained these known deep fields, not a second
        // receipt allocation. Preserve the request's actual original custody.
        drop(claim);
        if let Err(cause) = custody.release(released) {
            drop(request);
            return Err(Error::NativeWork { cause, custody: Some(custody) });
        }
        Ok((request, custody))
    }

    pub(crate) fn into_native_error(
        self, cause: acyclic_fs::NativeTreeMaterializationError,
    ) -> Error {
        let Self { claim, request, custody } = self;
        drop(claim);
        drop(request);
        Error::NativeWork { cause, custody: Some(custody) }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedBinding {
    digest: [u8; 32],
}

impl bounded_io::UploadRecord for SeedBinding {
    fn logical_owned_bytes(&self) -> Result<u64> {
        // The complete decoded record is an inline fixed-size digest.
        Ok(0)
    }
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaptureAttempt {
    operation_id: OperationId,
    selection: ForkSelection,
    attempt: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaptureResult {
    selection: ForkSelection,
    capture: Capture,
}

macro_rules! preparation_journal_records {
    ($($record:ty),+ $(,)?) => {$(
        impl bounded_io::PreparationRecord for $record {
            fn maximum_slot_bytes() -> u64 { bounded_io::preparation_slot_bytes::<Self>() }
            fn diagnostic_label_bytes() -> u64 {
                crate::contract::preparation_serde_diagnostic_label_bytes()
            }
        }
    )+};
}
preparation_journal_records!(AllocationClaim, SeedBinding, CaptureAttempt, CaptureResult);

#[derive(Serialize)]
struct CaptureResultRef<'a> {
    selection: &'a ForkSelection,
    capture: &'a Capture,
}
/// Idempotent local capture of an exact Stream-backed parent snapshot and its
/// Filesystem project/private resources. Other providers remain explicit
/// unsupported captures; no cross-provider transaction is implied.
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
    native_work: Option<acyclic_fs::NativeMaterializationWork>,
    // Every moved parent, grant and prepared-history buffer precedes custody.
    #[cfg(not(target_arch = "wasm32"))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

struct BoundSummary {
    selection: SummaryForkSelection,
    journal: Arc<dyn crate::executor::ExecutionJournal>,
    source: Box<dyn SummaryHistorySource>,
    #[cfg(not(target_arch = "wasm32"))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

trait SummaryHistorySource: acyclic_stream::ProviderPlatform {
    fn prepare<'a>(
        &'a self, parent: &'a Reducer, selection: SummaryForkSelection,
        journal: &'a dyn crate::executor::ExecutionJournal,
        resolver: &'a dyn ContentResidencyVerifier,
        io: Option<&'a super::PreparationIo>,
    ) -> BoxFuture<'a, Result<PreparedSummaryFork>>;

    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_with_native_work<'a>(
        &'a self, parent: &'a Reducer, selection: &'a SummaryForkSelection,
        journal: &'a dyn crate::executor::ExecutionJournal,
        resolver: &'a dyn ContentResidencyVerifier, io: &'a PreparationIo,
    ) -> Result<crate::NativeFutureBundle<'a, PreparedSummaryFork>>;
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
    ) -> BoxFuture<'a, Result<PreparedSummaryFork>> {
        Box::pin(async move {
            crate::fork::prepare_summary_fork_context_with_io(parent, selection, journal,
                &self.scope, &self.history, &self.cursor, resolver, io).await
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_with_native_work<'a>(
        &'a self, parent: &'a Reducer, selection: &'a SummaryForkSelection,
        journal: &'a dyn crate::executor::ExecutionJournal,
        resolver: &'a dyn ContentResidencyVerifier, io: &'a PreparationIo,
    ) -> Result<crate::NativeFutureBundle<'a, PreparedSummaryFork>> {
        let work = io.original_native_work().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid, reason: "summary source lacks its original native ledger",
        })?;
        crate::NativeFutureBundle::new(work, || async move {
            let selection = super::summary_fork::clone_selection(selection, Some(io))?;
            crate::fork::prepare_summary_fork_context_with_io(parent, selection, journal,
                &self.scope, &self.history, &self.cursor, resolver, Some(io)).await
        })
    }
}

struct PreparationAttempt<'a, A, O> {
    preparer: &'a FilesystemForkPreparer<A, O>,
    io: Option<PreparationIo>,
    task_preparation: Option<&'a TaskResourcePreparation>,
    #[cfg(not(target_arch = "wasm32"))]
    native_creation_guard: Option<Arc<crate::resource_preparation::NativeResourcePreparationCreationGuard>>,
}

impl<A, O> std::ops::Deref for PreparationAttempt<'_, A, O> {
    type Target = FilesystemForkPreparer<A, O>;
    fn deref(&self) -> &Self::Target { self.preparer }
}

impl<A, O> FilesystemForkPreparer<A, O> {
    /// Binds a persisted parent revision, authenticated parent authority, and
    /// the providers that own the selected history and project resources.
    #[allow(
        clippy::too_many_arguments,
        reason = "every provider and authority is bound explicitly"
    )]
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        parent: Reducer,
        verifier: AuthorityVerifier,
        scope: Scope,
        project: VolumeRef,
        stream_provider: ProviderRef,
        resolver: Arc<dyn ContentResidencyVerifier>,
    ) -> Result<Self> {
        stream_provider.validate()?;
        if stream_provider.family() != "stream" {
            return Err(Error::Invalid(
                "fork preparer requires a Stream history provider".into(),
            ));
        }
        ParentProjectController::new(&host, &parent, &verifier, &scope, project.clone())?;
        Ok(Self {
            host,
            parent,
            verifier,
            scope,
            project,
            stream_provider,
            resolver,
            capture_providers: Vec::new(),
            summary: None,
            history: None,
            task_preparation: None,
            project_source: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_work: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: None,
        })
    }

    /// Moves an already admitted parent and its authentic prepared history
    /// onto the same original native ledger; this creates no new grant.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments, reason = "original parent and custody are explicit")]
    pub fn new_with_native_work(
        host: Arc<FilesystemHost<A, O>>,
        parent: Reducer,
        verifier: AuthorityVerifier,
        scope: Scope,
        project: VolumeRef,
        stream_provider: ProviderRef,
        resolver: Arc<dyn ContentResidencyVerifier>,
        history: PreparedForkHistory,
        source_read: Option<ProjectSourceReadHandle>,
        custody: acyclic_fs::NativeAllocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        // Own data before the last-field guard even when validation rejects it.
        let this = Self {
            host, parent, verifier, scope, project, stream_provider, resolver,
            capture_providers: Vec::new(), summary: None, history: Some(history),
            task_preparation: None, project_source: source_read,
            native_work: Some(work.clone()), native_custody: Some(custody),
        };
        let reject = |code, reason| Error::NativeBoundary { code, reason };
        if !this.native_custody.as_ref().is_some_and(|guard| guard.matches_work(work)) {
            return Err(reject(crate::wire::ErrorCode::Invalid, "fork custody belongs to another original resource"));
        }
        this.stream_provider.validate_with_native_work(work)?;
        if this.stream_provider.family() != "stream" {
            return Err(reject(crate::wire::ErrorCode::Invalid, "fork history requires a Stream provider"));
        }
        if this.parent.authority().kind != crate::core::AggregateKind::Conversation {
            return Err(reject(crate::wire::ErrorCode::Invalid, "fork parent is not a conversation"));
        }
        this.verifier.verify_audience_with_native_work(this.parent.authority(), work)?;
        this.verifier.verify_with_native_work(&this.scope, work)?;
        if this.scope.agent().is_none()
            || this.parent.conversation().and_then(|state| state.agent) != this.scope.agent()
        {
            return Err(reject(crate::wire::ErrorCode::Unauthorized, "fork scope is not bound to the parent agent"));
        }
        this.project.validate_with_native_work(work)?;
        if this.project.provider() != &this.host.provider {
            return Err(reject(crate::wire::ErrorCode::Invalid, "fork project belongs to another provider"));
        }
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let history = this.history.as_ref().expect("native constructor owns prepared history");
        history.verify_with_work(&this.verifier, &mut Some(&mut io))?;
        if history.parent() != this.parent.authority()
            || history.parent_revision() != this.parent.revision()
        {
            return Err(reject(crate::wire::ErrorCode::Conflict, "fork history differs from the original parent"));
        }
        if this.project_source.as_ref().is_some_and(|handle|
            handle.authorization().project.provider() != &this.host.provider)
        {
            return Err(reject(crate::wire::ErrorCode::Unauthorized, "fork source belongs to another provider"));
        }
        Ok(this)
    }

    /// Admits the known logical Self header before its single SDK Arc attempt.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(mut self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        if !self.native_custody.as_ref().is_some_and(|guard| guard.matches_work(work)) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "fork preparer belongs to another original resource" });
        }
        let mut header = work.claim(std::mem::size_of::<Self>() as u64)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        self.native_custody.as_mut().expect("native custody was checked").absorb(&mut header)
            .map_err(|cause| Error::NativeWork { cause, custody: Some(header) })?;
        Ok(Arc::new(self))
    }

    /// Retains an operation-specific project read sealed by the original task
    /// owner. This does not change caller grants or authorize private/history IO.
    pub fn with_project_source_read_handle(mut self, handle: ProjectSourceReadHandle) -> Result<Self> {
        if handle.authorization().project.provider() != &self.host.provider {
            return Err(Error::Unauthorized("project source belongs to another Filesystem provider".into()));
        }
        self.project_source = Some(handle);
        Ok(self)
    }

    /// Binds a finite archive selection produced by the original history reader.
    /// This does not grant file access or publish a child.
    pub fn with_history_context(mut self, history: PreparedForkHistory) -> Result<Self> {
        history.verify(&self.verifier)?;
        if history.parent() != self.parent.authority()
            || history.parent_revision() != self.parent.revision()
        {
            return Err(Error::Conflict("fork history differs from the bound parent".into()));
        }
        self.history = Some(history);
        Ok(self)
    }

    /// Binds the original owner journal, receiving scope and immutable history
    /// cut. Preparation revalidates the complete Summary proof under its own
    /// ledger; no previously prepared projection substitutes for that work.
    pub fn with_summary_history<P: StreamProvider + 'static>(
        mut self,
        selection: SummaryForkSelection,
        journal: Arc<dyn crate::executor::ExecutionJournal>,
        scope: RuntimeScope,
        history: HistoryReader<P>,
        cursor: HistoryCursor,
    ) -> Result<Self> {
        selection.validate()?;
        if cursor.authority != *self.parent.authority()
            || cursor.through_revision != self.parent.revision()
            || journal.canonical_authority() != Some(self.parent.authority()) {
            return Err(Error::Conflict(
                "summary history differs from this original parent".into(),
            ));
        }
        self.summary = Some(BoundSummary {
            selection, journal, source: Box::new(OriginalSummaryHistory { scope, history, cursor }),
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: None,
        });
        Ok(self)
    }

    /// Moves the original summary source and its paid payload custody together.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments, reason = "original summary source and its custody are explicit")]
    pub fn with_summary_history_with_native_work<P: StreamProvider + 'static>(
        mut self,
        selection: SummaryForkSelection,
        journal: Arc<dyn crate::executor::ExecutionJournal>,
        scope: RuntimeScope,
        history: HistoryReader<P>,
        cursor: HistoryCursor,
        custody: acyclic_fs::NativeAllocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        // All moved payloads drop before the original guard on rejection.
        let mut registration = (
            selection, journal, OriginalSummaryHistory { scope, history, cursor }, custody,
        );
        self.require_native_projection_work(work)?;
        if !registration.3.matches_work(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "summary source custody belongs to another original resource" });
        }
        let io = PreparationIo::from_native_work(work)?;
        let mut observer = io.history_io();
        registration.0.validate_with_work(&mut Some(&mut observer))?;
        drop(observer);
        if registration.2.cursor.authority != *self.parent.authority()
            || registration.2.cursor.through_revision != self.parent.revision()
            || registration.1.canonical_authority() != Some(self.parent.authority())
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "summary history differs from this original parent" });
        }
        let mut header = work.claim(std::mem::size_of::<OriginalSummaryHistory<P>>() as u64)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        registration.3.absorb(&mut header)
            .map_err(|cause| Error::NativeWork { cause, custody: Some(header) })?;
        let (selection, journal, source, custody) = registration;
        self.summary = Some(BoundSummary {
            selection, journal, source: Box::new(source), native_custody: Some(custody),
        });
        Ok(self)
    }

    /// Pins an authenticated ordinary preparation task to this provider.
    /// The existing allocation records retain its origin; raw trusted
    /// preparation records cannot substitute for task-bound creation evidence.
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

    /// Registers an exact provider-owned resource capture. A provider may be
    /// bound once; unsupported selections remain explicit in the report.
    pub fn with_capture_provider(mut self, provider: Arc<dyn ForkCaptureProvider>) -> Result<Self> {
        provider.provider().validate()?;
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

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemForkPreparer<A, O> {
    /// Computes the exact foreign-reader references before immutable child
    /// admission, using the materializer's canonical authenticated history and
    /// attachment-manifest projection. This allocates no child resource and
    /// performs no capture or publication.
    pub async fn required_reference_grants(&self, request: &ForkRequest) -> Result<Vec<ReferenceGrant>> {
        self.check_request(request)?;
        let controller = ParentProjectController::new(
            self.host.as_ref(), &self.parent, &self.verifier, &self.scope, self.project.clone(),
        )?;
        let mut grants = controller.inherited_reference_grants(
            &self.parent, request.child_agent, &request.attached_agents,
            request.preparation.inherited_through_sequence,
            usize::try_from(request.preparation.maximum_inherited_messages)
                .map_err(|_| Error::Invalid("fork message bound is too large".into()))?,
            request.preparation.maximum_inherited_bytes,
            request.preparation.maximum_inherited_references as usize,
            self.resolver.as_ref(), self.history.as_ref(),
        ).await?;
        if request.preparation.summary.is_some() {
            let summary = self.prepare_bound_summary(request, None).await?
                .ok_or_else(|| Error::Invalid("summary source was not authenticated".into()))?;
            let (_, references) = super::summary_fork::split_references(&summary, None)?;
            let mut selected = grants.iter().map(|grant| Ok((grant.capability()?, grant.reader)))
                .collect::<Result<BTreeSet<_>>>()?;
            for reader in std::iter::once(request.child_agent).chain(request.attached_agents.iter().copied()) {
                for file in references.values() {
                    if file.volume().owner() != &crate::conversation::VolumeOwner::Agent(reader)
                        && selected.insert((file.read_capability()?, reader)) {
                        if selected.len() > request.preparation.maximum_inherited_references as usize {
                            return Err(Error::Invalid("summary reader grants exceed the original capture allowance".into()));
                        }
                        grants.push(ReferenceGrant { file: file.clone(), reader, attachment_manifest: None });
                    }
                }
            }
        }
        Ok(grants)
    }

    /// Authenticates foreign readers through the original history projector.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn required_reference_grants_with_native_work<'a>(
        &'a self, request: &'a ForkRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<ReferenceGrant>, acyclic_fs::NativeAllocation)>> {
        self.require_native_projection_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            self.check_request_with_native_work(request, &io)?;
            if let Some(preparation) = &self.task_preparation {
                preparation.verify_creation_with_native_work(work)?.await?;
            }
            let controller = ParentProjectController::new_with_native_work(
                self.host.as_ref(), &self.parent, &self.verifier, &self.scope, &self.project, work,
            )?;
            let maximum_messages = usize::try_from(request.preparation.maximum_inherited_messages)
                .map_err(|_| Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "fork message bound is too large" })?;
            let (mut grants, custody) = controller.inherited_reference_grants_with_native_work(
                &self.parent, request.child_agent, &request.attached_agents,
                request.preparation.inherited_through_sequence, maximum_messages,
                request.preparation.maximum_inherited_bytes,
                request.preparation.maximum_inherited_references as usize,
                self.resolver.as_ref(), self.history.as_ref(), work,
            ).await?;
            io.absorb_native_custody(custody)?;
            if request.preparation.summary.is_some() {
                let summary = self.prepare_bound_summary_with_native_work(request, &io).await?
                    .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                        reason: "summary source was not authenticated" })?;
                let (payloads, references) = super::summary_fork::split_references(&summary, Some(&io))?;
                let mut selected = BTreeSet::new();
                for grant in &grants {
                    let key = bounded_io::reference_key(&grant.file, Some(&io))?;
                    native_reference_selection(&mut selected, key, grant.reader, &io)?;
                }
                let candidates = references.len().checked_mul(request.attached_agents.len() + 1)
                    .and_then(|count| count.checked_add(grants.len()))
                    .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                        reason: "summary reader projection overflows" })?;
                native_projection_reserve(&mut grants,
                    candidates.min(request.preparation.maximum_inherited_references as usize), &io)?;
                for reader in std::iter::once(request.child_agent).chain(request.attached_agents.iter().copied()) {
                    for file in references.values() {
                        if file.volume().owner() == &crate::conversation::VolumeOwner::Agent(reader) {
                            continue;
                        }
                        let key = bounded_io::reference_key(file, Some(&io))?;
                        if native_reference_selection(&mut selected, key, reader, &io)? {
                            if selected.len() > request.preparation.maximum_inherited_references as usize {
                                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                                    reason: "summary reader grants exceed the original capture allowance" });
                            }
                            grants.push(ReferenceGrant {
                                file: bounded_io::clone_file(file, Some(&io))?,
                                reader, attachment_manifest: None,
                            });
                        }
                    }
                }
                drop(selected);
                drop(references);
                drop(payloads);
                drop(summary);
            }
            if let Some(preparation) = &self.task_preparation {
                preparation.verify_creation_with_native_work(work)?.await?;
            }
            let retained = native_projection_bytes(&grants, grants.capacity(), |grant| {
                let manifest = grant.attachment_manifest.as_ref()
                    .map(crate::conversation::FileRef::logical_owned_bytes).transpose()?.unwrap_or(0);
                grant.file.logical_owned_bytes()?.checked_add(manifest)
                    .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                        reason: "reference grant ownership overflows" })
            })?;
            let custody = io.into_native_custody(retained)?;
            Ok((grants, custody))
        })
    }

    /// Exact created identities for this provider's fork request, without IO.
    pub fn creation_resources(&self, request: &ForkRequest) -> Result<Vec<ResourceRef>> {
        request.validate()?;
        creation_volumes(request).map(|(volume,_)| {
            if volume.provider() != &self.host.provider {
                return Err(Error::Invalid(
                    "fork resource belongs to another provider".into(),
                ));
            }
            Ok(
                workspace_ref(self.host.provider.clone(), &volume.storage_name()?)?
                    .as_resource()
                    .clone(),
            )
        })
        .collect()
    }

    /// Projects the actual provider-qualified allocations on the original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn creation_resources_with_native_work(
        &self, request: &ForkRequest, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Vec<ResourceRef>, acyclic_fs::NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        self.check_request_with_native_work(request, &io)?;
        let mut resources = native_projection_vec::<ResourceRef>(creation_volumes(request).count(), work)?;
        for (volume, _) in creation_volumes(request) {
            resources.values.push(bounded_io::content_workspace(&self.host.provider, volume, Some(&io))?.into_resource());
        }
        let retained = native_projection_bytes(&resources.values, resources.values.capacity(),
            ResourceRef::logical_owned_bytes)?;
        io.absorb_native_custody(resources.custody)?;
        let custody = io.into_native_custody(retained)?;
        Ok((resources.values, custody))
    }

    /// Actual Filesystem operation that created the child project, not the
    /// enclosing Harness operation. This pure identity grants no access.
    pub fn project_creation_operation(&self, request: &ForkRequest) -> Result<acyclic_fs::OperationId> {
        request.validate()?;
        self.creation_operation(request, ForkAllocationRole::Project)
    }

    /// Actual Filesystem operation that created the optional unmounted result slot.
    pub fn result_creation_operation(&self, request: &ForkRequest) -> Result<acyclic_fs::OperationId> {
        request.validate()?;
        if request.preparation.child_result_volume.is_none() {
            return Err(Error::Invalid("fork has no result allocation".into()));
        }
        self.creation_operation(request, ForkAllocationRole::Result)
    }

    fn creation_operation(&self, request: &ForkRequest, role: ForkAllocationRole) -> Result<acyclic_fs::OperationId> {
        struct DigestWriter { hash: blake3::Hasher, length: u64, maximum: u64 }
        impl std::io::Write for DigestWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                let next = self.length.checked_add(bytes.len() as u64)
                    .ok_or_else(|| std::io::Error::other("fork request byte count overflows"))?;
                if next > self.maximum {
                    return Err(std::io::Error::other("fork request exceeds byte limit"));
                }
                self.hash.update(bytes);
                self.length = next;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
        }
        let mut digest = DigestWriter {
            hash: blake3::Hasher::new(), length: 0,
            maximum: self.journal_limit(MAX_REQUEST_BYTES),
        };
        serde_json::to_writer(&mut digest, request).map_err(|error| Error::Invalid(error.to_string()))?;
        let suffix = match role { ForkAllocationRole::Scratch => "scratch", ForkAllocationRole::Project => "project", ForkAllocationRole::Result => "result" };
        let key = IdempotencyKey::new(format!("fork:{}:{}:{suffix}",request.operation_id,digest.hash.finalize()))?;
        Ok(acyclic_fs::OperationId::from_bytes(super::filesystem_key(&key).into_bytes()))
    }

    /// Builds exact allocation-journal references. These are request data,
    /// never access grants; only a configured creation verifier consumes them.
    pub fn creation_proofs(
        &self,
        request: &ForkRequest,
        preparation: crate::TaskId,
    ) -> Result<Vec<crate::resource_preparation::ResourceCreationProofRef>> {
        let resources = self.creation_resources(request)?;
        resources
            .into_iter()
            .zip(creation_volumes(request))
            .map(|(resource, (volume, _))| {
                Ok(crate::resource_preparation::ResourceCreationProofRef {
                    provider: self.host.provider.clone(),
                    preparation,
                    resource,
                    evidence: allocation_ref(self.host.provider.clone(), volume)?
                        .as_resource()
                        .clone(),
                })
            })
            .collect()
    }

    /// Produces the original allocation-journal evidence without cloning keys.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn creation_proofs_with_native_work(
        &self, request: &ForkRequest, preparation: crate::TaskId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Vec<crate::resource_preparation::ResourceCreationProofRef>, acyclic_fs::NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        self.check_request_with_native_work(request, &io)?;
        let mut proofs = native_projection_vec::<crate::resource_preparation::ResourceCreationProofRef>(
            creation_volumes(request).count(), work)?;
        for (volume, _) in creation_volumes(request) {
            proofs.values.push(crate::resource_preparation::ResourceCreationProofRef {
                provider: self.host.provider.clone_with_io(&io)?,
                preparation,
                resource: bounded_io::content_workspace(&self.host.provider, volume, Some(&io))?.into_resource(),
                evidence: allocation_ref_io(&self.host.provider, volume, Some(&io))?.into_resource(),
            });
        }
        let retained = native_projection_bytes(&proofs.values, proofs.values.capacity(), |proof| {
            let evidence = proof.evidence.logical_owned_bytes()?;
            proof.provider.logical_owned_bytes()?.checked_add(proof.resource.logical_owned_bytes()?)
                .and_then(|bytes| bytes.checked_add(evidence))
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "creation proof ownership overflows" })
        })?;
        io.absorb_native_custody(proofs.custody)?;
        let custody = io.into_native_custody(retained)?;
        Ok((proofs.values, custody))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn require_native_projection_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?;
        if !self.native_custody.as_ref().is_some_and(|custody| custody.matches_work(work)) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "fork projection belongs to another original resource" });
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn check_request_with_native_work(&self, request: &ForkRequest, io: &PreparationIo) -> Result<()> {
        self.check_request_with_native_task(request, io, self.task_preparation.as_deref())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn check_request_with_native_task(
        &self, request: &ForkRequest, io: &PreparationIo,
        task: Option<&TaskResourcePreparation>,
    ) -> Result<()> {
        let work = io.original_native_work().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid, reason: "fork projection lacks its original native ledger",
        })?;
        self.require_native_projection_work(work)?;
        let reject = |code, reason| Error::NativeBoundary { code, reason };
        let mut observer = io.history_io();
        request.validate_with_work(&mut Some(&mut observer))?;
        drop(observer);
        self.verifier.verify_audience_with_native_work(self.parent.authority(), work)?;
        self.verifier.verify_with_native_work(&self.scope, work)?;
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
            volume.validate_with_native_work(work)?;
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
                    if let Some(authorization) = &request.authorized_source {
                        let handle = task.and_then(TaskResourcePreparation::project_source)
                            .or(self.project_source.as_ref())
                            .ok_or(reject(crate::wire::ErrorCode::Unauthorized, "fork source has no sealed project read"))?;
                        if handle.authorization() != authorization {
                            return Err(reject(crate::wire::ErrorCode::Unauthorized, "fork source differs from its sealed project read"));
                        }
                        if authorization.source_task != authorization.caller_task
                            && (request.preparation.inherited_through_sequence != 0
                                || request.preparation.summary.is_some())
                        {
                            return Err(reject(crate::wire::ErrorCode::Unsupported,
                                "foreign workspace source cannot inherit conversation history"));
                        }
                        handle.require_selection_with_native_work(request.operation_id, authorization.source_task,
                            volume, generation, authorization.native_snapshot.as_ref(), work)?;
                    } else if !io.contains_capability(self.scope.capabilities(), volume, "volume:read:")? {
                        return Err(reject(crate::wire::ErrorCode::Unauthorized, "selected project source-read is not granted"));
                    }
                }
                ResourceRevision::PrivateVolume { volume, .. } => {
                    let agent = self.scope.agent().ok_or(reject(crate::wire::ErrorCode::Unauthorized,
                        "scratch fork requires a parent agent"))?;
                    if volume.owner() != &crate::conversation::VolumeOwner::Agent(agent)
                        || !io.contains_capability(self.scope.capabilities(), volume, "volume:read:")?
                    {
                        return Err(reject(crate::wire::ErrorCode::Unauthorized, "scratch source-read is outside the direct parent"));
                    }
                }
                _ => {}
            }
        }
        if let Some(task) = task {
            let bound = task.request();
            if task.origin().intended_child != bound.child.operation_id
                || bound.child.parent != Some(task.origin().parent_task)
                || task.grants() != &task.origin().source_grants
            {
                return Err(reject(crate::wire::ErrorCode::Unauthorized,
                    "fork preparation differs from its genuine immutable owner admission"));
            }
            if let Some(original) = self.task_preparation.as_deref() {
                let prior = original.origin();
                let current = task.origin();
                if original.request() != bound || prior.owner != current.owner
                    || prior.parent_task != current.parent_task
                    || prior.preparation_task != current.preparation_task
                    || prior.intended_child != current.intended_child
                    || prior.admission_digest != current.admission_digest
                    || prior.request_digest != current.request_digest
                    || prior.source_grants != current.source_grants
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
            let encoded = io.encode_json(request, bound.maximum_bytes, true)?;
            request_hold = io.guard_bytes(&encoded)?;
            let payload_hold;
            let payload = io.encode_json(&bound.payload, bound.maximum_bytes, true)?;
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
            if let Some(authorization) = &request.authorized_source {
                if authorization.owner != task.origin().owner || authorization.caller_task != task.origin().parent_task
                    || task.project_source().is_none_or(|handle| handle.authorization() != authorization)
                {
                    return Err(reject(crate::wire::ErrorCode::Unauthorized, "fork preparation lacks its original owner's project source"));
                }
            }
            for selection in &request.selections {
                match &selection.revision {
                    ResourceRevision::Project { volume, .. } if request.authorized_source.is_none() => {
                        if !io.contains_capability(task.grants(), volume, "volume:read:")? {
                            return Err(reject(crate::wire::ErrorCode::Unauthorized, "preparation lacks its selected project source-read grant"));
                        }
                    }
                    ResourceRevision::PrivateVolume { volume, .. } => {
                        if !io.contains_capability(task.grants(), volume, "volume:read:")? {
                            return Err(reject(crate::wire::ErrorCode::Unauthorized, "preparation lacks its selected scratch source-read grant"));
                        }
                    }
                    _ => {}
                }
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
            if !summary.native_custody.as_ref().is_some_and(|custody| custody.matches_work(work)) {
                return Err(reject(crate::wire::ErrorCode::Unauthorized, "summary source has no original native custody"));
            }
            if &summary.selection != selection
                || self.parent.conversation().map_or(0, |state| state.logical_revision())
                    != request.preparation.inherited_through_sequence
            {
                return Err(reject(crate::wire::ErrorCode::Conflict, "summary fork differs from its original capture"));
            }
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_bound_summary_with_native_work(
        &self, request: &ForkRequest, io: &PreparationIo,
    ) -> Result<Option<PreparedSummaryFork>> {
        let Some(selection) = &request.preparation.summary else { return Ok(None) };
        let bound = self.summary.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "summary fork owner-bound checkpoint provider is not installed",
        })?;
        let work = io.original_native_work().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid, reason: "summary source lacks its original native ledger",
        })?;
        if !bound.native_custody.as_ref().is_some_and(|custody| custody.matches_work(work)) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "summary source has no original native custody" });
        }
        if selection != &bound.selection {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "summary fork request differs from its original capture" });
        }
        bound.source.prepare_with_native_work(&self.parent, selection, bound.journal.as_ref(),
            self.resolver.as_ref(), io)?.await.map(Some)
    }

    pub(super) async fn prepare_bound_summary(
        &self, request: &ForkRequest, io: Option<&super::PreparationIo>,
    ) -> Result<Option<PreparedSummaryFork>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            return self.prepare_bound_summary_with_native_work(request, io).await;
        }
        let Some(selection) = &request.preparation.summary else { return Ok(None) };
        let bound = self.summary.as_ref().ok_or_else(|| Error::Unsupported(
            "summary fork owner-bound checkpoint provider is not installed".into()))?;
        if selection != &bound.selection {
            return Err(Error::Conflict("summary fork request differs from its original capture".into()));
        }
        bound.source.prepare(&self.parent, super::summary_fork::clone_selection(selection, io)?,
            bound.journal.as_ref(), self.resolver.as_ref(), io).await.map(Some)
    }

    fn attempt(&self) -> Result<PreparationAttempt<'_, A, O>> {
        self.attempt_with_preparation(self.task_preparation.as_deref())
    }

    fn attempt_with_preparation<'a>(
        &'a self,
        task: Option<&'a TaskResourcePreparation>,
    ) -> Result<PreparationAttempt<'a, A, O>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.native_work {
            let io = PreparationIo::from_native_work(work)?;
            if let Some(task) = task {
                io.encode(task.request(), task.request().maximum_bytes)?;
            }
            return Ok(PreparationAttempt { preparer: self, io: Some(io), task_preparation: task,
                native_creation_guard: None });
        }
        let io = task.map(|task| {
            let cancellation = task.cancellation().ok_or_else(|| Error::Unauthorized(
                "task-bound preparation has no original invocation cancellation token".into()))?;
            PreparationIo::new_with_cancellation(task.request(), cancellation)
        }).transpose()?;
        if let (Some(io), Some(task)) = (&io, task) {
            io.encode(task.request(), task.request().maximum_bytes)?;
        }
        Ok(PreparationAttempt { preparer: self, io, task_preparation: task,
            #[cfg(not(target_arch = "wasm32"))]
            native_creation_guard: None })
    }

    /// Executes the existing exact capture under one authenticated preparation
    /// task without copying the selected parent reducer or retaining its lease
    /// in a reusable provider binding.
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
        self.attempt_with_preparation(Some(preparation))?.prepare_inner(request).await
    }

    /// Borrows the exact admitted request and genuine current preparation owner.
    /// All returned report buffers carry the original allocation ledger; the
    /// report must be dropped before its accompanying custody.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn prepare_for_task_with_native_work<'a>(
        &'a self, preparation: &'a Arc<TaskResourcePreparation>, request: &'a ForkRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ForkReport, acyclic_fs::NativeAllocation)>> {
        self.require_native_projection_work(work)?;
        let mut attempt = self.attempt_with_preparation(Some(preparation.as_ref()))?;
        self.check_request_with_native_task(request,
            attempt.io.as_ref().expect("original native preparer owns its IO"), Some(preparation.as_ref()))?;
        attempt.native_creation_guard = Some(preparation.owned_creation_guard_with_native_work(work)?);
        crate::NativeFutureBundle::new(work, || async move {
            preparation.verify_creation_with_native_work(work)?.await?;
            let io = attempt.io.as_ref().expect("original native preparer owns its IO");
            io.retain_owned(std::mem::size_of::<ForkReport>() as u64)?;
            let owned = request.clone_with_work(&mut Some(&mut io.history_io()))?;
            let report = attempt.prepare_checked_inner(owned).await?;
            let retained = io.owned_bytes()?;
            let custody = io.take_native_custody(retained)?;
            Ok((report, custody))
        })
    }

    /// Passively observes the exact retained preparation report under the
    /// genuine original settlement owner. This never creates a journal,
    /// restarts captures, or materializes inherited/summary content.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn reconcile_for_task_with_native_work<'a>(
        &'a self, preparation: &'a Arc<TaskResourcePreparation>, request: &'a ForkRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ForkReport>, acyclic_fs::NativeAllocation)>> {
        self.require_native_projection_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            preparation.verify_reconciliation_with_native_work(work)?.await?;
            let attempt = self.attempt_with_preparation(Some(preparation.as_ref()))?;
            let io = attempt.io.as_ref().expect("original native preparer owns its IO");
            self.check_request_with_native_task(request, io, Some(preparation.as_ref()))?;
            let journal = attempt.journal_ref(request)?;
            let report = match bounded_io::resolve(&self.host, &journal, Some(io)).await {
                Ok(_) => {
                    if attempt.read_claim(&journal, request).await? {
                        let report = attempt.read_report(&journal, request).await?;
                        if report.is_some() { attempt.verify_task_allocations(request).await?; }
                        report
                    } else { None }
                }
                Err(error) if journal_absent(&error) => None,
                Err(error) => return Err(error),
            };
            preparation.verify_reconciliation_with_native_work(work)?.await?;
            let retained = io.owned_bytes()?;
            let custody = io.take_native_custody(retained)?;
            Ok((report, custody))
        })
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> PreparationAttempt<'_, A, O> {

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

    fn clone_volume(&self, value: &VolumeRef) -> Result<VolumeRef> {
        self.io.as_ref().map_or_else(|| Ok(value.clone()),
            |io| value.clone_with_work(&mut Some(&mut io.history_io())))
    }

    fn clone_generation(&self, value: &crate::resources::GenerationRef) -> Result<crate::resources::GenerationRef> {
        self.io.as_ref().map_or_else(|| Ok(value.clone()), |io| value.clone_with_io(io))
    }

    fn clone_revision(&self, value: &ResourceRevision) -> Result<ResourceRevision> {
        self.io.as_ref().map_or_else(|| Ok(value.clone()),
            |io| value.clone_with_work(&mut Some(&mut io.history_io())))
    }

    fn clone_paths(&self, paths: &[String]) -> Result<Vec<String>> {
        let mut values = bounded_io::vector(paths.len(), self.io.as_ref())?;
        for path in paths { values.push(bounded_io::clone_text(path, self.io.as_ref())?); }
        Ok(values)
    }

    fn clone_origin(&self, origin: &ResourceCreationOrigin) -> Result<ResourceCreationOrigin> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = self.io.as_ref().filter(|io| io.original_native_work().is_some()) {
            let (source_grants, custody) = origin.source_grants
                .clone_with_native_work(io.original_native_work().expect("native IO"))?;
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
        self.io.as_ref().map_or_else(
            || ContentGrant::verify(&self.verifier, &self.scope, volume, VolumeOperation::Read),
            |io| ContentGrant::verify_with_io(&self.verifier, &self.scope, volume, VolumeOperation::Read, io))
    }

    fn key(&self, value: std::fmt::Arguments<'_>) -> Result<IdempotencyKey> {
        IdempotencyKey::new(bounded_io::format_text(value, self.io.as_ref())?)
    }

    fn reserve<T>(&self, values: &mut Vec<T>, capacity: usize) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = self.io.as_ref().filter(|io| io.original_native_work().is_some()) {
            return native_projection_reserve(values, capacity, io);
        }
        if capacity > values.capacity() {
            values.try_reserve_exact(capacity - values.len()).map_err(Error::Allocation)?;
        }
        Ok(())
    }

    fn reference_selection(
        &self, selected: &mut BTreeSet<(String, crate::AgentId)>,
        file: &crate::conversation::FileRef, reader: crate::AgentId,
    ) -> Result<bool> {
        let key = bounded_io::reference_key(file, self.io.as_ref())?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = self.io.as_ref().filter(|io| io.original_native_work().is_some()) {
            return native_reference_selection(selected, key, reader, io);
        }
        Ok(selected.insert((key, reader)))
    }

    fn reference_donor(&self, grants: &crate::Capabilities, reference: &ReferenceGrant) -> Result<()> {
        let Some(io) = &self.io else { return require_reference_donor(grants, reference) };
        if crate::runtime::read_granted_with_work(grants, &reference.file, &mut Some(&mut io.history_io()))?
        { return Ok(()) }
        if let Some(manifest) = &reference.attachment_manifest
            && crate::runtime::read_granted_with_work(grants, manifest, &mut Some(&mut io.history_io()))?
        { return Ok(()) }
        Err(bounded_io::unauthorized(Some(io), "preparation task cannot delegate an inherited reference it cannot read"))
    }

    fn failure(&self, code: crate::wire::ErrorCode, reason: &'static str) -> Error {
        #[cfg(not(target_arch = "wasm32"))]
        if self.io.as_ref().is_some_and(PreparationIo::uses_native_work) {
            return Error::NativeBoundary { code, reason };
        }
        match code {
            crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
            crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
            crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
            crate::wire::ErrorCode::Storage => Error::Storage(reason.into()),
            _ => Error::Invalid(reason.into()),
        }
    }

    fn digest(&self, bytes: &[u8]) -> Result<blake3::Hash> {
        let hash = WorkCounters { bytes_hashed: bytes.len() as u64, ..WorkCounters::UNCHARGED };
        if let Some(io) = &self.io { io.preflight(hash)?; }
        let digest = blake3::hash(bytes);
        if let Some(io) = &self.io { io.charge(hash)?; }
        Ok(digest)
    }


    fn encode<T: Serialize>(&self, value: &T, maximum_bytes: u64) -> Result<acyclic_stream::OwnedBytes> {
        match &self.io {
            Some(io) => io.encode(value, maximum_bytes),
            None => encode_record(value, maximum_bytes),
        }
    }

    fn validate_task_request(&self, request: &ForkRequest) -> Result<()> {
        if let Some(preparation) = &self.task_preparation {
            let bound = preparation.request();
            let payload =
                serde_json::to_value(request).map_err(|error| Error::Invalid(error.to_string()))?;
            if bound.payload != payload || bound.resources != self.creation_resources(request)? {
                return Err(Error::Unauthorized(
                    "fork request or resources differ from the admitted preparation".into(),
                ));
            }
            if bound.read_authorization != request.authorized_source {
                return Err(Error::Unauthorized("fork source authorization differs from its admitted request".into()));
            }
            match (&request.authorized_source, preparation.project_source()) {
                (Some(authorization), Some(handle))
                    if handle.authorization() == authorization
                        && authorization.owner == preparation.origin().owner
                        && authorization.caller_task == preparation.origin().parent_task => {}
                (None, None) => {}
                _ => return Err(Error::Unauthorized("fork preparation lacks its original owner's sealed project source".into())),
            }
            if request.preparation.maximum_inherited_bytes > bound.maximum_bytes {
                return Err(Error::Invalid("fork preparation exceeds admitted IO allowance".into()));
            }
            crate::contract::validate_json_byte_bound(request, bound.maximum_bytes)?;
            for selection in &request.selections {
                match &selection.revision {
                    ResourceRevision::Project { volume, generation } if request.authorized_source.is_some() => {
                        self.project_read_grant(request, volume, generation)?;
                    }
                    ResourceRevision::Project { volume, .. }
                    | ResourceRevision::PrivateVolume { volume, .. }
                        if !preparation.grants().contains(&volume.capability(VolumeOperation::Read)?) =>
                    {
                        return Err(Error::Unauthorized(
                            "preparation task lacks the exact selected workspace source-read grant".into(),
                        ));
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    fn project_read_grant<'a>(
        &'a self,
        request: &ForkRequest,
        volume: &VolumeRef,
        generation: &crate::resources::GenerationRef,
    ) -> Result<std::borrow::Cow<'a, ContentGrant>> {
        let Some(authorization) = &request.authorized_source else {
            return self.read_grant(volume).map(std::borrow::Cow::Owned);
        };
        let handle = match self.task_preparation {
            Some(preparation) => preparation.project_source(),
            None => self.project_source.as_ref(),
        }.ok_or_else(|| self.failure(crate::wire::ErrorCode::Unauthorized, "fork source descriptor has no sealed project read"))?;
        if handle.authorization() != authorization {
            return Err(self.failure(crate::wire::ErrorCode::Unauthorized, "fork source descriptor differs from its sealed project read"));
        }
        if authorization.source_task != authorization.caller_task
            && (request.preparation.inherited_through_sequence != 0 || request.preparation.summary.is_some())
        {
            return Err(self.failure(crate::wire::ErrorCode::Unsupported, "foreign workspace source cannot inherit conversation history"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = self.io.as_ref().and_then(PreparationIo::original_native_work) {
            handle.require_selection_with_native_work(request.operation_id, authorization.source_task,
                volume, generation, authorization.native_snapshot.as_ref(), work)?;
            return Ok(std::borrow::Cow::Borrowed(handle.grant()));
        }
        handle.require_selection(request.operation_id, authorization.source_task, volume, generation,
            authorization.native_snapshot.as_ref())?;
        handle.grant().require(volume, VolumeOperation::Read)?;
        Ok(std::borrow::Cow::Borrowed(handle.grant()))
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
        let encoded = self.encode(request, self.journal_limit(MAX_REQUEST_BYTES))?;
        encoded_hold = self.io.as_ref().map(|io| io.guard_bytes(&encoded)).transpose()?.flatten();
        let digest = *self.digest(&encoded)?.as_bytes();
        drop(encoded);
        drop(encoded_hold);
        for (volume, role) in creation_volumes(request) {
            let journal = allocation_ref_io(&self.host.provider, volume, self.io.as_ref())?;
            let retained = read_record_bounded::<A, O, AllocationClaim>(&self.host, &journal, "/claim.json", self.journal_limit(MAX_REQUEST_BYTES), self.io.as_ref())
            .await?
            .ok_or_else(|| self.failure(crate::wire::ErrorCode::Unauthorized, "task-bound allocation is missing"))?;
            let expected = AllocationClaim {
                operation_id: request.operation_id,
                preparation_digest: digest,
                task_origin: Some(self.clone_origin(preparation.origin())?),
                parent: self.clone_authority(&request.parent)?,
                child: self.clone_authority(&request.child)?,
                volume: self.clone_volume(volume)?,
                role,
            };
            if !same_allocation_claim(&retained, &expected) {
                return Err(self.failure(crate::wire::ErrorCode::Unauthorized, "allocation belongs to another preparation task"));
            }
        }
        Ok(())
    }

    fn authorize_request(&self, request: &ForkRequest) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = self.io.as_ref().filter(|io| io.original_native_work().is_some()) {
            return self.preparer.check_request_with_native_task(request, io, self.task_preparation);
        }
        request.validate()?;
        self.validate_task_request(request)?;
        if !self.scope.capabilities().contains(capability::FORK_PUBLISH) {
            return Err(Error::Unauthorized("parent project operation is not granted".into()));
        }
        if request.parent != *self.parent.authority()
            || request.preparation.child_project_volume.provider() != &self.host.provider
        {
            return Err(Error::Conflict(
                "fork request does not match this parent/provider boundary".into(),
            ));
        }
        if request.boundary.is_some() {
            return Err(Error::Unsupported(
                "this preparer cannot attest a cross-provider boundary".into(),
            ));
        }
        if self.parent.conversation().and_then(|state| state.agent) == Some(request.child_agent) {
            return Err(Error::Invalid(
                "fork child must have a distinct agent identity".into(),
            ));
        }
        for selection in &request.selections {
            match &selection.revision {
                ResourceRevision::History(reference)
                    if reference.as_resource().provider() != &self.stream_provider =>
                {
                    return Err(Error::Invalid(
                        "fork history belongs to another Stream provider".into(),
                    ));
                }
                ResourceRevision::Project { volume, generation } => {
                    self.project_read_grant(request, volume, generation)?;
                }
                ResourceRevision::PrivateVolume { volume, .. } => {
                    if volume.owner()
                        != &crate::conversation::VolumeOwner::Agent(self.scope.agent().ok_or_else(
                            || Error::Unauthorized("scratch fork requires a parent agent".into()),
                        )?)
                    {
                        return Err(Error::Unauthorized(
                            "scratch fork is not from the direct parent owner".into(),
                        ));
                    }
                    ContentGrant::verify(
                        &self.verifier,
                        &self.scope,
                        volume,
                        VolumeOperation::Read,
                    )?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn check_request(&self, request: &ForkRequest) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.io.as_ref().is_some_and(PreparationIo::uses_native_work) {
            return self.authorize_request(request);
        }
        self.authorize_request(request)?;
        if let Some(history) = &self.history {
            if history.through_sequence() != request.preparation.inherited_through_sequence {
                return Err(Error::Conflict("fork archive differs from its selected logical cut".into()));
            }
        } else if self.parent.conversation().is_some_and(|state|
            state.resident_after_sequence() > 0
                && request.preparation.inherited_through_sequence > 0)
        {
            return Err(Error::Invalid("archived fork requires its authenticated history capture".into()));
        }
        if let Some(selection) = &request.preparation.summary {
            let summary = self.summary.as_ref().ok_or_else(|| {
                Error::Unsupported(
                    "summary fork owner-bound checkpoint provider is not installed".into(),
                )
            })?;
            if &summary.selection != selection
                || self.parent.conversation().map_or(0, |conversation| conversation.logical_revision())
                    != request.preparation.inherited_through_sequence
            {
                return Err(Error::Conflict(
                    "summary fork request differs from its original capture".into(),
                ));
            }
        }
        if request.parent_revision != self.parent.revision() {
            return Err(Error::Conflict(
                "fork preparation is not at the bound parent revision".into(),
            ));
        }
        Ok(())
    }

    fn journal_ref(&self, request: &ForkRequest) -> Result<WorkspaceRef> {
        let name = bounded_io::format_text(
            format_args!("harness-fork-preparation-{}", request.operation_id), self.io.as_ref())?;
        match &self.io {
            Some(io) => WorkspaceRef::new(self.host.provider.clone_with_io(io)?, name.into_bytes(), None),
            None => workspace_ref(self.host.provider.clone(), &name),
        }
    }

    async fn read_claim(&self, journal: &WorkspaceRef, request: &ForkRequest) -> Result<bool> {
        read_request_claim(&self.host, journal, request, self.journal_limit(MAX_REQUEST_BYTES), self.io.as_ref()).await
    }

    async fn claim(&self, request: &ForkRequest) -> Result<WorkspaceRef> {
        let journal = self.journal_ref(request)?;
        let name = std::str::from_utf8(journal.as_resource().key())
            .map_err(|_| self.failure(crate::wire::ErrorCode::Invalid, "fork journal name is not UTF-8"))?;
        let creation_key = self.key(format_args!("fork:{}:journal", request.operation_id))?;
        bounded_io::create_guarded(&self.host, name, &creation_key, self.io.as_ref(), self.task_preparation,
            #[cfg(not(target_arch = "wasm32"))]
            self.native_creation_guard.as_ref(),
        ).await?;
        if self.read_claim(&journal, request).await? {
            return Ok(journal);
        }
        let bytes = self.encode(request, self.journal_limit(MAX_REQUEST_BYTES))?;
        let observed = bounded_io::resolve(&self.host, &journal, self.io.as_ref()).await?;
        let key = self.key(format_args!("fork:{}:claim", request.operation_id))?;
        match bounded_io::apply_guarded(&self.host, &journal, Some(&observed.generation), &[WorkspaceMutation::PutFile {
            path: bounded_io::clone_text("/request.json", self.io.as_ref())?,
            bytes,
        }], &key, self.io.as_ref(), self.task_preparation,
            #[cfg(not(target_arch = "wasm32"))]
            self.native_creation_guard.as_ref(),
        )
            .await
        {
            Ok(_) => {}
            Err(error) if error.code() == crate::wire::ErrorCode::Conflict => {}
            Err(error) => return Err(error),
        }
        if !self.read_claim(&journal, request).await? {
            return Err(Error::Indeterminate(request.operation_id));
        }
        Ok(journal)
    }


    async fn read_report(
        &self,
        journal: &WorkspaceRef,
        request: &ForkRequest,
    ) -> Result<Option<ForkReport>> {
        read_preparation_report(&self.host, journal, request, self.journal_limit(MAX_REPORT_BYTES), self.io.as_ref()).await
    }

    async fn commit_report(
        &self,
        journal: &WorkspaceRef,
        report: &ForkReport,
    ) -> Result<ForkReport> {
        let bytes = self.encode(report, self.journal_limit(MAX_REPORT_BYTES))?;
        let observed = bounded_io::resolve(&self.host, journal, self.io.as_ref()).await?;
        let key = self.key(format_args!("fork:{}:report", report.request.operation_id))?;
        match bounded_io::apply_guarded(&self.host, journal, Some(&observed.generation), &[WorkspaceMutation::PutFile {
            path: bounded_io::clone_text("/report.json", self.io.as_ref())?,
            bytes,
        }], &key, self.io.as_ref(), self.task_preparation,
            #[cfg(not(target_arch = "wasm32"))]
            self.native_creation_guard.as_ref(),
        )
            .await
        {
            Ok(_) => {}
            Err(error) if error.code() == crate::wire::ErrorCode::Conflict => {}
            Err(error) => return Err(error),
        }
        match self.read_report(journal, &report.request).await? {
            Some(committed) if committed == *report => Ok(committed),
            Some(_) => Err(self.failure(crate::wire::ErrorCode::Conflict, "fork operation has a different committed report")),
            None => Err(Error::Indeterminate(report.request.operation_id)),
        }
    }

    async fn capture_result(
        &self,
        journal: &WorkspaceRef,
        request: &ForkRequest,
        index: usize,
        selection: &ForkSelection,
    ) -> Result<Option<Capture>> {
        let path = bounded_io::format_text(format_args!("/capture-{index}-result.json"), self.io.as_ref())?;
        let result =
            read_record_bounded::<A, O, CaptureResult>(&self.host, journal, &path, self.journal_limit(MAX_CAPTURE_BYTES), self.io.as_ref())
                .await?;
        match result {
            Some(result) if result.selection == *selection => {
                let attempt_path = bounded_io::format_text(format_args!("/capture-{index}-attempt.json"), self.io.as_ref())?;
                let attempt = read_record_bounded::<A, O, CaptureAttempt>(&self.host, journal, &attempt_path, self.journal_limit(MAX_CAPTURE_BYTES), self.io.as_ref())
                .await?
                .ok_or_else(|| self.failure(crate::wire::ErrorCode::Storage, "fork capture result has no durable start"))?;
                if attempt.operation_id != request.operation_id || attempt.selection != *selection {
                    return Err(self.failure(crate::wire::ErrorCode::Conflict, "fork capture result has another start identity"));
                }
                validate_capture(selection, &result.capture)?;
                Ok(Some(result.capture))
            }
            Some(_) => Err(self.failure(crate::wire::ErrorCode::Conflict, "fork capture result belongs to another selection")),
            None => Ok(None),
        }
    }

    /// Returns true only to the writer that durably won this attempt. Once a
    /// start record exists, all other callers reconcile instead of invoking
    /// an effectful provider again.
    async fn start_capture(
        &self,
        journal: &WorkspaceRef,
        request: &ForkRequest,
        index: usize,
        selection: &ForkSelection,
    ) -> Result<bool> {
        let path = bounded_io::format_text(format_args!("/capture-{index}-attempt.json"), self.io.as_ref())?;
        if let Some(prior) =
            read_record_bounded::<A, O, CaptureAttempt>(&self.host, journal, &path, self.journal_limit(MAX_CAPTURE_BYTES), self.io.as_ref())
                .await?
        {
            if prior.operation_id != request.operation_id || prior.selection != *selection {
                return Err(self.failure(crate::wire::ErrorCode::Conflict, "fork capture attempt belongs to another selection"));
            }
            return Ok(false);
        }
        let attempt = CaptureAttempt {
            operation_id: request.operation_id,
            selection: ForkSelection { required: selection.required, revision: self.clone_revision(&selection.revision)? },
            attempt: Uuid::new_v4(),
        };
        let observed = bounded_io::resolve(&self.host, journal, self.io.as_ref()).await?;
        let key = self.key(format_args!("fork:{}:capture-attempt:{index}:{}", request.operation_id, attempt.attempt))?;
        match bounded_io::apply_guarded(&self.host, journal, Some(&observed.generation), &[WorkspaceMutation::PutFile {
            path: bounded_io::clone_text(&path, self.io.as_ref())?,
            bytes: self.encode(&attempt, self.journal_limit(MAX_CAPTURE_BYTES))?,
        }], &key, self.io.as_ref(), self.task_preparation,
            #[cfg(not(target_arch = "wasm32"))]
            self.native_creation_guard.as_ref(),
        )
            .await
        {
            Ok(_) => {}
            Err(error) if error.code() == crate::wire::ErrorCode::Conflict => {}
            Err(error) => return Err(error),
        }
        match read_record_bounded::<A, O, CaptureAttempt>(&self.host, journal, &path, self.journal_limit(MAX_CAPTURE_BYTES), self.io.as_ref())
            .await?
        {
            Some(prior) if prior == attempt => Ok(true),
            Some(prior)
                if prior.operation_id == request.operation_id && prior.selection == *selection =>
            {
                Ok(false)
            }
            Some(_) => Err(self.failure(crate::wire::ErrorCode::Conflict, "fork capture attempt belongs to another selection")),
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
            .capture_result(journal, request, index, selection)
            .await?
        {
            return if prior == capture {
                Ok(prior)
            } else {
                Err(self.failure(crate::wire::ErrorCode::Conflict, "fork capture has another committed result"))
            };
        }
        let path = bounded_io::format_text(format_args!("/capture-{index}-result.json"), self.io.as_ref())?;
        let observed = bounded_io::resolve(&self.host, journal, self.io.as_ref()).await?;
        let key = self.key(format_args!("fork:{}:capture-result:{index}", request.operation_id))?;
        match bounded_io::apply_guarded(&self.host, journal, Some(&observed.generation), &[WorkspaceMutation::PutFile {
            path,
            bytes: self.encode(&CaptureResult {
                selection: selection.clone(),
                capture: capture.clone(),
            }, self.journal_limit(MAX_CAPTURE_BYTES))?,
        }], &key, self.io.as_ref(), self.task_preparation,
            #[cfg(not(target_arch = "wasm32"))]
            self.native_creation_guard.as_ref(),
        )
            .await
        {
            Ok(_) => {}
            Err(error) if error.code() == crate::wire::ErrorCode::Conflict => {}
            Err(error) => return Err(error),
        }
        match self
            .capture_result(journal, request, index, selection)
            .await?
        {
            Some(committed) if committed == capture => Ok(committed),
            Some(_) => Err(self.failure(crate::wire::ErrorCode::Conflict, "fork capture has another committed result")),
            None => Err(Error::Indeterminate(request.operation_id)),
        }
    }

    async fn prepare_inner(&self, request: ForkRequest) -> Result<ForkReport> {
        self.check_request(&request)?;
        self.prepare_checked_inner(request).await
    }

    #[allow(
        clippy::too_many_lines,
        clippy::cognitive_complexity,
        reason = "one journaled fork preparation must bind each side effect to the same request"
    )]
    async fn prepare_checked_inner(&self, request: ForkRequest) -> Result<ForkReport> {
        if request.authorized_source.is_some() && self.task_preparation.is_none() {
            return Err(self.failure(crate::wire::ErrorCode::Unauthorized, "authorized workspace capture requires its original preparation lease"));
        }
        self.verify_task_lease(false).await?;
        let summary_before = self.io.as_ref().map(|io| io.owned_bytes()).transpose()?.unwrap_or(0);
        let _summary_hold;
        let summary_proof = self.prepare_bound_summary(&request, self.io.as_ref()).await?;
        _summary_hold = self.io.as_ref().map(|io| io.guard_owned(io.owned_bytes()? - summary_before)).transpose()?;
        let journal = self.claim(&request).await?;
        if let Some(report) = self.read_report(&journal, &request).await? {
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
            .ok_or_else(|| self.failure(crate::wire::ErrorCode::Invalid, "fork has no project selection"))?;
        let request_bytes = self.encode(&request, self.journal_limit(MAX_REQUEST_BYTES))?;
        let request_digest = self.digest(&request_bytes)?;
        for (volume, role) in creation_volumes(&request) {
            self.verify_task_lease(false).await?;
            self.host
                .claim_fork_allocation_bounded(AllocationClaim {
                    operation_id: request.operation_id,
                    preparation_digest: *request_digest.as_bytes(),
                    task_origin: self.task_preparation.map(|task| self.clone_origin(task.origin())).transpose()?,
                    parent: self.clone_authority(&request.parent)?,
                    child: self.clone_authority(&request.child)?,
                    volume: self.clone_volume(volume)?,
                    role,
                }, self.journal_limit(MAX_REQUEST_BYTES), self.io.as_ref(), self.task_preparation,
                    #[cfg(not(target_arch = "wasm32"))]
                    self.native_creation_guard.as_ref(),
                )
                .await?;
        }
        let private = self.clone_volume(&request.preparation.child_private_volume)?;
        let (source_private, private_generation, paths) = request
            .selections
            .iter()
            .find_map(|selection| {
                if let ResourceRevision::PrivateVolume {
                    volume,
                    generation,
                    paths,
                } = &selection.revision
                {
                    Some((volume, generation, paths))
                } else {
                    None
                }
            })
            .ok_or_else(|| self.failure(crate::wire::ErrorCode::Invalid, "fork has no parent scratch selection"))?;
        let private_key = self.key(format_args!("fork:{}:{request_digest}:scratch", request.operation_id))?;
        let private_grant = self.read_grant(source_private)?;
        let project_grant = self.project_read_grant(&request, source_project, source_generation)?;
        self.verify_task_lease(false).await?;
        let private_observation = bounded_io::fork_guarded(
            &self.host, source_private, &private_grant, private_generation, &private,
            Some(self.clone_paths(paths)?), &private_key, self.io.as_ref(), self.task_preparation,
            #[cfg(not(target_arch = "wasm32"))]
            self.native_creation_guard.as_ref(),
        ).await?;
        #[cfg(not(target_arch = "wasm32"))]
        let controller = if let Some(work) = self.io.as_ref().and_then(PreparationIo::original_native_work) {
            ParentProjectController::new_with_native_work(&self.host, &self.parent,
                &self.verifier, &self.scope, &self.project, work)?
        } else {
            ParentProjectController::new(&self.host, &self.parent, &self.verifier,
                &self.scope, self.project.clone())?
        };
        #[cfg(target_arch = "wasm32")]
        let controller = ParentProjectController::new(&self.host, &self.parent,
            &self.verifier, &self.scope, self.project.clone())?;
        let project_key = IdempotencyKey::new(bounded_io::format_text(
            format_args!("fork:{}:{request_digest}:project", request.operation_id), self.io.as_ref())?)?;
        self.verify_task_lease(false).await?;
        // The same authenticated original source-read was required above;
        // do not rebuild an ordinary grant or substitute provider-root scope.
        let child_project = bounded_io::fork_guarded(
            &self.host, source_project, &project_grant, source_generation,
            &request.preparation.child_project_volume, None, &project_key, self.io.as_ref(), self.task_preparation,
            #[cfg(not(target_arch = "wasm32"))]
            self.native_creation_guard.as_ref(),
        ).await?;
        let (child_result_volume, child_result_generation) = if let Some(volume) = &request.preparation.child_result_volume {
            self.verify_task_lease(false).await?;
            let key = self.key(format_args!("fork:{}:{request_digest}:result",request.operation_id))?;
            let result = bounded_io::fork_guarded(
                &self.host, source_private, &private_grant, private_generation, volume,
                Some(Vec::new()), &key, self.io.as_ref(), self.task_preparation,
                #[cfg(not(target_arch = "wasm32"))]
                self.native_creation_guard.as_ref(),
            ).await?;
            (Some(self.clone_volume(volume)?), Some(result.generation))
        } else { (None,None) };
        let (
            mut child_private_generation,
            mut inherited_context,
            mut reference_grants,
            attachment_manifests,
        ) = if request.preparation.inherited_through_sequence == 0 {
            (
                self.clone_generation(&private_observation.generation)?,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
        } else {
            let context_key = self.key(format_args!("fork:{}:{request_digest}:context", request.operation_id))?;
            let continuation_keys = if request.preparation.summary.is_some() {
                let keys = super::summary_fork::summary_keys(&context_key, self.io.as_ref())?;
                let mut values = bounded_io::vector(keys.len(), self.io.as_ref())?;
                values.extend(keys);
                values
            } else {
                Vec::new()
            };
            let inherited = controller
                .materialize_inherited_conversation_continued(
                    &self.parent,
                    &private,
                    &private_observation.generation,
                    request.child_agent,
                    &request.attached_agents,
                    request.preparation.inherited_through_sequence,
                    usize::try_from(request.preparation.maximum_inherited_messages)
                        .map_err(|_| self.failure(crate::wire::ErrorCode::Invalid, "fork message bound is too large"))?,
                    request.preparation.maximum_inherited_bytes,
                    request.preparation.maximum_inherited_references as usize,
                    self.resolver.as_ref(),
                    &context_key,
                    &continuation_keys,
                    self.history.as_ref().map(PreparedForkHistory::conversation),
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
            let bound = self.summary.as_ref().ok_or_else(|| {
                self.failure(crate::wire::ErrorCode::Unsupported, "summary fork provider is not installed")
            })?;
            let prepared = summary_proof.as_ref().ok_or_else(||
                self.failure(crate::wire::ErrorCode::Storage, "summary source proof is missing"))?;
            let key = self.key(format_args!("fork:{}:{request_digest}:context", request.operation_id))?;
            let prefix = inherited_context
                .first()
                .ok_or_else(|| self.failure(crate::wire::ErrorCode::Invalid, "summary fork omitted its logical prefix"))?;
            // Count the exact additional reader grants before either Summary
            // publication phase. Reuse original prefix grants when identical.
            let (_, summary_references) = super::summary_fork::split_references(prepared, self.io.as_ref())?;
            let mut planned_grants = BTreeSet::new();
            for grant in &reference_grants {
                self.reference_selection(&mut planned_grants, &grant.file, grant.reader)?;
            }
            for reader in
                std::iter::once(request.child_agent).chain(request.attached_agents.iter().copied())
            {
                for file in summary_references.values() {
                    if file.volume().owner() != &crate::conversation::VolumeOwner::Agent(reader) {
                        self.reference_selection(&mut planned_grants, file, reader)?;
                    }
                }
            }
            if planned_grants.len() > request.preparation.maximum_inherited_references as usize {
                return Err(self.failure(crate::wire::ErrorCode::Invalid, "summary fork reader grants exceed capture allowance"));
            }
            let (capture, generation) = controller
                .materialize_summary_context(
                    &self.parent,
                    prepared,
                    bound.journal.as_ref(),
                    &private,
                    &child_private_generation,
                    prefix,
                    request.preparation.maximum_inherited_bytes,
                    request.preparation.maximum_inherited_references as usize,
                    &key,
                    self.resolver.as_ref(),
                    self.io.as_ref(),
                    self.task_preparation,
                    #[cfg(not(target_arch = "wasm32"))]
                    self.native_creation_guard.as_ref(),
                )
                .await?;
            let mut granted = BTreeSet::new();
            for grant in &reference_grants {
                self.reference_selection(&mut granted, &grant.file, grant.reader)?;
            }
            let maximum_grants = reference_grants.len().checked_add(capture.references.len()
                .checked_mul(request.attached_agents.len() + 1)
                .ok_or_else(|| bounded_io::invalid(self.io.as_ref(), "summary readers overflow"))?)
                .ok_or_else(|| bounded_io::invalid(self.io.as_ref(), "summary grants overflow"))?;
            self.reserve(&mut reference_grants, maximum_grants)?;
            for reader in
                std::iter::once(request.child_agent).chain(request.attached_agents.iter().copied())
            {
                for file in &capture.references {
                    if file.volume().owner() != &crate::conversation::VolumeOwner::Agent(reader)
                        && self.reference_selection(&mut granted, file, reader)?
                    {
                        reference_grants.push(ReferenceGrant {
                            file: bounded_io::clone_file(file, self.io.as_ref())?,
                            reader,
                            attachment_manifest: None,
                        });
                    }
                }
            }
            let capacity = inherited_context.len().checked_add(capture.inherited_files().count())
                .ok_or_else(|| bounded_io::invalid(self.io.as_ref(), "summary context count overflows"))?;
            self.reserve(&mut inherited_context, capacity)?;
            for file in capture.inherited_files() {
                inherited_context.push(bounded_io::clone_file(file, self.io.as_ref())?);
            }
            child_private_generation = generation;
            Some(capture)
        } else {
            None
        };
        let mut captures = bounded_io::vector(request.selections.len(), self.io.as_ref())?;
        let shared_count = request.selections.iter().filter(|selection|
            matches!(&selection.revision, ResourceRevision::SharedVolume(volume) if volume.provider() == &self.host.provider))
            .count().checked_mul(request.attached_agents.len() + 1)
            .ok_or_else(|| bounded_io::invalid(self.io.as_ref(), "shared reader count overflows"))?;
        let mut shared_grants = bounded_io::vector(shared_count, self.io.as_ref())?;
        for (index, selection) in request.selections.iter().enumerate() {
            self.verify_task_lease(false).await?;
            let capture = match &selection.revision {
                ResourceRevision::History(_) => Capture::Captured(CapturedResource {
                    source: self.clone_revision(&selection.revision)?,
                    revision: self.clone_revision(&selection.revision)?,
                }),
                ResourceRevision::Project { .. } => Capture::Captured(CapturedResource {
                    source: self.clone_revision(&selection.revision)?,
                    revision: ResourceRevision::Project {
                        volume: self.clone_volume(&request.preparation.child_project_volume)?,
                        generation: self.clone_generation(&child_project.generation)?,
                    },
                }),
                ResourceRevision::PrivateVolume { paths, .. } => {
                    Capture::Captured(CapturedResource {
                        source: self.clone_revision(&selection.revision)?,
                        revision: ResourceRevision::PrivateVolume {
                            volume: self.clone_volume(&private)?,
                            generation: self.clone_generation(&private_observation.generation)?,
                            paths: self.clone_paths(paths)?,
                        },
                    })
                }
                ResourceRevision::SharedVolume(volume)
                    if volume.provider() == &self.host.provider =>
                {
                    self.read_grant(volume)?;
                    let reference = bounded_io::content_workspace(&self.host.provider, volume, self.io.as_ref())?;
                    bounded_io::resolve(&self.host, &reference, self.io.as_ref()).await?;
                    for reader in std::iter::once(request.child_agent)
                        .chain(request.attached_agents.iter().copied())
                    {
                        bounded_io::item(self.io.as_ref(), std::mem::size_of::<VolumeOperation>() as u64)?;
                        shared_grants.push(SharedGrant {
                            volume: self.clone_volume(volume)?,
                            child_agent: reader,
                            operations: BTreeSet::from([VolumeOperation::Read]),
                        });
                    }
                    Capture::Captured(CapturedResource {
                        source: self.clone_revision(&selection.revision)?,
                        revision: self.clone_revision(&selection.revision)?,
                    })
                }
                _ => {
                    if let Some(provider) = self
                        .capture_providers
                        .iter()
                        .find(|provider| provider.provider() == selection.revision.provider())
                    {
                        if let Some(committed) = self
                            .capture_result(&journal, &request, index, selection)
                            .await?
                        {
                            committed
                        } else {
                            let fresh = self
                                .start_capture(&journal, &request, index, selection)
                                .await?;
                            let captured = if fresh {
                                self.verify_task_lease(false).await?;
                                provider.capture(&request, selection).await?
                            } else {
                                provider
                                    .reconcile(&request, selection)
                                    .await?
                                    .ok_or(Error::Indeterminate(request.operation_id))?
                            };
                            self.commit_capture_result(
                                &journal, &request, index, selection, captured,
                            )
                            .await?
                        }
                    } else {
                        Capture::Unsupported(bounded_io::clone_text("resource capture provider is not installed", self.io.as_ref())?)
                    }
                }
            };
            captures.push(capture);
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
        if let Some(io) = &self.io {
            report.validate_with_work(&mut Some(&mut io.history_io()))?;
        } else {
            report.validate()?;
        }
        if let Some(preparation) = &self.task_preparation {
            for grant in &report.reference_grants {
                self.reference_donor(preparation.grants(), grant)?;
                if grant.reader == report.request.child_agent
                    && !{
                        let key = bounded_io::reference_key(&grant.file, self.io.as_ref())?;
                        preparation.request().child.grants.contains(&key)
                    }
                {
                    return Err(self.failure(crate::wire::ErrorCode::Unauthorized, "prepared child omitted an exact inherited-file read grant"));
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
                report.clone_with_work(&mut Some(&mut observer))?
                    .into_seed_with_work(&mut Some(&mut observer))?
            } else {
                report.clone().into_seed()?
            };
            if let Some(history) = &self.history {
                if let Some(io) = &self.io {
                    let mut observer = io.history_io();
                    self.parent.attest_fork_history_with_work(&mut seed, history.conversation(), &mut Some(&mut observer))?;
                } else {
                    self.parent.attest_fork_history(&mut seed, history.conversation())?;
                }
                report.history_witness = seed.history_witness;
            }
            self.host.bind_fork_seed_bounded(&seed, self.journal_limit(MAX_REQUEST_BYTES), self.io.as_ref(), self.task_preparation,
                #[cfg(not(target_arch = "wasm32"))]
                self.native_creation_guard.as_ref(),
            ).await?;
        }
        self.verify_task_lease(false).await?;
        self.commit_report(&journal, &report).await
    }
}

/// The member proof is produced by the canonical authenticated manifest
/// traversal and retained in the protected original report/seed binding.
pub(super) fn require_reference_donor(grants: &crate::Capabilities, reference: &ReferenceGrant) -> Result<()> {
    reference.validate()?;
    if crate::runtime::read_granted(grants, &reference.file)? {
        return Ok(());
    }
    if let Some(manifest) = &reference.attachment_manifest
        && crate::runtime::read_granted(grants, manifest)?
    {
        return Ok(());
    }
    Err(Error::Unauthorized(
        "preparation task cannot delegate an inherited reference it cannot read".into(),
    ))
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Claims exact child-volume identities for a custom parent-authorized
    /// preparation path. The stock preparer claims the same identities from
    /// its immutable request before creating either workspace.
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
        seed.validate()?;
        if parent.authority() != &seed.parent || parent.revision() != seed.parent_revision {
            return Err(Error::Conflict(
                "fork allocation is not at the selected parent revision".into(),
            ));
        }
        let source_project = seed
            .resources
            .iter()
            .find_map(|resource| {
                if let ResourceRevision::Project { volume, .. } = &resource.source {
                    Some(volume)
                } else {
                    None
                }
            })
            .ok_or_else(|| Error::Invalid("fork has no source project".into()))?;
        let controller =
            ParentProjectController::new(self, parent, verifier, scope, source_project.clone())?;
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
                if seed.history_witness != Some(verifier.fork_history_proof(seed)?) {
                    return Err(Error::Unauthorized("archived fork activation requires its original witness".into()));
                }
                // The signed seed pins this file's exact descriptor/digest.
                // Earlier retired cuts cannot be recomputed from a resident suffix.
                actual.format_version == 3
                    && actual.parent == seed.parent
                    && actual.parent_revision == seed.parent_revision
                    && actual.parent_agent == parent_agent
                    && actual.through_sequence == seed.inherited_through_sequence
                    && actual.attached_agents == seed.attached_agents
            } else {
                actual == InheritedConversationPrefix::select(
                    seed.parent.clone(), seed.parent_revision, seed.inherited_through_sequence,
                    &seed.attached_agents, conversation,
                )?
            };
            if !selected || actual.canonical_bytes()?.as_slice() != bytes.as_ref() {
                return Err(Error::Conflict(
                    "inherited conversation differs from the selected parent prefix".into(),
                ));
            }
        }
        let digest = seed_binding(seed)?.digest;
        let child_project = seed
            .resources
            .iter()
            .find_map(|resource| {
                if let ResourceRevision::Project { volume, .. } = &resource.revision {
                    Some(volume)
                } else {
                    None
                }
            })
            .ok_or_else(|| Error::Invalid("fork has no child project".into()))?;
        if source_project.provider() != &self.provider
            || child_project.provider() != &self.provider
            || seed.child_private_volume.provider() != &self.provider
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
            })
            .await?;
        }
        self.bind_fork_seed(seed).await
    }

    /// Reads the original protected claim through the same bounded codec as
    /// publication verification. A qualified current READ query and genuine
    /// immutable admission witness are required before any claim-provider IO.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn observe_original_fork_allocation_claim_with_native_work<'a, P: StreamProvider>(
        &'a self,
        witness: &'a crate::durable_host::TaskAdmissionReadonlyWitness<P>,
        query: &'a Arc<crate::runtime::NativeTaskContext>,
        admitted: &'a crate::resource_preparation::ResourcePreparationRequest,
        volume: &'a VolumeRef,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Option<OriginalForkAllocationClaim>>> {
        // The witness producer statically authenticates the actual issued query
        // before constructing its funded decoder future; no UUID grants a read.
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
            request.validate_with_work(&mut Some(&mut io.history_io()))?;
            let fork = io.value_decode_preparation::<ForkRequest>(
                &request.payload, crate::wire::ErrorCode::Invalid,
            )?;
            fork.validate_with_work(&mut Some(&mut io.history_io()))?;
            if fork.authorized_source != request.read_authorization
                || request.resources.len() != creation_volumes(&fork).count()
                || fork.preparation.maximum_inherited_bytes > request.maximum_bytes
            {
                return Err(reject("fork claim source or resources differ from their immutable request"));
            }
            let role = creation_volumes(&fork).find_map(|(selected, role)|
                (selected == volume).then_some(role))
                .ok_or_else(|| reject("fork claim volume is outside the witnessed request"))?;
            for (resource, (selected, _)) in request.resources.iter().zip(creation_volumes(&fork)) {
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
            let encoded_hold;
            let encoded = io.encode(&*fork, MAX_REQUEST_BYTES.min(request.maximum_bytes))?;
            encoded_hold = io.guard_owned(encoded.capacity() as u64)?;
            let hash = WorkCounters { bytes_hashed: encoded.len() as u64, ..WorkCounters::UNCHARGED };
            io.preflight(hash)?;
            let preparation_digest = *blake3::hash(&encoded).as_bytes();
            io.charge(hash)?;
            drop(encoded);
            drop(encoded_hold);

            let journal_hold;
            let before = io.owned_bytes()?;
            let journal = allocation_ref_io(&self.provider, volume, Some(&io))?;
            journal_hold = io.guard_owned(io.owned_bytes()? - before)?;
            witness.verify_query_with_native_work(query, work).await?;
            let claim = read_original_fork_claim_record_io(
                self, &journal, MAX_REQUEST_BYTES.min(request.maximum_bytes), Some(&io),
            ).await?;
            // Missing means authenticated absence, never a new allocation or a
            // search through a native-process intent or current Started record.
            witness.verify_query_with_native_work(query, work).await?;
            let Some(claim) = claim else { return Ok(None) };
            let origin = claim.task_origin.as_ref()
                .ok_or_else(|| reject("fork claim has no genuine original preparation origin"))?;
            witness.verify_preparation_origin_with_native_work(query, &request, origin, work)?;
            if claim.operation_id != fork.operation_id || claim.parent != fork.parent
                || claim.child != fork.child || claim.volume != *volume || claim.role != role
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
            Ok(Some(OriginalForkAllocationClaim { claim, request, custody }))
        })
    }

    pub(crate) async fn verify_fork_allocation(
        &self,
        seed: &ForkSeed,
        volume: &VolumeRef,
    ) -> Result<()> {
        self.verify_fork_allocation_io(seed,volume,None).await
    }

    pub(crate) async fn verify_fork_allocation_io(
        &self, seed: &ForkSeed, volume: &VolumeRef, io: Option<&PreparationIo>,
    ) -> Result<()> {
        let journal_hold;
        let journal = allocation_ref_io(&self.provider, volume, io)?;
        journal_hold = io.map(|io| {
            let resource = journal.as_resource();
            let provider = resource.provider();
            let bytes = [resource.key().len(),provider.namespace().len(),provider.family().len(),provider.version().len()]
                .into_iter().try_fold(0_u64, |total,bytes| total.checked_add(bytes as u64)
                    .ok_or_else(|| Error::Invalid("allocation journal ownership overflows".into())))?;
            io.guard_owned(bytes)
        }).transpose()?;
        let claim = read_original_fork_claim_record_io(
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
        let binding = read_record_decoded_bounded::<A,O,SeedBinding>(
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
        if binding != seed_binding_bounded(seed,io)? {
            return Err(Error::Conflict(
                "fork seed differs from its allocated publication".into(),
            ));
        }
        drop(journal);
        drop(journal_hold);
        Ok(())
    }

    async fn bind_fork_seed(&self, seed: &ForkSeed) -> Result<()> {
        self.bind_fork_seed_bounded(seed, MAX_REQUEST_BYTES, None, None,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        ).await
    }

    async fn bind_fork_seed_bounded(
        &self, seed: &ForkSeed, maximum_bytes: u64, io: Option<&PreparationIo>,
        preparation: Option<&crate::resource_preparation::TaskResourcePreparation>,
        #[cfg(not(target_arch = "wasm32"))]
        native_creation_guard: Option<&std::sync::Arc<crate::resource_preparation::NativeResourcePreparationCreationGuard>>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            native_creation_guard.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native seed binding requires its original issued creation guard",
            })?.verify_creation_with_native_work(work)?.await?;
        }
        let binding = seed_binding_bounded(seed, io)?;
        for (volume, role) in seed_creation_volumes(seed)? {
            let journal = allocation_ref_io(&self.provider, volume, io)?;
            let claim =
                read_record_bounded::<A, O, AllocationClaim>(self, &journal, "/claim.json", maximum_bytes, io)
                    .await?
                    .ok_or_else(|| bounded_io::unauthorized(
                        io, "fork child volume was not allocated",
                    ))?;
            if claim.operation_id != seed.operation_id
                || claim.parent != seed.parent
                || claim.child != seed.child
                || claim.volume != *volume
                || claim.role != role
            {
                return Err(bounded_io::conflict(
                    io, "fork child volume belongs to another preparation",
                ));
            }
            match read_record_bounded::<A, O, SeedBinding>(self, &journal, "/seed.json", maximum_bytes, io).await? {
                Some(prior) if prior == binding => continue,
                Some(_) => return Err(bounded_io::conflict(io, "fork allocation has another seed")),
                None => {}
            }
            let observed = bounded_io::resolve(self, &journal, io).await?;
            let key = IdempotencyKey::new(bounded_io::format_text(
                format_args!("fork:{}:seed", seed.operation_id), io,
            )?)?;
            match bounded_io::apply_guarded(self, &journal, Some(&observed.generation), &[WorkspaceMutation::PutFile {
                path: bounded_io::clone_text("/seed.json", io)?,
                bytes: encode_bounded(&binding, maximum_bytes, io)?,
            }], &key, io, preparation,
                #[cfg(not(target_arch = "wasm32"))]
                native_creation_guard,
            )
                .await
            {
                Ok(_) => {}
                Err(error) if error.code() == crate::wire::ErrorCode::Conflict => {}
                Err(error) => return Err(error),
            }
            match read_record_bounded::<A, O, SeedBinding>(self, &journal, "/seed.json", maximum_bytes, io).await? {
                Some(prior) if prior == binding => {}
                Some(_) => return Err(bounded_io::conflict(io, "fork allocation has another seed")),
                None => return Err(Error::Indeterminate(seed.operation_id)),
            }
        }
        Ok(())
    }

    async fn claim_fork_allocation(&self, claim: AllocationClaim) -> Result<()> {
        self.claim_fork_allocation_bounded(claim, MAX_REQUEST_BYTES, None, None,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        ).await
    }

    async fn claim_fork_allocation_bounded(
        &self, claim: AllocationClaim, maximum_bytes: u64, io: Option<&PreparationIo>,
        preparation: Option<&crate::resource_preparation::TaskResourcePreparation>,
        #[cfg(not(target_arch = "wasm32"))]
        native_creation_guard: Option<&std::sync::Arc<crate::resource_preparation::NativeResourcePreparationCreationGuard>>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            native_creation_guard.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native allocation claim requires its original issued creation guard",
            })?.verify_creation_with_native_work(work)?.await?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            claim.volume.validate_with_native_work(work)?;
        } else {
            claim.volume.validate()?;
        }
        #[cfg(target_arch = "wasm32")]
        claim.volume.validate()?;
        if claim.volume.provider() != &self.provider {
            return Err(bounded_io::invalid(
                io, "fork allocation belongs to another Filesystem provider",
            ));
        }
        let journal = allocation_ref_io(&self.provider, &claim.volume, io)?;
        let name = std::str::from_utf8(journal.as_resource().key())
            .map_err(|_| bounded_io::invalid(io, "allocation journal name is not UTF-8"))?;
        let creation_key = IdempotencyKey::new(bounded_io::format_text(
            format_args!("fork:{}:allocation-journal", claim.operation_id), io,
        )?)?;
        bounded_io::create_guarded(self, name, &creation_key, io, preparation,
            #[cfg(not(target_arch = "wasm32"))]
            native_creation_guard,
        ).await?;
        match read_record_bounded::<A, O, AllocationClaim>(self, &journal, "/claim.json", maximum_bytes, io).await? {
            Some(prior) if same_allocation_claim(&prior, &claim) => return Ok(()),
            Some(_) => {
                return Err(bounded_io::conflict(
                    io, "fork child volume is already allocated",
                ));
            }
            None => {}
        }
        let observed = bounded_io::resolve(self, &journal, io).await?;
        let key = IdempotencyKey::new(bounded_io::format_text(
            format_args!("fork:{}:allocation", claim.operation_id), io,
        )?)?;
        match bounded_io::apply_guarded(self, &journal, Some(&observed.generation), &[WorkspaceMutation::PutFile {
            path: bounded_io::clone_text("/claim.json", io)?,
            bytes: encode_bounded(&claim, maximum_bytes, io)?,
        }], &key, io, preparation,
            #[cfg(not(target_arch = "wasm32"))]
            native_creation_guard,
        )
            .await
        {
            Ok(_) => {}
            Err(error) if error.code() == crate::wire::ErrorCode::Conflict => {}
            Err(error) => return Err(error),
        }
        match read_record_bounded::<A, O, AllocationClaim>(self, &journal, "/claim.json", maximum_bytes, io).await? {
            Some(prior) if same_allocation_claim(&prior, &claim) => Ok(()),
            Some(_) => Err(bounded_io::conflict(io, "fork child volume is already allocated")),
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
    // Recovery changes only the current reservation: retain the original
    // creation fence without cloning either complete claim or lease strings.
    match (&retained.task_origin,&requested.task_origin) {
        (None,None) => true,
        (Some(prior),Some(current)) =>
            prior.owner == current.owner && prior.parent_task == current.parent_task
                && prior.preparation_task == current.preparation_task
                && prior.intended_child == current.intended_child
                && prior.admission_digest == current.admission_digest
                && prior.request_digest == current.request_digest
                && prior.source_grants == current.source_grants,
        _ => false,
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_reference_selection(
    selected: &mut BTreeSet<(String, crate::AgentId)>, key: String,
    reader: crate::AgentId, io: &PreparationIo,
) -> Result<bool> {
    let key_bytes = key.capacity() as u64;
    let selection = (key, reader);
    if selected.contains(&selection) {
        drop(selection);
        io.release(key_bytes)?;
        return Ok(false);
    }
    let bytes = std::mem::size_of::<(String, crate::AgentId)>() as u64;
    io.claim(bytes)?;
    Ok(selected.insert(selection))
}

#[cfg(not(target_arch = "wasm32"))]
fn native_projection_reserve<T>(
    values: &mut Vec<T>, capacity: usize, io: &PreparationIo,
) -> Result<()> {
    if capacity <= values.capacity() { return Ok(()) }
    let bytes = (capacity - values.capacity()).checked_mul(std::mem::size_of::<T>())
        .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "native projection growth overflows" })? as u64;
    io.claim(bytes)?;
    let copy = acyclic_fs::WorkCounters {
        bytes_copied: (values.len() * std::mem::size_of::<T>()) as u64, ..Default::default()
    };
    io.preflight(copy)?;
    io.allocation_attempt(bytes)?;
    values.try_reserve_exact(capacity - values.len()).map_err(|source| Error::NativeWork {
        cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(source), custody: None,
    })?;
    if values.capacity() > capacity {
        let excess = (values.capacity() - capacity).checked_mul(std::mem::size_of::<T>())
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native projection capacity overflows" })? as u64;
        io.claim(excess)?;
    }
    io.charge(copy)?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeProjectionVec<T> {
    values: Vec<T>,
    custody: acyclic_fs::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
fn native_projection_vec<T>(
    count: usize, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<NativeProjectionVec<T>> {
    let bytes = count.checked_mul(std::mem::size_of::<T>())
        .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "native projection vector ownership overflows" })? as u64;
    let mut custody = if bytes == 0 {
        work.retain_owned(0)
    } else {
        work.claim(bytes)
    }.map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let mut values = Vec::new();
    if let Err(source) = values.try_reserve_exact(count) {
        return Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(source),
            custody: Some(custody),
        });
    }
    if let Err(cause) = custody.reconcile_capacity(
        (values.capacity() * std::mem::size_of::<T>()) as u64,
    ) {
        return Err(Error::NativeWork { cause, custody: Some(custody) });
    }
    Ok(NativeProjectionVec { values, custody })
}

#[cfg(not(target_arch = "wasm32"))]
fn native_projection_bytes<T>(
    values: &[T], capacity: usize, owned: impl Fn(&T) -> Result<u64>,
) -> Result<u64> {
    let slots = capacity.checked_mul(std::mem::size_of::<T>())
        .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "native projection vector ownership overflows" })? as u64;
    values.iter().try_fold(slots, |bytes, value| {
        bytes.checked_add(owned(value)?)
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native projection payload ownership overflows" })
    })
}

fn allocation_ref(provider: ProviderRef, volume: &VolumeRef) -> Result<WorkspaceRef> {
    workspace_ref(
        provider,
        &format!("fork-allocation-{}", volume.storage_name()?),
    )
}

fn allocation_ref_io(provider: &ProviderRef, volume: &VolumeRef, io: Option<&PreparationIo>) -> Result<WorkspaceRef> {
    let Some(io) = io else { return allocation_ref(provider.clone(),volume); };
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = io.original_native_work() {
        let volume_hold;
        let volume_name = bounded_io::volume_storage_name(volume, Some(io))?;
        volume_hold = io.guard_owned(volume_name.capacity() as u64)?;
        let length = b"fork-allocation-".len().checked_add(volume_name.len())
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "allocation journal name overflows" })?;
        let copy = acyclic_fs::WorkCounters { bytes_copied: length as u64, ..Default::default() };
        work.admit(copy).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut key = native_projection_vec::<u8>(length, work)?;
        key.values.extend_from_slice(b"fork-allocation-");
        key.values.extend_from_slice(volume_name.as_bytes());
        work.spent(copy).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        drop(volume_name);
        drop(volume_hold);
        io.absorb_native_custody(key.custody)?;
        return WorkspaceRef::new(provider.clone_with_io(io)?, key.values, None);
    }
    let volume_hold;
    let volume_name = bounded_io::volume_storage_name(volume,Some(io))?;
    volume_hold = io.guard_owned(volume_name.len() as u64)?;
    let length = "fork-allocation-".len().checked_add(volume_name.len())
        .ok_or_else(|| Error::Invalid("allocation journal name overflows".into()))?;
    io.claim(length as u64)?;
    let name_hold = io.guard_owned(length as u64)?;
    io.allocation_attempt(length as u64)?;
    let mut name = String::with_capacity(length);
    name.push_str("fork-allocation-");
    name.push_str(&volume_name);
    drop(volume_name);
    drop(volume_hold);
    bounded_io::strings(io,&[provider.namespace(),provider.family(),provider.version()])?;
    io.claim(length as u64)?;
    let result = workspace_ref(provider.clone(),&name);
    drop(name);
    drop(name_hold);
    result
}

fn seed_binding(seed: &ForkSeed) -> Result<SeedBinding> {
    seed_binding_bounded(seed, None)
}

fn seed_binding_bounded(seed: &ForkSeed, io: Option<&PreparationIo>) -> Result<SeedBinding> {
    if let Some(io) = io {
        seed.validate_with_work(&mut Some(&mut io.history_io()))?;
    } else {
        seed.validate()?;
    }
    let encoded_hold;
    let encoded = encode_bounded(seed,MAX_REPORT_BYTES,io)?;
    encoded_hold = io.map(|io| io.guard_bytes(&encoded)).transpose()?.flatten();
    let hash = acyclic_fs::WorkCounters { bytes_hashed: encoded.len() as u64, ..Default::default() };
    if let Some(io) = io { io.preflight(hash)?; }
    let digest = *blake3::hash(&encoded).as_bytes();
    if let Some(io) = io { io.charge(hash)?; }
    drop(encoded);
    drop(encoded_hold);
    Ok(SeedBinding {digest})
}

fn encode_bounded<T: Serialize>(value: &T, maximum_bytes: u64, io: Option<&PreparationIo>) -> Result<acyclic_stream::OwnedBytes> {
    match io {
        Some(io) => io.encode(value, maximum_bytes),
        None => encode_record(value, maximum_bytes),
    }
}


async fn read_request_claim<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, journal: &WorkspaceRef, request: &ForkRequest, maximum_bytes: u64, io: Option<&PreparationIo>,
) -> Result<bool> {
    match read_record_bounded::<A, O, ForkRequest>(host, journal, "/request.json", maximum_bytes, io).await? {
        Some(claim) if claim == *request => Ok(true),
        Some(_) => Err(bounded_io::conflict(io, "fork operation is claimed by another request")),
        None => Ok(false),
    }
}

async fn read_preparation_report<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, journal: &WorkspaceRef, request: &ForkRequest, maximum_bytes: u64, io: Option<&PreparationIo>,
) -> Result<Option<ForkReport>> {
    let report = read_record_bounded::<A, O, ForkReport>(host, journal, "/report.json", maximum_bytes, io).await?;
    if let Some(report) = &report {
        if let Some(io) = io {
            report.validate_with_work(&mut Some(&mut io.history_io()))?;
        } else {
            report.validate()?;
        }
        if report.request != *request {
            return Err(bounded_io::conflict(io, "fork report does not match its claimed request"));
        }
    }
    Ok(report)
}

fn validate_capture(selection: &ForkSelection, capture: &Capture) -> Result<()> {
    match capture {
        Capture::Captured(resource) if resource.source == selection.revision => resource.validate(),
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
        Box::pin(async move { self.attempt()?.prepare_inner(request).await })
    }

    fn reconcile<'a>(&'a self, request: ForkRequest) -> BoxFuture<'a, Result<Option<ForkReport>>> {
        Box::pin(async move {
            let this = self.attempt()?;
            this.authorize_request(&request)?;
            if request.authorized_source.is_some() && this.task_preparation.is_none() {
                return Err(Error::Unauthorized("authorized workspace reconciliation requires its original preparation lease".into()));
            }
            this.verify_task_lease(true).await?;
            let summary_before = this.io.as_ref().map(|io| io.owned_bytes()).transpose()?.unwrap_or(0);
            let _summary_hold;
            let _summary_proof = this.prepare_bound_summary(&request, this.io.as_ref()).await?;
            _summary_hold = this.io.as_ref().map(|io| io.guard_owned(io.owned_bytes()? - summary_before)).transpose()?;
            let journal = this.journal_ref(&request)?;
            match bounded_io::resolve(&this.host, &journal, this.io.as_ref()).await {
                Ok(_) => {}
                Err(Error::NotFound(_)) => return Ok(None),
                Err(error) => return Err(error),
            }
            if !this.read_claim(&journal, &request).await? {
                return Ok(None);
            }
            let report = this.read_report(&journal, &request).await?;
            if report.is_some() {
                this.verify_task_allocations(&request).await?;
            }
            Ok(report)
        })
    }
}


fn encode_record<T: Serialize>(value: &T, maximum_bytes: u64) -> Result<acyclic_stream::OwnedBytes> {
    struct BoundedRecord {
        bytes: Vec<u8>,
        maximum: u64,
    }
    impl std::io::Write for BoundedRecord {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let length = (self.bytes.len() as u64).checked_add(bytes.len() as u64)
                .ok_or_else(|| std::io::Error::other("fork journal byte count overflows"))?;
            if length > self.maximum {
                return Err(std::io::Error::other("fork journal record exceeds byte limit"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    let mut record = BoundedRecord { bytes: Vec::new(), maximum: maximum_bytes };
    serde_json::to_writer(&mut record, value).map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(record.bytes.into())
}

async fn read_record<A: AsyncAuthorityStore, O: AsyncObjectStore, T: bounded_io::PreparationRecord>(
    host: &FilesystemHost<A, O>,
    journal: &WorkspaceRef,
    path: &str,
    maximum_bytes: u64,
) -> Result<Option<T>> {
    read_record_bounded(host, journal, path, maximum_bytes, None).await
}

async fn read_original_fork_claim_record_io<A: AsyncAuthorityStore, O: AsyncObjectStore>(
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

async fn read_record_bounded<A: AsyncAuthorityStore, O: AsyncObjectStore, T: bounded_io::PreparationRecord>(
    host: &FilesystemHost<A, O>, journal: &WorkspaceRef, path: &str, maximum_bytes: u64,
    io: Option<&PreparationIo>,
) -> Result<Option<T>> {
    read_record_decoded_bounded(host,journal,path,maximum_bytes,io, |bytes,io| {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            return Ok(io.decode_preparation::<T>(bytes)?.into_retained());
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

    #[test]
    fn record_encoder_stops_at_the_admitted_byte_boundary() -> Result<()> {
        assert_eq!(encode_record(&"x", 3)?.as_ref(), b"\"x\"");
        assert!(matches!(encode_record(&"x", 2), Err(Error::Invalid(_))));
        assert!(matches!(encode_record(&[0_u8; 64], 8), Err(Error::Invalid(_))));
        Ok(())
    }

    #[tokio::test]
    async fn allocation_retains_creation_fence_and_rejects_foreign_task_origins() -> Result<()> {
        let provider = ProviderRef::new("task-origin-test", "filesystem", "2")?;
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
            "child",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([71; 16])),
        )?;
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
        host.claim_fork_allocation(claim.clone()).await?;
        let mut recovery = claim.clone();
        if let Some(origin) = &mut recovery.task_origin {
            origin.creation_fence = crate::scheduler::LeaseFence {
                reservation_id: "recovered".into(),
                placement: "later-worker".into(),
            };
        }
        host.claim_fork_allocation(recovery).await?;
        let journal = allocation_ref(host.provider.clone(), &volume)?;
        let retained = read_record::<_, _, AllocationClaim>(&host, &journal, "/claim.json", 65_536)
            .await?
            .ok_or_else(|| Error::NotFound("retained allocation".into()))?;
        assert_eq!(retained, claim);
        // Decode the actual retained provider record through the same audited
        // codec used by the finite allocation verifier, not a Value heap label.
        let bytes = host.read(&journal,None,"/claim.json",65_536).await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::WorkBudget {
            source_bytes_read: 65_536,items_examined: 4_096,
            peak_allocation_bytes: 65_536,..Default::default()
        };
        let io = PreparationIo::from_budget(budget,&cancellation)?;
        let raw_hold = io.reserve_decode(bytes.len() as u64)?;
        let decoded: AllocationClaim = io.decode_json(&bytes)?;
        assert_eq!(decoded,claim);
        let owned = bounded_io::UploadRecord::logical_owned_bytes(&decoded)?;
        assert_eq!(io.owned_bytes()?,bytes.len() as u64+owned);
        drop(decoded);
        io.release(owned)?;
        drop(raw_hold);
        assert_eq!(io.owned_bytes()?,0);
        let denied = PreparationIo::from_budget(
            acyclic_fs::WorkBudget { peak_allocation_bytes:0,..budget },&cancellation,
        )?;
        assert!(denied.decode_json::<AllocationClaim>(&bytes).is_err());
        assert_eq!(denied.owned_bytes()?,0);
        #[cfg(not(target_arch = "wasm32"))]
        {
            let native_budget = acyclic_fs::WorkBudget {
                backend_read_operations: 65_536, backend_write_operations: 65_536,
                authority_records_read: 65_536, authority_bytes_read: 16_777_216,
                source_bytes_read: 16_777_216, object_bytes_read: 16_777_216,
                allocation_operations: 65_536, peak_allocation_bytes: 16_777_216,
                bytes_encoded: 16_777_216, bytes_copied: 16_777_216,
                bytes_hashed: 16_777_216, items_examined: 65_536,
                ..Default::default()
            };
            let native = acyclic_fs::NativeMaterializationWork::new(native_budget, &cancellation)
                .expect("original production journal ledger");
            let baseline = native.live_allocation_bytes();
            let native_io = PreparationIo::from_native_work(&native)?;
            // Read the real retained journal on the original Filesystem engine.
            // Cold read work is a lower bound, never an exact reconstructed bill.
            let decoded = read_original_fork_claim_record_io(
                &host, &journal, 65_536, Some(&native_io)).await?
                .expect("original retained allocation");
            assert_eq!(decoded, claim);
            let held = native_io.owned_bytes()?;
            let custody = native_io.into_native_custody(held)?;
            assert!(custody.matches_work(&native));
            assert!(custody.bytes() >= bounded_io::UploadRecord::logical_owned_bytes(&decoded)?);
            assert!(native.receipt().source_bytes_read > 0);
            assert_eq!(decoded.task_origin.as_ref().expect("original task origin").source_grants,
                claim.task_origin.as_ref().expect("original donor").source_grants);
            drop(decoded);
            drop(custody);
            assert_eq!(native.live_allocation_bytes(), baseline);

            for cancelled in [false, true] {
                let token = acyclic_fs::CancellationToken::new();
                let denied_budget = if cancelled { native_budget } else {
                    acyclic_fs::WorkBudget { peak_allocation_bytes:
                        acyclic_fs::NativeMaterializationWork::accounting_state_bytes(), ..native_budget }
                };
                let denied_native = acyclic_fs::NativeMaterializationWork::new(denied_budget, &token)
                    .expect("original denied decode ledger");
                let denied_io = PreparationIo::from_native_work(&denied_native)?;
                let prefix = denied_native.receipt();
                if cancelled { token.cancel(); }
                assert!(matches!(denied_io.decode_json::<AllocationClaim>(&bytes),
                    Err(Error::NativeWork { .. })));
                assert_eq!(denied_native.receipt().allocation_operations, prefix.allocation_operations);
                assert_eq!(denied_native.receipt().bytes_copied, prefix.bytes_copied);
                assert_eq!(denied_io.owned_bytes()?, 0);
            }
        }
        let mut malformed: serde_json::Value = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Storage(error.to_string()))?;
        malformed.as_object_mut().ok_or_else(|| Error::Invalid("claim object".into()))?
            .remove("role");
        let malformed = encode_record(&malformed,65_536)?;
        let rejected = PreparationIo::from_budget(budget,&cancellation)?;
        assert!(rejected.decode_json::<AllocationClaim>(&malformed).is_err());
        assert_eq!(rejected.owned_bytes()?,0);
        for role in [ForkAllocationRole::Project,ForkAllocationRole::Result] {
            let mut changed = claim.clone();
            changed.role = role;
            assert!(matches!(host.claim_fork_allocation(changed).await,
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
                host.claim_fork_allocation(changed).await,
                Err(Error::Conflict(_))
            ));
        }
        let mut raw = claim.clone();
        raw.task_origin = None;
        assert!(matches!(
            host.claim_fork_allocation(raw).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(
            read_record::<_, _, AllocationClaim>(&host, &journal, "/claim.json", 65_536)
                .await?
                .ok_or_else(|| Error::NotFound("retained allocation".into()))?,
            claim
        );
        Ok(())
    }
}
