//! Owner-bound Stream coordinator adapter for typed durable task admission.

use crate::contract::capability;
use crate::{
    Admission, BatchId, Capabilities, EffectId, Error, IdempotencyKey, InteractionId, OperationId, Outcome,
    Result, TaskId,
    conversation::{ContentResidencyVerifier, FileRef},
    core::{Authority, AuthorityVerifier, Scope},
    distributed::{DistributedCoordinator, SchedulerPayloadStore},
    registry::ComponentIdentity,
    runtime::{
        BatchCancellationReport, BatchCancellationStatus, DurableBatchRequest,
        DurableEffectObserver, DurableTaskHost, InputKey, RuntimeScope, TaskAdmissionRecord, TaskStateProvider,
        TaskChildrenPage, TaskRegistry, ToolPolicy, read_granted,
        require_descendant_grant, validate_policy_identity, validate_task_schemas,
    },
    scheduler::{
        DurableOwner, EntrypointRef, InboxItem, OperationSpec, Orchestration, ParentLink,
        ResourceRequest,
    },
    workflow::MachineRegistry,
};
#[cfg(test)]
use crate::{executor::ExecutionJournal, interaction::Interaction};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamKey, StreamError, StreamProvider,
    UnixMillisClock,
};
use acyclic_stream::OwnedBytes;
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::Mutex;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::store::history::HistoryIo as _;
#[cfg(feature = "filesystem")]
use crate::scheduler::LeaseFence;

mod headless;
pub use headless::TaskVolumeSnapshot;
#[cfg(feature = "filesystem")]
mod source_authorization;
mod mail;
pub(crate) use mail::received_copy_operation;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MailEvent {
    sender: TaskId,
    recipient: TaskId,
    message_id: OperationId,
    schema_revision: u32,
    route_revision: u32,
    payload: FileRef,
}

const MAIL_SCHEMA_REVISION: u32 = 1;
const MAIL_ROUTE_REVISION: u32 = 1;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MailLocation {
    sender: TaskId,
    message_id: OperationId,
    recipient: TaskId,
    schema_revision: u32,
    route_revision: u32,
    sequence: u64,
    value_digest: [u8; 32],
}

const MAX_MAIL_LOCATION_BYTES: usize = 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimerEvent {
    task_id: TaskId,
    operation_id: OperationId,
    deadline_unix_ms: u64,
}

#[derive(Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchCancellation {
    batch_id: BatchId,
    group_id: crate::GroupId,
}

/// Derives the canonical interaction identity for one task's original operation.
#[must_use]
pub fn task_interaction_id(task_id: TaskId, operation_id: OperationId) -> InteractionId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness/v2/task-interaction\0");
    hasher.update(&task_id.into_bytes());
    hasher.update(&operation_id.into_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    InteractionId::from_bytes(bytes)
}

/// Derives the same original ticket identity after funding the actual SDK hash.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub fn task_interaction_id_with_native_work(
    task_id: TaskId, operation_id: OperationId,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<InteractionId> {
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: b"harness/v2/task-interaction\0".len() as u64 + 32,
        ..Default::default()
    })?;
    Ok(task_interaction_id(task_id, operation_id))
}

fn worker_key_with_work(
    kind: &str, operation: OperationId, fence: &crate::scheduler::LeaseFence,
    revision: u64, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<IdempotencyKey> {
    let digest = crate::contract::canonical_json_digest_with_work(fence, acyclic_stream::MAX_RECORD_BYTES as u64, work)?;
    let mut key = String::new();
    if let Some(io) = work.as_deref_mut() {
        let digits = revision.checked_ilog10().map_or(1, |digits| digits + 1) as usize;
        let length = 115 + kind.len() + digits;
        io.admit_copy(length as u64)?;
        io.reserve_text(&mut key, length)?;
        io.copy_bytes(length as u64)?;
    }
    use std::fmt::Write as _;
    write!(&mut key, "task-worker:{kind}:{operation}:{}:{revision}",
        blake3::Hash::from_bytes(digest).to_hex()).expect("writing into String");
    IdempotencyKey::new(key)
}

fn host_fence_with_work(
    fence: &crate::scheduler::LeaseFence,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<crate::scheduler::LeaseFence> {
    match work.as_deref_mut() {
        Some(io) => {
            io.retain_owned(std::mem::size_of::<crate::scheduler::LeaseFence>() as u64)?;
            Ok(crate::scheduler::LeaseFence {
                reservation_id: io.clone_text(&fence.reservation_id)?,
                placement: io.clone_text(&fence.placement)?,
            })
        },
        None => Ok(fence.clone()),
    }
}

fn admission_boundary(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    code: crate::wire::ErrorCode, reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().and_then(|io| io.original_native_work()).is_some() {
        return Error::NativeBoundary { code, reason };
    }
    match code {
        crate::wire::ErrorCode::Storage => Error::Storage(reason.into()),
        crate::wire::ErrorCode::NotFound => Error::NotFound(reason.into()),
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}

/// Stream-backed admission and observation for one durable owner.
pub struct CoordinatorTaskHost<P> {
    coordinator: Arc<Mutex<DistributedCoordinator<P>>>,
    stream: std::sync::Arc<P>,
    payloads: Arc<dyn SchedulerPayloadStore>,
    reader: Arc<dyn ContentResidencyVerifier>,
    owner_scope: Scope,
    verifier: AuthorityVerifier,
    root_scope: RuntimeScope,
    tasks: TaskRegistry,
    machines: MachineRegistry,
    clock: Arc<dyn UnixMillisClock>,
    policy: Option<Arc<dyn ToolPolicy>>,
    policy_binding: Option<ComponentIdentity>,
    effects: Option<Arc<dyn DurableEffectObserver>>,
    session_limits: Option<crate::scheduler::SessionLimits>,
    resource_creation: Vec<Arc<dyn crate::resource_preparation::ResourceCreationProvider>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_configuration_custody: Option<acyclic_fs::NativeAllocation>,
}

struct CreatedTaskPublicationLease<P> {
    coordinator: Arc<Mutex<DistributedCoordinator<P>>>,
    scope: Scope,
    verifier: AuthorityVerifier,
    operation: OperationId,
    fence: Option<crate::scheduler::LeaseFence>,
    clock: Arc<dyn UnixMillisClock>,
    deadline_epoch_ms: Option<u64>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl<P: StreamProvider> CreatedTaskPublicationLease<P> {
    async fn verify_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let operation = coordinator.observe_operation(
            self.verifier.audience(), &self.scope, &self.verifier, self.operation, work,
        )?;
        let fence = self.fence.as_ref().ok_or_else(|| admission_boundary(
            work, crate::wire::ErrorCode::Conflict, "created resource task has no current execution owner",
        ))?;
        crate::scheduler::require_execution_owner(operation, fence, false, work)?;
        if self.deadline_epoch_ms.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline) {
            let mut text = String::new();
            if let Some(io) = work.as_deref_mut() {
                io.admit_copy(48)?;
                io.reserve_text(&mut text, 48)?;
                io.copy_bytes(48)?;
            }
            use std::fmt::Write as _;
            write!(&mut text, "task-cancel:{}", self.operation).expect("writing into String");
            coordinator.cancel_operation(
                self.verifier.audience(), &self.scope, &self.verifier, self.operation,
                IdempotencyKey::new(text)?, true, work,
            ).await?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().and_then(|io| io.original_native_work()).is_some() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "created resource task reached its original execution deadline" });
            }
            return Err(Error::Indeterminate(self.operation));
        }
        Ok(())
    }
}

impl<P: StreamProvider> crate::resource_preparation::CreatedTaskLease
    for CreatedTaskPublicationLease<P>
{
    fn verify(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.verify_with_work(&mut None).await })
    }
    fn fence(&self) -> Result<crate::scheduler::LeaseFence> {
        self.fence.clone().ok_or_else(|| {
            Error::Conflict("created resource task has no execution reservation".into())
        })
    }
    #[cfg(feature = "filesystem")]
    fn verify_with_io<'a>(
        &'a self, io: &'a crate::filesystem::PreparationIo,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, ()>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            return self.verify_with_native_work(work)
                .map(crate::store::history::HistoryContentFuture::Native);
        }
        io.history_future(|| async move {
            let mut observer = io.history_io();
            self.verify_with_work(&mut Some(&mut observer)).await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_with_native_work<'a>(
        &'a self, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        if !self._native_custody.as_ref().is_some_and(|custody| custody.matches_work(work)) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "created publication lease differs from its original SDK metadata ledger" });
        }
        crate::NativeFutureBundle::new(work, || async move {
            let mut io = crate::store::history::native_work_io(work)?;
            self.verify_with_work(&mut Some(&mut io)).await
        })
    }

}

/// Original root binding cache, shared by its issued read witnesses.
#[cfg(feature = "filesystem")]
pub struct RootBootstrapBindingWitness {
    proof: std::sync::OnceLock<RetainedRootBinding>,
    gate: tokio::sync::Mutex<()>,
    pending: std::sync::OnceLock<(OperationId,[u8;32],[u8;32])>,
}

#[cfg(feature = "filesystem")]
struct RetainedRootBinding {
    proof: crate::store::VerifiedRootBinding,
    #[cfg(not(target_arch = "wasm32"))]
    _custody: Option<acyclic_fs::NativeAllocation>,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl RootBootstrapBindingWitness {
    pub(crate) fn binding(&self) -> &crate::store::VerifiedRootBinding {
        &self.proof.get().expect("issued canonical root binding").proof
    }
}

/// Canonical admission reader for protected preparation bootstrap.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub struct TaskAdmissionReadonlyWitness<P> {
    host: Arc<CoordinatorTaskHost<P>>,
    admission: TaskAdmissionRecord,
    scratch: Option<crate::resource_preparation::ChildScratchCreationProof>,
    _native_custody: acyclic_fs::NativeAllocation,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl<P: StreamProvider> TaskAdmissionReadonlyWitness<P> {
    pub(crate) const fn task(&self) -> TaskId { TaskId::from_bytes(self.admission.operation_id.into_bytes()) }
    pub(crate) const fn admission_operation(&self) -> OperationId {
        self.admission.operation_id
    }
    pub(crate) fn prepared_stream(&self) -> &std::sync::Arc<P> { &self.host.stream }
    pub(crate) fn scratch_proof(
        &self,
    ) -> Option<&crate::resource_preparation::ChildScratchCreationProof> {
        self.scratch.as_ref()
    }
    pub(crate) async fn verify_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        if !self._native_custody.matches_work(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "preparation admission differs from its original metadata ledger",
            });
        }
        let mut io = crate::store::history::native_work_io(work)?;
        let observed = self.host.admission_with_work(
            self.admission_operation(), &mut Some(&mut io),
        ).await?;
        let matches = observed == self.admission;
        drop(observed);
        if !matches {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "preparation witness differs from its canonical task admission",
            });
        }
        Ok(())
    }

    fn require_query_with_native_work(
        &self, query: &Arc<crate::runtime::NativeTaskContext>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.host.require_preparation_query_with_native_work(query, work)?;
        let context = query.context();
        let scope = context.scope();
        if !self._native_custody.matches_work(work)
            || context.durable_task_id() != Some(self.task())
            || context.id() != self.admission.operation_id
            || scope.grants() != &self.admission.grants
            || scope.limits() != self.admission.limits
            || scope.resource_proofs() != self.admission.resource_proofs
            || scope.extensions() != self.admission.extensions.as_ref()
            || scope.run_limits() != self.admission.run_limits
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "preparation query differs from its original witnessed admission" });
        }
        Ok(())
    }

    pub(crate) async fn verify_query_with_native_work(
        &self, query: &Arc<crate::runtime::NativeTaskContext>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.require_query_with_native_work(query, work)?;
        self.verify_with_native_work(work).await?;
        self.require_query_with_native_work(query, work)
    }

    pub(crate) fn original_preparation_request_with_native_work<'a>(
        &'a self, query: &'a Arc<crate::runtime::NativeTaskContext>,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (
        crate::resource_preparation::ResourcePreparationRequest,
        acyclic_fs::NativeAllocation,
    )>> {
        self.require_query_with_native_work(query, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.verify_query_with_native_work(query, work).await?;
            let mut io = crate::store::history::native_work_io(work)?;
            let mut observer = Some(&mut io as &mut dyn crate::store::history::HistoryIo);
            let mut encoded_quota = crate::wire_codec::CodecQuota::new(&mut observer, 0)?;
            let encoded = crate::store::history::canonical_bytes_with_work(
                &self.admission.input, acyclic_stream::MAX_RECORD_BYTES as u64,
                encoded_quota.work(),
            )?;
            encoded_quota.adopt(encoded.capacity() as u64)?;
            let decode = crate::filesystem::PreparationIo::from_native_work(work)?;
            let pair = crate::filesystem::native_decoded(decode.decode(std::marker::PhantomData::<crate::resource_preparation::ResourcePreparationRequest>, &encoded, crate::wire::ErrorCode::Unauthorized)?)?;
            drop(encoded);
            drop(encoded_quota);
            drop(observer);
            drop(io);
            if let Err(error) = self.verify_query_with_native_work(query, work).await {
                let (request, custody) = pair;
                drop(request);
                return Err(match error {
                    Error::NativeWork { cause, custody: None } =>
                        Error::NativeWork { cause, custody: Some(custody) },
                    error => error,
                });
            }
            Ok(pair)
        })
    }

    pub(crate) fn verify_preparation_origin_with_native_work(
        &self, query: &Arc<crate::runtime::NativeTaskContext>,
        request: &crate::resource_preparation::ResourcePreparationRequest,
        origin: &crate::resource_preparation::ResourceCreationOrigin,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.require_query_with_native_work(query, work)?;
        let mut io = crate::store::history::native_work_io(work)?;
        let mut observer = Some(&mut io as &mut dyn crate::store::history::HistoryIo);
        request.validate(&mut observer)?;
        let admission_digest = self.admission.canonical_digest_with_work(&mut observer)?;
        let request_digest = crate::contract::canonical_json_digest_with_work(
            &self.admission.input, acyclic_stream::MAX_RECORD_BYTES as u64, &mut observer)?;
        let supplied_digest = crate::contract::canonical_json_digest_with_work(
            request, acyclic_stream::MAX_RECORD_BYTES as u64, &mut observer)?;
        if &origin.owner != self.host.verifier.audience() || origin.preparation_task != self.task()
            || self.admission.parent != Some(origin.parent_task)
            || request.child.parent != Some(origin.parent_task)
            || origin.intended_child != request.child.operation_id
            || origin.admission_digest != admission_digest
            || origin.source_grants != self.admission.grants
            || origin.request_digest != request_digest || supplied_digest != request_digest
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "protected fork origin differs from its original preparation admission" });
        }
        Ok(())
    }
}

/// One worker's exact task lease, bound to the coordinator's own Stream provider.
#[cfg(feature = "filesystem")]
pub struct TaskJournalOwner<P> {
    host: Arc<CoordinatorTaskHost<P>>,
    stream: std::sync::Arc<P>,
    task_id: TaskId,
    fence: crate::scheduler::LeaseFence,
    original_admission_digest: [u8; 32],
    workflow: crate::workflow::WorkflowAdmission,
    output_schema: Value,
    input_grants: crate::Capabilities,
    input_created: Option<Arc<crate::resource_preparation::CreatedResourceAccess>>,
    input_agent: Option<crate::AgentId>,
    root_binding: Option<Arc<RootBootstrapBindingWitness>>,
    deadline_epoch_ms: Option<u64>,
    input_limits: crate::conversation::Limits,
    extensions: Option<crate::core::ExtensionAdmission>,
    extension_schemas: Option<crate::core::SchemaRegistry>,
    extension_runtime: Option<Arc<crate::extension::ExtensionRuntime>>,
    invocation_cancellation: Option<acyclic_fs::CancellationToken>,
    #[cfg(not(target_arch = "wasm32"))]
    readonly_original: Option<(OperationSpec, LeaseFence)>,
    #[cfg(not(target_arch = "wasm32"))]
    _native_shared: bool,
    #[cfg(not(target_arch = "wasm32"))]
    _native_custody: Option<acyclic_fs::NativeAllocation>,
}

#[cfg(feature = "filesystem")]
impl<P: StreamProvider> TaskJournalOwner<P> {
    /// Returns the original task identity without exposing its lease or signer.
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub(crate) fn require_mail_scope(
        &self,
        scope: &RuntimeScope,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
            observer.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
        }
        crate::core::source_metadata_inspection_with_work(
            &(scope.grants(), &self.input_grants), work,
        )?;
        if scope.grants() != &self.input_grants || scope.limits() != self.input_limits {
            #[cfg(not(target_arch = "wasm32"))]
            if work.as_ref().is_some_and(|observer| observer.original_native_work().is_some()) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "mailbox scope differs from retained admission",
                });
            }
            return Err(Error::Unauthorized("mailbox scope differs from retained admission".into()));
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn require_mail_scope_with_native_work(
        &self, scope: &RuntimeScope, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.charge(acyclic_fs::WorkCounters::default())?;
        if !self._native_custody.as_ref().is_some_and(|custody|
            custody.matches_work(work))
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "mailbox scope differs from its original native admission" });
        }
        let mut observer = crate::store::history::native_work_io(work)?;
        self.require_mail_scope(scope, &mut Some(&mut observer))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn require_task_context_binding_with_native_work(
        &self, context: &crate::runtime::TaskContext,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        context.require_original_native_work(work)?;
        if !self._native_custody.as_ref().is_some_and(|custody| custody.matches_work(work)) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native read owner differs from its original admission ledger" });
        }
        let mut observer = crate::store::history::native_work_io(work)?;
        if context.durable_task_id() != Some(self.task_id)
            || context.id() != self.operation_id()
            || !context.original_host_binding_matches(self.host.as_ref(), self.host.policy.as_deref())
            || context.scope().limits() != self.input_limits
            || context.scope().run_limits().deadline_epoch_ms != self.deadline_epoch_ms
            || {
                let proofs = self.input_created.as_ref().map_or(&[][..], |access| access.proofs.as_slice());
                crate::core::source_metadata_inspection_with_work(
                    &(context.scope().grants(), &self.input_grants,
                        context.scope().resource_proofs(), proofs,
                        context.scope().extensions(), self.extensions.as_ref()),
                    &mut Some(&mut observer),
                )?;
                context.scope().grants() != &self.input_grants
                    || context.scope().resource_proofs() != proofs
                    || context.scope().extensions() != self.extensions.as_ref()
            }
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native read context differs from its original SDK owner admission" });
        }
        Ok(())
    }

    pub(crate) fn matches_task_binding(
        &self, task: TaskId, fence: &crate::scheduler::LeaseFence,
    ) -> bool {
        self.task_id == task && &self.fence == fence
    }

    pub(crate) fn original_host_binding_matches(
        &self,
        host: &CoordinatorTaskHost<P>,
    ) -> bool {
        Arc::ptr_eq(&self.host.coordinator, &host.coordinator)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn same_original_root_output_owner(&self, other: &Self) -> bool {
        self.root_binding.is_some() && other.root_binding.is_some()
            && self.readonly_original.is_none() && other.readonly_original.is_none()
            && Arc::ptr_eq(&self.host.coordinator, &other.host.coordinator)
            && self.task_id == other.task_id
            && self.workflow.request_digest == other.workflow.request_digest
            && self.fence == other.fence
            && self.input_agent == other.input_agent
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_owned_bytes(&self) -> Result<u64> {
        let mut bytes = std::mem::size_of::<Self>() as u64;
        let mut add = |owned: u64| -> Result<()> {
            bytes = bytes.checked_add(owned).ok_or(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow),
                custody: None,
            })?;
            Ok(())
        };
        for text in [
            &self.fence.reservation_id, &self.fence.placement,
            &self.workflow.initial.machine.name, &self.workflow.initial.machine.version,
        ] {
            add(text.capacity() as u64)?;
        }
        add(crate::core::value_owned_bytes(&self.workflow.initial.state)?)?;
        add(crate::core::value_owned_bytes(&self.output_schema)?)?;
        add(self.input_grants.logical_owned_bytes()?)?;
        if let Some(extensions) = &self.extensions { add(extensions.logical_owned_bytes()?)?; }
        if self.root_binding.is_some() { add(std::mem::size_of::<RootBootstrapBindingWitness>() as u64)?; }
        if let Some(original) = &self.readonly_original {
            add(original.0.logical_owned_bytes()?)?;
            add(original.1.reservation_id.capacity() as u64)?;
            add(original.1.placement.capacity() as u64)?;
        }
        Ok(bytes)
    }
    pub(crate) const fn input_limits(&self) -> crate::conversation::Limits {
        self.input_limits
    }

    /// Verifies only a kernel-authenticated created-volume input.
    pub(crate) async fn verify_created_input_with_io(
        &self, file: &FileRef, io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<bool> {
        self.validate_input_file(file, io)?;
        let mut reader = None;
        if let Some(access) = &self.input_created {
            for binding in &access.references {
                if let Some(io) = io {
                    let step = acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() };
                    io.preflight(step)?;
                    io.charge(step)?;
                }
                if &binding.file == file {
                    reader = Some(&binding.reader);
                    break;
                }
            }
            if reader.is_none() {
                for binding in &access.content {
                    if let Some(io) = io {
                        let step = acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() };
                        io.preflight(step)?;
                        io.charge(step)?;
                    }
                    if &binding.volume == file.volume() {
                        reader = Some(&binding.binding.reader);
                        break;
                    }
                }
            }
        }
        let Some(reader) = reader else { return Ok(false); };
        let bytes_hold;
        let bytes = match io {
            Some(io) => {
                let mut borrowed = io.history_io();
                self.verify(true, &mut Some(&mut borrowed)).await?;
                let (bytes, retained) = crate::store::history::HistoryIo::read_content(
                    &mut borrowed, reader.as_ref(), file)?.await?;
                bytes_hold = Some(io.guard_owned(retained)?);
                crate::store::history::HistoryIo::hashed_bytes(&mut borrowed, bytes.len() as u64)?;
                bytes
            }
            None => {
                bytes_hold = None;
                self.verify(true, &mut None).await?;
                acyclic_stream::OwnedBytes::from(reader.read(file, file.descriptor().byte_length(), None)?.await?)
            }
        };
        file.descriptor().verify(&bytes, &mut None)?;
        match io {
            Some(io) => {
                let mut borrowed = io.history_io();
                self.verify(true, &mut Some(&mut borrowed)).await?;
            }
            None => self.verify(true, &mut None).await?,
        }
        drop(bytes);
        drop(bytes_hold);
        Ok(true)
    }

    pub(crate) fn validate_native_configuration(
        &self, binding: &crate::model::NativeConfigurationBinding,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<(&crate::core::SchemaRegistry, [u8; 32])> {
        let (schemas, expected) = binding.verify_original(
            self.extensions.as_ref(), self.extension_schemas.as_ref(),
            self.extension_runtime.as_deref(), io)?;
        let file = &binding.configuration.content;
        self.validate_input_file(file, io)?;
        if file.descriptor().byte_length() > self.input_limits.render_bytes {
            return Err(Error::Invalid("native options exceed render byte limit".into()));
        }
        Ok((schemas, expected))
    }

    pub(crate) async fn verify_native_configuration(
        &self, binding: &crate::model::NativeConfigurationBinding,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<()> {
        let (schemas, expected) = self.validate_native_configuration(binding, io)?;
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = history.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
        let (bytes, retained) = match quota.work().as_deref_mut() {
            Some(observer) => crate::store::history::read_content_with_work(
                observer, self.host.reader.as_ref(), &binding.configuration.content).await?,
            None => (self.host.reader.read(&binding.configuration.content, self.input_limits.render_bytes, None)?.await?, 0),
        };
        quota.adopt(retained)?;
        match io {
            Some(io) => schemas.verify_configuration_binding(
                &binding.configuration, expected, &bytes, self.input_limits, Some(io)),
            None => schemas.verify_configuration_binding(
                &binding.configuration, expected, &bytes, self.input_limits, None),
        }
    }

    pub(crate) const fn input_grants(&self) -> &crate::Capabilities {
        &self.input_grants
    }

    /// Checks the original cached grant with bounded canonical identity work.
    pub fn require_volume_grant_measured(
        &self,
        volume: &crate::conversation::VolumeRef,
        operation: crate::conversation::VolumeOperation,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error> {
        let mut work = crate::store::history::HistoryWork::new(budget, cancellation);
        let result = work.preflight().and_then(|()|
            self.require_volume_grant(volume, operation, &mut Some(&mut work)));
        work.finish(result)
    }

    pub(crate) fn require_volume_grant(
        &self, volume: &crate::conversation::VolumeRef,
        operation: crate::conversation::VolumeOperation,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        #[cfg(not(target_arch = "wasm32"))]
        if self.readonly_original.is_some() && operation == crate::conversation::VolumeOperation::Write {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized, "historical READ owner cannot authorize namespace WRITE"));
        }
        volume.validate(work)?;
        let observed = work.is_some();
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let digest = {
            let encoded = crate::store::history::canonical_bytes_with_work(
                volume, acyclic_stream::MAX_RECORD_BYTES as u64, quota.work())?;
            quota.adopt(encoded.capacity() as u64)?;
            if let Some(io) = quota.work().as_deref_mut() { io.hashed_bytes(encoded.len() as u64)?; }
            blake3::hash(&encoded)
        };
        quota.relinquish(quota.retained())?;
        if let Some(io) = quota.work().as_deref_mut() {
            io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 64, items: 0 })?;
        }
        let hex = digest.to_hex();
        let prefix: &[u8] = match operation {
            crate::conversation::VolumeOperation::Read => b"volume:read:",
            crate::conversation::VolumeOperation::Write => b"volume:write:",
        };
        if let Some(io) = quota.work().as_deref_mut() {
            io.codec_charge(crate::contract::JsonCodecCharge::Copy { bytes: (prefix.len() + hex.len()) as u64 })?;
        }
        let mut grant = [0; 77];
        grant[..prefix.len()].copy_from_slice(prefix);
        grant[prefix.len()..prefix.len() + hex.len()].copy_from_slice(hex.as_bytes());
        let grant = std::str::from_utf8(&grant[..prefix.len() + hex.len()])
            .expect("volume grant is ASCII");
        if !self.input_grants.contains(grant)
            && !(observed && self.input_created.as_ref().is_some_and(|access| access.grants.contains(grant))) {
            return Err(admission_boundary(quota.work(), crate::wire::ErrorCode::Unauthorized, "task scope lacks required volume grant"));
        }
        Ok(())
    }

    /// Obtains the original currently selected scratch allocation.
    pub fn child_scratch_creation_proof(&self) -> Result<crate::resource_preparation::ChildScratchCreationProof> {
        let access = self.input_created.as_ref()
            .ok_or_else(|| Error::Unauthorized("task has no created scratch allocation".into()))?;
        let binding = access.current_content(&mut None)?
            .ok_or_else(|| Error::Unauthorized("task has no current scratch allocation".into()))?;
        let proof = binding.scratch_creation.as_ref()
            .ok_or_else(|| Error::Unauthorized("current scratch is not a verified filesystem allocation".into()))?;
        if proof.original_task() != self.task_id || Some(proof.agent()) != self.input_agent
            || proof.volume() != &binding.volume
        {
            return Err(Error::Unauthorized("scratch allocation belongs to another original task".into()));
        }
        Ok(proof.clone())
    }

    pub(crate) fn require_interaction_grant(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if !self.input_grants.contains(capability::INTERACTION_ROUTE) {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized, "task scope lacks interaction:route"));
        }
        Ok(())
    }

    pub(crate) fn task_binding(&self) -> (TaskId, &crate::scheduler::LeaseFence) {
        (self.task_id, &self.fence)
    }

    pub(crate) async fn suspend_quiescent_execution_with_work(
        &self,
        execution: OperationId,
        tail: u64,
        revision: u64,
        command: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.host.suspend_workflow(
            self.task_id,
            &self.fence,
            revision,
            Some(command),
            Some((execution, tail)),
            None,
            work,
        ).await
    }
    /// Consumes only the original journal's sealed current Pending witness and settled execution tail.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn release_quiescent_execution_with_native_work(
        &self, execution: OperationId, tail: u64,
        pending: &crate::filesystem::WorkflowReleaseWitness<'_>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        let mut io = crate::store::history::native_work_io(work)?;
        if pending.condition().0 != (self.task_id,&self.fence) {
            return Err(Error::Unauthorized("pending workflow belongs to another task or fence".into()));
        }
        let mut coordinator = self.host.coordinator.lock().await;
        coordinator.refresh(&mut Some(&mut io)).await?;
        let operation = coordinator.observe_operation(
            self.host.verifier.audience(),&self.host.owner_scope,&self.host.verifier,
            self.operation_id(),&mut Some(&mut io),
        )?;
        crate::scheduler::require_execution_owner(operation, &self.fence, true, &mut None)?;
        if operation.cancellation_requested {
            return Err(Error::Conflict("read-only close cannot settle task cancellation".into()));
        }
        if operation.phase == crate::scheduler::OperationPhase::Reconciling {
            return Ok(false);
        }
        let revision = operation.revision;
        let remaining = crate::store::history::HistoryIo::remaining(&io)?;
        let encoded = crate::contract::canonical_json_bytes_bounded(
            &self.fence,crate::contract::JsonBounds {
                maximum_bytes:remaining.bytes_encoded,maximum_items:remaining.items_examined,
                maximum_depth:256,
            },None, &mut |charge| crate::store::history::HistoryIo::codec_charge(&mut io,charge),
        )?;
        crate::store::history::HistoryIo::hashed_bytes(&mut io,encoded.len() as u64)?;
        let digest = blake3::hash(&encoded);
        let encoded_length = encoded.len() as u64;
        drop(encoded);
        crate::store::history::HistoryIo::release_owned(&mut io,encoded_length)?;
        let mut key = String::new();
        let digits = if revision == 0 {1} else {revision.ilog10() as usize+1};
        crate::store::history::HistoryIo::reserve_text(&mut io,&mut key,122+digits)?;
        use std::fmt::Write as _;
        write!(&mut key,"task-worker:release:{}:{}:{revision}",self.operation_id(),digest.to_hex())
            .map_err(|_| Error::Invalid("lease release key formatting failed".into()))?;
        let fence = crate::scheduler::LeaseFence {
            reservation_id:crate::store::history::HistoryIo::clone_text(&mut io,&self.fence.reservation_id)?,
            placement:crate::store::history::HistoryIo::clone_text(&mut io,&self.fence.placement)?,
        };
        coordinator.apply_if_execution_quiescent(
            IdempotencyKey::new(key)?,
            crate::scheduler::SchedulerEvent::LeaseReleased {operation_id:self.operation_id(),fence},
            execution,tail,Some(pending.condition().1),&mut Some(&mut io),
        ).await?;
        Ok(true)

    }

    pub(crate) fn validate_storage(
        &self, verifier: &AuthorityVerifier, maximum_payload_bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if verifier.audience() != self.host.verifier.audience()
            || maximum_payload_bytes > self.input_limits.file_bytes {
            return Err(admission_boundary(
                work, crate::wire::ErrorCode::Unauthorized,
                "journal storage exceeds admitted task authority or limits",
            ));
        }
        Ok(())
    }

    /// Returns the initial workflow identity derived from the retained task admission and its registered machine, rather than caller-selected state.
    #[must_use]
    pub const fn workflow_admission(&self) -> &crate::workflow::WorkflowAdmission {
        &self.workflow
    }

    pub(crate) fn invocation_cancellation(&self) -> Option<&acyclic_fs::CancellationToken> {
        self.invocation_cancellation.as_ref()
    }

    /// Binds this already-sealed owner to the caller's actual live operation token.
    pub fn with_invocation_cancellation(mut self, cancellation: &acyclic_fs::CancellationToken) -> Self {
        self.invocation_cancellation = Some(cancellation.clone());
        self
    }

    /// Derives a tool-effect workflow from this same original task reservation.
    pub(crate) async fn for_tool_workflow(
        mut self,
        context: &crate::runtime::ToolContext,
        tool: &crate::durable_tool::ResumableTool,
        invocation: &crate::tool::ToolInvocation,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<Self> {
        self.invocation_cancellation = Some(cancellation.clone());
        if context.task().durable_task_id() != Some(self.task_id)
            || context.task().owned_task_fence()? != self.fence
            || invocation.operation_id == self.operation_id()
        {
            return Err(Error::Unauthorized("tool workflow differs from original task owner".into()));
        }
        self.require_mail_scope(context.task().scope(), &mut None)?;
        let admission = self.host.observe_admission(self.task_id).await?;
        if context.task().scope().run_limits() != admission.run_limits
            || context.task().scope().extensions() != admission.extensions.as_ref()
        {
            return Err(Error::Unauthorized("tool workflow changed original admission policy".into()));
        }
        self.verify(false, &mut None).await?;
        self.workflow = tool.workflow_admission(context, invocation)?;
        self.output_schema = tool.definition.output_schema.clone();
        Ok(self)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn require_native_workflow_validation(&self) -> Result<()> {
        self.host.machines.require_native_work(&self.workflow.initial.machine)
    }

    pub(crate) fn validate_workflow_record(
        &self, record: &crate::workflow::WorkflowRecord,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        let conflict = |reason: &'static str| {
            #[cfg(not(target_arch = "wasm32"))]
            if io.and_then(crate::executor::PreparationIo::original_native_work).is_some() {
                return Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason };
            }
            Error::Conflict(reason.into())
        };
        if record.prior.machine != self.workflow.initial.machine {
            return Err(conflict("workflow implementation differs from task admission"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let proof = if let Some(io) = io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "workflow proof requires original native work",
            })?;
            self.host.machines.require_native_work(&record.prior.machine)?;
            let mut history = io.history_io();
            let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
            let mut quota = crate::wire_codec::CodecQuota::new(&mut observer, 0)?;
            let input = crate::core::copy_value(&record.input, &mut quota)?;
            quota.transfer();
            let (next, transition, custody) = self.host.machines.step_with_native_work(
                &record.prior, input, work,
            )?;
            (next, transition, Some(custody))
        } else {
            let (next, transition) = self.host.machines.step(&record.prior, &record.input)?;
            (next, transition, None)
        };
        #[cfg(target_arch = "wasm32")]
        let proof = self.host.machines.step(&record.prior, &record.input)?;
        if proof.0 != record.next || proof.1 != record.transition {
            return Err(conflict("workflow transition differs from pinned machine"));
        }
        if let crate::workflow::MachineStatus::Completed { value } = &proof.1.status {
            if io.is_some() {
                crate::contract::validate_json_schema_value(
                    &self.output_schema, value, "task output", io,
                )?;
            } else {
                crate::tool::validate_value(&self.output_schema, value, "task output", None)?;
            }
        }
        Ok(())
    }

    pub(crate) fn stream(&self) -> std::sync::Arc<P> {
        self.stream.clone()
    }

    pub(crate) fn operation_id(&self) -> OperationId {
        OperationId::from_bytes(self.task_id.into_bytes())
    }

    pub(crate) async fn condition(
        &self,
        write: crate::distributed::JournalWrite,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<acyclic_stream::CommitCondition> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if self.verify_original_read_witness()? {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized, "historical READ custody cannot prepare a journal write"));
        }
        if !matches!(write, crate::distributed::JournalWrite::Settlement)
            && self.deadline_epoch_ms.is_some_and(|deadline| self.host.clock.now_unix_millis() >= deadline)
        {
            if let Some(cancellation) = &self.invocation_cancellation {
                cancellation.cancel();
            }
            if work.is_none() {
                self.host.expire_task_deadline(self.task_id, Some(self.fence.clone())).await?;
            }
            return Err(Error::Indeterminate(self.operation_id()));
        }
        let result = self.host
            .coordinator
            .lock()
            .await
            .journal_condition(
                self.host.verifier.audience(),
                &self.host.owner_scope,
                &self.host.verifier,
                self.operation_id(),
                &self.fence,
                write,
                work,
            )
            .await;
        if result.is_err() {
            if let Some(cancellation) = &self.invocation_cancellation {
                cancellation.cancel();
            }
        }
        result
    }

    pub(crate) async fn verify(
        &self, allow_settlement: bool,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let write = if allow_settlement { crate::distributed::JournalWrite::Settlement }
            else { crate::distributed::JournalWrite::Fresh };
        self.condition(write, work).await.map(|_| ())
    }

    /// Checks the original owner fence on the caller's same native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn verify_with_native_work(
        &self, allow_settlement: bool, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut io = crate::store::history::native_work_io(work)?;
        if !allow_settlement && let Some(access) = self.input_created.as_ref() {
            if let Some(binding) = access.current_content(&mut Some(&mut io))? {
                if binding.binding.writer.is_none() {
                    return Err(admission_boundary(&Some(&mut io), crate::wire::ErrorCode::Unauthorized, "current created scratch has no WRITE custody"));
                }
                self.require_volume_grant(&binding.volume, crate::conversation::VolumeOperation::Write, &mut Some(&mut io))?;
            }
        }
        self.verify(allow_settlement, &mut Some(&mut io)).await

    }

    /// READ-only verification of the original reservation.
    fn verify_original_read_witness(&self) -> Result<bool> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = &self.readonly_original {
            if original.0.operation_id != self.operation_id()
                || &original.1 != &self.fence {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "READ owner differs from its protected original reservation",
                });
            }
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) async fn verify_read_access(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if self.verify_original_read_witness()? { return Ok(()); }
        self.verify(true, work).await
    }

    /// Shares an already SDK-observed owner without copying its original policy or deep metadata.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(
        mut self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        let owned = self.native_owned_bytes()?;
        if !self._native_custody.as_ref().is_some_and(|custody|
            custody.matches_work(work) && custody.bytes() >= owned)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native owner sharing requires its SDK metadata custody",
            });
        }
        work.charge(acyclic_fs::WorkCounters { allocation_operations: 1, ..Default::default() })?;
        self._native_shared = true;
        Ok(Arc::new(self))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn native_shared_work_matches(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> bool {
        self._native_shared && self._native_custody.as_ref().is_some_and(|custody|
            custody.matches_work(work))
    }

    /// Authorizes exact retained volume custody on that same native-work ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn require_volume_grant_with_native_work(
        &self, volume: &crate::conversation::VolumeRef,
        operation: crate::conversation::VolumeOperation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut io = crate::store::history::native_work_io(work)?;
        if operation == crate::conversation::VolumeOperation::Write {
            if let Some(binding) = self.input_created.as_ref()
                .and_then(|access| access.content.iter().find(|binding| &binding.volume == volume))
            {
                if binding.binding.writer.is_none() {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        reason: "created volume has no retained WRITE custody",
                    });
                }
            }
        }
        self.require_volume_grant(volume, operation, &mut Some(&mut io))

    }

    /// Reads the original owned task's actual terminal result on the same native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn outcome_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<crate::Outcome<FileRef>>,acyclic_fs::NativeAllocation)> {
        let mut io = crate::store::history::native_work_io(work)?;
        let mut coordinator = self.host.coordinator.lock().await;
        coordinator.refresh(&mut Some(&mut io)).await?;
        let operation = coordinator.observe_operation(
            self.host.verifier.audience(), &self.host.owner_scope, &self.host.verifier,
            self.operation_id(), &mut Some(&mut io),
        )?;
        if operation.phase != crate::scheduler::OperationPhase::Terminal {
            return Ok((None, work.retain_owned(0)?));
        }
        let outcome = operation.outcome.as_ref()
            .ok_or_else(|| Error::Invalid("terminal original task lacks its outcome".into()))?;
        if matches!(outcome, crate::Outcome::Indeterminate { .. }) {
            return Err(Error::Invalid("uncertain task outcome cannot be terminal".into()));
        }
        let output = outcome.clone_with_work(&mut Some(&mut io))?;
        Ok((Some(output), io.into_custody()))
    }

    /// Checks fresh dispatch authority under the supplied work budget.
    pub async fn verify_dispatch_measured(
        &self,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error> {
        let mut work = crate::store::history::HistoryWork::new(budget, cancellation);
        let result = async {
            work.preflight()?;
            self.condition(crate::distributed::JournalWrite::Fresh, &mut Some(&mut work)).await.map(|_| ())
        }.await;
        work.finish(result)
    }

    /// Authenticates one immutable resource request under this ordinary task's current uncancelled lease.
    pub async fn resource_preparation(
        self: &Arc<Self>,
        request: crate::resource_preparation::ResourcePreparationRequest,
    ) -> Result<crate::resource_preparation::TaskResourcePreparation> {
        self.resource_preparation_with_work(request, &mut None).await
    }

    /// Consumes the genuine paid request on this original SDK-shared owner.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn resource_preparation_with_native_work<'a>(
        self: &'a Arc<Self>,
        request: (
            crate::resource_preparation::ResourcePreparationRequest,
            acyclic_fs::NativeAllocation,
        ),
        native: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, crate::resource_preparation::TaskResourcePreparation>> {
        if !self.native_shared_work_matches(native) || !request.1.matches_work(native) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "preparation request differs from its original native owner or custody" });
        }
        let required = (std::mem::size_of::<crate::resource_preparation::ResourcePreparationRequest>() as u64)
            .checked_add(request.0.logical_owned_bytes()?)
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "preparation request ownership overflows" })?;
        if request.1.bytes() < required
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "preparation request differs from its original native owner or custody" });
        }
        let mut pending = Some(request);
        let future = crate::NativeFutureBundle::new(native, || {
            let (request, mut custody) = pending.take().expect("funded preparation builder runs once");
            async move {
                let mut io = match crate::store::history::native_work_io(native) {
                    Ok(io) => io,
                    Err(cause) => {
                        drop(request);
                        return Err(Error::NativeWork { cause, custody: Some(custody) });
                    }
                };
                let token = self.resource_preparation_with_work(request, &mut Some(&mut io)).await;
                let mut metadata = io.into_custody();
                match token {
                    Ok(mut token) => {
                        if let Err(cause) = custody.absorb(&mut metadata) {
                            drop(token);
                            return Err(Error::NativeWork { cause, custody: Some(custody) });
                        }
                        token.native_custody = Some(custody);
                        Ok(token)
                    }
                    Err(Error::NativeWork { cause, custody: None }) => {
                        drop(metadata);
                        Err(Error::NativeWork { cause, custody: Some(custody) })
                    }
                    Err(error) => {
                        drop(metadata);
                        drop(custody);
                        Err(error)
                    }
                }
            }
        });
        match future {
            Ok(future) => Ok(future),
            Err(error) => {
                let (request, custody) = pending.take().expect("denied builder retains its input");
                drop(request);
                match error {
                    Error::NativeWork { cause, custody: None } =>
                        Err(Error::NativeWork { cause, custody: Some(custody) }),
                    error => { drop(custody); Err(error) }
                }
            }
        }
    }

    async fn resource_preparation_with_work(
        self: &Arc<Self>,
        request: crate::resource_preparation::ResourcePreparationRequest,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::resource_preparation::TaskResourcePreparation> {
        #[cfg(not(target_arch = "wasm32"))]
        let native = work.as_deref().and_then(|observer| observer.original_native_work()).cloned();
        #[cfg(not(target_arch = "wasm32"))]
        if work.is_some() && native.is_none() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                reason: "preparation producer requires its original native observer" });
        }
        if let Some(observer) = work.as_deref_mut() {
            observer.retain_owned((std::mem::size_of::<crate::resource_preparation::TaskResourcePreparation>()
                - std::mem::size_of::<crate::resource_preparation::ResourcePreparationRequest>()) as u64)?;
        }
        let (parent_task, request_digest) = {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let input = crate::store::history::canonical_bytes_with_work(
                &request, self.input_limits.file_bytes, quota.work())?;
            quota.adopt(input.capacity() as u64)?;
            request.validate(quota.work())?;
            if let Some(checkpoint) = &request.source_checkpoint {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(native) = native.as_ref() {
                    self.validate_input_file(checkpoint, Some(&crate::filesystem::PreparationIo::from_native_work(native)?))?;
                } else {
                    self.validate_input_file(checkpoint, None)?;
                }
                #[cfg(target_arch = "wasm32")]
                self.validate_input_file(checkpoint, None)?;
            }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(native) = native.as_ref() {
                self.verify_with_native_work(false, native).await?;
            } else {
                self.verify(false, &mut None).await?;
            }
            #[cfg(target_arch = "wasm32")]
            self.verify(false, &mut None).await?;
            self.host.coordinator.lock().await.scheduler()
                .session_limits(self.operation_id(), &mut None)?.validate(quota.work())?;
            let admission = self.host.admission_with_work(self.operation_id(), quota.work()).await?;
            if quota.work().is_some() {
                let extent = (std::mem::size_of::<crate::runtime::TaskAdmissionRecord>() as u64)
                    .checked_add(admission.logical_owned_bytes()?)
                    .ok_or_else(|| admission_boundary(quota.work(), crate::wire::ErrorCode::Unauthorized, "preparation admission ownership overflows"))?;
                quota.adopt(extent)?;
            }
            let retained = crate::store::history::canonical_bytes_with_work(
                &admission.input, self.input_limits.file_bytes, quota.work())?;
            quota.adopt(retained.capacity() as u64)?;
            let parent = admission.parent.ok_or_else(|| admission_boundary(
                quota.work(), crate::wire::ErrorCode::Unauthorized, "resource preparation requires an admitted parent"))?;
            if let Some(observer) = quota.work().as_deref_mut() {
                let comparisons = (request.resources.len() as u64)
                    .checked_mul(request.child.resource_proofs.len() as u64)
                    .and_then(|comparisons| comparisons.checked_add(1))
                    .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                        reason: "preparation identity comparison work overflows" })?;
                observer.codec_charge(crate::contract::JsonCodecCharge::Work {
                    bytes: input.len() as u64, items: comparisons,
                })?;
            }
            if retained != input
                || request.child.parent != Some(parent)
                || request.child.operation_id == admission.operation_id
                || request.child.resource_proofs.iter().any(|proof|
                    request.resources.contains(&proof.resource) && proof.preparation != self.task_id)
                || !self.input_grants.contains(&request.creation_grant)
            {
                return Err(admission_boundary(quota.work(), crate::wire::ErrorCode::Unauthorized,
                    "resource request differs from its admitted input, parent or creation authority"));
            }
            let source_bound = crate::store::history::canonical_bytes_with_work(
                &(&request, &self.input_grants), request.maximum_bytes, quota.work())?;
            quota.adopt(source_bound.capacity() as u64)?;
            if let Some(observer) = quota.work().as_deref_mut() {
                observer.hashed_bytes(input.len() as u64)?;
            }
            (parent, *blake3::hash(&input).as_bytes())
        };
        #[cfg(not(target_arch = "wasm32"))]
        let source_io = native.as_ref().map(crate::filesystem::PreparationIo::from_native_work).transpose()?;
        #[cfg(target_arch = "wasm32")]
        let source_io: Option<crate::filesystem::PreparationIo> = None;
        let project_source = self.host.seal_prepared_project_source(&request, parent_task, source_io.as_ref()).await?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = source_io {
            let retained = io.owned_bytes()?;
            let observer = work.as_deref_mut().expect("native preparation observer");
            observer.absorb_native_custody(io.into_native_custody(retained)?)?;
            if project_source.is_some() {
                observer.release_owned(std::mem::size_of::<crate::source_authorization::ProjectSourceReadHandle>() as u64)?;
            }
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let owner = crate::core::copy_authority(self.host.verifier.audience(), &mut quota)?;
        let creation_fence = crate::scheduler::LeaseFence {
            reservation_id: crate::core::copy_text(&self.fence.reservation_id, &mut quota)?,
            placement: crate::core::copy_text(&self.fence.placement, &mut quota)?,
        };
        let source_grants = crate::core::copy_capabilities(&self.input_grants, &mut quota)?;
        let origin = crate::resource_preparation::ResourceCreationOrigin {
            owner, parent_task, preparation_task: self.task_id,
            intended_child: request.child.operation_id,
            admission_digest: self.original_admission_digest,
            request_digest, creation_fence, source_grants,
        };
        let token = crate::resource_preparation::TaskResourcePreparation {
            owner: self.clone(), request, origin, project_source,
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: None,
        };
        quota.transfer();
        Ok(token)
    }

    fn execution_write(
        operation: OperationId,
        event: &crate::executor::ExecutionEvent,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::distributed::JournalWrite> {
        use crate::{distributed::JournalWrite, executor::ExecutionEvent};
        Ok(match event {
            ExecutionEvent::ModelStarted {
                step,
                purpose,
                request_digest,
                ..
            } => {
                #[cfg(not(target_arch = "wasm32"))]
                let io = work.as_deref().and_then(|io| io.original_native_work())
                    .map(crate::filesystem::PreparationIo::from_native_work).transpose()?;
                #[cfg(target_arch = "wasm32")]
                let io: Option<crate::filesystem::PreparationIo> = None;
                JournalWrite::Model {
                    attempt_id: purpose.attempt_operation(operation, io.as_ref())?,
                    step: *step,
                    request_digest: *request_digest,
                }
            },
            ExecutionEvent::Started { .. }
            | ExecutionEvent::ContextPrepared { .. }
            | ExecutionEvent::ContextCompacted { .. }
            | ExecutionEvent::ToolStarted { .. } => JournalWrite::Fresh,
            ExecutionEvent::Model { .. }
            | ExecutionEvent::ToolCompleted { .. }
            | ExecutionEvent::ToolFailed { .. } => JournalWrite::Settlement,
        })
    }

    pub(crate) async fn verify_execution_with_work(
        &self,
        operation: OperationId,
        event: &crate::executor::ExecutionEvent,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.condition(Self::execution_write(operation, event, work)?, work)
            .await
            .map(|_| ())
    }

    pub(crate) async fn verify_model_history<'a>(
        &self, operation: OperationId,
        records: impl Iterator<Item=&'a crate::executor::ExecutionRecord> + Clone,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.host.verify_model_history(self.task_id,operation,records,work).await
    }

    pub(crate) fn require_effect_grants(
        &self, provider: &str, planning: bool,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if !self.input_grants.contains(capability::EFFECT_RUN) {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized, "task scope lacks effect:run"));
        }
        let mut permitted = false;
        for grant in self.input_grants.iter() {
            if let Some(io) = work.as_deref_mut() {
                io.codec_charge(crate::contract::JsonCodecCharge::Work {bytes:0,items:1})?;
            }
            if grant.strip_prefix("effect:provider:") == Some(provider) {
                permitted = true;
                break;
            }
        }
        if !permitted {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized, "task scope lacks the effect provider grant"));
        }
        if planning && !self.input_grants.contains(capability::EFFECT_PLAN) {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized, "task scope lacks effect:plan"));
        }
        Ok(())
    }

    pub(crate) fn validate_input_file(
        &self, file: &FileRef, io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<()> {
        if let Some(io) = io { io.check_cancellation()?; }
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = history.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(not(target_arch = "wasm32"))]
        let native = io.is_some_and(|io| io.original_native_work().is_some());
        #[cfg(target_arch = "wasm32")]
        let native = false;
        if native {
            if file.path().len() > self.input_limits.path_bytes
                || file.descriptor().byte_length() > self.input_limits.file_bytes {
                return Err(admission_boundary(&work, crate::wire::ErrorCode::Invalid,
                    "execution input file exceeds original task limits"));
            }
        } else { self.input_limits.validate_file(file, &mut None)?; }
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = history.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
        let allowed = crate::runtime::read_granted_with_work(&self.input_grants, file, &mut work)?
            || (io.is_some() && match self.input_created.as_ref() {
                Some(access) => crate::runtime::read_granted_with_work(&access.grants, file, &mut work)?,
                None => false,
            });
        if !allowed {
            return Err(admission_boundary(&work, crate::wire::ErrorCode::Unauthorized,
                "original task cannot read the execution input file"));
        }
        Ok(())
    }

    pub(crate) fn authenticated_binding_agent(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<crate::AgentId>> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if !self.input_grants.contains(capability::CONVERSATION_BIND) {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized, "task scope lacks conversation:bind"));
        }
        Ok(self.input_agent)
    }

    pub(crate) fn authenticated_fork_agent(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::AgentId> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
            io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
        }
        if !self.input_grants.contains(capability::FORK_PUBLISH) {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized, "task scope lacks fork:publish"));
        }
        self.input_agent.ok_or_else(|| admission_boundary(work, crate::wire::ErrorCode::Unauthorized, "task has no authenticated current agent"))
    }

    pub(crate) fn binding_origin(&self) -> (TaskId, [u8;32], &crate::scheduler::LeaseFence) {
        (self.task_id, self.workflow.request_digest, &self.fence)
    }

    /// Serializes root bind replay/commit/cache on this same owner handle only.
    pub(crate) async fn lock_root_binding(&self) -> Option<tokio::sync::MutexGuard<'_,()>> {
        Some(self.root_binding.as_ref()?.gate.lock().await)
    }

    pub(crate) fn check_root_binding(
        &self, audience: &Authority, operation: OperationId, intent: [u8; 32],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if let Some(proof) = self.root_binding.as_ref().and_then(|state| state.proof.get()).map(|retained| &retained.proof) {
            if proof.audience() != audience || proof.bind_operation() != operation || proof.intent_digest() != intent {
                return Err(admission_boundary(
                    work, crate::wire::ErrorCode::Conflict,
                    "root binding audience, operation or intent changed",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn pin_root_binding_choice(
        &self, operation: OperationId, intent: [u8; 32], audience_digest: [u8; 32],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        let Some(state) = &self.root_binding else { return Ok(()); };
        let choice = (operation,intent,audience_digest);
        if let Err(choice) = state.pending.set(choice) {
            if state.pending.get() != Some(&choice) {
                return Err(admission_boundary(
                    work, crate::wire::ErrorCode::Conflict,
                    "root binding publication choice changed",
                ));
            }
        }
        Ok(())
    }

    /// Borrows the canonical root-bind proof without exposing a scope or minting volume permission.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn authenticated_root_binding_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<&crate::store::VerifiedRootBinding> {
        let proof = self.root_binding.as_ref()
            .and_then(|state| state.proof.get())
            .map(|retained| &retained.proof)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "root creation lacks its canonical committed binding",
            })?;
        self.verify_root_binding_use_with_native_work(proof, false, work).await?;
        Ok(proof)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_root_binding_identity_with_native_work(
        &self, proof: &crate::store::VerifiedRootBinding, readonly: bool,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.charge(acyclic_fs::WorkCounters::default())?;
        if self.root_binding.is_none() || (!readonly && self.verify_original_read_witness()?) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "root creation requires its original live root owner" });
        }
        if proof.original_task() != self.task_id
            || proof.admission_digest() != self.original_admission_digest
            || proof.fence() != &self.fence
            || Some(proof.agent()) != self.input_agent
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "root binding differs from its original owner" });
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn verify_root_binding_use_with_native_work(
        &self, proof: &crate::store::VerifiedRootBinding, readonly: bool,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.verify_root_binding_identity_with_native_work(proof, readonly, work)?;
        if readonly { self.verify_read_access(&mut Some(&mut crate::store::history::native_work_io(work)?)).await }
        else { self.verify_with_native_work(false, work).await }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn root_bootstrap_binding_witness_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<RootBootstrapBindingWitness>> {
        self.authenticated_root_binding_with_native_work(work).await?;
        Ok(Arc::clone(self.root_binding.as_ref().expect("verified root cache")))
    }

    /// Uses only the original committed binding and original READ witness; expired historical custody never becomes a fresh creator or writer.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn authenticated_root_binding_for_read_with_native_work<'a>(
        &'a self, witness: &'a RootBootstrapBindingWitness,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<&'a crate::store::VerifiedRootBinding> {
        let proof = witness.binding();
        self.verify_root_binding_use_with_native_work(proof, true, work).await?;
        Ok(proof)
    }

    pub(crate) fn root_bootstrap_owner(&self) -> &Authority { self.host.verifier.audience() }

    /// Authenticates the configured Task creator's existing MAC.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn authenticate_root_bootstrap_creator_with_native_work(
        &self, verifier: &AuthorityVerifier,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.charge(acyclic_fs::WorkCounters::default())?;
        if self.root_binding.is_none() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "bootstrap creator requires an original root admission" });
        }
        verifier.verify_audience(self.host.verifier.audience(), &mut Some(&mut crate::store::history::native_work_io(work).map_err(|cause| crate::Error::NativeWork { cause, custody: None })?))?;
        verifier.verify(&self.host.owner_scope, &mut Some(&mut crate::store::history::native_work_io(work)?))?;
        self.verify_read_access(&mut Some(&mut crate::store::history::native_work_io(work)?)).await
    }

    /// Retains the existing coordinator-only publication guard, never this host or task context.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn root_bootstrap_publication_lease_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<dyn crate::resource_preparation::CreatedTaskLease>> {
        self.authenticated_root_binding_with_native_work(work).await?;
        let mut io = crate::store::history::native_work_io(work)?;
        crate::store::history::HistoryIo::retain_owned(
            &mut io, std::mem::size_of::<CreatedTaskPublicationLease<P>>() as u64,
        )?;
        let fence = host_fence_with_work(&self.fence, &mut Some(&mut io))?;
        self.host.created_publication_lease_with_work(
            self.operation_id(), Some(fence), self.deadline_epoch_ms, &mut Some(&mut io),
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn original_root_admission_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskAdmissionRecord, acyclic_fs::NativeAllocation)> {
        if self.root_binding.is_none() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "bootstrap source requires an original root admission" });
        }
        self.verify_read_access(&mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
        self.host.observe_admission_with_native_work(self.task_id, work).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn verify_root_bootstrap_context_with_native_work(
        &self, root: &TaskAdmissionRecord, binding: &crate::store::VerifiedRootBinding,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.verify_root_binding_identity_with_native_work(binding, true, work)?;
        if root.parent.is_some() || crate::runtime::task_id_for_admission(root.operation_id) != self.task_id {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "bootstrap admission differs from its actual root" });
        }
        let mut io = crate::store::history::native_work_io(work)?;
        let digest = crate::contract::canonical_json_digest_with_work(root, acyclic_stream::MAX_RECORD_BYTES as u64, &mut Some(&mut io))?;
        if digest != self.original_admission_digest {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "bootstrap admission differs from protected original workflow" });
        }
        self.verify_read_access(&mut Some(&mut crate::store::history::native_work_io(work)?)).await
    }

    pub(crate) fn retain_root_binding(
        &self, proof: crate::store::VerifiedRootBinding,
        #[cfg(not(target_arch = "wasm32"))] custody: Option<acyclic_fs::NativeAllocation>,
    ) -> Result<bool> {
        let retained = RetainedRootBinding {
            proof,
            #[cfg(not(target_arch = "wasm32"))]
            _custody: custody,
        };
        let proof = &retained.proof;
        #[cfg(not(target_arch = "wasm32"))]
        let native = retained._custody.as_ref().map(|custody| custody.work().clone());
        #[cfg(not(target_arch = "wasm32"))]
        let mut native_io = native.as_ref().map(crate::store::history::native_work_io).transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut work = native_io.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(target_arch = "wasm32")]
        let mut work: Option<&mut dyn crate::store::history::HistoryIo> = None;
        let agent = self.authenticated_binding_agent(&mut work)?;
        if self.root_binding.is_none() || proof.original_task() != self.task_id
            || proof.admission_digest() != self.workflow.request_digest
            || proof.fence() != &self.fence
            || agent != Some(proof.agent())
        {
            return Err(admission_boundary(&work, crate::wire::ErrorCode::Unauthorized,
                "root binding proof belongs to another owner or lease"));
        }
        let state = self.root_binding.as_ref().ok_or_else(|| admission_boundary(
            &work, crate::wire::ErrorCode::Unauthorized, "owner has no root binding cache",
        ))?;
        if let Err(incoming) = state.proof.set(retained) {
            let proof = &incoming.proof;
            let retained = &state.proof.get().ok_or_else(|| admission_boundary(
                &work, crate::wire::ErrorCode::Conflict, "root binding cache is unavailable",
            ))?.proof;
            if retained.audience() != proof.audience() || retained.bind_operation() != proof.bind_operation()
                || retained.intent_digest() != proof.intent_digest() {
                return Err(admission_boundary(&work, crate::wire::ErrorCode::Conflict,
                    "root binding audience, operation or intent changed"));
            }
            return Ok(false);
        }
        Ok(true)
    }

    pub(crate) fn authenticate_fork_parent(
        &self, parent: &Authority, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.authenticated_fork_agent(work)?;
        let audience = match &self.input_created {
            Some(access) => access.current_content(work)?.and_then(|binding| binding.authority.as_ref()),
            None if self.root_binding.is_some() => self.root_binding.as_ref().and_then(|state| state.proof.get()).map(|retained| retained.proof.audience()),
            None => None,
        }.ok_or_else(|| admission_boundary(work, crate::wire::ErrorCode::Unauthorized, "current caller has no authenticated publication audience"))?;
        crate::core::source_metadata_inspection_with_work(&(audience, parent), work)?;
        if audience != parent {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized, "fork parent differs from the current caller audience"));
        }
        Ok(())
    }

    pub(crate) fn authenticate_binding_scope(
        &self,
        audience: &Authority,
        scope: &Scope,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let agent = self.authenticated_binding_agent(&mut None)?.ok_or_else(|| Error::Unauthorized("binding owner has no authenticated agent".into()))?;
        if scope.agent() != Some(agent) || !scope.capabilities().contains(capability::CONVERSATION_BIND) {
            return Err(Error::Unauthorized("binding scope differs from the original task actor or grant".into()));
        }
        if self.root_binding.is_none() {
            let binding = match &self.input_created { Some(access) => access.current_content(&mut None)?, None => None };
            if binding.and_then(|binding| binding.authority.as_ref()) != Some(audience) {
                return Err(Error::Unauthorized("binding audience differs from the current sealed child".into()));
            }
        }
        self.host.verifier.verify_scope_for_with_work(audience, scope, work)
    }

    pub(crate) async fn append_conversation(
        &self,
        publication: &crate::store::operations::IndexedPublication,
        key: &StreamKey,
        write: crate::distributed::JournalWrite,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
        mut phase: Option<&mut crate::distributed::CommitPhase>,
    ) -> Result<bool> {
        if let Some(phase) = phase.as_deref_mut() {
            *phase = crate::distributed::CommitPhase::Uncommitted;
        }
        let fresh = !matches!(write, crate::distributed::JournalWrite::Settlement);
        let mut request = self
            .prepare_append_with_work(
                &publication.path,
                publication.expected_tail,
                key,
                &publication.bytes,
                write,
                work,
            )
            .await?;
        publication.add_index_with_work(&mut request)?;
        if fresh && self.deadline_epoch_ms.is_some_and(|deadline| self.host.clock.now_unix_millis() >= deadline) {
            return Err(Error::Indeterminate(self.operation_id()));
        }
        let (outcome, retained) = crate::distributed::commit(request, &self.stream, Some(publication.operation_id), phase, None, None).await?;
        let mut received = crate::wire_codec::CodecQuota::from_retained(work, retained);
        let result = match &outcome {
            acyclic_stream::CommitOutcome::Conflict(_) => Ok(false),
            acyclic_stream::CommitOutcome::Committed(envelope) => {
                publication.verify_with_work(envelope, received.work())
                    .map(|()| true)
                    .map_err(|error| {
                        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                        if error.is_native_failure() { return error; }
                        Error::Indeterminate(publication.operation_id)
                    })
            }
        };
        drop(outcome);
        drop(received);
        result
    }

    pub(crate) async fn append_execution_with_work(
        &self,
        operation: OperationId,
        expected_tail: u64,
        key: &StreamKey,
        bytes: OwnedBytes,
        event: &crate::executor::ExecutionEvent,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(admission_boundary(
                work, crate::wire::ErrorCode::Invalid,
                "execution record exceeds canonical Stream byte bound",
            ));
        }
        let path = crate::distributed::execution_path(operation, work)?;
        self.append(
            path, expected_tail, key, bytes,
            Self::execution_write(operation, event, work)?, work,
        ).await
    }

    pub(crate) async fn append_workflow_with_work(
        &self, admission: bool, expected_tail: u64, key: &StreamKey, bytes: OwnedBytes,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_ref().is_some_and(|io| io.original_native_work().is_some()) {
                return Err(Error::NativeBoundary {code:crate::wire::ErrorCode::Invalid,
                    reason:"workflow record exceeds canonical Stream byte bound"});
            }
            return Err(Error::Invalid("workflow record exceeds canonical Stream byte bound".into()));
        }
        let class = if admission {
            "workflow-admissions"
        } else {
            "workflows"
        };
        let mut path_text = String::new();
        let mut uuid = [0; 36];
        let operation = uuid::Uuid::from_bytes(self.operation_id().into_bytes()).hyphenated().encode_lower(&mut uuid);
        let length = "harness/v2/".len() + class.len() + 1 + operation.len();
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(length as u64)?;
            io.reserve_text(&mut path_text, length)?;
            io.copy_bytes(length as u64)?;
        } else {
            path_text.reserve_exact(length);
        }
        path_text.push_str("harness/v2/");
        path_text.push_str(class);
        path_text.push('/');
        path_text.push_str(operation);
        let temporary_capacity = path_text.capacity() as u64;
        if let Some(io) = work.as_deref_mut() {
            io.admit_owned(length as u64)?;
            io.admit_allocation(length as u64)?;
            io.copy_bytes(length as u64)?;
            io.retain_owned(length as u64)?;
        }
        let path = acyclic_stream::StreamPath::new(path_text)?;
        if let Some(io) = work.as_deref_mut() { io.release_owned(temporary_capacity)?; }
        let result = self.append(
            path,
            expected_tail,
            key,
            bytes,
            crate::distributed::JournalWrite::Fresh,
            work,
        )
        .await;
        let released = match work.as_deref_mut() {
            Some(io) => io.release_owned(length as u64),
            None => Ok(()),
        };
        match result {
            Ok(value) => { released?; Ok(value) }
            Err(error) => Err(error),
        }
    }

    async fn prepare_append_with_work<'a, 'b>(
        &self,
        path: &acyclic_stream::StreamPath,
        expected_tail: u64,
        key: &StreamKey,
        bytes: &OwnedBytes,
        write: crate::distributed::JournalWrite,
        work: &'a mut Option<&'b mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(acyclic_stream::CommitRequest, crate::wire_codec::CodecQuota<'a, 'b>)> {
        use acyclic_stream::CommitCondition;
        if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(admission_boundary(
                work, crate::wire::ErrorCode::Invalid,
                "owned journal record exceeds the canonical byte bound",
            ));
        }
        let owner = self.condition(write, work).await?;
        if let Some(io) = work.as_deref_mut() { io.begin_call()?; }
        let target = match self.stream.bounds(acyclic_stream::StreamPath::new(path.as_str())?).await {
            Ok(_) => CommitCondition::Tail {
                path: path.clone(),
                expected: expected_tail,
            },
            Err(StreamError::NotFound) if expected_tail == 0 => {
                CommitCondition::Absent { path: path.clone() }
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        let mut hash = blake3::Hasher::new();
        for part in [b"harness/v2/owned-journal\0".as_slice(), key.as_bytes(), &self.task_id.into_bytes()] {
            if let Some(io) = work.as_deref_mut() { io.hashed_bytes(part.len() as u64)?; }
            hash.update(part);
        }
        if let Some(io) = work.as_deref_mut() { io.admit_encoding(1)?; }
        let encoded_fence = crate::store::history::canonical_bytes_for_bounds_with_work(
            &self.fence, crate::contract::JsonBounds {
                maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
                maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64, maximum_depth: 256,
            }, work,
        )?;
        let hash_admission = match work.as_deref_mut() {
            Some(io) => io.hashed_bytes(encoded_fence.len() as u64),
            None => Ok(()),
        };
        if hash_admission.is_ok() { hash.update(&encoded_fence); }
        let retained_fence = encoded_fence.capacity() as u64;
        drop(encoded_fence);
        let release = match work.as_deref_mut() {
            Some(io) => io.release_owned(retained_fence),
            None => Ok(()),
        };
        hash_admission?;
        release?;
        let CommitCondition::Tail { expected, .. } = &owner else {
            return Err(Error::Invalid("coordinator tail condition required".into()));
        };
        for part in [expected.to_le_bytes().as_slice(), path.as_str().as_bytes(),
            &expected_tail.to_le_bytes(), &[u8::from(matches!(target, CommitCondition::Absent { .. }))]] {
            if let Some(io) = work.as_deref_mut() { io.hashed_bytes(part.len() as u64)?; }
            hash.update(part);
        }
        if let Some(io) = work.as_deref_mut() { io.hashed_bytes(bytes.len() as u64)?; }
        let payload_digest = blake3::hash(bytes);
        if let Some(io) = work.as_deref_mut() { io.hashed_bytes(32)?; }
        hash.update(payload_digest.as_bytes());
        let (key, held) = crate::distributed::digest_key(hash.finalize().as_bytes(), work)?;
        let mut request = crate::distributed::commit_request(key, held, work);
        crate::distributed::add_condition(&mut request, owner)?;
        crate::distributed::add_condition(&mut request, target)?;
        crate::distributed::add_record(&mut request, path.clone(), bytes.clone())?;
        Ok(request)
    }

    pub(crate) async fn append(
        &self,
        path: acyclic_stream::StreamPath,
        expected_tail: u64,
        key: &StreamKey,
        bytes: OwnedBytes,
        write: crate::distributed::JournalWrite,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        use acyclic_stream::{CommitOutcome, CommittedMutation};
        let request = self
            .prepare_append_with_work(&path, expected_tail, key, &bytes, write, work)
            .await?;
        let (outcome, retained) = crate::distributed::commit(request, &self.stream, Some(self.operation_id()), None, None, None).await?;
        let mut received = crate::wire_codec::CodecQuota::from_retained(work, retained);
        let result = match &outcome {
            CommitOutcome::Conflict(_) => Ok(false),
            CommitOutcome::Committed(envelope) => {
                let invalid = match envelope.mutations.as_slice() {
                    [CommittedMutation::Append(append)] => {
                        let valid = append.path == path
                            && append.start == expected_tail
                            && append.end == expected_tail + 1
                            && append.tail == append.end
                            && append.records.len() == 1
                            && append.records.first().is_some_and(|record| {
                                record.sequence == expected_tail
                                    && record.value == bytes
                                    && record.commit_id == envelope.commit_id
                            });
                        (!valid).then_some("invalid owned journal append")
                    }
                    _ => Some("invalid owned journal envelope"),
                };
                match invalid {
                    None => Ok(true),
                    Some(reason) => Err(admission_boundary(
                        received.work(), crate::wire::ErrorCode::Storage, reason,
                    )),
                }
            }
        };
        drop(outcome);
        drop(received);
        result
    }
}

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    async fn cancel_with_work(
        &self,
        task_id: TaskId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        let operation_id = OperationId::from_bytes(task_id.into_bytes());
        let mut key = String::new();
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(48)?;
            io.reserve_text(&mut key, 48)?;
            io.copy_bytes(48)?;
        }
        use std::fmt::Write as _;
        write!(&mut key, "task-cancel:{operation_id}").expect("writing into String");
        self.coordinator.lock().await.cancel_operation(
            self.verifier.audience(), &self.owner_scope, &self.verifier, operation_id,
            IdempotencyKey::new(key)?, true, work,
        ).await?;
        Ok(())
    }

    /// Requests cancellation using the original signed owner and retained retry identity on the existing native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn cancel_with_native_work(
        &self,
        task_id: TaskId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut io = crate::store::history::native_work_io(work)?;
        self.cancel_with_work(task_id, &mut Some(&mut io)).await
    }

    /// Authenticates operation controls against this task owner's trust root.
    pub async fn authorize_operation_control(
        &self,
        request: &crate::wire_api::OperationControlRequest,
    ) -> Result<()> {
        request.authenticate(&self.verifier)?;
        if &request.owner != self.verifier.audience() {
            return Err(Error::Unauthorized("operation control belongs to another owner".into()));
        }
        if !request.scope.capabilities().contains(capability::OPERATION_OBSERVE)
            && !request.scope.capabilities().contains(capability::OPERATION_CANCEL)
        {
            return Err(Error::Unauthorized("operation control capability is missing".into()));
        }
        Ok(())
    }

    /// Observes one exact operation through the existing coordinator.
    pub async fn observe_control(
        &self,
        request: &crate::wire_api::OperationControlRequest,
    ) -> Result<crate::scheduler::OperationState> {
        self.authorize_operation_control(request).await?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut None).await?;
        coordinator.observe_operation(
            &request.owner,
            &request.scope,
            &self.verifier,
            request.operation_id,
         &mut None,).cloned()
    }

    /// Cancels one operation with the caller's retained retry identity.
    pub async fn cancel_control(
        &self,
        request: &crate::wire_api::CancelControlRequest,
    ) -> Result<crate::scheduler::OperationState> {
        self.authorize_operation_control(&request.control).await?;
        let mut coordinator = self.coordinator.lock().await;
        let (_, state) = coordinator.cancel_operation(
            &request.control.owner,
            &request.control.scope,
            &self.verifier,
            request.control.operation_id,
            request.idempotency_key.clone(),
            request.recursive, &mut None,
        ).await?;
        Ok(state)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) async fn verify_model_history<'a>(
        &self, task: TaskId, operation: OperationId,
        records: impl Iterator<Item=&'a crate::executor::ExecutionRecord> + Clone,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if !records.clone().any(|record| {
            matches!(
                record.event,
                crate::executor::ExecutionEvent::ModelStarted { .. }
            )
        }) {
            return Ok(());
        }
        let task_operation = OperationId::from_bytes(task.into_bytes());
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            task_operation, work,
        )?;
        for record in records {
            if let crate::executor::ExecutionEvent::ModelStarted {
                step,
                purpose,
                request_digest,
                ..
            } = &record.event
            {
                coordinator.scheduler().require_model_claim(
                    task_operation,
                    purpose.attempt_operation(operation, None)?,
                    *step,
                    request_digest,
                    work,
                )?;
            }
        }
        Ok(())
    }

    async fn claim_model_dispatch_with_work(
        &self,
        task_id: TaskId,
        attempt_id: OperationId,
        step: u32,
        request_digest: [u8; 32],
        fence: &crate::scheduler::LeaseFence,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        let operation_id = OperationId::from_bytes(task_id.into_bytes());
        let admission = self.read_retained_admission_counted(operation_id, work).await.map(|(record, _)| record)?;
        let (scope, _) = self.authenticated_scope_with_work(&admission, work).await?;
        if !admission.grants.contains("model:generate") {
            return Err(admission_boundary(
                work, crate::wire::ErrorCode::Unauthorized,
                "task scope lacks model:generate",
            ));
        }
        self.validate_parent_scope_with_work(admission.parent, &scope, work).await?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let operation = coordinator.observe_operation(
            self.verifier.audience(), &self.owner_scope, &self.verifier, operation_id, work,
        )?;
        crate::scheduler::require_execution_owner(operation, fence, false, work)?;
        let session = coordinator.scheduler().session_limits(operation_id, work)?;
        let ceiling = (scope.limits().model_steps as u64)
            .min(scope.run_limits().max_steps.map_or(u64::MAX, |value| value as u64))
            .min(session.model_steps);
        let lease_digest = crate::contract::canonical_json_digest_with_work(
            fence, acyclic_stream::MAX_RECORD_BYTES as u64, work,
        )?;
        let mut uuid = [0; 36];
        let attempt = uuid::Uuid::from_bytes(attempt_id.into_bytes())
            .hyphenated().encode_lower(&mut uuid);
        let digits = if step == 0 { 1 } else { step.ilog10() as usize + 1 };
        let length = "model-dispatch:".len() + attempt.len() + 1 + digits + 1 + 64;
        let mut key = String::new();
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(length as u64)?;
            io.reserve_text(&mut key, length)?;
            io.copy_bytes(length as u64)?;
        } else {
            key.reserve_exact(length);
        }
        use std::fmt::Write as _;
        write!(&mut key, "model-dispatch:{attempt}:{step}:{}", blake3::Hash::from(lease_digest))
            .map_err(|_| admission_boundary(
                work, crate::wire::ErrorCode::Invalid,
                "model dispatch key formatting failed",
            ))?;
        let fence = fence.clone_with_work(work)?;
        coordinator.apply(
            operation_id,
            IdempotencyKey::new(key)?,
            crate::scheduler::SchedulerEvent::ModelDispatchClaimed {
                operation_id, attempt_id, step, request_digest, fence, ceiling,
            },
            work,
        ).await?;
        Ok(())
    }

    /// Claims through the same coordinator, restricted to this signed owner.
    pub async fn pull_work(
        &self,
        worker: &crate::distributed::Worker,
    ) -> Result<crate::distributed::WorkPull> {
        self.coordinator
            .lock()
            .await
            .pull_owned(self.verifier.audience(), &self.owner_scope, &self.verifier, worker, None, &mut None)
            .await
    }

    /// Claims this original operation without owner-wide task selection.
    pub async fn pull_operation(
        &self,
        worker: &crate::distributed::Worker,
        operation_id: OperationId,
    ) -> Result<crate::distributed::WorkPull> {
        self.pull_operation_with_work(worker, operation_id, &mut None).await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn observe_admission_with_native_work(
        &self, task: TaskId, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskAdmissionRecord, acyclic_fs::NativeAllocation)> {
        let mut io = crate::store::history::native_work_io(work)?;
        let admission = self.admission_with_work(
            OperationId::from_bytes(task.into_bytes()), &mut Some(&mut io),
        ).await?;
        let owned = (std::mem::size_of::<TaskAdmissionRecord>() as u64)
            .checked_add(admission.logical_owned_bytes()?)
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "task admission custody overflows" })?;
        let custody = io.take_native_custody(owned)?;
        Ok((admission, custody))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn outcome_with_native_work(
        &self, task: TaskId, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<Outcome<Value>>, acyclic_fs::NativeAllocation)> {
        let mut io = crate::store::history::native_work_io(work)?;
        let metadata = std::mem::size_of::<Option<Outcome<Value>>>() as u64;
        crate::store::history::HistoryIo::retain_owned(&mut io, metadata)?;
        let (outcome, owned) = self.outcome_with_work(task, &mut Some(&mut io)).await?;
        let custody = io.take_native_custody(owned.checked_add(metadata).ok_or(
            Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "task outcome custody overflows" },
        )?)?;
        Ok((outcome, custody))
    }

    /// Reads only the protected task's actual succeeded result.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn succeeded_result_with_native_work(
        &self, task: TaskId, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<(FileRef, acyclic_stream::OwnedBytes, acyclic_fs::NativeAllocation)>> {
        use crate::store::history::HistoryIo as _;
        let mut io = crate::store::history::native_work_io(work)?;
        let metadata = std::mem::size_of::<
            Option<(FileRef, acyclic_stream::OwnedBytes, acyclic_fs::NativeAllocation)>
        >() as u64;
        io.retain_owned(metadata)?;
        let reference = {
            let mut observer = Some(&mut io as &mut dyn crate::store::history::HistoryIo);
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh(&mut observer).await?;
            let operation = coordinator.observe_operation(
                self.verifier.audience(), &self.owner_scope, &self.verifier,
                OperationId::from_bytes(task.into_bytes()), &mut observer,
            )?;
            match operation.outcome.as_ref() {
                Some(Outcome::Succeeded(file)) => {
                    let mut quota = crate::wire_codec::CodecQuota::new(&mut observer, 0)?;
                    let copied = crate::core::copy_file(file, &mut quota)?;
                    let retained = quota.retained();
                    quota.transfer();
                    Some((copied, retained))
                }
                _ => None,
            }
        };
        let Some((reference, reference_owned)) = reference else {
            io.release_owned(metadata)?;
            return Ok(None);
        };
        let (value, bytes, value_owned, byte_retained) = self.read_json_payload_with_work(
            &reference, &mut Some(&mut io),
        ).await?;
        drop(value);
        io.release_owned(value_owned)?;
        let owned = metadata.checked_add(reference_owned)
            .and_then(|owned| owned.checked_add(byte_retained))
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "succeeded result custody overflows" })?;
        let custody = io.take_native_custody(owned)?;
        Ok(Some((reference, bytes, custody)))
    }

    async fn outcome_with_work(
        &self, task: TaskId, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Option<Outcome<Value>>, u64)> {
        let observed = {
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh(work).await?;
            let operation = coordinator.observe_operation(
                self.verifier.audience(), &self.owner_scope, &self.verifier,
                OperationId::from_bytes(task.into_bytes()), work,
            )?;
            operation.outcome.as_ref().map(|outcome| outcome.clone_with_work(work)).transpose()?
        };
        match observed {
            Some(Outcome::Succeeded(reference)) => {
                let (value, owned) = self.read_json_with_work(&reference, work).await?;
                Ok((Some(Outcome::Succeeded(value)), owned))
            },
            Some(Outcome::Failed { message }) => {
                let owned = if work.is_some() { message.capacity() as u64 } else { 0 };
                Ok((Some(Outcome::Failed { message }), owned))
            },
            Some(Outcome::Cancelled) => Ok((Some(Outcome::Cancelled), 0)),
            Some(Outcome::Indeterminate { operation_id }) => {
                Ok((Some(Outcome::Indeterminate { operation_id }), 0))
            },
            None => Ok((None, 0)),
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn pull_operation_with_native_work(
        &self, worker: &crate::distributed::Worker, operation_id: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(crate::distributed::WorkPull, acyclic_fs::NativeAllocation)> {
        let mut io = crate::store::history::native_work_io(work)?;
        crate::store::history::HistoryIo::retain_owned(&mut io,
            std::mem::size_of::<crate::distributed::WorkPull>()
                .max(std::mem::size_of::<crate::filesystem::TaskWorkerAttempt>()) as u64)?;
        let output = self.pull_operation_with_work(worker, operation_id, &mut Some(&mut io)).await?;
        let custody = io.into_custody();
        Ok((output, custody))
    }

    async fn pull_operation_with_work(
        &self, worker: &crate::distributed::Worker, operation_id: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::distributed::WorkPull> {
        self.coordinator.lock().await.pull_owned(self.verifier.audience(), &self.owner_scope, &self.verifier, worker, Some(operation_id), work).await
    }

    /// Recovers the coordinator's original reservation after a lost pull response.
    pub async fn recover_work(&self, task: TaskId) -> Result<crate::distributed::WorkPull> {
        self.recover_work_with_work(task, &mut None).await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn recover_work_with_native_work(
        &self, task: TaskId, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(crate::distributed::WorkPull, acyclic_fs::NativeAllocation)> {
        let mut io = crate::store::history::native_work_io(work)?;
        crate::store::history::HistoryIo::retain_owned(&mut io,
            std::mem::size_of::<crate::distributed::WorkPull>()
                .max(std::mem::size_of::<crate::filesystem::TaskWorkerAttempt>()) as u64)?;
        let output = self.recover_work_with_work(task, &mut Some(&mut io)).await?;
        let custody = io.into_custody();
        Ok((output, custody))
    }

    async fn recover_work_with_work(
        &self, task: TaskId, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::distributed::WorkPull> {
        use crate::distributed::WorkPull;
        use crate::scheduler::OperationPhase;

        let operation_id = OperationId::from_bytes(task.into_bytes());
        let admission = self.admission_with_work(operation_id, work).await?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let operation = coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            operation_id,
            work,
        )?;
        let Some(lease) = operation.work_lease_with_work(work)? else {
            return Ok(WorkPull::Idle);
        };
        drop(admission);
        if operation.cancellation_requested
            || !matches!(
                operation.phase,
                OperationPhase::Admitted | OperationPhase::Running | OperationPhase::Reconciling
            )
        {
            return Ok(WorkPull::Unresolved {
                lease,
                error: admission_boundary(work, crate::wire::ErrorCode::Conflict,
                    "retained task reservation requires reconciliation"),
            });
        }
        Ok(WorkPull::Claimed(lease))
    }

    /// Durably retains one timer identity without holding a waiting future.
    #[cfg(feature = "filesystem")]
    pub async fn poll_timer(
        self: &Arc<Self>,
        task: TaskId,
        fence: crate::scheduler::LeaseFence,
        operation: OperationId,
        deadline_unix_ms: u64,
    ) -> Result<bool> {
        let owner = self.journal_owner(task, fence).await?;
        self.poll_timer_with_work(&owner, operation, deadline_unix_ms, &mut None).await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn poll_timer_with_native_work(
        self: &Arc<Self>, task: TaskId, fence: crate::scheduler::LeaseFence,
        operation: OperationId, deadline_unix_ms: u64, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        let owner = self.journal_owner_with_native_work(task, &fence, work).await?;
        let mut io = crate::store::history::native_work_io(work)?;
        let result = self.poll_timer_with_work(&owner, operation, deadline_unix_ms, &mut Some(&mut io)).await;
        drop(owner);
        result
    }

    #[cfg(feature = "filesystem")]
    async fn poll_timer_with_work(
        &self, owner: &TaskJournalOwner<P>, operation: OperationId, deadline_unix_ms: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        if let Some(io) = custody.work().as_deref_mut() { io.check_cancellation()?; }
        if deadline_unix_ms == 0 {
            return Err(admission_boundary(custody.work(), crate::wire::ErrorCode::Invalid,
                "timer deadline is invalid"));
        }
        let (task, fence) = owner.task_binding();
        if !owner.input_grants.contains("timer:wait") {
            return Err(admission_boundary(custody.work(), crate::wire::ErrorCode::Unauthorized,
                "task scope lacks timer:wait"));
        }
        let (path, path_retained) = Self::timer_path_with_work(task, custody.work())?;
        custody.adopt(path_retained)?;
        let task_text = Self::timer_path_text(task);
        let mut operation_text = [0; 36];
        uuid::Uuid::from_bytes(operation.into_bytes()).hyphenated().encode_lower(&mut operation_text);
        if let Some(io) = custody.work().as_deref_mut() {
            io.hashed_bytes((task_text.len() + 1 + operation_text.len()) as u64)?;
        }
        let mut identity = blake3::Hasher::new();
        identity.update(&task_text);
        identity.update(b"/");
        identity.update(&operation_text);
        let digest = identity.finalize();
        let (key, key_retained) = {
            let mut key_custody = crate::wire_codec::CodecQuota::new(custody.work(), 0)?;
            let bytes = crate::core::copy_copy_vector(digest.as_bytes(), &mut key_custody)?;
            key_custody.transfer();
            let (bytes, retained) = crate::store::operations::owned_bytes_from_retained_vec(
                bytes, key_custody.work(),
            )?;
            (StreamKey::new(bytes)?, retained)
        };
        custody.adopt(key_retained)?;
        let encoded = crate::store::history::canonical_bytes_with_work(&TimerEvent {
            task_id: task,
            operation_id: operation,
            deadline_unix_ms,
        }, acyclic_stream::MAX_RECORD_BYTES as u64, custody.work())?;
        let (bytes, byte_retained) = crate::store::operations::owned_bytes_from_retained_vec(
            encoded, custody.work(),
        )?;
        custody.adopt(byte_retained)?;
        loop {
            self.verify_owner(task, fence, false, custody.work()).await?;
            let (tail, found) = self.timer_state_with_work(
                task, operation, deadline_unix_ms, custody.work(),
            ).await?;
            if found || owner.append(
                path.clone(), tail, &key, bytes.clone(),
                crate::distributed::JournalWrite::Fresh, custody.work(),
            ).await? {
                self.verify_owner(task, fence, false, custody.work()).await?;
                if let Some(io) = custody.work().as_deref_mut() { io.check_cancellation()?; }
                return Ok(self.clock.now_unix_millis() >= deadline_unix_ms);
            }
        }
    }

    async fn stage_payload_with_work(
        &self, operation: OperationId, key: &str, bytes: &[u8],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<FileRef> {
        let Some(io) = work.as_deref_mut() else {
            return self.payloads.stage(operation, key, bytes).await;
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            let original = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "payload staging requires the original native work observer",
            })?;
            let (reference, custody) = self.payloads
                .stage_with_native_work(operation, key, bytes, original)?.await?;
            io.absorb_native_custody(custody)?;
            Ok(reference)
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        {
            let _ = io;
            Err(Error::Unsupported("native payload staging is unavailable on this platform".into()))
        }
    }

    /// Publishes ref-only mail under the sender's exact uncancelled lease.
    #[cfg(feature = "filesystem")]
    pub async fn send_owned(
        self: &Arc<Self>,
        sender: TaskId,
        fence: crate::scheduler::LeaseFence,
        recipient: TaskId,
        message_id: OperationId,
        payload: FileRef,
    ) -> Result<FileRef> {
        self.send_owned_with_io(sender, &fence, recipient, message_id, payload, None).await
    }

    /// Resolves one exact retained parcel under its receiver's original lease and sole receive command.
    #[cfg(feature = "filesystem")]
    pub async fn read_received_mail_owned(
        self: &Arc<Self>,
        receiver: TaskId,
        fence: crate::scheduler::LeaseFence,
        receive_operation: OperationId,
        item: &InboxItem,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let owner = self.journal_owner(receiver, fence.clone()).await?;
        owner.verify(true, &mut None).await?;
        if item.task_id != receiver || item.sequence == 0 {
            return Err(Error::Unauthorized("parcel recipient or sequence differs".into()));
        }
        let path = format!("harness/v2/workflows/{}", owner.workflow_admission().operation_id);
        let workflow = acyclic_stream::Stream::new(std::sync::Arc::clone(&owner.stream()), &path)?;
        let tail = workflow.bounds().await
            .map_err(|error| Error::Unauthorized(format!("parcel receive checkpoint is absent: {error}")))?.tail;
        let index = tail.checked_sub(1)
            .ok_or_else(|| Error::Unauthorized("parcel receive checkpoint is absent".into()))?;
        let latest = self.read_mail_workflow_record(&owner, &workflow, index).await?;
        let pending = latest.transition.commands.as_slice();
        let receive = if let [command] = pending
            && command.operation_id == receive_operation
            && command.kind == crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND
        {
            command.clone()
        } else {
            let results = latest.input["commands"].as_array()
                .ok_or_else(|| Error::Unauthorized("parcel receive result is not committed".into()))?;
            let [result] = results.as_slice() else {
                return Err(Error::Unauthorized("parcel must consume one receive result".into()));
            };
            let exact = serde_json::to_value(Outcome::Succeeded(item.clone()))
                .map_err(|error| Error::Invalid(error.to_string()))?;
            if result["operation_id"] != serde_json::to_value(receive_operation)
                .map_err(|error| Error::Invalid(error.to_string()))?
                || result["value"] != exact
            {
                return Err(Error::Unauthorized("parcel differs from the committed receive result".into()));
            }
            let prior_index = latest.prior.revision.checked_sub(1)
                .ok_or_else(|| Error::Unauthorized("parcel receive outbox is absent".into()))?;
            let prior = self.read_mail_workflow_record(&owner, &workflow, prior_index).await?;
            let [command] = prior.transition.commands.as_slice() else {
                return Err(Error::Unauthorized("parcel requires a sole receive outbox".into()));
            };
            if command.operation_id != receive_operation
                || command.kind != crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND
                || prior.next != latest.prior
            {
                return Err(Error::Unauthorized("parcel receive operation differs".into()));
            }
            command.clone()
        };
        let receiver_admission = self.admission_with_work(owner.workflow_admission().operation_id, &mut None).await?;
        let selection: crate::filesystem::MailReceiveTaskCommand = crate::contract::json_from_slice(
            &self.read_mail_source(&receiver_admission, &receive.payload, &mut None).await?,
        ).map_err(|error| Error::Unauthorized(format!("invalid retained parcel selection: {error}")))?;
        if selection.after.checked_add(1) != Some(item.sequence) {
            return Err(Error::Unauthorized("parcel cursor differs from the retained receive".into()));
        }
        let records = self.mailbox(receiver)?.read(item.sequence - 1, 1).await?
            .try_collect::<Vec<_>>().await?;
        let [record] = records.as_slice() else {
            return Err(Error::Unauthorized("parcel source is absent".into()));
        };
        let event: MailEvent = crate::contract::json_from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if record.sequence.checked_add(1) != Some(item.sequence)
            || event.message_id.to_string() != item.message_id || event.payload != item.payload
            || crate::contract::canonical_json_bytes(&event)?.as_slice() != record.value.as_ref()
        {
            return Err(Error::Unauthorized("parcel differs from its retained mailbox item".into()));
        }
        let sender_admission = self.admission_with_work(OperationId::from_bytes(event.sender.into_bytes()), &mut None).await?;
        let direct = mail::authorize_route(
            event.sender, &sender_admission, receiver, &receiver_admission, None,
        )?;
        receiver_admission.limits.validate_file(&event.payload, &mut None)?;
        if !direct && !read_granted(&receiver_admission.grants, &event.payload)? {
            return Err(Error::Unauthorized("custom parcel requires recipient file authority".into()));
        }
        if !self.observe_mail_location(
            event.sender, receiver, event.message_id, &record.value, Some(record.sequence), None
        ).await? {
            return Err(Error::Unauthorized("parcel has no original atomic receipt".into()));
        }
        let bytes = self.read_mail_source(&sender_admission, &event.payload, &mut None).await?;
        owner.verify(true, &mut None).await?;
        Ok(bytes)
    }

    #[cfg(feature = "filesystem")]
    async fn read_mail_workflow_record(
        &self,
        owner: &TaskJournalOwner<P>,
        workflow: &acyclic_stream::Stream<P>,
        index: u64,
    ) -> Result<crate::workflow::WorkflowRecord> {
        let records = workflow.read(index, 1).await?.try_collect::<Vec<_>>().await?;
        let [record] = records.as_slice() else {
            return Err(Error::Unauthorized("parcel workflow record is absent".into()));
        };
        let reference: FileRef = crate::contract::json_from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        owner.input_limits().validate_file(&reference, &mut None)?;
        if record.sequence != index
            || crate::contract::canonical_json_bytes(&reference)?.as_slice() != record.value.as_ref()
        {
            return Err(Error::Storage("parcel workflow reference differs".into()));
        }
        let value = self.read_json_with_work(&reference, &mut None).await.map(|(value, _)| value)?;
        let envelope: crate::workflow::WorkflowRecord = serde_json::from_value(value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        envelope.validate(None)?;
        if envelope.prior.revision != index || envelope.next.revision != index + 1 {
            return Err(Error::Storage("parcel workflow revision differs".into()));
        }
        owner.validate_workflow_record(&envelope, None)?;
        Ok(envelope)
    }

    /// Requests a direct child's entire subtree to stop under the caller's retained lease.
    #[cfg(feature = "filesystem")]
    pub async fn cancel_child_owned(
        &self,
        parent: TaskId,
        fence: &crate::scheduler::LeaseFence,
        child: TaskId,
        operation: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::scheduler::OperationState> {
        self.verify_owner(parent, fence, false, work).await?;
        let admitted = self.admission_with_work(OperationId::from_bytes(parent.into_bytes()), work).await?;
        if !admitted.grants.contains(capability::OPERATION_CANCEL)
            || self.admission_with_work(OperationId::from_bytes(child.into_bytes()), work).await?.parent != Some(parent)
        {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized, "cancellation requires an authorized direct child"));
        }
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let parent_state = coordinator.observe_operation(
            self.verifier.audience(), &self.owner_scope, &self.verifier,
            OperationId::from_bytes(parent.into_bytes()),
         work,)?;
        crate::scheduler::require_execution_owner(parent_state, fence, false, work)?;
        let mut key = String::new();
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(91)?;
            io.reserve_text(&mut key, 91)?;
            io.copy_bytes(91)?;
        }
        use std::fmt::Write as _;
        write!(&mut key, "task-child-cancel:{parent}:{operation}").expect("writing into String");
        coordinator.cancel_child_operation(
            self.verifier.audience(), &self.owner_scope, &self.verifier,
            OperationId::from_bytes(child.into_bytes()),
            IdempotencyKey::new(key)?,
            OperationId::from_bytes(parent.into_bytes()), fence,
         work,).await
    }

    /// Starts or reattaches the exact already-admitted lease.
    pub async fn start_task(
        &self, lease: &crate::distributed::WorkLease, native: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut io = native.map(crate::store::history::native_work_io).transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut work = io.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let mut work: Option<&mut dyn crate::store::history::HistoryIo> = None;
        let _ = native;
        let operation_id = lease.operation.operation_id;
        let admission = self.admission_with_work(operation_id, &mut work).await?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut work).await?;
        let operation = coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            operation_id,
            &mut work,
        )?;
        if operation.spec != lease.operation
            || operation.reservation.as_ref() != Some(&lease.reservation)
        {
            return Err(admission_boundary(&mut work, crate::wire::ErrorCode::Conflict,
                "worker lease differs from retained admission"));
        }
        let fence = crate::scheduler::LeaseFence {
            reservation_id: match work.as_deref_mut() {
                Some(io) => io.clone_text(&lease.reservation.id)?,
                None => lease.reservation.id.clone(),
            },
            placement: match work.as_deref_mut() {
                Some(io) => io.clone_text(&lease.reservation.placement)?,
                None => lease.reservation.placement.clone(),
            },
        };
        if operation.phase == crate::scheduler::OperationPhase::Running {
            return crate::scheduler::require_execution_owner(operation, &fence, false, &mut None);
        }
        let key = worker_key_with_work("start", operation_id, &fence, operation.revision, &mut work)?;
        let event = if operation.phase == crate::scheduler::OperationPhase::Reconciling {
            crate::scheduler::SchedulerEvent::ReconciliationResumed { operation_id, fence }
        } else {
            crate::scheduler::SchedulerEvent::Started { operation_id, fence }
        };
        coordinator.apply(operation_id, key, event, &mut work).await?;
        drop(admission);
        Ok(())
    }

    pub(crate) async fn suspend_workflow(
        &self, task: TaskId, fence: &crate::scheduler::LeaseFence, revision: u64,
        waiting_command: Option<OperationId>, idle_execution: Option<(OperationId, u64)>,
        pending: Option<(&acyclic_stream::StreamPath, u64)>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let fence = host_fence_with_work(fence, work)?;
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            operation_id,
            work,
        )?;
        let key = worker_key_with_work("suspend", operation_id, &fence, revision, work)?;
        let event = crate::scheduler::SchedulerEvent::WorkflowSuspended {
            operation_id,
            fence,
            workflow_revision: revision,
            waiting_command,
        };
        if let Some((execution, tail)) = idle_execution {
            coordinator
                .apply_if_execution_quiescent(
                    key, event, execution, tail, pending.map(|(path, tail)| (path.as_str(), tail)), work,
                )
                .await?;
        } else {
            coordinator.apply(operation_id, key, event, work).await?;
        }
        let current = coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            operation_id,
            work,
        )?;
        if current.phase != crate::scheduler::OperationPhase::Suspended
            || current.cancellation_requested
            || current.workflow.as_ref().is_none_or(|slot| {
                slot.revision != revision || slot.waiting_command != waiting_command
            })
        {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Conflict,
                "workflow suspension receipt is no longer current"));
        }
        Ok(())
    }

    /// One bounded source page, filtered by the current signed owner projection.
    #[cfg(feature = "filesystem")]
    pub(crate) async fn workflow_wait_page(
        &self,
        after_revision: u64,
        through_revision: Option<u64>,
        limit: u32,
    ) -> Result<(u64, u64, u32, Vec<TaskId>)> {
        if limit == 0 {
            return Err(Error::Invalid("wake page limit is out of bounds".into()));
        }
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut None).await?;
        self.verifier.verify(&self.owner_scope, &mut None)?;
        if !self
            .owner_scope
            .capabilities()
            .contains("operation:observe")
        {
            return Err(Error::Unauthorized(
                "wake discovery requires operation:observe".into(),
            ));
        }
        let through_revision = through_revision.unwrap_or(coordinator.revision());
        if after_revision > through_revision || through_revision > coordinator.revision() {
            return Err(Error::Invalid(
                "wake cursor exceeds its coordinator snapshot".into(),
            ));
        }
        let count = u32::try_from(u64::from(limit.min(64)).min(through_revision - after_revision))
            .map_err(|_| Error::Storage("wake page count exceeds its allowance".into()))?;
        let events = if count == 0 {
            Vec::new()
        } else {
            crate::distributed::read_coordinator_event_page(&self.stream, after_revision, count)
                .await?
        };
        if events.len() != count as usize {
            return Err(Error::Storage("wake snapshot has missing events".into()));
        }
        let mut tasks = BTreeSet::new();
        for event in events {
            if !matches!(
                event,
                crate::scheduler::SchedulerEvent::WorkflowSuspended {
                    waiting_command: Some(_),
                    ..
                }
            ) {
                continue;
            }
            let operation = coordinator
                .scheduler()
                .operation(event.operation_id())
                .ok_or_else(|| Error::Storage("wake event operation is absent".into()))?;
            if operation.spec.owner.authority() != self.verifier.audience() {
                continue;
            }
            let operation = coordinator.observe_operation(
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                event.operation_id(),
             &mut None,).cloned()?;
            if operation.phase == crate::scheduler::OperationPhase::Suspended
                && !operation.cancellation_requested
                && operation
                    .workflow
                    .as_ref()
                    .is_some_and(|slot| slot.waiting_command.is_some())
            {
                tasks.insert(crate::runtime::task_id_for_admission(event.operation_id()));
            }
        }
        Ok((after_revision + u64::from(count), through_revision, count, tasks.into_iter().collect()))
    }

    /// Authenticated read of a passive slot; never claims execution ownership.
    #[cfg(feature = "filesystem")]
    pub async fn workflow_suspension(
        &self,
        task: TaskId,
    ) -> Result<Option<crate::scheduler::WorkflowSuspension>> {
        self.workflow_suspension_with_work(task, &mut None).await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn workflow_suspension_with_native_work(
        &self, task: TaskId, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<crate::scheduler::WorkflowSuspension>, acyclic_fs::NativeAllocation)> {
        let mut io = crate::store::history::native_work_io(work)?;
        crate::store::history::HistoryIo::retain_owned(&mut io,
            std::mem::size_of::<Option<crate::scheduler::WorkflowSuspension>>() as u64)?;
        let output = self.workflow_suspension_with_work(task, &mut Some(&mut io)).await?;
        let custody = io.into_custody();
        Ok((output, custody))
    }

    #[cfg(feature = "filesystem")]
    async fn workflow_suspension_with_work(
        &self, task: TaskId, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<crate::scheduler::WorkflowSuspension>> {
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let operation = coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            OperationId::from_bytes(task.into_bytes()),
            work,
        )?;
        if operation.phase != crate::scheduler::OperationPhase::Suspended
            || operation.cancellation_requested
        {
            return Ok(None);
        }
        operation.workflow.as_ref().map(|slot| slot.clone_with_work(work)).transpose()
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn workflow_stream(&self) -> std::sync::Arc<P> {
        self.stream.clone()
    }

    #[cfg(feature = "filesystem")]
    pub(crate) async fn timer_ready(
        &self, task: TaskId, operation: OperationId, deadline: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        let admission = self.admission_with_work(OperationId::from_bytes(task.into_bytes()), work).await?;
        drop(admission);
        let (_, retained) = self.timer_state_with_work(task, operation, deadline, work).await?;
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        Ok(retained && self.clock.now_unix_millis() >= deadline)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) async fn wake_command(
        &self, task: TaskId, revision: u64, waiting: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let operation = OperationId::from_bytes(task.into_bytes());
        let mut key = String::new();
        if let Some(io) = work.as_deref_mut() {
            let digits = revision.checked_ilog10().map_or(1, |digits| digits + 1) as usize;
            let length = 92 + digits;
            io.admit_copy(length as u64)?;
            io.reserve_text(&mut key, length)?;
            io.copy_bytes(length as u64)?;
        }
        use std::fmt::Write as _;
        write!(&mut key, "task-command-wake:{task}:{revision}:{waiting}").expect("writing into String");
        let input = self.stage_payload_with_work(operation, &key, b"null", work).await?;
        let admission = self.admission_with_work(operation, work).await?;
        admission.limits.validate_file(&input, &mut None)?;
        if !crate::runtime::read_granted_with_work(&admission.grants, &input, work)? {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized,
                "task cannot read its command wake"));
        }
        let (value, owned) = self.read_json_with_work(&input, work).await?;
        drop(value);
        if let Some(io) = work.as_deref_mut() { io.release_owned(owned)?; }
        self.coordinator
            .lock()
            .await
            .resume_workflow(self.verifier.audience(), &self.owner_scope, &self.verifier, operation, revision, input, IdempotencyKey::new(key)?, Some(waiting), work)
            .await?;
        Ok(())
    }

    /// Owner-authorized delivery into one exact durable workflow resume slot.
    pub async fn resume_workflow(
        &self,
        task: TaskId,
        revision: u64,
        input: FileRef,
        key: IdempotencyKey,
    ) -> Result<()> {
        self.resume_workflow_with_work(task, revision, input, key, &mut None).await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn resume_workflow_with_native_work(
        &self, task: TaskId, revision: u64, input: FileRef, key: IdempotencyKey,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut io = crate::store::history::native_work_io(work)?;
        self.resume_workflow_with_work(task, revision, input, key, &mut Some(&mut io)).await
    }

    async fn resume_workflow_with_work(
        &self, task: TaskId, revision: u64, input: FileRef, key: IdempotencyKey,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let admission = self.admission_with_work(operation_id, work).await?;
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
            if input.path().len() > admission.limits.path_bytes
                || input.descriptor().byte_length() > admission.limits.file_bytes
            {
                return Err(admission_boundary(work, crate::wire::ErrorCode::Invalid,
                    "workflow resume input exceeds original task limits"));
            }
        }
        admission.limits.validate_file(&input, &mut None)?;
        if !crate::runtime::read_granted_with_work(&admission.grants, &input, work)? {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized,
                "task cannot read its workflow resume input"));
        }
        let (value, owned) = self.read_json_with_work(&input, work).await?;
        drop(value);
        if let Some(io) = work.as_deref_mut() { io.release_owned(owned)?; }
        self.coordinator
            .lock()
            .await
            .resume_workflow(self.verifier.audience(), &self.owner_scope, &self.verifier, operation_id, revision, input, key, None, work)
            .await?;
        Ok(())
    }

    /// Reads the retained input under the current lease.
    pub async fn workflow_input(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
    ) -> Result<Option<(u64, Value)>> {
        self.workflow_input_with_work(task, fence, &mut None).await.map(|(input, _)| input)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn workflow_input_with_native_work(
        &self, task: TaskId, fence: &crate::scheduler::LeaseFence,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<(u64, Value)>, acyclic_fs::NativeAllocation)> {
        let mut io = crate::store::history::native_work_io(work)?;
        let metadata = std::mem::size_of::<Option<(u64, Value)>>() as u64;
        crate::store::history::HistoryIo::retain_owned(&mut io, metadata)?;
        let (input, bytes) = self.workflow_input_with_work(task, fence, &mut Some(&mut io)).await?;
        let custody = io.take_native_custody(bytes.checked_add(metadata).ok_or(
            Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "workflow input custody overflows" },
        )?)?;
        Ok((input, custody))
    }

    async fn workflow_input_with_work(
        &self, task: TaskId, fence: &crate::scheduler::LeaseFence,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Option<(u64, Value)>, u64)> {
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let admission = self.admission_with_work(operation_id, work).await?;
        let slot = {
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh(work).await?;
            let operation = coordinator.observe_operation(
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                operation_id,
                work,
            )?;
            crate::scheduler::require_execution_owner(operation, fence, false, &mut None)?;
            operation.workflow.as_ref().map(|slot| slot.clone_with_work(work)).transpose()?
        };
        let Some(slot) = slot else { return Ok((None, 0)) };
        let Some(input) = slot.input else {
            return Ok((None, 0));
        };
        admission.limits.validate_file(&input, &mut None)?;
        if !crate::runtime::read_granted_with_work(&admission.grants, &input, work)? {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized,
                "task cannot read its retained workflow input"));
        }
        let (value, owned) = self.read_json_with_work(&input, work).await?;
        self.verify_owner(task, fence, false, work).await?;
        Ok((Some((slot.revision, value)), owned))
    }

    pub(crate) async fn settle_task(
        &self, task: TaskId, fence: &crate::scheduler::LeaseFence, outcome: Outcome<Value>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let fence = fence.clone_with_work(work)?;
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let admission = self.admission_with_work(operation_id, work).await?;
        let settlement = matches!(&outcome, Outcome::Cancelled | Outcome::Indeterminate { .. });
        self.verify_owner(task, &fence, settlement, work).await?;
        let maximum = if matches!(&outcome, Outcome::Succeeded(_)) {
            admission.limits.file_bytes.saturating_add(14)
        } else {
            acyclic_stream::MAX_RECORD_BYTES as u64
        };
        let digest = crate::contract::canonical_json_digest_with_work(&outcome, maximum, work)?;
        let fence_digest = crate::contract::canonical_json_digest_with_work(&fence, acyclic_stream::MAX_RECORD_BYTES as u64, work)?;
        let mut text = String::new();
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(178)?;
            io.reserve_text(&mut text, 178)?;
            io.copy_bytes(178)?;
        }
        use std::fmt::Write as _;
        write!(&mut text, "task-settle:{operation_id}:{}:{}",
            blake3::Hash::from_bytes(fence_digest).to_hex(),
            blake3::Hash::from_bytes(digest).to_hex()).expect("writing into String");
        let key = IdempotencyKey::new(text)?;
        let outcome = match outcome {
            Outcome::Succeeded(value) => {
                if work.is_some() {
                    let validator = crate::contract::compile_json_schema_with_work(
                        &admission.output_schema, "task output", work,
                    )?;
                    if !validator.is_valid(&value) {
                        return Err(admission_boundary(work, crate::wire::ErrorCode::Invalid,
                            "task output failed validation"));
                    }
                } else {
                    crate::tool::validate_value(&admission.output_schema, &value, "task output", None)?;
                }
                let bytes = crate::store::history::canonical_bytes_with_work(
                    &value, admission.limits.file_bytes, work,
                )?;
                if bytes.len() as u64 > admission.limits.file_bytes {
                    return Err(admission_boundary(work, crate::wire::ErrorCode::Invalid,
                        "task result exceeds admitted file limit"));
                }
                let staged = self.stage_payload_with_work(operation_id, key.as_str(), &bytes, work).await;
                let capacity = bytes.capacity() as u64;
                drop(bytes);
                let release = match work.as_deref_mut() {
                    Some(io) => io.release_owned(capacity),
                    None => Ok(()),
                };
                let reference = staged?;
                release?;
                Outcome::Succeeded(reference)
            }
            Outcome::Failed { message } => Outcome::Failed { message },
            Outcome::Cancelled => Outcome::Cancelled,
            Outcome::Indeterminate { operation_id } => Outcome::Indeterminate { operation_id },
        };
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if !settlement
            && admission.run_limits.deadline_epoch_ms.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline)
        {
            let mut text = String::new();
            if let Some(io) = work.as_deref_mut() { io.reserve_text(&mut text, 48)?; }
            write!(&mut text, "task-cancel:{operation_id}").expect("writing into String");
            coordinator.cancel_operation(
                self.verifier.audience(), &self.owner_scope, &self.verifier, operation_id,
                IdempotencyKey::new(text)?, true, work,
            ).await?;
            return Err(Error::Indeterminate(operation_id));
        }
        let current = coordinator.observe_operation(
            self.verifier.audience(), &self.owner_scope, &self.verifier, operation_id, work,
        )?;
        crate::scheduler::require_execution_owner(current, &fence, settlement, &mut None)?;
        coordinator.apply(
            operation_id, key,
            crate::scheduler::SchedulerEvent::Completed {
                operation_id, outcome, fence: Some(fence), execution_duration_ns: None,
            },
            work,
        ).await?;
        Ok(())
    }

    async fn verify_owner(
        &self, task_id: TaskId, fence: &crate::scheduler::LeaseFence, settlement: bool,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if !settlement {
            let operation = OperationId::from_bytes(task_id.into_bytes());
            let admitted = self.read_retained_admission_counted(operation, work).await.map(|(record, _)| record)?;
            if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
            if admitted.run_limits.deadline_epoch_ms.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline) {
                self.expire_task_deadline_with_work(task_id, Some(fence), work).await?;
                return Err(Error::Indeterminate(operation));
            }
        }
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let operation = coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            OperationId::from_bytes(task_id.into_bytes()),
            work,
        )?;
        crate::scheduler::require_execution_owner(operation, fence, settlement, &mut None)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) async fn verify_native_command_result(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut io = crate::store::history::native_work_io(work)?;
        self.coordinator.lock().await.journal_condition(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            OperationId::from_bytes(task.into_bytes()),
            fence,
            crate::distributed::JournalWrite::Fresh,
            &mut Some(&mut io),
        ).await.map(|_| ())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn require_preparation_query_with_native_work(
        &self, query: &Arc<crate::runtime::NativeTaskContext>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskId> {
        query.context().require_original_native_work(work)?;
        let task = query.context().durable_task_id().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "preparation query has no original SDK task binding",
        })?;
        if !query.native_metadata_matches(work)
            || query.context().id().into_bytes() != task.into_bytes()
            || !query.context().original_host_binding_matches(self, self.policy.as_deref())
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "preparation query has no original SDK host and allocation source" });
        }
        Ok(task)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn task_preparation_readonly_witness_with_native_work<'a>(
        self: &'a Arc<Self>, query: &'a Arc<crate::runtime::NativeTaskContext>,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, TaskAdmissionReadonlyWitness<P>>> {
        let task = self.require_preparation_query_with_native_work(query, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let witness = self.task_admission_readonly_witness_with_native_work(task, work).await?;
            witness.verify_query_with_native_work(query, work).await?;
            Ok(witness)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Authenticates an original task admission for bounded read-only preparation queries.
    pub async fn task_admission_readonly_witness_with_native_work(
        self: &Arc<Self>, task: TaskId, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskAdmissionReadonlyWitness<P>> {
        use crate::store::history::HistoryIo;
        let mut io = crate::store::history::native_work_io(work)?;
        let admission = self.admission_with_work(
            OperationId::from_bytes(task.into_bytes()), &mut Some(&mut io),
        ).await?;
        let scratch = if admission.parent.is_none() && admission.resource_proofs.is_empty() {
            None
        } else {
            let (scope, scope_bytes) = self.authenticated_scope_with_work(
                &admission, &mut Some(&mut io),
            ).await?;
            let access = scope.created_access();
            let scratch = access.as_ref().map(|access| access.current_content(&mut Some(&mut io)))
                .transpose()?.flatten().and_then(|binding| binding.scratch_creation.as_ref())
                .cloned().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "child preparation has no original scratch creation proof",
                })?;
            if scratch.original_task() != task {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "preparation scratch proof belongs to another task",
                });
            }
            drop(access);
            drop(scope);
            io.release_owned(scope_bytes)?;
            Some(scratch)
        };
        io.retain_owned(std::mem::size_of::<TaskAdmissionReadonlyWitness<P>>() as u64)?;
        Ok(TaskAdmissionReadonlyWitness {
            host: Arc::clone(self), admission, scratch,
            _native_custody: io.into_custody(),
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn verify_original_root_bootstrap_source_with_native_work(
        &self, original: &(OperationSpec, LeaseFence),
        root: &TaskAdmissionRecord, binding: &crate::store::VerifiedRootBinding,
        native: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut io = crate::store::history::native_work_io(native)?;
        let mut observer = Some(&mut io as &mut dyn crate::store::history::HistoryIo);
        let declaration = &original.0;
        declaration.verify_owner(self.verifier.audience(), &self.verifier, &mut observer)?;
        if root.parent.is_some() || !root.resource_proofs.is_empty()
            || declaration.operation_id != root.operation_id
            || binding.original_task() != crate::runtime::task_id_for_admission(root.operation_id)
            || binding.fence() != &original.1
            || declaration.owner_scope.agent() != Some(binding.agent())
            || self.owner_scope.agent() != Some(binding.agent())
        {
            return Err(admission_boundary(&mut observer, crate::wire::ErrorCode::Unauthorized,
                "bootstrap binding differs from its canonical original root reservation"));
        }
        let digest = crate::contract::canonical_json_digest_with_work(
            root, acyclic_stream::MAX_RECORD_BYTES as u64, &mut observer,
        )?;
        if digest != binding.admission_digest() {
            return Err(admission_boundary(&mut observer, crate::wire::ErrorCode::Unauthorized,
                "bootstrap root input differs from its protected original workflow"));
        }
        Ok(())
    }

    /// Restores only configured, originally allocated ROOT content.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) async fn verify_root_bootstrap_access_with_native_work(
        &self, root: &TaskAdmissionRecord,
        native: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<crate::resource_preparation::CreatedResourceAccess>> {
        use crate::resource_preparation::{CreatedResourceAccess, RootBootstrapContentContext};
        let mut access: Option<CreatedResourceAccess> = None;
        for provider in &self.resource_creation {
            for source in provider.root_bootstrap_sources(root.operation_id) {
                native.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
                let Some((request, witness)) = source else { continue; };
                let binding = witness.binding();
                let mut io = crate::store::history::native_work_io(native)?;
                let original = crate::distributed::original_reservation_proof_with_work(
                    &self.stream, self.verifier.audience(), &self.owner_scope, &self.verifier,
                    root.operation_id, binding.fence(), &mut Some(&mut io),
                ).await?;
                let context = RootBootstrapContentContext::from_verified_original_root_with_native_work(
                    self, &original, root, binding, native,
                ).await?;
                let received = provider.verify_root_bootstrap_with_native_work(
                    context, request, native,
                )?.await?;
                let next = CreatedResourceAccess::consume_root_bootstrap_with_native_work(
                    context, request, received.0, received.1, native,
                )?;
                access = Some(match access.take() {
                    Some(current) => current.merge_root_bootstrap_with_native_work(next, native)?,
                    None => next,
                });
                drop(original);
                drop(io);
            }
        }
        Ok(access)
    }

    async fn resume_scope_with_work(
        &self, task: TaskId, operation: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(RuntimeScope, u64)> {
        if task.into_bytes() != operation.into_bytes() {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized,
                "task identity does not match its admission"));
        }
        let admission = self.admission_with_work(operation, work).await?;
        let (scope, scope_bytes) = self.authenticated_scope_with_work(&admission, work).await?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let (scope, scope_bytes) = if admission.parent.is_none() && admission.resource_proofs.is_empty()
            && let Some(native) = work.as_deref().and_then(|io| io.original_native_work()).cloned()
        {
            match self.verify_root_bootstrap_access_with_native_work(&admission, &native).await? {
                Some(access) => {
                    let restored = scope.with_created_access_with_work(&admission, Some(access), work)?;
                    drop(scope);
                    if let Some(io) = work.as_deref_mut() { io.release_owned(scope_bytes)?; }
                    restored
                }
                None => (scope, scope_bytes),
            }
        } else { (scope, scope_bytes) };
        let deadline_epoch_ms = admission.run_limits.deadline_epoch_ms;
        let has_created = !admission.resource_proofs.is_empty();
        let admission_bytes = (std::mem::size_of::<TaskAdmissionRecord>() as u64)
            .checked_add(admission.logical_owned_bytes()?)
            .ok_or_else(|| admission_boundary(work, crate::wire::ErrorCode::Invalid,
                "resumed admission custody overflows"))?;
        drop(admission);
        if let Some(io) = work.as_deref_mut() { io.release_owned(admission_bytes)?; }
        if !has_created { return Ok((scope, scope_bytes)); }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = work.as_deref().and_then(|io| io.original_native_work()).cloned();
        if let Some(io) = work.as_deref_mut() {
            io.retain_owned(std::mem::size_of::<CreatedTaskPublicationLease<P>>() as u64)?;
        }
        let fence = {
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh(work).await?;
            let observed = coordinator.observe_operation(
                self.verifier.audience(), &self.owner_scope, &self.verifier, operation, work,
            )?;
            match observed.reservation.as_ref() {
                Some(reservation) => Some(crate::scheduler::LeaseFence {
                    reservation_id: match work.as_deref_mut() {
                        Some(io) => io.clone_text(&reservation.id)?,
                        None => reservation.id.clone(),
                    },
                    placement: match work.as_deref_mut() {
                        Some(io) => io.clone_text(&reservation.placement)?,
                        None => reservation.placement.clone(),
                    },
                }),
                None => None,
            }
        };
        let lease = self.created_publication_lease_with_work(operation, fence, deadline_epoch_ms, work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let scope = match native.as_ref() {
            Some(native) => scope.with_created_publication_lease_with_native_work(lease, native)?,
            None => scope.with_created_publication_lease(lease)?,
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let scope = scope.with_created_publication_lease(lease)?;
        Ok((scope, scope_bytes))
    }

    fn created_publication_lease_with_work(
        &self, operation: OperationId, fence: Option<crate::scheduler::LeaseFence>,
        deadline_epoch_ms: Option<u64>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Arc<dyn crate::resource_preparation::CreatedTaskLease>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = work.as_deref().is_some_and(|io| io.original_native_work().is_some());
        let mut lease = CreatedTaskPublicationLease {
            coordinator: Arc::clone(&self.coordinator),
            scope: self.owner_scope.copy(work)?,
            verifier: self.verifier.copy(work)?,
            operation, fence, clock: Arc::clone(&self.clock), deadline_epoch_ms,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _native_custody: None,
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if native {
            let mut bytes = (std::mem::size_of::<CreatedTaskPublicationLease<P>>() as u64)
                .checked_add(lease.scope.logical_owned_bytes()?)
                .and_then(|bytes| bytes.checked_add(lease.verifier.logical_owned_bytes().ok()?))
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "created publication lease custody overflows" })?;
            if let Some(fence) = &lease.fence {
                bytes = bytes.checked_add(fence.reservation_id.capacity() as u64)
                    .and_then(|bytes| bytes.checked_add(fence.placement.capacity() as u64))
                    .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                        reason: "created publication fence custody overflows" })?;
            }
            lease._native_custody = Some(work.as_deref_mut().expect("native lease source").take_native_custody(bytes)?);
        }
        if let Some(io) = work.as_deref_mut() {
            io.codec_charge(crate::contract::JsonCodecCharge::AllocationOperation {
                bytes: std::mem::size_of::<CreatedTaskPublicationLease<P>>() as u64,
            })?;
        }
        let lease = Arc::new(lease);
        Ok(lease)
    }

    /// Binds journal publications to the authenticated task's exact current lease.
    #[cfg(feature = "filesystem")]
    pub async fn journal_owner(
        self: &Arc<Self>,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
    ) -> Result<TaskJournalOwner<P>> {
        self.journal_owner_with_work(
            task_id, std::borrow::Cow::Owned(fence), None, &mut None,
        ).await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn journal_owner_with_native_work(
        self: &Arc<Self>, task: TaskId, fence: &crate::scheduler::LeaseFence,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskJournalOwner<P>> {
        let mut io = crate::store::history::native_work_io(work)?;
        self.journal_owner_with_work(
            task, std::borrow::Cow::Borrowed(fence), None, &mut Some(&mut io),
        ).await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn readonly_fork_journal_owner_with_native_work<'a>(
        self: &'a Arc<Self>,
        witness: &'a TaskAdmissionReadonlyWitness<P>,
        query: &'a Arc<crate::runtime::NativeTaskContext>,
        claim: (crate::filesystem::AllocationClaim, crate::resource_preparation::ResourcePreparationRequest, acyclic_fs::NativeAllocation),
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (
        Arc<TaskJournalOwner<P>>,
        (crate::resource_preparation::ResourcePreparationRequest, acyclic_fs::NativeAllocation),
    )>> {
        if !claim.2.matches_work(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "protected fork claim differs from its original allocation context" });
        }
        let failed = |(_, _, custody), cause| Error::NativeWork { cause, custody: Some(custody) };
        let qualified = self.require_preparation_query_with_native_work(query, work)
            .and_then(|_| witness.require_query_with_native_work(query, work));
        if let Err(error) = qualified {
            return Err(match error {
                Error::NativeWork { cause, .. } => failed(claim, cause),
                error => error,
            });
        }
        let mut pending = Some(claim);
        let bundle = crate::NativeFutureBundle::new(work, || {
            let claim = pending.take().expect("funded original fork claim");
            async move {
                let result: Result<Arc<TaskJournalOwner<P>>> = async {
                    let origin = claim.0.task_origin.as_ref().expect("authenticated original fork origin");
                    witness.verify_query_with_native_work(query, work).await?;
                    witness.verify_preparation_origin_with_native_work(
                        query, &claim.1, origin, work,
                    )?;
                    let mut io = crate::store::history::native_work_io(work)?;
                    let original = crate::distributed::original_reservation_proof_with_work(
                        &self.stream, self.verifier.audience(), &self.owner_scope, &self.verifier,
                        witness.admission_operation(), &origin.creation_fence,
                        &mut Some(&mut io),
                    ).await?;
                    let owner = self.journal_owner_with_work(
                        witness.task(), std::borrow::Cow::Borrowed(&origin.creation_fence),
                        Some(original), &mut Some(&mut io),
                    ).await?;
                    if owner.original_admission_digest != origin.admission_digest {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                            reason: "original Started admission differs from the protected fork claim" });
                    }
                    owner.require_task_context_binding_with_native_work(query.context(), work)?;
                    owner.verify_read_access(&mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
                    witness.verify_query_with_native_work(query, work).await?;
                    owner.into_native_shared(work)
                }.await;
                match result {
                    Ok(owner) => {
                        let (claim, request, mut custody) = claim;
                        let retired = crate::filesystem::UploadRecord::logical_owned_bytes(&claim)?;
                        drop(claim);
                        if let Err(cause) = custody.release(retired) {
                            drop(request);
                            return Err(Error::NativeWork { cause, custody: Some(custody) });
                        }
                        Ok((owner, (request, custody)))
                    },
                    Err(Error::NativeWork { cause, .. }) => Err(failed(claim, cause)),
                    Err(error) => Err(error),
                }
            }
        });
        match bundle {
            Err(Error::NativeWork { cause, .. }) => Err(failed(pending.take()
                .expect("unfunded original fork claim"), cause)),
            result => result,
        }
    }

    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    pub(crate) async fn readonly_journal_owner_with_native_work(
        self: &Arc<Self>, task: TaskId, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskJournalOwner<P>> {
        let witness = self.task_admission_readonly_witness_with_native_work(task, work).await?;
        let observation = crate::filesystem::observe_preparation(&witness, work)?.await?;
        let Some(recorded) = observation else {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::NotFound,
                reason: "protected original native preparation journal is unavailable" });
        };
        if recorded.task != task {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "protected native preparation belongs to another task" });
        }
        let mut io = crate::store::history::native_work_io(work)?;
        let original = crate::distributed::original_reservation_proof_with_work(
            &self.stream, self.verifier.audience(), &self.owner_scope, &self.verifier,
            witness.admission_operation(), &recorded.original_fence, &mut Some(&mut io),
        ).await?;
        self.journal_owner_with_work(
            task, std::borrow::Cow::Borrowed(&recorded.original_fence),
            Some(original), &mut Some(&mut io),
        ).await
    }

    #[cfg(feature = "filesystem")]
    async fn journal_owner_with_work(
        self: &Arc<Self>, task_id: TaskId,
        fence: std::borrow::Cow<'_, crate::scheduler::LeaseFence>,
        original: Option<(OperationSpec, LeaseFence)>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<TaskJournalOwner<P>> {
        let operation = OperationId::from_bytes(task_id.into_bytes());
        #[cfg(not(target_arch = "wasm32"))]
        let native = work.as_deref().and_then(|io| io.original_native_work()).cloned();
        #[cfg(not(target_arch = "wasm32"))]
        if native.is_some() {
            work.as_deref_mut().expect("original native observer")
                .retain_owned(std::mem::size_of::<TaskJournalOwner<P>>() as u64)?;
            if original.is_none() {
                self.verify_owner(task_id, &fence, true, work).await?;
            }
        }
        let (admission, declared_agent) = if let Some(original) = &original {
            if original.0.operation_id != operation || &original.1 != fence.as_ref() {
                return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized,
                    "original READ reservation differs from its protected native preparation"));
            }
            (self.read_declared_admission_counted(&original.0, work).await.map(|(record, _)| record)?,
                original.0.owner_scope.agent())
        } else {
            #[cfg(not(target_arch = "wasm32"))]
            if native.is_some() {
                let admission = self.admission_with_work(operation, work).await?;
                let coordinator = self.coordinator.lock().await;
                let observed = coordinator.observe_operation(
                    self.verifier.audience(), &self.owner_scope, &self.verifier, operation, work,
                )?;
                (admission, observed.spec.owner_scope.agent())
            } else {
                let declaration = self.retained_declaration_with_work(operation, work).await?
                    .ok_or_else(|| admission_boundary(work, crate::wire::ErrorCode::NotFound,
                        "task admission"))?;
                let admission = self.read_declared_admission_counted(&declaration, work).await.map(|(record, _)| record)?;
                (admission, declaration.owner_scope.agent())
            }
            #[cfg(target_arch = "wasm32")]
            {
                let declaration = self.retained_declaration_with_work(operation, work).await?
                    .ok_or_else(|| admission_boundary(work, crate::wire::ErrorCode::NotFound,
                        "task admission"))?;
                let admission = self.read_declared_admission_counted(&declaration, work).await.map(|(record, _)| record)?;
                (admission, declaration.owner_scope.agent())
            }
        };
        let (scope, _) = self.authenticated_scope_with_work(&admission, work).await?;
        let input_is_root = admission.parent.is_none() && admission.resource_proofs.is_empty();
        let input_agent = match scope.current_created_agent()? {
            Some(agent) => Some(agent),
            None if input_is_root => {
                if declared_agent != self.owner_scope.agent() {
                    return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized,
                        "root current actor differs from original declaration"));
                }
                declared_agent
            }
            None => None,
        };
        let request_digest = crate::contract::canonical_json_digest_with_work(&admission, acyclic_stream::MAX_RECORD_BYTES as u64, work)?;
        let state = {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(native) = &native {
                let state = self.machines.initialize_with_native_work(
                    &admission.machine, &admission.input, native,
                )?;
                if !state.1.work().shares_ledger(native) {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                        reason: "initial machine state differs from its original metadata ledger" });
                }
                work.as_deref_mut().expect("original native observer").absorb_native_custody(state.1)?;
                state.0
            } else {
                self.machines.resolve(&admission.machine, work)?
                    .ok_or_else(|| Error::Unsupported("pinned task machine is unavailable".into()))?
                    .initialize(&admission.input)?
            }
            #[cfg(target_arch = "wasm32")]
            {
                self.machines.resolve(&admission.machine, work)?
                    .ok_or_else(|| Error::Unsupported("pinned task machine is unavailable".into()))?
                    .initialize(&admission.input)?
            }
        };
        let workflow = crate::workflow::WorkflowAdmission {
            operation_id: admission.operation_id, request_digest,
            initial: crate::workflow::MachineCheckpoint { machine: admission.machine, revision: 0, state },
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = &native {
            self.machines.validate_checkpoint_with_native_work(&workflow.initial, native)?;
        } else {
            self.machines.validate_checkpoint(&workflow.initial, None)?;
        }
        #[cfg(target_arch = "wasm32")]
        self.machines.validate_checkpoint(&workflow.initial, None)?;
        let fence = match fence {
            std::borrow::Cow::Owned(fence) => fence,
            std::borrow::Cow::Borrowed(fence) => host_fence_with_work(fence, work)?,
        };
        let root_binding = if input_is_root {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(native) = &native {
                let slot = native.claim(std::mem::size_of::<RootBootstrapBindingWitness>() as u64)?;
                work.as_deref_mut().expect("original native observer").absorb_native_custody(slot)?;
            }
            Some(Arc::new(RootBootstrapBindingWitness {
                proof: std::sync::OnceLock::new(), gate: tokio::sync::Mutex::new(()),
                pending: std::sync::OnceLock::new(),
            }))
        } else { None };
        let mut owner = TaskJournalOwner {
            host: Arc::clone(self), stream: self.coordinator.lock().await.journal_client(),
            original_admission_digest: workflow.request_digest,
            task_id, fence, workflow,
            output_schema: admission.output_schema, input_grants: admission.grants,
            input_created: scope.created_access(), input_agent, root_binding,
            deadline_epoch_ms: admission.run_limits.deadline_epoch_ms, input_limits: admission.limits,
            extensions: admission.extensions,
            extension_schemas: self.root_scope.extension_schema_registry(),
            extension_runtime: self.root_scope.extension_runtime(),
            invocation_cancellation: None,
            #[cfg(not(target_arch = "wasm32"))]
            readonly_original: original,
            #[cfg(not(target_arch = "wasm32"))]
            _native_shared: false,
            #[cfg(not(target_arch = "wasm32"))]
            _native_custody: None,
        };
        #[cfg(not(target_arch = "wasm32"))]
        if native.is_some() {
            let owned = owner.native_owned_bytes()?;
            owner._native_custody = Some(work.as_deref_mut().expect("original native observer")
                .take_native_custody(owned)?);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = &native {
            owner.verify_read_access(&mut Some(&mut crate::store::history::native_work_io(native)?)).await?;
        } else {
            owner.verify(true, &mut None).await?;
        }
        #[cfg(target_arch = "wasm32")]
        owner.verify(true, &mut None).await?;
        Ok(owner)
    }
    /// Binds a trusted owner, its exact task/machine registries, and immutable admission payload provider.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        coordinator: DistributedCoordinator<P>,
        stream: std::sync::Arc<P>,
        payloads: Arc<dyn SchedulerPayloadStore>,
        reader: Arc<dyn ContentResidencyVerifier>,
        owner: Authority,
        owner_scope: Scope,
        verifier: AuthorityVerifier,
        root_scope: RuntimeScope,
        tasks: TaskRegistry,
        machines: MachineRegistry,
        clock: Arc<dyn UnixMillisClock>,
    ) -> Result<Self> {
        Self::new_observed(
            coordinator, stream, payloads, reader, &owner, owner_scope, verifier,
            root_scope, tasks, machines, clock, None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_observed(
        coordinator: DistributedCoordinator<P>, stream: std::sync::Arc<P>,
        payloads: Arc<dyn SchedulerPayloadStore>, reader: Arc<dyn ContentResidencyVerifier>,
        owner: &Authority, owner_scope: Scope, verifier: AuthorityVerifier,
        root_scope: RuntimeScope, tasks: TaskRegistry, machines: MachineRegistry,
        clock: Arc<dyn UnixMillisClock>,
        mut work: Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        verifier.verify_audience(owner, &mut work)?;
        verifier.verify(&owner_scope, &mut work)?;
        for capability in [
            capability::OPERATION_DECLARE,
            capability::OPERATION_OBSERVE,
            capability::OPERATION_CANCEL,
        ] {
            if !owner_scope.capabilities().contains(capability) {
                return Err(admission_boundary(&work, crate::wire::ErrorCode::Unauthorized,
                    "owner scope lacks a required operation capability"));
            }
        }
        if !root_scope.grants().is_subset_of(owner_scope.capabilities()) {
            return Err(admission_boundary(&work, crate::wire::ErrorCode::Unauthorized,
                "runtime grants exceed the signed owner scope"));
        }
        if let Some(observer) = work.as_deref_mut() {
            observer.retain_owned(std::mem::size_of::<Self>() as u64)?;
            observer.retain_owned(
                std::mem::size_of::<Mutex<DistributedCoordinator<P>>>()
                    .saturating_sub(std::mem::size_of::<DistributedCoordinator<P>>()) as u64,
            )?;
            observer.codec_charge(crate::contract::JsonCodecCharge::AllocationOperation {
                bytes: std::mem::size_of::<Mutex<DistributedCoordinator<P>>>() as u64,
            })?;
        }
        Ok(Self {
            coordinator: Arc::new(Mutex::new(coordinator)),
            stream,
            payloads,
            reader,
            owner_scope,
            verifier,
            root_scope,
            tasks,
            machines,
            clock,
            policy: None,
            policy_binding: None,
            effects: None,
            session_limits: None,
            resource_creation: Vec::new(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_configuration_custody: None,
        })
    }

    /// Builds the same configured owner from guarded copies on the original ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_native_work(
        coordinator: DistributedCoordinator<P>, stream: std::sync::Arc<P>,
        payloads: Arc<dyn SchedulerPayloadStore>, reader: Arc<dyn ContentResidencyVerifier>,
        owner: &Authority, owner_scope: &Scope, verifier: &AuthorityVerifier,
        root_scope: &RuntimeScope, tasks: &TaskRegistry, machines: &MachineRegistry,
        clock: Arc<dyn UnixMillisClock>, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        use crate::store::history::HistoryIo as _;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let scope_copy = owner_scope.copy(&mut Some(&mut io.history_io()))?;
        let verifier_copy = verifier.copy(&mut Some(&mut io.history_io()))?;
        let root_copy = root_scope.clone_with_native_work(work)?;
        io.absorb_native_custody(root_copy.1)?;
        let tasks = tasks.clone_with_native_work(work)?;
        let machines = machines.copy(work)?;
        io.absorb_native_custody(machines.1)?;
        let mut host = Self::new_observed(
            coordinator, stream, payloads, reader, owner, scope_copy, verifier_copy,
            root_copy.0, tasks, machines.0, clock, Some(&mut io.history_io()),
        )?;
        let retained = io.owned_bytes()?;
        host.native_configuration_custody = Some(io.into_native_custody(retained)?);
        Ok(host)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn into_native_shared(self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        if !self.native_configuration_custody.as_ref()
            .is_some_and(|custody| custody.work().shares_ledger(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "host configuration differs from its original work",
            });
        }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
        Ok(Arc::new(self))
    }

    /// Current trusted wall clock shared by admission, workers and effect gates.
    #[must_use]
    pub fn now_unix_millis(&self) -> u64 {
        self.clock.now_unix_millis()
    }

    /// Reads the configured owner's exact current operation without granting dispatch, publication or another reservation.
    pub async fn observe_task(&self, task: TaskId) -> Result<crate::scheduler::OperationState> {
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut None).await?;
        coordinator.observe_operation(
            self.verifier.audience(), &self.owner_scope, &self.verifier,
            OperationId::from_bytes(task.into_bytes()),
         &mut None,).cloned()
    }

    /// Reads the original owner's current cancellation flag.
    pub async fn cancellation_requested(
        &self, task: TaskId, native: crate::NativeWork<'_>,
    ) -> Result<bool> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut io = native.map(crate::store::history::native_work_io).transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut work = io.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let mut work: Option<&mut dyn crate::store::history::HistoryIo> = None;
        let _ = native;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut work).await?;
        let operation = coordinator.observe_operation(
            self.verifier.audience(), &self.owner_scope, &self.verifier,
            OperationId::from_bytes(task.into_bytes()), &mut work,
        )?;
        Ok(operation.cancellation_requested)
    }

    /// Cancels only when the original admitted absolute deadline has elapsed.
    pub async fn expire_task_deadline(
        &self,
        task: TaskId,
        fence: Option<crate::scheduler::LeaseFence>,
    ) -> Result<()> {
        self.expire_task_deadline_with_work(task, fence.as_ref(), &mut None).await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn expire_task_deadline_with_native_work(
        &self, task: TaskId, fence: Option<crate::scheduler::LeaseFence>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut io = crate::store::history::native_work_io(work)?;
        self.expire_task_deadline_with_work(task, fence.as_ref(), &mut Some(&mut io)).await
    }

    async fn expire_task_deadline_with_work(
        &self, task: TaskId, fence: Option<&crate::scheduler::LeaseFence>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let operation = OperationId::from_bytes(task.into_bytes());
        let admission = self.read_retained_admission_counted(operation, work).await.map(|(record, _)| record)?;
        let deadline = admission.run_limits.deadline_epoch_ms.ok_or_else(|| {
            admission_boundary(work, crate::wire::ErrorCode::Invalid, "task has no admitted deadline")
        })?;
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if self.clock.now_unix_millis() < deadline {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Conflict,
                "task deadline has not elapsed"));
        }
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let state = coordinator.observe_operation(
            self.verifier.audience(), &self.owner_scope, &self.verifier, operation, work,
        )?;
        if state.outcome.as_ref().is_some_and(|outcome| !matches!(outcome, Outcome::Indeterminate { .. })) {
            return Ok(());
        }
        if let Some(fence) = fence {
            if state.reservation.as_ref().is_none_or(|reservation|
                reservation.id != fence.reservation_id || reservation.placement != fence.placement)
            {
                return Err(admission_boundary(work, crate::wire::ErrorCode::Conflict,
                    "deadline observer does not own the current reservation"));
            }
        }
        if state.cancellation_requested {
            return Ok(());
        }
        let mut text = String::new();
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(48)?;
            io.reserve_text(&mut text, 48)?;
            io.copy_bytes(48)?;
        }
        use std::fmt::Write as _;
        write!(&mut text, "task-cancel:{operation}").expect("writing into String");
        coordinator.cancel_operation(
            self.verifier.audience(), &self.owner_scope, &self.verifier, operation,
            IdempotencyKey::new(text)?, true, work,
        ).await?;
        Ok(())
    }

    fn check_configuration_work(&self, work: crate::NativeWork<'_>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work
            && !self.native_configuration_custody.as_ref().is_some_and(|custody| custody.matches_work(work))
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "configuration differs from original host work" });
        }
        let _ = work;
        Ok(())
    }

    /// Registers the explicitly trusted creation verifiers.
    pub fn with_resource_creation_providers(
        mut self, providers: &[Arc<dyn crate::resource_preparation::ResourceCreationProvider>],
        work: crate::NativeWork<'_>,
    ) -> Result<Self> {
        self.check_configuration_work(work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut io = work.map(crate::store::history::native_work_io).transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut observer = io.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = None;
        if !self.resource_creation.is_empty() {
            return Err(admission_boundary(&observer, crate::wire::ErrorCode::Conflict,
                "resource creation registry is already configured"));
        }
        for (index, provider) in providers.iter().enumerate() {
            provider.provider().validate_with_work(&mut observer)?;
            if providers[..index].iter().any(|bound| bound.provider() == provider.provider()) {
                return Err(admission_boundary(&observer, crate::wire::ErrorCode::Invalid,
                    "resource creation provider is registered twice"));
            }
        }
        let mut quota = crate::wire_codec::CodecQuota::new(&mut observer, 0)?;
        self.resource_creation = crate::core::copy_vector(providers, &mut quota, |provider, _| Ok(Arc::clone(provider)))?;
        let retained = quota.retained();
        quota.transfer();
        drop(quota);
        drop(observer);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io.as_mut() {
            let mut custody = crate::store::history::HistoryIo::take_native_custody(io, retained)?;
            self.native_configuration_custody.as_mut().expect("checked original host custody").absorb(&mut custody)?;
        }
        let _ = retained;
        Ok(self)
    }

    /// Requires these immutable shared ceilings for every root admitted by this host.
    pub fn with_session_limits(
        mut self, limits: crate::scheduler::SessionLimits, work: crate::NativeWork<'_>,
    ) -> Result<Self> {
        self.check_configuration_work(work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut io = work.map(crate::store::history::native_work_io).transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut observer = io.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = None;
        limits.validate(&mut observer)?;
        self.session_limits = Some(limits);
        Ok(self)
    }

    /// Admits a registered root using its original native input/metadata custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn admit_with_native_work<'a>(
        &'a self, request: (TaskAdmissionRecord, acyclic_fs::NativeAllocation),
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Admission<TaskId>, acyclic_fs::NativeAllocation)>> {
        work.charge(Default::default())?;
        if request.0.parent.is_some() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native root admission cannot replace an owned child fence" });
        }
        let required = (std::mem::size_of::<TaskAdmissionRecord>() as u64)
            .checked_add(request.0.logical_owned_bytes()?)
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native root admission ownership overflows" })?;
        if !request.1.matches_work(work) || request.1.bytes() < required {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "root admission differs from its original native custody" });
        }
        crate::NativeFutureBundle::new(work, || async move {
            use crate::store::history::HistoryIo;
            let mut io = crate::store::history::native_work_io(work)?;
            let (record, custody) = request;
            io.absorb_native_custody(custody)?;
            io.retain_owned(std::mem::size_of::<Admission<TaskId>>() as u64)?;
            let admitted = self.admit_record_with_work(record, None, &mut Some(&mut io)).await?;
            let custody = io.take_native_custody(std::mem::size_of::<Admission<TaskId>>() as u64)?;
            Ok((admitted, custody))
        })
    }

    async fn admit_record_with_work(
        &self,
        admission: TaskAdmissionRecord,
        parent_fence: Option<crate::scheduler::LeaseFence>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Admission<TaskId>> {
        admission.validate_with_work(work)?;
        if !admission.resource_proofs.is_empty() && parent_fence.is_none()
            && self.retained_declaration_with_work(admission.operation_id, work).await?.is_none()
        {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized,
                "created resource admission requires the current parent execution fence"));
        }
        let operation_id = admission.operation_id;
        let parent = admission.parent;
        let (scope, _) = self.authenticated_scope_with_work(&admission, work).await?;
        self.validate_parent_scope_with_work(parent, &scope, work).await?;
        if parent.is_some() {
            let mut granted = false;
            for grant in scope.grants().iter() {
                if let Some(io) = work.as_deref_mut() {
                    io.check_cancellation()?;
                    io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
                }
                if grant.strip_prefix("task:spawn:")
                    .and_then(|grant| grant.strip_prefix(admission.task.name.as_str()))
                    .and_then(|grant| grant.strip_prefix('@')) == Some(admission.task.version.as_str())
                {
                    granted = true;
                    break;
                }
            }
            if !granted {
                return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized,
                    "parent scope lacks the exact descendant grant"));
            }
        }
        if let Some(extensions) = scope.extensions() { extensions.validate(work)?; }
        if scope.extensions() != self.root_scope.extensions() {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Conflict,
                "task extensions differ from owner bindings"));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
            self.policy_identity_with_native_work(native)?;
        }
        let matches = if work.is_some() {
            admission.policy.as_ref() == self.policy_binding.as_ref()
        } else { admission.policy == self.policy.as_ref().map(|policy| policy.identity()) };
        if !matches {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Conflict,
                "task admission policy differs from owner"));
        }
        if admission.execution.is_some() {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Unsupported,
                "local durable host cannot admit a remote execution route"));
        }
        if admission.machine.name != admission.task.name
            || admission.machine.version != admission.task.version
            || admission.machine.digest == [0; 32]
            || self.machines.resolve(&admission.machine, work)?.is_none()
        {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Invalid,
                "task and machine identities disagree"));
        }
        self.tasks.validate_durable_admission_with_work(
            &admission.task, &admission.machine, Some(&admission.input_schema),
            &admission.output_schema, Some(&admission.input), work,
        )?;
        let bytes = crate::store::history::canonical_bytes_with_work(
            &admission.canonical_payload(), acyclic_stream::MAX_RECORD_BYTES as u64, work,
        )?;
        if bytes.len() as u64 > scope.limits().file_bytes {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Invalid,
                "durable task admission exceeds its file limit"));
        }
        let mut key = String::new();
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(51)?;
            io.reserve_text(&mut key, 51)?;
            io.copy_bytes(51)?;
        }
        use std::fmt::Write as _;
        write!(&mut key, "task-admission:{operation_id}").expect("writing into String");
        let (state, owner_scope) = if let Some(retained) =
            self.retained_declaration_with_work(operation_id, work).await?
        {
            if !self.admission_bytes_match_with_work(&retained.state, &bytes, true, work).await? {
                return Err(admission_boundary(work, crate::wire::ErrorCode::Conflict,
                    "task admission changed on retry"));
            }
            (retained.state, retained.owner_scope)
        } else {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let native = work.as_deref().and_then(|io| io.original_native_work()).cloned();
            let state = self.stage_payload_with_work(operation_id, &key, &bytes, work).await?;
            state.descriptor().verify(&bytes, work)?;
            if !self.admission_bytes_match_with_work(&state, &bytes, false, work).await? {
                return Err(admission_boundary(work, crate::wire::ErrorCode::Storage,
                    "staged task admission changed before publication"));
            }
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let owner_scope = if native.is_some() {
                self.owner_scope.copy(work)?
            } else { self.owner_scope.clone() };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let owner_scope = self.owner_scope.clone();
            (state, owner_scope)
        };
        let byte_capacity = bytes.capacity() as u64;
        drop(bytes);
        if let Some(io) = work.as_deref_mut() { io.release_owned(byte_capacity)?; }
        let parent = if let Some(parent) = parent {
            let mut buffer = [0; 36];
            let slot = uuid::Uuid::from_bytes(operation_id.into_bytes())
                .hyphenated().encode_lower(&mut buffer);
            Some(ParentLink {
                operation_id: OperationId::from_bytes(parent.into_bytes()),
                slot: match work.as_deref_mut() {
                    Some(io) => io.clone_text(slot)?,
                    None => slot.to_owned(),
                },
            })
        } else { None };
        let authority = Authority {
            kind: self.verifier.audience().kind,
            id: match work.as_deref_mut() {
                Some(io) => io.clone_text(&self.verifier.audience().id)?,
                None => self.verifier.audience().id.clone(),
            },
        };
        if let Some(io) = work.as_deref_mut() {
            io.retain_owned(std::mem::size_of::<OperationSpec>() as u64)?;
        }
        let spec = OperationSpec {
            operation_id, owner_scope, parent,
            owner: DurableOwner::Attached { authority },
            entrypoint: EntrypointRef {
                name: admission.task.name, version: admission.task.version,
                digest: admission.task.digest, result_schema: admission.output_schema,
            },
            dependencies: BTreeSet::new(), resources: ResourceRequest::default(),
            placement: BTreeMap::new(), orchestration: Orchestration::Leaf, state,
        };
        let key = IdempotencyKey::new(key)?;
        let outcome = if let Some(fence) = parent_fence {
            self.coordinator.lock().await.declare_operation(
                self.verifier.audience(), &self.owner_scope, &self.verifier, spec, key, Some(fence), work,
            ).await
        } else { self.declare_task_with_work(spec, key, work).await };
        match outcome {
            Ok(_) => Ok(Admission::Accepted(TaskId::from_bytes(operation_id.into_bytes()))),
            Err(Error::Indeterminate(_)) => Ok(Admission::Indeterminate { operation_id }),
            Err(error) => Err(error),
        }
    }

    async fn admission_bytes_match_with_work(
        &self, reference: &FileRef, bytes: &[u8], retained: bool,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let loaded = self.read_json_payload_with_work(reference, quota.work()).await;
        let (value, actual, value_bytes, byte_retained) = match loaded {
            Err(Error::NotFound(_)) if retained => return Err(admission_boundary(
                quota.work(), crate::wire::ErrorCode::Storage, "committed task admission content is missing")),
            other => other?,
        };
        quota.adopt(value_bytes)?;
        quota.adopt(byte_retained)?;
        let matches = actual.as_ref() == bytes;
        drop(actual);
        drop(value);
        Ok(matches)
    }

    async fn declare_task_with_work(
        &self,
        spec: OperationSpec,
        key: IdempotencyKey,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        let mut coordinator = self.coordinator.lock().await;
        if let Some(limits) = self.session_limits.filter(|_| spec.parent.is_none()) {
            coordinator
                .declare_session(
                    self.verifier.audience(),
                    &self.owner_scope,
                    &self.verifier,
                    spec,
                    limits,
                    key,
                    work,
                )
                .await
        } else {
            coordinator
                .declare_operation(self.verifier.audience(), &self.owner_scope, &self.verifier, spec, key, None, work)
                .await
        }
    }

    /// Pins the policy enforced by this owner, independently of caller contexts.
    pub fn with_policy(mut self, policy: Arc<dyn ToolPolicy>, work: crate::NativeWork<'_>) -> Result<Self> {
        self.check_configuration_work(work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            policy.supports_native_policy(work)?;
            let mut identity = policy.identity_with_native_work(work)?;
            crate::runtime::validate_policy_identity_with_native_work(&identity.0, &identity.1, work)?;
            self.native_configuration_custody.as_mut().expect("checked original host custody").absorb(&mut identity.1)?;
            self.policy_binding = Some(identity.0);
            self.policy = Some(policy);
            return Ok(self);
        }
        let identity = policy.identity();
        crate::runtime::validate_policy_identity(&identity)?;
        self.policy_binding = Some(identity);
        self.policy = Some(policy);
        Ok(self)
    }

    /// Binds the conversation-owned provider effect reconciler.
    #[must_use]
    pub fn with_effects(mut self, effects: Arc<dyn DurableEffectObserver>) -> Self {
        self.effects = Some(effects);
        self
    }

    pub(crate) async fn authenticated_scope_with_work(
        &self, admission: &TaskAdmissionRecord,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(RuntimeScope, u64)> {
        let mut visited = Vec::new();
        let access = self.verify_created_access_with_work(admission, &mut visited, work).await;
        let visited_bytes = (visited.capacity() * std::mem::size_of::<OperationId>()) as u64;
        drop(visited);
        if let Some(io) = work.as_deref_mut() { io.release_owned(visited_bytes)?; }
        self.root_scope.with_created_access_with_work(admission, access?, work)
    }

    fn reserve_proof_slots<T>(
        values: &mut Vec<T>, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<u64> {
        if values.len() < values.capacity() { return Ok(0); }
        let target = values.capacity().checked_mul(2).unwrap_or(usize::MAX).max(4);
        let bytes = target.checked_sub(values.capacity())
            .and_then(|slots| slots.checked_mul(std::mem::size_of::<T>()))
            .ok_or_else(|| admission_boundary(
                work, crate::wire::ErrorCode::Invalid, "resource proof vector ownership overflows",
            ))? as u64;
        let old = values.capacity();
        if let Some(io) = work.as_deref_mut() { io.admit_allocation(bytes)?; }
        values.try_reserve_exact(target - values.len()).map_err(|cause| {
            match work.as_deref() {
                Some(io) => io.allocation_failure(cause),
                None => Error::Storage(cause.to_string()),
            }
        })?;
        let retained = (values.capacity() - old).checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| admission_boundary(
                work, crate::wire::ErrorCode::Invalid, "resource proof vector ownership overflows",
            ))? as u64;
        if let Some(io) = work.as_deref_mut() { io.retain_owned(retained)?; }
        Ok(retained)
    }

    fn proof_contains<T: PartialEq>(
        values: &[T], wanted: &T,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        for value in values {
            if let Some(io) = work.as_deref_mut() {
                io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
            }
            if value == wanted { return Ok(true); }
        }
        Ok(false)
    }

    fn verify_created_access_with_work<'a, 'b>(
        &'a self, admission: &'a TaskAdmissionRecord, visited: &'a mut Vec<OperationId>,
        work: &'a mut Option<&'b mut dyn crate::store::history::HistoryIo>,
    ) -> impl std::future::Future<Output = Result<Option<crate::resource_preparation::CreatedResourceAccess>>>
        + acyclic_stream::ProviderTask + 'a {
        async move {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if admission.resource_proofs.is_empty() { return Ok(None); }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()).cloned() {
            return crate::NativeFutureBundle::new(&native, || {
                self.verify_created_access_body(admission, visited, work)
            })?.await;
        }
        Box::pin(self.verify_created_access_body(admission, visited, work)).await
        }
    }

    async fn verify_created_access_body(
        &self, admission: &TaskAdmissionRecord, visited: &mut Vec<OperationId>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<crate::resource_preparation::CreatedResourceAccess>> {
        use crate::wire::ErrorCode;
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if admission.resource_proofs.is_empty() { return Ok(None); }
        if Self::proof_contains(visited, &admission.operation_id, work)? {
            return Err(admission_boundary(work, ErrorCode::Unauthorized,
                "resource preparation proof is cyclic"));
        }
        Self::reserve_proof_slots(visited, work)?;
        visited.push(admission.operation_id);
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let parent = admission.parent.ok_or_else(|| admission_boundary(
            custody.work(), ErrorCode::Unauthorized, "created resource admission has no parent",
        ))?;
        let (parent_admission, held) = self.read_retained_admission_counted(
            OperationId::from_bytes(parent.into_bytes()), custody.work(),
        ).await?;
        custody.adopt(held)?;
        let mut receipts = Vec::new();
        let mut current_default: Option<(usize, OperationId)> = None;
        for proof in &admission.resource_proofs {
            let mut selected = None;
            for provider in &self.resource_creation {
                if let Some(io) = custody.work().as_deref_mut() {
                    io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
                }
                if provider.provider() == &proof.provider {
                    selected = Some(provider);
                    break;
                }
            }
            let provider = selected.ok_or_else(|| admission_boundary(
                custody.work(), ErrorCode::Unsupported, "resource proof verifier is not bound",
            ))?;
            let mut proof_custody = crate::wire_codec::CodecQuota::new(custody.work(), 0)?;
            let (preparation, held) = self.read_retained_admission_counted(
                OperationId::from_bytes(proof.preparation.into_bytes()), proof_custody.work(),
            ).await?;
            proof_custody.adopt(held)?;
            let (request, held) =
                crate::resource_preparation::ResourcePreparationRequest::from_borrowed_value_with_work(
                    &preparation.input, proof_custody.work(),
                )?;
            proof_custody.adopt(held)?;
            if preparation.parent != request.child.parent
                || preparation.operation_id.into_bytes() != proof.preparation.into_bytes()
                || request.provider != proof.provider
                || !request.resources.contains(&proof.resource)
                || !request.child.resource_proofs.contains(proof)
                || !preparation.grants.contains(&request.creation_grant)
            {
                return Err(admission_boundary(proof_custody.work(), ErrorCode::Unauthorized,
                    "resource proof differs from admitted preparation"));
            }
            let preparation_access = self.verify_created_access_with_work(
                &preparation, visited, proof_custody.work(),
            ).await?;
            let (preparation_scope, scope_held) = self.root_scope.with_created_access_with_work(
                &preparation, preparation_access, proof_custody.work(),
            )?;
            drop(preparation_scope);
            if let Some(io) = proof_custody.work().as_deref_mut() { io.release_owned(scope_held)?; }
            let preparation_parent = preparation.parent.ok_or_else(|| admission_boundary(
                proof_custody.work(), ErrorCode::Unauthorized, "resource preparation has no original parent",
            ))?;
            let (original_parent, held) = self.read_retained_admission_counted(
                OperationId::from_bytes(preparation_parent.into_bytes()), proof_custody.work(),
            ).await?;
            proof_custody.adopt(held)?;
            if !original_parent.grants.contains(&request.creation_grant)
                || !preparation.grants.is_subset_of(&original_parent.grants)
            {
                return Err(admission_boundary(proof_custody.work(), ErrorCode::Unauthorized,
                    "creation authority was not narrowed from the parent"));
            }
            let inherited = request.child != *admission;
            let mut origin_distance = 0;
            if inherited {
                if !parent_admission.resource_proofs.contains(proof) {
                    return Err(admission_boundary(proof_custody.work(), ErrorCode::Unauthorized,
                        "created resource proof is not inherited from the parent"));
                }
                let (original_child, held) = self.read_retained_admission_counted(
                    request.child.operation_id, proof_custody.work(),
                ).await?;
                proof_custody.adopt(held)?;
                let matches = original_child == request.child;
                drop(original_child);
                proof_custody.relinquish(held)?;
                if !matches {
                    return Err(admission_boundary(proof_custody.work(), ErrorCode::Unauthorized,
                        "created resource origin admission changed"));
                }
            }
            {
                let mut coordinator = self.coordinator.lock().await;
                coordinator.refresh(proof_custody.work()).await?;
                let prepared = coordinator.observe_operation(
                    self.verifier.audience(), &self.owner_scope, &self.verifier,
                    preparation.operation_id, proof_custody.work(),
                )?;
                if prepared.cancellation_requested
                    || !matches!(prepared.outcome, Some(Outcome::Succeeded(_)))
                {
                    return Err(admission_boundary(proof_custody.work(), ErrorCode::Unauthorized,
                        "resource preparation did not succeed"));
                }
                if inherited {
                    let mut lineage_custody = crate::wire_codec::CodecQuota::new(proof_custody.work(), 0)?;
                    let mut lineage = Vec::new();
                    let mut cursor = Some(OperationId::from_bytes(parent.into_bytes()));
                    let mut found = false;
                    while let Some(operation) = cursor {
                        if let Some(io) = lineage_custody.work().as_deref_mut() {
                            io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
                        }
                        if operation == request.child.operation_id {
                            origin_distance = lineage.len() + 1;
                            found = true;
                            break;
                        }
                        if Self::proof_contains(&lineage, &operation, lineage_custody.work())? {
                            return Err(admission_boundary(lineage_custody.work(), ErrorCode::Conflict,
                                "indexed operation lineage is cyclic"));
                        }
                        let held = Self::reserve_proof_slots(&mut lineage, lineage_custody.work())?;
                        lineage_custody.adopt(held)?;
                        lineage.push(operation);
                        let state = coordinator.observe_operation(
                            self.verifier.audience(), &self.owner_scope, &self.verifier, operation,
                            lineage_custody.work(),
                        )?;
                        cursor = state.spec.parent.as_ref().map(|link| link.operation_id);
                    }
                    if !found {
                        return Err(admission_boundary(lineage_custody.work(), ErrorCode::Unauthorized,
                            "resource proof belongs to another child lineage"));
                    }
                }
            }
            let receipt;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(native) = proof_custody.work().as_deref()
                .and_then(|io| io.original_native_work()).cloned()
            {
                let (verified, guard) = provider.verify_with_native_work(&request, proof, &native)?.await?;
                if !guard.matches_work(&native) {
                    drop(verified);
                    return Err(Error::NativeWork {
                        cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                        custody: Some(guard),
                    });
                }
                proof_custody.work().as_deref_mut().expect("native proof observer")
                    .absorb_native_custody(guard)?;
                receipt = verified;
            } else {
                receipt = provider.verify(&request, proof).await?;
            }
            #[cfg(any(not(feature = "filesystem"), target_arch = "wasm32"))]
            { receipt = provider.verify(&request, proof).await?; }
            if &receipt.origin.owner != self.verifier.audience()
                || receipt.origin.parent_task != preparation_parent
                || receipt.origin.preparation_task != proof.preparation
                || receipt.origin.intended_child != request.child.operation_id
                || receipt.origin.admission_digest != crate::contract::canonical_json_digest_with_work(&preparation, acyclic_stream::MAX_RECORD_BYTES as u64, proof_custody.work())?
                || receipt.origin.request_digest != crate::contract::canonical_json_digest_with_work(&request, acyclic_stream::MAX_RECORD_BYTES as u64, proof_custody.work())?
            {
                return Err(admission_boundary(proof_custody.work(), ErrorCode::Unauthorized,
                    "resource allocation provenance changed"));
            }
            if receipt.content.iter().any(|binding| binding.default_writer)
                && current_default.is_none_or(|(distance, _)| origin_distance < distance)
            {
                current_default = Some((origin_distance, receipt.origin.intended_child));
            }
            drop(original_parent);
            drop(request);
            drop(preparation);
            drop(proof_custody);
            Self::reserve_proof_slots(&mut receipts, custody.work())?;
            receipts.push(receipt);
        }
        visited.pop();
        let (access, _) = crate::resource_preparation::CreatedResourceAccess::consume(
            admission, &mut receipts, current_default.map(|(_, origin)| origin), custody.work(),
        )?;
        Ok(Some(access))
    }

    async fn read_json_with_work(
        &self, reference: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Value,u64)> {
        let (value, bytes, retained, byte_retained) = self.read_json_payload_with_work(reference, work).await?;
        drop(bytes);
        if let Some(io) = work.as_deref_mut() { io.release_owned(byte_retained)?; }
        Ok((value, retained))
    }

    async fn read_json_payload_with_work(
        &self, reference: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Value, acyclic_stream::OwnedBytes, u64, u64)> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
            reference.validate(&mut Some(&mut crate::store::history::native_work_io(native)?))?;
        } else {
            reference.validate(&mut None)?;
        }
        #[cfg(any(not(feature = "filesystem"), target_arch = "wasm32"))]
        reference.validate(&mut None)?;
        if reference.descriptor().media_type() != "application/json" {
            return Err(admission_boundary(
                work, crate::wire::ErrorCode::Invalid, "durable task payload is not JSON",
            ));
        }
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let (bytes, byte_retained) = match custody.work().as_deref_mut() {
            Some(io) => io.read_content(self.reader.as_ref(), reference)?.await?,
            None => (acyclic_stream::OwnedBytes::from(self.reader.read(reference, reference.descriptor().byte_length(), None)?.await?), 0),
        };
        custody.adopt(byte_retained)?;
        reference.descriptor().verify(&bytes, custody.work())?;
        let (value, retained) = match custody.work().as_deref_mut() {
            Some(io) => io.decode_json_value(&bytes)?,
            None => (crate::contract::json_from_slice(&bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?, 0),
        };
        custody.adopt(retained)?;
        let canonical = crate::store::history::canonical_bytes_with_work(
            &value, reference.descriptor().byte_length(), custody.work(),
        )?;

        let canonical_capacity = canonical.capacity() as u64;
        custody.adopt(canonical_capacity)?;
        let differs = canonical.as_slice() != bytes.as_ref();
        drop(canonical);
        custody.relinquish(canonical_capacity)?;
        if differs {
            return Err(admission_boundary(
                custody.work(), crate::wire::ErrorCode::Invalid, "durable task payload is not canonical JSON",
            ));
        }
        custody.transfer();
        Ok((value, bytes, retained, byte_retained))
    }

    async fn retained_declaration_with_work(
        &self,
        operation_id: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<OperationSpec>> {
        let mut declaration_key_capacity = 0;
        let declaration_key = if let Some(io) = work.as_deref_mut() {
            const PREFIX: &str = "task-admission:";
            let mut uuid = [0; 36];
            let operation = uuid::Uuid::from_bytes(operation_id.into_bytes()).hyphenated().encode_lower(&mut uuid);
            let mut text = String::new();
            io.admit_copy((PREFIX.len() + operation.len()) as u64)?;
            io.reserve_text(&mut text, PREFIX.len() + operation.len())?;
            io.copy_bytes((PREFIX.len() + operation.len()) as u64)?;
            text.push_str(PREFIX);
            text.push_str(operation);
            declaration_key_capacity = text.capacity() as u64;
            IdempotencyKey::new(text)?
        } else {
            IdempotencyKey::new(format!("task-admission:{operation_id}"))?
        };
        let indexed = crate::distributed::observe_declaration(
            &self.stream,
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            operation_id,
            &declaration_key,
            work,
        )
        .await;
        drop(declaration_key);
        let released = match work.as_deref_mut() {
            Some(io) => io.release_owned(declaration_key_capacity),
            None => Ok(()),
        };
        let indexed = match indexed {
            Ok(indexed) => { released?; indexed }
            Err(error) => return Err(error),
        };
        if indexed.is_some() {
            return Ok(indexed);
        }
        {
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh(work).await?;
            match coordinator.observe_operation(
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                operation_id,
                work,
            ) {
                Ok(operation) => Ok(Some(operation.spec.clone_with_work(work)?)),
                Err(Error::NotFound(_)) => Ok(None),
                Err(error) => Err(error),
            }
        }
    }

    async fn read_retained_admission_counted(
        &self, operation_id: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(TaskAdmissionRecord, u64)> {
        let spec = self.retained_declaration_with_work(operation_id, work).await?
            .ok_or_else(|| admission_boundary(
                work, crate::wire::ErrorCode::NotFound, "task admission",
            ))?;
        let bytes = if work.is_some() { spec.logical_owned_bytes()? } else { 0 };
        let admission = self.read_declared_admission_counted(&spec, work).await;
        drop(spec);
        let release = match work.as_deref_mut() {
            Some(io) => io.release_owned(bytes),
            None => Ok(()),
        };
        match admission {
            Ok(admission) => { release?; Ok(admission) }
            Err(error) => Err(error),
        }
    }

    async fn read_declared_admission_counted(
        &self, spec: &OperationSpec,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(TaskAdmissionRecord, u64)> {
        spec.verify_owner(self.verifier.audience(), &self.verifier, work)?;
        let operation_id = spec.operation_id;
        if let Some(limits) = self.session_limits {
            let mut coordinator = self.coordinator.lock().await;
            let retained = match coordinator.scheduler().session_limits(operation_id, work) {
                Err(error) if error.code() == crate::wire::ErrorCode::NotFound => {
                    coordinator.refresh(work).await?;
                    coordinator.scheduler().session_limits(operation_id, work)?
                }
                other => other?,
            };
            if retained != limits {
                return Err(admission_boundary(
                    work, crate::wire::ErrorCode::Conflict,
                    "retained session ceilings differ from owner bindings",
                ));
            }
        }
        let (value, held) = self.read_json_with_work(&spec.state, work).await.map_err(|error| match error {
            Error::NotFound(_) => admission_boundary(work, crate::wire::ErrorCode::Storage,
                "committed task admission content is missing"),
            other => other,
        })?;
        let (admission, added) = TaskAdmissionRecord::from_canonical_value_counted_with_work(value, work)?;
        let held = held.checked_add(added).ok_or_else(|| admission_boundary(
            work, crate::wire::ErrorCode::Invalid, "task admission custody overflows",
        ))?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let held = if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
            let actual = (std::mem::size_of::<TaskAdmissionRecord>() as u64)
                .checked_add(admission.logical_owned_bytes()?).ok_or_else(|| admission_boundary(
                    work, crate::wire::ErrorCode::Invalid, "task admission custody overflows",
                ))?;
            let retired = held.checked_sub(actual).ok_or_else(|| admission_boundary(
                work, crate::wire::ErrorCode::Invalid, "task admission producer custody is undersized",
            ))?;
            work.as_deref_mut().expect("native admission observer").release_owned(retired)?;
            actual
        } else { held };
        let policy_matches = if work.is_some() {
            admission.policy.as_ref() == self.policy_binding.as_ref()
        } else {
            admission.policy == self.policy.as_ref().map(|policy| policy.identity())
        };
        if !policy_matches {
            return Err(admission_boundary(
                work, crate::wire::ErrorCode::Conflict,
                "durable task policy differs from its admission",
            ));
        }
        if let Some(extensions) = &admission.extensions {
            extensions.validate(work)?;
        }
        if admission.extensions.as_ref() != self.root_scope.extensions() {
            return Err(admission_boundary(
                work, crate::wire::ErrorCode::Conflict,
                "retained task extensions differ from owner bindings",
            ));
        }
        let mut operation_text = [0; 36];
        let operation_text: &str = uuid::Uuid::from_bytes(operation_id.into_bytes())
            .hyphenated().encode_lower(&mut operation_text);
        let parent_matches = match (&spec.parent, admission.parent) {
            (None, None) => true,
            (Some(link), Some(parent)) =>
                link.operation_id == OperationId::from_bytes(parent.into_bytes())
                    && link.slot == operation_text,
            _ => false,
        };
        if admission.operation_id != operation_id
            || admission.task.name != spec.entrypoint.name
            || admission.task.version != spec.entrypoint.version
            || admission.task.digest != spec.entrypoint.digest
            || admission.output_schema != spec.entrypoint.result_schema
            || !parent_matches
            || admission.machine.name != admission.task.name
            || admission.machine.version != admission.task.version
            || self.machines.resolve(&admission.machine, &mut None)?.is_none()
        {
            return Err(admission_boundary(
                work, crate::wire::ErrorCode::Conflict,
                "durable task admission disagrees with its declaration",
            ));
        }
        self.tasks.validate_durable_admission_with_work(
            &admission.task,
            &admission.machine,
            Some(&admission.input_schema),
            &admission.output_schema,
            Some(&admission.input),
            work,
        )?;
        Ok((admission, held))
    }

    pub(crate) async fn admission_with_work(
        &self, operation_id: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<TaskAdmissionRecord> {
        let admission = self.read_retained_admission_counted(operation_id, work).await.map(|(record, _)| record)?;
        let (scope, held) = self.authenticated_scope_with_work(&admission, work).await?;
        drop(scope);
        if let Some(io) = work.as_deref_mut() { io.release_owned(held)?; }
        Ok(admission)
    }

    pub(crate) async fn validate_parent_scope_with_work(
        &self, parent: Option<TaskId>, scope: &RuntimeScope,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if let Some(parent) = parent {
            let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
            let (admitted, held) = self.read_retained_admission_counted(
                OperationId::from_bytes(parent.into_bytes()), custody.work(),
            ).await?;
            custody.adopt(held)?;
            let (authenticated, scope_held) = self.authenticated_scope_with_work(&admitted, custody.work()).await?;
            custody.adopt(scope_held)?;
            drop(authenticated);
            custody.relinquish(scope_held)?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(native) = custody.work().as_deref().and_then(|io| io.original_native_work()) {
                (&scope.limits()).validate(&mut Some(&mut crate::store::native_work_io(native).map_err(|cause| crate::Error::NativeWork { cause, custody: None })?))?;
                (&scope.run_limits()).validate(&mut Some(&mut crate::store::native_work_io(native).map_err(|cause| crate::Error::NativeWork { cause, custody: None })?))?;
                let parent_limits = admitted.limits;
                let child_limits = scope.limits();
                let mut grants_match = true;
                for grant in scope.grants().iter() {
                    native.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
                    if !admitted.grants.contains(grant)
                        && !scope.created_grants().is_some_and(|created| created.contains(grant))
                    {
                        grants_match = false;
                        break;
                    }
                }
                let limits_match = child_limits.file_bytes <= parent_limits.file_bytes
                    && child_limits.path_bytes <= parent_limits.path_bytes
                    && child_limits.attachments <= parent_limits.attachments
                    && child_limits.render_bytes <= parent_limits.render_bytes
                    && child_limits.model_steps <= parent_limits.model_steps
                    && child_limits.model_events_per_step <= parent_limits.model_events_per_step
                    && child_limits.tool_calls_per_step <= parent_limits.tool_calls_per_step
                    && child_limits.context_messages <= parent_limits.context_messages;
                let parent_run = admitted.run_limits;
                let child_run = scope.run_limits();
                let runs_match = [
                    (parent_run.concurrency.map(|v| v as u64), child_run.concurrency.map(|v| v as u64)),
                    (parent_run.max_steps.map(|v| v as u64), child_run.max_steps.map(|v| v as u64)),
                    (parent_run.deadline_epoch_ms, child_run.deadline_epoch_ms),
                ].into_iter().all(|(bound, child)| bound.is_none_or(|bound|
                    child.is_some_and(|child| child <= bound)));
                if !grants_match || !limits_match || !runs_match {
                    return Err(admission_boundary(
                        custody.work(), crate::wire::ErrorCode::Unauthorized,
                        "runtime scope cannot widen ancestor authority",
                    ));
                }
                return Ok(());
            }
            let allowed = Capabilities::new(admitted.grants.iter().chain(
                scope.created_grants().into_iter().flat_map(Capabilities::iter),
            ));
            RuntimeScope::new(allowed, admitted.limits)?
                .with_run_limits(admitted.run_limits, &mut None)?
                .narrow(scope.grants().clone(), scope.limits())?
                .with_run_limits(scope.run_limits(), &mut None)?;
        }
        Ok(())
    }



    fn mailbox(&self, task_id: TaskId) -> Result<acyclic_stream::Stream<P>> {
        acyclic_stream::Stream::new(std::sync::Arc::clone(&self.stream), format!("harness/v3/mail/by-recipient/{task_id}"))
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    fn batch_stream(&self, batch_id: BatchId) -> Result<acyclic_stream::Stream<P>> {
        acyclic_stream::Stream::new(std::sync::Arc::clone(&self.stream), format!("harness/v2/batches/{batch_id}"))
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    fn batch_cancellation_stream(&self, batch_id: BatchId) -> Result<acyclic_stream::Stream<P>> {
        acyclic_stream::Stream::new(std::sync::Arc::clone(&self.stream), format!("harness/v2/batch-cancellations/{batch_id}"))
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    async fn retained_batch_cancellation(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<BatchCancellation>> {
        let stream = self.batch_cancellation_stream(batch_id)?;
        let bounds = match stream.bounds().await {
            Ok(bounds) => bounds,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        if bounds.tail != 1 {
            return Err(Error::Storage(
                "batch cancellation history is not exactly retained".into(),
            ));
        }
        let records = stream.read(0, 2).await?.try_collect::<Vec<_>>().await?;
        let [record] = records.as_slice() else {
            return Err(Error::Storage(
                "batch cancellation record is missing".into(),
            ));
        };
        let declaration: BatchCancellation = crate::contract::json_from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if declaration.batch_id != batch_id
            || crate::contract::canonical_json_bytes(&declaration)? != record.value.as_ref()
        {
            return Err(Error::Storage(
                "batch cancellation record is not canonical".into(),
            ));
        }
        Ok(Some(declaration))
    }

    async fn declare_batch_cancellation(&self, request: &DurableBatchRequest) -> Result<()> {
        let declaration = BatchCancellation {
            batch_id: request.batch_id,
            group_id: request.group_id,
        };
        let bytes = crate::contract::canonical_json_bytes(&declaration)?;
        let operation_id = OperationId::from_bytes(request.batch_id.into_bytes());
        self.publish_control(
            &self.batch_cancellation_stream(request.batch_id)?,
            "batch-cancel",
            TaskId::from_bytes(request.batch_id.into_bytes()),
            operation_id,
            &bytes,
        )
        .await?;
        if self.retained_batch_cancellation(request.batch_id).await? != Some(declaration) {
            return Err(Error::Conflict(
                "batch cancellation belongs to another group".into(),
            ));
        }
        Ok(())
    }

    async fn retained_batch(&self, batch_id: BatchId) -> Result<Option<Value>> {
        let stream = self.batch_stream(batch_id)?;
        let bounds = match stream.bounds().await {
            Ok(bounds) => bounds,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        if bounds.tail != 1 {
            return Err(Error::Storage(
                "batch manifest history is not exactly retained".into(),
            ));
        }
        let records = stream.read(0, 2).await?.try_collect::<Vec<_>>().await?;
        let [record] = records.as_slice() else {
            return Err(Error::Storage("batch manifest record is missing".into()));
        };
        let reference: FileRef = crate::contract::json_from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if crate::contract::canonical_json_bytes(&reference)? != record.value.as_ref() {
            return Err(Error::Storage("batch manifest ref is not canonical".into()));
        }
        self.read_json_with_work(&reference, &mut None).await.map(|(value, _)| value)
            .map_err(|error| match error {
                Error::NotFound(_) => {
                    Error::Storage("committed batch manifest content is missing".into())
                }
                other => other,
            })
            .map(Some)
    }

    async fn commit_batch(&self, request: &DurableBatchRequest) -> Result<()> {
        let canonical = request.canonical_value();
        let bytes = crate::contract::canonical_json_bytes(&canonical)?;
        if bytes.len() as u64 > request.scope.limits().file_bytes {
            return Err(Error::Invalid("batch manifest exceeds file limit".into()));
        }
        let operation_id = OperationId::from_bytes(request.batch_id.into_bytes());
        let key = format!("batch-request:{}", request.batch_id);
        let reference = self.payloads.stage(operation_id, &key, &bytes).await?;
        reference.descriptor().verify(&bytes, &mut None)?;
        if self.read_json_with_work(&reference, &mut None).await.map(|(value, _)| value)? != canonical {
            return Err(Error::Storage("staged batch request changed".into()));
        }
        let published = crate::contract::canonical_json_bytes(&reference)?;
        self.publish_control(
            &self.batch_stream(request.batch_id)?,
            "batch",
            TaskId::from_bytes(request.batch_id.into_bytes()),
            operation_id,
            &published,
        )
        .await?;
        if self.retained_batch(request.batch_id).await? != Some(canonical) {
            return Err(Error::Conflict(
                "batch identity belongs to another request".into(),
            ));
        }
        Ok(())
    }

    async fn validate_batch(&self, request: &DurableBatchRequest) -> Result<()> {
        request.validate()?;
        if request.execution.is_some()
            || request.policy != self.policy.as_ref().map(|policy| policy.identity())
            || request.machine.name != request.task.name
            || request.machine.version != request.task.version
            || request.machine.digest == [0; 32]
            || self.machines.resolve(&request.machine, &mut None)?.is_none()
        {
            return Err(Error::Invalid(
                "batch implementation or policy is invalid".into(),
            ));
        }
        self.tasks.validate_durable_admission(
            &request.task,
            &request.machine,
            Some(&request.input_schema),
            &request.output_schema,
            None,
        )?;
        if request.scope.resource_proofs().is_empty() {
            self.root_scope.narrow(request.scope.grants().clone(), request.scope.limits())?
                .with_run_limits(request.scope.run_limits(), &mut None)?;
        } else if request.inputs.is_empty() {
            let parent = request.parent.ok_or_else(|| Error::Unauthorized("created batch has no parent".into()))?;
            let admitted = self.read_retained_admission_counted(OperationId::from_bytes(parent.into_bytes()), &mut None).await.map(|(record, _)| record)?;
            if request.scope.resource_proofs().iter().any(|proof| !admitted.resource_proofs.contains(proof)) {
                return Err(Error::Unauthorized("empty batch resource proofs differ from parent".into()));
            }
            self.authenticated_scope_with_work(&admitted, &mut None).await.map(|(scope, _)| scope)?
                .narrow(request.scope.grants().clone(), request.scope.limits())?
                .with_run_limits(request.scope.run_limits(), &mut None)?;
        } else {
            let first = request.member_admission(0)?;
            self.authenticated_scope_with_work(&first, &mut None).await.map(|(scope, _)| scope)?;
        }
        if request.parent.is_some() {
            require_descendant_grant(request.scope.grants(), &request.task)?;
        }
        if request.scope.extensions() != self.root_scope.extensions() {
            return Err(Error::Conflict(
                "batch extensions differ from owner bindings".into(),
            ));
        }
        validate_task_schemas(&request.input_schema, &request.output_schema, true)?;
        let input = crate::contract::compile_json_schema(&request.input_schema, "batch input", None)?;
        for value in &request.inputs {
            input.validate(value).map_err(|error| {
                Error::Invalid(format!("batch input failed validation: {error}"))
            })?;
        }
        Ok(())
    }

    fn event_key(kind: &str, task_id: TaskId, operation_id: OperationId) -> Result<StreamKey> {
        let identity = format!("harness/v2/{kind}/{task_id}/{operation_id}");
        StreamKey::new(OwnedBytes::copy_from_slice(
            blake3::hash(identity.as_bytes()).as_bytes(),
        ))
        .map_err(|error| Error::Invalid(error.to_string()))
    }



    /// Resolves only the authenticated donor's exact immutable parcel bytes.
    async fn read_mail_source(
        &self, admission: &TaskAdmissionRecord, payload: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        payload.validate(work)?;
        admission.limits.validate_file(payload, work)?;
        if !crate::runtime::read_granted_with_work(&admission.grants, payload, work)? {
            return Err(admission_boundary(work, crate::wire::ErrorCode::Unauthorized,
                "mail donor cannot read its payload"));
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let (scope, retained) = self.authenticated_scope_with_work(admission, quota.work()).await?;
        quota.adopt(retained)?;
        #[cfg(feature = "filesystem")]
        let access = scope.created_access();
        #[cfg(feature = "filesystem")]
        let mut reader: Option<&dyn ContentResidencyVerifier> = None;
        #[cfg(feature = "filesystem")]
        if let Some(access) = access.as_ref() {
            for binding in &access.references {
                if let Some(io) = quota.work().as_deref_mut() { io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?; }
                if &binding.file == payload { reader = Some(binding.reader.as_ref()); break; }
            }
            if reader.is_none() {
                for binding in &access.content {
                    if let Some(io) = quota.work().as_deref_mut() { io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?; }
                    if &binding.volume == payload.volume() { reader = Some(binding.binding.reader.as_ref()); break; }
                }
            }
        }
        #[cfg(not(feature = "filesystem"))]
        let reader = scope.created_reference(payload).map(|reader| reader.as_ref())
            .or_else(|| scope.created_content(Some(payload.volume())).map(|binding| binding.reader.as_ref()));
        let reader = reader.unwrap_or(self.reader.as_ref());
        let bytes = match quota.work().as_deref_mut() {
            Some(io) => crate::store::history::read_content_with_work(io, reader, payload).await?.0,
            None => reader.read(payload, payload.descriptor().byte_length(), None)?.await?,
        };
        payload.descriptor().verify(&bytes, quota.work())?;
        Ok(bytes)
    }

    fn timer_path_text(task: TaskId) -> [u8; 54] {
        let mut text = [0_u8; 54];
        let prefix = b"harness/v2/timers/";
        text[..prefix.len()].copy_from_slice(prefix);
        uuid::Uuid::from_bytes(task.into_bytes()).hyphenated()
            .encode_lower(&mut text[prefix.len()..]);
        text
    }

    fn timer_path_with_work(
        task: TaskId, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(acyclic_stream::StreamPath, u64)> {
        let text = Self::timer_path_text(task);
        let text = std::str::from_utf8(&text).expect("canonical ASCII timer path");
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(text.len() as u64)?;
            io.admit_allocation(text.len() as u64)?;
            io.retain_owned(text.len() as u64)?;
            io.copy_bytes(text.len() as u64)?;
        }
        let path = acyclic_stream::StreamPath::new(text)?;
        Ok((path, if work.is_some() { text.len() as u64 } else { 0 }))
    }

    fn timer_stream_with_work(
        &self, task: TaskId, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(acyclic_stream::Stream<P>, u64)> {
        let text = Self::timer_path_text(task);
        let text = std::str::from_utf8(&text).expect("canonical ASCII timer path");
        let owned = text.len() as u64 + std::mem::size_of::<acyclic_stream::Stream<P>>() as u64;
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(text.len() as u64)?;
            io.admit_allocation(text.len() as u64)?;
            io.retain_owned(owned)?;
            io.copy_bytes(text.len() as u64)?;
        }
        let stream = acyclic_stream::Stream::new(std::sync::Arc::clone(&self.stream), text)?;
        Ok((stream, if work.is_some() { owned } else { 0 }))
    }

    async fn timer_state_with_work(
        &self, task: TaskId, operation: OperationId, deadline_unix_ms: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(u64, bool)> {
        let (stream, owned) = self.timer_stream_with_work(task, work)?;
        if let Some(io) = work.as_deref_mut() { io.begin_call()?; }
        let tail = match stream.bounds().await {
            Ok(bounds) => bounds.tail,
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(error.into()),
        };
        let mut after = 0;
        let mut found = false;
        while after < tail {
            if let Some(io) = work.as_deref_mut() { io.begin_call()?; }
            let mut page = stream.read(after, (tail - after).min(64) as u32).await?;
            let mut count = 0;
            while let Some(record) = page.try_next().await? {
                let length = record.value.len() as u64;
                if let Some(io) = work.as_deref_mut() { io.receive_record(length)?; }
                count += 1;
                if count > 64 {
                    return Err(admission_boundary(work, crate::wire::ErrorCode::Storage,
                        "invalid timer replay page"));
                }
                if record.sequence != after || after >= tail {
                    return Err(admission_boundary(work, crate::wire::ErrorCode::Storage,
                        "timer replay sequence differs"));
                }
                let event: TimerEvent = match work.as_deref_mut() {
                    Some(io) => {
                        let (value, owned) = io.decode_json_value(&record.value)?;
                        let decoded = TimerEvent::deserialize(&value)
                            .map_err(|_| admission_boundary(&mut Some(&mut *io),
                                crate::wire::ErrorCode::Storage, "timer replay record is invalid"));
                        drop(value);
                        let release = io.release_owned(owned);
                        let event = decoded?;
                        release?;
                        event
                    },
                    None => crate::contract::json_from_slice(&record.value)
                        .map_err(|error| Error::Storage(error.to_string()))?,
                };
                drop(record);
                if let Some(io) = work.as_deref_mut() { io.release_record(length)?; }
                if event.task_id != task || event.deadline_unix_ms == 0 {
                    return Err(admission_boundary(work, crate::wire::ErrorCode::Conflict,
                        "timer history belongs to another task"));
                }
                if event.operation_id == operation {
                    if found || event.deadline_unix_ms != deadline_unix_ms {
                        return Err(admission_boundary(work, crate::wire::ErrorCode::Conflict,
                            "timer identity reused"));
                    }
                    found = true;
                }
                after += 1;
            }
            if count == 0 {
                return Err(admission_boundary(work, crate::wire::ErrorCode::Storage,
                    "invalid timer replay page"));
            }
        }
        drop(stream);
        if let Some(io) = work.as_deref_mut() {
            io.release_owned(owned)?;
        }
        Ok((tail, found))
    }

    async fn publish_control(
        &self,
        stream: &acyclic_stream::Stream<P>,
        kind: &str,
        task_id: TaskId,
        operation_id: OperationId,
        bytes: &[u8],
    ) -> Result<()> {
        if self
            .publish_control_at(stream, kind, task_id, operation_id, bytes, None)
            .await?
        {
            Ok(())
        } else {
            Err(Error::Conflict(
                "unconditional control append conflicted".into(),
            ))
        }
    }

    async fn publish_control_at(
        &self,
        stream: &acyclic_stream::Stream<P>,
        kind: &str,
        task_id: TaskId,
        operation_id: OperationId,
        bytes: &[u8],
        expected_tail: Option<u64>,
    ) -> Result<bool> {
        let key = Self::event_key(kind, task_id, operation_id)?;
        let key = if let Some(tail) = expected_tail {
            let mut digest = blake3::Hasher::new();
            digest.update(b"harness/v2/control-tail\0");
            digest.update(key.as_bytes());
            digest.update(&tail.to_le_bytes());
            StreamKey::new(OwnedBytes::copy_from_slice(digest.finalize().as_bytes()))?
        } else {
            key
        };
        let outcome = crate::distributed::append(
            (stream, &self.stream),
            [OwnedBytes::copy_from_slice(bytes)],
            0, expected_tail,
            key,
            operation_id,
            &mut None
        )
        .await?;
        match outcome {
            AppendOutcome::Committed(receipt) if receipt.end == receipt.start + 1 => {
                let records = stream
                    .read(receipt.start, 1)
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?;
                let [record] = records.as_slice() else {
                    return Err(Error::Conflict(
                        "control publication differs from its committed record".into(),
                    ));
                };
                if record.sequence != receipt.start
                    || record.commit_id != receipt.commit_id
                    || record.value.as_ref() != bytes
                {
                    return Err(Error::Conflict(
                        "control publication differs from its committed record".into(),
                    ));
                }
                Ok(true)
            }
            AppendOutcome::Committed(_) => {
                Err(Error::Storage("invalid control append receipt".into()))
            }
            AppendOutcome::TailConflict { .. } => Ok(false),
        }
    }
}
impl<P: StreamProvider> DurableTaskHost for CoordinatorTaskHost<P> {
    fn admit_owned<'a>(
        &'a self,
        request: TaskAdmissionRecord,
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
        Box::pin(async move {
            let parent = request
                .parent
                .ok_or_else(|| Error::Invalid("owned child admission has no parent".into()))?;
            self.verify_owner(parent, &fence, false, &mut None).await?;
            self.admit_record_with_work(request, Some(fence), &mut None).await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn admit_owned_with_native_work<'a>(
        &'a self,
        request: (TaskAdmissionRecord, acyclic_fs::NativeAllocation),
        fence: (crate::scheduler::LeaseFence, acyclic_fs::NativeAllocation),
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Admission<TaskId>, acyclic_fs::NativeAllocation)>> {
        work.charge(acyclic_fs::WorkCounters::default())?;
        let record_bytes = (std::mem::size_of::<TaskAdmissionRecord>() as u64)
            .checked_add(request.0.logical_owned_bytes()?)
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native child admission ownership overflows" })?;
        let fence_bytes = (std::mem::size_of::<crate::scheduler::LeaseFence>() as u64)
            .checked_add(fence.0.reservation_id.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(fence.0.placement.capacity() as u64))
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native child fence ownership overflows" })?;
        if !request.1.matches_work(work) || request.1.bytes() < record_bytes
            || !fence.1.matches_work(work) || fence.1.bytes() < fence_bytes
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "child admission or fence differs from its original custody" });
        }
        crate::NativeFutureBundle::new(work, || async move {
            use crate::store::history::HistoryIo;
            let mut io = crate::store::history::native_work_io(work)?;
            let (record, record_guard) = request;
            let (fence, fence_guard) = fence;
            io.absorb_native_custody(record_guard)?;
            io.absorb_native_custody(fence_guard)?;
            io.retain_owned(std::mem::size_of::<Admission<TaskId>>() as u64)?;
            let parent = record.parent.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "owned child admission has no original parent",
            })?;
            self.verify_owner(parent, &fence, false, &mut Some(&mut io)).await?;
            let admitted = self.admit_record_with_work(record, Some(fence), &mut Some(&mut io)).await?;
            let custody = io.take_native_custody(std::mem::size_of::<Admission<TaskId>>() as u64)?;
            Ok((admitted, custody))
        })
    }

    fn verify_dispatch_owner<'a>(
        &'a self,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.verify_owner(task_id, &fence, false, &mut None).await })
    }

    fn verify_execution_owner<'a>(
        &'a self,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.verify_owner(task_id, &fence, true, &mut None).await })
    }

    fn claim_model_dispatch<'a>(
        &'a self,
        task_id: TaskId,
        attempt_id: OperationId,
        step: u32,
        request_digest: [u8; 32],
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.claim_model_dispatch_with_work(
                task_id, attempt_id, step, request_digest, &fence, &mut None,
            ).await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        work.charge(acyclic_fs::WorkCounters::default())?;
        Ok(true)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn claim_model_dispatch_with_native_work<'a>(
        &'a self,
        task: TaskId,
        attempt: OperationId,
        step: u32,
        request_digest: [u8; 32],
        fence: &'a crate::scheduler::LeaseFence,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let mut io = crate::store::history::native_work_io(work)?;
            self.claim_model_dispatch_with_work(
                task, attempt, step, request_digest, fence, &mut Some(&mut io),
            ).await
        })
    }

    fn supports_batch_cancellation(&self) -> bool {
        true
    }

    fn reconcile_admission<'a>(
        &'a self,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<Option<(TaskId, TaskAdmissionRecord)>>> {
        Box::pin(async move {
            match self.admission_with_work(operation_id, &mut None).await {
                Ok(admission) => Ok(Some((
                    admission.task_id(),
                    admission,
                ))),
                Err(Error::NotFound(_)) => Ok(None),
                Err(error) => Err(error),
            }
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_admission_with_native_work<'a>(
        &'a self,
        operation: OperationId,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<(TaskId, TaskAdmissionRecord)>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            use crate::store::history::HistoryIo;
            let mut io = crate::store::history::native_work_io(work)?;
            let extra = std::mem::size_of::<Option<(TaskId, TaskAdmissionRecord)>>()
                - std::mem::size_of::<TaskAdmissionRecord>();
            io.retain_owned(extra as u64)?;
            let record = match self.read_retained_admission_counted(operation, &mut Some(&mut io)).await.map(|(record, _)| record) {
                Ok(record) => record,
                Err(Error::NativeBoundary { code: crate::wire::ErrorCode::NotFound, .. }) => {
                    io.retain_owned(std::mem::size_of::<TaskAdmissionRecord>() as u64)?;
                    let custody = io.take_native_custody(
                        std::mem::size_of::<Option<(TaskId, TaskAdmissionRecord)>>() as u64,
                    )?;
                    return Ok((None, custody));
                }
                Err(error) => return Err(error),
            };
            let (scope, _) = self.authenticated_scope_with_work(&record, &mut Some(&mut io)).await?;
            drop(scope);
            let owned = (std::mem::size_of::<Option<(TaskId, TaskAdmissionRecord)>>() as u64)
                .checked_add(record.logical_owned_bytes()?)
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "reconciled admission ownership overflows" })?;
            let custody = io.take_native_custody(owned)?;
            Ok((Some((TaskId::from_bytes(operation.into_bytes()), record)), custody))
        })
    }

    fn reconcile_batch<'a>(
        &'a self,
        request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Option<Vec<Admission<TaskId>>>>> {
        Box::pin(async move {
            self.validate_batch(&request).await?;
            let Some(retained) = self.retained_batch(request.batch_id).await? else {
                return Ok(None);
            };
            if retained != request.canonical_value() {
                return Err(Error::Conflict(
                    "batch identity belongs to another request".into(),
                ));
            }
            let cancelled = self.retained_batch_cancellation(request.batch_id).await?;
            if cancelled
                .as_ref()
                .is_some_and(|declaration| declaration.group_id != request.group_id)
            {
                return Err(Error::Conflict(
                    "batch cancellation belongs to another group".into(),
                ));
            }
            let mut entries = Vec::with_capacity(request.inputs.len());
            for index in 0..request.inputs.len() {
                let operation_id = request.operation_id(index);
                let expected = request.member_admission(index)?;
                match self.admission_with_work(operation_id, &mut None).await {
                    Ok(admission) if admission == expected => entries.push(Admission::Accepted(
                        admission.task_id(),
                    )),
                    Ok(_) => {
                        return Err(Error::Conflict(
                            "batch member belongs to another admission".into(),
                        ));
                    }
                    Err(Error::NotFound(_)) if cancelled.is_some() => {
                        entries.push(Admission::Rejected {
                            reason: "batch cancellation preceded member admission".into(),
                        });
                    }
                    Err(Error::NotFound(_)) => {
                        entries.push(Admission::Indeterminate { operation_id });
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(Some(entries))
        })
    }

    fn load_batch<'a>(
        &'a self,
        batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<DurableBatchRequest>>> {
        Box::pin(async move {
            let Some(value) = self.retained_batch(batch_id).await? else {
                return Ok(None);
            };
            let request = DurableBatchRequest::from_canonical_value(value)?;
            if request.batch_id != batch_id {
                return Err(Error::Conflict(
                    "batch manifest has another identity".into(),
                ));
            }
            self.validate_batch(&request).await?;
            Ok(Some(request))
        })
    }

    fn admit_batch<'a>(
        &'a self,
        request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Vec<Admission<TaskId>>>> {
        Box::pin(async move {
            let parent_fence = if request.scope.resource_proofs().is_empty() {
                None
            } else {
                let parent = request.parent.ok_or_else(|| Error::Unauthorized("created batch has no parent".into()))?;
                Some(request.scope.current_creation_parent_fence(parent).await?)
            };
            self.validate_batch(&request).await?;
            self.validate_parent_scope_with_work(request.parent, &request.scope, &mut None)
                .await?;
            self.commit_batch(&request).await?;
            let observed = self
                .reconcile_batch(request.clone())
                .await?
                .ok_or_else(|| Error::Storage("committed batch disappeared".into()))?;
            let mut entries = Vec::with_capacity(observed.len());
            for (index, admission) in observed.into_iter().enumerate() {
                match admission {
                    Admission::Indeterminate { operation_id } => {
                        let member = request.member_admission(index)?;
                        if member.operation_id != operation_id {
                            return Err(Error::Conflict("batch member operation changed".into()));
                        }
                        if self
                            .retained_batch_cancellation(request.batch_id)
                            .await?
                            .is_some()
                        {
                            entries.push(Admission::Rejected {
                                reason: "batch cancellation preceded member admission".into(),
                            });
                            continue;
                        }
                        let admitted = self.admit_record_with_work(member, parent_fence.clone(), &mut None).await?;
                        if self
                            .retained_batch_cancellation(request.batch_id)
                            .await?
                            .is_some()
                            && let Admission::Accepted(task_id) = &admitted
                        {
                            self.cancel(*task_id).await?;
                            entries.push(admitted);
                            continue;
                        }
                        entries.push(admitted);
                    }
                    other => entries.push(other),
                }
            }
            Ok(entries)
        })
    }

    fn cancel_batch<'a>(
        &'a self,
        batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<BatchCancellationReport>>> {
        Box::pin(async move {
            let Some(request) = self.load_batch(batch_id).await? else {
                return Ok(None);
            };
            self.declare_batch_cancellation(&request).await?;
            let admissions = self
                .reconcile_batch(request.clone())
                .await?
                .ok_or_else(|| {
                    Error::Storage("retained batch disappeared during cancellation".into())
                })?;
            let mut entries = Vec::with_capacity(admissions.len());
            for (index, admission) in admissions.into_iter().enumerate() {
                let status = match admission {
                    Admission::Accepted(task_id) => match self.cancel(task_id).await {
                        Ok(()) => BatchCancellationStatus::Requested,
                        Err(Error::Indeterminate(operation_id)) => {
                            BatchCancellationStatus::Indeterminate { operation_id }
                        }
                        Err(error) => BatchCancellationStatus::Unresolved {
                            message: error.to_string(),
                        },
                    },
                    Admission::Rejected { reason } => {
                        BatchCancellationStatus::NotAdmitted { reason }
                    }
                    Admission::Indeterminate { operation_id } => {
                        BatchCancellationStatus::Indeterminate { operation_id }
                    }
                };
                entries.push((InputKey { batch_id, index }, status));
            }
            Ok(Some(BatchCancellationReport {
                group_id: request.group_id,
                batch_id,
                entries,
            }))
        })
    }

    fn admit<'a>(
        &'a self,
        admission: TaskAdmissionRecord,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
        Box::pin(async move { self.admit_record_with_work(admission, None, &mut None).await })
    }
}

impl<P: StreamProvider> crate::runtime::TaskStateProvider for CoordinatorTaskHost<P> {

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_dispatch_owner_with_native_work<'a>(
        &'a self, task: TaskId, fence: &'a crate::scheduler::LeaseFence,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let mut io = crate::store::history::native_work_io(work)?;
            self.verify_owner(task, fence, false, &mut Some(&mut io)).await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_execution_owner_with_native_work<'a>(
        &'a self, task: TaskId, fence: &'a crate::scheduler::LeaseFence,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let mut io = crate::store::history::native_work_io(work)?;
            self.verify_owner(task, fence, true, &mut Some(&mut io)).await
        })
    }

    fn policy_identity(&self) -> Option<ComponentIdentity> {
        self.policy.as_ref().map(|policy| policy.identity())
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn policy_identity_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        work.charge(acyclic_fs::WorkCounters::default())?;
        if let Some(policy) = &self.policy {
            policy.supports_native_policy(work)?;
            let identity = policy.identity_with_native_work(work)?;
            let needed = (std::mem::size_of::<ComponentIdentity>() as u64)
                .checked_add(identity.0.name.capacity() as u64)
                .and_then(|bytes| bytes.checked_add(identity.0.version.capacity() as u64))
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "native policy identity custody overflows" })?;
            if !identity.1.matches_work(work) || identity.1.bytes() < needed {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "policy identity differs from its original native metadata custody" });
            }
            if self.policy_binding.as_ref() != Some(&identity.0) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "durable policy implementation changed after native binding" });
            }
            drop(identity);
        } else if self.policy_binding.is_some() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native durable policy binding has no configured producer" });
        }
        Ok(self.policy_binding.as_ref())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execution_identity_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        work.charge(acyclic_fs::WorkCounters::default())?;
        Ok(None)
    }

    fn observe_admission<'a>(
        &'a self,
        task_id: TaskId,
    ) -> BoxFuture<'a, Result<TaskAdmissionRecord>> {
        Box::pin(async move {
            let operation_id = OperationId::from_bytes(task_id.into_bytes());
            self.admission_with_work(operation_id, &mut None).await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn observe_admission_with_native_work<'a>(
        &'a self, task: TaskId, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (TaskAdmissionRecord, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            CoordinatorTaskHost::observe_admission_with_native_work(self, task, work).await
        })
    }

    fn children<'a>(
        &'a self,
        parent: TaskId,
        expected_revision: Option<u64>,
        after_slot: Option<String>,
        maximum: usize,
    ) -> BoxFuture<'a, Result<TaskChildrenPage>> {
        Box::pin(async move {
            let mut coordinator = self.coordinator.lock().await;
            coordinator
                .observe_children(
                    self.verifier.audience(),
                    &self.owner_scope,
                    &self.verifier,
                    OperationId::from_bytes(parent.into_bytes()),
                    expected_revision, after_slot.as_deref(), maximum,
                 &mut None,)
                .await.map(|(page, _)| page)
        })
    }

    fn resume_scope<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<RuntimeScope>> {
        Box::pin(async move {
            self.resume_scope_with_work(task_id, operation_id, &mut None).await.map(|value| value.0)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn resume_scope_with_native_work<'a>(
        &'a self, task: TaskId, operation: OperationId,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (RuntimeScope, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let mut io = crate::store::history::native_work_io(work)?;
            let (scope, bytes) = self.resume_scope_with_work(task, operation, &mut Some(&mut io)).await?;
            let custody = crate::store::history::HistoryIo::take_native_custody(&mut io, bytes)?;
            Ok((scope, custody))
        })
    }

    fn outcome<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<Option<Outcome<Value>>>> {
        Box::pin(async move { self.outcome_with_work(task_id, &mut None).await.map(|(outcome, _)| outcome) })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn outcome_with_native_work<'a>(
        &'a self, task: TaskId, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<Outcome<Value>>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            CoordinatorTaskHost::outcome_with_native_work(self, task, work).await
        })
    }

    fn cancel<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.cancel_with_work(task_id, &mut None).await })
    }

    fn send<'a>(
        &'a self,
        sender: TaskId,
        recipient: TaskId,
        message_id: OperationId,
        payload: FileRef,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let recipient_scope = self.admission_with_work(OperationId::from_bytes(recipient.into_bytes()), &mut None).await?;
            if !self.admission_with_work(OperationId::from_bytes(sender.into_bytes()), &mut None).await?
                .grants.contains(capability::MAIL_SEND)
                || !recipient_scope.grants.contains(capability::MAIL_READ)
                || !read_granted(&recipient_scope.grants, &payload)?
            {
                return Err(Error::Unauthorized("unfenced custom mail requires explicit mail and file grants".into()));
            }
            let bytes = self.mail_bytes(sender, recipient, message_id, payload, None).await?;
            self.publish_mail(sender, recipient, message_id, bytes, None, None, None)
                .await
        })
    }

    fn inbox<'a>(
        &'a self,
        task_id: TaskId,
        after: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<InboxItem>>> {
        Box::pin(self.inbox_page(task_id, after, limit, None))
    }

    fn wait_until<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
        deadline_unix_ms: u64,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if deadline_unix_ms == 0 {
                return Err(Error::Invalid("timer deadline is invalid".into()));
            }
            let admission = self
                .admission_with_work(OperationId::from_bytes(task_id.into_bytes()), &mut None)
                .await?;
            if !admission.grants.contains(capability::TIMER_WAIT) {
                return Err(Error::Unauthorized("task scope lacks timer:wait".into()));
            }
            let event = TimerEvent {
                task_id,
                operation_id,
                deadline_unix_ms,
            };
            let bytes = crate::contract::canonical_json_bytes(&event)?;
            let stream = self.timer_stream_with_work(task_id, &mut None).map(|(stream, _)| stream)?;
            loop {
                let (tail, found) = self
                    .timer_state_with_work(task_id, operation_id, deadline_unix_ms, &mut None)
                    .await?;
                if found {
                    break;
                }
                if self
                    .publish_control_at(
                        &stream,
                        "timers",
                        task_id,
                        operation_id,
                        &bytes,
                        Some(tail),
                    )
                    .await?
                {
                    break;
                }
            }
            loop {
                let now = self.clock.now_unix_millis();
                if now >= deadline_unix_ms {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(
                    deadline_unix_ms.saturating_sub(now).min(60_000),
                ))
                .await;
            }
        })
    }

    fn reconcile_effect<'a>(
        &'a self,
        task_id: TaskId,
        effect_id: EffectId,
    ) -> BoxFuture<'a, Result<crate::core::EffectStatus>> {
        Box::pin(async move {
            let admission = self
                .admission_with_work(OperationId::from_bytes(task_id.into_bytes()), &mut None)
                .await?;
            if !admission.grants.contains(capability::EFFECT_RUN) {
                return Err(Error::Unauthorized("task scope lacks effect:run".into()));
            }
            let effects = self
                .effects
                .as_ref()
                .ok_or_else(|| Error::Unsupported("durable effect observer is not bound".into()))?;
            effects.reconcile(effect_id).await
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        AgentId, Capabilities, GroupId,
        conversation::{FileDescriptor, Limits, VolumeClass, VolumeOwner, VolumeRef},
        core::{AggregateKind, AuthorityIssuer},
        resources::ProviderRef,
        runtime::TaskDefinition,
        workflow::{MachineIdentity, MachineStatus, MachineTransition, ResumableMachine},
    };
    use acyclic_stream::{MemoryStream, SystemUnixMillisClock};

    fn test_registries<O: 'static>(
        implementation: Arc<dyn ResumableMachine>, input: Value, output: Value,
    ) -> Result<(ComponentIdentity, MachineIdentity, TaskRegistry, MachineRegistry)> {
        let machine = implementation.identity().clone();
        let definition = TaskDefinition::<Value, O>::resumable(implementation.clone(), input, output)?;
        let task = definition.identity().clone();
        let mut tasks = TaskRegistry::default();
        tasks.register(definition)?;
        let mut machines = MachineRegistry::default();
        machines.register(implementation, &mut None)?;
        Ok((task, machine, tasks, machines))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn test_work(
        budget: acyclic_fs::WorkBudget, cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<acyclic_fs::NativeMaterializationWork> {
        acyclic_fs::NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
    }

    #[cfg(feature = "filesystem")]
    mod resource_preparation;
    #[cfg(feature = "filesystem")]
    pub(crate) mod created_resource_access;

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_worker_lifecycle_uses_original_memory_filesystem() -> Result<()> {
        use crate::distributed::WorkPull;
        let fixture = created_resource_access::CreationFixture::new().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = test_work(acyclic_fs::performance::work_budget(64 * 1024 * 1024, 64 * 1024 * 1024), &cancellation)?;
        let (admission, admission_guard) = fixture.host
            .observe_admission_with_native_work(fixture.root, &work).await?;
        assert_eq!(admission.operation_id.into_bytes(), fixture.root.into_bytes());
        drop(admission);
        drop(admission_guard);
        assert!(fixture.host.succeeded_result_with_native_work(fixture.root, &work).await?.is_none());
        let (recovered, recovered_guard) = fixture.host.recover_work_with_native_work(fixture.root, &work).await?;
        let WorkPull::Claimed(lease) = recovered else { panic!("original reservation was not recovered"); };
        assert_eq!(lease.reservation, fixture.root_lease.reservation);
        fixture.host.start_task(&lease, Some(&work)).await?;
        let mut stale = lease.clone();
        stale.reservation.placement.push_str("-foreign");
        assert!(fixture.host.start_task(&stale, Some(&work)).await.is_err());
        drop(stale);
        let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
        let waiting = OperationId::from_bytes([91; 16]);
        fixture.host.suspend_workflow(fixture.root, &fence, 1, Some(waiting), None, None, &mut Some(&mut crate::store::history::native_work_io(&work)?)).await?;
        drop(lease);
        drop(recovered_guard);
        let (slot, slot_guard) = fixture.host.workflow_suspension_with_native_work(fixture.root, &work).await?;
        assert_eq!(slot.as_ref().map(|slot| (slot.revision, slot.waiting_command)), Some((1, Some(waiting))));
        assert!(slot.as_ref().expect("suspended").input.is_none());
        drop(slot);
        drop(slot_guard);
        fixture.host.wake_command(fixture.root, 1, waiting, &mut Some(&mut crate::store::history::native_work_io(&work)?)).await?;
        let (pulled, pulled_guard) = fixture.host.pull_operation_with_native_work(
            &fixture.worker, OperationId::from_bytes(fixture.root.into_bytes()), &work,
        ).await?;
        let WorkPull::Claimed(lease) = pulled else { panic!("woken original task was not claimed"); };
        fixture.host.start_task(&lease, Some(&work)).await?;
        assert!(fixture.host.settle_task(fixture.root, &crate::scheduler::LeaseFence::from(&fixture.root_lease.reservation), Outcome::Succeeded(serde_json::json!({"stale":true})), &mut Some(&mut crate::store::history::native_work_io(&work)?)).await.is_err());
        let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
        let (input, input_guard) = fixture.host.workflow_input_with_native_work(fixture.root, &fence, &work).await?;
        assert_eq!(input, Some((1, Value::Null)));
        drop(input);
        drop(input_guard);
        fixture.host.settle_task(fixture.root, &fence, Outcome::Succeeded(serde_json::json!({"ok":true})), &mut Some(&mut crate::store::history::native_work_io(&work)?)).await?;
        drop(lease);
        drop(pulled_guard);
        let (outcome, outcome_guard) = fixture.host.outcome_with_native_work(fixture.root, &work).await?;
        assert_eq!(outcome, Some(Outcome::Succeeded(serde_json::json!({"ok":true}))));
        drop(outcome);
        drop(outcome_guard);
        let (reference, bytes, metadata_guard) = fixture.host
            .succeeded_result_with_native_work(fixture.root, &work).await?
            .expect("actual protected terminal result");
        assert_eq!(bytes.as_ref(), br#"{"ok":true}"#);
        reference.descriptor().verify(&bytes)?;
        drop(bytes);
        drop(reference);
        drop(metadata_guard);
        assert!(work.receipt().backend_read_operations > 0);
        assert!(work.receipt().backend_write_operations > 0);
        let before = work.receipt();
        cancellation.cancel();
        assert!(matches!(fixture.host.outcome_with_native_work(fixture.root, &work).await,
            Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
        assert_eq!(work.receipt().backend_read_operations, before.backend_read_operations);
        assert!(matches!(fixture.host.succeeded_result_with_native_work(fixture.root, &work).await,
            Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
        assert_eq!(work.receipt().backend_read_operations, before.backend_read_operations);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_workflow_resume_retries_the_original_key_and_input() -> Result<()> {
        use crate::store::history::HistoryIo as _;
        let fixture = created_resource_access::CreationFixture::new().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = test_work(acyclic_fs::performance::work_budget(64 * 1024 * 1024, 64 * 1024 * 1024), &cancellation)?;
        fixture.host.suspend_workflow(fixture.root, &crate::scheduler::LeaseFence::from(&fixture.root_lease.reservation), 1, None, None, None, &mut Some(&mut crate::store::history::native_work_io(&work)?)).await?;
        let operation = OperationId::from_bytes(fixture.root.into_bytes());
        let mut original_input = None;
        for _ in 0..2 {
            let (input, input_guard) = fixture.host.payloads.stage_with_native_work(
                operation, "native-existing-resume-input", br#"{"resume":true}"#, &work,
            )?.await?;
            let mut key_io = crate::store::history::native_work_io(&work)?;
            let identity = crate::contract::canonical_json_digest_with_work(
                &input, acyclic_stream::MAX_RECORD_BYTES as u64, &mut Some(&mut key_io),
            )?;
            if let Some(original) = original_input {
                assert_eq!(original, identity);
            } else {
                original_input = Some(identity);
            }
            let key = IdempotencyKey::new(key_io.clone_text("native-existing-resume-key")?)?;
            let key_guard = key_io.into_custody();
            fixture.host.resume_workflow_with_native_work(fixture.root, 1, input, key, &work).await?;
            drop(key_guard);
            drop(input_guard);
        }
        let (pulled, pulled_guard) = fixture.host.pull_operation_with_native_work(
            &fixture.worker, operation, &work,
        ).await?;
        let crate::distributed::WorkPull::Claimed(lease) = pulled else {
            panic!("original resumed task was not claimed");
        };
        fixture.host.start_task(&lease, Some(&work)).await?;
        let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
        let (input, input_guard) = fixture.host.workflow_input_with_native_work(fixture.root, &fence, &work).await?;
        assert_eq!(input, Some((1, serde_json::json!({"resume":true}))));
        drop(input);
        drop(input_guard);
        drop(lease);
        drop(pulled_guard);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_worker_observation_rejects_a_genuine_foreign_owner() -> Result<()> {
        let fixture = created_resource_access::CreationFixture::new().await?;
        let foreign = Authority { kind: AggregateKind::Task, id: "native-foreign-owner".into() };
        let issuer = AuthorityIssuer::new("native-foreign-owner", [92; 32], foreign.clone());
        let signed = issuer.issue("foreign", fixture.host.owner_scope.capabilities(), Some(AgentId::from_bytes([92; 16])), None, &mut None).expect("scope proof");
        let host = CoordinatorTaskHost::new(
            DistributedCoordinator::open(&fixture.stream, fixture.host.payloads.clone(), None, &mut None).await?,
            fixture.stream.clone(), fixture.host.payloads.clone(), fixture.host.reader.clone(),
            foreign, signed, issuer.verifier(&mut None).expect("verifier copy"), fixture.root_scope.clone(),
            fixture.host.tasks.clone(), fixture.host.machines.clone(), fixture.host.clock.clone(),
        )?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = test_work(acyclic_fs::performance::work_budget(64 * 1024 * 1024, 64 * 1024 * 1024), &cancellation)?;
        let (pending, pending_guard) = fixture.host.outcome_with_native_work(fixture.root, &work).await?;
        assert!(pending.is_none());
        drop(pending);
        drop(pending_guard);
        let before = work.receipt();
        assert!(host.outcome_with_native_work(fixture.root, &work).await.is_err());
        assert_eq!(work.receipt().backend_write_operations, before.backend_write_operations);
        Ok(())
    }

    struct MemoryPayloads {
        volume: VolumeRef,
        files: Mutex<BTreeMap<String, Vec<u8>>>,
    }

    impl MemoryPayloads {
        fn new() -> Result<Self> {
            Ok(Self {
                volume: VolumeRef::new(
                    ProviderRef::new("batch-test", "filesystem", "2")?,
                    "private",
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
                )?,
                files: Mutex::new(BTreeMap::new()),
            })
        }
    }

    #[tokio::test]
    async fn atomic_mail_envelope_reuse_and_hostile_inbox_controls() -> Result<()> {
        let stream = Arc::new(MemoryStream::default());
        let payloads = Arc::new(MemoryPayloads::new()?);
        let authority = Authority {
            kind: AggregateKind::Task,
            id: "mail-envelope-owner".into(),
        };
        let issuer = AuthorityIssuer::new("mail-envelope", [7; 32], authority.clone());
        let grants = Capabilities::new([
            "operation:declare".to_owned(),
            "operation:observe".to_owned(),
            "operation:cancel".to_owned(),
            "task:spawn:test.mail@1".to_owned(),
            "mail:send".to_owned(),
            "mail:read".to_owned(),
            payloads.volume.capability(crate::conversation::VolumeOperation::Read)?,
        ]);
        let signed = issuer.issue("owner", &grants, None, None, &mut None).expect("scope proof");
        let runtime_scope = RuntimeScope::new(grants, Limits::default())?;
        let implementation = Arc::new(BatchMachine {
            identity: MachineIdentity {
                name: "test.mail".into(),
                version: "1".into(),
                digest: [3; 32],
            },
            schema: serde_json::json!({"type":"integer"}),
        });
        let (task_identity, machine, tasks, machines) = test_registries::<Value>(
            implementation, serde_json::json!({"type":"integer"}), serde_json::json!({"type":"integer"}),
        )?;
        let host = CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?,
            stream.clone(),
            payloads.clone(),
            payloads.clone(),
            authority,
            signed,
            issuer.verifier(&mut None).expect("verifier copy"),
            runtime_scope.clone(),
            tasks,
            machines,
            Arc::new(SystemUnixMillisClock),
        )?;
        for index in 1..=3_u8 {
            host.admit(TaskAdmissionRecord {
                resource_proofs: Vec::new(),
                operation_id: OperationId::from_bytes([index; 16]),
                task: task_identity.clone(),
                machine: machine.clone(),
                input: serde_json::json!(0),
                input_schema: serde_json::json!({"type":"integer"}),
                output_schema: serde_json::json!({"type":"integer"}),
                parent: None,
                grants: runtime_scope.grants().clone(),
                limits: runtime_scope.limits(),
                run_limits: runtime_scope.run_limits(),
                policy: None,
                extensions: None,
                execution: None,
            }).await?;
        }
        let sender = TaskId::from_bytes([1; 16]);
        let recipient = TaskId::from_bytes([2; 16]);
        let other = TaskId::from_bytes([3; 16]);
        let message = OperationId::from_bytes([8; 16]);
        let payload = payloads.stage(message, "body", b"7").await?;
        let changed = payloads.stage(message, "changed", b"8").await?;
        host.send(sender, recipient, message, payload.clone()).await?;
        host.send(sender, recipient, message, payload.clone()).await?;
        let retained = host.mailbox(recipient)?.read(0, 1).await?
            .try_collect::<Vec<_>>().await?;
        let event = crate::contract::json_from_slice::<MailEvent>(&retained[0].value)
                .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(event.sender, sender);
        assert_eq!(event.recipient, recipient);
        assert_eq!(event.message_id, message);
        assert_eq!(event.schema_revision, MAIL_SCHEMA_REVISION);
        assert_eq!(event.route_revision, MAIL_ROUTE_REVISION);
        assert_eq!(host.inbox(recipient, 0, 4).await?.len(), 1);
        for (destination, body) in [(other, payload.clone()), (recipient, changed)] {
            assert!(matches!(
                host.send(sender, destination, message, body).await,
                Err(Error::Conflict(_))
            ));
        }
        for (schema_revision, route_revision) in [(2, 1), (1, 2)] {
            let altered = MailEvent {
                schema_revision, route_revision,
                ..crate::contract::json_from_slice::<MailEvent>(&retained[0].value)
                .map_err(|error| Error::Storage(error.to_string()))?
            };
            let bytes = OwnedBytes::from(crate::contract::canonical_json_bytes(&altered)?);
            assert!(matches!(
                host.publish_mail(sender, recipient, message, bytes, None, None, None).await,
                Err(Error::Conflict(_))
            ));
        }
        assert_eq!(host.mailbox(recipient)?.bounds().await?.tail, 1);
        assert!(host.inbox(other, 0, 4).await?.is_empty());
        assert!(host.outcome(recipient).await?.is_none());

        let original = serde_json::to_value(&event)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let mut hostile = Vec::new();
        let mut wrong_recipient = original.clone();
        wrong_recipient["recipient"] = serde_json::json!(other);
        hostile.push(("recipient".to_owned(), wrong_recipient));
        for field in ["schema_revision", "route_revision"] {
            for revision in [0_u32, 2, u32::MAX] {
                let mut value = original.clone();
                value[field] = serde_json::json!(revision);
                hostile.push((format!("{field}:{revision}"), value));
            }
        }
        for field in [
            "sender", "recipient", "message_id", "schema_revision", "route_revision", "payload",
        ] {
            let mut value = original.clone();
            assert!(value.as_object_mut().expect("mail object").remove(field).is_some());
            hostile.push((format!("missing:{field}"), value));
        }
        let mut unknown = original.clone();
        unknown["unexpected"] = serde_json::json!(true);
        hostile.push(("unknown-field".into(), unknown));
        for (index, (label, mut value)) in hostile.into_iter().enumerate() {
            let operation = OperationId::from_bytes([100 + index as u8; 16]);
            if value.get("message_id").is_some() {
                value["message_id"] = serde_json::json!(operation);
            }
            let bytes = OwnedBytes::from(crate::contract::canonical_json_bytes(&value)?);
            host.publish_mail(sender, recipient, operation, bytes, None, None, None).await?;
            assert!(matches!(
                host.inbox(recipient, 1 + index as u64, 1).await,
                Err(Error::Storage(_))
            ), "hostile envelope exposed: {label}");
        }
        for fault in 0..7_u8 {
            use acyclic_stream::{CommitCondition, CommitMutation, CommitOutcome, CommitRequest};
            let operation = OperationId::from_bytes([150 + fault; 16]);
            let event = MailEvent {
                sender, recipient, message_id: operation,
                schema_revision: MAIL_SCHEMA_REVISION,
                route_revision: MAIL_ROUTE_REVISION,
                payload: payload.clone(),
            };
            let bytes = OwnedBytes::from(crate::contract::canonical_json_bytes(&event)?);
            let mailbox = host.mailbox(recipient)?;
            let sequence = mailbox.bounds().await?.tail;
            let location = MailLocation {
                sender, recipient, message_id: operation, sequence,
                schema_revision: MAIL_SCHEMA_REVISION, route_revision: MAIL_ROUTE_REVISION,
                value_digest: *blake3::hash(&bytes).as_bytes(),
            };
            let mut receipt = serde_json::to_value(location)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            match fault {
                0 => receipt["schema_revision"] = serde_json::json!(0),
                1 => receipt["schema_revision"] = serde_json::json!(2),
                2 => receipt["route_revision"] = serde_json::json!(0),
                3 => receipt["route_revision"] = serde_json::json!(2),
                4 => { receipt.as_object_mut().expect("location object").remove("schema_revision"); }
                5 => { receipt.as_object_mut().expect("location object").remove("route_revision"); }
                _ => receipt["unexpected"] = serde_json::json!(true),
            }
            let location_path = acyclic_stream::StreamPath::new(format!("harness/v3/mail/by-intent/{sender}/{operation}"))?;
            let committed = stream.commit(CommitRequest {
                conditions: vec![
                    CommitCondition::Tail { path: mailbox.path().clone(), expected: sequence },
                    CommitCondition::Absent { path: location_path.clone() },
                ],
                mutations: vec![
                    CommitMutation::Append { path: mailbox.path().clone(), records: vec![bytes] },
                    CommitMutation::Append {
                        path: location_path,
                        records: vec![OwnedBytes::from(crate::contract::canonical_json_bytes(&receipt)?)],
                    },
                ],
                idempotency_key: StreamKey::new(OwnedBytes::from(vec![200 + fault]))?,
            }).await?;
            assert!(matches!(committed, CommitOutcome::Committed(_)));
            assert!(matches!(
                host.inbox(recipient, sequence, 1).await,
                Err(Error::Storage(_))
            ), "malformed atomic location exposed: {fault}");
        }
        Ok(())
    }

    impl SchedulerPayloadStore for MemoryPayloads {
        fn stage<'a>(
            &'a self,
            operation_id: OperationId,
            idempotency_key: &'a str,
            bytes: &'a [u8],
        ) -> BoxFuture<'a, Result<FileRef>> {
            Box::pin(async move {
                let path = format!(
                    "scheduler/{operation_id}/{}.json",
                    blake3::hash(idempotency_key.as_bytes()).to_hex(),
                );
                let mut files = self.files.lock().await;
                if let Some(existing) = files.get(&path) {
                    if existing != bytes {
                        return Err(Error::Conflict("staged identity changed bytes".into()));
                    }
                } else {
                    files.insert(path.clone(), bytes.to_vec());
                }
                FileRef::new(
                    self.volume.clone(),
                    path,
                    "immutable-1",
                    FileDescriptor::from_bytes(bytes, "application/json")?,
                    "payload.json",
                )
            })
        }
    }

    impl ContentResidencyVerifier for MemoryPayloads {
        fn verify<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.read(reference).await?;
                Ok(())
            })
        }

        fn read<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>> {
            Box::pin(async move {
                if reference.volume() != &self.volume {
                    return Err(Error::Unauthorized(
                        "payload belongs to another owner".into(),
                    ));
                }
                let bytes = self
                    .files
                    .lock()
                    .await
                    .get(reference.path())
                    .cloned()
                    .ok_or_else(|| Error::NotFound("payload".into()))?;
                reference.descriptor().verify(&bytes, &mut None)?;
                Ok(bytes.into())
            })
        }
    }

    struct BatchMachine {
        identity: MachineIdentity,
        schema: Value,
    }

    #[cfg(feature = "filesystem")]
    #[tokio::test]
    async fn wake_discovery_pages_filter_owners_and_recheck_current_cancellation() -> Result<()> {
        use crate::scheduler::{LeaseFence, Reservation, SchedulerEvent};
        let stream = Arc::new(MemoryStream::default());
        let payloads = Arc::new(MemoryPayloads::new()?);
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "discovery-owner".into(),
        };
        let foreign = Authority {
            kind: AggregateKind::Task,
            id: "foreign-owner".into(),
        };
        let issuer = AuthorityIssuer::new("discovery", [7; 32], owner.clone());
        let foreign_issuer = AuthorityIssuer::new("discovery", [8; 32], foreign.clone());
        let scope = issuer.issue("owner", &Capabilities::new(["operation:declare", "operation:observe", "operation:cancel"]), None, None, &mut None).expect("scope proof");
        let foreign_scope = foreign_issuer.issue("owner", scope.capabilities(), None, None, &mut None).expect("scope proof");
        let mut coordinator = DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?;
        for index in 1..=18u8 {
            let operation = OperationId::from_bytes([index; 16]);
            let state = payloads.stage(operation, "state", b"null").await?;
            let fence = LeaseFence {
                reservation_id: format!("lease-{index}"),
                placement: "worker".into(),
            };
            let events = [
                SchedulerEvent::Declared {
                    spec: Box::new(OperationSpec {
                        operation_id: operation,
                        owner_scope: if index % 2 == 0 {
                            foreign_scope.clone()
                        } else {
                            scope.clone()
                        },
                        parent: None,
                        owner: DurableOwner::Attached {
                            authority: if index % 2 == 0 {
                                foreign.clone()
                            } else {
                                owner.clone()
                            },
                        },
                        entrypoint: EntrypointRef {
                            name: "test.discovery".into(),
                            version: "1".into(),
                            digest: [2; 32],
                            result_schema: serde_json::json!({}),
                        },
                        dependencies: BTreeSet::new(),
                        resources: ResourceRequest::default(),
                        placement: BTreeMap::new(),
                        orchestration: Orchestration::Leaf,
                        state,
                    }),
                },
                SchedulerEvent::WaitingForCapacity {
                    operation_id: operation,
                },
                SchedulerEvent::Admitted {
                    operation_id: operation,
                    reservation: Reservation {
                        id: fence.reservation_id.clone(),
                        placement: fence.placement.clone(),
                        admitted: ResourceRequest::default(),
                    },
                },
                SchedulerEvent::Started {
                    operation_id: operation,
                    fence: fence.clone(),
                },
                SchedulerEvent::WorkflowSuspended {
                    operation_id: operation,
                    fence,
                    workflow_revision: 1,
                    waiting_command: (index != 1).then_some(OperationId::from_bytes([99; 16])),
                },
            ];
            for (step, event) in events.into_iter().enumerate() {
                let key = IdempotencyKey::new(format!("discovery-{index}-{step}"))?;
                if let SchedulerEvent::Declared { spec } = event {
                    let (authority, signed, verifier) = if index % 2 == 0 {
                        (&foreign, &foreign_scope, foreign_issuer.verifier(&mut None).expect("verifier copy"))
                    } else {
                        (&owner, &scope, issuer.verifier(&mut None).expect("verifier copy"))
                    };
                    coordinator
                        .declare_operation(authority, signed, &verifier, *spec, key, None, &mut None)
                        .await?;
                } else {
                    coordinator.apply(operation, key, event, &mut None).await?;
                }
            }
        }
        let cancel = |index| SchedulerEvent::CancellationRequested {
            operation_id: OperationId::from_bytes([index; 16]),
            recursive: false,
        };
        coordinator
            .apply(
                OperationId::from_bytes([3; 16]),
                IdempotencyKey::new("discovery-cancel-3")?,
                cancel(3), &mut None,
            )
            .await?;
        let host = CoordinatorTaskHost::new(
            coordinator,
            stream.clone(),
            payloads.clone(),
            payloads,
            owner,
            scope.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            RuntimeScope::new(scope.capabilities().clone(), Limits::default())?,
            TaskRegistry::default(),
            MachineRegistry::default(),
            Arc::new(SystemUnixMillisClock),
        )?;
        let first = host.workflow_wait_page(0, None, 64).await?;
        assert_eq!(first.events_read, 64);
        assert_eq!(first.next_revision, 64);
        assert_eq!(first.through_revision, 91);
        assert_eq!(
            first.tasks,
            [5, 7, 9, 11].map(|index| TaskId::from_bytes([index; 16]))
        );
        host.coordinator
            .lock()
            .await
            .apply(
                OperationId::from_bytes([17; 16]),
                IdempotencyKey::new("discovery-cancel-17")?,
                cancel(17), &mut None,
            )
            .await?;
        let second = host
            .workflow_wait_page(first.next_revision, Some(first.through_revision), 64)
            .await?;
        assert_eq!(second.events_read, 27);
        assert_eq!(second.next_revision, 91);
        assert_eq!(second.through_revision, 91);
        assert_eq!(
            second.tasks,
            [13, 15].map(|index| TaskId::from_bytes([index; 16]))
        );
        let finished = host.workflow_wait_page(91, Some(91), 64).await?;
        assert_eq!(finished.events_read, 0);
        assert!(finished.tasks.is_empty());
        let retry = host.workflow_wait_page(64, Some(91), 64).await?;
        assert_eq!(retry.tasks, second.tasks);
        let new_snapshot = host.workflow_wait_page(0, None, 1).await?;
        assert_eq!(new_snapshot.through_revision, 92);
        Ok(())
    }

    impl ResumableMachine for BatchMachine {
        fn identity(&self) -> &MachineIdentity {
            &self.identity
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        fn identity_with_native_work<'a>(
            &'a self, work: &acyclic_fs::NativeMaterializationWork,
        ) -> Result<&'a MachineIdentity> {
            work.remaining()?;
            Ok(&self.identity)
        }
        fn state_schema(&self) -> &Value {
            &self.schema
        }
        fn initialize(&self, input: &Value) -> Result<Value> {
            Ok(input.clone())
        }

        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        fn initialize_with_native_work(
            &self, input: &Value, work: &acyclic_fs::NativeMaterializationWork,
        ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
            let number = input.as_i64().map(serde_json::Number::from)
                .or_else(|| input.as_u64().map(serde_json::Number::from))
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "native fixture requires a fixed-width integer input" })?;
            let custody = work.retain_owned(0)?;
            Ok((Value::Number(number), custody))
        }
        fn transition(&self, state: &Value, _input: &Value) -> Result<MachineTransition> {
            Ok(MachineTransition {
                state: state.clone(),
                commands: Vec::new(),
                status: MachineStatus::Suspended,
            })
        }

        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        fn supports_native_work(&self) -> bool { true }

        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        fn transition_with_native_work(
            &self, state: &Value, _input: Value,
            work: &acyclic_fs::NativeMaterializationWork,
        ) -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
            let number = state.as_i64().map(serde_json::Number::from)
                .or_else(|| state.as_u64().map(serde_json::Number::from))
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "native fixture requires a fixed-width integer state" })?;
            let custody = work.retain_owned(0)?;
            Ok((MachineTransition {
                state: Value::Number(number), commands: Vec::new(), status: MachineStatus::Suspended,
            }, custody))
        }
    }

    #[cfg(feature = "filesystem")]
    struct EffectTaskFixture<P> {
        host: Arc<CoordinatorTaskHost<P>>,
        binding: TaskJournalOwner<P>,
        payloads: Arc<MemoryPayloads>,
        runtime_scope: RuntimeScope,
    }

    #[cfg(feature = "filesystem")]
    async fn effect_task_fixture<P: StreamProvider>(
        stream: std::sync::Arc<P>,
        provider: &str,
        additional: Vec<String>,
    ) -> Result<EffectTaskFixture<P>> {
        let payloads = Arc::new(MemoryPayloads::new()?);
        let (host, binding, runtime_scope) = effect_task_fixture_with_storage(
            stream, provider, additional, payloads.clone(), payloads.clone(), &payloads.volume,
        ).await?;
        Ok(EffectTaskFixture { host, binding, payloads, runtime_scope })
    }

    #[cfg(feature = "filesystem")]
    #[allow(clippy::type_complexity)]
    async fn effect_task_fixture_with_storage<P: StreamProvider>(
        stream: std::sync::Arc<P>,
        provider: &str,
        additional: Vec<String>,
        payloads: Arc<dyn SchedulerPayloadStore>,
        reader: Arc<dyn ContentResidencyVerifier>,
        payload_volume: &VolumeRef,
    ) -> Result<(Arc<CoordinatorTaskHost<P>>, TaskJournalOwner<P>, RuntimeScope)> {
        let authority = Authority {
            kind: AggregateKind::Task,
            id: "effect-task-owner".into(),
        };
        let issuer = AuthorityIssuer::new("effect-task", [51; 32], authority.clone());
        let mut grants = vec![
            "operation:declare".into(),
            "operation:observe".into(),
            "operation:cancel".into(),
            "task:spawn:test.effect@1".into(),
            "effect:run".into(),
            "effect:plan".into(),
            capability::effect_provider(provider),
            payload_volume.capability(crate::conversation::VolumeOperation::Read)?,
        ];
        grants.extend(additional);
        let grants = Capabilities::new(grants);
        let signed = issuer.issue("owner", &grants, None, None, &mut None).expect("scope proof");
        let runtime_scope = RuntimeScope::new(grants, Limits::default())?;
        let implementation = Arc::new(BatchMachine {
            identity: MachineIdentity {
                name: "test.effect".into(),
                version: "1".into(),
                digest: [52; 32],
            },
            schema: serde_json::json!({"type":"integer"}),
        });
        let (task_identity, machine, tasks, machines) = test_registries::<Value>(
            implementation, serde_json::json!({"type":"integer"}), serde_json::json!({"type":"integer"}),
        )?;
        let host = Arc::new(CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, reader.clone(), None, &mut None).await?,
            stream.clone(),
            payloads.clone(),
            reader,
            authority,
            signed,
            issuer.verifier(&mut None).expect("verifier copy"),
            runtime_scope.clone(),
            tasks,
            machines,
            Arc::new(SystemUnixMillisClock),
        )?);
        let operation = OperationId::from_bytes([53; 16]);
        let task = TaskId::from_bytes(operation.into_bytes());
        host.admit(TaskAdmissionRecord {
            resource_proofs: Vec::new(),
            operation_id: operation,
            task: task_identity,
            machine,
            input: serde_json::json!(0),
            input_schema: serde_json::json!({"type":"integer"}),
            output_schema: serde_json::json!({"type":"integer"}),
            parent: None,
            grants: runtime_scope.grants().clone(),
            limits: runtime_scope.limits(),
            run_limits: runtime_scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        })
        .await?;
        let crate::distributed::WorkPull::Claimed(lease) = host
            .pull_work(&crate::distributed::Worker {
                id: "effect-worker".into(),
                available: Default::default(),
                labels: BTreeMap::new(),
            })
            .await?
        else {
            return Err(Error::NotFound("effect lease".into()));
        };
        host.start_task(&lease, None).await?;
        let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
        let binding = host.journal_owner(task, fence.clone()).await?;
        Ok((host, binding, runtime_scope))
    }

    #[cfg(feature = "filesystem")]
    #[tokio::test]
    async fn bounded_routing_ack_loss_redelivery_cancel_and_replacement() -> Result<()> {
        use crate::distributed::{WorkPull, Worker};
        use std::sync::atomic::Ordering;
        for a in 0..4 {
            for b in 0..4 {
                for c in 0..4 {
                    for d in 0..4 {
                        if a == b || a == c || a == d || b == c || b == d || c == d
                        {
                            continue;
                        }
                        let provider =
                            Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
                        let stream = provider.clone();
                        let fixture = effect_task_fixture(
                            stream.clone(),
                            "test.routing",
                            vec![
                                "mail:send".into(),
                                "mail:read".into(),
                                "model:generate".into(),
                            ],
                        )
                        .await?;
                        let host = fixture.host;
                        let (task, fence) = fixture.binding.task_binding();
                        let fence = fence.clone();
                        let message = OperationId::from_bytes([54; 16]);
                        let payload = fixture.payloads.stage(message, "body", b"7").await?;
                        let mut fenced = false;
                        let mut published = false;
                        for action in [a, b, c, d] {
                            match action {
                                0 | 1 => {
                                    if !fenced {
                                        host.claim_model_dispatch(
                                            task,
                                            message,
                                            0,
                                            [7; 32],
                                            fence.clone(),
                                        )
                                        .await?;
                                        assert!(
                                            host.claim_model_dispatch(
                                                task,
                                                message,
                                                0,
                                                [8; 32],
                                                fence.clone()
                                            )
                                            .await
                                            .is_err()
                                        );
                                    }
                                    if action == 0 && !fenced {
                                        provider.commit_lose_ack.store(true, Ordering::SeqCst);
                                    }
                                    let sent = host
                                        .send_owned(
                                            task,
                                            fence.clone(),
                                            task,
                                            message,
                                            payload.clone(),
                                        )
                                        .await;
                                    provider.commit_lose_ack.store(false, Ordering::SeqCst);
                                    if fenced {
                                        assert!(
                                            sent.is_err(),
                                            "stale or cancelled sender succeeded"
                                        );
                                    } else {
                                        // A lost reply may report uncertainty after the original atomic publication.
                                        let inbox = host.inbox(task, 0, 2).await?;
                                        assert_eq!(inbox.len(), 1);
                                        assert_eq!(inbox[0].message_id, message.to_string());
                                        published = true;
                                        assert_eq!(
                                            host.send_owned(
                                                task,
                                                fence.clone(),
                                                task,
                                                message,
                                                payload.clone()
                                            )
                                            .await?
                                            .operation_id,
                                            message
                                        );
                                        assert!(
                                            host.send_owned(
                                                task,
                                                fence.clone(),
                                                task,
                                                message,
                                                fixture
                                                    .payloads
                                                    .stage(message, "changed", b"8")
                                                    .await?
                                            )
                                            .await
                                            .is_err()
                                        );
                                    }
                                }
                                2 => {
                                    if !host.cancellation_requested(task, None).await? {
                                        let WorkPull::Claimed(lease) =
                                            host.recover_work(task).await?
                                        else {
                                            return Err(Error::NotFound(
                                                "routing original lease".into(),
                                            ));
                                        };
                                        host.coordinator
                                            .lock()
                                            .await
                                            .release_lease(
                                                &lease,
                                                IdempotencyKey::new("routing-replace")?,
                                            )
                                            .await?;
                                        let WorkPull::Claimed(replacement) = host
                                            .pull_operation(
                                                &Worker {
                                                    id: "replacement".into(),
                                                    available: Default::default(),
                                                    labels: BTreeMap::new(),
                                                },
                                                OperationId::from_bytes(task.into_bytes()),
                                            )
                                            .await?
                                        else {
                                            return Err(Error::NotFound(
                                                "routing replacement lease".into(),
                                            ));
                                        };
                                        host.start_task(&replacement, None).await?;
                                        assert_ne!(
                                            crate::scheduler::LeaseFence::from(
                                                &replacement.reservation
                                            ),
                                            fence
                                        );
                                    }
                                    fenced = true;
                                }
                                _ => {
                                    host.cancel(task).await?;
                                    fenced = true;
                                }
                            }
                            assert_eq!(
                                host.mailbox(task)?
                                    .bounds()
                                    .await
                                    .map(|bounds| bounds.tail)
                                    .unwrap_or(0),
                                u64::from(published)
                            );
                        }
                        let inbox = host.inbox(task, 0, 2).await?;
                        assert_eq!(inbox.len(), usize::from(published));
                        assert!(
                            host.claim_model_dispatch(task, message, 0, [7; 32], fence)
                                .await
                                .is_err()
                        );
                    }
                }
            }
        }
        Ok(())
    }
    #[cfg(feature = "filesystem")]
    #[tokio::test]
    async fn recovery_after_pull_response_loss_keeps_original_reservation_without_writes()
    -> Result<()> {
        use crate::distributed::{WorkLease, WorkPull, Worker};
        use std::sync::atomic::Ordering;

        for mode in 0..3 {
            let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
            let stream = provider.clone();
            let fixture = effect_task_fixture(stream.clone(), "test.recovery", Vec::new()).await?;
            let host = fixture.host;
            let (task, _) = fixture.binding.task_binding();
            let WorkPull::Claimed(previous) = host.recover_work(task).await? else {
                return Err(Error::NotFound("original running reservation".into()));
            };
            host.coordinator
                .lock()
                .await
                .release_lease(&previous, IdempotencyKey::new("recover-fixture-release")?)
                .await?;
            if mode == 1 {
                provider.lose_ack.store(true, Ordering::SeqCst);
                provider.hide_receipt.store(true, Ordering::SeqCst);
            } else if mode == 2 {
                provider.location_fault.store(4, Ordering::SeqCst);
            }
            let response = host
                .pull_operation(
                    &Worker {
                        id: "reopened-worker".into(),
                        available: Default::default(),
                        labels: BTreeMap::new(),
                    },
                    OperationId::from_bytes(task.into_bytes()),
                )
                .await?;
            let expected = match response {
                WorkPull::Claimed(lease) if mode == 0 => lease,
                WorkPull::Unresolved { lease, .. } if mode != 0 => lease,
                _ => return Err(Error::Conflict("pull fault did not execute".into())),
            };
            let reopened = Arc::new(CoordinatorTaskHost::new(
                DistributedCoordinator::open(&stream, fixture.payloads.clone(), None, &mut None).await?,
                stream.clone(),
                host.payloads.clone(),
                host.reader.clone(),
                host.verifier.audience().clone(),
                host.owner_scope.clone(),
                host.verifier.clone(),
                host.root_scope.clone(),
                host.tasks.clone(),
                host.machines.clone(),
                host.clock.clone(),
            )?);
            drop(fixture.binding);
            drop(host);
            let journal = acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), "harness/v3/coordinator/events")?;
            let tail = journal.tail().await?;
            provider.forbid_writes.store(true, Ordering::SeqCst);
            let recovered = reopened.recover_work(task).await?;
            assert_eq!(journal.tail().await?, tail);
            assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
            assert!(
                reopened
                    .recover_work(TaskId::from_bytes([201; 16]))
                    .await
                    .is_err()
            );
            assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
            if mode == 2 {
                assert!(matches!(recovered, WorkPull::Idle));
                continue;
            }
            let WorkPull::Claimed(recovered) = recovered else {
                return Err(Error::NotFound("committed original reservation".into()));
            };
            assert_eq!(recovered, expected);
            assert_ne!(recovered.reservation, previous.reservation);
            provider.forbid_writes.store(false, Ordering::SeqCst);
            reopened.start_task(&recovered, None).await?;
            assert!(reopened.start_task(&previous, None).await.is_err());
            reopened.cancel(task).await?;
            let tail = journal.tail().await?;
            provider.forbid_writes.store(true, Ordering::SeqCst);
            let WorkPull::Unresolved { lease, .. } = reopened.recover_work(task).await? else {
                return Err(Error::Conflict(
                    "cancelled reservation was made ready".into(),
                ));
            };
            let WorkLease {
                reservation,
                operation,
                ..
            } = lease;
            assert_eq!(reservation, recovered.reservation);
            assert_eq!(operation, recovered.operation);
            assert_eq!(journal.tail().await?, tail);
            assert!(reopened.start_task(&recovered, None).await.is_err());
        }
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    #[tokio::test]
    async fn task_effect_dispatch_race_and_cancelled_settlement_use_one_owner() -> Result<()> {
        use crate::{
            core::{EffectGuarantee, EffectStatus, SchemaRegistry},
            effect_host::{ConversationEffectHost, TaskEffectPlan},
            effects::{EffectDispatch, EffectObservation, EffectProvider, EffectRegistry},
        };
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        struct ReceiptProvider {
            calls: AtomicUsize,
            known: AtomicBool,
            request: Mutex<Option<EffectDispatch>>,
        }
        impl EffectProvider for ReceiptProvider {
            fn id(&self) -> &str {
                "test.receipt"
            }
            fn guarantees(&self, _: &str) -> BTreeSet<EffectGuarantee> {
                BTreeSet::from([EffectGuarantee::AtMostOnce])
            }
            fn linearizable_reconciliation(&self) -> bool {
                false
            }
            fn dispatch<'a>(
                &'a self,
                request: EffectDispatch,
            ) -> BoxFuture<'a, Result<EffectObservation>> {
                Box::pin(async move {
                    self.calls.fetch_add(1, Ordering::SeqCst);
                    *self.request.lock().await = Some(request);
                    Err(Error::Storage("lost provider reply".into()))
                })
            }
            fn reconcile<'a>(
                &'a self,
                attempt: crate::EffectAttemptId,
            ) -> BoxFuture<'a, Result<Option<EffectObservation>>> {
                Box::pin(async move {
                    if !self.known.load(Ordering::SeqCst) {
                        return Ok(None);
                    }
                    let request = self
                        .request
                        .lock()
                        .await
                        .clone()
                        .ok_or_else(|| Error::NotFound("receipt".into()))?;
                    assert_eq!(request.attempt_id, attempt);
                    Ok(Some(EffectObservation {
                        provider: request.provider,
                        effect_id: request.effect_id,
                        attempt_id: attempt,
                        request_digest: request.request_digest,
                        guarantee: request.guarantee,
                        status: EffectStatus::Failed {
                            message: "observed exit".into(),
                        },
                    }))
                })
            }
        }

        let EffectTaskFixture {
            host,
            binding,
            payloads,
            runtime_scope,
        } = effect_task_fixture(
            Arc::new(MemoryStream::default()),
            "test.receipt",
            Vec::new(),
        )
        .await?;
        let stream = binding.stream();
        let operation = binding.operation_id();
        let (task, fence) = binding.task_binding();
        let request = payloads.stage(operation, "request", b"{}").await?;
        let schema = FileRef::new(
            request.volume().clone(),
            request.path(),
            request.version(),
            FileDescriptor::from_bytes(b"{}", "application/schema+json")?,
            "schema.json",
        )?;
        let plan = TaskEffectPlan {
            provider: "test.receipt".into(),
            guarantee: EffectGuarantee::AtMostOnce,
            effect_kind: "test.exit.v1".into(),
            request,
            result_schema: schema,
        };
        let receipt = Arc::new(ReceiptProvider {
            calls: AtomicUsize::new(0),
            known: AtomicBool::new(false),
            request: Mutex::new(None),
        });
        let mut providers = EffectRegistry::default().with_result_resolver(payloads.clone());
        providers.register(receipt.clone())?;
        let conversation = Authority {
            kind: AggregateKind::Conversation,
            id: "effect-conversation".into(),
        };
        let conversation_issuer =
            AuthorityIssuer::new("effect-conversation", [54; 32], conversation.clone());
        let conversation_scope = conversation_issuer.issue("owner", &runtime_scope.grants(), None, None, &mut None).expect("scope proof");
        let effects = ConversationEffectHost::new(
            Arc::new(MemoryStream::default()),
            conversation.clone(),
            conversation_issuer.clone(),
            conversation_scope,
            SchemaRegistry::new(),
            payloads.clone(),
            providers,
        )?;
        let command = OperationId::from_bytes([55; 16]);
        let (first, second) = tokio::join!(
            effects.run_task_effect(&binding, command, plan.clone()),
            effects.run_task_effect(&binding, command, plan.clone())
        );
        assert!(matches!(
            (&first, &second),
            (Err(_), Ok(EffectStatus::Indeterminate)) | (Ok(EffectStatus::Indeterminate), Err(_))
        ));
        assert_eq!(receipt.calls.load(Ordering::SeqCst), 1);
        let mut mutated = plan.clone();
        mutated.effect_kind = "changed.v1".into();
        assert!(matches!(
            effects
                .run_task_effect(&binding, command, mutated.clone())
                .await,
            Err(Error::Conflict(_))
        ));
        assert!(matches!(
            effects
                .reconcile_task_effect(&binding, command, &mutated)
                .await,
            Err(Error::Conflict(_))
        ));
        host.cancel(task).await?;
        assert!(
            effects
                .run_task_effect(&binding, command, plan.clone())
                .await
                .is_err()
        );
        assert_eq!(
            effects
                .reconcile_task_effect(&binding, command, &plan)
                .await?,
            EffectStatus::Indeterminate
        );
        receipt.known.store(true, Ordering::SeqCst);
        assert_eq!(
            effects
                .reconcile_task_effect(&binding, command, &plan)
                .await?,
            EffectStatus::Failed {
                message: "observed exit".into()
            }
        );
        let aggregate = crate::store::StreamAggregate::open(
            &stream,
            conversation,
            conversation_issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
        .await?;
        assert_eq!(aggregate.reducer().revision(), 3);
        assert_eq!(receipt.calls.load(Ordering::SeqCst), 1);
        host.settle_task(task, &fence, Outcome::Cancelled, &mut None).await?;
        assert!(
            effects
                .reconcile_task_effect(&binding, command, &plan)
                .await
                .is_err()
        );
        Ok(())
    }

    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    mod native_process_tests;

    #[tokio::test]
    async fn model_claims_use_pinned_ceiling_and_current_owner() -> Result<()> {
        model_claim_host(Arc::new(MemoryStream::default()))
            .await
            .map(|_| ())
    }

    #[cfg(feature = "filesystem-local")]
    #[tokio::test]
    async fn owned_task_journal_guards_survive_local_stream_reopen() -> Result<()> {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let payloads = model_claim_host(Arc::new(provider)).await?;
        let provider = acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let stream = Arc::new(provider);
        assert_eq!(
            acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), format!(
                    "harness/v2/workflows/{}",
                    OperationId::from_bytes([77; 16])
                ))?
                .tail()
                .await?,
            1
        );
        assert_eq!(
            acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), format!(
                    "harness/v2/execution/{}",
                    OperationId::from_bytes([78; 16])
                ))?
                .tail()
                .await?,
            75
        );
        let coordinator = DistributedCoordinator::open(&stream, payloads, None, &mut None).await?;
        let operation = coordinator
            .scheduler()
            .operation(OperationId::from_bytes([77; 16]))
            .ok_or_else(|| Error::NotFound("reopened owned task".into()))?;
        assert!(operation.cancellation_requested);
        assert!(operation.reservation.is_some());
        Ok(())
    }

    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn owned_task_journal_native_release_requires_original_settlement_and_binding()
    -> Result<()> {
        use crate::executor::{ExecutionEvent, ExecutionJournal as _, ToolFailureKind};
        use crate::filesystem::{
            FilesystemContentVerifier, FilesystemExecutionJournal, FilesystemHost,
            FilesystemSchedulerPayloadStore, FilesystemWorkflowJournal,
        };
        use crate::workflow::WorkflowJournal as _;

        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await.map_err(|error| Error::Storage(error.to_string()))?;
        let stream = Arc::new(provider);
        let filesystem_provider = ProviderRef::new("native-owned-release", "filesystem", "2")?;
        let filesystem = Arc::new(FilesystemHost::new(
            acyclic_fs::Fs::memory(), filesystem_provider.clone(),
        )?);
        let agent = AgentId::from_bytes([1; 16]);
        let volume = VolumeRef::new(
            filesystem_provider.clone(), "native-owned", VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )?;
        filesystem.create_volume(&volume).await?;
        let authority = Authority { kind: AggregateKind::Task, id: "effect-task-owner".into() };
        let issuer = AuthorityIssuer::new("effect-task", [51; 32], authority);
        let volume_grants = vec![
            volume.capability(crate::conversation::VolumeOperation::Read)?,
            volume.capability(crate::conversation::VolumeOperation::Write)?,
        ];
        let content_scope = issuer.issue("native-owned-content", &Capabilities::new(volume_grants.clone()), Some(agent), None, &mut None).expect("scope proof");
        let payloads = Arc::new(FilesystemSchedulerPayloadStore::new(
            filesystem.clone(), &volume, &issuer.verifier(&mut None).expect("verifier copy"), &content_scope, 65_536, None,
        )?);
        let reader = Arc::new(FilesystemContentVerifier::new(
            filesystem.clone(), &issuer.verifier(&mut None).expect("verifier copy"), &content_scope, 65_536, None,
        )?);
        let (host, owner, runtime_scope) = effect_task_fixture_with_storage(
            stream.clone(), "test.native-release", volume_grants,
            payloads, reader, &volume,
        ).await?;
        let (task, fence) = owner.task_binding();
        let fence = fence.clone();
        let operation = OperationId::from_bytes(task.into_bytes());
        let attempt = OperationId::from_bytes([54; 16]);
        let workflow = Arc::new(FilesystemWorkflowJournal::for_task(
            owner.clone(), filesystem.clone(), volume.clone(),
            issuer.verifier(&mut None).expect("verifier copy"), content_scope.clone(), 65_536,
        )?);
        let journal = FilesystemExecutionJournal::for_task(
            owner.clone(), attempt, attempt, filesystem.clone(), &volume,
            &issuer.verifier(&mut None).expect("verifier copy"), &content_scope, 65_536, None,
        )?;
        let units = 100_000;
        let bytes = 100_000_000;
        let budget = acyclic_fs::performance::work_budget(units, bytes);
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = test_work(budget, &cancellation)?;
        assert!(!journal.release_if_quiescent_with_native_work(&work, &workflow).await?);
        workflow.admit(owner.workflow_admission().clone()).await?;
        assert!(!journal.release_if_quiescent_with_native_work(&work, &workflow).await?);
        journal.append(attempt, "started".into(), ExecutionEvent::Started).await?;
        let invocation = crate::tool::ToolInvocation::for_model_call(
            attempt, 0, "native-owned-tool".into(), "test".into(), serde_json::json!({}),
            None,
        )?;
        let invocation = crate::executor::stage_json(&journal, attempt, "tool-invocation", &invocation, None).await?;
        journal.append(attempt, "tool-started".into(), ExecutionEvent::ToolStarted {
            step: 0, call_id: "native-owned-tool".into(), invocation,
        }).await?;
        let machine_identity = owner.workflow_admission().initial.machine.clone();
        let mut workflow_host = crate::workflow::DurableWorkflowHost::open(
            host.machines.clone(), owner.workflow_admission().initial.clone(), workflow.clone(),
        ).await?;
        let transition = workflow_host.step_checked_with_native_work(
            OperationId::from_bytes([57; 16]), IdempotencyKey::new("native-suspended-step")?,
            serde_json::json!(0), &work, |_| Ok(()),
        ).await?;
        assert!(matches!(transition.0.status, MachineStatus::Suspended));
        assert!(work.receipt().backend_read_operations > 0);
        assert!(work.receipt().backend_write_operations > 0);
        drop(transition);
        assert!(!journal.release_if_quiescent_with_native_work(&work, &workflow).await?);
        {
            let coordinator = host.coordinator.lock().await;
            let current = coordinator.scheduler().operation(operation)
                .ok_or_else(|| Error::NotFound("original active native release task".into()))?;
            assert_eq!(current.phase, crate::scheduler::OperationPhase::Running);
            assert!(current.reservation.is_some());
        }
        journal.append(attempt, "tool-failed".into(), ExecutionEvent::ToolFailed {
            step: 0, call_id: "native-owned-tool".into(), reason: ToolFailureKind::ExecutorRejected,
        }).await?;

        let cancelled = acyclic_fs::CancellationToken::new();
        let cancelled_work = test_work(budget, &cancelled)?;
        let cancelled_prefix = cancelled_work.receipt();
        cancelled.cancel();
        assert!(matches!(
            journal.release_if_quiescent_with_native_work(&cancelled_work, &workflow).await,
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None
            })
        ));
        assert_eq!(cancelled_work.receipt(), cancelled_prefix);
        let quota_work = test_work(acyclic_fs::WorkBudget { backend_read_operations: 0, ..budget }, &cancellation)?;
        let quota_live = quota_work.live_allocation_bytes();
        assert!(matches!(
            journal.release_if_quiescent_with_native_work(&quota_work, &workflow).await,
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::BudgetExceeded { counter: "backend_read_operations", .. }
                ), custody: None
            })
        ));
        assert_eq!(quota_work.receipt().backend_read_operations, 0);
        assert_eq!(quota_work.live_allocation_bytes(), quota_live);
        assert!(host.coordinator.lock().await.scheduler().operation(operation)
            .ok_or_else(|| Error::NotFound("quota-refused original task".into()))?
            .reservation.is_some());

        {
            let proof_io = crate::filesystem::PreparationIo::from_native_work(&work)?;
            let pending = workflow.prove_release_with_io(&proof_io).await?
                .ok_or_else(|| Error::NotFound("original Suspended source proof".into()))?;
            let execution_tail = acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), format!("harness/v2/execution/{attempt}"))?
                .tail().await?;
            workflow_host.step(
                OperationId::from_bytes([58; 16]), IdempotencyKey::new("native-next-suspended-step")?,
                serde_json::json!(0), |_| Ok(())
            ).await?;
            assert!(matches!(
                owner.release_quiescent_execution_with_native_work(
                    attempt, execution_tail, &pending, &work,
                ).await,
                Err(Error::Conflict(_))
            ));
            let coordinator = host.coordinator.lock().await;
            let reservation = coordinator.scheduler().operation(operation)
                .and_then(|current| current.reservation.as_ref())
                .ok_or_else(|| Error::NotFound("workflow-raced original lease".into()))?;
            assert_eq!(crate::scheduler::LeaseFence::from(reservation), fence);
        }
        {
            let proof_io = crate::filesystem::PreparationIo::from_native_work(&work)?;
            let pending = workflow.prove_release_with_io(&proof_io).await?
                .ok_or_else(|| Error::NotFound("current Suspended source proof".into()))?;
            let execution_tail = acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), format!("harness/v2/execution/{attempt}"))?
                .tail().await?;
            let invocation = crate::tool::ToolInvocation::for_model_call(
                attempt, 1, "interleaved-native-tool".into(), "test".into(), serde_json::json!({}),
                None,
            )?;
            let invocation = crate::executor::stage_json(&journal, attempt, "interleaved-tool-invocation", &invocation, None).await?;
            journal.append(attempt, "interleaved-tool-started".into(), ExecutionEvent::ToolStarted {
                step: 1, call_id: "interleaved-native-tool".into(), invocation,
            }).await?;
            assert!(matches!(
                owner.release_quiescent_execution_with_native_work(
                    attempt, execution_tail, &pending, &work,
                ).await,
                Err(Error::Conflict(_))
            ));
            assert!(!journal.release_if_quiescent_with_native_work(&work, &workflow).await?);
            let coordinator = host.coordinator.lock().await;
            let reservation = coordinator.scheduler().operation(operation)
                .and_then(|current| current.reservation.as_ref())
                .ok_or_else(|| Error::NotFound("execution-raced original lease".into()))?;
            assert_eq!(crate::scheduler::LeaseFence::from(reservation), fence);
        }
        journal.append(attempt, "interleaved-tool-failed".into(), ExecutionEvent::ToolFailed {
            step: 1, call_id: "interleaved-native-tool".into(), reason: ToolFailureKind::ExecutorRejected,
        }).await?;

        let workflow_path = format!("harness/v2/workflows/{operation}");
        let execution_path = format!("harness/v2/execution/{attempt}");
        let workflow_tail = acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), &workflow_path)?.tail().await?;
        let execution_tail = acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), &execution_path)?.tail().await?;
        assert_eq!(workflow_tail, 2);
        assert_eq!(execution_tail, 5);
        assert!(journal.release_if_quiescent_with_native_work(&work, &workflow).await?);
        assert!(host.coordinator.lock().await.scheduler().operation(operation)
            .ok_or_else(|| Error::NotFound("settled original task".into()))?
            .reservation.is_none());
        assert_eq!(acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), &workflow_path)?.tail().await?, workflow_tail);
        assert_eq!(acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), &execution_path)?.tail().await?, execution_tail);
        assert!(work.receipt().object_bytes_read > 0);

        let worker = crate::distributed::Worker {
            id: "replacement-native-worker".into(), available: Default::default(),
            labels: BTreeMap::new(),
        };
        let crate::distributed::WorkPull::Claimed(replacement) =
            host.pull_operation(&worker, operation).await?
        else { return Err(Error::NotFound("replacement native lease".into()).into()); };
        host.start_task(&replacement, None).await?;
        let replacement_owner = host.journal_owner(
            task, crate::scheduler::LeaseFence::from(&replacement.reservation),
        ).await?;
        assert_ne!(replacement_owner.task_binding().1, &fence);
        let replacement_workflow = FilesystemWorkflowJournal::for_task(
            replacement_owner.clone(), filesystem.clone(), volume.clone(),
            issuer.verifier(&mut None).expect("verifier copy"), content_scope.clone(), 65_536,
        )?;
        let denied_prefix = work.receipt();
        assert!(matches!(
            journal.release_if_quiescent_with_native_work(&work, &replacement_workflow).await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(work.receipt(), denied_prefix);
        let replacement_journal = FilesystemExecutionJournal::for_task(
            replacement_owner.clone(), OperationId::from_bytes([55; 16]),
            OperationId::from_bytes([55; 16]),
            filesystem.clone(), &volume, &issuer.verifier(&mut None).expect("verifier copy"), &content_scope, 65_536, None,
        )?;
        let other_volume = VolumeRef::new(
            filesystem_provider, "other-native-owned", VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )?;
        filesystem.create_volume(&other_volume).await?;
        let other_scope = issuer.issue("other-native-content", &Capabilities::new(vec![
            other_volume.capability(crate::conversation::VolumeOperation::Read)?,
            other_volume.capability(crate::conversation::VolumeOperation::Write)?,
        ]), Some(agent), None, &mut None).expect("scope proof");
        let other_volume_workflow = FilesystemWorkflowJournal::for_task(
            replacement_owner, filesystem.clone(), other_volume,
            issuer.verifier(&mut None).expect("verifier copy"), other_scope, 65_536,
        )?;
        assert!(matches!(
            replacement_journal.release_if_quiescent_with_native_work(&work, &other_volume_workflow).await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(work.receipt(), denied_prefix);
        let other_issuer = AuthorityIssuer::new("other-native-owner", [91; 32], Authority {
            kind: AggregateKind::Task, id: "other-native-owner".into(),
        });
        assert!(matches!(
            FilesystemWorkflowJournal::for_task(
                owner, filesystem.clone(), volume.clone(), other_issuer.verifier(&mut None).expect("verifier copy"),
                content_scope.clone(), 65_536,
            ),
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(work.receipt(), denied_prefix);

        let other_operation = OperationId::from_bytes([56; 16]);
        host.admit(TaskAdmissionRecord {
            operation_id: other_operation,
            task: host.tasks.get::<Value, Value>("test.effect")?.identity().clone(),
            machine: machine_identity, input: serde_json::json!(0),
            input_schema: serde_json::json!({"type":"integer"}),
            output_schema: serde_json::json!({"type":"integer"}), parent: None,
            grants: runtime_scope.grants().clone(), limits: runtime_scope.limits(),
            run_limits: runtime_scope.run_limits(), policy: None, extensions: None, execution: None,
        }).await?;
        let crate::distributed::WorkPull::Claimed(other_lease) =
            host.pull_operation(&worker, other_operation).await?
        else { return Err(Error::NotFound("other real native lease".into()).into()); };
        host.start_task(&other_lease, None).await?;
        let other_owner = host.journal_owner(
            TaskId::from_bytes(other_operation.into_bytes()),
            crate::scheduler::LeaseFence::from(&other_lease.reservation),
        ).await?;
        let other_workflow = FilesystemWorkflowJournal::for_task(
            other_owner, filesystem, volume, issuer.verifier(&mut None).expect("verifier copy"), content_scope, 65_536,
        )?;
        assert!(matches!(
            replacement_journal.release_if_quiescent_with_native_work(&work, &other_workflow).await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(work.receipt(), denied_prefix);
        assert!(host.coordinator.lock().await.scheduler().operation(operation)
            .ok_or_else(|| Error::NotFound("binding-refused original lease".into()))?
            .reservation.is_some());
        Ok(())
    }

    async fn model_claim_host<P: StreamProvider>(
        stream: std::sync::Arc<P>,
    ) -> Result<Arc<MemoryPayloads>> {
        let prepared_request = crate::model::PreparedModelRequest::prepare(
            crate::model::ModelRequest {
                model: crate::model::Model::new(
                    "synthetic",
                    "journal-test",
                    "1",
                    serde_json::json!({}),
                )?,
                messages: vec![crate::model::ModelMessage {
                    role: crate::model::ModelRole::User,
                    content: crate::model::ModelContent::Text("fixture request".into()),
                }],
                tools: Vec::new(),
                max_output_tokens: Some(32),
            },
            Limits::default(),
            None,
        )?;
        let model_digest = prepared_request.request_digest();
        let payloads = Arc::new(MemoryPayloads::new()?);
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "model-owner".into(),
        };
        let issuer = AuthorityIssuer::new("model-test", [7; 32], owner.clone());
        let owner_scope = issuer.issue("owner", &Capabilities::new([
                "operation:declare",
                "operation:observe",
                "operation:cancel",
                "task:spawn:test.model@1",
                "model:generate",
                "interaction:route",
            ]), None, None, &mut None).expect("scope proof");
        let scope = RuntimeScope::new(owner_scope.capabilities().clone(), Limits::default())?;
        let machine = MachineIdentity {
            name: "test.model".into(),
            version: "1".into(),
            digest: [2; 32],
        };
        let implementation: Arc<dyn ResumableMachine> = Arc::new(BatchMachine {
            identity: machine.clone(),
            schema: serde_json::json!({"type": "integer"}),
        });
        let (task, machine, tasks, machines) = test_registries::<i64>(
            implementation, serde_json::json!({"type": "integer"}), serde_json::json!({"type": "integer"}),
        )?;
        let host = Arc::new(CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?,
            stream.clone(),
            payloads.clone(),
            payloads.clone(),
            owner.clone(),
            owner_scope.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            scope.clone(),
            tasks,
            machines,
            Arc::new(SystemUnixMillisClock),
        )?);
        let operation_id = OperationId::from_bytes([77; 16]);
        let task_id = TaskId::from_bytes(operation_id.into_bytes());
        host.admit(TaskAdmissionRecord {
            resource_proofs: Vec::new(),
            operation_id,
            task,
            machine,
            input: serde_json::json!(0),
            input_schema: serde_json::json!({"type": "integer"}),
            output_schema: serde_json::json!({"type": "integer"}),
            parent: None,
            grants: scope.grants().clone(),
            limits: scope.limits(),
            run_limits: crate::runtime::TaskRunLimits {
                max_steps: Some(1),
                ..scope.run_limits()
            },
            policy: None,
            extensions: None,
            execution: None,
        })
        .await?;
        let fence = {
            let mut coordinator = host.coordinator.lock().await;
            coordinator
                .configure_session(
                    &owner,
                    &owner_scope,
                    &issuer.verifier(&mut None).expect("verifier copy"),
                    operation_id,
                    crate::scheduler::SessionLimits {
                        active_tasks: 1,
                        total_tasks: 1,
                        depth: 0,
                        model_steps: 2,
                    },
                    IdempotencyKey::new("model-session")?,
                )
                .await?;
            let lease = coordinator
                .pull(&crate::distributed::Worker {
                    id: "model-worker".into(),
                    available: crate::scheduler::ResourceSnapshot::default(),
                    labels: BTreeMap::new(),
                })
                .await?
                .ok_or_else(|| Error::NotFound("model lease".into()))?;
            let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new("model-start")?,
                    crate::scheduler::SchedulerEvent::Started {
                        operation_id,
                        fence: fence.clone(),
                    }, &mut None,
                )
                .await?;
            fence
        };
        host.verify_execution_owner(task_id, fence.clone()).await?;
        let attempt = OperationId::from_bytes([78; 16]);
        host.claim_model_dispatch(task_id, attempt, 0, model_digest, fence.clone())
            .await?;
        #[cfg(feature = "filesystem")]
        let mut workflow_prepare_io = None;
        #[cfg(feature = "filesystem")]
        let mut execution_prepare_io = None;
        #[cfg(feature = "filesystem")]
        let mut approval_prepare_io = None;
        #[cfg(feature = "filesystem")]
        let (workflow, filesystem, volume, content_scope, original_owner, original_pending) = {
            use crate::workflow::{DurableWorkflowHost, WorkflowJournal};
            let provider = ProviderRef::new("owned-workflow", "filesystem", "2")?;
            let filesystem = Arc::new(crate::filesystem::FilesystemHost::new(
                acyclic_fs::Fs::memory(),
                provider.clone(),
            )?);
            let volume = VolumeRef::new(
                provider,
                "owned-private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            )?;
            filesystem.create_volume(&volume).await?;
            let content_scope = issuer.issue("workflow-content", &Capabilities::new([
                    volume.capability(crate::conversation::VolumeOperation::Read)?,
                    volume.capability(crate::conversation::VolumeOperation::Write)?,
                ]), Some(AgentId::from_bytes([1; 16])), None, &mut None).expect("scope proof");
            let original_owner = host.journal_owner(task_id, fence.clone()).await?;
            assert!(
                original_owner
                    .validate_storage(&issuer.verifier(&mut None).expect("verifier copy"), scope.limits().file_bytes + 1, &mut None)
                    .is_err()
            );
            let foreign = AuthorityIssuer::new(
                "foreign-owner",
                [8; 32],
                Authority {
                    kind: AggregateKind::Task,
                    id: "foreign".into(),
                },
            );
            assert!(
                original_owner
                    .validate_storage(&foreign.verifier(&mut None).expect("verifier copy"), 65_536, &mut None)
                    .is_err()
            );
            let binding = host.journal_owner(task_id, fence.clone()).await?;
            let admission = binding.workflow_admission().clone();
            let journal = Arc::new(crate::filesystem::FilesystemWorkflowJournal::for_task(
                binding,
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(&mut None).expect("verifier copy"),
                content_scope.clone(),
                65_536,
            )?);
            let release_budget = acyclic_fs::performance::work_budget(100_000, 100_000_000);
            let release_cancellation = acyclic_fs::CancellationToken::new();
            let before_admission = crate::filesystem::PreparationIo::from_budget(
                release_budget, &release_cancellation,
            )?;
            assert!(journal.prove_release_with_io(&before_admission).await?.is_none());
            let mut wrong = admission.clone();
            wrong.initial.state = serde_json::json!(1);
            assert!(journal.admit(wrong).await.is_err());
            assert_eq!(journal.admit(admission.clone()).await?, admission);
            let admission_only = crate::filesystem::PreparationIo::from_budget(
                release_budget, &release_cancellation,
            )?;
            assert!(journal.prove_release_with_io(&admission_only).await?.is_none());
            let mut workflow = DurableWorkflowHost::open(
                host.machines.clone(),
                admission.initial,
                journal.clone(),
            )
            .await?;
            workflow
                .step(
                    OperationId::from_bytes([85; 16]),
                    IdempotencyKey::new("owned-step")?,
                    serde_json::json!(0), |_| Ok(())
                )
                .await?;
            let release_io = crate::filesystem::PreparationIo::from_budget(
                release_budget, &release_cancellation,
            )?;
            let witness = journal.prove_release_with_io(&release_io).await?
                .ok_or_else(|| Error::Invalid("committed Suspended workflow needs its original witness".into()))?;
            assert_eq!(witness.task_binding(), (task_id, &fence));
            assert_eq!(witness.path(), format!("harness/v2/workflows/{}", OperationId::from_bytes(task_id.into_bytes())));
            assert_eq!(witness.tail(), 1);
            assert!(release_io.receipt().object_bytes_read > 0);
            let denied_budget = acyclic_fs::WorkBudget { peak_allocation_bytes: 0, ..release_budget };
            let denied = crate::filesystem::PreparationIo::from_budget(denied_budget, &release_cancellation)?;
            assert!(journal.prove_release_with_io(&denied).await.is_err());
            denied.receipt().verify(denied_budget).map_err(|error| Error::Invalid(error.to_string()))?;
            let cancelled = crate::filesystem::PreparationIo::from_budget(
                release_budget, &release_cancellation,
            )?;
            release_cancellation.cancel();
            assert!(journal.prove_release_with_io(&cancelled).await.is_err());
            assert_eq!(cancelled.receipt(), acyclic_fs::WorkCounters::default());
            let records = journal.replay(0, 1).await?;
            let [record] = records.as_slice() else {
                return Err(Error::Invalid("one owned transition required".into()));
            };
            assert!(matches!(
                journal
                    .commit(0, record.idempotency_key.clone(), record.clone())
                    .await?,
                crate::workflow::WorkflowCommitOutcome::Replayed(_)
            ));
            let mut forged = record.clone();
            forged.next.state = serde_json::json!(1);
            forged.transition.state = serde_json::json!(1);
            assert!(
                journal
                    .commit(0, forged.idempotency_key.clone(), forged)
                    .await
                    .is_err()
            );
            let path =
                acyclic_stream::StreamPath::new(format!("harness/v2/workflows/{operation_id}"))?;
            let key = StreamKey::new(OwnedBytes::from_static(b"owned-before-replacement"))?;
            let original_pending = original_owner
                .prepare_append_with_work(
                    &path,
                    1,
                    &key,
                    &OwnedBytes::from_static(b"never published"),
                    crate::distributed::JournalWrite::Fresh,
                    &mut workflow_prepare_io,
                )
                .await?;
            (
                journal,
                filesystem,
                volume,
                content_scope,
                original_owner,
                original_pending,
            )
        };
        #[cfg(feature = "filesystem")]
        let (execution, execution_pending, model_request) = {
            use crate::executor::{ExecutionEvent, ExecutionJournal};
            let journal = crate::filesystem::FilesystemExecutionJournal::for_task(
                host.journal_owner(task_id, fence.clone()).await?,
                attempt,
                attempt,
                filesystem.clone(),
                &volume,
                &issuer.verifier(&mut None).expect("verifier copy"),
                &content_scope,
                65_536, None,
            )?;
            let model_request = journal
                .stage(
                    attempt,
                    "model-request".into(),
                    prepared_request.bytes().to_vec(),
                    "application/json",
                )
                .await?;
            let pending_operation = OperationId::from_bytes([80; 16]);
            let digest = blake3::hash(format!("{pending_operation}:pending-execution").as_bytes());
            let key = StreamKey::new(OwnedBytes::copy_from_slice(digest.as_bytes()))?;
            let bytes = OwnedBytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "operation_id": pending_operation,
                    "turn_operation": pending_operation,
                    "version": 2,
                    "retry_digest": digest.to_hex().to_string(),
                    "event": ExecutionEvent::Started { request_digest: [9; 32] },
                }))
                .map_err(|error| Error::Invalid(error.to_string()))?,
            );
            let path = acyclic_stream::StreamPath::new(format!(
                "harness/v2/execution/{pending_operation}"
            ))?;
            let pending = original_owner
                .prepare_append_with_work(
                    &path,
                    0,
                    &key,
                    &bytes,
                    crate::distributed::JournalWrite::Fresh,
                    &mut execution_prepare_io,
                )
                .await?;
            journal
                .append(
                    attempt,
                    "request".into(),
                    ExecutionEvent::Started {
                        request_digest: [9; 32],
                    },
                )
                .await?;
            assert!(
                journal
                    .append(
                        attempt,
                        "wrong-claim".into(),
                        ExecutionEvent::ModelStarted {
                            step: 0,
                            purpose: crate::executor::ModelPurpose::Response,
                            request_digest: [4; 32],
                            request: model_request.clone(),
                        }
                    )
                    .await
                    .is_err()
            );
            assert!(
                journal
                    .append(
                        attempt,
                        "absent-claim".into(),
                        ExecutionEvent::ModelStarted {
                            step: 1,
                            purpose: crate::executor::ModelPurpose::Response,
                            request_digest: model_digest,
                            request: model_request.clone(),
                        }
                    )
                    .await
                    .is_err()
            );
            let start = ExecutionEvent::ModelStarted {
                step: 0,
                purpose: crate::executor::ModelPurpose::Response,
                request_digest: model_digest,
                request: model_request.clone(),
            };
            assert!(
                journal
                    .append_if_tail(attempt, 1, "model".into(), start.clone())
                    .await?
            );
            assert!(
                journal
                    .append_if_tail(attempt, 1, "model".into(), start)
                    .await?
            );
            assert_eq!(journal.replay(attempt, 0, 64).await?.len(), 2);
            assert!(
                journal
                    .append_if_tail(
                        attempt,
                        2,
                        "uncharged-summary".into(),
                        ExecutionEvent::ModelStarted {
                            step: 0,
                            purpose: crate::executor::ModelPurpose::Summary,
                            request_digest: model_digest,
                            request: model_request.clone(),
                        },
                    )
                    .await
                    .is_err(),
                "a response claim must not authorize a summary admission"
            );
            assert_eq!(journal.replay(attempt, 0, 64).await?.len(), 2);
            assert!(matches!(
                journal.suspend_workflow_command_if_quiescent(1, attempt).await,
                Err(Error::Indeterminate(id)) if id == attempt
            ));
            let invocation = crate::tool::ToolInvocation::for_model_call(
                attempt,
                0,
                "owned-tool".into(),
                "test".into(),
                serde_json::json!({}),
                None,
            )?;
            let invocation =
                crate::executor::stage_json(&journal, attempt, "tool-invocation", &invocation, None)
                    .await?;
            journal
                .append(
                    attempt,
                    "tool".into(),
                    ExecutionEvent::ToolStarted {
                        step: 0,
                        call_id: "owned-tool".into(),
                        invocation,
                    },
                )
                .await?;
            assert!(matches!(
                journal.suspend_workflow_command_if_quiescent(1, attempt).await,
                Err(Error::Indeterminate(id)) if id == attempt
            ));
            assert!(journal.replay(operation_id, 0, 1).await.is_err());
            assert!(matches!(
                journal
                    .interaction_outcome(crate::InteractionId::from_bytes([91; 16]))
                    .await,
                Err(Error::Unsupported(_))
            ));
            let uncharged = OperationId::from_bytes([79; 16]);
            let unbound = crate::filesystem::FilesystemExecutionJournal::new(
                stream.clone(),
                filesystem.clone(),
                &volume,
                &issuer.verifier(&mut None).expect("verifier copy"), &crate::core::SchemaRegistry::new(),
                &content_scope,
                65_536, None,
            )?;
            unbound
                .append(
                    uncharged,
                    "request".into(),
                    ExecutionEvent::Started {
                        request_digest: [9; 32],
                    },
                )
                .await?;
            unbound
                .append(
                    uncharged,
                    "model".into(),
                    ExecutionEvent::ModelStarted {
                        step: 0,
                        purpose: crate::executor::ModelPurpose::Response,
                        request_digest: model_digest,
                        request: model_request.clone(),
                    },
                )
                .await?;
            let adopted = crate::filesystem::FilesystemExecutionJournal::for_task(
                host.journal_owner(task_id, fence.clone()).await?,
                uncharged,
                uncharged,
                filesystem.clone(),
                &volume,
                &issuer.verifier(&mut None).expect("verifier copy"),
                &content_scope,
                65_536, None,
            )?;
            assert!(
                adopted.replay(uncharged, 0, 64).await.is_err(),
                "cold replay must reject a model start lacking its retained charge"
            );
            (journal, pending, model_request)
        };
        host.claim_model_dispatch(task_id, attempt, 0, model_digest, fence.clone())
            .await?;
        assert!(
            host.claim_model_dispatch(task_id, attempt, 1, [4; 32], fence.clone())
                .await
                .is_err(),
            "the task ceiling is one despite the larger session ceiling"
        );
        assert!(
            host.claim_model_dispatch(
                task_id,
                crate::executor::ModelPurpose::Summary.attempt_operation(attempt, None)?,
                0,
                [5; 32],
                fence.clone(),
            )
            .await
            .is_err(),
            "summary admissions must consume the same exhausted task ceiling"
        );
        assert!(
            host.claim_model_dispatch(task_id, attempt, 0, [4; 32], fence.clone())
                .await
                .is_err()
        );
        #[cfg(feature = "filesystem")]
        let (
            approval_journal,
            conversation_issuer,
            approval_id,
            approval_request,
            approval_pending,
        ) = {
            use crate::executor::ExecutionJournal;
            let conversation = Authority {
                kind: AggregateKind::Conversation,
                id: "owned-approval".into(),
            };
            let conversation_issuer =
                AuthorityIssuer::new("owned-approval", [54; 32], conversation.clone());
            let conversation_scope = conversation_issuer.issue("owner", &Capabilities::new([
                    "conversation:bind".to_owned(),
                    "interaction:open".to_owned(),
                    volume.capability(crate::conversation::VolumeOperation::Read)?,
                    volume.capability(crate::conversation::VolumeOperation::Write)?,
                ]), Some(AgentId::from_bytes([1; 16])), None, &mut None).expect("scope proof");
            let mut aggregate = crate::store::StreamAggregate::open(
                &stream,
                conversation.clone(),
                conversation_issuer.verifier(&mut None).expect("verifier copy"),
                crate::core::SchemaRegistry::new(), None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None)
            .await?;
            aggregate
                .execute(crate::core::Command {
                    operation_id: OperationId::from_bytes([90; 16]),
                    idempotency_key: IdempotencyKey::new("owned-approval-bind")?,
                    expected_revision: 0,
                    scope: conversation_scope.clone(),
                    causal_parent: None,
                    action: crate::core::Action::BindConversation {
                        agent: AgentId::from_bytes([1; 16]),
                    },
                }, #[cfg(feature = "filesystem")] None, &mut None)
                .await?;
            let journal = crate::filesystem::FilesystemExecutionJournal::for_task(
                host.journal_owner(task_id, fence.clone()).await?,
                attempt,
                attempt,
                filesystem.clone(),
                &volume,
                &issuer.verifier(&mut None).expect("verifier copy"),
                &content_scope,
                65_536, None,
            )?
            .with_interaction_owner(
                &conversation_issuer.verifier(&mut None).expect("verifier copy"),
                &crate::core::SchemaRegistry::new(),
                &conversation_scope,
                &volume, None,
            )?;
            let unbound = crate::filesystem::FilesystemExecutionJournal::new(
                stream.clone(),
                filesystem.clone(),
                &volume,
                &issuer.verifier(&mut None).expect("verifier copy"), &crate::core::SchemaRegistry::new(),
                &content_scope,
                65_536, None,
            )?;
            let response_scope = conversation_issuer.issue("owner", &Capabilities::new([
                    "interaction:open".to_owned(),
                    volume.capability(crate::conversation::VolumeOperation::Read)?,
                    volume.capability(crate::conversation::VolumeOperation::Write)?,
                ]), Some(AgentId::from_bytes([1; 16])), None, &mut None).expect("scope proof");
            assert!(
                matches!(
                    unbound.with_interaction_owner(
                        &conversation_issuer.verifier(&mut None).expect("verifier copy"),
                        &crate::core::SchemaRegistry::new(),
                        &response_scope,
                        &volume, None,
                    ),
                    Err(Error::Unauthorized(_))
                ),
                "a task storage owner is not a retained task lease"
            );
            let id = crate::InteractionId::from_bytes([91; 16]);
            let request = Interaction::approval("approve exact action", attempt, [5; 32])?;
            journal.open_interaction(id, request.clone()).await?;
            journal.open_interaction(id, request.clone()).await?;
            assert_eq!(journal.interaction_outcome(id).await?, None);
            assert_eq!(acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), conversation.stream_path(&mut None)?)?.tail().await?, 2);
            assert!(matches!(
                journal
                    .open_interaction(id, Interaction::approval("changed", attempt, [6; 32])?)
                    .await,
                Err(Error::Conflict(_))
            ));
            let pending = original_owner
                .prepare_append_with_work(
                    &acyclic_stream::StreamPath::new(conversation.stream_path(&mut None)?)?,
                    2,
                    &StreamKey::new(OwnedBytes::from_static(b"approval-before-replacement"))?,
                    &OwnedBytes::from_static(b"must not publish"),
                    crate::distributed::JournalWrite::Fresh,
                    &mut approval_prepare_io,
                )
                .await?;
            (journal, conversation_issuer, id, request, pending)
        };
        let mut stale = fence.clone();
        stale.reservation_id.push_str("-stale");
        assert!(
            host.verify_execution_owner(task_id, stale.clone())
                .await
                .is_err()
        );
        assert!(
            host.claim_model_dispatch(task_id, attempt, 0, model_digest, stale)
                .await
                .is_err()
        );
        let fence = {
            let mut coordinator = host.coordinator.lock().await;
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new("model-release")?,
                    crate::scheduler::SchedulerEvent::LeaseReleased {
                        operation_id,
                        fence: fence.clone(),
                    }, &mut None,
                )
                .await?;
            let lease = coordinator
                .pull(&crate::distributed::Worker {
                    id: "replacement-worker".into(),
                    available: crate::scheduler::ResourceSnapshot::default(),
                    labels: BTreeMap::new(),
                })
                .await?
                .ok_or_else(|| Error::NotFound("replacement model lease".into()))?;
            let replacement = crate::scheduler::LeaseFence::from(&lease.reservation);
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new("model-restart")?,
                    crate::scheduler::SchedulerEvent::Started {
                        operation_id,
                        fence: replacement.clone(),
                    }, &mut None,
                )
                .await?;
            replacement
        };
        #[cfg(feature = "filesystem")]
        let (replacement_approval, cancellation_approval_pending) = {
            use crate::executor::ExecutionJournal;
            assert!(matches!(
                approval_pending.commit(&stream, Some(operation_id), None, None, None).await?,
                (acyclic_stream::CommitOutcome::Conflict(_), 0)
            ));
            assert!(
                approval_journal
                    .open_interaction(approval_id, approval_request.clone())
                    .await
                    .is_err()
            );
            assert!(
                approval_journal
                    .interaction_outcome(approval_id)
                    .await
                    .is_err()
            );
            let conversation_scope = conversation_issuer.issue("owner", &Capabilities::new([
                    "interaction:open".to_owned(),
                    volume.capability(crate::conversation::VolumeOperation::Read)?,
                    volume.capability(crate::conversation::VolumeOperation::Write)?,
                ]), Some(AgentId::from_bytes([1; 16])), None, &mut None).expect("scope proof");
            let binding = host.journal_owner(task_id, fence.clone()).await?;
            let pending = binding
                .prepare_append_with_work(
                    &acyclic_stream::StreamPath::new(
                        conversation_issuer.verifier(&mut None).expect("verifier copy").audience().stream_path(&mut None)?,
                    )?,
                    2,
                    &StreamKey::new(OwnedBytes::from_static(b"approval-before-cancellation"))?,
                    &OwnedBytes::from_static(b"must not publish"),
                    crate::distributed::JournalWrite::Fresh,
                    &mut approval_prepare_io,
                )
                .await?;
            let journal = crate::filesystem::FilesystemExecutionJournal::for_task(
                binding,
                attempt,
                attempt,
                filesystem.clone(),
                &volume,
                &issuer.verifier(&mut None).expect("verifier copy"),
                &content_scope,
                65_536, None,
            )?
            .with_interaction_owner(
                &conversation_issuer.verifier(&mut None).expect("verifier copy"),
                &crate::core::SchemaRegistry::new(),
                &conversation_scope,
                &volume, None,
            )?;
            journal
                .open_interaction(approval_id, approval_request.clone())
                .await?;
            assert_eq!(journal.interaction_outcome(approval_id).await?, None);
            assert_eq!(
                acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), conversation_issuer.verifier(&mut None).expect("verifier copy").audience().stream_path(&mut None)?)?
                    .tail()
                    .await?,
                2
            );
            (journal, pending)
        };
        host.claim_model_dispatch(task_id, attempt, 0, model_digest, fence.clone())
            .await?;
        assert!(
            host.claim_model_dispatch(task_id, attempt, 1, [4; 32], fence.clone())
                .await
                .is_err()
        );
        #[cfg(feature = "filesystem")]
        let (replacement_workflow, pending) = {
            use crate::workflow::WorkflowJournal;
            assert!(
                matches!(
                    original_pending.commit(&stream, Some(operation_id), None, None, None).await?,
                    (acyclic_stream::CommitOutcome::Conflict(_), 0)
                ),
                "lease replacement after preflight must atomically prevent publication"
            );
            assert_eq!(
                acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), format!("harness/v2/workflows/{operation_id}"))?
                    .tail()
                    .await?,
                1
            );
            let record = workflow
                .replay(0, 1)
                .await?
                .pop()
                .ok_or_else(|| Error::NotFound("owned workflow record".into()))?;
            assert!(
                workflow
                    .commit(0, record.idempotency_key.clone(), record.clone())
                    .await
                    .is_err(),
                "the previous lease cannot replay a checkpoint mutation"
            );
            assert!(original_owner.verify(false, &mut None).await.is_err());
            let binding = host.journal_owner(task_id, fence.clone()).await?;
            let path =
                acyclic_stream::StreamPath::new(format!("harness/v2/workflows/{operation_id}"))?;
            let key = StreamKey::new(OwnedBytes::from_static(b"owned-pending"))?;
            let pending = binding
                .prepare_append_with_work(
                    &path,
                    1,
                    &key,
                    &OwnedBytes::from_static(b"never published"),
                    crate::distributed::JournalWrite::Fresh,
                    &mut workflow_prepare_io,
                )
                .await?;
            let journal = crate::filesystem::FilesystemWorkflowJournal::for_task(
                binding,
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(&mut None).expect("verifier copy"),
                content_scope.clone(),
                65_536,
            )?;
            assert!(matches!(
                journal
                    .commit(0, record.idempotency_key.clone(), record)
                    .await?,
                crate::workflow::WorkflowCommitOutcome::Replayed(_)
            ));
            (journal, pending)
        };
        #[cfg(feature = "filesystem")]
        let execution_pending =
            {
                assert!(
                    matches!(
                        execution_pending.commit(&stream, Some(operation_id), None, None, None).await?,
                        (acyclic_stream::CommitOutcome::Conflict(_), 0)
                    ),
                    "lease replacement must atomically fence execution publication"
                );
                let pending_operation = OperationId::from_bytes([80; 16]);
                let path = acyclic_stream::StreamPath::new(format!(
                    "harness/v2/execution/{pending_operation}"
                ))?;
                assert!(matches!(
                    stream.bounds(acyclic_stream::StreamPath::new(path.as_str())?).await,
                    Err(StreamError::NotFound)
                ));
                let digest =
                    blake3::hash(format!("{pending_operation}:pending-execution").as_bytes());
                let key = StreamKey::new(OwnedBytes::copy_from_slice(digest.as_bytes()))?;
                let bytes = OwnedBytes::from(serde_json::to_vec(&serde_json::json!({
                "operation_id": pending_operation,
                "turn_operation": pending_operation,
                "version": 2,
                "retry_digest": digest.to_hex().to_string(),
                "event": crate::executor::ExecutionEvent::Started { request_digest: [9; 32] },
            })).map_err(|error| Error::Invalid(error.to_string()))?);
                host.journal_owner(task_id, fence.clone())
                    .await?
                    .prepare_append_with_work(
                        &path,
                        0,
                        &key,
                        &bytes,
                        crate::distributed::JournalWrite::Fresh,
                        &mut execution_prepare_io,
                    )
                    .await?
            };
        host.coordinator
            .lock()
            .await
            .cancel_operation(
                &owner,
                &owner_scope,
                &issuer.verifier(&mut None).expect("verifier copy"),
                operation_id,
                IdempotencyKey::new("model-cancel")?,
                false, &mut None,
            )
            .await?;
        host.verify_execution_owner(task_id, fence.clone()).await?;
        #[cfg(feature = "filesystem")]
        {
            use crate::executor::{ExecutionEvent, ExecutionJournal};
            assert!(
                matches!(
                    execution_pending.commit(&stream, Some(operation_id), None, None, None).await?,
                    (acyclic_stream::CommitOutcome::Conflict(_), 0)
                ),
                "cancellation must atomically fence execution publication"
            );
            assert!(matches!(
                stream
                    .bounds(acyclic_stream::StreamPath::new(&format!(
                        "harness/v2/execution/{}",
                        OperationId::from_bytes([80; 16])
                    ))?)
                    .await,
                Err(StreamError::NotFound)
            ));
            assert!(
                execution
                    .append(
                        attempt,
                        "model".into(),
                        ExecutionEvent::ModelStarted {
                            step: 0,
                            purpose: crate::executor::ModelPurpose::Response,
                            request_digest: model_digest,
                            request: model_request.clone(),
                        }
                    )
                    .await
                    .is_err(),
                "stale lease must reject even an exact retry"
            );
            assert!(
                execution
                    .append(
                        attempt,
                        "stale-tool-settlement".into(),
                        ExecutionEvent::ToolFailed {
                            step: 0,
                            call_id: "owned-tool".into(),
                            reason: crate::executor::ToolFailureKind::ExecutorRejected
                        }
                    )
                    .await
                    .is_err(),
                "a replaced lease cannot settle the retained dispatch"
            );
            let journal = crate::filesystem::FilesystemExecutionJournal::for_task(
                host.journal_owner(task_id, fence.clone()).await?,
                attempt,
                attempt,
                filesystem.clone(),
                &volume,
                &issuer.verifier(&mut None).expect("verifier copy"),
                &content_scope,
                65_536, None,
            )?;
            assert!(
                journal
                    .append(
                        attempt,
                        "model".into(),
                        ExecutionEvent::ModelStarted {
                            step: 0,
                            purpose: crate::executor::ModelPurpose::Response,
                            request_digest: model_digest,
                            request: model_request.clone(),
                        }
                    )
                    .await
                    .is_err(),
                "cancellation forbids new or retried starts"
            );
            let bytes = crate::contract::canonical_json_bytes(
                &serde_json::to_value(crate::model::ModelEvent::Content {
                    delta: "settled".into(),
                })
                .map_err(|error| Error::Invalid(error.to_string()))?,
            )?;
            let reference = journal
                .stage(
                    attempt,
                    "model-observation".into(),
                    bytes,
                    "application/json",
                )
                .await?;
            let observation = ExecutionEvent::Model {
                step: 0,
                purpose: crate::executor::ModelPurpose::Response,
                event: reference.clone(),
            };
            assert!(
                journal
                    .append(
                        attempt,
                        "missing-tool".into(),
                        ExecutionEvent::ToolFailed {
                            step: 0,
                            call_id: "missing".into(),
                            reason: crate::executor::ToolFailureKind::ExecutorRejected
                        }
                    )
                    .await
                    .is_err()
            );
            let tool_settlement = ExecutionEvent::ToolFailed {
                step: 0,
                call_id: "owned-tool".into(),
                reason: crate::executor::ToolFailureKind::ExecutorRejected,
            };
            journal
                .append(attempt, "tool-settlement".into(), tool_settlement.clone())
                .await?;
            journal
                .append(attempt, "tool-settlement".into(), tool_settlement.clone())
                .await?;
            assert!(
                journal
                    .append(attempt, "another-tool-settlement".into(), tool_settlement)
                    .await
                    .is_err()
            );
            for index in 0..70 {
                journal
                    .append(attempt, format!("settlement-{index}"), observation.clone())
                    .await?;
            }
            journal
                .append(attempt, "settlement-69".into(), observation.clone())
                .await?;
            assert_eq!(journal.replay(attempt, 0, 64).await?.len(), 64);
            assert_eq!(journal.replay(attempt, 64, 64).await?.len(), 10);
            let reopened = crate::filesystem::FilesystemExecutionJournal::for_task(
                host.journal_owner(task_id, fence.clone()).await?,
                attempt,
                attempt,
                filesystem.clone(),
                &volume,
                &issuer.verifier(&mut None).expect("verifier copy"),
                &content_scope,
                65_536, None,
            )?;
            reopened
                .append(attempt, "settlement-69".into(), observation.clone())
                .await?;
            assert!(
                reopened
                    .append(
                        attempt,
                        "settlement-69".into(),
                        ExecutionEvent::Model {
                            step: 1,
                            purpose: crate::executor::ModelPurpose::Response,
                            event: reference,
                        }
                    )
                    .await
                    .is_err()
            );
            assert_eq!(reopened.replay(attempt, 64, 64).await?.len(), 10);
            assert!(matches!(
                reopened.suspend_workflow_command_if_quiescent(1, attempt).await,
                Err(Error::Indeterminate(id)) if id == attempt
            ));
            let completed = crate::executor::stage_json(&reopened, attempt, "model-completed", &crate::model::ModelEvent::Completed {
                    metadata: Value::Null,
                }, None)
            .await?;
            reopened
                .append(
                    attempt,
                    "model-completed".into(),
                    ExecutionEvent::Model {
                        step: 0,
                        purpose: crate::executor::ModelPurpose::Response,
                        event: completed,
                    },
                )
                .await?;
            assert!(
                reopened
                    .append(attempt, "after-completed".into(), observation.clone(),)
                    .await
                    .is_err()
            );
            assert!(
                reopened
                    .suspend_workflow_command_if_quiescent(1, attempt)
                    .await
                    .is_err()
            );
            reopened
                .append(attempt, "settlement-69".into(), observation)
                .await?;
            assert_eq!(reopened.replay(attempt, 64, 64).await?.len(), 11);
        }
        #[cfg(feature = "filesystem")]
        {
            use crate::workflow::WorkflowJournal;
            assert!(
                matches!(
                    pending.commit(&stream, Some(operation_id), None, None, None).await?,
                    (acyclic_stream::CommitOutcome::Conflict(_), 0)
                ),
                "cancellation after preflight must atomically prevent publication"
            );
            assert_eq!(
                acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), format!("harness/v2/workflows/{operation_id}"))?
                    .tail()
                    .await?,
                1
            );
            let record = replacement_workflow
                .replay(0, 1)
                .await?
                .pop()
                .ok_or_else(|| Error::NotFound("cancelled workflow record".into()))?;
            assert!(
                replacement_workflow
                    .commit(0, record.idempotency_key.clone(), record)
                    .await
                    .is_err(),
                "cancellation must reject checkpoint mutation retries"
            );
            assert!(
                host.journal_owner(task_id, fence.clone())
                    .await?
                    .verify(false, &mut None)
                    .await
                    .is_err()
            );
        }
        #[cfg(feature = "filesystem")]
        {
            use crate::executor::ExecutionJournal;
            assert!(matches!(
                cancellation_approval_pending.commit(&stream, Some(operation_id), None, None, None).await?,
                (acyclic_stream::CommitOutcome::Conflict(_), 0)
            ));
            assert!(
                replacement_approval
                    .open_interaction(approval_id, approval_request)
                    .await
                    .is_err()
            );
            assert_eq!(
                replacement_approval
                    .interaction_outcome(approval_id)
                    .await?,
                None
            );
            assert_eq!(
                acyclic_stream::Stream::new(std::sync::Arc::clone(&stream), conversation_issuer.verifier(&mut None).expect("verifier copy").audience().stream_path(&mut None)?)?
                    .tail()
                    .await?,
                2
            );
        }
        assert!(
            host.claim_model_dispatch(task_id, attempt, 0, model_digest, fence)
                .await
                .is_err()
        );
        Ok(payloads)
    }

    #[tokio::test]
    async fn admission_retry_retains_original_owner_grant_after_renewal() -> Result<()> {
        for session in [false, true] {
            let stream = Arc::new(MemoryStream::default());
            let payloads = Arc::new(MemoryPayloads::new()?);
            let owner = Authority {
                kind: AggregateKind::Task,
                id: "batch-owner".into(),
            };
            let issuer = AuthorityIssuer::new("batch-test", [7; 32], owner.clone());
            let owner_scope = issuer.issue("owner", &Capabilities::new([
                    "operation:declare",
                    "operation:observe",
                    "operation:cancel",
                    "task:spawn:test.batch@1",
                ]), None, None, &mut None).expect("scope proof");
            let runtime_scope =
                RuntimeScope::new(owner_scope.capabilities().clone(), Limits::default())?;
            let machine = MachineIdentity {
                name: "test.batch".into(),
                version: "1".into(),
                digest: [2; 32],
            };
            let implementation: Arc<dyn ResumableMachine> = Arc::new(BatchMachine {
                identity: machine.clone(),
                schema: serde_json::json!({"type": "integer"}),
            });
            let (task, machine, tasks, machines) = test_registries::<i64>(
                implementation, serde_json::json!({"type": "integer"}), serde_json::json!({"type": "integer"}),
            )?;
            let coordinator = DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?;
            let host = CoordinatorTaskHost::new(
                coordinator,
                stream.clone(),
                payloads.clone(),
                payloads.clone(),
                owner.clone(),
                owner_scope.clone(),
                issuer.verifier(&mut None).expect("verifier copy"),
                runtime_scope.clone(),
                tasks.clone(),
                machines.clone(),
                Arc::new(SystemUnixMillisClock),
            )?;
            let limits = crate::scheduler::SessionLimits {
                active_tasks: 8,
                total_tasks: 16,
                depth: 4,
                model_steps: 100,
            };
            let host = if session {
                host.with_session_limits(limits, None)?
            } else {
                host
            };
            let root_admission = |operation_id, input| TaskAdmissionRecord {
                operation_id,
                resource_proofs: Vec::new(),
                task: task.clone(),
                machine: machine.clone(),
                input,
                input_schema: serde_json::json!({"type": "integer"}),
                output_schema: serde_json::json!({"type": "integer"}),
                parent: None,
                grants: runtime_scope.grants().clone(),
                limits: runtime_scope.limits(),
                run_limits: runtime_scope.run_limits(),
                policy: None,
                extensions: runtime_scope.extensions().cloned(),
                execution: None,
            };

            let operation_id = OperationId::from_bytes([88; 16]);
            let admission = root_admission(operation_id, serde_json::json!(0));
            assert!(matches!(
                host.admit(admission.clone()).await?,
                Admission::Accepted(_)
            ));
            let original = host
                .retained_declaration_with_work(operation_id, &mut None)
                .await?
                .ok_or_else(|| Error::NotFound("original admission".into()))?;
            let lease = host
                .coordinator
                .lock()
                .await
                .pull(&crate::distributed::Worker {
                    id: "renewal-worker".into(),
                    available: crate::scheduler::ResourceSnapshot::default(),
                    labels: BTreeMap::new(),
                })
                .await?
                .ok_or_else(|| Error::NotFound("parent lease".into()))?;
            host.start_task(&lease, None).await?;
            let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
            let mut child = root_admission(OperationId::from_bytes([89; 16]), serde_json::json!(0));
            child.parent = Some(TaskId::from_bytes(operation_id.into_bytes()));
            assert!(matches!(
                host.admit_owned(child.clone(), fence.clone()).await?,
                Admission::Accepted(_)
            ));
            let original_child = host
                .retained_declaration_with_work(child.operation_id, &mut None)
                .await?
                .ok_or_else(|| Error::NotFound("original child admission".into()))?;
            let renewed = issuer.issue("renewed-owner", owner_scope.capabilities(), None, None, &mut None).expect("scope proof");
            let reopened = CoordinatorTaskHost::new(
                DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?,
                stream.clone(),
                payloads.clone(),
                payloads.clone(),
                owner.clone(),
                renewed,
                issuer.verifier(&mut None).expect("verifier copy"),
                runtime_scope.clone(),
                tasks.clone(),
                machines.clone(),
                Arc::new(SystemUnixMillisClock),
            )?;
            let reopened = if session {
                reopened.with_session_limits(limits, None)?
            } else {
                reopened
            };
            let revision = reopened.coordinator.lock().await.revision();
            assert!(matches!(
                reopened.admit(admission.clone()).await?,
                Admission::Accepted(_)
            ));
            assert_eq!(reopened.coordinator.lock().await.revision(), revision);
            assert_eq!(
                reopened.retained_declaration_with_work(operation_id, &mut None).await?,
                Some(original.clone())
            );
            assert!(matches!(
                reopened.admit_owned(child.clone(), fence.clone()).await?,
                Admission::Accepted(_)
            ));
            assert_eq!(reopened.coordinator.lock().await.revision(), revision);
            assert_eq!(
                reopened.retained_declaration_with_work(child.operation_id, &mut None).await?,
                Some(original_child.clone())
            );
            let mut stale = fence.clone();
            stale.reservation_id.push_str("-stale");
            assert!(matches!(
                reopened.admit_owned(child.clone(), stale).await,
                Err(Error::Unauthorized(_))
            ));
            assert_eq!(reopened.coordinator.lock().await.revision(), revision);
            let mut changed = admission.clone();
            changed.input = serde_json::json!(1);
            assert!(matches!(
                reopened.admit(changed).await,
                Err(Error::Conflict(_))
            ));
            let impostor = AuthorityIssuer::new("batch-test", [8; 32], owner.clone());
            let impostor_host = CoordinatorTaskHost::new(
                DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?,
                stream.clone(),
                payloads.clone(),
                payloads.clone(),
                owner.clone(),
                impostor.issue("renewed-owner", owner_scope.capabilities(), None, None, &mut None).expect("scope proof"),
                impostor.verifier(&mut None).expect("verifier copy"),
                runtime_scope.clone(),
                tasks.clone(),
                machines.clone(),
                Arc::new(SystemUnixMillisClock),
            )?;
            let impostor_host = if session {
                impostor_host.with_session_limits(limits, None)?
            } else {
                impostor_host
            };
            assert!(matches!(
                impostor_host.admit(admission).await,
                Err(Error::Unauthorized(_))
            ));
            assert_eq!(
                reopened.retained_declaration_with_work(operation_id, &mut None).await?,
                Some(original)
            );
            reopened
                .coordinator
                .lock()
                .await
                .cancel_operation(
                    &owner,
                    &reopened.owner_scope,
                    &issuer.verifier(&mut None).expect("verifier copy"),
                    operation_id,
                    IdempotencyKey::new("renewal-parent-cancel")?,
                    false, &mut None,
                )
                .await?;
            let cancelled_revision = reopened.coordinator.lock().await.revision();
            assert!(matches!(
                reopened.admit_owned(child.clone(), fence).await,
                Err(Error::Conflict(_))
            ));
            assert_eq!(
                reopened.coordinator.lock().await.revision(),
                cancelled_revision
            );
            assert_eq!(
                reopened.retained_declaration_with_work(child.operation_id, &mut None).await?,
                Some(original_child)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn partially_admitted_batch_reopens_and_fills_only_missing_slots() -> Result<()> {
        let stream = Arc::new(MemoryStream::default());
        let payloads = Arc::new(MemoryPayloads::new()?);
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "batch-owner".into(),
        };
        let issuer = AuthorityIssuer::new("batch-test", [7; 32], owner.clone());
        let owner_scope = issuer.issue("owner", &Capabilities::new([
                "operation:declare",
                "operation:observe",
                "operation:cancel",
                "task:spawn:test.batch@1",
            ]), None, None, &mut None).expect("scope proof");
        let runtime_scope =
            RuntimeScope::new(owner_scope.capabilities().clone(), Limits::default())?;
        let machine = MachineIdentity {
            name: "test.batch".into(),
            version: "1".into(),
            digest: [2; 32],
        };
        let implementation: Arc<dyn ResumableMachine> = Arc::new(BatchMachine {
            identity: machine.clone(),
            schema: serde_json::json!({"type": "integer"}),
        });
        let (task, machine, tasks, machines) = test_registries::<i64>(
            implementation, serde_json::json!({"type": "integer"}), serde_json::json!({"type": "integer"}),
        )?;
        let coordinator = DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?;
        let host = CoordinatorTaskHost::new(
            coordinator,
            stream.clone(),
            payloads.clone(),
            payloads.clone(),
            owner.clone(),
            owner_scope.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            runtime_scope.clone(),
            tasks.clone(),
            machines.clone(),
            Arc::new(SystemUnixMillisClock),
        )?;
        let root_admission = |operation_id, input| TaskAdmissionRecord {
            resource_proofs: Vec::new(),
            operation_id,
            task: task.clone(),
            machine: machine.clone(),
            input,
            input_schema: serde_json::json!({"type": "integer"}),
            output_schema: serde_json::json!({"type": "integer"}),
            parent: None,
            grants: runtime_scope.grants().clone(),
            limits: runtime_scope.limits(),
            run_limits: runtime_scope.run_limits(),
            policy: None,
            extensions: runtime_scope.extensions().cloned(),
            execution: None,
        };
        assert!(matches!(
            host.admit(root_admission(
                OperationId::from_bytes([7; 16]),
                serde_json::json!("not an integer"),
            ))
            .await,
            Err(Error::Invalid(_))
        ));
        let parent_operation = OperationId::from_bytes([8; 16]);
        let parent = TaskId::from_bytes(parent_operation.into_bytes());
        assert!(matches!(host.admit(root_admission(
            parent_operation, serde_json::json!(0),
        )).await?, Admission::Accepted(id) if id == parent));
        let metadata_revision = host.coordinator.lock().await.revision();
        let unrelated_operation = OperationId::from_bytes([201; 16]);
        let mut unrelated = host
            .coordinator
            .lock()
            .await
            .scheduler()
            .operation(parent_operation)
            .ok_or_else(|| Error::NotFound("metadata parent".into()))?
            .spec
            .clone();
        unrelated.operation_id = unrelated_operation;
        let mut remote = DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?;
        remote
            .declare_operation(
                &owner,
                &owner_scope,
                &issuer.verifier(&mut None).expect("verifier copy"),
                unrelated,
                IdempotencyKey::new("unrelated-metadata-observation")?,
                None, &mut None,
            )
            .await?;
        drop(remote);
        assert_eq!(
            host.admission_with_work(parent_operation, &mut None).await?.operation_id,
            parent_operation
        );
        let cached = host.coordinator.lock().await;
        assert_eq!(cached.revision(), metadata_revision);
        assert!(
            cached.scheduler().operation(unrelated_operation).is_none(),
            "immutable metadata reads must not hydrate unrelated lifecycle history"
        );
        drop(cached);
        let request = DurableBatchRequest {
            group_id: GroupId::from_bytes([9; 16]),
            batch_id: BatchId::from_bytes([10; 16]),
            group_policy: crate::runtime::BatchGroupPolicy::CollectAll,
            task: task.clone(),
            machine: machine.clone(),
            inputs: vec![
                serde_json::json!(1),
                serde_json::json!(2),
                serde_json::json!(3),
            ],
            input_schema: serde_json::json!({"type": "integer"}),
            output_schema: serde_json::json!({"type": "integer"}),
            parent: Some(parent),
            scope: runtime_scope.clone(),
            policy: None,
            execution: None,
        };
        let bounded_parent_operation = OperationId::from_bytes([27; 16]);
        let bounded_parent = TaskId::from_bytes(bounded_parent_operation.into_bytes());
        let mut bounded = root_admission(bounded_parent_operation, serde_json::json!(0));
        bounded.grants = Capabilities::new(["task:spawn:test.batch@1"]);
        bounded.limits.model_steps = 2;
        bounded.run_limits = crate::runtime::TaskRunLimits {
            concurrency: Some(2),
            max_steps: Some(2),
            deadline_epoch_ms: None,
        };
        assert!(matches!(
            host.admit(bounded.clone()).await?,
            Admission::Accepted(_)
        ));
        for kind in 0..5 {
            let mut child = bounded.clone();
            child.operation_id = OperationId::new();
            child.parent = Some(bounded_parent);
            match kind {
                0 => child.grants = runtime_scope.grants().clone(),
                1 => child.limits.model_steps = 3,
                2 => child.run_limits.max_steps = Some(3),
                3 => child.run_limits.max_steps = None,
                _ => child.run_limits.concurrency = None,
            }
            let operation_id = child.operation_id;
            assert!(matches!(
                host.admit(child).await,
                Err(Error::Unauthorized(_))
            ));
            assert!(host.reconcile_admission(operation_id).await?.is_none());
        }
        let mut valid_child = bounded.clone();
        valid_child.operation_id = OperationId::new();
        valid_child.parent = Some(bounded_parent);
        valid_child.limits.model_steps = 1;
        valid_child.run_limits.max_steps = Some(1);
        valid_child.run_limits.concurrency = Some(1);
        assert!(matches!(
            host.admit(valid_child).await?,
            Admission::Accepted(_)
        ));
        let mut wide_batch = request.clone();
        wide_batch.batch_id = BatchId::from_bytes([28; 16]);
        wide_batch.parent = Some(bounded_parent);
        assert!(matches!(
            host.admit_batch(wide_batch.clone()).await,
            Err(Error::Unauthorized(_))
        ));
        assert!(host.retained_batch(wide_batch.batch_id).await?.is_none());
        let mut forged = request.clone();
        forged.task.digest = [99; 32];
        assert!(matches!(
            host.admit_batch(forged).await,
            Err(Error::Conflict(_))
        ));
        let mut altered_schema = request.clone();
        altered_schema.output_schema = serde_json::json!({"type": "number"});
        assert!(matches!(
            host.admit_batch(altered_schema).await,
            Err(Error::Conflict(_))
        ));
        host.commit_batch(&request).await?;
        let first = request.operation_id(0);
        assert!(matches!(host.admit(request.member_admission(0)?).await?,
            Admission::Accepted(id) if id.into_bytes() == first.into_bytes()));
        let reopened = CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, payloads.clone(), None, &mut None).await?,
            stream.clone(),
            payloads.clone(),
            payloads.clone(),
            owner,
            owner_scope,
            issuer.verifier(&mut None).expect("verifier copy"),
            runtime_scope.clone(),
            tasks,
            machines,
            Arc::new(SystemUnixMillisClock),
        )?;
        let restored = reopened
            .load_batch(request.batch_id)
            .await?
            .ok_or_else(|| Error::NotFound("retained batch request".into()))?;
        assert_eq!(restored.canonical_value(), request.canonical_value());
        let observed = reopened
            .reconcile_batch(request.clone())
            .await?
            .ok_or_else(|| Error::NotFound("committed batch".into()))?;
        assert!(matches!(
            observed.as_slice(),
            [
                Admission::Accepted(_),
                Admission::Indeterminate { .. },
                Admission::Indeterminate { .. }
            ]
        ));
        let completed = reopened.admit_batch(request.clone()).await?;
        assert_eq!(completed.len(), 3);
        for (index, admission) in completed.iter().enumerate() {
            let Admission::Accepted(task_id) = admission else {
                return Err(Error::Conflict("batch slot was not admitted".into()));
            };
            assert_eq!(
                task_id.into_bytes(),
                request.operation_id(index).into_bytes()
            );
        }
        assert_eq!(reopened.admit_batch(request.clone()).await?, completed);
        let mut root_request = request.clone();
        root_request.batch_id = BatchId::from_bytes([15; 16]);
        root_request.parent = None;
        let root_members = reopened.admit_batch(root_request.clone()).await?;
        assert!(
            root_members
                .iter()
                .all(|entry| matches!(entry, Admission::Accepted(_)))
        );
        assert_eq!(
            reopened
                .load_batch(root_request.batch_id)
                .await?
                .ok_or_else(|| Error::NotFound("root batch request".into()))?
                .canonical_value(),
            root_request.canonical_value()
        );
        let manifest = reopened.batch_stream(request.batch_id)?;
        assert_eq!(manifest.bounds().await?.tail, 1);
        let records = manifest.read(0, 2).await?.try_collect::<Vec<_>>().await?;
        assert_eq!(records.len(), 1);
        assert!(
            !std::str::from_utf8(&records[0].value)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .contains("inputs")
        );
        let mut collision = request.clone();
        collision.batch_id = BatchId::from_bytes([11; 16]);
        let mut claimed_admission = collision.member_admission(0)?;
        claimed_admission.input = serde_json::json!(999);
        assert!(matches!(
            reopened.admit(claimed_admission).await?,
            Admission::Accepted(_)
        ));
        reopened.commit_batch(&collision).await?;
        assert!(matches!(
            reopened.reconcile_batch(collision).await,
            Err(Error::Conflict(_))
        ));
        let mut root_collision = request.clone();
        root_collision.batch_id = BatchId::from_bytes([12; 16]);
        let mut claimed_admission = root_collision.member_admission(0)?;
        claimed_admission.parent = None;
        assert!(matches!(
            reopened.admit(claimed_admission).await?,
            Admission::Accepted(_)
        ));
        reopened.commit_batch(&root_collision).await?;
        assert!(matches!(
            reopened.reconcile_batch(root_collision).await,
            Err(Error::Conflict(_))
        ));
        let other_parent_operation = OperationId::from_bytes([13; 16]);
        let other_parent = TaskId::from_bytes(other_parent_operation.into_bytes());
        assert!(matches!(
            reopened
                .admit(root_admission(other_parent_operation, serde_json::json!(0),))
                .await?,
            Admission::Accepted(_)
        ));
        let mut foreign_parent = request.clone();
        foreign_parent.batch_id = BatchId::from_bytes([14; 16]);
        let mut claimed_admission = foreign_parent.member_admission(0)?;
        claimed_admission.parent = Some(other_parent);
        assert!(matches!(
            reopened.admit(claimed_admission).await?,
            Admission::Accepted(_)
        ));
        reopened.commit_batch(&foreign_parent).await?;
        assert!(matches!(
            reopened.reconcile_batch(foreign_parent).await,
            Err(Error::Conflict(_))
        ));
        let mut cancelled = request.clone();
        cancelled.batch_id = BatchId::from_bytes([16; 16]);
        cancelled.group_policy = crate::runtime::BatchGroupPolicy::CancelOnFailure;
        cancelled.parent = None;
        reopened.commit_batch(&cancelled).await?;
        let first_cancelled = cancelled.member_admission(0)?;
        assert!(matches!(
            reopened.admit(first_cancelled).await?,
            Admission::Accepted(_)
        ));
        let cancellation = reopened
            .cancel_batch(cancelled.batch_id)
            .await?
            .ok_or_else(|| Error::NotFound("retained cancellation".into()))?;
        assert_eq!(cancellation.group_id, cancelled.group_id);
        assert_eq!(cancellation.entries.len(), cancelled.inputs.len());
        assert!(matches!(
            &cancellation.entries[0].1,
            BatchCancellationStatus::Requested
        ));
        assert!(
            cancellation.entries[1..]
                .iter()
                .all(|(_, status)| matches!(status, BatchCancellationStatus::NotAdmitted { .. }))
        );
        assert_eq!(
            reopened
                .retained_batch_cancellation(cancelled.batch_id)
                .await?
                .ok_or_else(|| Error::NotFound("cancellation declaration".into()))?
                .group_id,
            cancelled.group_id
        );
        let after_cancel = reopened.admit_batch(cancelled.clone()).await?;
        assert!(matches!(
            after_cancel.as_slice(),
            [
                Admission::Accepted(_),
                Admission::Rejected { .. },
                Admission::Rejected { .. }
            ]
        ));
        assert_eq!(
            reopened.reconcile_batch(cancelled.clone()).await?,
            Some(after_cancel)
        );
        let repeated = reopened
            .cancel_batch(cancelled.batch_id)
            .await?
            .ok_or_else(|| Error::NotFound("replayed cancellation".into()))?;
        assert_eq!(repeated.entries.len(), cancelled.inputs.len());
        assert!(matches!(
            &repeated.entries[0].1,
            BatchCancellationStatus::Requested
        ));
        assert_eq!(
            reopened
                .batch_cancellation_stream(cancelled.batch_id)?
                .bounds()
                .await?
                .tail,
            1
        );
        let mut changed = request.clone();
        changed.inputs.swap(0, 1);
        assert!(matches!(
            reopened.reconcile_batch(changed).await,
            Err(Error::Conflict(_))
        ));
        // A retained control path must still contain exactly one record.
        // Full Stream history remains readable even when that invariant fails.
        manifest
            .append_at(OwnedBytes::from_static(b"unexpected-second-record"), 1)
            .await?;
        assert!(matches!(
            reopened.retained_batch(request.batch_id).await,
            Err(Error::Storage(_))
        ));
        assert_eq!(
            manifest
                .read(0, 2)
                .await?
                .try_collect::<Vec<_>>()
                .await?
                .len(),
            2
        );
        Ok(())
    }
}
