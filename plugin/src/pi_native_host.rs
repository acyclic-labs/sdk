//! Native Pi commands composed with the original task, effect and filesystem owners.
//! This host retains live capabilities only; routes and durable recovery live in the
//! existing admission, preparation, content-upload and effect journals.
//! Operator selection and lifecycle methods borrow the caller's original work
//! span; nested cleanup never creates another ledger or wraps constructor denial.
//! Pi dispatch requires the SDK's checked native-command lane and frozen task
//! context. Background driver results carry their original query/attempt custody;
//! consumers must drop the attempt, lease and result value before that custody.
//! Live-source and binding queries use that same span and retain original binding
//! DTO custody; a source query never substitutes a new scope or current head.
//! Finite lazy service construction funds resource metadata and the real known
//! host payload; the original protected owner is shared without credential copies.

use crate::{LocalAuthorityBackend, LocalObjectBackend,
    LocalLazyWorkspace, LocalSingleMount, PiScratchAllocation, PiTaskBinding, ServiceResources,
    pi_caller, pi_fork::{self, PiForkCoordinator, PiForkOperations, PreparedPiFork}, pi_native::{self, PiNativeLaunchIntent, PiNativeRunner,
        PiNativeRootSeed, PiNativeDrainedProcess, PiPreparedScratch}, pi_owner::PiOwnerConfig, pi_runtime::PiTaskRuntime,
    pi_task::{NATIVE_LAUNCH_TASK_COMMAND_KIND, PiTaskExecution, PiTaskInput}};
use acyclic_fs::{CancellationToken, Digest, GenerationId, LazySnapshotRef, MeasuredResult, NativeMaterializationWork,
    OperationReceipt, PublicationPermit, WorkBudget, WorkCounters};
use acyclic_fs::native_mount::NativeMountSession;
use acyclic_harness::{Error, JsonValueSchema, OperationId, Outcome, Result, TaskId,
    conversation::VolumeOperation,
    core::{EffectStatus, SchemaRegistry}, distributed::{WorkPull, Worker},
    durable_host::{TaskJournalOwner, task_interaction_id},
    effect_host::{ConversationEffectHost, TaskEffectPlan}, effects::EffectRegistry,
    fork::ForkReport,
    filesystem::{BorrowedNativeVolumeBinding, FilesystemExecutionJournal, FilesystemHost,
        NativeProcessAuthority, NativeProcessReceipt, NativePublicationCleanup, NativePublicationReservation, NativeReservationError, workspace_ref,
        NativeVolumeView, NativeVolumeNamespace, NativeReceiptError,
        NativePreparationJournalObservation, NativePreparationJournalError,
        RecursiveSourceSnapshot, TaskCommandHost, TaskCommandProgress, TaskWorkerAttempt},
    resources::GenerationRef, runtime::{ContentBindings, DurableTaskHost, NativeTaskContext, TaskAdmissionRecord, TaskContext},
    scheduler::{LeaseFence, ResourceSnapshot}, store::{HistoryReadLimits, HistoryReader},
    workflow::WorkflowCommand};
use acyclic_stream::{BoxProviderFuture, LocalStream, StreamClient};
use serde_json::Value;
use std::{collections::BTreeMap, sync::{Arc, RwLock}};

pub(crate) type PiNativeRuntime = PiTaskRuntime<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;
pub(crate) type PiNativeFilesystem = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;
pub(crate) type PiNativeLiveView = NativeVolumeView<LocalAuthorityBackend, LocalObjectBackend>;

pub(crate) struct PiNativeLiveSource {
    pub(crate) view: Arc<PiNativeLiveView>,
    pub(crate) workspace: Arc<LocalLazyWorkspace>,
    pub(crate) owner: Arc<TaskJournalOwner<LocalStream>>,
    pub(crate) context: Arc<NativeTaskContext>,
    pub(crate) binding: PiTaskBinding,
    pub(crate) original_work: NativeMaterializationWork,
    // The original prepared binding's guarded DTO copy outlives every field.
    _binding_custody: NativeAllocation,
}

/// Catalog identity plus an already authenticated selected source capability.
/// Neither paths nor the source's current head are used to reconstruct identity.
pub(crate) struct PiNativeColdLocator {
    pub(crate) parent: TaskId,
    pub(crate) sdk_operation: String,
    pub(crate) root_key: String,
    pub(crate) selected_source: Arc<LocalLazyWorkspace>,
    _custody: Option<NativeAllocation>,
}
type Scratch = PiPreparedScratch<LocalAuthorityBackend, LocalObjectBackend>;
type Journal = FilesystemExecutionJournal<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;
type Effects = ConversationEffectHost<LocalStream>;
type Reservation = NativePublicationReservation<LocalAuthorityBackend, LocalObjectBackend>;
type ReservationCleanup = NativePublicationCleanup<LocalAuthorityBackend, LocalObjectBackend>;

#[derive(Default)]
struct NativeGate {
    reservation: Option<Reservation>,
    cleanup: Option<ReservationCleanup>,
    cleanup_error: Option<NativeReservationError>,
    unwitnessed: Option<acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>>,
}

impl NativeGate {
    async fn reserve(&mut self, owner: &TaskJournalOwner<LocalStream>,
        workspace: acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
        volume: &acyclic_harness::conversation::VolumeRef, generation: GenerationId, command: OperationId,
        meter: &NativeMaterializationWork, cancellation: &CancellationToken,
        cleanup_cancellation: &CancellationToken, existing_only: bool,
    ) -> Result<PublicationPermit> {
        if let Some(reservation) = self.reservation.as_ref() {
            owner.native_view_owner().verify_with_native_work(meter).await?;
            owner.native_view_owner().require_volume_grant_with_native_work(volume, VolumeOperation::Write, meter)?;
            return Ok(reservation.permit());
        }
        if self.cleanup.is_some() { return Err(Error::Indeterminate(command)); }
        let original = acyclic_fs::OperationId::from_bytes(command.into_bytes());
        let acquired = if existing_only || self.unwitnessed.is_some() {
            owner.native_view_owner().recover_publication(workspace, volume, generation,
                original, meter, cancellation, cleanup_cancellation).await
        } else {
            owner.native_view_owner().reserve_publication(workspace, volume, generation,
                original, meter, cancellation, cleanup_cancellation).await
                .map(|receipt| OperationReceipt { value: Some(receipt.value), work: receipt.work })
        };
        match acquired {
            Ok(receipt) => {
                let Some(reservation) = receipt.value else { return Err(Error::Indeterminate(command)); };
                let permit = reservation.permit();
                self.reservation = Some(reservation);
                self.unwitnessed = None;
                remaining(meter)?;
                Ok(permit)
            }
            Err(failure) => {
                self.cleanup = failure.cleanup;
                self.cleanup_error = failure.cleanup_error;
                self.unwitnessed = failure.unwitnessed_workspace;
                Err(reservation_error(failure.error, command))
            }
        }
    }
    async fn release(&mut self, meter: &NativeMaterializationWork, cancellation: &CancellationToken,
        command: OperationId,
    ) -> Result<()> {
        if let Some(reservation) = self.reservation.take() {
            self.cleanup = Some(reservation.into_cleanup());
        }
        if self.unwitnessed.is_some() {
            return Err(Error::Indeterminate(command));
        }
        let Some(cleanup) = self.cleanup.take() else {
            return match self.cleanup_error.take() {
                Some(error) => Err(reservation_error(error, command)),
                None => Ok(()),
            };
        };
        match cleanup.release(meter, cancellation).await {
            Ok(_) => { self.cleanup_error = None; remaining(meter)?; Ok(()) }
            Err(failure) => {
                self.cleanup = failure.cleanup;
                self.cleanup_error = failure.cleanup_error;
                self.unwitnessed = failure.unwitnessed_workspace;
                Err(reservation_error(failure.error, command))
            }
        }
    }
}

struct PreparedNative {
    binding: PiTaskBinding,
    report: ForkReport,
    admission: TaskAdmissionRecord,
    source: RecursiveSourceSnapshot,
    frozen_source: LazySnapshotRef,
    native_lazy: LazySnapshotRef,
    workspace: Arc<LocalLazyWorkspace>,
    _metadata_custody: [Option<NativeAllocation>; 4],
    _container_custody: NativeAllocation,
}

impl TryFrom<PreparedPiFork> for PreparedNative {
    type Error = Error;
    fn try_from(prepared: PreparedPiFork) -> Result<Self> {
        let original = prepared.original_work()?.clone();
        let container = original.claim(std::mem::size_of::<Self>() as u64).map_err(native_error)?;
        let workspace = prepared.workspace.into_arc_with_native_work(&original)
            .map_err(crate::pi_lazy_capture::native_lazy_error)?;
        Ok(Self { binding: prepared.binding, report: prepared.report, admission: prepared.admission,
            source: prepared.source, frozen_source: prepared.frozen_source, native_lazy: prepared.native_lazy,
            workspace, _metadata_custody: prepared._metadata_custody, _container_custody: container })
    }
}

struct BoundEffect {
    runner: Arc<PiNativeRunner<LocalStream>>,
    effects: Arc<Effects>,
    owner: Arc<TaskJournalOwner<LocalStream>>,
    journal: Arc<Journal>,
    context: Arc<NativeTaskContext>,
    command: OperationId,
    staged: Option<(WorkflowCommand, TaskEffectPlan, acyclic_fs::NativeAllocation)>,
}

impl BoundEffect {
    fn context(&self) -> &TaskContext { self.context.context() }
}

#[derive(serde::Serialize)]
struct BorrowedEffectPlan<'a> {
    provider: &'a str,
    guarantee: &'a acyclic_harness::core::EffectGuarantee,
    effect_kind: &'a str,
    request: &'a acyclic_harness::conversation::FileRef,
    result_schema: &'a acyclic_harness::conversation::FileRef,
}

#[derive(Default)]
struct LiveHandles {
    scratch: Option<Scratch>,
    root_seed: Option<PiNativeRootSeed>,
    session: Option<(acyclic_harness::conversation::FileRef, acyclic_fs::NativeAllocation)>,
    preparation_work: Option<NativeMaterializationWork>,
    // Acquisition precedes snapshot validation. Keep the actual mount owner even
    // if that validation fails; a failed bind is not permission to drop a mount.
    project: Option<Arc<LocalSingleMount>>,
    project_allocation: Option<acyclic_fs::NativeAllocation>,
    project_work: Option<NativeMaterializationWork>,
    project_startup_owner: Option<NativeMountSession>,
    effect: Option<Arc<BoundEffect>>,
    scratch_gate: NativeGate,
    project_gate: NativeGate,
    mounts_drained: bool,
    drained: Option<PiNativeDrainedProcess>,
    scratch_released: bool,
    dispose_requested: bool,
    preparation_owner: Option<(Arc<TaskJournalOwner<LocalStream>>, Arc<NativeTaskContext>, OperationId)>,
}

impl LiveHandles {
    fn stop_startup_mount(&mut self, _work: &NativeMaterializationWork,
        command: OperationId,
    ) -> Result<()> {
        if let Some(session) = self.project_startup_owner.as_mut() {
            let volume_work = self.project_work.as_ref().ok_or(Error::Indeterminate(command))?;
            let stopped = session.stop_with_meter(&volume_work.native_mount_work())
                .map_err(|cause| pi_native::native_view_error(
                    acyclic_fs::native_mount::MountLifecycleError::Native(cause)))?;
            if !stopped || !session.cleanup_state().cleanup_complete {
                return Err(Error::Indeterminate(command));
            }
            self.project_startup_owner = None;
        }
        Ok(())
    }
}

#[derive(Default)]
struct NativeEntry {
    prepared: RwLock<Option<Arc<PreparedNative>>>,
    observed: RwLock<Option<Arc<PreparedNative>>>,
    view: RwLock<Option<Arc<PiNativeLiveView>>>,
    cold: RwLock<Option<Arc<PiNativeColdLocator>>>,
    handles: tokio::sync::Mutex<LiveHandles>,
    run: tokio::sync::Mutex<()>,
    operation_cancellation: CancellationToken,
    cleanup_cancellation: CancellationToken,
}

pub(crate) struct PiNativeCommandHost {
    runtime: Arc<PiNativeRuntime>,
    filesystem: Arc<PiNativeFilesystem>,
    stream: StreamClient<LocalStream>,
    resources: ServiceResources,
    protected: Arc<PiOwnerConfig>,
    entries: RwLock<BTreeMap<TaskId, Arc<NativeEntry>>>,
    // Original factory metadata stays funded while this reusable host escapes.
    _factory_custody: Option<NativeAllocation>,
}

impl PiNativeCommandHost {
    pub(crate) fn new(runtime: Arc<PiNativeRuntime>, filesystem: Arc<PiNativeFilesystem>,
        stream: StreamClient<LocalStream>, resources: ServiceResources, protected: Arc<PiOwnerConfig>,
    ) -> Result<Arc<Self>> {
        runtime.verify_owner(&protected)?;
        protected.native_runtime()?.validate()?;
        Ok(Arc::new(Self { runtime, filesystem, stream, resources, protected,
            entries: RwLock::new(BTreeMap::new()), _factory_custody: None }))
    }

    pub(crate) fn new_with_native_work(runtime: Arc<PiNativeRuntime>, filesystem: Arc<PiNativeFilesystem>,
        stream: StreamClient<LocalStream>, resources: &ServiceResources, protected: PiOwnerConfig,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        runtime.verify_owner_with_native_work(&protected, work)?;
        protected.native_runtime_with_native_work(work)?.validate_with_native_work(work)?;
        let protected = protected.into_native_shared(work)?;
        let mut resources = crate::pi_root_provider::service_resources_with_native_work(resources, work)?;
        // Arc allocator/header internals are opaque. This is the real known
        // payload allocation, claimed before constructing the escaping Arc.
        let custody = work.claim(std::mem::size_of::<Self>() as u64).map_err(native_error)?;
        let mut host = (Self { runtime, filesystem, stream, resources: resources.0, protected,
            entries: RwLock::new(BTreeMap::new()), _factory_custody: None }, custody);
        // Keep the physical values before their merged guard on every failure.
        host.1.absorb(&mut resources.1).map_err(native_error)?;
        host.0._factory_custody = Some(host.1);
        Ok(Arc::new(host.0))
    }

    pub(crate) fn original_owner(&self) -> Arc<PiOwnerConfig> {
        Arc::clone(&self.protected)
    }

    /// Called only after the ordinary preparation succeeded and sealed admission
    /// was observed. This does not admit a task, create a workspace or save routes.
    pub(crate) async fn register_prepared(&self, prepared: PreparedPiFork) -> Result<()> {
        self.register_binding(prepared, false).await
    }

    /// Read-only sealed publication observation cannot satisfy current-child
    /// dispatch admission. Native preparation still calls restore_admitted.
    pub(crate) async fn register_observed_source(&self, prepared: PreparedPiFork) -> Result<()> {
        self.register_binding(prepared, true).await
    }

    async fn register_binding(&self, prepared: PreparedPiFork, observed: bool) -> Result<()> {
        prepared.report.validate()?;
        let binding = &prepared.binding;
        binding.validate(None, binding.project_volumes.keys().map(String::as_str), false)?;
        let actual = self.runtime.runtime.task_host().observe_admission(binding.task_id).await?;
        if actual != prepared.admission || prepared.report.request.child.id != binding.task_id.to_string()
            || prepared.report.request.child_agent != binding.agent_id {
            return Err(unauthorized("Pi prepared child differs from its original sealed admission"));
        }
        let input = decode::<PiTaskInput>(actual.input.clone())?;
        validate_child_binding(binding, &input.binding)?;
        if !matches!(input.execution, PiTaskExecution::Child { .. }) || actual.parent.is_none()
            || binding.project_volumes.len() != 1
            || binding.project_volumes.values().next() != Some(&prepared.report.request.preparation.child_project_volume)
            || binding.scratch.volume != prepared.report.child_private_volume
            || binding.results.as_ref().map(|result| &result.volume) != prepared.report.child_result_volume.as_ref()
            || prepared.native_lazy.workspace_id != prepared.workspace.workspace().id()
        {
            return Err(unauthorized("Pi prepared selection differs from the actual original fork report"));
        }
        let PiScratchAllocation::Created(scratch) = &binding.scratch.allocation else {
            return Err(unauthorized("Pi native scratch has no original filesystem creator"));
        };
        let Some(result) = &binding.results else { return Err(unauthorized("Pi child has no unmounted result volume")); };
        let PiScratchAllocation::Created(result) = &result.allocation else {
            return Err(unauthorized("Pi native result has no original filesystem creator"));
        };
        if scratch.generation != prepared.report.child_private_generation
            || Some(&result.generation) != prepared.report.child_result_generation.as_ref()
        {
            return Err(unauthorized("Pi private generations differ from their actual creator report"));
        }
        let entry = self.entry(binding.task_id);
        let destination = if observed { &entry.observed } else { &entry.prepared };
        let mut retained = destination.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(prior) = retained.as_ref() {
            if prior.binding != prepared.binding || prior.admission != prepared.admission
                || prior.report != prepared.report || prior.source != prepared.source
                || prior.frozen_source != prepared.frozen_source || prior.native_lazy != prepared.native_lazy
            {
                return Err(Error::Conflict("Pi live capabilities already belong to another original preparation".into()));
            }
            return Ok(());
        }
        *retained = Some(Arc::new(PreparedNative::try_from(prepared)?));
        Ok(())
    }

    /// Retains only transient capabilities. The service remains the route catalog.
    pub(crate) async fn register_cold_locator_with_native_work(&self, task: TaskId, parent: TaskId,
        sdk_operation: &str, root_key: &str, selected_source: Arc<LocalLazyWorkspace>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let admitted = self.child_admission_with_work(task, work).await?;
        let input = &admitted.1;
        let original = pi_fork::original_operation_with_native_work(
            self.protected.policy().root_task, parent, sdk_operation, work,
        )?;
        let operations = PiForkOperations::new_with_native_work(original, work)?;
        let PiTaskExecution::Child { launch } = &input.execution else {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi cold locator cannot substitute a root" });
        };
        if sdk_operation.is_empty() || launch.operation_id != operations.native
            || task != acyclic_harness::runtime::task_id_for_admission(operations.child)
            || !input.binding.project_volumes.contains_key(root_key)
            || admitted.0.parent != Some(parent)
        {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi cold locator differs from the original native child admission" });
        }
        let mut sdk_operation = work.display(sdk_operation).map_err(native_error)?;
        let mut root_key = work.display(root_key).map_err(native_error)?;
        let custody = work.claim(std::mem::size_of::<PiNativeColdLocator>() as u64).map_err(native_error)?;
        let mut locator = (PiNativeColdLocator { parent, sdk_operation: sdk_operation.0,
            root_key: root_key.0, selected_source, _custody: None }, custody);
        locator.1.absorb(&mut sdk_operation.1).map_err(native_error)?;
        locator.1.absorb(&mut root_key.1).map_err(native_error)?;
        locator.0._custody = Some(locator.1);
        self.retain_cold_locator(task, locator.0)
    }


    fn retain_cold_locator(&self, task: TaskId, locator: PiNativeColdLocator) -> Result<()> {
        let entry = self.entry(task);
        let mut retained = entry.cold.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(prior) = retained.as_ref() {
            if prior.parent != locator.parent || prior.sdk_operation != locator.sdk_operation
                || prior.root_key != locator.root_key
                || prior.selected_source.workspace().id() != locator.selected_source.workspace().id()
            {
                return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
                    reason: "Pi cold locator changed its original selected source" });
            }
            return Ok(());
        }
        *retained = Some(Arc::new(locator));
        Ok(())
    }

    /// Uses the current original child owner; it never reclaims the parent,
    /// creates resources, re-admits the child, or captures a new source head.
    pub(crate) async fn hydrate_child(&self, shared: &Arc<NativeTaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>, fence: &LeaseFence,
        budget: WorkBudget, cancellation: &CancellationToken, work: &NativeMaterializationWork,
    ) -> Result<()> {
        if self.prepared(owner.task_id()).is_some() { return Ok(()); }
        let entry = self.entry(owner.task_id());
        let locator = entry.cold.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
            .ok_or_else(|| unauthorized("Pi cold child has no authenticated original source capability"))?;
        self.bind_invocation_work(owner.task_id(), work).await?;
        let coordinator = PiForkCoordinator::new_with_native_work(self.runtime.clone(), self.filesystem.clone(),
            self.stream.clone(), &self.resources, Arc::clone(&self.protected), work)?;
        let original = pi_fork::original_operation_with_native_work(self.protected.policy().root_task,
            locator.parent, &locator.sdk_operation, work)?;
        let plan = coordinator.load_plan(shared.context(), original, budget, cancellation).await?
            .ok_or_else(|| unauthorized("Pi cold child has no original immutable preparation plan"))?;
        if plan.commit.caller != locator.parent || plan.commit.sdk_operation != locator.sdk_operation
            || plan.request.child.task_id() != owner.task_id()
            || plan.frozen.workspace_id != locator.selected_source.workspace().id()
            || !plan.commit.child.project_volumes.contains_key(&locator.root_key)
        {
            return Err(unauthorized("Pi cold selected source differs from the original sealed preparation"));
        }
        let prepared = coordinator.restore_admitted(shared, owner, fence, &plan,
            locator.selected_source.clone(), budget, cancellation.clone()).await?;
        self.register_prepared(prepared).await
    }

    /// Read-only original SDK evidence. Missing records are physical state
    /// unknown, never a permit to adopt, relaunch or remove a namespace.
    pub(crate) async fn observe_preparation_journal(&self, task: TaskId,
        owner: &TaskJournalOwner<LocalStream>, work: &NativeMaterializationWork,
    ) -> std::result::Result<NativePreparationJournalObservation, NativePreparationJournalError> {
        let admission = self.child_admission_with_work(task, work).await.map_err(NativePreparationJournalError::Owner)?;
        if owner.task_id() != task {
            return Err(NativePreparationJournalError::Record("Pi native journal observer selected another original task"));
        }
        let proof = owner.child_scratch_creation_proof().map_err(NativePreparationJournalError::Owner)?;
        let receipt = acyclic_harness::filesystem::NativeVolumeView::<LocalAuthorityBackend, LocalObjectBackend>
            ::observe_preparation_journal(owner, &proof, admission.0.operation_id, work).await?;
        // SDK query work is already charged; journal.work is historical.
        Ok(receipt.value)
    }

    async fn original_allocation(&self, owner: &TaskJournalOwner<LocalStream>,
        command: OperationId, work: &NativeMaterializationWork,
    ) -> Result<(acyclic_harness::filesystem::NativePreparationRoot, acyclic_fs::NativeAllocation)> {
        let observation = self.observe_preparation_journal(owner.task_id(), owner, work).await
            .map_err(preparation_journal_error)?;
        let NativePreparationJournalObservation::Recorded(recorded) = observation else {
            return Err(Error::Indeterminate(command));
        };
        // The authenticated original record supplies options, never a current
        // path/head. Keep its decoded custody through the real physical query.
        let (allocation, custody) =
            acyclic_harness::filesystem::NativeVolumeView::<LocalAuthorityBackend, LocalObjectBackend>
                ::recover_allocation_with_native_work(owner, &recorded.manifest.options, work)?.await?;
        Ok((allocation.ok_or(Error::Indeterminate(command))?, custody))
    }

    pub(crate) fn live_view(&self, task: TaskId) -> Option<Arc<PiNativeLiveView>> {
        let entry = self.find_entry(task)?;
        let view = entry.view.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        view
    }

    pub(crate) fn binding_with_work(&self, task: TaskId, work: &NativeMaterializationWork)
        -> Result<Option<(PiTaskBinding, NativeAllocation)>> {
        let Some(prepared) = self.source_prepared(task) else { return Ok(None); };
        self.copy_binding_with_work(&prepared, work).map(Some)
    }

    fn copy_binding_with_work(&self, prepared: &PreparedNative, work: &NativeMaterializationWork)
        -> Result<(PiTaskBinding, NativeAllocation)> {
        let maximum = self.protected.policy().maximum_payload_bytes.min(prepared.admission.limits.file_bytes);
        let bytes = crate::pi_root_provider::canonical_pi_value_with_native_work(&prepared.binding, maximum, work)?;
        crate::pi_root_provider::guarded_pi_decode::<PiTaskBinding>(
            &bytes.0, maximum, self.protected.policy().maximum_payload_bytes, JsonValueSchema::None,
            crate::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES, work,
        )
    }

    #[cfg(test)]
    pub(crate) fn binding(&self, task: TaskId) -> Option<PiTaskBinding> {
        self.source_prepared(task).map(|prepared| prepared.binding.clone())
    }

    #[cfg(test)]
    pub(crate) fn source(&self, task: TaskId) -> Option<RecursiveSourceSnapshot> {
        self.source_prepared(task).map(|prepared| prepared.source.clone())
    }

    /// The exact originally prepared child workspace, not a current-head reopen.
    /// Checkpoint its live view under this child's owner before a grandchild fork.
    pub(crate) fn live_workspace(&self, task: TaskId) -> Option<Arc<LocalLazyWorkspace>> {
        self.source_prepared(task).map(|prepared| prepared.workspace.clone())
    }

    /// Original live source owner/context, available while its background run
    /// waits on native tool RPC. This conveys no authority to the requesting task;
    /// the recursive fork engine still independently authorizes a selected source.
    pub(crate) async fn live_source(&self, task: TaskId, work: &NativeMaterializationWork) -> Result<Option<PiNativeLiveSource>> {
        let Some(entry) = self.find_entry(task) else { return Ok(None); };
        let (effect, original_work) = {
            let handles = entry.handles.lock().await;
            let Some(effect) = handles.effect.as_ref() else { return Ok(None); };
            let original_work = handles.preparation_work.as_ref()
                .ok_or(Error::Indeterminate(effect.command))?.clone();
            (Arc::clone(effect), original_work)
        };
        let Some(view) = self.live_view(task) else { return Ok(None); };
        let Some(prepared) = self.prepared(task) else { return Ok(None); };
        effect.owner.verify_with_native_work(false, &original_work).await?;
        if effect.owner.task_id() != task || effect.context().durable_task_id() != Some(task) {
            return Err(unauthorized("Pi live source differs from its original owner/context"));
        }
        let (binding, binding_custody) = self.copy_binding_with_work(&prepared, work)?;
        Ok(Some(PiNativeLiveSource { view, workspace: prepared.workspace.clone(), owner: effect.owner.clone(),
            context: effect.context.clone(), binding, original_work, _binding_custody: binding_custody }))
    }

    /// The parent's immutable writeback baseline, never a recaptured head.
    pub(crate) fn frozen_source(&self, task: TaskId) -> Option<LazySnapshotRef> {
        self.source_prepared(task).map(|prepared| prepared.frozen_source)
    }

    /// The child's original authenticated FIRST native companion, not its final
    /// process publication. Terminal selections use selection_snapshot instead.
    pub(crate) fn source_frozen(&self, task: TaskId) -> Option<LazySnapshotRef> {
        self.source_prepared(task).map(|prepared| prepared.native_lazy)
    }

    pub(crate) fn selected_source_task(&self, task: TaskId) -> Option<TaskId> {
        self.source_prepared(task).map(|prepared|
            prepared.report.request.authorized_source.as_ref()
                .map_or(prepared.source.task, |source| source.source_task))
    }

    /// Approval display metadata only; authority remains the selected FS volume.
    pub(crate) fn with_selected_project_path<T>(&self, task: TaskId,
        volume: &acyclic_harness::conversation::VolumeRef,
        display: impl FnOnce(&std::path::Path) -> T,
    ) -> Option<T> {
        let prepared = self.source_prepared(task)?;
        if !prepared.binding.project_volumes.values().any(|selected| selected == volume) { return None; }
        let view = self.live_view(task)?;
        let mount = self.project_mount(task, volume)?;
        let mapping = view.manifest().volumes.iter().find(|mapping|
            &mapping.volume == volume && matches!(&mapping.namespace, NativeVolumeNamespace::LazyMount { .. }))?;
        if mapping.path != mount.path() { return None; }
        Some(display(&mapping.path))
    }

    /// Persist this actual first-selection snapshot before inspect/diff/import.
    /// Existing requests must replay their retained snapshot, never call again.
    pub(crate) async fn selection_snapshot(&self, task: TaskId,
        meter: &NativeMaterializationWork,
    ) -> Result<(LazySnapshotRef, acyclic_fs::NativeAllocation)> {
        async {
            let prepared = self.prepared(task).ok_or_else(|| unauthorized("Pi selected child has no authenticated creator companion"))?;
            let entry = self.find_entry(task).ok_or_else(|| unauthorized("Pi selected child has no original native handles"))?;
            let effect = entry.handles.lock().await.effect.clone()
                .ok_or_else(|| unauthorized("Pi selected child has not entered its original native operation"))?;
            let outcome = effect.owner.outcome_with_native_work(&meter).await?;
            if outcome.0.is_none() {
                let original_work = entry.handles.lock().await.preparation_work.as_ref()
                    .ok_or(Error::Indeterminate(effect.command))?.clone();
                effect.owner.verify_with_native_work(false, &original_work).await?;
                let view = self.live_view(task).ok_or_else(|| Error::Indeterminate(effect.command))?;
                let snapshots = view.checkpoint_projects_with_native_work(
                    effect.owner.native_view_owner(), &original_work)?.await?;
                if snapshots.0.len() != 1 {
                    return Err(unauthorized("Pi selected child has no unique authenticated project checkpoint"));
                }
                let snapshot = snapshots.0[0].1;
                drop(snapshots.0);
                return Ok((snapshot, snapshots.1));
            }
            let receipt = effect.runner.receipt_with_native_work(&meter).await.map_err(receipt_error)?
                .ok_or_else(|| Error::Indeterminate(effect.command))?;
            if receipt.task != task || receipt.command != effect.command || !receipt.observed {
                return Err(Error::Indeterminate(effect.command));
            }
            effect.runner.confirm_process_cleanup_with_work(&prepared.binding, effect.context(),
                effect.context().scope().limits().file_bytes, meter).await.map_err(receipt_error)?;
            let (snapshot, snapshot_custody) = prepared.workspace.snapshot_with_native_work(meter).await
                .map_err(|cause| pi_native::native_view_error(
                    acyclic_fs::native_mount::MountLifecycleError::Lazy(cause)))?;
            if snapshot.workspace_id != prepared.native_lazy.workspace_id {
                return Err(unauthorized("Pi terminal checkpoint replaced its original created workspace"));
            }
            Ok((snapshot, snapshot_custody))
        }.await
    }

    pub(crate) fn project_mount(&self, task: TaskId,
        volume: &acyclic_harness::conversation::VolumeRef,
    ) -> Option<Arc<LocalSingleMount>> {
        let prepared = self.prepared(task)?;
        if !prepared.binding.project_volumes.values().any(|bound| bound == volume) { return None; }
        let view = self.live_view(task)?;
        let entry = self.find_entry(task)?;
        let mount = entry.handles.try_lock().ok()?.project.clone()?;
        let mapping = view.manifest().volumes.iter().find(|mapping|
            &mapping.volume == volume && matches!(&mapping.namespace, NativeVolumeNamespace::LazyMount { .. }))?;
        if mapping.path != mount.path() { return None; }
        Some(mount)
    }

    pub(crate) fn project_work(&self, task: TaskId,
        volume: &acyclic_harness::conversation::VolumeRef,
    ) -> Option<NativeMaterializationWork> {
        let prepared = self.prepared(task)?;
        if !prepared.binding.project_volumes.values().any(|bound| bound == volume) { return None; }
        let view = self.live_view(task)?;
        let entry = self.find_entry(task)?;
        let handles = entry.handles.try_lock().ok()?;
        let mount = handles.project.as_ref()?;
        let mapping = view.manifest().volumes.iter().find(|mapping|
            &mapping.volume == volume && matches!(&mapping.namespace, NativeVolumeNamespace::LazyMount { .. }))?;
        if mapping.path != mount.path() { return None; }
        handles.project_work.clone()
    }

    pub(crate) fn project_publication(&self, task: TaskId,
        volume: &acyclic_harness::conversation::VolumeRef,
    ) -> Option<PublicationPermit> {
        let prepared = self.prepared(task)?;
        if !prepared.binding.project_volumes.values().any(|bound| bound == volume) { return None; }
        let view = self.live_view(task)?;
        if !view.manifest().volumes.iter().any(|mapping| &mapping.volume == volume
            && matches!(&mapping.namespace, NativeVolumeNamespace::LazyMount { .. })) { return None; }
        let entry = self.find_entry(task)?;
        let permit = entry.handles.try_lock().ok()?.project_gate.reservation.as_ref()?.permit();
        Some(permit)
    }

    /// The same token already held by this caller's actual native FS view.
    /// A prepared-only or stopped source cannot supply a caller invocation token.
    pub(crate) fn invocation_cancellation(&self, task: TaskId) -> Result<Option<CancellationToken>> {
        let Some(view) = self.live_view(task) else { return Ok(None); };
        Ok(Some(<PiNativeLiveView as acyclic_harness::filesystem::NativeProcessView<LocalStream>>
            ::operation_cancellation(view.as_ref()).clone()))
    }

    /// Borrow the original live invocation ledger; never initialize another cap
    /// for an IO event, prepared-only child, terminal task or selected source.
    pub(crate) async fn invocation_work(&self, task: TaskId) -> Result<NativeMaterializationWork> {
        let entry = self.find_entry(task).ok_or_else(|| unauthorized("Pi invocation has no original native handles"))?;
        if self.live_view(task).is_none() {
            return Err(unauthorized("Pi invocation has no original live view"));
        }
        let (bound, work) = {
            let handles = entry.handles.lock().await;
            let bound = handles.effect.clone().ok_or_else(|| unauthorized("Pi invocation has no original bound owner"))?;
            let work = handles.preparation_work.clone()
                .ok_or_else(|| unauthorized("Pi invocation has no original retained work ledger"))?;
            (bound, work)
        };
        if bound.owner.task_id() != task || bound.context().durable_task_id() != Some(task) {
            return Err(unauthorized("Pi invocation differs from its original native owner"));
        }
        work.cancellation().check().map_err(|_| native_error(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        self.child_admission_with_work(task, &work).await?;
        bound.owner.verify_with_native_work(false, &work).await?;
        Ok(work)
    }

    /// Returns only the original runner's warm owner. Terminal queries retain
    /// their real outcome custody; no lease, fence or source scope is recreated.
    pub(crate) async fn invocation_owner(&self, task: TaskId,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<TaskJournalOwner<LocalStream>>> {
        self.child_admission_with_work(task, work).await?;
        let entry = self.find_entry(task).ok_or_else(|| unauthorized("Pi owner has no original native handles"))?;
        let bound = entry.handles.lock().await.effect.clone()
            .ok_or_else(|| unauthorized("Pi owner has no original native runner"))?;
        let owner = bound.runner.original_owner();
        if owner.task_id() != task || bound.context().durable_task_id() != Some(task) {
            return Err(unauthorized("Pi owner differs from its original task and context"));
        }
        let outcome = owner.outcome_with_native_work(work).await?;
        if outcome.0.is_none() { owner.verify_with_native_work(false, work).await?; }
        Ok(Arc::clone(owner))
    }

    pub(crate) async fn request_scratch_disposal(&self, task: TaskId) -> Result<()> {
        self.child_admission(task).await?;
        self.entry(task).handles.lock().await.dispose_requested = true;
        Ok(())
    }

    /// Run this future in a service background job, never the session actor. The
    /// process may call that actor while the stock task driver awaits its result.
    pub(crate) async fn run_child(&self, task: TaskId) -> Result<(Option<TaskWorkerAttempt>, NativeAllocation)> {
        let entry = self.entry(task);
        let _run = entry.run.lock().await;
        let work = self.preparation_work(&entry).await?;
        self.child_admission_with_work(task, &work).await?;
        let observed = self.runtime.runtime.task_host().recover_work_with_native_work(task, &work).await?;
        match observed.0 {
            WorkPull::Claimed(lease) => {
                let attempted = self.runtime.runtime.resume_task_with_native_work(
                    lease, observed.1, self, 8, &work,
                ).await?;
                Ok((Some(attempted.0), attempted.1))
            }
            WorkPull::Unresolved { lease, error } =>
                Ok((Some(TaskWorkerAttempt::Unresolved { lease, error }), observed.1)),
            WorkPull::Idle => {
                drop(observed.1);
                let worker_custody;
                let worker;
                let (id, custody) = crate::guarded_cli_text(
                    &["pi-native:", &self.resources.instance_id], &work,
                )?;
                worker_custody = custody;
                worker = Worker {
                    id, available: ResourceSnapshot::default(), labels: BTreeMap::new(),
                };
                let result = self.runtime.runtime.run_operation_with_native_work(
                    &worker, task, self, 8, &work,
                ).await;
                drop(worker);
                drop(worker_custody);
                result
            }
        }
    }

    /// Resume only a reservation already retained by the original task host.
    /// Unlike run_child, an idle observation cannot acquire a new reservation.
    pub(crate) async fn recover_child(&self, task: TaskId) -> Result<(Option<TaskWorkerAttempt>, NativeAllocation)> {
        let entry = self.entry(task);
        let _run = entry.run.lock().await;
        let work = self.preparation_work(&entry).await?;
        self.child_admission_with_work(task, &work).await?;
        let observed = self.runtime.runtime.task_host().recover_work_with_native_work(task, &work).await?;
        match observed.0 {
            WorkPull::Claimed(lease) => {
                let attempted = self.runtime.runtime.resume_task_with_native_work(
                    lease, observed.1, self, 8, &work,
                ).await?;
                Ok((Some(attempted.0), attempted.1))
            }
            WorkPull::Unresolved { lease, error } =>
                Ok((Some(TaskWorkerAttempt::Unresolved { lease, error }), observed.1)),
            WorkPull::Idle => Ok((None, observed.1)),
        }
    }

    /// Signal every child before awaiting any service job or collector join.
    pub(crate) async fn signal_stop_child(&self, task: TaskId,
        meter: &NativeMaterializationWork,
    ) -> Result<()> {
        let cleanup = meter.cancellation();
        async {
            self.child_admission_with_work(task, meter).await?;
            let entry = self.entry(task);
            let effect = {
                let handles = entry.handles.lock().await;
                if handles.drained.is_some() && handles.mounts_drained { return Ok(()); }
                if handles.effect.is_none() && handles.preparation_owner.is_some() {
                    entry.operation_cancellation.cancel();
                    return Ok(());
                }
                handles.effect.clone()
            };
            if let Some(effect) = effect {
                if self.retained_unstarted_preparation(&entry, &effect, &meter).await? {
                    entry.operation_cancellation.cancel();
                    return Ok(());
                }
                effect.runner.signal_stop(&meter, cleanup).await.map_err(receipt_error)?;
                return Ok(());
            }
            if self.original_never_started(task, &meter).await? { return Ok(()); }
            Err(unauthorized("Pi stop requires its original supervisor or SDK preparation owner"))
        }.await
    }

    /// Background only: join the original SDK supervisor and its own collectors.
    pub(crate) async fn drain_stopped_child(&self, task: TaskId, deactivate: bool,
        meter: &NativeMaterializationWork,
    ) -> Result<()> {
        let cleanup = meter.cancellation();
        async {
            let entry = self.entry(task);
            let (effect, drained, interrupted, dispose) = {
                let handles = entry.handles.lock().await;
                (handles.effect.clone(), handles.drained.is_some() && handles.mounts_drained,
                    handles.preparation_owner.is_some(), deactivate || handles.dispose_requested)
            };
            if !drained {
                if let Some(effect) = effect {
                    if interrupted && self.retained_unstarted_preparation(&entry, &effect, &meter).await? {
                        return self.rollback_preparation(task, meter).await;
                    }
                    let receipt = effect.runner.request_stop(&meter, cleanup).await.map_err(receipt_error)?;
                    if receipt.task != task || receipt.command != effect.command || !receipt.observed {
                        return Err(Error::Indeterminate(effect.command));
                    }
                } else if interrupted {
                    return self.rollback_preparation(task, meter).await;
                } else if self.original_never_started(task, &meter).await? {
                    return Ok(());
                } else {
                    return Err(unauthorized("Pi drainage has no authenticated original attempt"));
                }
            }
            if dispose {
                self.dispose_child_scratch(task, meter).await?;
            } else {
                self.cleanup_child(task, meter).await?;
            }
            let retained = entry.handles.lock().await.effect.clone();
            if !deactivate {
                if let Some(effect) = retained {
                    let outcome = effect.owner.outcome_with_native_work(&meter).await?;
                    if outcome.0.is_none() {
                        let workflow = self.runtime.runtime.workflow_journal(task,
                            effect.context().owned_task_fence()?).await?;
                        // False is a real unsettled outbox: retain the same lease.
                        // Only the SDK's three-tail CAS can relinquish it.
                        effect.journal.release_if_quiescent_with_native_work(&meter, &workflow)
                            .await?;
                    }
                }
            }
            Ok(())
        }.await
    }

    /// Durable subtree intent first. The return value is effect observation,
    /// never an assertion that the process exited or its namespace was disposed.
    pub(crate) async fn cancel_child_with_native_work(&self, task: TaskId,
        work: &NativeMaterializationWork,
    ) -> Result<Option<(EffectStatus, NativeAllocation)>> {
        let _admission = self.child_admission_with_work(task, work).await?;
        self.runtime.runtime.task_host().cancel_with_native_work(task, work).await?;
        let Some(entry) = self.find_entry(task) else { return Ok(None); };
        let effect = entry.handles.lock().await.effect.clone();
        let Some(effect) = effect else { return Ok(None); };
        // Observe only authenticated terminal receipt data. Reconciliation can
        // await the actor's tool RPC, so cancellation never starts it before the
        // lifecycle owner has ordered every supervisor stop.
        let receipt = effect.runner.receipt_with_native_work(work).await.map_err(receipt_error)?;
        match receipt {
            Some(receipt) => {
                let (effect, custody) = receipt.into_effect_with_native_custody().map_err(receipt_error)?;
                Ok(Some((effect.status, custody)))
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn receipt(&self, task: TaskId) -> Result<Option<NativeProcessReceipt>> {
        let Some(entry) = self.find_entry(task) else { return Ok(None); };
        let effect = entry.handles.lock().await.effect.clone();
        match effect { Some(effect) => effect.runner.receipt().await, None => Ok(None) }
    }

    pub(crate) async fn output(&self, task: TaskId) -> Result<Option<acyclic_harness::conversation::FileRef>> {
        let Some(entry) = self.find_entry(task) else { return Ok(None); };
        let effect = entry.handles.lock().await.effect.clone();
        match effect { Some(effect) => effect.runner.output().await, None => Ok(None) }
    }

    /// Authenticates actual process drainage before abandoning its actual lazy
    /// drivers. SDK session scratch remains retained for later selected-source
    /// inspection. This is not the prelaunch materializer rollback operation.
    pub(crate) async fn cleanup_child(&self, task: TaskId,
        meter: &NativeMaterializationWork,
    ) -> Result<()> {
        let cancellation = meter.cancellation();
        async {
            let entry = self.find_entry(task).ok_or_else(|| unauthorized("Pi cleanup has no original live capabilities"))?;
            {
                let handles = entry.handles.lock().await;
                if handles.mounts_drained && handles.drained.is_some()
                    && handles.project_gate.reservation.is_none() && handles.project_gate.cleanup.is_none()
                    && handles.project_gate.unwitnessed.is_none()
                    && handles.scratch_gate.reservation.is_none() && handles.scratch_gate.cleanup.is_none()
                    && handles.scratch_gate.unwitnessed.is_none()
                    && handles.project_startup_owner.is_none()
                    && handles.root_seed.is_none()
                {
                    return Ok(());
                }
            }
            let effect = entry.handles.lock().await.effect.clone()
                .ok_or_else(|| unauthorized("Pi cleanup has no original native attempt"))?;
            let prepared = self.prepared(task).ok_or(Error::Indeterminate(effect.command))?;
            let view = self.live_view(task);
            let drained = effect.runner.confirm_process_cleanup_with_work(&prepared.binding, effect.context(),
                effect.context().scope().limits().file_bytes, meter).await.map_err(receipt_error)?;
            effect.owner.verify_with_native_work(true, &meter).await?;
            entry.handles.lock().await.stop_startup_mount(&meter, effect.command)?;
            if let Some(view) = view {
                view.cleanup_mounts_with_work(&effect.owner, effect.context(), &drained, &meter).await?;
                *entry.view.write().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                let mut handles = entry.handles.lock().await;
                handles.project = None;
                handles.mounts_drained = true;
                handles.drained = Some(drained);
            }
            let mut handles = entry.handles.lock().await;
            if !handles.mounts_drained { return Err(Error::Indeterminate(effect.command)); }
            handles.project_allocation = None;
            effect.owner.verify_with_native_work(true, meter).await?;
            handles.project_gate.release(meter, cancellation, effect.command).await?;
            handles.scratch_gate.release(meter, cancellation, effect.command).await?;
            Ok(())
        }.await
    }

    /// Explicit disposal is separate from normal cleanup: it deletes only the
    /// original child's private session scratch, under the SDK's opaque creator
    /// proof. A cold host lacking the original physical leaf witness cannot adopt
    /// that leaf from its route/path or a current filesystem observation.
    pub(crate) async fn dispose_child_scratch(&self, task: TaskId,
        meter: &NativeMaterializationWork,
    ) -> Result<()> {
        async {
            self.cleanup_child(task, meter).await?;
            let entry = self.find_entry(task).ok_or_else(|| unauthorized("Pi disposal has no original live capabilities"))?;
            let mut handles = entry.handles.lock().await;
            if handles.scratch_released { return Ok(()); }
            let effect = handles.effect.as_ref().cloned()
                .ok_or_else(|| unauthorized("Pi disposal has no original native attempt"))?;
            let scratch = handles.scratch.as_ref().ok_or(Error::Indeterminate(effect.command))?;
            let drained = handles.drained.as_ref().ok_or(Error::Indeterminate(effect.command))?;
            let proof = effect.owner.child_scratch_creation_proof()?;
            scratch.release_scratch_with_work(&effect.owner, effect.context(), effect.command,
                drained, &proof, meter).await?;
            // Only the actual protected deletion's successful return retires
            // this transient witness; failures leave the original leaf held.
            handles.scratch_released = true;
            handles.scratch = None;
            Ok(())
        }.await
    }

    /// Cancel and join this host's original run before rolling back an
    /// interrupted prelaunch preparation. Any dispatched effect/launch receipt
    /// retains uncertainty and forbids materializer rollback.
    pub(crate) async fn rollback_preparation(&self, task: TaskId,
        meter: &NativeMaterializationWork,
    ) -> Result<()> {
        async {
            self.child_admission_with_work(task, meter).await?;
            self.runtime.runtime.task_host().cancel_with_native_work(task, meter).await?;
            let entry = self.find_entry(task).ok_or_else(|| unauthorized("Pi rollback has no original live preparation"))?;
            entry.operation_cancellation.cancel();
            let _run = entry.run.lock().await;
            self.rollback_preparation_owned(task, &entry, meter, meter.cancellation()).await
        }.await
    }

    /// The task driver already serializes this invocation. It must not join its
    /// own background run while settling a cancelled, unstarted command.
    async fn rollback_preparation_owned(&self, task: TaskId, entry: &NativeEntry,
        meter: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<()> {
        let mut handles = entry.handles.lock().await;
        let (owner, context, command) = handles.preparation_owner.as_ref().cloned()
            .ok_or_else(|| unauthorized("Pi cold rollback cannot adopt original native allocation capabilities"))?;
        owner.verify_with_native_work(true, meter).await?;
        let admission = self.child_admission_with_work(task, meter).await?;
        let input = &admission.1;
        let recorded_effect = self.original_effect(context.context(), input, command, meter).await?;
        if recorded_effect.0.as_ref().is_some_and(|effect| effect.status != EffectStatus::Planned)
        { return Err(Error::Indeterminate(command)); }
        if let Some(bound) = &handles.effect
            && bound.runner.receipt_with_native_work(meter).await.map_err(receipt_error)?.is_some()
        { return Err(Error::Indeterminate(command)); }
        let creator = owner.child_scratch_creation_proof()?;
        creator.authorize_settlement_with_native_work(&owner, meter).await?;
        handles.stop_startup_mount(meter, command)?;
        if let Some(project) = handles.project.as_ref() {
            creator.authorize_settlement_with_native_work(&owner, meter).await?;
            let volume_work = handles.project_work.as_ref().ok_or(Error::Indeterminate(command))?;
            project.abandon_with_work(&volume_work.native_mount_work()).map_err(pi_native::native_view_error)?;
            handles.project = None;
        }
        if let Some(seed) = handles.root_seed.as_ref() {
            let _cleanup_receipt = if let Some(allocation) = seed.preparation_root() {
                let recorded = NativeVolumeView::<LocalAuthorityBackend, LocalObjectBackend>
                    ::inspect_preparation_with_native_work(&owner, allocation, meter)?.await?;
                if recorded.is_some() {
                    NativeVolumeView::<LocalAuthorityBackend, LocalObjectBackend>
                        ::cleanup_preparation_with_native_work(&owner, allocation, meter)?.await?
                } else {
                    None
                }
            } else {
                None
            };
            let LiveHandles { root_seed, project_startup_owner, project_work, .. } = &mut *handles;
            let no_mounts: [&LocalSingleMount; 0] = [];
            root_seed.as_mut().ok_or(Error::Indeterminate(command))?
                .cleanup_unprepared_with_work(&owner, &creator, &no_mounts, project_startup_owner,
                    project_work.as_ref(), meter).await?;
            *root_seed = None;
        } else {
            let allocation = self.original_allocation(&owner, command, meter).await?;
            let _cleanup_receipt = NativeVolumeView::<LocalAuthorityBackend, LocalObjectBackend>
                ::cleanup_preparation_with_native_work(&owner, &allocation.0, meter)?.await?;
            let LiveHandles { scratch, project_startup_owner, project_work, .. } = &mut *handles;
            let no_mounts: [&LocalSingleMount; 0] = [];
            scratch.as_mut().ok_or(Error::Indeterminate(command))?
                .cleanup_unstarted_rollback_with_work(&owner, context.context(), &creator, &no_mounts,
                    project_startup_owner, project_work.as_ref(), meter).await?;
        }
        *entry.view.write().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        handles.project_allocation = None;
        handles.scratch = None;
        handles.project_gate.release(meter, cancellation, command).await?;
        handles.scratch_gate.release(meter, cancellation, command).await?;
        Ok(())
    }

    async fn retained_unstarted_preparation(&self, entry: &NativeEntry,
        effect: &BoundEffect, meter: &NativeMaterializationWork,
    ) -> Result<bool> {
        if entry.handles.lock().await.preparation_owner.is_none() { return Ok(false); }
        if effect.runner.receipt_with_native_work(meter).await.map_err(receipt_error)?.is_some() { return Ok(false); }
        let admission = self.child_admission_with_work(effect.owner.task_id(), meter).await?;
        let input = &admission.1;
        let recorded_effect = self.original_effect(effect.context(), input, effect.command, meter).await?;
        Ok(recorded_effect.0.as_ref().is_none_or(|record| record.status == EffectStatus::Planned))
    }

    fn entry(&self, task: TaskId) -> Arc<NativeEntry> {
        self.entries.write().unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(task).or_default().clone()
    }

    fn find_entry(&self, task: TaskId) -> Option<Arc<NativeEntry>> {
        self.entries.read().unwrap_or_else(std::sync::PoisonError::into_inner).get(&task).cloned()
    }

    fn prepared(&self, task: TaskId) -> Option<Arc<PreparedNative>> {
        let entry = self.find_entry(task)?;
        let prepared = entry.prepared.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        prepared
    }

    fn source_prepared(&self, task: TaskId) -> Option<Arc<PreparedNative>> {
        if let Some(prepared) = self.prepared(task) { return Some(prepared); }
        let entry = self.find_entry(task)?;
        let observed = entry.observed.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        observed
    }

    async fn bind_invocation_work(&self, task: TaskId, work: &NativeMaterializationWork) -> Result<()> {
        let entry = self.entry(task);
        let mut handles = entry.handles.lock().await;
        if let Some(original) = handles.preparation_work.as_ref() {
            if !original.shares_ledger(work) {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Conflict,
                    reason: "Pi native command differs from its original invocation ledger",
                });
            }
        } else {
            handles.preparation_work = Some(work.clone());
        }
        Ok(())
    }

    async fn preparation_work(&self, entry: &NativeEntry) -> Result<NativeMaterializationWork> {
        let mut handles = entry.handles.lock().await;
        if handles.preparation_work.is_none() {
            handles.preparation_work = Some(NativeMaterializationWork::new(
                crate::pi_root_provider::allocation_budget(), &entry.operation_cancellation).map_err(native_construction)?);
        }
        handles.preparation_work.as_ref().cloned()
            .ok_or_else(|| unauthorized("Pi native preparation has no original work ledger"))
    }

    /// Settle native resources while this original command still owns its SDK
    /// lease. The outer task driver commits the terminal outcome only afterward.
    async fn settle_progress(&self, task: TaskId, progress: TaskCommandProgress) -> Result<TaskCommandProgress> {
        if matches!(&progress, TaskCommandProgress::Ready(_) | TaskCommandProgress::ReadyWithNativeCustody { .. }) {
            let entry = self.find_entry(task).ok_or_else(|| unauthorized("Pi ready command has no original native handles"))?;
            if entry.handles.lock().await.scratch.is_some() {
                let work = entry.handles.lock().await.preparation_work.as_ref().cloned()
                    .ok_or_else(|| unauthorized("Pi ready cleanup has no original invocation ledger"))?;
                self.dispose_child_scratch(task, &work).await?;
            }
        }
        Ok(progress)
    }

    async fn child_admission(&self, task: TaskId) -> Result<(TaskAdmissionRecord, PiTaskInput)> {
        self.runtime.verify_owner(&self.protected)?;
        let admission = self.runtime.runtime.task_host().observe_admission(task).await?;
        let input = decode::<PiTaskInput>(admission.input.clone())?;
        if task == self.protected.policy().root_task || admission.parent.is_none()
            || admission.task != *self.runtime.definition.identity() || input.binding.task_id != task
            || !matches!(input.execution, PiTaskExecution::Child { .. })
        {
            return Err(unauthorized("Pi native command requires its original registered child admission"));
        }
        input.binding.validate(None, input.binding.project_volumes.keys().map(String::as_str), true)?;
        Ok((admission, input))
    }

    async fn child_admission_with_work(&self, task: TaskId, work: &NativeMaterializationWork)
        -> Result<(TaskAdmissionRecord, PiTaskInput, NativeAllocation)> {
        self.runtime.verify_owner_with_native_work(&self.protected, work)?;
        let mut admission = self.runtime.runtime.task_host()
            .observe_admission_with_native_work(task, work).await?;
        let maximum = self.protected.policy().maximum_payload_bytes.min(admission.0.limits.file_bytes);
        let bytes = crate::pi_root_provider::canonical_pi_value_with_native_work(
            &admission.0.input, maximum, work,
        )?;
        let mut input = crate::pi_root_provider::guarded_pi_decode::<PiTaskInput>(
            &bytes.0, maximum, self.protected.policy().maximum_payload_bytes, JsonValueSchema::None,
            crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES, work,
        )?;
        if task == self.protected.policy().root_task || admission.0.parent.is_none()
            || admission.0.task != *self.runtime.definition.identity() || input.0.binding.task_id != task
            || !matches!(input.0.execution, PiTaskExecution::Child { .. }) {
            return Err(unauthorized("Pi native command requires its original registered child admission"));
        }
        input.0.binding.validate(None, input.0.binding.project_volumes.keys().map(String::as_str), true)?;
        admission.1.absorb(&mut input.1).map_err(native_error)?;
        Ok((admission.0, input.0, admission.1))
    }

    async fn original_never_started(&self, task: TaskId, meter: &NativeMaterializationWork,
    ) -> Result<bool> {
        let admission = self.child_admission_with_work(task, meter).await?;
        let input = &admission.1;
        let PiTaskExecution::Child { launch } = &input.execution else { unreachable!() };
        let entry = self.entry(task);
        {
            let handles = entry.handles.lock().await;
            if handles.effect.is_some() || handles.preparation_owner.is_some()
                || handles.scratch.is_some() || handles.root_seed.is_some() || handles.project.is_some()
                || handles.project_allocation.is_some() || handles.project_startup_owner.is_some()
                || handles.scratch_gate.reservation.is_some() || handles.scratch_gate.cleanup.is_some()
                || handles.scratch_gate.unwitnessed.is_some()
                || handles.project_gate.reservation.is_some() || handles.project_gate.cleanup.is_some()
                || handles.project_gate.unwitnessed.is_some() || self.live_view(task).is_some()
            { return Ok(false); }
        }
        // Observe the original lease only. Idle never authorizes a replacement
        // owner or adoption of a physical allocation.
        let observed = self.runtime.runtime.task_host().recover_work_with_native_work(task, meter).await?;
        let lease = match observed.0 {
            WorkPull::Claimed(lease) => lease,
            WorkPull::Unresolved { error, .. } => return Err(error),
            WorkPull::Idle => return Ok(false),
        };
        let fence = LeaseFence::from(&lease.reservation);
        let owner = self.runtime.runtime.task_host()
            .journal_owner_with_native_work(task, &fence, meter).await?;
        // An empty effect history cannot prove that allocation never happened:
        // only the actual task-owned SDK namespace witness can settle that fact.
        match self.observe_preparation_journal(task, &owner, meter).await.map_err(preparation_journal_error)? {
            NativePreparationJournalObservation::Recorded(_) => Ok(false),
            NativePreparationJournalObservation::MissingJournalPhysicalStateUnknown { .. } =>
                Err(Error::Indeterminate(launch.operation_id)),
        }
    }

    async fn authenticated_command(&self, context: &TaskContext, fence: &LeaseFence,
        command: &WorkflowCommand, payload: Value, recovery: bool, work: &NativeMaterializationWork,
    ) -> Result<((PiTaskInput, NativeAllocation), Arc<TaskJournalOwner<LocalStream>>, (PiNativeLaunchIntent, NativeAllocation))> {
        let task = context.durable_task_id().ok_or_else(|| unauthorized("Pi command has no durable child owner"))?;
        let admitted = self.child_admission_with_work(task, work).await?;
        let admission = &admitted.0;
        let input = &admitted.1;
        let PiTaskExecution::Child { launch } = &input.execution else { unreachable!() };
        if launch != command || command.kind != NATIVE_LAUNCH_TASK_COMMAND_KIND
            || context.scope().grants() != &admission.grants || context.scope().limits() != admission.limits
            || context.scope().run_limits() != admission.run_limits || context.owned_task_fence()? != *fence
        {
            return Err(unauthorized("Pi native command, scope or fence differs from its original admission"));
        }
        let original = PiNativeLaunchIntent::load_with_work(context, &command.payload, work).await?;
        let maximum = self.protected.policy().maximum_payload_bytes.min(context.scope().limits().file_bytes);
        let supplied_bytes = crate::pi_root_provider::canonical_pi_value_with_native_work(&payload, maximum, work)?;
        let supplied = crate::pi_root_provider::guarded_pi_decode::<PiNativeLaunchIntent>(
            &supplied_bytes.0, maximum, self.protected.policy().maximum_payload_bytes,
            JsonValueSchema::None, pi_native::PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES, work,
        )?;
        if original.0 != supplied.0 {
            return Err(unauthorized("Pi resolved native input differs from its immutable original file"));
        }
        let owner = self.runtime.runtime.task_host()
            .journal_owner_with_native_work(task, fence, work).await?.into_native_shared(work)?;
        owner.verify_with_native_work(recovery, work).await?;
        Ok(((admitted.1, admitted.2), owner, original))
    }

    fn result_bindings(&self, context: &TaskContext, input: &PiTaskInput) -> Result<ContentBindings> {
        let result = input.binding.results.as_ref().ok_or_else(|| unauthorized("Pi child has no original result selection"))?;
        context.created_content_bindings(&result.volume)
    }

    async fn journal(&self, owner: &TaskJournalOwner<LocalStream>, context: &TaskContext,
        input: &PiTaskInput, command: OperationId, work: &NativeMaterializationWork,
    ) -> Result<Arc<Journal>> {
        let storage = self.protected.authority_with_native_work(work)?;
        let conversation = self.protected.conversation_authority_with_native_work(owner.task_id(),
            input.binding.agent_id, context.scope().grants(), work)?;
        let interaction_verifier = conversation.0.1.verifier_with_native_work(work)?;
        let result = input.binding.results.as_ref().ok_or_else(|| unauthorized("Pi approval has no original result volume"))?;
        let journal_owner = self.runtime.runtime.task_host()
            .journal_owner_with_native_work(owner.task_id(), &context.owned_task_fence()?, work).await?;
        Journal::for_task_with_native_work(journal_owner, command, self.filesystem.clone(),
            &self.protected.policy().journal, &storage.0.verifier, &storage.0.signed,
            self.protected.policy().maximum_payload_bytes.min(context.scope().limits().file_bytes), work)?
            .with_interaction_owner_with_native_work(&interaction_verifier.0, &SchemaRegistry::new(),
                &conversation.0.2, &result.volume, work)?.into_native_shared(work)
    }

    fn authority(&self, context: &TaskContext, input: &PiTaskInput, journal: Arc<Journal>,
        work: &NativeMaterializationWork,
    ) -> Result<NativeProcessAuthority> {
        let bindings = self.result_bindings(context, input)?;
        let conversation = self.protected.conversation_authority_with_native_work(input.binding.task_id,
            input.binding.agent_id, context.scope().grants(), work)?;
        let verifier = conversation.0.1.verifier_with_native_work(work)?;
        Ok(NativeProcessAuthority { verifier: verifier.0, history_limits: history_limits(context)?,
            // Both stock artifacts and their results use the original unmounted RESULT binding.
            content: bindings.reader.clone(), result_content: bindings.reader,
            results: bindings.writer.ok_or_else(|| unauthorized("Pi sealed result has no original publisher"))?,
            approvals: journal, authority_custody: Some(verifier.1) })
    }

    fn effects(&self, context: &TaskContext, input: &PiTaskInput, runner: &PiNativeRunner<LocalStream>,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<Effects>> {
        let content = self.result_bindings(context, input)?.reader;
        let authorization = self.protected.conversation_authority_with_native_work(input.binding.task_id,
            input.binding.agent_id, context.scope().grants(), work)?;
        let mut registry = EffectRegistry::new_with_native_work(work)?
            .with_result_resolver_with_native_work(content.clone(), work)?;
        registry.register_with_native_work(runner.effect_provider(), work)?;
        let ((audience, issuer, scope), custody) = authorization;
        Effects::new_with_native_work(self.stream.clone(), audience, issuer, scope,
            SchemaRegistry::new(), content, registry, custody, work)?.into_arc_with_native_work(work)
    }

    async fn original_effect(&self, context: &TaskContext, input: &PiTaskInput, command: OperationId,
        work: &NativeMaterializationWork,
    ) -> Result<(Option<acyclic_harness::core::EffectState>, NativeAllocation)> {
        let conversation = self.protected.conversation_authority_with_native_work(input.binding.task_id,
            input.binding.agent_id, context.scope().grants(), work)?;
        let verifier = conversation.0.1.verifier_with_native_work(work)?;
        let history = HistoryReader::new_with_native_work(&self.stream, &conversation.0.0, &verifier.0, work)?;
        let cursor = history.0.pin_with_native_work(0, work).await?;
        let effect = acyclic_harness::effect_host::task_effect_id_with_native_work(
            input.binding.task_id, command, work,
        )?;
        history.0.effect_with_native_work(&cursor.0, effect, history_limits(context)?, work).await
    }

    async fn bind_recovery(&self, shared: Arc<NativeTaskContext>, input: &PiTaskInput,
        owner: Arc<TaskJournalOwner<LocalStream>>, command: OperationId,
        effect: &acyclic_harness::core::EffectState, work: &NativeMaterializationWork,
    ) -> Result<Arc<BoundEffect>> {
        let context = shared.context();
        let borrowed = BorrowedEffectPlan {
            provider: &effect.provider, guarantee: &effect.guarantee, effect_kind: &effect.effect_kind,
            request: &effect.request, result_schema: &effect.result_schema,
        };
        let maximum = self.protected.policy().maximum_payload_bytes.min(context.scope().limits().file_bytes);
        let bytes = crate::pi_root_provider::canonical_pi_value_with_native_work(&borrowed, maximum, work)?;
        let plan = crate::pi_root_provider::guarded_pi_decode::<TaskEffectPlan>(
            &bytes.0, maximum, self.protected.policy().maximum_payload_bytes, JsonValueSchema::None,
            crate::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES, work,
        )?;
        if plan.0.provider != pi_native::PI_NATIVE_PROVIDER {
            return Err(unauthorized("Pi original native command retained another effect provider"));
        }
        let journal = self.journal(&owner, context, input, command, work).await?;
        let authority = self.authority(context, input, journal.clone(), work)?;
        let runner = Arc::new(PiNativeRunner::recover(owner.clone(), command, &plan.0,
            task_interaction_id(input.binding.task_id, command), authority,
            work).await.map_err(receipt_error)?);
        let effects = self.effects(context, input, &runner, work)?;
        let bound = Arc::new(BoundEffect { runner, effects, owner, journal, context: shared, command,
            staged: None });
        self.entry(input.binding.task_id).handles.lock().await.effect = Some(bound.clone());
        Ok(bound)
    }

    /// Reuses the original live provider and SDK gates. The stock provider CAS
    /// below still arbitrates the same attempt if dispatch races this query.
    async fn retained_planned(&self, task: TaskId, command: OperationId,
        effect: &acyclic_harness::core::EffectState,
    ) -> Result<Option<Arc<BoundEffect>>> {
        if effect.status != EffectStatus::Planned { return Ok(None); }
        let Some(entry) = self.find_entry(task) else { return Ok(None); };
        let retained = {
            let handles = entry.handles.lock().await;
            if handles.scratch_gate.reservation.is_none() || handles.project_gate.reservation.is_none()
                || handles.mounts_drained { return Ok(None); }
            handles.effect.clone()
        };
        let Some(bound) = retained else { return Ok(None); };
        let Some((_, plan, _)) = bound.staged.as_ref() else { return Ok(None); };
        if bound.command != command || plan.provider != effect.provider || plan.guarantee != effect.guarantee
            || plan.effect_kind != effect.effect_kind || plan.request != effect.request
            || plan.result_schema != effect.result_schema
        {
            return Err(unauthorized("Pi Planned effect differs from its original retained SDK launch"));
        }
        if bound.runner.receipt().await?.is_some() { return Ok(None); }
        Ok(Some(bound))
    }

    async fn prepare_effect(&self, shared: Arc<NativeTaskContext>, input: &PiTaskInput,
        owner: Arc<TaskJournalOwner<LocalStream>>, command: &WorkflowCommand,
        mut retained_intent: (PiNativeLaunchIntent, NativeAllocation),
        existing_only: bool, work: &NativeMaterializationWork,
    ) -> Result<Arc<BoundEffect>> {
        let intent = &mut retained_intent.0;
        let context = shared.context();
        self.hydrate_child(&shared, &owner, &context.owned_task_fence()?,
            crate::pi_root_provider::allocation_budget(), work.cancellation(), work).await?;
        let prepared = self.prepared(owner.task_id()).ok_or_else(||
            unauthorized("Pi native first dispatch requires actual reopened sealed preparation capabilities"))?;
        validate_child_binding(&prepared.binding, &input.binding)?;
        intent.validate_with_work(work)?;
        if intent.invocation.environment.keys().any(|key| reserved_environment(key)) {
            return Err(unauthorized("Pi retained invocation supplied a replacement caller credential"));
        }
        let root_admission = self.runtime.runtime.task_host()
            .observe_admission_with_native_work(self.protected.policy().root_task, work).await?;
        let limits = pi_native::capture_limits(root_admission.0.limits, self.protected.policy().maximum_payload_bytes)?;
        if intent.invocation.timeout_ms != limits.timeout_ms
            || intent.invocation.control_timeout_ms != limits.control_timeout_ms
            || intent.invocation.cancellation_poll_ms != limits.cancellation_poll_ms
            || intent.invocation.maximum_output_bytes != limits.maximum_output_bytes
            || intent.invocation.maximum_result_bytes != limits.maximum_result_bytes
        {
            return Err(unauthorized("Pi native capture differs from its original protected root bounds"));
        }
        let maximum = self.protected.policy().maximum_payload_bytes.min(root_admission.0.limits.file_bytes);
        let root_bytes = crate::pi_root_provider::canonical_pi_value_with_native_work(
            &root_admission.0.input, maximum, work,
        )?;
        let root = crate::pi_root_provider::guarded_pi_decode::<PiTaskInput>(
            &root_bytes.0, maximum, self.protected.policy().maximum_payload_bytes, JsonValueSchema::None,
            crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES, work,
        )?;
        let root_session = &root.0.binding.native_session_id;
        let caller_environment = pi_caller::issued_environment_with_work(
            &self.protected, &root_session, &prepared.binding, work,
        )?;
        let mut invocation = self.protected.native_runtime_with_native_work(work)?
            .bind_invocation_with_work(intent, &caller_environment.0, work)?;
        retained_intent.1.absorb(&mut invocation.1).map_err(native_error)?;
        intent.invocation = invocation.0;
        drop(caller_environment);
        let entry = self.entry(owner.task_id());
        let mut handles = entry.handles.lock().await;
        if let Some(bound) = handles.effect.as_ref()
            && bound.staged.is_some()
        {
            return Ok(bound.clone());
        }
        if handles.preparation_owner.is_none() {
            handles.preparation_owner = Some((owner.clone(), Arc::clone(&shared), command.operation_id));
        }
        // All consumers of the live view use a separate lock, released before
        // native dispatch below. Neither the actor nor a grandchild fork waits
        // for the child's process future to finish.
        let view = if let Some(view) = self.live_view(owner.task_id()) { view } else {
            if handles.scratch.is_none() {
                let cancellation = work.cancellation();
                let budget = crate::pi_root_provider::allocation_budget();
                let _recovered = if existing_only {
                    Some(self.original_allocation(&owner, command.operation_id, work).await?)
                } else { None };
                let scratch_context = Arc::clone(&shared).with_output_volume_with_native_work(
                    &prepared.binding.scratch.volume, work)?;
                if handles.session.is_none() {
                    handles.session = Some(intent.publish_session_with_work(scratch_context.context(),
                        command.operation_id, work).await?);
                }
                let generation = generation(&handles.session.as_ref()
                    .ok_or_else(|| unauthorized("Pi session has no original publication custody"))?.0)?;
                let mut conversation = self.protected.conversation_authority_with_native_work(owner.task_id(),
                    input.binding.agent_id, context.scope().grants(), work)?;
                let mut verifier = conversation.0.1.verifier_with_native_work(work)?;
                conversation.1.absorb(&mut verifier.1).map_err(native_error)?;
                let (_, _, scope) = conversation.0;
                let authority = (verifier.0, scope, conversation.1);
                let namespace = NativeVolumeNamespace::MaterializedDirectory;
                let scratch_workspace = accept(self.filesystem.open_measured(&workspace_ref(
                    prepared.binding.scratch.volume.provider().clone(), &prepared.binding.scratch.volume.storage_name()?)?,
                    remaining(work)?, cancellation).await, work)?;
                let scratch_generation = GenerationId::new(Digest::from_bytes(
                    generation.as_resource().key().try_into().map_err(|_| unauthorized("Pi scratch generation is not canonical"))?));
                let scratch_permit = handles.scratch_gate.reserve(&owner, scratch_workspace,
                    &prepared.binding.scratch.volume, scratch_generation, command.operation_id,
                    work, cancellation, &entry.cleanup_cancellation, existing_only).await?;
                let options = pi_native::native_view_options_with_work(&self.resources, root_session,
                    owner.task_id(), 2, view_volume_share(budget, 2)?, work)?;
                if handles.root_seed.is_none() {
                    let session = &handles.session.as_ref()
                        .ok_or_else(|| unauthorized("Pi session has no original publication custody"))?.0;
                    handles.root_seed = Some(pi_native::native_root_seed_with_work(&self.resources, root_session,
                        &self.protected, &owner, &shared, &prepared.binding, &options.0, intent, session,
                        &[BorrowedNativeVolumeBinding { host: &self.filesystem, volume: &prepared.binding.scratch.volume,
                            generation: &generation, path: "scratch", writable: true, verifier: &authority.0, scope: &authority.1,
                            publication: scratch_permit, namespace: &namespace }], work).await?);
                }
                {
                    let LiveHandles { root_seed, session, .. } = &mut *handles;
                    let session = &session.as_ref()
                        .ok_or_else(|| unauthorized("Pi session has no original publication custody"))?.0;
                    root_seed.as_mut().ok_or(Error::Indeterminate(command.operation_id))?
                        .create_directories_into_with_work(&owner, &shared, &prepared.binding, &options.0, intent, session,
                            &[BorrowedNativeVolumeBinding { host: &self.filesystem, volume: &prepared.binding.scratch.volume,
                                generation: &generation, path: "scratch", writable: true, verifier: &authority.0, scope: &authority.1,
                                publication: scratch_permit, namespace: &namespace }], work).await?;
                }
                if handles.project_work.is_none() {
                    handles.project_work = Some(work.native_volume_work(options.0.work_per_volume).map_err(native_error)?);
                }
                let (root_key, project) = prepared.binding.project_volumes.iter().next()
                    .ok_or_else(|| unauthorized("Pi actual fork has no selected project"))?;
                let project_generation = GenerationRef::new_with_native_work(project.provider(),
                    prepared.native_lazy.authored_generation.digest().into_bytes(), None, work)?;
                let write_capability = project.capability_with_native_work(VolumeOperation::Write, work)?;
                let writable = context.scope().grants().contains(&write_capability.0);
                let project_permit = handles.project_gate.reserve(&owner, prepared.workspace.workspace().clone(),
                    project, prepared.native_lazy.authored_generation, command.operation_id,
                    work, cancellation, &entry.cleanup_cancellation, existing_only).await?;
                let project_capture = {
                    let LiveHandles { root_seed, project: retained_mount, project_allocation,
                        project_startup_owner, project_work, .. } = &mut *handles;
                    let seed = root_seed.as_ref().ok_or(Error::Indeterminate(command.operation_id))?;
                    seed.mount_project_with_work(&owner, &shared, &prepared.binding, root_key,
                        &prepared.workspace, self.filesystem.clone(), &project_generation.0, &authority,
                        project_permit, writable, retained_mount, project_allocation,
                        project_startup_owner, project_work.as_ref().ok_or(Error::Indeterminate(command.operation_id))?,
                        work, cancellation).await?
                };
                let route = project_capture.0.path.strip_prefix(&options.0.root)
                    .map_err(|_| Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                        reason: "Pi mounted project escaped its original native root" })?
                    .to_str().ok_or(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid,
                        reason: "Pi native project route is not UTF-8" })?;
                let bindings = [
                    BorrowedNativeVolumeBinding { host: &self.filesystem, volume: &prepared.binding.scratch.volume,
                        generation: &generation, path: "scratch", writable: true, verifier: &authority.0, scope: &authority.1,
                        publication: scratch_permit, namespace: &namespace },
                    BorrowedNativeVolumeBinding { host: &self.filesystem, volume: project,
                        generation: &project_generation.0, path: route, writable, verifier: &authority.0, scope: &authority.1,
                        publication: project_permit, namespace: &project_capture.0.namespace },
                ];
                let scratch_guard = shared.volume_callback_guard_with_native_work(&owner, work).await?;
                let guards = [scratch_guard, project_capture.2];
                let existing_lazy = [None, Some(&project_capture.1)];
                handles.scratch = Some(PiNativeRootSeed::into_prepared_mixed_with_work(
                    &mut handles.root_seed, &owner, &options.0, &bindings, &guards, &existing_lazy, work,
                ).await?);
            }
            Arc::clone(&handles.scratch.as_ref().ok_or(Error::Indeterminate(command.operation_id))?.view)
        };
        let original_view_owner = handles.preparation_owner.as_ref()
            .ok_or(Error::Indeterminate(command.operation_id))?.0.clone();
        let working_view = view.task_working_view_with_native_work(&original_view_owner, work)?.await?;
        shared.bind_working_view_with_native_work(working_view, work).await?;
        *entry.view.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&view));
        let result_volume = &prepared.binding.results.as_ref().ok_or_else(|| unauthorized("Pi result selection vanished"))?.volume;
        let result_context = Arc::clone(&shared).with_output_volume_with_native_work(result_volume, work)?;
        let request = intent.request_with_work(&prepared.binding, view.manifest(), work)?;
        let (stock, plan, staged_custody) = pi_native::stage_context_command_with_work(result_context.context(),
            command.operation_id, &request.0, work).await?;
        let journal = self.journal(&owner, context, input, command.operation_id, work).await?;
        let approval = pi_native::approve_command(&self.protected, &owner, context, &prepared.binding,
            &intent.invocation, view.manifest(), command.operation_id, &journal, work).await?;
        let authority = self.authority(context, input, journal.clone(), work)?;
        let runner = Arc::new(PiNativeRunner::bind_with_work(owner.clone(), &prepared.binding, command.operation_id,
            &plan, &intent.invocation, view, approval, authority, work).await?);
        let effects = self.effects(context, input, &runner, work)?;
        let bound = Arc::new(BoundEffect { runner, effects, owner, journal, context: shared,
            command: command.operation_id, staged: Some((stock, plan, staged_custody)) });
        handles.effect = Some(bound.clone());
        Ok(bound)
    }

    async fn execute_stock(&self, context: Arc<NativeTaskContext>, fence: LeaseFence,
        stock: WorkflowCommand, plan: &TaskEffectPlan, bound: &BoundEffect,
        work: &NativeMaterializationWork,
    ) -> Result<TaskCommandProgress> {
        let maximum = context.context().scope().limits().file_bytes.min(self.protected.policy().maximum_payload_bytes);
        let (payload, _payload_custody) = crate::pi_root_provider::guarded_pi_value(plan, maximum, work)?;
        // Keep actual input custody through the SDK-funded stock future's last
        // consumer. The native host never constructs a legacy boxed frame.
        self.runtime.runtime.commands().with_effects(bound.effects.clone())
            .execute_with_native_work(context, fence, stock, payload, work)?.await
    }

    async fn effect_progress(&self, bound: &BoundEffect, mut observed: (EffectStatus, NativeAllocation)) -> Result<TaskCommandProgress> {
        if !matches!(&observed.0, EffectStatus::Succeeded { .. } | EffectStatus::Failed { .. }) {
            return Ok(TaskCommandProgress::Indeterminate);
        }
        let task = bound.owner.task_id();
        let work = self.preparation_work(&self.entry(task)).await?;
        let receipt = bound.runner.receipt_with_native_work(&work).await.map_err(receipt_error)?
            .ok_or(Error::Indeterminate(bound.command))?;
        if !receipt.observed || receipt.task != task || receipt.command != bound.command
            || receipt.effect.status != observed.0
        {
            return Err(Error::Indeterminate(bound.command));
        }
        let (effect, mut original_custody) = receipt.into_effect_with_native_custody().map_err(receipt_error)?;
        let maximum = bound.context().scope().limits().file_bytes.min(self.protected.policy().maximum_payload_bytes);
        let mut completed = match effect.status {
            EffectStatus::Succeeded { result } => crate::pi_root_provider::guarded_pi_value(
                &Outcome::Succeeded(result), maximum, &work)?,
            EffectStatus::Failed { message } => crate::pi_root_provider::guarded_pi_value(
                &Outcome::<Value>::Failed { message }, maximum, &work)?,
            _ => return Err(Error::Indeterminate(bound.command)),
        };
        completed.1.absorb(&mut original_custody).map_err(native_error)?;
        completed.1.absorb(&mut observed.1).map_err(native_error)?;
        drop(observed.0);
        let (value, custody) = completed;
        Ok(TaskCommandProgress::ReadyWithNativeCustody { value, custody })
    }
}

impl TaskCommandHost for PiNativeCommandHost {
    fn execute<'a>(&'a self, context: TaskContext, fence: LeaseFence, command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move {
            if command.kind == NATIVE_LAUNCH_TASK_COMMAND_KIND {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi native launch requires the SDK checked native command lane",
                });
            }
            self.runtime.runtime.commands().execute(context, fence, command, payload).await
        })
    }

    fn reconcile<'a>(&'a self, context: TaskContext, fence: LeaseFence, command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move {
            if command.kind == NATIVE_LAUNCH_TASK_COMMAND_KIND {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi native reconciliation requires the SDK checked native command lane",
                });
            }
            self.runtime.runtime.commands().reconcile(context, fence, command, payload).await
        })
    }

    fn execute_with_native_work<'a>(&'a self, shared: Arc<NativeTaskContext>,
        fence: LeaseFence, command: WorkflowCommand, payload: Value,
        work: &'a NativeMaterializationWork,
    ) -> Result<acyclic_harness::NativeFutureBundle<'a, TaskCommandProgress>> {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            if command.kind != NATIVE_LAUNCH_TASK_COMMAND_KIND {
                return self.runtime.runtime.commands()
                    .execute_with_native_work(shared, fence, command, payload, work)?.await;
            }
            let context = shared.context();
            self.bind_invocation_work(context.durable_task_id()
                .ok_or_else(|| unauthorized("Pi native invocation has no original durable owner"))?, work).await?;
            let (admitted, owner, intent) = self.authenticated_command(context, &fence, &command, payload, false, work).await?;
            let input = &admitted.0;
            let recorded_effect = self.original_effect(context, input, command.operation_id, work).await?;
            if let Some(effect) = recorded_effect.0.as_ref() {
                if let Some(bound) = self.retained_planned(input.binding.task_id, command.operation_id, effect).await? {
                    let (stock, plan, _) = bound.staged.as_ref().ok_or_else(|| unauthorized("Pi retained launch lost its original plan"))?;
                    let progress = self.execute_stock(shared, fence, stock.clone(), plan, &bound, work).await?;
                    return self.settle_progress(input.binding.task_id, progress).await;
                }
                let planned = effect.status == EffectStatus::Planned;
                let recovery = self.bind_recovery(Arc::clone(&shared), &input, owner.clone(),
                    command.operation_id, effect, work).await?;
                if planned && recovery.runner.receipt_with_native_work(work).await.map_err(receipt_error)?.is_none() {
                    let bound = self.prepare_effect(Arc::clone(&shared), &input, owner, &command, intent, true, work).await?;
                    let (stock, plan, _) = bound.staged.as_ref()
                        .ok_or_else(|| unauthorized("Pi restored launch has no original staged plan"))?;
                    if plan.provider != effect.provider || plan.guarantee != effect.guarantee
                        || plan.effect_kind != effect.effect_kind || plan.request != effect.request
                        || plan.result_schema != effect.result_schema
                    {
                        return Err(unauthorized("Pi recovered Planned command changed its original effect plan"));
                    }
                    let progress = self.execute_stock(shared, fence, stock.clone(), plan, &bound, work).await?;
                    return self.settle_progress(input.binding.task_id, progress).await;
                }
                let progress = self.effect_progress(&recovery,
                    recovery.runner.reconcile(&recovery.effects).await?).await?;
                return self.settle_progress(input.binding.task_id, progress).await;
            }
            let bound = self.prepare_effect(Arc::clone(&shared), &input, owner, &command, intent, false, work).await?;
            let (stock, plan, _) = bound.staged.as_ref()
                .ok_or_else(|| unauthorized("Pi launch has no original staged plan"))?;
            let progress = self.execute_stock(shared, fence, stock.clone(), plan, &bound, work).await?;
            self.settle_progress(input.binding.task_id, progress).await
        })
    }

    fn reconcile_with_native_work<'a>(&'a self, shared: Arc<NativeTaskContext>,
        fence: LeaseFence, command: WorkflowCommand, payload: Value,
        work: &'a NativeMaterializationWork,
    ) -> Result<acyclic_harness::NativeFutureBundle<'a, TaskCommandProgress>> {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            if command.kind != NATIVE_LAUNCH_TASK_COMMAND_KIND {
                return self.runtime.runtime.commands()
                    .reconcile_with_native_work(shared, fence, command, payload, work)?.await;
            }
            let context = shared.context();
            self.bind_invocation_work(context.durable_task_id()
                .ok_or_else(|| unauthorized("Pi native recovery has no original durable owner"))?, work).await?;
            let (admitted, owner, _) = self.authenticated_command(context, &fence, &command, payload, true, work).await?;
            let input = &admitted.0;
            let recorded_effect = self.original_effect(context, input, command.operation_id, work).await?;
            let Some(effect) = recorded_effect.0.as_ref() else {
                let entry = self.entry(input.binding.task_id);
                if entry.handles.lock().await.preparation_owner.is_some() {
                    self.rollback_preparation_owned(input.binding.task_id, &entry, work,
                        work.cancellation()).await?;
                } else {
                    // Missing durable records never prove a physical namespace
                    // absent and cannot authorize another launch or cleanup.
                    return Err(Error::Indeterminate(command.operation_id));
                }
                return Ok(TaskCommandProgress::Ready(Value::Null));
            };
            let bound = self.bind_recovery(shared, &input, owner, command.operation_id, effect, work).await?;
            let progress = self.effect_progress(&bound, bound.runner.reconcile(&bound.effects).await?).await?;
            self.settle_progress(input.binding.task_id, progress).await
        })
    }
}

fn validate_child_binding(actual: &PiTaskBinding, admitted: &PiTaskBinding) -> Result<()> {
    if actual.native_session_id != admitted.native_session_id || actual.task_id != admitted.task_id
        || actual.agent_id != admitted.agent_id || actual.project_volumes != admitted.project_volumes
        || actual.scratch.volume != admitted.scratch.volume
        || actual.results.as_ref().map(|result| &result.volume) != admitted.results.as_ref().map(|result| &result.volume)
    {
        return Err(unauthorized("Pi native binding differs from its original admitted volume selection"));
    }
    Ok(())
}

fn generation(file: &acyclic_harness::conversation::FileRef) -> Result<GenerationRef> {
    let mut digest = [0; 32];
    hex::decode_to_slice(file.version(), &mut digest).map_err(|_| Error::Invalid("Pi session lacks its exact immutable generation".into()))?;
    GenerationRef::new(file.volume().provider().clone(), digest, None)
}

fn history_limits(context: &TaskContext) -> Result<HistoryReadLimits> {
    let limits = HistoryReadLimits { maximum_events: context.scope().limits().context_messages,
        maximum_bytes: context.scope().limits().file_bytes.min(crate::MAXIMUM_ADAPTER_STATE_BYTES) };
    if limits.maximum_events == 0 || limits.maximum_bytes == 0 {
        return Err(unauthorized("Pi native effect history has no original finite read allowance"));
    }
    Ok(limits)
}

fn view_volume_share(budget: WorkBudget, volumes: u64) -> Result<WorkBudget> {
    if volumes == 0 || !budget.has_finite_allowances() {
        return Err(Error::Invalid("Pi native namespace requires an original finite nonempty allowance".into()));
    }
    // This is a partition, never a widened cap. Both mapped volumes and layout
    // proofs spend the same original preparation meter. Later mixed publication
    // likewise composes real producer receipts under the original total cap.
    macro_rules! share { ($($field:ident),* $(,)?) => { Ok(WorkBudget {
        $($field: budget.$field / volumes,)*
    }) }; }
    share!(authority_records_read, authority_records_appended, authority_bytes_read, authority_bytes_written,
        object_probes, backend_read_operations, backend_write_operations, durability_operations,
        page_reads, page_writes, object_bytes_read, object_bytes_written, bytes_hashed, bytes_copied,
        bytes_encoded, source_bytes_read, source_path_components, source_entries_visited, output_bytes,
        items_examined, items_returned, allocation_operations, peak_allocation_bytes, materializations)
}

fn accept<T>(result: MeasuredResult<OperationReceipt<T>, Error>, meter: &NativeMaterializationWork) -> Result<T> {
    match result {
        Ok(receipt) => { meter.spent(receipt.work).map_err(native_error)?; remaining(meter)?; Ok(receipt.value) }
        Err(failure) => { meter.spent(*failure.work).map_err(native_error)?; Err(failure.error) }
    }
}

fn remaining(meter: &NativeMaterializationWork) -> Result<WorkBudget> { meter.remaining().map_err(native_error) }
fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}
fn native_construction((cause, work): (acyclic_fs::NativeTreeMaterializationError, WorkCounters)) -> Error {
    Error::NativeConstruction { cause, work }
}
fn unauthorized(message: &str) -> Error { Error::Unauthorized(message.into()) }
fn json_error(error: serde_json::Error) -> Error { Error::Invalid(error.to_string()) }
fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T> { serde_json::from_value(value).map_err(json_error) }
fn reserved_environment(key: &str) -> bool {
    ["ACYCLIC_PI_CALLER", "ACYCLIC_PI_ROUTE", "ACYCLIC_PI_TASK", "ACYCLIC_PI_TOKEN", "ACYCLIC_PI_SCOPE", "ACYCLIC_PI_LEASE"]
        .iter().any(|reserved| key.eq_ignore_ascii_case(reserved))
        || key.get(.."ACYCLIC_PI_CALLER_".len()).is_some_and(|prefix| prefix.eq_ignore_ascii_case("ACYCLIC_PI_CALLER_"))
}

fn reservation_error(error: NativeReservationError, command: OperationId) -> Error {
    match error {
        NativeReservationError::Owner(error) => error,
        NativeReservationError::Native(error) => native_error(error),
        NativeReservationError::Sdk(acyclic_fs::WorkspaceError::Filesystem(acyclic_fs::FsError::Authority(
            acyclic_fs::AuthorityStoreError::ReservationOriginalHeadUnavailable { .. },
        ))) => Error::Indeterminate(command),
        NativeReservationError::Sdk(error) => Error::Conflict(error.to_string()),
        NativeReservationError::Selection(message) => unauthorized(message),
    }
}

#[cfg(test)]
#[path = "pi_native_host_tests.rs"]
mod tests;


fn receipt_error(error: NativeReceiptError) -> Error {
    match error {
        NativeReceiptError::Owner(error) => error,
        NativeReceiptError::Native(error) => native_error(error),
        NativeReceiptError::Backend(source) => Error::NativeStream {
            code: acyclic_harness::wire::ErrorCode::Storage, source, custody: None,
        },
        NativeReceiptError::Record(reason) => Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict, reason,
        },
        NativeReceiptError::Decode { source, custody } => Error::NativeDecode {
            code: acyclic_harness::wire::ErrorCode::Storage, source, custody,
        },
        NativeReceiptError::Codec { code, source, custody } => Error::NativeCodec { code, source, custody },
        NativeReceiptError::Supervisor(source) => Error::NativeSupervisor { source, custody: None },
    }
}

fn preparation_journal_error(error: NativePreparationJournalError) -> Error {
    match error {
        NativePreparationJournalError::Owner(error) => error,
        NativePreparationJournalError::Native(error) => native_error(error),
        NativePreparationJournalError::Backend(source) => Error::NativeStream {
            code: acyclic_harness::wire::ErrorCode::Storage, source, custody: None,
        },
        NativePreparationJournalError::Record(reason) => Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict, reason,
        },
    }
}

#[cfg(test)]
#[path = "pi_native_host_tests.rs"]
mod tests;
