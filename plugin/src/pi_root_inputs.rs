//! Immutable root-preparation inputs in the existing owner volume.
//! Native reads use the original admitted task's content source. Every decoded
//! artifact retains original-ledger custody through its final metadata consumer.
//! Dispatch keeps the supplied SDK native context; cold reads obtain its genuine
//! original metadata custody once and transfer it into the SDK shared context.
//! Service-state preparation borrows its selection and moves the authenticated
//! admission's original binding/root into retained input without reconstructing them.

use super::{PiScratchAllocation, PiTaskBinding, pi_owner::PiOwnerConfig,
    pi_root::PiRootIntent, pi_root_allocation::PiRootAllocation};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};
use acyclic_harness::{Error, OperationId, Result, TaskId,
    conversation::FileRef, core::EffectGuarantee, effect_host::TaskEffectPlan,
    filesystem::{EFFECT_TASK_COMMAND_KIND, FilesystemHost},
    runtime::TaskContext, workflow::WorkflowCommand, contract::JsonValueSchema};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::path::Path;

/// Only this public input directory is readable by the root task.
pub(crate) const ROOT_INPUT_DIRECTORY: &str = "pi-inputs";
pub(crate) const ROOT_ALLOCATION_PROVIDER: &str = "acyclic.pi.root";
pub(crate) const ROOT_ALLOCATION_EFFECT: &str = "acyclic.pi.root.allocate@1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiRootRequest {
    pub(crate) binding: PiTaskBinding,
    pub(crate) root: PiRootIntent,
}

impl PiRootRequest {
    /// Copies retained root metadata directly, not through a codec round trip.
    /// Logical BTree entry slots are funded before insertion; opaque std map
    /// node/allocator backing is not invented as SDK allocation operations.
    pub(crate) fn clone_with_native_work(
        &self, work: &NativeMaterializationWork,
    ) -> Result<(Self, NativeAllocation)> {
        let slot = work.retain_owned(std::mem::size_of::<Self>() as u64).map_err(native_error)?;
        let session = work.display(&self.binding.native_session_id).map_err(native_error)?;
        let mut projects = (std::collections::BTreeMap::new(), work.retain_owned(0).map_err(native_error)?);
        for (key, volume) in &self.binding.project_volumes {
            let slot = work.retain_owned(std::mem::size_of::<(String, acyclic_harness::conversation::VolumeRef)>() as u64)
                .map_err(native_error)?;
            let key = work.display(key).map_err(native_error)?;
            let volume = volume.clone_with_native_work(work)?;
            let mut entry = combine_native_input(((key.0, volume.0), slot, [key.1, volume.1]))?;
            projects.1.absorb(&mut entry.1).map_err(native_error)?;
            projects.0.insert(entry.0.0, entry.0.1);
        }
        let scratch = copy_scratch(&self.binding.scratch, work)?;
        let results = self.binding.results.as_ref().map(|result| copy_scratch(result, work)).transpose()?;
        let root = match &self.root {
            PiRootIntent::ScratchOnly {} =>
                (PiRootIntent::ScratchOnly {}, work.retain_owned(0).map_err(native_error)?),
            PiRootIntent::Project { context_id, root_id, paths } => {
                let mut copied = (std::collections::BTreeMap::new(), work.retain_owned(0).map_err(native_error)?);
                for (key, path) in paths {
                    let slot = work.retain_owned(std::mem::size_of::<(String, String)>() as u64).map_err(native_error)?;
                    let key = work.display(key).map_err(native_error)?;
                    let path = work.display(path).map_err(native_error)?;
                    let mut entry = combine_native_input(((key.0, path.0), slot, [key.1, path.1]))?;
                    copied.1.absorb(&mut entry.1).map_err(native_error)?;
                    copied.0.insert(entry.0.0, entry.0.1);
                }
                (PiRootIntent::Project { context_id: *context_id, root_id: *root_id, paths: copied.0 }, copied.1)
            }
        };
        let (results, results_custody) = match results {
            Some((result, custody)) => (Some(result), Some(custody)),
            None => (None, None),
        };
        let combined = combine_native_input((Self {
            binding: PiTaskBinding {
                native_session_id: session.0, task_id: self.binding.task_id, agent_id: self.binding.agent_id,
                project_volumes: projects.0, scratch: scratch.0, results,
            },
            root: root.0,
        }, slot, [session.1, projects.1, scratch.1, root.1]));
        // Source results custody stays outside the data-first result tuple only
        // until the actual transfer; no new result allocation is inferred.
        let mut owned = (combined?, results_custody);
        if let Some(custody) = &mut owned.1 {
            owned.0.1.absorb(custody).map_err(native_error)?;
        }
        Ok(owned.0)
    }
}

fn copy_scratch(
    source: &super::PiScratchBinding, work: &NativeMaterializationWork,
) -> Result<(super::PiScratchBinding, NativeAllocation)> {
    let volume = source.volume.clone_with_native_work(work)?;
    match &source.allocation {
        PiScratchAllocation::Preparing => Ok((super::PiScratchBinding {
            volume: volume.0, allocation: PiScratchAllocation::Preparing,
        }, volume.1)),
        PiScratchAllocation::Created(creation) => {
            let reference = creation.generation.as_resource();
            let generation = acyclic_harness::resources::GenerationRef::new_with_native_work(
                reference.provider(), reference.key(), reference.version(), work,
            )?;
            combine_native_input((super::PiScratchBinding {
                volume: volume.0,
                allocation: PiScratchAllocation::Created(super::PiScratchCreation {
                    workspace_id: creation.workspace_id, generation: generation.0,
                }),
            }, volume.1, [generation.1]))
        }
    }
}

fn combine_native_input<T, const N: usize>(
    mut owned: (T, NativeAllocation, [NativeAllocation; N]),
) -> Result<(T, NativeAllocation)> {
    for extra in &mut owned.2 { owned.1.absorb(extra).map_err(native_error)?; }
    let (value, custody, _) = owned;
    Ok((value, custody))
}

fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}

/// Read-only preparation authenticated against the original task admission.
/// Data fields precede custody so destruction releases data before its guard.
pub(crate) struct PiRootPreparation {
    command: WorkflowCommand,
    plan: TaskEffectPlan,
    request: PiRootRequest,
    context: std::sync::Arc<acyclic_harness::NativeTaskContext>,
    _custody: [Option<NativeAllocation>; 4],
}

impl PiRootPreparation {
    /// The producer transfers the original guards with its decoded artifacts.
    pub(crate) fn from_original_parts(
        command: WorkflowCommand, plan: TaskEffectPlan, request: PiRootRequest,
        custody: [Option<NativeAllocation>; 4],
        context: std::sync::Arc<acyclic_harness::NativeTaskContext>,
    ) -> Self {
        Self { command, plan, request, context, _custody: custody }
    }

    pub(crate) async fn load_for_dispatch<P, A, O>(
        private_root: &acyclic_fs::NativeHostPrivateStateRoot,
        session: &Path, runtime: &super::pi_runtime::PiTaskRuntime<P, A, O>,
        _filesystem: &FilesystemHost<A, O>, retained: &PiRootRequest,
        fence: &acyclic_harness::scheduler::LeaseFence,
        context: std::sync::Arc<acyclic_harness::NativeTaskContext>,
        work: &NativeMaterializationWork,
    ) -> Result<(Self, acyclic_harness::durable_host::TaskJournalOwner<P>)>
    where P: acyclic_stream::StreamProvider + 'static,
        A: acyclic_fs::AsyncAuthorityStore + 'static,
        O: acyclic_fs::AsyncObjectStore + 'static,
    {
        let owner = PiOwnerConfig::load_readonly_with_native_work(private_root, session, work)?;
        runtime.verify_owner_with_native_work(&owner, work)?;
        owner.validate_root_with_native_work(&retained.binding, work)?;
        let admitted = super::pi_recovery::authenticate_pi_task_with_native_work(
            &runtime.runtime, &retained.binding,
            super::pi_recovery::PiTaskOrigin::Root(&retained.root), work,
        ).await?;
        let preparation = Self::load_original(owner, context, admitted, work).await?;
        let owner = runtime.runtime.task_host()
            .journal_owner_with_native_work(preparation.request.binding.task_id, fence, work).await?;
        owner.verify_with_native_work(false, work).await?;
        Ok((preparation, owner))
    }

    pub(crate) fn command(&self) -> &WorkflowCommand { &self.command }
    pub(crate) fn plan(&self) -> &TaskEffectPlan { &self.plan }
    pub(crate) fn request(&self) -> &PiRootRequest { &self.request }
    pub(crate) fn native_context(&self) -> &std::sync::Arc<acyclic_harness::NativeTaskContext> {
        &self.context
    }

    /// Reopens immutable artifacts without staging or selecting a new lease.
    pub(crate) async fn load<P, A, O>(
        private_root: &acyclic_fs::NativeHostPrivateStateRoot,
        session: &Path, runtime: &super::pi_runtime::PiTaskRuntime<P, A, O>,
        _filesystem: &FilesystemHost<A, O>, retained: &PiRootRequest,
        work: &NativeMaterializationWork,
    ) -> Result<Self>
    where P: acyclic_stream::StreamProvider + 'static,
        A: acyclic_fs::AsyncAuthorityStore + 'static,
        O: acyclic_fs::AsyncObjectStore + 'static,
    {
        let owner = PiOwnerConfig::load_readonly_with_native_work(private_root, session, work)?;
        runtime.verify_owner_with_native_work(&owner, work)?;
        owner.validate_root_with_native_work(&retained.binding, work)?;
        let admitted = super::pi_recovery::authenticate_pi_task_with_native_work(
            &runtime.runtime, &retained.binding,
            super::pi_recovery::PiTaskOrigin::Root(&retained.root), work,
        ).await?;
        let context = runtime.runtime.harness().durable_context_with_native_work(
            owner.policy().root_task,
            OperationId::from_bytes(owner.policy().root_task.into_bytes()), work,
        ).await?;
        let context = context.0.into_native_shared(context.1, work)?;
        Self::load_original(owner, context, admitted, work).await
    }

    /// Authenticates the borrowed service state without reminting root metadata.
    /// A supplied current context is reused unchanged; cold reads obtain only
    /// the genuine original SDK context and its metadata custody.
    pub(crate) async fn load_native_from_state<P, A, O>(
        private_root: &acyclic_fs::NativeHostPrivateStateRoot,
        session: &Path, runtime: &super::pi_runtime::PiTaskRuntime<P, A, O>,
        _filesystem: &FilesystemHost<A, O>, state: &super::AdapterState,
        context: Option<std::sync::Arc<acyclic_harness::NativeTaskContext>>,
        work: &NativeMaterializationWork,
    ) -> Result<Self>
    where P: acyclic_stream::StreamProvider + 'static,
        A: acyclic_fs::AsyncAuthorityStore + 'static,
        O: acyclic_fs::AsyncObjectStore + 'static,
    {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED).map_err(native_error)?;
        let super::PluginTaskBinding::Pi(binding) = &state.task_binding else {
            return Err(invalid("Pi root preparation requires a Pi state binding"));
        };
        let owner = PiOwnerConfig::load_readonly_with_native_work(private_root, session, work)?;
        runtime.verify_owner_with_native_work(&owner, work)?;
        owner.validate_root_with_native_work(binding, work)?;
        let admitted = super::pi_recovery::authenticate_pi_task_with_native_work(
            &runtime.runtime, binding, super::pi_recovery::PiTaskOrigin::RootState(state), work,
        ).await?;
        let context = match context {
            Some(context) => context,
            None => {
                let original = runtime.runtime.harness().durable_context_with_native_work(
                    binding.task_id, OperationId::from_bytes(binding.task_id.into_bytes()), work,
                ).await?;
                original.0.into_native_shared(original.1, work)?
            }
        };
        Self::load_original(owner, context, admitted, work).await
    }

    /// Uses the actual native command context and original execution fence.
    pub(crate) async fn load_native_from_state_for_dispatch<P, A, O>(
        private_root: &acyclic_fs::NativeHostPrivateStateRoot,
        session: &Path, runtime: &super::pi_runtime::PiTaskRuntime<P, A, O>,
        filesystem: &FilesystemHost<A, O>, state: &super::AdapterState,
        fence: &acyclic_harness::scheduler::LeaseFence,
        context: std::sync::Arc<acyclic_harness::NativeTaskContext>,
        work: &NativeMaterializationWork,
    ) -> Result<(Self, acyclic_harness::durable_host::TaskJournalOwner<P>)>
    where P: acyclic_stream::StreamProvider + 'static,
        A: acyclic_fs::AsyncAuthorityStore + 'static,
        O: acyclic_fs::AsyncObjectStore + 'static,
    {
        let preparation = Self::load_native_from_state(
            private_root, session, runtime, filesystem, state, Some(context), work,
        ).await?;
        let owner = runtime.runtime.task_host()
            .journal_owner_with_native_work(preparation.request.binding.task_id, fence, work).await?;
        owner.verify_with_native_work(false, work).await?;
        Ok((preparation, owner))
    }

    async fn load_original(
        owner: PiOwnerConfig,
        context: std::sync::Arc<acyclic_harness::NativeTaskContext>,
        admitted: (super::pi_task::PiTaskInput, NativeAllocation),
        work: &NativeMaterializationWork,
    ) -> Result<Self>
    {
        if context.context().durable_task_id() != Some(admitted.0.binding.task_id) {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi root preparation context belongs to another original task" });
        }
        let super::pi_task::PiTaskExecution::Host { root, preparation } = &admitted.0.execution else {
            return Err(invalid("Pi root preparation requires its original host admission"));
        };
        preparation.validate()?;
        if preparation.kind != EFFECT_TASK_COMMAND_KIND
            || preparation.operation_id != preparation_operation_with_native_work(owner.policy().root_task, work)? {
            return Err(invalid("Pi root preparation command identity differs"));
        }
        let maximum = owner.policy().maximum_payload_bytes.min(super::MAXIMUM_ADAPTER_STATE_BYTES);
        let plan = read_input::<TaskEffectPlan>(context.context(), &preparation.payload,
            "preparation", "application/json", maximum, work).await?;
        if plan.0.provider != ROOT_ALLOCATION_PROVIDER
            || plan.0.effect_kind != ROOT_ALLOCATION_EFFECT
            || plan.0.guarantee != EffectGuarantee::AtMostOnce {
            return Err(invalid("Pi root allocation provider contract differs"));
        }
        let request = read_input::<PiRootRequest>(context.context(), &plan.0.request,
            "root", "application/json", maximum, work).await?;
        validate_request(&owner, &request.0, work)?;
        if request.0.binding != admitted.0.binding || &request.0.root != root {
            return Err(invalid("Pi root preparation differs from its original admission"));
        }
        let schema = read_input::<serde_json::Value>(context.context(), &plan.0.result_schema,
            "allocation-schema", "application/schema+json", maximum, work).await?;
        let expected = PiRootAllocation::schema_with_native_work(&request.0.binding, work)?;
        if schema.0 != expected.0 {
            return Err(invalid("Pi root allocation result schema differs"));
        }
        drop(expected.0);
        drop(expected.1);
        drop(schema.0);
        drop(schema.1);
        drop(request.0);
        drop(request.1);
        // The original admitted command, binding and root move together. The
        // discarded comparison DTO drops before releasing its decoder custody.
        let super::pi_task::PiTaskInput { binding, execution } = admitted.0;
        let super::pi_task::PiTaskExecution::Host { root, preparation } = execution else {
            unreachable!();
        };
        Ok(Self::from_original_parts(preparation, plan.0, PiRootRequest { binding, root },
            [Some(admitted.1), Some(plan.1), None, None], context))
    }
}

trait RootInputArtifact: DeserializeOwned + Serialize {
    const VALUE_SCHEMA: JsonValueSchema;
    const DIAGNOSTIC_LABEL_BYTES: u64;
}
impl RootInputArtifact for PiRootRequest {
    const VALUE_SCHEMA: JsonValueSchema = JsonValueSchema::None;
    const DIAGNOSTIC_LABEL_BYTES: u64 = super::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES;
}
impl RootInputArtifact for TaskEffectPlan {
    const VALUE_SCHEMA: JsonValueSchema = JsonValueSchema::None;
    const DIAGNOSTIC_LABEL_BYTES: u64 = super::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES;
}
impl RootInputArtifact for serde_json::Value {
    const VALUE_SCHEMA: JsonValueSchema = JsonValueSchema::Root;
    const DIAGNOSTIC_LABEL_BYTES: u64 = 0;
}

async fn read_input<T: RootInputArtifact>(
    context: &TaskContext, reference: &FileRef, name: &str, media_type: &str,
    maximum: u64, work: &NativeMaterializationWork,
) -> Result<(T, NativeAllocation)> {
    if reference.path().strip_prefix(ROOT_INPUT_DIRECTORY)
        != Some(match name { "root" => "/root.json", "preparation" => "/preparation.json",
            "allocation-schema" => "/allocation-schema.json", _ => return Err(invalid("unknown Pi root artifact")) })
        || reference.descriptor().media_type() != media_type
        || reference.descriptor().byte_length() > maximum {
        return Err(invalid("Pi root preparation artifact path or media type differs"));
    }
    let bytes = context.read_file_with_native_work(reference, work).await?;
    // Fixed closed DTO headers are distinct from the SDK source shape's
    // admitted dynamic Value/String/collection overlap.
    let owned = std::mem::size_of::<T>() as u64;
    let decoded = super::pi_root_provider::guarded_pi_decode_with_code::<T>(
        &bytes.0, maximum, owned, acyclic_harness::wire::ErrorCode::Storage,
        T::VALUE_SCHEMA, T::DIAGNOSTIC_LABEL_BYTES, work,
    )?;
    let canonical = super::pi_root_provider::guarded_pi_bytes(&decoded.0, maximum, work)?;
    let equal = canonical.0 == bytes.0;
    drop(canonical.0);
    drop(canonical.1);
    drop(bytes.0);
    drop(bytes.1);
    if !equal {
        drop(decoded.0);
        drop(decoded.1);
        return Err(invalid("Pi preparation artifact is not in its first-party encoding"));
    }
    Ok(decoded)
}

/// Protected bootstrap only: pins stock schemas and original operation keys.
pub(crate) async fn stage_root_command<A, O>(
    owner: &PiOwnerConfig, filesystem: &FilesystemHost<A, O>, request: &PiRootRequest,
    work: &NativeMaterializationWork,
) -> Result<(WorkflowCommand, NativeAllocation)>
where A: acyclic_fs::AsyncAuthorityStore, O: acyclic_fs::AsyncObjectStore,
{
    let operation_id = preparation_operation_with_native_work(owner.policy().root_task, work)?;
    let request_reference = stage_root_request(owner, filesystem, request, work).await?;
    let schema = PiRootAllocation::schema_with_native_work(&request.binding, work)?;
    let result_schema = super::pi_root_provider::stage_pi_root_input_with_native_work(
        owner, filesystem, "allocation-schema", &schema.0, "application/schema+json", work,
    ).await?;
    drop(schema.0);
    drop(schema.1);
    let plan_custody = work.retain_owned(
        (ROOT_ALLOCATION_PROVIDER.len() + ROOT_ALLOCATION_EFFECT.len()) as u64,
    ).map_err(native_error)?;
    let plan = TaskEffectPlan {
        provider: ROOT_ALLOCATION_PROVIDER.to_owned(), guarantee: EffectGuarantee::AtMostOnce,
        effect_kind: ROOT_ALLOCATION_EFFECT.to_owned(), request: request_reference.0,
        result_schema: result_schema.0,
    };
    let mut payload = super::pi_root_provider::stage_pi_root_input_with_native_work(
        owner, filesystem, "preparation", &plan, "application/json", work,
    ).await?;
    drop(plan);
    drop(plan_custody);
    drop(request_reference.1);
    drop(result_schema.1);
    let retained = payload.1.bytes().checked_add(EFFECT_TASK_COMMAND_KIND.len() as u64)
        .ok_or_else(|| native_error(acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::Overflow)))?;
    payload.1.resize_retained(retained).map_err(native_error)?;
    Ok((WorkflowCommand { operation_id,
        kind: EFFECT_TASK_COMMAND_KIND.to_owned(), payload: payload.0 }, payload.1))
}

const PREPARATION_OPERATION_DOMAIN: &[u8] = b"acyclic.pi/root-preparation/v1";

/// Admits the exact original digest input before running the same engine.
pub(crate) fn preparation_operation_with_native_work(
    task: TaskId, work: &NativeMaterializationWork,
) -> Result<OperationId> {
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: PREPARATION_OPERATION_DOMAIN.len() as u64 + 16,
        ..acyclic_fs::WorkCounters::UNCHARGED
    }).map_err(native_error)?;
    Ok(preparation_operation(task))
}

pub(crate) fn preparation_operation(task: TaskId) -> OperationId {
    let mut digest = blake3::Hasher::new();
    digest.update(PREPARATION_OPERATION_DOMAIN);
    digest.update(&task.into_bytes());
    let mut operation = [0; 16];
    operation.copy_from_slice(&digest.finalize().as_bytes()[..16]);
    OperationId::from_bytes(operation)
}

fn validate_request(owner: &PiOwnerConfig, request: &PiRootRequest, work: &NativeMaterializationWork) -> Result<()> {
    owner.validate_root_with_native_work(&request.binding, work)?;
    request.root.validate_with_native_work(&request.binding, work)?;
    if !matches!(request.binding.scratch.allocation, PiScratchAllocation::Preparing) {
        return Err(invalid("Pi root preparation requires its original unallocated scratch intent"));
    }
    Ok(())
}

fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}

pub(crate) async fn stage_root_request<A, O>(
    owner: &PiOwnerConfig, filesystem: &FilesystemHost<A, O>, request: &PiRootRequest,
    work: &NativeMaterializationWork,
) -> Result<(FileRef, NativeAllocation)>
where A: acyclic_fs::AsyncAuthorityStore, O: acyclic_fs::AsyncObjectStore,
{
    validate_request(owner, request, work)?;
    super::pi_root_provider::stage_pi_root_input_with_native_work(
        owner, filesystem, "root", request, "application/json", work,
    ).await
}

fn invalid(reason: &'static str) -> Error {
    Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason }
}
