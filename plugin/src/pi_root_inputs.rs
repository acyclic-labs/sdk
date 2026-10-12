//! Immutable root-preparation inputs in the existing owner volume.

use super::{PiScratchAllocation, PiTaskBinding, pi_owner::PiOwnerConfig,
    pi_root::PiRootIntent, pi_root_allocation::PiRootAllocation};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};
use acyclic_harness::{Error, OperationId, Result, TaskId,
    conversation::FileRef, core::EffectGuarantee, effect_host::TaskEffectPlan,
    filesystem::EFFECT_TASK_COMMAND_KIND,
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

fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}

/// Read-only preparation authenticated against the original task admission.
pub(crate) struct PiRootPreparation {
    pub(crate) command: WorkflowCommand,
    pub(crate) plan: TaskEffectPlan,
    pub(crate) request: PiRootRequest,
    pub(crate) context: std::sync::Arc<acyclic_harness::NativeTaskContext>,
    pub(crate) custody: NativeAllocation,
}

impl PiRootPreparation {

    /// Authenticates the borrowed service state without reminting root metadata.
    pub(crate) async fn load(
        private_root: &acyclic_fs::NativeHostPrivateStateRoot,
        session: &Path, runtime: &super::pi_native_host::PiNativeRuntime,
        state: &super::AdapterState,
        context: Option<std::sync::Arc<acyclic_harness::NativeTaskContext>>,
        work: &NativeMaterializationWork,
    ) -> Result<Self>
    {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED).map_err(native_error)?;
        let super::PluginTaskBinding::Pi(binding) = &state.task_binding else {
            return Err(invalid("Pi root preparation requires a Pi state binding"));
        };
        let owner = PiOwnerConfig::load_readonly_with_native_work(private_root, session, work)?;
        runtime.verify_owner_with_native_work(&owner, work)?;
        owner.validate_root(binding, work)?;
        let admitted = super::pi_recovery::authenticate_pi_task(
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
        let expected = PiRootAllocation::schema(&request.0.binding, work)?;
        if schema.0 != expected.0 {
            return Err(invalid("Pi root allocation result schema differs"));
        }
        drop(expected.0);
        drop(expected.1);
        drop(schema.0);
        drop(schema.1);
        drop(request.0);
        drop(request.1);
        let super::pi_task::PiTaskInput { binding, execution } = admitted.0;
        let super::pi_task::PiTaskExecution::Host { root, preparation } = execution else {
            unreachable!();
        };
        let mut plan_custody = plan.1;
        let mut preparation = Self { command: preparation, plan: plan.0,
            request: PiRootRequest { binding, root }, context, custody: admitted.1 };
        preparation.custody.absorb(&mut plan_custody).map_err(native_error)?;
        Ok(preparation)
    }
}

async fn read_input<T: DeserializeOwned + Serialize>(
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
    let owned = std::mem::size_of::<T>() as u64;
    let decoded = super::pi_root_provider::guarded_pi_decode_with_code::<T>(
        bytes.as_ref(), maximum, owned, acyclic_harness::wire::ErrorCode::Storage,
        if name == "allocation-schema" { JsonValueSchema::Root } else { JsonValueSchema::None },
        if name == "allocation-schema" { 0 } else { super::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES }, work,
    )?;
    let canonical = super::pi_root_provider::guarded_pi_bytes(&decoded.0, maximum, work)?;
    let equal = canonical.0.as_slice() == bytes.as_ref();
    drop(canonical.0);
    drop(canonical.1);
    drop(bytes);
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
    let schema = PiRootAllocation::schema(&request.binding, work)?;
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
    owner.validate_root(&request.binding, work)?;
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
