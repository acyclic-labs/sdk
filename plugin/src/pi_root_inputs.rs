//! Immutable root-preparation inputs in the existing owner volume.

use super::{
    PiScratchAllocation, PiTaskBinding, pi_owner::PiOwnerConfig, pi_root::PiRootIntent,
    pi_root_allocation::PiRootAllocation,
};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};
use acyclic_harness::{
    Error, OperationId, Result, TaskId,
    conversation::FileRef,
    core::EffectGuarantee,
    effect_host::TaskEffectPlan,
    filesystem::{EFFECT_TASK_COMMAND_KIND, FilesystemHost},
    workflow::WorkflowCommand,
};
use serde::{Deserialize, Serialize};
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

/// Read-only preparation authenticated against the original task admission.
pub(crate) struct PiRootPreparation {
    pub(crate) command: WorkflowCommand,
    pub(crate) plan: TaskEffectPlan,
    pub(crate) request: PiRootRequest,
    pub(crate) context: std::sync::Arc<acyclic_harness::runtime::NativeTaskContext>,
    pub(crate) custody: NativeAllocation,
}

impl PiRootPreparation {
    /// Authenticates the borrowed service state without reminting root metadata.
    pub(crate) async fn load(
        private_root: &acyclic_fs::NativeHostPrivateStateRoot,
        session: &Path,
        runtime: &super::pi_native_host::PiNativeRuntime,
        filesystem: &FilesystemHost<super::LocalAuthorityBackend, super::LocalObjectBackend>,
        binding: &PiTaskBinding,
        origin: super::pi_recovery::PiTaskOrigin<'_>,
        context: Option<std::sync::Arc<acyclic_harness::runtime::NativeTaskContext>>,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(native_error)?;
        let owner = PiOwnerConfig::load_readonly_with_native_work(private_root, session, work)?;
        runtime.verify_owner_with_native_work(&owner, work)?;
        owner.validate_root(binding, work)?;
        let admitted =
            super::pi_recovery::authenticate_pi_task(&runtime.runtime, binding, origin, work)
                .await?;
        let context = match context {
            Some(context) => context,
            None => {
                let original = runtime
                    .runtime
                    .harness()
                    .durable_context_with_native_work(
                        binding.task_id,
                        OperationId::from_bytes(binding.task_id.into_bytes()),
                        work,
                    )
                    .await?;
                original.0.into_native_shared(original.1, work)?
            }
        };
        if context.context().durable_task_id() != Some(admitted.0.binding.task_id) {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi root preparation context belongs to another original task",
            });
        }
        let super::pi_task::PiTaskExecution::Host { root, preparation } = &admitted.0.execution
        else {
            return Err(invalid(
                "Pi root preparation requires its original host admission",
            ));
        };
        preparation.validate(&mut Some(&mut acyclic_harness::crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)))?;
        if preparation.kind != EFFECT_TASK_COMMAND_KIND
            || preparation.operation_id != preparation_operation(owner.policy().root_task, work)?
        {
            return Err(invalid("Pi root preparation command identity differs"));
        }
        let loaded = super::pi_root_provider::load_original_preparation_with_native_work(
            &owner,
            filesystem,
            &admitted.0.binding,
            root,
            context,
            work,
        )
        .await?;
        validate_request(&owner, &loaded.request, work)?;
        if &loaded.command != preparation {
            return Err(invalid(
                "Pi root preparation command differs from its original admission",
            ));
        }
        Ok(loaded)
    }
}

/// Protected bootstrap only: pins stock schemas and original operation keys.
pub(crate) async fn stage_root_command<A, O>(
    owner: &PiOwnerConfig,
    filesystem: &FilesystemHost<A, O>,
    request: &PiRootRequest,
    work: &NativeMaterializationWork,
) -> Result<(WorkflowCommand, NativeAllocation)>
where
    A: acyclic_fs::AsyncAuthorityStore,
    O: acyclic_fs::AsyncObjectStore,
{
    let operation_id = preparation_operation(owner.policy().root_task, work)?;
    let request_reference = stage_root_request(owner, filesystem, request, work).await?;
    let schema = PiRootAllocation::schema(&request.binding, work)?;
    let result_schema = super::pi_root_provider::stage_pi_root_input_with_native_work(
        owner,
        filesystem,
        "allocation-schema",
        &schema.0,
        work,
    )
    .await?;
    drop(schema.0);
    drop(schema.1);
    let plan_custody = work
        .claim(
            (ROOT_ALLOCATION_PROVIDER.len() + ROOT_ALLOCATION_EFFECT.len()) as u64,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )
        .map_err(native_error)?;
    let plan = TaskEffectPlan {
        provider: ROOT_ALLOCATION_PROVIDER.to_owned(),
        guarantee: EffectGuarantee::AtMostOnce,
        effect_kind: ROOT_ALLOCATION_EFFECT.to_owned(),
        request: request_reference.0,
        result_schema: result_schema.0,
    };
    let mut payload = super::pi_root_provider::stage_pi_root_input_with_native_work(
        owner,
        filesystem,
        "preparation",
        &plan,
        work,
    )
    .await?;
    drop(plan);
    drop(plan_custody);
    drop(request_reference.1);
    drop(result_schema.1);
    let retained = payload
        .1
        .bytes()
        .checked_add(EFFECT_TASK_COMMAND_KIND.len() as u64)
        .ok_or_else(|| {
            native_error(acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::Overflow,
            ))
        })?;
    payload.1.resize_retained(retained).map_err(native_error)?;
    Ok((
        WorkflowCommand {
            operation_id,
            kind: EFFECT_TASK_COMMAND_KIND.to_owned(),
            payload: payload.0,
        },
        payload.1,
    ))
}

const PREPARATION_OPERATION_DOMAIN: &[u8] = b"acyclic.pi/root-preparation/v1";

/// Admits the exact original digest input before running the same engine.
pub(crate) fn preparation_operation(
    task: TaskId,
    work: &NativeMaterializationWork,
) -> Result<OperationId> {
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: PREPARATION_OPERATION_DOMAIN.len() as u64 + 16,
        ..acyclic_fs::WorkCounters::UNCHARGED
    })
    .map_err(native_error)?;
    let mut digest = blake3::Hasher::new();
    digest.update(PREPARATION_OPERATION_DOMAIN);
    digest.update(&task.into_bytes());
    let mut operation = [0; 16];
    operation.copy_from_slice(&digest.finalize().as_bytes()[..16]);
    Ok(OperationId::from_bytes(operation))
}

fn validate_request(
    owner: &PiOwnerConfig,
    request: &PiRootRequest,
    work: &NativeMaterializationWork,
) -> Result<()> {
    owner.validate_root(&request.binding, work)?;
    request.root.validate(&request.binding, work)?;
    if !matches!(
        request.binding.scratch.allocation,
        PiScratchAllocation::Preparing
    ) {
        return Err(invalid(
            "Pi root preparation requires its original unallocated scratch intent",
        ));
    }
    Ok(())
}

fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork {
        cause,
        custody: None,
    }
}

pub(crate) async fn stage_root_request<A, O>(
    owner: &PiOwnerConfig,
    filesystem: &FilesystemHost<A, O>,
    request: &PiRootRequest,
    work: &NativeMaterializationWork,
) -> Result<(FileRef, NativeAllocation)>
where
    A: acyclic_fs::AsyncAuthorityStore,
    O: acyclic_fs::AsyncObjectStore,
{
    validate_request(owner, request, work)?;
    super::pi_root_provider::stage_pi_root_input_with_native_work(
        owner, filesystem, "root", request, work,
    )
    .await
}

fn invalid(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason,
    }
}
