//! Relationships between the original invocation and SDK-validated objects.
use super::*;
use crate::filesystem::PreparationIo;
use crate::wire::ErrorCode::{self, Invalid, Unauthorized};

pub(super) fn reject_if(
    rejected: bool,
    io: Option<&PreparationIo>,
    code: ErrorCode,
    reason: &'static str,
) -> Result<()> {
    if rejected {
        Err(recursive_reject(io, code, reason))
    } else {
        Ok(())
    }
}
pub(super) fn deny(rejected: bool, io: Option<&PreparationIo>) -> Result<()> {
    reject_if(
        rejected,
        io,
        Unauthorized,
        "recursive operation changed its admitted authority",
    )
}
pub(super) fn require_plan_arguments(
    plan: &Plan,
    arguments: &Value,
    io: Option<&PreparationIo>,
) -> Result<()> {
    deny(
        !match plan {
            Plan::Fork(plan) => recursive_encoded_equal(&plan.arguments, arguments, io)?,
            Plan::Merge(plan) => recursive_encoded_equal(&plan.arguments, arguments, io)?,
        },
        io,
    )
}
pub(super) fn validate_plan(plan: &Plan, io: Option<&PreparationIo>) -> Result<()> {
    match plan {
        Plan::Fork(plan) => {
            deny(plan.request.source_checkpoint.is_none(), io)?;
            recursive_decoded::<ForkRequest, _>(&plan.request.payload, io, |fork| {
                deny(
                    fork.child_agent != plan.arguments.id
                        || plan.preparation.parent != Some(plan.caller)
                        || plan.request.child.parent != Some(plan.caller)
                        || plan.preparation.operation_id == plan.request.child.operation_id
                        || plan.preparation.operation_id == fork.operation_id
                        || plan.request.child.operation_id == fork.operation_id
                        || plan.preparation.task_id() == plan.caller
                        || plan.request.child.task_id() == plan.caller
                        || !recursive_encoded_equal(&plan.request, &plan.preparation.input, io)?
                        || plan.preparation.machine.name != "acyclic.fork_preparation"
                        || plan.preparation.machine.version != "1"
                        || plan.request.command.as_ref().is_none_or(|command| {
                            command.kind != FORK_PREPARE_TASK_COMMAND_KIND
                                || command.operation_id != fork.operation_id
                        }),
                    io,
                )
            })
        }
        Plan::Merge(plan) => {
            deny(plan.source_project == plan.target_project, io)?;
            if let Some(source) = &plan.authorized_source {
                deny(
                    source.operation_id != plan.operation_id
                        || source.caller_task != plan.caller
                        || source.source_task != plan.arguments.source
                        || source.project != plan.source_project
                        || source.generation != plan.source_generation,
                    io,
                )?;
            }
            Ok(())
        }
    }
}
pub(super) fn validate_merge_receipt(
    plan: &RecursiveMergeToolPlan,
    receipt: &ProjectMergeReceipt,
    io: Option<&PreparationIo>,
) -> Result<()> {
    deny(
        receipt.operation_id != plan.operation_id
            || receipt.child != plan.source
            || receipt.source_project != plan.source_project
            || receipt.source_generation != plan.source_generation
            || receipt.target_project != plan.target_project
            || receipt.expected_target_generation != plan.expected_target_generation
            || receipt.notice != plan.notice,
        io,
    )
}
fn volume_granted(
    grants: &Capabilities,
    volume: &VolumeRef,
    operation: VolumeOperation,
    io: Option<&PreparationIo>,
) -> Result<bool> {
    match io {
        Some(io) => io.contains_capability(grants, volume, operation.capability_prefix()),
        None => Ok(grants.contains(&volume.capability(operation, &mut None)?)),
    }
}
pub(super) async fn validate_fork_binding<
    P: StreamProvider,
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    context: &ToolContext,
    arguments: &ForkToolArguments,
    binding: &RecursiveForkToolBinding<P, A, O>,
    cancellation: &CancellationToken,
    io: Option<&PreparationIo>,
) -> Result<()> {
    for selection in &binding.source.request.selections {
        recursive_inspect(io, 1)?;
        if let crate::fork::ResourceRevision::PrivateVolume { volume, paths, .. } =
            &selection.revision
        {
            let caller = binding
                .parent_scope
                .agent()
                .ok_or_else(|| recursive_reject(io, Unauthorized, "caller has no agent"))?;
            deny(
                !paths.is_empty()
                    || volume.owner() != &crate::conversation::VolumeOwner::Agent(caller)
                    || caller == arguments.id,
                io,
            )?;
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                deny(
                    volume_granted(&binding.child.grants, volume, operation, io)?
                        || volume_granted(
                            binding.child_scope.capabilities(),
                            volume,
                            operation,
                            io,
                        )?,
                    io,
                )?;
            }
        }
    }
    let selected = binding
        .child
        .input
        .pointer(&binding.child_message_pointer)
        .ok_or_else(|| recursive_reject(io, Invalid, "child message pointer is missing"))?;
    if selected.as_str() == Some(arguments.msg.as_str()) {
        return reject_if(
            binding.child_message_file_pointer.is_some(),
            io,
            Invalid,
            "inline child message has a file projection",
        );
    }
    let file: FileRef = decode(selected, io)?;
    let command: WorkflowCommand = decode(&binding.child.input, io)?;
    deny(
        command.payload != file || !binding.input_files.contains(&file),
        io,
    )?;
    let ordinary;
    let observer = if let Some(io) = io {
        io
    } else {
        ordinary = PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(binding.limits.work, binding.limits.bytes),
            cancellation,
        )?;
        &ordinary
    };
    let bytes = context.read_file(&file, Some(observer)).await?;
    if let Some(pointer) = &binding.child_message_file_pointer {
        let input: Value = recursive_decode_bytes(&bytes, Some(observer))?;
        return deny(
            input.pointer(pointer).and_then(Value::as_str) != Some(arguments.msg.as_str()),
            io,
        );
    }
    reject_if(
        command.kind != MODEL_TASK_COMMAND_KIND,
        io,
        Invalid,
        "non-model child requires an immutable message pointer",
    )?;
    let model: crate::filesystem::ModelTaskCommand =
        recursive_decode_bytes(&bytes, Some(observer))?;
    deny(
        !matches!(&model.input, crate::model::ModelContent::Text(message) if message == &arguments.msg),
        io,
    )
}
pub(super) fn validate_merge_binding<P: StreamProvider, A, O>(
    context: &ToolContext,
    binding: &RecursiveMergeToolBinding<P, A, O>,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let grants = context.effective_scope().grants();
    deny(
        !volume_granted(grants, &binding.project, VolumeOperation::Write, io)?
            || (binding.source_read.is_none()
                && !volume_granted(grants, &binding.source_project, VolumeOperation::Read, io)?),
        io,
    )
}
pub(super) fn merge_result(
    plan: &RecursiveMergeToolPlan,
    value: &Value,
    io: Option<&PreparationIo>,
) -> Result<Option<ProjectMergeReceipt>> {
    match value.get("status").and_then(Value::as_str) {
        Some("applied") => {
            let receipt = decode(
                value
                    .get("receipt")
                    .ok_or_else(|| recursive_reject(io, Invalid, "applied merge has no receipt"))?,
                io,
            )?;
            validate_merge_receipt(plan, &receipt, io)?;
            Ok(Some(receipt))
        }
        Some("no_changes" | "stale_target" | "conflicted" | "fenced" | "idempotency_conflict") => {
            Ok(None)
        }
        _ => Err(recursive_reject(
            io,
            Invalid,
            "merge is not a provider outcome",
        )),
    }
}
