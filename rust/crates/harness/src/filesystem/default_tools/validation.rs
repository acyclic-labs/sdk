//! Admission and exact replay checks for recursive tool operations.
use super::*;
use crate::executor::with_history_io;
use crate::filesystem::{PreparationIo, PreparationRecord};
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

fn deny(rejected: bool, io: Option<&PreparationIo>) -> Result<()> {
    reject_if(
        rejected,
        io,
        Unauthorized,
        "recursive operation changed its admitted authority",
    )
}

pub(super) fn check_source_command(
    command: &WorkflowCommand,
    intent: &RecursiveForkSourceIntent,
    io: Option<&PreparationIo>,
) -> Result<()> {
    with_history_io(io, |work| command.validate(work))?;
    deny(
        command.kind != FORK_SOURCE_STAGE
            || command.operation_id != derived(intent.request.operation_id, "source-stage", io)?,
        io,
    )
}

pub(super) fn check_command(
    command: &WorkflowCommand,
    stage: &str,
    plan: &Plan,
    io: Option<&PreparationIo>,
) -> Result<()> {
    with_history_io(io, |work| command.validate(work))?;
    let effect = match plan {
        Plan::Fork(plan) => {
            recursive_decoded::<ForkRequest, _>(&plan.request.payload, io, |request| {
                Ok(request.operation_id)
            })?
        }
        Plan::Merge(plan) => plan.operation_id,
    };
    deny(
        command.kind != command_kind(stage)
            || command.operation_id != derived(effect, stage, io)?
            || command_reference(plan, stage)
                .is_some_and(|reference| &command.payload != reference),
        io,
    )
}

pub(super) fn validate_source_intent(
    intent: &RecursiveForkSourceIntent,
    io: Option<&PreparationIo>,
) -> Result<()> {
    with_history_io(io, |work| intent.request.validate(work))?;
    deny(
        intent.request.child_agent != intent.arguments.id
            || intent.source_task != intent.arguments.source.unwrap_or(intent.caller)
            || intent.child_operation_id == intent.request.operation_id
            || task_id_for_admission(intent.child_operation_id) == intent.caller,
        io,
    )?;
    for selection in &intent.request.selections {
        recursive_inspect(io, 1)?;
        deny(
            matches!(&selection.revision,
            crate::fork::ResourceRevision::PrivateVolume { volume, paths, .. }
            if !paths.is_empty() || volume.owner() == &crate::conversation::VolumeOwner::Agent(intent.arguments.id)),
            io,
        )?;
    }
    Ok(())
}

pub(super) fn require_plan_arguments(
    plan: &Plan,
    arguments: &Value,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let matches = match plan {
        Plan::Fork(plan) => recursive_encoded_equal(&plan.arguments, arguments, io)?,
        Plan::Merge(plan) => recursive_encoded_equal(&plan.arguments, arguments, io)?,
    };
    deny(!matches, io)
}

pub(super) fn validate_plan(plan: &Plan, io: Option<&PreparationIo>) -> Result<()> {
    match plan {
        Plan::Fork(plan) => {
            with_history_io(io, |work| {
                plan.request.validate(work)?;
                plan.preparation.validate(work)
            })?;
            deny(plan.request.source_checkpoint.is_none(), io)?;
            recursive_decoded::<ForkRequest, _>(&plan.request.payload, io, |fork| {
                with_history_io(io, |work| fork.validate(work))?;
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
            })?;
            reject_if(
                !plan.child_message_pointer.starts_with('/')
                    || plan
                        .child_message_file_pointer
                        .as_ref()
                        .is_some_and(|pointer| !pointer.starts_with('/')),
                io,
                Invalid,
                "fork message pointer is invalid",
            )?;
        }
        Plan::Merge(plan) => {
            deny(plan.source_project == plan.target_project, io)?;
            with_history_io(io, |work| {
                plan.source_project.validate(work)?;
                plan.target_project.validate(work)?;
                plan.source_generation.validate(work)?;
                plan.expected_target_generation.validate(work)?;
                plan.notice.validate(work)
            })?;
            if let Some(source) = &plan.authorized_source {
                with_history_io(io, |work| source.validate(work))?;
                deny(
                    source.operation_id != plan.operation_id
                        || source.caller_task != plan.caller
                        || source.source_task != plan.arguments.source
                        || source.project != plan.source_project
                        || source.generation != plan.source_generation,
                    io,
                )?;
            }
        }
    }
    Ok(())
}

pub(super) fn validate_merge_receipt(
    plan: &RecursiveMergeToolPlan,
    receipt: &ProjectMergeReceipt,
    io: Option<&PreparationIo>,
) -> Result<()> {
    with_history_io(io, |work| receipt.validate_shape(work))?;
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
        Some(io) => io.contains_capability(
            grants,
            volume,
            match operation {
                VolumeOperation::Read => "volume:read:",
                VolumeOperation::Write => "volume:write:",
            },
        ),
        None => Ok(grants.contains(&volume.capability(operation, &mut None)?)),
    }
}

fn exact_file_granted(
    grants: &Capabilities,
    file: &FileRef,
    io: Option<&PreparationIo>,
) -> Result<bool> {
    match io {
        Some(io) => io.contains_capability(grants, file, "file:read:"),
        None => Ok(grants.contains(&file.read_capability(&mut None)?)),
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
    let effective = context.effective_scope();
    with_history_io(io, |work| {
        binding.source.request.validate(work)?;
        binding.child.validate(work)?;
        effective
            .limits()
            .validate_file(&binding.source_checkpoint, work)
    })?;
    deny(
        !with_history_io(io, |work| {
            crate::runtime::read_granted_with_work(
                effective.grants(),
                &binding.source_checkpoint,
                work,
            )
        })? || binding.source.request.operation_id != context.operation_id()
            || binding.source.request.child_agent != arguments.id
            || binding.child.parent != context.durable_task_id()
            || binding.parent.reducer().authority() != &binding.source.request.parent
            || binding.child_conversation.reducer().authority() != &binding.source.request.child
            || binding.child_scope.agent() != Some(arguments.id)
            || binding.parent_scope.agent()
                != binding
                    .parent
                    .reducer()
                    .conversation()
                    .and_then(|conversation| conversation.agent),
        io,
    )?;
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
    for file in &binding.input_files {
        with_history_io(io, |work| effective.limits().validate_file(file, work))?;
        deny(
            !with_history_io(io, |work| {
                crate::runtime::read_granted_with_work(effective.grants(), file, work)
            })? || !exact_file_granted(&binding.child.grants, file, io)?
                || !exact_file_granted(binding.child_scope.capabilities(), file, io)?,
            io,
        )?;
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
    with_history_io(io, |work| command.validate(work))?;
    deny(
        command.payload != file
            || !binding.input_files.contains(&file)
            || !with_history_io(io, |work| {
                crate::runtime::read_granted_with_work(&binding.child.grants, &file, work)
            })?,
        io,
    )?;
    let bytes = 'read: {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            break 'read context.read_file_with_native_work(&file, work).await?;
        }
        break 'read context
            .read_file_measured(
                &file,
                acyclic_fs::performance::work_budget(binding.limits.work, binding.limits.bytes),
                cancellation,
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
    };
    if let Some(pointer) = &binding.child_message_file_pointer {
        reject_if(
            !pointer.starts_with('/'),
            io,
            Invalid,
            "immutable child message pointer is invalid",
        )?;
        let input: Value = recursive_decode_bytes(&bytes, io)?;
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
    let model: crate::filesystem::ModelTaskCommand = recursive_decode_bytes(&bytes, io)?;
    deny(
        !matches!(&model.input, crate::model::ModelContent::Text(message) if message == &arguments.msg),
        io,
    )
}

pub(super) fn recursive_decode_bytes<T: PreparationRecord>(
    bytes: &[u8],
    io: Option<&PreparationIo>,
) -> Result<T> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io {
        return Ok(crate::filesystem::retain_decoded(io.decode(
            std::marker::PhantomData::<T>,
            bytes,
            crate::wire::ErrorCode::Storage,
        )?));
    }
    let _ = io;
    crate::contract::json_from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))
}

pub(super) fn validate_merge_binding<P: StreamProvider, A, O>(
    context: &ToolContext,
    source_task: TaskId,
    binding: &RecursiveMergeToolBinding<P, A, O>,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let effective = context.effective_scope();
    with_history_io(io, |work| {
        binding.verifier.verify_scope_for_with_work(
            binding.destination.reducer().authority(),
            &binding.scope,
            work,
        )
    })?;
    deny(
        !volume_granted(
            effective.grants(),
            &binding.project,
            VolumeOperation::Write,
            io,
        )? || binding.scope.agent()
            != binding
                .destination
                .reducer()
                .conversation()
                .and_then(|conversation| conversation.agent),
        io,
    )?;
    verify_volume(
        &binding.verifier,
        &binding.scope,
        &binding.project,
        VolumeOperation::Write,
        io,
    )?;
    if let Some(source) = &binding.source_read {
        let authorization = source.authorization();
        deny(
            Some(authorization.caller_task) != context.durable_task_id()
                || authorization.operation_id != context.operation_id()
                || authorization.source_task != source_task,
            io,
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        let work = io.and_then(PreparationIo::original_native_work);
        #[cfg(target_arch = "wasm32")]
        let work = None;
        source.require_selection(
            context.operation_id(),
            source_task,
            &binding.source_project,
            &binding.source_generation,
            authorization.native_snapshot.as_ref(),
            work,
        )?;
        source
            .grant()
            .require(&binding.source_project, VolumeOperation::Read, io)?;
    } else {
        deny(
            !volume_granted(
                effective.grants(),
                &binding.source_project,
                VolumeOperation::Read,
                io,
            )?,
            io,
        )?;
        verify_volume(
            &binding.verifier,
            &binding.scope,
            &binding.source_project,
            VolumeOperation::Read,
            io,
        )?;
    }
    Ok(())
}

fn verify_volume(
    verifier: &AuthorityVerifier,
    scope: &Scope,
    volume: &VolumeRef,
    operation: VolumeOperation,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let maximum = io
        .map(|io| io.remaining().map(|remaining| remaining.bytes_encoded))
        .transpose()?
        .unwrap_or(u64::MAX);
    with_history_io(io, |work| {
        ContentGrant::authorize_volume(verifier, scope, volume, operation, maximum, work)
    })
}

pub(super) fn command_reference<'a>(plan: &'a Plan, stage: &str) -> Option<&'a FileRef> {
    match (plan, stage) {
        (Plan::Fork(plan), "observe" | "fork-publish") => plan.request.source_checkpoint.as_ref(),
        _ => None,
    }
}
