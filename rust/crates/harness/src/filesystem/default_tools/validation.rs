//! Validation for portable recursive tools.

use super::*;

pub(super) fn check_command(
    command: &WorkflowCommand,
    stage: &str,
    plan: &Plan,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<()> {
    crate::executor::with_history_io(io, |work| command.validate(work))?;
    let effect = match plan {
        Plan::Fork(plan) => {
            recursive_decoded::<ForkRequest, _>(&plan.request.payload, io, |request| {
                Ok(request.operation_id)
            })?
        }
        Plan::Merge(plan) => plan.operation_id,
    };
    if command.kind != command_kind(stage) || command.operation_id != derived(effect, stage, io)? {
        return Err(recursive_reject(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "recursive command identity or kind changed",
        ));
    }
    Ok(())
}
pub(super) fn validate_source_intent(
    intent: &RecursiveForkSourceIntent,
    arguments: &Value,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<()> {
    crate::executor::with_history_io(io, |work| intent.request.validate(work))?;
    for selection in &intent.request.selections {
        recursive_inspect(io, 1)?;
        if matches!(&selection.revision, crate::fork::ResourceRevision::PrivateVolume { volume, paths, .. }
            if !paths.is_empty() || volume.owner() == &crate::conversation::VolumeOwner::Agent(intent.arguments.id))
        {
            return Err(recursive_reject(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "default fork must create a new agent with empty inherited scratch",
            ));
        }
    }
    if !recursive_encoded_equal(&intent.arguments, arguments, io)?
        || intent.request.child_agent != intent.arguments.id
        || intent.source_task != intent.arguments.source.unwrap_or(intent.caller)
        || intent.child_operation_id == intent.request.operation_id
        || task_id_for_admission(intent.child_operation_id) == intent.caller
    {
        return Err(recursive_reject(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "source intent changed original arguments, caller or independent allocation",
        ));
    }
    Ok(())
}
pub(super) fn check_source_command(
    command: &WorkflowCommand,
    intent: &RecursiveForkSourceIntent,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<()> {
    crate::executor::with_history_io(io, |work| command.validate(work))?;
    if command.kind != FORK_SOURCE_STAGE
        || command.operation_id != derived(intent.request.operation_id, "source-stage", io)?
    {
        return Err(recursive_reject(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "source staging command changed its original effect identity",
        ));
    }
    Ok(())
}
pub(super) fn validate_plan(
    plan: &Plan,
    arguments: &Value,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<()> {
    match plan {
        Plan::Fork(plan) => {
            crate::executor::with_history_io(io, |work| {
                plan.request.validate(work)?;
                plan.preparation.validate(work)
            })?;
            if plan.request.source_checkpoint.is_none() {
                return Err(recursive_reject(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "fork plan changed its original source checkpoint",
                ));
            }
            recursive_decoded::<ForkRequest, _>(&plan.request.payload, io, |fork| {
                crate::executor::with_history_io(io, |work| fork.validate(work))?;
                if !recursive_encoded_equal(&plan.arguments, arguments, io)?
                    || fork.child_agent != plan.arguments.id
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
                    })
                {
                    return Err(recursive_reject(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "fork plan changed original arguments, parent or independent identities",
                    ));
                }
                Ok(())
            })?;
            if plan.child_message_pointer.is_empty() || !plan.child_message_pointer.starts_with('/')
            {
                return Err(recursive_reject(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "fork producer message pointer is invalid",
                ));
            }
            if plan
                .child_message_file_pointer
                .as_ref()
                .is_some_and(|pointer| pointer.is_empty() || !pointer.starts_with('/'))
            {
                return Err(recursive_reject(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "fork immutable input message pointer is invalid",
                ));
            }
        }
        Plan::Merge(plan) => {
            if !recursive_encoded_equal(&plan.arguments, arguments, io)?
                || plan.source_project == plan.target_project
            {
                return Err(recursive_reject(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "merge plan changed original arguments or source/destination",
                ));
            }
            crate::executor::with_history_io(io, |work| {
                plan.source_project.validate(work)?;
                plan.target_project.validate(work)?;
                plan.source_generation.validate(work)?;
                plan.expected_target_generation.validate(work)?;
                plan.notice.validate(work)
            })?;
            if let Some(source) = &plan.authorized_source {
                crate::executor::with_history_io(io, |work| source.validate(work))?;
                if source.operation_id != plan.operation_id
                    || source.caller_task != plan.caller
                    || source.source_task != plan.arguments.source
                    || source.project != plan.source_project
                    || source.generation != plan.source_generation
                {
                    return Err(recursive_reject(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "merge plan source policy changed the original caller, operation or immutable selection",
                    ));
                }
            }
        }
    }
    Ok(())
}
pub(super) fn validate_merge_receipt(
    plan: &RecursiveMergeToolPlan,
    receipt: &ProjectMergeReceipt,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<()> {
    crate::executor::with_history_io(io, |work| receipt.validate_shape(work))?;
    if receipt.operation_id != plan.operation_id
        || receipt.child != plan.source
        || receipt.source_project != plan.source_project
        || receipt.source_generation != plan.source_generation
        || receipt.target_project != plan.target_project
        || receipt.expected_target_generation != plan.expected_target_generation
        || receipt.notice != plan.notice
    {
        return Err(recursive_reject(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "merge receipt changed the inspected source or CAS target",
        ));
    }
    Ok(())
}

pub(super) async fn validate_fork_binding<P, A, O>(
    context: &ToolContext,
    arguments: &ForkToolArguments,
    binding: &RecursiveForkToolBinding<P, A, O>,
    cancellation: &CancellationToken,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<()>
where
    P: StreamProvider,
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let effective = context.effective_scope();
    crate::executor::with_history_io(io, |work| {
        binding.source.request.validate(work)?;
        binding.child.validate(work)?;
        effective
            .limits()
            .validate_file(&binding.source_checkpoint, work)
    })?;
    if !crate::executor::with_history_io(io, |work| {
        crate::runtime::read_granted_with_work(effective.grants(), &binding.source_checkpoint, work)
    })? {
        return Err(recursive_reject(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "caller cannot read the original fork checkpoint",
        ));
    }
    if binding.source.request.operation_id != context.operation_id()
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
                .and_then(|conversation| conversation.agent)
    {
        return Err(recursive_reject(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "fork binding is not the actual original caller and new child",
        ));
    }
    for selection in &binding.source.request.selections {
        recursive_inspect(io, 1)?;
        if let crate::fork::ResourceRevision::PrivateVolume { volume, paths, .. } =
            &selection.revision
        {
            let caller_agent = binding.parent_scope.agent().ok_or_else(|| {
                recursive_reject(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "caller has no bound agent",
                )
            })?;
            if !paths.is_empty()
                || volume.owner() != &crate::conversation::VolumeOwner::Agent(caller_agent)
                || binding.parent_scope.agent() == Some(arguments.id)
            {
                return Err(recursive_reject(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "fork cannot substitute caller history/private identity",
                ));
            }
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                if recursive_volume_granted(&binding.child.grants, volume, operation, io)?
                    || recursive_volume_granted(
                        binding.child_scope.capabilities(),
                        volume,
                        operation,
                        io,
                    )?
                {
                    return Err(recursive_reject(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "default child cannot inherit broad parent-private authority",
                    ));
                }
            }
        }
    }
    for file in &binding.input_files {
        crate::executor::with_history_io(io, |work| effective.limits().validate_file(file, work))?;
        if !crate::executor::with_history_io(io, |work| {
            crate::runtime::read_granted_with_work(effective.grants(), file, work)
        })? || !recursive_exact_file_granted(&binding.child.grants, file, io)?
            || !recursive_exact_file_granted(binding.child_scope.capabilities(), file, io)?
        {
            return Err(recursive_reject(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "child input is not an actual donated exact-file read",
            ));
        }
    }
    let selected = binding
        .child
        .input
        .pointer(&binding.child_message_pointer)
        .ok_or_else(|| {
            recursive_reject(
                io,
                crate::wire::ErrorCode::Invalid,
                "child producer input does not retain fork msg",
            )
        })?;
    if selected.as_str() == Some(arguments.msg.as_str()) {
        if binding.child_message_file_pointer.is_some() {
            return Err(recursive_reject(
                io,
                crate::wire::ErrorCode::Invalid,
                "inline child message has an immutable-file projection",
            ));
        }
        return Ok(());
    }
    let file: FileRef = decode(selected, io)?;
    let command: WorkflowCommand = decode(&binding.child.input, io)?;
    crate::executor::with_history_io(io, |work| command.validate(work))?;
    if command.payload != file || !binding.input_files.contains(&file) {
        return Err(recursive_reject(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "child msg reference is not its exact admitted immutable command input",
        ));
    }
    if !crate::executor::with_history_io(io, |work| {
        crate::runtime::read_granted_with_work(&binding.child.grants, &file, work)
    })? {
        return Err(recursive_reject(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "child cannot read its exact initial message",
        ));
    }
    let bytes = 'read: {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(crate::filesystem::PreparationIo::original_native_work) {
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
        if pointer.is_empty() || !pointer.starts_with('/') {
            return Err(recursive_reject(
                io,
                crate::wire::ErrorCode::Invalid,
                "child immutable message pointer is invalid",
            ));
        }
        let input: Value = recursive_decode_bytes(&bytes, io)?;
        if input.pointer(pointer).and_then(Value::as_str) != Some(arguments.msg.as_str()) {
            return Err(recursive_reject(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "child producer immutable message differs from original fork msg",
            ));
        }
        return Ok(());
    }
    if command.kind != MODEL_TASK_COMMAND_KIND {
        return Err(recursive_reject(
            io,
            crate::wire::ErrorCode::Invalid,
            "non-model child input requires an explicit immutable message pointer",
        ));
    }
    let model: crate::filesystem::ModelTaskCommand = recursive_decode_bytes(&bytes, io)?;
    if !matches!(&model.input, crate::model::ModelContent::Text(message) if message == &arguments.msg)
    {
        return Err(recursive_reject(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "child initial model message differs from original fork msg",
        ));
    }
    Ok(())
}

pub(super) fn recursive_exact_file_granted(
    grants: &Capabilities,
    file: &FileRef,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<bool> {
    match io {
        Some(io) => io.contains_capability(grants, file, "file:read:"),
        None => Ok(grants.contains(&file.read_capability(&mut None)?)),
    }
}

pub(super) fn recursive_decode_bytes<T: crate::filesystem::PreparationRecord>(
    bytes: &[u8],
    io: Option<&crate::filesystem::PreparationIo>,
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

pub(super) fn validate_merge_binding<P, A, O>(
    context: &ToolContext,
    source_task: TaskId,
    binding: &RecursiveMergeToolBinding<P, A, O>,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<()>
where
    P: StreamProvider,
{
    let effective = context.effective_scope();
    crate::executor::with_history_io(io, |work| {
        binding.verifier.verify_scope_for_with_work(
            binding.destination.reducer().authority(),
            &binding.scope,
            work,
        )
    })?;
    if !recursive_volume_granted(
        effective.grants(),
        &binding.project,
        VolumeOperation::Write,
        io,
    )? {
        return Err(recursive_reject(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "caller lacks its exact merge destination write grant",
        ));
    }
    recursive_verify_volume(
        &binding.verifier,
        &binding.scope,
        &binding.project,
        VolumeOperation::Write,
        io,
    )?;
    match &binding.source_read {
        Some(source) => {
            let authorization = source.authorization();
            if Some(authorization.caller_task) != context.durable_task_id()
                || authorization.operation_id != context.operation_id()
                || authorization.source_task != source_task
            {
                return Err(recursive_reject(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "merge source handle belongs to another caller, operation or selected task",
                ));
            }
            #[cfg(not(target_arch = "wasm32"))]
            let work = io.and_then(crate::filesystem::PreparationIo::original_native_work);
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
        }
        None => {
            if !recursive_volume_granted(
                effective.grants(),
                &binding.source_project,
                VolumeOperation::Read,
                io,
            )? {
                return Err(recursive_reject(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "merge source has neither an original caller read grant nor a sealed selected-source handle",
                ));
            }
            recursive_verify_volume(
                &binding.verifier,
                &binding.scope,
                &binding.source_project,
                VolumeOperation::Read,
                io,
            )?;
        }
    }
    if binding.scope.agent()
        != binding
            .destination
            .reducer()
            .conversation()
            .and_then(|conversation| conversation.agent)
    {
        return Err(recursive_reject(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "merge destination is not the actual caller conversation",
        ));
    }
    Ok(())
}

pub(super) fn recursive_volume_granted(
    grants: &Capabilities,
    volume: &VolumeRef,
    operation: VolumeOperation,
    io: Option<&crate::filesystem::PreparationIo>,
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

pub(super) fn recursive_verify_volume(
    verifier: &AuthorityVerifier,
    scope: &Scope,
    volume: &VolumeRef,
    operation: VolumeOperation,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<()> {
    let maximum = io
        .map(|io| io.remaining().map(|remaining| remaining.bytes_encoded))
        .transpose()?
        .unwrap_or(u64::MAX);
    crate::executor::with_history_io(io, |work| {
        ContentGrant::authorize_volume(verifier, scope, volume, operation, maximum, work)
    })
}
pub(super) fn check_merge_binding<P: StreamProvider, A, O>(
    plan: &RecursiveMergeToolPlan,
    binding: &RecursiveMergeToolBinding<P, A, O>,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<()> {
    if plan.destination != *binding.destination.reducer().authority()
        || plan.source != binding.source
        || plan.source_project != binding.source_project
        || plan.source_generation != binding.source_generation
        || plan.target_project != binding.project
        || plan.notice != binding.notice
        || plan.authorized_source.as_ref()
            != binding
                .source_read
                .as_ref()
                .map(|source| source.authorization())
    {
        return Err(recursive_reject(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "restored merge changed its original inspected binding",
        ));
    }
    Ok(())
}
