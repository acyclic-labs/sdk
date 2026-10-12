use super::*;
/// Builds the creator request with the caller's optional accounting observer.
pub fn fork_preparation_request<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    preparer: &FilesystemForkPreparer<A, O>,
    fork: &ForkRequest,
    preparation_task: TaskId,
    mut child: TaskAdmissionRecord,
    command: WorkflowCommand,
    limits: ForkPreparationLimits,
    source_checkpoint: Option<crate::conversation::FileRef>,
    input_files: Vec<crate::conversation::FileRef>,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<ResourcePreparationRequest> {
    use crate::filesystem::default_tools::{encode, recursive_reject};
    crate::executor::with_history_io(io, |work| {
        fork.validate(work)?;
        command.validate(work)
    })?;
    if command.kind != FORK_PREPARE_TASK_COMMAND_KIND
        || fork.operation_id != command.operation_id
        || command.operation_id == child.operation_id
        || command.operation_id.into_bytes() == preparation_task.into_bytes()
        || child.operation_id.into_bytes() == preparation_task.into_bytes()
    {
        return Err(recursive_reject(
            io,
            crate::wire::ErrorCode::Invalid,
            "fork source, preparation, child and effect identities must remain distinct",
        ));
    }
    let volumes = [
        &fork.preparation.child_project_volume,
        &fork.preparation.child_private_volume,
    ]
    .into_iter()
    .chain(fork.preparation.child_result_volume.as_ref());
    if let Some(io) = io {
        for volume in volumes {
            for prefix in ["volume:read:", "volume:write:"] {
                if !io.contains_capability(&child.grants, volume, prefix)? {
                    return Err(recursive_reject(
                        Some(io),
                        crate::wire::ErrorCode::Unauthorized,
                        "native child claims lack an exact authenticated created-volume operation",
                    ));
                }
            }
        }
        for file in &input_files {
            if !io.contains_capability(&child.grants, file, "file:read:")? {
                return Err(recursive_reject(
                    Some(io),
                    crate::wire::ErrorCode::Unauthorized,
                    "native child claims lack an exact authenticated input-file read",
                ));
            }
        }
    } else {
        let mut grants = child.grants.iter().map(str::to_owned).collect::<Vec<_>>();
        for volume in volumes {
            grants.push(volume.capability(VolumeOperation::Read, &mut None)?);
            grants.push(volume.capability(VolumeOperation::Write, &mut None)?);
        }
        for file in &input_files {
            file.validate(&mut None)?;
            grants.push(file.read_capability()?);
        }
        child.grants = Capabilities::new(grants);
    }
    let (resources, proofs) = preparer.creation_allocations(fork, preparation_task, io)?;
    child.resource_proofs = proofs;
    crate::executor::with_history_io(io, |work| child.validate(work))?;
    let provider = match io {
        Some(io) => fork
            .preparation
            .child_project_volume
            .provider()
            .clone_with_io(io)?,
        None => fork.preparation.child_project_volume.provider().clone(),
    };
    let read_authorization = fork
        .authorized_source
        .as_ref()
        .map(|authorization| {
            crate::executor::with_history_io(io, |work| {
                authorization.clone_with_work_and_custody(work)
            })
            .map(|(authorization, _)| authorization)
        })
        .transpose()?;
    let request = ResourcePreparationRequest {
        provider,
        child,
        resources,
        creation_grant: crate::filesystem::fork_preparer_io::clone_text("fork:publish", io)?,
        maximum_resources: limits.resources,
        maximum_work: limits.work,
        maximum_bytes: limits.bytes,
        read_authorization,
        payload: encode(fork, io)?,
        command: Some(command),
        source_checkpoint,
        input_files,
    };
    crate::executor::with_history_io(io, |work| request.validate(work))?;
    Ok(request)
}
