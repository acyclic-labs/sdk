use super::*;

pub(super) fn decoded_tool_request(
    context: &acyclic_harness::runtime::ToolContext,
    invocation: &acyclic_harness::tool::ToolInvocation,
    work: &NativeMaterializationWork,
) -> Result<(PiBuiltinRequest, NativeAllocation)> {
    work.remaining()?;
    if invocation.name != PI_BUILTIN_COMMAND_KIND
        || invocation.operation_id != context.operation_id()
        || invocation.call_id != context.call_id()
    {
        return Err(denied(
            "builtin invocation differs from its original SDK command",
        ));
    }
    let decoded = PiBuiltinRequest::decode(&invocation.arguments, work)?;
    if context.task().durable_task_id() != Some(decoded.0.caller) {
        return Err(denied(
            "builtin input substitutes its original accepted task actor",
        ));
    }
    Ok(decoded)
}

pub(super) fn require_builtin_mapping_access(
    mapping: &acyclic_harness::filesystem::NativeVolumeMapping,
    io: &PiBuiltinIo,
) -> Result<()> {
    if !matches!(io, PiBuiltinIo::Read { .. }) && !mapping.writable {
        return Err(denied(
            "builtin native volume is not an original writable destination",
        ));
    }
    Ok(())
}

pub(super) async fn accepted_builtin_io(
    context: &acyclic_harness::runtime::ToolContext,
    invocation: &acyclic_harness::tool::ToolInvocation,
    work: &NativeMaterializationWork,
) -> Result<(acyclic_harness::tool::ToolResult, NativeAllocation)> {
    use acyclic_harness::conversation::VolumeOperation;
    let decoded = decoded_tool_request(context, invocation, work)?;
    let _request_custody = decoded.1;
    let request = decoded.0;
    let admission = context.original_admission_with_native_work(work)?.await?;
    let encoded = crate::pi_root_provider::guarded_pi_bytes(
        &admission.0.input,
        crate::MAXIMUM_ADAPTER_STATE_BYTES,
        work,
    )?;
    let input = crate::pi_root_provider::guarded_pi_decode::<crate::pi_task::PiTaskInput>(
        &encoded.0,
        crate::MAXIMUM_ADAPTER_STATE_BYTES,
        crate::MAXIMUM_ADAPTER_STATE_BYTES,
        acyclic_harness::wire::ErrorCode::Invalid,
        acyclic_harness::JsonValueSchema::None,
        crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
        work,
    )?;
    drop(encoded.0);
    drop(encoded.1);
    let _input_custody = input.1;
    let input = input.0;
    if admission.0.parent.is_none()
        || input.binding.task_id != request.caller
        || !matches!(
            input.execution,
            crate::pi_task::PiTaskExecution::Child { .. }
        )
    {
        return Err(denied(
            "working builtin does not belong to its original declared child task",
        ));
    }
    let manifest = context.native_working_manifest_with_native_work(work)?;
    validate_declared_working_roles(&input.binding, manifest)?;
    let (mapping, leaf) = select_native_path(&request.path, &input.binding, manifest)?;
    require_builtin_mapping_access(mapping, &request.io)?;
    let variants = match &request.io {
        PiBuiltinIo::Read { variants, .. } => variants.as_slice(),
        _ => &[],
    };
    let leaves = scoped_variants(variants, &mapping.path)?;
    let volume_role = if mapping.volume == input.binding.scratch.volume {
        "SCRATCH"
    } else {
        "PROJECT"
    };
    let response = match &request.io {
        PiBuiltinIo::Read { maximum_bytes, .. } | PiBuiltinIo::EditRead { maximum_bytes } => {
            let mut last_absence = None;
            for (leaf, candidate) in std::iter::once(leaf)
                .chain(leaves.into_iter().flatten())
                .zip(std::iter::once(&request.path).chain(variants.iter()))
            {
                let path = mounted_relative_path(manifest, mapping, leaf, work)?;
                if matches!(request.io, PiBuiltinIo::EditRead { .. }) {
                    context
                        .authorize_working_file_with_native_work(
                            &path.0,
                            VolumeOperation::Write,
                            work,
                        )
                        .await?;
                }
                match context
                    .read_working_file_with_native_work(&path.0, *maximum_bytes as usize, work)
                    .await
                {
                    Ok(bytes) => {
                        context
                            .authorize_working_file_with_native_work(
                                &path.0,
                                VolumeOperation::Read,
                                work,
                            )
                            .await?;
                        let (value, custody) =
                            read_response(bytes.as_ref(), candidate, volume_role, work)?;
                        return Ok((acyclic_harness::tool::ToolResult { value }, custody));
                    }
                    Err(error) if is_absent(&error) => last_absence = Some(error),
                    Err(error) => return Err(error),
                }
            }
            return Err(last_absence.expect("the requested candidate is always checked"));
        }
        PiBuiltinIo::Write { content } | PiBuiltinIo::EditWrite { content, .. } => {
            let path = mounted_relative_path(manifest, mapping, leaf, work)?;
            let expected = match &request.io {
                PiBuiltinIo::EditWrite {
                    expected_sha256, ..
                } => Some(expected_digest(expected_sha256)?),
                _ => None,
            };
            context
                .write_working_file_with_native_work(
                    &path.0,
                    content,
                    matches!(request.io, PiBuiltinIo::Write { .. }),
                    expected,
                    work,
                )
                .await?;
            context
                .authorize_working_file_with_native_work(&path.0, VolumeOperation::Write, work)
                .await?;
            #[derive(Serialize)]
            struct Response<'a> {
                written: bool,
                path: &'a str,
                volume_role: &'a str,
            }
            crate::pi_root_provider::guarded_pi_value(
                &Response {
                    written: true,
                    path: &request.path,
                    volume_role,
                },
                8192,
                work,
            )?
        }
    };
    Ok((
        acyclic_harness::tool::ToolResult { value: response.0 },
        response.1,
    ))
}
