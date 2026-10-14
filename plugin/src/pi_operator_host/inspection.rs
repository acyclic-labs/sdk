//! Operator request validation and passive file evidence.
use super::*;

/// Rejects invalid operator responses before staging an immutable Root control.
pub(crate) async fn validate_approval_request(
    runtime: &PiTaskRuntime,
    filesystem: &Arc<PiNativeFilesystem>,
    protected: &PiOwnerConfig,
    session: &str,
    request: &PiOperatorRequest,
    work: &NativeMaterializationWork,
) -> Result<()> {
    pi_operator::verify_request(protected, session, request, work)?;
    let PiOperatorBody::Approval {
        prepared, digest, ..
    } = &request.body
    else {
        return Err(denied("inspect cannot dispatch a Root approval"));
    };
    if digest != &prepared.approval_digest {
        return Err(denied("operator response has the wrong prepared digest"));
    }
    let (original, _original_custody) = read_prepared_approval(
        runtime,
        filesystem,
        protected,
        &prepared.command.sdk_operation,
        work,
    )
    .await?
    .ok_or_else(|| denied("operator operation has no original preparation"))?;
    let original = crate::pi_root_provider::digest_pi_value(
        &original,
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
        work,
    )?;
    let supplied = crate::pi_root_provider::digest_pi_value(
        prepared,
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
        work,
    )?;
    if original != supplied {
        return Err(denied(
            "operator substituted the prepared selection, generations or digest",
        ));
    }
    Ok(())
}

/// Signed responder authority stays in Root-only input storage.
pub(crate) async fn stage_operator_command(
    protected: &PiOwnerConfig,
    filesystem: &Arc<PiNativeFilesystem>,
    request: &PiOperatorRequest,
    work: &NativeMaterializationWork,
) -> Result<(WorkflowCommand, acyclic_fs::NativeAllocation)> {
    let root = protected.policy().root_task;
    let operation_id = derived(
        original_operation(root, root, request.body.operation(), work)?,
        "selected-writeback-publish",
        work,
    )?;
    let mut staged = crate::pi_root_provider::stage_pi_operator_input(
        protected,
        filesystem,
        operation_id,
        "command",
        request,
        work,
    )
    .await?;
    let mut kind = work.display(&format_args!("{PI_OPERATOR_COMMAND_KIND}"))?;
    staged.1.absorb(&mut kind.1)?;
    Ok((
        WorkflowCommand {
            operation_id,
            kind: kind.0,
            payload: staged.0,
        },
        staged.1,
    ))
}

pub(crate) async fn inspect_operator(
    runtime_filesystem_stream_resources: (
        &PiTaskRuntime,
        &Arc<PiNativeFilesystem>,
        &std::sync::Arc<LocalStream>,
        &crate::ServiceResources,
    ),
    protected: &PiOwnerConfig,
    session: &str,
    retained: &crate::AdapterState,
    roots: &std::collections::BTreeMap<String, crate::pi_root_provider::PiRootHandle>,
    request: &PiOperatorRequest,
    work: &NativeMaterializationWork,
) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    let (runtime, filesystem, stream, resources) = runtime_filesystem_stream_resources;
    let PiOperatorBody::Inspect {
        operation,
        project_files,
        file_evidence,
        ..
    } = &request.body
    else {
        return Err(denied("approval cannot use the passive inspection route"));
    };
    if !*project_files && file_evidence.is_some() {
        return Err(denied("file evidence requires explicit file projection"));
    }
    let Some((prepared, _prepared_custody)) =
        read_prepared_approval(runtime, filesystem, protected, operation, work).await?
    else {
        return crate::pi_root_provider::guarded_pi_value(
            &PiPendingResponse {
                status: "pending",
                operation,
                source: None,
                root: None,
                paths: None,
            },
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
            work,
        );
    };
    if prepared.session != session {
        return Err(denied(
            "prepared operation belongs to another original Root session",
        ));
    }
    let (status, native) =
        observe_writeback(filesystem, stream, resources, protected, &prepared, work).await?;
    let projection = if *project_files {
        if let Some(evidence) = file_evidence {
            if !(1..=262_144).contains(&evidence.maximum_bytes) {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Invalid,
                    reason: "file evidence requires 1..262144 bytes",
                });
            }
            if let Some(cursor) = &evidence.cursor
                && (cursor.operation != prepared.command.sdk_operation
                    || cursor.path != evidence.path
                    || prepared.command.root.as_deref() != Some(cursor.root.as_str())
                    || cursor.generation != prepared.imported)
            {
                return Err(denied(
                    "file evidence cursor changed its original operation, Root, path or generation",
                ));
            }
        }
        let projection = crate::pi_operator::evidence::read_imported_selected_files(
            protected,
            retained,
            roots,
            &prepared,
            file_evidence.as_ref(),
            work,
        )
        .await?;
        Some(projection)
    } else {
        None
    };
    let fields = projection.as_ref().map(|(value, _custody)| value);
    let field = |name: &str| -> Result<Option<&Value>> {
        fields
            .map(|value| {
                value.get(name).ok_or(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Storage,
                    reason: "immutable file projection omitted an authored evidence field",
                })
            })
            .transpose()
    };
    crate::pi_root_provider::guarded_pi_value(
        &PiInspectionResponse {
            observation: status_response(&prepared, status, native.as_ref()),
            prepared: &prepared,
            imported_selected_files: field("files")?,
            file_evidence_cursor: field("cursor")?,
            file_evidence_complete: field("complete")?,
        },
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
        work,
    )
}
