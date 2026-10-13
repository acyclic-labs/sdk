//! Passive approval and publication observations.
use super::*;

#[derive(Serialize)]
pub(super) struct PiWritebackResponse<'a> {
    pub(super) status: &'static str,
    pub(super) operation: &'a str,
    pub(super) source: acyclic_harness::TaskId,
    pub(super) root: &'a Option<String>,
    pub(super) paths: &'a [String],
    pub(super) approval_digest: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) receipt: Option<&'a acyclic_fs::MaterializationJournal>,
}
pub(super) fn status_response<'a>(
    descriptor: &'a PiPreparedApproval,
    status: &'static str,
    native: Option<&'a acyclic_fs::MaterializationJournal>,
) -> PiWritebackResponse<'a> {
    PiWritebackResponse {
        status,
        operation: &descriptor.command.sdk_operation,
        source: descriptor.command.source,
        root: &descriptor.command.root,
        paths: &descriptor.command.paths,
        approval_digest: &descriptor.approval_digest,
        receipt: if status == "published" { native } else { None },
    }
}
#[derive(Serialize)]
pub(super) struct PiPendingResponse<'a> {
    pub(super) status: &'static str,
    pub(super) operation: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) source: Option<acyclic_harness::TaskId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) root: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) paths: Option<&'a [Value]>,
}
#[derive(Serialize)]
pub(super) struct PiPassiveRpcResponse<'a, T: Serialize> {
    pub(super) session: &'a str,
    pub(super) native_session: &'a str,
    pub(super) current_task: acyclic_harness::TaskId,
    #[serde(flatten)]
    pub(super) observation: T,
}
#[derive(Serialize)]
pub(super) struct PiInspectionResponse<'a> {
    #[serde(flatten)]
    pub(super) observation: PiWritebackResponse<'a>,
    pub(super) prepared: &'a PiPreparedApproval,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) imported_selected_files: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) file_evidence_cursor: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) file_evidence_complete: Option<&'a Value>,
}

pub(super) async fn observe_writeback(
    filesystem: &Arc<PiNativeFilesystem>,
    stream: &Arc<LocalStream>,
    resources: &crate::ServiceResources,
    protected: &PiOwnerConfig,
    descriptor: &PiPreparedApproval,
    work: &NativeMaterializationWork,
) -> Result<(&'static str, Option<acyclic_fs::MaterializationJournal>)> {
    let (journal, id) =
        conversation_journal(filesystem, stream, protected, &descriptor.into(), work)?;
    let operation = acyclic_fs::OperationId::from_bytes(descriptor.operation.into_bytes());
    let store = resources.store.materialization(work.clone());
    let native = acyclic_fs::MaterializationJournalStore::load(&store, operation)
        .await
        .map_err(|error| match error {
            acyclic_fs::LocalCoreStateStoreError::LazyWork(source) => {
                crate::pi_fork_host::native_lazy_error(source)
            }
            source => Error::NativeWorkspacePublication {
                source: acyclic_fs::NativeWorkspacePublicationError::State(source),
                custody: None,
            },
        })?;
    if let Some(journal) = &native {
        if !journal.is_valid_for(operation) {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Storage,
                reason: "original native writeback journal is invalid",
            });
        }
        if journal.plan != descriptor.journal.plan
            || journal.root_identity != descriptor.journal.root_identity
            || journal.preparation_identity != descriptor.journal.preparation_identity
            || journal.preimages != descriptor.journal.preimages
        {
            return Err(denied(
                "native writeback differs from its approved preparation",
            ));
        }
    }
    let status = if native
        .as_ref()
        .is_some_and(|journal| journal.phase == MaterializationPhase::Applied)
    {
        ToolApprovalVerifier::verify(
            &journal,
            id,
            descriptor.operation,
            pi_operator::parse_digest(&descriptor.approval_digest)?,
        )
        .await?;
        "published"
    } else {
        let io = PreparationIo::from_native_work(work)?;
        match journal.interaction_outcome(id, Some(&io))?.await? {
            Some(InteractionOutcome::Declined {}) => "declined",
            Some(InteractionOutcome::Approved {}) => "pending",
            _ if native
                .as_ref()
                .is_some_and(|journal| journal.phase == MaterializationPhase::Prepared) =>
            {
                "prepared"
            }
            _ => "pending",
        }
    };
    Ok((status, native))
}

/// Caller proof is verified by the existing actor before entering this passive API.
pub(crate) async fn passive_status(
    runtime: &PiTaskRuntime,
    filesystem: &Arc<PiNativeFilesystem>,
    stream: &std::sync::Arc<LocalStream>,
    resources: &crate::ServiceResources,
    protected: &PiOwnerConfig,
    session: &str,
    native_session: &str,
    caller: acyclic_harness::TaskId,
    sdk_operation: &str,
    source: acyclic_harness::TaskId,
    root: &str,
    paths: &[Value],
    work: &NativeMaterializationWork,
) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    if caller != protected.policy().root_task {
        return Err(denied(
            "writeback status belongs only to its original Root caller",
        ));
    }
    let Some((prepared, _prepared_custody)) =
        read_prepared_approval(runtime, filesystem, protected, sdk_operation, work).await?
    else {
        return crate::pi_root_provider::guarded_pi_value(
            &PiPassiveRpcResponse {
                session,
                native_session,
                current_task: caller,
                observation: PiPendingResponse {
                    status: "pending",
                    operation: sdk_operation,
                    source: Some(source),
                    root: Some(root),
                    paths: Some(paths),
                },
            },
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
            work,
        );
    };
    if prepared.session != session
        || prepared.command.caller != caller
        || prepared.command.source != source
        || prepared.command.root.as_deref() != Some(root)
        || prepared.command.paths.len() != paths.len()
        || prepared
            .command
            .paths
            .iter()
            .zip(paths)
            .any(|(expected, supplied)| supplied.as_str() != Some(expected.as_str()))
    {
        return Err(denied(
            "passive status changed the original operation's selected source, Root or paths",
        ));
    }
    let (status, native) =
        observe_writeback(filesystem, stream, resources, protected, &prepared, work).await?;
    crate::pi_root_provider::guarded_pi_value(
        &PiPassiveRpcResponse {
            session,
            native_session,
            current_task: caller,
            observation: status_response(&prepared, status, native.as_ref()),
        },
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
        work,
    )
}
