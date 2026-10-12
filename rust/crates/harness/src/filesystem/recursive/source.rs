use super::*;
use crate::conversation::ContentResidencyVerifier;
use crate::{
    conversation::{ContentGrant, VolumeOwner},
    core::{AuthorityVerifier, Reducer, Scope},
    filesystem::{FilesystemHost, PreparationIo, fork_preparer_io},
    fork::{ForkHistoryPolicy, ForkPreparation, ForkSelection},
    resources::{ProviderRef, ResourceKind, ResourceRef, StreamRef},
    runtime::TaskContext,
    source_authorization::ProjectSourceReadHandle,
    store::{HistoryReadLimits, HistoryReader, StreamAggregate},
};
use acyclic_fs::CancellationToken;
use acyclic_stream::StreamProvider;

/// Selects a current-caller fork from authenticated providers and history.
pub async fn recursive_fork_source<P, A, O>(
    filesystem: Arc<FilesystemHost<A, O>>,
    stream: &Arc<P>,
    parent: &StreamAggregate<P>,
    verifier: AuthorityVerifier,
    signed: Scope,
    context: &TaskContext,
    source: RecursiveSourceSnapshot,
    stream_provider: ProviderRef,
    operation_id: OperationId,
    arguments: &ForkToolArguments,
    config: RecursiveForkConfig,
    source_read: Option<ProjectSourceReadHandle>,
    resolver: Arc<dyn ContentResidencyVerifier>,
    cancellation: &CancellationToken,
) -> Result<RetainedSourceBinding<A, O>>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    cancellation
        .check()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let caller = context
        .durable_task_id()
        .ok_or_else(|| Error::Unauthorized("fork has no admitted caller".into()))?;
    if caller != source.task || !context.scope().grants().contains("fork:publish") {
        return Err(Error::Unauthorized(
            "fork caller/source authority differs".into(),
        ));
    }
    verifier.verify(&signed, &mut None)?;
    let agent = parent
        .reducer()
        .conversation()
        .and_then(|conversation| conversation.agent)
        .ok_or_else(|| Error::Invalid("fork caller conversation has no agent".into()))?;
    if signed.agent() != Some(agent) || source.private_volume.owner() != &VolumeOwner::Agent(agent)
    {
        return Err(Error::Unauthorized(
            "fork source is not the current caller agent".into(),
        ));
    }
    for volume in [&source.project, &source.private_volume] {
        let read = volume.capability(VolumeOperation::Read, &mut None)?;
        if !context.scope().grants().contains(&read) {
            return Err(Error::Unauthorized(
                "caller lacks selected source read authority".into(),
            ));
        }
        ContentGrant::verify(
            &verifier,
            &signed,
            volume,
            VolumeOperation::Read,
            u64::MAX,
            &mut None,
        )?;
    }
    let selected = arguments.source_task(caller, config.limits, &mut None)?;
    let authorization = match (selected == caller, source_read.as_ref()) {
        (false, Some(handle)) => {
            let authorization = handle.authorization();
            if authorization.caller_task != caller {
                return Err(Error::Unauthorized(
                    "selected source approval belongs to another caller".into(),
                ));
            }
            handle.require_selection(
                operation_id,
                selected,
                &authorization.project,
                &authorization.generation,
                authorization.native_snapshot.as_ref(),
                None,
            )?;
            Some(authorization)
        }
        (false, None) => {
            return Err(Error::Unauthorized(
                "foreign source requires RootService approval".into(),
            ));
        }
        (true, Some(_)) => {
            return Err(Error::Invalid(
                "current source cannot substitute a foreign token".into(),
            ));
        }
        (true, None) => None,
    };
    let history_limits = config.history_limits;
    let request = fork_request(
        parent.reducer(),
        operation_id,
        arguments,
        (
            caller,
            authorization.map_or(&source.project, |authorization| &authorization.project),
            authorization.map_or(&source.project_generation, |authorization| {
                &authorization.generation
            }),
            &source.private_volume,
            &source.private_generation,
        ),
        config,
        &stream_provider,
        authorization.cloned(),
        None,
    )?;
    restore_recursive_fork_source(
        filesystem,
        stream,
        parent.reducer(),
        verifier,
        signed,
        source.project,
        stream_provider,
        request,
        history_limits,
        source_read,
        resolver,
        cancellation,
    )
    .await
}

/// Restores the immutable request at its authenticated parent checkpoint.
pub async fn restore_recursive_fork_source<P, A, O>(
    filesystem: Arc<FilesystemHost<A, O>>,
    stream: &Arc<P>,
    parent: &Reducer,
    verifier: AuthorityVerifier,
    signed: Scope,
    caller_project: VolumeRef,
    stream_provider: ProviderRef,
    request: ForkRequest,
    history_limits: HistoryReadLimits,
    source_read: Option<ProjectSourceReadHandle>,
    resolver: Arc<dyn ContentResidencyVerifier>,
    cancellation: &CancellationToken,
) -> Result<RetainedSourceBinding<A, O>>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    cancellation
        .check()
        .map_err(|error| Error::Storage(error.to_string()))?;
    request.validate(&mut None)?;
    if request.parent != *parent.authority() || request.parent_revision != parent.revision() {
        return Err(Error::Conflict(
            "restored source differs from the original parent cut".into(),
        ));
    }
    verifier.verify_scope_for_with_work(parent.authority(), &signed, &mut None)?;
    if request.authorized_source.as_ref()
        != source_read
            .as_ref()
            .map(ProjectSourceReadHandle::authorization)
    {
        return Err(Error::Unauthorized(
            "restored source differs from its workspace approval".into(),
        ));
    }
    cancellation
        .check()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let history = HistoryReader::new(stream, parent.authority(), &verifier, &mut None)?
        .fork_history(
            parent,
            request.preparation.inherited_through_sequence,
            history_limits,
            &mut None,
        )
        .await?;
    cancellation
        .check()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let preparer = FilesystemForkPreparer::new(
        filesystem,
        parent.copy(&mut None)?,
        verifier,
        signed,
        caller_project,
        stream_provider,
        resolver,
        Some(history),
        source_read,
        #[cfg(not(target_arch = "wasm32"))]
        None,
    )?;
    Ok(RetainedSourceBinding {
        preparer: Arc::new(preparer),
        request,
    })
}

pub(in crate::filesystem) fn fork_request(
    parent: &Reducer,
    operation: OperationId,
    arguments: &ForkToolArguments,
    (caller, project, generation, private, private_generation): (
        TaskId,
        &VolumeRef,
        &GenerationRef,
        &VolumeRef,
        &GenerationRef,
    ),
    config: RecursiveForkConfig,
    stream_provider: &ProviderRef,
    authorized_source: Option<crate::source_authorization::ProjectSourceAuthorization>,
    io: Option<&PreparationIo>,
) -> Result<ForkRequest> {
    let (authority, path, project, private) = crate::executor::with_history_io(io, |work| {
        let authority = match work.as_deref_mut() {
            Some(work) => work.clone_authority(parent.authority())?,
            None => parent.authority().clone(),
        };
        Ok::<_, Error>((
            authority,
            parent.authority().stream_path(work)?.into_bytes(),
            project.copy(work)?,
            private.copy(work)?,
        ))
    })?;
    let provider = match io {
        Some(io) => stream_provider.clone_with_io(io)?,
        None => stream_provider.clone(),
    };
    let version = crate::executor::with_history_io(io, |work| {
        crate::executor::preparation_format(format_args!("{}", parent.revision()), work)
    })?;
    let reference = crate::executor::with_history_io(io, |work| {
        StreamRef::from_resource_with_work(
            ResourceRef::new(ResourceKind::Stream, provider, path, Some(version), work)?,
            work,
        )
    })?;
    let generation = match io {
        Some(io) => generation.clone_with_io(io)?,
        None => generation.clone(),
    };
    let private_generation = match io {
        Some(io) => private_generation.clone_with_io(io)?,
        None => private_generation.clone(),
    };
    let mut selections = crate::model::vec_with_capacity(3, io)?;
    for revision in [
        ResourceRevision::History(reference),
        ResourceRevision::Project {
            volume: project,
            generation,
        },
        ResourceRevision::PrivateVolume {
            volume: private,
            generation: private_generation,
            paths: Vec::new(),
        },
    ] {
        selections.push(ForkSelection {
            required: true,
            revision,
        });
    }
    crate::executor::with_history_io(io, |work| {
        ForkRequest {
            operation_id: operation,
            parent: authority,
            parent_revision: parent.revision(),
            child: config.child,
            child_agent: arguments.id,
            attached_agents: Vec::new(),
            preparation: ForkPreparation {
                child_project_volume: config.child_project,
                child_private_volume: config.child_private,
                child_result_volume: config.child_result_volume,
                summary: None,
                inherited_through_sequence: 0,
                maximum_inherited_messages: config.inherited_messages,
                maximum_inherited_bytes: config.inherited_bytes,
                maximum_inherited_references: config.inherited_references,
            },
            selections,
            boundary: None,
            authorized_source,
        }
        .with_history_policy(
            parent,
            if arguments.source.unwrap_or(caller) == caller {
                ForkHistoryPolicy::default()
            } else {
                ForkHistoryPolicy::Fresh
            },
            work,
        )
    })
}
