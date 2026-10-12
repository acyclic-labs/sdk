use super::*;
/// Constructs the portable current-caller fork from actual providers and authenticated history.
#[allow(clippy::too_many_arguments, reason = "explicit provider, owner, source and finite-bound contracts")]
pub async fn recursive_fork_source<P, A, O>(
    filesystem: Arc<crate::filesystem::FilesystemHost<A, O>>,
    stream: &Arc<P>,
    parent: &crate::store::StreamAggregate<P>,
    verifier: crate::core::AuthorityVerifier,
    signed: crate::core::Scope,
    context: &crate::runtime::TaskContext,
    source: RecursiveSourceSnapshot,
    stream_provider: crate::resources::ProviderRef,
    operation_id: OperationId,
    arguments: &ForkToolArguments,
    config: RecursiveForkConfig,
    source_read: Option<crate::source_authorization::ProjectSourceReadHandle>,
    resolver: Arc<dyn crate::conversation::ContentResidencyVerifier>,
    cancellation: &acyclic_fs::CancellationToken,
) -> Result<RetainedSourceBinding<A, O>>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
    let caller = context.durable_task_id().ok_or_else(|| Error::Unauthorized("fork has no admitted caller".into()))?;
    if caller != source.task || !context.scope().grants().contains("fork:publish") {
        return Err(Error::Unauthorized("fork caller/source authority differs".into()));
    }
    verifier.verify(&signed, &mut None)?;
    let agent = parent.reducer().conversation().and_then(|conversation| conversation.agent)
        .ok_or_else(|| Error::Invalid("fork caller conversation has no agent".into()))?;
    if signed.agent() != Some(agent) || source.private_volume.owner() != &crate::conversation::VolumeOwner::Agent(agent) {
        return Err(Error::Unauthorized("fork source is not the actual current caller agent".into()));
    }
    for volume in [&source.project, &source.private_volume] {
        let read = volume.capability(VolumeOperation::Read, &mut None)?;
        if !context.scope().grants().contains(&read) {
            return Err(Error::Unauthorized("caller task lacks selected source read authority".into()));
        }
        crate::conversation::ContentGrant::verify(&verifier, &signed, volume, VolumeOperation::Read, u64::MAX, &mut None)?;
    }
    config.limits.validate(&mut None)?;
    let selected = arguments.source_task(caller, config.limits, &mut None)?;
    let limits = config.limits;
    let history_limits = config.history_limits;
    let mut request = fork_request(parent.reducer(), operation_id, arguments,
        (caller, &source.project, &source.project_generation, &source.private_volume, &source.private_generation),
        config, &stream_provider, None, None)?;
    if selected != caller {
        let source_read = source_read.as_ref().ok_or_else(|| Error::Unauthorized(
            "foreign workspace selection requires an operation-sealed RootService approval".into()))?;
        let authorization = source_read.authorization();
        if authorization.caller_task != caller {
            return Err(Error::Unauthorized("selected source approval belongs to another caller".into()));
        }
        source_read.require_selection(operation_id, selected, &authorization.project,
            &authorization.generation, authorization.native_snapshot.as_ref(), None)?;
        arguments.select_project(caller, selected, authorization.project.clone(),
            authorization.generation.clone(), limits, &mut request)?;
        request.authorized_source = Some(authorization.clone());
    } else if source_read.is_some() {
        return Err(Error::Invalid("current-source fork must not substitute a foreign-source token".into()));
    }
    cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
    let history = crate::store::HistoryReader::new(stream, parent.reducer().authority(), &verifier, &mut None)?
        .fork_history(parent.reducer(), request.preparation.inherited_through_sequence, history_limits, &mut None).await?;
    cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
    let preparer = FilesystemForkPreparer::new(filesystem, parent.reducer().copy(&mut None)?, verifier, signed,
        source.project, stream_provider, resolver,
        Some(history), source_read, #[cfg(not(target_arch = "wasm32"))] None,
    )?;
    Ok(RetainedSourceBinding { preparer: Arc::new(preparer), request })
}


/// Rebinds one supported original request to its exact authenticated parent checkpoint.
#[allow(clippy::too_many_arguments, reason = "original owner/provider/source identities stay explicit")]
pub async fn restore_recursive_fork_source<P, A, O>(
    filesystem: Arc<crate::filesystem::FilesystemHost<A, O>>,
    stream: &Arc<P>,
    parent: &crate::core::Reducer,
    verifier: crate::core::AuthorityVerifier,
    signed: crate::core::Scope,
    caller_project: VolumeRef,
    stream_provider: crate::resources::ProviderRef,
    request: ForkRequest,
    history_limits: crate::store::HistoryReadLimits,
    source_read: Option<crate::source_authorization::ProjectSourceReadHandle>,
    resolver: Arc<dyn crate::conversation::ContentResidencyVerifier>,
    cancellation: &acyclic_fs::CancellationToken,
) -> Result<RetainedSourceBinding<A, O>>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
    request.validate(&mut None)?;
    if request.parent != *parent.authority() || request.parent_revision != parent.revision() {
        return Err(Error::Conflict("restored fork source is not the original admitted parent cut".into()));
    }
    verifier.verify(&signed, &mut None)?;
    verifier.verify_audience(parent.authority(), &mut None)?;
    if request.authorized_source.as_ref() != source_read.as_ref().map(|handle| handle.authorization()) {
        return Err(Error::Unauthorized("restored source differs from the exact admitted workspace approval".into()));
    }
    cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
    let history = crate::store::HistoryReader::new(stream, parent.authority(), &(verifier), &mut None)?
        .fork_history(parent, request.preparation.inherited_through_sequence, history_limits, &mut None).await?;
    cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
    let preparer = FilesystemForkPreparer::new(filesystem, parent.copy(&mut None)?, verifier, signed,
        caller_project, stream_provider, resolver,
        Some(history), source_read, #[cfg(not(target_arch = "wasm32"))] None,
    )?;
    Ok(RetainedSourceBinding { preparer: Arc::new(preparer), request })
}


pub(in crate::filesystem) fn fork_request(
    parent: &crate::core::Reducer, operation: OperationId, arguments: &ForkToolArguments,
    (caller, project, generation, private, private_generation): (TaskId, &VolumeRef, &GenerationRef, &VolumeRef, &GenerationRef),
    config: RecursiveForkConfig, stream_provider: &crate::resources::ProviderRef,
    authorized_source: Option<crate::source_authorization::ProjectSourceAuthorization>,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<ForkRequest> {
    let selected = arguments.source.unwrap_or(caller);
    let (authority, path, project, private) = crate::executor::with_history_io(io, |work| {
        let authority = match work.as_deref_mut() { Some(work) => work.clone_authority(parent.authority())?, None => parent.authority().clone() };
        Ok::<_, Error>((authority, parent.authority().stream_path(work)?.into_bytes(), project.copy(work)?, private.copy(work)?))
    })?;
    let provider = match io { Some(io) => stream_provider.clone_with_io(io)?, None => stream_provider.clone() };
    let version = crate::contract::preparation_format(format_args!("{}", parent.revision()), io)?;
    let reference = crate::executor::with_history_io(io, |work| {
        let resource = crate::resources::ResourceRef::new(crate::resources::ResourceKind::Stream, provider, path, Some(version), work)?;
        crate::resources::StreamRef::from_resource_with_work(resource, work)
    })?;
    let generation = match io { Some(io) => generation.clone_with_io(io)?, None => generation.clone() };
    let private_generation = match io { Some(io) => private_generation.clone_with_io(io)?, None => private_generation.clone() };
    let mut selections = crate::model::vec_with_capacity(3, io)?;
    selections.push(crate::fork::ForkSelection { required: true, revision: ResourceRevision::History(reference) });
    selections.push(crate::fork::ForkSelection { required: true, revision: ResourceRevision::Project { volume: project, generation } });
    selections.push(crate::fork::ForkSelection { required: true, revision: ResourceRevision::PrivateVolume {
        volume: private, generation: private_generation, paths: Vec::new() } });
    crate::executor::with_history_io(io, |work| ForkRequest {
        operation_id: operation, parent: authority, parent_revision: parent.revision(), child: config.child,
        child_agent: arguments.id, attached_agents: Vec::new(),
        preparation: crate::fork::ForkPreparation {
            child_project_volume: config.child_project, child_private_volume: config.child_private,
            child_result_volume: config.child_result_volume, summary: None, inherited_through_sequence: 0,
            maximum_inherited_messages: config.inherited_messages, maximum_inherited_bytes: config.inherited_bytes,
            maximum_inherited_references: config.inherited_references,
        }, selections, boundary: None, authorized_source,
    }.with_history_policy(parent, if selected == caller { crate::fork::ForkHistoryPolicy::default() }
        else { crate::fork::ForkHistoryPolicy::Fresh }, work))
}
