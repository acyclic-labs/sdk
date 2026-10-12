//! Portable recursive resource preparation over ordinary task/workflow journals.

use super::{FilesystemForkPreparer, FORK_PREPARE_TASK_COMMAND_KIND};
use crate::{AgentId, Capabilities, Error, OperationId, Outcome, Result, TaskId,
    conversation::{Limits, VolumeOperation, VolumeRef},
    fork::{ForkReport, ForkRequest, ResourceRevision},
    resource_preparation::ResourcePreparationRequest,
    resources::GenerationRef,
    runtime::{TaskAdmissionRecord, TaskDefinition},
    workflow::{MachineIdentity, MachineStatus, MachineTransition, ResumableMachine, WorkflowCommand},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// Portable fork arguments.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ForkToolArguments {
    /// Chosen new child agent identity.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub id: AgentId,
    /// Exact initial child prompt.
    pub msg: String,
    /// Explicit workspace source task; absence selects the current calling task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "wasm", tsify(type = "string | undefined"))]
    pub source: Option<TaskId>,
}

impl ForkToolArguments {
    /// Resolves the default without performing IO or granting source authority.
    pub fn source_task(
        &self, caller: TaskId, limits: Limits,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<TaskId> {
        limits.validate(work)?;
        if let Some(observer) = work.as_deref_mut() {
            observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        }
        if self.msg.len() as u64 > limits.file_bytes {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(|observer| observer.original_native_work().is_some()) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "fork message exceeds admitted file bytes" });
            }
            return Err(Error::Invalid("fork message exceeds admitted file bytes".into()));
        }
        Ok(self.source.unwrap_or(caller))
    }

    /// Selects project data without granting read authority.
    pub fn select_project(
        &self,
        caller: TaskId,
        selected_task: TaskId,
        project: VolumeRef,
        generation: GenerationRef,
        limits: Limits,
        request: &mut ForkRequest,
    ) -> Result<()> {
        request.validate(&mut None)?;
        let revision = ResourceRevision::Project { volume: project, generation };
        revision.validate(&mut None)?;
        if let ResourceRevision::Project { volume, .. } = &revision
            && (volume == &request.preparation.child_project_volume
                || volume.provider() != request.preparation.child_project_volume.provider())
        {
            return Err(Error::Invalid("selected source does not match child provider/allocation".into()));
        }
        if self.source_task(caller, limits, &mut None)? != selected_task || request.child_agent != self.id {
            return Err(Error::Unauthorized("fork source or new child identity differs from selection".into()));
        }
        if selected_task != caller
            && (request.preparation.inherited_through_sequence != 0 || request.preparation.summary.is_some())
        {
            return Err(Error::Unsupported("foreign workspace source cannot inherit conversation history".into()));
        }
        let mut selected = request.selections.iter_mut().filter(|selection| {
            matches!(selection.revision, ResourceRevision::Project { .. })
        });
        let project_selection = selected.next().ok_or_else(|| Error::Invalid("fork has no project selection".into()))?;
        if selected.next().is_some() {
            return Err(Error::Invalid("fork repeats project selection".into()));
        }
        project_selection.revision = revision;
        Ok(())
    }
}

/// Pinned caller-owned workspace selection. References alone confer no rights.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecursiveSourceSnapshot {
    /// Actual current caller, independent of its agent and effect identities.
    pub task: TaskId,
    /// Caller-controlled project and exact selected generation.
    pub project: VolumeRef,
    /// Exact generation, never inferred from a native source or lazy mount.
    pub project_generation: GenerationRef,
    /// Caller scratch; children start empty but retain this real lineage.
    pub private_volume: VolumeRef,
    /// Exact scratch generation selected before allocation.
    pub private_generation: GenerationRef,
}

/// Explicit child allocation choices and inherited-history bounds.
#[derive(Clone, Debug)]
pub struct RecursiveForkConfig {
    /// New child conversation identity.
    pub child: crate::core::Authority,
    /// New isolated child project identity.
    pub child_project: VolumeRef,
    /// New child-owned scratch identity.
    pub child_private: VolumeRef,
    /// Optional separate child-owned unmounted output namespace.
    pub child_result_volume: Option<VolumeRef>,
    /// Admitted caller input/file bounds, separate from retained history count.
    pub limits: Limits,
    /// Maximum logical messages inherited, independently of model projections.
    pub inherited_messages: u64,
    /// Maximum child-owned inherited file and reference descriptor bytes.
    pub inherited_bytes: u64,
    /// Maximum immutable references retained by this capture.
    pub inherited_references: u32,
    /// Finite authenticated archive work and bytes.
    pub history_limits: crate::store::HistoryReadLimits,
}

/// Actual selected-provider binding and canonical immutable request.
pub struct RetainedSourceBinding<A, O> {
    /// Exact current caller snapshot; never a root binding reused for descendants.
    pub preparer: Arc<FilesystemForkPreparer<A, O>>,
    /// Original request to retain before allocation/publication.
    pub request: ForkRequest,
}

/// Constructs the portable current-caller fork from actual providers and authenticated history.
#[allow(clippy::too_many_arguments, reason = "explicit provider, owner, source and finite-bound contracts")]
pub async fn recursive_fork_source<P, A, O>(
    _runtime: &super::FilesystemTaskRuntime<P, A, O>,
    filesystem: Arc<super::FilesystemHost<A, O>>,
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
    let mut request = ForkRequest {
        operation_id, parent: parent.reducer().authority().clone(),
        parent_revision: parent.reducer().revision(), child: config.child, child_agent: arguments.id,
        attached_agents: Vec::new(),
        preparation: crate::fork::ForkPreparation {
            child_project_volume: config.child_project, child_private_volume: config.child_private,
            child_result_volume: config.child_result_volume,
            summary: None, inherited_through_sequence: 0,
            maximum_inherited_messages: config.inherited_messages,
            maximum_inherited_bytes: config.inherited_bytes,
            maximum_inherited_references: config.inherited_references,
        },
        selections: vec![
            crate::fork::ForkSelection { required: true, revision: ResourceRevision::History(
                crate::resources::StreamRef::new(stream_provider.clone(),
                    parent.reducer().authority().stream_path(&mut None)?.into_bytes(),
                    Some(parent.reducer().revision().to_string()))?) },
            crate::fork::ForkSelection { required: true, revision: ResourceRevision::Project {
                volume: source.project.clone(), generation: source.project_generation } },
            crate::fork::ForkSelection { required: true, revision: ResourceRevision::PrivateVolume {
                volume: source.private_volume, generation: source.private_generation, paths: Vec::new() } },
        ],
        boundary: None,
        authorized_source: None,
    }.with_history_policy(parent.reducer(), if selected == caller {
        crate::fork::ForkHistoryPolicy::default()
    } else {
        crate::fork::ForkHistoryPolicy::Fresh
    }, &mut None)?;
    if selected != caller {
        let source_read = source_read.as_ref().ok_or_else(|| Error::Unauthorized(
            "foreign workspace selection requires an operation-sealed RootService approval".into()))?;
        let authorization = source_read.authorization();
        if authorization.caller_task != caller {
            return Err(Error::Unauthorized("selected source approval belongs to another caller".into()));
        }
        source_read.require_selection(operation_id, selected, &authorization.project, &authorization.generation, authorization.native_snapshot.as_ref(), None)?;
        arguments.select_project(caller, selected, authorization.project.clone(),
            authorization.generation.clone(), config.limits, &mut request)?;
        request.authorized_source = Some(authorization.clone());
    } else if source_read.is_some() {
        return Err(Error::Invalid("current-source fork must not substitute a foreign-source token".into()));
    }
    cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
    let history = crate::store::HistoryReader::new(stream, parent.reducer().authority(), verifier.clone())?
        .fork_history(parent.reducer(), request.preparation.inherited_through_sequence, config.history_limits).await?;
    cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
    let mut preparer = FilesystemForkPreparer::new(filesystem, parent.reducer().clone_with_work(&mut None)?, verifier, signed,
        source.project, stream_provider, resolver)?.with_history_context(history)?;
    if let Some(source_read) = source_read {
        preparer = preparer.with_project_source_read_handle(source_read)?;
    }
    Ok(RetainedSourceBinding { preparer: Arc::new(preparer), request })
}
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct RecursiveForkSourceView<'a> {
    pub caller: TaskId,
    pub scope: &'a crate::core::Scope,
    pub project: &'a VolumeRef,
    pub project_generation: &'a GenerationRef,
    pub private_volume: &'a VolumeRef,
    pub private_generation: &'a GenerationRef,
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct RecursiveForkAllocationView<'a> {
    pub child: &'a crate::core::Authority,
    pub project: &'a VolumeRef,
    pub private_volume: &'a VolumeRef,
    pub result_volume: Option<&'a VolumeRef>,
    pub limits: Limits,
    pub inherited_messages: u64,
    pub inherited_bytes: u64,
    pub inherited_references: u32,
}

/// The shared native field factory for installed configured defaults and the public ordinary recursive source constructor.
#[cfg(not(target_arch = "wasm32"))]
#[allow(clippy::too_many_arguments, reason = "original source and child role identities stay explicit")]
pub(crate) fn recursive_fork_request_with_native_work(
    parent: &crate::core::Reducer, source: RecursiveForkSourceView<'_>,
    allocation: RecursiveForkAllocationView<'_>, stream_provider: &crate::resources::ProviderRef,
    operation: OperationId, arguments: &ForkToolArguments,
    source_read: Option<&crate::source_authorization::ProjectSourceReadHandle>,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(ForkRequest, acyclic_fs::NativeAllocation)> {
    use std::fmt::Write as _;
    let io = super::PreparationIo::from_native_work(work)?;
    let mut history = io.history_io();
    let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
    let selected = arguments.source_task(source.caller, allocation.limits, &mut observer)?;
    let caller_agent = parent.conversation().and_then(|conversation| conversation.agent);
    if caller_agent.is_none() || caller_agent != source.scope.agent()
        || caller_agent != match source.private_volume.owner() {
            crate::conversation::VolumeOwner::Agent(agent) => Some(*agent), _ => None,
        }
    {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
            reason: "native fork source is not the admitted current caller agent" });
    }
    let parent_authority = observer.as_deref_mut().expect("native fork observer").clone_authority(parent.authority())?;
    let child = observer.as_deref_mut().expect("native fork observer").clone_authority(allocation.child)?;
    let path = parent.authority().stream_path(&mut observer)?.into_bytes();
    let mut version = String::new();
    observer.as_deref_mut().expect("native fork observer").reserve_text(&mut version, 20)?;
    observer.as_deref_mut().expect("native fork observer").admit_copy(20)?;
    write!(&mut version, "{}", parent.revision()).map_err(|_| Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid, reason: "native revision formatting failed",
    })?;
    observer.as_deref_mut().expect("native fork observer").copy_bytes(version.len() as u64)?;
    drop(observer);
    drop(history);
    let provider = crate::resources::ProviderRef::new(
        crate::filesystem::fork_preparer_io::clone_text(stream_provider.namespace(), Some(&io))?, crate::filesystem::fork_preparer_io::clone_text(stream_provider.family(), Some(&io))?,
        crate::filesystem::fork_preparer_io::clone_text(stream_provider.version(), Some(&io))?,
    )?;
    let mut history = io.history_io();
    let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
    let reference = crate::resources::ResourceRef::new_with_work(
        crate::resources::ResourceKind::Stream, provider, path, Some(version), &mut observer,
    )?;
    let reference = crate::resources::StreamRef::from_resource_with_work(reference, &mut observer)?;
    let child_project = allocation.project.copy(&mut observer)?;
    let child_private = allocation.private_volume.copy(&mut observer)?;
    let child_result = allocation.result_volume.map(|volume| volume.copy(&mut observer)).transpose()?;
    let source_project = source_read.map_or(source.project, |handle| &handle.authorization().project)
        .copy(&mut observer)?;
    let scratch = source.private_volume.copy(&mut observer)?;
    let mut selections = Vec::new();
    let slots = 3 * std::mem::size_of::<crate::fork::ForkSelection>() as u64;
    observer.as_deref_mut().expect("native fork observer").admit_owned(slots)?;
    observer.as_deref_mut().expect("native fork observer").admit_allocation(slots)?;
    selections.try_reserve_exact(3).map_err(|cause|
        observer.as_deref().expect("native fork observer").allocation_failure(cause))?;
    observer.as_deref_mut().expect("native fork observer")
        .retain_owned(selections.capacity() as u64 * std::mem::size_of::<crate::fork::ForkSelection>() as u64)?;
    drop(observer);
    drop(history);
    let generation = source_read.map_or(source.project_generation, |handle| &handle.authorization().generation);
    let generation = GenerationRef::new_with_native_work(
        generation.as_resource().provider(), generation.as_resource().key(), generation.as_resource().version(), work)?;
    io.absorb_native_custody(generation.1)?;
    let private_generation = GenerationRef::new_with_native_work(
        source.private_generation.as_resource().provider(), source.private_generation.as_resource().key(),
        source.private_generation.as_resource().version(), work)?;
    io.absorb_native_custody(private_generation.1)?;
    selections.push(crate::fork::ForkSelection { required: true, revision: ResourceRevision::History(reference) });
    selections.push(crate::fork::ForkSelection { required: true,
        revision: ResourceRevision::Project { volume: source_project, generation: generation.0 } });
    selections.push(crate::fork::ForkSelection { required: true,
        revision: ResourceRevision::PrivateVolume { volume: scratch, generation: private_generation.0, paths: Vec::new() } });
    let mut history = io.history_io();
    let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
    let authorized_source = match source_read {
        Some(handle) => {
            let authorization = handle.authorization();
            if authorization.caller_task != source.caller {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native source approval belongs to another caller" });
            }
            handle.require_selection(operation, selected, &authorization.project, &authorization.generation, authorization.native_snapshot.as_ref(), Some(work))?;
            Some(authorization.clone_with_work_and_custody(&mut observer)?.0)
        }
        None if selected != source.caller => return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, reason: "foreign fork has no operation-sealed Project reader",
        }),
        None => None,
    };
    if selected == source.caller && source_read.is_some() {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "current fork cannot substitute a foreign reader" });
    }
    let request = ForkRequest {
        operation_id: operation, parent: parent_authority, parent_revision: parent.revision(),
        child, child_agent: arguments.id, attached_agents: Vec::new(),
        preparation: crate::fork::ForkPreparation {
            child_project_volume: child_project, child_private_volume: child_private,
            child_result_volume: child_result, summary: None, inherited_through_sequence: 0,
            maximum_inherited_messages: allocation.inherited_messages,
            maximum_inherited_bytes: allocation.inherited_bytes,
            maximum_inherited_references: allocation.inherited_references,
        }, selections, boundary: None, authorized_source,
    }.with_history_policy(parent, if selected == source.caller {
        crate::fork::ForkHistoryPolicy::default()
    } else { crate::fork::ForkHistoryPolicy::Fresh }, &mut observer)?;
    drop(observer);
    drop(history);
    let retained = io.owned_bytes()?;
    Ok((request, io.into_native_custody(retained)?))
}

/// Opens a native ordinary recursive fork at the actual caller's authenticated cut.
#[cfg(not(target_arch = "wasm32"))]
#[allow(clippy::too_many_arguments, reason = "original provider, source and authority identities stay explicit")]
pub async fn recursive_fork_source_with_native_work<P, A, O>(
    filesystem: Arc<super::FilesystemHost<A, O>>, stream: &Arc<P>,
    parent: crate::core::Reducer, verifier: &crate::core::AuthorityVerifier,
    signed: &crate::core::Scope, context: &crate::runtime::TaskContext,
    source: &RecursiveSourceSnapshot, stream_provider: &crate::resources::ProviderRef,
    operation: OperationId, arguments: &ForkToolArguments, config: &RecursiveForkConfig,
    source_read: Option<&crate::source_authorization::ProjectSourceReadHandle>,
    resolver: Arc<dyn crate::conversation::ContentResidencyVerifier>,
    cancellation: &acyclic_fs::CancellationToken, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(RetainedSourceBinding<A, O>, acyclic_fs::NativeAllocation)>
where P: acyclic_stream::StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    work.remaining()?;
    cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
    if context.durable_task_id() != Some(source.task) || !context.scope().grants().contains("fork:publish") {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
            reason: "native fork caller/source authority differs" });
    }
    if context.scope().current_created_authority_with_native_work(work)?
        .is_some_and(|authority| authority != parent.authority())
        || context.scope().current_created_private_volume_with_native_work(work)?
            .is_some_and(|volume| volume != &source.private_volume)
    {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
            reason: "native descendant source differs from its original created caller" });
    }
    let io = super::PreparationIo::from_native_work(work)?;
    {
        let mut history = io.history_io();
        verifier.verify_scope_for_with_work(parent.authority(), signed, &mut Some(&mut history))?;
    }
    for volume in [&source.project, &source.private_volume] {
        if !io.contains_capability(context.scope().grants(), volume, "volume:read:")? {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native caller lacks selected source read authority" });
        }
        crate::conversation::ContentGrant::authorize_volume(verifier, signed, volume, VolumeOperation::Read, (&io).remaining()?.bytes_encoded, &mut Some(&mut (&io).history_io()))?;
    }
    let request = recursive_fork_request_with_native_work(&parent, RecursiveForkSourceView {
        caller: source.task, scope: signed, project: &source.project,
        project_generation: &source.project_generation, private_volume: &source.private_volume,
        private_generation: &source.private_generation,
    }, RecursiveForkAllocationView { child: &config.child, project: &config.child_project,
        private_volume: &config.child_private, result_volume: config.child_result_volume.as_ref(),
        limits: config.limits, inherited_messages: config.inherited_messages,
        inherited_bytes: config.inherited_bytes, inherited_references: config.inherited_references,
    }, stream_provider, operation, arguments, source_read, work)?;
    restore_recursive_fork_source_with_native_work(filesystem, stream, parent, verifier, signed,
        &source.project, stream_provider, request, config.history_limits, source_read,
        resolver, cancellation, work).await
}

/// Rebinds one supported original request to its exact authenticated parent checkpoint.
#[allow(clippy::too_many_arguments, reason = "original owner/provider/source identities stay explicit")]
pub async fn restore_recursive_fork_source<P, A, O>(
    filesystem: Arc<super::FilesystemHost<A, O>>,
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
    let history = crate::store::HistoryReader::new(stream, parent.authority(), verifier.clone())?
        .fork_history(parent, request.preparation.inherited_through_sequence, history_limits).await?;
    cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
    let mut preparer = FilesystemForkPreparer::new(filesystem, parent.clone_with_work(&mut None)?, verifier, signed,
        caller_project, stream_provider, resolver)?.with_history_context(history)?;
    if let Some(source_read) = source_read {
        preparer = preparer.with_project_source_read_handle(source_read)?;
    }
    Ok(RetainedSourceBinding { preparer: Arc::new(preparer), request })
}

/// Rebinds the immutable request to the original native authenticated cut.
#[cfg(not(target_arch = "wasm32"))]
#[allow(clippy::too_many_arguments, reason = "original providers and transferred custody stay explicit")]
pub async fn restore_recursive_fork_source_with_native_work<P, A, O>(
    filesystem: Arc<super::FilesystemHost<A, O>>,
    stream: &Arc<P>,
    parent: crate::core::Reducer,
    verifier: &crate::core::AuthorityVerifier,
    signed: &crate::core::Scope,
    caller_project: &VolumeRef,
    stream_provider: &crate::resources::ProviderRef,
    request: (ForkRequest, acyclic_fs::NativeAllocation),
    history_limits: crate::store::HistoryReadLimits,
    source_read: Option<&crate::source_authorization::ProjectSourceReadHandle>,
    resolver: Arc<dyn crate::conversation::ContentResidencyVerifier>,
    cancellation: &acyclic_fs::CancellationToken,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(RetainedSourceBinding<A, O>, acyclic_fs::NativeAllocation)>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
    let io = super::PreparationIo::from_native_work(work)?;
    {
        let mut history = io.history_io();
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
        request.0.validate(&mut observer)?;
        if &request.0.parent != parent.authority() || request.0.parent_revision != parent.revision() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "restored source differs from original admitted parent cut" });
        }
        verifier.verify_scope_for_with_work(parent.authority(), signed, &mut observer)?;
        if request.0.authorized_source.as_ref() != source_read.map(|handle| handle.authorization()) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "restored source differs from immutable operation approval" });
        }
    }
    let reader = crate::store::HistoryReader::new_with_native_work(stream, parent.authority(), verifier, work)?;
    let retained_history = {
        let mut history = io.history_io();
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
        reader.0.fork_history_with_work(
            &parent, request.0.preparation.inherited_through_sequence, history_limits, &mut observer,
        ).await?
    };
    drop(reader);
    cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
    let (owned_verifier, owned_scope, project) = {
        let mut history = io.history_io();
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
        (verifier.copy(&mut observer)?, signed.copy(&mut observer)?,
            caller_project.copy(&mut observer)?)
    };
    let provider = stream_provider.clone_with_io(&io)?;
    let selected_read = match source_read {
        Some(handle) => {
            let mut history = io.history_io();
            let authorization = handle.authorization().clone_with_work_and_custody(
                &mut Some(&mut history))?.0;
            drop(history);
            let mut history = io.history_io();
            let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
            Some(crate::source_authorization::ProjectSourceReadHandle::seal_with_work(
                authorization, verifier, &mut observer,
            )?)
        }
        None => None,
    };
    let retained = io.owned_bytes()?;
    let preparer = FilesystemForkPreparer::new_with_native_work(
        filesystem, parent, owned_verifier, owned_scope, project, provider, resolver,
        retained_history, selected_read, io.into_native_custody(retained)?, work,
    )?.into_native_shared(work)?;
    Ok((RetainedSourceBinding { preparer, request: request.0 }, request.1))
}

/// Explicit finite preparation allowances, charged by the existing provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkPreparationLimits {
    /// Maximum created resource identities.
    pub resources: u32,
    /// Canonical provider work allowance.
    pub work: u64,
    /// Complete canonical request and provider IO allowance.
    pub bytes: u64,
}

/// Builds the creator request with the caller's optional accounting observer.
pub fn fork_preparation_request<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    preparer: &FilesystemForkPreparer<A, O>, fork: &ForkRequest,
    preparation_task: TaskId, mut child: TaskAdmissionRecord, command: WorkflowCommand,
    limits: ForkPreparationLimits, source_checkpoint: Option<crate::conversation::FileRef>,
    input_files: Vec<crate::conversation::FileRef>, io: Option<&super::PreparationIo>,
) -> Result<ResourcePreparationRequest> {
    use super::default_tools::{encode, recursive_observe, recursive_reject};
    recursive_observe(io, |work| {
        fork.validate(work)?;
        command.validate(work)
    })?;
    if command.kind != FORK_PREPARE_TASK_COMMAND_KIND
        || fork.operation_id != command.operation_id || command.operation_id == child.operation_id
        || command.operation_id.into_bytes() == preparation_task.into_bytes()
        || child.operation_id.into_bytes() == preparation_task.into_bytes()
    {
        return Err(recursive_reject(io, crate::wire::ErrorCode::Invalid,
            "fork source, preparation, child and effect identities must remain distinct"));
    }
    let volumes = [&fork.preparation.child_project_volume, &fork.preparation.child_private_volume]
        .into_iter().chain(fork.preparation.child_result_volume.as_ref());
    if let Some(io) = io {
        for volume in volumes {
            for prefix in ["volume:read:", "volume:write:"] {
                if !io.contains_capability(&child.grants, volume, prefix)? {
                    return Err(recursive_reject(Some(io), crate::wire::ErrorCode::Unauthorized,
                        "native child claims lack an exact authenticated created-volume operation"));
                }
            }
        }
        for file in &input_files {
            if !io.contains_capability(&child.grants, file, "file:read:")? {
                return Err(recursive_reject(Some(io), crate::wire::ErrorCode::Unauthorized,
                    "native child claims lack an exact authenticated input-file read"));
            }
        }
    } else {
        let mut grants = child.grants.iter().map(str::to_owned).collect::<Vec<_>>();
        for volume in volumes {
            grants.push(volume.capability(VolumeOperation::Read, &mut None)?);
            grants.push(volume.capability(VolumeOperation::Write, &mut None)?);
        }
        for file in &input_files {
            file.validate()?;
            grants.push(file.read_capability()?);
        }
        child.grants = Capabilities::new(grants);
    }
    let resources = preparer.creation_resources(fork, io)?;
    child.resource_proofs = preparer.creation_proofs(fork, preparation_task, io)?;
    recursive_observe(io, |work| child.validate_with_work(work))?;
    let provider = match io {
        Some(io) => fork.preparation.child_project_volume.provider().clone_with_io(io)?,
        None => fork.preparation.child_project_volume.provider().clone(),
    };
    let read_authorization = fork.authorized_source.as_ref().map(|authorization|
        recursive_observe(io, |work| authorization.clone_with_work_and_custody(work))
            .map(|(authorization, _)| authorization)).transpose()?;
    let request = ResourcePreparationRequest {
        provider, child, resources,
        creation_grant: super::fork_preparer_io::clone_text("fork:publish", io)?,
        maximum_resources: limits.resources, maximum_work: limits.work, maximum_bytes: limits.bytes,
        read_authorization, payload: encode(fork, io)?,
        command: Some(command), source_checkpoint, input_files,
    };
    recursive_observe(io, |work| request.validate(work))?;
    Ok(request)
}

/// One ordinary task: exact preparation input -> ref-only command -> retained report.
pub struct ForkPreparationMachine {
    identity: MachineIdentity,
    schema: Value,
    #[cfg(not(target_arch = "wasm32"))]
    _native_custody: Option<acyclic_fs::NativeAllocation>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
enum State {
    Ready { command: Box<WorkflowCommand>, request_digest: [u8; 32] },
    Waiting { operation_id: OperationId, request_digest: [u8; 32] },
    Settled,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandResults { commands: Vec<CommandResult> }
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandResult { operation_id: OperationId, value: Outcome<ForkReport> }

impl super::PreparationRecord for State {
    fn maximum_slot_bytes() -> u64 {
        super::default_tools::recursive_decode_slot_bytes::<Self>() + std::mem::size_of::<CommandResult>() as u64
    }
}
impl super::PreparationRecord for CommandResults {
    fn maximum_slot_bytes() -> u64 {
        super::default_tools::recursive_decode_slot_bytes::<Self>() + std::mem::size_of::<CommandResult>() as u64
    }
    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::ForkPreparationCommandResults
    }
}

const PREPARATION_STATE_SCHEMA: &[u8] = br#"{"oneOf":[{"type":"object","required":["phase","command","request_digest"],"properties":{"phase":{"const":"ready"},"command":{"type":"object"},"request_digest":{"type":"array","minItems":32,"maxItems":32,"items":{"type":"integer","minimum":0,"maximum":255}}},"additionalProperties":false},{"type":"object","required":["phase","operation_id","request_digest"],"properties":{"phase":{"const":"waiting"},"operation_id":{"type":"string"},"request_digest":{"type":"array","minItems":32,"maxItems":32,"items":{"type":"integer","minimum":0,"maximum":255}}},"additionalProperties":false},{"type":"object","required":["phase"],"properties":{"phase":{"const":"settled"}},"additionalProperties":false}]}"#;
const PREPARATION_INPUT_SCHEMA: &[u8] = br#"{"type":"object","required":["provider","child","resources","creation_grant","maximum_resources","maximum_work","maximum_bytes","payload","command"],"properties":{"provider":{"type":"object"},"child":{"type":"object"},"resources":{"type":"array","minItems":1,"items":{"type":"object"}},"creation_grant":{"const":"fork:publish"},"maximum_resources":{"type":"integer","minimum":1,"maximum":4294967295},"maximum_work":{"type":"integer","minimum":1,"maximum":18446744073709551614},"maximum_bytes":{"type":"integer","minimum":1,"maximum":18446744073709551614},"payload":{"type":"object"},"source_checkpoint":{"type":["object","null"]},"input_files":{"type":"array","items":{"type":"object"}},"read_authorization":{"type":["object","null"]},"command":{"type":"object","required":["operation_id","kind","payload"],"properties":{"operation_id":{"type":"string","minLength":1},"kind":{"const":"acyclic.fork.prepare.v1"},"payload":{"type":"object"}},"additionalProperties":false}},"additionalProperties":false}"#;
const PREPARATION_OUTPUT_SCHEMA: &[u8] = br#"{"type":"object","required":["request","captures","child_private_volume","child_private_generation","inherited_context","summary","inherited_through_sequence","shared_grants","reference_grants","history_witness","attachment_manifests"],"properties":{"request":{"type":"object"},"captures":{"type":"array"},"child_private_volume":{"type":"object"},"child_private_generation":{"type":"object"},"child_result_volume":{"type":["object","null"]},"child_result_generation":{"type":["object","null"]},"inherited_context":{"type":"array"},"summary":{"type":["object","null"]},"inherited_through_sequence":{"type":"integer","minimum":0},"shared_grants":{"type":"array"},"reference_grants":{"type":"array"},"history_witness":{"type":["array","null"]},"attachment_manifests":{"type":"array"}},"additionalProperties":false}"#;

fn preparation_implementation_digest(io: Option<&super::PreparationIo>) -> Result<[u8; 32]> {
    let mut digest = blake3::Hasher::new();
    for line in include_str!("recursive.rs").split_inclusive('\n') {
        let normalized = line.strip_suffix("\r\n");
        let bytes = normalized.unwrap_or(line).as_bytes();
        if let Some(io) = io {
            io.charge(acyclic_fs::WorkCounters {
                bytes_hashed: bytes.len() as u64 + u64::from(normalized.is_some()),
                ..Default::default()
            })?;
        }
        digest.update(bytes);
        if normalized.is_some() { digest.update(b"\n"); }
    }
    Ok(*digest.finalize().as_bytes())
}

impl ForkPreparationMachine {
    /// Registers mechanism only, with no provider or authority side effect.
    #[must_use]
    pub fn new() -> Self {
        Self {
            identity: MachineIdentity { name: "acyclic.fork_preparation".into(), version: "1".into(),
                digest: preparation_implementation_digest(None).expect("static preparation implementation") },
            schema: serde_json::from_slice(PREPARATION_STATE_SCHEMA).expect("static preparation state schema"),
            #[cfg(not(target_arch = "wasm32"))]
            _native_custody: None,
        }
    }

    /// Constructs the genuine machine on the original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_with_native_work(work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        let io = super::PreparationIo::from_native_work(work)?;
        let mut metadata;
        let mut custody;
        let bytes = std::mem::size_of::<Self>() as u64;
        io.retain_owned(bytes)?;
        io.allocation_attempt(bytes)?;
        let identity = MachineIdentity {
            name: crate::filesystem::fork_preparer_io::clone_text("acyclic.fork_preparation", Some(&io))?,
            version: crate::filesystem::fork_preparer_io::clone_text("1", Some(&io))?,
            digest: preparation_implementation_digest(Some(&io))?,
        };
        let schema;
        (schema, custody) = io.decode_preparation::<Value>(PREPARATION_STATE_SCHEMA, crate::wire::ErrorCode::Storage)?.into_native_parts()?;
        metadata = io.take_native_custody(io.owned_bytes()?)?;
        custody.absorb(&mut metadata).map_err(Error::from)?;
        Ok(Arc::new(Self { identity, schema, _native_custody: Some(custody) }))
    }

    /// The exact request is ordinary task input, not an out-of-band authority bag.
    pub fn definition(self: &Arc<Self>) -> Result<TaskDefinition<ResourcePreparationRequest, ForkReport>> {
        self.registered_definition()
    }

    #[cfg(target_arch = "wasm32")]
    pub(crate) fn wire_definition(self: &Arc<Self>) -> Result<TaskDefinition<Value, Value>> {
        self.registered_definition()
    }

    fn registered_definition<I, O>(self: &Arc<Self>) -> Result<TaskDefinition<I, O>> {
        let input = serde_json::from_slice(PREPARATION_INPUT_SCHEMA).expect("static preparation input schema");
        let output = serde_json::from_slice(PREPARATION_OUTPUT_SCHEMA).expect("static preparation output schema");
        TaskDefinition::resumable(self.clone(), input, output)?.requires("host")
    }
}

impl Default for ForkPreparationMachine { fn default() -> Self { Self::new() } }
impl ResumableMachine for ForkPreparationMachine {
    fn identity(&self) -> &MachineIdentity { &self.identity }
    fn state_schema(&self) -> &Value { &self.schema }
    fn initialize(&self, input: &Value) -> Result<Value> {
        self.initialize_engine(input, None)
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        self.transition_engine(state, input, None)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_work(&self) -> bool { true }
    #[cfg(not(target_arch = "wasm32"))]
    fn identity_with_native_work<'a>(
        &'a self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<&'a MachineIdentity> {
        work.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })
            .map_err(Error::from)?;
        Ok(&self.identity)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn initialize_with_native_work(
        &self, input: &Value, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        let io = super::PreparationIo::from_native_work(work)?;
        let state = self.initialize_engine(input, Some(&io))?;
        let custody = io.take_native_custody(
            super::default_tools::recursive_value_owned_bytes(&state)?,
        )?;
        Ok((state, custody))
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn transition_with_native_work(
        &self, state: &Value, input: Value, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        let io = super::PreparationIo::from_native_work(work)?;
        let transition = self.transition_engine(state, &input, Some(&io))?;
        drop(input);
        let custody = io.take_native_custody(
            super::default_tools::recursive_transition_owned_bytes(&transition, &io)?,
        )?;
        Ok((transition, custody))
    }
}

impl ForkPreparationMachine {
    fn initialize_engine(&self, input: &Value, io: Option<&super::PreparationIo>) -> Result<Value> {
        use super::default_tools::{recursive_box, decode, encode, recursive_observe, recursive_reject};
        let request: ResourcePreparationRequest = decode(input, io)?;
        recursive_observe(io, |work| request.validate(work))?;
        let command = request.command.ok_or_else(|| recursive_reject(
            io, crate::wire::ErrorCode::Invalid, "fork preparation has no admitted command"))?;
        if command.kind != FORK_PREPARE_TASK_COMMAND_KIND {
            return Err(recursive_reject(io, crate::wire::ErrorCode::Invalid, "fork preparation command kind differs"));
        }
        let request_digest = preparation_payload_digest(&request.payload, io)?;
        encode(&State::Ready { command: recursive_box(command, io)?, request_digest }, io)
    }

    fn transition_engine(&self, state: &Value, input: &Value, io: Option<&super::PreparationIo>) -> Result<MachineTransition> {
        use super::default_tools::{recursive_command_outbox, decode, encode, recursive_observe, recursive_reject};
        let state: State = decode(state, io)?;
        let (state, commands, status) = match state {
            State::Ready { command, request_digest } => {
                recursive_observe(io, |work| command.validate(work))?;
                if !input.is_null() || command.kind != FORK_PREPARE_TASK_COMMAND_KIND {
                    return Err(recursive_reject(io, crate::wire::ErrorCode::Invalid, "fork preparation initial trigger changed"));
                }
                (State::Waiting { operation_id: command.operation_id, request_digest },
                    recursive_command_outbox(*command, io)?, MachineStatus::Suspended)
            }
            State::Waiting { operation_id, request_digest } => {
                let results: CommandResults = decode(input, io)?;
                let mut results = results.commands.into_iter();
                let result = results.next().ok_or_else(|| recursive_reject(
                    io, crate::wire::ErrorCode::Invalid, "fork preparation result missing"))?;
                if results.next().is_some() || result.operation_id != operation_id {
                    return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "fork preparation result belongs to another command"));
                }
                let status = match result.value {
                    Outcome::Succeeded(report) => {
                        recursive_observe(io, |work| report.validate(work))?;
                        if report.request.operation_id != operation_id
                            || preparation_payload_digest(&report.request, io)? != request_digest
                        {
                            return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "fork report changed the exact admitted source/request"));
                        }
                        MachineStatus::Completed { value: encode(&report, io)? }
                    }
                    Outcome::Failed { message } => MachineStatus::Failed { message },
                    Outcome::Cancelled => {
                        let message = "fork preparation cancelled; allocation may remain uncertain";
                        MachineStatus::Failed { message: match io {
                            Some(io) => crate::filesystem::fork_preparer_io::clone_text(message, Some(&io))?,
                            None => message.into(),
                        }}
                    }
                    Outcome::Indeterminate { .. } => return Err(recursive_reject(
                        io, crate::wire::ErrorCode::Conflict, "uncertain preparation cannot settle as a completed report")),
                };
                (State::Settled, Vec::new(), status)
            }
            State::Settled => return Err(recursive_reject(io, crate::wire::ErrorCode::Conflict, "fork preparation is already settled")),
        };
        Ok(MachineTransition { state: encode(&state, io)?, commands, status })
    }
}

fn preparation_payload_digest<T: Serialize>(value: &T, io: Option<&super::PreparationIo>) -> Result<[u8; 32]> {
    let Some(io) = io else { return crate::contract::canonical_json_digest(value); };
    crate::contract::canonical_json_digest_with_work(
        value, io.remaining()?.bytes_encoded, &mut Some(&mut io.history_io()))
}
