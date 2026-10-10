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
use serde_json::{Value, json};
use std::sync::Arc;

/// Portable fork arguments. `id` is the NEW child agent, never its source or
/// the operation that prepares/admit it. Messages are retained by tool admission.
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
    /// This selects project data, not another agent's private transcript.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "wasm", tsify(type = "string | undefined"))]
    pub source: Option<TaskId>,
}

impl ForkToolArguments {
    /// Resolves the default without performing IO or granting source authority.
    pub fn source_task(&self, caller: TaskId, limits: Limits) -> Result<TaskId> {
        limits.validate()?;
        if self.msg.len() as u64 > limits.file_bytes {
            return Err(Error::Invalid("fork message exceeds admitted file bytes".into()));
        }
        Ok(self.source.unwrap_or(caller))
    }

    /// Selects project data without granting read authority. Foreign workspace
    /// selection requires an explicitly empty conversation; caller-owned empty
    /// scratch and the original history source identity are never substituted.
    pub fn select_project(
        &self,
        caller: TaskId,
        selected_task: TaskId,
        project: VolumeRef,
        generation: GenerationRef,
        limits: Limits,
        request: &mut ForkRequest,
    ) -> Result<()> {
        request.validate()?;
        let revision = ResourceRevision::Project { volume: project, generation };
        revision.validate()?;
        if let ResourceRevision::Project { volume, .. } = &revision
            && (volume == &request.preparation.child_project_volume
                || volume.provider() != request.preparation.child_project_volume.provider())
        {
            return Err(Error::Invalid("selected source does not match child provider/allocation".into()));
        }
        if self.source_task(caller, limits)? != selected_task || request.child_agent != self.id {
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

/// Actual selected-provider binding and canonical immutable request. No worker
/// starts here. Consumers admit `ForkPreparationMachine` under their original
/// parent and use its sealed token through `with_fork_preparer`.
pub struct RetainedSourceBinding<A, O> {
    /// Exact current caller snapshot; never a root binding reused for descendants.
    pub preparer: Arc<FilesystemForkPreparer<A, O>>,
    /// Original request to retain before allocation/publication.
    pub request: ForkRequest,
}

/// Constructs the portable current-caller fork from actual providers and
/// authenticated history. Foreign selection replaces only the project source;
/// it does not grant or inject another agent's transcript or scratch.
#[allow(clippy::too_many_arguments, reason = "explicit provider, owner, source and finite-bound contracts")]
pub async fn recursive_fork_source<P, A, O>(
    runtime: &super::FilesystemTaskRuntime<P, A, O>,
    filesystem: Arc<super::FilesystemHost<A, O>>,
    stream: &acyclic_stream::StreamClient<P>,
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
    verifier.verify(&signed)?;
    let agent = parent.reducer().conversation().and_then(|conversation| conversation.agent)
        .ok_or_else(|| Error::Invalid("fork caller conversation has no agent".into()))?;
    if signed.agent() != Some(agent) || source.private_volume.owner() != &crate::conversation::VolumeOwner::Agent(agent) {
        return Err(Error::Unauthorized("fork source is not the actual current caller agent".into()));
    }
    for volume in [&source.project, &source.private_volume] {
        let read = volume.capability(VolumeOperation::Read)?;
        if !context.scope().grants().contains(&read) {
            return Err(Error::Unauthorized("caller task lacks selected source read authority".into()));
        }
        crate::conversation::ContentGrant::verify(&verifier, &signed, volume, VolumeOperation::Read)?;
    }
    config.limits.validate()?;
    let selected = arguments.source_task(caller, config.limits)?;
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
                    parent.reducer().authority().stream_path()?.into_bytes(),
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
    })?;
    if selected != caller {
        let source_read = source_read.as_ref().ok_or_else(|| Error::Unauthorized(
            "foreign workspace selection requires an operation-sealed RootService approval".into()))?;
        let authorization = source_read.authorization();
        if authorization.caller_task != caller {
            return Err(Error::Unauthorized("selected source approval belongs to another caller".into()));
        }
        source_read.require_selection(operation_id, selected, &authorization.project,
            &authorization.generation, authorization.native_snapshot.as_ref())?;
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
    let resolver = Arc::new(super::FilesystemContentVerifier::new(
        filesystem.clone(), verifier.clone(), signed.clone(), config.limits.file_bytes,
    )?);
    let mut preparer = FilesystemForkPreparer::new(filesystem, parent.reducer().clone(), verifier, signed,
        source.project, stream_provider, resolver)?.with_history_context(history)?;
    if let Some(source_read) = source_read {
        preparer = preparer.with_project_source_read_handle(source_read)?;
    }
    Ok(RetainedSourceBinding { preparer: Arc::new(preparer), request })
}

/// Rebinds one supported original request to its exact authenticated parent
/// checkpoint. Later conversation/configuration revisions are never substituted
/// during retry. The existing snapshot loader must authenticate `parent`.
#[allow(clippy::too_many_arguments, reason = "original owner/provider/source identities stay explicit")]
pub async fn restore_recursive_fork_source<P, A, O>(
    filesystem: Arc<super::FilesystemHost<A, O>>,
    stream: &acyclic_stream::StreamClient<P>,
    parent: &crate::core::Reducer,
    verifier: crate::core::AuthorityVerifier,
    signed: crate::core::Scope,
    caller_project: VolumeRef,
    stream_provider: crate::resources::ProviderRef,
    request: ForkRequest,
    history_limits: crate::store::HistoryReadLimits,
    source_read: Option<crate::source_authorization::ProjectSourceReadHandle>,
    cancellation: &acyclic_fs::CancellationToken,
) -> Result<RetainedSourceBinding<A, O>>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
    request.validate()?;
    if request.parent != *parent.authority() || request.parent_revision != parent.revision() {
        return Err(Error::Conflict("restored fork source is not the original admitted parent cut".into()));
    }
    verifier.verify(&signed)?;
    verifier.verify_audience(parent.authority())?;
    if request.authorized_source.as_ref() != source_read.as_ref().map(|handle| handle.authorization()) {
        return Err(Error::Unauthorized("restored source differs from the exact admitted workspace approval".into()));
    }
    cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
    let history = crate::store::HistoryReader::new(stream, parent.authority(), verifier.clone())?
        .fork_history(parent, request.preparation.inherited_through_sequence, history_limits).await?;
    cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
    let resolver = Arc::new(super::FilesystemContentVerifier::new(
        filesystem.clone(), verifier.clone(), signed.clone(),
        request.preparation.maximum_inherited_bytes,
    )?);
    let mut preparer = FilesystemForkPreparer::new(filesystem, parent.clone(), verifier, signed,
        caller_project, stream_provider, resolver)?.with_history_context(history)?;
    if let Some(source_read) = source_read {
        preparer = preparer.with_project_source_read_handle(source_read)?;
    }
    Ok(RetainedSourceBinding { preparer: Arc::new(preparer), request })
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

/// Builds the real creator request consumed by the ordinary preparation task.
/// Requested created-volume grants are not authority until the existing owner
/// verifies its provider records and admits this exact child.
pub fn fork_preparation_request<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    preparer: &FilesystemForkPreparer<A, O>,
    fork: ForkRequest,
    preparation_task: TaskId,
    mut child: TaskAdmissionRecord,
    command: WorkflowCommand,
    limits: ForkPreparationLimits,
    source_checkpoint: Option<crate::conversation::FileRef>,
    input_files: Vec<crate::conversation::FileRef>,
) -> Result<ResourcePreparationRequest> {
    fork.validate()?;
    command.validate()?;
    if command.kind != FORK_PREPARE_TASK_COMMAND_KIND
        || fork.operation_id != command.operation_id
        || command.operation_id == child.operation_id
        || command.operation_id.into_bytes() == preparation_task.into_bytes()
        || child.operation_id.into_bytes() == preparation_task.into_bytes()
    {
        return Err(Error::Invalid("fork source, preparation, child and effect identities must remain distinct".into()));
    }
    let mut grants = child.grants.iter().map(str::to_owned).collect::<Vec<_>>();
    for volume in [&fork.preparation.child_project_volume, &fork.preparation.child_private_volume] {
        grants.push(volume.capability(VolumeOperation::Read)?);
        grants.push(volume.capability(VolumeOperation::Write)?);
    }
    if let Some(volume) = &fork.preparation.child_result_volume {
        grants.push(volume.capability(VolumeOperation::Read)?);
        grants.push(volume.capability(VolumeOperation::Write)?);
    }
    for file in &input_files {
        file.validate()?;
        grants.push(file.read_capability()?);
    }
    child.grants = Capabilities::new(grants);
    child = child.with_resource_proofs(preparer.creation_proofs(&fork, preparation_task)?)?;
    let request = ResourcePreparationRequest {
        provider: fork.preparation.child_project_volume.provider().clone(),
        resources: preparer.creation_resources(&fork)?,
        child,
        creation_grant: "fork:publish".into(),
        maximum_resources: limits.resources,
        maximum_work: limits.work,
        maximum_bytes: limits.bytes,
        read_authorization: fork.authorized_source.clone(),
        payload: serde_json::to_value(fork).map_err(|error| Error::Invalid(error.to_string()))?,
        command: Some(command),
        source_checkpoint,
        input_files,
    };
    request.validate()?;
    Ok(request)
}

/// One ordinary task: exact preparation input -> ref-only command -> retained
/// report. The surrounding parent explicitly publishes and admits the child;
/// construction never starts a worker or performs an automatic merge/writeback.
pub struct ForkPreparationMachine {
    identity: MachineIdentity,
    schema: Value,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
enum State {
    Ready { command: Box<WorkflowCommand>, request_digest: [u8; 32] },
    Waiting { operation_id: OperationId, request_digest: [u8; 32] },
    Settled,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandResults { commands: Vec<CommandResult> }
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandResult { operation_id: OperationId, value: Outcome<ForkReport> }

impl ForkPreparationMachine {
    /// Registers mechanism only, with no provider or authority side effect.
    #[must_use]
    pub fn new() -> Self {
        let implementation = include_str!("recursive.rs").replace("\r\n", "\n");
        Self {
            identity: MachineIdentity { name: "acyclic.fork_preparation".into(), version: "1".into(),
                digest: *blake3::hash(implementation.as_bytes()).as_bytes() },
            schema: json!({"oneOf":[
                {"type":"object","required":["phase","command","request_digest"],"properties":{
                    "phase":{"const":"ready"},"command":{"type":"object"},
                    "request_digest":{"type":"array","minItems":32,"maxItems":32,"items":{"type":"integer","minimum":0,"maximum":255}}},"additionalProperties":false},
                {"type":"object","required":["phase","operation_id","request_digest"],"properties":{
                    "phase":{"const":"waiting"},"operation_id":{"type":"string"},
                    "request_digest":{"type":"array","minItems":32,"maxItems":32,"items":{"type":"integer","minimum":0,"maximum":255}}},"additionalProperties":false},
                {"type":"object","required":["phase"],"properties":{
                    "phase":{"const":"settled"}},"additionalProperties":false}
            ]}),
        }
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
        let input = json!({
            "type":"object",
            "required":["provider","child","resources","creation_grant","maximum_resources","maximum_work","maximum_bytes","payload","command"],
            "properties":{
                "provider":{"type":"object"},"child":{"type":"object"},
                "resources":{"type":"array","minItems":1,"items":{"type":"object"}},
                "creation_grant":{"const":"fork:publish"},
                "maximum_resources":{"type":"integer","minimum":1,"maximum":u32::MAX},
                "maximum_work":{"type":"integer","minimum":1,"maximum":u64::MAX - 1},
                "maximum_bytes":{"type":"integer","minimum":1,"maximum":u64::MAX - 1},
                "payload":{"type":"object"},
                "source_checkpoint":{"type":["object","null"]},
                "input_files":{"type":"array","items":{"type":"object"}},
                "read_authorization":{"type":["object","null"]},
                "command":{"type":"object","required":["operation_id","kind","payload"],
                    "properties":{"operation_id":{"type":"string","minLength":1},
                        "kind":{"const":FORK_PREPARE_TASK_COMMAND_KIND},"payload":{"type":"object"}},
                    "additionalProperties":false}
            },"additionalProperties":false
        });
        let output = json!({
            "type":"object",
            "required":["request","captures","child_private_volume","child_private_generation","inherited_context","summary",
                "inherited_through_sequence","shared_grants","reference_grants","history_witness","attachment_manifests"],
            "properties":{
                "request":{"type":"object"},"captures":{"type":"array"},
                "child_private_volume":{"type":"object"},"child_private_generation":{"type":"object"},
                "child_result_volume":{"type":["object","null"]},
                "child_result_generation":{"type":["object","null"]},
                "inherited_context":{"type":"array"},"summary":{"type":["object","null"]},
                "inherited_through_sequence":{"type":"integer","minimum":0},
                "shared_grants":{"type":"array"},"reference_grants":{"type":"array"},
                "history_witness":{"type":["array","null"]},"attachment_manifests":{"type":"array"}
            },"additionalProperties":false
        });
        TaskDefinition::resumable(self.clone(), input, output)?.requires("host")
    }
}

impl Default for ForkPreparationMachine { fn default() -> Self { Self::new() } }
impl ResumableMachine for ForkPreparationMachine {
    fn identity(&self) -> &MachineIdentity { &self.identity }
    fn state_schema(&self) -> &Value { &self.schema }
    fn initialize(&self, input: &Value) -> Result<Value> {
        let request: ResourcePreparationRequest = serde_json::from_value(input.clone())
            .map_err(|error| Error::Invalid(format!("invalid fork preparation input: {error}")))?;
        request.validate()?;
        let command = request.command.ok_or_else(|| Error::Invalid("fork preparation has no admitted command".into()))?;
        if command.kind != FORK_PREPARE_TASK_COMMAND_KIND {
            return Err(Error::Invalid("fork preparation command kind differs".into()));
        }
        let request_digest = crate::contract::canonical_json_digest(&request.payload)?;
        serde_json::to_value(State::Ready { command: Box::new(command), request_digest })
            .map_err(|error| Error::Invalid(error.to_string()))
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        let state: State = serde_json::from_value(state.clone())
            .map_err(|error| Error::Invalid(format!("invalid fork preparation state: {error}")))?;
        let (state, commands, status) = match state {
            State::Ready { command, request_digest } => {
                command.validate()?;
                if !input.is_null() || command.kind != FORK_PREPARE_TASK_COMMAND_KIND {
                    return Err(Error::Invalid("fork preparation initial trigger changed".into()));
                }
                (State::Waiting { operation_id: command.operation_id, request_digest }, vec![*command], MachineStatus::Suspended)
            }
            State::Waiting { operation_id, request_digest } => {
                let results: CommandResults = serde_json::from_value(input.clone())
                    .map_err(|error| Error::Invalid(format!("invalid fork preparation result: {error}")))?;
                let mut results = results.commands.into_iter();
                let result = results.next().ok_or_else(|| Error::Invalid("fork preparation result missing".into()))?;
                if results.next().is_some() || result.operation_id != operation_id {
                    return Err(Error::Unauthorized("fork preparation result belongs to another command".into()));
                }
                let status = match result.value {
                    Outcome::Succeeded(report) => {
                        report.validate()?;
                        if report.request.operation_id != operation_id
                            || crate::contract::canonical_json_digest(&report.request)? != request_digest
                        {
                            return Err(Error::Unauthorized("fork report changed the exact admitted source/request".into()));
                        }
                        MachineStatus::Completed { value: serde_json::to_value(report)
                            .map_err(|error| Error::Invalid(error.to_string()))? }
                    }
                    Outcome::Failed { message } => MachineStatus::Failed { message },
                    Outcome::Cancelled => MachineStatus::Failed { message: "fork preparation cancelled; allocation may remain uncertain".into() },
                    Outcome::Indeterminate { .. } => return Err(Error::Conflict("uncertain preparation cannot settle as a completed report".into())),
                };
                (State::Settled, Vec::new(), status)
            }
            State::Settled => return Err(Error::Conflict("fork preparation is already settled".into())),
        };
        Ok(MachineTransition { state: serde_json::to_value(state).map_err(|error| Error::Invalid(error.to_string()))?, commands, status })
    }
}
