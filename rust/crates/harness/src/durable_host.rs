//! Owner-bound Stream coordinator adapter for typed durable task admission.

use crate::{
    Admission, BatchId, EffectId, Error, IdempotencyKey, InteractionId, OperationId, Outcome,
    Result, TaskId,
    conversation::{ContentResidencyVerifier, FileRef},
    core::{Authority, AuthorityVerifier, Scope},
    distributed::{ChildOperationPageRequest, DistributedCoordinator, SchedulerPayloadStore},
    durable_tool::DurableToolRunner,
    executor::ExecutionJournal,
    interaction::{Interaction, InteractionOutcome},
    registry::ComponentIdentity,
    runtime::{
        BatchCancellationReport, BatchCancellationStatus, DurableBatchRequest,
        DurableEffectObserver, DurableTaskHost, InputKey, MAX_BATCH_INPUTS, RuntimeScope,
        TaskAdmissionRecord, TaskChild, TaskChildrenPage, TaskRegistry, ToolPolicy,
        check_tool_approval, read_granted, require_descendant_grant, validate_policy_identity,
        validate_task_schemas,
    },
    scheduler::{
        DurableOwner, EntrypointRef, InboxItem, OperationSpec, Orchestration, ParentLink,
        ResourceRequest,
    },
    tool::ToolDefinition,
    workflow::MachineRegistry,
};
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamKey, IdempotencyOutcome, StreamClient, StreamError,
    StreamProvider, UnixMillisClock,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MailEvent {
    sender: TaskId,
    message_id: OperationId,
    payload: FileRef,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimerEvent {
    task_id: TaskId,
    operation_id: OperationId,
    deadline_unix_ms: u64,
}

#[derive(Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchCancellation {
    batch_id: BatchId,
    group_id: crate::GroupId,
}

fn task_interaction_id(task_id: TaskId, operation_id: OperationId) -> InteractionId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness/v2/task-interaction\0");
    hasher.update(&task_id.into_bytes());
    hasher.update(&operation_id.into_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    InteractionId::from_bytes(bytes)
}

/// Stream-backed admission and observation for one durable owner. More
/// specialized wait, mail, interaction, and effect providers can be composed
/// around this boundary without giving callers a coordinator handle.
pub struct CoordinatorTaskHost<P> {
    coordinator: Mutex<DistributedCoordinator<P>>,
    stream: StreamClient<P>,
    payloads: Arc<dyn SchedulerPayloadStore>,
    reader: Arc<dyn ContentResidencyVerifier>,
    owner: Authority,
    owner_scope: Scope,
    verifier: AuthorityVerifier,
    root_scope: RuntimeScope,
    tasks: TaskRegistry,
    machines: MachineRegistry,
    clock: Arc<dyn UnixMillisClock>,
    tools: Option<Arc<DurableToolRunner>>,
    policy: Option<Arc<dyn ToolPolicy>>,
    interactions: Option<Arc<dyn ExecutionJournal>>,
    effects: Option<Arc<dyn DurableEffectObserver>>,
    session_limits: Option<crate::scheduler::SessionLimits>,
}

/// One worker's exact task lease, bound to the coordinator's own Stream provider.
/// Journal constructors use this binding to atomically fence their publications.
#[cfg(feature = "filesystem")]
pub struct TaskJournalOwner<P> {
    host: Arc<CoordinatorTaskHost<P>>,
    stream: StreamClient<P>,
    task_id: TaskId,
    fence: crate::scheduler::LeaseFence,
    workflow: crate::workflow::WorkflowAdmission,
    maximum_payload_bytes: u64,
    output_schema: Value,
    input_grants: crate::Capabilities,
    input_limits: crate::conversation::Limits,
}

#[cfg(feature = "filesystem")]
impl<P: StreamProvider> TaskJournalOwner<P> {
    pub(crate) fn validate_input_file(&self, file: &FileRef) -> Result<()> {
        self.input_limits.validate_file(file)?;
        if !read_granted(&self.input_grants, file)? {
            return Err(Error::Unauthorized(
                "task cannot read the execution input file".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn task_binding(&self) -> (TaskId, crate::scheduler::LeaseFence) {
        (self.task_id, self.fence.clone())
    }

    pub(crate) fn validate_storage(
        &self,
        verifier: &AuthorityVerifier,
        maximum_payload_bytes: u64,
    ) -> Result<()> {
        if verifier.audience() != &self.host.owner
            || maximum_payload_bytes > self.maximum_payload_bytes
        {
            return Err(Error::Unauthorized(
                "journal storage exceeds admitted task authority or limits".into(),
            ));
        }
        Ok(())
    }

    /// Returns the initial workflow identity derived from the retained task
    /// admission and its registered machine, rather than caller-selected state.
    #[must_use]
    pub const fn workflow_admission(&self) -> &crate::workflow::WorkflowAdmission {
        &self.workflow
    }

    pub(crate) fn validate_workflow_record(
        &self,
        record: &crate::workflow::WorkflowRecord,
    ) -> Result<()> {
        if record.prior.machine != self.workflow.initial.machine {
            return Err(Error::Conflict(
                "workflow implementation differs from task admission".into(),
            ));
        }
        let (next, transition) = self.host.machines.step(&record.prior, &record.input)?;
        if next != record.next || transition != record.transition {
            return Err(Error::Conflict(
                "workflow transition differs from pinned machine".into(),
            ));
        }
        if let crate::workflow::MachineStatus::Completed { value } = &transition.status {
            crate::tool::validate_value(&self.output_schema, value, "task output")?;
        }
        Ok(())
    }

    pub(crate) fn stream(&self) -> StreamClient<P> {
        self.stream.clone()
    }

    pub(crate) fn operation_id(&self) -> OperationId {
        OperationId::from_bytes(self.task_id.into_bytes())
    }

    async fn condition(
        &self,
        write: crate::distributed::JournalWrite,
    ) -> Result<acyclic_stream::CommitCondition> {
        self.host
            .coordinator
            .lock()
            .await
            .journal_condition(
                &self.host.owner,
                &self.host.owner_scope,
                &self.host.verifier,
                self.operation_id(),
                &self.fence,
                write,
            )
            .await
    }

    pub(crate) async fn verify(&self, settlement: bool) -> Result<()> {
        use crate::distributed::JournalWrite;
        self.condition(if settlement {
            JournalWrite::Settlement
        } else {
            JournalWrite::Fresh
        })
        .await
        .map(|_| ())
    }

    fn execution_write(
        operation: OperationId,
        event: &crate::executor::ExecutionEvent,
    ) -> crate::distributed::JournalWrite {
        use crate::{distributed::JournalWrite, executor::ExecutionEvent};
        match event {
            ExecutionEvent::ModelStarted {
                step,
                request_digest,
            } => JournalWrite::Model {
                attempt_id: operation,
                step: *step,
                request_digest: *request_digest,
            },
            ExecutionEvent::Started { .. } | ExecutionEvent::ToolStarted { .. } => {
                JournalWrite::Fresh
            }
            ExecutionEvent::Model { .. }
            | ExecutionEvent::ToolCompleted { .. }
            | ExecutionEvent::ToolFailed { .. } => JournalWrite::Settlement,
        }
    }

    pub(crate) async fn verify_execution(
        &self,
        operation: OperationId,
        event: &crate::executor::ExecutionEvent,
    ) -> Result<()> {
        self.condition(Self::execution_write(operation, event))
            .await
            .map(|_| ())
    }

    pub(crate) async fn verify_model_history(
        &self,
        operation: OperationId,
        records: &[crate::executor::ExecutionRecord],
    ) -> Result<()> {
        if !records.iter().any(|record| {
            matches!(
                record.event,
                crate::executor::ExecutionEvent::ModelStarted { .. }
            )
        }) {
            return Ok(());
        }
        let mut coordinator = self.host.coordinator.lock().await;
        coordinator.refresh().await?;
        coordinator.observe_operation(
            &self.host.owner,
            &self.host.owner_scope,
            &self.host.verifier,
            self.operation_id(),
        )?;
        for record in records {
            if let crate::executor::ExecutionEvent::ModelStarted {
                step,
                request_digest,
            } = &record.event
            {
                coordinator.scheduler().require_model_claim(
                    self.operation_id(),
                    operation,
                    *step,
                    request_digest,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) async fn append_execution(
        &self,
        operation: OperationId,
        expected_tail: u64,
        key: &StreamKey,
        bytes: Bytes,
        event: &crate::executor::ExecutionEvent,
    ) -> Result<bool> {
        let path = acyclic_stream::StreamPath::new(format!("harness/v2/execution/{operation}"))?;
        self.append(
            path,
            expected_tail,
            key,
            bytes,
            Self::execution_write(operation, event),
        )
        .await
    }

    pub(crate) async fn append_workflow(
        &self,
        admission: bool,
        expected_tail: u64,
        key: &StreamKey,
        bytes: Bytes,
    ) -> Result<bool> {
        let class = if admission {
            "workflow-admissions"
        } else {
            "workflows"
        };
        let path =
            acyclic_stream::StreamPath::new(format!("harness/v2/{class}/{}", self.operation_id()))?;
        self.append(
            path,
            expected_tail,
            key,
            bytes,
            crate::distributed::JournalWrite::Fresh,
        )
        .await
    }

    async fn prepare_append(
        &self,
        path: &acyclic_stream::StreamPath,
        expected_tail: u64,
        key: &StreamKey,
        bytes: &Bytes,
        write: crate::distributed::JournalWrite,
    ) -> Result<acyclic_stream::CommitRequest> {
        use acyclic_stream::{CommitCondition, CommitMutation};
        let owner = self.condition(write).await?;
        let target = match self.stream.bounds(path.as_str()).await {
            Ok(_) => CommitCondition::Tail {
                path: path.clone(),
                expected: expected_tail,
            },
            Err(StreamError::NotFound) if expected_tail == 0 => {
                CommitCondition::Absent { path: path.clone() }
            }
            Err(error) => return Err(error.into()),
        };
        // Conflicting transactions are retained by Stream too. A refreshed owner
        // revision gets its own recovery key; the journal's CAS and retry index
        // still prevent duplicate logical records.
        let mut hash = blake3::Hasher::new();
        hash.update(b"harness/v2/owned-journal\0");
        hash.update(key.as_bytes());
        hash.update(&self.task_id.into_bytes());
        hash.update(&crate::contract::canonical_json_bytes(&self.fence)?);
        let CommitCondition::Tail { expected, .. } = &owner else {
            return Err(Error::Invalid("coordinator tail condition required".into()));
        };
        hash.update(&expected.to_le_bytes());
        hash.update(path.as_str().as_bytes());
        hash.update(&expected_tail.to_le_bytes());
        hash.update(&[u8::from(matches!(target, CommitCondition::Absent { .. }))]);
        hash.update(blake3::hash(bytes).as_bytes());
        let transaction_key = StreamKey::new(Bytes::copy_from_slice(hash.finalize().as_bytes()))?;
        Ok(acyclic_stream::CommitRequest {
            conditions: vec![owner, target],
            mutations: vec![CommitMutation::Append {
                path: path.clone(),
                records: vec![bytes.clone()],
            }],
            idempotency_key: transaction_key,
        })
    }

    async fn append(
        &self,
        path: acyclic_stream::StreamPath,
        expected_tail: u64,
        key: &StreamKey,
        bytes: Bytes,
        write: crate::distributed::JournalWrite,
    ) -> Result<bool> {
        use acyclic_stream::{CommitOutcome, CommittedMutation};
        let request = self
            .prepare_append(&path, expected_tail, key, &bytes, write)
            .await?;
        let transaction_key = request.idempotency_key.clone();
        let outcome = match self.stream.commit(request).await {
            Ok(outcome) => outcome,
            Err(StreamError::Unavailable) => {
                match self.stream.inspect_idempotency(transaction_key).await {
                    Ok(Some(observation)) => match observation.outcome {
                        IdempotencyOutcome::Commit(outcome) => outcome,
                        _ => {
                            return Err(Error::Conflict("journal recovery identity reused".into()));
                        }
                    },
                    Ok(None) | Err(_) => return Err(Error::Indeterminate(self.operation_id())),
                }
            }
            Err(error) => return Err(error.into()),
        };
        self.condition(write).await?;
        match outcome {
            CommitOutcome::Conflict(_) => Ok(false),
            CommitOutcome::Committed(envelope) => {
                let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
                    return Err(Error::Storage("invalid owned journal envelope".into()));
                };
                if append.path != path
                    || append.start != expected_tail
                    || append.end != expected_tail + 1
                    || append.tail != append.end
                    || append.records.len() != 1
                    || append.records.first().is_none_or(|record| {
                        record.sequence != expected_tail
                            || record.value != bytes
                            || record.commit_id != envelope.commit_id
                    })
                {
                    return Err(Error::Storage("invalid owned journal append".into()));
                }
                Ok(true)
            }
        }
    }
}

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    async fn verify_owner(
        &self,
        task_id: TaskId,
        fence: &crate::scheduler::LeaseFence,
        settlement: bool,
    ) -> Result<()> {
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh().await?;
        let operation = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            OperationId::from_bytes(task_id.into_bytes()),
        )?;
        crate::scheduler::require_execution_owner(&operation, fence, settlement)
    }

    /// Binds journal publications to the authenticated task's exact current lease.
    #[cfg(feature = "filesystem")]
    pub async fn journal_owner(
        self: &Arc<Self>,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
    ) -> Result<TaskJournalOwner<P>> {
        let admission = self
            .admission(OperationId::from_bytes(task_id.into_bytes()))
            .await?;
        let machine = self
            .machines
            .resolve(&admission.machine)
            .ok_or_else(|| Error::Unsupported("pinned task machine is unavailable".into()))?;
        let workflow = crate::workflow::WorkflowAdmission {
            operation_id: admission.operation_id,
            request_digest: crate::contract::canonical_json_digest(&admission)?,
            initial: crate::workflow::MachineCheckpoint {
                machine: admission.machine.clone(),
                revision: 0,
                state: machine.initialize(&admission.input)?,
            },
        };
        self.machines.validate_checkpoint(&workflow.initial)?;
        let stream = self.coordinator.lock().await.journal_client();
        let owner = TaskJournalOwner {
            host: self.clone(),
            stream,
            task_id,
            fence,
            workflow,
            maximum_payload_bytes: admission.limits.file_bytes,
            output_schema: admission.output_schema,
            input_grants: admission.grants,
            input_limits: admission.limits,
        };
        owner.verify(true).await?;
        Ok(owner)
    }
    /// Binds a trusted owner, its exact task/machine registries, and immutable
    /// admission payload provider. The host validates definitions even when a
    /// caller bypasses the high-level typed runtime.
    #[allow(
        clippy::too_many_arguments,
        reason = "each provider boundary is explicit at construction"
    )]
    pub fn new(
        coordinator: DistributedCoordinator<P>,
        stream: StreamClient<P>,
        payloads: Arc<dyn SchedulerPayloadStore>,
        reader: Arc<dyn ContentResidencyVerifier>,
        owner: Authority,
        owner_scope: Scope,
        verifier: AuthorityVerifier,
        root_scope: RuntimeScope,
        tasks: TaskRegistry,
        machines: MachineRegistry,
        clock: Arc<dyn UnixMillisClock>,
    ) -> Result<Self> {
        verifier.verify_audience(&owner)?;
        verifier.verify(&owner_scope)?;
        for capability in ["operation:declare", "operation:observe", "operation:cancel"] {
            if !owner_scope.capabilities().contains(capability) {
                return Err(Error::Unauthorized(format!(
                    "owner scope lacks {capability}"
                )));
            }
        }
        if !root_scope.grants().is_subset_of(owner_scope.capabilities()) {
            return Err(Error::Unauthorized(
                "runtime grants exceed the signed owner scope".into(),
            ));
        }
        Ok(Self {
            coordinator: Mutex::new(coordinator),
            stream,
            payloads,
            reader,
            owner,
            owner_scope,
            verifier,
            root_scope,
            tasks,
            machines,
            clock,
            tools: None,
            policy: None,
            interactions: None,
            effects: None,
            session_limits: None,
        })
    }

    /// Requires these immutable shared ceilings for every root admitted by this
    /// host. Root admission publishes its declaration and configuration together.
    pub fn with_session_limits(mut self, limits: crate::scheduler::SessionLimits) -> Result<Self> {
        limits.validate()?;
        self.session_limits = Some(limits);
        Ok(self)
    }

    async fn declare_task(
        &self,
        spec: OperationSpec,
        key: IdempotencyKey,
    ) -> Result<crate::distributed::CoordinatorApply> {
        let mut coordinator = self.coordinator.lock().await;
        if let Some(limits) = self.session_limits.filter(|_| spec.parent.is_none()) {
            coordinator
                .declare_session(
                    &self.owner,
                    &self.owner_scope,
                    &self.verifier,
                    spec,
                    limits,
                    key,
                )
                .await
        } else {
            coordinator
                .declare_operation(&self.owner, &self.owner_scope, &self.verifier, spec, key)
                .await
        }
    }

    /// Installs a separately replaceable ref-only durable tool runner.
    #[must_use]
    pub fn with_tools(mut self, tools: Arc<DurableToolRunner>) -> Self {
        self.tools = Some(tools);
        self
    }

    /// Pins the policy enforced by this owner, independently of caller contexts.
    pub fn with_policy(mut self, policy: Arc<dyn ToolPolicy>) -> Result<Self> {
        validate_policy_identity(&policy.identity())?;
        self.policy = Some(policy);
        Ok(self)
    }

    /// Binds the owner journal used to admit and reconcile addressable interactions.
    #[must_use]
    pub fn with_interactions(mut self, journal: Arc<dyn ExecutionJournal>) -> Self {
        self.interactions = Some(journal);
        self
    }

    /// Binds the conversation-owned provider effect reconciler.
    #[must_use]
    pub fn with_effects(mut self, effects: Arc<dyn DurableEffectObserver>) -> Self {
        self.effects = Some(effects);
        self
    }

    async fn read_json(&self, reference: &FileRef) -> Result<Value> {
        reference.validate()?;
        if reference.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid("durable task payload is not JSON".into()));
        }
        let bytes = self.reader.read(reference).await?;
        reference.descriptor().verify(&bytes)?;
        let value: Value =
            serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
        if crate::contract::canonical_json_bytes(&value)? != bytes {
            return Err(Error::Invalid(
                "durable task payload is not canonical JSON".into(),
            ));
        }
        Ok(value)
    }

    async fn admission(&self, operation_id: OperationId) -> Result<TaskAdmissionRecord> {
        let state = {
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh().await?;
            let state = coordinator.observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation_id,
            )?;
            if let Some(limits) = self.session_limits
                && coordinator.scheduler().session_limits(operation_id)? != limits
            {
                return Err(Error::Conflict(
                    "retained session ceilings differ from owner bindings".into(),
                ));
            }
            state
        };
        let value = self
            .read_json(&state.spec.state)
            .await
            .map_err(|error| match error {
                Error::NotFound(_) => {
                    Error::Storage("committed task admission content is missing".into())
                }
                other => other,
            })?;
        let admission = TaskAdmissionRecord::from_canonical_value(value)?;
        if admission.policy != self.policy.as_ref().map(|policy| policy.identity()) {
            return Err(Error::Conflict(
                "durable task policy differs from its admission".into(),
            ));
        }
        if let Some(extensions) = &admission.extensions {
            extensions.validate()?;
        }
        if admission.extensions.as_ref() != self.root_scope.extensions() {
            return Err(Error::Conflict(
                "retained task extensions differ from owner bindings".into(),
            ));
        }
        self.root_scope
            .narrow(admission.grants.clone(), admission.limits)?
            .with_run_limits(admission.run_limits)?;
        if admission.operation_id != operation_id
            || admission.task.name != state.spec.entrypoint.name
            || admission.task.version != state.spec.entrypoint.version
            || admission.task.digest != state.spec.entrypoint.digest
            || admission.output_schema != state.spec.entrypoint.result_schema
            || state.spec.parent
                != admission.parent.map(|parent| ParentLink {
                    operation_id: OperationId::from_bytes(parent.into_bytes()),
                    slot: operation_id.to_string(),
                })
            || admission.machine.name != admission.task.name
            || admission.machine.version != admission.task.version
            || self.machines.resolve(&admission.machine).is_none()
        {
            return Err(Error::Conflict(
                "durable task admission disagrees with its declaration".into(),
            ));
        }
        self.tasks.validate_durable_admission(
            &admission.task,
            &admission.machine,
            Some(&admission.input_schema),
            &admission.output_schema,
            Some(&admission.input),
        )?;
        Ok(admission)
    }

    async fn validate_parent_scope(
        &self,
        parent: Option<TaskId>,
        scope: &RuntimeScope,
    ) -> Result<()> {
        if let Some(parent) = parent {
            let admitted = self
                .admission(OperationId::from_bytes(parent.into_bytes()))
                .await?;
            RuntimeScope::new(admitted.grants, admitted.limits)?
                .with_run_limits(admitted.run_limits)?
                .narrow(scope.grants().clone(), scope.limits())?
                .with_run_limits(scope.run_limits())?;
        }
        Ok(())
    }

    fn validate_local_admission_policy(&self, admission: &TaskAdmissionRecord) -> Result<()> {
        if admission.policy != self.policy.as_ref().map(|policy| policy.identity()) {
            return Err(Error::Conflict(
                "task admission policy differs from owner".into(),
            ));
        }
        if admission.execution.is_some() {
            return Err(Error::Unsupported(
                "local durable host cannot admit a remote execution route".into(),
            ));
        }
        Ok(())
    }

    fn mailbox(&self, task_id: TaskId) -> Result<acyclic_stream::Stream<P>> {
        self.stream
            .stream(format!("harness/v2/mail/{task_id}"))
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    fn batch_stream(&self, batch_id: BatchId) -> Result<acyclic_stream::Stream<P>> {
        self.stream
            .stream(format!("harness/v2/batches/{batch_id}"))
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    fn batch_cancellation_stream(&self, batch_id: BatchId) -> Result<acyclic_stream::Stream<P>> {
        self.stream
            .stream(format!("harness/v2/batch-cancellations/{batch_id}"))
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    async fn retained_batch_cancellation(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<BatchCancellation>> {
        let stream = self.batch_cancellation_stream(batch_id)?;
        let bounds = match stream.bounds().await {
            Ok(bounds) => bounds,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        if bounds.tail != 1 {
            return Err(Error::Storage(
                "batch cancellation history is not exactly retained".into(),
            ));
        }
        let records = stream.read(0, 2).await?.try_collect::<Vec<_>>().await?;
        let [record] = records.as_slice() else {
            return Err(Error::Storage(
                "batch cancellation record is missing".into(),
            ));
        };
        let declaration: BatchCancellation = serde_json::from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if declaration.batch_id != batch_id
            || crate::contract::canonical_json_bytes(&declaration)? != record.value.as_ref()
        {
            return Err(Error::Storage(
                "batch cancellation record is not canonical".into(),
            ));
        }
        Ok(Some(declaration))
    }

    async fn declare_batch_cancellation(&self, request: &DurableBatchRequest) -> Result<()> {
        let declaration = BatchCancellation {
            batch_id: request.batch_id,
            group_id: request.group_id,
        };
        let bytes = crate::contract::canonical_json_bytes(&declaration)?;
        let operation_id = OperationId::from_bytes(request.batch_id.into_bytes());
        self.publish_control(
            &self.batch_cancellation_stream(request.batch_id)?,
            "batch-cancel",
            TaskId::from_bytes(request.batch_id.into_bytes()),
            operation_id,
            &bytes,
        )
        .await?;
        if self.retained_batch_cancellation(request.batch_id).await? != Some(declaration) {
            return Err(Error::Conflict(
                "batch cancellation belongs to another group".into(),
            ));
        }
        Ok(())
    }

    async fn retained_batch(&self, batch_id: BatchId) -> Result<Option<Value>> {
        let stream = self.batch_stream(batch_id)?;
        let bounds = match stream.bounds().await {
            Ok(bounds) => bounds,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        if bounds.tail != 1 {
            return Err(Error::Storage(
                "batch manifest history is not exactly retained".into(),
            ));
        }
        let records = stream.read(0, 2).await?.try_collect::<Vec<_>>().await?;
        let [record] = records.as_slice() else {
            return Err(Error::Storage("batch manifest record is missing".into()));
        };
        let reference: FileRef = serde_json::from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if crate::contract::canonical_json_bytes(&reference)? != record.value.as_ref() {
            return Err(Error::Storage("batch manifest ref is not canonical".into()));
        }
        self.read_json(&reference)
            .await
            .map_err(|error| match error {
                Error::NotFound(_) => {
                    Error::Storage("committed batch manifest content is missing".into())
                }
                other => other,
            })
            .map(Some)
    }

    async fn commit_batch(&self, request: &DurableBatchRequest) -> Result<()> {
        let canonical = request.canonical_value();
        let bytes = crate::contract::canonical_json_bytes(&canonical)?;
        if bytes.len() as u64 > request.scope.limits().file_bytes {
            return Err(Error::Invalid("batch manifest exceeds file limit".into()));
        }
        let operation_id = OperationId::from_bytes(request.batch_id.into_bytes());
        let key = format!("batch-request:{}", request.batch_id);
        let reference = self.payloads.stage(operation_id, &key, &bytes).await?;
        reference.descriptor().verify(&bytes)?;
        if self.read_json(&reference).await? != canonical {
            return Err(Error::Storage("staged batch request changed".into()));
        }
        let published = crate::contract::canonical_json_bytes(&reference)?;
        self.publish_control(
            &self.batch_stream(request.batch_id)?,
            "batch",
            TaskId::from_bytes(request.batch_id.into_bytes()),
            operation_id,
            &published,
        )
        .await?;
        if self.retained_batch(request.batch_id).await? != Some(canonical) {
            return Err(Error::Conflict(
                "batch identity belongs to another request".into(),
            ));
        }
        Ok(())
    }

    fn validate_batch(&self, request: &DurableBatchRequest) -> Result<()> {
        request.validate()?;
        if request.inputs.len() > MAX_BATCH_INPUTS
            || request.execution.is_some()
            || request.policy != self.policy.as_ref().map(|policy| policy.identity())
            || request.machine.name != request.task.name
            || request.machine.version != request.task.version
            || request.machine.digest == [0; 32]
            || self.machines.resolve(&request.machine).is_none()
        {
            return Err(Error::Invalid(
                "batch implementation or policy is invalid".into(),
            ));
        }
        self.tasks.validate_durable_admission(
            &request.task,
            &request.machine,
            Some(&request.input_schema),
            &request.output_schema,
            None,
        )?;
        self.root_scope
            .narrow(request.scope.grants().clone(), request.scope.limits())?
            .with_run_limits(request.scope.run_limits())?;
        if request.parent.is_some() {
            require_descendant_grant(request.scope.grants(), &request.task)?;
        }
        if request.scope.extensions() != self.root_scope.extensions() {
            return Err(Error::Conflict(
                "batch extensions differ from owner bindings".into(),
            ));
        }
        validate_task_schemas(&request.input_schema, &request.output_schema, true)?;
        let input = jsonschema::validator_for(&request.input_schema)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        for value in &request.inputs {
            input.validate(value).map_err(|error| {
                Error::Invalid(format!("batch input failed validation: {error}"))
            })?;
        }
        Ok(())
    }

    fn event_key(kind: &str, task_id: TaskId, operation_id: OperationId) -> Result<StreamKey> {
        let identity = format!("harness/v2/{kind}/{task_id}/{operation_id}");
        StreamKey::new(Bytes::copy_from_slice(
            blake3::hash(identity.as_bytes()).as_bytes(),
        ))
        .map_err(|error| Error::Invalid(error.to_string()))
    }

    fn timer_stream(&self, task_id: TaskId) -> Result<acyclic_stream::Stream<P>> {
        self.stream
            .stream(format!("harness/v2/timers/{task_id}"))
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    async fn publish_control(
        &self,
        stream: &acyclic_stream::Stream<P>,
        kind: &str,
        task_id: TaskId,
        operation_id: OperationId,
        bytes: &[u8],
    ) -> Result<()> {
        let key = Self::event_key(kind, task_id, operation_id)?;
        let outcome = match stream
            .append_batch(vec![Bytes::copy_from_slice(bytes)], None, Some(key.clone()))
            .await
        {
            Ok(outcome) => outcome,
            Err(StreamError::Unavailable) => match self.stream.inspect_idempotency(key).await {
                Ok(Some(observation)) => match observation.outcome {
                    IdempotencyOutcome::Append(outcome) => outcome,
                    _ => {
                        return Err(Error::Conflict(
                            "control identity has another operation kind".into(),
                        ));
                    }
                },
                Ok(None) | Err(_) => return Err(Error::Indeterminate(operation_id)),
            },
            Err(StreamError::IdempotencyMismatch) => {
                return Err(Error::Conflict(
                    "control identity reused with different content".into(),
                ));
            }
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        match outcome {
            AppendOutcome::Committed(receipt) if receipt.end == receipt.start + 1 => {
                let records = stream
                    .read(receipt.start, 1)
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?;
                let [record] = records.as_slice() else {
                    return Err(Error::Conflict(
                        "control publication differs from its committed record".into(),
                    ));
                };
                if record.sequence != receipt.start
                    || record.commit_id != receipt.commit_id
                    || record.value.as_ref() != bytes
                {
                    return Err(Error::Conflict(
                        "control publication differs from its committed record".into(),
                    ));
                }
                Ok(())
            }
            AppendOutcome::Committed(_) => {
                Err(Error::Storage("invalid control append receipt".into()))
            }
            AppendOutcome::TailConflict { .. } => Err(Error::Conflict(
                "unconditional control append conflicted".into(),
            )),
        }
    }
}

impl<P: StreamProvider> DurableTaskHost for CoordinatorTaskHost<P> {
    fn verify_dispatch_owner<'a>(
        &'a self,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.verify_owner(task_id, &fence, false).await })
    }

    fn verify_execution_owner<'a>(
        &'a self,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.verify_owner(task_id, &fence, true).await })
    }

    fn claim_model_dispatch<'a>(
        &'a self,
        task_id: TaskId,
        attempt_id: OperationId,
        step: u32,
        request_digest: [u8; 32],
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let operation_id = OperationId::from_bytes(task_id.into_bytes());
            let admission = self.admission(operation_id).await?;
            if !admission.grants.contains("model:generate") {
                return Err(Error::Unauthorized(
                    "task scope lacks model:generate".into(),
                ));
            }
            let scope = RuntimeScope::new(admission.grants, admission.limits)?
                .with_run_limits(admission.run_limits)?;
            self.validate_parent_scope(admission.parent, &scope).await?;
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh().await?;
            let operation = coordinator.observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation_id,
            )?;
            crate::scheduler::require_model_owner(&operation, &fence)?;
            let session = coordinator.scheduler().session_limits(operation_id)?;
            let ceiling = (scope.limits().model_steps as u64)
                .min(
                    scope
                        .run_limits()
                        .max_steps
                        .map_or(u64::MAX, |value| value as u64),
                )
                .min(session.model_steps);
            let lease_digest = blake3::hash(&crate::contract::canonical_json_bytes(&fence)?);
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new(format!(
                        "model-dispatch:{attempt_id}:{step}:{lease_digest}"
                    ))?,
                    crate::scheduler::SchedulerEvent::ModelDispatchClaimed {
                        operation_id,
                        attempt_id,
                        step,
                        request_digest,
                        fence,
                        ceiling,
                    },
                )
                .await?;
            Ok(())
        })
    }

    fn policy_identity(&self) -> Option<ComponentIdentity> {
        self.policy.as_ref().map(|policy| policy.identity())
    }

    fn supports_batch_cancellation(&self) -> bool {
        true
    }

    fn observe_admission<'a>(
        &'a self,
        task_id: TaskId,
    ) -> BoxFuture<'a, Result<TaskAdmissionRecord>> {
        Box::pin(async move {
            let operation_id = OperationId::from_bytes(task_id.into_bytes());
            self.admission(operation_id).await
        })
    }

    fn children<'a>(
        &'a self,
        parent: TaskId,
        expected_revision: Option<u64>,
        after_slot: Option<String>,
        maximum: usize,
    ) -> BoxFuture<'a, Result<TaskChildrenPage>> {
        Box::pin(async move {
            let mut coordinator = self.coordinator.lock().await;
            let page = coordinator
                .observe_children(
                    &self.owner,
                    &self.owner_scope,
                    &self.verifier,
                    OperationId::from_bytes(parent.into_bytes()),
                    ChildOperationPageRequest {
                        expected_revision,
                        after_slot: after_slot.as_deref(),
                        maximum,
                    },
                )
                .await?;
            Ok(TaskChildrenPage {
                revision: page.revision,
                entries: page
                    .entries
                    .into_iter()
                    .map(|entry| TaskChild {
                        slot: entry.slot,
                        task_id: TaskId::from_bytes(entry.operation_id.into_bytes()),
                    })
                    .collect(),
                next_after: page.next_after,
            })
        })
    }

    fn reconcile_admission<'a>(
        &'a self,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<Option<(TaskId, TaskAdmissionRecord)>>> {
        Box::pin(async move {
            match self.admission(operation_id).await {
                Ok(admission) => Ok(Some((
                    TaskId::from_bytes(operation_id.into_bytes()),
                    admission,
                ))),
                Err(Error::NotFound(_)) => Ok(None),
                Err(error) => Err(error),
            }
        })
    }

    fn reconcile_batch<'a>(
        &'a self,
        request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Option<Vec<Admission<TaskId>>>>> {
        Box::pin(async move {
            self.validate_batch(&request)?;
            let Some(retained) = self.retained_batch(request.batch_id).await? else {
                return Ok(None);
            };
            if retained != request.canonical_value() {
                return Err(Error::Conflict(
                    "batch identity belongs to another request".into(),
                ));
            }
            let cancelled = self.retained_batch_cancellation(request.batch_id).await?;
            if cancelled
                .as_ref()
                .is_some_and(|declaration| declaration.group_id != request.group_id)
            {
                return Err(Error::Conflict(
                    "batch cancellation belongs to another group".into(),
                ));
            }
            let mut entries = Vec::with_capacity(request.inputs.len());
            for index in 0..request.inputs.len() {
                let operation_id = request.operation_id(index);
                let expected = request.member_admission(index)?;
                match self.admission(operation_id).await {
                    Ok(admission) if admission == expected => entries.push(Admission::Accepted(
                        TaskId::from_bytes(operation_id.into_bytes()),
                    )),
                    Ok(_) => {
                        return Err(Error::Conflict(
                            "batch member belongs to another admission".into(),
                        ));
                    }
                    Err(Error::NotFound(_)) if cancelled.is_some() => {
                        entries.push(Admission::Rejected {
                            reason: "batch cancellation preceded member admission".into(),
                        });
                    }
                    Err(Error::NotFound(_)) => {
                        entries.push(Admission::Indeterminate { operation_id });
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(Some(entries))
        })
    }

    fn load_batch<'a>(
        &'a self,
        batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<DurableBatchRequest>>> {
        Box::pin(async move {
            let Some(value) = self.retained_batch(batch_id).await? else {
                return Ok(None);
            };
            let request = DurableBatchRequest::from_canonical_value(value)?;
            if request.batch_id != batch_id {
                return Err(Error::Conflict(
                    "batch manifest has another identity".into(),
                ));
            }
            self.validate_batch(&request)?;
            Ok(Some(request))
        })
    }

    fn admit_batch<'a>(
        &'a self,
        request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Vec<Admission<TaskId>>>> {
        Box::pin(async move {
            self.validate_batch(&request)?;
            self.validate_parent_scope(request.parent, &request.scope)
                .await?;
            self.commit_batch(&request).await?;
            let observed = self
                .reconcile_batch(request.clone())
                .await?
                .ok_or_else(|| Error::Storage("committed batch disappeared".into()))?;
            let mut entries = Vec::with_capacity(observed.len());
            for (index, admission) in observed.into_iter().enumerate() {
                match admission {
                    Admission::Indeterminate { operation_id } => {
                        let member = request.member_admission(index)?;
                        if member.operation_id != operation_id {
                            return Err(Error::Conflict("batch member operation changed".into()));
                        }
                        if self
                            .retained_batch_cancellation(request.batch_id)
                            .await?
                            .is_some()
                        {
                            entries.push(Admission::Rejected {
                                reason: "batch cancellation preceded member admission".into(),
                            });
                            continue;
                        }
                        let admitted = self.admit(member).await?;
                        if self
                            .retained_batch_cancellation(request.batch_id)
                            .await?
                            .is_some()
                            && let Admission::Accepted(task_id) = &admitted
                        {
                            self.cancel(*task_id).await?;
                            entries.push(admitted);
                            continue;
                        }
                        entries.push(admitted);
                    }
                    other => entries.push(other),
                }
            }
            Ok(entries)
        })
    }

    fn cancel_batch<'a>(
        &'a self,
        batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<BatchCancellationReport>>> {
        Box::pin(async move {
            let Some(request) = self.load_batch(batch_id).await? else {
                return Ok(None);
            };
            self.declare_batch_cancellation(&request).await?;
            let admissions = self
                .reconcile_batch(request.clone())
                .await?
                .ok_or_else(|| {
                    Error::Storage("retained batch disappeared during cancellation".into())
                })?;
            let mut entries = Vec::with_capacity(admissions.len());
            for (index, admission) in admissions.into_iter().enumerate() {
                let status = match admission {
                    Admission::Accepted(task_id) => match self.cancel(task_id).await {
                        Ok(()) => BatchCancellationStatus::Requested,
                        Err(Error::Indeterminate(operation_id)) => {
                            BatchCancellationStatus::Indeterminate { operation_id }
                        }
                        Err(error) => BatchCancellationStatus::Unresolved {
                            message: error.to_string(),
                        },
                    },
                    Admission::Rejected { reason } => {
                        BatchCancellationStatus::NotAdmitted { reason }
                    }
                    Admission::Indeterminate { operation_id } => {
                        BatchCancellationStatus::Indeterminate { operation_id }
                    }
                };
                entries.push((InputKey { batch_id, index }, status));
            }
            Ok(Some(BatchCancellationReport {
                group_id: request.group_id,
                batch_id,
                entries,
            }))
        })
    }

    fn resume_scope<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<RuntimeScope>> {
        Box::pin(async move {
            if task_id.into_bytes() != operation_id.into_bytes() {
                return Err(Error::Unauthorized(
                    "task identity does not match its admission".into(),
                ));
            }
            let admission = self.admission(operation_id).await?;
            self.root_scope
                .narrow(admission.grants, admission.limits)?
                .with_run_limits(admission.run_limits)?
                .with_replayed_extensions(admission.extensions)
        })
    }

    fn admit<'a>(
        &'a self,
        admission: TaskAdmissionRecord,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
        Box::pin(async move {
            let operation_id = admission.operation_id;
            let identity = admission.task.clone();
            let machine = admission.machine.clone();
            let output_schema = admission.output_schema.clone();
            let parent = admission.parent;
            let scope = RuntimeScope::new(admission.grants.clone(), admission.limits)?
                .with_run_limits(admission.run_limits)?
                .with_replayed_extensions(admission.extensions.clone())?;
            self.root_scope
                .narrow(scope.grants().clone(), scope.limits())?
                .with_run_limits(scope.run_limits())?;
            self.validate_parent_scope(parent, &scope).await?;
            if parent.is_some() {
                require_descendant_grant(scope.grants(), &identity)?;
            }
            if let Some(extensions) = scope.extensions() {
                extensions.validate()?;
            }
            if scope.extensions() != self.root_scope.extensions() {
                return Err(Error::Conflict(
                    "task extensions differ from owner bindings".into(),
                ));
            }
            self.validate_local_admission_policy(&admission)?;
            if machine.name != identity.name
                || machine.version != identity.version
                || machine.digest == [0; 32]
                || self.machines.resolve(&machine).is_none()
            {
                return Err(Error::Invalid(
                    "task and machine identities disagree".into(),
                ));
            }
            self.tasks.validate_durable_admission(
                &identity,
                &machine,
                Some(&admission.input_schema),
                &output_schema,
                Some(&admission.input),
            )?;
            jsonschema::validator_for(&output_schema)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            admission.validate()?;
            let canonical = admission.canonical_value();
            let bytes = crate::contract::canonical_json_bytes(&canonical)?;
            if bytes.len() as u64 > scope.limits().file_bytes {
                return Err(Error::Invalid(
                    "durable task admission exceeds its file limit".into(),
                ));
            }
            let key = format!("task-admission:{operation_id}");
            let state = self.payloads.stage(operation_id, &key, &bytes).await?;
            state.descriptor().verify(&bytes)?;
            if self.read_json(&state).await? != canonical {
                return Err(Error::Storage(
                    "staged task admission changed before publication".into(),
                ));
            }
            let spec = OperationSpec {
                operation_id,
                parent: parent.map(|parent| ParentLink {
                    operation_id: OperationId::from_bytes(parent.into_bytes()),
                    slot: operation_id.to_string(),
                }),
                owner: DurableOwner::Attached {
                    authority: self.owner.clone(),
                },
                entrypoint: EntrypointRef {
                    name: identity.name,
                    version: identity.version,
                    digest: identity.digest,
                    result_schema: output_schema,
                },
                dependencies: BTreeSet::new(),
                resources: ResourceRequest::default(),
                placement: BTreeMap::new(),
                orchestration: Orchestration::Leaf,
                state,
            };
            let outcome = self.declare_task(spec, IdempotencyKey::new(key)?).await;
            match outcome {
                Ok(_) => Ok(Admission::Accepted(TaskId::from_bytes(
                    operation_id.into_bytes(),
                ))),
                Err(Error::Indeterminate(_)) => Ok(Admission::Indeterminate { operation_id }),
                Err(error) => Err(error),
            }
        })
    }

    fn outcome<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<Option<Outcome<Value>>>> {
        Box::pin(async move {
            let operation_id = OperationId::from_bytes(task_id.into_bytes());
            let observed = {
                let mut coordinator = self.coordinator.lock().await;
                coordinator.refresh().await?;
                coordinator.observe_operation(
                    &self.owner,
                    &self.owner_scope,
                    &self.verifier,
                    operation_id,
                )?
            };
            match observed.outcome {
                Some(Outcome::Succeeded(reference)) => {
                    Ok(Some(Outcome::Succeeded(self.read_json(&reference).await?)))
                }
                Some(Outcome::Failed { message }) => Ok(Some(Outcome::Failed { message })),
                Some(Outcome::Cancelled) => Ok(Some(Outcome::Cancelled)),
                Some(Outcome::Indeterminate { operation_id }) => {
                    Ok(Some(Outcome::Indeterminate { operation_id }))
                }
                None => Ok(None),
            }
        })
    }

    fn cancel<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let operation_id = OperationId::from_bytes(task_id.into_bytes());
            self.coordinator
                .lock()
                .await
                .cancel_operation(
                    &self.owner,
                    &self.owner_scope,
                    &self.verifier,
                    operation_id,
                    IdempotencyKey::new(format!("task-cancel:{operation_id}"))?,
                    true,
                )
                .await?;
            Ok(())
        })
    }

    fn interact<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
        interaction: Interaction,
    ) -> BoxFuture<'a, Result<InteractionOutcome>> {
        Box::pin(async move {
            interaction.validate()?;
            if operation_id.into_bytes() == task_id.into_bytes() {
                return Err(Error::Invalid(
                    "interaction operation must differ from task admission".into(),
                ));
            }
            let admission = self
                .admission(OperationId::from_bytes(task_id.into_bytes()))
                .await?;
            if !admission.grants.contains("interaction:route") {
                return Err(Error::Unauthorized(
                    "task scope lacks interaction:route".into(),
                ));
            }
            let journal = self.interactions.as_ref().ok_or_else(|| {
                Error::Unsupported("durable interaction journal is not bound".into())
            })?;
            // A caller-selected operation ID is meaningful only within its
            // admitted task. Namespace the journal ticket by both identities
            // so another task cannot observe or resolve its interaction.
            let id = task_interaction_id(task_id, operation_id);
            journal.open_interaction(id, interaction).await?;
            Ok(journal
                .interaction_outcome(id)
                .await?
                .unwrap_or(InteractionOutcome::Indeterminate { operation_id }))
        })
    }

    fn send<'a>(
        &'a self,
        sender: TaskId,
        recipient: TaskId,
        message_id: OperationId,
        payload: FileRef,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            payload.validate()?;
            let sender_admission = self
                .admission(OperationId::from_bytes(sender.into_bytes()))
                .await?;
            if !sender_admission.grants.contains("mail:send") {
                return Err(Error::Unauthorized("sender scope lacks mail:send".into()));
            }
            let recipient_admission = self
                .admission(OperationId::from_bytes(recipient.into_bytes()))
                .await?;
            recipient_admission.limits.validate_file(&payload)?;
            if !read_granted(&recipient_admission.grants, &payload)? {
                return Err(Error::Unauthorized(
                    "recipient cannot read the mailed file".into(),
                ));
            }
            self.reader.verify(&payload).await?;
            let event = MailEvent {
                sender,
                message_id,
                payload,
            };
            let bytes = crate::contract::canonical_json_bytes(&event)?;
            let mailbox = self.mailbox(recipient)?;
            self.publish_control(&mailbox, "mail", recipient, message_id, &bytes)
                .await
        })
    }

    fn inbox<'a>(
        &'a self,
        task_id: TaskId,
        after: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<InboxItem>>> {
        Box::pin(async move {
            if limit == 0 || limit > 1_024 {
                return Err(Error::Invalid("inbox page bound is invalid".into()));
            }
            let recipient_admission = self
                .admission(OperationId::from_bytes(task_id.into_bytes()))
                .await?;
            if !recipient_admission.grants.contains("mail:read") {
                return Err(Error::Unauthorized(
                    "recipient scope lacks mail:read".into(),
                ));
            }
            let mailbox = self.mailbox(task_id)?;
            let bounds = match mailbox.bounds().await {
                Ok(bounds) => bounds,
                Err(StreamError::NotFound) => return Ok(Vec::new()),
                Err(error) => return Err(Error::Storage(error.to_string())),
            };
            if after > bounds.tail {
                return Err(Error::Invalid("inbox cursor is beyond the tail".into()));
            }
            let page_limit = u32::try_from(limit)
                .map_err(|_| Error::Invalid("inbox page bound is invalid".into()))?;
            let page = match mailbox.read(after, page_limit).await {
                Ok(records) => records.try_collect::<Vec<_>>().await?,
                Err(StreamError::NotFound) => return Ok(Vec::new()),
                Err(error) => return Err(Error::Storage(error.to_string())),
            };
            let mut items = Vec::with_capacity(page.len());
            for record in page {
                let value: Value = serde_json::from_slice(&record.value)
                    .map_err(|error| Error::Storage(error.to_string()))?;
                if crate::contract::canonical_json_bytes(&value)? != record.value.as_ref() {
                    return Err(Error::Storage("mail event is not canonical JSON".into()));
                }
                let event: MailEvent = serde_json::from_value(value)
                    .map_err(|error| Error::Storage(error.to_string()))?;
                event.payload.validate()?;
                recipient_admission.limits.validate_file(&event.payload)?;
                if !read_granted(&recipient_admission.grants, &event.payload)? {
                    return Err(Error::Unauthorized(
                        "mail history contains an unreadable payload".into(),
                    ));
                }
                self.admission(OperationId::from_bytes(event.sender.into_bytes()))
                    .await?;
                items.push(InboxItem {
                    task_id,
                    sequence: record
                        .sequence
                        .checked_add(1)
                        .ok_or_else(|| Error::Storage("inbox sequence exhausted".into()))?,
                    message_id: event.message_id.to_string(),
                    payload: event.payload,
                });
            }
            Ok(items)
        })
    }

    fn wait_until<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
        deadline_unix_ms: u64,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if deadline_unix_ms == 0 {
                return Err(Error::Invalid("timer deadline is invalid".into()));
            }
            let admission = self
                .admission(OperationId::from_bytes(task_id.into_bytes()))
                .await?;
            if !admission.grants.contains("timer:wait") {
                return Err(Error::Unauthorized("task scope lacks timer:wait".into()));
            }
            let event = TimerEvent {
                task_id,
                operation_id,
                deadline_unix_ms,
            };
            let bytes = crate::contract::canonical_json_bytes(&event)?;
            let stream = self.timer_stream(task_id)?;
            self.publish_control(&stream, "timers", task_id, operation_id, &bytes)
                .await?;
            loop {
                let now = self.clock.now_unix_millis();
                if now >= deadline_unix_ms {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(
                    deadline_unix_ms.saturating_sub(now).min(60_000),
                ))
                .await;
            }
        })
    }

    fn execute_tool<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
        definition: ToolDefinition,
        arguments: Value,
        scope: RuntimeScope,
        context: crate::runtime::ToolContext,
    ) -> BoxFuture<'a, Result<Outcome<Value>>> {
        Box::pin(async move {
            if operation_id.into_bytes() == task_id.into_bytes() {
                return Err(Error::Invalid(
                    "tool operation must differ from task admission".into(),
                ));
            }
            let admission = self
                .admission(OperationId::from_bytes(task_id.into_bytes()))
                .await?;
            self.root_scope
                .narrow(admission.grants, admission.limits)?
                .narrow(scope.grants().clone(), scope.limits())?;
            if context.task().durable_task_id() != Some(task_id)
                || context.task().scope().grants() != scope.grants()
                || context.task().scope().limits() != scope.limits()
                || context.operation_id() != operation_id
                || context.call_id() != operation_id.to_string()
            {
                return Err(Error::Unauthorized(
                    "tool context does not match the admitted task".into(),
                ));
            }
            let capability = format!("tool:call:{}", definition.name);
            if !scope.grants().contains(&capability) {
                return Err(Error::Unauthorized(format!(
                    "task scope lacks {capability}"
                )));
            }
            let tools = self
                .tools
                .as_ref()
                .ok_or_else(|| Error::Unsupported("durable tool runner is not bound".into()))?;
            if let Some(policy) = &self.policy {
                if admission.policy.as_ref() != Some(&policy.identity()) {
                    return Err(Error::Conflict(
                        "durable policy changed before tool dispatch".into(),
                    ));
                }
                if let Some((approval_id, request)) = context
                    .task()
                    .policy_approval(
                        policy.as_ref(),
                        &definition,
                        &crate::tool::ToolInvocation {
                            operation_id,
                            call_id: operation_id.to_string(),
                            name: definition.name.clone(),
                            arguments: arguments.clone(),
                        },
                    )
                    .await?
                {
                    check_tool_approval(self.interact(task_id, approval_id, request).await?)?;
                }
                if admission.policy.as_ref() != Some(&policy.identity()) {
                    return Err(Error::Conflict(
                        "durable policy changed before tool execution".into(),
                    ));
                }
            }
            tools
                .run_with_context(task_id, operation_id, definition, arguments, context)
                .await
        })
    }

    fn reconcile_effect<'a>(
        &'a self,
        task_id: TaskId,
        effect_id: EffectId,
    ) -> BoxFuture<'a, Result<crate::core::EffectStatus>> {
        Box::pin(async move {
            let admission = self
                .admission(OperationId::from_bytes(task_id.into_bytes()))
                .await?;
            if !admission.grants.contains("effect:run") {
                return Err(Error::Unauthorized("task scope lacks effect:run".into()));
            }
            let effects = self
                .effects
                .as_ref()
                .ok_or_else(|| Error::Unsupported("durable effect observer is not bound".into()))?;
            effects.reconcile(effect_id).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentId, Capabilities, GroupId,
        conversation::{FileDescriptor, Limits, VolumeClass, VolumeOwner, VolumeRef},
        core::{AggregateKind, AuthorityIssuer},
        resources::ProviderRef,
        runtime::TaskDefinition,
        workflow::{MachineIdentity, MachineStatus, MachineTransition, ResumableMachine},
    };
    use acyclic_stream::{MemoryStream, SystemUnixMillisClock};

    struct MemoryPayloads {
        volume: VolumeRef,
        files: Mutex<BTreeMap<String, Vec<u8>>>,
    }

    impl MemoryPayloads {
        fn new() -> Result<Self> {
            Ok(Self {
                volume: VolumeRef::new(
                    ProviderRef::new("batch-test", "filesystem", "2")?,
                    "private",
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
                )?,
                files: Mutex::new(BTreeMap::new()),
            })
        }
    }

    impl SchedulerPayloadStore for MemoryPayloads {
        fn stage<'a>(
            &'a self,
            operation_id: OperationId,
            idempotency_key: &'a str,
            bytes: &'a [u8],
        ) -> BoxFuture<'a, Result<FileRef>> {
            Box::pin(async move {
                let path = format!(
                    "scheduler/{operation_id}/{}.json",
                    blake3::hash(idempotency_key.as_bytes()).to_hex(),
                );
                let mut files = self.files.lock().await;
                if let Some(existing) = files.get(&path) {
                    if existing != bytes {
                        return Err(Error::Conflict("staged identity changed bytes".into()));
                    }
                } else {
                    files.insert(path.clone(), bytes.to_vec());
                }
                FileRef::new(
                    self.volume.clone(),
                    path,
                    "immutable-1",
                    FileDescriptor::from_bytes(bytes, "application/json")?,
                    "payload.json",
                )
            })
        }
    }

    impl ContentResidencyVerifier for MemoryPayloads {
        fn verify<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.read(reference).await?;
                Ok(())
            })
        }

        fn read<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
            Box::pin(async move {
                if reference.volume() != &self.volume {
                    return Err(Error::Unauthorized(
                        "payload belongs to another owner".into(),
                    ));
                }
                let bytes = self
                    .files
                    .lock()
                    .await
                    .get(reference.path())
                    .cloned()
                    .ok_or_else(|| Error::NotFound("payload".into()))?;
                reference.descriptor().verify(&bytes)?;
                Ok(bytes)
            })
        }
    }

    struct BatchMachine {
        identity: MachineIdentity,
        schema: Value,
    }

    impl ResumableMachine for BatchMachine {
        fn identity(&self) -> &MachineIdentity {
            &self.identity
        }
        fn state_schema(&self) -> &Value {
            &self.schema
        }
        fn initialize(&self, input: &Value) -> Result<Value> {
            Ok(input.clone())
        }
        fn transition(&self, state: &Value, _input: &Value) -> Result<MachineTransition> {
            Ok(MachineTransition {
                state: state.clone(),
                commands: Vec::new(),
                status: MachineStatus::Suspended,
            })
        }
    }

    #[tokio::test]
    async fn model_claims_use_pinned_ceiling_and_current_owner() -> Result<()> {
        model_claim_host(StreamClient::new(Arc::new(MemoryStream::default())))
            .await
            .map(|_| ())
    }

    #[cfg(feature = "filesystem-local")]
    #[tokio::test]
    async fn owned_task_journal_guards_survive_local_stream_reopen() -> Result<()> {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let payloads = model_claim_host(StreamClient::new(Arc::new(provider))).await?;
        let provider = acyclic_stream::LocalStream::open(directory.path(), Default::default())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let stream = StreamClient::new(Arc::new(provider));
        assert_eq!(
            stream
                .stream(format!(
                    "harness/v2/workflows/{}",
                    OperationId::from_bytes([77; 16])
                ))?
                .tail()
                .await?,
            1
        );
        assert_eq!(
            stream
                .stream(format!(
                    "harness/v2/execution/{}",
                    OperationId::from_bytes([78; 16])
                ))?
                .tail()
                .await?,
            74
        );
        // The coordinator journal is durable independently of the in-memory
        // Filesystem payloads used by this focused guard qualification.
        let coordinator = DistributedCoordinator::open(&stream, payloads).await?;
        let operation = coordinator
            .scheduler()
            .operation(OperationId::from_bytes([77; 16]))
            .ok_or_else(|| Error::NotFound("reopened owned task".into()))?;
        assert!(operation.cancellation_requested);
        assert!(operation.reservation.is_some());
        Ok(())
    }

    async fn model_claim_host<P: StreamProvider>(
        stream: StreamClient<P>,
    ) -> Result<Arc<MemoryPayloads>> {
        let payloads = Arc::new(MemoryPayloads::new()?);
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "model-owner".into(),
        };
        let issuer = AuthorityIssuer::new("model-test", [7; 32], owner.clone());
        let owner_scope = issuer.root(
            "owner",
            Capabilities::new([
                "operation:declare",
                "operation:observe",
                "operation:cancel",
                "task:spawn:test.model@1",
                "model:generate",
            ]),
        );
        let scope = RuntimeScope::new(owner_scope.capabilities().clone(), Limits::default())?;
        let machine = MachineIdentity {
            name: "test.model".into(),
            version: "1".into(),
            digest: [2; 32],
        };
        let implementation: Arc<dyn ResumableMachine> = Arc::new(BatchMachine {
            identity: machine.clone(),
            schema: serde_json::json!({"type": "integer"}),
        });
        let definition = TaskDefinition::<Value, i64>::resumable(
            implementation.clone(),
            serde_json::json!({"type": "integer"}),
            serde_json::json!({"type": "integer"}),
        )?;
        let task = definition.identity().clone();
        let mut tasks = TaskRegistry::default();
        tasks.register(definition)?;
        let mut machines = MachineRegistry::default();
        machines.register(implementation)?;
        let host = Arc::new(CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, payloads.clone()).await?,
            stream.clone(),
            payloads.clone(),
            payloads.clone(),
            owner.clone(),
            owner_scope.clone(),
            issuer.verifier(),
            scope.clone(),
            tasks,
            machines,
            Arc::new(SystemUnixMillisClock),
        )?);
        let operation_id = OperationId::from_bytes([77; 16]);
        let task_id = TaskId::from_bytes(operation_id.into_bytes());
        host.admit(TaskAdmissionRecord {
            operation_id,
            task,
            machine,
            input: serde_json::json!(0),
            input_schema: serde_json::json!({"type": "integer"}),
            output_schema: serde_json::json!({"type": "integer"}),
            parent: None,
            grants: scope.grants().clone(),
            limits: scope.limits(),
            run_limits: crate::runtime::TaskRunLimits {
                max_steps: Some(1),
                ..scope.run_limits()
            },
            policy: None,
            extensions: None,
            execution: None,
        })
        .await?;
        let fence = {
            let mut coordinator = host.coordinator.lock().await;
            coordinator
                .configure_session(
                    &owner,
                    &owner_scope,
                    &issuer.verifier(),
                    operation_id,
                    crate::scheduler::SessionLimits {
                        active_tasks: 1,
                        total_tasks: 1,
                        depth: 0,
                        model_steps: 2,
                    },
                    IdempotencyKey::new("model-session")?,
                )
                .await?;
            let lease = coordinator
                .pull(&crate::distributed::Worker {
                    id: "model-worker".into(),
                    available: crate::scheduler::ResourceSnapshot::default(),
                    labels: BTreeMap::new(),
                })
                .await?
                .ok_or_else(|| Error::NotFound("model lease".into()))?;
            let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new("model-start")?,
                    crate::scheduler::SchedulerEvent::Started {
                        operation_id,
                        fence: fence.clone(),
                    },
                )
                .await?;
            fence
        };
        host.verify_execution_owner(task_id, fence.clone()).await?;
        let attempt = OperationId::from_bytes([78; 16]);
        host.claim_model_dispatch(task_id, attempt, 0, [3; 32], fence.clone())
            .await?;
        #[cfg(feature = "filesystem")]
        let (workflow, filesystem, volume, content_scope, original_owner, original_pending) = {
            use crate::workflow::{DurableWorkflowHost, WorkflowJournal};
            let provider = ProviderRef::new("owned-workflow", "filesystem", "2")?;
            let filesystem = Arc::new(crate::filesystem::FilesystemHost::new(
                acyclic_fs::Fs::memory(),
                provider.clone(),
            )?);
            let volume = VolumeRef::new(
                provider,
                "owned-private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            )?;
            filesystem.create_volume(&volume).await?;
            let content_scope = issuer.root_for_agent(
                AgentId::from_bytes([1; 16]),
                "workflow-content",
                Capabilities::new([
                    volume.capability(crate::conversation::VolumeOperation::Read)?,
                    volume.capability(crate::conversation::VolumeOperation::Write)?,
                ]),
            );
            let original_owner = host.journal_owner(task_id, fence.clone()).await?;
            assert!(
                original_owner
                    .validate_storage(&issuer.verifier(), scope.limits().file_bytes + 1)
                    .is_err()
            );
            let foreign = AuthorityIssuer::new(
                "foreign-owner",
                [8; 32],
                Authority {
                    kind: AggregateKind::Task,
                    id: "foreign".into(),
                },
            );
            assert!(
                original_owner
                    .validate_storage(&foreign.verifier(), 65_536)
                    .is_err()
            );
            let binding = host.journal_owner(task_id, fence.clone()).await?;
            let admission = binding.workflow_admission().clone();
            let journal = Arc::new(crate::filesystem::FilesystemWorkflowJournal::for_task(
                binding,
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                content_scope.clone(),
                65_536,
            )?);
            let mut wrong = admission.clone();
            wrong.initial.state = serde_json::json!(1);
            assert!(journal.admit(wrong).await.is_err());
            assert_eq!(journal.admit(admission.clone()).await?, admission);
            let mut workflow = DurableWorkflowHost::open(
                host.machines.clone(),
                admission.initial,
                journal.clone(),
            )
            .await?;
            workflow
                .step(
                    OperationId::from_bytes([85; 16]),
                    IdempotencyKey::new("owned-step")?,
                    serde_json::json!(0),
                )
                .await?;
            let records = journal.replay(0, 1).await?;
            let [record] = records.as_slice() else {
                return Err(Error::Invalid("one owned transition required".into()));
            };
            assert!(matches!(
                journal
                    .commit(0, record.idempotency_key.clone(), record.clone())
                    .await?,
                crate::workflow::WorkflowCommitOutcome::Replayed(_)
            ));
            let mut forged = record.clone();
            forged.next.state = serde_json::json!(1);
            forged.transition.state = serde_json::json!(1);
            assert!(
                journal
                    .commit(0, forged.idempotency_key.clone(), forged)
                    .await
                    .is_err()
            );
            let path =
                acyclic_stream::StreamPath::new(format!("harness/v2/workflows/{operation_id}"))?;
            let key = StreamKey::new(Bytes::from_static(b"owned-before-replacement"))?;
            let original_pending = original_owner
                .prepare_append(
                    &path,
                    1,
                    &key,
                    &Bytes::from_static(b"never published"),
                    crate::distributed::JournalWrite::Fresh,
                )
                .await?;
            (
                journal,
                filesystem,
                volume,
                content_scope,
                original_owner,
                original_pending,
            )
        };
        #[cfg(feature = "filesystem")]
        let (execution, execution_pending) = {
            use crate::executor::{ExecutionEvent, ExecutionJournal};
            let journal = crate::filesystem::FilesystemExecutionJournal::for_task(
                host.journal_owner(task_id, fence.clone()).await?,
                attempt,
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                content_scope.clone(),
                65_536,
            )?;
            let pending_operation = OperationId::from_bytes([80; 16]);
            let digest = blake3::hash(format!("{pending_operation}:pending-execution").as_bytes());
            let key = StreamKey::new(Bytes::copy_from_slice(digest.as_bytes()))?;
            let bytes = Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "operation_id": pending_operation,
                    "retry_digest": digest.to_hex().to_string(),
                    "event": ExecutionEvent::Started { request_digest: [9; 32] },
                }))
                .map_err(|error| Error::Invalid(error.to_string()))?,
            );
            let path = acyclic_stream::StreamPath::new(format!(
                "harness/v2/execution/{pending_operation}"
            ))?;
            let pending = original_owner
                .prepare_append(
                    &path,
                    0,
                    &key,
                    &bytes,
                    crate::distributed::JournalWrite::Fresh,
                )
                .await?;
            journal
                .append(
                    attempt,
                    "request".into(),
                    ExecutionEvent::Started {
                        request_digest: [9; 32],
                    },
                )
                .await?;
            assert!(
                journal
                    .append(
                        attempt,
                        "wrong-claim".into(),
                        ExecutionEvent::ModelStarted {
                            step: 0,
                            request_digest: [4; 32]
                        }
                    )
                    .await
                    .is_err()
            );
            assert!(
                journal
                    .append(
                        attempt,
                        "absent-claim".into(),
                        ExecutionEvent::ModelStarted {
                            step: 1,
                            request_digest: [3; 32]
                        }
                    )
                    .await
                    .is_err()
            );
            let start = ExecutionEvent::ModelStarted {
                step: 0,
                request_digest: [3; 32],
            };
            assert!(
                journal
                    .append_if_tail(attempt, 1, "model".into(), start.clone())
                    .await?
            );
            assert!(
                journal
                    .append_if_tail(attempt, 1, "model".into(), start)
                    .await?
            );
            assert_eq!(journal.replay(attempt, 0, 64).await?.len(), 2);
            let invocation = crate::tool::ToolInvocation::for_model_call(
                attempt,
                0,
                "owned-tool".into(),
                "test".into(),
                serde_json::json!({}),
            );
            let invocation =
                crate::executor::stage_json(&journal, attempt, "tool-invocation", &invocation)
                    .await?;
            journal
                .append(
                    attempt,
                    "tool".into(),
                    ExecutionEvent::ToolStarted {
                        step: 0,
                        call_id: "owned-tool".into(),
                        invocation,
                    },
                )
                .await?;
            assert!(journal.replay(operation_id, 0, 1).await.is_err());
            assert!(matches!(
                journal
                    .interaction_outcome(crate::InteractionId::from_bytes([91; 16]))
                    .await,
                Err(Error::Unsupported(_))
            ));
            let uncharged = OperationId::from_bytes([79; 16]);
            let unbound = crate::filesystem::FilesystemExecutionJournal::new(
                stream.clone(),
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                content_scope.clone(),
                65_536,
            )?;
            unbound
                .append(
                    uncharged,
                    "request".into(),
                    ExecutionEvent::Started {
                        request_digest: [9; 32],
                    },
                )
                .await?;
            unbound
                .append(
                    uncharged,
                    "model".into(),
                    ExecutionEvent::ModelStarted {
                        step: 0,
                        request_digest: [3; 32],
                    },
                )
                .await?;
            let adopted = crate::filesystem::FilesystemExecutionJournal::for_task(
                host.journal_owner(task_id, fence.clone()).await?,
                uncharged,
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                content_scope.clone(),
                65_536,
            )?;
            assert!(
                adopted.replay(uncharged, 0, 64).await.is_err(),
                "cold replay must reject a model start lacking its retained charge"
            );
            (journal, pending)
        };
        host.claim_model_dispatch(task_id, attempt, 0, [3; 32], fence.clone())
            .await?;
        assert!(
            host.claim_model_dispatch(task_id, attempt, 1, [4; 32], fence.clone())
                .await
                .is_err(),
            "the task ceiling is one despite the larger session ceiling"
        );
        assert!(
            host.claim_model_dispatch(task_id, attempt, 0, [4; 32], fence.clone())
                .await
                .is_err()
        );
        let mut stale = fence.clone();
        stale.reservation_id.push_str("-stale");
        assert!(
            host.verify_execution_owner(task_id, stale.clone())
                .await
                .is_err()
        );
        assert!(
            host.claim_model_dispatch(task_id, attempt, 0, [3; 32], stale)
                .await
                .is_err()
        );
        let fence = {
            let mut coordinator = host.coordinator.lock().await;
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new("model-release")?,
                    crate::scheduler::SchedulerEvent::LeaseReleased {
                        operation_id,
                        fence: fence.clone(),
                    },
                )
                .await?;
            let lease = coordinator
                .pull(&crate::distributed::Worker {
                    id: "replacement-worker".into(),
                    available: crate::scheduler::ResourceSnapshot::default(),
                    labels: BTreeMap::new(),
                })
                .await?
                .ok_or_else(|| Error::NotFound("replacement model lease".into()))?;
            let replacement = crate::scheduler::LeaseFence::from(&lease.reservation);
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new("model-restart")?,
                    crate::scheduler::SchedulerEvent::Started {
                        operation_id,
                        fence: replacement.clone(),
                    },
                )
                .await?;
            replacement
        };
        // An admitted attempt whose execution-journal claim was not yet written
        // can continue under a new owner without spending another unit.
        host.claim_model_dispatch(task_id, attempt, 0, [3; 32], fence.clone())
            .await?;
        assert!(
            host.claim_model_dispatch(task_id, attempt, 1, [4; 32], fence.clone())
                .await
                .is_err()
        );
        #[cfg(feature = "filesystem")]
        let (replacement_workflow, pending) = {
            use crate::workflow::WorkflowJournal;
            assert!(
                matches!(
                    stream.commit(original_pending).await?,
                    acyclic_stream::CommitOutcome::Conflict(_)
                ),
                "lease replacement after preflight must atomically prevent publication"
            );
            assert_eq!(
                stream
                    .stream(format!("harness/v2/workflows/{operation_id}"))?
                    .tail()
                    .await?,
                1
            );
            let record = workflow
                .replay(0, 1)
                .await?
                .pop()
                .ok_or_else(|| Error::NotFound("owned workflow record".into()))?;
            assert!(
                workflow
                    .commit(0, record.idempotency_key.clone(), record.clone())
                    .await
                    .is_err(),
                "the previous lease cannot replay a checkpoint mutation"
            );
            assert!(original_owner.verify(false).await.is_err());
            let binding = host.journal_owner(task_id, fence.clone()).await?;
            let path =
                acyclic_stream::StreamPath::new(format!("harness/v2/workflows/{operation_id}"))?;
            let key = StreamKey::new(Bytes::from_static(b"owned-pending"))?;
            let pending = binding
                .prepare_append(
                    &path,
                    1,
                    &key,
                    &Bytes::from_static(b"never published"),
                    crate::distributed::JournalWrite::Fresh,
                )
                .await?;
            let journal = crate::filesystem::FilesystemWorkflowJournal::for_task(
                binding,
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                content_scope.clone(),
                65_536,
            )?;
            assert!(matches!(
                journal
                    .commit(0, record.idempotency_key.clone(), record)
                    .await?,
                crate::workflow::WorkflowCommitOutcome::Replayed(_)
            ));
            (journal, pending)
        };
        #[cfg(feature = "filesystem")]
        let execution_pending =
            {
                assert!(
                    matches!(
                        stream.commit(execution_pending).await?,
                        acyclic_stream::CommitOutcome::Conflict(_)
                    ),
                    "lease replacement must atomically fence execution publication"
                );
                let pending_operation = OperationId::from_bytes([80; 16]);
                let path = acyclic_stream::StreamPath::new(format!(
                    "harness/v2/execution/{pending_operation}"
                ))?;
                assert!(matches!(
                    stream.bounds(path.as_str()).await,
                    Err(StreamError::NotFound)
                ));
                let digest =
                    blake3::hash(format!("{pending_operation}:pending-execution").as_bytes());
                let key = StreamKey::new(Bytes::copy_from_slice(digest.as_bytes()))?;
                let bytes = Bytes::from(serde_json::to_vec(&serde_json::json!({
                "operation_id": pending_operation,
                "retry_digest": digest.to_hex().to_string(),
                "event": crate::executor::ExecutionEvent::Started { request_digest: [9; 32] },
            })).map_err(|error| Error::Invalid(error.to_string()))?);
                host.journal_owner(task_id, fence.clone())
                    .await?
                    .prepare_append(
                        &path,
                        0,
                        &key,
                        &bytes,
                        crate::distributed::JournalWrite::Fresh,
                    )
                    .await?
            };
        host.coordinator
            .lock()
            .await
            .cancel_operation(
                &owner,
                &owner_scope,
                &issuer.verifier(),
                operation_id,
                IdempotencyKey::new("model-cancel")?,
                false,
            )
            .await?;
        // Cancellation retains this owner only to settle an already dispatched attempt.
        host.verify_execution_owner(task_id, fence.clone()).await?;
        #[cfg(feature = "filesystem")]
        {
            use crate::executor::{ExecutionEvent, ExecutionJournal};
            assert!(
                matches!(
                    stream.commit(execution_pending).await?,
                    acyclic_stream::CommitOutcome::Conflict(_)
                ),
                "cancellation must atomically fence execution publication"
            );
            assert!(matches!(
                stream
                    .bounds(&format!(
                        "harness/v2/execution/{}",
                        OperationId::from_bytes([80; 16])
                    ))
                    .await,
                Err(StreamError::NotFound)
            ));
            assert!(
                execution
                    .append(
                        attempt,
                        "model".into(),
                        ExecutionEvent::ModelStarted {
                            step: 0,
                            request_digest: [3; 32]
                        }
                    )
                    .await
                    .is_err(),
                "stale lease must reject even an exact retry"
            );
            assert!(
                execution
                    .append(
                        attempt,
                        "stale-tool-settlement".into(),
                        ExecutionEvent::ToolFailed {
                            step: 0,
                            call_id: "owned-tool".into(),
                            reason: crate::executor::ToolFailureKind::ExecutorRejected
                        }
                    )
                    .await
                    .is_err(),
                "a replaced lease cannot settle the retained dispatch"
            );
            let journal = crate::filesystem::FilesystemExecutionJournal::for_task(
                host.journal_owner(task_id, fence.clone()).await?,
                attempt,
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                content_scope.clone(),
                65_536,
            )?;
            assert!(
                journal
                    .append(
                        attempt,
                        "model".into(),
                        ExecutionEvent::ModelStarted {
                            step: 0,
                            request_digest: [3; 32]
                        }
                    )
                    .await
                    .is_err(),
                "cancellation forbids new or retried starts"
            );
            let bytes = serde_json::to_vec(&crate::model::ModelEvent::Content {
                delta: "settled".into(),
            })
            .map_err(|error| Error::Invalid(error.to_string()))?;
            let reference = journal
                .stage(
                    attempt,
                    "model-observation".into(),
                    bytes,
                    "application/json",
                )
                .await?;
            let observation = ExecutionEvent::Model {
                step: 0,
                event: reference.clone(),
            };
            assert!(
                journal
                    .append(
                        attempt,
                        "missing-tool".into(),
                        ExecutionEvent::ToolFailed {
                            step: 0,
                            call_id: "missing".into(),
                            reason: crate::executor::ToolFailureKind::ExecutorRejected
                        }
                    )
                    .await
                    .is_err()
            );
            let tool_settlement = ExecutionEvent::ToolFailed {
                step: 0,
                call_id: "owned-tool".into(),
                reason: crate::executor::ToolFailureKind::ExecutorRejected,
            };
            journal
                .append(attempt, "tool-settlement".into(), tool_settlement.clone())
                .await?;
            journal
                .append(attempt, "tool-settlement".into(), tool_settlement.clone())
                .await?;
            assert!(
                journal
                    .append(attempt, "another-tool-settlement".into(), tool_settlement)
                    .await
                    .is_err()
            );
            for index in 0..70 {
                journal
                    .append(attempt, format!("settlement-{index}"), observation.clone())
                    .await?;
            }
            journal
                .append(attempt, "settlement-69".into(), observation.clone())
                .await?;
            assert_eq!(journal.replay(attempt, 0, 64).await?.len(), 64);
            assert_eq!(journal.replay(attempt, 64, 64).await?.len(), 10);
            let reopened = crate::filesystem::FilesystemExecutionJournal::for_task(
                host.journal_owner(task_id, fence.clone()).await?,
                attempt,
                filesystem.clone(),
                volume.clone(),
                issuer.verifier(),
                content_scope.clone(),
                65_536,
            )?;
            reopened
                .append(attempt, "settlement-69".into(), observation.clone())
                .await?;
            assert!(
                reopened
                    .append(
                        attempt,
                        "settlement-69".into(),
                        ExecutionEvent::Model {
                            step: 1,
                            event: reference,
                        }
                    )
                    .await
                    .is_err()
            );
            assert_eq!(reopened.replay(attempt, 64, 64).await?.len(), 10);
        }
        #[cfg(feature = "filesystem")]
        {
            use crate::workflow::WorkflowJournal;
            assert!(
                matches!(
                    stream.commit(pending).await?,
                    acyclic_stream::CommitOutcome::Conflict(_)
                ),
                "cancellation after preflight must atomically prevent publication"
            );
            assert_eq!(
                stream
                    .stream(format!("harness/v2/workflows/{operation_id}"))?
                    .tail()
                    .await?,
                1
            );
            let record = replacement_workflow
                .replay(0, 1)
                .await?
                .pop()
                .ok_or_else(|| Error::NotFound("cancelled workflow record".into()))?;
            assert!(
                replacement_workflow
                    .commit(0, record.idempotency_key.clone(), record)
                    .await
                    .is_err(),
                "cancellation must reject checkpoint mutation retries"
            );
            assert!(
                host.journal_owner(task_id, fence.clone())
                    .await?
                    .verify(false)
                    .await
                    .is_err()
            );
        }
        assert!(
            host.claim_model_dispatch(task_id, attempt, 0, [3; 32], fence)
                .await
                .is_err()
        );
        Ok(payloads)
    }

    #[tokio::test]
    async fn partially_admitted_batch_reopens_and_fills_only_missing_slots() -> Result<()> {
        let stream = StreamClient::new(Arc::new(MemoryStream::default()));
        let payloads = Arc::new(MemoryPayloads::new()?);
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "batch-owner".into(),
        };
        let issuer = AuthorityIssuer::new("batch-test", [7; 32], owner.clone());
        let owner_scope = issuer.root(
            "owner",
            Capabilities::new([
                "operation:declare",
                "operation:observe",
                "operation:cancel",
                "task:spawn:test.batch@1",
            ]),
        );
        let runtime_scope =
            RuntimeScope::new(owner_scope.capabilities().clone(), Limits::default())?;
        let machine = MachineIdentity {
            name: "test.batch".into(),
            version: "1".into(),
            digest: [2; 32],
        };
        let mut machines = MachineRegistry::default();
        let implementation: Arc<dyn ResumableMachine> = Arc::new(BatchMachine {
            identity: machine.clone(),
            schema: serde_json::json!({"type": "integer"}),
        });
        machines.register(implementation.clone())?;
        let definition = TaskDefinition::<Value, i64>::resumable(
            implementation,
            serde_json::json!({"type": "integer"}),
            serde_json::json!({"type": "integer"}),
        )?;
        let task = definition.identity().clone();
        let mut tasks = TaskRegistry::default();
        tasks.register(definition)?;
        let coordinator = DistributedCoordinator::open(&stream, payloads.clone()).await?;
        let host = CoordinatorTaskHost::new(
            coordinator,
            stream.clone(),
            payloads.clone(),
            payloads.clone(),
            owner.clone(),
            owner_scope.clone(),
            issuer.verifier(),
            runtime_scope.clone(),
            tasks.clone(),
            machines.clone(),
            Arc::new(SystemUnixMillisClock),
        )?;
        let root_admission = |operation_id, input| TaskAdmissionRecord {
            operation_id,
            task: task.clone(),
            machine: machine.clone(),
            input,
            input_schema: serde_json::json!({"type": "integer"}),
            output_schema: serde_json::json!({"type": "integer"}),
            parent: None,
            grants: runtime_scope.grants().clone(),
            limits: runtime_scope.limits(),
            run_limits: runtime_scope.run_limits(),
            policy: None,
            extensions: runtime_scope.extensions().cloned(),
            execution: None,
        };
        assert!(matches!(
            host.admit(root_admission(
                OperationId::from_bytes([7; 16]),
                serde_json::json!("not an integer"),
            ))
            .await,
            Err(Error::Invalid(_))
        ));
        let parent_operation = OperationId::from_bytes([8; 16]);
        let parent = TaskId::from_bytes(parent_operation.into_bytes());
        assert!(matches!(host.admit(root_admission(
            parent_operation, serde_json::json!(0),
        )).await?, Admission::Accepted(id) if id == parent));
        let request = DurableBatchRequest {
            group_id: GroupId::from_bytes([9; 16]),
            batch_id: BatchId::from_bytes([10; 16]),
            group_policy: crate::runtime::BatchGroupPolicy::CollectAll,
            task: task.clone(),
            machine: machine.clone(),
            inputs: vec![
                serde_json::json!(1),
                serde_json::json!(2),
                serde_json::json!(3),
            ],
            input_schema: serde_json::json!({"type": "integer"}),
            output_schema: serde_json::json!({"type": "integer"}),
            parent: Some(parent),
            scope: runtime_scope.clone(),
            policy: None,
            execution: None,
        };
        let bounded_parent_operation = OperationId::from_bytes([27; 16]);
        let bounded_parent = TaskId::from_bytes(bounded_parent_operation.into_bytes());
        let mut bounded = root_admission(bounded_parent_operation, serde_json::json!(0));
        bounded.grants = Capabilities::new(["task:spawn:test.batch@1"]);
        bounded.limits.model_steps = 2;
        bounded.run_limits = crate::runtime::TaskRunLimits {
            concurrency: Some(2),
            max_steps: Some(2),
            deadline_epoch_ms: None,
        };
        assert!(matches!(
            host.admit(bounded.clone()).await?,
            Admission::Accepted(_)
        ));
        for kind in 0..5 {
            let mut child = bounded.clone();
            child.operation_id = OperationId::new();
            child.parent = Some(bounded_parent);
            match kind {
                0 => child.grants = runtime_scope.grants().clone(),
                1 => child.limits.model_steps = 3,
                2 => child.run_limits.max_steps = Some(3),
                3 => child.run_limits.max_steps = None,
                _ => child.run_limits.concurrency = None,
            }
            let operation_id = child.operation_id;
            assert!(matches!(
                host.admit(child).await,
                Err(Error::Unauthorized(_))
            ));
            assert!(host.reconcile_admission(operation_id).await?.is_none());
        }
        let mut valid_child = bounded.clone();
        valid_child.operation_id = OperationId::new();
        valid_child.parent = Some(bounded_parent);
        valid_child.limits.model_steps = 1;
        valid_child.run_limits.max_steps = Some(1);
        valid_child.run_limits.concurrency = Some(1);
        assert!(matches!(
            host.admit(valid_child).await?,
            Admission::Accepted(_)
        ));
        let mut wide_batch = request.clone();
        wide_batch.batch_id = BatchId::from_bytes([28; 16]);
        wide_batch.parent = Some(bounded_parent);
        assert!(matches!(
            host.admit_batch(wide_batch.clone()).await,
            Err(Error::Unauthorized(_))
        ));
        assert!(host.retained_batch(wide_batch.batch_id).await?.is_none());
        let mut forged = request.clone();
        forged.task.digest = [99; 32];
        assert!(matches!(
            host.admit_batch(forged).await,
            Err(Error::Conflict(_))
        ));
        let mut altered_schema = request.clone();
        altered_schema.output_schema = serde_json::json!({"type": "number"});
        assert!(matches!(
            host.admit_batch(altered_schema).await,
            Err(Error::Conflict(_))
        ));
        host.commit_batch(&request).await?;
        let first = request.operation_id(0);
        assert!(matches!(host.admit(request.member_admission(0)?).await?,
            Admission::Accepted(id) if id.into_bytes() == first.into_bytes()));
        let reopened = CoordinatorTaskHost::new(
            DistributedCoordinator::open(&stream, payloads.clone()).await?,
            stream.clone(),
            payloads.clone(),
            payloads.clone(),
            owner,
            owner_scope,
            issuer.verifier(),
            runtime_scope.clone(),
            tasks,
            machines,
            Arc::new(SystemUnixMillisClock),
        )?;
        let restored = reopened
            .load_batch(request.batch_id)
            .await?
            .ok_or_else(|| Error::NotFound("retained batch request".into()))?;
        assert_eq!(restored.canonical_value(), request.canonical_value());
        let observed = reopened
            .reconcile_batch(request.clone())
            .await?
            .ok_or_else(|| Error::NotFound("committed batch".into()))?;
        assert!(matches!(
            observed.as_slice(),
            [
                Admission::Accepted(_),
                Admission::Indeterminate { .. },
                Admission::Indeterminate { .. }
            ]
        ));
        let completed = reopened.admit_batch(request.clone()).await?;
        assert_eq!(completed.len(), 3);
        for (index, admission) in completed.iter().enumerate() {
            let Admission::Accepted(task_id) = admission else {
                return Err(Error::Conflict("batch slot was not admitted".into()));
            };
            assert_eq!(
                task_id.into_bytes(),
                request.operation_id(index).into_bytes()
            );
        }
        assert_eq!(reopened.admit_batch(request.clone()).await?, completed);
        let mut root_request = request.clone();
        root_request.batch_id = BatchId::from_bytes([15; 16]);
        root_request.parent = None;
        let root_members = reopened.admit_batch(root_request.clone()).await?;
        assert!(
            root_members
                .iter()
                .all(|entry| matches!(entry, Admission::Accepted(_)))
        );
        assert_eq!(
            reopened
                .load_batch(root_request.batch_id)
                .await?
                .ok_or_else(|| Error::NotFound("root batch request".into()))?
                .canonical_value(),
            root_request.canonical_value()
        );
        let manifest = reopened.batch_stream(request.batch_id)?;
        assert_eq!(manifest.bounds().await?.tail, 1);
        let records = manifest.read(0, 2).await?.try_collect::<Vec<_>>().await?;
        assert_eq!(records.len(), 1);
        assert!(
            !std::str::from_utf8(&records[0].value)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .contains("inputs")
        );
        let mut collision = request.clone();
        collision.batch_id = BatchId::from_bytes([11; 16]);
        let mut claimed_admission = collision.member_admission(0)?;
        claimed_admission.input = serde_json::json!(999);
        assert!(matches!(
            reopened.admit(claimed_admission).await?,
            Admission::Accepted(_)
        ));
        reopened.commit_batch(&collision).await?;
        assert!(matches!(
            reopened.reconcile_batch(collision).await,
            Err(Error::Conflict(_))
        ));
        let mut root_collision = request.clone();
        root_collision.batch_id = BatchId::from_bytes([12; 16]);
        let mut claimed_admission = root_collision.member_admission(0)?;
        claimed_admission.parent = None;
        assert!(matches!(
            reopened.admit(claimed_admission).await?,
            Admission::Accepted(_)
        ));
        reopened.commit_batch(&root_collision).await?;
        assert!(matches!(
            reopened.reconcile_batch(root_collision).await,
            Err(Error::Conflict(_))
        ));
        let other_parent_operation = OperationId::from_bytes([13; 16]);
        let other_parent = TaskId::from_bytes(other_parent_operation.into_bytes());
        assert!(matches!(
            reopened
                .admit(root_admission(other_parent_operation, serde_json::json!(0),))
                .await?,
            Admission::Accepted(_)
        ));
        let mut foreign_parent = request.clone();
        foreign_parent.batch_id = BatchId::from_bytes([14; 16]);
        let mut claimed_admission = foreign_parent.member_admission(0)?;
        claimed_admission.parent = Some(other_parent);
        assert!(matches!(
            reopened.admit(claimed_admission).await?,
            Admission::Accepted(_)
        ));
        reopened.commit_batch(&foreign_parent).await?;
        assert!(matches!(
            reopened.reconcile_batch(foreign_parent).await,
            Err(Error::Conflict(_))
        ));
        let mut cancelled = request.clone();
        cancelled.batch_id = BatchId::from_bytes([16; 16]);
        cancelled.group_policy = crate::runtime::BatchGroupPolicy::CancelOnFailure;
        cancelled.parent = None;
        reopened.commit_batch(&cancelled).await?;
        let first_cancelled = cancelled.member_admission(0)?;
        assert!(matches!(
            reopened.admit(first_cancelled).await?,
            Admission::Accepted(_)
        ));
        let cancellation = reopened
            .cancel_batch(cancelled.batch_id)
            .await?
            .ok_or_else(|| Error::NotFound("retained cancellation".into()))?;
        assert_eq!(cancellation.group_id, cancelled.group_id);
        assert_eq!(cancellation.entries.len(), cancelled.inputs.len());
        assert!(matches!(
            &cancellation.entries[0].1,
            BatchCancellationStatus::Requested
        ));
        assert!(
            cancellation.entries[1..]
                .iter()
                .all(|(_, status)| matches!(status, BatchCancellationStatus::NotAdmitted { .. }))
        );
        assert_eq!(
            reopened
                .retained_batch_cancellation(cancelled.batch_id)
                .await?
                .ok_or_else(|| Error::NotFound("cancellation declaration".into()))?
                .group_id,
            cancelled.group_id
        );
        let after_cancel = reopened.admit_batch(cancelled.clone()).await?;
        assert!(matches!(
            after_cancel.as_slice(),
            [
                Admission::Accepted(_),
                Admission::Rejected { .. },
                Admission::Rejected { .. }
            ]
        ));
        assert_eq!(
            reopened.reconcile_batch(cancelled.clone()).await?,
            Some(after_cancel)
        );
        let repeated = reopened
            .cancel_batch(cancelled.batch_id)
            .await?
            .ok_or_else(|| Error::NotFound("replayed cancellation".into()))?;
        assert_eq!(repeated.entries.len(), cancelled.inputs.len());
        assert!(matches!(
            &repeated.entries[0].1,
            BatchCancellationStatus::Requested
        ));
        assert_eq!(
            reopened
                .batch_cancellation_stream(cancelled.batch_id)?
                .bounds()
                .await?
                .tail,
            1
        );
        let mut changed = request.clone();
        changed.inputs.swap(0, 1);
        assert!(matches!(
            reopened.reconcile_batch(changed).await,
            Err(Error::Conflict(_))
        ));
        // A retained control path must still contain exactly one record.
        // Full Stream history remains readable even when that invariant fails.
        manifest
            .append_at(Bytes::from_static(b"unexpected-second-record"), 1)
            .await?;
        assert!(matches!(
            reopened.retained_batch(request.batch_id).await,
            Err(Error::Storage(_))
        ));
        assert_eq!(
            manifest
                .read(0, 2)
                .await?
                .try_collect::<Vec<_>>()
                .await?
                .len(),
            2
        );
        Ok(())
    }
}
