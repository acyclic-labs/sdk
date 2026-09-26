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
        DurableEffectObserver, DurableTaskHost, InputKey, RuntimeScope, TaskAdmissionRecord,
        TaskChild, TaskChildrenPage, TaskRegistry, ToolPolicy, check_tool_approval, read_granted,
        require_descendant_grant, validate_policy_identity, validate_task_schemas,
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
}

impl<P: StreamProvider> CoordinatorTaskHost<P> {
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
        })
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
            coordinator.observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation_id,
            )?
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
        if bounds.trim_point != 0 || bounds.tail != 1 {
            return Err(Error::Storage(
                "batch cancellation history is not exactly retained".into(),
            ));
        }
        let records = stream
            .read(0, 2)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
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
        if bounds.trim_point != 0 || bounds.tail != 1 {
            return Err(Error::Storage(
                "batch manifest history is not exactly retained".into(),
            ));
        }
        let records = stream
            .read(0, 2)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
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
        if request.inputs.len() > 65_536
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
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
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
            let outcome = self
                .coordinator
                .lock()
                .await
                .declare_operation(
                    &self.owner,
                    &self.owner_scope,
                    &self.verifier,
                    spec,
                    IdempotencyKey::new(key)?,
                )
                .await;
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
            if after < bounds.trim_point {
                return Err(Error::Conflict(
                    "inbox history was trimmed before this cursor".into(),
                ));
            }
            if after > bounds.tail {
                return Err(Error::Invalid("inbox cursor is beyond the tail".into()));
            }
            let page_limit = u32::try_from(limit)
                .map_err(|_| Error::Invalid("inbox page bound is invalid".into()))?;
            let page = match mailbox.read(after, page_limit).await {
                Ok(records) => records
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?,
                Err(StreamError::NotFound) => return Ok(Vec::new()),
                Err(StreamError::PrefixNotRetained) => {
                    return Err(Error::Conflict(
                        "inbox history was trimmed before this cursor".into(),
                    ));
                }
                Err(error) => return Err(Error::Storage(error.to_string())),
            };
            let mut items = Vec::with_capacity(page.len());
            for record in page {
                let value: Value = serde_json::from_slice(&record.value)
                    .map_err(|error| Error::Storage(error.to_string()))?;
                if crate::contract::canonical_json_bytes(&value)
                    .map_err(|error| Error::Storage(error.to_string()))?
                    != record.value.as_ref()
                {
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
        assert_eq!(
            manifest
                .bounds()
                .await
                .map_err(|error| Error::Storage(error.to_string()))?
                .tail,
            1
        );
        let records = manifest
            .read(0, 2)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
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
                .await
                .map_err(|error| Error::Storage(error.to_string()))?
                .tail,
            1
        );
        let mut changed = request;
        changed.inputs.swap(0, 1);
        assert!(matches!(
            reopened.reconcile_batch(changed).await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }
}
