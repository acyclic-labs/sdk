//! Owner-bound Stream coordinator adapter for typed durable task admission.

use crate::{
    Admission, Capabilities, EffectId, Error, IdempotencyKey, InteractionId, OperationId, Outcome,
    Result, TaskId,
    conversation::{ContentResidencyVerifier, FileRef, Limits},
    core::{Authority, AuthorityVerifier, Scope},
    distributed::{DistributedCoordinator, SchedulerPayloadStore},
    durable_tool::DurableToolRunner,
    executor::ExecutionJournal,
    interaction::{Interaction, InteractionOutcome},
    registry::ComponentIdentity,
    runtime::{
        DurableEffectObserver, DurableTaskHost, RuntimeScope, ToolPolicy, check_tool_approval,
        read_granted, validate_policy_identity,
    },
    scheduler::{
        DurableOwner, EntrypointRef, InboxItem, OperationSpec, Orchestration, ParentLink,
        ResourceRequest,
    },
    tool::ToolDefinition,
    workflow::{MachineIdentity, MachineRegistry},
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

/// Canonical owner-retained admission data. The coordinator event carries only
/// its exact file ref, so neither input nor scope grants enter Stream history.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskAdmission {
    operation_id: OperationId,
    task: ComponentIdentity,
    machine: MachineIdentity,
    input: Value,
    grants: Capabilities,
    limits: Limits,
    policy: Option<ComponentIdentity>,
}

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
    machines: MachineRegistry,
    clock: Arc<dyn UnixMillisClock>,
    tools: Option<Arc<DurableToolRunner>>,
    policy: Option<Arc<dyn ToolPolicy>>,
    interactions: Option<Arc<dyn ExecutionJournal>>,
    effects: Option<Arc<dyn DurableEffectObserver>>,
}

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Binds a trusted owner and immutable admission payload provider.
    pub fn new(
        coordinator: DistributedCoordinator<P>,
        stream: StreamClient<P>,
        payloads: Arc<dyn SchedulerPayloadStore>,
        reader: Arc<dyn ContentResidencyVerifier>,
        owner: Authority,
        owner_scope: Scope,
        verifier: AuthorityVerifier,
        root_scope: RuntimeScope,
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
    #[must_use]
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

    async fn admission(&self, operation_id: OperationId) -> Result<TaskAdmission> {
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
        let value = self.read_json(&state.spec.state).await?;
        let admission: TaskAdmission =
            serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
        if admission.policy != self.policy.as_ref().map(|policy| policy.identity()) {
            return Err(Error::Conflict(
                "durable task policy differs from its admission".into(),
            ));
        }
        if admission.operation_id != operation_id
            || admission.task.name != state.spec.entrypoint.name
            || admission.task.version != state.spec.entrypoint.version
            || admission.task.digest != state.spec.entrypoint.digest
            || admission.machine.name != admission.task.name
            || admission.machine.version != admission.task.version
            || self.machines.resolve(&admission.machine).is_none()
        {
            return Err(Error::Conflict(
                "durable task admission disagrees with its declaration".into(),
            ));
        }
        Ok(admission)
    }

    fn mailbox(&self, task_id: TaskId) -> Result<acyclic_stream::Stream<P>> {
        self.stream
            .stream(format!("harness/v2/mail/{task_id}"))
            .map_err(|error| Error::Invalid(error.to_string()))
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
                if records.len() != 1
                    || records[0].sequence != receipt.start
                    || records[0].commit_id != receipt.commit_id
                    || records[0].value.as_ref() != bytes
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

    fn observe_identity<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<ComponentIdentity>> {
        Box::pin(async move {
            let operation_id = OperationId::from_bytes(task_id.into_bytes());
            Ok(self.admission(operation_id).await?.task)
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
            self.root_scope.narrow(admission.grants, admission.limits)
        })
    }

    fn admit<'a>(
        &'a self,
        operation_id: OperationId,
        identity: ComponentIdentity,
        machine: MachineIdentity,
        input: Value,
        output_schema: Value,
        parent: Option<TaskId>,
        scope: RuntimeScope,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
        Box::pin(async move {
            self.root_scope
                .narrow(scope.grants().clone(), scope.limits())?;
            if machine.name != identity.name
                || machine.version != identity.version
                || machine.digest == [0; 32]
                || self.machines.resolve(&machine).is_none()
            {
                return Err(Error::Invalid(
                    "task and machine identities disagree".into(),
                ));
            }
            jsonschema::validator_for(&output_schema)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let admission = TaskAdmission {
                operation_id,
                task: identity.clone(),
                machine,
                input,
                grants: scope.grants().clone(),
                limits: scope.limits(),
                policy: self.policy.as_ref().map(|policy| policy.identity()),
            };
            let canonical = serde_json::to_value(&admission)
                .map_err(|error| Error::Invalid(error.to_string()))?;
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
            let page = match mailbox.read(after, limit as u32).await {
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
                .run_with_context(task_id, operation_id, definition, arguments, Some(context))
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
