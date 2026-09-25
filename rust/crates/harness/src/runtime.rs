//! Typed, version-pinned task admission over the live and durable primitives.

use crate::{
    Admission, Capabilities, EffectId, Error, OperationId, Outcome, Result, TaskId,
    conversation::{ContentPublisher, ContentResidencyVerifier, FileRef, Limits, VolumeOperation},
    interaction::{Interaction, InteractionOutcome},
    live::{TaskGroup, TaskHandle},
    registry::{ComponentIdentity, validate_component_label},
    scheduler::InboxItem,
    tool::{ToolDefinition, ToolInvocation, ToolRegistry, validate_value},
    workflow::{MachineIdentity, ResumableMachine},
};
use futures::future::BoxFuture;
use futures::{StreamExt as _, stream};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    future::Future,
    marker::PhantomData,
    sync::Arc,
};

type LiveHandler<I, O> = dyn Fn(TaskContext, I) -> BoxFuture<'static, Result<O>> + Send + Sync;

enum TaskImplementation<I, O> {
    Live(Arc<LiveHandler<I, O>>),
    Resumable(Arc<dyn ResumableMachine>),
}

/// Immutable typed task definition. Live code is process-local; only an exact
/// registered resumable machine can be admitted through a durable host.
pub struct TaskDefinition<I, O> {
    identity: ComponentIdentity,
    input_schema: Value,
    output_schema: Value,
    requirements: BTreeSet<String>,
    implementation: TaskImplementation<I, O>,
    types: PhantomData<fn(I) -> O>,
}

impl<I, O> TaskDefinition<I, O> {
    /// Adapts an ordinary async function without claiming that its stack is durable.
    pub fn live<F, Fut>(
        name: impl Into<String>,
        version: impl Into<String>,
        handler: F,
    ) -> Result<Self>
    where
        F: Fn(TaskContext, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        let name = name.into();
        let version = version.into();
        validate_identity(&name, &version)?;
        let mut definition = Self {
            identity: ComponentIdentity {
                name,
                version,
                digest: [0; 32],
            },
            input_schema: serde_json::json!({}),
            output_schema: serde_json::json!({}),
            requirements: BTreeSet::new(),
            implementation: TaskImplementation::Live(Arc::new(move |context, input| {
                Box::pin(handler(context, input))
            })),
            types: PhantomData,
        };
        definition.refresh_digest()?;
        Ok(definition)
    }

    /// Binds a pinned state machine and its exact admission/result schemas.
    /// Durable work cannot inherit the permissive local-task defaults because
    /// both boundaries must remain interpretable after a process restart.
    pub fn resumable(
        machine: Arc<dyn ResumableMachine>,
        input_schema: Value,
        output_schema: Value,
    ) -> Result<Self> {
        let pinned = machine.identity();
        validate_identity(&pinned.name, &pinned.version)?;
        if pinned.digest == [0; 32] {
            return Err(Error::Invalid(
                "resumable task requires an implementation digest".into(),
            ));
        }
        validate_task_schemas(&input_schema, &output_schema, true)?;
        let mut definition = Self {
            identity: ComponentIdentity {
                name: pinned.name.clone(),
                version: pinned.version.clone(),
                digest: [0; 32],
            },
            input_schema,
            output_schema,
            requirements: BTreeSet::new(),
            implementation: TaskImplementation::Resumable(machine),
            types: PhantomData,
        };
        definition.refresh_digest()?;
        Ok(definition)
    }

    /// Pins both public JSON Schemas before registration.
    pub fn schemas(mut self, input: Value, output: Value) -> Result<Self> {
        validate_task_schemas(
            &input,
            &output,
            matches!(&self.implementation, TaskImplementation::Resumable(_)),
        )?;
        self.input_schema = input;
        self.output_schema = output;
        self.refresh_digest()?;
        Ok(self)
    }

    /// Declares a dependency available at admission.
    pub fn requires(mut self, requirement: impl Into<String>) -> Result<Self> {
        let requirement = requirement.into();
        if requirement.is_empty() || requirement.chars().any(char::is_whitespace) {
            return Err(Error::Invalid("task requirement is invalid".into()));
        }
        self.requirements.insert(requirement);
        self.refresh_digest()?;
        Ok(self)
    }

    fn refresh_digest(&mut self) -> Result<()> {
        let implementation = match &self.implementation {
            TaskImplementation::Live(_) => None,
            TaskImplementation::Resumable(machine) => Some(machine.identity().digest),
        };
        self.identity.digest = crate::contract::canonical_json_digest(&serde_json::json!({
            "contract": "harness.task.v2",
            "name": &self.identity.name,
            "version": &self.identity.version,
            "input_schema": &self.input_schema,
            "output_schema": &self.output_schema,
            "requirements": &self.requirements,
            "machine_digest": implementation,
        }))?;
        Ok(())
    }

    /// Returns the version and implementation identity pinned at admission.
    #[must_use]
    pub const fn identity(&self) -> &ComponentIdentity {
        &self.identity
    }
}

fn validate_task_schemas(input: &Value, output: &Value, resumable: bool) -> Result<()> {
    for (name, schema) in [("input", input), ("output", output)] {
        jsonschema::validator_for(schema)
            .map_err(|error| Error::Invalid(format!("invalid task {name} schema: {error}")))?;
        if resumable
            && (schema == &Value::Bool(true)
                || schema.as_object().is_some_and(serde_json::Map::is_empty))
        {
            return Err(Error::Invalid(format!(
                "resumable task {name} schema cannot accept every value"
            )));
        }
    }
    Ok(())
}

fn validate_identity(name: &str, version: &str) -> Result<()> {
    validate_component_label(name, "task name")?;
    validate_component_label(version, "task version")?;
    if name.contains('@') || version.contains('@') {
        return Err(Error::Invalid(
            "task name and version cannot contain the version separator".into(),
        ));
    }
    Ok(())
}

struct TaskEntry {
    identity: ComponentIdentity,
    requirements: BTreeSet<String>,
    definition: Arc<dyn Any + Send + Sync>,
}

/// Immutable typed definitions indexed by their exact name and version.
#[derive(Default)]
pub struct TaskRegistry(BTreeMap<(String, String), TaskEntry>);

impl TaskRegistry {
    /// Registers a single exact task version and rejects ambiguous aliases.
    pub fn register<I: 'static, O: 'static>(
        &mut self,
        definition: TaskDefinition<I, O>,
    ) -> Result<()>
    where
        TaskDefinition<I, O>: Send + Sync,
    {
        let name = definition.identity.name.clone();
        let version = definition.identity.version.clone();
        if self.0.contains_key(&(name.clone(), version.clone())) {
            return Err(Error::Conflict(format!(
                "task {name}@{version} is already registered"
            )));
        }
        self.0.insert(
            (name, version),
            TaskEntry {
                identity: definition.identity.clone(),
                requirements: definition.requirements.clone(),
                definition: Arc::new(definition),
            },
        );
        Ok(())
    }

    /// Resolves one Rust input/output pair. An unqualified name is accepted
    /// only when exactly one version is bound; `name@version` is always exact.
    pub fn get<I: 'static, O: 'static>(&self, name: &str) -> Result<Arc<TaskDefinition<I, O>>> {
        let stored = if let Some((logical, version)) = name.rsplit_once('@') {
            self.0
                .get(&(logical.to_owned(), version.to_owned()))
                .ok_or_else(|| Error::NotFound(format!("task {name}")))?
        } else {
            let mut matches = self.0.iter().filter(|((logical, _), _)| logical == name);
            let first = matches
                .next()
                .ok_or_else(|| Error::NotFound(format!("task {name}")))?;
            if matches.next().is_some() {
                return Err(Error::Conflict(format!(
                    "task {name} requires an exact version"
                )));
            }
            first.1
        };
        Arc::clone(&stored.definition)
            .downcast::<TaskDefinition<I, O>>()
            .map_err(|_| Error::Conflict(format!("task {name} has different input/output types")))
    }

    /// Resolves an exact pinned version without relying on name parsing.
    pub fn get_version<I: 'static, O: 'static>(
        &self,
        name: &str,
        version: &str,
    ) -> Result<Arc<TaskDefinition<I, O>>> {
        let stored = self
            .0
            .get(&(name.to_owned(), version.to_owned()))
            .ok_or_else(|| Error::NotFound(format!("task {name}@{version}")))?;
        Arc::clone(&stored.definition)
            .downcast::<TaskDefinition<I, O>>()
            .map_err(|_| {
                Error::Conflict(format!(
                    "task {name}@{version} has different input/output types"
                ))
            })
    }
}

/// Provider boundary for stable durable admission and outcome observation.
/// The host stages input before committing ref-only operation state and returns
/// `Indeterminate` when an acknowledgement is lost; callers reconcile by ID.
pub trait DurableTaskHost: Send + Sync {
    /// Policy identity enforced by this host at durable tool dispatch.
    fn policy_identity(&self) -> Option<ComponentIdentity> {
        None
    }

    /// Proves the exact definition recorded at admission before a typed
    /// observer may be reconstructed from a stable task identity.
    fn observe_identity<'a>(
        &'a self,
        _task_id: TaskId,
    ) -> BoxFuture<'a, Result<ComponentIdentity>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable task identity observation is not bound".into(),
            ))
        })
    }

    /// Authenticates a resumed task against its stable admission identity and
    /// the caller's pinned authority before a context is reconstructed.
    fn resume_scope<'a>(
        &'a self,
        _task_id: TaskId,
        _operation_id: OperationId,
    ) -> BoxFuture<'a, Result<RuntimeScope>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable task context authentication is not bound".into(),
            ))
        })
    }

    /// Admits or reconciles one exact implementation and input.
    fn admit<'a>(
        &'a self,
        operation_id: OperationId,
        identity: ComponentIdentity,
        machine: MachineIdentity,
        input: Value,
        output_schema: Value,
        parent: Option<TaskId>,
        scope: RuntimeScope,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>>;
    /// Observes a previously admitted task without re-executing it. `None`
    /// means ordinary pending work, never an unknown external outcome.
    fn outcome<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<Option<Outcome<Value>>>>;
    /// Waits for a terminal observation. Hosts with completion notification
    /// can override this bounded-poll fallback without changing task handles.
    fn wait_outcome<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<Outcome<Value>>> {
        Box::pin(async move {
            let mut delay_ms = 50_u64;
            loop {
                if let Some(outcome) = self.outcome(task_id).await? {
                    return Ok(outcome);
                }
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                delay_ms = delay_ms.saturating_mul(2).min(1_000);
            }
        })
    }
    /// Requests cancellation; an acknowledgement does not assert completion.
    fn cancel<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<()>>;

    /// Routes an addressable interaction through the durable owner journal.
    fn interact<'a>(
        &'a self,
        _task_id: TaskId,
        _operation_id: OperationId,
        _interaction: Interaction,
    ) -> BoxFuture<'a, Result<InteractionOutcome>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable interaction routing is not bound".into(),
            ))
        })
    }

    /// Publishes one ref-only, idempotent task message.
    fn send<'a>(
        &'a self,
        _sender: TaskId,
        _recipient: TaskId,
        _message_id: OperationId,
        _payload: FileRef,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Err(Error::Unsupported("durable task mail is not bound".into())) })
    }

    /// Reads a bounded, ordered page of committed inbox items.
    fn inbox<'a>(
        &'a self,
        _task_id: TaskId,
        _after: u64,
        _limit: usize,
    ) -> BoxFuture<'a, Result<Vec<InboxItem>>> {
        Box::pin(async { Err(Error::Unsupported("durable task inbox is not bound".into())) })
    }

    /// Registers or reconciles an absolute durable timer by operation identity.
    fn wait_until<'a>(
        &'a self,
        _task_id: TaskId,
        _operation_id: OperationId,
        _deadline_unix_ms: u64,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Err(Error::Unsupported("durable timers are not bound".into())) })
    }

    /// Observes an existing effect without dispatching it again.
    fn reconcile_effect<'a>(
        &'a self,
        _task_id: TaskId,
        _effect_id: EffectId,
    ) -> BoxFuture<'a, Result<crate::core::EffectStatus>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable effect reconciliation is not bound".into(),
            ))
        })
    }

    /// Reconciles or executes one exact, policy-checked tool operation. The
    /// host records intent before dispatch and returns uncertainty explicitly.
    fn execute_tool<'a>(
        &'a self,
        _task_id: TaskId,
        _operation_id: OperationId,
        _definition: ToolDefinition,
        _arguments: Value,
        _scope: RuntimeScope,
        _context: ToolContext,
    ) -> BoxFuture<'a, Result<Outcome<Value>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable tool execution is not bound".into(),
            ))
        })
    }
}

/// Replaceable owner-bound observer for an already planned provider effect.
/// Reconciliation never creates a new dispatch attempt.
pub trait DurableEffectObserver: Send + Sync {
    /// Returns the latest attested status after querying the pinned attempt.
    fn reconcile<'a>(
        &'a self,
        effect_id: EffectId,
    ) -> BoxFuture<'a, Result<crate::core::EffectStatus>>;
}

/// Replaceable local interaction router. Durable interactions instead use the
/// host's recorded request/answer boundary.
pub trait InteractionRouter: Send + Sync {
    /// Returns a typed outcome while retaining request and answer bytes in its
    /// own provider; callers must not journal either body.
    fn route<'a>(
        &'a self,
        operation_id: OperationId,
        interaction: Interaction,
    ) -> BoxFuture<'a, Result<InteractionOutcome>>;
}

/// Effective runtime authority and bounds; child scopes may only narrow them.
#[derive(Clone)]
pub struct RuntimeScope {
    grants: Capabilities,
    limits: Limits,
}

impl Default for RuntimeScope {
    fn default() -> Self {
        Self {
            grants: Capabilities::default(),
            limits: Limits::default(),
        }
    }
}

impl RuntimeScope {
    /// Creates a checked root scope.
    pub fn new(grants: Capabilities, limits: Limits) -> Result<Self> {
        limits.validate()?;
        Ok(Self { grants, limits })
    }

    /// Attenuates every grant and numeric bound.
    pub fn narrow(&self, grants: Capabilities, limits: Limits) -> Result<Self> {
        limits.validate()?;
        if !grants.is_subset_of(&self.grants)
            || limits.file_bytes > self.limits.file_bytes
            || limits.path_bytes > self.limits.path_bytes
            || limits.attachments > self.limits.attachments
            || limits.render_bytes > self.limits.render_bytes
            || limits.model_steps > self.limits.model_steps
            || limits.model_events_per_step > self.limits.model_events_per_step
            || limits.tool_calls_per_step > self.limits.tool_calls_per_step
            || limits.context_messages > self.limits.context_messages
        {
            return Err(Error::Unauthorized(
                "runtime scope cannot widen ancestor authority".into(),
            ));
        }
        Ok(Self { grants, limits })
    }

    /// Returns the exact effective grants.
    #[must_use]
    pub const fn grants(&self) -> &Capabilities {
        &self.grants
    }

    /// Returns the exact effective bounds.
    #[must_use]
    pub const fn limits(&self) -> Limits {
        self.limits
    }
}

/// Local or durable typed handle; durable observation is host-authoritative.
pub enum RuntimeTask<O> {
    /// Process-local task; its future is never persisted.
    Live(TaskHandle<Result<O>>),
    /// Host-backed task with pinned output validation.
    Durable {
        /// Stable task address at the durable host.
        task_id: TaskId,
        /// Provider that resolves the task after process recovery.
        host: Arc<dyn DurableTaskHost>,
        /// Pinned schema for the task result.
        output_schema: Value,
    },
}

impl<O> RuntimeTask<O> {
    /// Stable identity of the admitted local operation or durable task.
    #[must_use]
    pub fn identity(&self) -> String {
        match self {
            Self::Live(handle) => handle.id().to_string(),
            Self::Durable { task_id, .. } => task_id.to_string(),
        }
    }
}

/// Observes every admitted task in input order without cancelling siblings.
pub async fn join_runtime<O: DeserializeOwned + Send + 'static>(
    tasks: Vec<RuntimeTask<O>>,
) -> Vec<Result<Outcome<O>>> {
    let concurrency = tasks.len().clamp(1, 64);
    stream::iter(tasks)
        .map(|task| async move { task.result().await })
        .buffered(concurrency)
        .collect()
        .await
}

/// Folds observed outcomes in admission order, regardless of completion order.
pub async fn ordered_reduce_runtime<O: DeserializeOwned + Send + 'static, A, F>(
    tasks: Vec<RuntimeTask<O>>,
    initial: A,
    reducer: F,
) -> A
where
    F: FnMut(A, Result<Outcome<O>>) -> A,
{
    join_runtime(tasks).await.into_iter().fold(initial, reducer)
}

/// Observes whichever task terminates first. Other tasks remain admitted.
pub async fn race_runtime<O: DeserializeOwned + Send + 'static>(
    tasks: Vec<RuntimeTask<O>>,
) -> Option<(String, Result<Outcome<O>>)> {
    let concurrency = tasks.len().clamp(1, 64);
    stream::iter(tasks)
        .map(|task| async move {
            let id = task.identity();
            (id, task.result().await)
        })
        .buffer_unordered(concurrency)
        .next()
        .await
}

/// Returns the first success or every observed non-successful outcome.
pub async fn first_success_runtime<O: DeserializeOwned + Send + 'static>(
    tasks: Vec<RuntimeTask<O>>,
) -> std::result::Result<O, Vec<(String, Result<Outcome<O>>)>> {
    let concurrency = tasks.len().clamp(1, 64);
    let mut completions = stream::iter(tasks)
        .map(|task| async move {
            let id = task.identity();
            (id, task.result().await)
        })
        .buffer_unordered(concurrency);
    let mut others = Vec::new();
    while let Some((id, result)) = completions.next().await {
        match result {
            Ok(Outcome::Succeeded(value)) => return Ok(value),
            result => others.push((id, result)),
        }
    }
    Err(others)
}

/// Returns the first required successes or every observed non-success.
pub async fn quorum_runtime<O: DeserializeOwned + Send + 'static>(
    tasks: Vec<RuntimeTask<O>>,
    required: usize,
) -> std::result::Result<Vec<O>, Vec<(String, Result<Outcome<O>>)>> {
    if required == 0 {
        return Ok(Vec::new());
    }
    let concurrency = tasks.len().clamp(1, 64);
    let mut completions = stream::iter(tasks)
        .map(|task| async move {
            let id = task.identity();
            (id, task.result().await)
        })
        .buffer_unordered(concurrency);
    let mut values = Vec::with_capacity(required);
    let mut others = Vec::new();
    while let Some((id, result)) = completions.next().await {
        match result {
            Ok(Outcome::Succeeded(value)) => {
                values.push(value);
                if values.len() == required {
                    return Ok(values);
                }
            }
            result => others.push((id, result)),
        }
    }
    Err(others)
}

impl<O: DeserializeOwned> RuntimeTask<O> {
    /// Requests cancellation without claiming a confirmed cancelled outcome.
    pub async fn cancel(&self) -> Result<()> {
        match self {
            Self::Live(handle) => {
                handle.cancel();
                Ok(())
            }
            Self::Durable { task_id, host, .. } => host.cancel(*task_id).await,
        }
    }

    /// Waits for a terminal outcome, preserving uncertainty and cancellation.
    pub async fn result(self) -> Result<Outcome<O>> {
        match self {
            Self::Live(handle) => Ok(match handle.result().await {
                Outcome::Succeeded(Ok(value)) => Outcome::Succeeded(value),
                Outcome::Succeeded(Err(Error::Indeterminate(operation_id))) => {
                    Outcome::Indeterminate { operation_id }
                }
                Outcome::Succeeded(Err(error)) => Outcome::Failed {
                    message: error.to_string(),
                },
                Outcome::Failed { message } => Outcome::Failed { message },
                Outcome::Cancelled => Outcome::Cancelled,
                Outcome::Indeterminate { operation_id } => Outcome::Indeterminate { operation_id },
            }),
            Self::Durable {
                task_id,
                host,
                output_schema,
            } => match host.wait_outcome(task_id).await? {
                Outcome::Succeeded(value) => {
                    validate_value(&output_schema, &value, "task output")?;
                    let value = serde_json::from_value(value)
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    Ok(Outcome::Succeeded(value))
                }
                Outcome::Failed { message } => Ok(Outcome::Failed { message }),
                Outcome::Cancelled => Ok(Outcome::Cancelled),
                Outcome::Indeterminate { operation_id } => {
                    Ok(Outcome::Indeterminate { operation_id })
                }
            },
        }
    }
}

/// Immutable typed runtime assembled from task/tool registries and providers.
pub struct AgentHarness {
    tasks: TaskRegistry,
    tools: ToolRegistry,
    live: TaskGroup,
    concurrency: usize,
    scope: RuntimeScope,
    host: Option<Arc<dyn DurableTaskHost>>,
    interactions: Option<Arc<dyn InteractionRouter>>,
    content: Option<ContentBindings>,
    policy: Option<Arc<dyn ToolPolicy>>,
    policy_identity: Option<ComponentIdentity>,
}

/// An admission decision for one exact, schema-valid tool invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ToolPolicyDecision {
    /// The invocation may proceed under its existing capability grant.
    Allow,
    /// The invocation is rejected before dispatch.
    Deny {
        /// Explanation retained with the denied decision.
        reason: String,
    },
    /// An addressable approval must resolve for this exact action.
    RequireApproval {
        /// Ref-free text used to create the approval request.
        prompt: String,
    },
}

/// Replaceable policy evaluated after schema and scope validation, before dispatch.
pub trait ToolPolicy: Send + Sync {
    /// Immutable implementation identity pinned across admission and replay.
    fn identity(&self) -> ComponentIdentity;
    /// Policy implementations must be deterministic for an admitted revision.
    fn evaluate<'a>(
        &'a self,
        invocation: &'a ToolInvocation,
        scope: &'a RuntimeScope,
    ) -> BoxFuture<'a, Result<ToolPolicyDecision>>;
}

pub(crate) fn validate_policy_identity(identity: &ComponentIdentity) -> Result<()> {
    validate_component_label(&identity.name, "policy name")?;
    validate_component_label(&identity.version, "policy version")?;
    if identity.digest == [0; 32] {
        return Err(Error::Invalid(
            "policy implementation digest is empty".into(),
        ));
    }
    Ok(())
}

pub(crate) fn check_tool_approval(outcome: InteractionOutcome) -> Result<()> {
    match outcome {
        InteractionOutcome::Approved => Ok(()),
        InteractionOutcome::Indeterminate { operation_id } => {
            Err(Error::Indeterminate(operation_id))
        }
        InteractionOutcome::Declined => Err(Error::InteractionRejected(
            crate::InteractionRejection::Declined,
        )),
        InteractionOutcome::Cancelled => Err(Error::InteractionRejected(
            crate::InteractionRejection::Cancelled,
        )),
        InteractionOutcome::Expired => Err(Error::InteractionRejected(
            crate::InteractionRejection::Expired,
        )),
        InteractionOutcome::Denied => Err(Error::InteractionRejected(
            crate::InteractionRejection::Denied,
        )),
        InteractionOutcome::Answered { .. } => Err(Error::Conflict(
            "approval resolved with a question answer".into(),
        )),
    }
}

/// Provider-owned, scope-bound file handles. The reader verifies every exact
/// ref; the optional writer can only publish into its original owner's volume.
#[derive(Clone)]
pub struct ContentBindings {
    /// Authenticated, exact-version read and residency boundary.
    pub reader: Arc<dyn ContentResidencyVerifier>,
    /// Authenticated writer for one volume; read-only agents omit it.
    pub writer: Option<Arc<dyn ContentPublisher>>,
}

/// Immutable admission bindings assembled before tasks can run. Each optional
/// provider boundary remains independently replaceable.
pub struct Bindings {
    /// Version-pinned typed task definitions.
    pub tasks: TaskRegistry,
    /// Version-pinned tool definitions and executors.
    pub tools: ToolRegistry,
    /// Effective authority and bounds.
    pub scope: RuntimeScope,
    /// Maximum simultaneously running local members in one group.
    pub concurrency: usize,
    /// Optional durable scheduler and effect/wait boundary.
    pub durable_host: Option<Arc<dyn DurableTaskHost>>,
    /// Optional local interaction router.
    pub interactions: Option<Arc<dyn InteractionRouter>>,
    /// Optional owner-bound content access for tasks and tools.
    pub content: Option<ContentBindings>,
    /// Optional invocation policy; absence permits calls with explicit grants.
    pub policy: Option<Arc<dyn ToolPolicy>>,
}

impl Bindings {
    /// Validates all dependency edges and seals the runtime composition.
    pub fn build(self) -> Result<Arc<AgentHarness>> {
        AgentHarness::with_policy(
            self.tasks,
            self.tools,
            self.scope,
            self.concurrency,
            self.durable_host,
            self.interactions,
            self.content,
            self.policy,
        )
    }
}

impl AgentHarness {
    /// Validates task dependencies once before accepting work.
    pub fn new(
        tasks: TaskRegistry,
        tools: ToolRegistry,
        scope: RuntimeScope,
        concurrency: usize,
        host: Option<Arc<dyn DurableTaskHost>>,
    ) -> Result<Arc<Self>> {
        Self::with_interactions(tasks, tools, scope, concurrency, host, None)
    }

    /// Composes the same typed runtime with independently replaceable local
    /// interaction routing.
    pub fn with_interactions(
        tasks: TaskRegistry,
        tools: ToolRegistry,
        scope: RuntimeScope,
        concurrency: usize,
        host: Option<Arc<dyn DurableTaskHost>>,
        interactions: Option<Arc<dyn InteractionRouter>>,
    ) -> Result<Arc<Self>> {
        Self::with_content(tasks, tools, scope, concurrency, host, interactions, None)
    }

    /// Binds exact owner-mediated content access without coupling Harness core
    /// to Filesystem, Objects, or another concrete storage implementation.
    pub fn with_content(
        tasks: TaskRegistry,
        tools: ToolRegistry,
        scope: RuntimeScope,
        concurrency: usize,
        host: Option<Arc<dyn DurableTaskHost>>,
        interactions: Option<Arc<dyn InteractionRouter>>,
        content: Option<ContentBindings>,
    ) -> Result<Arc<Self>> {
        Self::with_policy(
            tasks,
            tools,
            scope,
            concurrency,
            host,
            interactions,
            content,
            None,
        )
    }

    /// Seals a replaceable tool policy into the immutable runtime composition.
    pub fn with_policy(
        tasks: TaskRegistry,
        tools: ToolRegistry,
        scope: RuntimeScope,
        concurrency: usize,
        host: Option<Arc<dyn DurableTaskHost>>,
        interactions: Option<Arc<dyn InteractionRouter>>,
        content: Option<ContentBindings>,
        policy: Option<Arc<dyn ToolPolicy>>,
    ) -> Result<Arc<Self>> {
        if concurrency == 0 {
            return Err(Error::Invalid(
                "runtime concurrency must be positive".into(),
            ));
        }
        if let Some(policy) = &policy {
            validate_policy_identity(&policy.identity())?;
        }
        if host.as_ref().is_some_and(|host| {
            host.policy_identity() != policy.as_ref().map(|policy| policy.identity())
        }) {
            return Err(Error::Conflict(
                "runtime and durable host policies differ".into(),
            ));
        }
        validate_task_dependencies(
            &tasks,
            &tools,
            &scope,
            host.is_some(),
            interactions.is_some(),
            content.as_ref(),
            policy.is_some(),
        )?;
        let policy_identity = policy.as_ref().map(|policy| policy.identity());
        Ok(Arc::new(Self {
            tasks,
            tools,
            live: TaskGroup::new(concurrency),
            concurrency,
            scope,
            host,
            interactions,
            content,
            policy,
            policy_identity,
        }))
    }

    /// Resolves one registered task with exact Rust input/output types.
    pub fn task<I: 'static, O: 'static>(&self, name: &str) -> Result<Arc<TaskDefinition<I, O>>> {
        self.tasks.get(name)
    }

    /// Resolves one model-visible tool definition by exact registered name.
    pub fn tool(&self, name: &str) -> Result<ToolDefinition> {
        self.tools
            .get(name)
            .map(|tool| tool.definition.clone())
            .ok_or_else(|| Error::NotFound(format!("tool {name}")))
    }

    /// Admits a process-local task after validating its registration and input.
    pub async fn spawn<I, O>(
        self: &Arc<Self>,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
    ) -> Result<RuntimeTask<O>>
    where
        I: Serialize + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        self.spawn_in_group(
            &self.live,
            self.scope.clone(),
            Vec::new(),
            definition,
            input,
        )
        .await
    }

    async fn spawn_in_group<I, O>(
        self: &Arc<Self>,
        group: &TaskGroup,
        scope: RuntimeScope,
        policies: Vec<(ComponentIdentity, Arc<dyn ToolPolicy>)>,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
    ) -> Result<RuntimeTask<O>>
    where
        I: Serialize + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        let registered = self
            .tasks
            .get_version::<I, O>(&definition.identity.name, &definition.identity.version)?;
        if !Arc::ptr_eq(&registered, definition) {
            return Err(Error::Conflict(
                "task definition is not the registered version".into(),
            ));
        }
        let TaskImplementation::Live(handler) = &definition.implementation else {
            return Err(Error::Unsupported(
                "resumable task needs stable durable admission".into(),
            ));
        };
        validate_value(
            &definition.input_schema,
            &serde_json::to_value(&input).map_err(|error| Error::Invalid(error.to_string()))?,
            "task input",
        )?;
        let handler = Arc::clone(handler);
        let output_schema = definition.output_schema.clone();
        let runtime = Arc::clone(self);
        let context_group = group.clone();
        let (identity, receive_identity) = tokio::sync::oneshot::channel();
        let admitted = group
            .try_spawn(async move {
                let task_id = receive_identity
                    .await
                    .map_err(|_| Error::Conflict("task identity was not admitted".into()))?;
                let output = handler(
                    TaskContext {
                        harness: runtime,
                        task_id,
                        durable_task: None,
                        group: context_group,
                        scope,
                        policy_overrides: policies,
                    },
                    input,
                )
                .await?;
                validate_value(
                    &output_schema,
                    &serde_json::to_value(&output)
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                    "task output",
                )?;
                Ok(output)
            })
            .await;
        let handle = match admitted {
            Admission::Accepted(handle) => handle,
            Admission::Rejected { reason } => return Err(Error::Conflict(reason)),
            Admission::Indeterminate { operation_id } => {
                return Err(Error::Indeterminate(operation_id));
            }
        };
        let _ = identity.send(*handle.id());
        Ok(RuntimeTask::Live(handle))
    }

    /// Admits a pinned resumable task through a host; live futures are rejected.
    pub async fn admit<I, O>(
        self: &Arc<Self>,
        operation_id: OperationId,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
        parent: Option<TaskId>,
    ) -> Result<Admission<RuntimeTask<O>>>
    where
        I: Serialize + Send + 'static,
        O: DeserializeOwned + Send + 'static,
    {
        self.admit_scoped(operation_id, definition, input, parent, self.scope.clone())
            .await
    }

    /// Reattaches only to an admission pinned to this registered resumable
    /// definition. The caller cannot choose an output type by assertion.
    pub async fn attach<I: 'static, O: DeserializeOwned + Send + 'static>(
        &self,
        task_id: TaskId,
        definition: &Arc<TaskDefinition<I, O>>,
    ) -> Result<RuntimeTask<O>> {
        let registered = self
            .tasks
            .get_version::<I, O>(&definition.identity.name, &definition.identity.version)?;
        if !Arc::ptr_eq(&registered, definition)
            || !matches!(&definition.implementation, TaskImplementation::Resumable(_))
        {
            return Err(Error::Conflict(
                "task definition is not the registered resumable version".into(),
            ));
        }
        let host = self
            .host
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task host is not bound".into()))?;
        if host.observe_identity(task_id).await? != definition.identity {
            return Err(Error::Conflict(
                "durable task identity differs from the pinned definition".into(),
            ));
        }
        Ok(RuntimeTask::Durable {
            task_id,
            host: Arc::clone(host),
            output_schema: definition.output_schema.clone(),
        })
    }

    async fn admit_scoped<I, O>(
        self: &Arc<Self>,
        operation_id: OperationId,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
        parent: Option<TaskId>,
        scope: RuntimeScope,
    ) -> Result<Admission<RuntimeTask<O>>>
    where
        I: Serialize + Send + 'static,
        O: DeserializeOwned + Send + 'static,
    {
        let registered = self
            .tasks
            .get_version::<I, O>(&definition.identity.name, &definition.identity.version)?;
        if !Arc::ptr_eq(&registered, definition) {
            return Err(Error::Conflict(
                "task definition is not the registered version".into(),
            ));
        }
        let TaskImplementation::Resumable(machine) = &definition.implementation else {
            return Ok(Admission::Rejected {
                reason: "live tasks are local-only".into(),
            });
        };
        let Some(host) = &self.host else {
            return Ok(Admission::Rejected {
                reason: "durable task host is not bound".into(),
            });
        };
        let value =
            serde_json::to_value(input).map_err(|error| Error::Invalid(error.to_string()))?;
        validate_value(&definition.input_schema, &value, "task input")?;
        Ok(
            match host
                .admit(
                    operation_id,
                    definition.identity.clone(),
                    machine.identity().clone(),
                    value,
                    definition.output_schema.clone(),
                    parent,
                    scope,
                )
                .await?
            {
                Admission::Accepted(task_id) => Admission::Accepted(RuntimeTask::Durable {
                    task_id,
                    host: Arc::clone(host),
                    output_schema: definition.output_schema.clone(),
                }),
                Admission::Rejected { reason } => Admission::Rejected { reason },
                Admission::Indeterminate { operation_id } => {
                    Admission::Indeterminate { operation_id }
                }
            },
        )
    }

    /// Explicitly cancels this runtime's local task group.
    pub fn cancel_local(&self) {
        self.live.cancel();
    }

    /// Reconstructs a pinned task context after the host resumes its state machine.
    pub async fn durable_context(
        self: &Arc<Self>,
        task_id: TaskId,
        operation_id: OperationId,
    ) -> Result<TaskContext> {
        let host = self
            .host
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task host is not bound".into()))?;
        let admitted = host.resume_scope(task_id, operation_id).await?;
        let scope = self.scope.narrow(admitted.grants, admitted.limits)?;
        Ok(TaskContext {
            harness: Arc::clone(self),
            task_id: operation_id,
            durable_task: Some(task_id),
            group: self.live.clone(),
            scope,
            policy_overrides: Vec::new(),
        })
    }
}

fn validate_task_dependencies(
    tasks: &TaskRegistry,
    tools: &ToolRegistry,
    scope: &RuntimeScope,
    has_host: bool,
    has_interactions: bool,
    content: Option<&ContentBindings>,
    has_policy: bool,
) -> Result<()> {
    fn visit(
        key: &(String, String),
        tasks: &TaskRegistry,
        tools: &ToolRegistry,
        scope: &RuntimeScope,
        has_host: bool,
        has_interactions: bool,
        content: Option<&ContentBindings>,
        has_policy: bool,
        marks: &mut BTreeMap<(String, String), u8>,
    ) -> Result<()> {
        match marks.get(key).copied() {
            Some(1) => {
                return Err(Error::Invalid(format!(
                    "task dependency cycle at {}@{}",
                    key.0, key.1
                )));
            }
            Some(2) => return Ok(()),
            _ => {}
        }
        marks.insert(key.clone(), 1);
        if let Some(entry) = tasks.0.get(key) {
            for requirement in &entry.requirements {
                if let Some(target) = requirement.strip_prefix("task:") {
                    let (target, version) = target.rsplit_once('@').ok_or_else(|| {
                        Error::Invalid(format!("invalid task dependency {requirement}"))
                    })?;
                    let target_key = (target.to_owned(), version.to_owned());
                    if !tasks.0.contains_key(&target_key) {
                        return Err(Error::Invalid(format!(
                            "missing task dependency {requirement}"
                        )));
                    }
                    visit(
                        &target_key,
                        tasks,
                        tools,
                        scope,
                        has_host,
                        has_interactions,
                        content,
                        has_policy,
                        marks,
                    )?;
                } else if let Some(target) = requirement.strip_prefix("tool:") {
                    let (target, version) = target.rsplit_once('@').ok_or_else(|| {
                        Error::Invalid(format!("invalid tool dependency {requirement}"))
                    })?;
                    if tools.get_version(target, version).is_none() {
                        return Err(Error::Invalid(format!(
                            "missing tool dependency {requirement}"
                        )));
                    }
                } else if requirement == "host" && has_host
                    || requirement == "model" && scope.grants.contains("model:generate")
                    || requirement == "context" && scope.grants.contains("context:build")
                    || requirement == "interactions"
                        && (has_interactions || has_host)
                        && scope.grants.contains("interaction:route")
                    || requirement == "policy" && has_policy
                    || requirement == "content" && content.is_some()
                    || requirement == "content:write"
                        && content
                            .and_then(|binding| binding.writer.as_ref())
                            .is_some_and(|writer| {
                                writer
                                    .volume()
                                    .capability(VolumeOperation::Write)
                                    .is_ok_and(|grant| scope.grants.contains(&grant))
                            })
                    || requirement
                        .strip_prefix("grant:")
                        .is_some_and(|grant| scope.grants.contains(grant))
                {
                    // The pinned composition satisfies this provider/capability dependency.
                } else {
                    return Err(Error::Invalid(format!(
                        "task {} has missing dependency {requirement}",
                        entry.identity.name
                    )));
                }
            }
        }
        marks.insert(key.clone(), 2);
        Ok(())
    }
    let mut marks = BTreeMap::new();
    for key in tasks.0.keys() {
        visit(
            key,
            tasks,
            tools,
            scope,
            has_host,
            has_interactions,
            content,
            has_policy,
            &mut marks,
        )?;
    }
    Ok(())
}

/// Capability-scoped context passed to live task code.
#[derive(Clone)]
pub struct TaskContext {
    harness: Arc<AgentHarness>,
    task_id: OperationId,
    durable_task: Option<TaskId>,
    group: TaskGroup,
    scope: RuntimeScope,
    policy_overrides: Vec<(ComponentIdentity, Arc<dyn ToolPolicy>)>,
}

impl TaskContext {
    /// Returns this invocation's stable local operation identity.
    #[must_use]
    pub const fn id(&self) -> OperationId {
        self.task_id
    }

    /// Admitted durable owner, absent for a process-local task.
    #[must_use]
    pub const fn durable_task_id(&self) -> Option<TaskId> {
        self.durable_task
    }

    /// Returns the exact effective authority and bounds.
    #[must_use]
    pub const fn scope(&self) -> &RuntimeScope {
        &self.scope
    }

    /// Produces an attenuated view for descendants and tool calls.
    pub fn scoped(&self, grants: Capabilities, limits: Limits) -> Result<Self> {
        let mut child = self.clone();
        child.scope = self.scope.narrow(grants, limits)?;
        Ok(child)
    }

    /// Adds a locally pinned policy layer. Durable tasks cannot change their
    /// owner's admitted policy identity after admission.
    pub fn scoped_policy(&self, policy: Arc<dyn ToolPolicy>) -> Result<Self> {
        if self.durable_task.is_some() {
            return Err(Error::Unsupported(
                "durable scoped policy requires owner-host admission".into(),
            ));
        }
        let identity = policy.identity();
        validate_policy_identity(&identity)?;
        if let Some(parent) = &self.harness.policy
            && self.harness.policy_identity.as_ref() == Some(&identity)
        {
            if Arc::ptr_eq(parent, &policy) {
                return Ok(self.clone());
            }
            return Err(Error::Conflict(
                "scoped policy identity is already bound to another implementation".into(),
            ));
        }
        for (existing, bound) in &self.policy_overrides {
            if existing == &identity {
                if Arc::ptr_eq(bound, &policy) {
                    return Ok(self.clone());
                }
                return Err(Error::Conflict(
                    "scoped policy identity is already bound to another implementation".into(),
                ));
            }
        }
        let mut child = self.clone();
        child.policy_overrides.push((identity, policy));
        Ok(child)
    }

    /// Resolves one registered task without widening its scope.
    pub fn task<I: 'static, O: 'static>(&self, name: &str) -> Result<Arc<TaskDefinition<I, O>>> {
        self.harness.task(name)
    }

    /// Resolves one pinned tool contract with a Rust input/output view.
    pub fn tool<I, O>(&self, name: &str) -> Result<ToolRef<I, O>> {
        Ok(ToolRef {
            definition: self.harness.tool(name)?,
            types: PhantomData,
        })
    }

    /// Reads one immutable file through the owner's authenticated provider.
    /// A ref alone does not grant access; the effective task scope must retain
    /// a whole-volume, exact-file, or bounded-directory read capability.
    pub async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>> {
        file.validate()?;
        self.scope.limits.validate_file(file)?;
        if !read_granted(&self.scope.grants, file)? {
            return Err(Error::Unauthorized(
                "task scope cannot read this file".into(),
            ));
        }
        let content = self
            .harness
            .content
            .as_ref()
            .ok_or_else(|| Error::Unsupported("content reader is not bound".into()))?;
        let bytes = content.reader.read(file).await?;
        file.descriptor().verify(&bytes)?;
        Ok(bytes)
    }

    /// Stages exact bytes before a task may publish their ref. The provider is
    /// bound to one original writer and the task scope cannot widen that grant.
    pub async fn stage_file(
        &self,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
    ) -> Result<FileRef> {
        if bytes.len() as u64 > self.scope.limits.file_bytes
            || path.len() > self.scope.limits.path_bytes
        {
            return Err(Error::Invalid("file exceeds task content limits".into()));
        }
        let content = self
            .harness
            .content
            .as_ref()
            .ok_or_else(|| Error::Unsupported("content publisher is not bound".into()))?;
        let writer = content
            .writer
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("task has no owner-bound content writer".into()))?;
        if !self
            .scope
            .grants
            .contains(&writer.volume().capability(VolumeOperation::Write)?)
        {
            return Err(Error::Unauthorized(
                "task scope cannot write this volume".into(),
            ));
        }
        let file = writer
            .stage(operation_id, path, bytes, media_type, display_name)
            .await?;
        self.scope.limits.validate_file(&file)?;
        if file.volume() != writer.volume()
            || file.path() != path
            || file.display_name() != display_name
            || file.descriptor().media_type() != media_type
        {
            return Err(Error::Storage(
                "content publisher returned another file identity".into(),
            ));
        }
        file.descriptor().verify(bytes)?;
        content.reader.verify(&file).await?;
        Ok(file)
    }

    /// Calls a local tool under this task's scope and validates both schemas.
    /// Durable calls use the host's recorded effect boundary, not this live path.
    pub async fn call<I, O>(&self, tool: &ToolRef<I, O>, input: I) -> Result<O>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        if self.durable_task.is_some() {
            return Err(Error::Unsupported(
                "durable tool calls require recorded effect admission".into(),
            ));
        }
        let binding = self
            .harness
            .tools
            .get_version(&tool.definition.name, &tool.definition.revision)
            .ok_or_else(|| Error::NotFound(format!("tool {}", tool.definition.name)))?;
        if binding.definition != tool.definition {
            return Err(Error::Conflict(
                "tool definition changed after lookup".into(),
            ));
        }
        let required = format!("tool:call:{}", tool.definition.name);
        if !self.scope.grants.contains(&required) {
            return Err(Error::Unauthorized(format!("scope lacks {required}")));
        }
        let arguments =
            serde_json::to_value(input).map_err(|error| Error::Invalid(error.to_string()))?;
        validate_value(&tool.definition.input_schema, &arguments, "tool input")?;
        let invocation = ToolInvocation {
            call_id: OperationId::new().to_string(),
            name: tool.definition.name.clone(),
            arguments,
        };
        self.authorize_tool(&tool.definition, &invocation).await?;
        let context = ToolContext::new(self.clone(), invocation.call_id.clone())?;
        let result = binding
            .executor
            .execute_with_context(context, invocation)
            .await?;
        validate_value(&tool.definition.output_schema, &result.value, "tool output")?;
        serde_json::from_value(result.value).map_err(|error| Error::Invalid(error.to_string()))
    }

    /// Executes a durable tool under a stable operation ID. A lost outcome is
    /// represented as indeterminate, never retried through the live executor.
    pub async fn call_durable<I, O>(
        &self,
        operation_id: OperationId,
        tool: &ToolRef<I, O>,
        input: I,
    ) -> Result<Outcome<O>>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let task_id = self.durable_task.ok_or_else(|| {
            Error::Unsupported("durable tool calls require an admitted task".into())
        })?;
        let binding = self
            .harness
            .tools
            .get_version(&tool.definition.name, &tool.definition.revision)
            .ok_or_else(|| Error::NotFound(format!("tool {}", tool.definition.name)))?;
        if binding.definition != tool.definition {
            return Err(Error::Conflict(
                "tool definition changed after lookup".into(),
            ));
        }
        let required = format!("tool:call:{}", tool.definition.name);
        if !self.scope.grants.contains(&required) {
            return Err(Error::Unauthorized(format!("scope lacks {required}")));
        }
        let arguments =
            serde_json::to_value(input).map_err(|error| Error::Invalid(error.to_string()))?;
        validate_value(&tool.definition.input_schema, &arguments, "tool input")?;
        let host = self
            .harness
            .host
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task host is not bound".into()))?;
        let context = ToolContext::new(self.clone(), operation_id.to_string())?;
        Ok(
            match host
                .execute_tool(
                    task_id,
                    operation_id,
                    tool.definition.clone(),
                    arguments,
                    self.scope.clone(),
                    context,
                )
                .await?
            {
                Outcome::Succeeded(value) => {
                    validate_value(&tool.definition.output_schema, &value, "tool output")?;
                    Outcome::Succeeded(
                        serde_json::from_value(value)
                            .map_err(|error| Error::Invalid(error.to_string()))?,
                    )
                }
                Outcome::Failed { message } => Outcome::Failed { message },
                Outcome::Cancelled => Outcome::Cancelled,
                Outcome::Indeterminate { operation_id } => Outcome::Indeterminate { operation_id },
            },
        )
    }

    /// Applies the bound policy to one exact action before its executor is dispatched.
    /// The durable host invokes this again at its own authoritative boundary;
    /// clients cannot bypass it by calling the host directly.
    pub(crate) async fn authorize_tool(
        &self,
        definition: &ToolDefinition,
        invocation: &ToolInvocation,
    ) -> Result<()> {
        if let Some(policy) = &self.harness.policy {
            if self.harness.policy_identity.as_ref() != Some(&policy.identity()) {
                return Err(Error::Conflict(
                    "runtime policy identity changed after binding".into(),
                ));
            }
            if let Some((approval_id, request)) = self
                .policy_approval(policy.as_ref(), definition, invocation)
                .await?
            {
                check_tool_approval(self.interact(approval_id, request).await?)?;
            }
        }
        for (identity, policy) in &self.policy_overrides {
            if &policy.identity() != identity {
                return Err(Error::Conflict(
                    "scoped policy identity changed after binding".into(),
                ));
            }
            if let Some((approval_id, request)) = self
                .policy_approval(policy.as_ref(), definition, invocation)
                .await?
            {
                check_tool_approval(self.interact(approval_id, request).await?)?;
            }
        }
        Ok(())
    }

    /// Computes a policy decision without routing it through a caller-owned
    /// host. Durable owners route the returned request on their own boundary.
    pub(crate) async fn policy_approval(
        &self,
        policy: &dyn ToolPolicy,
        definition: &ToolDefinition,
        invocation: &ToolInvocation,
    ) -> Result<Option<(OperationId, Interaction)>> {
        let identity = policy.identity();
        let decision = policy.evaluate(invocation, &self.scope).await?;
        if policy.identity() != identity {
            return Err(Error::Conflict(
                "policy identity changed during evaluation".into(),
            ));
        }
        match decision {
            ToolPolicyDecision::Allow => Ok(None),
            ToolPolicyDecision::Deny { reason } => Err(Error::Unauthorized(reason)),
            ToolPolicyDecision::RequireApproval { prompt } => {
                let digest = crate::contract::canonical_json_digest(&(
                    &self.task_id,
                    identity,
                    definition,
                    invocation,
                ))?;
                let mut id_bytes = [0_u8; 16];
                id_bytes.copy_from_slice(
                    &blake3::hash(
                        &[b"harness:tool-approval:v2".as_slice(), digest.as_slice()].concat(),
                    )
                    .as_bytes()[..16],
                );
                let approval_id = OperationId::from_bytes(id_bytes);
                let action_id = OperationId::parse(&invocation.call_id)?;
                Ok(Some((
                    approval_id,
                    Interaction::approval(prompt, action_id, digest)?,
                )))
            }
        }
    }

    /// Spawns a local child; durable work must use stable host admission instead.
    pub async fn spawn<I, O>(
        &self,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
    ) -> Result<RuntimeTask<O>>
    where
        I: Serialize + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        if self.durable_task.is_some() {
            return Err(Error::Unsupported(
                "durable descendants need stable admission".into(),
            ));
        }
        // A parent may await its child while holding its own group permit.
        // An independently bounded descendant group prevents recursive
        // fork-join from deadlocking at concurrency one.
        let descendants = self.group.child(self.harness.concurrency);
        self.harness
            .spawn_in_group(
                &descendants,
                self.scope.clone(),
                self.policy_overrides.clone(),
                definition,
                input,
            )
            .await
    }

    /// Returns a separately cancellable bounded group for additional local work.
    #[must_use]
    pub fn group(&self) -> RuntimeGroup {
        RuntimeGroup {
            harness: Arc::clone(&self.harness),
            group: self.group.child(self.harness.concurrency),
            scope: self.scope.clone(),
            policy_overrides: self.policy_overrides.clone(),
        }
    }

    /// Admits a durable child using an explicit stable operation identity.
    pub async fn admit<I, O>(
        &self,
        operation_id: OperationId,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
    ) -> Result<Admission<RuntimeTask<O>>>
    where
        I: Serialize + Send + 'static,
        O: DeserializeOwned + Send + 'static,
    {
        let parent = self.durable_task.ok_or_else(|| {
            Error::Unsupported("a live task cannot claim durable descendant ownership".into())
        })?;
        self.harness
            .admit_scoped(
                operation_id,
                definition,
                input,
                Some(parent),
                self.scope.clone(),
            )
            .await
    }

    /// Routes an interaction by stable identity; its request/answer bytes stay in owner storage.
    pub async fn interact(
        &self,
        operation_id: OperationId,
        interaction: Interaction,
    ) -> Result<InteractionOutcome> {
        interaction.validate()?;
        if !self.scope.grants.contains("interaction:route") {
            return Err(Error::Unauthorized("scope lacks interaction:route".into()));
        }
        if let Some(task) = self.durable_task {
            let host = self
                .harness
                .host
                .as_ref()
                .ok_or_else(|| Error::Unsupported("durable task host is not bound".into()))?;
            host.interact(task, operation_id, interaction).await
        } else {
            let router = self.harness.interactions.as_ref().ok_or_else(|| {
                Error::Unsupported("local interaction router is not bound".into())
            })?;
            router.route(operation_id, interaction).await
        }
    }

    /// Opens a schema-constrained question by stable identity. The outcome
    /// carries an immutable answer ref, never the participant's inline text.
    pub async fn ask(
        &self,
        operation_id: OperationId,
        prompt: impl Into<String>,
        response_schema: Value,
    ) -> Result<InteractionOutcome> {
        self.interact(
            operation_id,
            Interaction::question(prompt, response_schema)?,
        )
        .await
    }

    /// Sends one immutable ref to a task; the host enforces recipient access.
    pub async fn send(
        &self,
        recipient: TaskId,
        message_id: OperationId,
        payload: FileRef,
    ) -> Result<()> {
        payload.validate()?;
        if !self.scope.grants.contains("mail:send") {
            return Err(Error::Unauthorized("task scope lacks mail:send".into()));
        }
        let sender = self
            .durable_task
            .ok_or_else(|| Error::Unsupported("mail requires a durable task".into()))?;
        let host = self
            .harness
            .host
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task host is not bound".into()))?;
        host.send(sender, recipient, message_id, payload).await
    }

    /// Reads a bounded durable inbox page after the last observed sequence.
    pub async fn inbox(&self, after: u64, limit: usize) -> Result<Vec<InboxItem>> {
        if limit == 0 || limit > 1_024 {
            return Err(Error::Invalid("inbox page bound is invalid".into()));
        }
        if !self.scope.grants.contains("mail:read") {
            return Err(Error::Unauthorized("task scope lacks mail:read".into()));
        }
        let task = self
            .durable_task
            .ok_or_else(|| Error::Unsupported("inbox requires a durable task".into()))?;
        let host = self
            .harness
            .host
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task host is not bound".into()))?;
        host.inbox(task, after, limit).await
    }

    /// Waits at a recorded absolute timer boundary; live tasks use Tokio time.
    pub async fn sleep_until(
        &self,
        operation_id: OperationId,
        deadline_unix_ms: u64,
    ) -> Result<()> {
        if deadline_unix_ms == 0 {
            return Err(Error::Invalid("timer deadline is invalid".into()));
        }
        if !self.scope.grants.contains("timer:wait") {
            return Err(Error::Unauthorized("task scope lacks timer:wait".into()));
        }
        if let Some(task) = self.durable_task {
            let host = self
                .harness
                .host
                .as_ref()
                .ok_or_else(|| Error::Unsupported("durable task host is not bound".into()))?;
            host.wait_until(task, operation_id, deadline_unix_ms).await
        } else {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .as_millis();
            let remaining = u128::from(deadline_unix_ms).saturating_sub(now);
            let remaining = u64::try_from(remaining).unwrap_or(u64::MAX);
            tokio::time::sleep(std::time::Duration::from_millis(remaining)).await;
            Ok(())
        }
    }

    /// Queries an existing effect by identity and never redispatches it.
    pub async fn reconcile_effect(&self, effect_id: EffectId) -> Result<crate::core::EffectStatus> {
        let task = self.durable_task.ok_or_else(|| {
            Error::Unsupported("effect reconciliation requires a durable task".into())
        })?;
        let host = self
            .harness
            .host
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task host is not bound".into()))?;
        host.reconcile_effect(task, effect_id).await
    }
}

pub(crate) fn read_granted(grants: &Capabilities, file: &FileRef) -> Result<bool> {
    if grants.contains(&file.read_capability()?)
        || grants.contains(&file.volume().capability(VolumeOperation::Read)?)
    {
        return Ok(true);
    }
    if file
        .volume()
        .directory_read_capability("")
        .is_ok_and(|capability| grants.contains(&capability))
    {
        return Ok(true);
    }
    let mut prefix = String::new();
    for segment in file
        .path()
        .split('/')
        .take(file.path().split('/').count().saturating_sub(1))
    {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(segment);
        if file
            .volume()
            .directory_read_capability(&prefix)
            .is_ok_and(|capability| grants.contains(&capability))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Typed live group whose cancellation is independent of sibling groups.
pub struct RuntimeGroup {
    harness: Arc<AgentHarness>,
    group: TaskGroup,
    scope: RuntimeScope,
    policy_overrides: Vec<(ComponentIdentity, Arc<dyn ToolPolicy>)>,
}

/// A typed view of one version-pinned tool registration.
pub struct ToolRef<I, O> {
    definition: ToolDefinition,
    types: PhantomData<fn(I) -> O>,
}

impl<I, O> ToolRef<I, O> {
    /// Returns the exact model-visible contract pinned at lookup.
    #[must_use]
    pub const fn definition(&self) -> &ToolDefinition {
        &self.definition
    }
}

impl RuntimeGroup {
    /// Admits one registered typed member into this group.
    pub async fn spawn<I, O>(
        &self,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
    ) -> Result<RuntimeTask<O>>
    where
        I: Serialize + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        self.harness
            .spawn_in_group(
                &self.group,
                self.scope.clone(),
                self.policy_overrides.clone(),
                definition,
                input,
            )
            .await
    }

    /// Closes admission while already accepted members and descendants drain.
    pub fn close(&self) {
        self.group.close();
    }

    /// Requests cancellation of this group's members and descendants.
    pub fn cancel(&self) {
        self.group.cancel();
    }
}

/// A task context bound to one exact tool call identity.
#[derive(Clone)]
pub struct ToolContext {
    task: TaskContext,
    call_id: String,
}

impl ToolContext {
    /// Creates a tool context only for a nonempty stable call identity.
    pub fn new(task: TaskContext, call_id: impl Into<String>) -> Result<Self> {
        let call_id = call_id.into();
        if call_id.trim().is_empty() {
            return Err(Error::Invalid("tool call ID is empty".into()));
        }
        Ok(Self { task, call_id })
    }

    /// Returns the enclosing scoped task context.
    #[must_use]
    pub const fn task(&self) -> &TaskContext {
        &self.task
    }

    /// Returns the exact call identity.
    #[must_use]
    pub fn call_id(&self) -> &str {
        &self.call_id
    }
}

impl std::ops::Deref for ToolContext {
    type Target = TaskContext;

    fn deref(&self) -> &Self::Target {
        &self.task
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef};
    use crate::resources::ProviderRef;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FixedPolicy {
        identity: ComponentIdentity,
        decision: ToolPolicyDecision,
    }

    impl ToolPolicy for FixedPolicy {
        fn identity(&self) -> ComponentIdentity {
            self.identity.clone()
        }

        fn evaluate<'a>(
            &'a self,
            _invocation: &'a ToolInvocation,
            _scope: &'a RuntimeScope,
        ) -> BoxFuture<'a, Result<ToolPolicyDecision>> {
            let decision = self.decision.clone();
            Box::pin(async move { Ok(decision) })
        }
    }

    #[tokio::test]
    async fn scoped_live_policy_preserves_ancestor_and_blocks_denied_tool() -> Result<()> {
        let root_policy: Arc<dyn ToolPolicy> = Arc::new(FixedPolicy {
            identity: ComponentIdentity {
                name: "root.policy".into(),
                version: "1".into(),
                digest: [1; 32],
            },
            decision: ToolPolicyDecision::Allow,
        });
        let child_policy: Arc<dyn ToolPolicy> = Arc::new(FixedPolicy {
            identity: ComponentIdentity {
                name: "child.policy".into(),
                version: "1".into(),
                digest: [2; 32],
            },
            decision: ToolPolicyDecision::Deny {
                reason: "child veto".into(),
            },
        });
        let scope = RuntimeScope::new(
            Capabilities::new(["tool:call:test.echo"]),
            Limits::default(),
        )?;
        let harness = AgentHarness::with_policy(
            TaskRegistry::default(),
            ToolRegistry::new(),
            scope.clone(),
            1,
            None,
            None,
            None,
            Some(root_policy),
        )?;
        let context = TaskContext {
            harness,
            task_id: OperationId::new(),
            durable_task: None,
            group: TaskGroup::new(1),
            scope,
            policy_overrides: Vec::new(),
        };
        let narrowed = context.scoped_policy(child_policy)?;
        let definition = ToolDefinition {
            name: "test.echo".into(),
            revision: "1".into(),
            description: "Echo".into(),
            input_schema: serde_json::json!({}),
            output_schema: serde_json::json!({}),
        };
        let invocation = ToolInvocation {
            call_id: OperationId::new().to_string(),
            name: definition.name.clone(),
            arguments: serde_json::json!({}),
        };
        assert!(
            matches!(narrowed.authorize_tool(&definition, &invocation).await,
            Err(Error::Unauthorized(reason)) if reason == "child veto")
        );
        assert_eq!(narrowed.policy_overrides.len(), 1);
        Ok(())
    }

    struct TestMachine {
        identity: MachineIdentity,
        state_schema: Value,
    }

    impl ResumableMachine for TestMachine {
        fn identity(&self) -> &MachineIdentity {
            &self.identity
        }
        fn state_schema(&self) -> &Value {
            &self.state_schema
        }
        fn initialize(&self, input: &Value) -> Result<Value> {
            Ok(input.clone())
        }
        fn transition(
            &self,
            state: &Value,
            _input: &Value,
        ) -> Result<crate::workflow::MachineTransition> {
            Ok(crate::workflow::MachineTransition {
                state: state.clone(),
                commands: Vec::new(),
                status: crate::workflow::MachineStatus::Suspended,
            })
        }
    }

    #[test]
    fn resumable_task_requires_pinned_nontrivial_input_and_output_schemas() -> Result<()> {
        let machine: Arc<dyn ResumableMachine> = Arc::new(TestMachine {
            identity: MachineIdentity {
                name: "test.machine".into(),
                version: "1".into(),
                digest: [1; 32],
            },
            state_schema: serde_json::json!({"type": "object"}),
        });
        let input = serde_json::json!({"type": "object", "required": ["value"]});
        let output = serde_json::json!({"type": "integer"});
        assert!(
            TaskDefinition::<Value, u32>::resumable(
                machine.clone(),
                serde_json::json!({}),
                output.clone()
            )
            .is_err()
        );
        assert!(
            TaskDefinition::<Value, u32>::resumable(
                machine.clone(),
                input.clone(),
                Value::Bool(true)
            )
            .is_err()
        );
        let definition = TaskDefinition::<Value, u32>::resumable(machine, input, output)?;
        assert!(
            definition
                .schemas(
                    serde_json::json!({}),
                    serde_json::json!({"type": "integer"})
                )
                .is_err()
        );
        Ok(())
    }

    struct IdentityHost {
        identity: ComponentIdentity,
        polls: Arc<AtomicUsize>,
    }

    impl DurableTaskHost for IdentityHost {
        fn observe_identity<'a>(
            &'a self,
            _task_id: TaskId,
        ) -> BoxFuture<'a, Result<ComponentIdentity>> {
            Box::pin(async { Ok(self.identity.clone()) })
        }
        fn admit<'a>(
            &'a self,
            _operation_id: OperationId,
            _identity: ComponentIdentity,
            _machine: MachineIdentity,
            _input: Value,
            _output_schema: Value,
            _parent: Option<TaskId>,
            _scope: RuntimeScope,
        ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
            Box::pin(async {
                Ok(Admission::Rejected {
                    reason: "unused".into(),
                })
            })
        }
        fn outcome<'a>(
            &'a self,
            _task_id: TaskId,
        ) -> BoxFuture<'a, Result<Option<Outcome<Value>>>> {
            Box::pin(async move {
                if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(None)
                } else {
                    Ok(Some(Outcome::Succeeded(serde_json::json!(7))))
                }
            })
        }
        fn cancel<'a>(&'a self, _task_id: TaskId) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn durable_reattachment_requires_the_exact_admitted_definition() -> Result<()> {
        let make = |name: &str, digest: [u8; 32]| -> Result<TaskDefinition<Value, u32>> {
            TaskDefinition::resumable(
                Arc::new(TestMachine {
                    identity: MachineIdentity {
                        name: name.into(),
                        version: "1".into(),
                        digest,
                    },
                    state_schema: serde_json::json!({"type": "object"}),
                }),
                serde_json::json!({"type": "object"}),
                serde_json::json!({"type": "integer"}),
            )
        };
        let mut tasks = TaskRegistry::default();
        tasks.register(make("test.first", [1; 32])?)?;
        tasks.register(make("test.second", [2; 32])?)?;
        let first = tasks.get::<Value, u32>("test.first")?;
        let admitted = first.identity().clone();
        let polls = Arc::new(AtomicUsize::new(0));
        let harness = Bindings {
            tasks,
            tools: ToolRegistry::new(),
            scope: RuntimeScope::new(Capabilities::new([] as [String; 0]), Limits::default())?,
            concurrency: 1,
            durable_host: Some(Arc::new(IdentityHost {
                identity: admitted,
                polls: Arc::clone(&polls),
            })),
            interactions: None,
            content: None,
            policy: None,
        }
        .build()?;
        let second = harness.task::<Value, u32>("test.second")?;
        let task_id = TaskId::from_bytes([5; 16]);
        assert!(matches!(
            harness.attach(task_id, &second).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(
            harness.attach(task_id, &first).await?.result().await?,
            Outcome::Succeeded(7)
        );
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test]
    fn private_root_directory_grant_reads_descendants_without_write_authority() -> Result<()> {
        let volume = VolumeRef::new(
            ProviderRef::new("local", "filesystem", "2")?,
            "private",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([77; 16])),
        )?;
        let file = FileRef::new(
            volume.clone(),
            "notes/nested/message.txt",
            "version-1",
            FileDescriptor::from_bytes(b"hello", "text/plain")?,
            "message.txt",
        )?;
        let grants = Capabilities::new([volume.directory_read_capability("")?]);
        assert!(read_granted(&grants, &file)?);
        assert!(!grants.contains(&volume.capability(VolumeOperation::Write)?));
        Ok(())
    }

    #[test]
    fn local_task_names_share_the_bounded_component_contract() -> Result<()> {
        TaskDefinition::live("leaf", "1", |_context, value: u32| async move { Ok(value) })?;
        assert!(
            TaskDefinition::live(
                "bad/name",
                "1",
                |_context, value: u32| async move { Ok(value) }
            )
            .is_err()
        );
        assert!(
            TaskDefinition::live("leaf", "bad version", |_context, value: u32| async move {
                Ok(value)
            })
            .is_err()
        );
        assert!(
            TaskDefinition::live("leaf@other", "1", |_context, value: u32| async move {
                Ok(value)
            })
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn pinned_task_versions_coexist_without_implicit_latest_selection() -> Result<()> {
        let mut tasks = TaskRegistry::default();
        tasks.register(TaskDefinition::live(
            "versioned",
            "1",
            |_, value: u32| async move { Ok(value + 1) },
        )?)?;
        tasks.register(TaskDefinition::live(
            "versioned",
            "2",
            |_, value: u32| async move { Ok(value + 2) },
        )?)?;
        assert!(matches!(
            tasks.get::<u32, u32>("versioned"),
            Err(Error::Conflict(_))
        ));
        let harness = Bindings {
            tasks,
            tools: ToolRegistry::new(),
            scope: RuntimeScope::new(Capabilities::new([] as [String; 0]), Limits::default())?,
            concurrency: 2,
            durable_host: None,
            interactions: None,
            content: None,
            policy: None,
        }
        .build()?;
        for (version, expected) in [("1", 11), ("2", 12)] {
            let definition = harness.task::<u32, u32>(&format!("versioned@{version}"))?;
            assert_eq!(
                harness.spawn(&definition, 10).await?.result().await?,
                Outcome::Succeeded(expected)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn recursive_child_admission_does_not_consume_the_parent_permit() -> Result<()> {
        let mut tasks = TaskRegistry::default();
        let definition =
            TaskDefinition::live("test.recursive", "1", |context, depth: u32| async move {
                if depth == 0 {
                    return Ok(0_u32);
                }
                let definition = context.task::<u32, u32>("test.recursive")?;
                match context
                    .spawn(&definition, depth - 1)
                    .await?
                    .result()
                    .await?
                {
                    Outcome::Succeeded(value) => Ok(value + 1),
                    outcome => Err(Error::Conflict(format!(
                        "recursive child did not succeed: {outcome:?}"
                    ))),
                }
            })?;
        tasks.register(definition)?;
        let harness = Bindings {
            tasks,
            tools: ToolRegistry::new(),
            scope: RuntimeScope::new(Capabilities::new([] as [String; 0]), Limits::default())?,
            concurrency: 1,
            durable_host: None,
            interactions: None,
            content: None,
            policy: None,
        }
        .build()?;
        let definition = harness.task::<u32, u32>("test.recursive")?;
        let observed = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            harness.spawn(&definition, 128).await?.result(),
        )
        .await
        .map_err(|error| Error::Conflict(error.to_string()))??;
        assert_eq!(observed, Outcome::Succeeded(128));
        Ok(())
    }

    #[tokio::test]
    async fn ordered_join_polls_later_tasks_while_first_is_waiting() -> Result<()> {
        let signal = Arc::new(tokio::sync::Notify::new());
        let mut tasks = TaskRegistry::default();
        let definition = TaskDefinition::live("test.join", "1", {
            let signal = Arc::clone(&signal);
            move |_, wait: bool| {
                let signal = Arc::clone(&signal);
                async move {
                    if wait {
                        signal.notified().await;
                    } else {
                        signal.notify_one();
                    }
                    Ok(wait)
                }
            }
        })?;
        tasks.register(definition)?;
        let harness = Bindings {
            tasks,
            tools: ToolRegistry::new(),
            scope: RuntimeScope::new(Capabilities::new([] as [String; 0]), Limits::default())?,
            concurrency: 2,
            durable_host: None,
            interactions: None,
            content: None,
            policy: None,
        }
        .build()?;
        let definition = harness.task::<bool, bool>("test.join")?;
        let first = harness.spawn(&definition, true).await?;
        let second = harness.spawn(&definition, false).await?;
        let joined = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            join_runtime(vec![first, second]),
        )
        .await
        .map_err(|error| Error::Conflict(error.to_string()))?;
        assert_eq!(joined.len(), 2);
        assert!(matches!(&joined[0], Ok(Outcome::Succeeded(true))));
        assert!(matches!(&joined[1], Ok(Outcome::Succeeded(false))));
        Ok(())
    }

    #[test]
    fn scoped_authority_never_widens_grants_or_limits() -> Result<()> {
        let root = RuntimeScope::new(
            Capabilities::new(["tool:call:test.echo"]),
            Limits::default(),
        )?;
        let mut narrowed = root.limits();
        narrowed.file_bytes /= 2;
        narrowed.render_bytes /= 2;
        let child = root.narrow(Capabilities::new([] as [String; 0]), narrowed)?;
        assert!(!child.grants().contains("tool:call:test.echo"));
        assert!(
            child
                .narrow(Capabilities::new(["tool:call:test.echo"]), narrowed)
                .is_err()
        );
        assert!(
            child
                .narrow(Capabilities::new([] as [String; 0]), root.limits())
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn tool_approval_keeps_terminal_outcomes_distinct() {
        for (outcome, rejection) in [
            (
                InteractionOutcome::Declined,
                crate::InteractionRejection::Declined,
            ),
            (
                InteractionOutcome::Cancelled,
                crate::InteractionRejection::Cancelled,
            ),
            (
                InteractionOutcome::Expired,
                crate::InteractionRejection::Expired,
            ),
            (
                InteractionOutcome::Denied,
                crate::InteractionRejection::Denied,
            ),
        ] {
            assert_eq!(
                check_tool_approval(outcome),
                Err(Error::InteractionRejected(rejection))
            );
        }
        assert_eq!(check_tool_approval(InteractionOutcome::Approved), Ok(()));
    }
}
