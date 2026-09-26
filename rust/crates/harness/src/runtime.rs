//! Typed, version-pinned task admission over the live and durable primitives.

use crate::{
    Admission, BatchId, Capabilities, EffectId, Error, GroupId, InteractionId, OperationId,
    Outcome, Result, TaskId,
    context::{Context, ContextInput, ContextPipeline},
    conversation::{
        ContentPublisher, ContentResidencyVerifier, FileRef, Limits, PrivateDirectoryPage,
        VolumeClass, VolumeOperation, VolumeRef, verified_content_bytes,
    },
    core::{ExtensionAdmission, Reducer, Scope},
    durable_tool::{ResumableToolRegistry, ResumableToolSession},
    executor::ModelEventAdmission,
    extension::{ExtensionLeases, ExtensionRuntime},
    interaction::{
        Interaction, InteractionOutcome, InteractionResolution, InteractionResponse,
        InteractionTicket, ResolutionReceipt,
    },
    live::{TaskGroup, TaskHandle},
    model::{
        Model, ModelContent, ModelContentPart, ModelEvent, ModelMessage, ModelProvider,
        ModelRequest,
    },
    registry::{ComponentIdentity, validate_component_label},
    resources::{ArtifactRef, GenerationRef, SandboxRef},
    scheduler::InboxItem,
    tool::{ToolDefinition, ToolInvocation, ToolRegistry, validate_value},
    workflow::{MachineIdentity, ResumableMachine, WorkflowJournal},
};
use futures::future::BoxFuture;
use futures::{StreamExt as _, stream, stream::BoxStream};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    future::Future,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
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
        self.identity.digest = task_definition_digest(
            &self.identity.name,
            &self.identity.version,
            &self.input_schema,
            &self.output_schema,
            &self.requirements,
            implementation,
        )?;
        Ok(())
    }

    /// Returns the version and implementation identity pinned at admission.
    #[must_use]
    pub const fn identity(&self) -> &ComponentIdentity {
        &self.identity
    }

    /// Returns exact provider/task requirements pinned into this definition.
    #[must_use]
    pub(crate) fn requirements(&self) -> &BTreeSet<String> {
        &self.requirements
    }
}

/// One source of truth for Rust and WASM task registration identities.
pub(crate) fn task_definition_digest(
    name: &str,
    version: &str,
    input_schema: &Value,
    output_schema: &Value,
    requirements: &BTreeSet<String>,
    machine_digest: Option<[u8; 32]>,
) -> Result<[u8; 32]> {
    validate_identity(name, version)?;
    validate_task_schemas(input_schema, output_schema, machine_digest.is_some())?;
    if machine_digest == Some([0; 32])
        || requirements
            .iter()
            .any(|value| value.is_empty() || value.chars().any(char::is_whitespace))
    {
        return Err(Error::Invalid(
            "task dependency or machine digest is invalid".into(),
        ));
    }
    crate::contract::canonical_json_digest(&serde_json::json!({
        "contract": "harness.task.v2",
        "name": name,
        "version": version,
        "input_schema": input_schema,
        "output_schema": output_schema,
        "requirements": requirements,
        "machine_digest": machine_digest,
    }))
}

pub(crate) fn validate_task_schemas(input: &Value, output: &Value, resumable: bool) -> Result<()> {
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

#[derive(Clone)]
struct TaskEntry {
    identity: ComponentIdentity,
    resumable: bool,
    machine: Option<MachineIdentity>,
    input_schema: Value,
    output_schema: Value,
    requirements: BTreeSet<String>,
    definition: Arc<dyn Any + Send + Sync>,
}

/// Immutable typed definitions indexed by their exact name and version.
#[derive(Clone, Default)]
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
                resumable: matches!(&definition.implementation, TaskImplementation::Resumable(_)),
                machine: match &definition.implementation {
                    TaskImplementation::Live(_) => None,
                    TaskImplementation::Resumable(machine) => Some(machine.identity().clone()),
                },
                input_schema: definition.input_schema.clone(),
                output_schema: definition.output_schema.clone(),
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

    pub(crate) fn validate_durable_admission(
        &self,
        identity: &ComponentIdentity,
        machine: &MachineIdentity,
        input_schema: Option<&Value>,
        output_schema: &Value,
        input: Option<&Value>,
    ) -> Result<()> {
        let entry = self
            .0
            .get(&(identity.name.clone(), identity.version.clone()))
            .ok_or_else(|| {
                Error::NotFound(format!("task {}@{}", identity.name, identity.version))
            })?;
        if &entry.identity != identity
            || !entry.resumable
            || entry.machine.as_ref() != Some(machine)
            || input_schema.is_some_and(|schema| schema != &entry.input_schema)
            || &entry.output_schema != output_schema
        {
            return Err(Error::Conflict(
                "durable admission differs from its registered task definition".into(),
            ));
        }
        if let Some(input) = input {
            validate_value(&entry.input_schema, input, "task input")?;
        }
        Ok(())
    }
}

/// Provider-neutral, owner-attested execution route pinned at admission.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPlacement {
    /// Versioned execution provider that qualified and owns this route.
    pub provider: ComponentIdentity,
    /// Immutable registered task build, distinct from the task's logical identity.
    pub build: ArtifactRef,
    /// Ready environment when the route is tied to a specific sandbox.
    pub environment: Option<SandboxRef>,
    /// Nonzero provider attestation over readiness, resources, mounts, and
    /// scoped credential bindings; never a credential or endpoint itself.
    pub readiness_revision: [u8; 32],
}

impl ExecutionPlacement {
    /// Rejects unqualified or structurally invalid placement before admission.
    pub fn validate(&self) -> Result<()> {
        validate_component_label(&self.provider.name, "execution provider name")?;
        validate_component_label(&self.provider.version, "execution provider version")?;
        if self.provider.digest == [0; 32] || self.readiness_revision == [0; 32] {
            return Err(Error::Invalid("execution qualification is empty".into()));
        }
        self.build.validate()?;
        if let Some(environment) = &self.environment {
            environment.validate()?;
        }
        Ok(())
    }
}

/// Exact owner-retained task admission. It is stored as an immutable payload;
/// durable events contain only its content reference.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdmissionRecord {
    /// Idempotent admission operation.
    pub operation_id: OperationId,
    /// Registered task implementation.
    pub task: ComponentIdentity,
    /// Pinned resumable machine.
    pub machine: MachineIdentity,
    /// Schema-validated input, retained outside event history.
    pub input: Value,
    /// Exact schema used to admit the input.
    pub input_schema: Value,
    /// Pinned output contract.
    pub output_schema: Value,
    /// Owning parent, if any.
    pub parent: Option<TaskId>,
    /// Effective grants at admission.
    pub grants: Capabilities,
    /// Effective numeric limits at admission.
    pub limits: Limits,
    /// Effective task concurrency, step, and deadline bounds.
    pub run_limits: TaskRunLimits,
    /// Exact dispatch policy revision.
    pub policy: Option<ComponentIdentity>,
    /// Pinned extension selection.
    pub extensions: Option<ExtensionAdmission>,
    /// Qualified immutable execution route, or the bound local host.
    pub execution: Option<ExecutionPlacement>,
}

/// One owner-retained direct task child, independent of fork ancestry.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskChild {
    /// Stable parent slot, independent of completion order.
    pub slot: String,
    /// Exact admitted child identity.
    pub task_id: TaskId,
}

/// Bounded hierarchy observation; a changed revision invalidates continuation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskChildrenPage {
    /// Retained owner revision observed for this page.
    pub revision: u64,
    /// Direct, same-owner children in slot order.
    pub entries: Vec<TaskChild>,
    /// Last slot to pass with `revision` for the next page.
    pub next_after: Option<String>,
}

impl TaskAdmissionRecord {
    /// Validates the provider-neutral request independently of registration.
    /// The owner additionally checks the exact registered task and authority.
    pub fn validate(&self) -> Result<()> {
        validate_identity(&self.task.name, &self.task.version)?;
        validate_component_label(&self.machine.name, "machine name")?;
        validate_component_label(&self.machine.version, "machine version")?;
        if self.task.digest == [0; 32]
            || self.machine.digest == [0; 32]
            || self.machine.name != self.task.name
            || self.machine.version != self.task.version
        {
            return Err(Error::Invalid(
                "task and machine identities disagree".into(),
            ));
        }
        validate_task_schemas(&self.input_schema, &self.output_schema, true)?;
        validate_value(&self.input_schema, &self.input, "task input")?;
        self.limits.validate()?;
        self.run_limits.validate()?;
        if let Some(policy) = &self.policy {
            validate_component_label(&policy.name, "policy name")?;
            validate_component_label(&policy.version, "policy version")?;
            if policy.digest == [0; 32] {
                return Err(Error::Invalid("policy digest is empty".into()));
            }
        }
        if let Some(extensions) = &self.extensions {
            extensions.validate()?;
        }
        if let Some(execution) = &self.execution {
            execution.validate()?;
        }
        Ok(())
    }

    /// Ref-only v2 envelope retained by the owner outside event history.
    #[must_use]
    pub fn canonical_value(&self) -> Value {
        serde_json::json!({
            "contract": "harness.task-admission.v2",
            "operation_id": self.operation_id,
            "task": self.task,
            "machine": self.machine,
            "input": self.input,
            "input_schema": self.input_schema,
            "output_schema": self.output_schema,
            "parent": self.parent,
            "grants": self.grants,
            "limits": self.limits,
            "run_limits": self.run_limits,
            "policy": self.policy,
            "extensions": self.extensions,
            "execution": self.execution,
        })
    }

    /// Rejects missing, extra, or noncanonical v2 admission fields.
    pub fn from_canonical_value(value: Value) -> Result<Self> {
        let canonical = value.clone();
        let mut body = value;
        let fields = body
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("task admission must be an object".into()))?;
        if fields.remove("contract") != Some(Value::String("harness.task-admission.v2".into())) {
            return Err(Error::Invalid("unsupported task admission contract".into()));
        }
        let admission: Self =
            serde_json::from_value(body).map_err(|error| Error::Invalid(error.to_string()))?;
        admission.validate()?;
        if admission.canonical_value() != canonical {
            return Err(Error::Invalid("task admission is not canonical v2".into()));
        }
        Ok(admission)
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

    /// Qualified execution provider this host routes to, if not process-local.
    fn execution_identity(&self) -> Option<ComponentIdentity> {
        None
    }

    /// Authenticates every admission binding for an independently supplied
    /// spawner. Providers may map task IDs to operation IDs arbitrarily.
    fn observe_admission<'a>(
        &'a self,
        _task_id: TaskId,
    ) -> BoxFuture<'a, Result<TaskAdmissionRecord>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "full durable admission observation is not bound".into(),
            ))
        })
    }

    /// Discovers direct owner-retained children at one pinned hierarchy revision.
    fn children<'a>(
        &'a self,
        _parent: TaskId,
        _expected_revision: Option<u64>,
        _after_slot: Option<String>,
        _maximum: usize,
    ) -> BoxFuture<'a, Result<TaskChildrenPage>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable task hierarchy is not bound".into(),
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

    /// Admits or reconciles one exact immutable task request.
    fn admit<'a>(
        &'a self,
        _request: TaskAdmissionRecord,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable task admission is not bound".into(),
            ))
        })
    }
    /// Resolves an uncertain admission by its original operation alone. `None`
    /// means the owner has authoritatively observed that it was not committed.
    fn reconcile_admission<'a>(
        &'a self,
        _operation_id: OperationId,
    ) -> BoxFuture<'a, Result<Option<(TaskId, TaskAdmissionRecord)>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable admission reconciliation is not bound".into(),
            ))
        })
    }
    /// Publishes one immutable batch request before declaring any indexed
    /// child. A repeated ID with another request must conflict.
    fn admit_batch<'a>(
        &'a self,
        _request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Vec<Admission<TaskId>>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable batch admission is not bound".into(),
            ))
        })
    }
    /// Observes the committed batch and its indexed admissions without
    /// submitting any absent member or retrying an uncertain effect.
    fn reconcile_batch<'a>(
        &'a self,
        _request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Option<Vec<Admission<TaskId>>>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable batch reconciliation is not bound".into(),
            ))
        })
    }
    /// Loads the owner's original immutable batch request by identity. This
    /// permits reconciliation after the caller has lost its input vector.
    fn load_batch<'a>(
        &'a self,
        _batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<DurableBatchRequest>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable batch lookup is not bound".into(),
            ))
        })
    }
    /// Publishes one durable group cancellation declaration and reconciles
    /// requests for every accepted member. It never asserts terminal outcomes.
    fn cancel_batch<'a>(
        &'a self,
        _batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<BatchCancellationReport>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable batch cancellation is not bound".into(),
            ))
        })
    }
    /// Whether this owner durably declares cancellation before applying it.
    fn supports_batch_cancellation(&self) -> bool {
        false
    }
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

/// Owner-bound durable task state, independently replaceable from admission
/// and execution. Returned observations are authoritative for typed handles.
pub trait TaskStateProvider: Send + Sync {
    /// Exact policy revision enforced when state-bound effects are reconciled.
    fn policy_identity(&self) -> Option<ComponentIdentity>;
    /// Execution route retained and observable by this state owner.
    fn execution_identity(&self) -> Option<ComponentIdentity> {
        None
    }
    /// Reads the complete immutable admission for cross-provider attestation.
    fn observe_admission<'a>(
        &'a self,
        task_id: TaskId,
    ) -> BoxFuture<'a, Result<TaskAdmissionRecord>>;
    /// Discovers direct owner-retained children with revision-checked pagination.
    fn children<'a>(
        &'a self,
        _parent: TaskId,
        _expected_revision: Option<u64>,
        _after_slot: Option<String>,
        _maximum: usize,
    ) -> BoxFuture<'a, Result<TaskChildrenPage>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable task hierarchy is not bound".into(),
            ))
        })
    }
    /// Authenticates and reconstructs the admitted scope after process restart.
    fn resume_scope<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<RuntimeScope>>;
    /// Observes a terminal result or ordinary pending state without dispatch.
    fn outcome<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<Option<Outcome<Value>>>>;
    /// Waits for a terminal result; providers may override polling.
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
    /// Requests cancellation without claiming a terminal result.
    fn cancel<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<()>>;
    /// Routes an addressable interaction through owner-retained state.
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
    /// Publishes one idempotent ref-only mailbox item.
    fn send<'a>(
        &'a self,
        _sender: TaskId,
        _recipient: TaskId,
        _message_id: OperationId,
        _payload: FileRef,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Err(Error::Unsupported("durable task mail is not bound".into())) })
    }
    /// Reads a bounded ordered inbox page.
    fn inbox<'a>(
        &'a self,
        _task_id: TaskId,
        _after: u64,
        _limit: usize,
    ) -> BoxFuture<'a, Result<Vec<InboxItem>>> {
        Box::pin(async { Err(Error::Unsupported("durable task inbox is not bound".into())) })
    }
    /// Retains or reconciles an absolute timer wait.
    fn wait_until<'a>(
        &'a self,
        _task_id: TaskId,
        _operation_id: OperationId,
        _deadline_unix_ms: u64,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Err(Error::Unsupported("durable timers are not bound".into())) })
    }
    /// Observes one recorded effect without redispatching it.
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
    /// Journals and reconciles an exact tool operation under the owning task.
    /// The state owner may dispatch through any independently bound executor.
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

struct HostTaskState(Arc<dyn DurableTaskHost>);

impl TaskStateProvider for HostTaskState {
    fn policy_identity(&self) -> Option<ComponentIdentity> {
        self.0.policy_identity()
    }
    fn execution_identity(&self) -> Option<ComponentIdentity> {
        self.0.execution_identity()
    }
    fn observe_admission<'a>(
        &'a self,
        task_id: TaskId,
    ) -> BoxFuture<'a, Result<TaskAdmissionRecord>> {
        self.0.observe_admission(task_id)
    }
    fn children<'a>(
        &'a self,
        parent: TaskId,
        expected_revision: Option<u64>,
        after_slot: Option<String>,
        maximum: usize,
    ) -> BoxFuture<'a, Result<TaskChildrenPage>> {
        self.0
            .children(parent, expected_revision, after_slot, maximum)
    }
    fn resume_scope<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<RuntimeScope>> {
        self.0.resume_scope(task_id, operation_id)
    }
    fn outcome<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<Option<Outcome<Value>>>> {
        self.0.outcome(task_id)
    }
    fn wait_outcome<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<Outcome<Value>>> {
        self.0.wait_outcome(task_id)
    }
    fn cancel<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<()>> {
        self.0.cancel(task_id)
    }
    fn interact<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
        interaction: Interaction,
    ) -> BoxFuture<'a, Result<InteractionOutcome>> {
        self.0.interact(task_id, operation_id, interaction)
    }
    fn send<'a>(
        &'a self,
        sender: TaskId,
        recipient: TaskId,
        message_id: OperationId,
        payload: FileRef,
    ) -> BoxFuture<'a, Result<()>> {
        self.0.send(sender, recipient, message_id, payload)
    }
    fn inbox<'a>(
        &'a self,
        task_id: TaskId,
        after: u64,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<InboxItem>>> {
        self.0.inbox(task_id, after, limit)
    }
    fn wait_until<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
        deadline_unix_ms: u64,
    ) -> BoxFuture<'a, Result<()>> {
        self.0.wait_until(task_id, operation_id, deadline_unix_ms)
    }
    fn reconcile_effect<'a>(
        &'a self,
        task_id: TaskId,
        effect_id: EffectId,
    ) -> BoxFuture<'a, Result<crate::core::EffectStatus>> {
        self.0.reconcile_effect(task_id, effect_id)
    }
    fn execute_tool<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
        definition: ToolDefinition,
        arguments: Value,
        scope: RuntimeScope,
        context: ToolContext,
    ) -> BoxFuture<'a, Result<Outcome<Value>>> {
        self.0
            .execute_tool(task_id, operation_id, definition, arguments, scope, context)
    }
}

/// Independently replaceable durable admission boundary. A spawner commits
/// identities and requests; the bound state host remains the authority for
/// typed observation, resumed scope, and cancellation.
pub trait TaskSpawner: Send + Sync {
    /// Exact policy revision enforced during child admission.
    fn policy_identity(&self) -> Option<ComponentIdentity>;
    /// Execution route this spawner can actually dispatch to.
    fn execution_identity(&self) -> Option<ComponentIdentity> {
        None
    }
    /// Publishes or reconciles one complete caller-identified admission.
    /// The spawner receives the same immutable record the state owner must
    /// later attest; individual arguments cannot silently fall out of sync.
    fn admit<'a>(
        &'a self,
        request: TaskAdmissionRecord,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>>;
    /// Observes one possibly lost admission without resubmission.
    fn reconcile_admission<'a>(
        &'a self,
        _operation_id: OperationId,
    ) -> BoxFuture<'a, Result<Option<(TaskId, TaskAdmissionRecord)>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable admission reconciliation is not bound".into(),
            ))
        })
    }
    /// Publishes the immutable request before any indexed child admission.
    fn admit_batch<'a>(
        &'a self,
        _request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Vec<Admission<TaskId>>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable batch admission is not bound".into(),
            ))
        })
    }
    /// Observes the original batch and current indexed admissions only.
    fn reconcile_batch<'a>(
        &'a self,
        _request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Option<Vec<Admission<TaskId>>>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable batch reconciliation is not bound".into(),
            ))
        })
    }
    /// Loads a retained request by identity after the caller loses its inputs.
    fn load_batch<'a>(
        &'a self,
        _batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<DurableBatchRequest>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable batch lookup is not bound".into(),
            ))
        })
    }
    /// Retains an idempotent cancellation declaration at the batch owner.
    fn cancel_batch<'a>(
        &'a self,
        _batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<BatchCancellationReport>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable batch cancellation is not bound".into(),
            ))
        })
    }
    /// Whether this spawner can retain a cancellation declaration.
    fn supports_batch_cancellation(&self) -> bool {
        false
    }
}

struct HostTaskSpawner(Arc<dyn DurableTaskHost>);

impl TaskSpawner for HostTaskSpawner {
    fn policy_identity(&self) -> Option<ComponentIdentity> {
        self.0.policy_identity()
    }
    fn execution_identity(&self) -> Option<ComponentIdentity> {
        self.0.execution_identity()
    }
    fn admit<'a>(
        &'a self,
        request: TaskAdmissionRecord,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
        self.0.admit(request)
    }
    fn reconcile_admission<'a>(
        &'a self,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<Option<(TaskId, TaskAdmissionRecord)>>> {
        self.0.reconcile_admission(operation_id)
    }
    fn admit_batch<'a>(
        &'a self,
        request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Vec<Admission<TaskId>>>> {
        self.0.admit_batch(request)
    }
    fn reconcile_batch<'a>(
        &'a self,
        request: DurableBatchRequest,
    ) -> BoxFuture<'a, Result<Option<Vec<Admission<TaskId>>>>> {
        self.0.reconcile_batch(request)
    }
    fn load_batch<'a>(
        &'a self,
        batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<DurableBatchRequest>>> {
        self.0.load_batch(batch_id)
    }
    fn cancel_batch<'a>(
        &'a self,
        batch_id: BatchId,
    ) -> BoxFuture<'a, Result<Option<BatchCancellationReport>>> {
        self.0.cancel_batch(batch_id)
    }
    fn supports_batch_cancellation(&self) -> bool {
        self.0.supports_batch_cancellation()
    }
}

/// One replaceable execution route. It packages a qualified placement with
/// the exact admission and observation authorities that can run it; selecting
/// it never falls back to a process-local task or another host.
pub trait ExecutionProvider: Send + Sync {
    /// Immutable provider implementation pinned in every admitted placement.
    fn identity(&self) -> ComponentIdentity;
    /// Durable spawner for this provider's task build and environment.
    fn spawner(&self) -> Arc<dyn TaskSpawner>;
    /// Owner-bound observation and cancellation for the same route.
    fn state(&self) -> Arc<dyn TaskStateProvider>;
    /// Read-only qualification of one exact admitted task and its accessible
    /// input. Upload, mount, and credential preparation belongs to admission
    /// under its stable operation ID, with reconciliation on uncertainty.
    fn qualify<'a>(
        &'a self,
        request: &'a TaskAdmissionRecord,
    ) -> BoxFuture<'a, Result<ExecutionPlacement>>;
    /// Batch qualification must cover every input before the manifest is
    /// published. A provider cannot silently substitute single-task routing.
    fn qualify_batch<'a>(
        &'a self,
        _request: &'a DurableBatchRequest,
    ) -> BoxFuture<'a, Result<ExecutionPlacement>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "execution provider does not qualify batches".into(),
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

/// Owner-mediated interaction ledger. Routing cannot grant resolution authority;
/// every decision carries a signed responder scope and exact CAS version.
/// The optional value is absent when the owner has no admitted ticket.
pub type InteractionInspection = Option<(InteractionTicket, Option<InteractionResolution>)>;

/// Owner-mediated resolver for versioned questions and approvals.
pub trait InteractionResolver: Send + Sync {
    /// Reads the admitted request and current decision before a CAS reply.
    fn inspect<'a>(
        &'a self,
        scope: Scope,
        id: InteractionId,
    ) -> BoxFuture<'a, Result<InteractionInspection>>;

    /// Stages and validates a question, choice, or form answer before CAS.
    fn resolve_answer<'a>(
        &'a self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        response: InteractionResponse,
    ) -> BoxFuture<'a, Result<ResolutionReceipt>>;

    /// Accepts or declines only the ticket's pinned approval action.
    fn resolve_approval<'a>(
        &'a self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        approved: bool,
        reason: Option<String>,
    ) -> BoxFuture<'a, Result<ResolutionReceipt>>;
}

/// Task-local execution bounds pinned separately from content/render limits.
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskRunLimits {
    /// Maximum concurrent tasks in a child group, when bounded.
    pub concurrency: Option<usize>,
    /// Maximum model steps for this scoped task, when bounded.
    pub max_steps: Option<usize>,
    /// Absolute Unix deadline in milliseconds, when bounded.
    pub deadline_epoch_ms: Option<u64>,
}

impl TaskRunLimits {
    /// Rejects zero, nonportable, or nonrepresentable runtime bounds.
    pub fn validate(&self) -> Result<()> {
        const MAX_JS_INTEGER: u64 = (1_u64 << 53) - 1;
        if self
            .concurrency
            .is_some_and(|value| value == 0 || value as u64 > MAX_JS_INTEGER)
            || self
                .max_steps
                .is_some_and(|value| value == 0 || value as u64 > MAX_JS_INTEGER)
            || self
                .deadline_epoch_ms
                .is_some_and(|value| value == 0 || value > MAX_JS_INTEGER)
        {
            return Err(Error::Invalid("task run limits are invalid".into()));
        }
        Ok(())
    }

    fn remaining(&self) -> Result<Option<std::time::Duration>> {
        let Some(deadline) = self.deadline_epoch_ms else {
            return Ok(None);
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Invalid("system time precedes Unix epoch".into()))?
            .as_millis();
        if u128::from(deadline) <= now {
            return Err(Error::Invalid("task deadline has expired".into()));
        }
        Ok(Some(std::time::Duration::from_millis(
            u64::try_from(u128::from(deadline) - now)
                .map_err(|_| Error::Invalid("task deadline is not representable".into()))?,
        )))
    }

    fn allows(&self, child: &Self) -> Result<()> {
        child.validate()?;
        for (ancestor, descendant) in [
            (
                self.concurrency.map(|value| value as u64),
                child.concurrency.map(|value| value as u64),
            ),
            (
                self.max_steps.map(|value| value as u64),
                child.max_steps.map(|value| value as u64),
            ),
            (self.deadline_epoch_ms, child.deadline_epoch_ms),
        ] {
            if ancestor.is_some_and(|bound| descendant.is_none_or(|value| value > bound)) {
                return Err(Error::Unauthorized(
                    "task scope cannot widen run limits".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Effective runtime authority and bounds; child scopes may only narrow them.
#[derive(Clone, Default)]
pub struct RuntimeScope {
    grants: Capabilities,
    limits: Limits,
    run_limits: TaskRunLimits,
    extensions: Option<ExtensionAdmission>,
    extensions_sealed: bool,
    extension_runtime: Option<Arc<ExtensionRuntime>>,
}

impl RuntimeScope {
    /// Creates a checked root scope.
    pub fn new(grants: Capabilities, limits: Limits) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            grants,
            limits,
            run_limits: TaskRunLimits::default(),
            extensions: None,
            extensions_sealed: false,
            extension_runtime: None,
        })
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
        Ok(Self {
            grants,
            limits,
            run_limits: self.run_limits,
            extensions: self.extensions.clone(),
            extensions_sealed: true,
            extension_runtime: self.extension_runtime.clone(),
        })
    }

    /// Further bounds task concurrency, steps, and deadline for descendants.
    pub fn with_run_limits(mut self, limits: TaskRunLimits) -> Result<Self> {
        self.run_limits.allows(&limits)?;
        self.run_limits = limits;
        Ok(self)
    }

    /// Exact task execution bounds pinned at admission.
    #[must_use]
    pub const fn run_limits(&self) -> TaskRunLimits {
        self.run_limits
    }

    fn concurrency_bound(&self, configured: usize) -> usize {
        self.run_limits
            .concurrency
            .map_or(configured, |bound| configured.min(bound))
    }

    /// Pins one committed agent selection for this scope and all descendants.
    /// Calling this on an already pinned scope cannot retarget it.
    pub fn with_extensions_from(mut self, agent: &Reducer) -> Result<Self> {
        if self.extensions_sealed {
            return Err(Error::Unauthorized(
                "descendant scope cannot change extension selection".into(),
            ));
        }
        let selection = agent.extension_admission()?;
        if self.extensions.is_some() && self.extensions != selection {
            return Err(Error::Conflict(
                "runtime scope extension selection is already pinned".into(),
            ));
        }
        self.extensions = selection;
        if let Some(runtime) = &self.extension_runtime {
            runtime.validate_admission(self.extensions.as_ref())?;
        }
        self.extensions_sealed = true;
        Ok(self)
    }

    /// Exact extension bindings selected when this scope was admitted.
    #[must_use]
    pub const fn extensions(&self) -> Option<&ExtensionAdmission> {
        self.extensions.as_ref()
    }

    /// Binds process-local executable implementations for this scope.
    pub fn with_extension_runtime(mut self, runtime: Arc<ExtensionRuntime>) -> Result<Self> {
        runtime.validate_admission(self.extensions.as_ref())?;
        self.extension_runtime = Some(runtime);
        Ok(self)
    }

    /// Returns the executable extension selection attached to this scope.
    #[must_use]
    pub fn extension_runtime(&self) -> Option<Arc<ExtensionRuntime>> {
        self.extension_runtime.clone()
    }

    /// Whether this scope was pinned by admission or derived from another.
    #[must_use]
    pub const fn is_sealed(&self) -> bool {
        self.extensions_sealed
    }

    pub(crate) fn with_replayed_extensions(
        mut self,
        extensions: Option<ExtensionAdmission>,
    ) -> Result<Self> {
        if let Some(extensions) = &extensions {
            extensions.validate()?;
        }
        self.extensions = extensions;
        if let Some(runtime) = &self.extension_runtime {
            runtime.validate_admission(self.extensions.as_ref())?;
        }
        self.extensions_sealed = true;
        Ok(self)
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
        host: Arc<dyn TaskStateProvider>,
        /// Pinned schema for the task result.
        output_schema: Value,
        /// Native extension implementations retained by this admission.
        extensions: Option<ExtensionLeases>,
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

/// Streams typed task outcomes in completion order with their admitted IDs.
/// Dropping the stream does not cancel tasks; keep a group handle when the
/// caller needs explicit descendant cancellation.
pub fn completion_stream_runtime<O: DeserializeOwned + Send + 'static>(
    tasks: Vec<RuntimeTask<O>>,
) -> BoxStream<'static, (String, Result<Outcome<O>>)> {
    let concurrency = tasks.len().clamp(1, 64);
    stream::iter(tasks)
        .map(|task| async move {
            let id = task.identity();
            (id, task.result().await)
        })
        .buffer_unordered(concurrency)
        .boxed()
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
    completion_stream_runtime(tasks).next().await
}

/// Returns the first success or every observed non-successful outcome.
pub async fn first_success_runtime<O: DeserializeOwned + Send + 'static>(
    tasks: Vec<RuntimeTask<O>>,
) -> std::result::Result<O, Vec<(String, Result<Outcome<O>>)>> {
    let mut completions = completion_stream_runtime(tasks);
    let mut others = Vec::new();
    while let Some((id, result)) = completions.next().await {
        match result {
            Ok(Outcome::Succeeded(value)) => return Ok(value),
            result => others.push((id, result)),
        }
    }
    Err(others)
}

/// An impossible quorum retains both partial successes and non-successes.
pub struct QuorumFailure<O> {
    /// Caller-specified threshold that was not met.
    pub required: usize,
    /// Successful values with their task IDs, in completion order.
    pub successes: Vec<(String, O)>,
    /// Failed, cancelled, uncertain, or unobservable members.
    pub others: Vec<(String, Result<Outcome<O>>)>,
}

/// Returns the first required successes or every observed partial outcome.
pub async fn quorum_runtime<O: DeserializeOwned + Send + 'static>(
    tasks: Vec<RuntimeTask<O>>,
    required: usize,
) -> std::result::Result<Vec<O>, QuorumFailure<O>> {
    if required == 0 {
        return Ok(Vec::new());
    }
    let mut completions = completion_stream_runtime(tasks);
    let mut successes = Vec::new();
    let mut others = Vec::new();
    while let Some((id, result)) = completions.next().await {
        match result {
            Ok(Outcome::Succeeded(value)) => {
                successes.push((id, value));
                if successes.len() == required {
                    return Ok(successes.into_iter().map(|(_, value)| value).collect());
                }
            }
            result => others.push((id, result)),
        }
    }
    Err(QuorumFailure {
        required,
        successes,
        others,
    })
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
                extensions: _,
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
#[derive(Clone)]
pub struct AgentHarness {
    tasks: TaskRegistry,
    tools: ToolRegistry,
    resumable_tools: ResumableToolRegistry,
    live: TaskGroup,
    concurrency: usize,
    scope: RuntimeScope,
    extension_runtime: Option<Arc<ExtensionRuntime>>,
    host: Option<Arc<dyn DurableTaskHost>>,
    state: Option<Arc<dyn TaskStateProvider>>,
    spawner: Option<Arc<dyn TaskSpawner>>,
    execution: Option<Arc<dyn ExecutionProvider>>,
    interactions: Option<Arc<dyn InteractionRouter>>,
    interaction_resolver: Option<Arc<dyn InteractionResolver>>,
    content: Option<ContentBindings>,
    artifacts: Option<ContentBindings>,
    policy: Option<Arc<dyn ToolPolicy>>,
    policy_identity: Option<ComponentIdentity>,
    model: Option<ModelBinding>,
    context: ContextPipeline,
    fork_preparer: Option<ForkBinding>,
    workspaces: Option<Arc<dyn crate::merge::ProjectWorkspaceProvider>>,
}

#[derive(Clone)]
struct ModelBinding {
    model: Model,
    provider: Arc<dyn ModelProvider>,
}

#[derive(Clone)]
struct ForkBinding {
    parent: crate::core::Authority,
    revision: u64,
    preparer: Arc<dyn crate::fork::ForkPreparer>,
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

#[allow(
    clippy::needless_pass_by_value,
    reason = "the terminal outcome is consumed by this admission boundary"
)]
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

/// Artifact access uses the same immutable, digest-verified file contract as
/// conversation content, but is selected independently of workspace files.
pub type ArtifactBindings = ContentBindings;

/// Immutable admission bindings assembled before tasks can run. Each optional
/// provider boundary remains independently replaceable.
pub struct Bindings {
    /// Version-pinned typed task definitions.
    pub tasks: TaskRegistry,
    /// Version-pinned tool definitions and executors.
    pub tools: ToolRegistry,
    /// Exact resumable tool implementations, never substituted by live executors.
    pub resumable_tools: ResumableToolRegistry,
    /// Effective authority and bounds.
    pub scope: RuntimeScope,
    /// Maximum simultaneously running local members in one group.
    pub concurrency: usize,
    /// Optional durable scheduler and effect/wait boundary.
    pub durable_host: Option<Arc<dyn DurableTaskHost>>,
    /// Owner-bound retained task state and observation, independent of admission.
    pub state: Option<Arc<dyn TaskStateProvider>>,
    /// Independently replaceable durable admission and batch boundary.
    pub spawner: Option<Arc<dyn TaskSpawner>>,
    /// Qualified route packaging a compatible task build, spawner, and state
    /// owner. Explicit `state`/`spawner` bindings may replace either member
    /// only when they advertise this same execution identity.
    pub execution: Option<Arc<dyn ExecutionProvider>>,
    /// Optional local interaction router.
    pub interactions: Option<Arc<dyn InteractionRouter>>,
    /// Owner-mediated, signed CAS interaction resolution.
    pub interaction_resolver: Option<Arc<dyn InteractionResolver>>,
    /// Optional owner-bound content access for tasks and tools.
    pub content: Option<ContentBindings>,
    /// Independently selected immutable artifact storage.
    pub artifacts: Option<ArtifactBindings>,
    /// Optional invocation policy; absence permits calls with explicit grants.
    pub policy: Option<Arc<dyn ToolPolicy>>,
    /// Parent-bound fork capture; publication remains with the owning aggregate.
    pub fork_preparer: Option<Arc<dyn crate::fork::ForkPreparer>>,
    /// Parent-bound project fork and inspected join provider.
    pub workspaces: Option<Arc<dyn crate::merge::ProjectWorkspaceProvider>>,
}

impl Bindings {
    /// Starts a local composition with no implicit authority or providers.
    /// Callers add only the boundaries their runtime actually uses.
    #[must_use]
    pub fn local() -> Self {
        Self {
            tasks: TaskRegistry::default(),
            tools: ToolRegistry::default(),
            resumable_tools: ResumableToolRegistry::default(),
            scope: RuntimeScope::default(),
            concurrency: 64,
            durable_host: None,
            state: None,
            spawner: None,
            execution: None,
            interactions: None,
            interaction_resolver: None,
            content: None,
            artifacts: None,
            policy: None,
            fork_preparer: None,
            workspaces: None,
        }
    }

    /// Selects a qualified execution provider for new durable admissions.
    /// Its state and spawner are used unless explicitly replaced with peers
    /// advertising the same immutable execution identity.
    #[must_use]
    pub fn execution(mut self, value: Arc<dyn ExecutionProvider>) -> Self {
        self.execution = Some(value);
        self
    }

    /// Validates all dependency edges and seals the runtime composition.
    pub fn build(self) -> Result<Arc<AgentHarness>> {
        if self.fork_preparer.is_some() && !self.scope.grants().contains("fork:publish") {
            return Err(Error::Unauthorized(
                "fork preparer requires fork:publish in the root scope".into(),
            ));
        }
        if self.execution.is_some() && self.durable_host.is_some() {
            return Err(Error::Conflict(
                "execution route and legacy durable host are both bound".into(),
            ));
        }
        let execution = self.execution;
        let state = self
            .state
            .or_else(|| execution.as_ref().map(|route| route.state()));
        let spawner = self
            .spawner
            .or_else(|| execution.as_ref().map(|route| route.spawner()));
        let durable_ready = self.durable_host.is_some() || (state.is_some() && spawner.is_some());
        let runtime = AgentHarness::with_policy_internal(
            self.tasks,
            self.tools,
            self.resumable_tools,
            self.scope,
            self.concurrency,
            self.durable_host,
            self.interactions,
            self.content,
            self.artifacts,
            self.policy,
            durable_ready,
        )?;
        let runtime = match state {
            Some(state) => runtime.bind_state(state)?,
            None => runtime,
        };
        let runtime = match spawner {
            Some(spawner) => runtime.bind_spawner(spawner)?,
            None => runtime,
        };
        let runtime = match execution {
            Some(execution) => runtime.bind_execution(execution)?,
            None => runtime,
        };
        let runtime = match self.interaction_resolver {
            Some(resolver) => runtime.bind_interaction_resolver(resolver),
            None => runtime,
        };
        let runtime = match self.fork_preparer {
            Some(preparer) => runtime.bind_fork_preparer(preparer)?,
            None => runtime,
        };
        Ok(match self.workspaces {
            Some(workspaces) => runtime.bind_workspaces(workspaces)?,
            None => runtime,
        })
    }
}

impl Default for Bindings {
    fn default() -> Self {
        Self::local()
    }
}

fn validate_children_request(
    parent: TaskId,
    after_slot: Option<&str>,
    maximum: usize,
) -> Result<()> {
    if parent.into_bytes() == [0; 16]
        || maximum == 0
        || maximum > 1_024
        || after_slot.is_some_and(|slot| slot.len() > 255 || slot.chars().any(char::is_control))
    {
        return Err(Error::Invalid("task child page request is invalid".into()));
    }
    Ok(())
}

fn validate_children_page(
    page: &TaskChildrenPage,
    expected_revision: Option<u64>,
    after_slot: Option<&str>,
    maximum: usize,
) -> Result<()> {
    if expected_revision.is_some_and(|revision| revision != page.revision)
        || page.entries.len() > maximum
        || page
            .next_after
            .as_ref()
            .is_some_and(|next| page.entries.last().is_none_or(|last| &last.slot != next))
    {
        return Err(Error::Invalid(
            "task child page does not match its request".into(),
        ));
    }
    let mut previous = after_slot;
    let mut ids = BTreeSet::new();
    for child in &page.entries {
        if child.task_id.into_bytes() == [0; 16]
            || child.slot.trim().is_empty()
            || child.slot.len() > 255
            || child.slot.chars().any(char::is_control)
            || previous.is_some_and(|slot| child.slot.as_str() <= slot)
            || !ids.insert(child.task_id)
        {
            return Err(Error::Invalid(
                "task children are not in stable slot order".into(),
            ));
        }
        previous = Some(&child.slot);
    }
    Ok(())
}

impl AgentHarness {
    fn validate_observed_admission(&self, observed: &TaskAdmissionRecord) -> Result<()> {
        observed.validate()?;
        self.tasks.validate_durable_admission(
            &observed.task,
            &observed.machine,
            Some(&observed.input_schema),
            &observed.output_schema,
            Some(&observed.input),
        )?;
        self.scope
            .narrow(observed.grants.clone(), observed.limits)?
            .with_run_limits(observed.run_limits)?;
        if observed.policy != self.policy_identity
            || observed.extensions.as_ref() != self.scope.extensions()
        {
            return Err(Error::Conflict(
                "retained admission changed owner bindings".into(),
            ));
        }
        self.validate_execution_placement(observed.execution.as_ref())
    }

    fn validate_execution_placement(&self, observed: Option<&ExecutionPlacement>) -> Result<()> {
        match (&self.execution, observed) {
            (Some(provider), Some(placement)) if placement.provider == provider.identity() => {
                placement.validate()?;
            }
            (None, None) => {}
            _ => {
                return Err(Error::Conflict(
                    "retained execution route differs from runtime".into(),
                ));
            }
        }
        Ok(())
    }

    async fn reconcile_scoped_admission<I, O>(
        &self,
        operation_id: OperationId,
        definition: &Arc<TaskDefinition<I, O>>,
        expected: &TaskAdmissionRecord,
        scope: &RuntimeScope,
        host: &Arc<dyn TaskStateProvider>,
        spawner: &Arc<dyn TaskSpawner>,
    ) -> Result<Option<Admission<RuntimeTask<O>>>>
    where
        I: Serialize + Send + 'static,
        O: DeserializeOwned + Send + 'static,
    {
        let retained_admission = match spawner.reconcile_admission(operation_id).await {
            Ok(retained) => retained,
            Err(Error::Unsupported(_)) => None,
            Err(error) => return Err(error),
        };
        let Some((task_id, retained)) = retained_admission else {
            return Ok(None);
        };
        if retained.operation_id != operation_id {
            return Err(Error::Conflict(
                "spawner reconciled another operation".into(),
            ));
        }
        let observed = match host.observe_admission(task_id).await {
            Ok(observed) => observed,
            Err(Error::NotFound(_)) => {
                return Ok(Some(Admission::Indeterminate { operation_id }));
            }
            Err(error) => return Err(error),
        };
        let mut base = observed.clone();
        base.execution = None;
        if base != *expected || observed != retained {
            return Err(Error::Conflict(
                "operation belongs to another admission request".into(),
            ));
        }
        self.validate_observed_admission(&observed)?;
        let extension_leases = self.retain_task_extensions_existing(definition, scope)?;
        Ok(Some(Admission::Accepted(RuntimeTask::Durable {
            task_id,
            host: Arc::clone(host),
            output_schema: definition.output_schema.clone(),
            extensions: Some(extension_leases),
        })))
    }

    fn bind_execution(
        self: &Arc<Self>,
        execution: Arc<dyn ExecutionProvider>,
    ) -> Result<Arc<Self>> {
        let identity = execution.identity();
        validate_component_label(&identity.name, "execution provider name")?;
        validate_component_label(&identity.version, "execution provider version")?;
        if identity.digest == [0; 32] {
            return Err(Error::Invalid("execution provider digest is empty".into()));
        }
        if self
            .state
            .as_ref()
            .and_then(|state| state.execution_identity())
            .as_ref()
            != Some(&identity)
            || self
                .spawner
                .as_ref()
                .and_then(|spawner| spawner.execution_identity())
                .as_ref()
                != Some(&identity)
        {
            return Err(Error::Conflict(
                "execution provider, state, and spawner routes differ".into(),
            ));
        }
        let mut bound = self.as_ref().clone();
        bound.execution = Some(execution);
        Ok(Arc::new(bound))
    }

    fn bind_state(self: &Arc<Self>, state: Arc<dyn TaskStateProvider>) -> Result<Arc<Self>> {
        if state.policy_identity() != self.policy_identity {
            return Err(Error::Conflict("runtime and state policies differ".into()));
        }
        let mut bound = self.as_ref().clone();
        bound.state = Some(state);
        Ok(Arc::new(bound))
    }

    fn assert_durable_policy_bindings(&self) -> Result<()> {
        if self
            .host
            .as_ref()
            .is_some_and(|host| host.policy_identity() != self.policy_identity)
            || self
                .state
                .as_ref()
                .is_some_and(|state| state.policy_identity() != self.policy_identity)
            || self
                .spawner
                .as_ref()
                .is_some_and(|spawner| spawner.policy_identity() != self.policy_identity)
            || self.execution.as_ref().is_some_and(|execution| {
                let identity = execution.identity();
                self.state
                    .as_ref()
                    .and_then(|state| state.execution_identity())
                    .as_ref()
                    != Some(&identity)
                    || self
                        .spawner
                        .as_ref()
                        .and_then(|spawner| spawner.execution_identity())
                        .as_ref()
                        != Some(&identity)
            })
        {
            return Err(Error::Conflict(
                "durable policy implementation changed after binding".into(),
            ));
        }
        Ok(())
    }

    async fn verify_spawner_admission(
        &self,
        task_id: TaskId,
        expected: &TaskAdmissionRecord,
    ) -> Result<()> {
        self.assert_durable_policy_bindings()?;
        let host = self
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state host is not bound".into()))?;
        match host.observe_admission(task_id).await {
            Ok(observed) if observed == *expected => self.validate_observed_admission(&observed),
            Ok(_) => Err(Error::Conflict(
                "spawner and state host admission bindings differ".into(),
            )),
            Err(Error::NotFound(_)) => Err(Error::Indeterminate(expected.operation_id)),
            Err(error) => Err(error),
        }
    }

    /// Replaces only durable admission. Accepted work must still be observable
    /// through this runtime's separately bound owner state host.
    fn bind_spawner(self: &Arc<Self>, spawner: Arc<dyn TaskSpawner>) -> Result<Arc<Self>> {
        if self.state.is_none() {
            return Err(Error::Invalid(
                "durable spawner requires a state/observation host".into(),
            ));
        }
        if spawner.policy_identity() != self.policy_identity {
            return Err(Error::Conflict(
                "runtime and spawner policies differ".into(),
            ));
        }
        let mut bound = self.as_ref().clone();
        bound.spawner = Some(spawner);
        Ok(Arc::new(bound))
    }

    /// Binds parent-controlled project operations independently of content.
    fn bind_workspaces(
        self: &Arc<Self>,
        workspaces: Arc<dyn crate::merge::ProjectWorkspaceProvider>,
    ) -> Result<Arc<Self>> {
        if !self.scope.grants().contains("fork:publish")
            && !self.scope.grants().contains("project:merge")
        {
            return Err(Error::Unauthorized(
                "project workspaces require fork:publish or project:merge".into(),
            ));
        }
        workspaces.provider().validate()?;
        workspaces.project().validate()?;
        if workspaces.project().class() != crate::conversation::VolumeClass::Project
            || workspaces.project().provider() != workspaces.provider()
        {
            return Err(Error::Invalid("workspace binding is not a project".into()));
        }
        let mut bound = self.as_ref().clone();
        bound.workspaces = Some(workspaces);
        Ok(Arc::new(bound))
    }

    /// Returns the parent-authorized workspace boundary only while this
    /// immutable runtime scope retains project fork or merge authority.
    pub fn workspaces(&self) -> Result<Arc<dyn crate::merge::ProjectWorkspaceProvider>> {
        if !self.scope.grants().contains("fork:publish")
            && !self.scope.grants().contains("project:merge")
        {
            return Err(Error::Unauthorized(
                "runtime scope lacks project authority".into(),
            ));
        }
        self.workspaces
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::Unsupported("project workspace provider is not bound".into()))
    }

    /// Selects a newly captured parent revision for future work. Existing task
    /// contexts keep their previous immutable binding and cannot be retargeted.
    pub fn at_parent_snapshot(
        self: &Arc<Self>,
        preparer: Arc<dyn crate::fork::ForkPreparer>,
    ) -> Result<Arc<Self>> {
        if !self.scope.grants().contains("fork:publish") {
            return Err(Error::Unauthorized(
                "runtime scope lacks fork:publish".into(),
            ));
        }
        if let Some(current) = &self.fork_preparer {
            let (parent, revision) = preparer.parent_snapshot();
            if parent != &current.parent {
                return Err(Error::Unauthorized(
                    "fork snapshot belongs to another parent conversation".into(),
                ));
            }
            if revision <= current.revision {
                return Err(Error::Conflict(
                    "fork snapshot must advance the parent revision".into(),
                ));
            }
        } else {
            return Err(Error::Unauthorized(
                "parent fork control is not bound".into(),
            ));
        }
        self.bind_fork_preparer(preparer)
    }

    /// Derives an immutable application view with no more authority or
    /// resources than its parent. Registrations, providers, and the local
    /// ownership group stay pinned; only grants and bounds are attenuated.
    pub fn scoped(self: &Arc<Self>, grants: Capabilities, limits: Limits) -> Result<Arc<Self>> {
        let mut child = self.as_ref().clone();
        child.scope = self.scope.narrow(grants, limits)?;
        child.fork_preparer = None;
        child.workspaces = None;
        Ok(Arc::new(child))
    }

    /// Narrows the effective task execution bounds for newly admitted work.
    /// The scoped view has its own bounded local group; existing work keeps
    /// the authority and group pinned at its admission.
    pub fn scoped_run_limits(self: &Arc<Self>, limits: TaskRunLimits) -> Result<Arc<Self>> {
        let mut child = self.as_ref().clone();
        child.scope = self.scope.clone().with_run_limits(limits)?;
        child.live = TaskGroup::new(child.scope.concurrency_bound(self.concurrency));
        child.fork_preparer = None;
        child.workspaces = None;
        Ok(Arc::new(child))
    }

    /// Selects a different qualified execution route for newly admitted work
    /// under attenuated authority. Already admitted tasks retain their route.
    pub fn scoped_execution(
        self: &Arc<Self>,
        grants: Capabilities,
        limits: Limits,
        execution: Arc<dyn ExecutionProvider>,
    ) -> Result<Arc<Self>> {
        let scoped = self.scoped(grants, limits)?;
        let mut routed = scoped.as_ref().clone();
        routed.host = None;
        routed.state = Some(execution.state());
        routed.spawner = Some(execution.spawner());
        routed.execution = None;
        let routed = Arc::new(routed);
        routed.assert_durable_policy_bindings()?;
        routed.bind_execution(execution)
    }

    /// Binds a resolver separately from interaction presentation/routing.
    #[must_use]
    pub fn bind_interaction_resolver(
        self: &Arc<Self>,
        resolver: Arc<dyn InteractionResolver>,
    ) -> Arc<Self> {
        let mut bound = self.as_ref().clone();
        bound.interaction_resolver = Some(resolver);
        Arc::new(bound)
    }

    /// Observes an addressable interaction without changing its state.
    pub async fn inspect_interaction(
        &self,
        scope: Scope,
        id: InteractionId,
    ) -> Result<Option<(InteractionTicket, Option<InteractionResolution>)>> {
        self.interaction_resolver
            .as_ref()
            .ok_or_else(|| Error::Unsupported("interaction resolver is not bound".into()))?
            .inspect(scope, id)
            .await
    }

    /// Submits one signed, versioned answer through its conversation owner.
    pub async fn resolve_answer(
        &self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        response: InteractionResponse,
    ) -> Result<ResolutionReceipt> {
        let receipt = self
            .interaction_resolver
            .as_ref()
            .ok_or_else(|| Error::Unsupported("interaction resolver is not bound".into()))?
            .resolve_answer(operation_id, scope, id, expected_version, response)
            .await?;
        receipt.validate()?;
        if receipt.operation_id != operation_id
            || receipt.version != expected_version
            || receipt.id
                != uuid::Uuid::parse_str(&id.to_string())
                    .map_err(|error| Error::Invalid(error.to_string()))?
            || !matches!(receipt.outcome, InteractionOutcome::Answered { .. })
        {
            return Err(Error::Conflict(
                "interaction resolver returned another answer".into(),
            ));
        }
        Ok(receipt)
    }

    /// Submits one signed decision for the ticket's exact approved intent.
    pub async fn resolve_approval(
        &self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        approved: bool,
        reason: Option<String>,
    ) -> Result<ResolutionReceipt> {
        let receipt = self
            .interaction_resolver
            .as_ref()
            .ok_or_else(|| Error::Unsupported("interaction resolver is not bound".into()))?
            .resolve_approval(operation_id, scope, id, expected_version, approved, reason)
            .await?;
        receipt.validate()?;
        if receipt.operation_id != operation_id
            || receipt.version != expected_version
            || receipt.id
                != uuid::Uuid::parse_str(&id.to_string())
                    .map_err(|error| Error::Invalid(error.to_string()))?
            || !matches!(
                (&receipt.outcome, approved),
                (InteractionOutcome::Approved, true) | (InteractionOutcome::Declined, false)
            )
        {
            return Err(Error::Conflict(
                "interaction resolver returned another decision".into(),
            ));
        }
        Ok(receipt)
    }

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
            None,
        )
    }

    /// Seals a replaceable tool policy into the immutable runtime composition.
    #[allow(
        clippy::too_many_arguments,
        reason = "each independently replaceable runtime boundary is explicit"
    )]
    pub fn with_policy(
        tasks: TaskRegistry,
        tools: ToolRegistry,
        scope: RuntimeScope,
        concurrency: usize,
        host: Option<Arc<dyn DurableTaskHost>>,
        interactions: Option<Arc<dyn InteractionRouter>>,
        content: Option<ContentBindings>,
        artifacts: Option<ArtifactBindings>,
        policy: Option<Arc<dyn ToolPolicy>>,
    ) -> Result<Arc<Self>> {
        let durable_ready = host.is_some();
        Self::with_policy_internal(
            tasks,
            tools,
            ResumableToolRegistry::default(),
            scope,
            concurrency,
            host,
            interactions,
            content,
            artifacts,
            policy,
            durable_ready,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "each independently replaceable runtime boundary is explicit"
    )]
    fn with_policy_internal(
        tasks: TaskRegistry,
        tools: ToolRegistry,
        resumable_tools: ResumableToolRegistry,
        scope: RuntimeScope,
        concurrency: usize,
        host: Option<Arc<dyn DurableTaskHost>>,
        interactions: Option<Arc<dyn InteractionRouter>>,
        content: Option<ContentBindings>,
        artifacts: Option<ArtifactBindings>,
        policy: Option<Arc<dyn ToolPolicy>>,
        durable_ready: bool,
    ) -> Result<Arc<Self>> {
        if concurrency == 0 {
            return Err(Error::Invalid(
                "runtime concurrency must be positive".into(),
            ));
        }
        scope.run_limits().validate()?;
        let concurrency = scope.concurrency_bound(concurrency);
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
        if resumable_tools.definitions().next().is_some() && !durable_ready {
            return Err(Error::Invalid(
                "resumable tools require durable task admission and state".into(),
            ));
        }
        for definition in resumable_tools.definitions() {
            if tools
                .get_version(&definition.name, &definition.revision)
                .is_some()
            {
                return Err(Error::Conflict(format!(
                    "tool {}@{} has both live and resumable implementations",
                    definition.name, definition.revision,
                )));
            }
        }
        validate_task_dependencies(
            &tasks,
            &tools,
            &resumable_tools,
            &scope,
            durable_ready,
            interactions.is_some(),
            content.as_ref(),
            artifacts.as_ref(),
            policy.is_some(),
        )?;
        let policy_identity = policy.as_ref().map(|policy| policy.identity());
        let extension_runtime = scope.extension_runtime();
        let spawner = host
            .as_ref()
            .map(|host| Arc::new(HostTaskSpawner(Arc::clone(host))) as Arc<dyn TaskSpawner>);
        let state = host
            .as_ref()
            .map(|host| Arc::new(HostTaskState(Arc::clone(host))) as Arc<dyn TaskStateProvider>);
        Ok(Arc::new(Self {
            tasks,
            tools,
            resumable_tools,
            live: TaskGroup::new(concurrency),
            concurrency,
            scope,
            extension_runtime,
            host,
            state,
            spawner,
            execution: None,
            interactions,
            interaction_resolver: None,
            content,
            artifacts,
            policy,
            policy_identity,
            model: None,
            context: ContextPipeline::default(),
            fork_preparer: None,
            workspaces: None,
        }))
    }

    /// Seals the parent-authorized fork provider during binding construction.
    fn bind_fork_preparer(
        self: &Arc<Self>,
        preparer: Arc<dyn crate::fork::ForkPreparer>,
    ) -> Result<Arc<Self>> {
        let (parent, revision) = preparer.parent_snapshot();
        if parent.kind != crate::core::AggregateKind::Conversation || revision == 0 {
            return Err(Error::Invalid(
                "fork preparer needs a bound parent conversation revision".into(),
            ));
        }
        parent.stream_path()?;
        let mut bound = self.as_ref().clone();
        bound.fork_preparer = Some(ForkBinding {
            parent: parent.clone(),
            revision,
            preparer,
        });
        Ok(Arc::new(bound))
    }

    fn fork_binding(&self, request: &crate::fork::ForkRequest) -> Result<&ForkBinding> {
        if !self.scope.grants().contains("fork:publish") {
            return Err(Error::Unauthorized(
                "runtime scope lacks fork:publish".into(),
            ));
        }
        request.validate()?;
        let binding = self
            .fork_preparer
            .as_ref()
            .ok_or_else(|| Error::Unsupported("fork preparer is not bound".into()))?;
        if request.parent != binding.parent
            || request.parent_revision != binding.revision
            || binding.preparer.parent_snapshot() != (&binding.parent, binding.revision)
        {
            return Err(Error::Conflict(
                "fork request is outside the bound parent revision".into(),
            ));
        }
        Ok(binding)
    }

    /// Captures the exact requested resources without publishing a child.
    pub async fn prepare_fork(
        &self,
        request: crate::fork::ForkRequest,
    ) -> Result<crate::fork::ForkReport> {
        let binding = self.fork_binding(&request)?;
        let report = binding.preparer.prepare(request.clone()).await?;
        report.validate()?;
        if report.request != request {
            return Err(Error::Conflict("fork preparer changed the request".into()));
        }
        Ok(report)
    }

    /// Observes an uncertain preparation without redispatching captures.
    pub async fn reconcile_fork(
        &self,
        request: crate::fork::ForkRequest,
    ) -> Result<Option<crate::fork::ForkReport>> {
        let binding = self.fork_binding(&request)?;
        let report = binding.preparer.reconcile(request.clone()).await?;
        if let Some(report) = &report {
            report.validate()?;
            if report.request != request {
                return Err(Error::Conflict(
                    "reconciled fork changed the request".into(),
                ));
            }
        }
        Ok(report)
    }

    /// Binds a model only for scoped task/tool model requests. The default
    /// turn executor has its own pinned model binding.
    pub fn bind_model(
        self: &Arc<Self>,
        model: Model,
        provider: Arc<dyn ModelProvider>,
    ) -> Result<Arc<Self>> {
        Model::new(
            model.provider.clone(),
            model.name.clone(),
            model.revision.clone(),
            model.options.clone(),
        )?;
        let mut bound = self.as_ref().clone();
        bound.model = Some(ModelBinding { model, provider });
        Ok(Arc::new(bound))
    }

    /// Replaces the ordered context stages for future local task work.
    pub fn bind_context(self: &Arc<Self>, context: ContextPipeline) -> Arc<Self> {
        let mut bound = self.as_ref().clone();
        bound.context = context;
        Arc::new(bound)
    }

    /// Selects a different model for future local child work without changing
    /// the parent's pinned task, tool, policy, or authority bindings.
    pub fn scoped_model(
        self: &Arc<Self>,
        grants: Capabilities,
        limits: Limits,
        model: Model,
        provider: Arc<dyn ModelProvider>,
    ) -> Result<Arc<Self>> {
        self.scoped(grants, limits)?.bind_model(model, provider)
    }

    /// Narrows authority while replacing only the child context pipeline.
    pub fn scoped_context(
        self: &Arc<Self>,
        grants: Capabilities,
        limits: Limits,
        context: ContextPipeline,
    ) -> Result<Arc<Self>> {
        Ok(self.scoped(grants, limits)?.bind_context(context))
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

    /// Verifies an already staged input or output through a bound content or
    /// artifact owner and the runtime's effective read grant.
    pub async fn verify_readable_file(&self, file: &FileRef) -> Result<()> {
        file.validate()?;
        self.scope.limits.validate_file(file)?;
        if !read_granted(self.scope.grants(), file)? {
            return Err(Error::Unauthorized(
                "task scope cannot read this file".into(),
            ));
        }
        let mut failure = Error::Unsupported("content or artifact reader is not bound".into());
        for binding in [self.content.as_ref(), self.artifacts.as_ref()]
            .into_iter()
            .flatten()
        {
            match verified_content_bytes(binding.reader.as_ref(), file).await {
                Ok(_) => return Ok(()),
                Err(error) => failure = error,
            }
        }
        Err(failure)
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
        if self.execution.is_some() {
            return Err(Error::Unsupported(
                "live task closures cannot cross an execution provider; register a resumable task and admit it with a stable operation ID".into(),
            ));
        }
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
        let extension_leases = self.retain_task_extensions(definition, &scope)?;
        validate_value(
            &definition.input_schema,
            &serde_json::to_value(&input).map_err(|error| Error::Invalid(error.to_string()))?,
            "task input",
        )?;
        let handler = Arc::clone(handler);
        let output_schema = definition.output_schema.clone();
        let runtime = Arc::clone(self);
        let context_group = group.clone();
        let descendants = context_group.child(scope.concurrency_bound(self.concurrency));
        let (identity, receive_identity) = tokio::sync::oneshot::channel();
        let admitted = group
            .try_spawn(async move {
                let task_id = receive_identity
                    .await
                    .map_err(|_| Error::Conflict("task identity was not admitted".into()))?;
                let remaining = scope.run_limits().remaining()?;
                let run = handler(
                    TaskContext {
                        harness: runtime,
                        task_id,
                        durable_task: None,
                        descendants,
                        scope,
                        policy_overrides: policies,
                        model_steps: Arc::new(AtomicUsize::new(0)),
                    },
                    input,
                );
                let output = match remaining {
                    Some(duration) => tokio::time::timeout(duration, run)
                        .await
                        .map_err(|_| Error::Invalid("task deadline has expired".into()))??,
                    None => run.await?,
                };
                drop(extension_leases);
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

    /// Looks up a lost durable admission acknowledgement without redispatching
    /// its input or trusting a caller-selected task definition.
    pub async fn reconcile_admission(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<(TaskId, ComponentIdentity)>> {
        self.assert_durable_policy_bindings()?;
        let spawner = self
            .spawner
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task spawner is not bound".into()))?;
        let host = self
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
        let Some((task_id, admission)) = spawner.reconcile_admission(operation_id).await? else {
            return Ok(None);
        };
        if admission.operation_id != operation_id {
            return Err(Error::Conflict(
                "spawner reconciled another operation".into(),
            ));
        }
        self.validate_observed_admission(&admission)?;
        let identity = admission.task.clone();
        let registered = self
            .tasks
            .0
            .get(&(identity.name.clone(), identity.version.clone()))
            .ok_or_else(|| Error::Conflict("reconciled task is not registered".into()))?;
        if registered.identity != identity || !registered.resumable {
            return Err(Error::Conflict(
                "reconciled task implementation changed".into(),
            ));
        }
        match host.observe_admission(task_id).await {
            Ok(observed) if observed == admission => self.validate_observed_admission(&observed)?,
            Ok(_) => {
                return Err(Error::Conflict(
                    "spawner and state host admission bindings differ".into(),
                ));
            }
            Err(Error::NotFound(_)) => return Err(Error::Indeterminate(operation_id)),
            Err(error) => return Err(error),
        }
        Ok(Some((task_id, identity)))
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
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
        let observed = host.observe_admission(task_id).await?;
        self.validate_observed_admission(&observed)?;
        if observed.task != definition.identity
            || observed.output_schema != definition.output_schema
        {
            return Err(Error::Conflict(
                "durable task differs from the pinned definition".into(),
            ));
        }
        let extensions = self.retain_task_extensions_existing(definition, &self.scope)?;
        Ok(RuntimeTask::Durable {
            task_id,
            host: Arc::clone(host),
            output_schema: definition.output_schema.clone(),
            extensions: Some(extensions),
        })
    }

    /// Discovers direct durable children from their owner, not from fork or
    /// conversation history. Continuation must carry the observed revision.
    pub async fn children(
        &self,
        parent: TaskId,
        expected_revision: Option<u64>,
        after_slot: Option<String>,
        maximum: usize,
    ) -> Result<TaskChildrenPage> {
        self.assert_durable_policy_bindings()?;
        validate_children_request(parent, after_slot.as_deref(), maximum)?;
        let state = self
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
        let page = state
            .children(parent, expected_revision, after_slot.clone(), maximum)
            .await?;
        validate_children_page(&page, expected_revision, after_slot.as_deref(), maximum)?;
        Ok(page)
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
        self.assert_durable_policy_bindings()?;
        let registered = self
            .tasks
            .get_version::<I, O>(&definition.identity.name, &definition.identity.version)?;
        if !Arc::ptr_eq(&registered, definition) {
            return Err(Error::Conflict(
                "task definition is not the registered version".into(),
            ));
        }
        if parent.is_some() {
            require_task_spawn(&scope, definition)?;
        }
        let TaskImplementation::Resumable(machine) = &definition.implementation else {
            return Ok(Admission::Rejected {
                reason: "live tasks are local-only".into(),
            });
        };
        let Some(host) = &self.state else {
            return Ok(Admission::Rejected {
                reason: "durable task state is not bound".into(),
            });
        };
        let spawner = self
            .spawner
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task spawner is not bound".into()))?;
        let value =
            serde_json::to_value(input).map_err(|error| Error::Invalid(error.to_string()))?;
        validate_value(&definition.input_schema, &value, "task input")?;
        let mut expected = TaskAdmissionRecord {
            operation_id,
            task: definition.identity.clone(),
            machine: machine.identity().clone(),
            input: value.clone(),
            input_schema: definition.input_schema.clone(),
            output_schema: definition.output_schema.clone(),
            parent,
            grants: scope.grants().clone(),
            limits: scope.limits(),
            run_limits: scope.run_limits(),
            policy: self.policy_identity.clone(),
            extensions: scope.extensions().cloned(),
            execution: None,
        };
        // Reconcile an already committed operation before checking extension
        // admission state. This path must remain available after a local or
        // registry-wide disable; only the subsequent new-admission path is
        // gated by `retain_task_extensions`.
        if let Some(admission) = self
            .reconcile_scoped_admission(operation_id, definition, &expected, &scope, host, spawner)
            .await?
        {
            return Ok(admission);
        }
        if let Some(execution) = &self.execution {
            let placement = execution.qualify(&expected).await?;
            placement.validate()?;
            if placement.provider != execution.identity() {
                return Err(Error::Conflict(
                    "execution qualifier returned another provider".into(),
                ));
            }
            expected.execution = Some(placement);
        }
        let extension_leases = self.retain_task_extensions(definition, &scope)?;
        Ok(match spawner.admit(expected.clone()).await? {
            Admission::Accepted(task_id) => {
                self.verify_spawner_admission(task_id, &expected).await?;
                Admission::Accepted(RuntimeTask::Durable {
                    task_id,
                    host: Arc::clone(host),
                    output_schema: definition.output_schema.clone(),
                    extensions: extension_leases,
                })
            }
            Admission::Rejected { reason } => Admission::Rejected { reason },
            Admission::Indeterminate { operation_id } => Admission::Indeterminate { operation_id },
        })
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
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
        let admitted = host.resume_scope(task_id, operation_id).await?;
        let run_limits = admitted.run_limits();
        let scope = self
            .scope
            .narrow(admitted.grants, admitted.limits)?
            .with_run_limits(run_limits)?;
        let group = self.live.child(scope.concurrency_bound(self.concurrency));
        let descendants = group.child(scope.concurrency_bound(self.concurrency));
        Ok(TaskContext {
            harness: Arc::clone(self),
            task_id: operation_id,
            durable_task: Some(task_id),
            descendants,
            scope,
            policy_overrides: Vec::new(),
            model_steps: Arc::new(AtomicUsize::new(0)),
        })
    }
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "validates the complete immutable task dependency graph"
)]
fn validate_task_dependencies(
    tasks: &TaskRegistry,
    tools: &ToolRegistry,
    resumable_tools: &ResumableToolRegistry,
    scope: &RuntimeScope,
    has_host: bool,
    has_interactions: bool,
    content: Option<&ContentBindings>,
    artifacts: Option<&ArtifactBindings>,
    has_policy: bool,
) -> Result<()> {
    #[allow(
        clippy::too_many_arguments,
        reason = "walk context remains explicit for dependency validation"
    )]
    fn visit(
        key: &(String, String),
        tasks: &TaskRegistry,
        tools: &ToolRegistry,
        resumable_tools: &ResumableToolRegistry,
        scope: &RuntimeScope,
        has_host: bool,
        has_interactions: bool,
        content: Option<&ContentBindings>,
        artifacts: Option<&ArtifactBindings>,
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
                        resumable_tools,
                        scope,
                        has_host,
                        has_interactions,
                        content,
                        artifacts,
                        has_policy,
                        marks,
                    )?;
                } else if let Some(target) = requirement.strip_prefix("tool:") {
                    let (target, version) = target.rsplit_once('@').ok_or_else(|| {
                        Error::Invalid(format!("invalid tool dependency {requirement}"))
                    })?;
                    if tools.get_version(target, version).is_none()
                        && resumable_tools.get(target, version).is_none()
                    {
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
                    || requirement == "artifacts" && artifacts.is_some()
                    || requirement == "content:write"
                        && content
                            .and_then(|binding| binding.writer.as_ref())
                            .is_some_and(|writer| {
                                writer
                                    .volume()
                                    .capability(VolumeOperation::Write)
                                    .is_ok_and(|grant| scope.grants.contains(&grant))
                            })
                    || requirement == "artifacts:write"
                        && artifacts
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
                    || requirement.strip_prefix("extension:").is_some_and(|value| {
                        let Some((name, version)) = value.rsplit_once('@') else {
                            return false;
                        };
                        let Ok(version) = version.parse::<u32>() else {
                            return false;
                        };
                        scope.extension_runtime().is_some_and(|runtime| {
                            runtime.accepts_new_admissions()
                                && runtime.selected().iter().any(|identity| {
                                    identity.name == name && identity.version == version
                                })
                        })
                    })
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
            resumable_tools,
            scope,
            has_host,
            has_interactions,
            content,
            artifacts,
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
    descendants: TaskGroup,
    scope: RuntimeScope,
    policy_overrides: Vec<(ComponentIdentity, Arc<dyn ToolPolicy>)>,
    model_steps: Arc<AtomicUsize>,
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

    /// Attenuates task execution bounds for this context and its descendants.
    pub fn scoped_run_limits(&self, limits: TaskRunLimits) -> Result<Self> {
        let mut child = self.clone();
        child.scope = self.scope.clone().with_run_limits(limits)?;
        child.descendants = self
            .descendants
            .child(child.scope.concurrency_bound(self.harness.concurrency));
        Ok(child)
    }

    /// Overrides only this live task's model binding for itself and local
    /// descendants. Durable work needs a recorded provider transition.
    pub fn scoped_model(
        &self,
        grants: Capabilities,
        limits: Limits,
        model: Model,
        provider: Arc<dyn ModelProvider>,
    ) -> Result<Self> {
        if self.durable_task.is_some() {
            return Err(Error::Unsupported(
                "durable model override requires a recorded effect".into(),
            ));
        }
        let mut child = self.scoped(grants, limits)?;
        child.harness = child.harness.bind_model(model, provider)?;
        Ok(child)
    }

    /// Places newly admitted descendants on another qualified provider. A
    /// resumed task must record the transition as a durable effect first.
    pub fn scoped_execution(
        &self,
        grants: Capabilities,
        limits: Limits,
        execution: Arc<dyn ExecutionProvider>,
    ) -> Result<Self> {
        if self.durable_task.is_some() {
            return Err(Error::Unsupported(
                "durable execution override requires a recorded effect".into(),
            ));
        }
        let mut child = self.scoped(grants, limits)?;
        child.harness = self.harness.scoped_execution(
            child.scope.grants().clone(),
            child.scope.limits(),
            execution,
        )?;
        Ok(child)
    }

    /// Builds a transient model view from explicit sources for live task code.
    /// A resumable task must record this provider boundary before recovery.
    pub async fn build_context(&self, input: &ContextInput) -> Result<Context> {
        if self.durable_task.is_some() {
            return Err(Error::Unsupported(
                "durable context build requires a recorded effect".into(),
            ));
        }
        let context = self.harness.context.run(input).await?;
        if context.messages.len() > self.scope.limits().context_messages {
            return Err(Error::Invalid("model context count exceeds limit".into()));
        }
        for message in &context.messages {
            message.content.validate_limits(self.scope.limits())?;
        }
        Ok(context)
    }

    async fn validate_model_messages(&self, messages: &[ModelMessage]) -> Result<()> {
        if messages.is_empty() || messages.len() > self.scope.limits().context_messages {
            return Err(Error::Invalid("model context count is invalid".into()));
        }
        for message in messages {
            message.content.validate_limits(self.scope.limits())?;
            let parts = match &message.content {
                ModelContent::Text(_) => continue,
                ModelContent::Part(part) => std::slice::from_ref(part),
                ModelContent::Parts(parts) => parts.as_slice(),
            };
            for part in parts {
                if let ModelContentPart::File { file, .. } = part {
                    if !read_granted(self.scope.grants(), file)? {
                        return Err(Error::Unauthorized(
                            "task scope cannot project this file".into(),
                        ));
                    }
                    let content =
                        self.harness.content.as_ref().ok_or_else(|| {
                            Error::Unsupported("content reader is not bound".into())
                        })?;
                    content.reader.verify(file).await?;
                }
            }
        }
        Ok(())
    }

    /// Replaces this live task's context stages without changing its parent.
    pub fn scoped_context(
        &self,
        grants: Capabilities,
        limits: Limits,
        context: ContextPipeline,
    ) -> Result<Self> {
        if self.durable_task.is_some() {
            return Err(Error::Unsupported(
                "durable context override requires owner-host admission".into(),
            ));
        }
        let mut child = self.scoped(grants, limits)?;
        child.harness = child.harness.bind_context(context);
        Ok(child)
    }

    /// Runs one bounded, provider-neutral model request from live task code.
    /// Durable tasks must use a resumable recorded effect instead of hidden I/O.
    pub async fn model_events(
        &self,
        messages: Vec<ModelMessage>,
        max_output_tokens: Option<u32>,
    ) -> Result<Vec<ModelEvent>> {
        if self.durable_task.is_some() {
            return Err(Error::Unsupported(
                "durable model request requires a recorded effect".into(),
            ));
        }
        if !self.scope.grants().contains("model:generate") {
            return Err(Error::Unauthorized(
                "task scope lacks model:generate".into(),
            ));
        }
        let deadline = self
            .scope
            .run_limits()
            .remaining()?
            .map(|remaining| tokio::time::Instant::now() + remaining);
        let binding = self
            .harness
            .model
            .as_ref()
            .ok_or_else(|| Error::Unsupported("task model provider is not bound".into()))?;
        if max_output_tokens == Some(0) {
            return Err(Error::Invalid(
                "model output token bound must be positive".into(),
            ));
        }
        self.validate_model_messages(&messages).await?;
        let tools = self
            .harness
            .tools
            .definitions()?
            .into_iter()
            .filter(|tool| {
                self.scope
                    .grants()
                    .contains(&format!("tool:call:{}", tool.name))
            })
            .collect();
        let step_bound = self
            .scope
            .run_limits()
            .max_steps
            .map_or(self.scope.limits().model_steps, |bound| {
                bound.min(self.scope.limits().model_steps)
            });
        self.model_steps
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                (used < step_bound).then_some(used + 1)
            })
            .map_err(|_| Error::Invalid("task exceeded its model step limit".into()))?;
        let request = ModelRequest {
            model: binding.model.clone(),
            messages,
            tools,
            max_output_tokens,
        };
        let mut events = Vec::new();
        let mut admission = ModelEventAdmission::default();
        let mut bytes = 0_u64;
        let mut stream = binding.provider.generate(request);
        loop {
            let next = match deadline {
                Some(deadline) => tokio::time::timeout_at(deadline, stream.next())
                    .await
                    .map_err(|_| Error::Invalid("task deadline has expired".into()))?,
                None => stream.next().await,
            };
            let Some(event) = next else { break };
            let event = event?;
            admission.observe(&event, self.scope.limits())?;
            let event_bytes = crate::contract::canonical_json_bytes(&event)?;
            bytes = bytes
                .checked_add(event_bytes.len() as u64)
                .ok_or_else(|| Error::Invalid("model output size overflow".into()))?;
            if bytes > self.scope.limits().file_bytes {
                return Err(Error::Invalid("model output exceeds file limit".into()));
            }
            events.push(event);
        }
        if !admission.completed() {
            return Err(Error::Invalid(
                "model stream ended without completion".into(),
            ));
        }
        Ok(events)
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
        self.read_bound_file(self.harness.content.as_ref(), file)
            .await
    }

    /// Resolves an immutable artifact without coupling its provider to the
    /// workspace/content binding. The same owner-mediated grant is required.
    pub async fn read_artifact(&self, file: &FileRef) -> Result<Vec<u8>> {
        self.read_bound_file(self.harness.artifacts.as_ref(), file)
            .await
    }

    /// Discovers a private owner directory without a prior `FileRef`. An attached
    /// reader must retain the owner's signed subtree capability; the provider
    /// authenticates that grant again on every page.
    pub async fn list_private_directory(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        after: Option<&str>,
        maximum_entries: u32,
    ) -> Result<PrivateDirectoryPage> {
        self.require_private_directory(volume, granted_prefix, path)?;
        if maximum_entries == 0 || maximum_entries > 4096 {
            return Err(Error::Invalid(
                "private directory page limit is invalid".into(),
            ));
        }
        let content = self
            .harness
            .content
            .as_ref()
            .ok_or_else(|| Error::Unsupported("content reader is not bound".into()))?;
        let page = content
            .reader
            .list_private_directory(
                volume,
                granted_prefix,
                path,
                expected_generation,
                after,
                maximum_entries,
            )
            .await?;
        page.validate()?;
        if page.generation.as_resource().provider() != volume.provider()
            || expected_generation.is_some_and(|expected| &page.generation != expected)
            || page.entries.len() > maximum_entries as usize
        {
            return Err(Error::Conflict(
                "private directory provider returned another page".into(),
            ));
        }
        for entry in &page.entries {
            crate::conversation::validate_content_path(&entry.name)?;
            if entry.name.contains('/')
                || (path.is_empty() && entry.name == ".system")
                || after.is_some_and(|cursor| entry.name.as_bytes() <= cursor.as_bytes())
            {
                return Err(Error::Conflict(
                    "private directory provider returned a forbidden name".into(),
                ));
            }
        }
        Ok(page)
    }

    /// Resolves the current file at a named private path into verified pinned
    /// bytes; the returned `FileRef` can later be shared independently.
    pub async fn read_private_path(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
    ) -> Result<(FileRef, Vec<u8>)> {
        self.require_private_directory(volume, granted_prefix, path)?;
        if path.is_empty() {
            return Err(Error::Invalid("private file path is empty".into()));
        }
        let content = self
            .harness
            .content
            .as_ref()
            .ok_or_else(|| Error::Unsupported("content reader is not bound".into()))?;
        let (file, bytes) = content
            .reader
            .read_private_path(volume, granted_prefix, path, expected_generation)
            .await?;
        file.validate()?;
        self.scope.limits.validate_file(&file)?;
        if file.volume() != volume || file.path() != path {
            return Err(Error::Conflict(
                "private path provider returned another file".into(),
            ));
        }
        file.descriptor().verify(&bytes)?;
        Ok((file, bytes))
    }

    fn require_private_directory(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
    ) -> Result<()> {
        volume.validate()?;
        if volume.class() != VolumeClass::AgentPrivate
            || crate::conversation::is_internal_path(path)
            || (!path.is_empty() && crate::conversation::validate_content_path(path).is_err())
            || (!granted_prefix.is_empty()
                && crate::conversation::validate_content_path(granted_prefix).is_err())
        {
            return Err(Error::Invalid(
                "private directory path or volume is invalid".into(),
            ));
        }
        let exact = volume.directory_read_capability(granted_prefix)?;
        let full = volume.capability(VolumeOperation::Read)?;
        if !self.scope.grants().contains(&exact) && !self.scope.grants().contains(&full) {
            return Err(Error::Unauthorized(
                "task scope cannot discover this directory".into(),
            ));
        }
        Ok(())
    }

    async fn read_bound_file(
        &self,
        binding: Option<&ContentBindings>,
        file: &FileRef,
    ) -> Result<Vec<u8>> {
        file.validate()?;
        self.scope.limits.validate_file(file)?;
        if !read_granted(&self.scope.grants, file)? {
            return Err(Error::Unauthorized(
                "task scope cannot read this file".into(),
            ));
        }
        let content =
            binding.ok_or_else(|| Error::Unsupported("content reader is not bound".into()))?;
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
        self.stage_bound_file(
            self.harness.content.as_ref(),
            operation_id,
            path,
            bytes,
            media_type,
            display_name,
        )
        .await
    }

    /// Publishes immutable artifact bytes under a caller-retained operation ID.
    /// The artifact binding may use a different provider and owner volume.
    pub async fn stage_artifact(
        &self,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
    ) -> Result<FileRef> {
        self.stage_bound_file(
            self.harness.artifacts.as_ref(),
            operation_id,
            path,
            bytes,
            media_type,
            display_name,
        )
        .await
    }

    async fn stage_bound_file(
        &self,
        binding: Option<&ContentBindings>,
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
        let content =
            binding.ok_or_else(|| Error::Unsupported("content publisher is not bound".into()))?;
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
        let operation_id = OperationId::new();
        let invocation = ToolInvocation {
            operation_id,
            call_id: operation_id.to_string(),
            name: tool.definition.name.clone(),
            arguments,
        };
        binding.executor.authorize(Some(&self.scope), &invocation)?;
        self.authorize_tool(&tool.definition, &invocation).await?;
        let context = ToolContext::new(
            self.clone(),
            invocation.operation_id,
            invocation.call_id.clone(),
        )?;
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
        let state = self
            .harness
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
        let context = ToolContext::new(self.clone(), operation_id, operation_id.to_string())?;
        Ok(
            match state
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

    /// Opens one registered, version-pinned resumable tool under this task's
    /// exact call scope. The owner-supplied journal retains the admission and
    /// subsequent transitions; callers must reconcile the same operation
    /// after uncertainty rather than creating another invocation.
    pub async fn open_resumable_tool<I: Serialize>(
        &self,
        operation_id: OperationId,
        name: &str,
        revision: &str,
        input: I,
        journal: Arc<dyn WorkflowJournal>,
    ) -> Result<ResumableToolSession> {
        let tool = self
            .harness
            .resumable_tools
            .get(name, revision)
            .ok_or_else(|| Error::NotFound(format!("resumable tool {name}@{revision}")))?;
        let arguments =
            serde_json::to_value(input).map_err(|error| Error::Invalid(error.to_string()))?;
        let call_id = operation_id.to_string();
        let invocation = ToolInvocation {
            operation_id,
            call_id: call_id.clone(),
            name: name.to_owned(),
            arguments,
        };
        let context = ToolContext::new(self.clone(), operation_id, call_id)?;
        tool.open(context, invocation, journal).await
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
        require_task_spawn(&self.scope, definition)?;
        // A parent may await its child while holding its own group permit.
        // An independently bounded descendant group prevents recursive
        // fork-join from deadlocking at concurrency one.
        self.harness
            .spawn_in_group(
                &self.descendants,
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
            group: self
                .descendants
                .child(self.scope.concurrency_bound(self.harness.concurrency)),
            scope: self.scope.clone(),
            policy_overrides: self.policy_overrides.clone(),
            durable_parent: self.durable_task,
            id: None,
            batch_policy: BatchGroupPolicy::CollectAll,
        }
    }

    /// Retains an identity before admitting a durable batch. The ID is
    /// caller-owned and survives loss of the in-process group handle.
    #[must_use]
    pub fn group_with_id(&self, id: GroupId) -> RuntimeGroup {
        let mut group = self.group();
        group.id = Some(id);
        group
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
                .state
                .as_ref()
                .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
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
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
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
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
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
                .state
                .as_ref()
                .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
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
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
        host.reconcile_effect(task, effect_id).await
    }
}

fn require_task_spawn<I, O>(scope: &RuntimeScope, definition: &TaskDefinition<I, O>) -> Result<()> {
    require_descendant_grant(scope.grants(), &definition.identity)
}

impl AgentHarness {
    fn retain_task_extensions<I, O>(
        &self,
        definition: &TaskDefinition<I, O>,
        scope: &RuntimeScope,
    ) -> Result<Option<ExtensionLeases>> {
        let requirements = definition
            .requirements()
            .iter()
            .filter(|requirement| requirement.starts_with("extension:"))
            .cloned()
            .collect::<Vec<_>>();
        if requirements.is_empty() {
            return Ok(None);
        }
        let runtime = self
            .extension_runtime
            .as_ref()
            .ok_or_else(|| Error::Unsupported("task requires a native extension runtime".into()))?;
        runtime.validate_admission(scope.extensions())?;
        Ok(Some(runtime.retain_for_task(requirements)?))
    }

    fn retain_task_extensions_existing<I, O>(
        &self,
        definition: &TaskDefinition<I, O>,
        scope: &RuntimeScope,
    ) -> Result<ExtensionLeases> {
        let requirements = definition
            .requirements()
            .iter()
            .filter(|requirement| requirement.starts_with("extension:"))
            .cloned()
            .collect::<Vec<_>>();
        if requirements.is_empty() {
            return Ok(ExtensionLeases::empty());
        }
        let runtime = self.extension_runtime.as_ref().ok_or_else(|| {
            Error::Unsupported("retained task requires a native extension runtime".into())
        })?;
        runtime.validate_admission(scope.extensions())?;
        runtime.retain_for_existing(requirements)
    }
}

pub(crate) fn require_descendant_grant(
    grants: &Capabilities,
    identity: &ComponentIdentity,
) -> Result<()> {
    let capability = format!("task:spawn:{}@{}", identity.name, identity.version,);
    if !grants.contains(&capability) {
        return Err(Error::Unauthorized(format!("scope lacks {capability}")));
    }
    Ok(())
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
    durable_parent: Option<TaskId>,
    id: Option<GroupId>,
    batch_policy: BatchGroupPolicy,
}

/// Ordered immutable batch selection. Reusing an ID with a different body,
/// task definition, group, or effective scope must conflict at admission.
pub struct Batch<I> {
    /// Caller-retained identity allocated before dispatch.
    pub id: BatchId,
    /// Complete ordered input list.
    pub inputs: Vec<I>,
}

impl<I> Batch<I> {
    /// Creates an ordered batch with a stable identity.
    #[must_use]
    pub fn new(id: BatchId, inputs: Vec<I>) -> Self {
        Self { id, inputs }
    }
}

/// One unambiguous slot in an admitted batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InputKey {
    /// Batch owning this input.
    pub batch_id: BatchId,
    /// Zero-based position in the immutable ordered request.
    pub index: usize,
}

/// Group behavior retained with the exact batch identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BatchGroupPolicy {
    /// Preserve every admission and terminal outcome without cancelling siblings.
    CollectAll,
    /// Request owner-retained sibling cancellation after a definitive failure.
    CancelOnFailure,
}

/// Exact immutable batch request presented to a durable owner. Inputs remain
/// private staged bytes in the host; only the manifest ref enters its Stream.
#[derive(Clone)]
pub struct DurableBatchRequest {
    /// Stable group identity.
    pub group_id: GroupId,
    /// Stable batch identity.
    pub batch_id: BatchId,
    /// Group completion behavior pinned at admission.
    pub group_policy: BatchGroupPolicy,
    /// Pinned task definition.
    pub task: ComponentIdentity,
    /// Pinned resumable implementation.
    pub machine: MachineIdentity,
    /// Ordered, schema-validated inputs.
    pub inputs: Vec<Value>,
    /// Pinned input schema checked again by the durable owner.
    pub input_schema: Value,
    /// Pinned output schema.
    pub output_schema: Value,
    /// Durable parent owning descendants, or none for a root-owned batch.
    pub parent: Option<TaskId>,
    /// Effective immutable authority at batch admission.
    pub scope: RuntimeScope,
    /// Exact policy implementation enforced by the host.
    pub policy: Option<ComponentIdentity>,
    /// Qualified immutable route shared by the batch's indexed members.
    pub execution: Option<ExecutionPlacement>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableBatchWire {
    contract: String,
    group_id: GroupId,
    batch_id: BatchId,
    group_policy: BatchGroupPolicy,
    task: ComponentIdentity,
    machine: MachineIdentity,
    inputs: Vec<Value>,
    input_schema: Value,
    output_schema: Value,
    parent: Option<TaskId>,
    grants: Capabilities,
    limits: Limits,
    run_limits: TaskRunLimits,
    extensions: Option<ExtensionAdmission>,
    policy: Option<ComponentIdentity>,
    execution: Option<ExecutionPlacement>,
}

impl DurableBatchRequest {
    /// Validates the complete retained manifest before any child is observed
    /// or admitted. A syntactically valid JSON envelope is not sufficient.
    pub fn validate(&self) -> Result<()> {
        if self.inputs.len() > 65_536 {
            return Err(Error::Invalid("batch has too many inputs".into()));
        }
        validate_identity(&self.task.name, &self.task.version)?;
        validate_component_label(&self.machine.name, "machine name")?;
        validate_component_label(&self.machine.version, "machine version")?;
        if self.task.digest == [0; 32]
            || self.machine.digest == [0; 32]
            || self.machine.name != self.task.name
            || self.machine.version != self.task.version
        {
            return Err(Error::Invalid(
                "batch implementation identity is invalid".into(),
            ));
        }
        validate_task_schemas(&self.input_schema, &self.output_schema, true)?;
        self.scope.limits().validate()?;
        self.scope.run_limits().validate()?;
        if let Some(policy) = &self.policy {
            validate_policy_identity(policy)?;
        }
        if let Some(execution) = &self.execution {
            execution.validate()?;
        }
        for input in &self.inputs {
            validate_value(&self.input_schema, input, "batch input")?;
        }
        if crate::contract::canonical_json_bytes(&self.canonical_value())?.len() as u64
            > self.scope.limits().file_bytes
        {
            return Err(Error::Invalid("batch request exceeds file limit".into()));
        }
        Ok(())
    }

    /// Reconstructs only the exact v2 request stored by its owner; unknown or
    /// omitted fields cannot silently change the admitted child identities.
    pub fn from_canonical_value(value: Value) -> Result<Self> {
        let canonical = value.clone();
        let wire: DurableBatchWire =
            serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
        if wire.contract != "harness.batch.v2" {
            return Err(Error::Invalid("unsupported batch contract".into()));
        }
        let scope = RuntimeScope::new(wire.grants, wire.limits)?
            .with_run_limits(wire.run_limits)?
            .with_replayed_extensions(wire.extensions)?;
        let request = Self {
            group_id: wire.group_id,
            batch_id: wire.batch_id,
            group_policy: wire.group_policy,
            task: wire.task,
            machine: wire.machine,
            inputs: wire.inputs,
            input_schema: wire.input_schema,
            output_schema: wire.output_schema,
            parent: wire.parent,
            scope,
            policy: wire.policy,
            execution: wire.execution,
        };
        if request.canonical_value() != canonical {
            return Err(Error::Invalid("batch request is not canonical v2".into()));
        }
        request.validate()?;
        Ok(request)
    }

    /// Canonical batch identity binds all executable inputs and authority.
    pub fn canonical_value(&self) -> Value {
        serde_json::json!({
            "contract": "harness.batch.v2",
            "group_id": self.group_id,
            "batch_id": self.batch_id,
            "group_policy": self.group_policy,
            "task": self.task,
            "machine": self.machine,
            "inputs": self.inputs,
            "input_schema": self.input_schema,
            "output_schema": self.output_schema,
            "parent": self.parent,
            "grants": self.scope.grants(),
            "limits": self.scope.limits(),
            "run_limits": self.scope.run_limits(),
            "extensions": self.scope.extensions(),
            "policy": self.policy,
            "execution": self.execution,
        })
    }

    /// Derives the sole admission record for one indexed member. The owner
    /// and caller use this same construction during admit and reconciliation.
    pub fn member_admission(&self, index: usize) -> Result<TaskAdmissionRecord> {
        let input = self
            .inputs
            .get(index)
            .ok_or_else(|| Error::Invalid("batch member index is missing".into()))?;
        Ok(TaskAdmissionRecord {
            operation_id: self.operation_id(index),
            task: self.task.clone(),
            machine: self.machine.clone(),
            input: input.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            parent: self.parent,
            grants: self.scope.grants().clone(),
            limits: self.scope.limits(),
            run_limits: self.scope.run_limits(),
            policy: self.policy.clone(),
            extensions: self.scope.extensions().cloned(),
            execution: self.execution.clone(),
        })
    }

    /// Stable per-index operation independent of admission or completion order.
    #[must_use]
    pub fn operation_id(&self, index: usize) -> OperationId {
        batch_member_operation_id(self.group_id, self.batch_id, index)
    }
}

/// One canonical operation identity shared by native and WASM batch facades.
#[must_use]
pub fn batch_member_operation_id(
    group_id: GroupId,
    batch_id: BatchId,
    index: usize,
) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness/v2/batch-member\0");
    hasher.update(&group_id.into_bytes());
    hasher.update(&batch_id.into_bytes());
    hasher.update(&(index as u64).to_be_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

/// One exact input slot, preserving the admission decision before and after
/// terminal observation without introducing another key-bearing shape.
pub struct BatchEntry<T> {
    /// Stable batch/index key.
    pub key: InputKey,
    /// Accepted value, rejection, or unresolved operation.
    pub admission: Admission<T>,
}

/// Ordered admission receipt; every submitted input retains a slot.
pub struct AdmissionBatch<O> {
    /// Owning group identity.
    pub group_id: GroupId,
    /// Immutable request identity.
    pub batch_id: BatchId,
    /// Exactly one entry per input, in request order.
    pub entries: Vec<BatchEntry<RuntimeTask<O>>>,
    policy: BatchGroupPolicy,
    spawner: Arc<dyn TaskSpawner>,
    extensions: Option<ExtensionLeases>,
}

/// Ordered outcome of all observable batch slots. `complete` is false while
/// an admission, task outcome, or observation remains unresolved.
pub struct BatchOutcome<O> {
    /// Stable group identity.
    pub group_id: GroupId,
    /// Stable batch identity.
    pub batch_id: BatchId,
    /// One result for every submitted input.
    pub entries: Vec<BatchEntry<Result<Outcome<O>>>>,
    /// Whether every slot has a definitive admission or terminal result.
    pub complete: bool,
    /// Owner-retained cancellation request, if a failure triggered the policy.
    pub cancellation: Option<Result<BatchCancellationReport>>,
}

/// One cancellation request, never a claim that the task is terminal.
pub enum BatchCancellationStatus {
    /// The owner accepted an idempotent cancellation request; observe outcome separately.
    Requested,
    /// The member was definitively rejected at admission.
    NotAdmitted {
        /// Stable explanation for why this member was never admitted.
        reason: String,
    },
    /// Admission or cancellation has an unresolved operation identity.
    Indeterminate {
        /// Operation whose admission or cancellation must be reconciled.
        operation_id: OperationId,
    },
    /// The owner did not confirm the request; retry reconciliation by batch ID.
    Unresolved {
        /// Bounded provider explanation retained for batch reconciliation.
        message: String,
    },
}

/// Per-member request report for a retained batch. Requests are non-atomic;
/// a later admission can race this observation until an owner seals the batch.
pub struct BatchCancellationReport {
    /// Owning group.
    pub group_id: GroupId,
    /// Caller-retained batch.
    pub batch_id: BatchId,
    /// Every original input slot in order.
    pub entries: Vec<(InputKey, BatchCancellationStatus)>,
}

async fn observe_batch_entry<O: DeserializeOwned + Send + 'static>(
    entry: BatchEntry<RuntimeTask<O>>,
) -> BatchEntry<Result<Outcome<O>>> {
    let admission = match entry.admission {
        Admission::Accepted(task) => Admission::Accepted(task.result().await),
        Admission::Rejected { reason } => Admission::Rejected { reason },
        Admission::Indeterminate { operation_id } => Admission::Indeterminate { operation_id },
    };
    BatchEntry {
        key: entry.key,
        admission,
    }
}

async fn request_batch_cancellation(
    spawner: &Arc<dyn TaskSpawner>,
    group_id: GroupId,
    batch_id: BatchId,
    count: usize,
) -> Result<BatchCancellationReport> {
    let report = spawner
        .cancel_batch(batch_id)
        .await?
        .ok_or_else(|| Error::Storage("retained batch disappeared during cancellation".into()))?;
    if report.group_id != group_id
        || report.batch_id != batch_id
        || report.entries.len() != count
        || report
            .entries
            .iter()
            .enumerate()
            .any(|(index, (key, _))| key.batch_id != batch_id || key.index != index)
    {
        return Err(Error::Conflict(
            "batch owner returned another cancellation report".into(),
        ));
    }
    Ok(report)
}

fn cancellation_unresolved(cancellation: &Option<Result<BatchCancellationReport>>) -> bool {
    cancellation.as_ref().is_some_and(|result| match result {
        Err(_) => true,
        Ok(report) => report.entries.iter().any(|(_, status)| {
            matches!(
                status,
                BatchCancellationStatus::Indeterminate { .. }
                    | BatchCancellationStatus::Unresolved { .. }
            )
        }),
    })
}

fn batch_is_complete<O>(
    entries: &[BatchEntry<Result<Outcome<O>>>],
    cancellation: &Option<Result<BatchCancellationReport>>,
) -> bool {
    entries.iter().all(|entry| match &entry.admission {
        Admission::Accepted(Ok(
            Outcome::Succeeded(_) | Outcome::Failed { .. } | Outcome::Cancelled,
        ))
        | Admission::Rejected { .. } => true,
        Admission::Accepted(Ok(Outcome::Indeterminate { .. }) | Err(_))
        | Admission::Indeterminate { .. } => false,
    }) && cancellation.as_ref().is_none_or(|result| {
        result.as_ref().is_ok_and(|report| {
            report.entries.iter().all(|(_, status)| {
                matches!(
                    status,
                    BatchCancellationStatus::Requested
                        | BatchCancellationStatus::NotAdmitted { .. }
                )
            })
        })
    })
}

impl<O: DeserializeOwned + Send + 'static> AdmissionBatch<O> {
    /// Observes accepted members concurrently while preserving every original
    /// input slot, including rejected and unresolved admissions. Dropping the
    /// wait never cancels accepted children.
    pub async fn join(self) -> BatchOutcome<O> {
        let Self {
            group_id,
            batch_id,
            entries,
            policy,
            spawner,
            // Keep native implementations retained until every admitted
            // member has reached an observable terminal state. The leases
            // are intentionally batch-scoped, not copied into each handle.
            extensions,
        } = self;
        let _extensions = extensions;
        let count = entries.len();
        // Cancellation must observe every admitted sibling: a failed member
        // behind a window of suspended waits otherwise never gets polled.
        let concurrency = if policy == BatchGroupPolicy::CancelOnFailure {
            count.max(1)
        } else {
            count.clamp(1, 64)
        };
        let mut slots = entries
            .iter()
            .map(|entry| {
                let admission = match &entry.admission {
                    Admission::Accepted(_) => Admission::Accepted(Err(Error::Indeterminate(
                        batch_member_operation_id(group_id, batch_id, entry.key.index),
                    ))),
                    Admission::Rejected { reason } => Admission::Rejected {
                        reason: reason.clone(),
                    },
                    Admission::Indeterminate { operation_id } => Admission::Indeterminate {
                        operation_id: *operation_id,
                    },
                };
                BatchEntry {
                    key: entry.key,
                    admission,
                }
            })
            .collect::<Vec<BatchEntry<Result<Outcome<O>>>>>();
        let mut completions = stream::iter(entries)
            .map(observe_batch_entry)
            .buffer_unordered(concurrency);
        let mut cancellation = None;
        while let Some(entry) = completions.next().await {
            let failed = matches!(
                &entry.admission,
                Admission::Rejected { .. }
                    | Admission::Accepted(Ok(Outcome::Failed { .. } | Outcome::Cancelled))
            );
            if failed && policy == BatchGroupPolicy::CancelOnFailure && cancellation.is_none() {
                cancellation =
                    Some(request_batch_cancellation(&spawner, group_id, batch_id, count).await);
            }
            let index = entry.key.index;
            let Some(slot) = slots.get_mut(index) else {
                cancellation = Some(Err(Error::Conflict(
                    "batch owner returned an invalid member index".into(),
                )));
                break;
            };
            *slot = entry;
            if cancellation_unresolved(&cancellation) {
                break;
            }
        }
        let entries = slots;
        let complete = batch_is_complete(&entries, &cancellation);
        BatchOutcome {
            group_id,
            batch_id,
            entries,
            complete,
            cancellation,
        }
    }
}

/// A typed ordered map cannot silently discard a rejected admission, failed
/// child, or unknown outcome. Successful entries remain available on failure.
pub enum RuntimeMap<O> {
    /// Every input admitted and completed successfully, in input order.
    Complete(Vec<O>),
    /// One entry per input, including admission or observation errors.
    Incomplete(Vec<Result<Outcome<O>>>),
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
    /// Selects the immutable completion policy for subsequently admitted
    /// durable batches. Reusing a batch ID under another policy conflicts.
    #[must_use]
    pub fn with_batch_policy(mut self, policy: BatchGroupPolicy) -> Self {
        self.batch_policy = policy;
        self
    }

    fn durable_batch_request<I, O>(
        &self,
        definition: &Arc<TaskDefinition<I, O>>,
        batch: Batch<I>,
    ) -> Result<DurableBatchRequest>
    where
        I: Serialize + 'static,
        O: 'static,
    {
        self.harness.assert_durable_policy_bindings()?;
        if self.batch_policy == BatchGroupPolicy::CancelOnFailure
            && !self
                .harness
                .spawner
                .as_ref()
                .is_some_and(|spawner| spawner.supports_batch_cancellation())
        {
            return Err(Error::Unsupported(
                "durable cancel-on-failure requires owner-retained batch cancellation".into(),
            ));
        }
        let group_id = self.id.ok_or_else(|| {
            Error::Invalid("durable batch requires a caller-retained group ID".into())
        })?;
        let parent = self.durable_parent;
        if !self.policy_overrides.is_empty() {
            return Err(Error::Unsupported(
                "durable batch policy overrides require a pinned host policy".into(),
            ));
        }
        if batch.inputs.len() > 65_536 {
            return Err(Error::Invalid("batch has too many inputs".into()));
        }
        let registered = self
            .harness
            .tasks
            .get_version::<I, O>(&definition.identity.name, &definition.identity.version)?;
        if !Arc::ptr_eq(&registered, definition) {
            return Err(Error::Conflict(
                "batch task definition is not the registered version".into(),
            ));
        }
        if parent.is_some() {
            require_task_spawn(&self.scope, definition)?;
        }
        let TaskImplementation::Resumable(machine) = &definition.implementation else {
            return Err(Error::Unsupported(
                "durable batch cannot admit a process-local task".into(),
            ));
        };
        let inputs = batch
            .inputs
            .into_iter()
            .map(|input| {
                let value = serde_json::to_value(input)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                validate_value(&definition.input_schema, &value, "batch input")?;
                Ok(value)
            })
            .collect::<Result<Vec<_>>>()?;
        let request = DurableBatchRequest {
            group_id,
            batch_id: batch.id,
            group_policy: self.batch_policy,
            task: definition.identity.clone(),
            machine: machine.identity().clone(),
            inputs,
            input_schema: definition.input_schema.clone(),
            output_schema: definition.output_schema.clone(),
            parent,
            scope: self.scope.clone(),
            policy: self.harness.policy_identity.clone(),
            execution: None,
        };
        request.validate()?;
        Ok(request)
    }

    async fn admitted_batch<O>(
        &self,
        request: &DurableBatchRequest,
        decisions: Vec<Admission<TaskId>>,
        host: Arc<dyn TaskStateProvider>,
        extensions: Option<ExtensionLeases>,
    ) -> Result<AdmissionBatch<O>> {
        if decisions.len() != request.inputs.len() {
            return Err(Error::Storage("batch host omitted an input slot".into()));
        }
        let mut entries = Vec::with_capacity(decisions.len());
        for (index, admission) in decisions.into_iter().enumerate() {
            let operation_id = request.operation_id(index);
            let admission = match admission {
                Admission::Accepted(task_id) => {
                    let expected = request.member_admission(index)?;
                    match self
                        .harness
                        .verify_spawner_admission(task_id, &expected)
                        .await
                    {
                        Ok(()) => Admission::Accepted(RuntimeTask::Durable {
                            task_id,
                            host: Arc::clone(&host),
                            output_schema: request.output_schema.clone(),
                            extensions: None,
                        }),
                        Err(Error::Indeterminate(returned)) if returned == operation_id => {
                            Admission::Indeterminate { operation_id }
                        }
                        Err(error) => return Err(error),
                    }
                }
                Admission::Rejected { reason } => Admission::Rejected { reason },
                Admission::Indeterminate {
                    operation_id: returned,
                } => {
                    if returned != operation_id {
                        return Err(Error::Storage(
                            "batch host returned another operation identity".into(),
                        ));
                    }
                    Admission::Indeterminate { operation_id }
                }
            };
            entries.push(BatchEntry {
                key: InputKey {
                    batch_id: request.batch_id,
                    index,
                },
                admission,
            });
        }
        let spawner = self
            .harness
            .spawner
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable batch spawner is not bound".into()))?;
        if request.group_policy == BatchGroupPolicy::CancelOnFailure
            && !spawner.supports_batch_cancellation()
        {
            return Err(Error::Unsupported(
                "durable batch cancellation is not bound".into(),
            ));
        }
        Ok(AdmissionBatch {
            group_id: request.group_id,
            batch_id: request.batch_id,
            entries,
            policy: request.group_policy,
            spawner: Arc::clone(spawner),
            extensions,
        })
    }

    /// Publishes the exact batch request before any child admission. A lost
    /// acknowledgement retains indexed identities for explicit reconciliation.
    pub async fn spawn_batch<I, O>(
        &self,
        definition: &Arc<TaskDefinition<I, O>>,
        batch: Batch<I>,
    ) -> Result<AdmissionBatch<O>>
    where
        I: Serialize + 'static,
        O: 'static,
    {
        let mut request = self.durable_batch_request(definition, batch)?;
        let host = self
            .harness
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable batch state is not bound".into()))?;
        let spawner = self
            .harness
            .spawner
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable batch spawner is not bound".into()))?;
        let retained_batch = spawner.load_batch(request.batch_id).await?;
        if let Some(retained) = retained_batch.as_ref() {
            retained.validate()?;
            let mut base = retained.clone();
            base.execution = None;
            if base.canonical_value() != request.canonical_value() {
                return Err(Error::Conflict(
                    "batch identity belongs to another request".into(),
                ));
            }
            self.harness
                .validate_execution_placement(retained.execution.as_ref())?;
            request = retained.clone();
        } else if let Some(execution) = &self.harness.execution {
            let placement = execution.qualify_batch(&request).await?;
            placement.validate()?;
            if placement.provider != execution.identity() {
                return Err(Error::Conflict(
                    "batch qualifier returned another provider".into(),
                ));
            }
            request.execution = Some(placement);
            request.validate()?;
        }
        // `spawn_batch` may reconcile a retained request before asking the
        // owner to admit its missing members, so it remains a new-admission
        // boundary. Disabled runtimes must reject this path; the explicit
        // reconcile_batch APIs below are reserved for retained operations.
        let extension_leases = self
            .harness
            .retain_task_extensions(definition, &request.scope)?;
        let decisions = spawner.admit_batch(request.clone()).await?;
        self.admitted_batch(&request, decisions, Arc::clone(host), extension_leases)
            .await
    }

    /// Observes one exact retained request without admitting absent members.
    pub async fn reconcile_batch<I, O>(
        &self,
        definition: &Arc<TaskDefinition<I, O>>,
        batch: Batch<I>,
    ) -> Result<Option<AdmissionBatch<O>>>
    where
        I: Serialize + 'static,
        O: 'static,
    {
        let expected = self.durable_batch_request(definition, batch)?;
        let host = self
            .harness
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable batch state is not bound".into()))?;
        let spawner = self
            .harness
            .spawner
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable batch spawner is not bound".into()))?;
        let Some(request) = spawner.load_batch(expected.batch_id).await? else {
            return Ok(None);
        };
        request.validate()?;
        let mut base = request.clone();
        base.execution = None;
        if base.canonical_value() != expected.canonical_value() {
            return Err(Error::Conflict(
                "retained batch differs from its requested inputs or scope".into(),
            ));
        }
        self.harness
            .validate_execution_placement(request.execution.as_ref())?;
        let Some(decisions) = spawner.reconcile_batch(request.clone()).await? else {
            return Err(Error::Storage("retained batch disappeared".into()));
        };
        let extension_leases = self
            .harness
            .retain_task_extensions_existing(definition, &request.scope)?;
        self.admitted_batch(
            &request,
            decisions,
            Arc::clone(host),
            Some(extension_leases),
        )
        .await
        .map(Some)
    }

    /// Reattaches to an owner's immutable batch by its caller-retained ID,
    /// without reconstructing the original inputs or resubmitting absent work.
    pub async fn reconcile_batch_id<I, O>(
        &self,
        definition: &Arc<TaskDefinition<I, O>>,
        batch_id: BatchId,
    ) -> Result<Option<AdmissionBatch<O>>>
    where
        I: 'static,
        O: 'static,
    {
        self.harness.assert_durable_policy_bindings()?;
        if !self.policy_overrides.is_empty() {
            return Err(Error::Unsupported(
                "durable batch policy overrides require a pinned host policy".into(),
            ));
        }
        let group_id = self.id.ok_or_else(|| {
            Error::Invalid("durable batch requires a caller-retained group ID".into())
        })?;
        let parent = self.durable_parent;
        let registered = self
            .harness
            .tasks
            .get_version::<I, O>(&definition.identity.name, &definition.identity.version)?;
        if !Arc::ptr_eq(&registered, definition) {
            return Err(Error::Conflict("batch task definition changed".into()));
        }
        if parent.is_some() {
            require_task_spawn(&self.scope, definition)?;
        }
        let TaskImplementation::Resumable(machine) = &definition.implementation else {
            return Err(Error::Unsupported("batch task is not resumable".into()));
        };
        let host = self
            .harness
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable batch state is not bound".into()))?;
        let spawner = self
            .harness
            .spawner
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable batch spawner is not bound".into()))?;
        let Some(request) = spawner.load_batch(batch_id).await? else {
            return Ok(None);
        };
        request.validate()?;
        if request.group_id != group_id
            || request.group_policy != self.batch_policy
            || request.parent != parent
            || request.task != *definition.identity()
            || request.machine != *machine.identity()
            || request.input_schema != definition.input_schema
            || request.output_schema != definition.output_schema
            || request.scope.grants() != self.scope.grants()
            || request.scope.limits() != self.scope.limits()
            || request.scope.run_limits() != self.scope.run_limits()
            || request.scope.extensions() != self.scope.extensions()
            || request.policy.as_ref() != self.harness.policy_identity.as_ref()
        {
            return Err(Error::Conflict(
                "retained batch differs from its pinned group bindings".into(),
            ));
        }
        self.harness
            .validate_execution_placement(request.execution.as_ref())?;
        let decisions = spawner
            .reconcile_batch(request.clone())
            .await?
            .ok_or_else(|| Error::Storage("retained batch disappeared".into()))?;
        let extension_leases = self
            .harness
            .retain_task_extensions_existing(definition, &request.scope)?;
        self.admitted_batch(
            &request,
            decisions,
            Arc::clone(host),
            Some(extension_leases),
        )
        .await
        .map(Some)
    }

    /// Retains one owner-side cancellation declaration before requesting any
    /// accepted member. Repeating this call reconciles the same declaration;
    /// neither an acknowledgement nor this report asserts terminal outcomes.
    pub async fn cancel_batch_id<I, O>(
        &self,
        definition: &Arc<TaskDefinition<I, O>>,
        batch_id: BatchId,
    ) -> Result<Option<BatchCancellationReport>>
    where
        I: 'static,
        O: DeserializeOwned + Send + 'static,
    {
        let Some(admitted) = self.reconcile_batch_id(definition, batch_id).await? else {
            return Ok(None);
        };
        let spawner = self
            .harness
            .spawner
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable batch spawner is not bound".into()))?;
        let report = spawner.cancel_batch(batch_id).await?.ok_or_else(|| {
            Error::Storage("retained batch disappeared during cancellation".into())
        })?;
        if report.group_id != admitted.group_id
            || report.batch_id != admitted.batch_id
            || report.entries.len() != admitted.entries.len()
            || report
                .entries
                .iter()
                .enumerate()
                .any(|(index, (key, _))| *key != InputKey { batch_id, index })
        {
            return Err(Error::Storage(
                "batch owner returned another cancellation report".into(),
            ));
        }
        Ok(Some(report))
    }

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
        if self.durable_parent.is_some() {
            return Err(Error::Unsupported(
                "durable descendants require stable batch admission".into(),
            ));
        }
        require_task_spawn(&self.scope, definition)?;
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

    /// Admits independent live tasks in input order without waiting for their
    /// results. Every accepted handle remains observable if a later admission
    /// fails; unlike `map`, this does not impose a failure policy.
    pub async fn spawn_many<I, O>(
        &self,
        definition: &Arc<TaskDefinition<I, O>>,
        inputs: impl IntoIterator<Item = I>,
    ) -> Vec<Result<RuntimeTask<O>>>
    where
        I: Serialize + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        let mut admissions = Vec::new();
        for input in inputs {
            admissions.push(self.spawn(definition, input).await);
        }
        admissions
    }

    /// Admits all independent inputs before awaiting any result, then returns
    /// either ordered outputs or every input's exact outcome. This collect-all
    /// helper is local-only; durable batches require retained batch identities.
    pub async fn map<I, O>(
        &self,
        definition: &Arc<TaskDefinition<I, O>>,
        inputs: impl IntoIterator<Item = I>,
    ) -> RuntimeMap<O>
    where
        I: Serialize + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        let admissions = self.spawn_many(definition, inputs).await;
        let mut outcomes = Vec::with_capacity(admissions.len());
        let mut accepted = Vec::new();
        for admission in admissions {
            match admission {
                Ok(task) => {
                    outcomes.push(None);
                    accepted.push(task);
                }
                Err(error) => outcomes.push(Some(Err(error))),
            }
        }
        let mut completed = join_runtime(accepted).await.into_iter();
        let outcomes = outcomes
            .into_iter()
            .map(|entry| {
                entry.unwrap_or_else(|| {
                    completed.next().unwrap_or_else(|| {
                        Err(Error::Storage("accepted task result is missing".into()))
                    })
                })
            })
            .collect::<Vec<_>>();
        if outcomes
            .iter()
            .all(|entry| matches!(entry, Ok(Outcome::Succeeded(_))))
        {
            RuntimeMap::Complete(
                outcomes
                    .into_iter()
                    .map(|entry| match entry {
                        Ok(Outcome::Succeeded(value)) => value,
                        _ => unreachable!("all outcomes were successful"),
                    })
                    .collect(),
            )
        } else {
            RuntimeMap::Incomplete(outcomes)
        }
    }

    /// Observes admitted members in input order. Observation does not cancel
    /// other members or convert uncertainty into success.
    pub async fn join<O: DeserializeOwned + Send + 'static>(
        &self,
        tasks: Vec<RuntimeTask<O>>,
    ) -> Vec<Result<Outcome<O>>> {
        join_runtime(tasks).await
    }

    /// Folds every admitted outcome in input order, even when completion
    /// order differs or an observation is uncertain.
    pub async fn ordered_reduce<O, A, F>(
        &self,
        tasks: Vec<RuntimeTask<O>>,
        initial: A,
        reducer: F,
    ) -> A
    where
        O: DeserializeOwned + Send + 'static,
        F: FnMut(A, Result<Outcome<O>>) -> A,
    {
        ordered_reduce_runtime(tasks, initial, reducer).await
    }

    /// Streams admitted outcomes as they complete, retaining each task ID.
    /// Dropping this stream does not cancel unfinished members.
    pub fn as_completed<O: DeserializeOwned + Send + 'static>(
        &self,
        tasks: Vec<RuntimeTask<O>>,
    ) -> BoxStream<'static, (String, Result<Outcome<O>>)> {
        completion_stream_runtime(tasks)
    }

    /// Returns the first terminal outcome; group cancellation remains explicit.
    pub async fn race<O: DeserializeOwned + Send + 'static>(
        &self,
        tasks: Vec<RuntimeTask<O>>,
    ) -> Option<(String, Result<Outcome<O>>)> {
        race_runtime(tasks).await
    }

    /// Returns the first success, or every non-success once all members settle.
    pub async fn first_success<O: DeserializeOwned + Send + 'static>(
        &self,
        tasks: Vec<RuntimeTask<O>>,
    ) -> std::result::Result<O, Vec<(String, Result<Outcome<O>>)>> {
        first_success_runtime(tasks).await
    }

    /// Collects the first `required` successes, or reports all non-successes.
    pub async fn quorum<O: DeserializeOwned + Send + 'static>(
        &self,
        tasks: Vec<RuntimeTask<O>>,
        required: usize,
    ) -> std::result::Result<Vec<O>, QuorumFailure<O>> {
        quorum_runtime(tasks, required).await
    }

    /// Closes admission while already accepted members and descendants drain.
    pub fn close(&self) {
        self.group.close();
    }

    /// Requests cancellation only of this process's live group and descendants.
    /// Use `cancel_batch_id` for each durable batch retained by its owner.
    pub fn cancel_local(&self) {
        self.group.cancel();
    }
}

/// A task context bound to one exact tool call identity.
#[derive(Clone)]
pub struct ToolContext {
    task: TaskContext,
    operation_id: OperationId,
    call_id: String,
}

impl ToolContext {
    /// Creates a tool context with separate runtime and provider call identities.
    pub fn new(
        task: TaskContext,
        operation_id: OperationId,
        call_id: impl Into<String>,
    ) -> Result<Self> {
        let call_id = call_id.into();
        if call_id.trim().is_empty() {
            return Err(Error::Invalid("tool call ID is empty".into()));
        }
        Ok(Self {
            task,
            operation_id,
            call_id,
        })
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

    /// Returns the stable effect identity for execution and reconciliation.
    #[must_use]
    pub const fn operation_id(&self) -> OperationId {
        self.operation_id
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

    struct CompletedModel {
        requests: std::sync::Mutex<Vec<ModelRequest>>,
    }

    impl ModelProvider for CompletedModel {
        fn generate<'a>(
            &'a self,
            request: ModelRequest,
        ) -> futures::stream::BoxStream<'a, Result<ModelEvent>> {
            let Ok(mut requests) = self.requests.lock() else {
                return Box::pin(futures::stream::iter([Err(Error::Storage(
                    "test model lock poisoned".into(),
                ))]));
            };
            requests.push(request);
            Box::pin(futures::stream::iter([Ok(ModelEvent::Completed {
                metadata: Value::Null,
            })]))
        }

        fn reconcile<'a>(
            &'a self,
            _attempt: crate::model::ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            Box::pin(async { Ok(None) })
        }
    }

    #[tokio::test]
    async fn live_task_model_override_is_scoped_and_durable_direct_call_is_rejected() -> Result<()>
    {
        let root_provider = Arc::new(CompletedModel {
            requests: std::sync::Mutex::new(Vec::new()),
        });
        let child_provider = Arc::new(CompletedModel {
            requests: std::sync::Mutex::new(Vec::new()),
        });
        let scope = RuntimeScope::new(Capabilities::new(["model:generate"]), Limits::default())?;
        let harness = AgentHarness::new(
            TaskRegistry::default(),
            ToolRegistry::new(),
            scope.clone(),
            2,
            None,
        )?
        .bind_model(
            Model::new("root", "model", "1", Value::Null)?,
            root_provider.clone(),
        )?;
        let context = TaskContext {
            harness,
            task_id: OperationId::new(),
            durable_task: None,
            descendants: TaskGroup::new(2),
            scope,
            policy_overrides: Vec::new(),
            model_steps: Arc::new(AtomicUsize::new(0)),
        };
        let message = ModelMessage {
            role: crate::model::ModelRole::User,
            content: crate::model::ModelContent::Text("hello".into()),
        };
        context.model_events(vec![message.clone()], None).await?;
        let child = context.scoped_model(
            Capabilities::new(["model:generate"]),
            Limits::default(),
            Model::new("child", "model", "2", Value::Null)?,
            child_provider.clone(),
        )?;
        child.model_events(vec![message.clone()], None).await?;
        let bounded = child.scoped_run_limits(TaskRunLimits {
            concurrency: None,
            max_steps: Some(2),
            deadline_epoch_ms: None,
        })?;
        assert!(bounded.model_events(vec![message], None).await.is_err());
        assert_eq!(
            root_provider
                .requests
                .lock()
                .unwrap_or_else(|error| error.into_inner())[0]
                .model
                .provider,
            "root"
        );
        assert_eq!(
            child_provider
                .requests
                .lock()
                .unwrap_or_else(|error| error.into_inner())[0]
                .model
                .provider,
            "child"
        );
        let foreign = FileRef::new(
            VolumeRef::new(
                ProviderRef::new("other", "filesystem", "2")?,
                "private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(crate::AgentId::from_bytes([77; 16])),
            )?,
            "evidence/image.png",
            "generation-1",
            FileDescriptor::from_bytes(b"image", "image/png")?,
            "image.png",
        )?;
        assert!(matches!(
            context
                .model_events(
                    vec![ModelMessage {
                        role: crate::model::ModelRole::User,
                        content: ModelContent::Part(ModelContentPart::File {
                            file: foreign,
                            policy: crate::model::FileProjectionPolicy::Native,
                        }),
                    }],
                    None
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(
            root_provider
                .requests
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .len(),
            1
        );
        let mut durable = child;
        durable.durable_task = Some(TaskId::new());
        assert!(matches!(
            durable
                .model_events(
                    vec![ModelMessage {
                        role: crate::model::ModelRole::User,
                        content: crate::model::ModelContent::Text("hello".into()),
                    }],
                    None
                )
                .await,
            Err(Error::Unsupported(_))
        ));
        let unregistered = Arc::new(TaskDefinition::live(
            "test.local.child",
            "1",
            |_, value: u32| async move { Ok(value) },
        )?);
        assert!(matches!(
            durable.group().spawn(&unregistered, 1).await,
            Err(Error::Unsupported(_))
        ));
        Ok(())
    }

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
            None,
            Some(root_policy),
        )?;
        let context = TaskContext {
            harness,
            task_id: OperationId::new(),
            durable_task: None,
            descendants: TaskGroup::new(1),
            scope,
            policy_overrides: Vec::new(),
            model_steps: Arc::new(AtomicUsize::new(0)),
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
            operation_id: OperationId::new(),
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
        record: TaskAdmissionRecord,
        polls: Arc<AtomicUsize>,
    }

    impl DurableTaskHost for IdentityHost {
        fn reconcile_admission<'a>(
            &'a self,
            operation_id: OperationId,
        ) -> BoxFuture<'a, Result<Option<(TaskId, TaskAdmissionRecord)>>> {
            Box::pin(async move {
                Ok(Some((
                    TaskId::from_bytes(operation_id.into_bytes()),
                    self.record.clone(),
                )))
            })
        }
        fn observe_admission<'a>(
            &'a self,
            _task_id: TaskId,
        ) -> BoxFuture<'a, Result<TaskAdmissionRecord>> {
            Box::pin(async { Ok(self.record.clone()) })
        }
        fn admit<'a>(
            &'a self,
            _request: TaskAdmissionRecord,
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

    struct SplitState {
        task_id: TaskId,
        record: TaskAdmissionRecord,
    }

    impl TaskStateProvider for SplitState {
        fn policy_identity(&self) -> Option<ComponentIdentity> {
            None
        }
        fn observe_admission<'a>(
            &'a self,
            task_id: TaskId,
        ) -> BoxFuture<'a, Result<TaskAdmissionRecord>> {
            Box::pin(async move {
                if task_id != self.task_id {
                    return Err(Error::NotFound("task".into()));
                }
                Ok(self.record.clone())
            })
        }
        fn resume_scope<'a>(
            &'a self,
            task_id: TaskId,
            operation_id: OperationId,
        ) -> BoxFuture<'a, Result<RuntimeScope>> {
            Box::pin(async move {
                if task_id != self.task_id || operation_id != self.record.operation_id {
                    return Err(Error::NotFound("task".into()));
                }
                RuntimeScope::new(self.record.grants.clone(), self.record.limits)?
                    .with_run_limits(self.record.run_limits)
            })
        }
        fn outcome<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<Option<Outcome<Value>>>> {
            Box::pin(async move {
                if task_id != self.task_id {
                    return Err(Error::NotFound("task".into()));
                }
                Ok(Some(Outcome::Succeeded(serde_json::json!(7))))
            })
        }
        fn cancel<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                if task_id != self.task_id {
                    return Err(Error::NotFound("task".into()));
                }
                Ok(())
            })
        }
    }

    struct SplitSpawner {
        task_id: TaskId,
        record: TaskAdmissionRecord,
    }

    #[test]
    fn v2_task_admission_and_execution_placement_fixtures_are_canonical() -> Result<()> {
        let admission_fixture = include_str!("../fixtures/v2/task-admission.json").trim();
        let admission_value: Value = serde_json::from_str(admission_fixture)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let admission = TaskAdmissionRecord::from_canonical_value(admission_value)?;
        assert_eq!(
            crate::contract::canonical_json_bytes(&admission.canonical_value())?,
            admission_fixture.as_bytes()
        );
        let placement_fixture = include_str!("../fixtures/v2/execution-placement.json").trim();
        let placement: ExecutionPlacement = serde_json::from_str(placement_fixture)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        placement.validate()?;
        let placement_value =
            serde_json::to_value(&placement).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(
            crate::contract::canonical_json_bytes(&placement_value)?,
            crate::contract::canonical_json_bytes(
                &serde_json::from_str::<Value>(placement_fixture)
                    .map_err(|error| Error::Invalid(error.to_string()))?
            )?
        );
        Ok(())
    }

    #[test]
    fn task_children_validation_matches_typescript_boundary() -> Result<()> {
        let parent = TaskId::from_bytes([1; 16]);
        assert!(validate_children_request(TaskId::from_bytes([0; 16]), None, 1).is_err());
        assert!(validate_children_request(parent, None, 0).is_err());
        assert!(validate_children_request(parent, None, 1_025).is_err());
        assert!(validate_children_request(parent, Some("\u{7f}"), 1).is_err());
        assert!(validate_children_request(parent, Some(&"x".repeat(256)), 1).is_err());
        validate_children_request(parent, Some("résumé"), 1)?;

        let child = TaskChild {
            slot: "a".into(),
            task_id: TaskId::from_bytes([2; 16]),
        };
        let child_task_id = child.task_id;
        let mut page = TaskChildrenPage {
            revision: 3,
            entries: vec![child],
            next_after: Some("a".into()),
        };
        validate_children_page(&page, Some(3), None, 1)?;
        assert!(validate_children_page(&page, Some(4), None, 1).is_err());
        assert!(validate_children_page(&page, None, Some("a"), 1).is_err());
        assert!(validate_children_page(&page, None, None, 0).is_err());

        page.next_after = Some("b".into());
        assert!(validate_children_page(&page, None, None, 1).is_err());
        page.next_after = Some("a".into());
        page.entries[0].task_id = TaskId::from_bytes([0; 16]);
        assert!(validate_children_page(&page, None, None, 1).is_err());
        page.entries[0].task_id = child_task_id;
        page.entries[0].slot = "".into();
        assert!(validate_children_page(&page, None, None, 1).is_err());
        page = TaskChildrenPage {
            revision: 3,
            entries: vec![
                TaskChild {
                    slot: "a".into(),
                    task_id: child_task_id,
                },
                TaskChild {
                    slot: "a".into(),
                    task_id: TaskId::from_bytes([3; 16]),
                },
            ],
            next_after: None,
        };
        assert!(validate_children_page(&page, None, None, 2).is_err());
        page.entries[1].slot = "b".into();
        page.entries[1].task_id = child_task_id;
        assert!(validate_children_page(&page, None, None, 2).is_err());
        Ok(())
    }

    impl TaskSpawner for SplitSpawner {
        fn policy_identity(&self) -> Option<ComponentIdentity> {
            None
        }
        fn admit<'a>(
            &'a self,
            _request: TaskAdmissionRecord,
        ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
            Box::pin(async move { Ok(Admission::Accepted(self.task_id)) })
        }
        fn reconcile_admission<'a>(
            &'a self,
            _operation_id: OperationId,
        ) -> BoxFuture<'a, Result<Option<(TaskId, TaskAdmissionRecord)>>> {
            Box::pin(async move { Ok(Some((self.task_id, self.record.clone()))) })
        }
    }

    #[tokio::test]
    async fn independent_state_and_spawner_attest_the_full_request() -> Result<()> {
        let machine = Arc::new(TestMachine {
            identity: MachineIdentity {
                name: "test.split".into(),
                version: "1".into(),
                digest: [8; 32],
            },
            state_schema: serde_json::json!({"type": "object"}),
        });
        let mut tasks = TaskRegistry::default();
        tasks.register(TaskDefinition::<Value, u32>::resumable(
            machine.clone(),
            serde_json::json!({"type": "object"}),
            serde_json::json!({"type": "integer"}),
        )?)?;
        let definition = tasks.get::<Value, u32>("test.split")?;
        let scope = RuntimeScope::new(Capabilities::new([] as [String; 0]), Limits::default())?;
        let operation_id = OperationId::from_bytes([9; 16]);
        let task_id = TaskId::from_bytes([10; 16]);
        let input = serde_json::json!({"value": 3});
        let record = TaskAdmissionRecord {
            operation_id,
            task: definition.identity().clone(),
            machine: machine.identity().clone(),
            input: input.clone(),
            input_schema: definition.input_schema.clone(),
            output_schema: definition.output_schema.clone(),
            parent: None,
            grants: scope.grants().clone(),
            limits: scope.limits(),
            run_limits: scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        };
        assert!(TaskAdmissionRecord::from_canonical_value(record.canonical_value())? == record);
        let mut unknown = record.canonical_value();
        unknown
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("task admission object".into()))?
            .insert("legacy_content".into(), Value::Null);
        assert!(TaskAdmissionRecord::from_canonical_value(unknown).is_err());
        let harness = Bindings {
            tasks,
            tools: ToolRegistry::new(),
            resumable_tools: ResumableToolRegistry::default(),
            scope,
            concurrency: 1,
            durable_host: None,
            state: Some(Arc::new(SplitState {
                task_id,
                record: record.clone(),
            })),
            spawner: Some(Arc::new(SplitSpawner {
                task_id,
                record: record.clone(),
            })),
            execution: None,
            interactions: None,
            interaction_resolver: None,
            content: None,
            artifacts: None,
            policy: None,
            fork_preparer: None,
            workspaces: None,
        }
        .build()?;
        let admitted = harness
            .admit(operation_id, &definition, input, None)
            .await?;
        let Admission::Accepted(task) = admitted else {
            return Err(Error::Storage("split task was not admitted".into()));
        };
        assert_eq!(task.identity(), task_id.to_string());
        assert_eq!(task.result().await?, Outcome::Succeeded(7));
        assert_eq!(
            harness
                .reconcile_admission(operation_id)
                .await?
                .map(|value| value.0),
            Some(task_id)
        );
        assert!(matches!(
            harness
                .admit(
                    operation_id,
                    &definition,
                    serde_json::json!({"value": 4}),
                    None
                )
                .await,
            Err(Error::Conflict(_))
        ));
        let mut changed_request = record.clone();
        changed_request.input = serde_json::json!({"value": 4});
        let mismatched_spawner = harness.bind_spawner(Arc::new(SplitSpawner {
            task_id,
            record: changed_request,
        }))?;
        assert!(matches!(
            mismatched_spawner.reconcile_admission(operation_id).await,
            Err(Error::Conflict(_))
        ));
        let mut changed_schema = record;
        changed_schema.input_schema =
            serde_json::json!({"type": "object", "additionalProperties": true});
        assert!(matches!(
            harness.validate_observed_admission(&changed_schema),
            Err(Error::Conflict(_))
        ));
        Ok(())
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
        let operation_id = OperationId::from_bytes([5; 16]);
        let scope = RuntimeScope::new(Capabilities::new([] as [String; 0]), Limits::default())?;
        let record = TaskAdmissionRecord {
            operation_id,
            task: first.identity().clone(),
            machine: match &first.implementation {
                TaskImplementation::Resumable(machine) => machine.identity().clone(),
                TaskImplementation::Live(_) => unreachable!(),
            },
            input: serde_json::json!({}),
            input_schema: first.input_schema.clone(),
            output_schema: first.output_schema.clone(),
            parent: None,
            grants: scope.grants().clone(),
            limits: scope.limits(),
            run_limits: scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        };
        let polls = Arc::new(AtomicUsize::new(0));
        let harness = Bindings {
            tasks,
            tools: ToolRegistry::new(),
            resumable_tools: ResumableToolRegistry::default(),
            scope,
            concurrency: 1,
            durable_host: Some(Arc::new(IdentityHost {
                record,
                polls: Arc::clone(&polls),
            })),
            state: None,
            spawner: None,
            execution: None,
            interactions: None,
            interaction_resolver: None,
            content: None,
            artifacts: None,
            policy: None,
            fork_preparer: None,
            workspaces: None,
        }
        .build()?;
        let second = harness.task::<Value, u32>("test.second")?;
        let task_id = TaskId::from_bytes([5; 16]);
        let recovered = harness
            .reconcile_admission(OperationId::from_bytes([5; 16]))
            .await?
            .ok_or_else(|| Error::NotFound("reconciled task".into()))?;
        assert_eq!(recovered.0, task_id);
        assert_eq!(&recovered.1, first.identity());
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
    fn batch_members_have_stable_indexed_identities_and_pinned_authority() -> Result<()> {
        let task = ComponentIdentity {
            name: "test.batch".into(),
            version: "1".into(),
            digest: [3; 32],
        };
        let scope = RuntimeScope::new(
            Capabilities::new(["task:spawn:test.batch@1"]),
            Limits::default(),
        )?;
        require_descendant_grant(scope.grants(), &task)?;
        assert!(matches!(
            require_descendant_grant(&Capabilities::new([] as [String; 0]), &task),
            Err(Error::Unauthorized(_))
        ));
        let mut request = DurableBatchRequest {
            group_id: GroupId::from_bytes([1; 16]),
            batch_id: BatchId::from_bytes([2; 16]),
            group_policy: BatchGroupPolicy::CollectAll,
            task,
            machine: MachineIdentity {
                name: "test.batch".into(),
                version: "1".into(),
                digest: [4; 32],
            },
            inputs: vec![
                serde_json::json!({"value": 1}),
                serde_json::json!({"value": 2}),
            ],
            input_schema: serde_json::json!({"type": "object"}),
            output_schema: serde_json::json!({"type": "integer"}),
            parent: Some(TaskId::from_bytes([5; 16])),
            scope,
            policy: None,
            execution: None,
        };
        assert_ne!(request.operation_id(0), request.operation_id(1));
        let original = crate::contract::canonical_json_digest(&request.canonical_value())?;
        let restored = DurableBatchRequest::from_canonical_value(request.canonical_value())?;
        assert_eq!(restored.canonical_value(), request.canonical_value());
        let mut unknown = request.canonical_value();
        unknown
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("batch object".into()))?
            .insert("legacy_manifest".into(), Value::Null);
        assert!(DurableBatchRequest::from_canonical_value(unknown).is_err());
        let mut missing_policy = request.canonical_value();
        missing_policy
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("batch object".into()))?
            .remove("group_policy");
        assert!(DurableBatchRequest::from_canonical_value(missing_policy).is_err());
        let mut invalid_input = request.canonical_value();
        invalid_input
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("batch object".into()))?
            .insert("inputs".into(), serde_json::json!([1]));
        assert!(DurableBatchRequest::from_canonical_value(invalid_input).is_err());
        let mut invalid_schema = request.canonical_value();
        invalid_schema
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("batch object".into()))?
            .insert("input_schema".into(), serde_json::json!({}));
        assert!(DurableBatchRequest::from_canonical_value(invalid_schema).is_err());
        request.inputs.swap(0, 1);
        assert_ne!(
            original,
            crate::contract::canonical_json_digest(&request.canonical_value())?
        );
        request.inputs.swap(0, 1);
        request.scope = RuntimeScope::new(Capabilities::new([] as [String; 0]), Limits::default())?;
        assert_ne!(
            original,
            crate::contract::canonical_json_digest(&request.canonical_value())?
        );
        Ok(())
    }

    #[tokio::test]
    async fn batch_join_keeps_rejection_and_unknown_admission_in_original_slots() -> Result<()> {
        let group_id = GroupId::from_bytes([20; 16]);
        let batch_id = BatchId::from_bytes([21; 16]);
        let batch = AdmissionBatch::<u32> {
            group_id,
            batch_id,
            policy: BatchGroupPolicy::CollectAll,
            spawner: Arc::new(SplitSpawner {
                task_id: TaskId::from_bytes([23; 16]),
                record: TaskAdmissionRecord::from_canonical_value(
                    serde_json::from_str(include_str!("../fixtures/v2/task-admission.json"))
                        .map_err(|error| Error::Invalid(format!("fixture is invalid: {error}")))?,
                )?,
            }),
            extensions: None,
            entries: vec![
                BatchEntry {
                    key: InputKey { batch_id, index: 0 },
                    admission: Admission::Rejected {
                        reason: "capacity".into(),
                    },
                },
                BatchEntry {
                    key: InputKey { batch_id, index: 1 },
                    admission: Admission::Indeterminate {
                        operation_id: OperationId::from_bytes([22; 16]),
                    },
                },
            ],
        };
        let observed = batch.join().await;
        assert_eq!(observed.group_id, group_id);
        assert_eq!(observed.batch_id, batch_id);
        assert!(!observed.complete);
        assert!(matches!(
            observed.entries[0].admission,
            Admission::Rejected { .. }
        ));
        assert!(matches!(
            observed.entries[1].admission,
            Admission::Indeterminate { .. }
        ));
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
            resumable_tools: ResumableToolRegistry::default(),
            scope: RuntimeScope::new(Capabilities::new([] as [String; 0]), Limits::default())?,
            concurrency: 2,
            durable_host: None,
            state: None,
            spawner: None,
            execution: None,
            interactions: None,
            interaction_resolver: None,
            content: None,
            artifacts: None,
            policy: None,
            fork_preparer: None,
            workspaces: None,
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
            resumable_tools: ResumableToolRegistry::default(),
            scope: RuntimeScope::new(
                Capabilities::new(["task:spawn:test.recursive@1"]),
                Limits::default(),
            )?,
            concurrency: 1,
            durable_host: None,
            state: None,
            spawner: None,
            execution: None,
            interactions: None,
            interaction_resolver: None,
            content: None,
            artifacts: None,
            policy: None,
            fork_preparer: None,
            workspaces: None,
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
    async fn group_map_preserves_input_order_and_partial_failure() -> Result<()> {
        let mut tasks = TaskRegistry::default();
        tasks.register(TaskDefinition::live(
            "test.map.leaf",
            "1",
            |_, value: u32| async move {
                if value == 3 {
                    Err(Error::Conflict("leaf rejected three".into()))
                } else {
                    Ok(value * 2)
                }
            },
        )?)?;
        tasks.register(TaskDefinition::live(
            "test.map.parent",
            "1",
            |context, inputs: Vec<u32>| async move {
                let leaf = context.task::<u32, u32>("test.map.leaf")?;
                let group = context.group();
                match group.map(&leaf, inputs).await {
                    RuntimeMap::Complete(values) => Ok(values),
                    RuntimeMap::Incomplete(entries) => {
                        assert_eq!(entries.len(), 4);
                        assert_eq!(entries[0], Ok(Outcome::Succeeded(2)));
                        assert_eq!(entries[1], Ok(Outcome::Succeeded(4)));
                        assert!(matches!(&entries[2], Ok(Outcome::Failed { .. })));
                        assert_eq!(entries[3], Ok(Outcome::Succeeded(8)));
                        let members = group
                            .spawn_many(&leaf, [1, 3])
                            .await
                            .into_iter()
                            .collect::<Result<Vec<_>>>()?;
                        let partial =
                            group.quorum(members, 2).await.err().ok_or_else(|| {
                                Error::Conflict("impossible quorum succeeded".into())
                            })?;
                        assert_eq!(partial.required, 2);
                        assert_eq!(partial.successes.len(), 1);
                        assert_eq!(partial.successes[0].1, 2);
                        assert_eq!(partial.others.len(), 1);
                        Ok(Vec::new())
                    }
                }
            },
        )?)?;
        let harness = Bindings {
            tasks,
            tools: ToolRegistry::new(),
            resumable_tools: ResumableToolRegistry::default(),
            scope: RuntimeScope::new(
                Capabilities::new(["task:spawn:test.map.leaf@1"]),
                Limits::default(),
            )?,
            concurrency: 1,
            durable_host: None,
            state: None,
            spawner: None,
            execution: None,
            interactions: None,
            interaction_resolver: None,
            content: None,
            artifacts: None,
            policy: None,
            fork_preparer: None,
            workspaces: None,
        }
        .build()?;
        let parent = harness.task::<Vec<u32>, Vec<u32>>("test.map.parent")?;
        assert_eq!(
            harness
                .spawn(&parent, vec![1, 2, 3, 4])
                .await?
                .result()
                .await?,
            Outcome::Succeeded(Vec::new())
        );
        assert_eq!(
            harness
                .spawn(&parent, vec![4, 2, 1])
                .await?
                .result()
                .await?,
            Outcome::Succeeded(vec![8, 4, 2])
        );
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
            resumable_tools: ResumableToolRegistry::default(),
            scope: RuntimeScope::new(Capabilities::new([] as [String; 0]), Limits::default())?,
            concurrency: 2,
            durable_host: None,
            state: None,
            spawner: None,
            execution: None,
            interactions: None,
            interaction_resolver: None,
            content: None,
            artifacts: None,
            policy: None,
            fork_preparer: None,
            workspaces: None,
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
        let harness =
            AgentHarness::new(TaskRegistry::default(), ToolRegistry::new(), root, 1, None)?;
        let view = harness.scoped(Capabilities::new([] as [String; 0]), narrowed)?;
        assert!(harness.scope.grants().contains("tool:call:test.echo"));
        assert!(!view.scope.grants().contains("tool:call:test.echo"));
        assert!(
            view.scoped(Capabilities::new(["tool:call:test.echo"]), narrowed)
                .is_err()
        );
        let bounded = harness.scoped_run_limits(TaskRunLimits {
            concurrency: Some(1),
            max_steps: Some(3),
            deadline_epoch_ms: Some(1_900_000_000_000),
        })?;
        assert_eq!(bounded.concurrency, 1);
        assert_eq!(bounded.scope.run_limits().max_steps, Some(3));
        assert!(
            bounded
                .scoped_run_limits(TaskRunLimits {
                    concurrency: Some(2),
                    max_steps: Some(3),
                    deadline_epoch_ms: Some(1_900_000_000_000),
                })
                .is_err()
        );
        assert!(
            bounded
                .scoped_run_limits(TaskRunLimits {
                    concurrency: Some(1),
                    max_steps: None,
                    deadline_epoch_ms: Some(1_900_000_000_000),
                })
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
