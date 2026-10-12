//! Typed, version-pinned task admission over the live and durable primitives.

mod native_startup;
use native_startup::StartupIo;
use crate::contract::capability;
use crate::{
    Admission, BatchId, Capabilities, EffectId, Error, GroupId, InteractionId, OperationId,
    Outcome, Result, TaskId,
    context::ContextPipeline,
    conversation::{
        ContentPublisher, ContentResidencyVerifier, FileRef, Limits, MAX_PRIVATE_DIRECTORY_PAGE,
        PrivateDirectoryPage, VolumeOperation, VolumeRef, verified_content_bytes,
    },
    core::{ExtensionAdmission, Reducer, SchemaRegistry, Scope},
    durable_tool::ResumableToolRegistry,
    executor::ModelEventAdmission,
    extension::ExtensionRuntime,
    interaction::{
        Interaction, InteractionOutcome, InteractionResolution, InteractionResponse,
        InteractionTicket, ResolutionReceipt,
    },
    live::{TaskGroup, TaskHandle},
    model::{Model, ModelEvent, ModelMessage, ModelProvider, ModelRequest},
    registry::{ComponentIdentity, validate_component_label},
    resources::{ArtifactRef, GenerationRef, SandboxRef},
    scheduler::InboxItem,
    tool::{ToolDefinition, ToolInvocation, ToolRegistry, validate_value},
    workflow::{
        DurableWorkflowHost, MachineCheckpoint, MachineIdentity, MachineRegistry, MachineStatus,
        MachineTransition, ResumableMachine, WorkflowAdmission, WorkflowJournal,
    },
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use futures::{StreamExt as _, stream};
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

/// Default number of direct children requested by the SDK facade.
pub const DEFAULT_CHILD_PAGE: usize = 256;
/// Default number of entries requested by the private-directory SDK facade.
pub const DEFAULT_PRIVATE_DIRECTORY_PAGE: usize = 256;

#[cfg(not(target_arch = "wasm32"))]
type LiveHandler<I, O> = dyn Fn(TaskContext, I) -> BoxFuture<'static, Result<O>> + Send + Sync;
#[cfg(target_arch = "wasm32")]
type LiveHandler<I, O> = dyn Fn(TaskContext, I) -> BoxFuture<'static, Result<O>>;
#[cfg(not(target_arch = "wasm32"))]
type StoredDefinition = dyn Any + Send + Sync;
#[cfg(target_arch = "wasm32")]
type StoredDefinition = dyn Any;

enum TaskImplementation<I, O> {
    Live(Arc<LiveHandler<I, O>>),
    Resumable(Arc<dyn ResumableMachine>),
}

/// Immutable typed task definition.
pub struct TaskDefinition<I, O> {
    identity: ComponentIdentity,
    input_schema: Value,
    output_schema: Value,
    requirements: BTreeSet<String>,
    implementation: TaskImplementation<I, O>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_definition_source: Option<(acyclic_fs::NativeMaterializationWork, u64)>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_schema_custody: Option<acyclic_fs::NativeAllocation>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_metadata_custody: Option<acyclic_fs::NativeAllocation>,
}

impl<I, O> TaskDefinition<I, O> {
    /// Adapts an ordinary async function without claiming that its stack is durable.
    pub fn live<F, Fut>(
        name: impl Into<String>,
        version: impl Into<String>,
        handler: F,
    ) -> Result<Self>
    where
        F: Fn(TaskContext, I) -> Fut + acyclic_stream::ProviderPlatform + 'static,
        Fut: Future<Output = Result<O>> + acyclic_stream::ProviderTask + 'static,
    {
        let name = name.into();
        let version = version.into();
        validate_identity(&name, &version, None)?;
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
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_definition_source: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_schema_custody: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata_custody: None,
        };
        definition.refresh_digest(None)?;
        Ok(definition)
    }

    /// Binds a pinned state machine and its exact admission/result schemas.
    pub fn resumable(
        machine: Arc<dyn ResumableMachine>,
        input_schema: Value,
        output_schema: Value,
    ) -> Result<Self> {
        Ok(Self::resumable_with_io(machine, input_schema, output_schema, None)?.0)
    }

    /// Moves genuinely guarded schema values into the original definition.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn resumable_with_native_work(
        machine: Arc<dyn ResumableMachine>, input_schema: Value, output_schema: Value,
        schema_custody: acyclic_fs::NativeAllocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let mut schemas = (input_schema, output_schema, schema_custody);
        if !machine.supports_native_work() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "machine has no native definition callbacks",
            });
        }
        machine.identity_with_native_work(work)?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let result = Self::resumable_with_io(
            machine, std::mem::take(&mut schemas.0), std::mem::take(&mut schemas.1), Some(&io),
        );
        let retained = result.as_ref().map_or(0, |(_, retained)| *retained);
        let ((mut definition, _), custody) = crate::workflow::finish_native_io(result, io, retained)?;
        definition.native_definition_source = Some((work.clone(), custody.bytes()));
        definition.native_schema_custody = Some(schemas.2);
        Ok((definition, custody))
    }

    fn resumable_with_io(
        machine: Arc<dyn ResumableMachine>, input_schema: Value, output_schema: Value,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Self, u64)> {
        let pinned = task_machine_identity(machine.as_ref(), io)?;
        validate_identity(&pinned.name, &pinned.version, io)?;
        if pinned.digest == [0; 32] {
            return Err(task_workflow_error(task_original_work(io)?, crate::wire::ErrorCode::Invalid,
                "resumable task requires an implementation digest"));
        }
        crate::executor::with_history_io(io, |work| validate_task_schemas(&input_schema, &output_schema, true, work))?;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        }
        let mut definition = Self {
            identity: ComponentIdentity {
                name: task_copy_text(&pinned.name, io)?,
                version: task_copy_text(&pinned.version, io)?,
                digest: [0; 32],
            },
            input_schema, output_schema, requirements: BTreeSet::new(),
            implementation: TaskImplementation::Resumable(machine),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_definition_source: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_schema_custody: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata_custody: None,
        };
        let retained = crate::workflow::workflow_owned_bytes(io)?;
        definition.refresh_digest(io)?;
        Ok((definition, retained))
    }

    /// Pins both public JSON Schemas before registration.
    pub fn schemas(mut self, input: Value, output: Value) -> Result<Self> {
        validate_task_schemas(
            &input,
            &output,
            matches!(&self.implementation, TaskImplementation::Resumable(_)),
            &mut None,
        )?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        { self.native_definition_source = None; }
        self.input_schema = input;
        self.output_schema = output;
        self.refresh_digest(None)?;
        Ok(self)
    }

    /// Declares a dependency available at admission.
    pub fn requires(mut self, requirement: impl Into<String>) -> Result<Self> {
        let requirement = requirement.into();
        if requirement.is_empty() || requirement.chars().any(char::is_whitespace) {
            return Err(Error::Invalid("task requirement is invalid".into()));
        }
        self.requirements.insert(requirement);
        self.refresh_digest(None)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        { self.native_definition_source = None; }
        Ok(self)
    }

    fn refresh_digest(&mut self, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        let implementation = match &self.implementation {
            TaskImplementation::Live(_) => None,
            TaskImplementation::Resumable(machine) => Some(task_machine_identity(machine.as_ref(), io)?.digest),
        };
        self.identity.digest = task_definition_digest_with_io(
            &self.identity.name, &self.identity.version, &self.input_schema, &self.output_schema,
            &self.requirements, implementation, io,
        )?;
        Ok(())
    }

    /// Opens the registered task's retained workflow under its exact lease.
    pub async fn open(
        self: &Arc<Self>,
        context: &TaskContext,
        fence: crate::scheduler::LeaseFence,
        journal: Arc<dyn WorkflowJournal>,
    ) -> Result<ResumableTaskSession>
    where
        I: 'static,
        O: 'static,
    {
        Ok(self.open_with_io(context, fence, journal, None).await?.0)
    }

    /// Opens the same registered workflow using the original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn open_with_native_work(
        self: &Arc<Self>,
        context: Arc<NativeTaskContext>,
        fence: &crate::scheduler::LeaseFence,
        journal: Arc<dyn WorkflowJournal>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<ResumableTaskSession>
    where I: 'static, O: 'static,
    {
        self.require_native_identity(work)?;
        if !journal.supports_native_work() {
            return Err(task_workflow_error(Some(work), crate::wire::ErrorCode::Unsupported,
                "workflow journal has no native callbacks"));
        }
        let task = context.context().durable_task.ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "native workflow requires an original durable task context",
        })?;
        if !context.native_metadata_matches(work)
            || !context.context().original_task_binding_matches(task, fence)
            || journal.task_binding() != Some((task, fence))
        {
            return Err(task_workflow_error(Some(work), crate::wire::ErrorCode::Unauthorized,
                "task context or journal differs from its original native owner"));
        }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let result = async {
            let fence = task_copy_with_io(Some(&io), |quota| Ok(crate::scheduler::LeaseFence {
                reservation_id: crate::core::copy_text(&fence.reservation_id, quota)?,
                placement: crate::core::copy_text(&fence.placement, quota)?,
            }))?;
            let fence_retained = io.owned_bytes()?;
            let (session, retained) = self.open_with_io(
                context.context(), fence, journal, Some(&io),
            ).await?;
            Ok((session, fence_retained.checked_add(retained).ok_or(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout, custody: None,
            })?))
        }.await;
        let retained = result.as_ref().map_or(0, |(_, retained)| *retained);
        let ((mut session, _), custody) = crate::workflow::finish_native_io(result, io, retained)?;
        session.native_context = Some(context);
        session.native_metadata_custody = Some(custody);
        Ok(session)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn require_native_open(&self) -> Result<()> {
        let TaskImplementation::Resumable(machine) = &self.implementation else {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "live tasks have no resumable workflow",
            });
        };
        if self.native_definition_source.is_none() || self.native_schema_custody.is_none()
            || self.native_metadata_custody.is_none()
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "task definition was not published with native custody",
            });
        }
        if !machine.supports_native_work() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "machine has no native initialization or transition callback",
            });
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn require_native_identity(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        self.require_native_open()?;
        if let TaskImplementation::Resumable(machine) = &self.implementation {
            machine.identity_with_native_work(work)?;
        }
        Ok(())
    }

    async fn open_with_io(
        self: &Arc<Self>, context: &TaskContext,
        fence: crate::scheduler::LeaseFence, journal: Arc<dyn WorkflowJournal>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(ResumableTaskSession, u64)>
    where I: 'static, O: 'static,
    {
        let native = task_original_work(io)?;
        let registered = if io.is_some() {
            context.harness.tasks.0.iter().find(|((name, version), _)| {
                name == &self.identity.name && version == &self.identity.version
            }).filter(|(_, entry)| {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                { entry.native_custody.is_some() }
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                { let _ = entry; true }
            })
                .and_then(|(_, entry)| task_definition_arc::<I, O>(entry))
                .ok_or_else(|| task_workflow_error(native, crate::wire::ErrorCode::Conflict,
                    "task definition is not the registered version"))?
        } else {
            context.harness.tasks.get_version::<I, O>(&self.identity.name, &self.identity.version,
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] None)?
        };
        if !Arc::ptr_eq(&registered, self) {
            return Err(task_workflow_error(native, crate::wire::ErrorCode::Conflict,
                "task definition is not the registered version"));
        }
        let TaskImplementation::Resumable(machine) = &self.implementation else {
            return Err(task_workflow_error(native, crate::wire::ErrorCode::Unsupported,
                "live tasks have no resumable workflow"));
        };
        let task_id = context.durable_task.ok_or_else(|| {
            task_workflow_error(native, crate::wire::ErrorCode::Unauthorized,
                "task workflow requires a durable task context")
        })?;
        let host = context.harness.host.as_ref().ok_or_else(|| {
            task_workflow_error(native, crate::wire::ErrorCode::Unsupported,
                "task workflow ownership is not bound")
        })?;
        verify_task_execution_owner(host.as_ref(), task_id, &fence, io).await?;
        if journal.task_binding() != Some((task_id, &fence)) {
            return Err(task_workflow_error(native, crate::wire::ErrorCode::Unauthorized,
                "workflow journal is not bound to this task lease"));
        }
        let retained = observe_task_open_admission(host.as_ref(), task_id, io).await?;
        context.harness.validate_observed_admission(&retained.0, io).await?;
        if retained.0.task != self.identity
            || retained.0.machine != *task_machine_identity(machine.as_ref(), io)?
            || retained.0.input_schema != self.input_schema
            || retained.0.output_schema != self.output_schema
            || context.task_id != retained.0.operation_id
            || context.scope.grants() != &retained.0.grants
            || context.scope.limits() != retained.0.limits
            || context.scope.run_limits() != retained.0.run_limits
            || context.scope.extensions() != retained.0.extensions.as_ref()
        {
            return Err(task_workflow_error(native, crate::wire::ErrorCode::Conflict,
                "task workflow differs from its admitted definition or scope"));
        }
        let request_digest = crate::executor::preparation_digest(&retained.0, io)?;
        let initialized = initialize_task_machine(machine.as_ref(), &retained.0.input, io)?;
        let admission = WorkflowAdmission {
            operation_id: retained.0.operation_id, request_digest,
            initial: MachineCheckpoint {
                machine: retained.0.machine, revision: 0, state: initialized.0,
            },
        };
        let mut registry = MachineRegistry::default();
        let before_registry = crate::workflow::workflow_owned_bytes(io)?;
        crate::executor::with_history_io(io, |work| registry.register(machine.clone(), work))?;
        let registry_retained = crate::workflow::workflow_owned_bytes(io)?.checked_sub(before_registry)
            .ok_or_else(|| task_workflow_error(native, crate::wire::ErrorCode::Conflict,
                "workflow registry custody changed"))?;
        registry.validate_checkpoint(&admission.initial, io)?;
        match observe_workflow_open_admission(journal.as_ref(), io).await?.0 {
            Some(existing) if existing == admission => {}
            Some(_) => return Err(task_workflow_error(native, crate::wire::ErrorCode::Conflict,
                "task workflow admission changed")),
            None => {
                let copy = task_copy_with_io(io, |quota| Ok(WorkflowAdmission {
                    operation_id: admission.operation_id, request_digest: admission.request_digest,
                    initial: MachineCheckpoint {
                        machine: task_copy_machine(&admission.initial.machine, quota)?,
                        revision: admission.initial.revision,
                        state: crate::core::copy_value(&admission.initial.state, quota)?,
                    },
                }))?;
                if admit_workflow_open(journal.as_ref(), copy, io).await?.0 != admission {
                    return Err(task_workflow_error(native, crate::wire::ErrorCode::Conflict,
                        "task workflow admission changed"));
                }
            }
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let workflow = if let Some(work) = native {
            DurableWorkflowHost::open_with_native_work(registry, admission.initial, journal, work).await?
        } else {
            DurableWorkflowHost::open(registry, admission.initial, journal).await?
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let workflow = DurableWorkflowHost::open(registry, admission.initial, journal).await?;
        validate_latest_task_transition(&workflow, &self.output_schema, io).await?;
        verify_task_execution_owner(host.as_ref(), task_id, &fence, io).await?;
        let before_schema = crate::workflow::workflow_owned_bytes(io)?;
        let output_schema = task_copy_value(&self.output_schema, io)?;
        let schema_retained = crate::workflow::workflow_owned_bytes(io)?.checked_sub(before_schema)
            .ok_or_else(|| task_workflow_error(native, crate::wire::ErrorCode::Conflict,
                "workflow output schema custody changed"))?;
        let retained = registry_retained.checked_add(schema_retained).ok_or_else(|| {
            task_workflow_error(native, crate::wire::ErrorCode::Conflict,
                "workflow metadata custody overflow")
        })?;
        Ok((ResumableTaskSession {
            task_id, fence, host: host.clone(), workflow, output_schema,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_context: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata_custody: None,
        }, retained))
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

fn validate_task_transition(output_schema: &Value, transition: &MachineTransition) -> Result<()> {
    if let MachineStatus::Completed { value } = &transition.status {
        validate_value(output_schema, value, "task output", None)?;
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
enum RegisteredTaskOpening<'a> {
    Ordinary(crate::scheduler::LeaseFence),
    Native {
        fence: &'a crate::scheduler::LeaseFence,
        context: Arc<NativeTaskContext>,
        definition: Arc<dyn RegisteredTaskDefinition>,
    },
}

#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
enum OrdinaryRegisteredTaskOpening {
    Ordinary(crate::scheduler::LeaseFence),
}
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
type RegisteredTaskOpening<'a> = OrdinaryRegisteredTaskOpening;


fn task_original_work(io: Option<&crate::executor::PreparationIo>) -> Result<Option<&NativeWork>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io {
        return io.original_native_work().map(Some).ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native workflow requires original work",
        });
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    Ok(None)
}

fn task_machine_identity<'a>(
    machine: &'a dyn ResumableMachine, io: Option<&crate::executor::PreparationIo>,
) -> Result<&'a MachineIdentity> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        return machine.identity_with_native_work(work);
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    Ok(machine.identity())
}

fn inspect_task_metadata(io: Option<&crate::executor::PreparationIo>, items: u64) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        use crate::store::history::HistoryIo as _;
        let mut observer = io.history_io();
        observer.check_cancellation()?;
        crate::store::history::HistoryIo::codec_charge(&mut observer, crate::contract::JsonCodecCharge::Inspect { items })?;
    }
    #[cfg(not(feature = "filesystem"))]
    let _ = (io, items);
    Ok(())
}

fn task_workflow_error(
    work: Option<&NativeWork>, code: crate::wire::ErrorCode, reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::NativeBoundary { code, reason };
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = work;
    match code {
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::NotFound => Error::NotFound(reason.into()),
        crate::wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        _ => Error::Conflict(reason.into()),
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_task_open_value<T>(
    value: T, custody: acyclic_fs::NativeAllocation, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(T, Option<NativeCustody>)> {
    if !custody.matches_work(work) {
        drop(value);
        return Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            custody: Some(custody),
        });
    }
    Ok((value, Some(custody)))
}

async fn verify_task_execution_owner(
    host: &dyn DurableTaskHost, task: TaskId, fence: &crate::scheduler::LeaseFence,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        return host.verify_execution_owner_with_native_work(task, fence, work)?.await;
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    host.verify_execution_owner(task, fence.clone()).await
}

async fn observe_task_open_admission(
    host: &dyn DurableTaskHost, task: TaskId, io: Option<&crate::executor::PreparationIo>,
) -> Result<(TaskAdmissionRecord, Option<NativeCustody>)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        let (value, custody) = host.observe_admission_with_native_work(task, work)?.await?;
        return native_task_open_value(value, custody, work);
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    Ok((host.observe_admission(task).await?, None))
}

fn initialize_task_machine(
    machine: &dyn ResumableMachine, input: &Value,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(Value, Option<NativeCustody>)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        let (value, custody) = machine.initialize_with_native_work(input, work)?;
        return native_task_open_value(value, custody, work);
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    Ok((machine.initialize(input)?, None))
}

async fn observe_workflow_open_admission(
    journal: &dyn WorkflowJournal, io: Option<&crate::executor::PreparationIo>,
) -> Result<(Option<WorkflowAdmission>, Option<NativeCustody>)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        let (value, custody) = journal.admission_with_native_work(work)?.await?;
        return native_task_open_value(value, custody, work);
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    Ok((journal.admission().await?, None))
}

async fn admit_workflow_open(
    journal: &dyn WorkflowJournal, admission: WorkflowAdmission,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(WorkflowAdmission, Option<NativeCustody>)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        let (value, custody) = journal.admit_with_native_work(admission, work)?.await?;
        return native_task_open_value(value, custody, work);
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    Ok((journal.admit(admission).await?, None))
}

async fn validate_latest_task_transition(
    workflow: &DurableWorkflowHost, schema: &Value,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = task_original_work(io)? {
        let (transition, custody) = workflow.latest_transition_with_native_work(work).await?;
        let latest = native_task_open_value(transition, custody, work)?;
        if let Some(transition) = &latest.0 {
            if let MachineStatus::Completed { value } = &transition.status {
                crate::contract::validate_json_schema_value(schema, value, "task output", io)?;
            }
        }
        return Ok(());
    }
    if let Some(transition) = workflow.latest_transition().await? {
        validate_task_transition(schema, &transition)?;
    }
    Ok(())
}

/// A registered task's current checkpoint and bounded workflow replay index.
pub struct ResumableTaskSession {
    task_id: TaskId,
    fence: crate::scheduler::LeaseFence,
    host: Arc<dyn DurableTaskHost>,
    workflow: DurableWorkflowHost,
    output_schema: Value,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_context: Option<Arc<NativeTaskContext>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_metadata_custody: Option<acyclic_fs::NativeAllocation>,
}

impl ResumableTaskSession {
    /// Returns the owner-retained task identity.
    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Returns the latest committed task checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &MachineCheckpoint {
        self.workflow.checkpoint()
    }

    /// Reads the latest committed outbox/status for recovery without advancing.
    pub async fn latest_transition(&self) -> Result<Option<MachineTransition>> {
        self.workflow.latest_transition().await
    }

    /// Returns the actual latest transition and its original retained guard.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn latest_transition_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<MachineTransition>, acyclic_fs::NativeAllocation)> {
        self.require_native_session(work)?;
        self.workflow.latest_transition_with_native_work(work).await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn require_native_session(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        self.workflow.require_native_work()?;
        let context = self.native_context.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "task session was not opened with native custody",
        })?;
        if !context.native_metadata_matches(work)
            || self.native_metadata_custody.as_ref().is_none_or(|held| {
                !held.matches_work(work)
            })
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "task session belongs to another native ledger",
            });
        }
        Ok(())
    }

    /// Commits one deterministic task transition and its outbox.
    pub async fn step(
        &mut self,
        operation_id: OperationId,
        idempotency_key: crate::IdempotencyKey,
        input: Value,
    ) -> Result<MachineTransition> {
        Ok(self.step_with_io(operation_id, idempotency_key, input, None).await?.0)
    }

    /// Consumes original response custody and retains it through state publication.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn step_with_native_work(
        &mut self,
        operation_id: OperationId,
        idempotency_key: crate::IdempotencyKey,
        input: Value,
        custody: acyclic_fs::NativeAllocation,
    ) -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        let mut input = (input, custody);
        self.require_native_session(input.1.work())?;
        let io = crate::filesystem::PreparationIo::from_native_work(input.1.work())?;
        let result = self.step_with_io(
            operation_id, idempotency_key, std::mem::take(&mut input.0), Some(&io),
        ).await;
        match result {
            Ok((transition, custody)) => {
                if let Some(mut guard) = custody {
                    input.1.absorb(&mut guard).map_err(|cause| Error::NativeWork {
                        cause, custody: Some(guard),
                    })?;
                }
                Ok((transition, input.1))
            }
            Err(Error::NativeWork { cause, custody: incoming }) => {
                if let Some(mut guard) = incoming {
                    if !input.1.work().shares_ledger(guard.work())
                        || input.1.absorb(&mut guard).is_err()
                    {
                        return Err(Error::NativeWork { cause, custody: Some(guard) });
                    }
                }
                Err(Error::NativeWork { cause, custody: Some(input.1) })
            }
            Err(error) => Err(error),
        }
    }

    async fn step_with_io(
        &mut self,
        operation_id: OperationId,
        idempotency_key: crate::IdempotencyKey,
        input: Value,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(MachineTransition, Option<NativeCustody>)> {
        self.verify_step_owner(io).await?;
        let output_schema = &self.output_schema;
        let check = |transition: &MachineTransition| {
            if io.is_some() {
                if let MachineStatus::Completed { value } = &transition.status {
                    crate::contract::validate_json_schema_value(
                        output_schema, value, "task output", io,
                    )?;
                }
                Ok(())
            } else {
                validate_task_transition(output_schema, transition)
            }
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let output = if let Some(io) = io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native task state requires original work",
            })?;
            let (transition, custody) = self.workflow.step_checked_with_native_work(
                operation_id, idempotency_key, input, work, check,
            ).await?;
            (transition, Some(custody))
        } else {
            (self.workflow.step(operation_id, idempotency_key, input, check).await?, None)
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let output = (self.workflow.step(operation_id, idempotency_key, input, check).await?, None);
        self.verify_step_owner(io).await?;
        Ok(output)
    }

    async fn verify_step_owner(&self, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native task ownership requires original work",
            })?;
            return self.host.verify_dispatch_owner_with_native_work(
                self.task_id, &self.fence, work,
            )?.await;
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = io;
        self.host.verify_dispatch_owner(self.task_id, self.fence.clone()).await
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
    task_definition_digest_with_io(name, version, input_schema, output_schema, requirements, machine_digest, None)
}

#[derive(Serialize)]
struct TaskDefinitionDigest<'a> {
    contract: &'static str,
    name: &'a str,
    version: &'a str,
    input_schema: &'a Value,
    output_schema: &'a Value,
    requirements: &'a BTreeSet<String>,
    machine_digest: Option<[u8; 32]>,
}

fn task_definition_digest_with_io(
    name: &str, version: &str, input_schema: &Value, output_schema: &Value,
    requirements: &BTreeSet<String>, machine_digest: Option<[u8; 32]>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<[u8; 32]> {
    validate_identity(name, version, io)?;
    crate::executor::with_history_io(io, |work| validate_task_schemas(input_schema, output_schema, machine_digest.is_some(), work))?;
    let mut invalid = machine_digest == Some([0; 32]);
    if !invalid {
        for value in requirements {
            inspect_task_metadata(io, 1)?;
            if value.is_empty() || value.chars().any(char::is_whitespace) {
                invalid = true;
                break;
            }
        }
    }
    if invalid {
        return Err(task_workflow_error(task_original_work(io)?, crate::wire::ErrorCode::Invalid,
            "task dependency or machine digest is invalid"));
    }
    crate::executor::preparation_digest(&TaskDefinitionDigest {
        contract: "harness.task.v2", name, version, input_schema, output_schema,
        requirements, machine_digest,
    }, io)
}

pub(crate) fn scope_metadata_boundary(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    code: crate::wire::ErrorCode, reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}

/// Builds the two identities that durable hosts retain for one resumable task.
pub fn task_admission_identities(
    name: &str,
    version: &str,
    input_schema: &Value,
    output_schema: &Value,
    requirements: &BTreeSet<String>,
    machine_digest: &[u8],
) -> Result<(ComponentIdentity, MachineIdentity)> {
    let machine_digest: [u8; 32] = machine_digest
        .try_into()
        .map_err(|_| Error::Invalid("machine digest must contain exactly 32 bytes".into()))?;
    let task_digest = task_definition_digest(
        name,
        version,
        input_schema,
        output_schema,
        requirements,
        Some(machine_digest),
    )?;
    Ok((
        ComponentIdentity {
            name: name.to_owned(),
            version: version.to_owned(),
            digest: task_digest,
        },
        MachineIdentity {
            name: name.to_owned(),
            version: version.to_owned(),
            digest: machine_digest,
        },
    ))
}

pub(crate) fn validate_task_schemas(
    input: &Value, output: &Value, resumable: bool,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    for (label, schema, refusal) in [
        ("task input", input, "resumable task input schema cannot accept every value"),
        ("task output", output, "resumable task output schema cannot accept every value"),
    ] {
        crate::contract::compile_json_schema_with_work(schema, label, work)?;
        if resumable
            && (schema == &Value::Bool(true)
                || schema.as_object().is_some_and(serde_json::Map::is_empty))
        {
            return Err(crate::store::guarded_boundary(work, crate::wire::ErrorCode::Invalid, refusal));
        }
    }
    Ok(())
}
fn validate_identity(
    name: &str, version: &str, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.is_some() {
        inspect_task_metadata(io, 2)?;
        if !crate::contract::is_valid_component_label(name)
            || !crate::contract::is_valid_component_label(version)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "task name or version is invalid",
            });
        }
    } else {
        validate_component_label(name, "task name")?;
        validate_component_label(version, "task version")?;
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        validate_component_label(name, "task name")?;
        validate_component_label(version, "task version")?;
    }
    if name.contains('@') || version.contains('@') {
        return Err(task_workflow_error(task_original_work(io)?, crate::wire::ErrorCode::Invalid,
            "task name and version cannot contain the version separator"));
    }
    Ok(())
}

/// Runs closed physical field producers against the original preparation ledger.
fn task_copy_with_io<T>(
    io: Option<&crate::executor::PreparationIo>,
    copy: impl FnOnce(&mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<T>,
) -> Result<T> {
    #[cfg(feature = "filesystem")]
    let mut observer = io.map(|io| io.history_io());
    #[cfg(feature = "filesystem")]
    let mut work = observer.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
    #[cfg(not(feature = "filesystem"))]
    let mut work = { let _ = io; None };
    let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
    let value = copy(&mut quota)?;
    quota.transfer();
    Ok(value)
}

fn task_copy_text(source: &str, io: Option<&crate::executor::PreparationIo>) -> Result<String> {
    task_copy_with_io(io, |quota| crate::core::copy_text(source, quota))
}

fn task_copy_value(source: &Value, io: Option<&crate::executor::PreparationIo>) -> Result<Value> {
    task_copy_with_io(io, |quota| crate::core::copy_value(source, quota))
}

fn task_copy_component(source: &ComponentIdentity, quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<ComponentIdentity> {
    Ok(ComponentIdentity {
        name: crate::core::copy_text(&source.name, quota)?,
        version: crate::core::copy_text(&source.version, quota)?, digest: source.digest,
    })
}

fn task_copy_machine(source: &MachineIdentity, quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<MachineIdentity> {
    Ok(MachineIdentity {
        name: crate::core::copy_text(&source.name, quota)?,
        version: crate::core::copy_text(&source.version, quota)?, digest: source.digest,
    })
}

fn task_copy_requirements(source: &BTreeSet<String>, quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<BTreeSet<String>> {
    let mut output = BTreeSet::new();
    for requirement in source {
        if let Some(io) = quota.work().as_deref_mut() {
            io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            io.retain_owned(std::mem::size_of::<String>() as u64)?;
            quota.adopt(std::mem::size_of::<String>() as u64)?;
        }
        output.insert(crate::core::copy_text(requirement, quota)?);
    }
    Ok(output)
}

fn task_copy_key(source: &(String, String), io: Option<&crate::executor::PreparationIo>) -> Result<(String, String)> {
    task_copy_with_io(io, |quota| Ok((
        crate::core::copy_text(&source.0, quota)?, crate::core::copy_text(&source.1, quota)?,
    )))
}

fn task_registered_portable_admission(
    operation: OperationId, input: Value, parent: Option<TaskId>, scope: &RuntimeScope,
    entry: &TaskEntry, machine: &MachineIdentity,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<TaskAdmissionRecord> {
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    if let Some(io) = quota.work().as_deref_mut() {
        let bytes = (std::mem::size_of::<TaskAdmissionRecord>() - std::mem::size_of::<Value>()) as u64;
        io.retain_owned(bytes)?;
        quota.adopt(bytes)?;
    }
    let task = task_copy_component(&entry.identity, &mut quota)?;
    let machine = task_copy_machine(machine, &mut quota)?;
    let input_schema = crate::core::copy_value(&entry.input_schema, &mut quota)?;
    let output_schema = crate::core::copy_value(&entry.output_schema, &mut quota)?;
    let grants = crate::core::copy_capabilities(scope.grants(), &mut quota)?;
    let resource_proofs = RuntimeScope::copy_resource_proofs(scope.resource_proofs(), &mut quota)?;
    let extensions = scope.extensions().map(|extension| extension.copy_with_quota(&mut quota)).transpose()?;
    let record = TaskAdmissionRecord {
        operation_id: operation, task, machine, input, input_schema, output_schema,
        parent, resource_proofs, grants, limits: scope.limits(), run_limits: scope.run_limits(),
        policy: None, extensions, execution: None,
    };
    record.validate(quota.work())?;
    quota.transfer();
    Ok(record)
}

struct TaskEntry {
    identity: ComponentIdentity,
    machine: Option<MachineIdentity>,
    input_schema: Value,
    output_schema: Value,
    requirements: BTreeSet<String>,
    definition: Arc<StoredDefinition>,
    workflow_definition: Option<Arc<dyn RegisteredTaskDefinition>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl TaskEntry {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn require_native_definition(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        let definition = self.workflow_definition.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "live tasks have no native workflow definition",
        })?;
        definition.require_native_identity(work)
    }
}

impl Clone for TaskEntry {
    fn clone(&self) -> Self {
        self.clone_with_io(None).expect("unobserved task entry cloning is infallible")
    }
}

impl TaskEntry {
    fn clone_with_io(&self, io: Option<&crate::executor::PreparationIo>) -> Result<Self> {
        Ok(Self {
            identity: task_copy_with_io(io, |quota| task_copy_component(&self.identity, quota))?,
            machine: task_copy_with_io(io, |quota| self.machine.as_ref().map(|value| task_copy_machine(value, quota)).transpose())?,
            input_schema: task_copy_value(&self.input_schema, io)?,
            output_schema: task_copy_value(&self.output_schema, io)?,
            requirements: task_copy_with_io(io, |quota| task_copy_requirements(&self.requirements, quota))?,
            definition: self.definition.clone(), workflow_definition: self.workflow_definition.clone(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        })
    }
}

fn task_definition_arc<I: 'static, O: 'static>(entry: &TaskEntry) -> Option<Arc<TaskDefinition<I, O>>> {
    #[cfg(not(target_arch = "wasm32"))]
    { entry.definition.clone().downcast::<TaskDefinition<I, O>>().ok() }
    #[cfg(target_arch = "wasm32")]
    { entry.definition.downcast_ref::<Arc<TaskDefinition<I, O>>>().cloned() }
}

trait RegisteredTaskDefinition: acyclic_stream::ProviderPlatform {
    fn open_workflow(
        self: Arc<Self>,
        context: TaskContext,
        fence: crate::scheduler::LeaseFence,
        journal: Arc<dyn WorkflowJournal>,
    ) -> BoxFuture<'static, Result<ResumableTaskSession>>;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn require_native_identity(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()>;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn open_workflow_with_native_work<'a>(
        self: Arc<Self>, context: Arc<NativeTaskContext>,
        fence: &'a crate::scheduler::LeaseFence, journal: Arc<dyn WorkflowJournal>,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ResumableTaskSession>>;
}

impl<I: 'static, O: 'static> RegisteredTaskDefinition for TaskDefinition<I, O>
where
    TaskDefinition<I, O>: acyclic_stream::ProviderPlatform,
{
    fn open_workflow(
        self: Arc<Self>,
        context: TaskContext,
        fence: crate::scheduler::LeaseFence,
        journal: Arc<dyn WorkflowJournal>,
    ) -> BoxFuture<'static, Result<ResumableTaskSession>> {
        Box::pin(async move { self.open(&context, fence, journal).await })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn require_native_identity(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        TaskDefinition::require_native_identity(self, work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn open_workflow_with_native_work<'a>(
        self: Arc<Self>, context: Arc<NativeTaskContext>,
        fence: &'a crate::scheduler::LeaseFence, journal: Arc<dyn WorkflowJournal>,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ResumableTaskSession>> {
        TaskDefinition::require_native_identity(self.as_ref(), work)?;
        if !journal.supports_native_work() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "workflow journal has no native callbacks",
            });
        }
        crate::NativeFutureBundle::new(work, || async move {
            self.open_with_native_work(context, fence, journal, work).await
        })
    }
}

/// Immutable typed definitions indexed by their exact name and version.
#[derive(Default)]
pub struct TaskRegistry(
    BTreeMap<(String, String), TaskEntry>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Option<acyclic_fs::NativeAllocation>,
);

impl Clone for TaskRegistry {
    fn clone(&self) -> Self {
        self.clone_with_io(None).expect("unobserved task registry cloning is infallible")
    }
}

impl TaskRegistry {
    /// Registers a single exact task version and rejects ambiguous aliases.
    pub fn register<I: 'static, O: 'static>(
        &mut self,
        definition: TaskDefinition<I, O>,
    ) -> Result<()>
    where
        TaskDefinition<I, O>: acyclic_stream::ProviderPlatform,
    {
        let (key, entry) = self.prepare_registration(definition, None)?;
        self.0.insert(key, entry);
        Ok(())
    }

    /// Publishes the genuine SDK definition and retains all escaping custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn register_with_native_work<I: 'static, O: 'static>(
        &mut self, definition: TaskDefinition<I, O>, custody: acyclic_fs::NativeAllocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()>
    where TaskDefinition<I, O>: acyclic_stream::ProviderPlatform,
    {
        let mut input = (definition, custody);
        let source = input.0.native_definition_source.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "task definition has no original native constructor receipt",
        })?;
        if input.0.native_schema_custody.is_none()
            || !source.0.shares_ledger(work)
            || !input.1.work().shares_ledger(work)
            || input.1.bytes() < source.1
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "task definition metadata differs from its original constructor",
            });
        }
        input.0.native_metadata_custody = Some(input.1);
        input.0.require_native_open()?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let self_slot = if self.1.is_none() {
            let slot_io = crate::filesystem::PreparationIo::from_native_work(work)?;
            slot_io.retain_owned(std::mem::size_of::<Self>() as u64)?;
            Some(slot_io.into_native_custody(std::mem::size_of::<Self>() as u64)?)
        } else { None };
        let result = self.prepare_registration(input.0, Some(&io));
        let retained = io.owned_bytes()?;
        let ((key, mut entry), custody) = crate::workflow::finish_native_io(result, io, retained)?;
        entry.native_custody = Some(custody);
        if self.1.is_none() { self.1 = self_slot; }
        self.0.insert(key, entry);
        Ok(())
    }

    fn prepare_registration<I: 'static, O: 'static>(
        &self, definition: TaskDefinition<I, O>, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<((String, String), TaskEntry)>
    where TaskDefinition<I, O>: acyclic_stream::ProviderPlatform,
    {
        let native = task_original_work(io)?;
        let observed_machine = if io.is_some() {
            match &definition.implementation {
                TaskImplementation::Live(_) => return Err(task_workflow_error(native,
                    crate::wire::ErrorCode::Unsupported, "live tasks have no native workflow")),
                TaskImplementation::Resumable(machine) => Some(task_copy_with_io(io, |quota|
                    task_copy_machine(task_machine_identity(machine.as_ref(), io)?, quota))?),
            }
        } else { None };
        let name = task_copy_text(&definition.identity.name, io)?;
        let version = task_copy_text(&definition.identity.version, io)?;
        let duplicate = if io.is_some() {
            let mut found = false;
            for ((existing_name, existing_version), _) in &self.0 {
                inspect_task_metadata(io, 1)?;
                if existing_name == &name && existing_version == &version { found = true; break; }
            }
            found
        } else { self.0.contains_key(&(name.clone(), version.clone())) };
        if duplicate {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if io.is_some() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict, reason: "task version is already registered",
                });
            }
            return Err(Error::Conflict(format!("task {name}@{version} is already registered")));
        }
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.retain_owned((std::mem::size_of::<TaskEntry>()
                + std::mem::size_of::<(String, String)>()) as u64)?;
        }
        let copied = if io.is_some() {
            Some((
                task_copy_with_io(io, |quota| task_copy_component(&definition.identity, quota))?,
                observed_machine,
                task_copy_value(&definition.input_schema, io)?,
                task_copy_value(&definition.output_schema, io)?,
                task_copy_with_io(io, |quota| task_copy_requirements(&definition.requirements, quota))?,
            ))
        } else { None };
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.allocation_attempt(std::mem::size_of::<TaskDefinition<I, O>>() as u64)?;
        }
        let definition = Arc::new(definition);
        let (identity, machine, input_schema, output_schema, requirements) = if let Some(copied) = copied {
            copied
        } else {
            let identity = definition.identity.clone();
            let machine = match &definition.implementation {
                TaskImplementation::Live(_) => None,
                TaskImplementation::Resumable(machine) => Some(machine.identity().clone()),
            };
            (identity, machine, definition.input_schema.clone(), definition.output_schema.clone(),
                definition.requirements.clone())
        };
        let workflow_definition: Option<Arc<dyn RegisteredTaskDefinition>> = match &definition.implementation {
            TaskImplementation::Live(_) => None,
            TaskImplementation::Resumable(_) => Some(definition.clone()),
        };
        #[cfg(not(target_arch = "wasm32"))]
        let stored_definition: Arc<StoredDefinition> = definition;
        #[cfg(target_arch = "wasm32")]
        let stored_definition: Arc<StoredDefinition> = Arc::new(definition);
        Ok(((name, version), TaskEntry {
            identity, machine, input_schema, output_schema,
            requirements, workflow_definition, definition: stored_definition,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        }))
    }

    /// Resolves one Rust input/output pair.
    pub fn get<I: 'static, O: 'static>(&self, name: &str) -> Result<Arc<TaskDefinition<I, O>>> {
        if let Some((logical, version)) = name.rsplit_once('@') {
            return self.get_version(logical, version, #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] None);
        }
        let mut matches = self.0.iter().filter(|((logical, _), _)| logical == name);
        let (_, stored) = matches.next()
            .ok_or_else(|| Error::NotFound(format!("task {name}")))?;
        if matches.next().is_some() {
            return Err(Error::Conflict(format!("task {name} requires an exact version")));
        }
        task_definition_arc::<I, O>(stored)
            .ok_or_else(|| Error::Conflict(format!("task {name} has different input/output types")))
    }

    /// Resolves an exact pinned version without relying on name parsing.
    pub fn get_version<I: 'static, O: 'static>(
        &self, name: &str, version: &str,
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        work: Option<&acyclic_fs::NativeMaterializationWork>,
    ) -> Result<Arc<TaskDefinition<I, O>>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            let mut selected = None;
            for ((logical, pinned_version), entry) in &self.0 {
                inspect_task_metadata(Some(&io), 1)?;
                if logical == name && pinned_version == version { selected = Some(entry); break; }
            }
            let stored = selected.ok_or_else(|| task_workflow_error(Some(work),
                crate::wire::ErrorCode::NotFound, "native task version is unavailable"))?;
            if stored.native_custody.is_none() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                    reason: "registry entry was not copied with native custody" });
            }
            stored.require_native_definition(work)?;
            return task_definition_arc::<I, O>(stored).ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "task version has different input/output types",
            });
        }
        let stored = self.0.get(&(name.to_owned(), version.to_owned()))
            .ok_or_else(|| Error::NotFound(format!("task {name}@{version}")))?;
        task_definition_arc::<I, O>(stored)
            .ok_or_else(|| Error::Conflict(format!("task {name}@{version} has different input/output types")))
    }

    /// Copies actual registry metadata under the original work before growth.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_with_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Self> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let result = self.clone_with_io(Some(&io));
        let (mut registry, custody) = crate::workflow::finish_native_io(
            result, io, std::mem::size_of::<Self>() as u64,
        )?;
        registry.1 = Some(custody);
        Ok(registry)
    }

    fn clone_with_io(&self, io: Option<&crate::executor::PreparationIo>) -> Result<Self> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io { io.retain_owned(std::mem::size_of::<Self>() as u64)?; }
        let mut registry = Self::default();
        for (key, entry) in &self.0 {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(work) = task_original_work(io)? {
                entry.require_native_definition(work)?;
                let entry_io = crate::filesystem::PreparationIo::from_native_work(work)?;
                let result = (|| {
                    entry_io.retain_owned((std::mem::size_of::<TaskEntry>()
                        + std::mem::size_of::<(String, String)>()) as u64)?;
                    let key = task_copy_key(key, Some(&entry_io))?;
                    Ok((key, entry.clone_with_io(Some(&entry_io))?))
                })();
                let retained = entry_io.owned_bytes()?;
                let ((key, mut copied), custody) = crate::workflow::finish_native_io(
                    result, entry_io, retained,
                )?;
                copied.native_custody = Some(custody);
                registry.0.insert(key, copied);
                continue;
            }
            registry.0.insert(task_copy_key(key, io)?, entry.clone_with_io(io)?);
        }
        Ok(registry)
    }

    /// Uses the same registered metadata for an erased stock child command.
    fn registered_admission(
        &self, operation: OperationId, name: &str, version: &str, input: Value,
        parent: Option<TaskId>, expected_digest: Option<[u8; 32]>, scope: &RuntimeScope,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<TaskAdmissionRecord> {
        let entry = if work.is_none() {
            self.0.get(&(name.to_owned(), version.to_owned()))
        } else {
            let mut found = None;
            for ((registered_name, registered_version), entry) in &self.0 {
                work.as_deref_mut().expect("observed registry lookup")
                    .codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
                if registered_name == name && registered_version == version {
                    found = Some(entry);
                    break;
                }
            }
            found
        }.ok_or_else(|| {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) { return Error::NativeBoundary {
                code: crate::wire::ErrorCode::NotFound, reason: "registered child task version is absent",
            }; }
            Error::NotFound(format!("task {name}@{version}"))
        })?;
        let machine = entry.machine.as_ref().ok_or_else(|| {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) { return Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported, reason: "live task cannot be a durable child command",
            }; }
            Error::Unsupported("live task cannot be a durable command".into())
        })?;
        if expected_digest.is_some_and(|digest| digest != entry.identity.digest) {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) { return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "root definition differs from the registered task",
            }); }
            return Err(Error::Conflict("root definition differs from the registered task".into()));
        }
        let mut descendant = parent.is_none();
        for grant in scope.grants().iter() {
            if let Some(io) = work.as_deref_mut() {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            if grant.strip_prefix("task:spawn:").and_then(|grant| grant.strip_prefix(name))
                .and_then(|grant| grant.strip_prefix('@')) == Some(version)
            {
                descendant = true;
                break;
            }
        }
        if !descendant {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) { return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized, reason: "original scope lacks the registered descendant grant",
            }); }
            require_descendant_grant(scope.grants(), &entry.identity)?;
        }
        if parent.is_some() && (scope.extensions().is_some() || entry.requirements.iter().any(|requirement| requirement.starts_with("extension:"))) {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) { return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported, reason: "stock child extension runner is not composed",
            }); }
            return Err(Error::Unsupported("stock child extension runner is not composed".into()));
        }
        if work.is_none() {
            return TaskAdmissionRecord::from_parts(
                operation, name, version, input, entry.input_schema.clone(), entry.output_schema.clone(),
                &entry.requirements, &machine.digest, parent, scope.grants().clone(),
                scope.limits(), scope.run_limits(), None, scope.extensions().cloned(), None,
            ).and_then(|record| record.with_resource_proofs(scope.resource_proofs().to_vec()));
        }
        task_registered_portable_admission(operation, input, parent, scope, entry, machine, work)
    }

    pub(crate) fn validate_durable_admission(
        &self, identity: &ComponentIdentity, machine: &MachineIdentity,
        input_schema: Option<&Value>, output_schema: &Value, input: Option<&Value>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let mut quota = crate::wire_codec::CodecQuota::new(work,0)?;
        let name = crate::core::copy_text(&identity.name, &mut quota)?;
        let version = crate::core::copy_text(&identity.version, &mut quota)?;
        let lookup = (name,version);
        let entry = match self.0.get(&lookup) {
            Some(entry) => entry,
            None => {
                #[cfg(all(feature = "filesystem",not(target_arch = "wasm32")))]
                if quota.work().as_ref().is_some_and(|io| io.original_native_work().is_some()) {
                    return Err(Error::NativeBoundary {code:crate::wire::ErrorCode::NotFound,
                        reason:"pinned durable task revision is unavailable"});
                }
                return Err(Error::NotFound(format!("task {}@{}",identity.name,identity.version)));
            }
        };
        if &entry.identity != identity
            || entry.workflow_definition.is_none()
            || entry.machine.as_ref() != Some(machine)
            || input_schema.is_some_and(|schema| schema != &entry.input_schema)
            || &entry.output_schema != output_schema
        {
            return Err(Error::Conflict(
                "durable admission differs from its registered task definition".into(),
            ));
        }
        if let Some(input) = input {
            crate::contract::validate_json_schema_value_with_work(
                &entry.input_schema,input,"task input",quota.work(),
            )?;
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
    /// Nonzero provider attestation over readiness, resources, mounts, and scoped credential bindings; never a credential or endpoint itself.
    pub readiness_revision: [u8; 32],
}

impl ExecutionPlacement {
    /// Rejects unqualified or invalid placement before admission.
    pub fn validate(&self, work: &mut Option<&mut dyn crate::store::HistoryIo>) -> Result<()> {
        if let Some(io) = work.as_deref() { io.check_cancellation()?; }
        if !crate::contract::is_valid_component_label(&self.provider.name)
            || !crate::contract::is_valid_component_label(&self.provider.version)
            || self.provider.digest == [0; 32] || self.readiness_revision == [0; 32]
        {
            return Err(crate::store::guarded_boundary(work, crate::wire::ErrorCode::Invalid,
                "execution qualification is invalid"));
        }
        self.build.validate(work)?;
        if let Some(environment) = &self.environment { environment.validate(work)?; }
        Ok(())
    }
}

/// Exact owner-retained task admission.
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
    /// Exact created-resource evidence, authenticated by the bound provider.
    pub resource_proofs: Vec<crate::resource_preparation::ResourceCreationProofRef>,
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

#[derive(Serialize)]
struct CanonicalTaskAdmission<'a> {
    contract: &'static str,
    #[serde(flatten)]
    admission: &'a TaskAdmissionRecord,
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

/// Canonical ordinary task identity for a caller-retained admission operation.
#[must_use]
pub fn task_id_for_admission(operation: OperationId) -> TaskId {
    TaskId::from_bytes(operation.into_bytes())
}

impl TaskAdmissionRecord {
    /// Canonical task identity of this ordinary admission.
    #[must_use]
    pub fn task_id(&self) -> TaskId {
        task_id_for_admission(self.operation_id)
    }

    /// Concrete admission payload ownership; allocator-node backing is excluded.
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let overflow = || {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            { Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "task admission ownership overflows",
            } }
            #[cfg(any(not(feature = "filesystem"), target_arch = "wasm32"))]
            { Error::Invalid("task admission ownership overflows".into()) }
        };
        let component = |identity: &ComponentIdentity| -> Result<u64> {
            (identity.name.capacity() as u64).checked_add(identity.version.capacity() as u64)
                .ok_or_else(overflow)
        };
        let slots = self.resource_proofs.capacity()
            .checked_mul(std::mem::size_of::<crate::resource_preparation::ResourceCreationProofRef>())
            .ok_or_else(overflow)? as u64;
        let mut owned = slots.checked_add(component(&self.task)?)
            .and_then(|bytes| bytes.checked_add(self.machine.name.capacity() as u64))
            .and_then(|bytes| bytes.checked_add(self.machine.version.capacity() as u64))
            .ok_or_else(overflow)?;
        for value in [&self.input, &self.input_schema, &self.output_schema] {
            owned = owned.checked_add(crate::contract::json_value_logical_owned_bytes(value)?)
                .ok_or_else(overflow)?;
        }
        owned = owned.checked_add(self.grants.logical_owned_bytes()?).ok_or_else(overflow)?;
        for proof in &self.resource_proofs {
            for bytes in [proof.provider.logical_owned_bytes()?,
                proof.resource.logical_owned_bytes()?, proof.evidence.logical_owned_bytes()?]
            {
                owned = owned.checked_add(bytes).ok_or_else(overflow)?;
            }
        }
        if let Some(policy) = &self.policy {
            owned = owned.checked_add(component(policy)?).ok_or_else(overflow)?;
        }
        if let Some(extensions) = &self.extensions {
            owned = owned.checked_add(extensions.logical_owned_bytes()?).ok_or_else(overflow)?;
        }
        if let Some(execution) = &self.execution {
            let build = execution.build.as_resource().logical_owned_bytes()?;
            owned = owned.checked_add(component(&execution.provider)?)
                .and_then(|bytes| bytes.checked_add(build)).ok_or_else(overflow)?;
            if let Some(environment) = &execution.environment {
                owned = owned.checked_add(environment.as_resource().logical_owned_bytes()?)
                    .ok_or_else(overflow)?;
            }
        }
        Ok(owned)
    }

    /// Copies the original admission fields on the caller's allocation context.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        use crate::store::history::HistoryIo as _;
        fn resource(
            source: &crate::resources::ResourceRef,
            quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
        ) -> Result<crate::resources::ResourceRef> {
            let provider = crate::core::copy_provider(source.provider(), quota)?;
            let key = crate::core::copy_copy_vector(source.key(), quota)?;
            let version = source.version().map(|value| crate::core::copy_text(value, quota)).transpose()?;
            crate::resources::ResourceRef::new(
                source.kind(), provider, key, version, quota.work(),
            )
        }
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        io.check_cancellation()?;
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut io);
        let mut quota = crate::wire_codec::CodecQuota::new(
            &mut observer, std::mem::size_of::<Self>() as u64,
        )?;
        let task = task_copy_component(&self.task, &mut quota)?;
        let machine = MachineIdentity {
            name: crate::core::copy_text(&self.machine.name, &mut quota)?,
            version: crate::core::copy_text(&self.machine.version, &mut quota)?,
            digest: self.machine.digest,
        };
        let input = crate::core::copy_value(&self.input, &mut quota)?;
        let input_schema = crate::core::copy_value(&self.input_schema, &mut quota)?;
        let output_schema = crate::core::copy_value(&self.output_schema, &mut quota)?;
        let resource_proofs = RuntimeScope::copy_resource_proofs(&self.resource_proofs, &mut quota)?;
        let (grants, grant_custody) = self.grants.clone_with_native_work(work)?;
        let grant_bytes = grant_custody.bytes();
        quota.work().as_deref_mut().expect("native admission clone observer")
            .absorb_native_custody(grant_custody)?;
        quota.adopt(grant_bytes)?;
        let policy = self.policy.as_ref().map(|policy| task_copy_component(policy, &mut quota)).transpose()?;
        let extensions = self.extensions.as_ref().map(|source| -> Result<_> {
            let copied = source.copy(quota.work())?;
            quota.adopt(copied.logical_owned_bytes()?)?;
            Ok(copied)
        }).transpose()?;
        let execution = self.execution.as_ref().map(|source| -> Result<_> {
            let provider = task_copy_component(&source.provider, &mut quota)?;
            let build = resource(source.build.as_resource(), &mut quota)?;
            let build = ArtifactRef::from_resource_with_work(build, quota.work())?;
            let environment = source.environment.as_ref().map(|source| -> Result<_> {
                let copied = resource(source.as_resource(), &mut quota)?;
                SandboxRef::from_resource_with_work(copied, quota.work())
            }).transpose()?;
            Ok(ExecutionPlacement { provider, build, environment,
                readiness_revision: source.readiness_revision })
        }).transpose()?;
        let copied = Self {
            operation_id: self.operation_id, task, machine, input, input_schema, output_schema,
            parent: self.parent, resource_proofs, grants, limits: self.limits,
            run_limits: self.run_limits, policy, extensions, execution,
        };
        let held = quota.retained();
        quota.transfer();
        drop(quota);
        drop(observer);
        let custody = io.take_native_custody(held)?;
        if !custody.matches_work(work) {
            drop(copied);
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                custody: Some(custody),
            });
        }
        Ok((copied, custody))
    }

    /// Constructs and validates an exact admission envelope from the fields supplied by an SDK boundary.
    #[allow(
        clippy::too_many_arguments,
        reason = "flat SDK-boundary constructor taking each envelope field"
    )]
    pub fn from_parts(
        operation_id: OperationId,
        name: &str,
        version: &str,
        input: Value,
        input_schema: Value,
        output_schema: Value,
        requirements: &BTreeSet<String>,
        machine_digest: &[u8],
        parent: Option<TaskId>,
        grants: Capabilities,
        limits: Limits,
        run_limits: TaskRunLimits,
        policy: Option<ComponentIdentity>,
        extensions: Option<ExtensionAdmission>,
        execution: Option<ExecutionPlacement>,
    ) -> Result<Self> {
        let (task, machine) = task_admission_identities(
            name,
            version,
            &input_schema,
            &output_schema,
            requirements,
            machine_digest,
        )?;
        let record = Self {
            operation_id,
            task,
            machine,
            input,
            input_schema,
            output_schema,
            parent,
            resource_proofs: Vec::new(),
            grants,
            limits,
            run_limits,
            policy,
            extensions,
            execution,
        };
        record.validate(&mut None)?;
        Ok(record)
    }

    /// Binds exact provider evidence into this immutable child admission.
    pub fn with_resource_proofs(
        mut self,
        proofs: Vec<crate::resource_preparation::ResourceCreationProofRef>,
    ) -> Result<Self> {
        self.resource_proofs = proofs;
        self.validate(&mut None)?;
        Ok(self)
    }

    /// Validates the admission on the optional history observer.
    pub fn validate(
        &self, work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if [&self.task.name, &self.task.version, &self.machine.name, &self.machine.version]
            .into_iter().any(|label| !crate::contract::is_valid_component_label(label))
            || self.task.name.contains('@') || self.task.version.contains('@')
        {
            return Err(crate::store::guarded_boundary(work, crate::wire::ErrorCode::Invalid,
                "task or machine identity is invalid"));
        }
        if self.task.digest == [0; 32]
            || self.machine.digest == [0; 32]
            || self.machine.name != self.task.name
            || self.machine.version != self.task.version
        {
            return Err(crate::store::guarded_boundary(work, crate::wire::ErrorCode::Invalid, "task and machine identities disagree"));
        }
        if !self.resource_proofs.is_empty() && self.parent.is_none() {
            return Err(crate::store::guarded_boundary(work, crate::wire::ErrorCode::Invalid, "resource-derived admission requires a parent"));
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut created = BTreeSet::new();
        for proof in &self.resource_proofs {
            proof.validate(quota.work())?;
            let bytes = crate::store::history::canonical_bytes_with_work(
                &proof.resource, acyclic_stream::MAX_RECORD_BYTES as u64, quota.work(),
            )?;
            quota.adopt(bytes.capacity() as u64)?;
            let slot = std::mem::size_of::<Vec<u8>>() as u64;
            if let Some(io) = quota.work().as_deref_mut() { io.retain_owned(slot)?; }
            quota.adopt(slot)?;
            if !created.insert(bytes) {
                return Err(crate::store::guarded_boundary(quota.work(), crate::wire::ErrorCode::Invalid, "task admission repeats a created resource proof"));
            }
        }
        validate_task_schemas(&self.input_schema, &self.output_schema, true, quota.work())?;
        crate::contract::validate_json_schema_value_with_work(
            &self.input_schema, &self.input, "task input", quota.work(),
        )?;
        self.limits.validate(quota.work())?;
        self.run_limits.validate(quota.work())?;
        if let Some(policy) = &self.policy {
            if !crate::contract::is_valid_component_label(&policy.name)
                || !crate::contract::is_valid_component_label(&policy.version)
            {
                return Err(crate::store::guarded_boundary(quota.work(), crate::wire::ErrorCode::Invalid,
                    "policy identity is invalid"));
            }
            if policy.digest == [0; 32] {
                return Err(crate::store::guarded_boundary(quota.work(), crate::wire::ErrorCode::Invalid, "policy digest is empty"));
            }
        }
        if let Some(extensions) = &self.extensions {
            extensions.validate(quota.work())?;
        }
        if let Some(execution) = &self.execution { execution.validate(quota.work())?; }
        Ok(())
    }

    pub(crate) fn canonical_payload(&self) -> impl Serialize + '_ {
        CanonicalTaskAdmission {contract:"harness.task-admission.v2",admission:self}
    }

    pub(crate) fn canonical_digest_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<[u8; 32]> {
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let bytes = crate::store::history::canonical_bytes_with_work(
            &self.canonical_payload(), acyclic_stream::MAX_RECORD_BYTES as u64, custody.work(),
        )?;
        custody.adopt(bytes.capacity() as u64)?;
        if let Some(io) = custody.work().as_deref_mut() { io.hashed_bytes(bytes.len() as u64)?; }
        Ok(*blake3::hash(&bytes).as_bytes())
    }

    /// Ref-only v2 envelope retained by the owner outside event history.
    #[must_use]
    pub fn canonical_value(&self) -> Value {
        serde_json::to_value(self.canonical_payload()).expect("task admission serializes")
    }

    /// Decodes the canonical admission and reports its new container bytes.
    pub fn from_canonical_value(
        value: Value, work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<(Self, u64)> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let canonical = crate::store::history::canonical_bytes_with_work(
            &value, acyclic_stream::MAX_RECORD_BYTES as u64, quota.work(),
        )?;
        quota.adopt(canonical.capacity() as u64)?;
        let mut body = value;
        let fields = match body.as_object_mut() {
            Some(fields) => fields,
            None => return Err(crate::store::guarded_boundary(quota.work(), crate::wire::ErrorCode::Invalid, "task admission must be an object")),
        };
        if !matches!(fields.remove("contract"), Some(Value::String(contract)) if contract == "harness.task-admission.v2") {
            return Err(crate::store::guarded_boundary(quota.work(), crate::wire::ErrorCode::Invalid, "unsupported task admission contract"));
        }
        let (admission, added) = Self::from_owned_body_with_work(body, quota.work())?;
        admission.validate(quota.work())?;
        let expected = crate::store::history::canonical_bytes_with_work(
            &admission.canonical_payload(), acyclic_stream::MAX_RECORD_BYTES as u64, quota.work(),
        )?;
        quota.adopt(expected.capacity() as u64)?;
        if expected != canonical {
            return Err(crate::store::guarded_boundary(quota.work(), crate::wire::ErrorCode::Invalid, "task admission is not canonical v2"));
        }
        Ok((admission, added))
    }

    /// Consumes the original paid JSON tree. Only new typed containers grow.
    pub(crate) fn from_owned_body_with_work(
        body: Value, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
            let mut quota = crate::wire_codec::CodecQuota::new(work, std::mem::size_of::<Self>() as u64)?;
            crate::contract::json_owned_value_shape_bounded(
                &body, crate::contract::JsonBounds {
                    maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
                    maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64, maximum_depth: 256,
                }, crate::contract::JsonValueSchema::TaskAdmissionRecord,
                &mut |charge| quota.work().as_deref_mut()
                    .expect("native admission schema observer").codec_charge(charge),
            )?;
            let mut fields = Self::owned_fields(body, quota.work())?;
            let operation_id = Self::decode_owned_fixed(
                Self::take_owned_field(&mut fields, "operation_id", quota.work())?, quota.work(),
            )?;
            let task = Self::decode_owned_fixed(
                Self::take_owned_field(&mut fields, "task", quota.work())?, quota.work(),
            )?;
            let machine = Self::decode_owned_fixed(
                Self::take_owned_field(&mut fields, "machine", quota.work())?, quota.work(),
            )?;
            let input = Self::take_owned_field(&mut fields, "input", quota.work())?;
            let input_schema = Self::take_owned_field(&mut fields, "input_schema", quota.work())?;
            let output_schema = Self::take_owned_field(&mut fields, "output_schema", quota.work())?;
            let parent = Self::decode_owned_fixed(
                Self::take_owned_field(&mut fields, "parent", quota.work())?, quota.work(),
            )?;
            let proof_values = Self::owned_array(
                Self::take_owned_field(&mut fields, "resource_proofs", quota.work())?, quota.work(),
            )?;
            let mut resource_proofs = Vec::new();
            let slots = Self::reserve_owned_slots(&mut resource_proofs, proof_values.len(), quota.work())?;
            quota.adopt(slots)?;
            for value in proof_values {
                let (proof, slots) = Self::decode_owned_proof(value, quota.work())?;
                quota.adopt(slots)?;
                resource_proofs.push(proof);
            }
            let grant_values = Self::owned_array(
                Self::take_owned_field(&mut fields, "grants", quota.work())?, quota.work(),
            )?;
            if let Some(io) = quota.work().as_deref_mut() {
                io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: grant_values.len() as u64 })?;
            }
            if grant_values.iter().any(|value| !value.is_string()) {
                return Err(crate::store::guarded_boundary(quota.work(), crate::wire::ErrorCode::Invalid, "task capability is not a string"));
            }
            let slots = grant_values.len().checked_mul(std::mem::size_of::<String>())
                .ok_or_else(|| crate::store::guarded_boundary(quota.work(), crate::wire::ErrorCode::Invalid, "task capability slots overflow"))? as u64;
            if let Some(io) = quota.work().as_deref_mut() {
                io.admit_owned(slots)?;
                io.retain_owned(slots)?;
            }
            quota.adopt(slots)?;
            let grants = Capabilities::new(grant_values.into_iter().map(|value| {
                let Value::String(text) = value else { unreachable!("checked capability shape") };
                text
            }));
            let limits = Self::decode_owned_fixed(
                Self::take_owned_field(&mut fields, "limits", quota.work())?, quota.work(),
            )?;
            let run_limits = Self::decode_owned_fixed(
                Self::take_owned_field(&mut fields, "run_limits", quota.work())?, quota.work(),
            )?;
            let policy = Self::decode_owned_fixed(
                Self::take_owned_field(&mut fields, "policy", quota.work())?, quota.work(),
            )?;
            let extension_value = Self::take_owned_field(&mut fields, "extensions", quota.work())?;
            let extensions = if extension_value.is_null() { None } else {
                let (extension, slots) = ExtensionAdmission::from_owned_value_with_work(
                    extension_value, quota.work(),
                )?;
                quota.adopt(slots)?;
                Some(extension)
            };
            let execution_value = Self::take_owned_field(&mut fields, "execution", quota.work())?;
            let execution = if execution_value.is_null() { None } else {
                let (execution, slots) = Self::decode_owned_execution(execution_value, quota.work())?;
                quota.adopt(slots)?;
                Some(execution)
            };
            if !fields.is_empty() {
                return Err(crate::store::guarded_boundary(quota.work(), crate::wire::ErrorCode::Invalid, "task admission has unknown fields"));
            }
            let admission = Self { operation_id, task, machine, input, input_schema, output_schema,
                parent, resource_proofs, grants, limits, run_limits, policy, extensions, execution };
            let added = quota.retained();
            quota.transfer();
            return Ok((admission, added));
        }
        let admission: Self = serde_json::from_value(body)
            .map_err(|source| Error::Invalid(source.to_string()))?;
        Ok((admission, 0))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn owned_fields(
        value: Value, work: &Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<serde_json::Map<String, Value>> {
        match value {
            Value::Object(fields) => Ok(fields),
            _ => Err(crate::store::guarded_boundary(work, crate::wire::ErrorCode::Invalid, "task metadata must be an object")),
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn take_owned_field(
        fields: &mut serde_json::Map<String, Value>, name: &'static str,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Value> {
        if let Some(io) = work.as_deref_mut() {
            io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
        }
        fields.remove(name).ok_or_else(|| crate::store::guarded_boundary(work, crate::wire::ErrorCode::Invalid, "task metadata field is missing"))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn owned_array(
        value: Value, work: &Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Vec<Value>> {
        match value {
            Value::Array(values) => Ok(values),
            _ => Err(crate::store::guarded_boundary(work, crate::wire::ErrorCode::Invalid, "task metadata must be an array")),
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reserve_owned_slots<T>(
        values: &mut Vec<T>, maximum: usize,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<u64> {
        if maximum == 0 { return Ok(0); }
        let bytes = maximum.checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| crate::store::guarded_boundary(work, crate::wire::ErrorCode::Invalid, "task metadata slots overflow"))? as u64;
        let io = work.as_deref_mut().expect("native metadata observer");
        io.admit_allocation(bytes)?;
        values.try_reserve_exact(maximum).map_err(|cause| io.allocation_failure(cause))?;
        let owned = values.capacity().checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "task metadata capacity overflows" })? as u64;
        io.retain_owned(owned)?;
        Ok(owned)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn decode_owned_fixed<T: serde::de::DeserializeOwned>(
        value: Value, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<T> {
        const LABELS: &str = concat!(
            "ComponentIdentity MachineIdentity TaskId OperationId ResourceKind Limits TaskRunLimits ",
            "name version digest parent task operation_id kind file_bytes path_bytes attachments ",
            "render_bytes model_steps model_events_per_step tool_calls_per_step context_messages ",
            "concurrency max_steps deadline_epoch_ms namespace family policy readiness_revision ",
            "``, ``, ``, ``, ``, ``, workspace generation artifact sandbox checkpoint stream context run ",
            "a sequence a string an integer a UUID 32 elements hexadecimal hyphen urn uuid ",
        );
        let native = work.as_deref().and_then(|io| io.original_native_work())
            .expect("native fixed metadata observer").clone();
        let mut diagnostic_custody = None;
        let decoded = crate::contract::json_from_value_bounded(
            std::borrow::Cow::Owned(value), crate::contract::JsonBounds {
                maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
                maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64, maximum_depth: 256,
            }, 0, crate::wire::ErrorCode::Invalid, crate::contract::JsonValueSchema::None,
            &mut |charge| {
                if let crate::contract::JsonCodecCharge::Admission { value_shape, .. } = charge {
                    let diagnostic = crate::contract::json_typed_diagnostic_owned_bytes(
                        value_shape, LABELS.len() as u64, Some(crate::wire::ErrorCode::Invalid),
                    )?;
                    diagnostic_custody = Some(native.claim(diagnostic, 0, acyclic_fs::WorkCounters::UNCHARGED)
                        .map_err(|cause| Error::NativeWork { cause, custody: None })?);
                    Ok(())
                } else {
                    work.as_deref_mut().expect("native fixed metadata observer").codec_charge(charge)
                }
            },
        );
        match decoded {
            Err(Error::NativeDecode { code, source, custody: None }) =>
                Err(Error::NativeDecode { code, source, custody: diagnostic_custody }),
            other => other,
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn decode_owned_provider(
        value: Value, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::resources::ProviderRef> {
        let mut fields = Self::owned_fields(value, work)?;
        let namespace = Self::decode_owned_fixed(Self::take_owned_field(&mut fields, "namespace", work)?, work)?;
        let family = Self::decode_owned_fixed(Self::take_owned_field(&mut fields, "family", work)?, work)?;
        let version = Self::decode_owned_fixed(Self::take_owned_field(&mut fields, "version", work)?, work)?;
        if !fields.is_empty() { return Err(crate::store::guarded_boundary(work, crate::wire::ErrorCode::Invalid, "provider has unknown fields")); }
        crate::resources::ProviderRef::new(namespace, family, version, work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn decode_owned_resource(
        value: Value, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(crate::resources::ResourceRef, u64)> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut fields = Self::owned_fields(value, quota.work())?;
        let kind = Self::decode_owned_fixed(Self::take_owned_field(&mut fields, "kind", quota.work())?, quota.work())?;
        let provider = Self::decode_owned_provider(
            Self::take_owned_field(&mut fields, "provider", quota.work())?, quota.work(),
        )?;
        let values = Self::owned_array(Self::take_owned_field(&mut fields, "key", quota.work())?, quota.work())?;
        let mut key = Vec::new();
        quota.work().as_deref_mut().expect("native resource observer").reserve_bytes(&mut key, values.len())?;
        quota.adopt(key.capacity() as u64)?;
        for value in values {
            if let Some(io) = quota.work().as_deref_mut() {
                io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
            }
            let byte = value.as_u64().filter(|byte| *byte <= u8::MAX as u64)
                .ok_or_else(|| crate::store::guarded_boundary(quota.work(), crate::wire::ErrorCode::Invalid, "resource key byte is invalid"))?;
            key.push(byte as u8);
        }
        let version = Self::decode_owned_fixed(Self::take_owned_field(&mut fields, "version", quota.work())?, quota.work())?;
        if !fields.is_empty() { return Err(crate::store::guarded_boundary(quota.work(), crate::wire::ErrorCode::Invalid, "resource has unknown fields")); }
        let reference = crate::resources::ResourceRef::new(kind, provider, key, version, quota.work())?;
        let added = quota.retained();
        quota.transfer();
        Ok((reference, added))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn decode_owned_proof(
        value: Value, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(crate::resource_preparation::ResourceCreationProofRef, u64)> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut fields = Self::owned_fields(value, quota.work())?;
        let provider = Self::decode_owned_provider(Self::take_owned_field(&mut fields, "provider", quota.work())?, quota.work())?;
        let preparation = Self::decode_owned_fixed(Self::take_owned_field(&mut fields, "preparation", quota.work())?, quota.work())?;
        let (resource, added) = Self::decode_owned_resource(Self::take_owned_field(&mut fields, "resource", quota.work())?, quota.work())?;
        quota.adopt(added)?;
        let (evidence, added) = Self::decode_owned_resource(Self::take_owned_field(&mut fields, "evidence", quota.work())?, quota.work())?;
        quota.adopt(added)?;
        if !fields.is_empty() { return Err(crate::store::guarded_boundary(quota.work(), crate::wire::ErrorCode::Invalid, "resource proof has unknown fields")); }
        let proof = crate::resource_preparation::ResourceCreationProofRef { provider, preparation, resource, evidence };
        let added = quota.retained();
        quota.transfer();
        Ok((proof, added))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn decode_owned_execution(
        value: Value, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(ExecutionPlacement, u64)> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut fields = Self::owned_fields(value, quota.work())?;
        let provider = Self::decode_owned_fixed(Self::take_owned_field(&mut fields, "provider", quota.work())?, quota.work())?;
        let (build, added) = Self::decode_owned_resource(Self::take_owned_field(&mut fields, "build", quota.work())?, quota.work())?;
        quota.adopt(added)?;
        let build = ArtifactRef::from_resource_with_work(build, quota.work())?;
        let environment_value = Self::take_owned_field(&mut fields, "environment", quota.work())?;
        let environment = if environment_value.is_null() { None } else {
            let (environment, added) = Self::decode_owned_resource(environment_value, quota.work())?;
            quota.adopt(added)?;
            Some(SandboxRef::from_resource_with_work(environment, quota.work())?)
        };
        let readiness_revision = Self::decode_owned_fixed(Self::take_owned_field(&mut fields, "readiness_revision", quota.work())?, quota.work())?;
        if !fields.is_empty() { return Err(crate::store::guarded_boundary(quota.work(), crate::wire::ErrorCode::Invalid, "execution placement has unknown fields")); }
        let execution = ExecutionPlacement { provider, build, environment, readiness_revision };
        let added = quota.retained();
        quota.transfer();
        Ok((execution, added))
    }
}

/// Provider boundary for stable durable admission and outcome observation.
pub trait DurableTaskHost: TaskStateProvider {
    /// Requires the current uncancelled running lease before a fresh task step or dispatch.
    fn verify_dispatch_owner<'a>(
        &'a self,
        _task_id: TaskId,
        _fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable dispatch ownership is not bound".into(),
            ))
        })
    }

    /// Checks that this lease still owns execution or reconciliation.
    fn verify_execution_owner<'a>(
        &'a self,
        _task_id: TaskId,
        _fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable execution ownership is not bound".into(),
            ))
        })
    }

    /// Native execution support is an allocation-free preflight, not authority.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        Ok(false)
    }

    /// Charges one exact durable model attempt under the current task owner.
    fn claim_model_dispatch<'a>(
        &'a self,
        _task_id: TaskId,
        _attempt_id: OperationId,
        _step: u32,
        _request_digest: [u8; 32],
        _fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "durable model accounting is not bound".into(),
            ))
        })
    }

    /// Claims the same durable model attempt on its original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn claim_model_dispatch_with_native_work<'a>(
        &'a self,
        _task: TaskId,
        _attempt: OperationId,
        _step: u32,
        _request_digest: [u8; 32],
        _fence: &'a crate::scheduler::LeaseFence,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "durable host has no original-ledger model claim",
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
    /// Admits a child under its exact current parent's execution fence.
    fn admit_owned<'a>(
        &'a self,
        _request: TaskAdmissionRecord,
        _fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
        Box::pin(async {
            Err(Error::Unsupported("owned child admission is not bound".into()))
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn admit_owned_with_native_work<'a>(
        &'a self,
        request: (TaskAdmissionRecord, acyclic_fs::NativeAllocation),
        fence: (crate::scheduler::LeaseFence, acyclic_fs::NativeAllocation),
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Admission<TaskId>, acyclic_fs::NativeAllocation)>> {
        drop(request);
        drop(fence);
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "durable host has no original-ledger owned admission producer",
        })
    }
    /// Resolves an uncertain admission by its original operation alone.
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

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_admission_with_native_work<'a>(
        &'a self,
        _operation: OperationId,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<(TaskId, TaskAdmissionRecord)>, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "durable host has no original-ledger admission reconciliation producer",
        })
    }
    /// Publishes one immutable batch request before declaring any indexed child.
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
    /// Observes the committed batch and its indexed admissions without submitting any absent member or retrying an uncertain effect.
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
    /// Loads the owner's original immutable batch request by identity.
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
    /// Publishes one durable group cancellation declaration and reconciles requests for every accepted member.
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
}

/// Owner-bound durable task state, independently replaceable from admission and execution.
pub trait TaskStateProvider: acyclic_stream::ProviderPlatform {
    /// Exact policy revision enforced when state-bound effects are reconciled.
    fn policy_identity(&self) -> Option<ComponentIdentity>;
    /// Execution route retained and observable by this state owner.
    fn execution_identity(&self) -> Option<ComponentIdentity> {
        None
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn policy_identity_with_native_work(
        &self, _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "task state has no original native policy binding source" })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execution_identity_with_native_work(
        &self, _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "task state has no original native execution binding source" })
    }
    /// Reads the complete immutable admission for cross-provider attestation.
    fn observe_admission<'a>(
        &'a self,
        task_id: TaskId,
    ) -> BoxFuture<'a, Result<TaskAdmissionRecord>>;

    /// Native admission observation must preserve its original metadata custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn observe_admission_with_native_work<'a>(
        &'a self,
        _task: TaskId,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (TaskAdmissionRecord, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "task state has no original-ledger admission observer",
        })
    }
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

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn resume_scope_with_native_work<'a>(
        &'a self,
        _task: TaskId,
        _operation: OperationId,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (RuntimeScope, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "task state has no original-ledger scope producer",
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_dispatch_owner_with_native_work<'a>(
        &'a self, _task: TaskId, _fence: &'a crate::scheduler::LeaseFence,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "task state has no original-ledger fresh owner verifier" })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_execution_owner_with_native_work<'a>(
        &'a self, _task: TaskId, _fence: &'a crate::scheduler::LeaseFence,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "task state has no original-ledger settlement owner verifier" })
    }
    /// Observes a terminal result or ordinary pending state without dispatch.
    fn outcome<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<Option<Outcome<Value>>>>;

    /// Native terminal observation never re-executes the admitted task.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn outcome_with_native_work<'a>(
        &'a self,
        _task: TaskId,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<Outcome<Value>>, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "task state has no original-ledger outcome observer",
        })
    }
    /// Waits for a terminal result; providers may override polling.
    fn wait_outcome<'a>(&'a self, task_id: TaskId) -> BoxFuture<'a, Result<Outcome<Value>>> {
        Box::pin(async move {
            let mut delay_ms = 50_u64;
            loop {
                if let Some(outcome) = self.outcome(task_id).await? {
                    return Ok(outcome);
                }
                crate::live::sleep_millis(delay_ms).await?;
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

    /// Pure discovery before request validation or future construction.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_interactions(&self, _work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "task state has no original-ledger interaction route" })
    }

    /// Native routing is independently replaceable, never an ordinary fallback.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn interact_with_native_work<'a>(
        &'a self, _task_id: TaskId, _operation_id: OperationId,
        _interaction: Interaction, _request_custody: acyclic_fs::NativeAllocation,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (InteractionOutcome, acyclic_fs::NativeAllocation)>> {
        let _paid = (_interaction, _request_custody);
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "task state has no original-ledger interaction route" })
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
}

struct HostTaskSpawner(
    Arc<dyn DurableTaskHost>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Option<acyclic_fs::NativeAllocation>,
);

impl HostTaskSpawner {
    fn new_observed(host: Arc<dyn DurableTaskHost>, io: StartupIo<'_>) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let custody = if let Some(io) = io {
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
            io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
            Some(io.take_native_custody(std::mem::size_of::<Self>() as u64)?)
        } else { None };
        let _ = io;
        Ok(Self(host, #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] custody))
    }
}



/// Independently replaceable durable admission boundary.
pub trait TaskSpawner: acyclic_stream::ProviderPlatform {
    /// Exact policy revision enforced during child admission.
    fn policy_identity(&self) -> Option<ComponentIdentity>;
    /// Execution route this spawner can actually dispatch to.
    fn execution_identity(&self) -> Option<ComponentIdentity> {
        None
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn policy_identity_with_native_work(
        &self, _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "task spawner has no original native policy binding source" })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execution_identity_with_native_work(
        &self, _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "task spawner has no original native execution binding source" })
    }
    /// Publishes or reconciles one complete caller-identified admission.
    fn admit<'a>(
        &'a self,
        request: TaskAdmissionRecord,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>>;
    /// Publishes a child only while this exact original parent lease is current.
    fn admit_owned<'a>(
        &'a self,
        _request: TaskAdmissionRecord,
        _fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
        Box::pin(async {
            Err(Error::Unsupported("owned child spawner is not bound".into()))
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn admit_owned_with_native_work<'a>(
        &'a self,
        request: (TaskAdmissionRecord, acyclic_fs::NativeAllocation),
        fence: (crate::scheduler::LeaseFence, acyclic_fs::NativeAllocation),
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Admission<TaskId>, acyclic_fs::NativeAllocation)>> {
        drop(request);
        drop(fence);
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "task spawner has no original-ledger owned admission producer",
        })
    }
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

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_admission_with_native_work<'a>(
        &'a self,
        _operation: OperationId,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<(TaskId, TaskAdmissionRecord)>, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "task spawner has no original-ledger admission reconciliation producer",
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

impl TaskSpawner for HostTaskSpawner {
    fn policy_identity(&self) -> Option<ComponentIdentity> {
        self.0.policy_identity()
    }
    fn execution_identity(&self) -> Option<ComponentIdentity> {
        self.0.execution_identity()
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn policy_identity_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        self.0.policy_identity_with_native_work(work)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execution_identity_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&ComponentIdentity>> {
        self.0.execution_identity_with_native_work(work)
    }
    fn admit<'a>(
        &'a self,
        request: TaskAdmissionRecord,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
        self.0.admit(request)
    }
    fn admit_owned<'a>(
        &'a self,
        request: TaskAdmissionRecord,
        fence: crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<Admission<TaskId>>> {
        self.0.admit_owned(request, fence)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn admit_owned_with_native_work<'a>(
        &'a self,
        request: (TaskAdmissionRecord, acyclic_fs::NativeAllocation),
        fence: (crate::scheduler::LeaseFence, acyclic_fs::NativeAllocation),
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Admission<TaskId>, acyclic_fs::NativeAllocation)>> {
        self.0.admit_owned_with_native_work(request, fence, work)
    }
    fn reconcile_admission<'a>(
        &'a self,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<Option<(TaskId, TaskAdmissionRecord)>>> {
        self.0.reconcile_admission(operation_id)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_admission_with_native_work<'a>(
        &'a self,
        operation: OperationId,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<(TaskId, TaskAdmissionRecord)>, acyclic_fs::NativeAllocation)>> {
        self.0.reconcile_admission_with_native_work(operation, work)
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

/// One replaceable execution route.
pub trait ExecutionProvider: acyclic_stream::ProviderPlatform {
    /// Immutable provider implementation pinned in every admitted placement.
    fn identity(&self) -> ComponentIdentity;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn identity_with_native_work(
        &self, _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<&ComponentIdentity> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "execution provider has no original native identity source" })
    }
    /// Durable spawner for this provider's task build and environment.
    fn spawner(&self) -> Arc<dyn TaskSpawner>;
    /// Owner-bound observation and cancellation for the same route.
    fn state(&self) -> Arc<dyn TaskStateProvider>;
    /// Read-only qualification of one exact admitted task and its accessible input.
    fn qualify<'a>(
        &'a self,
        request: &'a TaskAdmissionRecord,
    ) -> BoxFuture<'a, Result<ExecutionPlacement>>;
    /// Batch qualification must cover every input before the manifest is published.
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
pub trait DurableEffectObserver: acyclic_stream::ProviderPlatform {
    /// Returns the latest attested status after querying the pinned attempt.
    fn reconcile<'a>(
        &'a self,
        effect_id: EffectId,
    ) -> BoxFuture<'a, Result<crate::core::EffectStatus>>;
}

/// Replaceable local interaction router.
pub trait InteractionRouter: acyclic_stream::ProviderPlatform {
    /// Routes an interaction while retaining its payloads in the provider.
    fn route<'a>(
        &'a self,
        operation_id: OperationId,
        interaction: Interaction,
    ) -> BoxFuture<'a, Result<InteractionOutcome>>;

    /// Pure native support discovery, before building a callback future.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_interactions(&self, _work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "interaction router has no original-ledger implementation" })
    }

    /// Moves the paid request into the original owner's guarded journal route.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn route_with_native_work<'a>(
        &'a self, _operation_id: OperationId, _interaction: Interaction,
        _request_custody: acyclic_fs::NativeAllocation,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (InteractionOutcome, acyclic_fs::NativeAllocation)>> {
        let _paid = (_interaction, _request_custody);
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "interaction router has no original-ledger route" })
    }
}

/// Owner-mediated interaction ledger.
pub type InteractionInspection = Option<(InteractionTicket, Option<InteractionResolution>)>;

/// Owner-mediated resolver for versioned questions and approvals.
pub trait InteractionResolver: acyclic_stream::ProviderPlatform {
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
    /// Rejects zero or nonportable runtime bounds.
    pub fn validate(&self, work: &mut Option<&mut dyn crate::store::HistoryIo>) -> Result<()> {
        if let Some(io) = work.as_deref() { io.check_cancellation()?; }
        if !self.is_valid() {
            return Err(crate::store::guarded_boundary(work, crate::wire::ErrorCode::Invalid,
                "task run limits are invalid"));
        }
        Ok(())
    }

    fn validate_observed(&self, io: StartupIo<'_>) -> Result<()> {
        native_startup::item(io, 0)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            let mut observer = io.map(crate::filesystem::PreparationIo::history_io);
            self.validate(&mut observer.as_mut().map(|io| io as &mut dyn crate::store::HistoryIo))
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        self.validate(&mut None)
    }

    fn is_valid(&self) -> bool {
        [self.concurrency.map(|value| value as u64),
            self.max_steps.map(|value| value as u64), self.deadline_epoch_ms]
            .into_iter().flatten().all(|value|
                value > 0 && value <= crate::conversation::MAX_EXACT_JS_INTEGER)
    }

    fn check_deadline(&self) -> Result<()> {
        let Some(deadline) = self.deadline_epoch_ms else {
            return Ok(());
        };
        let now = acyclic_stream::UnixMillisClock::now_unix_millis(&acyclic_stream::SystemUnixMillisClock);
        if deadline <= now {
            return Err(Error::Invalid("task deadline has expired".into()));
        }
        Ok(())
    }

    fn allows(&self, child: &Self) -> bool {
        [
            (self.concurrency.map(|value| value as u64), child.concurrency.map(|value| value as u64)),
            (self.max_steps.map(|value| value as u64), child.max_steps.map(|value| value as u64)),
            (self.deadline_epoch_ms, child.deadline_epoch_ms),
        ].into_iter().all(|(ancestor, descendant)|
            ancestor.is_none_or(|bound| descendant.is_some_and(|value| value <= bound)))
    }
}

/// Effective runtime authority and bounds; child scopes may only narrow them.
#[derive(Default)]
pub struct RuntimeScope {
    grants: Capabilities,
    limits: Limits,
    run_limits: TaskRunLimits,
    extensions: Option<ExtensionAdmission>,
    extensions_sealed: bool,
    extension_runtime: Option<Arc<ExtensionRuntime>>,
    extension_schemas: Option<SchemaRegistry>,
    resource_proofs: Vec<crate::resource_preparation::ResourceCreationProofRef>,
    created_access: Option<Arc<crate::resource_preparation::CreatedResourceAccess>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _native_grants_custody: Option<acyclic_fs::NativeAllocation>,
}

impl Clone for RuntimeScope {
    fn clone(&self) -> Self {
        Self {
            grants: self.grants.clone(),
            limits: self.limits,
            run_limits: self.run_limits,
            extensions: self.extensions.clone(),
            extensions_sealed: self.extensions_sealed,
            extension_runtime: self.extension_runtime.clone(),
            extension_schemas: self.extension_schemas.clone(),
            resource_proofs: self.resource_proofs.clone(),
            created_access: self.created_access.clone(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _native_grants_custody: None,
        }
    }
}

impl RuntimeScope {
    /// Creates a checked root scope.
    pub fn new(grants: Capabilities, limits: Limits) -> Result<Self> {
        limits.validate(&mut None)?;
        Ok(Self {
            grants,
            limits,
            run_limits: TaskRunLimits::default(),
            extensions: None,
            extensions_sealed: false,
            extension_runtime: None,
            extension_schemas: None,
            resource_proofs: Vec::new(),
            created_access: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _native_grants_custody: None,
        })
    }

    /// Moves already-paid grants into a checked native root, without a copy.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new_with_native_work(
        grants: (Capabilities, acyclic_fs::NativeAllocation),
        limits: Limits,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        (&limits).validate(&mut Some(&mut crate::store::native_work_io(work).map_err(|cause| crate::Error::NativeWork { cause, custody: None })?))?;
        let custody = work.claim(std::mem::size_of::<Self>() as u64, 0, acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let scope = Self {
            grants: grants.0,
            limits,
            run_limits: TaskRunLimits::default(),
            extensions: None,
            extensions_sealed: false,
            extension_runtime: None,
            extension_schemas: None,
            resource_proofs: Vec::new(),
            created_access: None,
            _native_grants_custody: Some(grants.1),
        };
        Ok((scope, custody))
    }

    /// Checks native session bounds without allocating a denial.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn validate_session_limits_with_native_work(
        limits: &crate::scheduler::SessionLimits,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.charge(acyclic_fs::WorkCounters::default())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        if limits.active_tasks == 0
            || limits.total_tasks == 0
            || limits.model_steps == 0
            || limits.active_tasks > limits.total_tasks
            || [limits.active_tasks, limits.total_tasks, limits.model_steps]
                .iter()
                .any(|value| *value > crate::conversation::MAX_EXACT_JS_INTEGER)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "session limits are invalid",
            });
        }
        Ok(())
    }

    /// Attenuates every grant and numeric bound.
    pub fn narrow(&self, grants: Capabilities, limits: Limits) -> Result<Self> {
        limits.validate(&mut None)?;
        if !self.allows_authority(&grants, &limits) {
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
            extension_schemas: self.extension_schemas.clone(),
            resource_proofs: self.resource_proofs.clone(),
            created_access: self.created_access.clone(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _native_grants_custody: None,
        })
    }

    fn allows_authority(&self, grants: &Capabilities, limits: &Limits) -> bool {
        grants.is_subset_of(&self.grants)
            && limits.file_bytes <= self.limits.file_bytes
            && limits.path_bytes <= self.limits.path_bytes
            && limits.attachments <= self.limits.attachments
            && limits.render_bytes <= self.limits.render_bytes
            && limits.model_steps <= self.limits.model_steps
            && limits.model_events_per_step <= self.limits.model_events_per_step
            && limits.tool_calls_per_step <= self.limits.tool_calls_per_step
            && limits.context_messages <= self.limits.context_messages
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn validate_narrowing_with_native_work(
        &self,
        grants: &Capabilities,
        limits: &Limits,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        (limits).validate(&mut Some(&mut crate::store::native_work_io(work).map_err(|cause| crate::Error::NativeWork { cause, custody: None })?))?;
        if !self.allows_authority(grants, limits) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "runtime scope cannot widen ancestor authority",
            });
        }
        Ok(())
    }

    /// Copies existing grants and concrete metadata on the original ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let grants = self.grants.clone_with_native_work(work)?;
        self.clone_native_metadata(grants, self.limits, self.extensions_sealed, work)
    }

    /// Attenuates paid grants using the ordinary checks and native metadata copy.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn narrow_with_native_work(
        &self,
        grants: (Capabilities, acyclic_fs::NativeAllocation),
        limits: Limits,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        self.validate_narrowing_with_native_work(&grants.0, &limits, work)?;
        self.clone_native_metadata(grants, limits, true, work)
    }

    /// Extends the caller's existing quota with the same physical proof copy.
    pub(crate) fn copy_resource_proofs(
        proofs: &[crate::resource_preparation::ResourceCreationProofRef],
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Vec<crate::resource_preparation::ResourceCreationProofRef>> {
        if quota.work().is_none() {
            return Ok(proofs.to_vec());
        }
        crate::core::copy_vector(proofs, quota, |proof, quota| {
            quota.work().as_deref_mut().expect("proof vector copy observer")
                .codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            let (copied, retained) = crate::core::resource_creation_proof_clone_with_custody(
                proof, quota.work(),
            )?;
            quota.adopt(retained)?;
            Ok(copied)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn clone_native_metadata(
        &self,
        grants: (Capabilities, acyclic_fs::NativeAllocation),
        limits: Limits,
        sealed: bool,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        use crate::store::history::HistoryIo as _;
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let extension_schemas = match &self.extension_schemas {
            Some(schemas) => Some(schemas.copy(&mut Some(&mut io))?),
            None => None,
        };
        let extensions = match &self.extensions {
            Some(extensions) => Some(extensions.copy(&mut Some(&mut io))?),
            None => None,
        };
        let resource_proofs = {
            let mut observer = Some(&mut io as &mut dyn crate::store::history::HistoryIo);
            let mut quota = crate::wire_codec::CodecQuota::from_retained(&mut observer, 0);
            let copied = Self::copy_resource_proofs(&self.resource_proofs, &mut quota)?;
            quota.transfer();
            copied
        };
        let scope = Self {
            grants: grants.0,
            limits,
            run_limits: self.run_limits,
            extensions,
            extensions_sealed: sealed,
            extension_runtime: self.extension_runtime.clone(),
            extension_schemas,
            resource_proofs,
            created_access: self.created_access.clone(),
            _native_grants_custody: Some(grants.1),
        };
        Ok((scope, io.into_custody()))
    }

    /// Further bounds concurrency, steps, and deadline for descendants.
    pub fn with_run_limits(
        mut self, limits: TaskRunLimits, work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<Self> {
        self.validate_run_limit_attenuation(&limits, work)?;
        self.run_limits = limits;
        Ok(self)
    }

    pub(crate) fn validate_run_limit_attenuation(
        &self, limits: &TaskRunLimits, work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        limits.validate(work)?;
        if !self.run_limits.allows(limits) {
            return Err(crate::store::guarded_boundary(work, crate::wire::ErrorCode::Unauthorized,
                "task scope cannot widen run limits"));
        }
        Ok(())
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
    pub fn with_extensions_from(self, agent: &Reducer) -> Result<Self> {
        self.with_original_extensions(agent.original_extension_binding()?)
    }

    pub(crate) fn with_original_extensions(
        mut self,
        original: (Option<ExtensionAdmission>, SchemaRegistry),
    ) -> Result<Self> {
        if self.extensions_sealed {
            return Err(Error::Unauthorized(
                "descendant scope cannot change extension selection".into(),
            ));
        }
        let (selection, schemas) = original;
        if self.extensions.is_some() && self.extensions != selection {
            return Err(Error::Conflict(
                "runtime scope extension selection is already pinned".into(),
            ));
        }
        self.extensions = selection;
        self.extension_schemas = Some(schemas);
        if let Some(runtime) = &self.extension_runtime {
            self.validate_extension_runtime(runtime)?;
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
        self.validate_extension_runtime(&runtime)?;
        if let Some(original) = &self.extension_runtime
            && !original.selected().eq(runtime.selected())
        {
            return Err(Error::Conflict(
                "runtime scope cannot retarget linked extension implementations".into(),
            ));
        }
        self.extension_runtime = Some(runtime);
        Ok(self)
    }

    fn validate_extension_runtime(&self, runtime: &ExtensionRuntime) -> Result<()> {
        runtime.validate_admission(self.extensions.as_ref(), &mut None)?;
        if let Some(schemas) = &self.extension_schemas {
            for identity in runtime.selected() {
                if schemas.implementation_digest(&identity.name, identity.version, &mut None)?
                    != identity.digest
                {
                    return Err(Error::Conflict(
                        "linked implementation differs from original admission registry".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn extension_schema_registry(&self) -> Option<SchemaRegistry> {
        self.extension_schemas.clone()
    }

    pub(crate) fn extension_schema_registry_ref(&self) -> Option<&SchemaRegistry> {
        self.extension_schemas.as_ref()
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
            extensions.validate(&mut None)?;
        }
        if self.extensions_sealed && self.extensions != extensions {
            return Err(Error::Conflict(
                "replay cannot retarget original extension admission".into(),
            ));
        }
        self.extensions = extensions;
        if let Some(runtime) = &self.extension_runtime {
            self.validate_extension_runtime(runtime)?;
        }
        self.extensions_sealed = true;
        Ok(self)
    }

    /// Exact durable proof references retained for descendant scope attenuation.
    #[must_use]
    pub fn resource_proofs(&self) -> &[crate::resource_preparation::ResourceCreationProofRef] {
        &self.resource_proofs
    }

    pub(crate) fn with_created_access_with_work(
        &self, admission: &TaskAdmissionRecord,
        access: Option<crate::resource_preparation::CreatedResourceAccess>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if let Some(access) = &access {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(|io| io.original_native_work().is_some())
                && !access.has_native_custody()
            {
                return Err(scope_metadata_boundary(work, crate::wire::ErrorCode::Unauthorized,
                    "native created resource seal requires its original payload custody"));
            }
            if access.operation != admission.operation_id
                || access.admission_digest != admission.canonical_digest_with_work(work)?
                || access.proofs != admission.resource_proofs
            {
                return Err(scope_metadata_boundary(work, crate::wire::ErrorCode::Unauthorized,
                    "created resource seal changed admission"));
            }
        } else if !admission.resource_proofs.is_empty() {
            return Err(scope_metadata_boundary(work, crate::wire::ErrorCode::Unauthorized,
                "created resource proof is not authenticated"));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
            self.validate_narrowing_with_native_work(&self.grants, &admission.limits, native)?;
        } else {
            admission.limits.validate(&mut None)?;
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        admission.limits.validate(&mut None)?;
        if !self.allows_authority(&self.grants, &admission.limits) {
            return Err(scope_metadata_boundary(work, crate::wire::ErrorCode::Unauthorized,
                "runtime scope cannot widen ancestor authority"));
        }
        for grant in admission.grants.iter() {
            if let Some(io) = work.as_deref_mut() {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            if !self.grants.contains(grant)
                && !access.as_ref().is_some_and(|access| access.grants.contains(grant))
            {
                return Err(scope_metadata_boundary(work, crate::wire::ErrorCode::Unauthorized,
                    "runtime scope cannot widen ancestor authority"));
            }
        }
        if self.extensions != admission.extensions {
            return Err(scope_metadata_boundary(work, crate::wire::ErrorCode::Conflict,
                "replay cannot retarget original extension admission"));
        }
        if let Some(extensions) = &admission.extensions { extensions.validate(work)?; }
        let mut quota = crate::wire_codec::CodecQuota::new(
            work, std::mem::size_of::<Self>() as u64,
        )?;
        let grants = crate::core::copy_capabilities(&admission.grants, &mut quota)?;
        let proofs = access.as_ref().map_or(self.resource_proofs.as_slice(), |access| access.proofs.as_slice());
        let resource_proofs = Self::copy_resource_proofs(proofs, &mut quota)?;
        let extensions = admission.extensions.as_ref()
            .map(|extensions| extensions.copy_with_quota(&mut quota)).transpose()?;
        let extension_schemas = self.extension_schemas.as_ref()
            .map(|schemas| schemas.copy(quota.work())).transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if access.is_some()
            && let Some(native) = quota.work().as_deref().and_then(|io| io.original_native_work())
        {
            native.charge(acyclic_fs::WorkCounters {
                allocation_operations: 1, ..Default::default()
            }).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        }
        let created_access = access.map(Arc::new).or_else(|| self.created_access.clone());
        let scope = Self {
            grants, limits: admission.limits, run_limits: self.run_limits,
            extensions, extensions_sealed: true,
            extension_runtime: self.extension_runtime.clone(), extension_schemas,
            resource_proofs, created_access,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _native_grants_custody: None,
        };
        let scope = scope.with_run_limits(admission.run_limits, quota.work())?;
        if let Some(runtime) = &scope.extension_runtime {
            runtime.validate_admission(scope.extensions.as_ref(), quota.work())?;
            if let Some(schemas) = &scope.extension_schemas {
                for identity in runtime.selected() {
                    if schemas.implementation_digest(
                        &identity.name, identity.version, quota.work(),
                    )? != identity.digest {
                        return Err(scope_metadata_boundary(quota.work(), crate::wire::ErrorCode::Conflict,
                            "linked implementation differs from original admission registry"));
                    }
                }
            }
        }
        let held = if quota.work().is_some() { quota.retained() } else { 0 };
        quota.transfer();
        Ok((scope, held))
    }

    fn resume_sealed(&self, admitted: Self, operation: OperationId) -> Result<Self> {
        let mut permitted = self.clone();
        if let Some(access) = &admitted.created_access {
            if access.operation != operation || access.proofs != admitted.resource_proofs {
                return Err(Error::Unauthorized("created resource seal belongs to another task".into()));
            }
            permitted.grants = Capabilities::new(
                self.grants.iter().chain(access.grants.iter()),
            );
            permitted.resource_proofs = admitted.resource_proofs.clone();
            permitted.created_access = admitted.created_access.clone();
        } else if !admitted.resource_proofs.is_empty() {
            return Err(Error::Unauthorized("unsealed created resource scope".into()));
        }
        permitted.narrow(admitted.grants, admitted.limits)?
            .with_run_limits(admitted.run_limits, &mut None)?
            .with_replayed_extensions(admitted.extensions)
    }

    pub(crate) async fn current_creation_parent_fence(&self, parent: TaskId) -> Result<crate::scheduler::LeaseFence> {
        let access = self.created_access.as_ref().ok_or_else(|| {
            Error::Unauthorized("created child has no inherited parent seal".into())
        })?;
        if access.operation.into_bytes() != parent.into_bytes() {
            return Err(Error::Unauthorized("created child seal belongs to another parent".into()));
        }
        let lease = access.publication_lease.as_ref().ok_or_else(|| {
            Error::Unauthorized("created child parent lease is not bound".into())
        })?;
        lease.verify().await?;
        lease.fence()
    }

    /// Returns the actor authenticated by the scratch allocation.
    pub(crate) fn current_created_agent(
        &self, work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<Option<crate::AgentId>> {
        self.current_created_private_volume(work)?.map(|volume| match volume.owner() {
            crate::conversation::VolumeOwner::Agent(agent) => Ok(*agent),
            _ => Err(scope_metadata_boundary(work, crate::wire::ErrorCode::Unauthorized,
                "current created scratch is not agent-owned")),
        }).transpose()
    }

    pub(crate) fn current_created_private_volume(
        &self, work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<Option<&VolumeRef>> {
        if let Some(io) = work.as_deref() { io.check_cancellation()?; }
        Ok(self.created_access.as_ref().map(|access| access.current_content(work))
            .transpose()?.flatten().map(|binding| &binding.volume))
    }

    pub(crate) fn current_created_authority(
        &self, work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<Option<&crate::core::Authority>> {
        if let Some(io) = work.as_deref() { io.check_cancellation()?; }
        Ok(self.created_access.as_ref().map(|access| access.current_content(work))
            .transpose()?.flatten().and_then(|binding| binding.authority.as_ref()))
    }

    pub(crate) fn created_grants(&self) -> Option<&Capabilities> {
        self.created_access.as_ref().map(|access| &access.grants)
    }

    pub(crate) fn with_created_publication_lease(
        mut self, lease: Arc<dyn crate::resource_preparation::CreatedTaskLease>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if let Some(access) = &mut self.created_access {
            if access.publication_lease.is_some() { return Ok(self); }
            let access = Arc::get_mut(access).ok_or_else(|| crate::store::guarded_boundary(work, crate::wire::ErrorCode::Invalid, "created publication seal is already shared",
            ))?;
            access.publication_lease = Some(lease);
        }
        Ok(self)
    }

    async fn verify_created_write(&self, volume: &VolumeRef, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        if self.created_content(Some(volume)).is_some() {
            let lease = self.created_access.as_ref()
                .and_then(|access| access.publication_lease.as_ref())
                .ok_or_else(|| Error::Unauthorized("created resource publication lease is not bound".into()))?;
            #[cfg(feature = "filesystem")]
            if let Some(io) = io { return io.verify_publication_lease(lease.as_ref()).await; }
            lease.verify().await?;
        }
        let _ = io;
        Ok(())
    }

    pub(crate) fn created_content(&self, volume: Option<&VolumeRef>) -> Option<&ContentBindings> {
        self.created_access.as_ref()?.content.iter().find(|binding| {
            volume.map_or(binding.default_writer, |volume| &binding.volume == volume)
        }).map(|binding| &binding.binding)
    }

    fn created_artifact_content(&self) -> Option<&ContentBindings> {
        self.created_access.as_ref()?.content.iter().find(|binding| binding.artifact_writer)
            .map(|binding| &binding.binding)
    }

    /// Finds only an exact provider-authenticated, owner-consumed reference.
    pub(crate) fn created_reference(
        &self,
        file: &FileRef,
    ) -> Option<&Arc<dyn crate::conversation::ContentResidencyVerifier>> {
        self.created_access.as_ref()?.references.iter()
            .find(|binding| &binding.file == file)
            .map(|binding| &binding.reader)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn created_access(&self) -> Option<Arc<crate::resource_preparation::CreatedResourceAccess>> {
        self.created_access.clone()
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
        extensions: Option<Vec<crate::extension::ExtensionLease>>,
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

/// An impossible quorum retains both partial successes and non-successes.
pub struct QuorumFailure<O> {
    /// Caller-specified threshold that was not met.
    pub required: usize,
    /// Successful values with their task IDs, in completion order.
    pub successes: Vec<(String, O)>,
    /// Failed, cancelled, uncertain, or unobservable members.
    pub others: Vec<(String, Result<Outcome<O>>)>,
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
                    validate_value(&output_schema, &value, "task output", None)?;
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
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_configuration_custody: Option<acyclic_fs::NativeAllocation>,
}

impl Clone for AgentHarness {
    fn clone(&self) -> Self {
        Self {
            tasks: self.tasks.clone(), tools: self.tools.clone(),
            resumable_tools: self.resumable_tools.clone(), live: self.live.clone(),
            concurrency: self.concurrency, scope: self.scope.clone(),
            extension_runtime: self.extension_runtime.clone(), host: self.host.clone(),
            state: self.state.clone(), spawner: self.spawner.clone(),
            execution: self.execution.clone(), interactions: self.interactions.clone(),
            interaction_resolver: self.interaction_resolver.clone(), content: self.content.clone(),
            artifacts: self.artifacts.clone(), policy: self.policy.clone(),
            policy_identity: self.policy_identity.clone(), model: self.model.clone(),
            context: self.context.clone(), fork_preparer: self.fork_preparer.clone(),
            workspaces: self.workspaces.clone(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_configuration_custody: None,
        }
    }
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
pub trait ToolPolicy: acyclic_stream::ProviderPlatform {
    /// Immutable implementation identity pinned across admission and replay.
    fn identity(&self) -> ComponentIdentity;
    /// Policy implementations must be deterministic for an admitted revision.
    fn evaluate<'a>(
        &'a self,
        invocation: &'a ToolInvocation,
        scope: &'a RuntimeScope,
    ) -> BoxFuture<'a, Result<ToolPolicyDecision>>;

    /// Rejects opaque policies before any native callback or future allocation.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_policy(&self, _work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "policy has no original-ledger implementation",
        })
    }

    /// Returns the owned identity with custody for its inline Self and strings.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn identity_with_native_work(
        &self, _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ComponentIdentity, acyclic_fs::NativeAllocation)> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "policy has no original-ledger identity",
        })
    }

    /// Evaluates the same predicate against the originally issued scope.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn evaluate_with_native_work<'a>(
        &'a self,
        _invocation: &'a ToolInvocation,
        _scope: &'a RuntimeScope,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolPolicyDecision, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "policy has no original-ledger evaluator",
        })
    }
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

/// Provider capabilities visible to task dependency admission.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TaskDependencyEnvironment {
    /// A model provider is bound for this composition.
    pub model: bool,
    /// A context builder is bound for this composition.
    pub context: bool,
    /// An interaction router is bound for this composition.
    pub interactions: bool,
    /// A policy provider is bound for this composition.
    pub policy: bool,
    /// A durable host is bound for this composition.
    pub host: bool,
    /// Owner-retained task state is bound for this composition.
    pub state: bool,
    /// Durable admission is bound for this composition.
    pub spawner: bool,
    /// Owner-retained content is bound for this composition.
    pub content: bool,
    /// Owner-retained artifacts are bound for this composition.
    pub artifacts: bool,
    /// The content writer and its capability grant are both available.
    pub content_write: bool,
    /// The artifact writer and its capability grant are both available.
    pub artifacts_write: bool,
    /// Exact extension versions selected for this composition.
    pub extensions: BTreeSet<(String, u32)>,
}

/// Validates an immutable task dependency graph and its provider requirements.
pub(crate) fn validate_task_requirements(
    tasks: &BTreeMap<(String, String), BTreeSet<String>>,
    tools: &BTreeSet<(String, String)>, environment: &TaskDependencyEnvironment,
    grants: &BTreeSet<String>, io: StartupIo<'_>,
) -> Result<()> {
    let mut marks = BTreeMap::new();
    for key in tasks.keys() {
        visit_task_dependency(key, &mut marks, tasks, tools, environment, grants, io)?;
    }
    Ok(())
}

fn visit_task_dependency(
    key: &(String, String), marks: &mut BTreeMap<(String, String), u8>,
    tasks: &BTreeMap<(String, String), BTreeSet<String>>, tools: &BTreeSet<(String, String)>,
    environment: &TaskDependencyEnvironment, grants: &BTreeSet<String>, io: StartupIo<'_>,
) -> Result<()> {
    native_startup::item(io, 0)?;
    match marks.get(key).copied() {
        Some(1) => {
            return Err(native_startup::error(io, crate::wire::ErrorCode::Invalid,
                "task dependency cycle"));
        }
        Some(2) => return Ok(()),
        _ => {}
    }
    let requirements = tasks.get(key).ok_or_else(|| {
        native_startup::error(io, crate::wire::ErrorCode::Invalid,
            "unsatisfied task requirement")
    })?;
    native_startup::item(io, std::mem::size_of::<((String, String), u8)>())?;
    marks.insert((native_startup::text(&key.0, io)?, native_startup::text(&key.1, io)?), 1);
    for requirement in requirements {
        validate_requirement(requirement, marks, tasks, tools, environment, grants, io)?;
    }
    *marks.get_mut(key).expect("the original task was marked before walking its dependencies") = 2;
    Ok(())
}

fn validate_requirement(
    requirement: &str, marks: &mut BTreeMap<(String, String), u8>,
    tasks: &BTreeMap<(String, String), BTreeSet<String>>, tools: &BTreeSet<(String, String)>,
    environment: &TaskDependencyEnvironment, grants: &BTreeSet<String>, io: StartupIo<'_>,
) -> Result<()> {
    native_startup::item(io, 0)?;
    if let Some(target) = parse_dependency_target(requirement, "task:", io)? {
        if !tasks.contains_key(&target) {
            return Err(native_startup::error(io, crate::wire::ErrorCode::Invalid,
                "unsatisfied task requirement"));
        }
        return visit_task_dependency(&target, marks, tasks, tools, environment, grants, io);
    }
    if let Some(target) = parse_dependency_target(requirement, "tool:", io)? {
        return tools.contains(&target).then_some(()).ok_or_else(|| {
            native_startup::error(io, crate::wire::ErrorCode::Invalid,
                "unsatisfied tool requirement")
        });
    }
    if provider_requirement_satisfied(requirement, environment, grants, io)? {
        return Ok(());
    }
    Err(native_startup::error(io, crate::wire::ErrorCode::Invalid,
        "unsatisfied task requirement"))
}

fn provider_requirement_satisfied(
    requirement: &str, environment: &TaskDependencyEnvironment, grants: &BTreeSet<String>,
    io: StartupIo<'_>,
) -> Result<bool> {
    let available = match requirement {
        "model" => Some(environment.model),
        "context" => Some(environment.context),
        "interactions" => Some(environment.interactions),
        "policy" => Some(environment.policy),
        "host" => Some(environment.host),
        "state" => Some(environment.state),
        "spawner" => Some(environment.spawner),
        "content" => Some(environment.content),
        "artifacts" => Some(environment.artifacts),
        "content:write" => Some(environment.content_write),
        "artifacts:write" => Some(environment.artifacts_write),
        _ => None,
    };
    if let Some(available) = available {
        return if available {
            Ok(true)
        } else {
            Err(native_startup::error(io, crate::wire::ErrorCode::Invalid,
                "unsatisfied task requirement"))
        };
    }
    if let Some(grant) = requirement.strip_prefix("grant:") {
        return Ok(!grant.is_empty() && grants.contains(grant));
    }
    if let Some(extension) = requirement.strip_prefix("extension:") {
        let (name, version) = extension.rsplit_once('@').ok_or_else(|| {
            native_startup::error(io, crate::wire::ErrorCode::Invalid,
                "invalid extension dependency")
        })?;
        let version = version.parse::<u32>().map_err(|_| {
            native_startup::error(io, crate::wire::ErrorCode::Invalid,
                "invalid extension dependency")
        })?;
        return Ok(!name.is_empty()
            && !name.contains('@')
            && environment
                .extensions
                .contains(&(native_startup::text(name, io)?, version)));
    }
    Ok(false)
}


fn parse_dependency_target(
    requirement: &str, prefix: &str, io: StartupIo<'_>,
) -> Result<Option<(String, String)>> {
    let Some(value) = requirement.strip_prefix(prefix) else {
        return Ok(None);
    };
    let target = value
        .rsplit_once('@')
        .filter(|(name, version)| !name.is_empty() && !version.is_empty() && !name.contains('@'))
        .ok_or_else(|| native_startup::error(io, crate::wire::ErrorCode::Invalid,
            "invalid task dependency"))?;
    Ok(Some((native_startup::text(target.0, io)?, native_startup::text(target.1, io)?)))
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

/// Binds typed and stock tool approvals to the same runtime-owned action.
pub(crate) fn tool_approval_request(
    task: TaskId,
    policy: &ComponentIdentity,
    definition: &ToolDefinition,
    invocation: &ToolInvocation,
    prompt: String,
) -> Result<(OperationId, Interaction)> {
    let (operation, digest) = tool_approval_binding(task, policy, definition, invocation)?;
    Ok((
        operation,
        Interaction::approval(prompt, invocation.operation_id, digest)?,
    ))
}

/// Moves an already-paid policy prompt into the exact canonical approval action.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn tool_approval_request_with_native_work(
    task: TaskId,
    policy: &ComponentIdentity,
    definition: &ToolDefinition,
    invocation: &ToolInvocation,
    prompt: String,
    prompt_custody: acyclic_fs::NativeAllocation,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<((OperationId, Interaction), acyclic_fs::NativeAllocation)> {
    let paid = (prompt, prompt_custody);
    if !paid.1.work().shares_ledger(work)
        || paid.1.bytes() < paid.0.capacity() as u64
    {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
            reason: "approval prompt does not retain its original ledger" });
    }
    work.remaining()?;
    if paid.0.trim().is_empty() {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "approval prompt is empty" });
    }
    let (operation, digest) = tool_approval_binding_with_native_work(task, policy, definition, invocation, work)?;
    if digest == [0; 32] {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "approval action digest is empty" });
    }
    let (prompt, custody) = paid;
    Ok(((operation, Interaction::Approval {
        prompt, operation_id: invocation.operation_id, action_digest: digest,
    }), custody))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
type NativeWork = acyclic_fs::NativeMaterializationWork;
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
type NativeWork = ();

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn validate_native_policy_identity(
    paid: &(ComponentIdentity, Option<NativeCustody>), work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let custody = paid.1.as_ref().ok_or(Error::NativeBoundary {
        code: crate::wire::ErrorCode::Conflict,
        reason: "policy identity does not retain its original ledger",
    })?;
    validate_policy_identity_custody_with_native_work(&paid.0, custody, work)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn validate_policy_identity_custody_with_native_work(
    identity: &ComponentIdentity, custody: &acyclic_fs::NativeAllocation,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let bytes = (std::mem::size_of::<ComponentIdentity>() as u64)
        .checked_add(identity.name.capacity() as u64)
        .and_then(|bytes| bytes.checked_add(identity.version.capacity() as u64));
    if !custody.work().shares_ledger(work) || bytes.is_none_or(|bytes| custody.bytes() < bytes) {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
            reason: "policy identity does not retain its original ledger" });
    }
    work.remaining()?;
    Ok(())
}

/// Shared SDK startup boundary for already-owned policy metadata.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn validate_policy_identity_with_native_work(
    identity: &ComponentIdentity, custody: &acyclic_fs::NativeAllocation,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    validate_policy_identity_custody_with_native_work(identity, custody, work)?;
    if !crate::contract::is_valid_component_label(&identity.name) {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "policy name is invalid" });
    }
    if !crate::contract::is_valid_component_label(&identity.version) {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "policy version is invalid" });
    }
    if identity.digest == [0; 32] {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "policy implementation digest is empty" });
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn validate_native_policy_decision(
    paid: &(ToolPolicyDecision, Option<NativeCustody>), work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let bytes = match &paid.0 {
        ToolPolicyDecision::Allow => 0,
        ToolPolicyDecision::Deny { reason } => reason.capacity() as u64,
        ToolPolicyDecision::RequireApproval { prompt } => prompt.capacity() as u64,
    };
    if !paid.1.as_ref().is_some_and(|custody| custody.work().shares_ledger(work)
        && custody.bytes() >= bytes)
    {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
            reason: "policy decision does not retain its original ledger" });
    }
    work.remaining()?;
    Ok(())
}

/// Derives retained approval authority without evaluating or dispatching a policy.
pub(crate) fn tool_approval_binding(
    task: TaskId,
    policy: &ComponentIdentity,
    definition: &ToolDefinition,
    invocation: &ToolInvocation,
) -> Result<(OperationId, [u8; 32])> {
    tool_approval_binding_with_work(task, policy, definition, invocation, None)
}

/// Canonical passive wake identity: no policy evaluation or prompt construction.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn tool_approval_binding_with_native_work(
    task: TaskId, policy: &ComponentIdentity, definition: &ToolDefinition,
    invocation: &ToolInvocation, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(OperationId, [u8; 32])> {
    tool_approval_binding_with_work(task, policy, definition, invocation, Some(work))
}

fn tool_approval_binding_with_work(
    task: TaskId, policy: &ComponentIdentity, definition: &ToolDefinition,
    invocation: &ToolInvocation, work: Option<&NativeWork>,
) -> Result<(OperationId, [u8; 32])> {
    let digest = match work {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        Some(work) => {
            work.remaining()?;
            if !crate::contract::is_valid_component_label(&policy.name)
                || !crate::contract::is_valid_component_label(&policy.version)
                || policy.digest == [0; 32]
                || !crate::contract::is_valid_component_label(&invocation.name)
                || invocation.call_id.is_empty()
                || invocation.call_id.len() > crate::COMPONENT_LABEL_MAX_BYTES
                || invocation.call_id.chars().any(char::is_control)
                || invocation.call_id.contains(crate::contract::COMPONENT_LABEL_FORBIDDEN_SEPARATORS)
                || crate::contract::COMPONENT_LABEL_FORBIDDEN_EXACT.contains(&invocation.call_id.as_str())
            {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "invalid tool approval identity" });
            }
            let mut io = crate::store::history::native_work_io(work)?;
            let mut observed: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut io);
            crate::contract::canonical_json_digest_with_work(
                &(task, policy, definition, invocation), work.remaining()?.bytes_encoded, &mut observed,
            )?
        }
        _ => {
            validate_policy_identity(policy)?;
            ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), &mut None)?;
            crate::contract::canonical_json_digest(&(task, policy, definition, invocation))?
        }
    };
    let prefix = b"harness:tool-approval:v2";
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters {
            bytes_hashed: (prefix.len() + digest.len()) as u64, ..Default::default()
        })?;
    }
    let mut hash = blake3::Hasher::new();
    hash.update(prefix);
    hash.update(&digest);
    let mut id_bytes = [0; 16];
    id_bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    Ok((OperationId::from_bytes(id_bytes), digest))
}

/// Provider-owned, scope-bound file handles.
#[derive(Clone)]
pub struct ContentBindings {
    /// Authenticated, exact-version read and residency boundary.
    pub reader: Arc<dyn ContentResidencyVerifier>,
    /// Authenticated writer for one volume; read-only agents omit it.
    pub writer: Option<Arc<dyn ContentPublisher>>,
}

/// Artifact access uses immutable file references.
pub type ArtifactBindings = ContentBindings;

/// Immutable admission bindings assembled before tasks can run.
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
    /// Qualified route packaging a compatible task build, spawner, and state owner.
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
    #[must_use]
    pub fn local() -> Self {
        Self {
            tasks: TaskRegistry::default(),
            tools: ToolRegistry::default(),
            resumable_tools: ResumableToolRegistry::default(),
            scope: RuntimeScope::default(),
            concurrency: tokio::sync::Semaphore::MAX_PERMITS,
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
    #[must_use]
    pub fn execution(mut self, value: Arc<dyn ExecutionProvider>) -> Self {
        self.execution = Some(value);
        self
    }

    /// Validates all dependency edges and seals the runtime composition.
    pub fn build(self) -> Result<Arc<AgentHarness>> {
        if self.fork_preparer.is_some() && !self.scope.grants().contains(capability::FORK_PUBLISH) {
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
        let host_available = self.durable_host.is_some();
        let state_available = host_available || state.is_some();
        let spawner_available = host_available || spawner.is_some();
        let durable_ready = host_available || (state_available && spawner_available);
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
            state_available,
            spawner_available,
            None,
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

pub(crate) fn validate_children_request(
    parent: TaskId,
    after_slot: Option<&str>,
    maximum: usize,
) -> Result<()> {
    validate_child_page_request("task child", parent.into_bytes(), after_slot, maximum)
}

pub(crate) fn validate_children_page(
    page: &TaskChildrenPage,
    expected_revision: Option<u64>,
    after_slot: Option<&str>,
    maximum: usize,
) -> Result<()> {
    validate_child_page(
        "task child",
        (page.revision, page.next_after.as_deref()),
        page.entries
            .iter()
            .map(|child| (child.slot.as_str(), child.task_id.into_bytes())),
        (expected_revision, after_slot, maximum),
    )
}

/// Bounds one direct-child page request of a task or operation hierarchy.
pub(crate) fn validate_child_page_request(
    label: &str,
    parent: [u8; 16],
    after_slot: Option<&str>,
    maximum: usize,
) -> Result<()> {
    if parent == [0; 16]
        || maximum == 0
        || after_slot.is_some_and(|slot| slot.chars().any(char::is_control))
    {
        return Err(Error::Invalid(format!("{label} page request is invalid")));
    }
    Ok(())
}

/// Validates a pinned child page against its requested cursor and bounds.
pub(crate) fn validate_child_page<'a>(
    label: &str,
    (revision, next_after): (u64, Option<&str>),
    entries: impl Iterator<Item = (&'a str, [u8; 16])>,
    (expected_revision, after_slot, maximum): (Option<u64>, Option<&str>, usize),
) -> Result<()> {
    let mut previous = after_slot;
    let mut last = None;
    let mut ids = BTreeSet::new();
    for (slot, id) in entries {
        if id == [0; 16]
            || slot.trim().is_empty()
            || slot.chars().any(char::is_control)
            || previous.is_some_and(|previous| slot <= previous)
            || !ids.insert(id)
        {
            return Err(Error::Invalid(format!(
                "{label} entries are not in stable slot order"
            )));
        }
        previous = Some(slot);
        last = Some(slot);
    }
    if expected_revision.is_some_and(|expected| expected != revision)
        || ids.len() > maximum
        || next_after.is_some_and(|next| last != Some(next))
    {
        return Err(Error::Invalid(format!(
            "{label} page does not match its request"
        )));
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) type NativeCustody = acyclic_fs::NativeAllocation;
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
pub(crate) type NativeCustody = ();

enum ContextSource<'a> {
    Ordinary(std::marker::PhantomData<&'a ()>),
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Native {
        work: &'a acyclic_fs::NativeMaterializationWork,
        purpose: NativeContextPurpose<'a>,
    },
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
enum NativeContextPurpose<'a> {
    Read,
    Dispatch {
        fence: &'a crate::scheduler::LeaseFence,
        mailbox: Arc<dyn OwnedTaskMailbox>,
    },
    Reconciliation {
        fence: &'a crate::scheduler::LeaseFence,
        mailbox: Arc<dyn OwnedTaskMailbox>,
    },
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum IssuedContextPurpose {
    Read,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Dispatch,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Reconciliation,
}

impl AgentHarness {
    async fn validate_observed_admission(
        &self, observed: &TaskAdmissionRecord, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = task_original_work(io)? {
            let mut observer = crate::store::history::native_work_io(work)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            observed.validate(&mut Some(&mut observer))?;
            self.tasks.validate_durable_admission(
                &observed.task, &observed.machine, Some(&observed.input_schema),
                &observed.output_schema, Some(&observed.input), &mut Some(&mut observer),
            )?;
            if observed.resource_proofs.is_empty() {
                self.scope.validate_narrowing_with_native_work(&observed.grants, &observed.limits, work)?;
                self.scope.validate_run_limit_attenuation(&observed.run_limits, &mut Some(&mut crate::store::native_work_io(work).map_err(|cause| crate::Error::NativeWork { cause, custody: None })?))?;
            } else {
                let state = self.state.as_ref().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "resource-derived admission state is not bound",
                })?;
                let admitted = state.resume_scope_with_native_work(
                    observed.task_id(), observed.operation_id, work,
                )?.await?;
                if !admitted.1.matches_work(work) {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                        reason: "resumed scope differs from its original metadata ledger" });
                }
                let observed_digest = observed.canonical_digest_with_work(&mut Some(&mut observer))?;
                if admitted.0.created_access.as_ref().is_none_or(|access|
                    access.admission_digest != observed_digest)
                {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                        reason: "resource scope seal differs from observed admission" });
                }
                self.validate_native_resumed_scope(&admitted.0, observed.operation_id, work)?;
            }
            if observed.policy != self.policy_identity
                || observed.extensions.as_ref() != self.scope.extensions()
            {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "retained admission changed owner bindings" });
            }
            return match (&self.execution, observed.execution.as_ref()) {
                (Some(provider), Some(placement)) if &placement.provider == provider.identity_with_native_work(work)? => Ok(()),
                (None, None) => Ok(()),
                _ => Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "retained execution route differs from runtime" }),
            };
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = io;
        observed.validate(&mut None)?;
        self.tasks.validate_durable_admission(
            &observed.task,
            &observed.machine,
            Some(&observed.input_schema),
            &observed.output_schema,
            Some(&observed.input),
            &mut None,
        )?;
        if observed.resource_proofs.is_empty() {
            self.scope.narrow(observed.grants.clone(), observed.limits)?
                .with_run_limits(observed.run_limits, &mut None)?;
        } else {
            let state = self.state.as_ref().ok_or_else(|| {
                Error::Unsupported("resource-derived admission state is not bound".into())
            })?;
            let admitted = state.resume_scope(
                observed.task_id(), observed.operation_id,
            ).await?;
            let observed_digest = crate::contract::canonical_json_digest(
                &observed.canonical_value(),
            )?;
            if admitted.created_access.as_ref().is_none_or(|access| {
                access.admission_digest != observed_digest
            }) {
                return Err(Error::Unauthorized("resource scope seal differs from observed admission".into()));
            }
            self.scope.resume_sealed(admitted, observed.operation_id)?;
        }
        if observed.policy != self.policy_identity
            || observed.extensions.as_ref() != self.scope.extensions()
        {
            return Err(Error::Conflict(
                "retained admission changed owner bindings".into(),
            ));
        }
        match (&self.execution, observed.execution.as_ref()) {
            (Some(provider), Some(placement)) if placement.provider == provider.identity() => {
                placement.validate(&mut None)?;
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
        self.validate_observed_admission(&observed, None).await?;
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

    fn assert_durable_policy_bindings(&self, work: Option<&NativeWork>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.charge(acyclic_fs::WorkCounters::default())
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        }
        macro_rules! identity {
            ($provider:expr, $ordinary:ident, $native:ident) => {{
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                {
                    if let Some(work) = work {
                        $provider.$native(work)?.map(std::borrow::Cow::Borrowed)
                    } else {
                        $provider.$ordinary().map(std::borrow::Cow::Owned)
                    }
                }
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                { $provider.$ordinary().map(std::borrow::Cow::Owned) }
            }};
        }
        let mut mismatch = false;
        if let Some(host) = &self.host {
            mismatch = identity!(host, policy_identity, policy_identity_with_native_work).as_deref()
                != self.policy_identity.as_ref();
        }
        if !mismatch && let Some(state) = &self.state {
            mismatch = identity!(state, policy_identity, policy_identity_with_native_work).as_deref()
                != self.policy_identity.as_ref();
        }
        if !mismatch && let Some(spawner) = &self.spawner {
            mismatch = identity!(spawner, policy_identity, policy_identity_with_native_work).as_deref()
                != self.policy_identity.as_ref();
        }
        if !mismatch && let Some(execution) = &self.execution {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let current = match work {
                Some(work) => std::borrow::Cow::Borrowed(execution.identity_with_native_work(work)?),
                None => std::borrow::Cow::Owned(execution.identity()),
            };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let current: std::borrow::Cow<'_, ComponentIdentity> = std::borrow::Cow::Owned(execution.identity());
            mismatch = match &self.state {
                Some(state) => identity!(state, execution_identity, execution_identity_with_native_work).as_deref() != Some(current.as_ref()),
                None => true,
            };
            if !mismatch {
                mismatch = match &self.spawner {
                    Some(spawner) => identity!(spawner, execution_identity, execution_identity_with_native_work).as_deref() != Some(current.as_ref()),
                    None => true,
                };
            }
        }
        if mismatch {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "durable provider binding changed after native source configuration" });
            }
            return Err(Error::Conflict("durable policy implementation changed after binding".into()));
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = work;
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn original_host_binding_matches(
        &self, host: &dyn DurableTaskHost, policy: Option<&dyn ToolPolicy>,
    ) -> bool {
        self.host.as_ref().is_some_and(|bound| std::ptr::eq(bound.as_ref(), host))
            && match (self.policy.as_deref(), policy) {
                (Some(bound), Some(policy)) => std::ptr::eq(bound, policy),
                (None, None) => true,
                _ => false,
            }
    }

    async fn verify_spawner_admission(
        &self,
        task_id: TaskId,
        expected: &TaskAdmissionRecord,
    ) -> Result<()> {
        self.assert_durable_policy_bindings(None)?;
        let host = self
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state host is not bound".into()))?;
        match host.observe_admission(task_id).await {
            Ok(observed) if observed == *expected => self.validate_observed_admission(&observed, None).await,
            Ok(_) => Err(Error::Conflict(
                "spawner and state host admission bindings differ".into(),
            )),
            Err(Error::NotFound(_)) => Err(Error::Indeterminate(expected.operation_id)),
            Err(error) => Err(error),
        }
    }

    /// Replaces only durable admission.
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
        if !self.scope.grants().contains(capability::FORK_PUBLISH)
            && !self.scope.grants().contains(capability::PROJECT_MERGE)
        {
            return Err(Error::Unauthorized(
                "project workspaces require fork:publish or project:merge".into(),
            ));
        }
        workspaces.provider().validate(&mut None)?;
        workspaces.project().validate(&mut None)?;
        if workspaces.project().class() != crate::conversation::VolumeClass::Project
            || workspaces.project().provider() != workspaces.provider()
        {
            return Err(Error::Invalid("workspace binding is not a project".into()));
        }
        let mut bound = self.as_ref().clone();
        bound.workspaces = Some(workspaces);
        Ok(Arc::new(bound))
    }

    /// Returns the authorized workspace boundary.
    pub fn workspaces(&self) -> Result<Arc<dyn crate::merge::ProjectWorkspaceProvider>> {
        if !self.scope.grants().contains(capability::FORK_PUBLISH)
            && !self.scope.grants().contains(capability::PROJECT_MERGE)
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

    /// Selects a newly captured parent revision for future work.
    pub fn at_parent_snapshot(
        self: &Arc<Self>,
        preparer: Arc<dyn crate::fork::ForkPreparer>,
    ) -> Result<Arc<Self>> {
        if !self.scope.grants().contains(capability::FORK_PUBLISH) {
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

    /// Derives an immutable application view with no more authority or resources than its parent.
    pub fn scoped(self: &Arc<Self>, grants: Capabilities, limits: Limits) -> Result<Arc<Self>> {
        let mut child = self.as_ref().clone();
        child.scope = self.scope.narrow(grants, limits)?;
        child.fork_preparer = None;
        child.workspaces = None;
        Ok(Arc::new(child))
    }

    /// Narrows the effective task execution bounds for newly admitted work.
    pub fn scoped_run_limits(self: &Arc<Self>, limits: TaskRunLimits) -> Result<Arc<Self>> {
        let mut child = self.as_ref().clone();
        child.scope = self.scope.clone().with_run_limits(limits, &mut None)?;
        child.live = TaskGroup::new(child.scope.concurrency_bound(self.concurrency), None)?;
        child.fork_preparer = None;
        child.workspaces = None;
        Ok(Arc::new(child))
    }

    /// Selects a different qualified execution route for newly admitted work under attenuated authority.
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
        routed.assert_durable_policy_bindings(None)?;
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
        Self::with_content(tasks, tools, scope, concurrency, host, None, None)
    }

    /// Binds owner-mediated content access.
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
        let host_available = host.is_some();
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
            host_available,
            host_available,
            None,
        )
    }

    /// Builds the same original runtime composition from paid configuration copies.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[allow(clippy::too_many_arguments, reason = "the original runtime provider configuration plus native work")]
    pub fn with_policy_with_native_work(
        tasks: &TaskRegistry, tools: &ToolRegistry, resumable_tools: &ResumableToolRegistry,
        scope: &RuntimeScope,
        concurrency: usize, host: Option<Arc<dyn DurableTaskHost>>,
        interactions: Option<Arc<dyn InteractionRouter>>, content: Option<ContentBindings>,
        artifacts: Option<ArtifactBindings>, policy: Option<Arc<dyn ToolPolicy>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        if let Some(policy) = &policy { policy.supports_native_policy(work)?; }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        scope.run_limits().validate_observed(Some(&io))?;
        if concurrency == 0 {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "runtime concurrency must be positive" });
        }
        let tasks = tasks.clone_with_native_work(work)?;
        let tools = tools.clone_with_native_work(work)?;
        io.absorb_native_custody(tools.1)?;
        let resumable_tools = resumable_tools.clone_with_native_work(work)?;
        io.absorb_native_custody(resumable_tools.1)?;
        let scope = scope.clone_with_native_work(work)?;
        io.absorb_native_custody(scope.1)?;
        let host_available = host.is_some();
        Self::with_policy_internal(
            tasks, tools.0, resumable_tools.0, scope.0, concurrency,
            host, interactions, content, artifacts, policy,
            host_available, host_available, host_available, Some(&io),
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
        state_available: bool,
        spawner_available: bool,
        startup: StartupIo<'_>,
    ) -> Result<Arc<Self>> {
        if concurrency == 0 {
            return Err(native_startup::error(startup, crate::wire::ErrorCode::Invalid,
                "runtime concurrency must be positive"));
        }
        scope.run_limits().validate_observed(startup)?;
        let concurrency = scope.concurrency_bound(concurrency);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native_policy_identity = if let Some(io) = startup {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported, reason: "startup requires original native work",
            })?;
            let identity = match &policy {
                Some(policy) => {
                    policy.supports_native_policy(work)?;
                    let identity = policy.identity_with_native_work(work)?;
                    validate_policy_identity_with_native_work(&identity.0, &identity.1, work)?;
                    io.absorb_native_custody(identity.1)?;
                    Some(identity.0)
                }
                None => None,
            };
            if let Some(host) = &host
                && host.policy_identity_with_native_work(work)? != identity.as_ref()
            {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "runtime and durable host policies differ" });
            }
            identity
        } else { None };
        if startup.is_none() {
            if let Some(policy) = &policy { validate_policy_identity(&policy.identity())?; }
            if host.as_ref().is_some_and(|host| {
                host.policy_identity() != policy.as_ref().map(|policy| policy.identity())
            }) {
                return Err(Error::Conflict("runtime and durable host policies differ".into()));
            }
        }
        if resumable_tools.definitions().next().is_some() && !durable_ready {
            return Err(native_startup::error(startup, crate::wire::ErrorCode::Invalid,
                "resumable tools require durable task admission and state"));
        }
        for definition in resumable_tools.definitions() {
            native_startup::item(startup, 0)?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let adapter = if let Some(io) = startup {
                tools.get_version(&definition.name, &definition.revision, Some(io))?
            } else { tools.get_version(&definition.name, &definition.revision, None)? };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let adapter = tools.get_version(&definition.name, &definition.revision, None)?;
            if let Some(adapter) = adapter {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let pinned = if let Some(io) = startup {
                    resumable_tools.get(&definition.name, &definition.revision, &mut Some(&mut io.history_io()))?
                } else { resumable_tools.get(&definition.name, &definition.revision, &mut None)? };
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                let pinned = resumable_tools.get(&definition.name, &definition.revision, &mut None)?;
                let pinned = pinned.ok_or_else(|| native_startup::error(startup,
                    crate::wire::ErrorCode::Invalid, "resumable registry differs from its definitions"))?;
                let same_adapter = adapter.definition == *definition && {
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    let same_identity = if let Some(io) = startup {
                        let work = io.original_native_work().ok_or(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Unsupported, reason: "startup requires original native work",
                        })?;
                        adapter.executor.resumable_machine_identity_with_native_work(work)?
                            == Some(pinned.machine.identity_with_native_work(work)?)
                    } else {
                        adapter.executor.resumable_machine_identity().as_ref()
                            == Some(pinned.machine.identity())
                    };
                    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                    let same_identity = adapter.executor.resumable_machine_identity().as_ref()
                        == Some(pinned.machine.identity());
                    same_identity
                };
                if same_adapter { continue; }
                return Err(native_startup::error(startup, crate::wire::ErrorCode::Conflict,
                    "live and resumable implementations differ"));
            }
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let dependency_io = startup.map(|io| {
            crate::filesystem::PreparationIo::from_native_work(io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported, reason: "startup requires original native work",
            })?)
        }).transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let dependency_observer = dependency_io.as_ref();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let dependency_observer = None;
        validate_task_dependencies(
            &tasks,
            &tools,
            &resumable_tools,
            &scope,
            durable_ready,
            state_available,
            spawner_available,
            interactions.is_some(),
            content.as_ref(),
            artifacts.as_ref(),
            policy.is_some(),
            dependency_observer,
        )?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        drop(dependency_io);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let policy_identity = if startup.is_some() { native_policy_identity }
            else { policy.as_ref().map(|policy| policy.identity()) };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let policy_identity = policy.as_ref().map(|policy| policy.identity());
        let extension_runtime = scope.extension_runtime();
        let spawner = host.as_ref().map(|host| {
            HostTaskSpawner::new_observed(Arc::clone(host), startup)
                .map(|source| Arc::new(source) as Arc<dyn TaskSpawner>)
        }).transpose()?;
        let state = host.as_ref().map(|host| Arc::clone(host) as Arc<dyn TaskStateProvider>);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let live = if let Some(io) = startup {
            TaskGroup::new(concurrency, Some(io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported, reason: "startup requires original native work",
            })?))?
        } else { TaskGroup::new(concurrency, None)? };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let live = TaskGroup::new(concurrency, None)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native_configuration_custody = if let Some(io) = startup {
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
            io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
            Some(io.take_native_custody(io.owned_bytes()?)?)
        } else { None };
        Ok(Arc::new(Self {
            tasks,
            tools,
            resumable_tools,
            live,
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
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_configuration_custody,
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
        parent.stream_path(&mut None)?;
        let mut bound = self.as_ref().clone();
        bound.fork_preparer = Some(ForkBinding {
            parent: parent.clone(),
            revision,
            preparer,
        });
        Ok(Arc::new(bound))
    }

    fn fork_binding(&self, request: &crate::fork::ForkRequest) -> Result<&ForkBinding> {
        if !self.scope.grants().contains(capability::FORK_PUBLISH) {
            return Err(Error::Unauthorized(
                "runtime scope lacks fork:publish".into(),
            ));
        }
        request.validate(&mut None)?;
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
        report.validate(&mut None)?;
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
            report.validate(&mut None)?;
            if report.request != request {
                return Err(Error::Conflict(
                    "reconciled fork changed the request".into(),
                ));
            }
        }
        Ok(report)
    }

    /// Binds a model only for scoped task/tool model requests.
    pub fn bind_model(
        self: &Arc<Self>,
        model: Model,
        provider: Arc<dyn ModelProvider>,
    ) -> Result<Arc<Self>> {
        model.validate()?;
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

    /// Selects a different model for future local child work without changing the parent's pinned task, tool, policy, or authority bindings.
    pub fn scoped_model(
        self: &Arc<Self>,
        grants: Capabilities,
        limits: Limits,
        model: Model,
        provider: Arc<dyn ModelProvider>,
    ) -> Result<Arc<Self>> {
        self.scoped(grants, limits)?.bind_model(model, provider)
    }

    /// Resolves one registered task with exact Rust input/output types.
    pub fn task<I: 'static, O: 'static>(&self, name: &str) -> Result<Arc<TaskDefinition<I, O>>> {
        self.tasks.get(name)
    }

    /// Resolves one model-visible tool definition by exact registered name.
    pub fn tool(&self, name: &str) -> Result<ToolDefinition> {
        self.tools
            .get(name, None)?
            .map(|tool| tool.definition.clone())
            .ok_or_else(|| Error::NotFound(format!("tool {name}")))
    }

    /// Verifies an already staged input or output through a bound content or artifact owner and the runtime's effective read grant.
    pub async fn verify_readable_file(&self, file: &FileRef) -> Result<()> {
        file.validate(&mut None)?;
        self.scope.limits.validate_file(file, &mut None)?;
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
            match verified_content_bytes(binding.reader.as_ref(), file, (file).descriptor().byte_length(), None).await.map(|(bytes, _)| bytes) {
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
            None,
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
        operation_id: Option<OperationId>,
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
        self.spawn_local_in_group(group, scope, policies, definition, input, operation_id)
            .await
    }

    pub(crate) async fn spawn_local_with_operation<I, O>(
        self: &Arc<Self>,
        operation_id: OperationId,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
    ) -> Result<RuntimeTask<O>>
    where
        I: Serialize + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        self.spawn_local_in_group(
            &self.live,
            self.scope.clone(),
            Vec::new(),
            definition,
            input,
            Some(operation_id),
        )
        .await
    }

    async fn spawn_local_in_group<I, O>(
        self: &Arc<Self>,
        group: &TaskGroup,
        scope: RuntimeScope,
        policies: Vec<(ComponentIdentity, Arc<dyn ToolPolicy>)>,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
        operation_id: Option<OperationId>,
    ) -> Result<RuntimeTask<O>>
    where
        I: Serialize + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        let registered = self
            .tasks
            .get_version::<I, O>(&definition.identity.name, &definition.identity.version,
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] None)?;
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
         None,)?;
        let handler = Arc::clone(handler);
        let output_schema = definition.output_schema.clone();
        let runtime = Arc::clone(self);
        let context_group = group.clone();
        let descendants = context_group.child(scope.concurrency_bound(self.concurrency), None)?;
        let (identity, receive_identity) = tokio::sync::oneshot::channel();
        let admitted = group
            .try_spawn_with_operation(operation_id.unwrap_or_default(), async move {
                let task_id = receive_identity
                    .await
                    .map_err(|_| Error::Conflict("task identity was not admitted".into()))?;
                scope.run_limits().check_deadline()?;
                let deadline = scope.run_limits().deadline_epoch_ms;
                let run = handler(
                    TaskContext {
                        harness: runtime,
                        task_id,
                        durable_task: None,
                        descendants,
                        scope,
                        policy_overrides: policies,
                        owned_interactions: None,
                        owned_mailbox: None,
                        output_content: None,
                        #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
                        native_root_output: std::sync::OnceLock::new(),
                        model_steps: Arc::new((AtomicUsize::new(0), None)),
                        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                        native_metadata_source: None,
                    },
                    input,
                );
                let output = match deadline {
                    Some(deadline) => crate::live::timeout_until(deadline, run).await??,
                    None => run.await?,
                };
                drop(extension_leases);
                validate_value(
                    &output_schema,
                    &serde_json::to_value(&output)
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                    "task output",
                 None,)?;
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

    /// Looks up a lost durable admission acknowledgement without redispatching its input or trusting a caller-selected task definition.
    pub async fn reconcile_admission(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<(TaskId, ComponentIdentity)>> {
        self.assert_durable_policy_bindings(None)?;
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
        self.validate_observed_admission(&admission, None).await?;
        let identity = admission.task.clone();
        let registered = self
            .tasks
            .0
            .get(&(identity.name.clone(), identity.version.clone()))
            .ok_or_else(|| Error::Conflict("reconciled task is not registered".into()))?;
        if registered.identity != identity || registered.workflow_definition.is_none() {
            return Err(Error::Conflict(
                "reconciled task implementation changed".into(),
            ));
        }
        match host.observe_admission(task_id).await {
            Ok(observed) if observed == admission => self.validate_observed_admission(&observed, None).await?,
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

    /// Reattaches only to an admission pinned to this registered resumable definition.
    pub async fn attach<I: 'static, O: DeserializeOwned + Send + 'static>(
        &self,
        task_id: TaskId,
        definition: &Arc<TaskDefinition<I, O>>,
    ) -> Result<RuntimeTask<O>> {
        let registered = self
            .tasks
            .get_version::<I, O>(&definition.identity.name, &definition.identity.version,
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] None)?;
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
        self.validate_observed_admission(&observed, None).await?;
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

    /// Opens an admitted registered task from its retained input and workflow.
    pub async fn open_task<I: 'static, O: 'static>(
        self: &Arc<Self>,
        task_id: TaskId,
        definition: &Arc<TaskDefinition<I, O>>,
        fence: crate::scheduler::LeaseFence,
        journal: Arc<dyn WorkflowJournal>,
    ) -> Result<ResumableTaskSession> {
        let state = self
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
        let retained = state.observe_admission(task_id).await?;
        let context = self.durable_context(task_id, retained.operation_id).await?;
        definition.open(&context, fence, journal).await
    }

    /// Builds the stock child request from the existing registered metadata.
    #[cfg(feature = "filesystem")]
    pub fn registered_child_admission(
        &self,
        operation: OperationId,
        name: &str,
        version: &str,
        input: Value,
        parent: TaskId,
        scope: &RuntimeScope,
    ) -> Result<TaskAdmissionRecord> {
        self.assert_durable_policy_bindings(None)?;
        self.tasks.registered_admission(
            operation, name, version, input, Some(parent), None, scope, &mut None,
        )
    }

    /// Builds a registered descendant request using the original guarded input.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn registered_child_admission_with_native_work(
        &self, operation: OperationId, name: &str, version: &str,
        input: (Value, acyclic_fs::NativeAllocation), parent: TaskId, scope: &RuntimeScope,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskAdmissionRecord, acyclic_fs::NativeAllocation)> {
        if !input.1.matches_work(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "child input belongs to another original metadata ledger" });
        }
        self.assert_durable_policy_bindings(Some(work))?;
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        crate::store::history::HistoryIo::absorb_native_custody(&mut io, input.1)?;
        let record = self.tasks.registered_admission(
            operation, name, version, input.0, Some(parent), None, scope, &mut Some(&mut io),
        )?;
        let bytes = (std::mem::size_of::<TaskAdmissionRecord>() as u64)
            .checked_add(record.logical_owned_bytes()?)
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native admission metadata overflows" })?;
        let custody = crate::store::history::HistoryIo::take_native_custody(&mut io, bytes)?;
        Ok((record, custody))
    }
    /// Builds a registered root admission from an already guarded native input.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn registered_root_admission_with_native_work<I, O>(
        &self, operation: OperationId, definition: &Arc<TaskDefinition<I, O>>,
        input: (Value, acyclic_fs::NativeAllocation),
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskAdmissionRecord, acyclic_fs::NativeAllocation)> {
        work.charge(Default::default())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        if !input.1.matches_work(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "root input belongs to another original resource ledger" });
        }
        self.assert_durable_policy_bindings(Some(work))?;
        if self.execution.is_some() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                reason: "native root admission requires the configured local durable host" });
        }
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        use crate::store::history::HistoryIo as _;
        let (value, custody) = input;
        io.absorb_native_custody(custody)?;
        let identity = definition.identity();
        let mut record = self.tasks.registered_admission(
            operation, &identity.name, &identity.version, value, None, Some(identity.digest),
            &self.scope, &mut Some(&mut io),
        )?;
        record.policy = match self.policy_identity.as_ref() {
            None => None,
            Some(identity) => Some(ComponentIdentity {
                name: io.clone_text(&identity.name)?,
                version: io.clone_text(&identity.version)?,
                digest: identity.digest,
            }),
        };
        record.validate(&mut Some(&mut io))?;
        let bytes = (std::mem::size_of::<TaskAdmissionRecord>() as u64)
            .checked_add(record.logical_owned_bytes()?)
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native admission metadata overflows" })?;
        let custody = io.take_native_custody(bytes)?;
        Ok((record, custody))
    }

    /// Reopens an admitted task through its exact registered definition without requiring the worker to know its Rust input/output types.
    pub async fn open_registered_task(
        self: &Arc<Self>,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
        journal: Arc<dyn WorkflowJournal>,
    ) -> Result<ResumableTaskSession> {
        self.open_registered_task_body(
            task_id, RegisteredTaskOpening::Ordinary(fence), journal, None,
        ).await
    }

    /// Reopens the actual registered definition on the original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn open_registered_task_with_native_work(
        self: &Arc<Self>, task_id: TaskId, fence: &crate::scheduler::LeaseFence,
        journal: Arc<dyn WorkflowJournal>, context: Arc<NativeTaskContext>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<ResumableTaskSession> {
        if !context.native_metadata_matches(work)
            || !Arc::ptr_eq(&context.context().harness, self)
            || !context.context().original_task_binding_matches(task_id, fence)
            || journal.task_binding() != Some((task_id, fence))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native workflow context or journal owner differs",
            });
        }
        if !journal.supports_native_work() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "workflow journal has no native callbacks",
            });
        }
        let identity = journal.original_machine_identity().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native workflow journal has no original admitted identity",
        })?;
        let entry = self.tasks.0.iter().find_map(|((name, version), entry)| {
            (name == &identity.name && version == &identity.version
                && entry.machine.as_ref() == Some(identity)).then_some(entry)
        }).ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "original native task definition is unavailable",
        })?;
        if entry.native_custody.is_none() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "registered task metadata lacks native custody",
            });
        }
        let definition = entry.workflow_definition.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "live tasks have no resumable workflow",
        })?;
        definition.require_native_identity(work)?;
        let opening = RegisteredTaskOpening::Native {
            fence, context, definition: definition.clone(),
        };
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let result = self.open_registered_task_body(task_id, opening, journal, Some(&io)).await;
        let (session, _temporary_custody) = crate::workflow::finish_native_io(result, io, 0)?;
        Ok(session)
    }

    async fn open_registered_task_body(
        self: &Arc<Self>, task_id: TaskId, opening: RegisteredTaskOpening<'_>,
        journal: Arc<dyn WorkflowJournal>, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<ResumableTaskSession> {
        let native = task_original_work(io)?;
        let state = self.state.as_ref().ok_or_else(|| {
            task_workflow_error(native, crate::wire::ErrorCode::Unsupported,
                "durable task state is not bound")
        })?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let retained = if let Some(work) = native {
            let (value, custody) = state.observe_admission_with_native_work(task_id, work)?.await?;
            native_task_open_value(value, custody, work)?
        } else {
            (state.observe_admission(task_id).await?, None)
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let retained = (state.observe_admission(task_id).await?, None::<NativeCustody>);
        self.validate_observed_admission(&retained.0, io).await?;
        let entry = if io.is_some() {
            self.tasks.0.iter().find_map(|((name, version), entry)| {
                (name == &retained.0.task.name && version == &retained.0.task.version)
                    .then_some(entry)
            })
        } else {
            self.tasks.0.get(&(retained.0.task.name.clone(), retained.0.task.version.clone()))
        }.ok_or_else(|| task_workflow_error(native, crate::wire::ErrorCode::NotFound,
            "admitted task definition"))?;
        let definition = entry.workflow_definition.as_ref().ok_or_else(|| {
            task_workflow_error(native, crate::wire::ErrorCode::Unsupported,
                "live tasks have no resumable workflow")
        })?;
        match opening {
            RegisteredTaskOpening::Ordinary(fence) => {
                let definition = definition.clone();
                let context = self.durable_context(task_id, retained.0.operation_id).await?;
                definition.open_workflow(context, fence, journal).await
            }
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            RegisteredTaskOpening::Native { fence, context, definition: selected } => {
                if !Arc::ptr_eq(definition, &selected) {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Conflict,
                        reason: "admitted native task definition changed",
                    });
                }
                let work = task_original_work(io)?.ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "native registered opening requires original work",
                })?;
                selected.open_workflow_with_native_work(context, fence, journal, work)?.await
            }
        }
    }

    /// Discovers direct durable children from their owner, not from fork or conversation history.
    pub async fn children(
        &self,
        parent: TaskId,
        expected_revision: Option<u64>,
        after_slot: Option<String>,
        maximum: usize,
    ) -> Result<TaskChildrenPage> {
        self.assert_durable_policy_bindings(None)?;
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
        self.assert_durable_policy_bindings(None)?;
        let registered = self
            .tasks
            .get_version::<I, O>(&definition.identity.name, &definition.identity.version,
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] None)?;
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
        validate_value(&definition.input_schema, &value, "task input", None)?;
        let mut expected = TaskAdmissionRecord::from_parts(
            operation_id,
            &definition.identity.name,
            &definition.identity.version,
            value,
            definition.input_schema.clone(),
            definition.output_schema.clone(),
            &definition.requirements,
            &machine.identity().digest,
            parent,
            scope.grants().clone(),
            scope.limits(),
            scope.run_limits(),
            self.policy_identity.clone(),
            scope.extensions().cloned(),
            None,
        )?.with_resource_proofs(scope.resource_proofs().to_vec())?;
        if let Some(admission) = self
            .reconcile_scoped_admission(operation_id, definition, &expected, &scope, host, spawner)
            .await?
        {
            return Ok(admission);
        }
        if let Some(execution) = &self.execution {
            let placement = execution.qualify(&expected).await?;
            placement.validate(&mut None)?;
            if placement.provider != execution.identity() {
                return Err(Error::Conflict(
                    "execution qualifier returned another provider".into(),
                ));
            }
            expected.execution = Some(placement);
        }
        let extension_leases = self.retain_task_extensions(definition, &scope)?;
        let admitted = if expected.resource_proofs.is_empty() {
            spawner.admit(expected.clone()).await?
        } else {
            let parent = parent.ok_or_else(|| {
                Error::Unauthorized("created child admission has no parent".into())
            })?;
            let fence = scope.current_creation_parent_fence(parent).await?;
            spawner.admit_owned(expected.clone(), fence).await?
        };
        Ok(match admitted {
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

    /// Reconstructs a pinned task context after the host resumes its state machine.
    pub async fn durable_context(
        self: &Arc<Self>,
        task_id: TaskId,
        operation_id: OperationId,
    ) -> Result<TaskContext> {
        self.durable_context_engine(
            task_id, operation_id, ContextSource::Ordinary(std::marker::PhantomData),
        ).await.map(|(context, _)| context)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn durable_context_with_native_work(
        self: &Arc<Self>, task: TaskId, operation: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskContext, acyclic_fs::NativeAllocation)> {
        let (context, custody) = self.durable_context_engine(
            task, operation, ContextSource::Native { work, purpose: NativeContextPurpose::Read },
        ).await?;
        Ok((context, custody.expect("native context source transfers its original custody")))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) async fn durable_context_for_dispatch_with_native_work(
        self: &Arc<Self>, task: TaskId, operation: OperationId,
        fence: &crate::scheduler::LeaseFence, mailbox: Arc<dyn OwnedTaskMailbox>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskContext, acyclic_fs::NativeAllocation)> {
        let (context, custody) = self.durable_context_engine(
            task, operation, ContextSource::Native {
                work, purpose: NativeContextPurpose::Dispatch { fence, mailbox },
            },
        ).await?;
        Ok((context, custody.expect("native dispatch source transfers its original custody")))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) async fn durable_context_for_reconciliation_with_native_work(
        self: &Arc<Self>, task: TaskId, operation: OperationId,
        fence: &crate::scheduler::LeaseFence, mailbox: Arc<dyn OwnedTaskMailbox>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskContext, acyclic_fs::NativeAllocation)> {
        let (context, custody) = self.durable_context_engine(
            task, operation, ContextSource::Native {
                work, purpose: NativeContextPurpose::Reconciliation { fence, mailbox },
            },
        ).await?;
        Ok((context, custody.expect("native reconciliation source transfers its original custody")))
    }

    async fn durable_context_engine(
        self: &Arc<Self>, task: TaskId, operation: OperationId, source: ContextSource<'_>,
    ) -> Result<(TaskContext, Option<NativeCustody>)> {
        let host = match &self.state {
            Some(host) => host,
            None => {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if matches!(&source, ContextSource::Native { .. }) {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                        reason: "durable task state is not bound" });
                }
                return Err(Error::Unsupported("durable task state is not bound".into()));
            }
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native_work = match &source {
            ContextSource::Native { work, .. } => Some(*work),
            ContextSource::Ordinary(_) => None,
        };
        let (scope, descendants, model_steps, owned_mailbox, custody, issued_purpose) = match source {
            ContextSource::Ordinary(_) => {
                let admitted = host.resume_scope(task, operation).await?;
                let scope = self.scope.resume_sealed(admitted, operation)?;
                let group = self.live.child(scope.concurrency_bound(self.concurrency), None)?;
                let descendants = group.child(scope.concurrency_bound(self.concurrency), None)?;
                (scope, descendants, Arc::new((AtomicUsize::new(0), None)), None, None, IssuedContextPurpose::Read)
            }
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            ContextSource::Native { work, purpose } => {
                work.charge(acyclic_fs::WorkCounters::default())
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let issued_purpose = match &purpose {
                    NativeContextPurpose::Read => IssuedContextPurpose::Read,
                    NativeContextPurpose::Dispatch { .. } => IssuedContextPurpose::Dispatch,
                    NativeContextPurpose::Reconciliation { .. } => IssuedContextPurpose::Reconciliation,
                };
                let mailbox = match purpose {
                    NativeContextPurpose::Read => None,
                    NativeContextPurpose::Dispatch { fence, mailbox } => {
                        if !mailbox.matches_task_binding(task, fence) {
                            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                                reason: "dispatch mailbox differs from the original task and fence" });
                        }
                        host.verify_dispatch_owner_with_native_work(task, fence, work)?.await?;
                        Some(mailbox)
                    }
                    NativeContextPurpose::Reconciliation { fence, mailbox } => {
                        if !mailbox.matches_task_binding(task, fence) {
                            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                                reason: "reconciliation mailbox differs from the original task and fence" });
                        }
                        host.verify_execution_owner_with_native_work(task, fence, work)?.await?;
                        Some(mailbox)
                    }
                };
                let extra_self = (std::mem::size_of::<TaskContext>() - std::mem::size_of::<RuntimeScope>()) as u64;
                let mut custody = work.claim(extra_self, 0, acyclic_fs::WorkCounters::UNCHARGED)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let mut admitted = host.resume_scope_with_native_work(
                    task, operation, work,
                )?.await?;
                if !admitted.1.matches_work(work) {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                        reason: "resumed scope differs from its original metadata ledger" });
                }
                self.validate_native_resumed_scope(&admitted.0, operation, work)?;
                admitted.0.extension_runtime = self.scope.extension_runtime.clone();
                admitted.0.extension_schemas = self.scope.extension_schemas.clone();
                admitted.0.extensions_sealed = true;
                if let Some(mailbox) = &mailbox {
                    mailbox.validate_scope_with_native_work(&admitted.0, work)?;
                }
                custody.absorb(&mut admitted.1)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let bound = admitted.0.concurrency_bound(self.concurrency);
                let group = self.live.child(bound, Some(work))?;
                let descendants = group.child(bound, Some(work))?;
                let model_custody = work.claim(std::mem::size_of::<(AtomicUsize, Option<NativeCustody>)>() as u64, u64::from((std::mem::size_of::<(AtomicUsize, Option<NativeCustody>)>() as u64) != 0), acyclic_fs::WorkCounters::UNCHARGED)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                let model_steps = Arc::new((AtomicUsize::new(0), Some(model_custody)));
                (admitted.0, descendants, model_steps, mailbox, Some(custody), issued_purpose)
            }
        };
        let mut context = TaskContext {
            harness: Arc::clone(self), task_id: operation, durable_task: Some(task),
            descendants, scope, policy_overrides: Vec::new(), owned_interactions: None,
            owned_mailbox, output_content: None, model_steps,
            #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
            native_root_output: std::sync::OnceLock::new(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata_source: None,
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let (Some(work), Some(custody)) = (native_work, custody.as_ref()) {
            context.mark_native_metadata_source(work, custody)?;
            context.native_metadata_source.as_mut().expect("issued metadata source")
                .purpose = issued_purpose;
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = issued_purpose;
        Ok((context, custody))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn validate_native_resumed_scope(
        &self, admitted: &RuntimeScope, operation: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        (&admitted.limits).validate(&mut Some(&mut crate::store::native_work_io(work).map_err(|cause| crate::Error::NativeWork { cause, custody: None })?))?;
        (&admitted.run_limits).validate(&mut Some(&mut crate::store::native_work_io(work).map_err(|cause| crate::Error::NativeWork { cause, custody: None })?))?;
        let created = admitted.created_access.as_ref();
        if let Some(access) = created {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: admitted.resource_proofs.len() as u64, ..Default::default()
            }).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            if access.operation != operation || access.proofs != admitted.resource_proofs {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "created resource seal belongs to another task" });
            }
        } else if !admitted.resource_proofs.is_empty() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "resumed resource scope has no original creation seal" });
        }
        for grant in admitted.grants.iter() {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: 1, ..Default::default()
            }).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            if !self.scope.grants.contains(grant)
                && !created.is_some_and(|access| access.grants.contains(grant))
            {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "resumed scope widens original admitted authority" });
            }
        }
        if !self.scope.allows_authority(&self.scope.grants, &admitted.limits)
            || admitted.extensions != self.scope.extensions
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "resumed scope changes original authority bounds or extensions" });
        }
        if !self.scope.run_limits.allows(&admitted.run_limits) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "resumed scope widens original task run limits" });
        }
        Ok(())
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
    has_state: bool,
    has_spawner: bool,
    has_interactions: bool,
    content: Option<&ContentBindings>,
    artifacts: Option<&ArtifactBindings>,
    has_policy: bool,
    io: StartupIo<'_>,
) -> Result<()> {
    let mut task_requirements = BTreeMap::new();
    for (key, entry) in &tasks.0 {
        native_startup::item(io, std::mem::size_of::<((String, String), BTreeSet<String>)>())?;
        let mut requirements = BTreeSet::new();
        for requirement in &entry.requirements {
            native_startup::item(io, std::mem::size_of::<String>())?;
            requirements.insert(native_startup::text(requirement, io)?);
        }
        task_requirements.insert((native_startup::text(&key.0, io)?, native_startup::text(&key.1, io)?), requirements);
    }
    let mut available_tools = BTreeSet::new();
    for requirement in task_requirements.values().flat_map(BTreeSet::iter) {
        let Some((name, version)) = requirement
            .strip_prefix("tool:")
            .and_then(|value| value.rsplit_once('@'))
        else {
            continue;
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut history = io.map(|io| io.history_io());
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut work = history.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let mut work = None;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let preparation = io;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let preparation = None;
        let available = tools.get_version(name, version, preparation)?.is_some()
            || resumable_tools.get(name, version, &mut work)?.is_some();
        if available {
            native_startup::item(io, std::mem::size_of::<(String, String)>())?;
            available_tools.insert((native_startup::text(name, io)?, native_startup::text(version, io)?));
        }
    }
    let created_default = scope.created_content(None);
    let content = created_default.or(content);
    let artifacts = created_default.or(artifacts);
    let created_read = scope.created_access.as_ref()
        .is_some_and(|access| !access.content.is_empty());
    let content_write = native_startup::content_write(content, scope, io)?;
    let artifacts_write = native_startup::content_write(artifacts, scope, io)?;
    let mut extensions = BTreeSet::new();
    if let Some(runtime) = scope.extension_runtime().filter(|runtime| runtime.accepts_new_admissions()) {
        for identity in runtime.selected() {
            native_startup::item(io, std::mem::size_of::<(String, u32)>())?;
            extensions.insert((native_startup::text(&identity.name, io)?, identity.version));
        }
    }
    let environment = TaskDependencyEnvironment {
        model: scope.grants.contains(capability::MODEL_GENERATE),
        context: scope.grants.contains(capability::CONTEXT_BUILD),
        interactions: (has_interactions || has_host)
            && scope.grants.contains(capability::INTERACTION_ROUTE),
        policy: has_policy,
        host: has_host,
        state: has_state,
        spawner: has_spawner,
        content: content.is_some() || created_read,
        artifacts: artifacts.is_some() || created_read,
        content_write,
        artifacts_write,
        extensions,
    };
    let mut grants = BTreeSet::new();
    for grant in scope.grants.iter() {
        native_startup::item(io, std::mem::size_of::<String>())?;
        grants.insert(native_startup::text(grant, io)?);
    }
    validate_task_requirements(&task_requirements, &available_tools, &environment, &grants, io)
}

/// One admitted task's mailbox borrowed from its current owner token.
pub(crate) trait OwnedTaskMailbox: acyclic_stream::ProviderPlatform {
    fn validate_scope(&self, scope: &RuntimeScope) -> Result<()>;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn validate_scope_with_native_work(
        &self, _scope: &RuntimeScope, _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "mailbox has no original-ledger admitted scope verifier" })
    }
    #[cfg(feature = "filesystem")]
    fn task_binding(&self) -> (TaskId, &crate::scheduler::LeaseFence);
    #[cfg(feature = "filesystem")]
    fn matches_task_binding(&self, task: TaskId, fence: &crate::scheduler::LeaseFence) -> bool;
    fn send<'a>(
        &'a self, recipient: TaskId, operation: OperationId, payload: FileRef,
    ) -> BoxFuture<'a, Result<crate::conversation::FileRef>>;
    fn inbox<'a>(&'a self, after: u64, maximum: usize) -> BoxFuture<'a, Result<Vec<InboxItem>>>;
    fn read_received<'a>(
        &'a self, receive_operation: OperationId, item: &'a InboxItem,
    ) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>>;

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn send_with_native_work<'a>(
        &'a self,
        _recipient: TaskId,
        _operation: OperationId,
        _payload: &'a FileRef,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (crate::conversation::FileRef, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "owned mailbox has no original native-ledger sender",
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn inbox_with_native_work<'a>(
        &'a self,
        _after: u64,
        _maximum: usize,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<InboxItem>, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "owned mailbox has no original native-ledger inbox",
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn read_received_with_native_work<'a>(
        &'a self,
        _receive_operation: OperationId,
        _item: &'a InboxItem,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "owned mailbox has no original native-ledger received parcel reader",
        })
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native_context;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub use native_context::NativeTaskContext;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
mod native_volume_callback;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub use native_volume_callback::NativeVolumeCallbackGuard;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub(crate) use native_volume_callback::NativeVolumePublicationGuard;
/// Capability-scoped context passed to live task code.
pub struct TaskContext {
    harness: Arc<AgentHarness>,
    task_id: OperationId,
    durable_task: Option<TaskId>,
    descendants: TaskGroup,
    scope: RuntimeScope,
    policy_overrides: Vec<(ComponentIdentity, Arc<dyn ToolPolicy>)>,
    owned_interactions: Option<Arc<dyn InteractionRouter>>,
    owned_mailbox: Option<Arc<dyn OwnedTaskMailbox>>,
    output_content: Option<ContentBindings>,
    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    native_root_output: std::sync::OnceLock<(Arc<crate::filesystem::NativeRootOutputSource>, ContentBindings)>,
    model_steps: Arc<(AtomicUsize, Option<NativeCustody>)>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_metadata_source: Option<native_context::NativeMetadataSource>,
}

impl Clone for TaskContext {
    fn clone(&self) -> Self {
        Self {
            harness: Arc::clone(&self.harness),
            task_id: self.task_id,
            durable_task: self.durable_task,
            descendants: self.descendants.clone(),
            scope: self.scope.clone(),
            policy_overrides: self.policy_overrides.clone(),
            owned_interactions: self.owned_interactions.clone(),
            owned_mailbox: self.owned_mailbox.clone(),
            output_content: self.output_content.clone(),
            #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
            native_root_output: self.native_root_output.clone(),
            model_steps: Arc::clone(&self.model_steps),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata_source: None,
        }
    }
}

impl AsRef<TaskContext> for TaskContext {
    fn as_ref(&self) -> &TaskContext { self }
}

#[cfg(feature = "filesystem")]
struct ContextContentVerifier<P: acyclic_stream::StreamProvider, C> {
    context: Arc<C>,
    owner: Arc<crate::durable_host::TaskJournalOwner<P>>,
    #[cfg(not(target_arch = "wasm32"))]
    custody: Option<acyclic_fs::NativeAllocation>,
}

/// Retains an issued context's content reader under its exact original owner.
#[cfg(feature = "filesystem")]
pub fn content_residency_verifier<P, C>(
    context: Arc<C>, owner: Arc<crate::durable_host::TaskJournalOwner<P>>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Arc<dyn ContentResidencyVerifier>>
where P: acyclic_stream::StreamProvider + 'static,
    C: AsRef<TaskContext> + acyclic_stream::ProviderPlatform + 'static,
{
    let issued = context.as_ref().as_ref();
    if issued.durable_task_id() != Some(owner.task_id()) || issued.id() != owner.operation_id() {
        return Err(crate::conversation::content_error(io, crate::wire::ErrorCode::Unauthorized,
            "content reader differs from its original task owner"));
    }
    #[cfg(not(target_arch = "wasm32"))]
    let native = io.and_then(crate::executor::PreparationIo::original_native_work);
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = native {
        if !owner.native_shared_work_matches(work) {
            return Err(crate::conversation::content_error(io, crate::wire::ErrorCode::Unauthorized, "content reader lacks original shared owner custody"));
        }
    }
    owner.require_task_context_binding(issued, io)?;
    #[cfg(not(target_arch = "wasm32"))]
    let custody = native.map(|work| {
        work.claim(std::mem::size_of::<ContextContentVerifier<P, C>>() as u64, 1,
            acyclic_fs::WorkCounters::UNCHARGED).map_err(Error::from)
    }).transpose()?;
    Ok(Arc::new(ContextContentVerifier { context, owner,
        #[cfg(not(target_arch = "wasm32"))]
        custody,
    }))
}

#[cfg(feature = "filesystem")]
impl<P, C> ContextContentVerifier<P, C>
where P: acyclic_stream::StreamProvider,
    C: AsRef<TaskContext>,
{
    async fn authorize(&self, reference: &FileRef, io: Option<&crate::conversation::ContentIo>,
        binding_io: Option<&crate::conversation::ContentIo>,
    ) -> Result<()> {
        self.owner.require_task_context_binding(self.context.as_ref().as_ref(), binding_io)?;
        self.owner.validate_input_file(reference, io)?;
        let mut history = io.map(crate::executor::PreparationIo::history_io);
        self.owner.verify_read_access(&mut history.as_mut().map(|history|
            history as &mut dyn crate::store::HistoryIo)).await
    }
}

#[cfg(feature = "filesystem")]
impl<P, C> ContentResidencyVerifier for ContextContentVerifier<P, C>
where P: acyclic_stream::StreamProvider + 'static,
    C: AsRef<TaskContext> + acyclic_stream::ProviderPlatform + 'static,
{
    fn read<'a>(&'a self, reference: &'a FileRef, maximum_bytes: u64,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if reference.descriptor().byte_length() > maximum_bytes {
                return Err(crate::conversation::content_error(io, crate::wire::ErrorCode::Invalid, "content exceeds the callback byte bound"));
            }
            let context = self.context.as_ref().as_ref();
            #[cfg(not(target_arch = "wasm32"))]
            let native_io = self.custody.as_ref().map(|custody|
                crate::executor::PreparationIo::from_native_work(custody.work())).transpose()?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io {
                let work = io.original_native_work().ok_or(crate::conversation::content_error(Some(io), crate::wire::ErrorCode::Unsupported, "content reader requires original native work"))?;
                if !self.custody.as_ref().is_some_and(|custody| custody.work().shares_allocation_context(work)) {
                    return Err(crate::conversation::content_error(Some(io), crate::wire::ErrorCode::Unauthorized, "content callback differs from its original native ledger"));
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            let binding_io = io.or(native_io.as_ref());
            #[cfg(target_arch = "wasm32")]
            let binding_io = io;
            self.authorize(reference, io, binding_io).await?;
            #[cfg(not(target_arch = "wasm32"))]
            let bytes = match io.and_then(crate::executor::PreparationIo::original_native_work) {
                Some(work) => context.read_file_with_native_work(reference, work).await?,
                None => context.read_file(reference).await?,
            };
            #[cfg(target_arch = "wasm32")]
            let bytes = context.read_file(reference).await?;
            self.authorize(reference, io, binding_io).await?;
            Ok(bytes)
        })
    }
}

enum ContentPublication<'a> {
    PathBound,
    OperationBound,
    Replacement(&'a FileRef),
}

impl TaskContext {
    /// Selects a trusted host-provided output binding under the unchanged task authority.
    pub fn with_content_bindings(mut self, binding: ContentBindings) -> Result<Self> {
        let writer = binding.writer.as_ref()
            .ok_or_else(|| Error::Unauthorized("output binding has no writer".into()))?;
        if !self.scope.grants().contains(&writer.volume().capability(VolumeOperation::Write, &mut None)?) {
            return Err(Error::Unauthorized("task cannot select this output volume".into()));
        }
        if let Some(sealed) = self.scope.created_content(Some(writer.volume())) {
            if sealed.writer.as_ref().is_none_or(|original| !Arc::ptr_eq(original, writer))
                || !Arc::ptr_eq(&sealed.reader, &binding.reader)
            {
                return Err(Error::Unauthorized("output differs from authenticated created binding".into()));
            }
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        { self.native_metadata_source = None; }
        self.output_content = Some(binding);
        Ok(self)
    }

    /// Selects an existing authenticated created destination without cloning the harness, widening grants or acquiring another owner reservation.
    pub fn with_output_volume(self, volume: &VolumeRef) -> Result<Self> {
        let binding = self.scope.created_content(Some(volume))
            .ok_or_else(|| Error::Unauthorized("output volume has no admitted creation binding".into()))?
            .clone();
        self.with_content_bindings(binding)
    }

    /// Returns only the exact creator-authenticated handles already admitted to this task.
    pub fn created_content_bindings(&self, volume: &VolumeRef) -> Result<ContentBindings> {
        volume.validate(&mut None)?;
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            if !self.scope.grants().contains(&volume.capability(operation, &mut None)?) {
                return Err(Error::Unauthorized("task lacks original created-content read/write authority".into()));
            }
        }
        let binding = self.scope.created_content(Some(volume))
            .ok_or_else(|| Error::Unauthorized("volume has no authenticated task creation binding".into()))?;
        if binding.writer.as_ref().is_none_or(|writer| writer.volume() != volume) {
            return Err(Error::Unauthorized("created content lacks its exact admitted writer".into()));
        }
        Ok(binding.clone())
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn with_owned_mailbox(
        mut self, task: TaskId, mailbox: Arc<dyn OwnedTaskMailbox>,
    ) -> Result<Self> {
        if self.durable_task != Some(task) {
            return Err(Error::Unauthorized("mailbox owner differs from task context".into()));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        { self.native_metadata_source = None; }
        self.owned_mailbox = Some(mailbox);
        Ok(self)
    }

    /// Returns only the original owner fence already bound to this exact task context.
    #[cfg(feature = "filesystem")]
    pub fn owned_task_fence(&self) -> Result<crate::scheduler::LeaseFence> {
        let owner = self.owned_mailbox.as_ref()
            .ok_or_else(|| Error::Unauthorized("task context has no original owner".into()))?;
        owner.validate_scope(&self.scope)?;
        let (task, fence) = owner.task_binding();
        if self.durable_task != Some(task) {
            return Err(Error::Unauthorized("task context differs from original owner".into()));
        }
        Ok(fence.clone())
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn original_task_binding_matches(
        &self, task: TaskId, fence: &crate::scheduler::LeaseFence,
    ) -> bool {
        self.durable_task == Some(task) && self.owned_mailbox.as_ref()
            .is_some_and(|owner| owner.matches_task_binding(task, fence))
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn with_owned_interactions(
        mut self,
        task: TaskId,
        router: Arc<dyn InteractionRouter>,
    ) -> Result<Self> {
        if self.durable_task != Some(task) {
            return Err(Error::Unauthorized(
                "interaction owner differs from task context".into(),
            ));
        }
        self.owned_interactions = Some(router);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        { self.native_metadata_source = None; }
        Ok(self)
    }

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
    #[cfg(feature = "filesystem")]
    pub(crate) fn original_host_binding_matches(
        &self, host: &dyn DurableTaskHost, policy: Option<&dyn ToolPolicy>,
    ) -> bool {
        self.harness.original_host_binding_matches(host, policy)
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
        child.scope = self.scope.clone().with_run_limits(limits, &mut None)?;
        child.descendants = self
            .descendants
            .child(child.scope.concurrency_bound(self.harness.concurrency), None)?;
        Ok(child)
    }

    /// Overrides only this live task's model binding for itself and local descendants.
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

    /// Places newly admitted descendants on another qualified provider.
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

    async fn verify_model_files(&self, messages: &[ModelMessage]) -> Result<()> {
        let limits = self.scope.limits();
        let mut files = Vec::new();
        let mut configurations = Vec::new();
        for message in messages {
            message.content.validate_limits(limits, &mut None)?;
            for file in message.content.file_refs() {
                limits.validate_file(file, &mut None)?;
                if !read_granted(self.scope.grants(), file)? {
                    return Err(Error::Unauthorized(
                        "task scope cannot project this file".into(),
                    ));
                }
                files.push(file);
            }
            configurations.extend(message.content.native_configurations());
        }
        let proofs = configurations
            .iter()
            .map(|binding| {
                binding.verify_original(
                    self.scope.extensions.as_ref(),
                    self.scope.extension_schemas.as_ref(),
                    self.scope.extension_runtime.as_deref(), None,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        if files.is_empty() {
            return Ok(());
        }
        for (binding, (schemas, expected)) in configurations.iter().zip(proofs) {
            let content = self.content_binding(Some(binding.configuration.content.volume()))
                .ok_or_else(|| Error::Unsupported("content reader is not bound".into()))?;
            let bytes = content.reader.read(&binding.configuration.content, binding.configuration.content.descriptor().byte_length(), None)?.await?;
            schemas.verify_configuration_binding(
                &binding.configuration,
                expected,
                &bytes,
                limits, None,
            )?;
        }
        for file in files {
            if !configurations
                .iter()
                .any(|binding| &binding.configuration.content == file)
            {
                self.content_binding(Some(file.volume()))
                    .ok_or_else(|| Error::Unsupported("content reader is not bound".into()))?
                    .reader.verify(file, file.descriptor().byte_length(), None)?.await?;
            }
        }
        Ok(())
    }

    /// Runs one bounded, provider-neutral model request from live task code.
    #[allow(
        clippy::too_many_lines,
        reason = "keep authorization, token admission and bounded streaming in execution order"
    )]
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
        if !self.scope.grants().contains(capability::MODEL_GENERATE) {
            return Err(Error::Unauthorized(
                "task scope lacks model:generate".into(),
            ));
        }
        self.scope.run_limits().check_deadline()?;
        let deadline = self.scope.run_limits().deadline_epoch_ms;
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
        let tools = self
            .harness
            .tools
            .definitions(None)?
            .into_iter()
            .filter(|tool| {
                self.scope
                    .grants()
                    .contains(&capability::tool_call(&tool.name))
            })
            .collect();
        let step_bound = self
            .scope
            .run_limits()
            .max_steps
            .map_or(self.scope.limits().model_steps, |bound| {
                bound.min(self.scope.limits().model_steps)
            });
        let request = ModelRequest {
            model: binding.model.clone(),
            messages,
            tools,
            max_output_tokens,
        };
        let request = crate::model::PreparedModelRequest::prepare(request, self.scope.limits(), None)?;
        self.verify_model_files(&request.request().messages).await?;
        let capacity = binding.provider.context_capacity(&binding.model)?;
        capacity.validate()?;
        let output_tokens = max_output_tokens.unwrap_or(capacity.output_tokens);
        if output_tokens > capacity.output_tokens {
            return Err(Error::Invalid(
                "model output budget exceeds selected capacity".into(),
            ));
        }
        let request = if max_output_tokens.is_none() {
            let mut bounded = request.request().clone();
            bounded.max_output_tokens = Some(output_tokens);
            crate::model::PreparedModelRequest::prepare(bounded, self.scope.limits(), None)?
        } else {
            request
        };
        let input_tokens = binding
            .provider
            .count_tokens(&request)?
            .validate(&request, &mut None)?;
        if input_tokens
            .checked_add(u64::from(output_tokens))
            .is_none_or(|total| total > u64::from(capacity.context_tokens))
        {
            return Err(Error::Invalid(
                "model request exceeds selected token capacity".into(),
            ));
        }
        let step = self
            .model_steps.0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                (used < step_bound).then_some(used + 1)
            })
            .map_err(|_| Error::Invalid("task exceeded its model step limit".into()))?;
        let mut events = Vec::new();
        let mut admission = ModelEventAdmission::default();
        let mut bytes = 0_u64;
        let dispatch = crate::model::ModelDispatch {
            operation_id: self.task_id,
            step: u32::try_from(step)
                .map_err(|_| Error::Invalid("model step exceeds portable bound".into()))?,
            request_digest: request.request_digest(),
        };
        let mut stream = binding.provider.generate(request, dispatch);
        loop {
            let next = match deadline {
                Some(deadline) => crate::live::timeout_until(deadline, stream.next()).await?,
                None => stream.next().await,
            };
            let Some(event) = next else { break };
            let event = event?;
            admission.observe(&event, self.scope.limits(), None)?;
            let event_bytes = crate::contract::canonical_json_bytes(&event)?;
            bytes = bytes
                .checked_add(event_bytes.len() as u64)
                .ok_or_else(|| Error::Invalid("model output size overflow".into()))?;
            if bytes > self.scope.limits().file_bytes {
                return Err(Error::Invalid("model output exceeds file limit".into()));
            }
            events.push(event);
        }
        if !admission.completed {
            return Err(Error::Invalid(
                "model stream ended without completion".into(),
            ));
        }
        Ok(events)
    }

    /// Adds a locally pinned policy layer.
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

    fn installed_root_output_content(&self) -> Option<&ContentBindings> {
        #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
        {
            return self.native_root_output.get().map(|binding| &binding.1);
        }
        #[cfg(not(all(feature = "native-execution", not(target_arch = "wasm32"))))]
        None
    }

    fn content_binding(&self, volume: Option<&VolumeRef>) -> Option<&ContentBindings> {
        if let Some(binding) = self.installed_root_output_content() {
            if volume.is_none_or(|volume| binding.writer.as_ref().is_some_and(|writer| writer.volume() == volume)) {
                return Some(binding);
            }
        }
        self.output_content.as_ref().filter(|binding| {
            volume.is_none_or(|volume| binding.writer.as_ref().is_some_and(|writer|
                writer.volume() == volume))
        }).or_else(|| self.scope.created_content(volume)).or(self.harness.content.as_ref())
    }

    /// Reads one immutable file through the owner's authenticated provider.
    pub async fn read_file(&self, file: &FileRef) -> Result<acyclic_stream::OwnedBytes> {
        self.read_bound_file(self.content_binding(Some(file.volume())), file)
            .await
    }

    /// Reads through the original bound provider on the caller's native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn read_file_with_native_work(
        &self, file: &FileRef, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_stream::OwnedBytes> {
        self.read_native_file(file, &self.scope, work).await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn authorize_native_file_read(&self, file: &FileRef, scope: &RuntimeScope,
        io: &crate::filesystem::PreparationIo,
    ) -> Result<()> {
        let mut history = Some(io.history_io());
        let mut observer = history.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
        scope.limits.validate_file(file, &mut observer)?;
        if !read_granted_with_work(&scope.grants, file, &mut observer)? {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "task scope cannot read this file" });
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    async fn read_native_file(&self, file: &FileRef, scope: &RuntimeScope,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let result = async {
            self.authorize_native_file_read(file, scope, &io)?;
            let reader = if let Some(reader) = self.scope.created_reference(file) {
                reader.as_ref()
            } else {
                self.content_binding(Some(file.volume()))
                    .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                        reason: "content reader is not bound" })?.reader.as_ref()
            };
            let output = reader.read(
                file,scope.limits.file_bytes,Some(&io),
            )?.await?;
            if file.descriptor().byte_length() != output.len() as u64 {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage,
                    reason: "content reader returned another file length" });
            }
            let hash = acyclic_fs::WorkCounters {bytes_hashed:output.len() as u64,..Default::default()};
            io.preflight(hash)?;
            let verified = file.descriptor().verify(&output, &mut None);
            io.charge(hash)?;
            verified?;
            Ok(output)
        }.await;
        result
    }

    /// Resolves an immutable artifact without coupling its provider to the workspace/content binding.
    pub async fn read_artifact(&self, file: &FileRef) -> Result<acyclic_stream::OwnedBytes> {
        self.read_bound_file(
            self.scope.created_content(Some(file.volume())).or(self.harness.artifacts.as_ref()), file,
        )
            .await
    }

    /// Discovers a private owner directory without a prior `FileRef`.
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
        if maximum_entries == 0 || maximum_entries as usize > MAX_PRIVATE_DIRECTORY_PAGE {
            return Err(Error::Invalid(
                "private directory page limit is invalid".into(),
            ));
        }
        let content = self.content_binding(Some(volume))
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
                None,
            )?
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
            crate::conversation::validate_content_path(&entry.name, &mut None)?;
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

    /// Resolves the current file at a named private path into verified pinned bytes; the returned `FileRef` can later be shared independently.
    pub async fn read_private_path(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        self.require_private_directory(volume, granted_prefix, path)?;
        if path.is_empty() {
            return Err(Error::Invalid("private file path is empty".into()));
        }
        let content = self.content_binding(Some(volume))
            .ok_or_else(|| Error::Unsupported("content reader is not bound".into()))?;
        let (file, bytes, _hold) = content
            .reader
            .read_private_path(volume, granted_prefix, path, expected_generation, self.scope.limits.file_bytes, None)?
            .await?;
        file.validate(&mut None)?;
        self.scope.limits.validate_file(&file, &mut None)?;
        if file.volume() != volume || file.path() != path {
            return Err(Error::Conflict(
                "private path provider returned another file".into(),
            ));
        }
        file.descriptor().verify(&bytes, &mut None)?;
        Ok((file, bytes))
    }

    fn require_private_directory(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
    ) -> Result<()> {
        volume.validate(&mut None)?;
        if crate::conversation::is_internal_path(path)
            || (!path.is_empty() && crate::conversation::validate_content_path(path, &mut None).is_err())
            || (!granted_prefix.is_empty()
                && crate::conversation::validate_content_path(granted_prefix, &mut None).is_err())
        {
            return Err(Error::Invalid(
                "private directory path or volume is invalid".into(),
            ));
        }
        let exact = volume.directory_read_capability(granted_prefix, &mut None)?;
        let full = volume.capability(VolumeOperation::Read, &mut None)?;
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
    ) -> Result<acyclic_stream::OwnedBytes> {
        file.validate(&mut None)?;
        self.scope.limits.validate_file(file, &mut None)?;
        if !read_granted(&self.scope.grants, file)? {
            return Err(Error::Unauthorized(
                "task scope cannot read this file".into(),
            ));
        }
        #[cfg(feature = "filesystem")]
        if let Some(reader) = self.scope.created_reference(file) {
            let bytes = reader.read(file, file.descriptor().byte_length(), None)?.await?;
            file.descriptor().verify(&bytes, &mut None)?;
            return Ok(bytes);
        }
        let content =
            binding.ok_or_else(|| Error::Unsupported("content reader is not bound".into()))?;
        let bytes = content.reader.read(file, file.descriptor().byte_length(), None)?.await?;
        file.descriptor().verify(&bytes, &mut None)?;
        Ok(bytes)
    }

    /// Stages exact bytes before a task may publish their ref.
    pub async fn stage_file(
        &self,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
    ) -> Result<FileRef> {
        self.stage_bound_file(
            self.content_binding(None),
            operation_id,
            path,
            bytes,
            media_type,
            display_name,
            ContentPublication::PathBound,
        )
        .await
    }

    /// Publishes to an explicitly selected authenticated created volume.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact volume, retry identity, body and file metadata stay explicit"
    )]
    pub async fn stage_file_in(
        &self,
        volume: &VolumeRef,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
    ) -> Result<FileRef> {
        let binding = self.content_binding(Some(volume));
        if self.bound_content_writer(binding)?.volume() != volume {
            return Err(Error::Unauthorized("selected volume is not the bound destination".into()));
        }
        self.stage_bound_file(
            binding, operation_id, path, bytes, media_type, display_name,
            ContentPublication::PathBound,
        ).await
    }

    /// Publishes one exact file contract per operation through its original writer.
    pub async fn stage_file_once(
        &self,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
    ) -> Result<FileRef> {
        self.stage_bound_file(
            self.content_binding(None),
            operation_id,
            path,
            bytes,
            media_type,
            display_name,
            ContentPublication::OperationBound,
        )
        .await
    }

    /// Observes the original once-publication, never a refreshed source/head.
    pub async fn observe_staged_file_once(&self, operation_id: OperationId, io: Option<&crate::executor::PreparationIo>) -> Result<Option<FileRef>> {
        #[cfg(feature = "filesystem")]
        let writer = match io {
            Some(io) => self.bound_content_writer_measured(self.content_binding(None), &self.scope, io)?,
            None => self.bound_content_writer(self.content_binding(None))?,
        };
        #[cfg(not(feature = "filesystem"))]
        let writer = self.bound_content_writer(self.content_binding(None))?;
        if io.is_some() { self.scope.verify_created_write(writer.volume(), io).await?; }
        let (file, hold) = writer.observe_staged_once(operation_id, self.scope.limits.file_bytes, io)?.await?;
        if io.is_some() { self.scope.verify_created_write(writer.volume(), io).await?; }
        if let Some(hold) = hold { hold.into_retained(); }
        if let Some(file) = &file {
            #[cfg(feature = "filesystem")]
            let mut observer = io.map(crate::filesystem::PreparationIo::history_io);
            #[cfg(feature = "filesystem")]
            let mut work = observer.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
            #[cfg(not(feature = "filesystem"))]
            let mut work = None;
            self.scope.limits.validate_file(file, &mut work)?;
            if file.volume() != writer.volume() {
                return Err(Error::Storage("original publisher receipt belongs to another volume".into()));
            }
        }
        Ok(file)
    }

    #[cfg(feature = "filesystem")]
    pub async fn observe_staged_file_once_measured(
        &self, operation_id: OperationId, budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Option<FileRef>>, Error> {
        let io = crate::filesystem::PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let result = async {
            let writer = self.bound_content_writer_measured(self.content_binding(None), &self.scope, &io)?;
            let (file, hold) = writer.observe_staged_once(operation_id, self.scope.limits.file_bytes, Some(&io))?.await?;
            if let Some(hold) = hold { hold.into_retained(); }
            if let Some(file) = &file {
                io.retain_owned(crate::filesystem::UploadRecord::logical_owned_bytes(file)?)?;
                self.scope.limits.validate_file(file, &mut None)?;
                if file.volume() != writer.volume() {
                    return Err(Error::Storage("original publisher receipt belongs to another volume".into()));
                }
            }
            Ok(file)
        }.await;
        match result {
            Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
            Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
        }
    }

    /// Reads through the exact original task scope and owner-backed reader.
    #[cfg(feature = "filesystem")]
    pub async fn read_file_measured(
        &self, file: &FileRef, budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<acyclic_stream::OwnedBytes>, Error> {
        let io = crate::filesystem::PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let result = async {
            file.validate(&mut None)?;
            self.scope.limits.validate_file(file, &mut None)?;
            if !read_granted_with_work(&self.scope.grants, file, &mut Some(&mut (&io).history_io()))? {
                return Err(Error::Unauthorized("task scope cannot read this file".into()));
            }
            let reader = if let Some(reader) = self.scope.created_reference(file) {
                reader.as_ref()
            } else {
                self.content_binding(Some(file.volume()))
                    .ok_or_else(|| Error::Unsupported("content reader is not bound".into()))?.reader.as_ref()
            };
            let bytes = reader.read(file, self.scope.limits.file_bytes, Some(&io))?.await?;
            io.retain_owned(bytes.retained_capacity().unwrap_or(0) as u64)?;
            let hash = acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() };
            io.preflight(hash)?;
            let verified = file.descriptor().verify(&bytes, &mut None);
            io.charge(hash)?;
            verified?;
            Ok(bytes)
        }.await;
        match result {
            Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
            Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
        }
    }

    /// The same once-publication contract with real cumulative provider work.
    #[cfg(feature = "filesystem")]
    #[allow(clippy::too_many_arguments, reason = "complete once-publication identity and finite caller budget")]
    pub async fn stage_file_once_measured(
        &self, operation_id: OperationId, path: &str, bytes: &[u8],
        media_type: &str, display_name: &str, budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<FileRef>, Error> {
        let io = crate::filesystem::PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let result = async {
            if bytes.len() as u64 > self.scope.limits.file_bytes || path.len() > self.scope.limits.path_bytes {
                return Err(Error::Invalid("file exceeds task content limits".into()));
            }
            let content = self.content_binding(None)
                .ok_or_else(|| Error::Unsupported("content publisher is not bound".into()))?;
            let writer = self.bound_content_writer_measured(Some(content), &self.scope, &io)?;
            if !content.reader.supports_read(writer.volume(), Some(&io))? {
                return Err(Error::Unsupported("original content reader has no finite engine".into()));
            }
            let (file, hold) = writer.stage_once(operation_id, path, bytes, media_type,
                display_name, Some(&io))?.await?;
            if let Some(hold) = hold { hold.into_retained(); } else {
                io.retain_owned(crate::filesystem::UploadRecord::logical_owned_bytes(&file)?)?;
            }
            self.scope.limits.validate_file(&file, &mut None)?;
            if file.volume() != writer.volume() || file.path() != path
                || file.display_name() != display_name || file.descriptor().media_type() != media_type {
                return Err(Error::Storage("content publisher returned another file identity".into()));
            }
            let hash = acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() };
            io.preflight(hash)?;
            let verified = file.descriptor().verify(bytes, &mut None);
            io.charge(hash)?;
            verified?;
            content.reader.verify(&file, self.scope.limits.file_bytes, Some(&io))?.await?;
            Ok(file)
        }.await;
        match result {
            Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
            Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
        }
    }

    #[cfg(feature = "filesystem")]
    fn bound_content_writer_measured<'a>(
        &self, binding: Option<&'a ContentBindings>, scope: &RuntimeScope, io: &crate::filesystem::PreparationIo,
    ) -> Result<&'a Arc<dyn ContentPublisher>> {
        let content = binding.ok_or_else(|| Error::Unsupported("content publisher is not bound".into()))?;
        let writer = content.writer.as_ref()
            .ok_or_else(|| Error::Unauthorized("task has no owner-bound content writer".into()))?;
        let capability = io.capability(writer.volume(), "volume:write:")?;
        if !scope.grants.contains(&capability) {
            return Err(Error::Unauthorized("task scope cannot write this volume".into()));
        }
        Ok(writer)
    }

    /// Uses the same once-publication writer and native-work ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn stage_file_once_with_native_work(
        &self, operation_id: OperationId, path: &str, bytes: &[u8],
        media_type: &str, display_name: &str, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(FileRef,acyclic_fs::NativeAllocation)> {
        self.stage_native_file(operation_id, path, bytes, media_type, display_name, None,
            &self.scope, work).await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    async fn stage_native_file(&self, operation_id: OperationId, path: &str, bytes: &[u8],
        media_type: &str, display_name: &str, source: Option<&FileRef>, scope: &RuntimeScope,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(FileRef, acyclic_fs::NativeAllocation)> {
        self.require_original_native_dispatch(work)?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let result = async {
            if bytes.len() as u64 > scope.limits.file_bytes || path.len() > scope.limits.path_bytes {
                return Err(Error::Invalid("file exceeds task content limits".into()).into());
            }
            let content = self.content_binding(source.map(FileRef::volume))
                .ok_or_else(|| Error::Unsupported("content publisher is not bound".into()))?;
            let writer = self.bound_content_writer_measured(Some(content),scope,&io)?;
            if !content.reader.supports_read(writer.volume(),Some(&io))? {
                return Err(Error::Unsupported("original content reader has no native-work engine".into()).into());
            }
            if let Some(source) = source {
                self.authorize_native_file_read(source, scope, &io)?;
                if source.volume() != writer.volume() || source.path() != path {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                        reason: "source differs from the owner-bound destination" });
                }
            }
            writer.supports_stage(Some(&io))?;
            self.scope.verify_created_write(writer.volume(), Some(&io)).await?;
            let output = if let Some(source) = source {
                content.reader.verify(source, scope.limits.file_bytes, Some(&io))?.await?;
                writer.stage_at(operation_id, source, bytes, Some(&io))?.await?
            } else {
                writer.stage_once(operation_id, path, bytes, media_type, display_name, Some(&io))?.await?
            };
            self.scope.verify_created_write(writer.volume(), Some(&io)).await?;
            let file = &output.0;
            scope.limits.validate_file(file, &mut None)?;
            if file.volume() != writer.volume() || file.path() != path
                || file.display_name() != display_name || file.descriptor().media_type() != media_type
                || file.descriptor().byte_length() != bytes.len() as u64
            {
                return Err(Error::Storage("content publisher returned another file identity".into()).into());
            }
            let hash = acyclic_fs::WorkCounters {bytes_hashed:bytes.len() as u64,..Default::default()};
            io.preflight(hash)?;
            let verified = file.descriptor().verify(bytes, &mut None);
            io.charge(hash)?;
            verified?;
            content.reader.verify(file,scope.limits.file_bytes,Some(&io))?.await?;
            let (file, hold) = output;
            let retained = hold.as_ref().map_or(0, |hold| hold.bytes());
            if let Some(hold) = hold { hold.into_retained(); }
            Ok((file, io.take_native_custody(retained)?))
        }.await;
        result
    }

    /// Publishes immutable artifact bytes under a caller-retained operation ID.
    pub async fn stage_artifact(
        &self,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
    ) -> Result<FileRef> {
        self.stage_bound_file(
            self.installed_root_output_content()
                .or_else(|| self.scope.created_artifact_content())
                .or_else(|| self.scope.created_content(None))
                .or(self.harness.artifacts.as_ref()),
            operation_id,
            path,
            bytes,
            media_type,
            display_name,
            ContentPublication::PathBound,
        )
        .await
    }

    /// Replaces a pinned source through the existing owner-bound publisher.
    pub async fn stage_file_at(
        &self,
        operation_id: OperationId,
        source: &FileRef,
        bytes: &[u8],
    ) -> Result<FileRef> {
        self.scope.limits.validate_file(source, &mut None)?;
        if !read_granted(&self.scope.grants, source)? {
            return Err(Error::Unauthorized(
                "task scope cannot read this source".into(),
            ));
        }
        self.stage_bound_file(
            self.content_binding(Some(source.volume())),
            operation_id,
            source.path(),
            bytes,
            source.descriptor().media_type(),
            source.display_name(),
            ContentPublication::Replacement(source),
        )
        .await
    }
    fn bound_content_writer<'a>(
        &self,
        binding: Option<&'a ContentBindings>,
    ) -> Result<&'a Arc<dyn ContentPublisher>> {
        let content =
            binding.ok_or_else(|| Error::Unsupported("content publisher is not bound".into()))?;
        let writer = content
            .writer
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("task has no owner-bound content writer".into()))?;
        if !self
            .scope
            .grants
            .contains(&writer.volume().capability(VolumeOperation::Write, &mut None)?)
        {
            return Err(Error::Unauthorized(
                "task scope cannot write this volume".into(),
            ));
        }
        Ok(writer)
    }

    /// Checks the original content destination without reading or publishing bytes.
    pub(crate) fn authorize_content_publication(&self, source: Option<&FileRef>) -> Result<()> {
        let writer = self.bound_content_writer(self.content_binding(source.map(FileRef::volume)))?;
        if source.is_some_and(|source| source.volume() != writer.volume()) {
            return Err(Error::Unauthorized(
                "source differs from the owner-bound destination".into(),
            ));
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one owner-bound publication path retains its binding, identity, bytes, metadata and receipt mode"
    )]
    async fn stage_bound_file(
        &self,
        binding: Option<&ContentBindings>,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        publication: ContentPublication<'_>,
    ) -> Result<FileRef> {
        if bytes.len() as u64 > self.scope.limits.file_bytes
            || path.len() > self.scope.limits.path_bytes
        {
            return Err(Error::Invalid("file exceeds task content limits".into()));
        }
        let content =
            binding.ok_or_else(|| Error::Unsupported("content publisher is not bound".into()))?;
        let writer = self.bound_content_writer(Some(content))?;
        self.scope.verify_created_write(writer.volume(), None).await?;
        let file = match publication {
            ContentPublication::Replacement(source) => {
                if source.volume() != writer.volume() || source.path() != path {
                    return Err(Error::Unauthorized(
                        "source differs from the owner-bound destination".into(),
                    ));
                }
                content.reader.verify(source, source.descriptor().byte_length(), None)?.await?;
                writer.stage_at(operation_id, source, bytes, None)?.await?.0
            }
            ContentPublication::OperationBound => {
                writer
                    .stage_once(operation_id, path, bytes, media_type, display_name, None)?
                    .await?.0
            }
            ContentPublication::PathBound => {
                writer
                    .stage(operation_id, path, bytes, media_type, display_name, None)?
                    .await?.0
            }
        };
        self.scope.verify_created_write(writer.volume(), None).await?;
        self.scope.limits.validate_file(&file, &mut None)?;
        if file.volume() != writer.volume()
            || file.path() != path
            || file.display_name() != display_name
            || file.descriptor().media_type() != media_type
        {
            return Err(Error::Storage(
                "content publisher returned another file identity".into(),
            ));
        }
        file.descriptor().verify(bytes, &mut None)?;
        content.reader.verify(&file, file.descriptor().byte_length(), None)?.await?;
        Ok(file)
    }

    /// Calls a local tool under this task's scope and validates both schemas.
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
            .get_version(&tool.definition.name, &tool.definition.revision, None)?
            .ok_or_else(|| Error::NotFound(format!("tool {}", tool.definition.name)))?;
        if binding.definition != tool.definition {
            return Err(Error::Conflict(
                "tool definition changed after lookup".into(),
            ));
        }
        let required = capability::tool_call(&tool.definition.name);
        if !self.scope.grants.contains(&required) {
            return Err(Error::Unauthorized(format!("scope lacks {required}")));
        }
        let arguments =
            serde_json::to_value(input).map_err(|error| Error::Invalid(error.to_string()))?;
        validate_value(&tool.definition.input_schema, &arguments, "tool input", None)?;
        let operation_id = OperationId::new();
        let invocation = ToolInvocation {
            operation_id,
            call_id: operation_id.to_string(),
            name: tool.definition.name.clone(),
            arguments,
        };
        let context = ToolContext::new(
            self.clone(),
            invocation.operation_id,
            invocation.call_id.clone(),
        )?;
        binding
            .executor
            .authorize_with_context(&context, &invocation)?;
        if let Some(operation) = self.authorize_tool(&tool.definition, &invocation).await? {
            return Err(Error::Indeterminate(operation));
        }
        let result = binding
            .executor
            .execute_with_context(context, invocation)
            .await?;
        validate_value(&tool.definition.output_schema, &result.value, "tool output", None)?;
        serde_json::from_value(result.value).map_err(|error| Error::Invalid(error.to_string()))
    }

    /// Returns any pending approval before dispatching the action.
    pub(crate) async fn authorize_tool(
        &self, definition: &ToolDefinition, invocation: &ToolInvocation,
    ) -> Result<Option<OperationId>> {
        self.authorize_tool_policies(definition, invocation, None, None, &self.scope).await
    }

    /// Borrows the original configured policy and its protected binding identity.
    pub(crate) fn stock_tool_policy_binding(&self) -> Option<(&ComponentIdentity, &Arc<dyn ToolPolicy>)> {
        self.harness.policy_identity.as_ref().zip(self.harness.policy.as_ref())
    }

    /// Borrows scoped vetoes in their original admission order.
    pub(crate) fn stock_tool_policy_overrides(
        &self,
    ) -> impl Iterator<Item = (&ComponentIdentity, &Arc<dyn ToolPolicy>)> {
        self.policy_overrides.iter().map(|(identity, policy)| (identity, policy))
    }

    /// A borrowed scope can restrict the original grant, never replace its sealed selections, created-access owner, or runtime provenance.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn require_native_scope_restriction(
        &self, scope: &RuntimeScope, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let limits = scope.limits;
        let run = scope.run_limits;
        let mut io = crate::store::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        limits.validate(&mut Some(&mut io))?;
        run.validate(&mut Some(&mut io))?;
        let same_runtime = match (&self.scope.extension_runtime, &scope.extension_runtime) {
            (None, None) => true,
            (Some(original), Some(candidate)) => Arc::ptr_eq(original, candidate),
            _ => false,
        };
        let same_created_access = match (&self.scope.created_access, &scope.created_access) {
            (None, None) => true,
            (Some(original), Some(candidate)) => Arc::ptr_eq(original, candidate),
            _ => false,
        };
        if !self.scope.extensions_sealed || !scope.extensions_sealed
            || self.scope.extensions != scope.extensions
            || self.scope.extension_schemas != scope.extension_schemas
            || self.scope.resource_proofs != scope.resource_proofs
            || !same_runtime || !same_created_access
            || !self.scope.allows_authority(&scope.grants, &limits)
            || !self.scope.run_limits.allows(&run)
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native scope cannot widen or replace its original scope" });
        }
        Ok(())
    }

    async fn authorize_tool_policies(
        &self, definition: &ToolDefinition, invocation: &ToolInvocation,
        work: Option<&NativeWork>, command_router: Option<&dyn InteractionRouter>, scope: &RuntimeScope,
    ) -> Result<Option<OperationId>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.remaining()?;
            if let Some(router) = command_router { router.supports_native_interactions(work)?; }
            for (_, policy) in self.stock_tool_policy_binding().into_iter().chain(self.stock_tool_policy_overrides()) {
                policy.supports_native_policy(work)?;
            }
        }
        if self.harness.policy.is_some() && self.harness.policy_identity.is_none() {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "runtime policy identity changed after binding" });
            }
            return Err(Error::Conflict("runtime policy identity changed after binding".into()));
        }
        for (identity, policy) in self.stock_tool_policy_binding().into_iter().chain(self.stock_tool_policy_overrides()) {
            let bound_reason = if self.harness.policy.as_ref().is_some_and(|global| Arc::ptr_eq(global, policy)) {
                "runtime policy identity changed after binding"
            } else {
                "scoped policy identity changed after binding"
            };
            if let Some(paid) = self.policy_approval(
                policy.as_ref(), Some((identity, bound_reason)), definition, invocation, work, scope,
            ).await? {
                let ((approval_id, request), custody) = paid;
                let outcome: (InteractionOutcome, Option<NativeCustody>) = match work {
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    Some(work) => {
                        let (value, custody) = self.interact_with_native_work_and_router(
                            approval_id, request, custody.expect("native approval retains its prompt"), work, command_router, scope,
                        ).await?;
                        (value, Some(custody))
                    }
                    _ => (self.interact(approval_id, request).await?, None),
                };
                if let InteractionOutcome::Indeterminate { operation_id } = &outcome.0 {
                    return Ok(Some(*operation_id));
                }
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if work.is_some() && matches!(&outcome.0, InteractionOutcome::Answered { .. }) {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                        reason: "approval resolved with a question answer" });
                }
                check_tool_approval(outcome.0)?;
            }
        }
        Ok(None)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) async fn stock_policy_approval(
        &self, policy: &dyn ToolPolicy, scope: &RuntimeScope,
        definition: &ToolDefinition, invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<((OperationId, Interaction), acyclic_fs::NativeAllocation)>> {
        self.require_original_native_work(work)?;
        self.require_native_scope_restriction(scope, work)?;
        let expected = self.stock_tool_policy_binding().into_iter().chain(self.stock_tool_policy_overrides())
            .find_map(|(identity, original)| std::ptr::eq(original.as_ref(), policy).then_some(identity))
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "policy is not an original task binding" })?;
        Ok(self.policy_approval(policy, Some((expected, "policy identity changed after binding")), definition, invocation, Some(work), scope)
            .await?.map(|paid| (paid.0, paid.1.expect("native approval retains its prompt"))))
    }

    async fn policy_approval(
        &self, policy: &dyn ToolPolicy, expected: Option<(&ComponentIdentity, &'static str)>,
        definition: &ToolDefinition, invocation: &ToolInvocation, work: Option<&NativeWork>, scope: &RuntimeScope,
    ) -> Result<Option<((OperationId, Interaction), Option<NativeCustody>)>> {
        let identity: (ComponentIdentity, Option<NativeCustody>) = match work {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Some(work) => {
                policy.supports_native_policy(work)?;
                let (value, custody) = policy.identity_with_native_work(work)?;
                let paid = (value, Some(custody));
                validate_native_policy_identity(&paid, work)?;
                paid
            }
            _ => (policy.identity(), None),
        };
        if let Some((expected, reason)) = expected {
            if expected != &identity.0 {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if work.is_some() {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason });
                }
                return Err(Error::Conflict(reason.into()));
            }
        }
        let decision: (ToolPolicyDecision, Option<NativeCustody>) = match work {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Some(work) => {
                let future = policy.evaluate_with_native_work(invocation, scope, work)?;
                if !future.work().shares_ledger(work) {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                        reason: "policy evaluator belongs to another ledger" });
                }
                let (value, custody) = future.await?;
                let paid = (value, Some(custody));
                validate_native_policy_decision(&paid, work)?;
                paid
            }
            _ => (policy.evaluate(invocation, scope).await?, None),
        };
        let after: (ComponentIdentity, Option<NativeCustody>) = match work {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Some(work) => {
                let (value, custody) = policy.identity_with_native_work(work)?;
                let paid = (value, Some(custody));
                validate_native_policy_identity(&paid, work)?;
                paid
            }
            _ => (policy.identity(), None),
        };
        if after.0 != identity.0 {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "policy identity changed during evaluation" });
            }
            return Err(Error::Conflict("policy identity changed during evaluation".into()));
        }
        drop(after);
        let (decision, custody) = decision;
        match decision {
            ToolPolicyDecision::Allow => Ok(None),
            ToolPolicyDecision::Deny { reason } => {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if let Some(custody) = custody {
                    return Err(Error::NativeDiagnostic { code: crate::wire::ErrorCode::Unauthorized,
                        message: reason, custody });
                }
                Err(Error::Unauthorized(reason))
            }
            ToolPolicyDecision::RequireApproval { prompt } => {
                let task = self.durable_task.unwrap_or_else(|| TaskId::from_bytes(self.task_id.into_bytes()));
                match work {
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    Some(work) => {
                        let (value, custody) = tool_approval_request_with_native_work(
                            task, &identity.0, definition, invocation, prompt,
                            custody.expect("native decision retains its prompt"), work,
                        )?;
                        Ok(Some((value, Some(custody))))
                    }
                    _ => Ok(Some((tool_approval_request(task, &identity.0, definition, invocation, prompt)?, None))),
                }
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
        self.harness
            .spawn_in_group(
                &self.descendants,
                self.scope.clone(),
                self.policy_overrides.clone(),
                definition,
                input,
                None,
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
                .child(self.scope.concurrency_bound(self.harness.concurrency), None)
                .expect("ordinary child group"),
            scope: self.scope.clone(),
            policy_overrides: self.policy_overrides.clone(),
            durable_parent: self.durable_task,
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
        interaction.validate(&mut None)?;
        if !self.scope.grants.contains(capability::INTERACTION_ROUTE) {
            return Err(Error::Unauthorized("scope lacks interaction:route".into()));
        }
        if let Some(task) = self.durable_task {
            if operation_id.into_bytes() == task.into_bytes() {
                return Err(Error::Invalid(
                    "interaction operation must differ from task admission".into(),
                ));
            }
            if let Some(router) = &self.owned_interactions {
                return router.route(operation_id, interaction).await;
            }
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

    /// Pure support discovery for the original binding, before callback boxing or constructing an owned interaction request.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn supports_native_interactions(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        self.require_original_native_work(work)?;
        if self.durable_task.is_some() {
            if let Some(router) = &self.owned_interactions {
                return router.supports_native_interactions(work);
            }
            self.harness.state.as_ref().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported, reason: "durable task state is not bound",
            })?.supports_native_interactions(work)
        } else {
            self.harness.interactions.as_ref().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported, reason: "local interaction router is not bound",
            })?.supports_native_interactions(work)
        }
    }

    /// Routes an already-paid request through this context's original binding.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn interact_with_native_work(
        &self, operation_id: OperationId, interaction: Interaction,
        request_custody: acyclic_fs::NativeAllocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(InteractionOutcome, acyclic_fs::NativeAllocation)> {
        self.interact_with_native_work_and_router(operation_id, interaction, request_custody, work, None, &self.scope).await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    async fn interact_with_native_work_and_router(
        &self, operation_id: OperationId, interaction: Interaction,
        request_custody: acyclic_fs::NativeAllocation,
        work: &acyclic_fs::NativeMaterializationWork,
        command_router: Option<&dyn InteractionRouter>,
        scope: &RuntimeScope,
    ) -> Result<(InteractionOutcome, acyclic_fs::NativeAllocation)> {
        let paid = (interaction, request_custody);
        self.require_original_native_work(work)?;
        if !paid.1.work().shares_ledger(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "interaction request belongs to another original ledger" });
        }
        if let Interaction::Approval { prompt, .. } = &paid.0 {
            if paid.1.bytes() < prompt.capacity() as u64 {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "approval request does not retain its original prompt" });
            }
        }
        let router = command_router.or_else(|| if self.durable_task.is_some() {
            self.owned_interactions.as_ref().map(|router| router.as_ref())
        } else {
            self.harness.interactions.as_ref().map(|router| router.as_ref())
        });
        let state = if router.is_none() && self.durable_task.is_some() {
            Some(self.harness.state.as_ref().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported, reason: "durable task state is not bound",
            })?)
        } else { None };
        if let Some(router) = router {
            router.supports_native_interactions(work)?;
        } else if let Some(state) = state {
            state.supports_native_interactions(work)?;
        } else {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                reason: "local interaction router is not bound" });
        }
        let mut io = crate::store::history::native_work_io(work)?;
        let mut observed: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut io);
        paid.0.validate(&mut observed)?;
        if !scope.grants.contains(capability::INTERACTION_ROUTE) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "scope lacks interaction:route" });
        }
        if self.durable_task.is_some_and(|task| operation_id.into_bytes() == task.into_bytes()) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "interaction operation must differ from task admission" });
        }
        let future = if let Some(router) = router {
            let (interaction, request_custody) = paid;
            router.route_with_native_work(operation_id, interaction, request_custody, work)?
        } else if let (Some(task), Some(state)) = (self.durable_task, state) {
            let (interaction, request_custody) = paid;
            state.interact_with_native_work(task, operation_id, interaction, request_custody, work)?
        } else {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                reason: "local interaction router is not bound" });
        };
        if !future.work().shares_ledger(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "interaction route belongs to another original ledger" });
        }
        let (value, custody) = future.await?;
        let paid = (value, custody);
        if !paid.1.work().shares_ledger(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "interaction outcome belongs to another original ledger" });
        }
        if let InteractionOutcome::Answered { answer } = &paid.0 {
            let owned = answer.logical_owned_bytes().map_err(|error| {
                if error.is_native_failure() { error } else {
                    Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                        reason: "interaction answer ownership overflows" }
                }
            })?;
            let bytes = owned.checked_add(std::mem::size_of::<FileRef>() as u64)
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "interaction answer ownership overflows" })?;
            if paid.1.bytes() < bytes {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "interaction answer does not retain its original metadata" });
            }
        }
        work.remaining()?;
        Ok((paid.0, paid.1))
    }

    /// Sends a donor-authorized parcel under the retained owner fence and route.
    pub async fn send(
        &self,
        recipient: TaskId,
        message_id: OperationId,
        payload: FileRef,
    ) -> Result<crate::conversation::FileRef> {
        payload.validate(&mut None)?;
        let mailbox = self.owned_mailbox.as_ref()
            .ok_or_else(|| Error::Unauthorized("mail requires a retained task mailbox owner".into()))?;
        mailbox.validate_scope(&self.scope)?;
        mailbox.send(recipient, message_id, payload).await
    }

    /// Reads only one retained parcel selected by this task's original receive command.
    pub async fn read_received_mail(
        &self, receive_operation: OperationId, item: &InboxItem,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(original) = work.as_deref().and_then(|io| io.original_native_work()) {
            return self.native_mailbox(original)?.read_received_with_native_work(receive_operation, item, original)?.await;
        }
        if work.is_some() {
            return Err(crate::scheduler::diagnostic(work, crate::wire::ErrorCode::Unsupported,
                "parcel read requires an original native observer"));
        }
        let mailbox = self.owned_mailbox.as_ref()
            .ok_or_else(|| Error::Unauthorized("parcel read requires a retained mailbox owner".into()))?;
        mailbox.validate_scope(&self.scope)?;
        mailbox.read_received(receive_operation, item).await
    }

    /// Reads a bounded durable inbox page after the last observed sequence.
    pub async fn inbox(&self, after: u64, limit: usize) -> Result<Vec<InboxItem>> {
        if limit == 0 {
            return Err(Error::Invalid("inbox page bound is invalid".into()));
        }
        let mailbox = self.owned_mailbox.as_ref()
            .ok_or_else(|| Error::Unauthorized("inbox requires a retained task mailbox owner".into()))?;
        mailbox.validate_scope(&self.scope)?;
        mailbox.inbox(after, limit).await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_mailbox(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<&Arc<dyn OwnedTaskMailbox>> {
        self.owned_task_binding_with_native_work(work)?;
        self.owned_mailbox.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "mail requires an original native task mailbox owner",
        })
    }

    /// Sends the same exact parcel with the original native owner and work.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn send_with_native_work<'a>(
        &'a self,
        recipient: TaskId,
        message_id: OperationId,
        payload: &'a FileRef,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (crate::conversation::FileRef, acyclic_fs::NativeAllocation)>> {
        self.native_mailbox(work)?.send_with_native_work(recipient, message_id, payload, work)
    }

    /// Reads a bounded original-owner inbox page without rebuilding a scope.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn inbox_with_native_work<'a>(
        &'a self,
        after: u64,
        limit: usize,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<InboxItem>, acyclic_fs::NativeAllocation)>> {
        let mailbox = self.native_mailbox(work)?;
        if limit == 0 {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "inbox page bound is invalid",
            });
        }
        mailbox.inbox_with_native_work(after, limit, work)
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
    ) -> Result<Option<Vec<crate::extension::ExtensionLease>>> {
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
        runtime.validate_admission(scope.extensions(), &mut None)?;
        Ok(Some(runtime.retain(requirements, false)?))
    }

    fn retain_task_extensions_existing<I, O>(
        &self,
        definition: &TaskDefinition<I, O>,
        scope: &RuntimeScope,
    ) -> Result<Vec<crate::extension::ExtensionLease>> {
        let requirements = definition
            .requirements()
            .iter()
            .filter(|requirement| requirement.starts_with("extension:"))
            .cloned()
            .collect::<Vec<_>>();
        if requirements.is_empty() {
            return Ok(Vec::new());
        }
        let runtime = self.extension_runtime.as_ref().ok_or_else(|| {
            Error::Unsupported("retained task requires a native extension runtime".into())
        })?;
        runtime.validate_admission(scope.extensions(), &mut None)?;
        runtime.retain(requirements, true)
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
    read_granted_with_work(grants, file, &mut None)
}

pub(crate) fn read_granted_with_work(
    grants: &Capabilities,
    file: &FileRef,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<bool> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
    }
    file.validate(work)?;
    if contains_content_grant_with_work(grants, file, "file:read:", work)?
        || contains_content_grant_with_work(grants, file.volume(), "volume:read:", work)?
        || contains_content_grant_with_work(
            grants, &(file.volume(), ""), "directory:read:", work,
        )?
    {
        return Ok(true);
    }
    for (slash, _) in file.path().match_indices('/') {
        let prefix = &file.path()[..slash];
        if !crate::conversation::is_internal_path(prefix)
            && contains_content_grant_with_work(
                grants, &(file.volume(), prefix), "directory:read:", work,
            )?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn contains_content_grant_with_work<T: Serialize + ?Sized>(
    grants: &Capabilities,
    value: &T,
    prefix: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<bool> {
    let digest = crate::contract::canonical_json_digest_with_work(
        value, acyclic_stream::MAX_RECORD_BYTES as u64, work,
    )?;
    let digest = blake3::Hash::from(digest).to_hex();
    let mut capability = [0; "directory:read:".len() + 64];
    let end = prefix.len() + digest.len();
    if let Some(io) = work.as_deref_mut() {
        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        io.copy_bytes(end as u64)?;
    }
    capability[..prefix.len()].copy_from_slice(prefix.as_bytes());
    capability[prefix.len()..end].copy_from_slice(digest.as_bytes());
    let capability = std::str::from_utf8(&capability[..end])
        .expect("canonical capability prefixes and hash are ASCII");
    Ok(grants.contains(capability))
}

/// Typed live group whose cancellation is independent of sibling groups.
pub struct RuntimeGroup {
    harness: Arc<AgentHarness>,
    group: TaskGroup,
    scope: RuntimeScope,
    policy_overrides: Vec<(ComponentIdentity, Arc<dyn ToolPolicy>)>,
    durable_parent: Option<TaskId>,
}

/// Ordered immutable batch selection.
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

/// Exact immutable batch request presented to a durable owner.
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
    resource_proofs: Vec<crate::resource_preparation::ResourceCreationProofRef>,
    grants: Capabilities,
    limits: Limits,
    run_limits: TaskRunLimits,
    extensions: Option<ExtensionAdmission>,
    policy: Option<ComponentIdentity>,
    execution: Option<ExecutionPlacement>,
}

impl DurableBatchRequest {
    /// Constructs and validates an exact immutable batch envelope from SDK boundary fields.
    #[allow(
        clippy::too_many_arguments,
        reason = "flat SDK-boundary constructor taking each envelope field"
    )]
    pub fn from_parts(
        group_id: GroupId,
        batch_id: BatchId,
        group_policy: BatchGroupPolicy,
        name: &str,
        version: &str,
        inputs: Vec<Value>,
        input_schema: Value,
        output_schema: Value,
        requirements: &BTreeSet<String>,
        machine_digest: &[u8],
        parent: Option<TaskId>,
        grants: Capabilities,
        limits: Limits,
        run_limits: TaskRunLimits,
        extensions: Option<ExtensionAdmission>,
        policy: Option<ComponentIdentity>,
        execution: Option<ExecutionPlacement>,
    ) -> Result<Self> {
        let (task, machine) = task_admission_identities(
            name,
            version,
            &input_schema,
            &output_schema,
            requirements,
            machine_digest,
        )?;
        let scope = RuntimeScope::new(grants, limits)?
            .with_run_limits(run_limits, &mut None)?
            .with_replayed_extensions(extensions)?;
        let request = Self {
            group_id,
            batch_id,
            group_policy,
            task,
            machine,
            inputs,
            input_schema,
            output_schema,
            parent,
            scope,
            policy,
            execution,
        };
        request.validate()?;
        Ok(request)
    }

    /// Retains proof references in a request without granting a scope or installing content handles.
    pub fn with_resource_proofs(
        mut self,
        proofs: Vec<crate::resource_preparation::ResourceCreationProofRef>,
    ) -> Result<Self> {
        if self.scope.resource_proofs != proofs {
            self.scope.created_access = None;
        }
        self.scope.resource_proofs = proofs;
        self.validate()?;
        Ok(self)
    }

    /// Validates the complete retained manifest before any child is observed or admitted.
    pub fn validate(&self) -> Result<()> {
        validate_identity(&self.task.name, &self.task.version, None)?;
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
        if !self.scope.resource_proofs.is_empty() && self.parent.is_none() {
            return Err(Error::Invalid("created batch scope has no parent".into()));
        }
        let mut resources = BTreeSet::new();
        for proof in &self.scope.resource_proofs {
            proof.validate(&mut None)?;
            if !resources.insert(crate::contract::canonical_json_bytes(&proof.resource)?) {
                return Err(Error::Invalid("batch repeats a created resource proof".into()));
            }
        }
        validate_task_schemas(&self.input_schema, &self.output_schema, true, &mut None)?;
        self.scope.limits().validate(&mut None)?;
        self.scope.run_limits().validate(&mut None)?;
        if let Some(policy) = &self.policy {
            validate_policy_identity(policy)?;
        }
        if let Some(execution) = &self.execution {
            execution.validate(&mut None)?;
        }
        for input in &self.inputs {
            validate_value(&self.input_schema, input, "batch input", None)?;
        }
        if crate::contract::canonical_json_bytes(&self.canonical_value())?.len() as u64
            > self.scope.limits().file_bytes
        {
            return Err(Error::Invalid("batch request exceeds file limit".into()));
        }
        Ok(())
    }

    /// Reconstructs only the exact v2 request stored by its owner; unknown or omitted fields cannot silently change the admitted child identities.
    pub fn from_canonical_value(value: Value) -> Result<Self> {
        let canonical = value.clone();
        let wire: DurableBatchWire =
            serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
        if wire.contract != "harness.batch.v2" {
            return Err(Error::Invalid("unsupported batch contract".into()));
        }
        let mut scope = RuntimeScope::new(wire.grants, wire.limits)?
            .with_run_limits(wire.run_limits, &mut None)?
            .with_replayed_extensions(wire.extensions)?;
        scope.resource_proofs = wire.resource_proofs;
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
            "resource_proofs": self.scope.resource_proofs(),
            "grants": self.scope.grants(),
            "limits": self.scope.limits(),
            "run_limits": self.scope.run_limits(),
            "extensions": self.scope.extensions(),
            "policy": self.policy,
            "execution": self.execution,
        })
    }

    /// Derives the sole admission record for one indexed member.
    pub fn member_admission(&self, index: usize) -> Result<TaskAdmissionRecord> {
        let input = self
            .inputs
            .get(index)
            .ok_or_else(|| Error::Invalid("batch member index is missing".into()))?;
        Ok(TaskAdmissionRecord {
            resource_proofs: self.scope.resource_proofs().to_vec(),
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

/// One exact input slot, preserving the admission decision before and after terminal observation without introducing another key-bearing shape.
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
    extensions: Option<Vec<crate::extension::ExtensionLease>>,
}

/// Ordered outcome of all observable batch slots.
pub struct BatchOutcome<O> {
    /// Stable group identity.
    pub group_id: GroupId,
    /// Stable batch identity.
    pub batch_id: BatchId,
    /// One result for every submitted input.
    pub entries: Vec<BatchEntry<Result<Outcome<O>>>>,
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

/// Per-member request report for a retained batch.
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

impl<O> BatchOutcome<O> {
    /// Reports whether every admission, outcome, and cancellation is definitive.
    pub fn is_complete(&self) -> bool {
        self.entries.iter().all(|entry| match &entry.admission {
            Admission::Accepted(Ok(
                Outcome::Succeeded(_) | Outcome::Failed { .. } | Outcome::Cancelled,
            ))
            | Admission::Rejected { .. } => true,
            Admission::Accepted(Ok(Outcome::Indeterminate { .. }) | Err(_))
            | Admission::Indeterminate { .. } => false,
        }) && self.cancellation.as_ref().is_none_or(|result| {
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
}

impl<O: DeserializeOwned + Send + 'static> AdmissionBatch<O> {
    /// Observes accepted members concurrently while preserving every original input slot, including rejected and unresolved admissions.
    pub async fn join(self) -> BatchOutcome<O> {
        let Self {
            group_id,
            batch_id,
            entries,
            policy,
            spawner,
            extensions,
        } = self;
        let _extensions = extensions;
        let count = entries.len();
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
        BatchOutcome {
            group_id,
            batch_id,
            entries,
            cancellation,
        }
    }
}

/// A typed ordered map cannot silently discard a rejected admission, failed child, or unknown outcome.
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
                None,
            )
            .await
    }

    /// Admits independent live tasks in input order without waiting for their results.
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

    /// Admits all independent inputs before awaiting any result, then returns either ordered outputs or every input's exact outcome.
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

    /// Observes admitted members in input order.
    pub async fn join<O: DeserializeOwned + Send + 'static>(
        &self,
        tasks: Vec<RuntimeTask<O>>,
    ) -> Vec<Result<Outcome<O>>> {
        join_runtime(tasks).await
    }

    /// Collects the first `required` successes, or reports all non-successes.
    pub async fn quorum<O: DeserializeOwned + Send + 'static>(
        &self,
        tasks: Vec<RuntimeTask<O>>,
        required: usize,
    ) -> std::result::Result<Vec<O>, QuorumFailure<O>> {
        if required == 0 {
            return Ok(Vec::new());
        }
        let concurrency = tasks.len().clamp(1, 64);
        let completions = stream::iter(tasks).map(|task| async move {
            let id = task.identity();
            (id, task.result().await)
        }).buffer_unordered(concurrency);
        futures::pin_mut!(completions);
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

    /// Closes admission while already accepted members and descendants drain.
    pub fn close(&self) {
        self.group.close();
    }

}

#[derive(Clone)]
enum ToolTaskContext {
    Ordinary(TaskContext),
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Native(Arc<NativeTaskContext>),
}

/// A task context bound to one exact tool call identity.
pub struct ToolContext {
    task: ToolTaskContext,
    operation_id: OperationId,
    call_id: String,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_interactions: Option<Arc<dyn InteractionRouter>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    effective_scope: Option<(RuntimeScope, Option<acyclic_fs::NativeAllocation>)>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    accepted_native_tool: std::sync::OnceLock<(TaskId, OperationId)>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl Clone for ToolContext {
    fn clone(&self) -> Self {
        Self {
            task: self.task.clone(),
            operation_id: self.operation_id,
            call_id: self.call_id.clone(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_interactions: self.native_interactions.clone(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            effective_scope: self.effective_scope.as_ref().map(|(scope, _)| (scope.clone(), None)),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            accepted_native_tool: std::sync::OnceLock::new(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        }
    }
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
            task: ToolTaskContext::Ordinary(task),
            operation_id,
            call_id,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_interactions: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            effective_scope: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            accepted_native_tool: std::sync::OnceLock::new(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        })
    }

    /// Returns the enclosing scoped task context.
    #[must_use]
    pub fn task(&self) -> &TaskContext {
        match &self.task {
            ToolTaskContext::Ordinary(task) => task,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            ToolTaskContext::Native(task) => task.context(),
        }
    }

    /// Borrows this tool's attenuated authority without replacing its task.
    #[must_use]
    pub fn effective_scope(&self) -> &RuntimeScope {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some((scope, _)) = &self.effective_scope {
            return scope;
        }
        self.task().scope()
    }

    /// Returns pending approval through this command's protected route.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) async fn authorize_tool(
        &self, definition: &ToolDefinition, invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<OperationId>> {
        let task = self.task();
        task.require_original_native_work(work)?;
        task.require_native_scope_restriction(self.effective_scope(), work)?;
        if invocation.operation_id != self.operation_id() || invocation.call_id != self.call_id() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native policy invocation differs from its original command" });
        }
        task.authorize_tool_policies(
            definition, invocation, Some(work), self.stock_native_interactions(), self.effective_scope(),
        ).await
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
        self.task()
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl ToolContext {
    /// Checks effective READ and limits before replay or content IO.
    pub fn authorize_file_read_with_native_work(&self, file: &FileRef,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.task().authorize_native_file_read(file, self.effective_scope(),
            &crate::filesystem::PreparationIo::from_native_work(work)?)
    }

    /// Checks effective WRITE through the original bound destination.
    pub fn authorize_content_publication_with_native_work(&self, source: Option<&FileRef>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let task = self.task();
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        if let Some(source) = source { task.authorize_native_file_read(source, self.effective_scope(), &io)?; }
        let writer = task.bound_content_writer_measured(task.content_binding(source.map(FileRef::volume)),
            self.effective_scope(), &io)?;
        if source.is_some_and(|source| source.volume() != writer.volume()) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "source differs from the owner-bound destination" });
        }
        Ok(())
    }

    /// Reads original content under the effective tool scope.
    pub async fn read_file_with_native_work(&self, file: &FileRef,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_stream::OwnedBytes> {
        self.task().read_native_file(file, self.effective_scope(), work).await
    }

    /// Publishes once under the effective tool scope.
    pub async fn stage_file_once_with_native_work(&self, operation_id: OperationId, path: &str,
        bytes: &[u8], media_type: &str, display_name: &str, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(FileRef, acyclic_fs::NativeAllocation)> {
        self.task().stage_native_file(operation_id, path, bytes, media_type, display_name, None,
            self.effective_scope(), work).await
    }

    /// Replaces the pinned source generation under the effective tool scope.
    pub async fn stage_file_at_with_native_work(&self, operation_id: OperationId, source: &FileRef,
        bytes: &[u8], work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(FileRef, acyclic_fs::NativeAllocation)> {
        self.task().stage_native_file(operation_id, source.path(), bytes, source.descriptor().media_type(),
            source.display_name(), Some(source), self.effective_scope(), work).await
    }

    /// Observes the original publication under effective WRITE and limits.
    pub async fn observe_staged_file_once_with_native_work(&self, operation_id: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<FileRef>, acyclic_fs::NativeAllocation)> {
        let task = self.task();
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let scope = self.effective_scope();
        let writer = task.bound_content_writer_measured(task.content_binding(None), scope, &io)?;
        task.scope.verify_created_write(writer.volume(), Some(&io)).await?;
        let (file, hold) = writer.observe_staged_once(operation_id, scope.limits.file_bytes, Some(&io))?.await?;
        task.scope.verify_created_write(writer.volume(), Some(&io)).await?;
        if let Some(file) = &file {
            scope.limits.validate_file(file, &mut Some(&mut io.history_io()))?;
            if file.volume() != writer.volume() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage,
                    reason: "content publisher observed another volume" });
            }
        }
        let retained = hold.as_ref().map_or(0, |hold| hold.bytes());
        if let Some(hold) = hold { hold.into_retained(); }
        Ok((file, io.take_native_custody(retained)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef};
    use crate::model::{ModelContent, ModelContentPart};
    use crate::resources::ProviderRef;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn retained_proof_copy_input() -> crate::resource_preparation::ResourceCreationProofRef {
        use crate::resources::{ResourceKind, ResourceRef};
        let provider = ProviderRef::new("org.example".into(), "filesystem".into(), "1".into(), &mut None)
            .expect("validated SDK provider reference");
        let resource = ResourceRef::new(
            ResourceKind::Artifact, provider.clone(), b"resource-key".to_vec().into(), Some("v1".into()), &mut None
        ).expect("validated SDK resource reference");
        let evidence = ResourceRef::new(
            ResourceKind::Artifact, provider.clone(), b"evidence-key".to_vec().into(), Some("v2".into()), &mut None
        ).expect("validated SDK evidence reference");
        crate::resource_preparation::ResourceCreationProofRef {
            provider, preparation: TaskId::new(), resource, evidence,
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_proof_vector_copy_reports_actual_prefix_and_cleans_nested_denial() {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork,
            NativeTreeMaterializationError, WorkBudget, WorkError};
        let proofs = [retained_proof_copy_input()];
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget {
            allocation_operations: 32,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 8192,
            bytes_copied: 1,
            items_examined: 32,
            ..WorkBudget::default()
        }, &cancellation).expect("original field-copy ledger");
        let mut io = crate::store::history::native_work_io(&work)
            .expect("original copy observer");
        assert!(matches!(RuntimeScope::copy_resource_proofs(&proofs,
            &mut crate::wire_codec::CodecQuota::from_retained(&mut Some(&mut io), 0)), Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Work(
                WorkError::BudgetExceeded { counter: "bytes_copied", .. }),
            ..
        })));
        assert_eq!(work.receipt().bytes_copied, 0);
        assert_eq!(io.into_custody().bytes(), 0);
        let mut io = crate::store::history::native_work_io(&work)
            .expect("fresh custody on the same original ledger");
        cancellation.cancel();
        assert!(matches!(RuntimeScope::copy_resource_proofs(&proofs,
            &mut crate::wire_codec::CodecQuota::from_retained(&mut Some(&mut io), 0)), Err(Error::NativeWork { cause: NativeTreeMaterializationError::Cancelled, .. })));
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_root_moves_original_caps_without_sealing_or_minting_proofs() {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget {
            allocation_operations: 2,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 4096,
            bytes_copied: 4,
            items_examined: 1,
            ..WorkBudget::default()
        }, &cancellation).expect("original ledger");
        let paid = Capabilities::new_with_native_work(["read"], &work)
            .expect("original grants");
        let original_bytes = paid.1.bytes();
        let prefix = work.receipt();
        let (scope, custody) = RuntimeScope::new_with_native_work(
            paid, Limits::default(), &work,
        ).expect("native root");
        assert_eq!(scope.grants().iter().collect::<Vec<_>>(), ["read"]);
        assert_eq!(scope._native_grants_custody.as_ref().unwrap().bytes(), original_bytes);
        assert!(scope._native_grants_custody.as_ref().unwrap().work().shares_ledger(&work));
        assert_eq!(custody.bytes(), std::mem::size_of::<RuntimeScope>() as u64);
        assert_eq!(work.receipt().bytes_copied, prefix.bytes_copied);
        assert_eq!(work.receipt().allocation_operations, prefix.allocation_operations);
        assert!(!scope.is_sealed());
        assert!(scope.resource_proofs().is_empty());
        assert!(scope.created_access().is_none());
        let ordinary = scope.clone();
        assert!(ordinary._native_grants_custody.is_none());
        assert_eq!(ordinary.grants(), scope.grants());
        drop(ordinary);
        drop(scope);
        drop(custody);
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_scope_keeps_independent_input_custody_and_scalar_denial_codes() {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
        let cancellation = CancellationToken::new();
        let budget = WorkBudget {
            allocation_operations: 2,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 4096,
            bytes_copied: 4,
            items_examined: 1,
            ..WorkBudget::default()
        };
        let original = NativeMaterializationWork::new(budget, &cancellation)
            .expect("original grants ledger");
        let construction = NativeMaterializationWork::new(budget, &cancellation)
            .expect("independent construction ledger");
        let paid = Capabilities::new_with_native_work(["read"], &original)
            .expect("paid input");
        let (scope, custody) = RuntimeScope::new_with_native_work(
            paid, Limits::default(), &construction,
        ).expect("independent guards are legitimate");
        assert!(scope._native_grants_custody.as_ref().unwrap().work().shares_ledger(&original));
        assert!(custody.work().shares_ledger(&construction));
        let invalid = Limits { file_bytes: 0, ..Limits::default() };
        assert!(matches!(crate::store::native_work_io(&construction).map_err(|cause| crate::Error::NativeWork { cause, custody: None }).and_then(|mut io| (&invalid).validate(&mut Some(&mut io))),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. })));
        let scope = crate::store::native_work_io(&construction).map_err(|cause| crate::Error::NativeWork { cause, custody: None }).and_then(|mut io| scope.with_run_limits(TaskRunLimits { concurrency: Some(1), ..TaskRunLimits::default() }, &mut Some(&mut io))).expect("bounded concurrency");
        assert!(matches!(crate::store::native_work_io(&construction).map_err(|cause| crate::Error::NativeWork { cause, custody: None }).and_then(|mut io| scope.with_run_limits(TaskRunLimits::default(), &mut Some(&mut io))), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        drop(custody);
        cancellation.cancel();
        assert!(matches!(crate::store::native_work_io(&construction).map_err(|cause| crate::Error::NativeWork { cause, custody: None }).and_then(|mut io| (&Limits::default()).validate(&mut Some(&mut io))), Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_scope_clone_and_narrow_preserve_readonly_grants_and_original_bounds() {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget {
            allocation_operations: 8,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 8192,
            bytes_copied: 64,
            items_examined: 16,
            ..WorkBudget::default()
        }, &cancellation).expect("original ledger");
        let (root, root_custody) = RuntimeScope::new_with_native_work(
            Capabilities::new_with_native_work(["read"], &work).expect("paid read-only grants"),
            Limits::default(), &work,
        ).expect("read-only root");
        let root = crate::store::native_work_io(&work).map_err(|cause| crate::Error::NativeWork { cause, custody: None }).and_then(|mut io| root.with_run_limits(TaskRunLimits { max_steps: Some(2), ..TaskRunLimits::default() }, &mut Some(&mut io))).expect("original execution bound");
        let (copy, copy_custody) = root.clone_with_native_work(&work).expect("native clone");
        assert_eq!(copy.grants(), root.grants());
        assert_eq!(copy.run_limits(), root.run_limits());
        assert!(!copy.is_sealed());
        assert!(copy.resource_proofs().is_empty());
        assert!(copy.created_access().is_none());
        assert!(!copy.grants().contains("write"));
        assert!(copy._native_grants_custody.as_ref().unwrap().work().shares_ledger(&work));
        let narrower = Limits { file_bytes: 1, ..Limits::default() };
        let (child, child_custody) = root.narrow_with_native_work(
            Capabilities::new_with_native_work(["read"], &work).expect("paid child grants"),
            narrower, &work,
        ).expect("native attenuation");
        assert!(child.is_sealed());
        assert_eq!(child.limits(), narrower);
        assert_eq!(child.run_limits(), root.run_limits());
        assert!(child.resource_proofs().is_empty());
        assert!(!child.grants().contains("write"));
        assert!(matches!(root.narrow_with_native_work(
            Capabilities::new_with_native_work(["write"], &work).expect("requested widening"),
            Limits::default(), &work,
        ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(root.grants().iter().collect::<Vec<_>>(), ["read"]);
        assert_eq!(root.limits(), Limits::default());
        drop(child);
        drop(child_custody);
        drop(copy);
        drop(copy_custody);
        drop(root);
        drop(root_custody);
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_session_scalar_validation_preserves_the_original_policy() {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
        use crate::scheduler::SessionLimits;
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget {
            allocation_operations: 1,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes(),
            ..WorkBudget::default()
        }, &cancellation).expect("scalar validator original ledger");
        let valid = SessionLimits { active_tasks: 1, total_tasks: 2, depth: 0, model_steps: 3 };
        let too_large = crate::conversation::MAX_EXACT_JS_INTEGER + 1;
        for limits in [
            valid,
            SessionLimits { depth: u32::MAX, ..valid },
            SessionLimits { active_tasks: 0, ..valid },
            SessionLimits { total_tasks: 0, ..valid },
            SessionLimits { model_steps: 0, ..valid },
            SessionLimits { active_tasks: 3, ..valid },
            SessionLimits { active_tasks: too_large, total_tasks: too_large, ..valid },
            SessionLimits { total_tasks: too_large, ..valid },
            SessionLimits { model_steps: too_large, ..valid },
        ] {
            let native = RuntimeScope::validate_session_limits_with_native_work(&limits, &work);
            let allowed = limits.validate(&mut None).is_ok();
            assert_eq!(native.is_ok(), allowed);
            if !allowed {
                assert!(matches!(native, Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    ..
                })));
            }
        }
        cancellation.cancel();
        assert!(matches!(RuntimeScope::validate_session_limits_with_native_work(&valid, &work),
            Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
    }

    fn generated_interface_fields(source: &str, name: &str) -> BTreeSet<String> {
        let header = format!("export interface {name} {{");
        let bytes = source.as_bytes();
        let Some(header_offset) = bytes
            .windows(header.len())
            .position(|window| window == header.as_bytes())
        else {
            return BTreeSet::new();
        };
        let start = header_offset + header.len();
        let mut depth = 1_u32;
        let Some(end) = bytes[start..]
            .iter()
            .enumerate()
            .find_map(|(offset, byte)| {
                match byte {
                    b'{' => depth += 1,
                    b'}' => depth -= 1,
                    _ => {}
                }
                (depth == 0).then_some(offset)
            })
        else {
            return BTreeSet::new();
        };
        let body = std::str::from_utf8(&bytes[start..start + end]).unwrap_or_default();
        body.lines()
            .filter_map(|line| {
                let field = line.trim().strip_prefix("readonly ")?;
                Some(field.split_once(':')?.0.trim().to_owned())
            })
            .collect()
    }

    fn serialized_object_fields(value: &Value) -> BTreeSet<String> {
        value
            .as_object()
            .map(|fields| fields.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn dependency_fixture(
        requirements: &[(&str, &[&str])],
    ) -> BTreeMap<(String, String), BTreeSet<String>> {
        requirements
            .iter()
            .map(|(name, requirements)| {
                (
                    (String::from(*name), String::from("1")),
                    requirements
                        .iter()
                        .map(|value| String::from(*value))
                        .collect(),
                )
            })
            .collect()
    }

    #[test]
    fn task_requirement_projection_rejects_missing_versions_and_cycles() {
        let tools = BTreeSet::from([(String::from("tool"), String::from("2"))]);
        let environment = TaskDependencyEnvironment::default();
        let grants = BTreeSet::new();
        let missing = dependency_fixture(&[("parent", &["task:leaf@1"])]);
        assert!(validate_task_requirements(&missing, &tools, &environment, &grants, None).is_err());

        let cycle = dependency_fixture(&[("left", &["task:right@1"]), ("right", &["task:left@1"])]);
        let result = validate_task_requirements(&cycle, &tools, &environment, &grants, None);
        assert!(result.is_err());
    }

    #[test]
    fn native_task_requirements_keep_provider_grants_bound() -> Result<()> {
        let definition =
            TaskDefinition::live("provider-bound", "1", |_context, _input: ()| async {
                Ok::<(), Error>(())
            })?
            .requires("model")?
            .requires("context")?
            .requires("interactions")?;
        let mut tasks = TaskRegistry::default();
        tasks.register(definition)?;
        let tools = ToolRegistry::default();
        let all_grants = ["model:generate", "context:build", "interaction:route"];
        let complete_scope = RuntimeScope::new(
            Capabilities::new(all_grants.iter().copied()),
            Limits::default(),
        )?;
        assert!(
            validate_task_dependencies(
                &tasks,
                &tools,
                &ResumableToolRegistry::default(),
                &complete_scope,
                false,
                false,
                false,
                true,
                None,
                None,
                false,
                None,
            )
            .is_ok()
        );

        for missing_grant in all_grants {
            let scope = RuntimeScope::new(
                Capabilities::new(
                    all_grants
                        .iter()
                        .copied()
                        .filter(|grant| *grant != missing_grant),
                ),
                Limits::default(),
            )?;
            assert!(
                validate_task_dependencies(
                    &tasks,
                    &tools,
                    &ResumableToolRegistry::default(),
                    &scope,
                    false,
                    false,
                    false,
                    true,
                    None,
                    None,
                    false,
                    None,
                )
                .is_err()
            );
        }
        Ok(())
    }

    #[test]
    fn native_task_requirements_accept_state_without_spawner() -> Result<()> {
        let state_task = TaskDefinition::live("state-only", "1", |_context, _input: ()| async {
            Ok::<(), Error>(())
        })?
        .requires("state")?;
        let mut bindings = Bindings::local();
        bindings.tasks.register(state_task)?;
        bindings.state = Some(Arc::new(StateOnlyProvider));
        assert!(bindings.build().is_ok());
        Ok(())
    }

    #[test]
    fn task_requirement_projection_checks_grants_and_extensions() {
        let tasks =
            dependency_fixture(&[("root", &["grant:task:run", "extension:example.state@3"])]);
        let mut environment = TaskDependencyEnvironment::default();
        environment
            .extensions
            .insert((String::from("example.state"), 3));
        let grants = BTreeSet::from([String::from("task:run")]);
        assert!(
            validate_task_requirements(&tasks, &BTreeSet::new(), &environment, &grants, None).is_ok()
        );
        environment.extensions.clear();
        assert!(
            validate_task_requirements(&tasks, &BTreeSet::new(), &environment, &grants, None).is_err()
        );
    }

    fn generated_wasm_declarations() -> Result<Option<String>> {
        let workspace_typescript =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../typescript");
        let path =
            workspace_typescript.join("packages/harness/generated/wasm/acyclic_harness_wasm.d.ts");
        match std::fs::read_to_string(path) {
            Ok(source) => Ok(Some(source)),
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && !workspace_typescript.is_dir() =>
            {
                Ok(None)
            }
            Err(error) => Err(Error::Storage(format!(
                "generated WASM declaration is unavailable: {error}"
            ))),
        }
    }

    #[test]
    fn generated_admission_fields_match_serialized_contracts() -> Result<()> {
        let Some(source) = generated_wasm_declarations()? else {
            eprintln!(
                "skipping generated WASM declaration freshness check: TypeScript package is absent"
            );
            return Ok(());
        };
        let schema = serde_json::json!({"type": "integer"});
        let requirements = BTreeSet::new();
        let mut extension_value: Value =
            serde_json::from_str(include_str!("../fixtures/v2/extension-admission.json"))
                .map_err(|error| Error::Invalid(error.to_string()))?;
        extension_value["selected"] = serde_json::json!([
            { "name": "example.state", "version": 1 }
        ]);
        extension_value["configurations"] = serde_json::json!([serde_json::from_str::<Value>(
            include_str!("../fixtures/v2/extension-configuration.json")
        )
        .map_err(|error| Error::Invalid(error.to_string()))?]);
        let extensions: ExtensionAdmission = serde_json::from_value(extension_value)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        extensions.validate(&mut None)?;
        let policy = ComponentIdentity {
            name: "fixture.policy".into(),
            version: "1".into(),
            digest: [45; 32],
        };
        let execution: ExecutionPlacement =
            serde_json::from_str(include_str!("../fixtures/v2/execution-placement.json"))
                .map_err(|error| Error::Invalid(error.to_string()))?;
        execution.validate(&mut None)?;
        let task = TaskAdmissionRecord::from_parts(
            OperationId::from_bytes([41; 16]),
            "fixture.task",
            "1",
            serde_json::json!(7),
            schema.clone(),
            schema.clone(),
            &requirements,
            &[42; 32],
            None,
            Capabilities::new([] as [String; 0]),
            Limits::default(),
            TaskRunLimits {
                concurrency: None,
                max_steps: None,
                deadline_epoch_ms: None,
            },
            Some(policy.clone()),
            Some(extensions.clone()),
            Some(execution.clone()),
        )?;
        let batch = DurableBatchRequest::from_parts(
            GroupId::from_bytes([43; 16]),
            BatchId::from_bytes([44; 16]),
            BatchGroupPolicy::CollectAll,
            "fixture.task",
            "1",
            vec![serde_json::json!(7), serde_json::json!(8)],
            schema.clone(),
            schema,
            &requirements,
            &[42; 32],
            None,
            Capabilities::new([] as [String; 0]),
            Limits::default(),
            TaskRunLimits {
                concurrency: None,
                max_steps: None,
                deadline_epoch_ms: None,
            },
            Some(extensions),
            Some(policy),
            Some(execution),
        )?;
        let task_wire = task.canonical_value();
        let batch_wire = batch.canonical_value();
        for (name, value) in [
            ("WasmTaskAdmissionWire", &task_wire),
            ("WasmDurableBatchWire", &batch_wire),
        ] {
            assert_eq!(
                serialized_object_fields(value),
                generated_interface_fields(&source, name),
                "generated declaration drift for serialized {name}"
            );
        }
        for (name, value) in [
            ("WasmMachineIdentityWire", &task_wire["task"]),
            ("WasmMachineIdentityWire", &task_wire["machine"]),
            ("WasmNativeLimitsWire", &task_wire["limits"]),
            ("WasmTaskRunLimitsWire", &task_wire["run_limits"]),
            ("WasmMachineIdentityWire", &batch_wire["task"]),
            ("WasmMachineIdentityWire", &batch_wire["machine"]),
            ("WasmNativeLimitsWire", &batch_wire["limits"]),
            ("WasmTaskRunLimitsWire", &batch_wire["run_limits"]),
        ] {
            assert_eq!(
                serialized_object_fields(value),
                generated_interface_fields(&source, name),
                "generated declaration drift for serialized nested {name}"
            );
        }
        for (name, value) in [
            ("WasmMachineIdentityWire", &task_wire["policy"]),
            ("WasmExtensionAdmissionWire", &task_wire["extensions"]),
            ("WasmEventReferenceWire", &task_wire["extensions"]["source"]),
            (
                "WasmAuthorityWire",
                &task_wire["extensions"]["source"]["authority"],
            ),
            (
                "WasmExtensionDependencyWire",
                &task_wire["extensions"]["selected"][0],
            ),
            (
                "WasmExtensionConfigurationWire",
                &task_wire["extensions"]["configurations"][0],
            ),
            (
                "WasmFileRefWire",
                &task_wire["extensions"]["configurations"][0]["content"],
            ),
            (
                "WasmVolumeRefWire",
                &task_wire["extensions"]["configurations"][0]["content"]["volume"],
            ),
            (
                "WasmAgentVolumeOwnerWire",
                &task_wire["extensions"]["configurations"][0]["content"]["volume"]["owner"],
            ),
            (
                "WasmProviderRefWire",
                &task_wire["extensions"]["configurations"][0]["content"]["volume"]["provider"],
            ),
            (
                "WasmFileDescriptorWire",
                &task_wire["extensions"]["configurations"][0]["content"]["descriptor"],
            ),
            ("WasmExecutionPlacementWire", &task_wire["execution"]),
            (
                "WasmMachineIdentityWire",
                &task_wire["execution"]["provider"],
            ),
            ("WasmResourceRefWire", &task_wire["execution"]["build"]),
            (
                "WasmResourceRefWire",
                &task_wire["execution"]["environment"],
            ),
        ] {
            assert_eq!(
                serialized_object_fields(value),
                generated_interface_fields(&source, name),
                "generated declaration drift for populated nested {name}"
            );
        }
        Ok(())
    }

    struct CompletedModel {
        requests: std::sync::Mutex<Vec<(crate::model::ModelDispatch, ModelRequest)>>,
    }

    impl ModelProvider for CompletedModel {
        fn context_capacity(&self, _: &Model) -> Result<crate::context::ModelContextCapacity> {
            Ok(crate::context::ModelContextCapacity {
                context_tokens: 131_072,
                output_tokens: 4_096,
            })
        }

        fn count_tokens(
            &self,
            request: &crate::model::PreparedModelRequest,
        ) -> Result<crate::context::ModelTokenCount> {
            Ok(crate::context::ModelTokenCount {
                request_digest: request.request_digest(),
                fixed_tokens: u32::try_from(request.bytes().len())
                    .map_err(|_| Error::Invalid("fixture token count overflow".into()))?,
                message_tokens: vec![0; request.request().messages.len()],
            })
        }

        fn generate<'a>(
            &'a self,
            request: crate::model::PreparedModelRequest,
            dispatch: crate::model::ModelDispatch,
        ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
            assert_eq!(dispatch.request_digest, request.request_digest());
            let request = request.request().clone();
            let Ok(mut requests) = self.requests.lock() else {
                return Box::pin(futures::stream::iter([Err(Error::Storage(
                    "test model lock poisoned".into(),
                ))]));
            };
            requests.push((dispatch, request));
            Box::pin(futures::stream::iter([Ok(ModelEvent::Completed {
                metadata: Value::Null,
            })]))
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
            ToolRegistry::default(),
            scope.clone(),
            2,
            None,
        )?
        .bind_model(
            Model::new("root".into(), "model".into(), "1".into(), Value::Null)?,
            root_provider.clone(),
        )?;
        let context = TaskContext {
            harness,
            task_id: OperationId::new(),
            durable_task: None,
            descendants: TaskGroup::new(2, None).unwrap(),
            scope,
            policy_overrides: Vec::new(),
            owned_interactions: None,
            owned_mailbox: None,
            output_content: None,
            #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
            native_root_output: std::sync::OnceLock::new(),
            model_steps: Arc::new((AtomicUsize::new(0), None)),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata_source: None,
        };
        let message = ModelMessage {
            role: crate::model::ModelRole::User,
            content: crate::model::ModelContent::Text("hello".into()),
        };
        context
            .clone()
            .model_events(vec![message.clone()], None)
            .await?;
        let child = context.scoped_model(
            Capabilities::new(["model:generate"]),
            Limits::default(),
            Model::new("child".into(), "model".into(), "2".into(), Value::Null)?,
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
                .1
                .model
                .provider,
            "root"
        );
        assert_eq!(
            child_provider
                .requests
                .lock()
                .unwrap_or_else(|error| error.into_inner())[0]
                .1
                .model
                .provider,
            "child"
        );
        let root_dispatch = root_provider.requests.lock().unwrap()[0].0;
        let child_dispatch = child_provider.requests.lock().unwrap()[0].0;
        assert_eq!(root_dispatch.operation_id, context.task_id);
        assert_eq!(child_dispatch.operation_id, context.task_id);
        assert_eq!(root_dispatch.step, 0);
        assert_eq!(child_dispatch.step, 1);
        let foreign = FileRef::new(
            VolumeRef::new(
                ProviderRef::new("other".into(), "filesystem".into(), "2".into(), &mut None)?,
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
                            policy: crate::model::FileProjectionPolicy::Native(Box::new(
                                crate::model::NativeMediaPolicy {
                                    intent: crate::model::NativeMediaIntent::Image {
                                        detail: crate::model::ImageDetail::Auto
                                    },
                                    maximum_bytes: crate::conversation::MAX_EXACT_JS_INTEGER,
                                    maximum_work: 4096,
                                    configuration: None
                                }
                            )),
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

        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        fn supports_native_policy(&self, _work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
            Ok(())
        }

        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        fn identity_with_native_work(
            &self, work: &acyclic_fs::NativeMaterializationWork,
        ) -> Result<(ComponentIdentity, acyclic_fs::NativeAllocation)> {
            native_fixture_policy_identity(&self.identity, work)
        }

        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        fn evaluate_with_native_work<'a>(
            &'a self, _invocation: &'a ToolInvocation, _scope: &'a RuntimeScope,
            work: &'a acyclic_fs::NativeMaterializationWork,
        ) -> Result<crate::NativeFutureBundle<'a, (ToolPolicyDecision, acyclic_fs::NativeAllocation)>> {
            crate::NativeFutureBundle::new(work, || async move {
                use crate::store::history::HistoryIo;
                let mut io = crate::store::history::native_work_io(work)?;
                let value = match &self.decision {
                    ToolPolicyDecision::Allow => ToolPolicyDecision::Allow,
                    ToolPolicyDecision::Deny { reason } => ToolPolicyDecision::Deny {
                        reason: io.clone_text(reason)?,
                    },
                    ToolPolicyDecision::RequireApproval { prompt } => ToolPolicyDecision::RequireApproval {
                        prompt: io.clone_text(prompt)?,
                    },
                };
                Ok((value, io.into_custody()))
            })
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_fixture_policy_identity(
        identity: &ComponentIdentity, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ComponentIdentity, acyclic_fs::NativeAllocation)> {
        use crate::store::history::HistoryIo;
        let mut io = crate::store::history::native_work_io(work)?;
        io.retain_owned(std::mem::size_of::<ComponentIdentity>() as u64)?;
        let name = io.clone_text(&identity.name)?;
        let version = io.clone_text(&identity.version)?;
        Ok((ComponentIdentity { name, version, digest: identity.digest }, io.into_custody()))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_policy_fixture_inputs() -> (FixedPolicy, ToolDefinition, ToolInvocation) {
        let policy = FixedPolicy {
            identity: ComponentIdentity { name: "test.policy".into(), version: "1".into(), digest: [9; 32] },
            decision: ToolPolicyDecision::RequireApproval { prompt: "Approve λ 🦀?".into() },
        };
        let definition = ToolDefinition {
            name: "test.echo".into(), revision: "1".into(), description: "Echo".into(),
            input_schema: serde_json::json!({"type":"object"}),
            output_schema: serde_json::json!({}),
            projection_schema: crate::tool::json_projection_schema(serde_json::json!({})),
        };
        let invocation = ToolInvocation {
            operation_id: OperationId::new(), call_id: "provider-call".into(),
            name: definition.name.clone(),
            arguments: serde_json::json!({"z":[true,null],"a":"λ"}),
        };
        (policy, definition, invocation)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_policy_approval_keeps_exact_binding_and_paid_utf8_prompt() -> Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
            .expect("original policy ledger");
        let (policy, definition, invocation) = native_policy_fixture_inputs();
        let scope = RuntimeScope::new(Capabilities::new(["tool:call:test.echo"]), Limits::default())?;
        let before = work.receipt().allocation_operations;
        let (mut identity, custody) = policy.identity_with_native_work(&work)?;
        assert_eq!(custody.bytes(), std::mem::size_of::<ComponentIdentity>() as u64
            + identity.name.capacity() as u64 + identity.version.capacity() as u64);
        assert_eq!(work.receipt().allocation_operations, before + 2);
        validate_policy_identity_with_native_work(&identity, &custody, &work)?;
        identity.name.clear();
        let observed = work.receipt();
        assert!(matches!(validate_policy_identity_with_native_work(&identity, &custody, &work),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason: "policy name is invalid" })));
        assert_eq!(work.receipt(), observed);
        drop((identity, custody));
        let (decision, custody) = policy.evaluate_with_native_work(&invocation, &scope, &work)?.await?;
        let ToolPolicyDecision::RequireApproval { prompt } = decision else { panic!("fixture approval"); };
        assert_eq!(custody.bytes(), prompt.capacity() as u64);
        let pointer = prompt.as_ptr();
        let task = TaskId::new();
        let passive = tool_approval_binding_with_native_work(task, &policy.identity, &definition, &invocation, &work)?;
        assert_eq!(passive, tool_approval_binding(task, &policy.identity, &definition, &invocation)?);
        let expected = tool_approval_request(task, &policy.identity, &definition, &invocation, prompt.clone())?;
        let (actual, custody) = tool_approval_request_with_native_work(
            task, &policy.identity, &definition, &invocation, prompt, custody, &work,
        )?;
        assert_eq!(actual, expected);
        let Interaction::Approval { prompt, .. } = &actual.1 else { panic!("approval request"); };
        assert_eq!(prompt.as_ptr(), pointer);
        assert!(custody.work().shares_ledger(&work));
        assert!(work.receipt().bytes_hashed > 32);
        let paid = (actual, custody);
        let live = work.live_allocation_bytes();
        assert!(live >= acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + paid.1.bytes());
        drop(paid);
        assert_eq!(work.live_allocation_bytes(), acyclic_fs::NativeMaterializationWork::accounting_state_bytes());
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_policy_identity_denies_cancelled_and_exhausted_copy_before_strings() -> Result<()> {
        let (policy, _, _) = native_policy_fixture_inputs();
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut budget = acyclic_fs::WorkBudget::default();
        budget.bytes_copied = 0;
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
        let before = work.receipt();
        assert!(matches!(policy.identity_with_native_work(&work), Err(Error::NativeWork { .. })));
        assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(work.receipt().bytes_copied, 0);
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
            .expect("cancelled-policy original ledger");
        cancellation.cancel();
        assert!(matches!(policy.identity_with_native_work(&work), Err(Error::NativeWork { .. })));
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_policy_approval_rejects_foreign_prompt_before_encoding() -> Result<()> {
        use crate::store::history::HistoryIo;
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
            .expect("original ledger");
        let foreign = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
            .expect("foreign ledger");
        let (policy, definition, invocation) = native_policy_fixture_inputs();
        let mut io = crate::store::history::native_work_io(&foreign)?;
        let prompt = io.clone_text("Foreign prompt")?;
        let custody = io.into_custody();
        let before = original.receipt();
        assert!(matches!(tool_approval_request_with_native_work(
            TaskId::new(), &policy.identity, &definition, &invocation, prompt, custody, &original,
        ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, .. })));
        assert_eq!(original.receipt(), before);
        assert_eq!(foreign.live_allocation_bytes(), acyclic_fs::NativeMaterializationWork::accounting_state_bytes());
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn opaque_policy_native_defaults_never_call_ordinary_body() -> Result<()> {
        struct OpaquePolicy;
        impl ToolPolicy for OpaquePolicy {
            fn identity(&self) -> ComponentIdentity { panic!("ordinary identity must not run"); }
            fn evaluate<'a>(
                &'a self, _: &'a ToolInvocation, _: &'a RuntimeScope,
            ) -> BoxFuture<'a, Result<ToolPolicyDecision>> { panic!("ordinary evaluator must not run"); }
        }
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
            .expect("original ledger");
        let (_, _, invocation) = native_policy_fixture_inputs();
        let scope = RuntimeScope::new(Capabilities::new(["tool:call:test.echo"]), Limits::default())?;
        let before = work.receipt();
        assert!(matches!(OpaquePolicy.supports_native_policy(&work),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, .. })));
        assert!(matches!(OpaquePolicy.identity_with_native_work(&work),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, .. })));
        assert!(matches!(OpaquePolicy.evaluate_with_native_work(&invocation, &scope, &work),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, .. })));
        assert_eq!(work.receipt(), before);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn opaque_interaction_router_rejects_native_before_ordinary_route_or_box() -> Result<()> {
        use crate::store::history::HistoryIo;
        struct OpaqueRouter;
        impl InteractionRouter for OpaqueRouter {
            fn route<'a>(
                &'a self, _: OperationId, _: Interaction,
            ) -> BoxFuture<'a, Result<InteractionOutcome>> { panic!("ordinary route must not run"); }
        }
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
            .expect("original interaction ledger");
        let before = work.receipt();
        assert!(matches!(OpaqueRouter.supports_native_interactions(&work),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, .. })));
        assert_eq!(work.receipt(), before);
        let mut io = crate::store::history::native_work_io(&work)?;
        let prompt = io.clone_text("Approve λ?")?;
        let request = Interaction::Approval { prompt, operation_id: OperationId::new(), action_digest: [1; 32] };
        let custody = io.into_custody();
        let before = work.receipt();
        assert!(matches!(OpaqueRouter.route_with_native_work(
            OperationId::new(), request, custody, &work,
        ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, .. })));
        assert_eq!(work.receipt(), before);
        assert_eq!(work.live_allocation_bytes(), acyclic_fs::NativeMaterializationWork::accounting_state_bytes());
        Ok(())
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
            ToolRegistry::default(),
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
            descendants: TaskGroup::new(1, None).unwrap(),
            scope,
            policy_overrides: Vec::new(),
            owned_interactions: None,
            owned_mailbox: None,
            output_content: None,
            #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
            native_root_output: std::sync::OnceLock::new(),
            model_steps: Arc::new((AtomicUsize::new(0), None)),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata_source: None,
        };
        let narrowed = context.scoped_policy(child_policy)?;
        let definition = ToolDefinition {
            name: "test.echo".into(),
            revision: "1".into(),
            description: "Echo".into(),
            input_schema: serde_json::json!({}),
            output_schema: serde_json::json!({}),
            projection_schema: crate::tool::json_projection_schema(serde_json::json!({})),
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
        let (identity, policy) = narrowed.stock_tool_policy_binding().expect("original root policy");
        assert_eq!(identity.name, "root.policy");
        assert!(std::ptr::eq(policy, narrowed.harness.policy.as_ref().expect("bound policy")));
        let overrides = narrowed.stock_tool_policy_overrides().collect::<Vec<_>>();
        assert_eq!(overrides[0].0.name, "child.policy");
        assert!(std::ptr::eq(overrides[0].1, &narrowed.policy_overrides[0].1));
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::default(), &cancellation)
                .expect("native policy source ledger");
            struct OpaqueCommandRouter;
            impl InteractionRouter for OpaqueCommandRouter {
                fn route<'a>(
                    &'a self, _: OperationId, _: Interaction,
                ) -> BoxFuture<'a, Result<InteractionOutcome>> { panic!("ordinary command route must not run"); }
            }
            let before = work.receipt();
            assert!(matches!(narrowed.authorize_tool_policies(
                &definition, &invocation, Some(&work), Some(&OpaqueCommandRouter), &narrowed.scope,
            ).await, Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, .. })));
            assert_eq!(work.receipt(), before);
            assert!(narrowed.policy_approval(policy.as_ref(),
                Some((identity, "runtime policy identity changed after binding")),
                &definition, &invocation, Some(&work), &narrowed.scope).await?.is_none());
            let error = narrowed.policy_approval(overrides[0].1.as_ref(),
                Some((overrides[0].0, "scoped policy identity changed after binding")),
                &definition, &invocation, Some(&work), &narrowed.scope).await.err().expect("original scoped veto");
            match error {
                Error::NativeDiagnostic { code: crate::wire::ErrorCode::Unauthorized, message, custody } => {
                    assert_eq!(message, "child veto");
                    assert!(custody.work().shares_ledger(&work));
                    assert_eq!(custody.bytes(), message.capacity() as u64);
                    drop(message);
                    drop(custody);
                }
                other => panic!("native scoped policy cause: {other:?}"),
            }
            struct ChangingPolicy {
                original: ComponentIdentity,
                changed: std::sync::atomic::AtomicBool,
                scope_address: usize,
            }
            impl ToolPolicy for ChangingPolicy {
                fn identity(&self) -> ComponentIdentity {
                    let mut identity = self.original.clone();
                    if self.changed.load(Ordering::SeqCst) { identity.digest[0] ^= 1; }
                    identity
                }
                fn evaluate<'a>(
                    &'a self, _: &'a ToolInvocation, scope: &'a RuntimeScope,
                ) -> BoxFuture<'a, Result<ToolPolicyDecision>> {
                    Box::pin(async move {
                        assert_eq!(scope as *const RuntimeScope as usize, self.scope_address);
                        self.changed.store(true, Ordering::SeqCst);
                        Ok(ToolPolicyDecision::Allow)
                    })
                }
                fn supports_native_policy(&self, _: &acyclic_fs::NativeMaterializationWork) -> Result<()> { Ok(()) }
                fn identity_with_native_work(
                    &self, work: &acyclic_fs::NativeMaterializationWork,
                ) -> Result<(ComponentIdentity, acyclic_fs::NativeAllocation)> {
                    let (mut identity, custody) = native_fixture_policy_identity(&self.original, work)?;
                    if self.changed.load(Ordering::SeqCst) { identity.digest[0] ^= 1; }
                    Ok((identity, custody))
                }
                fn evaluate_with_native_work<'a>(
                    &'a self, _: &'a ToolInvocation, scope: &'a RuntimeScope,
                    work: &'a acyclic_fs::NativeMaterializationWork,
                ) -> Result<crate::NativeFutureBundle<'a, (ToolPolicyDecision, acyclic_fs::NativeAllocation)>> {
                    crate::NativeFutureBundle::new(work, || async move {
                        assert_eq!(scope as *const RuntimeScope as usize, self.scope_address);
                        self.changed.store(true, Ordering::SeqCst);
                        Ok((ToolPolicyDecision::Allow, work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?))
                    })
                }
            }
            let effective = narrowed.scope.narrow(Capabilities::new(std::iter::empty::<&str>()), narrowed.scope.limits)?;
            narrowed.require_native_scope_restriction(&effective, &work)?;
            assert!(!effective.grants().contains("tool:call:test.echo"));
            assert!(narrowed.scope.grants().contains("tool:call:test.echo"));
            let mut foreign = effective.clone();
            foreign.grants = Capabilities::new(["tool:call:foreign"]);
            let before = work.receipt();
            assert!(matches!(narrowed.require_native_scope_restriction(&foreign, &work),
                Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
            assert_eq!(work.receipt(), before);
            let mut invalid = effective.clone();
            invalid.run_limits.max_steps = Some(0);
            assert!(matches!(narrowed.require_native_scope_restriction(&invalid, &work),
                Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. })));
            let changing = ChangingPolicy { original: identity.clone(),
                changed: std::sync::atomic::AtomicBool::new(false),
                scope_address: &effective as *const RuntimeScope as usize };
            assert!(matches!(narrowed.policy_approval(&changing,
                Some((identity, "runtime policy identity changed after binding")),
                &definition, &invocation, Some(&work), &effective).await,
                Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "policy identity changed during evaluation" })));
            assert_eq!(work.live_allocation_bytes(), acyclic_fs::NativeMaterializationWork::accounting_state_bytes());
        }
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

    struct StateOnlyProvider;

    impl TaskStateProvider for StateOnlyProvider {
        fn policy_identity(&self) -> Option<ComponentIdentity> {
            None
        }
        fn observe_admission<'a>(
            &'a self,
            _task_id: TaskId,
        ) -> BoxFuture<'a, Result<TaskAdmissionRecord>> {
            Box::pin(async { Err(Error::Unsupported("state-only test provider".into())) })
        }
        fn resume_scope<'a>(
            &'a self,
            _task_id: TaskId,
            _operation_id: OperationId,
        ) -> BoxFuture<'a, Result<RuntimeScope>> {
            Box::pin(async { Err(Error::Unsupported("state-only test provider".into())) })
        }
        fn outcome<'a>(
            &'a self,
            _task_id: TaskId,
        ) -> BoxFuture<'a, Result<Option<Outcome<Value>>>> {
            Box::pin(async { Err(Error::Unsupported("state-only test provider".into())) })
        }
        fn cancel<'a>(&'a self, _task_id: TaskId) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Err(Error::Unsupported("state-only test provider".into())) })
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
    }

    impl crate::runtime::TaskStateProvider for IdentityHost {
        fn observe_admission<'a>(
            &'a self,
            _task_id: TaskId,
        ) -> BoxFuture<'a, Result<TaskAdmissionRecord>> {
            Box::pin(async { Ok(self.record.clone()) })
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

        fn policy_identity(&self) -> Option<ComponentIdentity> {
            None
        }

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
                    .with_run_limits(self.record.run_limits, &mut None)
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

    #[tokio::test]
    async fn local_stock_admission_coexists_with_a_durable_execution_route() -> Result<()> {
        struct RouteProbe {
            spawner: Arc<dyn TaskSpawner>,
            state: Arc<dyn TaskStateProvider>,
        }
        impl ExecutionProvider for RouteProbe {
            fn identity(&self) -> ComponentIdentity {
                ComponentIdentity {
                    name: "test.route".into(),
                    version: "1".into(),
                    digest: [1; 32],
                }
            }
            fn spawner(&self) -> Arc<dyn TaskSpawner> {
                Arc::clone(&self.spawner)
            }
            fn state(&self) -> Arc<dyn TaskStateProvider> {
                Arc::clone(&self.state)
            }
            fn qualify<'a>(
                &'a self,
                _: &'a TaskAdmissionRecord,
            ) -> BoxFuture<'a, Result<ExecutionPlacement>> {
                Box::pin(async { Err(Error::Conflict("local turn reached durable route".into())) })
            }
        }
        let record = TaskAdmissionRecord::from_canonical_value(
            serde_json::from_str(include_str!("../fixtures/v2/task-admission.json"))
                .map_err(|error| Error::Invalid(error.to_string()))?,
            &mut None,
        )?.0;
        let task_id = TaskId::from_bytes([4; 16]);
        let definition =
            TaskDefinition::live("test.local", "1", |context, input: u64| async move {
                Ok((context.task_id, input))
            })?;
        let mut bindings = Bindings::default();
        bindings.tasks.register(definition)?;
        let runtime = bindings.build()?;
        let definition = runtime.task::<u64, (OperationId, u64)>("test.local@1")?;
        let mut routed = runtime.as_ref().clone();
        routed.execution = Some(Arc::new(RouteProbe {
            spawner: Arc::new(SplitState {
                task_id,
                record: record.clone(),
            }),
            state: Arc::new(SplitState { task_id, record }),
        }));
        let routed = Arc::new(routed);
        let operation = OperationId::new();
        assert!(matches!(
            routed.spawn(&definition, 7).await,
            Err(Error::Unsupported(_))
        ));
        let RuntimeTask::Live(handle) = routed
            .spawn_local_with_operation(operation, &definition, 7)
            .await?
        else {
            return Err(Error::Conflict("local admission changed task kind".into()));
        };
        assert!(matches!(handle.result().await,
            Outcome::Succeeded(Ok((id, value))) if id == operation && value == 7));
        Ok(())
    }

    #[test]
    fn v2_task_admission_and_execution_placement_fixtures_are_canonical() -> Result<()> {
        let admission_fixture = include_str!("../fixtures/v2/task-admission.json").trim();
        let admission_value: Value = serde_json::from_str(admission_fixture)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let admission = TaskAdmissionRecord::from_canonical_value(admission_value, &mut None)?.0;
        assert_eq!(
            crate::contract::canonical_json_bytes(&admission.canonical_value())?,
            admission_fixture.as_bytes()
        );
        let placement_fixture = include_str!("../fixtures/v2/execution-placement.json").trim();
        let placement: ExecutionPlacement = serde_json::from_str(placement_fixture)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        placement.validate(&mut None)?;
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
        validate_children_request(parent, None, usize::MAX)?;
        assert!(validate_children_request(parent, Some("\u{7f}"), 1).is_err());
        validate_children_request(parent, Some(&"x".repeat(1_024)), 1)?;
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

    impl TaskSpawner for SplitState {
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
            resource_proofs: Vec::new(),
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
        assert!(TaskAdmissionRecord::from_canonical_value(record.canonical_value(), &mut None)?.0 == record);
        let mut unknown = record.canonical_value();
        unknown
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("task admission object".into()))?
            .insert("legacy_content".into(), Value::Null);
        assert!(TaskAdmissionRecord::from_canonical_value(unknown, &mut None).is_err());
        let harness = Bindings {
            tasks,
            tools: ToolRegistry::default(),
            resumable_tools: ResumableToolRegistry::default(),
            scope,
            concurrency: 1,
            durable_host: None,
            state: Some(Arc::new(SplitState {
                task_id,
                record: record.clone(),
            })),
            spawner: Some(Arc::new(SplitState {
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
        let mismatched_spawner = harness.bind_spawner(Arc::new(SplitState {
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
            harness.validate_observed_admission(&changed_schema, None).await,
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
            resource_proofs: Vec::new(),
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
            tools: ToolRegistry::default(),
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
            ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None)?,
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
        assert!(!grants.contains(&volume.capability(VolumeOperation::Write, &mut None)?));
        for capability in [
            file.read_capability()?,
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.directory_read_capability("notes")?,
            volume.directory_read_capability("notes/nested")?,
        ] {
            let grants = Capabilities::new([capability]);
            assert!(read_granted_with_work(&grants, &file, &mut None)?);
        }
        let unrelated = Capabilities::new([
            volume.directory_read_capability("notes/other")?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]);
        assert!(!read_granted_with_work(&unrelated, &file, &mut None)?);
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
            spawner: Arc::new(SplitState {
                task_id: TaskId::from_bytes([23; 16]),
                record: TaskAdmissionRecord::from_canonical_value(
                    serde_json::from_str(include_str!("../fixtures/v2/task-admission.json"))
                        .map_err(|error| Error::Invalid(format!("fixture is invalid: {error}")))?,
                    &mut None,
                )?.0,
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
        assert!(!observed.is_complete());
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
            tools: ToolRegistry::default(),
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
            tools: ToolRegistry::default(),
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
                        assert!(matches!(entries[0], Ok(Outcome::Succeeded(2))));
                        assert!(matches!(entries[1], Ok(Outcome::Succeeded(4))));
                        assert!(matches!(&entries[2], Ok(Outcome::Failed { .. })));
                        assert!(matches!(entries[3], Ok(Outcome::Succeeded(8))));
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
            tools: ToolRegistry::default(),
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
            tools: ToolRegistry::default(),
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
            AgentHarness::new(TaskRegistry::default(), ToolRegistry::default(), root, 1, None)?;
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
            assert!(matches!(check_tool_approval(outcome),
                Err(Error::InteractionRejected(actual)) if actual == rejection));
        }
        assert!(check_tool_approval(InteractionOutcome::Approved).is_ok());
    }

    #[tokio::test]
    async fn tool_approval_binds_runtime_action_and_accepts_provider_call_names() -> Result<()> {
        let policy = FixedPolicy {
            identity: ComponentIdentity {
                name: "approval.policy".into(),
                version: "1".into(),
                digest: [9; 32],
            },
            decision: ToolPolicyDecision::RequireApproval {
                prompt: "Approve echo".into(),
            },
        };
        let task = TaskId::from_bytes([42; 16]);
        let scope = RuntimeScope::default();
        let context = TaskContext {
            harness: AgentHarness::new(
                TaskRegistry::default(),
                ToolRegistry::default(),
                scope.clone(),
                1,
                None,
            )?,
            task_id: OperationId::from_bytes([41; 16]),
            durable_task: Some(task),
            descendants: TaskGroup::new(1, None).unwrap(),
            scope,
            policy_overrides: Vec::new(),
            owned_interactions: None,
            owned_mailbox: None,
            output_content: None,
            #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
            native_root_output: std::sync::OnceLock::new(),
            model_steps: Arc::new((AtomicUsize::new(0), None)),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata_source: None,
        };
        let definition = ToolDefinition {
            name: "test.echo".into(),
            revision: "1".into(),
            description: "Echo".into(),
            input_schema: serde_json::json!({"type":"object"}),
            output_schema: serde_json::json!({}),
            projection_schema: crate::tool::json_projection_schema(serde_json::json!({})),
        };
        let invocation = ToolInvocation::for_model_call(
            OperationId::from_bytes([40; 16]),
            0,
            "provider-call-name".into(),
            definition.name.clone(),
            serde_json::json!({"text":"hello"}),
            None,
        )?;
        let approval = context
            .policy_approval(&policy, None, &definition, &invocation, None, &context.scope)
            .await?.map(|paid| paid.0)
            .ok_or_else(|| Error::NotFound("policy approval".into()))?;
        assert!(
            matches!(&approval.1, Interaction::Approval { operation_id, .. } if *operation_id == invocation.operation_id)
        );
        assert_eq!(
            approval,
            tool_approval_request(
                task,
                &policy.identity,
                &definition,
                &invocation,
                "Approve echo".into()
            )?
        );
        assert_eq!(
            context
                .policy_approval(&policy, None, &definition, &invocation, None, &context.scope)
                .await?.map(|paid| paid.0),
            Some(approval.clone())
        );
        for (changed_task, changed_policy, changed_definition, changed_invocation) in [
            (
                TaskId::from_bytes([43; 16]),
                policy.identity.clone(),
                definition.clone(),
                invocation.clone(),
            ),
            (
                task,
                ComponentIdentity {
                    version: "2".into(),
                    digest: [8; 32],
                    ..policy.identity.clone()
                },
                definition.clone(),
                invocation.clone(),
            ),
            (
                task,
                policy.identity.clone(),
                ToolDefinition {
                    revision: "2".into(),
                    ..definition.clone()
                },
                invocation.clone(),
            ),
            (
                task,
                policy.identity.clone(),
                definition.clone(),
                ToolInvocation {
                    arguments: serde_json::json!({"text":"different"}),
                    ..invocation.clone()
                },
            ),
            (
                task,
                policy.identity.clone(),
                definition.clone(),
                ToolInvocation::for_model_call(
                    OperationId::from_bytes([40; 16]),
                    1,
                    invocation.call_id.clone(),
                    invocation.name.clone(),
                    invocation.arguments.clone(),
                    None,
                )?,
            ),
        ] {
            assert_ne!(
                approval.0,
                tool_approval_request(
                    changed_task,
                    &changed_policy,
                    &changed_definition,
                    &changed_invocation,
                    "Approve echo".into()
                )?
                .0
            );
        }
        Ok(())
    }
    #[test]
    fn created_resource_proofs_are_exact_canonical_child_data() -> Result<()> {
        use crate::resource_preparation::ResourceCreationProofRef;
        use crate::resources::{ProviderRef, ResourceKind, ResourceRef};

        let wire: Value = serde_json::from_str(include_str!("../fixtures/v2/task-admission.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let mut admission = TaskAdmissionRecord::from_canonical_value(wire.clone(), &mut None)?.0;
        let provider = ProviderRef::new("test".into(), "creator".into(), "1".into(), &mut None)?;
        let proof = ResourceCreationProofRef {
            provider: provider.clone(),
            preparation: TaskId::from_bytes([71; 16]),
            resource: ResourceRef::new(
                ResourceKind::Sandbox,
                provider.clone(),
                b"child".to_vec().into(),
                None, &mut None
            )?,
            evidence: ResourceRef::new(
                ResourceKind::Stream,
                provider,
                b"allocation".to_vec().into(),
                None, &mut None
            )?,
        };
        assert!(
            admission
                .clone()
                .with_resource_proofs(vec![proof.clone()])
                .is_err()
        );
        admission.parent = Some(TaskId::from_bytes([72; 16]));
        let original_grants = admission.grants.clone();
        let child = admission.with_resource_proofs(vec![proof.clone()])?;
        assert_eq!(child.grants, original_grants);
        let canonical = child.canonical_value();
        assert!(TaskAdmissionRecord::from_canonical_value(canonical.clone(), &mut None)?.0 == child);
        assert!(
            child
                .clone()
                .with_resource_proofs(vec![proof.clone(), proof.clone()])
                .is_err()
        );
        let mut changed = proof;
        changed.preparation = TaskId::from_bytes([73; 16]);
        let changed_child = child.clone().with_resource_proofs(vec![changed])?;
        assert_ne!(
            crate::contract::canonical_json_digest(&canonical)?,
            crate::contract::canonical_json_digest(&changed_child.canonical_value())?
        );
        let mut missing = wire;
        missing
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("fixture is not an object".into()))?
            .remove("resource_proofs");
        assert!(TaskAdmissionRecord::from_canonical_value(missing, &mut None).is_err());
        Ok(())
    }
}
