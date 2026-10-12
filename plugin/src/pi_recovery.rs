//! Service recovery over original task admissions and retained reservations.

use super::pi_root::PiRootIntent;
use super::pi_task::{PiTaskExecution, PiTaskInput, PiTaskMachine};
use super::{AdapterState, PiTaskBinding, PluginTaskBinding};
use acyclic_fs::{
    NativeAllocation, NativeMaterializationWork, NativeTreeMaterializationError, WorkError,
};
use acyclic_harness::runtime::{TaskSpawner, TaskStateProvider};
use acyclic_harness::{
    Error, Outcome, Result, TaskId,
    conversation::FileRef,
    distributed::{WorkLease, WorkPull},
    filesystem::FilesystemTaskRuntime,
    runtime::DurableTaskHost,
};

struct PiTaskSelection<'a> {
    binding: &'a PiTaskBinding,
    parent: Option<TaskId>,
    preparation: Option<TaskId>,
    alias: Option<&'a str>,
}

/// The caller's retained origin must match both immutable admission lineage and execution mode.
pub(crate) enum PiTaskOrigin<'a> {
    Root(&'a PiRootIntent),
    RootState(&'a AdapterState),
    Child(TaskId),
}

impl PiTaskOrigin<'_> {
    fn parent(&self) -> Option<TaskId> {
        match self {
            Self::Root(_) | Self::RootState(_) => None,
            Self::Child(parent) => Some(*parent),
        }
    }
}

fn pi_binding(binding: &PluginTaskBinding) -> Result<&PiTaskBinding> {
    match binding {
        PluginTaskBinding::Pi(binding) => Ok(binding.as_ref()),
        PluginTaskBinding::HostManaged => Err(invalid("Pi recovery requires a Pi task binding")),
    }
}

/// Plans recovery from the existing route graph.
fn session_tasks<'a>(
    state: &'a AdapterState,
    work: &NativeMaterializationWork,
) -> Result<(Vec<PiTaskSelection<'a>>, NativeAllocation)> {
    let mut custody = {
        work.cancellation()
            .check()
            .map_err(|_| native_error(NativeTreeMaterializationError::Cancelled))?;
        work.claim(
            recovery_metadata_quota(state)?,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )
        .map_err(native_error)
    }?;
    super::validate_state_version(state).map_err(Error::Invalid)?;
    super::validate_state_bounds(state).map_err(Error::Invalid)?;
    let root = pi_binding(&state.task_binding)?;
    if state.root_agent_id.is_empty() || state.routes.contains_key(&state.root_agent_id) {
        return Err(invalid("Pi recovery root agent is missing or duplicated"));
    }
    let parent_task = |parent: &str| {
        if parent == state.root_agent_id {
            Ok(root.task_id)
        } else {
            state
                .routes
                .get(parent)
                .ok_or_else(|| invalid("Pi recovery has no retained parent route"))
                .and_then(|route| pi_binding(&route.task_binding))
                .map(|binding| binding.task_id)
        }
    };
    let mut children = std::collections::BTreeMap::<&str, Vec<&super::Route>>::new();
    for route in state.routes.values() {
        parent_task(&route.parent_agent_id)?;
        children
            .entry(&route.parent_agent_id)
            .or_default()
            .push(route);
    }
    let count = state
        .routes
        .len()
        .checked_add(state.pending.len())
        .and_then(|count| count.checked_add(1))
        .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?;
    {
        let mut slots = vector_slots::<PiTaskSelection<'_>>(count, work)?;
        custody.absorb(&mut slots).map_err(native_error)?;
    }
    let mut tasks = Vec::with_capacity(count);
    tasks.push(PiTaskSelection {
        binding: root,
        parent: None,
        preparation: None,
        alias: None,
    });
    let mut queue = std::collections::VecDeque::from([state.root_agent_id.as_str()]);
    while let Some(parent) = queue.pop_front() {
        for route in children.remove(parent).unwrap_or_default() {
            tasks.push(PiTaskSelection {
                binding: pi_binding(&route.task_binding)?,
                parent: Some(parent_task(parent)?),
                preparation: None,
                alias: Some(route.agent_id.as_str()),
            });
            queue.push_back(&route.agent_id);
        }
    }
    if !children.is_empty() {
        return Err(invalid("Pi recovery route graph contains a parent cycle"));
    }
    for pending in &state.pending {
        tasks.push(PiTaskSelection {
            binding: pi_binding(&pending.task_binding)?,
            parent: Some(parent_task(&pending.parent_agent_id)?),
            preparation: if matches!(
                pi_binding(&pending.task_binding)?.scratch.allocation,
                super::PiScratchAllocation::Preparing
            ) && pending.roots.is_empty()
            {
                Some(TaskId::from_bytes(
                    super::pi_fork::PiForkOperations::new(
                        super::pi_fork::original_operation(
                            root.task_id,
                            parent_task(&pending.parent_agent_id)?,
                            &pending.tool_use_id,
                            &work,
                        )?,
                        &work,
                    )?
                    .preparation
                    .into_bytes(),
                ))
            } else {
                None
            },
            alias: pending.agent_id.as_deref(),
        });
    }
    Ok((tasks, custody))
}

/// Read-only service observations.
#[derive(serde::Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum PiTaskObservation {
    Pending {
        #[serde(rename = "task")]
        task_id: TaskId,
    },
    Reserved {
        #[serde(rename = "task", serialize_with = "serialize_lease_task")]
        lease: WorkLease,
    },
    Unresolved {
        #[serde(rename = "task", serialize_with = "serialize_lease_task")]
        lease: WorkLease,
        #[serde(serialize_with = "crate::service::serialize_control_error")]
        error: Error,
    },
    #[serde(rename = "preparing")]
    Preparation {
        #[serde(rename = "task")]
        task_id: TaskId,
        preparation: TaskId,
        phase: PiPreparationPhase,
    },
    Outcome {
        #[serde(rename = "task")]
        task_id: TaskId,
        outcome: Outcome<FileRef>,
    },
}

fn serialize_lease_task<S: serde::Serializer>(
    lease: &WorkLease,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serde::Serialize::serialize(
        &TaskId::from_bytes(lease.operation.operation_id.into_bytes()),
        serializer,
    )
}

#[derive(serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PiPreparationPhase {
    Pending,
    Reserved,
    Uncertain,
    Succeeded,
    Failed,
    Cancelled,
}

/// The same complete graph observer, supplied the caller's ORIGINAL native work.
pub(crate) async fn observe_pi_session<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>,
    state: &AdapterState,
    work: &NativeMaterializationWork,
) -> Result<(Vec<PiTaskObservation>, NativeAllocation)>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    let mut custody = work
        .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
        .map_err(native_error)?;
    let mut tasks = session_tasks(state, work)?;
    let pi_definition = PiTaskMachine::new(work)?.definition(work)?;
    let preparation_definition = if tasks.0.iter().any(|task| task.preparation.is_some()) {
        Some(
            std::sync::Arc::new(acyclic_harness::filesystem::ForkPreparationMachine::new())
                .definition()?,
        )
    } else {
        None
    };
    for task in &mut tasks.0 {
        if let Some(preparation) = task.preparation {
            let mut admission = runtime
                .task_host()
                .observe_admission_with_native_work(preparation, work)
                .await?;
            let mut request = decode_recovery_value::<
                acyclic_harness::resource_preparation::ResourcePreparationRequest,
            >(
                std::mem::take(&mut admission.0.input),
                acyclic_harness::JsonValueSchema::ResourcePreparationRequest,
                work,
            )?;
            request.0.validate(&mut None)?;
            let child = decode_recovery_value::<PiTaskInput>(
                std::mem::take(&mut request.0.child.input),
                acyclic_harness::JsonValueSchema::None,
                work,
            )?;
            if admission.0.task
                != *preparation_definition
                    .as_ref()
                    .expect("retained preparation definition")
                    .identity()
                || admission.0.parent != task.parent
                || request.0.child.parent != task.parent
                || request.0.child.operation_id.into_bytes() != task.binding.task_id.into_bytes()
                || child.0.binding != *task.binding
            {
                return Err(invalid(
                    "Pi preparing child differs from its original resource admission",
                ));
            }
            authenticate_original_alias(
                runtime,
                preparation,
                admission.0.operation_id,
                &child.0,
                task.alias,
                work,
            )
            .await?;
            let retained = runtime
                .task_host()
                .reconcile_admission_with_native_work(request.0.child.operation_id, work)?
                .await?;
            match &retained.0 {
                None => continue,
                Some((task_id, admitted))
                    if *task_id == task.binding.task_id
                        && admitted.operation_id == request.0.child.operation_id => {}
                _ => return Err(invalid("Pi preparation reconciled another child operation")),
            }
            task.preparation = None;
        }
        let origin = match task.parent {
            None => PiTaskOrigin::RootState(state),
            Some(parent) => PiTaskOrigin::Child(parent),
        };
        let input = authenticate_pi_task_inner(
            runtime,
            task.binding,
            origin,
            work,
            pi_definition.identity(),
        )
        .await?;
        authenticate_original_alias(
            runtime,
            task.binding.task_id,
            input.0.1,
            &input.0.0,
            task.alias,
            work,
        )
        .await?;
    }
    {
        let mut slots = vector_slots::<PiTaskObservation>(tasks.0.len(), work)?;
        custody.absorb(&mut slots).map_err(native_error)?;
    }
    let mut observations = Vec::with_capacity(tasks.0.len());
    for task in tasks.0 {
        if let Some(preparation) = task.preparation {
            let recovered = (async {
                let mut io = acyclic_harness::store::native_work_io(work)?;
                acyclic_harness::store::HistoryIo::retain_owned(
                    &mut io,
                    std::mem::size_of::<acyclic_harness::distributed::WorkPull>().max(
                        std::mem::size_of::<acyclic_harness::filesystem::TaskWorkerAttempt>(),
                    ) as u64,
                )?;
                let value = runtime
                    .task_host()
                    .recover_work(preparation, &mut Some(&mut io))
                    .await?;
                Ok::<_, acyclic_harness::Error>((value, io.into_custody()))
            })
            .await?;
            let phase = match recovered.0 {
                WorkPull::Claimed(_) => PiPreparationPhase::Reserved,
                WorkPull::Unresolved { .. } => PiPreparationPhase::Uncertain,
                WorkPull::Idle => {
                    let outcome = runtime
                        .task_host()
                        .outcome_with_native_work(preparation, work)
                        .await?;
                    match outcome.0 {
                        None => PiPreparationPhase::Pending,
                        Some(Outcome::Succeeded(_)) => PiPreparationPhase::Succeeded,
                        Some(Outcome::Failed { .. }) => PiPreparationPhase::Failed,
                        Some(Outcome::Cancelled) => PiPreparationPhase::Cancelled,
                        Some(Outcome::Indeterminate { .. }) => PiPreparationPhase::Uncertain,
                    }
                }
            };
            observations.push(PiTaskObservation::Preparation {
                task_id: task.binding.task_id,
                preparation,
                phase,
            });
            continue;
        }
        let task_id = task.binding.task_id;
        let mut recovered = (async {
            let mut io = acyclic_harness::store::native_work_io(work)?;
            acyclic_harness::store::HistoryIo::retain_owned(
                &mut io,
                std::mem::size_of::<acyclic_harness::distributed::WorkPull>().max(
                    std::mem::size_of::<acyclic_harness::filesystem::TaskWorkerAttempt>(),
                ) as u64,
            )?;
            let value = runtime
                .task_host()
                .recover_work(task_id, &mut Some(&mut io))
                .await?;
            Ok::<_, acyclic_harness::Error>((value, io.into_custody()))
        })
        .await?;
        custody.absorb(&mut recovered.1).map_err(native_error)?;
        let observation = match recovered.0 {
            WorkPull::Claimed(lease) => PiTaskObservation::Reserved { lease },
            WorkPull::Unresolved { lease, error } => PiTaskObservation::Unresolved { lease, error },
            WorkPull::Idle => {
                let mut outcome = runtime
                    .task_host()
                    .outcome_with_native_work(task_id, work)
                    .await?;
                custody.absorb(&mut outcome.1).map_err(native_error)?;
                match outcome.0 {
                    Some(outcome) => PiTaskObservation::Outcome {
                        task_id,
                        outcome: match outcome {
                            Outcome::Succeeded(value) => {
                                let mut decoded = decode_recovery_value::<FileRef>(
                                    value,
                                    acyclic_harness::JsonValueSchema::None,
                                    work,
                                )?;
                                custody.absorb(&mut decoded.1).map_err(native_error)?;
                                Outcome::Succeeded(decoded.0)
                            }
                            Outcome::Failed { message } => Outcome::Failed { message },
                            Outcome::Cancelled => Outcome::Cancelled,
                            Outcome::Indeterminate { operation_id } => {
                                Outcome::Indeterminate { operation_id }
                            }
                        },
                    },
                    None => PiTaskObservation::Pending { task_id },
                }
            }
        };
        observations.push(observation);
    }
    Ok((observations, custody))
}

/// Routing labels are metadata, but must still match the originally admitted native input.
async fn authenticate_original_alias<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>,
    task: TaskId,
    operation: acyclic_harness::OperationId,
    input: &PiTaskInput,
    alias: Option<&str>,
    work: &NativeMaterializationWork,
) -> Result<()>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    let PiTaskExecution::Child { launch } = &input.execution else {
        return Ok(());
    };
    if launch.kind != super::pi_task::NATIVE_LAUNCH_TASK_COMMAND_KIND {
        return Ok(());
    }
    let alias = alias.ok_or_else(|| invalid("Pi child catalog has no original agent alias"))?;
    {
        if launch.payload.descriptor().media_type() != "application/json"
            || launch.payload.descriptor().byte_length() > super::MAXIMUM_ADAPTER_STATE_BYTES
        {
            return Err(invalid(
                "Pi original native intent exceeds its retained input boundary",
            ));
        }
        let context = runtime
            .harness()
            .durable_context_with_native_work(
                task,
                acyclic_harness::OperationId::from_bytes(task.into_bytes()),
                work,
            )
            .await?;
        let bytes = context
            .0
            .read_file_with_native_work(&launch.payload, work)
            .await?;
        drop(context);
        let intent =
            super::pi_root_provider::guarded_pi_decode::<super::pi_native::PiNativeLaunchIntent>(
                &bytes,
                super::MAXIMUM_ADAPTER_STATE_BYTES,
                std::mem::size_of::<super::pi_native::PiNativeLaunchIntent>() as u64,
                acyclic_harness::wire::ErrorCode::Invalid,
                acyclic_harness::JsonValueSchema::None,
                super::pi_native::PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES,
                work,
            )?;
        intent.0.validate(work)?;
        if intent.0.agent_alias != alias {
            return Err(invalid(
                "Pi catalog alias differs from its original native launch input",
            ));
        }
        drop(intent);
        drop(bytes);
    }
    Ok(())
}

/// Transient authenticated references, never another persisted lease registry.
pub(crate) async fn authenticate_pi_task<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>,
    binding: &PiTaskBinding,
    origin: PiTaskOrigin<'_>,
    work: &NativeMaterializationWork,
) -> Result<(PiTaskInput, NativeAllocation)>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    let definition = PiTaskMachine::new(work)?.definition(work)?;
    let admitted =
        authenticate_pi_task_inner(runtime, binding, origin, work, definition.identity()).await?;
    Ok((admitted.0.0, admitted.1))
}

async fn authenticate_pi_task_inner<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>,
    binding: &PiTaskBinding,
    origin: PiTaskOrigin<'_>,
    work: &NativeMaterializationWork,
    identity: &acyclic_harness::registry::ComponentIdentity,
) -> Result<(
    (PiTaskInput, acyclic_harness::OperationId),
    NativeAllocation,
)>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    binding.validate_with_native_work(
        None,
        binding.project_volumes.keys().map(String::as_str),
        true,
        work,
    )?;
    let mut admission = runtime
        .task_host()
        .observe_admission_with_native_work(binding.task_id, work)
        .await?;
    let mut admitted = decode_recovery_value::<PiTaskInput>(
        std::mem::take(&mut admission.0.input),
        acyclic_harness::JsonValueSchema::None,
        work,
    )?;
    if admission.0.task != *identity
        || admission.0.parent != origin.parent()
        || admitted.0.binding.native_session_id != binding.native_session_id
        || admitted.0.binding.task_id != binding.task_id
        || admitted.0.binding.agent_id != binding.agent_id
        || admitted.0.binding.project_volumes != binding.project_volumes
        || admitted.0.binding.scratch.volume != binding.scratch.volume
        || admitted
            .0
            .binding
            .results
            .as_ref()
            .map(|result| &result.volume)
            != binding.results.as_ref().map(|result| &result.volume)
    {
        return Err(invalid(
            "Pi binding differs from its original task admission",
        ));
    }
    match (&admitted.0.execution, origin) {
        (PiTaskExecution::Host { root: original, .. }, PiTaskOrigin::Root(retained))
            if original == retained =>
        {
            original.validate(binding, work)?
        }
        (PiTaskExecution::Host { root: original, .. }, PiTaskOrigin::RootState(retained))
            if root_matches_state(original, retained) =>
        {
            original.validate(binding, work)?
        }
        (PiTaskExecution::Child { .. }, PiTaskOrigin::Child(_)) => {}
        _ => {
            return Err(invalid(
                "Pi execution origin differs from its original admission",
            ));
        }
    }
    for volume in binding
        .project_volumes
        .values()
        .chain(std::iter::once(&binding.scratch.volume))
        .chain(binding.results.as_ref().map(|result| &result.volume))
    {
        let read = {
            let mut io = acyclic_harness::store::native_work_io(work)?;
            let value = volume.capability(
                acyclic_harness::conversation::VolumeOperation::Read,
                &mut Some(&mut io),
            )?;
            (value, io.into_custody())
        };
        if !admission.0.grants.contains(&read.0) {
            return Err(invalid("Pi selection lacks its original read grant"));
        }
    }
    let scratch_write = {
        let mut io = acyclic_harness::store::native_work_io(work)?;
        let value = binding.scratch.volume.capability(
            acyclic_harness::conversation::VolumeOperation::Write,
            &mut Some(&mut io),
        )?;
        (value, io.into_custody())
    };
    if !admission.0.grants.contains(&scratch_write.0) {
        return Err(invalid("Pi scratch lacks its original write grant"));
    }
    if let Some(result) = &binding.results {
        let write = {
            let mut io = acyclic_harness::store::native_work_io(work)?;
            let value = result.volume.capability(
                acyclic_harness::conversation::VolumeOperation::Write,
                &mut Some(&mut io),
            )?;
            (value, io.into_custody())
        };
        if !admission.0.grants.contains(&write.0) {
            return Err(invalid(
                "Pi result storage lacks its original created write grant",
            ));
        }
    }
    let operation = admission.0.operation_id;
    admitted.1.absorb(&mut admission.1).map_err(native_error)?;
    Ok(((admitted.0, operation), admitted.1))
}

/// Compares the original immutable Root DTO directly with retained references; no path String, root map or replacement authority is constructed.
fn root_matches_state(original: &PiRootIntent, retained: &AdapterState) -> bool {
    match original {
        PiRootIntent::ScratchOnly {} => {
            retained.root_context_id == [0; 16]
                && retained.root_id == [0; 16]
                && retained.roots.is_empty()
        }
        PiRootIntent::Project {
            context_id,
            root_id,
            paths,
        } => {
            *context_id == retained.root_context_id
                && *root_id == retained.root_id
                && paths.len() == retained.roots.len()
                && paths
                    .iter()
                    .zip(&retained.roots)
                    .all(|((key, path), (retained_key, root))| {
                        key == retained_key && root.path.to_str() == Some(path.as_str())
                    })
        }
    }
}

fn invalid(message: &'static str) -> Error {
    Error::Invalid(message.to_owned())
}

fn native_error(cause: NativeTreeMaterializationError) -> Error {
    Error::NativeWork {
        cause,
        custody: None,
    }
}

/// Closed first-party recovery DTOs consume their owned Value rather than cloning it.
fn decode_recovery_value<T: serde::de::DeserializeOwned>(
    value: serde_json::Value,
    schema: acyclic_harness::JsonValueSchema,
    work: &NativeMaterializationWork,
) -> Result<(T, NativeAllocation)> {
    let mut custody = work
        .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
        .map_err(native_error)?;
    let mut retained_owned = 0;
    let remaining = work.remaining().map_err(native_error)?;
    let bounds = acyclic_harness::JsonBounds {
        maximum_bytes: super::MAXIMUM_ADAPTER_STATE_BYTES,
        maximum_items: remaining.items_examined,
        maximum_depth: 64,
    };
    let result = acyclic_harness::json_from_value_bounded::<T, _>(
        std::borrow::Cow::Owned(value),
        bounds,
        std::mem::size_of::<T>() as u64,
        acyclic_harness::wire::ErrorCode::Invalid,
        schema,
        &mut |charge| {
            if let acyclic_harness::JsonCodecCharge::Admission {
                maximum_owned_bytes,
                value_shape,
            } = charge
            {
                let (additional, diagnostics) = recovery_conversion_quota(value_shape)?;
                retained_owned = checked_quota(maximum_owned_bytes, additional)?;
                custody
                    .resize(checked_quota(retained_owned, diagnostics)?)
                    .map_err(native_error)
            } else {
                super::pi_root_provider::native_buffer_codec_charge(work, &mut custody, charge)
            }
        },
    );
    match result {
        Ok(value) => {
            custody.release(custody.bytes().checked_sub(retained_owned).ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?).map_err(native_error)?;
            Ok((value, custody))
        }
        Err(Error::NativeDecode {
            code,
            source,
            custody: None,
        }) => Err(Error::NativeDecode {
            code,
            source,
            custody: Some(custody),
        }),
        Err(error) => {
            // SDK has physically dropped the owned source/partial DTO first.
            drop(custody);
            Err(error)
        }
    }
}

fn recovery_conversion_quota(shape: acyclic_harness::JsonValueShape) -> Result<(u64, u64)> {
    let slot = [
        std::mem::size_of::<acyclic_harness::resource_preparation::ResourcePreparationRequest>(),
        std::mem::size_of::<acyclic_harness::runtime::TaskAdmissionRecord>(),
        std::mem::size_of::<acyclic_harness::resource_preparation::ResourceCreationProofRef>(),
        std::mem::size_of::<acyclic_harness::resources::ResourceRef>(),
        std::mem::size_of::<acyclic_harness::resources::ProviderRef>(),
        std::mem::size_of::<acyclic_harness::source_authorization::ProjectSourceAuthorization>(),
        std::mem::size_of::<acyclic_harness::workflow::WorkflowCommand>(),
        std::mem::size_of::<PiTaskInput>(),
        std::mem::size_of::<PiTaskBinding>(),
        std::mem::size_of::<FileRef>(),
        std::mem::size_of::<(String, serde_json::Value)>(),
    ]
    .into_iter()
    .max()
    .expect("closed recovery slot manifest") as u64;
    let typed = shape
        .lexical_items()
        .checked_add(shape.virtual_items())
        .and_then(|nodes| nodes.checked_mul(8))
        .and_then(|nodes| nodes.checked_mul(slot))
        .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?;
    let additional = acyclic_harness::json_owned_value_conversion_additional_bytes(shape)?;
    let labels = checked_quota(
        acyclic_harness::preparation_serde_diagnostic_label_bytes(),
        checked_quota(
            super::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
            super::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
                "ResourcePreparationRequest",
                "struct ResourcePreparationRequest",
                "provider",
                "child",
                "resources",
                "creation_grant",
                "maximum_resources",
                "maximum_work",
                "maximum_bytes",
                "payload",
                "command",
                "source_checkpoint",
                "input_files",
                "read_authorization",
                "ResourceCreationProofRef",
                "struct ResourceCreationProofRef",
                "preparation",
                "resource",
                "evidence",
                "TaskAdmissionRecord",
                "struct TaskAdmissionRecord",
                "operation_id",
                "task",
                "machine",
                "input",
                "input_schema",
                "output_schema",
                "parent",
                "resource_proofs",
                "grants",
                "limits",
                "run_limits",
                "policy",
                "extensions",
                "execution",
                "ComponentIdentity",
                "struct ComponentIdentity",
                "name",
                "version",
                "TaskRunLimits",
                "struct TaskRunLimits",
                "concurrency",
                "max_steps",
                "deadline_epoch_ms",
                "ExecutionPlacement",
                "struct ExecutionPlacement",
                "provider",
                "build",
                "environment",
                "readiness_revision",
                "Authority",
                "struct Authority",
                "kind",
                "id",
                "AggregateKind",
                "agent",
                "conversation",
                "session",
                "turn",
                "task",
                "ExtensionAdmission",
                "struct ExtensionAdmission",
                "source",
                "selected",
                "configurations",
                "Limits",
                "struct Limits",
            ]),
        )?,
    )?;
    let diagnostics = acyclic_harness::json_typed_diagnostic_owned_bytes(
        shape,
        labels,
        Some(acyclic_harness::wire::ErrorCode::Invalid),
    )?;
    Ok((checked_quota(typed, additional)?, diagnostics))
}

pub(crate) fn vector_slots<T>(
    count: usize,
    work: &NativeMaterializationWork,
) -> Result<NativeAllocation> {
    let bytes = count
        .checked_mul(std::mem::size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?;
    if count == 0 {
        return work
            .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(native_error);
    }
    // Exactly one with_capacity allocation attempt follows this admission.
    work.claim(
        bytes,
        u64::from((bytes) != 0),
        acyclic_fs::WorkCounters::UNCHARGED,
    )
    .map_err(native_error)
}

fn checked_quota(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))
}

fn binding_metadata_quota(binding: &PiTaskBinding) -> Result<u64> {
    let mut bytes = checked_quota(4096, binding.native_session_id.len() as u64)?;
    for (key, _) in &binding.project_volumes {
        bytes = checked_quota(bytes, key.len() as u64)?;
    }
    for volume in binding
        .project_volumes
        .values()
        .chain(std::iter::once(&binding.scratch.volume))
        .chain(binding.results.as_ref().map(|result| &result.volume))
    {
        let provider = volume.provider();
        let strings = [
            provider.namespace(),
            provider.family(),
            provider.version(),
            volume.id(),
        ];
        let mut source = strings.iter().try_fold(4096_u64, |size, text| {
            checked_quota(size, text.len() as u64)
        })?;
        if let acyclic_harness::conversation::VolumeOwner::Project(owner) = volume.owner() {
            source = checked_quota(source, owner.len() as u64)?;
        }
        bytes = checked_quota(
            bytes,
            source.checked_mul(16).ok_or_else(|| {
                native_error(NativeTreeMaterializationError::Work(WorkError::Overflow))
            })?,
        )?;
    }
    Ok(bytes)
}

fn recovery_metadata_quota(state: &AdapterState) -> Result<u64> {
    let mut bytes = binding_metadata_quota(pi_binding(&state.task_binding)?)?;
    for root in state.roots.values() {
        let path = root
            .path
            .to_str()
            .ok_or_else(|| invalid("Pi root path has no supported wire representation"))?;
        bytes = checked_quota(
            bytes,
            checked_quota(
                4096,
                (path.len() as u64).checked_mul(8).ok_or_else(|| {
                    native_error(NativeTreeMaterializationError::Work(WorkError::Overflow))
                })?,
            )?,
        )?;
    }
    for route in state.routes.values() {
        bytes = checked_quota(
            bytes,
            binding_metadata_quota(pi_binding(&route.task_binding)?)?,
        )?;
        bytes = checked_quota(bytes, 4096)?;
    }
    for pending in &state.pending {
        bytes = checked_quota(
            bytes,
            binding_metadata_quota(pi_binding(&pending.task_binding)?)?,
        )?;
        bytes = checked_quota(
            bytes,
            checked_quota(4096, pending.tool_use_id.len() as u64)?,
        )?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod allocation_tests {
    use super::*;

    #[test]
    fn native_graph_vector_claim_precedes_growth_and_keeps_original_ledger() {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = NativeMaterializationWork::new(
            super::super::pi_root_provider::allocation_budget(),
            &cancellation,
        )
        .unwrap();
        let before = work.receipt();
        let custody = vector_slots::<PiTaskObservation>(3, &work).unwrap();
        assert_eq!(
            work.receipt().allocation_operations,
            before.allocation_operations + 1
        );
        let mut observations = Vec::with_capacity(3);
        for index in 0..3 {
            observations.push(PiTaskObservation::Pending {
                task_id: TaskId::from_bytes([index + 1; 16]),
            });
        }
        assert_eq!(observations.capacity(), 3);
        assert_eq!(
            custody.bytes(),
            (3 * std::mem::size_of::<PiTaskObservation>()) as u64
        );
        let original = work
            .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
            .unwrap();
        assert!(custody.shares_ledger(&original));
        drop(observations);
        drop(custody);
        drop(original);
        assert_eq!(work.live_allocation_bytes(), 0);
        cancellation.cancel();
        assert!(matches!(
            vector_slots::<PiTaskObservation>(3, &work),
            Err(Error::NativeWork {
                cause: NativeTreeMaterializationError::Cancelled,
                ..
            })
        ));
    }

    #[test]
    fn native_graph_rejects_foreign_custody_without_releasing_observed_payload_guard() {
        let cancellation = acyclic_fs::CancellationToken::new();
        let first = NativeMaterializationWork::new(
            super::super::pi_root_provider::allocation_budget(),
            &cancellation,
        )
        .unwrap();
        let second = NativeMaterializationWork::new(
            super::super::pi_root_provider::allocation_budget(),
            &cancellation,
        )
        .unwrap();
        let mut retained = first
            .claim(8, 0, acyclic_fs::WorkCounters::UNCHARGED)
            .unwrap();
        let mut observed = (
            "original producer",
            second
                .claim(16, 0, acyclic_fs::WorkCounters::UNCHARGED)
                .unwrap(),
        );
        assert!(matches!(
            retained.absorb(&mut observed.1).map_err(native_error),
            Err(Error::NativeWork {
                cause: NativeTreeMaterializationError::InvalidLayout,
                ..
            })
        ));
        assert_eq!(retained.bytes(), 8);
        assert_eq!(observed.1.bytes(), 16);
        assert_eq!(second.live_allocation_bytes(), 16);
        drop(observed);
        drop(retained);
        assert_eq!(first.live_allocation_bytes(), 0);
        assert_eq!(second.live_allocation_bytes(), 0);
    }
}
