//! Service recovery over original task admissions and retained reservations.
//! Native observations use the same complete retained graph and caller work;
//! original producer custody survives typed decoding and final reply encoding.

use super::pi_root::PiRootIntent;
use super::pi_task::{PiTaskExecution, PiTaskInput, PiTaskMachine};
use super::{AdapterState, PiTaskBinding, PluginTaskBinding};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
use acyclic_harness::{
    Error, Outcome, Result, TaskId,
    conversation::FileRef,
    distributed::{WorkLease, WorkPull},
    durable_host::TaskJournalOwner,
    filesystem::FilesystemTaskRuntime,
    runtime::DurableTaskHost,
    scheduler::LeaseFence,
};
use std::sync::Arc;

struct PiTaskSelection<'a> {
    binding: &'a PiTaskBinding,
    parent: Option<TaskId>,
    preparation: Option<TaskId>,
    alias: Option<&'a str>,
}

/// The caller's retained origin must match both immutable admission lineage
/// and execution mode. Root recovery also authenticates its physical selection.
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

/// Plans recovery from the existing route graph. The plan holds references,
/// and grants no authority to reopen a task or workspace.
fn session_tasks<'a>(
    state: &'a AdapterState, work: Option<&NativeMaterializationWork>,
) -> Result<NativeOwned<Vec<PiTaskSelection<'a>>>> {
    let mut custody = work.map(|work| {
        work.cancellation().check().map_err(|_| native_error(NativeTreeMaterializationError::Cancelled))?;
        work.retain_owned(recovery_metadata_quota(state)?).map_err(native_error)
    }).transpose()?;
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
    let count = state.routes.len().checked_add(state.pending.len()).and_then(|count| count.checked_add(1))
        .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?;
    if let Some(work) = work {
        let mut slots = vector_slots::<PiTaskSelection<'_>>(count, work)?;
        custody.as_mut().expect("native source metadata custody").absorb(&mut slots).map_err(native_error)?;
    }
    let mut tasks = Vec::with_capacity(count);
    tasks.push(PiTaskSelection {
        binding: root, parent: None, preparation: None, alias: None,
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
            preparation: if matches!(pi_binding(&pending.task_binding)?.scratch.allocation,
                super::PiScratchAllocation::Preparing) && pending.roots.is_empty() {
                Some(TaskId::from_bytes(super::pi_fork::PiForkOperations::new(
                    super::pi_fork::original_operation(root.task_id,
                        parent_task(&pending.parent_agent_id)?, &pending.tool_use_id),
                ).preparation.into_bytes()))
            } else { None },
            alias: pending.agent_id.as_deref(),
        });
    }
    Ok(NativeOwned { value: tasks, custody })
}

/// Read-only service observations. A retained reservation is a reference;
/// consumers must still acquire the current owner before dispatch or settlement.
pub(crate) enum PiTaskObservation {
    Pending {
        task_id: TaskId,
    },
    Reserved {
        lease: WorkLease,
    },
    Unresolved {
        lease: WorkLease,
        error: Error,
    },
    Preparation {
        task_id: TaskId,
        preparation: TaskId,
        phase: PiPreparationPhase,
    },
    Outcome {
        task_id: TaskId,
        outcome: Outcome<FileRef>,
    },
}

#[derive(serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PiPreparationPhase { Pending, Reserved, Uncertain, Succeeded, Failed, Cancelled }

/// Authenticates the complete retained graph before observing task states.
/// Frozen, cancelled and completed routes do not implicitly start execution.
pub(crate) async fn observe_pi_session<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>, state: &AdapterState,
) -> Result<Vec<PiTaskObservation>>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    Ok(observe_pi_session_inner(runtime, state, None).await?.value)
}

/// The same complete graph observer, supplied the caller's ORIGINAL native work.
/// No reservation, lifecycle transition, namespace mutation or worker dispatch
/// occurs here. Payload and its original custody travel to the final encoder.
pub(crate) async fn observe_pi_session_with_native_work<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>, state: &AdapterState,
    work: &NativeMaterializationWork,
) -> Result<(Vec<PiTaskObservation>, NativeAllocation)>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    let observed = observe_pi_session_inner(runtime, state, Some(work)).await?;
    Ok((observed.value, observed.custody.expect("native graph custody")))
}

async fn observe_pi_session_inner<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>, state: &AdapterState,
    work: Option<&NativeMaterializationWork>,
) -> Result<NativeOwned<Vec<PiTaskObservation>>>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    let mut custody = work.map(|work| work.retain_owned(0).map_err(native_error)).transpose()?;
    let mut tasks = session_tasks(state, work)?;
    let _schema_custody = schema_custody(work)?;
    let pi_definition = Arc::new(PiTaskMachine::new()).definition()?;
    let preparation_definition = if tasks.value.iter().any(|task| task.preparation.is_some()) {
        Some(Arc::new(acyclic_harness::filesystem::ForkPreparationMachine::new()).definition()?)
    } else { None };
    for task in &mut tasks.value {
        if let Some(preparation) = task.preparation {
            let mut admission = observe_admission(runtime, preparation, work).await?;
            let mut request = decode_recovery_value::<acyclic_harness::resource_preparation::ResourcePreparationRequest>(
                std::mem::take(&mut admission.value.input),
                acyclic_harness::JsonValueSchema::ResourcePreparationRequest, work)?;
            request.value.validate()?;
            let child = decode_recovery_value::<PiTaskInput>(
                std::mem::take(&mut request.value.child.input), acyclic_harness::JsonValueSchema::None, work)?;
            if admission.value.task != *preparation_definition.as_ref()
                .expect("retained preparation definition").identity() || admission.value.parent != task.parent
                || request.value.child.parent != task.parent
                || request.value.child.operation_id.into_bytes() != task.binding.task_id.into_bytes()
                || child.value.binding != *task.binding
            {
                return Err(invalid("Pi preparing child differs from its original resource admission"));
            }
            authenticate_original_alias(runtime, preparation, admission.value.operation_id,
                &child.value, task.alias, work).await?;
            let retained = match work {
                Some(work) => NativeOwned::native(runtime.task_host()
                    .reconcile_admission_with_native_work(request.value.child.operation_id, work).await?),
                None => NativeOwned::ordinary(runtime.task_host()
                    .reconcile_admission(request.value.child.operation_id).await?),
            };
            match &retained.value {
                None => continue,
                Some((task_id, admitted)) if *task_id == task.binding.task_id
                    && admitted.operation_id == request.value.child.operation_id => {}
                _ => return Err(invalid("Pi preparation reconciled another child operation")),
            }
            task.preparation = None;
        }
        let origin = match task.parent {
            None => PiTaskOrigin::RootState(state),
            Some(parent) => PiTaskOrigin::Child(parent),
        };
        let input = authenticate_pi_task_inner(runtime, task.binding, origin, work, pi_definition.identity()).await?;
        authenticate_original_alias(runtime, task.binding.task_id, input.value.1,
            &input.value.0, task.alias, work).await?;
    }
    if let Some(work) = work {
        let mut slots = vector_slots::<PiTaskObservation>(tasks.value.len(), work)?;
        custody.as_mut().expect("native graph custody").absorb(&mut slots).map_err(native_error)?;
    }
    let mut observations = Vec::with_capacity(tasks.value.len());
    for task in tasks.value {
        if let Some(preparation) = task.preparation {
            let recovered = recover_work(runtime, preparation, work).await?;
            let phase = match recovered.value {
                WorkPull::Claimed(_) => PiPreparationPhase::Reserved,
                WorkPull::Unresolved { .. } => PiPreparationPhase::Uncertain,
                WorkPull::Idle => {
                    let outcome = observe_outcome(runtime, preparation, work).await?;
                    match outcome.value {
                        None => PiPreparationPhase::Pending,
                        Some(Outcome::Succeeded(_)) => PiPreparationPhase::Succeeded,
                        Some(Outcome::Failed { .. }) => PiPreparationPhase::Failed,
                        Some(Outcome::Cancelled) => PiPreparationPhase::Cancelled,
                        Some(Outcome::Indeterminate { .. }) => PiPreparationPhase::Uncertain,
                    }
                }
            };
            observations.push(PiTaskObservation::Preparation {
                task_id: task.binding.task_id, preparation, phase,
            });
            continue;
        }
        let task_id = task.binding.task_id;
        let mut recovered = recover_work(runtime, task_id, work).await?;
        transfer_custody(&mut custody, &mut recovered)?;
        let observation = match recovered.value {
            WorkPull::Claimed(lease) => PiTaskObservation::Reserved { lease },
            WorkPull::Unresolved { lease, error } => PiTaskObservation::Unresolved { lease, error },
            WorkPull::Idle => {
                let mut outcome = observe_outcome(runtime, task_id, work).await?;
                transfer_custody(&mut custody, &mut outcome)?;
                match outcome.value {
                    Some(outcome) => PiTaskObservation::Outcome {
                        task_id,
                        outcome: match outcome {
                            Outcome::Succeeded(value) => {
                                let mut decoded = decode_recovery_value::<FileRef>(
                                    value, acyclic_harness::JsonValueSchema::None, work)?;
                                transfer_custody(&mut custody, &mut decoded)?;
                                Outcome::Succeeded(decoded.value)
                            }
                            Outcome::Failed { message } => Outcome::Failed { message },
                            Outcome::Cancelled => Outcome::Cancelled,
                            Outcome::Indeterminate { operation_id } => Outcome::Indeterminate { operation_id },
                        },
                    },
                    None => PiTaskObservation::Pending { task_id },
                }
            }
        };
        observations.push(observation);
    }
    Ok(NativeOwned { value: observations, custody })
}

/// Routing labels are metadata, but must still match the originally admitted
/// native input. A mutable catalog never authorizes relabelling another task.
async fn authenticate_original_alias<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>,
    task: TaskId,
    operation: acyclic_harness::OperationId,
    input: &PiTaskInput,
    alias: Option<&str>, work: Option<&NativeMaterializationWork>,
) -> Result<()>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    let PiTaskExecution::Child { launch } = &input.execution else { return Ok(()); };
    if launch.kind != super::pi_task::NATIVE_LAUNCH_TASK_COMMAND_KIND { return Ok(()); }
    let alias = alias.ok_or_else(|| invalid("Pi child catalog has no original agent alias"))?;
    match work {
        Some(work) => {
            if launch.payload.descriptor().media_type() != "application/json"
                || launch.payload.descriptor().byte_length() > super::MAXIMUM_ADAPTER_STATE_BYTES {
                return Err(invalid("Pi original native intent exceeds its retained input boundary"));
            }
            let bytes = runtime.task_host().read_recipient_file_with_native_work(task, &launch.payload, work).await?;
            let intent = super::pi_root_provider::guarded_pi_decode::<super::pi_native::PiNativeLaunchIntent>(
                &bytes.0, super::MAXIMUM_ADAPTER_STATE_BYTES,
                std::mem::size_of::<super::pi_native::PiNativeLaunchIntent>() as u64,
                acyclic_harness::JsonValueSchema::None,
                super::pi_native::PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES, work)?;
            intent.0.validate_with_work(work)?;
            if intent.0.agent_alias != alias {
                return Err(invalid("Pi catalog alias differs from its original native launch input"));
            }
            drop(intent);
            drop(bytes);
        }
        None => {
            let context = runtime.harness().durable_context(task, operation).await?;
            let intent = super::pi_native::PiNativeLaunchIntent::load(&context, &launch.payload).await?;
            if intent.agent_alias != alias {
                return Err(invalid("Pi catalog alias differs from its original native launch input"));
            }
        }
    }
    Ok(())
}

/// Transient authenticated references, never another persisted lease registry.
pub(crate) struct PiTaskActivation<P> {
    pub(crate) lease: WorkLease,
    pub(crate) owner: TaskJournalOwner<P>,
}

/// Reopens only an existing attempt's settlement owner. It never starts a
/// task or recreates native directories, including after cancellation.
pub(crate) async fn recover_pi_receipt_owner<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>,
    binding: &PiTaskBinding,
    origin: PiTaskOrigin<'_>,
) -> Result<TaskJournalOwner<P>>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    authenticate_pi_task(runtime, binding, origin).await?;
    let host = runtime.task_host();
    let lease = match host.recover_work(binding.task_id).await? {
        WorkPull::Claimed(lease) | WorkPull::Unresolved { lease, .. } => lease,
        WorkPull::Idle => return Err(invalid("Pi task has no retained attempt to reconcile")),
    };
    // A cancelled admission that never started has no settlement owner. The
    // shared host checks phase and current fence; a lease reference alone is
    // insufficient, and cancellation cannot be converted into a fresh start.
    let fence = LeaseFence::from(&lease.reservation);
    host.verify_execution_owner(binding.task_id, fence.clone()).await?;
    host.journal_owner(binding.task_id, fence).await
}

/// Reopen the original task before accessing its selected workspaces. This
/// never pulls new work, fabricates a fence, or publishes a replacement effect.
pub(crate) async fn recover_pi_task<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>,
    binding: &PiTaskBinding,
    origin: PiTaskOrigin<'_>,
) -> Result<PiTaskActivation<P>>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    authenticate_pi_task(runtime, binding, origin).await?;
    let host = runtime.task_host();
    let lease = match host.recover_work(binding.task_id).await? {
        WorkPull::Claimed(lease) => lease,
        WorkPull::Idle => return Err(invalid("Pi task has no retained execution reservation")),
        WorkPull::Unresolved { error, .. } => return Err(error),
    };
    host.start_task(&lease).await?;
    let owner = host
        .journal_owner(binding.task_id, LeaseFence::from(&lease.reservation)).await?;
    host.verify_dispatch_owner(binding.task_id, LeaseFence::from(&lease.reservation)).await?;
    Ok(PiTaskActivation { lease, owner })
}

pub(crate) async fn authenticate_pi_task<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>, binding: &PiTaskBinding,
    origin: PiTaskOrigin<'_>,
) -> Result<PiTaskInput>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    let definition = Arc::new(PiTaskMachine::new()).definition()?;
    Ok(authenticate_pi_task_inner(runtime, binding, origin, None, definition.identity()).await?.value.0)
}

pub(crate) async fn authenticate_pi_task_with_native_work<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>, binding: &PiTaskBinding,
    origin: PiTaskOrigin<'_>, work: &NativeMaterializationWork,
) -> Result<(PiTaskInput, NativeAllocation)>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    let _schema_custody = schema_custody(Some(work))?;
    let definition = Arc::new(PiTaskMachine::new()).definition()?;
    let admitted = authenticate_pi_task_inner(runtime, binding, origin, Some(work), definition.identity()).await?;
    Ok((admitted.value.0, admitted.custody.expect("native authenticated input custody")))
}

async fn authenticate_pi_task_inner<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>, binding: &PiTaskBinding,
    origin: PiTaskOrigin<'_>, work: Option<&NativeMaterializationWork>,
    identity: &acyclic_harness::registry::ComponentIdentity,
) -> Result<NativeOwned<(PiTaskInput, acyclic_harness::OperationId)>>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    let _metadata_custody = work.map(|work| {
        work.cancellation().check().map_err(|_| native_error(NativeTreeMaterializationError::Cancelled))?;
        work.retain_owned(binding_metadata_quota(binding)?).map_err(native_error)
    }).transpose()?;
    binding.validate(None, binding.project_volumes.keys().map(String::as_str), true)?;
    let mut admission = observe_admission(runtime, binding.task_id, work).await?;
    let mut admitted = decode_recovery_value::<PiTaskInput>(
        std::mem::take(&mut admission.value.input), acyclic_harness::JsonValueSchema::None, work)?;
    if admission.value.task != *identity
        || admission.value.parent != origin.parent()
        || admitted.value.binding.native_session_id != binding.native_session_id
        || admitted.value.binding.task_id != binding.task_id
        || admitted.value.binding.agent_id != binding.agent_id
        || admitted.value.binding.project_volumes != binding.project_volumes
        || admitted.value.binding.scratch.volume != binding.scratch.volume
        || admitted.value.binding.results.as_ref().map(|result| &result.volume)
            != binding.results.as_ref().map(|result| &result.volume)
    {
        return Err(invalid("Pi binding differs from its original task admission"));
    }
    match (&admitted.value.execution, origin) {
        (PiTaskExecution::Host { root: original, .. }, PiTaskOrigin::Root(retained))
            if original == retained => original.validate(binding).map_err(Error::Invalid)?,
        (PiTaskExecution::Host { root: original, .. }, PiTaskOrigin::RootState(retained))
            if root_matches_state(original, retained) =>
                original.validate(binding).map_err(Error::Invalid)?,
        (PiTaskExecution::Child { .. }, PiTaskOrigin::Child(_)) => {}
        _ => return Err(invalid("Pi execution origin differs from its original admission")),
    }
    for volume in binding.project_volumes.values()
        .chain(std::iter::once(&binding.scratch.volume))
        .chain(binding.results.as_ref().map(|result| &result.volume))
    {
        let read = volume.capability(acyclic_harness::conversation::VolumeOperation::Read)?;
        if !admission.value.grants.contains(&read) {
            return Err(invalid("Pi selection lacks its original read grant"));
        }
    }
    let scratch_write = binding.scratch.volume.capability(acyclic_harness::conversation::VolumeOperation::Write)?;
    if !admission.value.grants.contains(&scratch_write) {
        return Err(invalid("Pi scratch lacks its original write grant"));
    }
    if let Some(result) = &binding.results
        && !admission.value.grants.contains(
            &result.volume.capability(acyclic_harness::conversation::VolumeOperation::Write)?)
    {
        return Err(invalid("Pi result storage lacks its original created write grant"));
    }
    // The moved input still owns SDK-produced strings. Transfer its actual
    // original producer guard, not merely a reconstructed DTO-size summary.
    let operation = admission.value.operation_id;
    transfer_custody(&mut admitted.custody, &mut admission)?;
    Ok(NativeOwned { value: (admitted.value, operation), custody: admitted.custody })
}

/// Compares the original immutable Root DTO directly with retained references;
/// no path String, root map or replacement authority is constructed.
fn root_matches_state(original: &PiRootIntent, retained: &AdapterState) -> bool {
    match original {
        PiRootIntent::ScratchOnly {} => retained.root_context_id == [0; 16]
            && retained.root_id == [0; 16] && retained.roots.is_empty(),
        PiRootIntent::Project { context_id, root_id, paths } =>
            *context_id == retained.root_context_id && *root_id == retained.root_id
            && paths.len() == retained.roots.len()
            && paths.iter().zip(&retained.roots).all(|((key, path), (retained_key, root))|
                key == retained_key && root.path.to_str() == Some(path.as_str())),
    }
}

fn invalid(message: &'static str) -> Error { Error::Invalid(message.to_owned()) }

fn native_error(cause: NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}

/// Payload precedes custody so every early return destroys the actual DTO first.
struct NativeOwned<T> {
    value: T,
    custody: Option<NativeAllocation>,
}

impl<T> NativeOwned<T> {
    fn ordinary(value: T) -> Self { Self { value, custody: None } }
    fn native((value, custody): (T, NativeAllocation)) -> Self {
        Self { value, custody: Some(custody) }
    }
}

/// Closed first-party recovery DTOs consume their owned Value rather than
/// cloning it. This is conservative logical field/sequence ownership admission,
/// not a claim about serde's BTree nodes, allocator capacity or RSS.
fn decode_recovery_value<T: serde::de::DeserializeOwned>(
    value: serde_json::Value, schema: acyclic_harness::JsonValueSchema,
    work: Option<&NativeMaterializationWork>,
) -> Result<NativeOwned<T>> {
    let Some(work) = work else {
        return serde_json::from_value(value).map(NativeOwned::ordinary)
            .map_err(|_| invalid("Pi recovery content differs from its closed admitted DTO"));
    };
    let mut custody = work.retain_owned(0).map_err(native_error)?;
    let mut retained_owned = 0;
    let remaining = work.remaining().map_err(native_error)?;
    let bounds = acyclic_harness::JsonBounds {
        maximum_bytes: super::MAXIMUM_ADAPTER_STATE_BYTES,
        maximum_items: remaining.items_examined, maximum_depth: 64,
    };
    // The genuine SDK consuming converter preserves map order and moves the
    // original resident Strings. Its SDK-issued shape admits NEW ownership
    // before serde; the caller's full admission/outcome guard remains alive.
    let result = acyclic_harness::json_from_value_owned_bounded_native_trusted::<T, _>(
        value, bounds, std::mem::size_of::<T>() as u64,
        acyclic_harness::wire::ErrorCode::Invalid, schema, &mut |charge| {
            if let acyclic_harness::JsonCodecCharge::Admission { maximum_owned_bytes, value_shape } = charge {
                let (additional, diagnostics) = recovery_conversion_quota(value_shape)?;
                retained_owned = checked_quota(maximum_owned_bytes, additional)?;
                custody.resize_retained(checked_quota(retained_owned, diagnostics)?).map_err(native_error)
            } else {
                super::pi_root_provider::native_buffer_codec_charge(work, &mut custody, charge)
            }
        });
    match result {
        Ok(value) => {
            custody.substitute(retained_owned).map_err(native_error)?;
            Ok(NativeOwned { value, custody: Some(custody) })
        }
        Err(Error::NativeDecode { code, source, custody: None }) =>
            Err(Error::NativeDecode { code, source, custody: Some(custody) }),
        Err(error) => {
            // SDK has physically dropped the owned source/partial DTO first.
            drop(custody);
            Err(error)
        }
    }
}

fn recovery_conversion_quota(shape: acyclic_harness::JsonValueShape) -> Result<(u64, u64)> {
    // Concrete closed recovery slots mirror the SDK PreparationRequest manifest
    // plus Pi's binding/command types. Eight slots per actual resident/virtual
    // node cover tagged serde Content, typed containers and collection slack.
    // This is not an arbitrary Deserialize, allocator-node or RSS theorem.
    let slot = [
        std::mem::size_of::<acyclic_harness::resource_preparation::ResourcePreparationRequest>(),
        std::mem::size_of::<acyclic_harness::runtime::TaskAdmissionRecord>(),
        std::mem::size_of::<acyclic_harness::resource_preparation::ResourceCreationProofRef>(),
        std::mem::size_of::<acyclic_harness::resources::ResourceRef>(),
        std::mem::size_of::<acyclic_harness::resources::ProviderRef>(),
        std::mem::size_of::<acyclic_harness::source_authorization::ProjectSourceAuthorization>(),
        std::mem::size_of::<acyclic_harness::workflow::WorkflowCommand>(),
        std::mem::size_of::<PiTaskInput>(), std::mem::size_of::<PiTaskBinding>(),
        std::mem::size_of::<FileRef>(), std::mem::size_of::<(String, serde_json::Value)>(),
    ].into_iter().max().expect("closed recovery slot manifest") as u64;
    let typed = shape.lexical_items().checked_add(shape.virtual_items())
        .and_then(|nodes| nodes.checked_mul(8)).and_then(|nodes| nodes.checked_mul(slot))
        .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?;
    let additional = acyclic_harness::json_owned_value_conversion_additional_bytes(shape)?;
    // Stock preparation/ref diagnostics plus these actual admission/Pi fields;
    // use the SDK's shared recipe, not a resident-byte ratio or guessed label cap.
    let labels = checked_quota(
        acyclic_harness::preparation_serde_diagnostic_label_bytes(),
        checked_quota(super::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
            super::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
                "ResourcePreparationRequest", "struct ResourcePreparationRequest",
                "provider", "child", "resources", "creation_grant", "maximum_resources",
                "maximum_work", "maximum_bytes", "payload", "command", "source_checkpoint",
                "input_files", "read_authorization",
                "ResourceCreationProofRef", "struct ResourceCreationProofRef",
                "preparation", "resource", "evidence",
                "TaskAdmissionRecord", "struct TaskAdmissionRecord",
                "operation_id", "task", "machine", "input", "input_schema", "output_schema",
                "parent", "resource_proofs", "grants", "limits", "run_limits", "policy",
                "extensions", "execution",
                "ComponentIdentity", "struct ComponentIdentity", "name", "version",
                "TaskRunLimits", "struct TaskRunLimits",
                "concurrency", "max_steps", "deadline_epoch_ms",
                "ExecutionPlacement", "struct ExecutionPlacement",
                "provider", "build", "environment", "readiness_revision",
                "Authority", "struct Authority", "kind", "id",
                "AggregateKind", "agent", "conversation", "session", "turn", "task",
                "ExtensionAdmission", "struct ExtensionAdmission",
                "source", "selected", "configurations", "Limits", "struct Limits",
            ]))?)?;
    let diagnostics = acyclic_harness::json_typed_diagnostic_owned_bytes(
        shape, labels, Some(acyclic_harness::ErrorCode::Invalid))?;
    Ok((checked_quota(typed, additional)?, diagnostics))
}

fn transfer_custody<T>(
    retained: &mut Option<NativeAllocation>, observed: &mut NativeOwned<T>,
) -> Result<()> {
    match (retained.as_mut(), observed.custody.as_mut()) {
        (Some(retained), Some(custody)) => retained.absorb(custody).map_err(native_error)?,
        (None, Some(_)) => *retained = observed.custody.take(),
        _ => {}
    }
    Ok(())
}

pub(crate) fn vector_slots<T>(count: usize, work: &NativeMaterializationWork) -> Result<NativeAllocation> {
    let bytes = count.checked_mul(std::mem::size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?;
    if count == 0 { return work.retain_owned(0).map_err(native_error); }
    // Exactly one with_capacity allocation attempt follows this admission.
    work.claim(bytes).map_err(native_error)
}

fn schema_custody(work: Option<&NativeMaterializationWork>) -> Result<Option<NativeAllocation>> {
    // The two closed first-party machine/definition schemas plus their retained
    // implementation identity sources. This opaque logical quota precedes
    // construction; no allocator-node/RSS or hidden allocation count is claimed.
    work.map(|work| work.retain_owned(256 * 1024).map_err(native_error)).transpose()
}

fn checked_quota(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))
}


fn binding_metadata_quota(binding: &PiTaskBinding) -> Result<u64> {
    let mut bytes = checked_quota(4096, binding.native_session_id.len() as u64)?;
    for (key, _) in &binding.project_volumes { bytes = checked_quota(bytes, key.len() as u64)?; }
    for volume in binding.project_volumes.values()
        .chain(std::iter::once(&binding.scratch.volume))
        .chain(binding.results.as_ref().map(|result| &result.volume))
    {
        let provider = volume.provider();
        let strings = [provider.namespace(), provider.family(), provider.version(), volume.id()];
        let mut source = strings.iter().try_fold(4096_u64, |size, text|
            checked_quota(size, text.len() as u64))?;
        if let acyclic_harness::conversation::VolumeOwner::Project(owner) = volume.owner() {
            source = checked_quota(source, owner.len() as u64)?;
        }
        // Logical fields for canonical capability/storage-name production,
        // validation sequences and bounded escaping, never BTree node storage.
        bytes = checked_quota(bytes, source.checked_mul(16)
            .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?)?;
    }
    Ok(bytes)
}

fn recovery_metadata_quota(state: &AdapterState) -> Result<u64> {
    let mut bytes = binding_metadata_quota(pi_binding(&state.task_binding)?)?;
    for root in state.roots.values() {
        let path = root.path.to_str().ok_or_else(|| invalid("Pi root path has no supported wire representation"))?;
        bytes = checked_quota(bytes, checked_quota(4096, (path.len() as u64).checked_mul(8)
            .ok_or_else(|| native_error(NativeTreeMaterializationError::Work(WorkError::Overflow)))?)?)?;
    }
    for route in state.routes.values() {
        bytes = checked_quota(bytes, binding_metadata_quota(pi_binding(&route.task_binding)?)?)?;
        bytes = checked_quota(bytes, 4096)?;
    }
    for pending in &state.pending {
        bytes = checked_quota(bytes, binding_metadata_quota(pi_binding(&pending.task_binding)?)?)?;
        bytes = checked_quota(bytes, checked_quota(4096, pending.tool_use_id.len() as u64)?)?;
    }
    // Existing validation's local Strings and reference Vecs are admitted
    // before session_tasks grows them. This does not exclude newly produced
    // SDK values, which each arrive with their own original producer guard.
    Ok(bytes)
}

async fn observe_admission<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>, task: TaskId,
    work: Option<&NativeMaterializationWork>,
) -> Result<NativeOwned<acyclic_harness::runtime::TaskAdmissionRecord>>
where P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static, O: acyclic_fs::AsyncObjectStore + 'static,
{
    match work {
        Some(work) => Ok(NativeOwned::native(runtime.task_host().observe_admission_with_native_work(task, work).await?)),
        None => Ok(NativeOwned::ordinary(runtime.task_host().observe_admission(task).await?)),
    }
}

async fn recover_work<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>, task: TaskId,
    work: Option<&NativeMaterializationWork>,
) -> Result<NativeOwned<WorkPull>>
where P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static, O: acyclic_fs::AsyncObjectStore + 'static,
{
    match work {
        Some(work) => Ok(NativeOwned::native(runtime.task_host().recover_work_with_native_work(task, work).await?)),
        None => Ok(NativeOwned::ordinary(runtime.task_host().recover_work(task).await?)),
    }
}

async fn observe_outcome<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>, task: TaskId,
    work: Option<&NativeMaterializationWork>,
) -> Result<NativeOwned<Option<Outcome<serde_json::Value>>>>
where P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static, O: acyclic_fs::AsyncObjectStore + 'static,
{
    match work {
        Some(work) => Ok(NativeOwned::native(runtime.task_host().outcome_with_native_work(task, work).await?)),
        None => Ok(NativeOwned::ordinary(runtime.task_host().outcome(task).await?)),
    }
}

#[cfg(test)]
mod allocation_tests {
    use super::*;

    #[test]
    fn native_graph_vector_claim_precedes_growth_and_keeps_original_ledger() {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = NativeMaterializationWork::new(super::super::pi_root_provider::allocation_budget(),
            &cancellation).unwrap();
        let before = work.receipt();
        let custody = vector_slots::<PiTaskObservation>(3, &work).unwrap();
        assert_eq!(work.receipt().allocation_operations, before.allocation_operations + 1);
        let mut observations = Vec::with_capacity(3);
        for index in 0..3 {
            observations.push(PiTaskObservation::Pending { task_id: TaskId::from_bytes([index + 1; 16]) });
        }
        assert_eq!(observations.capacity(), 3);
        assert_eq!(custody.bytes(), (3 * std::mem::size_of::<PiTaskObservation>()) as u64);
        let original = work.retain_owned(0).unwrap();
        assert!(custody.shares_ledger(&original));
        drop(observations);
        drop(custody);
        drop(original);
        assert_eq!(work.live_allocation_bytes(), 0);
        cancellation.cancel();
        assert!(matches!(vector_slots::<PiTaskObservation>(3, &work),
            Err(Error::NativeWork { cause: NativeTreeMaterializationError::Cancelled, .. })));
    }

    #[test]
    fn native_graph_rejects_foreign_custody_without_releasing_observed_payload_guard() {
        let cancellation = acyclic_fs::CancellationToken::new();
        let first = NativeMaterializationWork::new(super::super::pi_root_provider::allocation_budget(),
            &cancellation).unwrap();
        let second = NativeMaterializationWork::new(super::super::pi_root_provider::allocation_budget(),
            &cancellation).unwrap();
        let mut retained = Some(first.retain_owned(8).unwrap());
        let mut observed = NativeOwned::native(("original producer", second.retain_owned(16).unwrap()));
        assert!(matches!(transfer_custody(&mut retained, &mut observed),
            Err(Error::NativeWork { cause: NativeTreeMaterializationError::InvalidLayout, .. })));
        assert_eq!(retained.as_ref().unwrap().bytes(), 8);
        assert_eq!(observed.custody.as_ref().unwrap().bytes(), 16);
        assert_eq!(second.live_allocation_bytes(), 16);
        drop(observed);
        drop(retained);
        assert_eq!(first.live_allocation_bytes(), 0);
        assert_eq!(second.live_allocation_bytes(), 0);
    }
}
