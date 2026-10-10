//! Service recovery over original task admissions and retained reservations.

use super::pi_root::PiRootIntent;
use super::pi_task::{PiTaskExecution, PiTaskInput, PiTaskMachine};
use super::{AdapterState, PiTaskBinding, PluginTaskBinding, display};
use acyclic_harness::{
    Outcome, TaskId,
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
}

/// The caller's retained origin must match both immutable admission lineage
/// and execution mode. Root recovery also authenticates its physical selection.
pub(crate) enum PiTaskOrigin<'a> {
    Root(&'a PiRootIntent),
    Child(TaskId),
}

impl PiTaskOrigin<'_> {
    fn parent(&self) -> Option<TaskId> {
        match self {
            Self::Root(_) => None,
            Self::Child(parent) => Some(*parent),
        }
    }
}

fn pi_binding(binding: &PluginTaskBinding) -> Result<&PiTaskBinding, String> {
    match binding {
        PluginTaskBinding::Pi(binding) => Ok(binding.as_ref()),
        PluginTaskBinding::HostManaged => Err("Pi recovery requires a Pi task binding".to_owned()),
    }
}

/// Plans recovery from the existing route graph. The plan holds references,
/// and grants no authority to reopen a task or workspace.
fn session_tasks(state: &AdapterState) -> Result<Vec<PiTaskSelection<'_>>, String> {
    super::validate_state_version(state)?;
    super::validate_state_bounds(state)?;
    let root = pi_binding(&state.task_binding)?;
    if state.root_agent_id.is_empty() || state.routes.contains_key(&state.root_agent_id) {
        return Err("Pi recovery root agent is missing or duplicated".to_owned());
    }
    let parent_task = |parent: &str| {
        if parent == state.root_agent_id {
            Ok(root.task_id)
        } else {
            state
                .routes
                .get(parent)
                .ok_or_else(|| "Pi recovery has no retained parent route".to_owned())
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
    let mut tasks = vec![PiTaskSelection {
        binding: root,
        parent: None,
    }];
    let mut queue = std::collections::VecDeque::from([state.root_agent_id.as_str()]);
    while let Some(parent) = queue.pop_front() {
        for route in children.remove(parent).unwrap_or_default() {
            tasks.push(PiTaskSelection {
                binding: pi_binding(&route.task_binding)?,
                parent: Some(parent_task(parent)?),
            });
            queue.push_back(&route.agent_id);
        }
    }
    if !children.is_empty() {
        return Err("Pi recovery route graph contains a parent cycle".to_owned());
    }
    for pending in &state.pending {
        tasks.push(PiTaskSelection {
            binding: pi_binding(&pending.task_binding)?,
            parent: Some(parent_task(&pending.parent_agent_id)?),
        });
    }
    Ok(tasks)
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
        error: String,
    },
    Outcome {
        task_id: TaskId,
        outcome: Outcome<FileRef>,
    },
}

/// Authenticates the complete retained graph before observing task states.
/// Frozen, cancelled and completed routes do not implicitly start execution.
pub(crate) async fn observe_pi_session<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>,
    state: &AdapterState,
) -> Result<Vec<PiTaskObservation>, String>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    let tasks = session_tasks(state)?;
    let root = PiRootIntent::from_state(state)?;
    for task in &tasks {
        let origin = match task.parent {
            None => PiTaskOrigin::Root(&root),
            Some(parent) => PiTaskOrigin::Child(parent),
        };
        authenticate_pi_task(runtime, task.binding, origin).await?;
    }
    let mut observations = Vec::with_capacity(tasks.len());
    for task in tasks {
        let task_id = task.binding.task_id;
        let host = runtime.task_host();
        let observation = match host.recover_work(task_id).await.map_err(display)? {
            WorkPull::Claimed(lease) => PiTaskObservation::Reserved { lease },
            WorkPull::Unresolved { lease, error } => PiTaskObservation::Unresolved {
                lease,
                error: display(error),
            },
            WorkPull::Idle => match host.outcome(task_id).await.map_err(display)? {
                Some(outcome) => PiTaskObservation::Outcome {
                    task_id,
                    outcome: match outcome {
                        Outcome::Succeeded(value) => {
                            Outcome::Succeeded(serde_json::from_value(value).map_err(display)?)
                        }
                        Outcome::Failed { message } => Outcome::Failed { message },
                        Outcome::Cancelled => Outcome::Cancelled,
                        Outcome::Indeterminate { operation_id } => {
                            Outcome::Indeterminate { operation_id }
                        }
                    },
                },
                None => PiTaskObservation::Pending { task_id },
            },
        };
        observations.push(observation);
    }
    Ok(observations)
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
) -> Result<TaskJournalOwner<P>, String>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    authenticate_pi_task(runtime, binding, origin).await?;
    let host = runtime.task_host();
    let lease = match host.recover_work(binding.task_id).await.map_err(display)? {
        WorkPull::Claimed(lease) | WorkPull::Unresolved { lease, .. } => lease,
        WorkPull::Idle => return Err("Pi task has no retained attempt to reconcile".to_owned()),
    };
    // A cancelled admission that never started has no settlement owner. The
    // shared host checks phase and current fence; a lease reference alone is
    // insufficient, and cancellation cannot be converted into a fresh start.
    let fence = LeaseFence::from(&lease.reservation);
    host.verify_execution_owner(binding.task_id, fence.clone())
        .await
        .map_err(display)?;
    host.journal_owner(binding.task_id, fence)
        .await
        .map_err(display)
}

/// Reopen the original task before accessing its selected workspaces. This
/// never pulls new work, fabricates a fence, or publishes a replacement effect.
pub(crate) async fn recover_pi_task<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>,
    binding: &PiTaskBinding,
    origin: PiTaskOrigin<'_>,
) -> Result<PiTaskActivation<P>, String>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    authenticate_pi_task(runtime, binding, origin).await?;
    let host = runtime.task_host();
    let lease = match host.recover_work(binding.task_id).await.map_err(display)? {
        WorkPull::Claimed(lease) => lease,
        WorkPull::Idle => return Err("Pi task has no retained execution reservation".to_owned()),
        WorkPull::Unresolved { error, .. } => return Err(display(error)),
    };
    host.start_task(&lease).await.map_err(display)?;
    let owner = host
        .journal_owner(binding.task_id, LeaseFence::from(&lease.reservation))
        .await
        .map_err(display)?;
    host.verify_dispatch_owner(binding.task_id, LeaseFence::from(&lease.reservation))
        .await
        .map_err(display)?;
    Ok(PiTaskActivation { lease, owner })
}

pub(crate) async fn authenticate_pi_task<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>,
    binding: &PiTaskBinding,
    origin: PiTaskOrigin<'_>,
) -> Result<PiTaskInput, String>
where
    P: acyclic_stream::StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    binding.validate(
        None,
        binding.project_volumes.keys().map(String::as_str).collect(),
        true,
    )?;
    let host = runtime.task_host();
    let admission = host
        .observe_admission(binding.task_id)
        .await
        .map_err(display)?;
    let admitted: PiTaskInput = serde_json::from_value(admission.input.clone()).map_err(display)?;
    let machine = Arc::new(PiTaskMachine::new());
    if admission.task != *machine.definition().map_err(display)?.identity()
        || admission.parent != origin.parent()
        || admitted.binding.native_session_id != binding.native_session_id
        || admitted.binding.task_id != binding.task_id
        || admitted.binding.agent_id != binding.agent_id
        || admitted.binding.project_volumes != binding.project_volumes
        || admitted.binding.scratch.volume != binding.scratch.volume
    {
        return Err("Pi binding differs from its original task admission".to_owned());
    }
    match (&admitted.execution, origin) {
        (PiTaskExecution::Host { root: original, .. }, PiTaskOrigin::Root(retained))
            if original == retained =>
        {
            original.validate(binding)?;
        }
        (PiTaskExecution::Child { .. }, PiTaskOrigin::Child(_)) => {}
        _ => return Err("Pi execution origin differs from its original admission".to_owned()),
    }
    // Reject a selection outside the original grants before start_task can
    // publish a lifecycle transition. A project may deliberately be read-only.
    for volume in binding
        .project_volumes
        .values()
        .chain(std::iter::once(&binding.scratch.volume))
    {
        let read = volume
            .capability(acyclic_harness::conversation::VolumeOperation::Read)
            .map_err(display)?;
        if !admission.grants.contains(&read) {
            return Err("Pi selection lacks its original read grant".to_owned());
        }
    }
    let scratch_write = binding
        .scratch
        .volume
        .capability(acyclic_harness::conversation::VolumeOperation::Write)
        .map_err(display)?;
    if !admission.grants.contains(&scratch_write) {
        return Err("Pi scratch lacks its original write grant".to_owned());
    }
    Ok(admitted)
}
