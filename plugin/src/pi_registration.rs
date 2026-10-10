//! Original Pi admission, before any selected native root is opened.

use super::*;
use acyclic_harness::{
    AgentId, Capabilities, TaskId,
    conversation::{Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    resources::ProviderRef,
    runtime::TaskRunLimits,
    scheduler::SessionLimits,
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use pi_owner::{PiOwnerConfig, PiOwnerPolicy};
use pi_root::PiRootIntent;
use pi_root_inputs::{PiRootRequest, ROOT_ALLOCATION_PROVIDER, ROOT_INPUT_DIRECTORY};

fn identity(domain: &str, session: &str) -> [u8; 16] {
    let digest = blake3::hash(format!("acyclic.pi/{domain}/v1/{session}").as_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(&digest.as_bytes()[..16]);
    id
}

/// The catalog holds its existing opening reservation for this entire operation.
/// Interrupted registration reloads the original owner and exact deterministic
/// request; it never creates another task, key, budget or selected allocation.
pub(crate) async fn register(
    resources: &ServiceResources,
    session: &str,
    path: PathBuf,
) -> Result<SessionController, String> {
    let directory = resources.session_directory(session);
    fs::create_dir_all(&directory).map_err(display)?;
    let (previous, _) = load_state(&directory)?;
    if !previous.root_session_id.is_empty() {
        if previous.root_session_id != session || !matches!(previous.task_binding, PluginTaskBinding::Pi(_)) {
            return Err("Pi original session selection differs".to_owned());
        }
        return SessionController::open(directory, resources).await;
    }
    let path = path.canonicalize().map_err(display)?;
    let provider = ProviderRef::new("acyclic.pi", "filesystem", "1").map_err(display)?;
    let task = TaskId::from_bytes(identity("task", session));
    let agent = AgentId::from_bytes(identity("agent", session));
    let root_id = identity("root", session);
    let context_id = identity("context", session);
    let journal = VolumeRef::new(provider.clone(), "journal", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent)).map_err(display)?;
    let scratch = VolumeRef::new(provider.clone(), "scratch", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent)).map_err(display)?;
    let project = VolumeRef::new(provider.clone(), "project", VolumeClass::Project, VolumeOwner::Project(hex::encode(root_id))).map_err(display)?;
    let binding = PiTaskBinding {
        native_session_id: session.to_owned(), task_id: task, agent_id: agent,
        project_volumes: BTreeMap::from([(hex::encode(root_id), project.clone())]),
        scratch: PiScratchBinding { volume: scratch.clone(), allocation: PiScratchAllocation::Preparing },
    };
    let root = PiRootIntent::Project {
        context_id, root_id,
        paths: BTreeMap::from([(hex::encode(root_id), path.to_str().ok_or_else(|| "Pi root path is not UTF-8".to_owned())?.to_owned())]),
    };
    root.validate(&binding)?;
    let mut grants = vec![
        "operation:declare".to_owned(), "operation:observe".to_owned(), "operation:cancel".to_owned(),
        "task:spawn:acyclic.pi@1".to_owned(), "effect:plan".to_owned(), "effect:run".to_owned(),
        "fork:publish".to_owned(),
        format!("effect:provider:{ROOT_ALLOCATION_PROVIDER}"),
        journal.directory_read_capability(ROOT_INPUT_DIRECTORY).map_err(display)?,
    ];
    for volume in [&scratch, &project] {
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            grants.push(volume.capability(operation).map_err(display)?);
        }
    }
    let policy = PiOwnerPolicy {
        root_task: task, agent, journal: journal.clone(), grants: Capabilities::new(grants),
        limits: Limits::default(), run_limits: TaskRunLimits::default(),
        session_limits: SessionLimits { active_tasks: 16, total_tasks: 256, depth: 16, model_steps: 4096 },
        concurrency: 16, maximum_payload_bytes: 1024 * 1024,
    };
    let owner = if directory.join("pi-owner.v1").exists() {
        let original = PiOwnerConfig::load(&directory)?;
        if original.policy() != &policy { return Err("Pi original root policy differs".to_owned()); }
        original
    } else {
        if directory.join("stream").exists() {
            return Err("Pi original task storage exists but its owner credential is missing".to_owned());
        }
        let owner = PiOwnerConfig::new(policy)?;
        owner.save_new(&directory)?;
        owner
    };
    let filesystem = Arc::new(acyclic_harness::filesystem::FilesystemHost::new(resources.fs.clone(), provider).map_err(display)?);
    // Journal storage is owner infrastructure, not a selected task allocation.
    // Its creation operation is fixed, and its grant is not exposed to Pi.
    let cancellation = CancellationToken::new();
    resources.fs.create_workspace_with_operation_measured(
        journal.storage_name().map_err(display)?, VolumeConfig::portable(Lifecycle::Durable),
        OperationId::from_bytes(identity("journal", session)), native_root_source::budget(), &cancellation,
    ).await.map_err(|failure| format!("{}; work={:?}", failure.error, failure.work))?;
    let stream = StreamClient::new(Arc::new(LocalStream::open(directory.join("stream"), LocalStreamLimits::default()).await.map_err(display)?));
    let runtime = pi_runtime::PiTaskRuntime::open(stream, filesystem.clone(), journal, owner.authority()?).await.map_err(display)?;
    let request = PiRootRequest { binding, root };
    let command = pi_root_inputs::stage_root_command(&owner, &filesystem, &request).await.map_err(display)?;
    runtime.admit_root(request.binding.clone(), request.root.clone(), command).await.map_err(display)?;
    let state = AdapterState {
        version: ADAPTER_STATE_VERSION, active: true,
        root_session_id: session.to_owned(), root_agent_id: session.to_owned(),
        root_context_id: context_id, root_id, pending_root_registration: true,
        task_binding: PluginTaskBinding::Pi(Box::new(request.binding)),
        roots: BTreeMap::from([(hex::encode(root_id), RootBinding { root_id, path, allocation: RootAllocation::Preparing })]),
        ..AdapterState::default()
    };
    // A catalog entry is published only after authenticated durable admission.
    save_state(&directory, &state, Survives::PowerLoss)?;
    SessionController::open(directory, resources).await
}

