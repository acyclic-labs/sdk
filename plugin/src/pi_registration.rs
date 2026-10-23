//! Original Pi admission, before any selected native root is opened.

use super::*;
use acyclic_harness::store::HistoryIo;
use acyclic_harness::{
    AgentId, Capabilities, TaskId,
    conversation::{Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    resources::ProviderRef,
    runtime::TaskRunLimits,
    scheduler::SessionLimits,
};
use acyclic_stream::{LocalStream, LocalStreamLimits};
use pi_owner::{PiOwnerConfig, PiOwnerPolicy};
use pi_root::PiRootIntent;
use pi_root_inputs::{PiRootRequest, ROOT_ALLOCATION_PROVIDER, ROOT_INPUT_DIRECTORY};

fn identity(
    domain: &str,
    session: &str,
    work: &acyclic_fs::NativeMaterializationWork,
) -> acyclic_harness::Result<[u8; 16]> {
    let bytes = (b"acyclic.pi/".len() as u64)
        .checked_add(domain.len() as u64)
        .and_then(|bytes| bytes.checked_add(b"/v1/".len() as u64))
        .and_then(|bytes| bytes.checked_add(session.len() as u64))
        .ok_or(acyclic_harness::Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::Overflow,
            ),
            custody: None,
        })?;
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: bytes,
        ..acyclic_fs::WorkCounters::UNCHARGED
    })?;
    // Identical original BLAKE3 input, streamed without an owned format buffer.
    let mut hasher = blake3::Hasher::new();
    for part in [
        b"acyclic.pi/".as_slice(),
        domain.as_bytes(),
        b"/v1/".as_slice(),
        session.as_bytes(),
    ] {
        hasher.update(part);
    }
    let mut id = [0; 16];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Ok(id)
}

/// The catalog holds its existing opening reservation for this entire operation.
pub(crate) async fn register(
    resources: &ServiceResources,
    session: &str,
    path: PathBuf,
    native_runtime: Option<super::pi_native::PiNativeHostRuntime>,
    work: &acyclic_fs::NativeMaterializationWork,
) -> acyclic_harness::Result<SessionController> {
    work.admit(acyclic_fs::WorkCounters::UNCHARGED)?;
    let directory = resources.session_directory(session);
    let relative_directory = resources.private_session_data(&directory)?;
    fs::create_dir_all(&directory)
        .map_err(|error| acyclic_harness::Error::Storage(display(error)))?;
    let mut previous = load_state(&directory).map_err(acyclic_harness::Error::Storage)?;
    if !previous.0.root_session_id.is_empty() {
        if previous.0.root_session_id != session
            || !matches!(previous.0.task_binding, PluginTaskBinding::Pi(_))
        {
            return Err(acyclic_harness::Error::Invalid(
                "Pi original session selection differs".to_owned(),
            ));
        }
        return SessionController::open(directory, resources, work).await;
    }
    let path = path
        .canonicalize()
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    let path_text = path
        .to_str()
        .ok_or_else(|| acyclic_harness::Error::Invalid("Pi root path is not UTF-8".to_owned()))?;
    let owned = (std::mem::size_of::<PiRootRequest>()
        + std::mem::size_of::<AdapterState>()
        + std::mem::size_of::<PiTaskBinding>()
        + std::mem::size_of::<(String, VolumeRef)>()
        + std::mem::size_of::<(String, String)>()
        + std::mem::size_of::<(String, RootBinding)>()) as u64;
    let mut startup_custody = work.claim(owned, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
    let mut metadata = acyclic_harness::store::HistoryWork::new(
        acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
    );
    let provider_metadata = ProviderRef::new(
        metadata.clone_text("acyclic.pi")?,
        metadata.clone_text("filesystem")?,
        metadata.clone_text("1")?,
        &mut Some(&mut metadata),
    )?;
    let provider = &provider_metadata;
    let [journal_provider, scratch_provider, project_provider] = [(); 3].map(|_| {
        ProviderRef::new(
            metadata.clone_text(provider.namespace())?,
            metadata.clone_text(provider.family())?,
            metadata.clone_text(provider.version())?,
            &mut Some(&mut metadata),
        )
    });
    let task = TaskId::from_bytes(identity("task", session, work)?);
    let agent = AgentId::from_bytes(identity("agent", session, work)?);
    let root_id = identity("root", session, work)?;
    let context_id = identity("context", session, work)?;
    let root_label = work.display(&format_args!("{:032x}", u128::from_be_bytes(root_id)))?;
    let owner_label = work.display(&root_label.0)?;
    let journal_metadata = VolumeRef::new(
        journal_provider?,
        metadata.clone_text("journal")?,
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut Some(&mut metadata),
    )?;
    let journal = &journal_metadata;
    let scratch_metadata = VolumeRef::new(
        scratch_provider?,
        metadata.clone_text("scratch")?,
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut Some(&mut metadata),
    )?;
    let scratch = &scratch_metadata;
    let project_owner = VolumeOwner::Project(owner_label.0);
    let project_metadata = VolumeRef::new(
        project_provider?,
        metadata.clone_text("project")?,
        VolumeClass::Project,
        project_owner,
        &mut Some(&mut metadata),
    )?;
    let project = &project_metadata;
    startup_custody.absorb(&mut metadata.into_custody()?)?;
    let binding_session = work.display(session)?;
    let project_key = work.display(&root_label.0)?;
    let mut binding_io = acyclic_harness::store::HistoryWork::new(
        acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
    );
    let project_copy = project.copy(&mut Some(&mut binding_io))?;
    let scratch_copy = scratch.copy(&mut Some(&mut binding_io))?;
    let _binding_custody = binding_io.into_custody()?;
    let path_key = work.display(&root_label.0)?;
    let root_path = work.display(path_text)?;
    let binding = PiTaskBinding {
        native_session_id: binding_session.0,
        task_id: task,
        agent_id: agent,
        project_volumes: BTreeMap::from([(project_key.0, project_copy)]),
        scratch: PiScratchBinding {
            volume: scratch_copy,
            allocation: PiScratchAllocation::Preparing,
        },
        results: None,
    };
    let root = PiRootIntent::Project {
        context_id,
        root_id,
        paths: BTreeMap::from([(path_key.0, root_path.0)]),
    };
    root.validate(&binding, work)?;
    const GRANTS: &[&str] = &[
        "operation:declare",
        "operation:observe",
        "operation:cancel",
        "operation:wake",
        "task:spawn:acyclic.pi@1",
        "effect:plan",
        "effect:run",
        "fork:publish",
        "conversation:bind",
        "interaction:route",
        "interaction:open",
        "interaction:observe",
        "pi:source:select",
        "mail:send",
        "mail:read",
        "task:spawn:acyclic.fork_preparation@1",
        "tool:call:acyclic.pi.builtin",
    ];
    let grants = [
        work.display(&format_args!("effect:provider:{ROOT_ALLOCATION_PROVIDER}"))?,
        work.display(&format_args!(
            "effect:provider:{}",
            super::pi_native::PI_NATIVE_PROVIDER
        ))?,
        {
            let mut io = acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            );
            let value =
                journal.directory_read_capability(ROOT_INPUT_DIRECTORY, &mut Some(&mut io))?;
            (value, io.into_custody()?)
        },
        {
            let mut io = acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            );
            let value = journal.directory_read_capability(
                super::pi_root_provider::OPERATOR_INPUT_DIRECTORY,
                &mut Some(&mut io),
            )?;
            (value, io.into_custody()?)
        },
        {
            let mut io = acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            );
            let value = scratch.capability(VolumeOperation::Read, &mut Some(&mut io))?;
            (value, io.into_custody()?)
        },
        {
            let mut io = acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            );
            let value = scratch.capability(VolumeOperation::Write, &mut Some(&mut io))?;
            (value, io.into_custody()?)
        },
        {
            let mut io = acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            );
            let value = project.capability(VolumeOperation::Read, &mut Some(&mut io))?;
            (value, io.into_custody()?)
        },
        {
            let mut io = acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            );
            let value = project.capability(VolumeOperation::Write, &mut Some(&mut io))?;
            (value, io.into_custody()?)
        },
    ];
    let capabilities = Capabilities::copy(
        GRANTS
            .iter()
            .copied()
            .chain(grants.iter().map(|grant| grant.0.as_str())),
        work,
    )?;
    let mut policy_io = acyclic_harness::store::HistoryWork::new(
        acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
    );
    let policy_journal = journal.copy(&mut Some(&mut policy_io))?;
    let policy = (
        PiOwnerPolicy {
            root_task: task,
            agent,
            journal: policy_journal,
            grants: capabilities.0,
            limits: Limits::default(),
            run_limits: TaskRunLimits::default(),
            session_limits: SessionLimits {
                active_tasks: 16,
                total_tasks: 256,
                depth: 16,
                model_steps: 4096,
            },
            concurrency: 16,
            maximum_payload_bytes: 1024 * 1024,
        },
        policy_io.into_custody()?,
        capabilities.1,
    );
    let owner_exists = directory
        .join("pi-owner.v1")
        .try_exists()
        .map_err(|error| acyclic_harness::Error::Storage(display(error)))?;
    let owner = if owner_exists {
        let original = PiOwnerConfig::load(&resources.data, relative_directory, work)?;
        if original.policy() != &policy.0 {
            return Err(acyclic_harness::Error::Unauthorized(
                "Pi original root policy differs".to_owned(),
            ));
        }
        drop(policy);
        drop(native_runtime);
        original
    } else {
        let stream_exists = directory
            .join("stream")
            .try_exists()
            .map_err(|error| acyclic_harness::Error::Storage(display(error)))?;
        if stream_exists {
            return Err(acyclic_harness::Error::Invalid(
                "Pi original task storage exists but its owner credential is missing".to_owned(),
            ));
        }
        let owner = PiOwnerConfig::new(&policy.0, native_runtime.as_ref(), work)?;
        owner.save_new(&resources.data, relative_directory, work)?;
        owner
    };
    let filesystem = acyclic_harness::filesystem::FilesystemHost::new(
        resources.fs.clone(),
        provider,
        Some(&acyclic_harness::filesystem::PreparationIo::from_native_work(work)?),
    )?;
    let journal_name = {
        let mut io = acyclic_harness::store::HistoryWork::new(
            acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
        );
        let value = journal.storage_name(&mut Some(&mut io))?;
        (value, io.into_custody()?)
    };
    let journal_generation = resources
        .fs
        .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
            &journal_name.0,
            acyclic_fs::VolumeInitialization {
                config: VolumeConfig::portable(Lifecycle::Durable),
                access_policy: Some(acyclic_harness::filesystem::INITIAL_ACCESS_POLICY),
            },
            OperationId::from_bytes(identity("journal", session, work)?),
            work,
            work.cancellation(),
            None,
        )
        .await
        .map_err(|source| acyclic_harness::Error::NativeWorkspace {
            source: Box::new(source),
            custody: None,
        })?;
    drop(journal_generation);
    drop(journal_name);
    let stream =
        match LocalStream::open(directory.join("stream"), LocalStreamLimits::default()).await {
            Ok(stream) => Arc::new(stream),
            Err(error) => {
                let (message, custody) = work.display(&error)?;
                return Err(acyclic_harness::Error::NativeDiagnostic {
                    code: acyclic_harness::wire::ErrorCode::Storage,
                    message,
                    custody,
                });
            }
        };
    let runtime = pi_runtime::PiTaskRuntime::open_with_native_work(
        stream,
        filesystem.clone(),
        journal,
        owner.authority(work)?,
        work,
    )
    .await?;
    let request = PiRootRequest { binding, root };
    let command = pi_root_inputs::stage_root_command(&owner, &filesystem, &request, work).await?;
    let admission = runtime
        .admit_root(&request.binding, &request.root, &command.0, work)
        .await?;
    drop(admission.0);
    drop(admission.1);
    drop(command.1);
    let state_session = work.display(session)?;
    let state_agent = work.display(session)?;
    let state_key = work.display(&root_label.0)?;
    work.charge(acyclic_fs::WorkCounters {
        allocation_operations: 1,
        ..acyclic_fs::WorkCounters::UNCHARGED
    })?;
    let state = AdapterState {
        version: ADAPTER_STATE_VERSION,
        active: true,
        root_session_id: state_session.0,
        root_agent_id: state_agent.0,
        root_context_id: context_id,
        root_id,
        pending_root_registration: true,
        task_binding: PluginTaskBinding::Pi(Box::new(request.binding)),
        roots: BTreeMap::from([(
            state_key.0,
            RootBinding {
                root_id,
                path,
                allocation: RootAllocation::Preparing,
            },
        )]),
        ..AdapterState::default()
    };
    // A catalog entry is published only after authenticated durable admission.
    previous
        .1
        .save(&directory, &state, Survives::PowerLoss)
        .map_err(acyclic_harness::Error::Storage)?;
    drop(state);
    drop(request.root);
    drop(runtime);
    drop(filesystem);
    drop(owner);
    drop(provider_metadata);
    let controller = SessionController::open(directory, resources, work).await?;
    controller.retain_original_input(startup_custody)
}
