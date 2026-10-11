use super::*;
use crate::pi_owner::{PiOwnerConfig, PiOwnerPolicy};
use crate::pi_recovery::{
    PiTaskObservation, PiTaskOrigin, observe_pi_session, observe_pi_session_with_native_work,
    authenticate_pi_task_with_native_work, recover_pi_receipt_owner, recover_pi_task,
};
use crate::pi_root::PiRootIntent;
use crate::pi_runtime::{PiRuntimeAuthority, PiTaskRuntime};
use crate::{AdapterState, PiScratchBinding, PiTaskBinding, PluginTaskBinding};
use acyclic_harness::{
    Admission, AgentId, Capabilities, TaskId,
    conversation::{Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    distributed::{WorkPull, Worker},
    filesystem::{FilesystemHost, FilesystemTaskRuntime},
    resources::ProviderRef,
    runtime::{DurableTaskHost, RuntimeScope, TaskRunLimits},
    scheduler::{LeaseFence, ResourceSnapshot, SessionLimits},
    workflow::MachineRegistry,
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use std::{collections::BTreeMap, path::Path};

#[path = "pi_task_model_tests.rs"]
mod bounded_model;

type Runtime = FilesystemTaskRuntime<
    LocalStream,
    acyclic_fs::LocalAuthorityBackend,
    acyclic_fs::LocalObjectBackend,
>;
type OwnedRuntime =
    PiTaskRuntime<LocalStream, acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;

/// Descriptor-only fixture for admission/recovery and deterministic workflow
/// checks. These tests do not dispatch a provider or claim allocation evidence.
fn preparation() -> WorkflowCommand {
    WorkflowCommand {
        operation_id: OperationId::from_bytes([86; 16]),
        kind: EFFECT_TASK_COMMAND_KIND.to_owned(),
        payload: FileRef::new(
            binding().scratch.volume,
            "preparation.json",
            "fixture",
            acyclic_harness::conversation::FileDescriptor::from_bytes(b"{}", "application/json")
                .unwrap(),
            "preparation.json",
        )
        .unwrap(),
    }
}

fn binding() -> PiTaskBinding {
    let provider = ProviderRef::new("pi-recovery-test", "filesystem", "1").unwrap();
    let agent_id = AgentId::from_bytes([31; 16]);
    PiTaskBinding {
        native_session_id: "pi-root".to_owned(),
        task_id: TaskId::from_bytes([32; 16]),
        agent_id,
        project_volumes: BTreeMap::new(),
        results: None,
        scratch: PiScratchBinding {
            volume: VolumeRef::new(
                provider,
                "scratch",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent_id),
            )
            .unwrap(),
            allocation: PiScratchAllocation::Preparing,
        },
    }
}

fn session_state(binding: &PiTaskBinding) -> AdapterState {
    let mut root = binding.clone();
    root.scratch.allocation = PiScratchAllocation::Created(crate::PiScratchCreation {
        workspace_id: [37; 16],
        generation: acyclic_harness::resources::GenerationRef::new(
            root.scratch.volume.provider().clone(),
            [38; 32],
            None,
        )
        .unwrap(),
    });
    AdapterState {
        version: crate::ADAPTER_STATE_VERSION,
        active: true,
        root_session_id: binding.native_session_id.clone(),
        root_agent_id: binding.native_session_id.clone(),
        task_binding: PluginTaskBinding::Pi(Box::new(root)),
        ..AdapterState::default()
    }
}

#[tokio::test]
async fn pi_service_recovery_rejects_legacy_workspace_access() {
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path().join("service");
    let binding = binding();
    let session = data.join("sessions").join(
        blake3::hash(binding.native_session_id.as_bytes())
            .to_hex()
            .as_str(),
    );
    std::fs::create_dir_all(&session).unwrap();
    let mut state = session_state(&binding);
    let root_id = [43; 16];
    let root_key = crate::root_key(crate::WorkspaceRootId::from_bytes(root_id));
    let inaccessible_root = directory.path().join("unopened-native-root");
    state.root_id = root_id;
    state.roots.insert(
        root_key.clone(),
        crate::RootBinding {
            root_id,
            path: inaccessible_root.clone(),
            allocation: crate::RootAllocation::Created(crate::RootCreation {
                repository_workspace_id: [44; 16],
                source_identity: [45; 16],
                source_epoch: 1,
                native_root_identity: [46; 16],
            }),
        },
    );
    let PluginTaskBinding::Pi(root_binding) = &mut state.task_binding else {
        panic!("test session must retain its Pi binding");
    };
    root_binding.project_volumes.insert(
        root_key,
        VolumeRef::new(
            binding.scratch.volume.provider().clone(),
            "project",
            VolumeClass::Project,
            VolumeOwner::Project("project".to_owned()),
        )
        .unwrap(),
    );
    crate::validate_state_bounds(&state).unwrap();
    crate::save_state(&session, &state, crate::Survives::PowerLoss).unwrap();
    let expected = serde_json::to_value(&state).unwrap();
    let Err(error) = crate::ServiceResources::open(data).await else {
        panic!("task-bound Pi session entered host-managed service recovery");
    };
    assert!(error.contains("owning task runtime"), "{error}");
    let Err(error) = crate::ControlPlane::open(session.clone()).await else {
        panic!("task-bound Pi session entered host-managed control recovery");
    };
    assert!(error.contains("owning task runtime"), "{error}");
    assert!(!inaccessible_root.exists());
    assert_eq!(
        serde_json::to_value(crate::load_saved_state(&session).unwrap()).unwrap(),
        expected
    );
}

async fn open(
    root: &Path,
    binding: &PiTaskBinding,
    create: bool,
) -> (Runtime, Arc<TaskDefinition<PiTaskInput, FileRef>>) {
    let (owner, _work) = open_owner(root, binding, create).await;
    (owner.runtime, owner.definition)
}

async fn open_owner(
    root: &Path, binding: &PiTaskBinding, create: bool,
) -> (OwnedRuntime, acyclic_fs::NativeMaterializationWork) {
    open_owner_with(root, binding, create, |_| {})
        .await
        .unwrap()
}

async fn open_owner_with(
    root: &Path,
    binding: &PiTaskBinding,
    create: bool,
    configure: impl FnOnce(&mut PiRuntimeAuthority),
) -> acyclic_harness::Result<(OwnedRuntime, acyclic_fs::NativeMaterializationWork)> {
    let work = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new(),
    ).map_err(|(cause, work)| acyclic_harness::Error::NativeConstruction { cause, work })?;
    let journal = VolumeRef::new(
        binding.scratch.volume.provider().clone(),
        "journal",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(binding.agent_id),
    )
    .unwrap();
    let grants = Capabilities::new(
        [
            "operation:declare".to_owned(),
            "operation:observe".to_owned(),
            "operation:cancel".to_owned(),
            "operation:wake".to_owned(),
            "task:spawn:acyclic.pi@1".to_owned(),
            binding
                .scratch
                .volume
                .capability(VolumeOperation::Read)
                .unwrap(),
            binding
                .scratch
                .volume
                .capability(VolumeOperation::Write)
                .unwrap(),
        ]
        .into_iter()
        .chain(
            binding
                .project_volumes
                .values()
                .map(|volume| volume.capability(VolumeOperation::Read).unwrap()),
        ),
    );
    let config = if create {
        let policy = PiOwnerPolicy {
            root_task: binding.task_id,
            agent: binding.agent_id,
            journal: journal.clone(),
            grants,
            limits: Limits {
                file_bytes: 65_536,
                path_bytes: 1024,
                attachments: 16,
                render_bytes: 65_536,
                model_steps: 16,
                model_events_per_step: 128,
                tool_calls_per_step: 16,
                context_messages: 128,
            },
            run_limits: TaskRunLimits {
                concurrency: Some(4),
                max_steps: Some(16),
                deadline_epoch_ms: None,
            },
            session_limits: SessionLimits {
                active_tasks: 4,
                total_tasks: 8,
                depth: 3,
                model_steps: 16,
            },
            concurrency: 4,
            maximum_payload_bytes: 65_536,
        };
        let config = PiOwnerConfig::new_with_native_runtime_with_native_work(&policy, None, &work)?;
        config.save_new_with_native_work(root, &work)?;
        config
    } else {
        PiOwnerConfig::load_with_native_work(root, &work)?
    };
    config.validate_root(binding)?;
    let mut authority = config.authority_with_native_work(&work)?;
    configure(&mut authority.0);
    let filesystem = Arc::new(
        FilesystemHost::new(
            acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(root.join("filesystem")))
                .await
                .unwrap(),
            config.policy().journal.provider().clone(),
        )
        .unwrap(),
    );
    if create {
        filesystem
            .create_volume(&config.policy().journal)
            .await
            .unwrap();
    }
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(root.join("stream"), LocalStreamLimits::default())
            .await
            .unwrap(),
    ));
    let runtime = PiTaskRuntime::open_with_native_work(
        stream, filesystem, &journal, authority, &work,
    ).await?;
    Ok((runtime, work))
}

#[tokio::test]
async fn pi_root_missing_or_foreign_credentials_fail_before_storage_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let binding = binding();
    let (owner, original_work) = open_owner(directory.path(), &binding, true).await;
    owner
        .admit_root_with_native_work(binding.clone(), PiRootIntent::ScratchOnly {}, preparation(), &original_work)
        .await
        .unwrap();
    let original = owner
        .runtime
        .task_host()
        .observe_admission(binding.task_id)
        .await
        .unwrap();
    drop(owner);
    let config = PiOwnerConfig::load_with_native_work(directory.path(), &original_work).unwrap();
    let filesystem = directory.path().join("filesystem");
    let retained = directory.path().join("retained-filesystem");
    std::fs::rename(&filesystem, &retained).unwrap();
    let mut foreign = binding.clone();
    foreign.task_id = TaskId::from_bytes([74; 16]);
    assert!(
        open_owner_with(directory.path(), &foreign, false, |_| {})
            .await
            .is_err()
    );
    assert!(!filesystem.exists());
    std::fs::remove_file(directory.path().join("pi-owner.v1")).unwrap();
    assert!(
        open_owner_with(directory.path(), &binding, false, |_| {})
            .await
            .is_err()
    );
    assert!(!filesystem.exists());
    assert!(!directory.path().join("pi-owner.v1").exists());
    // Restoring the original owner bytes and storage recovers the original
    // admission; there is no substitute key or fabricated fresh root.
    config.save_new_with_native_work(directory.path(), &original_work).unwrap();
    std::fs::rename(&retained, &filesystem).unwrap();
    let (owner, _original_work) = open_owner(directory.path(), &binding, false).await;
    assert!(
        owner
            .runtime
            .task_host()
            .observe_admission(binding.task_id)
            .await
            .unwrap()
            == original
    );
}

#[tokio::test]
async fn pi_root_physical_selection_reopens_only_from_original_admission() {
    let directory = tempfile::tempdir().unwrap();
    let selected = directory.path().join("selected source with spaces-λ");
    let root_id = [81; 16];
    let key = crate::root_key(crate::WorkspaceRootId::from_bytes(root_id));
    let mut binding = binding();
    binding.project_volumes.insert(
        key.clone(),
        VolumeRef::new(
            binding.scratch.volume.provider().clone(),
            "selected-project",
            VolumeClass::Project,
            VolumeOwner::Project("selected-project".to_owned()),
        )
        .unwrap(),
    );
    let mut state = session_state(&binding);
    state.root_id = root_id;
    state.root_context_id = [82; 16];
    state.pending_root_registration = true;
    state.task_binding = PluginTaskBinding::Pi(Box::new(binding.clone()));
    state.roots.insert(
        key.clone(),
        crate::RootBinding {
            root_id,
            path: selected.clone(),
            allocation: crate::RootAllocation::Preparing,
        },
    );
    let intent = PiRootIntent::from_state(&state).unwrap();
    let (owner, original_work) = open_owner(directory.path(), &binding, true).await;
    // A retained route, even with valid host credentials, cannot admit itself.
    assert!(observe_pi_session(&owner.runtime, &state).await.is_err());
    owner
        .admit_root_with_native_work(binding.clone(), intent.clone(), preparation(), &original_work)
        .await
        .unwrap();
    let original = owner
        .runtime
        .task_host()
        .observe_admission(binding.task_id)
        .await
        .unwrap();
    crate::save_state(directory.path(), &state, crate::Survives::PowerLoss).unwrap();
    assert!(
        matches!(observe_pi_session(&owner.runtime, &state).await.unwrap().as_slice(),
        [PiTaskObservation::Pending { task_id }] if *task_id == binding.task_id)
    );
    for changed in [
        {
            let mut changed = state.clone();
            changed.roots.get_mut(&key).unwrap().path = directory.path().join("foreign source");
            changed
        },
        {
            let mut changed = state.clone();
            changed.root_context_id = [83; 16];
            changed
        },
    ] {
        crate::validate_state_bounds(&changed).unwrap();
        assert!(observe_pi_session(&owner.runtime, &changed).await.is_err());
        let changed_intent = PiRootIntent::from_state(&changed).unwrap();
        assert!(
            owner
                .admit_root_with_native_work(binding.clone(), changed_intent, preparation(), &original_work)
                .await
                .is_err()
        );
    }
    assert!(!selected.exists());
    assert!(!directory.path().join("foreign source").exists());
    assert!(matches!(
        owner
            .runtime
            .task_host()
            .recover_work(binding.task_id)
            .await
            .unwrap(),
        WorkPull::Idle
    ));
    drop(owner);
    let state = crate::load_saved_state(directory.path()).unwrap();
    assert_eq!(PiRootIntent::from_state(&state).unwrap(), intent);
    let filesystem = Arc::new(
        FilesystemHost::new(
            acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(
                directory.path().join("filesystem"),
            ))
            .await
            .unwrap(),
            binding.scratch.volume.provider().clone(),
        )
        .unwrap(),
    );
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(
            directory.path().join("stream"),
            LocalStreamLimits::default(),
        )
        .await
        .unwrap(),
    ));
    let reopen_work = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new(),
    ).unwrap();
    let reopened = PiTaskRuntime::reopen(
        &PiOwnerConfig::load_with_native_work(directory.path(), &reopen_work).unwrap(),
        &state,
        stream,
        filesystem,
        &reopen_work,
    )
    .await
    .unwrap();
    let owner = &reopened.0;
    let observed = &reopened.1;
    assert!(
        matches!(observed.as_slice(), [PiTaskObservation::Pending { task_id }] if *task_id == binding.task_id)
    );
    assert!(
        owner
            .runtime
            .task_host()
            .observe_admission(binding.task_id)
            .await
            .unwrap()
            == original
    );
    assert!(!selected.exists());
}

#[test]
fn pi_root_intent_and_execution_mode_are_required_and_bounded() {
    let binding = binding();
    let machine = Arc::new(PiTaskMachine::new());
    for input in [
        json!({"binding":binding,"launch":null}),
        json!({"binding":binding,"execution":{"kind":"host"}}),
        json!({"binding":binding,"execution":{"kind":"child","launch":null}}),
        json!({"binding":binding,"execution":{"kind":"host","root":{"kind":"scratch_only"}}}),
        json!({"binding":binding,"execution":{"kind":"host","root":{"kind":"scratch_only","paths":{}},"preparation":preparation()}}),
    ] {
        assert!(
            machine.initialize(&input).is_err(),
            "accepted invalid input: {input}"
        );
    }
    let input = PiTaskInput {
        binding: binding.clone(),
        execution: PiTaskExecution::Host {
            root: PiRootIntent::ScratchOnly {},
            preparation: preparation(),
        },
    };
    let encoded = serde_json::to_value(input).unwrap();
    assert_eq!(
        machine.initialize(&encoded).unwrap(),
        json!({"state":"preparing","command":preparation()})
    );
    let mut binding = binding;
    let root_id = [2; 16];
    let key = hex::encode(root_id);
    binding.project_volumes.insert(
        key.clone(),
        VolumeRef::new(
            binding.scratch.volume.provider().clone(),
            "project",
            VolumeClass::Project,
            VolumeOwner::Project("project".to_owned()),
        )
        .unwrap(),
    );
    assert!(PiRootIntent::ScratchOnly {}.validate(&binding).is_err());
    let directory = tempfile::tempdir().unwrap();
    let path = directory
        .path()
        .join("selected")
        .to_str()
        .unwrap()
        .to_owned();
    let valid = PiRootIntent::Project {
        context_id: [1; 16],
        root_id,
        paths: BTreeMap::from([(key.clone(), path.clone())]),
    };
    valid.validate(&binding).unwrap();
    for invalid_path in [
        "relative/path".to_owned(),
        format!("{path}\0"),
        directory
            .path()
            .join("..")
            .join("escape")
            .to_str()
            .unwrap()
            .to_owned(),
        format!("{path}{}", "x".repeat(128 * 1024)),
    ] {
        let invalid = PiRootIntent::Project {
            context_id: [1; 16],
            root_id,
            paths: BTreeMap::from([(key.clone(), invalid_path)]),
        };
        assert!(invalid.validate(&binding).is_err(), "accepted invalid path");
    }
    for (context_id, selected_root) in [([0; 16], root_id), ([1; 16], [0; 16]), ([1; 16], [3; 16])]
    {
        let invalid = PiRootIntent::Project {
            context_id,
            root_id: selected_root,
            paths: BTreeMap::from([(key.clone(), path.clone())]),
        };
        assert!(
            invalid.validate(&binding).is_err(),
            "accepted invalid identity"
        );
    }
    assert!(!directory.path().join("selected").exists());
}

#[tokio::test]
async fn pi_runtime_reopen_authenticates_retained_native_binding_without_starting_work() {
    let directory = tempfile::tempdir().unwrap();
    let binding = binding();
    let (owner, original_work) = open_owner(directory.path(), &binding, true).await;
    owner
        .admit_root_with_native_work(binding.clone(), PiRootIntent::ScratchOnly {}, preparation(), &original_work)
        .await
        .unwrap();
    let original = owner
        .runtime
        .task_host()
        .observe_admission(binding.task_id)
        .await
        .unwrap();
    drop(owner);
    let filesystem = Arc::new(
        FilesystemHost::new(
            acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(
                directory.path().join("filesystem"),
            ))
            .await
            .unwrap(),
            binding.scratch.volume.provider().clone(),
        )
        .unwrap(),
    );
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(
            directory.path().join("stream"),
            LocalStreamLimits::default(),
        )
        .await
        .unwrap(),
    ));
    let mut state = session_state(&binding);
    let reopen_work = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new(),
    ).unwrap();
    let reopened = PiTaskRuntime::reopen(
        &PiOwnerConfig::load_with_native_work(directory.path(), &reopen_work).unwrap(),
        &state,
        stream.clone(),
        filesystem.clone(),
        &reopen_work,
    )
    .await
    .unwrap();
    let owner = &reopened.0;
    let observed = &reopened.1;
    assert!(
        matches!(observed.as_slice(), [PiTaskObservation::Pending { task_id }] if *task_id == binding.task_id)
    );
    assert!(
        owner
            .runtime
            .task_host()
            .observe_admission(binding.task_id)
            .await
            .unwrap()
            == original
    );
    state.root_session_id = "forged-native-session".to_owned();
    let PluginTaskBinding::Pi(root) = &mut state.task_binding else {
        unreachable!()
    };
    root.native_session_id = state.root_session_id.clone();
    crate::validate_state_bounds(&state).unwrap();
    let foreign_reopen_work = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new(),
    ).unwrap();
    assert!(
        PiTaskRuntime::reopen(
            &PiOwnerConfig::load_with_native_work(directory.path(), &foreign_reopen_work).unwrap(),
            &state,
            stream,
            filesystem,
            &foreign_reopen_work,
        )
        .await
        .is_err()
    );
    assert!(
        owner
            .runtime
            .task_host()
            .observe_admission(binding.task_id)
            .await
            .unwrap()
            == original
    );
    assert!(matches!(
        owner
            .runtime
            .task_host()
            .recover_work(binding.task_id)
            .await
            .unwrap(),
        WorkPull::Idle
    ));
}

#[tokio::test]
async fn pi_root_registration_replays_original_admission_and_rejects_foreign_owner() {
    let directory = tempfile::tempdir().unwrap();
    let binding = binding();
    let (owner, original_work) = open_owner(directory.path(), &binding, true).await;
    assert!(
        owner
            .runtime
            .task_host()
            .recover_work(binding.task_id)
            .await
            .is_err()
    );
    let mut foreign = binding.clone();
    foreign.agent_id = AgentId::from_bytes([71; 16]);
    foreign.scratch.volume = VolumeRef::new(
        foreign.scratch.volume.provider().clone(),
        "scratch",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(foreign.agent_id),
    )
    .unwrap();
    assert!(
        owner
            .admit_root_with_native_work(foreign, PiRootIntent::ScratchOnly {}, preparation(), &original_work)
            .await
            .is_err()
    );
    let mut another_root = binding.clone();
    another_root.task_id = TaskId::from_bytes([73; 16]);
    let another_task = another_root.task_id;
    assert!(
        owner
            .admit_root_with_native_work(another_root, PiRootIntent::ScratchOnly {}, preparation(), &original_work)
            .await
            .is_err()
    );
    assert!(
        owner
            .runtime
            .task_host()
            .observe_admission(another_task)
            .await
            .is_err()
    );
    let journal = VolumeRef::new(
        binding.scratch.volume.provider().clone(),
        "journal",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(binding.agent_id),
    )
    .unwrap();
    let mut journal_selection = binding.clone();
    journal_selection.scratch.volume = journal.clone();
    assert!(
        owner
            .admit_root_with_native_work(
                journal_selection,
                PiRootIntent::ScratchOnly {},
                preparation(),
                &original_work,
            )
            .await
            .is_err()
    );
    assert!(
        owner
            .runtime
            .task_host()
            .observe_admission(binding.task_id)
            .await
            .is_err()
    );
    let accepted = owner.admit_root_with_native_work(
        binding.clone(), PiRootIntent::ScratchOnly {}, preparation(), &original_work,
    ).await.unwrap();
    assert!(matches!(accepted.0, Admission::Accepted(_)));
    drop(accepted.0);
    drop(accepted.1);
    let admission = owner
        .runtime
        .task_host()
        .observe_admission(binding.task_id)
        .await
        .unwrap();
    assert_eq!(admission.parent, None);
    assert_eq!(admission.task, *owner.definition.identity());
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        assert!(
            !admission
                .grants
                .contains(&journal.capability(operation).unwrap())
        );
    }
    assert_eq!(
        admission.input,
        serde_json::to_value(PiTaskInput {
            binding: binding.clone(),
            execution: PiTaskExecution::Host {
                root: PiRootIntent::ScratchOnly {},
                preparation: preparation(),
            }
        })
        .unwrap()
    );
    // Admission does not create the selected scratch volume or reserve work.
    let storage = acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(
        directory.path().join("filesystem"),
    ))
    .await
    .unwrap();
    assert!(
        storage
            .open_workspace(&binding.scratch.volume.storage_name().unwrap())
            .await
            .is_err()
    );
    assert!(matches!(
        owner
            .runtime
            .task_host()
            .recover_work(binding.task_id)
            .await
            .unwrap(),
        WorkPull::Idle
    ));
    drop(owner);
    let (owner, original_work) = open_owner(directory.path(), &binding, false).await;
    let accepted = owner.admit_root_with_native_work(
        binding.clone(), PiRootIntent::ScratchOnly {}, preparation(), &original_work,
    ).await.unwrap();
    assert!(matches!(accepted.0, Admission::Accepted(_)));
    drop(accepted.0);
    drop(accepted.1);
    assert!(
        owner
            .runtime
            .task_host()
            .observe_admission(binding.task_id)
            .await
            .unwrap()
            == admission
    );
    let task_id = binding.task_id;
    let mut changed = binding;
    changed.native_session_id = "different-native-session".to_owned();
    assert!(
        owner
            .admit_root_with_native_work(changed, PiRootIntent::ScratchOnly {}, preparation(), &original_work)
            .await
            .is_err()
    );
    assert!(
        owner
            .runtime
            .task_host()
            .observe_admission(task_id)
            .await
            .unwrap()
            == admission
    );
}

#[tokio::test]
async fn pi_root_runtime_rejects_forged_and_widened_authority_before_admission() {
    let directory = tempfile::tempdir().unwrap();
    let binding = binding();
    let (owner, original_work) = open_owner(directory.path(), &binding, true).await;
    owner
        .admit_root_with_native_work(binding.clone(), PiRootIntent::ScratchOnly {}, preparation(), &original_work)
        .await
        .unwrap();
    let original = owner
        .runtime
        .task_host()
        .observe_admission(binding.task_id)
        .await
        .unwrap();
    drop(owner);
    for fault in [
        "key",
        "widening",
        "journal",
        "payload",
        "concurrency",
        "agent",
        "audience",
    ] {
        let rejected =
            open_owner_with(directory.path(), &binding, false, |authority| match fault {
                "key" => {
                    authority.verifier = AuthorityIssuer::new(
                        "pi-recovery-test",
                        [72; 32],
                        Authority {
                            kind: AggregateKind::Task,
                            id: binding.task_id.to_string(),
                        },
                    )
                    .verifier();
                }
                "widening" => {
                    authority.scope = RuntimeScope::new(
                        Capabilities::new(["foreign:grant"]),
                        authority.scope.limits(),
                    )
                    .unwrap();
                }
                "journal" => {
                    authority.scope = RuntimeScope::new(
                        authority.signed.capabilities().clone(),
                        authority.scope.limits(),
                    )
                    .unwrap();
                }
                "payload" => {
                    authority.maximum_payload_bytes = authority.scope.limits().file_bytes + 1;
                }
                "concurrency" => authority.concurrency = 0,
                "agent" => {
                    let issuer = AuthorityIssuer::new(
                        "pi-recovery-test",
                        [33; 32],
                        Authority {
                            kind: AggregateKind::Task,
                            id: binding.task_id.to_string(),
                        },
                    );
                    authority.verifier = issuer.verifier();
                    authority.signed =
                        issuer.root("pi-owner", authority.signed.capabilities().clone());
                }
                "audience" => {
                    let issuer = AuthorityIssuer::new(
                        "pi-recovery-test",
                        [33; 32],
                        Authority {
                            kind: AggregateKind::Task,
                            id: "not-a-task-identity".to_owned(),
                        },
                    );
                    authority.verifier = issuer.verifier();
                    authority.signed = issuer.root_for_agent(
                        binding.agent_id,
                        "pi-owner",
                        authority.signed.capabilities().clone(),
                    );
                }
                _ => unreachable!(),
            })
            .await;
        assert!(rejected.is_err(), "accepted {fault}");
    }
    let (owner, _original_work) = open_owner(directory.path(), &binding, false).await;
    assert!(
        owner
            .runtime
            .task_host()
            .observe_admission(binding.task_id)
            .await
            .unwrap()
            == original
    );
    assert!(matches!(
        owner
            .runtime
            .task_host()
            .recover_work(binding.task_id)
            .await
            .unwrap(),
        WorkPull::Idle
    ));
}

#[tokio::test]
async fn pi_task_recovers_the_original_reservation_after_lost_pull_and_host_rebuild() {
    let directory = tempfile::tempdir().unwrap();
    let binding = binding();
    let (runtime, definition) = open(directory.path(), &binding, true).await;
    let operation = OperationId::from_bytes(binding.task_id.into_bytes());
    let accepted = runtime
        .harness()
        .admit(
            operation,
            &definition,
            PiTaskInput {
                binding: binding.clone(),
                execution: PiTaskExecution::Host {
                    root: PiRootIntent::ScratchOnly {},
                    preparation: preparation(),
                },
            },
            None,
        )
        .await
        .unwrap();
    assert!(matches!(accepted, Admission::Accepted(_)));
    // Read the original lease only to assert its identity. Production recovery
    // does not depend on receiving or serializing this response.
    let WorkPull::Claimed(original) = runtime
        .task_host()
        .pull_operation(
            &Worker {
                id: "pi-worker".to_owned(),
                available: ResourceSnapshot::default(),
                labels: BTreeMap::new(),
            },
            operation,
        )
        .await
        .unwrap()
    else {
        panic!("root was not admitted");
    };
    drop(accepted);
    drop(runtime);
    let (runtime, _) = open(directory.path(), &binding, false).await;
    let mut state = session_state(&binding);
    let PluginTaskBinding::Pi(root_binding) = &state.task_binding else {
        panic!("test session must retain its Pi binding");
    };
    let mut forged_child = root_binding.as_ref().clone();
    forged_child.native_session_id = "pi-child".to_owned();
    forged_child.task_id = TaskId::from_bytes([39; 16]);
    forged_child.agent_id = AgentId::from_bytes([40; 16]);
    forged_child.scratch.volume = VolumeRef::new(
        binding.scratch.volume.provider().clone(),
        "scratch",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(forged_child.agent_id),
    )
    .unwrap();
    state.routes.insert(
        "pi-child".to_owned(),
        crate::Route {
            task_binding: PluginTaskBinding::Pi(Box::new(forged_child)),
            agent_id: "pi-child".to_owned(),
            turn_id: "turn".to_owned(),
            context_id: [41; 16],
            root_id: [42; 16],
            parent_agent_id: binding.native_session_id.clone(),
            roots: BTreeMap::new(),
            mount_path: directory.path().join("native-must-stay-absent"),
            lifecycle: crate::RouteLifecycle::Active,
        },
    );
    crate::validate_state_bounds(&state).unwrap();
    assert!(observe_pi_session(&runtime, &state).await.is_err());
    assert!(!directory.path().join("native-must-stay-absent").exists());
    match runtime
        .task_host()
        .recover_work(binding.task_id)
        .await
        .unwrap()
    {
        WorkPull::Claimed(lease) => {
            assert_eq!(lease.reservation, original.reservation);
            assert_eq!(lease.operation_revision, original.operation_revision);
        }
        _ => panic!("session authentication changed the root reservation"),
    }
    state.routes.clear();
    let mut observations = observe_pi_session(&runtime, &state).await.unwrap();
    assert_eq!(observations.len(), 1);
    match observations.remove(0) {
        PiTaskObservation::Reserved { lease } => {
            assert_eq!(lease.reservation, original.reservation);
            assert_eq!(lease.operation_revision, original.operation_revision);
        }
        _ => panic!("observation did not retain original reservation"),
    }
    let query = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new()).unwrap();
    let guarded = observe_pi_session_with_native_work(&runtime, &state, &query).await.unwrap();
    assert!(matches!(&guarded.0[0], PiTaskObservation::Reserved { lease }
        if lease.reservation == original.reservation && lease.operation_revision == original.operation_revision));
    assert!(guarded.1.bytes() > 0);
    assert!(query.live_allocation_bytes() >= guarded.1.bytes());
    drop(guarded);
    assert_eq!(query.live_allocation_bytes(), 0);
    let active = recover_pi_task(
        &runtime,
        &binding,
        PiTaskOrigin::Root(&PiRootIntent::ScratchOnly {}),
    )
    .await
    .unwrap();
    assert_eq!(active.lease.operation, original.operation);
    assert_eq!(active.lease.reservation, original.reservation);
    assert_eq!(
        active.owner.workflow_admission().initial.machine,
        *PiTaskMachine::new().identity()
    );
    assert_eq!(active.owner.workflow_admission().operation_id, operation);
    runtime
        .task_host()
        .verify_dispatch_owner(binding.task_id, LeaseFence::from(&active.lease.reservation))
        .await
        .unwrap();

    for forged in [
        PiTaskBinding {
            native_session_id: "another-session".to_owned(),
            ..binding.clone()
        },
        PiTaskBinding {
            agent_id: AgentId::from_bytes([34; 16]),
            ..binding.clone()
        },
        PiTaskBinding {
            scratch: PiScratchBinding {
                allocation: PiScratchAllocation::Created(crate::PiScratchCreation {
                    workspace_id: [0; 16],
                    generation: acyclic_harness::resources::GenerationRef::new(
                        binding.scratch.volume.provider().clone(),
                        [36; 32],
                        None,
                    )
                    .unwrap(),
                }),
                ..binding.scratch.clone()
            },
            ..binding.clone()
        },
    ] {
        assert!(
            recover_pi_task(
                &runtime,
                &forged,
                PiTaskOrigin::Root(&PiRootIntent::ScratchOnly {})
            )
            .await
            .is_err()
        );
    }
    assert!(
        recover_pi_task(
            &runtime,
            &binding,
            PiTaskOrigin::Child(TaskId::from_bytes([35; 16]))
        )
        .await
        .is_err()
    );
    runtime.task_host().cancel(binding.task_id).await.unwrap();
    assert!(
        recover_pi_task(
            &runtime,
            &binding,
            PiTaskOrigin::Root(&PiRootIntent::ScratchOnly {})
        )
        .await
        .is_err()
    );
    assert!(
        runtime
            .task_host()
            .verify_dispatch_owner(binding.task_id, LeaseFence::from(&active.lease.reservation))
            .await
            .is_err()
    );
    let unresolved = runtime
        .task_host()
        .recover_work(binding.task_id)
        .await
        .unwrap();
    match unresolved {
        WorkPull::Unresolved { lease, .. } => assert_eq!(lease.reservation, original.reservation),
        _ => panic!("cancelled original reservation was lost"),
    }
    drop(active);
    drop(runtime);
    let (runtime, _) = open(directory.path(), &binding, false).await;
    let owner = recover_pi_receipt_owner(
        &runtime,
        &binding,
        PiTaskOrigin::Root(&PiRootIntent::ScratchOnly {}),
    )
    .await
    .unwrap();
    assert_eq!(owner.workflow_admission().operation_id, operation);
    assert!(
        runtime
            .task_host()
            .verify_dispatch_owner(binding.task_id, LeaseFence::from(&original.reservation))
            .await
            .is_err()
    );
    let mut observations = observe_pi_session_with_native_work(&runtime, &state, &query).await.unwrap();
    match observations.0.remove(0) {
        PiTaskObservation::Unresolved { lease, error } => {
            assert_eq!(lease.reservation, original.reservation);
            assert!(matches!(error, acyclic_harness::Error::Conflict(_)));
        }
        _ => panic!("cancelled attempt was converted into executable or completed work"),
    }
    drop(observations);
    assert_eq!(query.live_allocation_bytes(), 0);
}

#[tokio::test]
async fn pi_session_observation_does_not_reserve_pending_or_restart_completed_tasks() {
    let directory = tempfile::tempdir().unwrap();
    let binding = binding();
    let (runtime, definition) = open(directory.path(), &binding, true).await;
    let state = session_state(&binding);
    let operation = OperationId::from_bytes(binding.task_id.into_bytes());
    assert!(matches!(
        runtime
            .harness()
            .admit(
                operation,
                &definition,
                PiTaskInput {
                    binding: binding.clone(),
                    execution: PiTaskExecution::Host {
                        root: PiRootIntent::ScratchOnly {},
                        preparation: preparation(),
                    }
                },
                None,
            )
            .await
            .unwrap(),
        Admission::Accepted(_)
    ));
    for _ in 0..2 {
        let mut observed = observe_pi_session(&runtime, &state).await.unwrap();
        assert!(
            matches!(observed.remove(0), PiTaskObservation::Pending { task_id } if task_id == binding.task_id)
        );
        let query = acyclic_fs::NativeMaterializationWork::new(
            crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new()).unwrap();
        let observed = observe_pi_session_with_native_work(&runtime, &state, &query).await.unwrap();
        assert!(matches!(&observed.0[0], PiTaskObservation::Pending { task_id } if *task_id == binding.task_id));
        assert!(observed.1.bytes() > 0);
        drop(observed);
        assert_eq!(query.live_allocation_bytes(), 0);
        assert!(
            recover_pi_receipt_owner(
                &runtime,
                &binding,
                PiTaskOrigin::Root(&PiRootIntent::ScratchOnly {})
            )
            .await
            .is_err()
        );
    }
    let WorkPull::Claimed(lease) = runtime
        .task_host()
        .pull_operation(
            &Worker {
                id: "pi-worker".to_owned(),
                available: ResourceSnapshot::default(),
                labels: BTreeMap::new(),
            },
            operation,
        )
        .await
        .unwrap()
    else {
        panic!("observation claimed or changed pending work");
    };
    let active = recover_pi_task(
        &runtime,
        &binding,
        PiTaskOrigin::Root(&PiRootIntent::ScratchOnly {}),
    )
    .await
    .unwrap();
    runtime
        .task_host()
        .settle_task(
            binding.task_id,
            LeaseFence::from(&lease.reservation),
            Outcome::Failed {
                message: "known failure".to_owned(),
            },
        )
        .await
        .unwrap();
    drop(active);
    drop(runtime);
    let (runtime, _) = open(directory.path(), &binding, false).await;
    for _ in 0..2 {
        let mut observed = observe_pi_session(&runtime, &state).await.unwrap();
        assert!(matches!(observed.remove(0), PiTaskObservation::Outcome {
            task_id, outcome: Outcome::Failed { message }
        } if task_id == binding.task_id && message == "known failure"));
        let query = acyclic_fs::NativeMaterializationWork::new(
            crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new()).unwrap();
        let observed = observe_pi_session_with_native_work(&runtime, &state, &query).await.unwrap();
        assert!(matches!(&observed.0[0], PiTaskObservation::Outcome {
            task_id, outcome: Outcome::Failed { message },
        } if *task_id == binding.task_id && message == "known failure"));
        drop(observed);
        assert_eq!(query.live_allocation_bytes(), 0);
        assert!(
            recover_pi_task(
                &runtime,
                &binding,
                PiTaskOrigin::Root(&PiRootIntent::ScratchOnly {})
            )
            .await
            .is_err()
        );
        assert!(
            recover_pi_receipt_owner(
                &runtime,
                &binding,
                PiTaskOrigin::Root(&PiRootIntent::ScratchOnly {})
            )
            .await
            .is_err()
        );
    }
}

#[test]
fn pi_root_workflow_retains_preparation_until_known_allocation_and_never_reemits_it() {
    let machine = PiTaskMachine::new();
    let command = preparation();
    let initial = machine
        .initialize(
            &serde_json::to_value(PiTaskInput {
                binding: binding(),
                execution: PiTaskExecution::Host {
                    root: PiRootIntent::ScratchOnly {},
                    preparation: command.clone(),
                },
            })
            .unwrap(),
        )
        .unwrap();
    let allocating = machine.transition(&initial, &Value::Null).unwrap();
    assert_eq!(allocating.commands, vec![command.clone()]);
    assert_eq!(
        allocating.state,
        json!({"state":"allocating","operation":command.operation_id})
    );
    assert!(machine.transition(&allocating.state, &Value::Null).is_err());
    for outcome in [
        Outcome::<FileRef>::Cancelled,
        Outcome::Indeterminate {
            operation_id: command.operation_id,
        },
    ] {
        let input = json!({"commands":[{"operation_id":command.operation_id,"value":outcome}]});
        assert!(machine.transition(&allocating.state, &input).is_err());
    }
    let result = command.payload.clone();
    let mut wrong = json!({"commands":[{"operation_id":OperationId::from_bytes([87;16]),"value":Outcome::Succeeded(result.clone())}]});
    assert!(machine.transition(&allocating.state, &wrong).is_err());
    *wrong.get_mut("commands").unwrap() = json!([]);
    assert!(machine.transition(&allocating.state, &wrong).is_err());
    let known = json!({"commands":[{"operation_id":command.operation_id,"value":Outcome::Succeeded(result.clone())}]});
    let hosting = machine.transition(&allocating.state, &known).unwrap();
    assert_eq!(
        hosting.state,
        json!({"state":"hosting","allocation":result,"reply":null})
    );
    assert!(hosting.commands.is_empty());
    assert!(matches!(hosting.status, MachineStatus::Suspended));
    for _ in 0..3 {
        let observed = machine.transition(&hosting.state, &Value::Null).unwrap();
        assert_eq!(observed.state, hosting.state);
        assert!(observed.commands.is_empty());
        assert!(matches!(observed.status, MachineStatus::Suspended));
    }
    assert!(machine.transition(&hosting.state, &known).is_err());
    let failure = machine.transition(&allocating.state, &json!({"commands":[{"operation_id":command.operation_id,"value":Outcome::<FileRef>::Failed { message:"allocation failed".to_owned() }}]})).unwrap();
    assert!(matches!(failure.status, MachineStatus::Failed { .. }));
    assert!(failure.commands.is_empty());
}

#[test]
fn pi_workflow_checkpoints_pin_one_effect_command_and_reject_extra_state() {
    use acyclic_harness::{conversation::FileDescriptor, workflow::MachineCheckpoint};
    let machine = Arc::new(PiTaskMachine::new());
    let mut registry = MachineRegistry::default();
    registry.register(machine.clone()).unwrap();
    let result = FileRef::new(
        binding().scratch.volume,
        "result.json",
        "v1",
        FileDescriptor::from_bytes(b"{}", "application/json").unwrap(),
        "result.json",
    )
    .unwrap();
    let command = WorkflowCommand {
        operation_id: OperationId::from_bytes([61; 16]),
        kind: EFFECT_TASK_COMMAND_KIND.to_owned(),
        payload: result.clone(),
    };
    let checkpoint = |state| MachineCheckpoint {
        machine: machine.identity().clone(),
        revision: 0,
        state,
    };
    for state in [
        PiTaskState::Preparing {
            command: preparation(),
        },
        PiTaskState::Allocating {
            operation: preparation().operation_id,
        },
        PiTaskState::Hosting {
            allocation: result.clone(),
            reply: None,
        },
        PiTaskState::Ready {
            command: command.clone(),
        },
        PiTaskState::Awaiting {
            operation: command.operation_id,
        },
        PiTaskState::Completed {
            result: result.clone(),
        },
        PiTaskState::Failed {
            message: "known failure".to_owned(),
        },
    ] {
        registry
            .validate_checkpoint(&checkpoint(encode(&state).unwrap()))
            .unwrap();
    }
    let mut foreign_command = command.clone();
    foreign_command.kind = "acyclic.mail.v1".to_owned();
    let foreign_state = encode(&PiTaskState::Ready {
        command: foreign_command,
    })
    .unwrap();
    for state in [
        json!({"state":"unknown"}),
        json!({"state":"hosting","allocation":result,"command":command}),
        json!({"state":"hosting"}),
        json!({"state":"preparing"}),
        json!({"state":"allocating"}),
        json!({"state":"awaiting"}),
        foreign_state.clone(),
    ] {
        assert!(registry.validate_checkpoint(&checkpoint(state)).is_err());
    }
    assert!(machine.transition(&foreign_state, &Value::Null).is_err());
    assert!(
        machine
            .transition(
                &json!({"state":"hosting","allocation":result,"command":command}),
                &Value::Null
            )
            .is_err()
    );
    let ready = checkpoint(
        machine
            .initialize(
                &serde_json::to_value(PiTaskInput {
                    binding: binding(),
                    execution: PiTaskExecution::Child {
                        launch: command.clone(),
                    },
                })
                .unwrap(),
            )
            .unwrap(),
    );
    let (awaiting, dispatched) = registry.step(&ready, &Value::Null).unwrap();
    assert_eq!(dispatched.commands, vec![command.clone()]);
    assert!(registry.step(&awaiting, &Value::Null).is_err());
    let (completed, settled) = registry
        .step(
            &awaiting,
            &json!({"commands":[{
                "operation_id": command.operation_id,
                "value": Outcome::Succeeded(result.clone()),
            }]}),
        )
        .unwrap();
    assert!(settled.commands.is_empty());
    assert!(
        matches!(settled.status, MachineStatus::Completed { value } if value == serde_json::to_value(result).unwrap())
    );
    assert!(registry.step(&completed, &Value::Null).is_err());
}

#[test]
fn pi_worker_does_not_accept_forged_or_uncertain_command_completion() {
    let machine = PiTaskMachine::new();
    let operation = OperationId::from_bytes([41; 16]);
    let state = encode(&PiTaskState::Awaiting { operation }).unwrap();
    for input in [
        json!({"commands":[]}),
        json!({"commands":[{"operation_id":OperationId::from_bytes([42;16]),"value":{"Failed":{"message":"forged"}}}]}),
        json!({"commands":[{"operation_id":operation,"value":{"Indeterminate":{"operation_id":operation}}}]}),
        json!({"commands":[{"operation_id":operation,"value":"Cancelled"}]}),
    ] {
        assert!(machine.transition(&state, &input).is_err());
    }
    let known_failure = machine
        .transition(
            &state,
            &json!({"commands":[{
                "operation_id":operation,"value":{"Failed":{"message":"known process failure"}}
            }]}),
        )
        .unwrap();
    assert!(matches!(known_failure.status, MachineStatus::Failed { .. }));
    assert!(known_failure.commands.is_empty());
    assert!(
        machine
            .transition(&known_failure.state, &Value::Null)
            .is_err()
    );
}

#[test]
fn pi_root_control_bounded_checkpoint_replay_preserves_allocation_and_original_operations() {
    use acyclic_harness::workflow::MachineCheckpoint;
    let machine = Arc::new(PiTaskMachine::new());
    let mut registry = MachineRegistry::default();
    registry.register(machine.clone()).unwrap();
    let allocation = preparation().payload;
    let mut checkpoint = MachineCheckpoint {
        machine: machine.identity().clone(),
        revision: 0,
        state: encode(&PiTaskState::Hosting {
            allocation: allocation.clone(),
            reply: None,
        }).unwrap(),
    };
    // Bounded state-space exploration uses the installed production machine,
    // not a root harness/model provider or a simulated effect ledger.
    for (index, kind) in [
        "acyclic.task.observe.v1", "acyclic.mail.send.v1",
        "acyclic.mail.receive.v1", "acyclic.timer.v1",
        "acyclic.pi.fork", "acyclic.pi.import",
        "acyclic.pi.writeback", "acyclic.pi.observe-source",
        "acyclic.pi.inspect", "acyclic.pi.diff", "acyclic.pi.operator-approval",
        "acyclic.pi.builtin",
    ].into_iter().enumerate() {
        let command = WorkflowCommand {
            operation_id: OperationId::from_bytes([100 + index as u8; 16]),
            kind: kind.to_owned(),
            payload: allocation.clone(),
        };
        for _ in 0..3 {
            let (passive, transition) = registry.step(&checkpoint, &Value::Null).unwrap();
            assert_eq!(passive.state, checkpoint.state);
            assert!(transition.commands.is_empty());
            assert!(matches!(transition.status, MachineStatus::Suspended));
        }
        let input = json!({"command":command});
        let (controlling, dispatch) = registry.step(&checkpoint, &input).unwrap();
        assert_eq!(dispatch.commands, vec![command.clone()]);
        assert!(matches!(dispatch.status, MachineStatus::Suspended));
        assert_eq!(hosting_allocation(&controlling.state).unwrap(), Some(allocation.clone()));
        // Restore through the same registry checkpoint decoder before ACK.
        let restored: MachineCheckpoint = serde_json::from_value(
            serde_json::to_value(&controlling).unwrap()).unwrap();
        registry.validate_checkpoint(&restored).unwrap();
        let ack = json!({"commands":[{"operation_id":command.operation_id,"value":{"accepted":index}}]});
        let (committed, settled) = registry.step(&restored, &ack).unwrap();
        assert!(settled.commands.is_empty());
        assert!(matches!(settled.status, MachineStatus::Suspended));
        assert_eq!(hosting_allocation(&committed.state).unwrap(), Some(allocation.clone()));
        let reopened: MachineCheckpoint = serde_json::from_value(
            serde_json::to_value(&committed).unwrap()).unwrap();
        registry.validate_checkpoint(&reopened).unwrap();
        assert!(registry.step(&reopened, &ack).is_err());
        assert!(registry.step(&reopened, &input).is_err());
        assert_eq!(control_reply(&reopened.state).unwrap().unwrap().operation_id, command.operation_id);
        checkpoint = reopened;
    }
}

#[test]
fn pi_root_control_rejects_malformed_commands_and_fabricated_effect_ack_before_transition() {
    let machine = PiTaskMachine::new();
    let allocation = preparation().payload;
    let hosting = encode(&PiTaskState::Hosting {
        allocation: allocation.clone(), reply: None,
    }).unwrap();
    let command = WorkflowCommand {
        operation_id: OperationId::from_bytes([111; 16]),
        kind: "acyclic.task.observe.v1".to_owned(),
        payload: allocation,
    };
    for input in [
        json!({}),
        json!({"command":command,"extra":true}),
        json!({"command":{"operation_id":command.operation_id,"kind":command.kind}}),
        json!({"command":WorkflowCommand { kind:EFFECT_TASK_COMMAND_KIND.to_owned(), ..command.clone() }}),
        json!({"command":WorkflowCommand { kind:"caller.fabricated".to_owned(), ..command.clone() }}),
        json!({"command":WorkflowCommand { kind:"acyclic.pi.fork.v1".to_owned(), ..command.clone() }}),
        json!({"command":WorkflowCommand { kind:"acyclic.pi.import.v1".to_owned(), ..command.clone() }}),
        json!({"command":WorkflowCommand { kind:"acyclic.pi.writeback.v1".to_owned(), ..command.clone() }}),
        json!({"command":WorkflowCommand { kind:"acyclic.pi.inspect.v1".to_owned(), ..command.clone() }}),
        json!({"command":WorkflowCommand { kind:"acyclic.pi.diff.v1".to_owned(), ..command.clone() }}),
        json!({"command":WorkflowCommand { kind:"acyclic.pi.operator-approval.v1".to_owned(), ..command.clone() }}),
        json!({"command":WorkflowCommand { kind:"acyclic.pi.builtin.v1".to_owned(), ..command.clone() }}),
    ] {
        assert!(machine.transition(&hosting, &input).is_err());
    }
    let controlling = machine.transition(&hosting, &json!({"command":command})).unwrap();
    for input in [
        Value::Null,
        json!({"commands":[]}),
        json!({"commands":[{"operation_id":OperationId::from_bytes([112;16]),"value":null}]}),
        json!({"commands":[{"operation_id":command.operation_id,"value":null},{"operation_id":command.operation_id,"value":null}]}),
        json!({"commands":[{"operation_id":command.operation_id,"value":null,"effect_id":"fabricated"}]}),
        json!({"commands":[{"operation_id":command.operation_id,"value":null}],"extra":true}),
    ] {
        assert!(machine.transition(&controlling.state, &input).is_err());
    }
}

#[tokio::test]
async fn pi_pending_preparation_unknown_admission_fails_before_future_native_child_is_admitted() {
    let directory = tempfile::tempdir().unwrap();
    let root = binding();
    let (runtime, definition) = open(directory.path(), &root, true).await;
    runtime.harness().admit(
        OperationId::from_bytes(root.task_id.into_bytes()), &definition,
        PiTaskInput {
            binding: root.clone(),
            execution: PiTaskExecution::Host {
                root: PiRootIntent::ScratchOnly {}, preparation: preparation(),
            },
        }, None,
    ).await.unwrap();
    let mut state = session_state(&root);
    let mut child = root.clone();
    child.task_id = TaskId::from_bytes([121; 16]);
    child.native_session_id = "not-yet-native".to_owned();
    child.agent_id = AgentId::from_bytes([123; 16]);
    child.scratch.volume = VolumeRef::new(root.scratch.volume.provider().clone(), "scratch",
        VolumeClass::AgentPrivate, VolumeOwner::Agent(child.agent_id)).unwrap();
    state.pending.push_back(crate::PendingSpawn {
        task_binding: PluginTaskBinding::Pi(Box::new(child.clone())),
        agent_id: Some("future-child".to_owned()),
        parent_agent_id: state.root_agent_id.clone(),
        tool_use_id: "unknown-preparation".to_owned(),
        active_root_id: None,
        expires_at_millis: u64::MAX,
        workspace_name: "pending-only".to_owned(),
        fork_key: [122; 16],
        roots: BTreeMap::new(),
        mount_path: directory.path().join("agents")
            .join(blake3::hash(&child.task_id.into_bytes()).to_hex().as_str()).join("view"),
        lifecycle: crate::PendingSpawnLifecycle::Preparing,
    });
    // No synthetic publication, claimed fence or future child admission:
    // recovery must look up the ordinary preparation admission and fail closed.
    crate::validate_state_bounds(&state).unwrap();
    assert!(observe_pi_session(&runtime, &state).await.is_err());
    assert!(runtime.task_host().reconcile_admission(
        OperationId::from_bytes(child.task_id.into_bytes())
    ).await.unwrap().is_none());
    assert!(!state.pending[0].mount_path.exists());
    state.pending[0].parent_agent_id = "unretained-parent".to_owned();
    let error = observe_pi_session(&runtime, &state).await.err().unwrap();
    assert!(matches!(error, acyclic_harness::Error::Invalid(message) if message.contains("parent")));
}

#[test]
fn pi_root_control_bounded_malformed_input_mutations_never_emit_commands() {
    let machine = PiTaskMachine::new();
    let allocation = preparation().payload;
    let hosting = encode(&PiTaskState::Hosting {
        allocation: allocation.clone(), reply: None,
    }).unwrap();
    let valid = json!({"command":WorkflowCommand {
        operation_id: OperationId::from_bytes([123; 16]),
        kind: "acyclic.task.observe.v1".to_owned(),
        payload: allocation,
    }});
    // Exhaust the bounded primitive replacement domain at each required
    // command field. Rejection precedes command emission in the actual machine.
    for field in ["operation_id", "kind", "payload"] {
        for replacement in [Value::Null, json!(false), json!(0), json!([]), json!({})] {
            let mut mutated = valid.clone();
            mutated["command"][field] = replacement;
            assert!(machine.transition(&hosting, &mutated).is_err(), "{field}: {mutated}");
        }
    }
    let mut missing = valid;
    missing.as_object_mut().unwrap().remove("command");
    assert!(machine.transition(&hosting, &missing).is_err());
}

fn actual_preparing_session(
    fixture: &crate::pi_fork::tests::Fixture,
    plan: &crate::pi_fork::PiForkPlan,
) -> AdapterState {
    let PiRootIntent::Project { context_id, root_id, paths } = &fixture.root_request.root else {
        panic!("real fork fixture must retain its original project selection");
    };
    let mut root = fixture.root_request.binding.clone();
    root.scratch.allocation = PiScratchAllocation::Created(fixture.root_allocation.scratch.clone());
    let mut state = AdapterState {
        version: crate::ADAPTER_STATE_VERSION,
        active: true,
        root_session_id: root.native_session_id.clone(),
        root_agent_id: root.native_session_id.clone(),
        root_context_id: *context_id,
        root_id: *root_id,
        task_binding: PluginTaskBinding::Pi(Box::new(root)),
        ..AdapterState::default()
    };
    for (key, creation) in &fixture.root_allocation.roots {
        state.roots.insert(key.clone(), crate::RootBinding {
            root_id: *root_id,
            path: Path::new(&paths[key]).to_path_buf(),
            allocation: crate::RootAllocation::Created(creation.clone()),
        });
    }
    state.pending.push_back(crate::PendingSpawn {
        task_binding: PluginTaskBinding::Pi(Box::new(plan.commit.child.clone())),
        agent_id: Some(plan.commit.alias.clone()),
        parent_agent_id: state.root_agent_id.clone(),
        tool_use_id: plan.commit.sdk_operation.clone(),
        active_root_id: Some(*root_id),
        expires_at_millis: u64::MAX,
        workspace_name: plan.commit.alias.clone(),
        fork_key: plan.original.into_bytes(),
        roots: BTreeMap::new(),
        mount_path: crate::pi_native::planned_native_paths(&fixture.resources,
            &state.root_session_id, plan.commit.child.task_id, paths.keys().next().map(String::as_str))
            .unwrap().view_root,
        lifecycle: crate::PendingSpawnLifecycle::Preparing,
    });
    crate::validate_state_bounds(&state).unwrap();
    state
}

#[tokio::test]
async fn pi_real_preparation_pending_reserved_and_succeeded_never_report_future_native_child_success() {
    use crate::pi_recovery::PiPreparationPhase;
    let fixture = crate::pi_fork::tests::Fixture::new().await;
    let plan = fixture.plan().await;
    let state = actual_preparing_session(&fixture, &plan);
    let host = fixture.runtime.runtime.task_host();
    host.admit_owned(plan.preparation.clone(), fixture.fence.clone()).await.unwrap();
    let query = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new()).unwrap();
    let observed = observe_pi_session_with_native_work(&fixture.runtime.runtime, &state, &query).await.unwrap();
    assert!(matches!(&observed.0[1], PiTaskObservation::Preparation {
        task_id, preparation, phase: PiPreparationPhase::Pending,
    } if *task_id == plan.commit.child.task_id && *preparation == plan.preparation.task_id()));
    drop(observed);
    assert_eq!(query.live_allocation_bytes(), 0);
    let partial = fixture.runtime.runtime.run_operation(&Worker {
        id: "pi-preparing-observation".into(), available: ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    }, plan.preparation.task_id(), &fixture.runtime.runtime.commands(), 1).await.unwrap().unwrap();
    assert!(matches!(partial, acyclic_harness::filesystem::TaskWorkerAttempt::Progress(
        acyclic_harness::filesystem::TaskWorkerOutcome::Yielded { .. })));
    let observed = observe_pi_session_with_native_work(&fixture.runtime.runtime, &state, &query).await.unwrap();
    assert!(matches!(&observed.0[1], PiTaskObservation::Preparation {
        phase: PiPreparationPhase::Reserved, ..
    }));
    drop(observed);
    assert_eq!(query.live_allocation_bytes(), 0);
    let report = fixture.prepare_with(&fixture.coordinator(), &fixture.context, &fixture.journal,
        &fixture.fence, &plan, fixture.source.clone(),
        crate::pi_root_provider::allocation_budget(), acyclic_fs::CancellationToken::new())
        .await.unwrap().unwrap();
    report.value.validate().unwrap();
    assert!(host.observe_admission(plan.request.child.task_id()).await.is_err());
    let reopen_work = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new()).unwrap();
    let reopened = OwnedRuntime::open_with_native_work(
        fixture.stream.clone(), fixture.filesystem.clone(), &fixture.owner.policy().journal,
        fixture.owner.authority_with_native_work(&reopen_work).unwrap(), &reopen_work,
    ).await.unwrap();
    for _ in 0..2 {
        let observed = observe_pi_session_with_native_work(&reopened.runtime, &state, &query).await.unwrap();
        assert!(matches!(&observed.0[1], PiTaskObservation::Preparation {
            task_id, preparation, phase: PiPreparationPhase::Succeeded,
        } if *task_id == plan.commit.child.task_id && *preparation == plan.preparation.task_id()));
        drop(observed);
        assert_eq!(query.live_allocation_bytes(), 0);
        assert!(host.observe_admission(plan.commit.child.task_id).await.is_err());
        assert!(state.routes.is_empty());
        assert!(matches!(state.pending[0].lifecycle, crate::PendingSpawnLifecycle::Preparing));
    }
}

#[tokio::test]
async fn pi_real_preparation_changed_planned_binding_parent_and_foreign_key_fail_before_child_admission() {
    let fixture = crate::pi_fork::tests::Fixture::new().await;
    let plan = fixture.plan().await;
    let state = actual_preparing_session(&fixture, &plan);
    fixture.runtime.runtime.task_host().admit_owned(
        plan.preparation.clone(), fixture.fence.clone()).await.unwrap();
    let query = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new()).unwrap();
    for mutation in 0..5 {
        let mut changed = state.clone();
        match mutation {
            0 => {
                let PluginTaskBinding::Pi(child) = &mut changed.pending[0].task_binding else { unreachable!() };
                child.native_session_id.push_str("-retargeted");
            },
            1 => changed.pending[0].parent_agent_id = "foreign-parent".into(),
            2 => changed.pending[0].tool_use_id.push_str("-foreign-original-operation"),
            3 => changed.pending[0].agent_id = Some("wrong-original-alias".into()),
            _ => changed.pending[0].agent_id = None,
        }
        assert!(observe_pi_session_with_native_work(&fixture.runtime.runtime, &changed, &query).await.is_err());
        assert_eq!(query.live_allocation_bytes(), 0);
        assert!(fixture.runtime.runtime.task_host().observe_admission(
            plan.request.child.task_id()).await.is_err());
    }
    let mut stale = fixture.fence.clone();
    stale.reservation_id = "stale-original-parent-fence".into();
    assert!(fixture.prepare_with(&fixture.coordinator(), &fixture.context, &fixture.journal, &stale,
        &plan, fixture.source.clone(), crate::pi_root_provider::allocation_budget(),
        acyclic_fs::CancellationToken::new()).await.is_err());
    assert!(fixture.runtime.runtime.task_host().observe_admission(
        plan.request.child.task_id()).await.is_err());
}

#[tokio::test]
async fn pi_real_root_committed_control_reopen_keeps_original_context_and_next_allocation() {
    let fixture = crate::pi_fork::tests::Fixture::new().await;
    let task = fixture.owner.policy().root_task;
    let host = fixture.runtime.runtime.task_host();
    let WorkPull::Claimed(lease) = host.recover_work(task).await.unwrap() else {
        panic!("fixture retains original root control owner");
    };
    let admission = host.observe_admission(task).await.unwrap();
    let settled = fixture.runtime.runtime.resume_task(
        lease, &fixture.runtime.runtime.commands(), 8).await;
    assert!(matches!(settled, acyclic_harness::filesystem::TaskWorkerAttempt::Progress(
        acyclic_harness::filesystem::TaskWorkerOutcome::Suspended { .. })));
    let committed = fixture.runtime.runtime.observe_workflow_checkpoint(task).await.unwrap();
    let allocation = hosting_allocation(&committed.state).unwrap().unwrap();
    let reply = control_reply(&committed.state).unwrap().unwrap();
    let accounting = acyclic_harness::distributed::read_coordinator_event_page(
        &fixture.stream, 0, 1024).await.unwrap();
    assert!(accounting.len() < 1024, "bounded fixture must fit one complete accounting page");
    let reopen_work = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new()).unwrap();
    let reopened = OwnedRuntime::open_with_native_work(
        fixture.stream.clone(), fixture.filesystem.clone(), &fixture.owner.policy().journal,
        fixture.owner.authority_with_native_work(&reopen_work).unwrap(), &reopen_work,
    ).await.unwrap();
    assert_eq!(reopened.runtime.observe_workflow_checkpoint(task).await.unwrap(), committed);
    assert_eq!(reopened.runtime.task_host().observe_admission(task).await.unwrap(), admission);
    assert!(matches!(reopened.runtime.task_host().recover_work(task).await.unwrap(), WorkPull::Idle));
    // Exact same committed ACK and command both fail before a new outbox
    // dispatch. Inspection/reopen has not taken another root owner/context.
    let machine = PiTaskMachine::new();
    assert!(machine.transition(&committed.state, &json!({"commands":[reply]})).is_err());
    let duplicate = fixture.root_control.clone();
    assert_eq!(reply.operation_id, duplicate.operation_id);
    assert!(machine.transition(&committed.state, &json!({"command":duplicate})).is_err());
    assert_eq!(acyclic_harness::distributed::read_coordinator_event_page(
        &fixture.stream, 0, 1024).await.unwrap(), accounting);
    let operation = OperationId::from_bytes([126; 16]);
    let payload = crate::pi_root_provider::stage_pi_input(
        &fixture.owner, &fixture.filesystem, operation, "next-timer", &json!({"deadline_unix_ms":0}),
        crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new(),
    ).await.unwrap().value;
    let input = crate::pi_task::PiControlInput { command: WorkflowCommand {
        operation_id: operation, kind: "acyclic.timer.v1".into(), payload,
    }};
    let input = crate::pi_root_provider::stage_pi_input(
        &fixture.owner, &fixture.filesystem, operation, "next-control", &input,
        crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new(),
    ).await.unwrap().value;
    let suspension = reopened.runtime.task_host().workflow_suspension(task).await.unwrap().unwrap();
    reopened.runtime.task_host().resume_workflow(task, suspension.revision, input,
        acyclic_harness::IdempotencyKey::new("pi-root-control/distinct-next").unwrap()).await.unwrap();
    let next = reopened.runtime.run_operation(&Worker {
        id: "pi-root-control-next".into(), available: ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    }, task, &reopened.runtime.commands(), 8).await.unwrap().unwrap();
    assert!(matches!(next, acyclic_harness::filesystem::TaskWorkerAttempt::Progress(
        acyclic_harness::filesystem::TaskWorkerOutcome::Suspended { .. })));
    let next = reopened.runtime.observe_workflow_checkpoint(task).await.unwrap();
    assert_eq!(hosting_allocation(&next.state).unwrap(), Some(allocation));
    assert_eq!(control_reply(&next.state).unwrap().unwrap().operation_id, operation);
    assert_eq!(reopened.runtime.task_host().observe_admission(task).await.unwrap(), admission);
    let accounting = acyclic_harness::distributed::read_coordinator_event_page(
        &fixture.stream, 0, 1024).await.unwrap();
    assert!(accounting.len() < 1024);
    assert!(!accounting.iter().any(|record| matches!(
        &record.event, acyclic_harness::scheduler::SchedulerEvent::ModelDispatchClaimed { .. }
    )), "passive native root controls must not charge harness model attempts");
}

#[tokio::test]
async fn pi_operator_control_kind_does_not_authorize_invalid_original_command_fingerprint() {
    use crate::pi_operator::{PiOperatorBody, PiOperatorRequest};
    let fixture = crate::pi_fork::tests::Fixture::new().await;
    let work = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new(),
    ).unwrap();
    let session = &fixture.root_request.binding.native_session_id;
    let PiRootIntent::Project { paths, .. } = &fixture.root_request.root else {
        panic!("operator fixture must retain original root selection");
    };
    let cwd = Path::new(paths.values().next().unwrap()).to_path_buf();
    let (control, control_custody) = crate::pi_operator::build_control_request(
        &fixture.owner, cwd, PiOperatorBody::Inspect {
            session: session.clone(), operation: "original-operator-fingerprint".into(),
            project_files: false, file_evidence: None,
        }, &work,
    ).unwrap();
    let (mut request, request_custody) = PiOperatorRequest::from_value_with_native_work(
        &control.arguments, &work,
    ).unwrap();
    crate::pi_operator::verify_request(&fixture.owner, session, &request, &work).unwrap();
    // The machine accepts only the installed command envelope; authorization
    // remains with the production operator verifier under the original owner.
    let machine = PiTaskMachine::new();
    let hosting = encode(&PiTaskState::Hosting {
        allocation: fixture.root_control.payload.clone(), reply: None,
    }).unwrap();
    let command = WorkflowCommand {
        operation_id: OperationId::from_bytes([127; 16]),
        kind: "acyclic.pi.operator-approval".into(),
        payload: fixture.root_control.payload.clone(),
    };
    assert_eq!(machine.transition(&hosting, &json!({"command":command})).unwrap().commands,
        vec![command]);
    request.body = PiOperatorBody::Inspect {
        session: session.clone(), operation: "changed-operator-fingerprint".into(),
        project_files: false, file_evidence: None,
    };
    assert!(crate::pi_operator::verify_request(&fixture.owner, session, &request, &work).is_err());
    request.operator = crate::pi_caller::issue(
        &fixture.owner, session, &fixture.root_request.binding,
    ).unwrap();
    assert!(crate::pi_operator::verify_request(&fixture.owner, session, &request, &work).is_err());
    drop(request);
    drop(request_custody);
    drop(control);
    drop(control_custody);
}

#[tokio::test]
async fn pi_native_recovery_rejects_original_quota_and_cancellation_before_graph_growth_or_sdk_reads() {
    let directory = tempfile::tempdir().unwrap();
    let binding = binding();
    let (runtime, definition) = open(directory.path(), &binding, true).await;
    runtime.harness().admit(OperationId::from_bytes(binding.task_id.into_bytes()), &definition,
        PiTaskInput { binding: binding.clone(), execution: PiTaskExecution::Host {
            root: PiRootIntent::ScratchOnly {}, preparation: preparation(),
        } }, None).await.unwrap();
    let state = session_state(&binding);
    let mut budget = crate::pi_root_provider::allocation_budget();
    budget.peak_allocation_bytes = 1;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let before = work.receipt();
    assert!(matches!(observe_pi_session_with_native_work(&runtime, &state, &work).await,
        Err(acyclic_harness::Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::BudgetExceeded { .. }), ..
        })));
    assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
    assert_eq!(work.receipt().backend_read_operations, before.backend_read_operations);
    assert_eq!(work.live_allocation_bytes(), 0);
    let work = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
    let before = work.receipt();
    cancellation.cancel();
    assert!(matches!(observe_pi_session_with_native_work(&runtime, &state, &work).await,
        Err(acyclic_harness::Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
    assert_eq!(work.receipt(), before);
    assert_eq!(work.live_allocation_bytes(), 0);
    assert!(matches!(runtime.task_host().recover_work(binding.task_id).await.unwrap(), WorkPull::Idle));
}

#[tokio::test]
async fn pi_native_authentication_retains_original_admission_through_typed_input_last_use() {
    let directory = tempfile::tempdir().unwrap();
    let binding = binding();
    let (runtime, definition) = open(directory.path(), &binding, true).await;
    runtime.harness().admit(OperationId::from_bytes(binding.task_id.into_bytes()), &definition,
        PiTaskInput { binding: binding.clone(), execution: PiTaskExecution::Host {
            root: PiRootIntent::ScratchOnly {}, preparation: preparation(),
        } }, None).await.unwrap();
    let work = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new()).unwrap();
    let input = authenticate_pi_task_with_native_work(&runtime, &binding,
        PiTaskOrigin::Root(&PiRootIntent::ScratchOnly {}), &work).await.unwrap();
    assert_eq!(input.0.binding, binding);
    assert!(matches!(input.0.execution, PiTaskExecution::Host { .. }));
    assert!(input.1.bytes() > 0);
    let same = work.retain_owned(0).unwrap();
    assert!(input.1.shares_ledger(&same));
    assert!(work.live_allocation_bytes() >= input.1.bytes());
    drop(same);
    drop(input);
    assert_eq!(work.live_allocation_bytes(), 0);
}

#[cfg(not(target_arch = "wasm32"))]
#[path = "../../rust/crates/harness/tests/support/stream.rs"]
mod native_original_stream;

/// Direct native SPI regressions. Recovery and host/worker integration retain
/// their separate sections above; these execute the production machine itself.
#[cfg(not(target_arch = "wasm32"))]
mod native_transition {
    use super::*;
    use acyclic_fs::{CancellationToken, NativeAllocation, NativeMaterializationWork};
    use acyclic_harness::{
        IdempotencyKey,
        filesystem::FilesystemWorkflowJournal,
        runtime::ResumableTaskSession,
        workflow::{WorkflowJournal, WorkflowRecord},
    };
    use futures::TryStreamExt as _;
    use std::sync::atomic::Ordering;

    type NativeStream = native_original_stream::LostSessionAck<LocalStream>;
    type NativeRuntime = PiTaskRuntime<NativeStream, acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;
    type NativeJournal = FilesystemWorkflowJournal<NativeStream, acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;

    struct OriginalJournalFixture {
        binding: PiTaskBinding,
        command: WorkflowCommand,
        owner: PiOwnerConfig,
        runtime: NativeRuntime,
        filesystem: Arc<FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>,
        provider: Arc<NativeStream>,
        stream: StreamClient<NativeStream>,
        fence: LeaseFence,
        _startup_work: NativeMaterializationWork,
        _admission: (Admission<TaskId>, NativeAllocation),
        _directory: tempfile::TempDir,
        // Original admitted command metadata outlives every fixture consumer.
        _command_custody: NativeAllocation,
    }

    impl OriginalJournalFixture {
        async fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let binding = binding();
            let journal = VolumeRef::new(binding.scratch.volume.provider().clone(), "journal",
                VolumeClass::AgentPrivate, VolumeOwner::Agent(binding.agent_id)).unwrap();
            let grants = Capabilities::new([
                "operation:declare".to_owned(), "operation:observe".to_owned(),
                "operation:cancel".to_owned(), "operation:wake".to_owned(),
                "task:spawn:acyclic.pi@1".to_owned(), "effect:plan".to_owned(),
                "effect:run".to_owned(), "conversation:bind".to_owned(),
                format!("effect:provider:{}", crate::pi_root_inputs::ROOT_ALLOCATION_PROVIDER),
                journal.directory_read_capability(crate::pi_root_inputs::ROOT_INPUT_DIRECTORY).unwrap(),
                binding.scratch.volume.capability(VolumeOperation::Read).unwrap(),
                binding.scratch.volume.capability(VolumeOperation::Write).unwrap(),
            ]);
            let original = work();
            let owner = PiOwnerConfig::new_with_native_runtime_with_native_work(&PiOwnerPolicy {
                root_task: binding.task_id, agent: binding.agent_id, journal: journal.clone(), grants,
                limits: Limits { file_bytes: 1_048_576, path_bytes: 4096,
                    render_bytes: 1_048_576, ..Limits::default() },
                run_limits: TaskRunLimits { concurrency: Some(4), max_steps: Some(128),
                    deadline_epoch_ms: None },
                session_limits: SessionLimits { active_tasks: 4, total_tasks: 16, depth: 4, model_steps: 128 },
                concurrency: 4, maximum_payload_bytes: 1_048_576,
            }, None, &original).unwrap();
            owner.save_new_with_native_work(directory.path(), &original).unwrap();
            let filesystem = Arc::new(FilesystemHost::new(
                acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(directory.path().join("filesystem")))
                    .await.unwrap(), journal.provider().clone(),
            ).unwrap());
            filesystem.create_volume(&journal).await.unwrap();
            let (command, command_custody) = crate::pi_root_inputs::stage_root_command(
                &owner, &filesystem, &crate::pi_root_inputs::PiRootRequest {
                    binding: binding.clone(), root: PiRootIntent::ScratchOnly {},
                }, &original,
            ).await.unwrap();
            let provider = Arc::new(NativeStream::new(
                LocalStream::open(directory.path().join("stream"), LocalStreamLimits::default()).await.unwrap(),
            ));
            let stream = StreamClient::new(provider.clone());
            let runtime = PiTaskRuntime::open_with_native_work(
                stream.clone(), filesystem.clone(), &journal,
                owner.authority_with_native_work(&original).unwrap(), &original,
            ).await.unwrap();
            let admission = runtime.admit_root_with_native_work(
                binding.clone(), PiRootIntent::ScratchOnly {}, command.clone(), &original,
            ).await.unwrap();
            let WorkPull::Claimed(lease) = runtime.runtime.task_host().pull_operation(
                &Worker { id: "pi-native-original-journal".to_owned(), available: ResourceSnapshot::default(),
                    labels: BTreeMap::new() },
                OperationId::from_bytes(binding.task_id.into_bytes()),
            ).await.unwrap() else { panic!("actual original Pi admission must reserve its task"); };
            runtime.runtime.task_host().start_task(&lease).await.unwrap();
            let fence = LeaseFence::from(&lease.reservation);
            Self { _directory: directory, binding, command, owner, runtime, filesystem, provider, stream, fence,
                _startup_work: original, _admission: admission, _command_custody: command_custody }
        }

        async fn session(&self, work: &NativeMaterializationWork) -> acyclic_harness::Result<ResumableTaskSession> {
            let context = self.runtime.runtime.task_context_with_native_work(
                self.binding.task_id, &self.fence, work,
            ).await?;
            self.runtime.runtime.open_task_with_native_work(
                self.binding.task_id, &self.fence, context, work,
            ).await
        }

        async fn journal(&self, work: &NativeMaterializationWork) -> Arc<NativeJournal> {
            self.runtime.runtime.workflow_journal_with_native_work(
                self.binding.task_id, &self.fence, work,
            ).await.unwrap()
        }

        fn journal_path(&self) -> String {
            format!("harness/v2/workflows/{}", OperationId::from_bytes(self.binding.task_id.into_bytes()))
        }

        async fn native_step(
            &self, session: &mut ResumableTaskSession, operation: u8, key: &str,
            input: &Value, work: &NativeMaterializationWork,
        ) -> acyclic_harness::Result<(MachineTransition, NativeAllocation)> {
            let (input, custody) = crate::pi_root_provider::guarded_pi_value(
                input, self.owner.policy().maximum_payload_bytes, work,
            )?;
            session.step_with_native_work(OperationId::from_bytes([operation; 16]),
                IdempotencyKey::new(key).unwrap(), input, custody).await
        }

        async fn replay(&self, journal: &NativeJournal, work: &NativeMaterializationWork)
            -> (Vec<WorkflowRecord>, NativeAllocation)
        {
            journal.replay_with_native_work(0, 16, work).unwrap().await.unwrap()
        }

        async fn original_pointer(&self, revision: u64) -> FileRef {
            let records = self.stream.stream(self.journal_path()).unwrap().read(revision, 1)
                .await.unwrap().try_collect::<Vec<_>>().await.unwrap();
            let [record] = records.as_slice() else { panic!("original committed workflow pointer"); };
            serde_json::from_slice(&record.value).unwrap()
        }

        async fn hosting(
            &self, session: &mut ResumableTaskSession, work: &NativeMaterializationWork,
        ) {
            let emitted = self.native_step(session, 151, "native-root-preparing", &Value::Null, work)
                .await.unwrap();
            assert_eq!(emitted.0.commands, vec![self.command.clone()]);
            drop(emitted.0);
            drop(emitted.1);
            // This boundary tests checkpoint settlement, not provider execution:
            // it supplies an explicit settled Outcome and an authentic uploaded
            // FileRef, never inferring success from a path or absent effect.
            let uploaded = crate::pi_root_provider::stage_pi_input_with_native_work(
                &self.owner, &self.filesystem, OperationId::from_bytes([152; 16]),
                "native-test-settlement", &json!({"settled":"explicit workflow input"}), work,
            ).await.unwrap();
            let input = result(self.command.operation_id, Outcome::Succeeded(uploaded.0.clone()));
            let settled = self.native_step(session, 152, "native-root-settlement", &input, work)
                .await.unwrap();
            assert_eq!(settled.0.state["allocation"], serde_json::to_value(&uploaded.0).unwrap());
            assert_eq!(session.checkpoint().revision, 2);
            drop(settled.0);
            drop(settled.1);
            drop(uploaded.0);
            drop(uploaded.1);
        }
    }

    fn work() -> NativeMaterializationWork {
        NativeMaterializationWork::new(
            crate::pi_root_provider::allocation_budget(), &CancellationToken::new(),
        ).unwrap()
    }

    fn control() -> WorkflowCommand {
        WorkflowCommand {
            operation_id: OperationId::from_bytes([91; 16]),
            kind: "acyclic.pi.inspect".to_owned(),
            payload: preparation().payload,
        }
    }

    fn result(operation: OperationId, value: Outcome<FileRef>) -> Value {
        json!({"commands":[{"operation_id":operation,"value":value}]})
    }

    // Mirror the settled SDK contract: the actual incoming body's original
    // guard spans the callback and is merged with NEW output allocations.
    fn step(
        machine: &PiTaskMachine, state: &Value, input: &Value,
        work: &NativeMaterializationWork,
    ) -> acyclic_harness::Result<(MachineTransition, NativeAllocation)> {
        let original_state = crate::pi_root_provider::guarded_pi_value(state, 4 * 1024 * 1024, work)?;
        let (input, mut input_custody) =
            crate::pi_root_provider::guarded_pi_value(input, 4 * 1024 * 1024, work)?;
        let next = machine.transition_with_native_work(&original_state.0, input, work);
        drop(original_state.0);
        drop(original_state.1);
        match next {
            Ok((transition, mut output_custody)) => {
                input_custody.absorb(&mut output_custody).unwrap();
                Ok((transition, input_custody))
            }
            Err(error) => Err(error),
        }
    }

    #[test]
    fn native_all_states_preserve_ordinary_commands_schema_and_settlement() {
        let machine = PiTaskMachine::new();
        assert!(machine.supports_native_work());
        let command = preparation();
        let control = control();
        let cases = [
            (json!({"state":"preparing","command":command}), Value::Null),
            (json!({"state":"ready","command":command}), Value::Null),
            (json!({"state":"allocating","operation":command.operation_id}),
                result(command.operation_id, Outcome::Succeeded(command.payload.clone()))),
            (json!({"state":"awaiting","operation":command.operation_id}),
                result(command.operation_id, Outcome::Succeeded(command.payload.clone()))),
            (json!({"state":"awaiting","operation":command.operation_id}),
                result(command.operation_id, Outcome::Failed { message: "known failure".to_owned() })),
            (json!({"state":"hosting","allocation":command.payload}), Value::Null),
            (json!({"state":"hosting","allocation":command.payload,
                "reply":{"operation_id":command.operation_id,"value":{"retained":[1,2,3]}}}),
                json!({"command":control})),
            (json!({"state":"controlling","allocation":command.payload,"operation":control.operation_id}),
                json!({"commands":[{"operation_id":control.operation_id,"value":{"retained":[1,2,3]}}]})),
        ];
        for (state, input) in cases {
            let expected = machine.transition(&state, &input).unwrap();
            let work = work();
            let actual = step(&machine, &state, &input, &work).unwrap();
            assert_eq!(actual.0, expected);
            assert_eq!(work.receipt(), actual.1.work().receipt());
            assert!(actual.1.bytes() > 0);
            assert_eq!(work.live_allocation_bytes(), actual.1.bytes());
            drop(actual.0);
            drop(actual.1);
            assert_eq!(work.live_allocation_bytes(), 0);
        }
        for state in [
            json!({"state":"completed","result":command.payload}),
            json!({"state":"failed","message":"known failure"}),
        ] {
            let work = work();
            assert!(step(&machine, &state, &Value::Null, &work).is_err());
            assert_eq!(work.live_allocation_bytes(), 0);
        }
    }

    #[test]
    fn native_large_control_reply_moves_original_body_and_owned_extraction_keeps_pointer() {
        let machine = PiTaskMachine::new();
        let work = work();
        let command = control();
        let state = json!({"state":"controlling","allocation":command.payload,"operation":command.operation_id});
        let source = json!({"commands":[{"operation_id":command.operation_id,"value":"x".repeat(256 * 1024)}]});
        let (input, mut input_custody) =
            crate::pi_root_provider::guarded_pi_value(&source, 1024 * 1024, &work).unwrap();
        let pointer = input["commands"][0]["value"].as_str().unwrap().as_ptr();
        let before = work.receipt();
        let (transition, mut output_custody) =
            machine.transition_with_native_work(&state, input, &work).unwrap();
        assert_eq!(transition.state["reply"]["value"].as_str().unwrap().as_ptr(), pointer);
        assert!(output_custody.bytes() < 64 * 1024);
        assert!(work.receipt().bytes_copied - before.bytes_copied < 64 * 1024);
        input_custody.absorb(&mut output_custody).unwrap();
        assert_eq!(work.live_allocation_bytes(), input_custody.bytes());
        let (reply, mut metadata_custody) = control_reply_owned(transition.state, &work).unwrap();
        input_custody.absorb(&mut metadata_custody).unwrap();
        let reply = reply.unwrap();
        assert_eq!(reply.operation_id, command.operation_id);
        assert_eq!(reply.value.as_str().unwrap().as_ptr(), pointer);
        let final_wire = crate::pi_root_provider::guarded_pi_bytes(
            &reply.value, 1024 * 1024, &work,
        ).unwrap();
        assert_eq!(input_custody.work().receipt(), work.receipt());
        drop(final_wire.0);
        drop(final_wire.1);
        drop(reply);
        drop(transition.commands);
        drop(transition.status);
        drop(input_custody);
        assert_eq!(work.live_allocation_bytes(), 0);
    }

    #[test]
    fn native_null_hosting_duplicates_checkpoint_using_observed_original_codec() {
        let machine = PiTaskMachine::new();
        let work = work();
        let command = control();
        let original = crate::pi_root_provider::guarded_pi_value(&json!({
            "state":"hosting","allocation":command.payload,
            "reply":{"operation_id":command.operation_id,"value":"y".repeat(128 * 1024)}
        }), 1024 * 1024, &work).unwrap();
        let pointer = original.0["reply"]["value"].as_str().unwrap().as_ptr();
        let before = work.receipt();
        let next = machine.transition_with_native_work(&original.0, Value::Null, &work).unwrap();
        assert_eq!(next.0.state, original.0);
        assert_ne!(next.0.state["reply"]["value"].as_str().unwrap().as_ptr(), pointer);
        assert!(work.receipt().bytes_copied - before.bytes_copied >= 128 * 1024);
        assert!(next.1.bytes() >= 128 * 1024);
        assert!(next.1.shares_ledger(&original.1));
        drop(original.0);
        drop(original.1);
        assert_eq!(work.live_allocation_bytes(), next.1.bytes());
        drop(next.0);
        drop(next.1);
        assert_eq!(work.live_allocation_bytes(), 0);
    }

    #[test]
    fn native_quota_and_original_cancellation_precede_state_and_command_vector_growth() {
        let machine = PiTaskMachine::new();
        let state = json!({"state":"preparing","command":preparation()});
        let preserved = state.clone();
        let mut budget = crate::pi_root_provider::allocation_budget();
        budget.peak_allocation_bytes = 1;
        let work = NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap();
        let before = work.receipt();
        let failure = machine.transition_with_native_work(&state, Value::Null, &work).err().unwrap();
        assert!(matches!(&failure, acyclic_harness::Error::NativeWork { .. }));
        assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
        drop(failure);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert_eq!(state, preserved);
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(
            crate::pi_root_provider::allocation_budget(), &cancellation,
        ).unwrap();
        let before = work.receipt();
        cancellation.cancel();
        let failure = machine.transition_with_native_work(&state, Value::Null, &work).err().unwrap();
        assert!(matches!(&failure, acyclic_harness::Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
        }));
        assert_eq!(work.receipt(), before);
        drop(failure);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert_eq!(state, preserved);
    }

    #[test]
    fn native_bounded_replay_model_preserves_first_reference_and_one_effect_command() {
        use std::collections::BTreeSet;
        let command = preparation();
        let control = control();
        let mut frontier = vec![
            (json!({"state":"preparing","command":command}), 0usize),
            (json!({"state":"ready","command":command}), 0usize),
        ];
        let inputs = [
            Value::Null,
            json!({}),
            json!({"command":control}),
            result(command.operation_id, Outcome::Succeeded(command.payload.clone())),
            result(command.operation_id, Outcome::Failed { message: "settled failure".to_owned() }),
            result(command.operation_id, Outcome::Cancelled),
            result(command.operation_id, Outcome::Indeterminate { operation_id: command.operation_id }),
            result(OperationId::from_bytes([99; 16]), Outcome::Succeeded(command.payload.clone())),
            json!({"commands":[{"operation_id":control.operation_id,"value":{"observed":true}}]}),
            json!({"commands":[]}),
            json!({"commands":[{},{}]}),
            json!({"commands":[],"unknown":true}),
        ];
        let mut visited = BTreeSet::new();
        let mut settled = 0;
        for _ in 0..6 {
            let mut next = Vec::new();
            for (state, effects) in frontier {
                if !visited.insert(serde_json::to_string(&(state.clone(), effects)).unwrap()) {
                    continue;
                }
                let machine = PiTaskMachine::new();
                for input in &inputs {
                    let expected = machine.transition(&state, input);
                    let work = work();
                    let actual = step(&machine, &state, input, &work);
                    // Crash/reload reconstruction changes neither acceptance
                    // nor the exact output of the pinned production machine.
                    let restored: Value = serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
                    let replay = step(&PiTaskMachine::new(), &restored, input, &work);
                    assert_eq!(actual.is_ok(), expected.is_ok());
                    assert_eq!(actual.is_ok(), replay.is_ok());
                    if let (Ok(actual), Ok(replay)) = (actual, replay) {
                        assert_eq!(actual.0, expected.unwrap());
                        assert_eq!(actual.0, replay.0);
                        let emitted = effects + actual.0.commands.iter()
                            .filter(|command| command.kind == EFFECT_TASK_COMMAND_KIND).count();
                        assert!(emitted <= 1);
                        for emitted in &actual.0.commands {
                            assert!(emitted == &command || emitted == &control);
                        }
                        if let Some(reference) = actual.0.state.get("allocation").or_else(|| actual.0.state.get("result")) {
                            assert_eq!(reference, &serde_json::to_value(&command.payload).unwrap());
                            settled += 1;
                        }
                        next.push((actual.0.state.clone(), emitted));
                        drop(actual.0);
                        drop(actual.1);
                        drop(replay.0);
                        drop(replay.1);
                    }
                    assert_eq!(work.live_allocation_bytes(), 0);
                }
            }
            frontier = next;
        }
        assert!(visited.len() >= 8);
        assert!(settled > 0);
    }

    #[test]
    fn native_seeded_fuzz_rejects_wrong_operation_duplicates_and_unknown_fields() {
        let machine = PiTaskMachine::new();
        let command = control();
        let state = json!({"state":"controlling","allocation":command.payload,"operation":command.operation_id});
        let preserved = state.clone();
        let mut seed = 0x5eed_91u64;
        for index in 0..128 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let valid = json!({"operation_id":command.operation_id,"value":{"seed":seed}});
            let input = match index % 8 {
                0 => json!({"commands":[valid]}),
                1 => json!({"commands":[valid.clone(),valid]}),
                2 => json!({"commands":[]}),
                3 => json!({"commands":[{"operation_id":OperationId::from_bytes([99;16]),"value":seed}]}),
                4 => json!({"commands":[valid],"unknown":seed}),
                5 => json!({"commands":[{"operation_id":command.operation_id,"value":seed,"unknown":true}]}),
                6 => json!({"commands":[{"operation_id":command.operation_id}]}),
                _ => json!({"commands":[{"operation_id":seed,"value":null}]}),
            };
            let work = work();
            let actual = step(&machine, &state, &input, &work);
            let expected = machine.transition(&state, &input);
            assert_eq!(actual.is_ok(), index % 8 == 0);
            assert_eq!(actual.is_ok(), expected.is_ok());
            if let Ok(actual) = actual {
                assert_eq!(actual.0, expected.unwrap());
                drop(actual.0);
                drop(actual.1);
            }
            assert_eq!(state, preserved);
            assert_eq!(work.live_allocation_bytes(), 0);
        }
        for malformed in [
            json!({"state":"controlling","allocation":command.payload,"operation":command.operation_id,"unknown":true}),
            json!({"state":"hosting","allocation":command.payload,
                "reply":{"operation_id":command.operation_id,"value":null,"unknown":true}}),
            json!({"state":"hosting","allocation":{},"reply":null}),
        ] {
            let work = work();
            assert!(machine.transition_with_native_work(&malformed, Value::Null, &work).is_err());
            assert!(control_reply_owned(malformed, &work).is_err());
            assert_eq!(work.live_allocation_bytes(), 0);
        }
    }

    #[test]
    fn native_deterministic_work_prefixes_never_accept_partial_transition() {
        let machine = PiTaskMachine::new();
        let state = json!({"state":"ready","command":preparation()});
        let expected = machine.transition(&state, &Value::Null).unwrap();
        let mut failures = 0;
        let mut successes = 0;
        for maximum in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 8192] {
            let mut budget = crate::pi_root_provider::allocation_budget();
            budget.items_examined = maximum;
            let work = NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap();
            match machine.transition_with_native_work(&state, Value::Null, &work) {
                Ok(actual) => {
                    successes += 1;
                    assert_eq!(actual.0, expected);
                    assert_eq!(actual.1.work().receipt(), work.receipt());
                    drop(actual.0);
                    drop(actual.1);
                }
                Err(error) => {
                    failures += 1;
                    assert!(matches!(&error, acyclic_harness::Error::NativeWork { .. }));
                    assert!(work.receipt().items_examined <= maximum);
                    drop(error);
                }
            }
            assert_eq!(work.live_allocation_bytes(), 0);
        }
        assert!(failures > 0);
        assert!(successes > 0);
    }

    #[test]
    fn native_closed_state_quota_denies_before_typed_parse_or_transition_vector() {
        let machine = PiTaskMachine::new();
        let state = json!({"state":"preparing","command":preparation()});
        let probe = work();
        let encoded = crate::pi_root_provider::guarded_pi_bytes(
            &MetadataValue(&state, MetadataRole::State), NATIVE_METADATA_MAXIMUM, &probe,
        ).unwrap();
        let typed_quota = native_metadata_quota::<PiTaskState>(
            encoded.0.len() as u64, probe.receipt().items_examined,
        ).unwrap();
        let header_allocations = probe.receipt().allocation_operations;
        let mut budget = crate::pi_root_provider::allocation_budget();
        budget.peak_allocation_bytes = encoded.1.bytes() + typed_quota - 1;
        drop(encoded.0);
        drop(encoded.1);
        let work = NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap();
        let failure = machine.transition_with_native_work(&state, Value::Null, &work).err().unwrap();
        assert!(matches!(&failure, acyclic_harness::Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::BudgetExceeded { .. }), ..
        }));
        // Only the separately observed canonical header Vec was built: opaque
        // closed-state admission denies before serde constructs typed metadata,
        // before the shared engine, and before its one-command output Vec.
        assert_eq!(work.receipt().allocation_operations, header_allocations);
        drop(failure);
        assert_eq!(work.live_allocation_bytes(), 0);
        assert_eq!(state["state"], "preparing");
    }

    #[tokio::test]
    async fn native_original_filesystem_journal_commits_large_reply_and_replays_exact_identity() {
        let fixture = OriginalJournalFixture::new().await;
        let original_admission = fixture.runtime.runtime.task_host()
            .observe_admission(fixture.binding.task_id).await.unwrap();
        let original = work();
        let mut session = fixture.session(&original).await.unwrap();
        let journal = fixture.journal(&original).await;
        assert!(journal.supports_native_work());
        fixture.hosting(&mut session, &original).await;
        let uploaded = crate::pi_root_provider::stage_pi_input_with_native_work(
            &fixture.owner, &fixture.filesystem, OperationId::from_bytes([153; 16]),
            "native-control", &json!({"inspect":"original source"}), &original,
        ).await.unwrap();
        let command = WorkflowCommand { operation_id: OperationId::from_bytes([154; 16]),
            kind: "acyclic.pi.inspect".to_owned(), payload: uploaded.0.clone() };
        let request = fixture.native_step(&mut session, 153, "native-control-command",
            &json!({"command":command}), &original).await.unwrap();
        assert_eq!(request.0.commands, vec![command.clone()]);
        drop(request.0);
        drop(request.1);
        let (input, custody) = crate::pi_root_provider::guarded_pi_value(&json!({
            "commands":[{"operation_id":command.operation_id,"value":"q".repeat(128 * 1024)}]
        }), fixture.owner.policy().maximum_payload_bytes, &original).unwrap();
        let pointer = input["commands"][0]["value"].as_str().unwrap().as_ptr();
        let returned = session.step_with_native_work(OperationId::from_bytes([155; 16]),
            IdempotencyKey::new("native-large-original-reply").unwrap(), input, custody).await.unwrap();
        assert_eq!(returned.0.state["reply"]["value"].as_str().unwrap().as_ptr(), pointer);
        assert_eq!(session.checkpoint().revision, 4);
        assert_eq!(returned.1.work().receipt(), original.receipt());
        let published = fixture.original_pointer(3).await;
        let replay = fixture.replay(&journal, &original).await;
        assert_eq!(replay.0.len(), 4);
        assert_eq!(replay.0[3].transition, returned.0);
        assert_eq!(replay.0[3].next, *session.checkpoint());
        assert_eq!(replay.1.work().receipt(), original.receipt());
        assert!(replay.1.shares_ledger(&returned.1));
        let (reply, mut metadata) = control_reply_owned(returned.0.state, &original).unwrap();
        let mut returned_custody = returned.1;
        returned_custody.absorb(&mut metadata).unwrap();
        let reply = reply.unwrap();
        assert_eq!(reply.value.as_str().unwrap().as_ptr(), pointer);
        drop(reply);
        drop(returned.0.commands);
        drop(returned.0.status);
        drop(returned_custody);
        drop(replay.0);
        drop(replay.1);
        let retried = fixture.native_step(&mut session, 155, "native-large-original-reply",
            &json!({"commands":[{"operation_id":command.operation_id,"value":"q".repeat(128 * 1024)}]}),
            &original).await.unwrap();
        assert_eq!(retried.0.state, session.checkpoint().state);
        assert_eq!(fixture.original_pointer(3).await, published);
        assert_eq!(fixture.stream.bounds(&fixture.journal_path()).await.unwrap().tail, 4);
        assert_eq!(fixture.runtime.runtime.task_host().observe_admission(fixture.binding.task_id)
            .await.unwrap(), original_admission);
        assert!(original.receipt().peak_allocation_bytes > 128 * 1024);
        drop(retried.0);
        drop(retried.1);
        drop(uploaded.0);
        drop(uploaded.1);
        drop(session);
        drop(journal);
        assert_eq!(original.live_allocation_bytes(), 0);
    }

    #[tokio::test]
    async fn native_original_journal_quota_and_cancellation_reject_before_backend_io() {
        let fixture = OriginalJournalFixture::new().await;
        for cancel in [false, true] {
            let cancellation = CancellationToken::new();
            let mut budget = crate::pi_root_provider::allocation_budget();
            if !cancel { budget.peak_allocation_bytes = 1; }
            let original = NativeMaterializationWork::new(budget, &cancellation).unwrap();
            // A denied original constructor is a real prefix failure, not
            // permission to substitute an ordinary-open session/new meter.
            let mut session = if cancel {
                Some(fixture.session(&original).await.unwrap())
            } else {
                None
            };
            let initial = session.as_ref().map(|session| session.checkpoint().clone());
            fixture.provider.forbid_writes.store(true, Ordering::SeqCst);
            let reads = fixture.provider.observation_reads.load(Ordering::SeqCst);
            let writes = fixture.provider.observation_writes.load(Ordering::SeqCst);
            let before = original.receipt();
            let failure = if let Some(session) = session.as_mut() {
                let custody = original.retain_owned(0).unwrap(); // Null owns no payload.
                cancellation.cancel();
                session.step_with_native_work(OperationId::from_bytes([156; 16]),
                    IdempotencyKey::new("native-reject-before-journal-io").unwrap(), Value::Null, custody)
                    .await.err().unwrap()
            } else {
                fixture.session(&original).await.err().unwrap()
            };
            assert!(matches!(&failure, acyclic_harness::Error::NativeWork { .. }));
            if let Some(session) = session.as_ref() {
                assert_eq!(Some(session.checkpoint()), initial.as_ref());
            }
            assert_eq!(original.receipt().backend_read_operations, before.backend_read_operations);
            assert_eq!(original.receipt().backend_write_operations, before.backend_write_operations);
            assert_eq!(fixture.provider.observation_reads.load(Ordering::SeqCst), reads);
            assert_eq!(fixture.provider.observation_writes.load(Ordering::SeqCst), writes);
            fixture.provider.forbid_writes.store(false, Ordering::SeqCst);
            drop(failure);
            drop(session);
            assert_eq!(original.live_allocation_bytes(), 0);
        }
        let observation = work();
        let session = fixture.session(&observation).await.unwrap();
        assert_eq!(session.checkpoint().revision, 0);
        drop(session);
        drop(fixture);
        assert_eq!(observation.live_allocation_bytes(), 0);
    }

    #[tokio::test]
    async fn native_original_journal_stale_revision_and_atomic_tail_race_accept_one_step() {
        let fixture = OriginalJournalFixture::new().await;
        let original = work();
        let mut first = fixture.session(&original).await.unwrap();
        let mut second = fixture.session(&original).await.unwrap();
        let (left, right) = tokio::join!(
            fixture.native_step(&mut first, 157, "native-tail-race-left", &Value::Null, &original),
            fixture.native_step(&mut second, 158, "native-tail-race-right", &Value::Null, &original),
        );
        assert_ne!(left.is_ok(), right.is_ok());
        let (committed, rejected, stale) = if left.is_ok() {
            (left, right, &mut second)
        } else {
            (right, left, &mut first)
        };
        let rejected = rejected.err().unwrap();
        assert_eq!(rejected.code(), acyclic_harness::wire::ErrorCode::Conflict);
        let committed = committed.unwrap();
        assert_eq!(committed.0.commands, vec![fixture.command.clone()]);
        assert_eq!(stale.checkpoint().revision, 0);
        let failure = fixture.native_step(stale, 159, "native-stale-original-revision",
            &Value::Null, &original).await.err().unwrap();
        assert_eq!(failure.code(), acyclic_harness::wire::ErrorCode::Conflict);
        assert_eq!(stale.checkpoint().revision, 0);
        let journal = fixture.journal(&original).await;
        let replay = fixture.replay(&journal, &original).await;
        assert_eq!(replay.0.len(), 1);
        assert_eq!(replay.0[0].prior.revision, 0);
        assert_eq!(replay.0[0].next.revision, 1);
        assert_eq!(replay.0[0].transition, committed.0);
        assert_eq!(fixture.stream.bounds(&fixture.journal_path()).await.unwrap().tail, 1);
        drop(failure);
        drop(rejected);
        drop(committed.0);
        drop(committed.1);
        drop(replay.0);
        drop(replay.1);
        drop(first);
        drop(second);
        drop(journal);
        assert_eq!(original.live_allocation_bytes(), 0);
    }

    #[tokio::test]
    async fn native_original_journal_lost_ack_and_rebuild_retry_retain_generation_and_outcome() {
        let fixture = OriginalJournalFixture::new().await;
        let original = work();
        let mut session = fixture.session(&original).await.unwrap();
        fixture.provider.commit_lose_ack.store(true, Ordering::SeqCst);
        let accepted = fixture.native_step(&mut session, 160, "native-original-lost-ack",
            &Value::Null, &original).await.unwrap();
        assert!(!fixture.provider.commit_lose_ack.load(Ordering::SeqCst));
        assert_eq!(accepted.0.commands, vec![fixture.command.clone()]);
        let pointer = fixture.original_pointer(0).await;
        let expected = accepted.0;
        drop(session);
        let mut recovered = fixture.session(&original).await.unwrap();
        let exact = fixture.native_step(&mut recovered, 160, "native-original-lost-ack",
            &Value::Null, &original).await.unwrap();
        assert_eq!(exact.0, expected);
        assert_eq!(recovered.checkpoint().revision, 1);
        assert_eq!(fixture.original_pointer(0).await, pointer);
        assert_eq!(fixture.stream.bounds(&fixture.journal_path()).await.unwrap().tail, 1);
        let journal = fixture.journal(&original).await;
        let replay = fixture.replay(&journal, &original).await;
        assert_eq!(replay.0[0].transition, expected);
        assert_eq!(replay.0[0].operation_id, OperationId::from_bytes([160; 16]));
        assert_eq!(replay.1.work().receipt(), original.receipt());
        drop(expected);
        drop(accepted.1);
        drop(exact.0);
        drop(exact.1);
        drop(replay.0);
        drop(replay.1);
        drop(recovered);
        drop(journal);
        assert_eq!(original.live_allocation_bytes(), 0);
    }

    #[tokio::test]
    async fn native_original_journal_replaced_fence_and_cancelled_owner_reject_exact_retry() {
        let mut fixture = OriginalJournalFixture::new().await;
        let original = work();
        let mut original_session = fixture.session(&original).await.unwrap();
        fixture.hosting(&mut original_session, &original).await;
        let mut wrong_fence = fixture.fence.clone();
        wrong_fence.reservation_id.push_str("-foreign");
        assert!(fixture.runtime.runtime.workflow_journal_with_native_work(
            fixture.binding.task_id, &wrong_fence, &original,
        ).await.is_err());
        let prior_fence = fixture.fence.clone();
        fixture.runtime.runtime.task_host().suspend_workflow(fixture.binding.task_id,
            prior_fence.clone(), 2).await.unwrap();
        let resume = crate::pi_root_provider::stage_pi_input_with_native_work(
            &fixture.owner, &fixture.filesystem, OperationId::from_bytes([161; 16]),
            "native-original-resume", &Value::Null, &original,
        ).await.unwrap();
        fixture.runtime.runtime.task_host().resume_workflow(fixture.binding.task_id, 2,
            resume.0.clone(), IdempotencyKey::new("native-original-resume").unwrap()).await.unwrap();
        let WorkPull::Claimed(replacement) = fixture.runtime.runtime.task_host().pull_operation(
            &Worker { id: "pi-native-replacement".to_owned(), available: ResourceSnapshot::default(),
                labels: BTreeMap::new() }, OperationId::from_bytes(fixture.binding.task_id.into_bytes()),
        ).await.unwrap() else { panic!("actual suspended root must reclaim its original task"); };
        fixture.runtime.runtime.task_host().start_task(&replacement).await.unwrap();
        fixture.fence = LeaseFence::from(&replacement.reservation);
        assert_ne!(fixture.fence, prior_fence);
        let stale = fixture.native_step(&mut original_session, 151, "native-root-preparing",
            &Value::Null, &original).await.err().unwrap();
        assert_eq!(original_session.checkpoint().revision, 2);
        assert_eq!(stale.code(), acyclic_harness::wire::ErrorCode::Unauthorized);
        let mut current = fixture.session(&original).await.unwrap();
        let checkpoint = current.checkpoint().clone();
        fixture.runtime.runtime.task_host().cancel(fixture.binding.task_id).await.unwrap();
        let cancelled = fixture.native_step(&mut current, 151, "native-root-preparing",
            &Value::Null, &original).await.err().unwrap();
        assert_eq!(current.checkpoint(), &checkpoint);
        assert_eq!(fixture.stream.bounds(&fixture.journal_path()).await.unwrap().tail, 2);
        assert!(fixture.runtime.runtime.task_host().outcome(fixture.binding.task_id).await.unwrap().is_none(),
            "a cancellation request does not fabricate terminal settlement");
        drop(stale);
        drop(cancelled);
        drop(current);
        drop(original_session);
        drop(resume.0);
        drop(resume.1);
        assert_eq!(original.live_allocation_bytes(), 0);
    }

    #[tokio::test]
    async fn native_original_journal_seeded_adversarial_inputs_preserve_retained_control_model() {
        let fixture = OriginalJournalFixture::new().await;
        let original = work();
        let mut session = fixture.session(&original).await.unwrap();
        fixture.hosting(&mut session, &original).await;
        let uploaded = crate::pi_root_provider::stage_pi_input_with_native_work(
            &fixture.owner, &fixture.filesystem, OperationId::from_bytes([162; 16]),
            "native-fuzz-control", &json!({"inspect":"original"}), &original,
        ).await.unwrap();
        let command = WorkflowCommand { operation_id: OperationId::from_bytes([163; 16]),
            kind: "acyclic.pi.inspect".to_owned(), payload: uploaded.0.clone() };
        let sent = fixture.native_step(&mut session, 162, "native-fuzz-control-command",
            &json!({"command":command}), &original).await.unwrap();
        assert_eq!(sent.0.commands, vec![command.clone()]);
        drop(sent.0);
        drop(sent.1);
        let checkpoint = session.checkpoint().clone();
        let published = fixture.original_pointer(2).await;
        let machine = PiTaskMachine::new();
        let mut seed = 0x91_f00d_u64;
        for index in 0..8u8 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let valid = json!({"operation_id":command.operation_id,"value":{"seed":seed}});
            let input = match index % 4 {
                0 => json!({"commands":[valid.clone(),valid]}),
                1 => json!({"commands":[{"operation_id":OperationId::from_bytes([99;16]),"value":seed}]}),
                2 => json!({"commands":[valid],"unknown":seed}),
                _ => json!({"commands":[{"operation_id":command.operation_id,"value":seed,"unknown":true}]}),
            };
            assert!(machine.transition(&checkpoint.state, &input).is_err());
            let failure = fixture.native_step(&mut session, 170 + index,
                &format!("native-original-fuzz-{index}"), &input, &original).await.err().unwrap();
            assert_eq!(failure.code(), acyclic_harness::wire::ErrorCode::Invalid);
            assert_eq!(session.checkpoint(), &checkpoint);
            assert_eq!(fixture.original_pointer(2).await, published);
            assert_eq!(fixture.stream.bounds(&fixture.journal_path()).await.unwrap().tail, 3);
            drop(failure);
        }
        let accepted = fixture.native_step(&mut session, 178, "native-original-fuzz-settlement",
            &json!({"commands":[{"operation_id":command.operation_id,"value":{"seed":seed}}]}),
            &original).await.unwrap();
        assert!(accepted.0.commands.is_empty());
        assert_eq!(accepted.0.state["reply"]["operation_id"], serde_json::to_value(command.operation_id).unwrap());
        assert_eq!(accepted.0.state["allocation"], checkpoint.state["allocation"]);
        assert_eq!(accepted.1.work().receipt(), original.receipt());
        assert_eq!(session.checkpoint().revision, 4);
        let journal = fixture.journal(&original).await;
        let replay = fixture.replay(&journal, &original).await;
        assert_eq!(replay.0.len(), 4);
        assert_eq!(replay.0[3].transition, accepted.0);
        assert_eq!(replay.0[2].transition.commands, vec![command]);
        drop(accepted.0);
        drop(accepted.1);
        drop(replay.0);
        drop(replay.1);
        drop(uploaded.0);
        drop(uploaded.1);
        drop(session);
        drop(journal);
        assert_eq!(original.live_allocation_bytes(), 0);
    }

    #[tokio::test]
    async fn native_original_journal_deterministic_budget_prefixes_publish_no_partial_checkpoint() {
        let fixture = OriginalJournalFixture::new().await;
        for maximum in [1, 2, 4, 8, 16, 32, 64, 128] {
            let mut budget = crate::pi_root_provider::allocation_budget();
            budget.items_examined = maximum;
            let original = NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap();
            let failure = match fixture.session(&original).await {
                Ok(mut session) => {
                    let checkpoint = session.checkpoint().clone();
                    let custody = original.retain_owned(0).unwrap();
                    let failure = session.step_with_native_work(OperationId::from_bytes([179; 16]),
                        IdempotencyKey::new("native-original-budget-prefix").unwrap(), Value::Null, custody)
                        .await.err().unwrap();
                    assert_eq!(session.checkpoint(), &checkpoint);
                    drop(session);
                    failure
                }
                Err(failure) => failure,
            };
            assert!(matches!(&failure, acyclic_harness::Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::BudgetExceeded { .. }), ..
            }));
            assert!(original.receipt().items_examined <= maximum);
            drop(failure);
            assert_eq!(original.live_allocation_bytes(), 0);
        }
        let original = work();
        let mut session = fixture.session(&original).await.unwrap();
        let accepted = fixture.native_step(&mut session, 179, "native-original-budget-prefix",
            &Value::Null, &original).await.unwrap();
        assert_eq!(accepted.0.commands, vec![fixture.command.clone()]);
        assert_eq!(session.checkpoint().revision, 1);
        assert_eq!(accepted.1.work().receipt(), original.receipt());
        let journal = fixture.journal(&original).await;
        let replay = fixture.replay(&journal, &original).await;
        assert_eq!(replay.0.len(), 1);
        assert_eq!(replay.0[0].transition, accepted.0);
        assert_eq!(fixture.stream.bounds(&fixture.journal_path()).await.unwrap().tail, 1);
        drop(accepted.0);
        drop(accepted.1);
        drop(replay.0);
        drop(replay.1);
        drop(session);
        drop(journal);
        assert_eq!(original.live_allocation_bytes(), 0);
    }

    #[test]
    fn native_initialization_uses_original_ledger_and_matches_both_execution_modes() {
        let machine_work = work();
        let machine = PiTaskMachine::new_with_native_work(&machine_work).unwrap();
        let definition = machine.definition_with_native_work(&machine_work).unwrap();
        assert_eq!(definition.1.work().receipt(), machine_work.receipt());
        let mut registry = MachineRegistry::default();
        let registry_custody = registry.register_with_native_work(machine.clone(), &machine_work).unwrap();
        for execution in [
            PiTaskExecution::Host { root: PiRootIntent::ScratchOnly {}, preparation: preparation() },
            PiTaskExecution::Child { launch: preparation() },
        ] {
            let input = serde_json::to_value(PiTaskInput { binding: binding(), execution }).unwrap();
            let expected = machine.initialize(&input).unwrap();
            let original = work();
            let admitted = crate::pi_root_provider::guarded_pi_value(&input,
                NATIVE_METADATA_MAXIMUM, &original).unwrap();
            let before = original.receipt();
            let initialized = machine.initialize_with_native_work(&admitted.0, &original).unwrap();
            assert_eq!(initialized.0, expected);
            assert!(initialized.1.shares_ledger(&admitted.1));
            assert_eq!(initialized.1.work().receipt(), original.receipt());
            assert!(original.receipt().allocation_operations > before.allocation_operations);
            assert_eq!(original.receipt().backend_read_operations, before.backend_read_operations);
            assert_eq!(original.receipt().backend_write_operations, before.backend_write_operations);
            let registered = registry.initialize_with_native_work(machine.identity(),
                &admitted.0, &original).unwrap();
            assert_eq!(registered.0, expected);
            assert!(registered.1.shares_ledger(&initialized.1));
            let final_wire = crate::pi_root_provider::guarded_pi_bytes(
                &registered.0, NATIVE_METADATA_MAXIMUM, &original,
            ).unwrap();
            assert_eq!(registered.1.work().receipt(), original.receipt());
            drop(final_wire.0);
            drop(final_wire.1);
            drop(admitted.0);
            drop(admitted.1);
            drop(initialized.0);
            drop(initialized.1);
            assert!(original.live_allocation_bytes() >= registered.1.bytes());
            drop(registered.0);
            drop(registered.1);
            assert_eq!(original.live_allocation_bytes(), 0);
        }
        assert!(machine_work.live_allocation_bytes() > 0);
        drop(definition.0);
        drop(definition.1);
        drop(registry);
        drop(registry_custody);
        drop(machine);
        assert_eq!(machine_work.live_allocation_bytes(), 0);
    }

    #[test]
    fn native_initialization_denies_original_quota_cancellation_and_malformed_binding() {
        let machine = PiTaskMachine::new();
        let input = serde_json::to_value(PiTaskInput {
            binding: binding(),
            execution: PiTaskExecution::Host { root: PiRootIntent::ScratchOnly {}, preparation: preparation() },
        }).unwrap();
        let preserved = input.clone();
        let mut budget = crate::pi_root_provider::allocation_budget();
        budget.peak_allocation_bytes = 1;
        let original = NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap();
        let before = original.receipt();
        let denied_machine = PiTaskMachine::new_with_native_work(&original).err().unwrap();
        assert!(matches!(&denied_machine, acyclic_harness::Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::BudgetExceeded { .. }), ..
        }));
        assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
        drop(denied_machine);
        let failure = machine.initialize_with_native_work(&input, &original).err().unwrap();
        assert!(matches!(&failure, acyclic_harness::Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::BudgetExceeded { .. }), ..
        }));
        assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
        drop(failure);
        assert_eq!(original.live_allocation_bytes(), 0);
        assert_eq!(input, preserved);
        let cancellation = CancellationToken::new();
        let original = NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
        let before = original.receipt();
        cancellation.cancel();
        let denied_machine = PiTaskMachine::new_with_native_work(&original).err().unwrap();
        assert!(matches!(&denied_machine, acyclic_harness::Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
        }));
        assert_eq!(original.receipt(), before);
        drop(denied_machine);
        let failure = machine.initialize_with_native_work(&input, &original).err().unwrap();
        assert!(matches!(&failure, acyclic_harness::Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
        }));
        assert_eq!(original.receipt(), before);
        drop(failure);
        assert_eq!(original.live_allocation_bytes(), 0);
        assert_eq!(input, preserved);
        for malformed in [
            json!({"binding":binding(),"execution":{"kind":"host","root":{"kind":"scratch_only"},"preparation":preparation()},"unknown":true}),
            json!({"binding":{},"execution":{"kind":"child","launch":preparation()}}),
            json!({"binding":binding(),"execution":{"kind":"host","root":{"kind":"scratch_only"},"preparation":preparation(),"unknown":true}}),
        ] {
            let original = work();
            let failure = machine.initialize_with_native_work(&malformed, &original).err().unwrap();
            assert_eq!(failure.code(), acyclic_harness::wire::ErrorCode::Invalid);
            drop(failure);
            assert_eq!(original.live_allocation_bytes(), 0);
        }
    }
}
