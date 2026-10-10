use super::*;
use crate::pi_owner::{PiOwnerConfig, PiOwnerPolicy};
use crate::pi_recovery::{
    PiTaskObservation, PiTaskOrigin, observe_pi_session, recover_pi_receipt_owner, recover_pi_task,
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
    let owner = open_owner(root, binding, create).await;
    (owner.runtime, owner.definition)
}

async fn open_owner(root: &Path, binding: &PiTaskBinding, create: bool) -> OwnedRuntime {
    open_owner_with(root, binding, create, |_| {})
        .await
        .unwrap()
}

async fn open_owner_with(
    root: &Path,
    binding: &PiTaskBinding,
    create: bool,
    configure: impl FnOnce(&mut PiRuntimeAuthority),
) -> acyclic_harness::Result<OwnedRuntime> {
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
        let config = PiOwnerConfig::new(PiOwnerPolicy {
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
        })
        .map_err(acyclic_harness::Error::Invalid)?;
        config
            .save_new(root)
            .map_err(acyclic_harness::Error::Invalid)?;
        config
    } else {
        PiOwnerConfig::load(root).map_err(acyclic_harness::Error::Invalid)?
    };
    config
        .validate_root(binding)
        .map_err(acyclic_harness::Error::Invalid)?;
    let mut authority = config
        .authority()
        .map_err(acyclic_harness::Error::Invalid)?;
    configure(&mut authority);
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
    PiTaskRuntime::open(stream, filesystem, journal, authority).await
}

#[tokio::test]
async fn pi_root_missing_or_foreign_credentials_fail_before_storage_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let binding = binding();
    let owner = open_owner(directory.path(), &binding, true).await;
    owner
        .admit_root(binding.clone(), PiRootIntent::ScratchOnly {}, preparation())
        .await
        .unwrap();
    let original = owner
        .runtime
        .task_host()
        .observe_admission(binding.task_id)
        .await
        .unwrap();
    drop(owner);
    let config = PiOwnerConfig::load(directory.path()).unwrap();
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
    config.save_new(directory.path()).unwrap();
    std::fs::rename(&retained, &filesystem).unwrap();
    let owner = open_owner(directory.path(), &binding, false).await;
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
    let owner = open_owner(directory.path(), &binding, true).await;
    // A retained route, even with valid host credentials, cannot admit itself.
    assert!(observe_pi_session(&owner.runtime, &state).await.is_err());
    owner
        .admit_root(binding.clone(), intent.clone(), preparation())
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
                .admit_root(binding.clone(), changed_intent, preparation())
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
    let (owner, observed) = PiTaskRuntime::reopen(
        &PiOwnerConfig::load(directory.path()).unwrap(),
        &state,
        stream,
        filesystem,
    )
    .await
    .unwrap();
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
    let owner = open_owner(directory.path(), &binding, true).await;
    owner
        .admit_root(binding.clone(), PiRootIntent::ScratchOnly {}, preparation())
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
    let (owner, observed) = PiTaskRuntime::reopen(
        &PiOwnerConfig::load(directory.path()).unwrap(),
        &state,
        stream.clone(),
        filesystem.clone(),
    )
    .await
    .unwrap();
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
    assert!(
        PiTaskRuntime::reopen(
            &PiOwnerConfig::load(directory.path()).unwrap(),
            &state,
            stream,
            filesystem
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
    let owner = open_owner(directory.path(), &binding, true).await;
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
            .admit_root(foreign, PiRootIntent::ScratchOnly {}, preparation())
            .await
            .is_err()
    );
    let mut another_root = binding.clone();
    another_root.task_id = TaskId::from_bytes([73; 16]);
    let another_task = another_root.task_id;
    assert!(
        owner
            .admit_root(another_root, PiRootIntent::ScratchOnly {}, preparation())
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
            .admit_root(
                journal_selection,
                PiRootIntent::ScratchOnly {},
                preparation()
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
    assert!(matches!(
        owner
            .admit_root(binding.clone(), PiRootIntent::ScratchOnly {}, preparation())
            .await
            .unwrap(),
        Admission::Accepted(_)
    ));
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
    let owner = open_owner(directory.path(), &binding, false).await;
    assert!(matches!(
        owner
            .admit_root(binding.clone(), PiRootIntent::ScratchOnly {}, preparation())
            .await
            .unwrap(),
        Admission::Accepted(_)
    ));
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
            .admit_root(changed, PiRootIntent::ScratchOnly {}, preparation())
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
    let owner = open_owner(directory.path(), &binding, true).await;
    owner
        .admit_root(binding.clone(), PiRootIntent::ScratchOnly {}, preparation())
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
    let owner = open_owner(directory.path(), &binding, false).await;
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
    let mut observations = observe_pi_session(&runtime, &state).await.unwrap();
    match observations.remove(0) {
        PiTaskObservation::Unresolved { lease, error } => {
            assert_eq!(lease.reservation, original.reservation);
            assert!(!error.is_empty());
        }
        _ => panic!("cancelled attempt was converted into executable or completed work"),
    }
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
        json!({"state":"hosting","allocation":result})
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
