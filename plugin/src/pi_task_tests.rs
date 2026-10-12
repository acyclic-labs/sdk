use super::*;
use acyclic_harness::runtime::TaskStateProvider;
use crate::pi_fork::tests::{policy, work};
use crate::pi_owner::{PiOwnerConfig, PiOwnerPolicy};
use crate::pi_recovery::{
    PiTaskObservation, observe_pi_session,
};
use crate::pi_root::PiRootIntent;
use crate::pi_runtime::PiTaskRuntime;
use crate::{AdapterState, PiScratchBinding, PiTaskBinding, PluginTaskBinding};
use acyclic_harness::{
    Admission, AgentId, Capabilities, TaskId,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    distributed::{WorkPull, Worker},
    filesystem::FilesystemHost,
    resources::ProviderRef,
    runtime::{DurableTaskHost, RuntimeScope},
    scheduler::{LeaseFence, ResourceSnapshot},
};
use acyclic_stream::{LocalStream, LocalStreamLimits};
use std::{collections::BTreeMap, path::Path};

#[path = "pi_task_model_tests.rs"]
mod bounded_model;

type OwnedRuntime =
    PiTaskRuntime<LocalStream>;

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

fn result(operation: OperationId, value: Outcome<FileRef>) -> Value {
    json!({"commands":[{"operation_id":operation,"value":value}]})
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
    let (_lock, configured, private_root, work) = crate::tests::configured_test_service_bootstrap(&data).await.unwrap();
    let Err(_) = crate::ServiceResources::open(data, configured, private_root, &work).await else {
        panic!("task-bound Pi session entered host-managed service recovery");
    };
    let Err(_) = crate::ControlPlane::open(session.clone()).await else {
        panic!("task-bound Pi session entered host-managed control recovery");
    };
    assert!(!inaccessible_root.exists());
    assert_eq!(
        serde_json::to_value(crate::load_saved_state(&session).unwrap()).unwrap(),
        expected
    );
}

async fn storage(root: &Path, provider: &ProviderRef) -> (
    Arc<FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>,
    Arc<LocalStream>,
) {
    let filesystem = Arc::new(FilesystemHost::new(
        acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(root.join("filesystem"))).await.unwrap(),
        provider.clone(),
    ).unwrap());
    let stream = Arc::new(LocalStream::open(root.join("stream"), LocalStreamLimits::default()).await.unwrap());
    (filesystem, stream)
}

async fn open(
    root: &Path,
    binding: &PiTaskBinding,
    create: bool,
) -> acyclic_harness::Result<(OwnedRuntime, acyclic_fs::NativeMaterializationWork)> {
    let work = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new(),
    ).map_err(|(cause, work)| acyclic_harness::Error::NativeConstruction { cause, work })?;
    let private_root = acyclic_fs::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(root, &work)
        .map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
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
            ..policy()
        };
        let config = PiOwnerConfig::new(&policy, None, &work)?;
        config.save_new_with_native_work(&private_root, Path::new(""), &work)?;
        config
    } else {
        PiOwnerConfig::load_with_native_work(&private_root, Path::new(""), &work)?
    };
    config.validate_root(binding, &work)?;
    let authority = config.authority_with_native_work(&work)?;
    let (filesystem, stream) = storage(root, config.policy().journal.provider()).await;
    if create {
        filesystem.create_volume(&config.policy().journal).await.unwrap();
    }
    let runtime = PiTaskRuntime::open_with_native_work(
        stream, filesystem, &journal, authority, &work,
    ).await?;
    Ok((runtime, work))
}

#[tokio::test]
async fn pi_root_missing_or_foreign_credentials_fail_before_storage_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let binding = binding();
    let (owner, original_work) = open(directory.path(), &binding, true).await.unwrap();
    owner
        .admit_root(&binding, &PiRootIntent::ScratchOnly {}, &preparation(), &original_work)
        .await
        .unwrap();
    let original = owner
        .runtime
        .task_host()
        .observe_admission(binding.task_id)
        .await
        .unwrap();
    drop(owner);
    let config = PiOwnerConfig::load_with_native_work(
        &acyclic_fs::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(directory.path(), &original_work).unwrap(),
        Path::new(""), &original_work).unwrap();
    let filesystem = directory.path().join("filesystem");
    let retained = directory.path().join("retained-filesystem");
    std::fs::rename(&filesystem, &retained).unwrap();
    let mut foreign = binding.clone();
    foreign.task_id = TaskId::from_bytes([74; 16]);
    assert!(
        open(directory.path(), &foreign, false)
            .await
            .is_err()
    );
    assert!(!filesystem.exists());
    std::fs::remove_file(directory.path().join("pi-owner.v1")).unwrap();
    assert!(
        open(directory.path(), &binding, false)
            .await
            .is_err()
    );
    assert!(!filesystem.exists());
    assert!(!directory.path().join("pi-owner.v1").exists());
    config.save_new_with_native_work(
        &acyclic_fs::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(directory.path(), &original_work).unwrap(),
        Path::new(""), &original_work).unwrap();
    std::fs::rename(&retained, &filesystem).unwrap();
    let (owner, _original_work) = open(directory.path(), &binding, false).await.unwrap();
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
    let (owner, original_work) = open(directory.path(), &binding, true).await.unwrap();
    // A retained route, even with valid host credentials, cannot admit itself.
    assert!(observe_pi_session(&owner.runtime, &state, &original_work).await.is_err());
    owner
        .admit_root(&binding, &intent, &preparation(), &original_work)
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
        matches!(observe_pi_session(&owner.runtime, &state, &original_work).await.unwrap().0.as_slice(),
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
        assert!(observe_pi_session(&owner.runtime, &changed, &original_work).await.is_err());
        let changed_intent = PiRootIntent::from_state(&changed).unwrap();
        assert!(
            owner
                .admit_root(&binding, &changed_intent, &preparation(), &original_work)
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
    let (filesystem, stream) = storage(directory.path(), binding.scratch.volume.provider()).await;
    let reopen_work = crate::pi_fork::tests::work();
    let reopened = PiTaskRuntime::reopen(
        &PiOwnerConfig::load_with_native_work(
        &acyclic_fs::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(directory.path(), &reopen_work).unwrap(),
        Path::new(""), &reopen_work).unwrap(),
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
fn pi_root_intent_rejects_unselected_or_unbounded_paths_and_invalid_identity() {
    let mut binding = binding();
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
    assert!(PiRootIntent::ScratchOnly {}.validate(&binding, &crate::pi_fork::tests::work()).is_err());
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
    let (owner, original_work) = open(directory.path(), &binding, true).await.unwrap();
    owner
        .admit_root(&binding, &PiRootIntent::ScratchOnly {}, &preparation(), &original_work)
        .await
        .unwrap();
    let original = owner
        .runtime
        .task_host()
        .observe_admission(binding.task_id)
        .await
        .unwrap();
    drop(owner);
    let (filesystem, stream) = storage(directory.path(), binding.scratch.volume.provider()).await;
    let mut state = session_state(&binding);
    let reopen_work = crate::pi_fork::tests::work();
    let reopened = PiTaskRuntime::reopen(
        &PiOwnerConfig::load_with_native_work(
        &acyclic_fs::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(directory.path(), &reopen_work).unwrap(),
        Path::new(""), &reopen_work).unwrap(),
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
    let foreign_reopen_work = crate::pi_fork::tests::work();
    assert!(
        PiTaskRuntime::reopen(
            &PiOwnerConfig::load_with_native_work(
        &acyclic_fs::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(directory.path(), &foreign_reopen_work).unwrap(),
        Path::new(""), &foreign_reopen_work).unwrap(),
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
    let (owner, original_work) = open(directory.path(), &binding, true).await.unwrap();
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
            .admit_root(&foreign, &PiRootIntent::ScratchOnly {}, &preparation(), &original_work)
            .await
            .is_err()
    );
    let mut another_root = binding.clone();
    another_root.task_id = TaskId::from_bytes([73; 16]);
    let another_task = another_root.task_id;
    assert!(
        owner
            .admit_root(&another_root, &PiRootIntent::ScratchOnly {}, &preparation(), &original_work)
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
            .admit_root(&journal_selection, &PiRootIntent::ScratchOnly {}, &preparation(), &original_work)
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
    let accepted = owner.admit_root(
        &binding, &PiRootIntent::ScratchOnly {}, &preparation(), &original_work,
    ).await.unwrap();
    assert!(matches!(accepted.0, Admission::Accepted(_)));
    drop(accepted);
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
            .open_workspace_with_native_work(&binding.scratch.volume.storage_name().unwrap(), &crate::pi_fork::tests::work(), &acyclic_fs::CancellationToken::new())
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
    let (owner, original_work) = open(directory.path(), &binding, false).await.unwrap();
    let accepted = owner.admit_root(&binding, &PiRootIntent::ScratchOnly {}, &preparation(), &original_work).await.unwrap();
    assert!(matches!(accepted.0, Admission::Accepted(_)));
    drop(accepted);
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
            .admit_root(&changed, &PiRootIntent::ScratchOnly {}, &preparation(), &original_work)
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
    let (owner, original_work) = open(directory.path(), &binding, true).await.unwrap();
    owner
        .admit_root(&binding, &PiRootIntent::ScratchOnly {}, &preparation(), &original_work)
        .await
        .unwrap();
    let original = owner
        .runtime
        .task_host()
        .observe_admission(binding.task_id)
        .await
        .unwrap();
    drop(owner);
    let preparation_work = work();
    let private_root = acyclic_fs::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(
        directory.path(), &preparation_work).unwrap();
    let config = PiOwnerConfig::load_with_native_work(&private_root, Path::new(""), &preparation_work).unwrap();
    let (filesystem, stream) = storage(directory.path(), config.policy().journal.provider()).await;
    for fault in [
        "key",
        "widening",
        "journal",
        "payload",
        "concurrency",
        "agent",
        "audience",
    ] {
        let work = work();
        let mut issued = config.authority_with_native_work(&work).unwrap();
        let authority = &mut issued.0;
        match fault {
                "key" => {
                    authority.verifier = AuthorityIssuer::new(
                        "pi-recovery-test",
                        [72; 32],
                        Authority {
                            kind: AggregateKind::Task,
                            id: binding.task_id.to_string(),
                        },
                    )
                    .verifier(&mut None).unwrap();
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
                    authority.verifier = issuer.verifier(&mut None).unwrap();
                    authority.signed =
                        issuer.issue("pi-owner", authority.signed.capabilities(), None, None, &mut None).expect("scope proof");
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
                    authority.verifier = issuer.verifier(&mut None).unwrap();
                    authority.signed = issuer.issue("pi-owner", authority.signed.capabilities(), Some(binding.agent_id), None, &mut None).expect("scope proof");
                }
                _ => unreachable!(),
        }
        let rejected = PiTaskRuntime::open_with_native_work(stream.clone(), filesystem.clone(),
            &config.policy().journal, issued, &work).await;
        assert!(rejected.is_err(), "accepted {fault}");
    }
    let (owner, _original_work) = open(directory.path(), &binding, false).await.unwrap();
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
async fn pi_pending_preparation_unknown_admission_fails_before_future_native_child_is_admitted() {
    let directory = tempfile::tempdir().unwrap();
    let root = binding();
    let (owner, _work) = open(directory.path(), &root, true).await.unwrap();
    let (runtime, definition) = (owner.runtime, owner.definition);
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
    crate::validate_state_bounds(&state).unwrap();
    let query = crate::pi_fork::tests::work();
    assert!(observe_pi_session(&runtime, &state, &query).await.is_err());
    assert!(runtime.task_host().reconcile_admission(
        OperationId::from_bytes(child.task_id.into_bytes())
    ).await.unwrap().is_none());
    assert!(!state.pending[0].mount_path.exists());
    state.pending[0].parent_agent_id = "unretained-parent".to_owned();
    let error = observe_pi_session(&runtime, &state, &query).await.err().unwrap();
    assert!(matches!(error, acyclic_harness::Error::Invalid(_)));
}

fn actual_preparing_session(
    fixture: &crate::pi_fork::tests::Fixture,
    plan: &crate::pi_fork::PiForkPlan,
) -> AdapterState {
    let PiRootIntent::Project { root_id, paths, .. } = &fixture.root_request.root else { unreachable!() };
    let mut state = fixture.state();
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
            &state.root_session_id, plan.commit.child.task_id, paths.keys().next().map(String::as_str), &crate::pi_fork::tests::work())
            .unwrap().0.view_root,
        lifecycle: crate::PendingSpawnLifecycle::Preparing,
    });
    crate::validate_state_bounds(&state).unwrap();
    state
}

#[tokio::test]
async fn pi_real_preparation_pending_reserved_and_succeeded_never_report_future_native_child_success() {
    use crate::pi_recovery::PiPreparationPhase;
    let fixture = crate::pi_fork::tests::Fixture::new(None).await;
    let plan = fixture.plan(&crate::pi_fork::tests::work()).await;
    let state = actual_preparing_session(&fixture, &plan);
    let host = fixture.runtime.runtime.task_host();
    host.admit_owned(plan.preparation.clone(), fixture.fence.clone()).await.unwrap();
    let query = crate::pi_fork::tests::work();
    let query_baseline = query.live_allocation_bytes();
    let observed = observe_pi_session(&fixture.runtime.runtime, &state, &query).await.unwrap();
    assert!(matches!(&observed.0[1], PiTaskObservation::Preparation {
        task_id, preparation, phase: PiPreparationPhase::Pending,
    } if *task_id == plan.commit.child.task_id && *preparation == plan.preparation.task_id()));
    drop(observed);
    assert_eq!(query.live_allocation_bytes(), query_baseline);
    let partial = fixture.runtime.runtime.run_operation_with_native_work(&Worker {
        id: "pi-preparing-observation".into(), available: ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    }, plan.preparation.task_id(), &fixture.runtime.runtime.commands(), 1, &query).await.unwrap();
    assert!(matches!(partial.0, Some( acyclic_harness::filesystem::TaskWorkerAttempt::Progress(
        acyclic_harness::filesystem::TaskWorkerOutcome::Yielded { .. }))));
    drop(partial);
    let observed = observe_pi_session(&fixture.runtime.runtime, &state, &query).await.unwrap();
    assert!(matches!(&observed.0[1], PiTaskObservation::Preparation {
        phase: PiPreparationPhase::Reserved, ..
    }));
    drop(observed);
    assert_eq!(query.live_allocation_bytes(), query_baseline);
    let report = fixture.prepare_with(&fixture.coordinator(&crate::pi_fork::tests::work()).unwrap(), &fixture.context, &fixture.journal,
        &fixture.fence, &plan, fixture.root_handle.workspace.clone(),
        crate::pi_root_provider::allocation_budget(), acyclic_fs::CancellationToken::new())
        .await.unwrap().unwrap();
    report.0.validate().unwrap();
    assert!(host.observe_admission(plan.request.child.task_id()).await.is_err());
    let reopen_work = crate::pi_fork::tests::work();
    let reopened = OwnedRuntime::open_with_native_work(
        fixture.stream.clone(), fixture.filesystem.clone(), &fixture.owner.policy().journal,
        fixture.owner.authority_with_native_work(&reopen_work).unwrap(), &reopen_work,
    ).await.unwrap();
    for _ in 0..2 {
        let observed = observe_pi_session(&reopened.runtime, &state, &query).await.unwrap();
        assert!(matches!(&observed.0[1], PiTaskObservation::Preparation {
            task_id, preparation, phase: PiPreparationPhase::Succeeded,
        } if *task_id == plan.commit.child.task_id && *preparation == plan.preparation.task_id()));
        drop(observed);
        assert_eq!(query.live_allocation_bytes(), query_baseline);
        assert!(host.observe_admission(plan.commit.child.task_id).await.is_err());
        assert!(state.routes.is_empty());
        assert!(matches!(state.pending[0].lifecycle, crate::PendingSpawnLifecycle::Preparing));
    }
}

#[tokio::test]
async fn pi_real_preparation_changed_planned_binding_parent_and_foreign_key_fail_before_child_admission() {
    let fixture = crate::pi_fork::tests::Fixture::new(None).await;
    let plan = fixture.plan(&crate::pi_fork::tests::work()).await;
    let state = actual_preparing_session(&fixture, &plan);
    fixture.runtime.runtime.task_host().admit_owned(
        plan.preparation.clone(), fixture.fence.clone()).await.unwrap();
    let query = crate::pi_fork::tests::work();
    let query_baseline = query.live_allocation_bytes();
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
        assert!(observe_pi_session(&fixture.runtime.runtime, &changed, &query).await.is_err());
        assert_eq!(query.live_allocation_bytes(), query_baseline);
        assert!(fixture.runtime.runtime.task_host().observe_admission(
            plan.request.child.task_id()).await.is_err());
    }
    let mut stale = fixture.fence.clone();
    stale.reservation_id = "stale-original-parent-fence".into();
    assert!(fixture.prepare_with(&fixture.coordinator(&crate::pi_fork::tests::work()).unwrap(), &fixture.context, &fixture.journal, &stale,
        &plan, fixture.root_handle.workspace.clone(), crate::pi_root_provider::allocation_budget(),
        acyclic_fs::CancellationToken::new()).await.is_err());
    assert!(fixture.runtime.runtime.task_host().observe_admission(
        plan.request.child.task_id()).await.is_err());
}

#[tokio::test]
async fn pi_real_root_committed_control_reopen_keeps_original_context_and_next_allocation() {
    let fixture = crate::pi_fork::tests::Fixture::new(None).await;
    let task = fixture.owner.policy().root_task;
    let host = fixture.runtime.runtime.task_host();
    let original = work();
    let recovery = host.recover_work_with_native_work(task, &original).await.unwrap();
    let WorkPull::Claimed(lease) = recovery.0 else {
        panic!("fixture retains original root control owner");
    };
    let admission = host.observe_admission(task).await.unwrap();
    let settled = fixture.runtime.runtime.resume_task_with_native_work(
        lease, recovery.1, &fixture.runtime.runtime.commands(), 8, &original).await.unwrap();
    assert!(matches!(settled.0, acyclic_harness::filesystem::TaskWorkerAttempt::Progress(
        acyclic_harness::filesystem::TaskWorkerOutcome::Suspended { .. })));
    let committed = fixture.runtime.runtime.observe_workflow_checkpoint(task).await.unwrap();
    let allocation: FileRef = serde_json::from_value(committed.state["allocation"].clone()).unwrap();
    let (reply, _reply_custody) = control_reply(committed.state.clone(), &original).unwrap();
    let reply = reply.unwrap();
    let accounting = acyclic_harness::distributed::read_coordinator_event_page(
        &fixture.stream, 0, 1024).await.unwrap();
    assert!(accounting.len() < 1024, "bounded fixture must fit one complete accounting page");
    let reopen_work = crate::pi_fork::tests::work();
    let reopened = OwnedRuntime::open_with_native_work(
        fixture.stream.clone(), fixture.filesystem.clone(), &fixture.owner.policy().journal,
        fixture.owner.authority_with_native_work(&reopen_work).unwrap(), &reopen_work,
    ).await.unwrap();
    assert_eq!(reopened.runtime.observe_workflow_checkpoint(task).await.unwrap(), committed);
    assert_eq!(reopened.runtime.task_host().observe_admission(task).await.unwrap(), admission);
    assert!(matches!(reopened.runtime.task_host().recover_work(task).await.unwrap(), WorkPull::Idle));
    let machine = PiTaskMachine::new(&work()).unwrap();
    assert!(machine.transition_with_native_work(&committed.state, json!({"commands":[reply]}), &original).is_err());
    let duplicate = fixture.root_control.clone();
    assert_eq!(reply.operation_id, duplicate.operation_id);
    assert!(machine.transition_with_native_work(&committed.state, json!({"command":duplicate}), &original).is_err());
    assert_eq!(acyclic_harness::distributed::read_coordinator_event_page(
        &fixture.stream, 0, 1024).await.unwrap(), accounting);
    let operation = OperationId::from_bytes([126; 16]);
    let (payload, _payload_custody) = crate::pi_root_provider::stage_pi_input(
        &fixture.owner, &fixture.filesystem, operation, "next-timer", &json!({"deadline_unix_ms":0}),
        &reopen_work,
    ).await.unwrap();
    let input = crate::pi_task::PiControlInput { command: WorkflowCommand {
        operation_id: operation, kind: "acyclic.timer.v1".into(), payload,
    }};
    let (input, _input_custody) = crate::pi_root_provider::stage_pi_input(
        &fixture.owner, &fixture.filesystem, operation, "next-control", &input,
        &reopen_work,
    ).await.unwrap();
    let suspension = reopened.runtime.task_host().workflow_suspension(task).await.unwrap().unwrap();
    reopened.runtime.task_host().resume_workflow(task, suspension.revision, input, acyclic_harness::IdempotencyKey::new("pi-root-control/distinct-next").unwrap(), None).await.unwrap();
    let next = reopened.runtime.run_operation_with_native_work(&Worker {
        id: "pi-root-control-next".into(), available: ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    }, task, &reopened.runtime.commands(), 8, &reopen_work).await.unwrap();
    assert!(matches!(next.0, Some(acyclic_harness::filesystem::TaskWorkerAttempt::Progress(
        acyclic_harness::filesystem::TaskWorkerOutcome::Suspended { .. }))));
    let next = reopened.runtime.observe_workflow_checkpoint(task).await.unwrap();
    assert_eq!(next.state["allocation"], serde_json::to_value(allocation).unwrap());
    let (reply, _next_reply_custody) = control_reply(next.state, &reopen_work).unwrap();
    assert_eq!(reply.unwrap().operation_id, operation);
    assert_eq!(reopened.runtime.task_host().observe_admission(task).await.unwrap(), admission);
    let accounting = acyclic_harness::distributed::read_coordinator_event_page(
        &fixture.stream, 0, 1024).await.unwrap();
    assert!(accounting.len() < 1024);
    assert!(!accounting.iter().any(|record| matches!(
        record, acyclic_harness::scheduler::SchedulerEvent::ModelDispatchClaimed { .. }
    )), "passive native root controls must not charge harness model attempts");
}

#[tokio::test]
async fn pi_operator_control_kind_does_not_authorize_invalid_original_command_fingerprint() {
    use crate::pi_operator::{PiOperatorBody, PiOperatorRequest};
    let fixture = crate::pi_fork::tests::Fixture::new(None).await;
    let work = crate::pi_fork::tests::work();
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
    let machine = PiTaskMachine::new(&work()).unwrap();
    let hosting = json!({"state":"hosting","allocation":fixture.root_control.payload});
    let command = WorkflowCommand {
        operation_id: OperationId::from_bytes([127; 16]),
        kind: "acyclic.pi.operator-approval".into(),
        payload: fixture.root_control.payload.clone(),
    };
    assert_eq!(machine.transition_with_native_work(&hosting, json!({"command":command}), &work).unwrap().0.commands,
        vec![command]);
    request.body = PiOperatorBody::Inspect {
        session: session.clone(), operation: "changed-operator-fingerprint".into(),
        project_files: false, file_evidence: None,
    };
    assert!(crate::pi_operator::verify_request(&fixture.owner, session, &request, &work).is_err());
    request.operator = crate::pi_caller::issue(
        &fixture.owner, session, &fixture.root_request.binding, &work,
    ).unwrap().0;
    assert!(crate::pi_operator::verify_request(&fixture.owner, session, &request, &work).is_err());
    drop(request);
    drop(request_custody);
    drop(control);
    drop(control_custody);
}

#[tokio::test]
async fn pi_native_recovery_cancellation_precedes_graph_growth_or_sdk_reads() {
    let directory = tempfile::tempdir().unwrap();
    let binding = binding();
    let (owner, _work) = open(directory.path(), &binding, true).await.unwrap();
    let (runtime, definition) = (owner.runtime, owner.definition);
    runtime.harness().admit(OperationId::from_bytes(binding.task_id.into_bytes()), &definition,
        PiTaskInput { binding: binding.clone(), execution: PiTaskExecution::Host {
            root: PiRootIntent::ScratchOnly {}, preparation: preparation(),
        } }, None).await.unwrap();
    let state = session_state(&binding);
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
    let work_baseline = work.live_allocation_bytes();
    let before = work.receipt();
    cancellation.cancel();
    assert!(matches!(observe_pi_session(&runtime, &state, &work).await,
        Err(acyclic_harness::Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
    assert_eq!(work.receipt(), before);
    assert_eq!(work.live_allocation_bytes(), work_baseline);
    assert!(matches!(runtime.task_host().recover_work(binding.task_id).await.unwrap(), WorkPull::Idle));
}

#[cfg(not(target_arch = "wasm32"))]
mod native_transition {
    use super::*;
    use acyclic_fs::{CancellationToken, NativeAllocation, NativeMaterializationWork};
    use acyclic_harness::{
        IdempotencyKey,
        workflow::WorkflowJournal,
    };

    fn step(
        machine: &PiTaskMachine, state: &Value, input: &Value,
        work: &NativeMaterializationWork,
    ) -> acyclic_harness::Result<(MachineTransition, NativeAllocation)> {
        let state = crate::pi_root_provider::guarded_pi_value(state, 4 * 1024 * 1024, work)?;
        let (input, mut input_custody) = crate::pi_root_provider::guarded_pi_value(input, 4 * 1024 * 1024, work)?;
        let mut transition = machine.transition_with_native_work(&state.0, input, work)?;
        transition.1.absorb(&mut input_custody).unwrap();
        Ok(transition)
    }

    #[test]
    fn native_all_states_preserve_commands_and_settlement() {
        let machine = PiTaskMachine::new(&work()).unwrap();
        assert!(machine.supports_native_work());
        let command = preparation();
        let control = WorkflowCommand {
            operation_id: OperationId::from_bytes([91; 16]),
            kind: "acyclic.pi.inspect".into(), payload: preparation().payload,
        };
        let cases = [
            (json!({"state":"preparing","command":command}), Value::Null,
                json!({"state":"allocating","operation":command.operation_id}), vec![command.clone()], MachineStatus::Suspended),
            (json!({"state":"ready","command":command}), Value::Null,
                json!({"state":"awaiting","operation":command.operation_id}), vec![command.clone()], MachineStatus::Suspended),
            (json!({"state":"allocating","operation":command.operation_id}),
                result(command.operation_id, Outcome::Succeeded(command.payload.clone())),
                json!({"state":"hosting","allocation":command.payload,"reply":null}), vec![], MachineStatus::Suspended),
            (json!({"state":"awaiting","operation":command.operation_id}),
                result(command.operation_id, Outcome::Succeeded(command.payload.clone())),
                json!({"state":"completed","result":command.payload}), vec![],
                MachineStatus::Completed { value: serde_json::to_value(&command.payload).unwrap() }),
            (json!({"state":"awaiting","operation":command.operation_id}),
                result(command.operation_id, Outcome::Failed { message: "known failure".to_owned() }),
                json!({"state":"failed","message":"known failure"}), vec![],
                MachineStatus::Failed { message: "known failure".to_owned() }),
            (json!({"state":"hosting","allocation":command.payload}), Value::Null,
                json!({"state":"hosting","allocation":command.payload,"reply":null}), vec![], MachineStatus::Suspended),
            (json!({"state":"hosting","allocation":command.payload,
                "reply":{"operation_id":command.operation_id,"value":{"retained":[1,2,3]}}}),
                json!({"command":control}),
                json!({"state":"controlling","allocation":command.payload,"operation":control.operation_id}),
                vec![control.clone()], MachineStatus::Suspended),
            (json!({"state":"controlling","allocation":command.payload,"operation":control.operation_id}),
                json!({"commands":[{"operation_id":control.operation_id,"value":{"retained":[1,2,3]}}]}),
                json!({"state":"hosting","allocation":command.payload,
                    "reply":{"operation_id":control.operation_id,"value":{"retained":[1,2,3]}}}), vec![], MachineStatus::Suspended),
        ];
        for (state, input, state_next, commands, status) in cases {
            let work = work();
            let work_baseline = work.live_allocation_bytes();
            let actual = step(&machine, &state, &input, &work).unwrap();
            assert_eq!(actual.0, MachineTransition { state: state_next, commands, status });
            assert!(actual.1.work().shares_ledger(&work));
            drop(actual);
            assert_eq!(work.live_allocation_bytes(), work_baseline);
        }
        for state in [
            json!({"state":"completed","result":command.payload}),
            json!({"state":"failed","message":"known failure"}),
        ] {
            let work = work();
            let work_baseline = work.live_allocation_bytes();
            assert!(step(&machine, &state, &Value::Null, &work).is_err());
            assert_eq!(work.live_allocation_bytes(), work_baseline);
        }
    }

    #[test]
    fn native_cancellation_precedes_state_and_command_vector_growth() {
        let machine = PiTaskMachine::new(&work()).unwrap();
        let state = json!({"state":"preparing","command":preparation()});
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(
            crate::pi_root_provider::allocation_budget(), &cancellation,
        ).unwrap();
        let work_baseline = work.live_allocation_bytes();
        let before = work.receipt();
        cancellation.cancel();
        let failure = machine.transition_with_native_work(&state, Value::Null, &work).err().unwrap();
        assert!(matches!(&failure, acyclic_harness::Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
        }));
        assert_eq!(work.receipt(), before);
        drop(failure);
        assert_eq!(work.live_allocation_bytes(), work_baseline);
    }

    #[tokio::test]
    async fn native_root_reply_survives_concurrent_settlement_and_reopen() {
        let fixture = crate::pi_fork::tests::Fixture::new(None).await;
        let task = fixture.root_request.binding.task_id;
        let admission = fixture.runtime.runtime.task_host().observe_admission(task).await.unwrap();
        let original = work();
        let baseline = original.live_allocation_bytes();
        let context = fixture.runtime.runtime.task_context_with_native_work(task, &fixture.fence, &original).await.unwrap();
        let mut first = fixture.runtime.runtime.open_task_with_native_work(task, &fixture.fence, context.clone(), &original).await.unwrap();
        let mut second = fixture.runtime.runtime.open_task_with_native_work(task, &fixture.fence, context.clone(), &original).await.unwrap();
        let prior = first.checkpoint().clone();
        let input = json!({"commands":[{"operation_id":fixture.root_control.operation_id,"value":"q".repeat(128 * 1024)}]});
        let left = crate::pi_root_provider::guarded_pi_value(&input, fixture.owner.policy().maximum_payload_bytes, &original).unwrap();
        let right = crate::pi_root_provider::guarded_pi_value(&input, fixture.owner.policy().maximum_payload_bytes, &original).unwrap();
        let (left, right) = tokio::join!(
            first.step_with_native_work(OperationId::from_bytes([155; 16]), IdempotencyKey::new("native-large-left").unwrap(), left.0, left.1),
            second.step_with_native_work(OperationId::from_bytes([156; 16]), IdempotencyKey::new("native-large-right").unwrap(), right.0, right.1),
        );
        assert_ne!(left.is_ok(), right.is_ok());
        let (accepted, rejected, committed, stale, operation, key) = if left.is_ok() {
            (left, right, &first, &mut second, 155, "native-large-left")
        } else {
            (right, left, &second, &mut first, 156, "native-large-right")
        };
        let rejected = rejected.err().unwrap();
        assert_eq!(rejected.code(), acyclic_harness::wire::ErrorCode::Conflict);
        let accepted = accepted.unwrap();
        assert_eq!(accepted.0.state["reply"]["value"], json!("q".repeat(128 * 1024)));
        assert_eq!(committed.checkpoint().revision, prior.revision + 1);
        assert_eq!(stale.checkpoint(), &prior);
        let next = crate::pi_root_provider::guarded_pi_value(&input, fixture.owner.policy().maximum_payload_bytes, &original).unwrap();
        let rejected_stale = stale.step_with_native_work(OperationId::from_bytes([157; 16]),
            IdempotencyKey::new("native-large-stale").unwrap(), next.0, next.1).await.err().unwrap();
        assert_eq!(rejected_stale.code(), acyclic_harness::wire::ErrorCode::Conflict);
        assert_eq!(stale.checkpoint(), &prior);
        let journal = fixture.runtime.runtime.workflow_journal_with_native_work(task, &fixture.fence, &original).await.unwrap();
        let replay = journal.replay_with_native_work(0, 16, &original).unwrap().await.unwrap();
        let record = replay.0.last().unwrap();
        assert_eq!(record.transition, accepted.0);
        assert_eq!(record.next, *committed.checkpoint());
        assert_eq!(record.operation_id, OperationId::from_bytes([operation; 16]));
        assert!(replay.1.shares_ledger(&accepted.1));
        let (reply, mut metadata) = control_reply(accepted.0.state, &original).unwrap();
        let mut custody = accepted.1;
        custody.absorb(&mut metadata).unwrap();
        assert_eq!(reply.as_ref().unwrap().value, json!("q".repeat(128 * 1024)));
        drop(reply);
        drop(accepted.0.commands);
        drop(accepted.0.status);
        drop(custody);
        drop(rejected);
        drop(rejected_stale);
        drop(replay);
        drop(first);
        drop(second);
        let mut recovered = fixture.runtime.runtime.open_task_with_native_work(task, &fixture.fence, context.clone(), &original).await.unwrap();
        let checkpoint = recovered.checkpoint().clone();
        let retry = crate::pi_root_provider::guarded_pi_value(&input, fixture.owner.policy().maximum_payload_bytes, &original).unwrap();
        let retried = recovered.step_with_native_work(OperationId::from_bytes([operation; 16]),
            IdempotencyKey::new(key).unwrap(), retry.0, retry.1).await.unwrap();
        assert_eq!(retried.0.state, checkpoint.state);
        assert_eq!(recovered.checkpoint(), &checkpoint);
        assert_eq!(fixture.runtime.runtime.task_host().observe_admission(task).await.unwrap(), admission);
        let replay = journal.replay_with_native_work(0, 16, &original).unwrap().await.unwrap();
        assert_eq!(replay.0.last().unwrap().next, checkpoint);
        assert_eq!(replay.0.len() as u64, prior.revision + 1);
        drop(replay);
        drop(retried);
        let denied = crate::pi_root_provider::guarded_pi_value(&input, fixture.owner.policy().maximum_payload_bytes, &original).unwrap();
        let before = original.receipt();
        original.cancellation().cancel();
        let denied = recovered.step_with_native_work(OperationId::from_bytes([158; 16]),
            IdempotencyKey::new("native-large-cancelled").unwrap(), denied.0, denied.1).await.err().unwrap();
        assert!(matches!(&denied, acyclic_harness::Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. }));
        assert_eq!(original.receipt().backend_read_operations, before.backend_read_operations);
        assert_eq!(original.receipt().backend_write_operations, before.backend_write_operations);
        assert_eq!(recovered.checkpoint(), &checkpoint);
        assert!(original.receipt().peak_allocation_bytes > 128 * 1024);
        drop(denied);
        drop(recovered);
        drop(context);
        drop(journal);
        drop(checkpoint);
        drop(prior);
        assert_eq!(original.live_allocation_bytes(), baseline);
        let observation = work();
        let mut wrong_fence = fixture.fence.clone();
        wrong_fence.reservation_id.push_str("-foreign");
        assert!(fixture.runtime.runtime.workflow_journal_with_native_work(task, &wrong_fence, &observation).await.is_err());
        let context = fixture.runtime.runtime.task_context_with_native_work(task, &fixture.fence, &observation).await.unwrap();
        let mut current = fixture.runtime.runtime.open_task_with_native_work(task, &fixture.fence, context, &observation).await.unwrap();
        let checkpoint = current.checkpoint().clone();
        fixture.runtime.runtime.task_host().cancel(task, Some(&observation)).await.unwrap();
        let retry = crate::pi_root_provider::guarded_pi_value(&input, fixture.owner.policy().maximum_payload_bytes, &observation).unwrap();
        let before = observation.receipt();
        assert!(current.step_with_native_work(OperationId::from_bytes([operation; 16]),
            IdempotencyKey::new(key).unwrap(), retry.0, retry.1).await.is_err());
        assert_eq!(current.checkpoint(), &checkpoint);
        assert_eq!(observation.receipt().backend_write_operations, before.backend_write_operations);
        assert!(fixture.runtime.runtime.task_host().outcome(task).await.unwrap().is_none());

    }

    #[test]
    fn native_initialization_uses_original_ledger_and_matches_both_execution_modes() {
        let machine_work = work();
        let machine_work_baseline = machine_work.live_allocation_bytes();
        let machine = PiTaskMachine::new(&machine_work).unwrap();
        for execution in [
            PiTaskExecution::Host { root: PiRootIntent::ScratchOnly {}, preparation: preparation() },
            PiTaskExecution::Child { launch: preparation() },
        ] {
            let expected = json!({"state": if matches!(&execution, PiTaskExecution::Host { .. }) { "preparing" } else { "ready" }, "command":preparation()});
            let input = serde_json::to_value(PiTaskInput { binding: binding(), execution }).unwrap();
            let original = work();
            let original_baseline = original.live_allocation_bytes();
            let admitted = crate::pi_root_provider::guarded_pi_value(&input,
                NATIVE_METADATA_MAXIMUM, &original).unwrap();
            let before = original.receipt();
            let initialized = machine.initialize_with_native_work(&admitted.0, &original).unwrap();
            assert_eq!(initialized.0, expected);
            assert!(initialized.1.shares_ledger(&admitted.1));
            assert_eq!(original.receipt().backend_read_operations, before.backend_read_operations);
            assert_eq!(original.receipt().backend_write_operations, before.backend_write_operations);
            drop(admitted);
            drop(initialized);
            assert_eq!(original.live_allocation_bytes(), original_baseline);
        }
        drop(machine);
        assert_eq!(machine_work.live_allocation_bytes(), machine_work_baseline);
    }

    #[test]
    fn native_initialization_denies_cancellation_and_malformed_binding() {
        let machine = PiTaskMachine::new(&work()).unwrap();
        let input = serde_json::to_value(PiTaskInput {
            binding: binding(),
            execution: PiTaskExecution::Host { root: PiRootIntent::ScratchOnly {}, preparation: preparation() },
        }).unwrap();
        let cancellation = CancellationToken::new();
        let original = NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &cancellation).unwrap();
        let original_baseline = original.live_allocation_bytes();
        let before = original.receipt();
        cancellation.cancel();
        let denied_machine = PiTaskMachine::new(&original).err().unwrap();
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
        assert_eq!(original.live_allocation_bytes(), original_baseline);
        for malformed in [
            json!({"binding":binding(),"launch":null}),
            json!({"binding":binding(),"execution":{"kind":"host"}}),
            json!({"binding":binding(),"execution":{"kind":"child","launch":null}}),
            json!({"binding":binding(),"execution":{"kind":"host","root":{"kind":"scratch_only"}}}),
            json!({"binding":binding(),"execution":{"kind":"host","root":{"kind":"scratch_only","paths":{}},"preparation":preparation()}}),
            json!({"binding":binding(),"execution":{"kind":"host","root":{"kind":"scratch_only"},"preparation":preparation()},"unknown":true}),
            json!({"binding":{},"execution":{"kind":"child","launch":preparation()}}),
            json!({"binding":binding(),"execution":{"kind":"host","root":{"kind":"scratch_only"},"preparation":preparation(),"unknown":true}}),
        ] {
            let original = work();
            let original_baseline = original.live_allocation_bytes();
            let failure = machine.initialize_with_native_work(&malformed, &original).err().unwrap();
            assert_eq!(failure.code(), acyclic_harness::wire::ErrorCode::Invalid);
            drop(failure);
            assert_eq!(original.live_allocation_bytes(), original_baseline);
        }
    }
}
