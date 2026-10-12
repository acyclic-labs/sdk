use super::*;
use crate::pi_fork::tests::{policy, work};
use crate::pi_owner::{PiOwnerConfig, PiOwnerPolicy};
use crate::pi_recovery::{PiTaskObservation, observe_pi_session};
use crate::pi_root::PiRootIntent;
use crate::pi_runtime::PiTaskRuntime;
use crate::{AdapterState, PiTaskBinding, PluginTaskBinding};
use acyclic_harness::{
    AgentId, Capabilities, TaskId,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    distributed::{WorkPull, Worker},
    filesystem::FilesystemHost,
    runtime::{TaskSpawner as _, TaskStateProvider},
    scheduler::ResourceSnapshot,
};
use acyclic_stream::{LocalStream, LocalStreamLimits};
use std::{collections::BTreeMap, path::Path};

#[path = "pi_task_model_tests.rs"]
mod bounded_model;

fn binding() -> PiTaskBinding {
    let mut binding = crate::pi_binding::tests::binding("pi-root", 31);
    binding.task_id = TaskId::from_bytes([32; 16]);
    binding.scratch.allocation = PiScratchAllocation::Preparing;
    binding
}

fn preparation() -> WorkflowCommand {
    WorkflowCommand {
        operation_id: OperationId::from_bytes([86; 16]),
        kind: EFFECT_TASK_COMMAND_KIND.into(),
        payload: FileRef::new(
            binding().scratch.volume,
            "preparation.json".into(),
            "fixture".into(),
            acyclic_harness::conversation::FileDescriptor::from_bytes(
                b"{}",
                "application/json",
                &mut None,
            )
            .unwrap(),
            "preparation.json".into(),
            &mut None,
        )
        .unwrap(),
    }
}

fn result(operation: OperationId, value: Outcome<FileRef>) -> Value {
    json!({"commands":[{"operation_id":operation,"value":value}]})
}

fn session_state(binding: &PiTaskBinding) -> AdapterState {
    let mut root = binding.clone();
    root.scratch.allocation = crate::pi_binding::tests::binding("pi-root", 31)
        .scratch
        .allocation;
    AdapterState {
        version: crate::ADAPTER_STATE_VERSION,
        active: true,
        root_session_id: root.native_session_id.clone(),
        root_agent_id: root.native_session_id.clone(),
        task_binding: PluginTaskBinding::Pi(Box::new(root)),
        ..AdapterState::default()
    }
}

async fn storage(
    path: &Path,
    provider: &acyclic_harness::resources::ProviderRef,
) -> (
    Arc<FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>,
    Arc<LocalStream>,
) {
    let raw = acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(path.join("filesystem")))
        .await
        .unwrap();
    (
        Arc::new(FilesystemHost::new(raw, provider.clone()).unwrap()),
        Arc::new(
            LocalStream::open(path.join("stream"), LocalStreamLimits::default())
                .await
                .unwrap(),
        ),
    )
}

async fn open(
    path: &Path,
    binding: &PiTaskBinding,
) -> acyclic_harness::Result<(
    PiTaskRuntime<LocalStream>,
    acyclic_fs::NativeMaterializationWork,
)> {
    let work = work();
    let root =
        acyclic_fs::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(path, &work)
            .map_err(|cause| acyclic_harness::Error::NativeWork {
                cause,
                custody: None,
            })?;
    let owner = PiOwnerConfig::load_with_native_work(&root, Path::new(""), &work)?;
    owner.validate_root(binding, &work)?;
    let (filesystem, stream) = storage(path, owner.policy().journal.provider()).await;
    let runtime = PiTaskRuntime::open_with_native_work(
        stream,
        filesystem,
        &owner.policy().journal,
        owner.authority_with_native_work(&work)?,
        &work,
    )
    .await?;
    Ok((runtime, work))
}

async fn fixture(
    binding: &PiTaskBinding,
) -> (
    tempfile::TempDir,
    PiTaskRuntime<LocalStream>,
    acyclic_fs::NativeMaterializationWork,
) {
    let directory = tempfile::tempdir().unwrap();
    let journal = VolumeRef::new(
        binding.scratch.volume.provider().clone(),
        "journal".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(binding.agent_id),
        &mut None,
    )
    .unwrap();
    let grants = Capabilities::new(
        [
            "operation:declare".into(),
            "operation:observe".into(),
            "operation:cancel".into(),
            "operation:wake".into(),
            "task:spawn:acyclic.pi@1".into(),
            binding
                .scratch
                .volume
                .capability(VolumeOperation::Read, &mut None)
                .unwrap(),
            binding
                .scratch
                .volume
                .capability(VolumeOperation::Write, &mut None)
                .unwrap(),
        ]
        .into_iter()
        .chain(
            binding
                .project_volumes
                .values()
                .map(|volume| volume.capability(VolumeOperation::Read, &mut None).unwrap()),
        ),
    );
    let owner = PiOwnerConfig::new(
        &PiOwnerPolicy {
            root_task: binding.task_id,
            agent: binding.agent_id,
            journal: journal.clone(),
            grants,
            ..policy()
        },
        None,
        &work(),
    )
    .unwrap();
    crate::pi_owner::tests::publish_owner(&owner, directory.path(), &work()).unwrap();
    let (filesystem, _) = storage(directory.path(), journal.provider()).await;
    filesystem.create_volume(&journal).await.unwrap();
    drop(filesystem);
    let (runtime, work) = open(directory.path(), binding).await.unwrap();
    (directory, runtime, work)
}

#[tokio::test]
async fn root_admission_is_original_and_does_not_start_work() {
    let binding = binding();
    let (directory, runtime, work) = fixture(&binding).await;
    for mutation in 0..3 {
        let mut foreign = binding.clone();
        match mutation {
            0 => {
                foreign.agent_id = AgentId::from_bytes([71; 16]);
                foreign.scratch.volume = VolumeRef::new(
                    foreign.scratch.volume.provider().clone(),
                    "scratch".into(),
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(foreign.agent_id),
                    &mut None,
                )
                .unwrap();
            }
            1 => foreign.task_id = TaskId::from_bytes([73; 16]),
            _ => {
                foreign.scratch.volume = VolumeRef::new(
                    foreign.scratch.volume.provider().clone(),
                    "journal".into(),
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(foreign.agent_id),
                    &mut None,
                )
                .unwrap()
            }
        }
        assert!(
            runtime
                .admit_root(
                    &foreign,
                    &PiRootIntent::ScratchOnly {},
                    &preparation(),
                    &work
                )
                .await
                .is_err()
        );
        assert!(
            runtime
                .runtime
                .task_host()
                .observe_admission(foreign.task_id)
                .await
                .is_err()
        );
    }
    runtime
        .admit_root(
            &binding,
            &PiRootIntent::ScratchOnly {},
            &preparation(),
            &work,
        )
        .await
        .unwrap();
    let admission = runtime
        .runtime
        .task_host()
        .observe_admission(binding.task_id)
        .await
        .unwrap();
    assert_eq!(admission.parent, None);
    let journal = VolumeRef::new(
        binding.scratch.volume.provider().clone(),
        "journal".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(binding.agent_id),
        &mut None,
    )
    .unwrap();
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        assert!(
            !admission
                .grants
                .contains(&journal.capability(operation, &mut None).unwrap())
        );
    }
    assert!(matches!(
        runtime
            .runtime
            .task_host()
            .recover_work(binding.task_id, &mut None)
            .await
            .unwrap(),
        WorkPull::Idle
    ));
    drop(runtime);
    let (runtime, work) = open(directory.path(), &binding).await.unwrap();
    runtime
        .admit_root(
            &binding,
            &PiRootIntent::ScratchOnly {},
            &preparation(),
            &work,
        )
        .await
        .unwrap();
    let mut changed = binding.clone();
    changed.native_session_id.push_str("-foreign");
    assert!(
        runtime
            .admit_root(
                &changed,
                &PiRootIntent::ScratchOnly {},
                &preparation(),
                &work
            )
            .await
            .is_err()
    );
    assert!(
        runtime
            .runtime
            .task_host()
            .observe_admission(binding.task_id)
            .await
            .unwrap()
            == admission
    );
}

#[tokio::test]
async fn credentials_are_checked_before_reopening_storage() {
    let binding = binding();
    let (directory, runtime, work) = fixture(&binding).await;
    runtime
        .admit_root(
            &binding,
            &PiRootIntent::ScratchOnly {},
            &preparation(),
            &work,
        )
        .await
        .unwrap();
    let original = runtime
        .runtime
        .task_host()
        .observe_admission(binding.task_id)
        .await
        .unwrap();
    drop(runtime);
    let private = acyclic_fs::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(
        directory.path(),
        &work,
    )
    .unwrap();
    let owner = PiOwnerConfig::load_with_native_work(&private, Path::new(""), &work).unwrap();
    std::fs::rename(
        directory.path().join("filesystem"),
        directory.path().join("retained"),
    )
    .unwrap();
    let mut foreign = binding.clone();
    foreign.task_id = TaskId::from_bytes([74; 16]);
    assert!(open(directory.path(), &foreign).await.is_err());
    assert!(!directory.path().join("filesystem").exists());
    std::fs::remove_file(directory.path().join("pi-owner.v1")).unwrap();
    assert!(open(directory.path(), &binding).await.is_err());
    assert!(!directory.path().join("filesystem").exists());
    assert!(!directory.path().join("pi-owner.v1").exists());
    owner
        .save_new_with_native_work(&private, Path::new(""), &work)
        .unwrap();
    std::fs::rename(
        directory.path().join("retained"),
        directory.path().join("filesystem"),
    )
    .unwrap();
    let (runtime, _) = open(directory.path(), &binding).await.unwrap();
    assert!(
        runtime
            .runtime
            .task_host()
            .observe_admission(binding.task_id)
            .await
            .unwrap()
            == original
    );
}

#[tokio::test]
async fn selected_root_recovery_requires_original_admission() {
    let mut binding = binding();
    let key = hex::encode([81; 16]);
    binding.project_volumes.insert(
        key.clone(),
        VolumeRef::new(
            binding.scratch.volume.provider().clone(),
            "selected-project".into(),
            VolumeClass::Project,
            VolumeOwner::Project("selected-project".into()),
            &mut None,
        )
        .unwrap(),
    );
    let (directory, runtime, work) = fixture(&binding).await;
    let selected = directory.path().join("selected source with spaces-λ");
    let mut state = session_state(&binding);
    state.root_id = [81; 16];
    state.root_context_id = [82; 16];
    state.pending_root_registration = true;
    state.task_binding = PluginTaskBinding::Pi(Box::new(binding.clone()));
    state.roots.insert(
        key.clone(),
        crate::RootBinding {
            root_id: state.root_id,
            path: selected.clone(),
            allocation: crate::RootAllocation::Preparing,
        },
    );
    let intent = PiRootIntent::from_state(&state).unwrap();
    assert!(
        observe_pi_session(&runtime.runtime, &state, &work)
            .await
            .is_err()
    );
    runtime
        .admit_root(&binding, &intent, &preparation(), &work)
        .await
        .unwrap();
    let admission = runtime
        .runtime
        .task_host()
        .observe_admission(binding.task_id)
        .await
        .unwrap();
    for mutation in 0..3 {
        let mut changed = state.clone();
        match mutation {
            0 => {
                changed.roots.get_mut(&key).unwrap().path = directory.path().join("foreign source")
            }
            1 => changed.root_context_id = [83; 16],
            _ => {
                changed.root_session_id.push_str("-foreign");
                let PluginTaskBinding::Pi(root) = &mut changed.task_binding else {
                    unreachable!()
                };
                root.native_session_id = changed.root_session_id.clone();
            }
        }
        assert!(
            observe_pi_session(&runtime.runtime, &changed, &work)
                .await
                .is_err()
        );
        if mutation < 2 {
            assert!(
                runtime
                    .admit_root(
                        &binding,
                        &PiRootIntent::from_state(&changed).unwrap(),
                        &preparation(),
                        &work
                    )
                    .await
                    .is_err()
            );
        }
    }
    crate::save_state(directory.path(), &state, crate::Survives::PowerLoss).unwrap();
    drop(runtime);
    let state = crate::load_saved_state(directory.path()).unwrap();
    let (filesystem, stream) = storage(directory.path(), binding.scratch.volume.provider()).await;
    let private = acyclic_fs::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(
        directory.path(),
        &work,
    )
    .unwrap();
    let owner = PiOwnerConfig::load_with_native_work(&private, Path::new(""), &work).unwrap();
    let reopened = PiTaskRuntime::reopen(&owner, &state, stream, filesystem, &work)
        .await
        .unwrap();
    assert!(
        matches!(reopened.1.as_slice(), [PiTaskObservation::Pending { task_id }] if *task_id == binding.task_id)
    );
    assert!(
        reopened
            .0
            .runtime
            .task_host()
            .observe_admission(binding.task_id)
            .await
            .unwrap()
            == admission
    );
    let query = crate::pi_fork::tests::work();
    query.cancellation().cancel();
    assert!(
        observe_pi_session(&reopened.0.runtime, &state, &query)
            .await
            .is_err()
    );
    assert!(matches!(
        reopened
            .0
            .runtime
            .task_host()
            .recover_work(binding.task_id, &mut None)
            .await
            .unwrap(),
        WorkPull::Idle
    ));
    assert!(!selected.exists());
    assert!(!directory.path().join("foreign source").exists());
}

#[test]
fn root_selection_rejects_unselected_paths_and_invalid_identity() {
    let mut binding = binding();
    let key = hex::encode([2; 16]);
    binding.project_volumes.insert(
        key.clone(),
        VolumeRef::new(
            binding.scratch.volume.provider().clone(),
            "project".into(),
            VolumeClass::Project,
            VolumeOwner::Project("project".into()),
            &mut None,
        )
        .unwrap(),
    );
    let directory = tempfile::tempdir().unwrap();
    let path = directory
        .path()
        .join("selected")
        .to_str()
        .unwrap()
        .to_owned();
    let original = PiRootIntent::Project {
        context_id: [1; 16],
        root_id: [2; 16],
        paths: BTreeMap::from([(key.clone(), path.clone())]),
    };
    original.validate(&binding, &work()).unwrap();
    assert!(
        PiRootIntent::ScratchOnly {}
            .validate(&binding, &work())
            .is_err()
    );
    for path in [
        "relative/path".into(),
        format!("{path}\0"),
        directory
            .path()
            .join("..")
            .join("escape")
            .to_str()
            .unwrap()
            .into(),
        format!("{path}{}", "x".repeat(128 * 1024)),
    ] {
        assert!(
            PiRootIntent::Project {
                context_id: [1; 16],
                root_id: [2; 16],
                paths: BTreeMap::from([(key.clone(), path)])
            }
            .validate(&binding, &work())
            .is_err()
        );
    }
    for (context_id, root_id) in [([0; 16], [2; 16]), ([1; 16], [0; 16]), ([1; 16], [3; 16])] {
        assert!(
            PiRootIntent::Project {
                context_id,
                root_id,
                paths: BTreeMap::from([(key.clone(), path.clone())])
            }
            .validate(&binding, &work())
            .is_err()
        );
    }
    assert!(!directory.path().join("selected").exists());
}

fn preparing_state(
    fixture: &crate::pi_fork::tests::Fixture,
    plan: &crate::pi_fork::PiForkPlan,
) -> AdapterState {
    let PiRootIntent::Project { root_id, paths, .. } = &fixture.root_request.root else {
        unreachable!()
    };
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
        mount_path: crate::pi_native::planned_native_paths(
            &fixture.resources,
            &state.root_session_id,
            plan.commit.child.task_id,
            paths.keys().next().map(String::as_str),
            &work(),
        )
        .unwrap()
        .0
        .view_root,
        lifecycle: crate::PendingSpawnLifecycle::Preparing,
    });
    state
}

#[tokio::test]
async fn preparing_phases_never_claim_a_future_native_child() {
    use crate::pi_recovery::PiPreparationPhase;
    let fixture = crate::pi_fork::tests::Fixture::new(None).await;
    let plan = fixture.plan(&work()).await;
    let state = preparing_state(&fixture, &plan);
    crate::save_state(
        &fixture.resources.session_directory(&state.root_session_id),
        &state,
        crate::Survives::PowerLoss,
    )
    .unwrap();
    assert!(
        crate::ControlPlane::open(fixture.resources.session_directory(&state.root_session_id))
            .await
            .is_err()
    );
    let host = fixture.runtime.runtime.task_host();
    let query = work();
    assert!(
        observe_pi_session(&fixture.runtime.runtime, &state, &query)
            .await
            .is_err()
    );
    assert!(
        host.reconcile_admission(OperationId::from_bytes(
            plan.commit.child.task_id.into_bytes()
        ))
        .await
        .unwrap()
        .is_none()
    );
    host.admit_owned(plan.preparation.clone(), fixture.fence.clone())
        .await
        .unwrap();
    let observed = observe_pi_session(&fixture.runtime.runtime, &state, &query)
        .await
        .unwrap();
    assert!(
        matches!(&observed.0[1], PiTaskObservation::Preparation { task_id, preparation, phase: PiPreparationPhase::Pending }
        if *task_id == plan.commit.child.task_id && *preparation == plan.preparation.task_id())
    );
    drop(observed);
    for mutation in 0..5 {
        let mut changed = state.clone();
        match mutation {
            0 => {
                let PluginTaskBinding::Pi(child) = &mut changed.pending[0].task_binding else {
                    unreachable!()
                };
                child.native_session_id.push_str("-foreign");
            }
            1 => changed.pending[0].parent_agent_id = "foreign-parent".into(),
            2 => changed.pending[0].tool_use_id.push_str("-foreign"),
            3 => changed.pending[0].agent_id = Some("wrong-alias".into()),
            _ => changed.pending[0].agent_id = None,
        }
        assert!(
            observe_pi_session(&fixture.runtime.runtime, &changed, &query)
                .await
                .is_err()
        );
        assert!(
            host.observe_admission(plan.commit.child.task_id)
                .await
                .is_err()
        );
    }
    let worker = Worker {
        id: "pi-preparing-observation".into(),
        available: ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    };
    {
        let io = acyclic_harness::filesystem::PreparationIo::from_native_work(&query).unwrap();
        let value = fixture
            .runtime
            .runtime
            .run_operation(
                &worker,
                plan.preparation.task_id(),
                &fixture.runtime.runtime.commands(),
                1,
                Some(&io),
            )
            .await
            .unwrap();
        let acyclic_harness::filesystem::PreparationIo::Native { custody, .. } = io else {
            unreachable!()
        };
        (
            value,
            custody
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    };
    let observed = observe_pi_session(&fixture.runtime.runtime, &state, &query)
        .await
        .unwrap();
    assert!(matches!(
        &observed.0[1],
        PiTaskObservation::Preparation {
            phase: PiPreparationPhase::Reserved,
            ..
        }
    ));
    drop(observed);
    let coordinator = fixture.coordinator(&work()).unwrap();
    let mut stale = fixture.fence.clone();
    stale.reservation_id.push_str("-foreign");
    assert!(
        fixture
            .prepare_with(
                &coordinator,
                &fixture.context,
                &fixture.journal,
                &stale,
                &plan,
                fixture.root_handle.workspace.clone(),
                crate::pi_root_provider::allocation_budget(),
                acyclic_fs::CancellationToken::new()
            )
            .await
            .is_err()
    );
    fixture
        .prepare_with(
            &coordinator,
            &fixture.context,
            &fixture.journal,
            &fixture.fence,
            &plan,
            fixture.root_handle.workspace.clone(),
            crate::pi_root_provider::allocation_budget(),
            acyclic_fs::CancellationToken::new(),
        )
        .await
        .unwrap()
        .unwrap();
    let reopened = PiTaskRuntime::open_with_native_work(
        fixture.stream.clone(),
        fixture.filesystem.clone(),
        &fixture.owner.policy().journal,
        fixture.owner.authority_with_native_work(&query).unwrap(),
        &query,
    )
    .await
    .unwrap();
    for _ in 0..2 {
        let observed = observe_pi_session(&reopened.runtime, &state, &query)
            .await
            .unwrap();
        assert!(matches!(
            &observed.0[1],
            PiTaskObservation::Preparation {
                phase: PiPreparationPhase::Succeeded,
                ..
            }
        ));
        assert!(
            host.observe_admission(plan.commit.child.task_id)
                .await
                .is_err()
        );
        assert!(state.routes.is_empty());
    }
}

#[tokio::test]
async fn committed_root_reopens_and_accepts_a_distinct_control() {
    let fixture = crate::pi_fork::tests::Fixture::new(None).await;
    let task = fixture.owner.policy().root_task;
    let work = work();
    let recovery = {
        let mut io = acyclic_harness::store::history::native_work_io(&work).unwrap();
        acyclic_harness::store::history::HistoryIo::retain_owned(
            &mut io,
            std::mem::size_of::<WorkPull>().max(std::mem::size_of::<
                acyclic_harness::filesystem::TaskWorkerAttempt,
            >()) as u64,
        )
        .unwrap();
        let value = fixture
            .runtime
            .runtime
            .task_host()
            .recover_work(task, &mut Some(&mut io))
            .await
            .unwrap();
        (value, io.into_custody())
    };
    let WorkPull::Claimed(lease) = recovery.0 else {
        panic!("original control lease")
    };
    fixture
        .runtime
        .runtime
        .resume_task_with_native_work(
            lease,
            recovery.1,
            &fixture.runtime.runtime.commands(),
            8,
            &work,
        )
        .await
        .unwrap();
    let checkpoint = fixture
        .runtime
        .runtime
        .observe_workflow_checkpoint(task)
        .await
        .unwrap();
    let admission = fixture
        .runtime
        .runtime
        .task_host()
        .observe_admission(task)
        .await
        .unwrap();
    let reopened = PiTaskRuntime::open_with_native_work(
        fixture.stream.clone(),
        fixture.filesystem.clone(),
        &fixture.owner.policy().journal,
        fixture.owner.authority_with_native_work(&work).unwrap(),
        &work,
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .runtime
            .observe_workflow_checkpoint(task)
            .await
            .unwrap(),
        checkpoint
    );
    assert!(matches!(
        reopened
            .runtime
            .task_host()
            .recover_work(task, &mut None)
            .await
            .unwrap(),
        WorkPull::Idle
    ));
    let operation = OperationId::from_bytes([126; 16]);
    let payload = crate::pi_root_provider::stage_pi_input(
        &fixture.owner,
        &fixture.filesystem,
        operation,
        "next-timer",
        &json!({"deadline_unix_ms":0}),
        &work,
    )
    .await
    .unwrap();
    let control = PiControlInput {
        command: WorkflowCommand {
            operation_id: operation,
            kind: "acyclic.timer.v1".into(),
            payload: payload.0.clone(),
        },
    };
    let input = crate::pi_root_provider::stage_pi_input(
        &fixture.owner,
        &fixture.filesystem,
        operation,
        "next-control",
        &control,
        &work,
    )
    .await
    .unwrap();
    let suspension = reopened
        .runtime
        .task_host()
        .workflow_suspension(task)
        .await
        .unwrap()
        .unwrap();
    reopened
        .runtime
        .task_host()
        .resume_workflow(
            task,
            suspension.revision,
            input.0.clone(),
            acyclic_harness::IdempotencyKey::new("pi-root-control/distinct-next").unwrap(),
            None,
        )
        .await
        .unwrap();
    let worker = Worker {
        id: "pi-root-control-next".into(),
        available: ResourceSnapshot::default(),
        labels: BTreeMap::new(),
    };
    {
        let io = acyclic_harness::filesystem::PreparationIo::from_native_work(&work).unwrap();
        let value = reopened
            .runtime
            .run_operation(&worker, task, &reopened.runtime.commands(), 8, Some(&io))
            .await
            .unwrap();
        let acyclic_harness::filesystem::PreparationIo::Native { custody, .. } = io else {
            unreachable!()
        };
        (
            value,
            custody
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    };
    let next = reopened
        .runtime
        .observe_workflow_checkpoint(task)
        .await
        .unwrap();
    assert_eq!(next.state["allocation"], checkpoint.state["allocation"]);
    assert_eq!(
        control_reply(next.state, &work)
            .unwrap()
            .0
            .unwrap()
            .operation_id,
        operation
    );
    assert!(
        reopened
            .runtime
            .task_host()
            .observe_admission(task)
            .await
            .unwrap()
            == admission
    );
}

#[tokio::test]
async fn native_reply_survives_concurrent_settlement_and_reopen() {
    use acyclic_harness::{IdempotencyKey, workflow::WorkflowJournal};
    let fixture = crate::pi_fork::tests::Fixture::new(None).await;
    let task = fixture.root_request.binding.task_id;
    let work = work();
    let context = fixture
        .runtime
        .runtime
        .task_context_with_native_work(task, &fixture.fence, &work)
        .await
        .unwrap();
    let mut first = fixture
        .runtime
        .runtime
        .open_task_with_native_work(task, &fixture.fence, context.clone(), &work)
        .await
        .unwrap();
    let mut second = fixture
        .runtime
        .runtime
        .open_task_with_native_work(task, &fixture.fence, context.clone(), &work)
        .await
        .unwrap();
    let prior = first.checkpoint().clone();
    let input = json!({"commands":[{"operation_id":fixture.root_control.operation_id,"value":"q".repeat(128 * 1024)}]});
    let left = crate::pi_root_provider::guarded_pi_value(
        &input,
        fixture.owner.policy().maximum_payload_bytes,
        &work,
    )
    .unwrap();
    let right = crate::pi_root_provider::guarded_pi_value(
        &input,
        fixture.owner.policy().maximum_payload_bytes,
        &work,
    )
    .unwrap();
    let (left, right) = tokio::join!(
        first.step_with_native_work(
            OperationId::from_bytes([155; 16]),
            IdempotencyKey::new("native-large-left").unwrap(),
            left.0,
            left.1
        ),
        second.step_with_native_work(
            OperationId::from_bytes([156; 16]),
            IdempotencyKey::new("native-large-right").unwrap(),
            right.0,
            right.1
        )
    );
    assert_ne!(left.is_ok(), right.is_ok());
    let (accepted, rejected, stale, operation, key) = if left.is_ok() {
        (left, right, &mut second, 155, "native-large-left")
    } else {
        (right, left, &mut first, 156, "native-large-right")
    };
    assert_eq!(
        rejected.err().unwrap().code(),
        acyclic_harness::wire::ErrorCode::Conflict
    );
    let accepted = accepted.unwrap();
    assert_eq!(
        accepted.0.state["reply"]["value"],
        json!("q".repeat(128 * 1024))
    );
    assert_eq!(stale.checkpoint(), &prior);
    let next = crate::pi_root_provider::guarded_pi_value(
        &input,
        fixture.owner.policy().maximum_payload_bytes,
        &work,
    )
    .unwrap();
    assert_eq!(
        stale
            .step_with_native_work(
                OperationId::from_bytes([157; 16]),
                IdempotencyKey::new("native-large-stale").unwrap(),
                next.0,
                next.1
            )
            .await
            .err()
            .unwrap()
            .code(),
        acyclic_harness::wire::ErrorCode::Conflict
    );
    drop(accepted);
    drop(first);
    drop(second);
    let mut recovered = fixture
        .runtime
        .runtime
        .open_task_with_native_work(task, &fixture.fence, context.clone(), &work)
        .await
        .unwrap();
    let checkpoint = recovered.checkpoint().clone();
    let retry = crate::pi_root_provider::guarded_pi_value(
        &input,
        fixture.owner.policy().maximum_payload_bytes,
        &work,
    )
    .unwrap();
    let retried = recovered
        .step_with_native_work(
            OperationId::from_bytes([operation; 16]),
            IdempotencyKey::new(key).unwrap(),
            retry.0,
            retry.1,
        )
        .await
        .unwrap();
    assert_eq!(retried.0.state, checkpoint.state);
    assert_eq!(recovered.checkpoint(), &checkpoint);
    drop(retried);
    let journal = fixture
        .runtime
        .runtime
        .workflow_journal_with_native_work(task, &fixture.fence, &work)
        .await
        .unwrap();
    let replay = journal
        .replay_with_native_work(0, 16, &work)
        .unwrap()
        .await
        .unwrap();
    assert_eq!(replay.0.last().unwrap().next, checkpoint);
    drop(replay);
    let mut wrong = fixture.fence.clone();
    wrong.reservation_id.push_str("-foreign");
    assert!(
        fixture
            .runtime
            .runtime
            .workflow_journal_with_native_work(task, &wrong, &work)
            .await
            .is_err()
    );
    let denied = crate::pi_root_provider::guarded_pi_value(
        &input,
        fixture.owner.policy().maximum_payload_bytes,
        &work,
    )
    .unwrap();
    work.cancellation().cancel();
    assert!(
        recovered
            .step_with_native_work(
                OperationId::from_bytes([158; 16]),
                IdempotencyKey::new("native-large-cancelled").unwrap(),
                denied.0,
                denied.1
            )
            .await
            .is_err()
    );
    assert_eq!(recovered.checkpoint(), &checkpoint);
    let query = crate::pi_fork::tests::work();
    let mut current = fixture
        .runtime
        .runtime
        .open_task_with_native_work(task, &fixture.fence, context, &query)
        .await
        .unwrap();
    fixture
        .runtime
        .runtime
        .task_host()
        .cancel(task, Some(&query))
        .await
        .unwrap();
    let retry = crate::pi_root_provider::guarded_pi_value(
        &input,
        fixture.owner.policy().maximum_payload_bytes,
        &query,
    )
    .unwrap();
    assert!(
        current
            .step_with_native_work(
                OperationId::from_bytes([operation; 16]),
                IdempotencyKey::new(key).unwrap(),
                retry.0,
                retry.1
            )
            .await
            .is_err()
    );
    assert!(
        fixture
            .runtime
            .runtime
            .task_host()
            .outcome(task)
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn malformed_task_inputs_and_cancelled_work_are_denied() {
    let machine = PiTaskMachine::new(&work()).unwrap();
    for input in [
        json!({"binding":binding(),"launch":null}),
        json!({"binding":binding(),"execution":{"kind":"host"}}),
        json!({"binding":binding(),"execution":{"kind":"child","launch":null}}),
        json!({"binding":binding(),"execution":{"kind":"host","root":{"kind":"scratch_only"}}}),
        json!({"binding":binding(),"execution":{"kind":"host","root":{"kind":"scratch_only","paths":{}},"preparation":preparation()}}),
        json!({"binding":binding(),"execution":{"kind":"host","root":{"kind":"scratch_only"},"preparation":preparation()},"unknown":true}),
        json!({"binding":{},"execution":{"kind":"child","launch":preparation()}}),
        json!({"binding":binding(),"execution":{"kind":"host","root":{"kind":"scratch_only"},"preparation":preparation(),"unknown":true}}),
    ] {
        assert_eq!(
            machine
                .initialize_with_native_work(&input, &work())
                .err()
                .unwrap()
                .code(),
            acyclic_harness::wire::ErrorCode::Invalid
        );
    }
    let original = work();
    original.cancellation().cancel();
    assert!(PiTaskMachine::new(&original).is_err());
    assert!(
        machine
            .initialize_with_native_work(
                &serde_json::to_value(PiTaskInput {
                    binding: binding(),
                    execution: PiTaskExecution::Child {
                        launch: preparation()
                    }
                })
                .unwrap(),
                &original
            )
            .is_err()
    );
}
