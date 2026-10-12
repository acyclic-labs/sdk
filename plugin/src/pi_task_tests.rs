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

#[tokio::test]
async fn native_reply_survives_concurrent_settlement_and_reopen() {
    use acyclic_harness::{IdempotencyKey, workflow::WorkflowJournal};
    let fixture = crate::pi_root_provider::tests::Fixture::new(PiRootIntent::ScratchOnly {}).await;
    let runtime = &fixture.runtime.runtime;
    let guard = |value: &Value, native: &acyclic_fs::NativeMaterializationWork| {
        crate::pi_root_provider::guarded_pi_value(
            value,
            fixture.owner.policy().maximum_payload_bytes,
            native,
        )
        .unwrap()
    };
    let task = fixture.request.binding.task_id;
    let fence = acyclic_harness::scheduler::LeaseFence::from(&fixture.lease.reservation);
    let work = work();
    let context = runtime
        .task_context_with_native_work(task, &fence, &work)
        .await
        .unwrap();
    let acyclic_harness::effects::EffectStatus::Succeeded { result: allocation } =
        fixture.execute().await.unwrap().0
    else {
        panic!("original root allocation");
    };
    let mut journal = runtime
        .open_task_with_native_work(task, &fence, context.clone(), &work)
        .await
        .unwrap();
    let preparation = crate::pi_root_inputs::stage_root_command(
        &fixture.owner,
        &fixture.filesystem,
        &fixture.request,
        &work,
    )
    .await
    .unwrap();
    let settled = guard(
        &result(preparation.0.operation_id, Outcome::Succeeded(allocation)),
        &work,
    );
    journal
        .step_with_native_work(
            OperationId::from_bytes([151; 16]),
            IdempotencyKey::new("native-large-allocation").unwrap(),
            settled.0,
            settled.1,
        )
        .await
        .unwrap();
    let operation = OperationId::from_bytes([152; 16]);
    let timer = crate::pi_root_provider::stage_pi_input(
        &fixture.owner,
        &fixture.filesystem,
        operation,
        "large-reply-timer",
        &json!({"deadline_unix_ms":0}),
        &work,
    )
    .await
    .unwrap();
    let control = PiControlInput {
        command: WorkflowCommand {
            operation_id: operation,
            kind: "acyclic.timer.v1".into(),
            payload: timer.0,
        },
    };
    let admitted = guard(&serde_json::to_value(&control).unwrap(), &work);
    journal
        .step_with_native_work(
            OperationId::from_bytes([153; 16]),
            IdempotencyKey::new("native-large-control").unwrap(),
            admitted.0,
            admitted.1,
        )
        .await
        .unwrap();
    drop(journal);
    let mut first = runtime
        .open_task_with_native_work(task, &fence, context.clone(), &work)
        .await
        .unwrap();
    let mut second = runtime
        .open_task_with_native_work(task, &fence, context.clone(), &work)
        .await
        .unwrap();
    let prior = first.checkpoint().clone();
    let input = json!({"commands":[{"operation_id":operation,"value":"q".repeat(128 * 1024)}]});
    let left = guard(&input, &work);
    let right = guard(&input, &work);
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
    let reply = control_reply(accepted.0.state, &work).unwrap();
    assert_eq!(reply.0.unwrap().value, json!("q".repeat(128 * 1024)));
    drop(reply.1);
    assert_eq!(stale.checkpoint(), &prior);
    let next = guard(&input, &work);
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
    drop(accepted.1);
    drop(first);
    drop(second);
    let mut recovered = runtime
        .open_task_with_native_work(task, &fence, context.clone(), &work)
        .await
        .unwrap();
    let checkpoint = recovered.checkpoint().clone();
    let retry = guard(&input, &work);
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
    let journal = runtime
        .workflow_journal_with_native_work(task, &fence, &work)
        .await
        .unwrap();
    let replay = journal
        .replay_with_native_work(0, 16, &work)
        .unwrap()
        .await
        .unwrap();
    assert_eq!(replay.0.last().unwrap().next, checkpoint);
    drop(replay);
    let mut wrong = fence.clone();
    wrong.reservation_id.push_str("-foreign");
    assert!(
        runtime
            .workflow_journal_with_native_work(task, &wrong, &work)
            .await
            .is_err()
    );
    let denied = guard(&input, &work);
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
    let mut current = runtime
        .open_task_with_native_work(task, &fence, context, &query)
        .await
        .unwrap();
    runtime
        .task_host()
        .cancel(task, Some(&query))
        .await
        .unwrap();
    let retry = guard(&input, &query);
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
    assert!(runtime.task_host().outcome(task).await.unwrap().is_none());
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
