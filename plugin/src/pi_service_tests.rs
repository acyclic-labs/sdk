
use super::*;
use acyclic_harness::runtime::TaskStateProvider;
use crate::{
    ADAPTER_STATE_VERSION, AdapterState, ConcurrentServiceControl, ControlCommand, ControlRequest,
    PiScratchAllocation, PluginTaskBinding, RootAllocation, RootBinding, ServiceResources, SessionController, Survives,
    load_saved_state, save_state,
};
use acyclic_harness::distributed::{WorkPull, Worker};
use acyclic_stream::{LocalStream, LocalStreamLimits};
use serde_json::Value;
use std::{collections::BTreeMap, path::PathBuf};

async fn open_service_resources(data: PathBuf) -> (crate::service_process::ServiceLock, ServiceResources, BTreeMap<String, SessionController>) {
    let (service_lock, configured, root, work) = crate::tests::configured_test_service_bootstrap(&data).await.unwrap();
    let (resources, sessions) = ServiceResources::open(data, configured, root, &work).await.unwrap();
    (service_lock, resources, sessions)
}

async fn prepare_service(data: &Path, reserve: bool) -> (crate::service_process::ServiceLock, ServiceResources, AdapterState) {
    let (service_lock, configured, private_root, native_work) = crate::tests::configured_test_service_bootstrap(data).await.unwrap();
    let (resources, sessions) = ServiceResources::open(data.to_path_buf(), configured, private_root, &native_work).await.unwrap();
    assert!(sessions.is_empty());
    let directory = resources.session_directory("pi-root-input");
    fs::create_dir_all(&directory).unwrap();
    let (owner, filesystem, request, raw) =
        root_input_fixture(&directory, Some(resources.fs.clone()), |_| {}).await;
    let command = stage_root_command(&owner, &filesystem, &request, &native_work)
        .await
        .unwrap();
    let filesystem = Arc::new(filesystem);
    let stream = Arc::new(
        LocalStream::open(directory.join("stream"), LocalStreamLimits::default())
            .await
            .unwrap(),
    );
    let runtime = crate::pi_runtime::PiTaskRuntime::open_with_native_work(
        stream,
        filesystem,
        &owner.policy.journal,
        owner.authority_with_native_work(&native_work).unwrap(),
        &native_work,
    )
    .await
    .unwrap();
    let admission = runtime
        .admit_root(&request.binding, &request.root, &command.0, &native_work)
        .await
        .unwrap();
    drop(admission);
    drop(command.1);
    if reserve {
        let WorkPull::Claimed(lease) = runtime
            .runtime
            .task_host()
            .pull_work(&Worker {
                id: "pi-service-original".to_owned(),
                available: Default::default(),
                labels: Default::default(),
            })
            .await
            .unwrap()
        else {
            panic!("root did not retain its original reservation");
        };
        runtime
            .runtime
            .task_host()
            .start_task(&lease, None)
            .await
            .unwrap();
    }
    let PiRootIntent::Project {
        context_id,
        root_id,
        paths,
    } = request.root
    else {
        panic!("fixture must select its original project");
    };
    let state = AdapterState {
        version: ADAPTER_STATE_VERSION,
        active: true,
        root_session_id: request.binding.native_session_id.clone(),
        root_agent_id: request.binding.native_session_id.clone(),
        root_context_id: context_id,
        root_id,
        pending_root_registration: true,
        task_binding: PluginTaskBinding::Pi(Box::new(request.binding)),
        roots: paths
            .into_iter()
            .map(|(key, path)| {
                (
                    key,
                    RootBinding {
                        root_id,
                        path: PathBuf::from(path),
                        allocation: RootAllocation::Preparing,
                    },
                )
            })
            .collect(),
        ..AdapterState::default()
    };
    save_state(&directory, &state, Survives::PowerLoss).unwrap();
    drop(runtime);
    drop(raw);
    (service_lock, resources, state)
}

fn status_request(cwd: PathBuf, command: ControlCommand) -> ControlRequest {
    ControlRequest {
        version: 1,
        command,
        cwd,
        argv: Vec::new(),
        name: String::new(),
        arguments: Value::Null,
    }
}

#[tokio::test]
async fn pi_service_actor_cold_reopens_original_pending_and_reserved_tasks_without_native_access() {
    for reserved in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("service");
        let (_service_lock, resources, state) = prepare_service(&data, reserved).await;
        let private_root = resources.private_state_root.clone();
        let session = resources.session_directory(&state.root_session_id);
        let controller = SessionController::open(session.clone(), &resources, &work())
            .await
            .unwrap();
        let service = ConcurrentServiceControl::from_parts(
            resources,
            BTreeMap::from([(state.root_session_id.clone(), controller)]),
            &work(),
        ).unwrap();
        let expected = json!({
            "session": state.root_session_id,
            "native_session": match &state.task_binding {PluginTaskBinding::Pi(binding) => &binding.native_session_id, _ => unreachable!()},
            "tasks": [{"state": if reserved {"reserved"} else {"pending"},
                "task": match &state.task_binding {PluginTaskBinding::Pi(binding) => binding.task_id, _ => unreachable!()}}],
        });
        for _ in 0..2 {
            let handle = service.session(&state.root_session_id).await.unwrap();
            let mut original_native_work = None;
            let status = handle.dispatch(status_request(
                directory.path().to_path_buf(),
                ControlCommand::Agents,
            ), &mut original_native_work)
                .await
                .unwrap();
            assert_eq!(status.0, expected);
            assert!(status.1.as_ref().unwrap().work().shares_ledger(original_native_work.as_ref().unwrap()));
            for command in [
                ControlCommand::Git,
                ControlCommand::Discard,
                ControlCommand::Hook,
            ] {
                let error = handle.dispatch(status_request(directory.path().to_path_buf(), command), &mut None)
                    .await
                    .unwrap_err();
                assert!(matches!(error, acyclic_harness::Error::Invalid(_)));
            }
            assert_eq!(service.shared_roots.live_roots().await, 0);
            assert!(!state.roots.values().next().unwrap().path.exists());
        }
        service.shutdown().await.unwrap();
        assert_eq!(
            serde_json::to_value(load_saved_state(&session).unwrap()).unwrap(),
            serde_json::to_value(&state).unwrap()
        );
        let configured = ServiceResources::configure(&data).await.unwrap();
        let reopened = ConcurrentServiceControl::open(data, configured, private_root, &work()).await.unwrap();
        let handle = reopened.session(&state.root_session_id).await.unwrap();
        let mut original_native_work = None;
        let status = handle.dispatch(status_request(
            directory.path().to_path_buf(),
            ControlCommand::Agents,
        ), &mut original_native_work)
            .await
            .unwrap();
        assert_eq!(status.0, expected);
        assert!(status.1.as_ref().unwrap().work().shares_ledger(original_native_work.as_ref().unwrap()));
        assert!(
            reopened
                .route_session(directory.path())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(reopened.shared_roots.live_roots().await, 0);
        drop(handle);
        reopened.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn pi_service_deactivation_cancels_original_task_before_persisting_inactive_state() {
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path().join("service");
    let (_service_lock, resources, state) = prepare_service(&data, true).await;
    let private_root = resources.private_state_root.clone();
    let session = resources.session_directory(&state.root_session_id);
    let controller = SessionController::open(session.clone(), &resources, &work())
        .await
        .unwrap();
    let service = ConcurrentServiceControl::from_parts(
        resources,
        BTreeMap::from([(state.root_session_id.clone(), controller)]),
        &work(),
    ).unwrap();
    service.begin_drain();
    service.shutdown().await.unwrap();
    let saved = load_saved_state(&session).unwrap();
    assert!(!saved.active);
    let configured = ServiceResources::configure(&data).await.unwrap();
    let (resources, sessions) = ServiceResources::open(data, configured, private_root, &work()).await.unwrap();
    assert!(sessions.is_empty());
    let mut controller = SessionController::open(session, &resources, &work()).await.unwrap();
    let status = controller.dispatch(status_request(
        directory.path().to_path_buf(),
        ControlCommand::Agents,
    ), &mut None)
        .await
        .unwrap();
    assert_eq!(status.0.pointer("/tasks/0/state").unwrap(), "unresolved");
    let PluginTaskBinding::Pi(binding) = &state.task_binding else {
        unreachable!();
    };
    assert_eq!(
        status.0.pointer("/tasks/0/task").unwrap(),
        &serde_json::to_value(binding.task_id).unwrap()
    );
    assert!(!state.roots.values().next().unwrap().path.exists());
    drop(controller);
    resources.shared_roots.collection.stop().await;
}

#[tokio::test]
async fn pi_service_recovery_refuses_missing_original_storage_without_recreating_it() {
    for missing in ["pi-owner.v1", "stream"] {
        let directory = tempfile::tempdir().unwrap();
        let (_service_lock, resources, state) = prepare_service(directory.path(), false).await;
        let session = resources.session_directory(&state.root_session_id);
        let original = session.join(missing);
        fs::rename(&original, session.join(format!("withheld-{missing}"))).unwrap();

        let Err(error) = SessionController::open(session.clone(), &resources, &work()).await else {
            panic!("recovery accepted missing original {missing}");
        };
        if missing == "stream" {
            assert!(matches!(error, acyclic_harness::Error::Invalid(_)));
        } else {
            assert!(matches!(error, acyclic_harness::Error::Storage(_)));
        }
        assert!(!original.exists());
        assert!(!state.roots.values().next().unwrap().path.exists());
        assert_eq!(resources.shared_roots.live_roots().await, 0);
        assert_eq!(
            serde_json::to_value(load_saved_state(&session).unwrap()).unwrap(),
            serde_json::to_value(&state).unwrap()
        );
        resources.shared_roots.collection.stop().await;
    }
}

#[tokio::test]
async fn pi_registration_failure_releases_catalog_reservation_without_replacing_owner() {
    let directory = tempfile::tempdir().unwrap();
    let selected = directory.path().join("selected");
    fs::create_dir(&selected).unwrap();
    fs::write(selected.join("user-file"), b"native original").unwrap();
    let (_service_lock, resources, sessions) = open_service_resources(directory.path().join("service")).await;
    assert!(sessions.is_empty());
    let session = resources.session_directory("pi-corrupt-original");
    fs::create_dir_all(&session).unwrap();
    let owner_file = session.join("pi-owner.v1");
    let corrupt = b"retained original owner is corrupt";
    fs::write(&owner_file, corrupt).unwrap();
    let service = ConcurrentServiceControl::from_parts(resources, BTreeMap::new(), &work()).unwrap();
    for _ in 0..2 {
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            service.start_session("pi-corrupt-original", "pi", selected.clone(), json!({}), &mut None),
        ).await.expect("a failed opening must not retain an unnotified catalog waiter");
        assert!(result.is_err());
        assert_eq!(fs::read(&owner_file).unwrap(), corrupt);
        assert!(load_saved_state(&session).unwrap().root_session_id.is_empty());
        assert_eq!(service.shared_roots.live_roots().await, 0);
        assert_eq!(fs::read(selected.join("user-file")).unwrap(), b"native original");
    }
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn pi_service_recovery_rejects_another_valid_owner_with_matching_route_ids() {
    let directory = tempfile::tempdir().unwrap();
    let (_service_lock, resources, state) = prepare_service(&directory.path().join("original"), true).await;
    let (_foreign_service_lock, foreign, foreign_state) = prepare_service(&directory.path().join("foreign"), false).await;
    assert_eq!(
        serde_json::to_value(&state.task_binding).unwrap(),
        serde_json::to_value(&foreign_state.task_binding).unwrap()
    );
    let session = resources.session_directory(&state.root_session_id);
    let owner_path = session.join("pi-owner.v1");
    let original_bytes = fs::read(&owner_path).unwrap();
    let foreign_bytes = fs::read(
        foreign
            .session_directory(&foreign_state.root_session_id)
            .join("pi-owner.v1"),
    )
    .unwrap();
    assert_ne!(original_bytes, foreign_bytes);
    fs::write(&owner_path, foreign_bytes).unwrap();
    let replacement = crate::pi_owner::PiOwnerConfig::load_readonly_with_native_work(
        &resources.private_state_root, resources.private_session_data(&session).unwrap(), &work(),
    ).unwrap();
    let PluginTaskBinding::Pi(binding) = &state.task_binding else {
        unreachable!()
    };
    replacement.validate_root(binding, &work()).unwrap();
    assert!(
        SessionController::open(session.clone(), &resources, &work())
            .await
            .is_err()
    );
    assert!(!state.roots.values().next().unwrap().path.exists());
    assert_eq!(resources.shared_roots.live_roots().await, 0);
    assert_eq!(
        serde_json::to_value(load_saved_state(&session).unwrap()).unwrap(),
        serde_json::to_value(&state).unwrap()
    );

    fs::write(owner_path, original_bytes).unwrap();
    let mut recovered = SessionController::open(session, &resources, &work()).await.unwrap();
    let observed = recovered.dispatch(status_request(
        directory.path().to_path_buf(),
        ControlCommand::Agents,
    ), &mut None)
        .await
        .unwrap();
    assert_eq!(observed.0.pointer("/tasks/0/state").unwrap(), "reserved");
    assert_eq!(
        observed.0.pointer("/tasks/0/task").unwrap(),
        &serde_json::to_value(binding.task_id).unwrap()
    );
    drop(recovered);
    resources.shared_roots.collection.stop().await;
    foreign.shared_roots.collection.stop().await;
}

#[tokio::test]
async fn pi_original_root_registration_precedes_selected_allocation_and_reuses_admission() {
    let directory = tempfile::tempdir().unwrap();
    let project = directory.path().join("project");
    fs::create_dir(&project).unwrap();
    fs::write(project.join("user.txt"), b"unchanged native content").unwrap();
    let (_service_lock, resources, existing) = open_service_resources(directory.path().join("service")).await;
    assert!(existing.is_empty());
    let work = work();
    let prepared = pi_registration::register(&resources, "pi-registered-root", project.clone(), None, &work).await.unwrap();
    let before = prepared.state();
    assert!(before.pending_root_registration);
    assert!(before.roots.values().all(|root| matches!(root.allocation, RootAllocation::Preparing)));
    assert!(before.require_created_roots().is_err());
    let PluginTaskBinding::Pi(original) = &before.task_binding else { panic!("original Pi binding"); };
    let original_task = original.task_id;
    assert!(matches!(original.scratch.allocation, PiScratchAllocation::Preparing));
    let original_scratch = original.scratch.volume.clone_with_native_work(&work).unwrap();
    let scratch_name = original_scratch.0.storage_name_with_native_work(&work).unwrap();
    assert!(resources.fs.open_workspace_with_native_work(
        &scratch_name.0, &work, work.cancellation(),
    ).await.is_err());
    assert_eq!(resources.shared_roots.live_roots().await, 0);
    let owner_path = resources.session_directory(&before.root_session_id).join("pi-owner.v1");
    let original_owner = fs::read(&owner_path).unwrap();
    let session_key = work.display(&before.root_session_id).unwrap();
    let service = ConcurrentServiceControl::from_parts(resources,
        BTreeMap::from([(session_key.0, prepared)]), &work).unwrap();
    let (first, status) = service.start_session("pi-registered-root", "pi", project.clone(), json!({}), &mut None).await.unwrap();
    assert_eq!(status.0.pointer("/tasks/0/state").unwrap(), "pending");
    first.with_snapshot(|snapshot| {
        let crate::service::SessionSnapshot::Pi {
            original_task_id, pending_root_registration, roots_created, scratch_created, ..
        } = snapshot.expect("original Pi snapshot") else { panic!("original Pi snapshot"); };
        assert_eq!(*original_task_id, original_task);
        assert!(!*pending_root_registration);
        assert!(*roots_created);
        assert!(*scratch_created);
    }).unwrap();
    assert!(service.fs.open_workspace_with_native_work(
        &scratch_name.0, &work, work.cancellation(),
    ).await.is_ok());
    drop(scratch_name);
    drop(original_scratch);
    assert_eq!(service.shared_roots.live_roots().await, 1);
    let (_, repeated) = service.start_session("pi-registered-root", "pi", project.clone(), json!({}), &mut None).await.unwrap();
    assert_eq!(status.0, repeated.0);
    assert_eq!(original_owner, fs::read(&owner_path).unwrap());
    assert_eq!(fs::read(project.join("user.txt")).unwrap(), b"unchanged native content");
    let replacement = directory.path().join("replacement");
    fs::create_dir(&replacement).unwrap();
    assert!(service.start_session("pi-registered-root", "pi", replacement, json!({}), &mut None).await.is_err());
    let cancelled = first.dispatch(ControlRequest {
        version: 1, command: ControlCommand::Hook, cwd: project,
        argv: Vec::new(), name: "pi:TaskCancel".to_owned(),
        arguments: json!({"session_id": "pi-registered-root", "task": original_task,
            "caller":status.0.get("caller").unwrap()}),
    }, &mut None).await.unwrap();
    assert_eq!(cancelled.0.pointer("/tasks/0/state").unwrap(), "outcome");
    assert_eq!(original_owner, fs::read(owner_path).unwrap());
    service.shutdown().await.unwrap();
    drop(service);
    drop(session_key.1);
}

#[tokio::test]
async fn interrupted_pi_registration_never_regenerates_missing_original_owner() {
    let directory = tempfile::tempdir().unwrap();
    let selected = directory.path().join("selected");
    fs::create_dir(&selected).unwrap();
    let (_service_lock, resources, sessions) = open_service_resources(directory.path().join("service")).await;
    assert!(sessions.is_empty());
    let session = resources.session_directory("pi-interrupted-original");
    fs::create_dir_all(&session).unwrap();
    let stream = LocalStream::open(session.join("stream"), LocalStreamLimits::default()).await.unwrap();
    let service = ConcurrentServiceControl::from_parts(resources, BTreeMap::new(), &work()).unwrap();
    let error = service.start_session("pi-interrupted-original", "pi", selected, json!({}), &mut None).await
        .err().expect("missing original owner cannot be bootstrapped again");
    assert!(matches!(error, acyclic_harness::Error::Invalid(_) | acyclic_harness::Error::Storage(_)));
    assert!(!session.join("pi-owner.v1").exists());
    assert!(session.join("stream").is_dir());
    assert_eq!(service.shared_roots.live_roots().await, 0);
    service.shutdown().await.unwrap();
    drop(stream);
}

#[test]
fn pi_scoped_native_response_keeps_original_ledger_and_exact_caller_identity() {
    let work = work();
    let caller = acyclic_harness::TaskId::from_bytes([7; 16]);
    for acknowledgement in [false, true] {
        let original = crate::pi_root_provider::guarded_pi_value(
            &json!({"operation":"sdk-original","files":["selected/a","selected/b"]}),
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64, &work,
        ).unwrap();
        let response = if acknowledgement {
            crate::service_session::guarded_scoped_reply("root-original", "native-child-original", caller,
                (crate::service_session::PiAcknowledgement { acknowledgement: original.0 }, original.1))
        } else {
            crate::service_session::guarded_scoped_reply("root-original", "native-child-original", caller, original)
        }.unwrap();
        assert_eq!(response.0["session"], "root-original");
        assert_eq!(response.0["native_session"], "native-child-original");
        assert_eq!(response.0["caller_task"], serde_json::to_value(caller).unwrap());
        assert!(response.0.get("tasks").is_none(), "finite replies must not invent a graph");
        let body = if acknowledgement { &response.0["acknowledgement"] } else { &response.0 };
        assert_eq!(body["operation"], "sdk-original");
        assert_eq!(body["files"], json!(["selected/a", "selected/b"]));
        assert!(response.1.as_ref().unwrap().work().shares_ledger(&work));
        drop(response);
    }
}

#[test]
fn pi_scoped_native_response_cannot_reset_an_exhausted_original_quota() {
    let work = work();
    let original = crate::pi_root_provider::guarded_pi_value(
        &json!({"operation":"already-owned-sdk-body"}),
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64, &work,
    ).unwrap();
    let mut blocker = work.retain_owned(0).unwrap();
    let remaining = work.remaining().unwrap().peak_allocation_bytes;
    blocker.resize_retained(remaining).unwrap();
    let error = crate::service_session::guarded_scoped_reply(
        "root-original", "native-original", acyclic_harness::TaskId::from_bytes([8; 16]),
        original,
    ).unwrap_err();
    let acyclic_harness::Error::NativeWork {
        cause: acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::BudgetExceeded { .. }),
        custody: Some(error_custody),
    } = &error else { panic!("original native quota denial lost its typed cause/custody"); };
    assert!(error_custody.shares_ledger(&blocker));
    assert_eq!(blocker.bytes(), remaining);
}

#[test]
fn pi_original_query_span_is_reused_without_resetting_cancellation_or_counters() {
    let mut span = None;
    let work = crate::service_session::PiSession::query_work(&mut span).unwrap();
    let reused = crate::service_session::PiSession::query_work(&mut span).unwrap();
    assert!(reused.shares_ledger(&work));
    let before = work.receipt();
    work.cancellation().cancel();
    let reused = crate::service_session::PiSession::query_work(&mut span).unwrap();
    assert!(matches!(crate::pi_root_provider::guarded_pi_value(
        &"must-not-use-a-fresh-query-cap", 4096, &reused,
    ), Err(acyclic_harness::Error::NativeWork {
        cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
    })));
    assert_eq!(work.receipt(), before);
}
