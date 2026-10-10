//! Actual service catalog/actor recovery without native allocation.

use super::*;
use crate::{
    ADAPTER_STATE_VERSION, AdapterState, ConcurrentServiceControl, ControlCommand, ControlRequest,
    PiScratchAllocation, PluginTaskBinding, RootAllocation, RootBinding, ServiceResources, SessionController, Survives,
    load_saved_state, save_state,
};
use acyclic_harness::distributed::{WorkPull, Worker};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use serde_json::Value;
use std::{collections::BTreeMap, path::PathBuf};

async fn prepare_service(data: &Path, reserve: bool) -> (ServiceResources, AdapterState) {
    let (resources, sessions) = ServiceResources::open(data.to_path_buf()).await.unwrap();
    assert!(sessions.is_empty());
    let directory = resources.session_directory("pi-root-input");
    fs::create_dir_all(&directory).unwrap();
    let (owner, filesystem, request, raw) =
        root_input_fixture_on_fs(&directory, resources.fs.clone(), |_| {}).await;
    let command = stage_root_command(&owner, &filesystem, &request)
        .await
        .unwrap();
    let filesystem = Arc::new(filesystem);
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(directory.join("stream"), LocalStreamLimits::default())
            .await
            .unwrap(),
    ));
    let runtime = crate::pi_runtime::PiTaskRuntime::open(
        stream,
        filesystem,
        owner.policy.journal.clone(),
        owner.authority().unwrap(),
    )
    .await
    .unwrap();
    runtime
        .admit_root(request.binding.clone(), request.root.clone(), command)
        .await
        .unwrap();
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
            .start_task(&lease)
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
    (resources, state)
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
        let (resources, state) = prepare_service(&data, reserved).await;
        let session = resources.session_directory(&state.root_session_id);
        let controller = SessionController::open(session.clone(), &resources)
            .await
            .unwrap();
        let service = ConcurrentServiceControl::from_parts(
            resources,
            BTreeMap::from([(state.root_session_id.clone(), controller)]),
        );
        let expected = json!({
            "session": state.root_session_id,
            "tasks": [{"state": if reserved {"reserved"} else {"pending"},
                "task": match &state.task_binding {PluginTaskBinding::Pi(binding) => binding.task_id, _ => unreachable!()}}],
        });
        for _ in 0..2 {
            let handle = service.session(&state.root_session_id).await.unwrap();
            assert_eq!(
                handle
                    .dispatch(status_request(
                        directory.path().to_path_buf(),
                        ControlCommand::Agents
                    ))
                    .await
                    .unwrap(),
                expected
            );
            for command in [
                ControlCommand::Git,
                ControlCommand::Discard,
                ControlCommand::Hook,
            ] {
                assert!(
                    handle
                        .dispatch(status_request(directory.path().to_path_buf(), command))
                        .await
                        .unwrap_err()
                        .contains("owning task runtime")
                );
            }
            assert_eq!(service.shared_roots.live_roots().await, 0);
            assert!(!state.roots.values().next().unwrap().path.exists());
        }
        service.shutdown().await.unwrap();
        assert_eq!(
            serde_json::to_value(load_saved_state(&session).unwrap()).unwrap(),
            serde_json::to_value(&state).unwrap()
        );
        let reopened = ConcurrentServiceControl::open(data).await.unwrap();
        let handle = reopened.session(&state.root_session_id).await.unwrap();
        assert_eq!(
            handle
                .dispatch(status_request(
                    directory.path().to_path_buf(),
                    ControlCommand::Agents
                ))
                .await
                .unwrap(),
            expected
        );
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
    let (resources, state) = prepare_service(&data, true).await;
    let session = resources.session_directory(&state.root_session_id);
    let controller = SessionController::open(session.clone(), &resources)
        .await
        .unwrap();
    let service = ConcurrentServiceControl::from_parts(
        resources,
        BTreeMap::from([(state.root_session_id.clone(), controller)]),
    );
    service.begin_drain();
    service.shutdown().await.unwrap();
    let saved = load_saved_state(&session).unwrap();
    assert!(!saved.active);
    let (resources, sessions) = ServiceResources::open(data).await.unwrap();
    assert!(sessions.is_empty());
    let mut controller = SessionController::open(session, &resources).await.unwrap();
    let status = controller
        .dispatch(status_request(
            directory.path().to_path_buf(),
            ControlCommand::Agents,
        ))
        .await
        .unwrap();
    assert_eq!(status.pointer("/tasks/0/state").unwrap(), "unresolved");
    let PluginTaskBinding::Pi(binding) = &state.task_binding else {
        unreachable!();
    };
    assert_eq!(
        status.pointer("/tasks/0/task").unwrap(),
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
        let (resources, state) = prepare_service(directory.path(), false).await;
        let session = resources.session_directory(&state.root_session_id);
        let original = session.join(missing);
        fs::rename(&original, session.join(format!("withheld-{missing}"))).unwrap();

        let Err(error) = SessionController::open(session.clone(), &resources).await else {
            panic!("recovery accepted missing original {missing}");
        };
        assert!(error.contains(if missing == "stream" {
            "original task provider is missing"
        } else {
            "original owner:"
        }));
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
    let (resources, sessions) = ServiceResources::open(directory.path().join("service")).await.unwrap();
    assert!(sessions.is_empty());
    let session = resources.session_directory("pi-corrupt-original");
    fs::create_dir_all(&session).unwrap();
    let owner_file = session.join("pi-owner.v1");
    let corrupt = b"retained original owner is corrupt";
    fs::write(&owner_file, corrupt).unwrap();
    let service = ConcurrentServiceControl::from_parts(resources, BTreeMap::new());
    for _ in 0..2 {
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            service.start_session("pi-corrupt-original", "pi", selected.clone()),
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
    let (resources, state) = prepare_service(&directory.path().join("original"), true).await;
    let (foreign, foreign_state) = prepare_service(&directory.path().join("foreign"), false).await;
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
    // The replacement is a valid protected policy with the same route IDs;
    // only the original task admission can distinguish its signing key.
    let replacement = crate::pi_owner::PiOwnerConfig::load(&session).unwrap();
    let PluginTaskBinding::Pi(binding) = &state.task_binding else {
        unreachable!()
    };
    replacement.validate_root(binding).unwrap();
    assert!(
        SessionController::open(session.clone(), &resources)
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
    let mut recovered = SessionController::open(session, &resources).await.unwrap();
    let observed = recovered
        .dispatch(status_request(
            directory.path().to_path_buf(),
            ControlCommand::Agents,
        ))
        .await
        .unwrap();
    assert_eq!(observed.pointer("/tasks/0/state").unwrap(), "reserved");
    assert_eq!(
        observed.pointer("/tasks/0/task").unwrap(),
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
    let service = ConcurrentServiceControl::open(directory.path().join("service")).await.unwrap();
    let (first, status) = service.start_session("pi-registered-root", "pi", project.clone()).await.unwrap();
    assert_eq!(status.pointer("/tasks/0/state").unwrap(), "pending");
    let state = first.snapshot().unwrap().unwrap();
    let owner_path = service.session_directory(&state.root_session_id).join("pi-owner.v1");
    let original_owner = fs::read(&owner_path).unwrap();
    assert!(state.pending_root_registration);
    assert!(state.roots.values().all(|root| matches!(root.allocation, RootAllocation::Preparing)));
    assert!(state.require_created_roots().is_err());
    let PluginTaskBinding::Pi(binding) = &state.task_binding else { panic!("original Pi binding"); };
    assert!(matches!(binding.scratch.allocation, PiScratchAllocation::Preparing));
    assert!(service.fs.open_workspace(binding.scratch.volume.storage_name().unwrap()).await.is_err());
    assert_eq!(service.shared_roots.live_roots().await, 0);
    let (second, repeated) = service.start_session("pi-registered-root", "pi", project.clone()).await.unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(status, repeated);
    assert_eq!(original_owner, fs::read(&owner_path).unwrap());
    assert_eq!(fs::read(project.join("user.txt")).unwrap(), b"unchanged native content");
    let replacement = directory.path().join("replacement");
    fs::create_dir(&replacement).unwrap();
    assert!(service.start_session("pi-registered-root", "pi", replacement).await.is_err());
    let cancelled = first.dispatch(ControlRequest {
        version: 1, command: ControlCommand::Hook, cwd: project,
        argv: Vec::new(), name: "pi:TaskCancel".to_owned(),
        arguments: json!({"session_id": state.root_session_id, "task": binding.task_id}),
    }).await.unwrap();
    assert_eq!(cancelled.pointer("/tasks/0/state").unwrap(), "outcome");
    assert_eq!(original_owner, fs::read(owner_path).unwrap());
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn interrupted_pi_registration_never_regenerates_missing_original_owner() {
    let directory = tempfile::tempdir().unwrap();
    let selected = directory.path().join("selected");
    fs::create_dir(&selected).unwrap();
    let (resources, sessions) = ServiceResources::open(directory.path().join("service")).await.unwrap();
    assert!(sessions.is_empty());
    let session = resources.session_directory("pi-interrupted-original");
    fs::create_dir_all(&session).unwrap();
    let stream = LocalStream::open(session.join("stream"), LocalStreamLimits::default()).await.unwrap();
    let service = ConcurrentServiceControl::from_parts(resources, BTreeMap::new());
    let error = service.start_session("pi-interrupted-original", "pi", selected).await
        .err().expect("missing original owner cannot be bootstrapped again");
    assert!(error.contains("owner credential is missing"));
    assert!(!session.join("pi-owner.v1").exists());
    assert!(session.join("stream").is_dir());
    assert_eq!(service.shared_roots.live_roots().await, 0);
    service.shutdown().await.unwrap();
    drop(stream);
}
