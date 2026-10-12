use super::*;
use crate::pi_registration;
use crate::{
    AdapterState, ConcurrentServiceControl, ControlCommand, ControlRequest, PiScratchAllocation,
    PluginTaskBinding, RootAllocation, RootBinding, ServiceResources, SessionController, Survives,
    load_saved_state, save_state,
};
use acyclic_harness::{
    distributed::{WorkPull, Worker},
    runtime::TaskStateProvider,
};
use acyclic_stream::{LocalStream, LocalStreamLimits};
use std::fs;
use std::{collections::BTreeMap, path::PathBuf};

async fn resources(data: &Path) -> (crate::service_process::ServiceLock, ServiceResources) {
    let (lock, configured, private, work) = crate::tests::configured_test_service_bootstrap(data)
        .await
        .unwrap();
    let (resources, _) = ServiceResources::open(data.to_path_buf(), configured, private, &work)
        .await
        .unwrap();
    (lock, resources)
}

async fn prepare(
    data: &Path,
) -> (
    crate::service_process::ServiceLock,
    ServiceResources,
    AdapterState,
    crate::pi_runtime::PiTaskRuntime<LocalStream>,
) {
    let (lock, resources) = resources(data).await;
    let session = resources.session_directory("pi-root-input");
    fs::create_dir_all(&session).unwrap();
    let (owner, filesystem, request) =
        root_input_fixture(&session, &resources.fs, root_policy()).await;
    let work = work();
    let command = stage_root_command(&owner, &filesystem, &request, &work)
        .await
        .unwrap();
    let stream = Arc::new(
        LocalStream::open(session.join("stream"), LocalStreamLimits::default())
            .await
            .unwrap(),
    );
    let runtime = crate::pi_runtime::PiTaskRuntime::open_with_native_work(
        stream,
        Arc::new(filesystem),
        &owner.policy.journal,
        owner.authority_with_native_work(&work).unwrap(),
        &work,
    )
    .await
    .unwrap();
    runtime
        .admit_root(&request.binding, &request.root, &command.0, &work)
        .await
        .unwrap();
    let PiRootIntent::Project {
        context_id,
        root_id,
        paths,
    } = request.root
    else {
        unreachable!()
    };
    let state = AdapterState {
        version: crate::ADAPTER_STATE_VERSION,
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
    save_state(&session, &state, Survives::PowerLoss).unwrap();
    (lock, resources, state, runtime)
}

fn request(path: &Path, command: ControlCommand) -> ControlRequest {
    ControlRequest {
        version: 1,
        command,
        cwd: path.into(),
        argv: Vec::new(),
        name: String::new(),
        arguments: serde_json::Value::Null,
    }
}

#[tokio::test]
async fn cold_reopen_observes_pending_and_reserved_without_native_access() {
    for reserved in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("service");
        let (_lock, resources, state, runtime) = prepare(&data).await;
        let PluginTaskBinding::Pi(binding) = &state.task_binding else {
            unreachable!()
        };
        if reserved {
            let WorkPull::Claimed(lease) = runtime
                .runtime
                .task_host()
                .pull_work(&Worker {
                    id: "pi-service-original".into(),
                    available: Default::default(),
                    labels: Default::default(),
                })
                .await
                .unwrap()
            else {
                panic!("original lease")
            };
            runtime
                .runtime
                .task_host()
                .start_task(&lease, None)
                .await
                .unwrap();
        }
        drop(runtime);
        let private = resources.private_state_root.clone();
        let session = resources.session_directory(&state.root_session_id);
        let controller = SessionController::open(session.clone(), &resources, &work())
            .await
            .unwrap();
        let service = ConcurrentServiceControl::from_parts(
            resources,
            BTreeMap::from([(state.root_session_id.clone(), controller)]),
            &work(),
        )
        .unwrap();
        let expected = json!({"session":state.root_session_id,"native_session":binding.native_session_id,"tasks":[{"state":if reserved {"reserved"} else {"pending"},"task":binding.task_id}]});
        for _ in 0..2 {
            let handle = service.session(&state.root_session_id).await.unwrap();
            assert_eq!(
                handle
                    .dispatch(request(directory.path(), ControlCommand::Agents), &mut None)
                    .await
                    .unwrap()
                    .0,
                expected
            );
            for command in [
                ControlCommand::Git,
                ControlCommand::Discard,
                ControlCommand::Hook,
            ] {
                assert!(
                    handle
                        .dispatch(request(directory.path(), command), &mut None)
                        .await
                        .is_err()
                );
            }
            assert!(!state.roots.values().next().unwrap().path.exists());
        }
        service.shutdown().await.unwrap();
        assert_eq!(
            serde_json::to_value(load_saved_state(&session).unwrap()).unwrap(),
            serde_json::to_value(&state).unwrap()
        );
        let configured = ServiceResources::configure(&data).await.unwrap();
        let reopened = ConcurrentServiceControl::open(data, configured, private, &work())
            .await
            .unwrap();
        assert_eq!(
            reopened
                .session(&state.root_session_id)
                .await
                .unwrap()
                .dispatch(request(directory.path(), ControlCommand::Agents), &mut None)
                .await
                .unwrap()
                .0,
            expected
        );
        assert!(
            reopened
                .route_session(directory.path())
                .await
                .unwrap()
                .is_none()
        );
        reopened.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn deactivation_cancels_before_persisting_inactive_state() {
    let directory = tempfile::tempdir().unwrap();
    let (_lock, resources, state, runtime) = prepare(directory.path()).await;
    let PluginTaskBinding::Pi(binding) = &state.task_binding else {
        unreachable!()
    };
    let WorkPull::Claimed(lease) = runtime
        .runtime
        .task_host()
        .pull_work(&Worker {
            id: "pi-service-original".into(),
            available: Default::default(),
            labels: Default::default(),
        })
        .await
        .unwrap()
    else {
        panic!("original lease")
    };
    runtime
        .runtime
        .task_host()
        .start_task(&lease, None)
        .await
        .unwrap();
    drop(runtime);
    let private = resources.private_state_root.clone();
    let session = resources.session_directory(&state.root_session_id);
    let controller = SessionController::open(session.clone(), &resources, &work())
        .await
        .unwrap();
    let service = ConcurrentServiceControl::from_parts(
        resources,
        BTreeMap::from([(state.root_session_id.clone(), controller)]),
        &work(),
    )
    .unwrap();
    service.begin_drain();
    service.shutdown().await.unwrap();
    assert!(!load_saved_state(&session).unwrap().active);
    let configured = ServiceResources::configure(directory.path()).await.unwrap();
    let (resources, sessions) =
        ServiceResources::open(directory.path().into(), configured, private, &work())
            .await
            .unwrap();
    assert!(sessions.is_empty());
    let mut controller = SessionController::open(session, &resources, &work())
        .await
        .unwrap();
    let status = controller
        .dispatch(request(directory.path(), ControlCommand::Agents), &mut None)
        .await
        .unwrap();
    assert_eq!(status.0["tasks"][0]["state"], "unresolved");
    assert_eq!(
        status.0["tasks"][0]["task"],
        serde_json::to_value(binding.task_id).unwrap()
    );
    assert!(!state.roots.values().next().unwrap().path.exists());
    resources.shared_roots.collection.stop().await;
}

#[tokio::test]
async fn recovery_preserves_missing_storage_and_rejects_a_foreign_valid_owner() {
    for missing in ["pi-owner.v1", "stream"] {
        let directory = tempfile::tempdir().unwrap();
        let (_lock, resources, state, runtime) = prepare(directory.path()).await;
        drop(runtime);
        let session = resources.session_directory(&state.root_session_id);
        let path = session.join(missing);
        fs::rename(&path, session.join(format!("withheld-{missing}"))).unwrap();
        assert!(
            SessionController::open(session.clone(), &resources, &work())
                .await
                .is_err()
        );
        assert!(!path.exists());
        assert!(!state.roots.values().next().unwrap().path.exists());
        assert_eq!(
            serde_json::to_value(load_saved_state(&session).unwrap()).unwrap(),
            serde_json::to_value(&state).unwrap()
        );
        resources.shared_roots.collection.stop().await;
    }
    let directory = tempfile::tempdir().unwrap();
    let (_lock, resources, state, runtime) = prepare(&directory.path().join("original")).await;
    let PluginTaskBinding::Pi(binding) = &state.task_binding else {
        unreachable!()
    };
    let WorkPull::Claimed(lease) = runtime
        .runtime
        .task_host()
        .pull_work(&Worker {
            id: "pi-service-original".into(),
            available: Default::default(),
            labels: Default::default(),
        })
        .await
        .unwrap()
    else {
        panic!("original lease")
    };
    runtime
        .runtime
        .task_host()
        .start_task(&lease, None)
        .await
        .unwrap();
    drop(runtime);
    let (_foreign_lock, foreign, other, runtime) = prepare(&directory.path().join("foreign")).await;
    drop(runtime);
    assert_eq!(
        serde_json::to_value(&state.task_binding).unwrap(),
        serde_json::to_value(&other.task_binding).unwrap()
    );
    let session = resources.session_directory(&state.root_session_id);
    let path = session.join("pi-owner.v1");
    let bytes = fs::read(&path).unwrap();
    fs::write(
        &path,
        fs::read(
            foreign
                .session_directory(&other.root_session_id)
                .join("pi-owner.v1"),
        )
        .unwrap(),
    )
    .unwrap();
    PiOwnerConfig::load_readonly_with_native_work(
        &resources.private_state_root,
        resources.private_session_data(&session).unwrap(),
        &work(),
    )
    .unwrap()
    .validate_root(binding, &work())
    .unwrap();
    assert!(
        SessionController::open(session.clone(), &resources, &work())
            .await
            .is_err()
    );
    assert!(!state.roots.values().next().unwrap().path.exists());
    assert_eq!(
        serde_json::to_value(load_saved_state(&session).unwrap()).unwrap(),
        serde_json::to_value(&state).unwrap()
    );
    fs::write(path, bytes).unwrap();
    let mut recovered = SessionController::open(session, &resources, &work())
        .await
        .unwrap();
    let status = recovered
        .dispatch(request(directory.path(), ControlCommand::Agents), &mut None)
        .await
        .unwrap();
    assert_eq!(
        status.0["tasks"][0]["task"],
        serde_json::to_value(binding.task_id).unwrap()
    );
    assert_eq!(status.0["tasks"][0]["state"], "reserved");
    resources.shared_roots.collection.stop().await;
    foreign.shared_roots.collection.stop().await;
}

#[tokio::test]
async fn failed_registration_releases_waiters_and_never_replaces_credentials() {
    for retained in ["corrupt", "stream"] {
        let directory = tempfile::tempdir().unwrap();
        let selected = directory.path().join("selected");
        fs::create_dir(&selected).unwrap();
        fs::write(selected.join("user-file"), b"native original").unwrap();
        let (_lock, resources) = resources(&directory.path().join("service")).await;
        let session = resources.session_directory("interrupted");
        fs::create_dir_all(&session).unwrap();
        let original = if retained == "corrupt" {
            fs::write(session.join("pi-owner.v1"), b"corrupt original owner").unwrap();
            None
        } else {
            Some(
                LocalStream::open(session.join("stream"), LocalStreamLimits::default())
                    .await
                    .unwrap(),
            )
        };
        let service =
            ConcurrentServiceControl::from_parts(resources, BTreeMap::new(), &work()).unwrap();
        for _ in 0..2 {
            assert!(
                tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    service.start_session(
                        "interrupted",
                        "pi",
                        selected.clone(),
                        json!({}),
                        &mut None
                    )
                )
                .await
                .unwrap()
                .is_err()
            );
            if retained == "corrupt" {
                assert_eq!(
                    fs::read(session.join("pi-owner.v1")).unwrap(),
                    b"corrupt original owner"
                );
            } else {
                assert!(!session.join("pi-owner.v1").exists());
            }
            assert_eq!(
                fs::read(selected.join("user-file")).unwrap(),
                b"native original"
            );
        }
        service.shutdown().await.unwrap();
        drop(original);
    }
}

#[tokio::test]
async fn registration_reuses_original_admission_before_allocating_selected_roots() {
    let directory = tempfile::tempdir().unwrap();
    let project = directory.path().join("project");
    fs::create_dir(&project).unwrap();
    fs::write(project.join("user.txt"), b"unchanged native content").unwrap();
    let (_lock, resources) = resources(&directory.path().join("service")).await;
    let work = work();
    let prepared =
        pi_registration::register(&resources, "registered", project.clone(), None, &work)
            .await
            .unwrap();
    let state = prepared.state().clone();
    assert!(state.pending_root_registration);
    assert!(state.require_created_roots().is_err());
    let PluginTaskBinding::Pi(binding) = &state.task_binding else {
        unreachable!()
    };
    assert!(matches!(
        binding.scratch.allocation,
        PiScratchAllocation::Preparing
    ));
    let scratch = binding.scratch.volume.storage_name(&mut None).unwrap();
    assert!(
        resources
            .fs
            .open_workspace_with_native_work(&scratch, &work, work.cancellation())
            .await
            .is_err()
    );
    let owner = resources
        .session_directory(&state.root_session_id)
        .join("pi-owner.v1");
    let bytes = fs::read(&owner).unwrap();
    let service = ConcurrentServiceControl::from_parts(
        resources,
        BTreeMap::from([(state.root_session_id.clone(), prepared)]),
        &work,
    )
    .unwrap();
    let (first, status) = service
        .start_session("registered", "pi", project.clone(), json!({}), &mut None)
        .await
        .unwrap();
    assert_eq!(status.0["tasks"][0]["state"], "pending");
    assert!(
        service
            .fs
            .open_workspace_with_native_work(&scratch, &work, work.cancellation())
            .await
            .is_ok()
    );
    assert_eq!(
        status.0,
        service
            .start_session("registered", "pi", project.clone(), json!({}), &mut None)
            .await
            .unwrap()
            .1
            .0
    );
    let replacement = directory.path().join("replacement");
    fs::create_dir(&replacement).unwrap();
    assert!(
        service
            .start_session("registered", "pi", replacement, json!({}), &mut None)
            .await
            .is_err()
    );
    let cancelled = first.dispatch(ControlRequest { version: 1, command: ControlCommand::Hook, cwd: project.clone(), argv: Vec::new(), name: "pi:TaskCancel".into(), arguments: json!({"session_id":"registered","task":binding.task_id,"caller":status.0["caller"]}) }, &mut None).await.unwrap();
    assert_eq!(cancelled.0["tasks"][0]["state"], "outcome");
    assert_eq!(fs::read(owner).unwrap(), bytes);
    assert_eq!(
        fs::read(project.join("user.txt")).unwrap(),
        b"unchanged native content"
    );
    service.shutdown().await.unwrap();
}

#[test]
fn scoped_response_reuses_original_identity_and_exhausted_quota() {
    let work = work();
    let caller = acyclic_harness::TaskId::from_bytes([7; 16]);
    for acknowledgement in [false, true] {
        let original = crate::pi_root_provider::guarded_pi_value(
            &json!({"operation":"sdk-original","files":["selected/a","selected/b"]}),
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
            &work,
        )
        .unwrap();
        let response = if acknowledgement {
            crate::service_session::guarded_scoped_reply(
                "root-original",
                "native-original",
                caller,
                (
                    crate::service_session::PiAcknowledgement {
                        acknowledgement: original.0,
                    },
                    original.1,
                ),
            )
        } else {
            crate::service_session::guarded_scoped_reply(
                "root-original",
                "native-original",
                caller,
                original,
            )
        }
        .unwrap();
        assert_eq!(response.0["session"], "root-original");
        assert_eq!(response.0["native_session"], "native-original");
        assert_eq!(
            response.0["caller_task"],
            serde_json::to_value(caller).unwrap()
        );
        let body = if acknowledgement {
            &response.0["acknowledgement"]
        } else {
            &response.0
        };
        assert_eq!(body["files"], json!(["selected/a", "selected/b"]));
        assert_eq!(body["operation"], "sdk-original");
        assert!(response.0.get("tasks").is_none());
    }
    let original = crate::pi_root_provider::guarded_pi_value(
        &json!({"operation":"already-owned-sdk-body"}),
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
        &work,
    )
    .unwrap();
    let mut blocker = work
        .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
        .unwrap();
    blocker
        .resize_retained(work.remaining().unwrap().peak_allocation_bytes)
        .unwrap();
    assert!(
        crate::service_session::guarded_scoped_reply(
            "root-original",
            "native-original",
            caller,
            original
        )
        .is_err()
    );
    let mut span = None;
    let query = crate::service_session::PiSession::query_work(&mut span).unwrap();
    query.cancellation().cancel();
    let reused = crate::service_session::PiSession::query_work(&mut span).unwrap();
    assert!(
        crate::pi_root_provider::guarded_pi_value(&"must-not-use-a-fresh-query-cap", 4096, &reused)
            .is_err()
    );
}
