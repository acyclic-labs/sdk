//! Original actor recovery and native reply custody through scoped transport.

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

fn native_query_work() -> acyclic_fs::NativeMaterializationWork {
    acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new(),
    ).unwrap()
}

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
        root_input_fixture_on_fs(&directory, resources.fs.clone(), |_| {}).await;
    let command = stage_root_command(&owner, &filesystem, &request, &native_work)
        .await
        .unwrap();
    let filesystem = Arc::new(filesystem);
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(directory.join("stream"), LocalStreamLimits::default())
            .await
            .unwrap(),
    ));
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
        .admit_root_with_native_work(request.binding.clone(), request.root.clone(), command.0, &native_work)
        .await
        .unwrap();
    drop(admission.0);
    drop(admission.1);
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
        let controller = SessionController::open(session.clone(), &resources, &native_query_work())
            .await
            .unwrap();
        let service = ConcurrentServiceControl::from_parts(
            resources,
            BTreeMap::from([(state.root_session_id.clone(), controller)]),
        );
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
            let original = original_native_work.as_ref().unwrap().retain_owned(0).unwrap();
            assert!(status.1.as_ref().unwrap().shares_ledger(&original));
            for command in [
                ControlCommand::Git,
                ControlCommand::Discard,
                ControlCommand::Hook,
            ] {
                let rejection = if command == ControlCommand::Hook {
                    "Pi session refuses another host's control"
                } else {
                    "Pi workspace operations require their owning task runtime"
                };
                let error = handle.dispatch(status_request(directory.path().to_path_buf(), command), &mut None)
                    .await
                    .unwrap_err();
                assert!(matches!(error, acyclic_harness::Error::Invalid(message) if message == rejection));
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
        let reopened = ConcurrentServiceControl::open(data, configured, private_root, &native_query_work()).await.unwrap();
        let handle = reopened.session(&state.root_session_id).await.unwrap();
        let mut original_native_work = None;
        let status = handle.dispatch(status_request(
            directory.path().to_path_buf(),
            ControlCommand::Agents,
        ), &mut original_native_work)
            .await
            .unwrap();
        assert_eq!(status.0, expected);
        let original = original_native_work.as_ref().unwrap().retain_owned(0).unwrap();
        assert!(status.1.as_ref().unwrap().shares_ledger(&original));
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
    let controller = SessionController::open(session.clone(), &resources, &native_query_work())
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
    let configured = ServiceResources::configure(&data).await.unwrap();
    let (resources, sessions) = ServiceResources::open(data, configured, private_root, &native_query_work()).await.unwrap();
    assert!(sessions.is_empty());
    let mut controller = SessionController::open(session, &resources, &native_query_work()).await.unwrap();
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

        let Err(error) = SessionController::open(session.clone(), &resources, &native_query_work()).await else {
            panic!("recovery accepted missing original {missing}");
        };
        if missing == "stream" {
            assert!(matches!(error, acyclic_harness::Error::Invalid(message)
                if message.contains("original task provider is missing")));
        } else {
            let original_cause = fs::File::open(&original).unwrap_err().to_string();
            assert!(matches!(error, acyclic_harness::Error::Storage(message) if message == original_cause));
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
    let service = ConcurrentServiceControl::from_parts(resources, BTreeMap::new());
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
    // The replacement is a valid protected policy with the same route IDs;
    // only the original task admission can distinguish its signing key.
    let replacement = crate::pi_owner::PiOwnerConfig::load_readonly_with_native_work(
        &resources.private_state_root, resources.private_session_data(&session).unwrap(), &native_query_work(),
    ).unwrap();
    let PluginTaskBinding::Pi(binding) = &state.task_binding else {
        unreachable!()
    };
    replacement.validate_root(binding).unwrap();
    assert!(
        SessionController::open(session.clone(), &resources, &native_query_work())
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
    let mut recovered = SessionController::open(session, &resources, &native_query_work()).await.unwrap();
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
    let prepared = pi_registration::register(&resources, "pi-registered-root", project.clone(), None, &native_query_work()).await.unwrap();
    let before = prepared.state().clone();
    assert!(before.pending_root_registration);
    assert!(before.roots.values().all(|root| matches!(root.allocation, RootAllocation::Preparing)));
    assert!(before.require_created_roots().is_err());
    let PluginTaskBinding::Pi(original) = &before.task_binding else { panic!("original Pi binding"); };
    assert!(matches!(original.scratch.allocation, PiScratchAllocation::Preparing));
    assert!(resources.fs.open_workspace(original.scratch.volume.storage_name().unwrap()).await.is_err());
    assert_eq!(resources.shared_roots.live_roots().await, 0);
    let owner_path = resources.session_directory(&before.root_session_id).join("pi-owner.v1");
    let original_owner = fs::read(&owner_path).unwrap();
    let service = ConcurrentServiceControl::from_parts(resources,
        BTreeMap::from([(before.root_session_id.clone(), prepared)]));
    let (first, status) = service.start_session("pi-registered-root", "pi", project.clone(), json!({}), &mut None).await.unwrap();
    assert_eq!(status.0.pointer("/tasks/0/state").unwrap(), "pending");
    let state = first.snapshot().unwrap().unwrap();
    assert!(!state.pending_root_registration);
    state.require_created_roots().unwrap();
    let PluginTaskBinding::Pi(binding) = &state.task_binding else { panic!("original Pi binding"); };
    assert_eq!(binding.task_id, original.task_id);
    assert!(matches!(binding.scratch.allocation, PiScratchAllocation::Created(_)));
    assert!(service.fs.open_workspace(binding.scratch.volume.storage_name().unwrap()).await.is_ok());
    assert_eq!(service.shared_roots.live_roots().await, 1);
    let (second, repeated) = service.start_session("pi-registered-root", "pi", project.clone(), json!({}), &mut None).await.unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(status.0, repeated.0);
    assert_eq!(original_owner, fs::read(&owner_path).unwrap());
    assert_eq!(fs::read(project.join("user.txt")).unwrap(), b"unchanged native content");
    let replacement = directory.path().join("replacement");
    fs::create_dir(&replacement).unwrap();
    assert!(service.start_session("pi-registered-root", "pi", replacement, json!({}), &mut None).await.is_err());
    let cancelled = first.dispatch(ControlRequest {
        version: 1, command: ControlCommand::Hook, cwd: project,
        argv: Vec::new(), name: "pi:TaskCancel".to_owned(),
        arguments: json!({"session_id": state.root_session_id, "task": binding.task_id,
            "caller":status.0.get("caller").unwrap()}),
    }, &mut None).await.unwrap();
    assert_eq!(cancelled.0.pointer("/tasks/0/state").unwrap(), "outcome");
    assert_eq!(original_owner, fs::read(owner_path).unwrap());
    service.shutdown().await.unwrap();
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
    let service = ConcurrentServiceControl::from_parts(resources, BTreeMap::new());
    let error = service.start_session("pi-interrupted-original", "pi", selected, json!({}), &mut None).await
        .err().expect("missing original owner cannot be bootstrapped again");
    assert!(matches!(error, acyclic_harness::Error::Invalid(message) | acyclic_harness::Error::Storage(message)
        if message.contains("owner credential is missing")));
    assert!(!session.join("pi-owner.v1").exists());
    assert!(session.join("stream").is_dir());
    assert_eq!(service.shared_roots.live_roots().await, 0);
    service.shutdown().await.unwrap();
    drop(stream);
}

#[test]
fn pi_scoped_native_response_keeps_original_ledger_and_exact_caller_identity() {
    #[derive(serde::Serialize)]
    struct Body<'a> { operation: &'a str, files: [&'a str; 2] }
    let work = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new(),
    ).unwrap();
    let sentinel = work.retain_owned(0).unwrap();
    let caller = acyclic_harness::TaskId::from_bytes([7; 16]);
    for acknowledgement in [false, true] {
        let original = crate::pi_root_provider::guarded_pi_value(
            &Body { operation: "sdk-original", files: ["selected/a", "selected/b"] },
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64, &work,
        ).unwrap();
        let before = work.receipt();
        let response = crate::service_session::guarded_scoped_reply(
            "root-original", "native-child-original", caller, original, acknowledgement,
        ).unwrap();
        assert_eq!(response.0["session"], "root-original");
        assert_eq!(response.0["native_session"], "native-child-original");
        assert_eq!(response.0["caller_task"], serde_json::to_value(caller).unwrap());
        assert!(response.0.get("tasks").is_none(), "finite replies must not invent a graph");
        let body = if acknowledgement { &response.0["acknowledgement"] } else { &response.0 };
        assert_eq!(body["operation"], "sdk-original");
        assert_eq!(body["files"], json!(["selected/a", "selected/b"]));
        assert!(response.1.as_ref().unwrap().shares_ledger(&sentinel));
        assert!(work.receipt().bytes_encoded > before.bytes_encoded);
        assert!(work.receipt().bytes_copied > before.bytes_copied);
        // Final consumers own the tuple: the Value dies before its custody.
        drop(response);
    }
}

#[test]
fn pi_scoped_native_response_cannot_reset_an_exhausted_original_quota() {
    #[derive(serde::Serialize)]
    struct Body<'a> { operation: &'a str }
    let work = acyclic_fs::NativeMaterializationWork::new(
        crate::pi_root_provider::allocation_budget(), &acyclic_fs::CancellationToken::new(),
    ).unwrap();
    let original = crate::pi_root_provider::guarded_pi_value(
        &Body { operation: "already-owned-sdk-body" },
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64, &work,
    ).unwrap();
    let mut blocker = work.retain_owned(0).unwrap();
    let remaining = work.remaining().unwrap().peak_allocation_bytes;
    blocker.resize_retained(remaining).unwrap();
    let before = work.receipt();
    let error = crate::service_session::guarded_scoped_reply(
        "root-original", "native-original", acyclic_harness::TaskId::from_bytes([8; 16]),
        original, false,
    ).unwrap_err();
    let acyclic_harness::Error::NativeWork {
        cause: acyclic_fs::NativeTreeMaterializationError::Work(acyclic_fs::WorkError::BudgetExceeded { .. }),
        custody: Some(error_custody),
    } = &error else { panic!("original native quota denial lost its typed cause/custody"); };
    assert!(error_custody.shares_ledger(&blocker));
    assert_eq!(blocker.bytes(), remaining);
    assert!(work.receipt().allocation_operations >= before.allocation_operations);
    assert!(work.receipt().peak_allocation_bytes >= before.peak_allocation_bytes);
}

#[test]
fn pi_native_session_source_preserves_value_before_guard_destruction_order() {
    let session = include_str!("service_session.rs");
    let wrapper = session.split("pub(crate) fn guarded_scoped_reply<T: Serialize>(").nth(1).unwrap()
        .split("\nimpl PiSession {").next().unwrap();
    let encode = wrapper.find("pi_root_provider::guarded_pi_value(").unwrap();
    let original_work = wrapper.find("reply.1.work()").unwrap();
    let destroy_body = wrapper.find("drop(reply.0)").unwrap();
    let transfer = wrapper.find("encoded.1.absorb(&mut reply.1)").unwrap();
    let return_custody = wrapper.find("Some(encoded.1)").unwrap();
    assert!(encode < original_work && original_work < destroy_body);
    assert!(destroy_body < transfer && transfer < return_custody);
    for forbidden in ["NativeMaterializationWork::new", ".clone()", "json!(", "Map::new",
        "Mutex", "mem::forget", "Box::leak"]
    {
        assert!(!wrapper.contains(forbidden), "native wrapper must not use {forbidden}");
    }
    assert_eq!(session.matches("return guarded_scoped_reply(").count(), 5);
    let original_mail_work = session.split("async fn mail_work(").nth(1).unwrap()
        .split("async fn send_control(").next().unwrap();
    assert!(original_mail_work.contains("if caller == owner.policy().root_task"));
    let child_mail_work = original_mail_work.split("} else {").nth(1).unwrap();
    assert!(child_mail_work.contains("invocation_work(caller).await"));
    assert!(child_mail_work.contains("native_host_with_native_work(query_work)?"));
    assert!(!child_mail_work.contains("NativeMaterializationWork::new"));
    let send = session.split("async fn send_control(").nth(1).unwrap()
        .split("fn authorize_subtree(").next().unwrap();
    assert!(send.contains(".with_work(work).control_command_with_native_work(command.0).await"));
    assert!(send.contains("let command = pi_mail::stage_send("));
    assert!(send.contains("let command = pi_mail::prepare_send("));
    assert!(send.contains("fence, command.0, &work)"));
    assert!(!send.contains("_command_custody"));
    assert!(send.find("work.retain_owned(message.len()").unwrap()
        < send.find("message: message.to_owned()").unwrap());
    let operator = include_str!("pi_operator_session.rs");
    assert!(operator.contains("control_command_with_native_work(command.0).await"));
    assert!(operator.contains("let command = pi_operator_host::stage_operator_command("));
    assert!(!operator.contains("_command_custody"));
    assert!(operator.contains(".map(|(value, allocation)| (value, Some(allocation)))"));
    assert!(!operator.contains(".map(|value| (value, None))"));
    assert!(!session.contains(".control_command(command)"));
    let query_scope = session.split("pub(crate) fn query_work(").nth(1).unwrap()
        .split("async fn mail_work(").next().unwrap();
    assert!(query_scope.contains("RootError::NativeConstruction { cause, work }"));
    assert!(query_scope.find("if let Some(work) = original_native_work").unwrap()
        < query_scope.find("NativeMaterializationWork::new").unwrap());
    assert!(query_scope.contains("*original_native_work = Some(work.clone())"));
    assert!(!operator.contains("NativeMaterializationWork::new"));
    assert!(!original_mail_work.contains("NativeMaterializationWork::new"));
    assert!(!operator.contains(".map_err(display)"));
    assert!(operator.contains("native_host_with_native_work(&work)?"));
    assert!(operator.contains("let protected = native.original_owner()"));
    assert!(!operator.contains("owner.clone()"));
    assert!(operator.contains("PiSelectedHost::new_with_native_work("));
    assert!(operator.contains("&self.resources, Arc::clone(&protected), &self.state, &self.roots, native, &work"));
    assert!(!session.contains(".native_host(") && !operator.contains(".native_host("));
    assert!(operator.contains("&self.state.root_session_id, &self.binding(caller)?.native_session_id,"),
        "passive status must forward its genuine native binding, not the Root alias");
    let passive = operator.split("async fn writeback_status(").nth(1).unwrap();
    assert!(!passive.contains("string(arguments,"));
    assert!(passive.find("work.retain_owned(retained)").unwrap()
        < passive.find("Vec::with_capacity(selected.len())").unwrap());
    assert!(passive.contains("caller, operation, source, root, &paths, work).await"));
    for helper in ["inspect_operator", "validate_approval_request", "stage_operator_command", "passive_status"] {
        assert!(operator.contains(&format!("pi_operator_host::{helper}(&self.runtime,")),
            "operator producer must authenticate its original readonly SDK context");
    }
    let drive = session.split("let progress = driver.drive_root().await?;").nth(1).unwrap()
        .split("handles = driver.created_roots().await;").next().unwrap();
    let release_attempt = drive.find("drop(progress.1)").unwrap();
    assert!(drive.find("drop(lease)").unwrap() < release_attempt);
    assert!(drive.find("drop(attempt)").unwrap() < release_attempt);
    assert!(release_attempt < drive.find("return Err(error)").unwrap(),
        "native primary error keeps its intrinsic custody after lease/input DTOs die");
    assert!(!drive.contains("format!") && !drive.contains(".clone()"));
    assert_eq!(session.matches("PiRootPreparation::load_native_from_state(").count(), 3);
    assert!(!session.contains("PiRootIntent::from_state("));
    assert!(!session.contains("binding: binding.as_ref().clone()"),
        "Root preparation must move the admitted DTO under its real decoder custody");
    assert_eq!(session.matches("preparation, original_native_work,").count(), 2);
    assert_eq!(session.matches(".into_native_shared(original_native_work)?").count(), 2);
    assert!(!session.contains("owner.clone()"));
    assert!(session.contains("readonly_task_context_with_native_work("));
    assert!(session.contains("Some(Arc::clone(read_context)), original_native_work"));
    assert!(session.contains("guard_created_root_source_with_native_work("));
    assert!(session.contains("expected_allocation, original_native_work,"));
    assert!(session.contains("shared_roots.acquire_with_native_work("));
    assert!(!session.contains("shared_roots.acquire("));
    assert!(!session.contains("let mut work = acyclic_fs::WorkCounters::default()"));
    assert!(!session.contains("work.remaining(budget)"));
    assert!(session.find("drop(allocation.0.0)").unwrap()
        < session.find("drop(allocation.1)").unwrap(),
        "original observed FileRef must die before native allocation custody");
    assert!(session.find("for (key, creation) in allocation.0.1.roots").unwrap()
        < session.find("drop(allocation.1)").unwrap());
    assert!(session.contains("LazyWorkspace::resume_root_observation_with_native_work("));
    assert!(!session.contains("LazyWorkspace::resume_root_observation_measured("));
    assert!(session.contains("RootError::NativeSource { source, custody }"));
    assert!(session.contains("original_native_work.display(key)"));
    assert!(session.contains("PiRootHandle::from_observed_with_native_work("));
    assert!(session.contains("resumed, physical, key, original_native_work,"));
    assert!(!session.contains("handles.insert(key.clone()"));
    assert!(session.contains("rebind_source_with_native_work(original_native_work)"));
    assert!(!session.contains("rebind_source_measured("));
    assert!(session.contains("RootError::NativeSource { source, custody: None }"));
    assert!(session.find("drop(rebound.0)").unwrap()
        < session.find("drop(rebound.1)").unwrap());
    assert!(!session.contains("observed_sources.insert(key.clone()"));
    assert!(session.contains("original.source_epoch = rebound.0.source.epoch"));
    assert!(session.contains("self.resources.fs.open_workspace_with_native_work("));
    assert!(!session.contains("self.resources.fs.open_workspace_measured("));
    assert!(session.contains("RootError::NativeFilesystem { source, custody }"));
    assert!(session.contains("distributed.lineage().register_root_with_native_work("));
    assert!(session.contains("RootError::NativeLineage { source, custody }"));
    assert!(!session.contains("distributed.lineage().register_root("));
    assert!(session.find("drop(lineage.0)").unwrap()
        < session.find("drop(lineage.1)").unwrap());
    assert!(session.contains("distributed.contexts().register_root_with_native_work("));
    assert!(session.contains("(context_roots, context_custody),"));
    assert!(session.contains("RootError::NativeContext { source, custody }"));
    assert!(!session.contains("distributed.contexts().register_root("));
    assert!(session.find("resize_retained(actual_context_bytes)").unwrap()
        < session.find("distributed.contexts().register_root_with_native_work(").unwrap());
    assert!(session.find("drop(context.0)").unwrap()
        < session.find("drop(context.1)").unwrap());
    assert!(session.find("drop(workspace_name.0)").unwrap()
        < session.find("drop(workspace_name.1)").unwrap());
    assert_eq!(session.matches("recover_native_children(&query_work).await?").count(), 2);
    assert!(session.contains("cancel_native_subtree(caller_task, task, &query_work).await?"));
    assert!(!session.contains("recover_native_children().await"));
    assert!(session.contains("pi_caller::issue_with_work("));
    assert!(session.contains("pi_caller::verify_with_work("));
    assert!(!session.contains("pi_caller::issue(") && !session.contains("pi_caller::verify("));
    assert!(!session.contains(".verify_owner(") && !operator.contains(".verify_owner("));
    assert!(!session.contains("PiOwnerConfig::load(") && !operator.contains("PiOwnerConfig::load("));
    assert_eq!(session.matches("PiOwnerConfig::load_with_native_work(").count(), 3);
    assert_eq!(session.matches("PiOwnerConfig::load_readonly_with_native_work(").count(), 1);
    assert_eq!(operator.matches("PiOwnerConfig::load_with_native_work(").count(), 2);
    assert!(session.contains("Scope::from_value_with_native_work(value, &query_work)"));
    assert!(session.contains("if let Some((scope, _custody)) = &caller"));
    assert!(!session.contains("Scope as serde::Deserialize"));
    assert!(operator.contains("PiOperatorRequest::from_value_with_native_work(arguments, &work)"));
    assert!(!operator.contains("serde::Deserialize::deserialize(arguments)"));
    assert!(session.find("retain_owned(context_bytes)").unwrap()
        < session.find("Vec::with_capacity(self.roots.len())").unwrap());
    assert!(!session.contains("binding.project_volumes.clone()"));
    let issued = session.split("if let Some(mut reply) = issued_caller {").nth(1).unwrap()
        .split("\n        }").next().unwrap();
    assert!(issued.find("drop(reply.0)").unwrap()
        < issued.find("encoded.1.absorb(&mut reply.1)").unwrap());
    assert!(!send.contains(".map_err(display)"));
    assert!(!wrapper.contains(".map_err(display)"));
    assert!(!session.contains("let mut native_session = self.state.root_session_id.clone()"));
    assert!(!session.contains("native_session = session.clone()"));
    assert!(session.contains("authenticated_caller_task.unwrap_or(root_binding.task_id)"));
    assert!(session.contains("session.dispatch(request, original_native_work).await"));
    assert!(session.contains("observe_pi_session_with_native_work("));
    assert!(session.find("drop(observations.0)").unwrap()
        < session.find("encoded.1.absorb(&mut observations.1)").unwrap());
    let actor_fields = session.split("pub(crate) struct PiSession {").nth(1).unwrap()
        .split("\n}").next().unwrap();
    assert!(actor_fields.contains(
        "Option<acyclic_harness::filesystem::TaskWorkerAttempt>, acyclic_fs::NativeAllocation,"),
        "actor job storage must retain the genuine worker attempt tuple");
    assert!(actor_fields.find("state: AdapterState").unwrap()
        < actor_fields.find("native_startup_custody: Option<acyclic_fs::NativeAllocation>").unwrap());
    assert!(actor_fields.find("state: AdapterState").unwrap()
        < actor_fields.find("native_catalog_custody: Vec<").unwrap());
    assert!(actor_fields.find("native_catalog_custody: Vec<").unwrap()
        < actor_fields.find("native_catalog_backing_custody: Option<").unwrap());
    assert!(actor_fields.trim_end().ends_with("native_catalog_backing_custody: Option<acyclic_fs::NativeAllocation>,"));
    assert!(actor_fields.find("state: AdapterState").unwrap()
        < actor_fields.find("native_pending_backing_custody: Option<").unwrap());
    let pending_backing = session.split("pub(super) fn replace_native_pending_backing_custody(").nth(1).unwrap()
        .split("pub(super) fn prepare_native_catalog_slot(").next().unwrap();
    assert!(pending_backing.contains("self.native_pending_backing_custody.replace(custody)"));
    assert!(!pending_backing.contains("drop(") && !pending_backing.contains(".absorb(")
        && !pending_backing.contains(".clone()"));
    let growth = session.split("pub(super) fn prepare_native_catalog_slot(").nth(1).unwrap()
        .split("pub(super) fn install_native_catalog_custody(").next().unwrap();
    assert!(growth.find("work.claim(bytes as u64)").unwrap()
        < growth.find("Vec::with_capacity(capacity)").unwrap());
    assert!(growth.find("self.native_catalog_custody.extend(old.0)").unwrap()
        < growth.find("drop(old.1)").unwrap());
    assert!(!growth.contains(".absorb(") && !growth.contains(".clone()"));
    let install = session.split("pub(super) fn install_native_catalog_custody(").nth(1).unwrap()
        .split("pub(super) fn take_native_catalog_custody(").next().unwrap();
    assert!(install.contains("std::mem::replace(&mut entry.2, custody)"));
    assert!(!install.contains("drop(") && !install.contains(".clone()"));
    let attach = session.split("pub(crate) fn retain_original_input(").nth(1).unwrap()
        .split("pub(crate) async fn close(").next().unwrap();
    assert!(attach.contains("let mut owned = (self, custody)"));
    assert!(attach.contains("existing.absorb(&mut owned.1)"));
    assert!(session.contains("adapter_state::load_state_with_native_work("));
    assert!(session.contains("&resources.private_state_root, relative_directory, work,"));
    assert!(session.contains("relative_directory: PathBuf"));
    assert!(session.contains("work.copy_path(relative_directory)"));
    assert!(session.contains("original_session_directory_matches(relative_directory, &state.root_session_id, work)?"));
    assert!(!session.contains("directory != resources.session_directory(&state.root_session_id)"));
    assert!(session.contains("service_resources_with_native_work(resources, work)?"));
    assert!(!session.contains("resources: resources.clone()"));
    assert!(!session.contains("self.resources.clone()") && !operator.contains("self.resources.clone()"));
    assert!(!session.contains("load_state(&directory)"));
    assert_eq!(session.matches("self.slots.save_with_native_work(").count(), 2);
    assert!(!session.contains("self.slots.save("));
    let lifecycle = include_str!("pi_lifecycle_session.rs");
    assert!(lifecycle.contains("route.lifecycle = RouteLifecycle::StopRequested"));
    assert!(lifecycle.contains("route.lifecycle = RouteLifecycle::Frozen"));
    assert!(!lifecycle.contains("routes.remove(") && !lifecycle.contains("pending.remove("));
    assert!(!lifecycle.contains("take_native_catalog_custody("),
        "cancel/drain must retain original component guards while their DTOs remain resident");
    let open = session.split("impl SessionController {").nth(1).unwrap()
        .split("pub(crate) fn state(").next().unwrap();
    assert!(open.find("drop(loaded.0)").unwrap() < open.find("drop(loaded.1)").unwrap());
    assert!(open.find("loaded.0.0, loaded.0.1, resources, work).await?").unwrap()
        < open.find("custody.absorb(&mut loaded.1)").unwrap());
    assert!(open.find("retain_owned(std::mem::size_of::<PiSession>()").unwrap()
        < open.find("Self::Pi(Box::new(session))").unwrap());
}

#[test]
fn pi_original_query_span_is_reused_without_resetting_cancellation_or_counters() {
    let mut span = None;
    let work = crate::service_session::PiSession::query_work(&mut span).unwrap();
    let original = work.retain_owned(0).unwrap();
    let reused = crate::service_session::PiSession::query_work(&mut span).unwrap();
    let proof = reused.retain_owned(0).unwrap();
    assert!(original.shares_ledger(&proof));
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

#[test]
fn pi_service_fixture_retains_original_staged_command_through_native_admission() {
    let source = include_str!("pi_service_tests.rs");
    let fixture = source.split("async fn prepare_service(").nth(1).unwrap()
        .split("\nasync fn ").next().unwrap();
    assert!(fixture.contains("PiTaskRuntime::open_with_native_work("));
    assert!(fixture.contains("owner.authority_with_native_work(&native_work)"));
    assert!(fixture.contains(".admit_root_with_native_work("));
    assert!(!fixture.contains("PiTaskRuntime::open(") && !fixture.contains(".admit_root("));
    assert!(fixture.contains("let command = stage_root_command("));
    assert!(fixture.find("command.0, &native_work)").unwrap()
        < fixture.find("drop(command.1)").unwrap());
    assert!(fixture.find("drop(admission.0)").unwrap()
        < fixture.find("drop(admission.1)").unwrap());
    assert!(fixture.contains("crate::tests::configured_test_service_bootstrap(data).await"));
    assert!(fixture.contains("ServiceResources::open(data.to_path_buf(), configured, private_root, &native_work)"));
    assert!(fixture.contains("(service_lock, resources, state)"));
}
