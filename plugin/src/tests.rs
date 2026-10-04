#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_in_result
)]

use super::*;
use acyclic_fs::GitCommand;
use acyclic_fs::kernel::NameEncoding;

fn root_repository_workspace_id(control: &ControlPlane) -> [u8; 16] {
    control.state.roots[&root_key(WorkspaceRootId::from_bytes(control.state.root_id))]
        .repository_workspace_id
}

async fn active_root_workspace_id(control: &ControlPlane) -> acyclic_fs::WorkspaceId {
    let root_id = WorkspaceRootId::from_bytes(control.state.root_id);
    control
        .distributed
        .contexts()
        .resolve(WorkspaceContextId::from_bytes(
            control.state.root_context_id,
        ))
        .await
        .expect("root context")
        .roots[&root_id]
        .workspace_id
}

async fn root_has_file_contents(
    control: &ControlPlane,
    portable_name: &str,
    expected: &[u8],
) -> bool {
    let Ok(workspace) = control
        .distributed
        .workspace(active_root_workspace_id(control).await)
        .await
    else {
        return false;
    };
    let Ok(head) = workspace.head().await else {
        return false;
    };
    let Ok(page) = head.list_directory("/", None, 1024).await else {
        return false;
    };
    let limits = VolumeLimits::default();
    let Some(path) = page.entries.into_iter().find_map(|entry| {
        let path = NamespacePath::new(vec![entry.name], limits).ok()?;
        (namespace_path_text(&path).ok()?.trim_start_matches('/') == portable_name).then_some(path)
    }) else {
        return false;
    };
    let Ok(lookup) = head
        .lookup_paths(&[path], WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
    else {
        return false;
    };
    matches!(
        lookup.value.as_slice(),
        [Some(acyclic_fs::kernel::FileRecord {
            payload: acyclic_fs::kernel::FilePayload::InlineRegular(data),
            ..
        })] if data.as_bytes() == expected
    )
}

#[test]
fn native_posix_names_are_presented_when_they_are_valid_utf8() {
    use acyclic_fs::kernel::LogicalName;

    let limits = VolumeLimits::default();
    let readable = LogicalName::new(
        NameEncoding::PosixBytes,
        b"shared.txt".to_vec(),
        limits.maximum_component_bytes,
    )
    .expect("valid POSIX name");
    let invalid = LogicalName::new(
        NameEncoding::PosixBytes,
        vec![0xff],
        limits.maximum_component_bytes,
    )
    .expect("valid non-UTF-8 POSIX name");
    assert_eq!(
        namespace_path_text(&NamespacePath::new(vec![readable], limits).expect("path"))
            .expect("readable native name"),
        "/shared.txt"
    );
    assert!(
        namespace_path_text(&NamespacePath::new(vec![invalid], limits).expect("path")).is_err()
    );
}

fn route_path(route: &Route) -> PathBuf {
    route.active_path().expect("route active path")
}

#[test]
#[ignore = "invoked as a separate process by mount lifecycle tests"]
fn projected_mount_writer_child() {
    let Some(path) = std::env::var_os("ACYCLIC_TEST_PROJECTED_WRITE_PATH") else {
        return;
    };
    fs::write(PathBuf::from(path), b"late parent").expect("external writer completes");
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "taking ownership keeps temporary json! values ergonomic at every fixture call site"
)]
fn native_hook_request(
    host: &str,
    event: &str,
    session_id: &str,
    cwd: &Path,
    extra: Value,
) -> ControlRequest {
    let mut arguments = extra.as_object().cloned().unwrap_or_default();
    arguments.insert("session_id".to_owned(), json!(session_id));
    arguments.insert("cwd".to_owned(), json!(cwd));
    ControlRequest {
        version: 1,
        command: ControlCommand::Hook,
        cwd: cwd.to_path_buf(),
        argv: Vec::new(),
        name: format!("{host}:{event}"),
        arguments: Value::Object(arguments),
    }
}

#[test]
fn a_session_ending_after_an_unflushed_save_keeps_it_in_its_terminal_state() {
    run_large_stack("terminal-save-covers-unflushed", terminal_save_case);
}

async fn terminal_save_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    let data = temporary.path().join("state");
    fs::create_dir_all(&root).expect("root");
    let service = ConcurrentServiceControl::open(data.clone())
        .await
        .expect("service");
    for (event, extra) in [
        ("SessionStart", json!({})),
        // Saved unflushed, the last effect of its request.
        ("UserPromptSubmit", json!({"turn_id":"last-turn"})),
        // Nothing is mounted or leased, so only its terminal save flushes.
        ("SessionEnd", json!({})),
    ] {
        service
            .dispatch_request(native_hook_request("codex", event, "session", &root, extra))
            .await
            .expect(event);
    }
    service.shutdown().await.expect("service shutdown");
    let directory = data
        .join("sessions")
        .join(blake3::hash(b"session").to_hex().as_str());
    let (slots, [_, _, unflushed]) = StateSlots::read(&directory).expect("slots");
    let state = load_saved_state(&directory).expect("terminal session state");
    assert!(!state.active);
    assert!(state.root_turns.contains("last-turn"));
    // The terminal save is the newest flushed save; the unflushed slot
    // holds an older generation and is never loaded again.
    assert!(unflushed.generation() < slots.flushed.into_iter().max().flatten());
}

#[test]
fn concurrent_service_isolates_sessions_and_fences_reused_ids() {
    run_large_stack("concurrent-service", concurrent_service_case);
}

async fn concurrent_service_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root_a = temporary.path().join("root-a");
    let root_b = temporary.path().join("root-b");
    fs::create_dir_all(&root_a).expect("root a");
    fs::create_dir_all(&root_b).expect("root b");
    let service = ConcurrentServiceControl::open(temporary.path().join("state"))
        .await
        .expect("service");
    for (session, root) in [("a", &root_a), ("b", &root_b)] {
        service
            .dispatch_request(native_hook_request(
                "claude-code",
                "SessionStart",
                session,
                root,
                json!({}),
            ))
            .await
            .expect("session start");
    }

    let handle_a = service.session("a").await.expect("session a");
    let resume_a = handle_a.pause().await;
    let request_b = ControlRequest {
        version: 1,
        command: ControlCommand::Agents,
        cwd: root_b.clone(),
        argv: Vec::new(),
        name: String::new(),
        arguments: Value::Null,
    };
    tokio::time::timeout(
        std::time::Duration::from_millis(250),
        service.dispatch_request(request_b),
    )
    .await
    .expect("session b must not queue behind session a")
    .expect("session b request");
    let _ = resume_a.send(());

    let first_epoch = handle_a.key.epoch;
    service.end_session("a", true).await.expect("end a");
    service
        .dispatch_request(native_hook_request(
            "claude-code",
            "SessionStart",
            "a",
            &root_a,
            json!({}),
        ))
        .await
        .expect("resume a");
    let resumed = service.session("a").await.expect("resumed a");
    assert!(resumed.key.epoch > first_epoch);
    assert!(!handle_a.is_active());

    service.drain_sessions(true).await.expect("service drain");
}

#[test]
fn accepted_session_operation_survives_waiter_cancellation() {
    run_large_stack(
        "accepted-session-operation",
        accepted_session_operation_case,
    );
}

async fn accepted_session_operation_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    fs::create_dir_all(&root).expect("root");
    let service = Arc::new(
        ConcurrentServiceControl::open(temporary.path().join("state"))
            .await
            .expect("service"),
    );
    service
        .dispatch_request(native_hook_request(
            "codex",
            "SessionStart",
            "session",
            &root,
            json!({}),
        ))
        .await
        .expect("session start");
    let handle = service.session("session").await.expect("session");
    let resume = handle.pause().await;
    let service_for_request = Arc::clone(&service);
    let root_for_request = root.clone();
    let waiter = tokio::spawn(async move {
        service_for_request
            .dispatch_request(native_hook_request(
                "codex",
                "UserPromptSubmit",
                "session",
                &root_for_request,
                json!({"turn_id":"durable-turn"}),
            ))
            .await
    });
    tokio::task::yield_now().await;
    waiter.abort();
    let _ = resume.send(());
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if handle
                .snapshot()
                .expect("snapshot")
                .is_some_and(|state| state.root_turns.contains("durable-turn"))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("accepted operation must complete after waiter cancellation");
    service.drain_sessions(true).await.expect("service drain");
}

#[test]
fn a_stop_request_drains_without_waiting_for_a_busy_session() {
    run_large_stack("stop-before-drain", stop_before_drain_case);
}

async fn stop_before_drain_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    let data = temporary.path().join("state");
    fs::create_dir_all(&root).expect("root");
    let service = ConcurrentServiceControl::open(data.clone())
        .await
        .expect("service");
    service
        .dispatch_request(native_hook_request(
            "codex",
            "SessionStart",
            "session",
            &root,
            json!({}),
        ))
        .await
        .expect("session start");
    let handle = service.session("session").await.expect("session");
    let resume = handle.pause().await;
    service.begin_drain();
    assert_eq!(
        service.lifecycle.load(std::sync::atomic::Ordering::Acquire),
        SERVICE_DRAINING
    );
    let drain = tokio::spawn(async move { service.shutdown().await });
    tokio::task::yield_now().await;
    assert!(!drain.is_finished(), "paused actor must delay actual drain");
    resume.send(()).expect("resume session actor");
    tokio::time::timeout(std::time::Duration::from_secs(10), drain)
        .await
        .expect("drain completes")
        .expect("drain task")
        .expect("service shutdown");
    let state = load_saved_state(
        &data
            .join("sessions")
            .join(blake3::hash(b"session").to_hex().as_str()),
    )
    .expect("terminal session state");
    assert!(!state.active, "explicit shutdown must deactivate sessions");
}

#[test]
fn session_close_is_fifo_idempotent_and_durable() {
    run_large_stack("session-close-order", session_close_order_case);
}

#[test]
fn saturated_session_data_queue_reserves_shutdown_capacity() {
    run_large_stack("session-saturated-close", session_saturated_close_case);
}

async fn session_saturated_close_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    fs::create_dir_all(&root).expect("root");
    let service = Arc::new(
        ConcurrentServiceControl::open(temporary.path().join("state"))
            .await
            .expect("service"),
    );
    service
        .dispatch_request(native_hook_request(
            "codex",
            "SessionStart",
            "session",
            &root,
            json!({}),
        ))
        .await
        .expect("session start");
    let handle = service.session("session").await.expect("session");
    let resume = handle.pause().await;
    let mut operations = Vec::new();
    for _ in 0..SESSION_DATA_CAPACITY {
        let handle = Arc::clone(&handle);
        let root = root.clone();
        operations.push(tokio::spawn(async move {
            handle
                .dispatch(ControlRequest {
                    version: 1,
                    command: ControlCommand::Ping,
                    cwd: root,
                    argv: Vec::new(),
                    name: String::new(),
                    arguments: Value::Null,
                })
                .await
        }));
    }
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while handle.data_slots.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("data queue saturation");
    let overflow = handle
        .dispatch(ControlRequest {
            version: 1,
            command: ControlCommand::Ping,
            cwd: root,
            argv: Vec::new(),
            name: String::new(),
            arguments: Value::Null,
        })
        .await;
    assert_eq!(
        overflow.expect_err("overflow must fail"),
        "Acyclic session operation queue is full"
    );
    let close_service = Arc::clone(&service);
    let close = tokio::spawn(async move { close_service.end_session("session", true).await });
    tokio::task::yield_now().await;
    assert_eq!(
        handle.state.load(std::sync::atomic::Ordering::Acquire),
        SESSION_CLOSING
    );
    let _ = resume.send(());
    for operation in operations {
        operation.await.expect("operation task").expect("operation");
    }
    close.await.expect("close task").expect("close");
    assert_eq!(
        handle.state.load(std::sync::atomic::Ordering::Acquire),
        SESSION_CLOSED
    );
}

async fn session_close_order_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    fs::create_dir_all(&root).expect("root");
    let service = Arc::new(
        ConcurrentServiceControl::open(temporary.path().join("state"))
            .await
            .expect("service"),
    );
    service
        .dispatch_request(native_hook_request(
            "codex",
            "SessionStart",
            "session",
            &root,
            json!({}),
        ))
        .await
        .expect("session start");
    let handle = service.session("session").await.expect("session");
    let resume = handle.pause().await;

    let operation_service = Arc::clone(&service);
    let operation_root = root.clone();
    let operation = tokio::spawn(async move {
        operation_service
            .dispatch_request(native_hook_request(
                "codex",
                "UserPromptSubmit",
                "session",
                &operation_root,
                json!({"turn_id":"before-close"}),
            ))
            .await
    });
    tokio::task::yield_now().await;
    let close_service = Arc::clone(&service);
    let close = tokio::spawn(async move { close_service.end_session("session", true).await });
    tokio::task::yield_now().await;
    let duplicate_service = Arc::clone(&service);
    let duplicate =
        tokio::spawn(async move { duplicate_service.end_session("session", true).await });
    let _ = resume.send(());

    operation.await.expect("operation task").expect("operation");
    close.await.expect("close task").expect("close");
    duplicate
        .await
        .expect("duplicate task")
        .expect("duplicate close");
    service
        .end_session("session", true)
        .await
        .expect("late duplicate close");

    let state = load_saved_state(
        &service
            .data
            .join("sessions")
            .join(blake3::hash(b"session").to_hex().as_str()),
    )
    .expect("terminal state");
    assert!(!state.active);
    assert!(state.root_turns.contains("before-close"));
}

#[test]
fn compatibility_commit_keeps_the_lazy_workspace_snapshot() {
    assert!(!git_requires_exact_workspace(&[
        "commit".to_owned(),
        "-m".to_owned(),
        "snapshot".to_owned(),
    ]));
    assert!(git_requires_exact_workspace(&["status".to_owned()]));
}

#[test]
fn plugin_install_assets_are_discovered_without_build_paths() {
    let embedded_marketplace: Value =
        serde_json::from_str(include_str!("../.agents/plugins/marketplace.json"))
            .expect("embedded marketplace");
    assert_eq!(
        embedded_marketplace["plugins"][0]["source"]["path"], ".",
        "the embedded marketplace is rooted at the plugin directory"
    );

    let temporary = tempfile::tempdir().expect("temporary directory");
    let packaged = temporary.path().join("package");
    let installed = temporary.path().join("installed");
    for root in [&packaged, &installed] {
        fs::create_dir_all(root.join(".agents/plugins")).expect("manifest directory");
        fs::write(root.join("plugin.json"), b"{}").expect("plugin manifest");
        fs::write(root.join(".agents/plugins/marketplace.json"), b"{}").expect("marketplace");
    }

    assert_eq!(
        discover_plugin_root(&packaged.join("bin/acyclic"), &installed),
        Some(packaged)
    );
    assert_eq!(
        discover_plugin_root(&temporary.path().join("bin/acyclic"), &installed),
        Some(installed)
    );
}

#[test]
fn shared_service_isolates_multiple_sessions() {
    run_large_stack("shared-service-e2e", shared_service_case);
}

#[test]
fn nested_roots_route_to_the_deepest_unambiguous_session() {
    run_large_stack("nested-root-routing", nested_root_routing_case);
}

async fn nested_root_routing_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let parent = temporary.path().join("root");
    let nested = parent.join("nested");
    let nested_child = nested.join("child");
    fs::create_dir_all(&nested_child).expect("nested roots");
    let mut service = ServiceControl::open(temporary.path().join("state"))
        .await
        .expect("service");
    for (session_id, cwd) in [("parent", &parent), ("nested", &nested)] {
        service
            .dispatch_native_hook(
                "claude-code",
                "SessionStart",
                json!({"session_id":session_id,"cwd":cwd}),
                cwd,
            )
            .await
            .expect("register root");
    }
    assert_eq!(
        service.session_for_cwd(&parent).expect("parent route"),
        Some("parent".to_owned())
    );
    assert_eq!(
        service
            .session_for_cwd(&nested_child)
            .expect("deepest route"),
        Some("nested".to_owned())
    );

    service
        .dispatch_native_hook(
            "claude-code",
            "SessionStart",
            json!({"session_id":"ambiguous","cwd":nested}),
            &nested,
        )
        .await
        .expect("register identical nested root");
    let error = service
        .session_for_cwd(&nested_child)
        .expect_err("equal-depth roots must fail closed");
    assert!(error.contains("multiple Acyclic sessions"), "{error}");
    service.shutdown().await.expect("shutdown");
}

#[test]
fn shared_root_first_acquire_is_singleflight() {
    run_large_stack("shared-root-singleflight", shared_root_singleflight_case);
}

async fn shared_root_singleflight_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    fs::create_dir(&root).expect("root");
    let registry = SharedRootRegistry::default();
    let (left, right) = tokio::join!(
        registry.acquire(&root, SharedRootAdmission::Fresh),
        registry.acquire(&root, SharedRootAdmission::Fresh)
    );
    let left = left.expect("first acquire");
    let right = right.expect("second acquire");
    assert!(Arc::ptr_eq(&left, &right));
    assert_eq!(registry.live_roots().await, 1);
}

#[test]
fn shared_root_observations_publish_monotonic_source_epochs() {
    run_large_stack("shared-root-observations", shared_root_observations_case);
}

async fn shared_root_observations_case() {
    use acyclic_fs::{WatchEpoch, WatchInvalidationReason, WatchSequence};
    use std::sync::mpsc;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    fs::create_dir(&root).expect("root");
    let shared = SharedRootRegistry::default()
        .acquire(&root, SharedRootAdmission::Fresh)
        .await
        .expect("shared physical root");
    let (first_entered_tx, first_entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let first_root = Arc::clone(&shared);
    let first = std::thread::spawn(move || {
        first_root.observe_with(|| {
            first_entered_tx
                .send(())
                .expect("first observation entered");
            release_rx.recv().expect("release first observation");
            Ok(WatchBatch::RescanRequired {
                epoch: WatchEpoch::from_u64(1),
                reason: WatchInvalidationReason::NativeRescanRequired,
            })
        })
    });
    first_entered_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("first observation reached watcher poll");
    let (second_started_tx, second_started_rx) = mpsc::channel();
    let (second_entered_tx, second_entered_rx) = mpsc::channel();
    let second_root = Arc::clone(&shared);
    let second = std::thread::spawn(move || {
        second_started_tx.send(()).expect("second thread started");
        second_root.observe_with(|| {
            second_entered_tx
                .send(())
                .expect("second observation entered");
            Ok(WatchBatch::Changes {
                epoch: WatchEpoch::from_u64(1),
                first_sequence: WatchSequence::from_u64(1),
                next_sequence: WatchSequence::from_u64(1),
                changes: Vec::new(),
            })
        })
    });
    second_started_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("second observer started");
    assert!(
        second_entered_rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err(),
        "second observation must wait for the first publication"
    );
    release_tx.send(()).expect("release first observation");
    let first = first
        .join()
        .expect("first thread")
        .expect("first observation");
    let second = second
        .join()
        .expect("second thread")
        .expect("second observation");
    assert_eq!(second.prior_source.epoch, first.source.epoch);
    assert_eq!(second.source.epoch, first.source.epoch);
    assert_eq!(
        shared.reference.lock().expect("published reference").epoch,
        second.source.epoch
    );
}

#[cfg(unix)]
#[test]
fn shared_root_rejects_live_path_replacement() {
    run_large_stack("shared-root-replacement", shared_root_replacement_case);
}

#[cfg(unix)]
async fn shared_root_replacement_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    let detached = temporary.path().join("detached");
    fs::create_dir(&root).expect("root");
    let registry = SharedRootRegistry::default();
    let held = registry
        .acquire(&root, SharedRootAdmission::Fresh)
        .await
        .expect("initial acquire");
    fs::rename(&root, &detached).expect("detach admitted root");
    fs::create_dir(&root).expect("replacement root");
    let Err(error) = registry.acquire(&root, SharedRootAdmission::Fresh).await else {
        panic!("replacement must fail closed");
    };
    assert!(
        error.contains("identity") && error.contains("changed"),
        "{error}"
    );
    drop(held);
    registry.prune().await;
    let replacement = registry
        .acquire(&root, SharedRootAdmission::Fresh)
        .await
        .expect("released registration must not pin the old root identity");
    assert_eq!(registry.live_roots().await, 1);
    drop(replacement);
}

async fn shared_service_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root_a = temporary.path().join("root-a");
    let root_b = temporary.path().join("root-b");
    let state = temporary.path().join("state");
    fs::create_dir(&root_a).expect("root a");
    fs::create_dir(&root_b).expect("root b");
    let mut service = ServiceControl::open(state.clone()).await.expect("service");
    for (session_id, root) in [("a", &root_a), ("b", &root_b)] {
        service
            .dispatch_request(ControlRequest {
                version: 1,
                command: ControlCommand::Hook,
                cwd: root.clone(),
                argv: Vec::new(),
                name: "codex:SessionStart".to_owned(),
                arguments: json!({
                    "session_id": session_id,
                    "cwd": root.display().to_string(),
                }),
            })
            .await
            .expect("session start");
    }
    assert_eq!(service.sessions.len(), 2);
    assert_eq!(
        service.session_for_cwd(&root_a).expect("route a"),
        Some("a".to_owned())
    );
    assert_eq!(
        service.session_for_cwd(&root_b).expect("route b"),
        Some("b".to_owned())
    );
    service
        .dispatch_request(ControlRequest {
            version: 1,
            command: ControlCommand::Hook,
            cwd: root_a.clone(),
            argv: Vec::new(),
            name: "codex:SessionStart".to_owned(),
            arguments: json!({"session_id":"same-root","cwd":root_a}),
        })
        .await
        .expect("second session on same root");
    let first_context = service.sessions["a"]
        .distributed
        .contexts()
        .resolve(WorkspaceContextId::from_bytes(
            service.sessions["a"].state.root_context_id,
        ))
        .await
        .expect("first root context");
    let second_context = service.sessions["same-root"]
        .distributed
        .contexts()
        .resolve(WorkspaceContextId::from_bytes(
            service.sessions["same-root"].state.root_context_id,
        ))
        .await
        .expect("second root context");
    assert_ne!(
        first_context
            .roots
            .values()
            .next()
            .expect("first root")
            .workspace_name,
        second_context
            .roots
            .values()
            .next()
            .expect("second root")
            .workspace_name
    );
    assert_ne!(
        root_repository_workspace_id(&service.sessions["a"]),
        root_repository_workspace_id(&service.sessions["same-root"])
    );
    let root_a_key = root_key(WorkspaceRootId::from_bytes(
        service.sessions["a"].state.root_id,
    ));
    let same_root_key = root_key(WorkspaceRootId::from_bytes(
        service.sessions["same-root"].state.root_id,
    ));
    assert!(Arc::ptr_eq(
        &service.sessions["a"].physical_roots[&root_a_key],
        &service.sessions["same-root"].physical_roots[&same_root_key],
    ));
    assert_eq!(service.shared_roots.live_roots().await, 2);

    let outside = temporary.path().join("outside");
    fs::create_dir(&outside).expect("outside root");
    let error = service
        .dispatch_request(ControlRequest {
            version: 1,
            command: ControlCommand::Agents,
            cwd: outside,
            argv: Vec::new(),
            name: String::new(),
            arguments: Value::Null,
        })
        .await
        .expect_err("an active service must reject an unregistered root");
    assert!(error.contains("outside every registered"), "{error}");
    assert_eq!(service.sessions.len(), 3);
    assert_eq!(service.shared_roots.live_roots().await, 2);

    let child_path = {
        let control = service.sessions.get_mut("a").expect("session a");
        control
            .user_prompt(json!({"session_id":"a","turn_id":"root-turn"}))
            .expect("root turn");
        control
            .pre_tool(json!({
                "session_id":"a","turn_id":"root-turn","tool_use_id":"spawn-child",
                "tool_name":"spawn_agent","tool_input":{}
            }))
            .await
            .expect("child spawn permit");
        control
            .subagent_start(json!({
                "session_id":"a","turn_id":"child-turn","agent_id":"child",
                "agent_type":"explorer"
            }))
            .await
            .expect("child start");
        route_path(&control.state.routes["child"])
    };
    assert!(
        service
            .dispatch_native_hook(
                "codex",
                "SessionStart",
                json!({"session_id":"escape","cwd":child_path.display().to_string()}),
                &child_path,
            )
            .await
            .is_err(),
        "a child mount must never be registered as another session's physical root"
    );
    let error = service
        .dispatch_request(ControlRequest {
            version: 1,
            command: ControlCommand::Agents,
            cwd: child_path.clone(),
            argv: Vec::new(),
            name: String::new(),
            arguments: cli_routing(root_a.clone()),
        })
        .await
        .expect_err("a child must not select its parent with -C");
    assert!(error.contains("authority boundaries"), "{error}");
    let error = service
        .dispatch_request(ControlRequest {
            version: 1,
            command: ControlCommand::Agents,
            cwd: root_a.clone(),
            argv: Vec::new(),
            name: String::new(),
            arguments: cli_routing(child_path.clone()),
        })
        .await
        .expect_err("a root must inspect descendants through agent refs, not -C");
    assert!(
        error.contains("authority boundaries") || error.contains("multiple Acyclic sessions"),
        "{error}"
    );
    service
        .dispatch_request(ControlRequest {
            version: 1,
            command: ControlCommand::Agents,
            cwd: child_path.clone(),
            argv: Vec::new(),
            name: String::new(),
            arguments: cli_routing(child_path.clone()),
        })
        .await
        .expect("a child may select its own mounted context");
    {
        let control = service.sessions.get_mut("a").expect("session a");
        control
            .subagent_stop(json!({
                "session_id":"a","turn_id":"child-turn","agent_id":"child"
            }))
            .await
            .expect("child stop");
        assert!(
            control
                .pre_tool(json!({
                    "session_id":"a","turn_id":"child-turn","tool_use_id":"stopped-spawn",
                    "tool_name":"spawn_agent","tool_input":{}
                }))
                .await
                .is_err(),
            "a stopped child must not create a spawn permit"
        );
        assert!(control.state.pending.is_empty());
    }

    fs::write(root_a.join("shared.txt"), b"shared update").expect("shared root update");
    service.sessions["a"].physical_roots[&root_a_key]
        .source
        .inner()
        .invalidate();
    for session_id in ["a", "same-root"] {
        let root_agent = service.sessions[session_id].state.root_agent_id.clone();
        let mut observed = false;
        for _ in 0..80 {
            service
                .sessions
                .get_mut(session_id)
                .expect("session")
                .sync_agent(&root_agent)
                .await
                .expect("shared root reconciliation");
            if root_has_file_contents(
                &service.sessions[session_id],
                "shared.txt",
                b"shared update",
            )
            .await
            {
                observed = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(
            observed,
            "{session_id} did not observe the shared root update"
        );
    }

    fs::write(root_a.join("recovered.txt"), b"recovered update")
        .expect("failed-consumer root update");
    service.sessions["a"].physical_roots[&root_a_key]
        .source
        .inner()
        .invalidate();
    service
        .sessions
        .get_mut("a")
        .expect("first shared-root session")
        .fail_after_watch_poll = true;
    let root_agent = service.sessions["a"].state.root_agent_id.clone();
    let mut injected = false;
    for _ in 0..80 {
        match service
            .sessions
            .get_mut("a")
            .expect("first shared-root session")
            .sync_agent(&root_agent)
            .await
        {
            Err(error) if error.contains("injected failure") => {
                injected = true;
                break;
            }
            Ok(()) => tokio::time::sleep(std::time::Duration::from_millis(25)).await,
            Err(error) => panic!("unexpected reconciliation failure: {error}"),
        }
    }
    assert!(injected, "shared watcher failure was not injected");
    let root_agent = service.sessions["same-root"].state.root_agent_id.clone();
    let mut recovered = false;
    for _ in 0..80 {
        service
            .sessions
            .get_mut("same-root")
            .expect("same-root session")
            .sync_agent(&root_agent)
            .await
            .expect("reconcile after peer capture failure");
        if root_has_file_contents(
            &service.sessions["same-root"],
            "recovered.txt",
            b"recovered update",
        )
        .await
        {
            recovered = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(recovered, "peer lost the consumed shared watcher batch");
    service
        .dispatch_native_hook("codex", "SessionEnd", json!({"session_id":"a"}), &root_a)
        .await
        .expect("first shared-root session end");
    assert_eq!(service.shared_roots.live_roots().await, 2);
    let shared_roots = service.shared_roots.clone();
    service.shutdown().await.expect("shutdown");
    assert_eq!(shared_roots.live_roots().await, 0);

    let resumed = ServiceControl::open(state).await.expect("resume service");
    assert_eq!(resumed.sessions.len(), 2);
    assert!(!resumed.sessions.contains_key("a"));
    assert_eq!(resumed.shared_roots.live_roots().await, 2);
    resumed.shutdown().await.expect("resumed shutdown");
}

#[test]
fn sequential_same_root_sessions_reuse_the_durable_source_identity() {
    run_large_stack("sequential-same-root", sequential_same_root_case);
}

async fn sequential_same_root_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    let state = temporary.path().join("state");
    fs::create_dir(&root).expect("root");
    let mut service = ServiceControl::open(state.clone()).await.expect("service");
    service
        .dispatch_native_hook(
            "codex",
            "SessionStart",
            json!({"session_id":"first","cwd":root}),
            &root,
        )
        .await
        .expect("first session");
    let first_state = &service.sessions["first"].state;
    let first_identity = first_state.roots[&hex::encode(first_state.root_id)].source_identity;
    service
        .dispatch_native_hook("codex", "SessionEnd", json!({"session_id":"first"}), &root)
        .await
        .expect("first session end");
    assert_eq!(service.shared_roots.live_roots().await, 0);

    service
        .dispatch_native_hook(
            "codex",
            "SessionStart",
            json!({"session_id":"second","cwd":root}),
            &root,
        )
        .await
        .expect("second session");
    assert_eq!(
        service.sessions["second"].state.roots
            [&hex::encode(service.sessions["second"].state.root_id)]
            .source_identity,
        first_identity
    );
    service.shutdown().await.expect("shutdown");

    let resumed = ServiceControl::open(state)
        .await
        .expect("both durable sessions reopen without an identity collision");
    assert_eq!(resumed.sessions.keys().collect::<Vec<_>>(), vec!["second"]);
    resumed.shutdown().await.expect("resumed shutdown");
}

#[test]
fn service_replay_deletes_the_older_conflicting_same_root_session() {
    run_large_stack("conflicting-same-root", conflicting_same_root_case);
}

async fn conflicting_same_root_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    let state = temporary.path().join("state");
    fs::create_dir(&root).expect("root");
    let mut service = ServiceControl::open(state.clone()).await.expect("service");
    for session_id in ["older", "newer"] {
        service
            .dispatch_native_hook(
                "codex",
                "SessionStart",
                json!({"session_id":session_id,"cwd":root}),
                &root,
            )
            .await
            .expect("session start");
        service
            .dispatch_native_hook(
                "codex",
                "SessionEnd",
                json!({"session_id":session_id}),
                &root,
            )
            .await
            .expect("session end");
    }
    let older = service.session_directory("older");
    let newer = service.session_directory("newer");
    service.shutdown().await.expect("shutdown");

    let mut conflicting = load_saved_state(&older).expect("older state");
    conflicting.active = true;
    for binding in conflicting.roots.values_mut() {
        binding.source_identity = [9; 16];
    }
    save_state(&older, &conflicting, Survives::PowerLoss).expect("conflicting state");
    std::thread::sleep(std::time::Duration::from_millis(20));
    let mut current = load_saved_state(&newer).expect("newer state");
    current.active = true;
    save_state(&newer, &current, Survives::PowerLoss).expect("refresh newer state");

    let resumed = ServiceControl::open(state)
        .await
        .expect("service replay discards the older conflict");
    assert!(!older.exists());
    assert!(newer.exists());
    assert_eq!(resumed.sessions.keys().collect::<Vec<_>>(), vec!["newer"]);
    resumed.shutdown().await.expect("resumed shutdown");
}

#[test]
fn cli_registers_fresh_cwd_without_init() {
    run_large_stack("fresh-cwd-e2e", fresh_cwd_case);
}

async fn fresh_cwd_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    fs::create_dir(&root).expect("root");
    let mut service = ServiceControl::open(temporary.path().join("state"))
        .await
        .expect("service");
    let agents = service
        .dispatch_request(ControlRequest {
            version: 1,
            command: ControlCommand::Agents,
            cwd: root.clone(),
            argv: Vec::new(),
            name: String::new(),
            arguments: Value::Null,
        })
        .await
        .expect("implicit registration");
    assert!(agents["agents"].as_array().is_some_and(Vec::is_empty));
    assert_eq!(service.sessions.len(), 1);
    assert_eq!(
        service
            .sessions
            .values()
            .next()
            .expect("session")
            .state
            .roots
            .values()
            .next()
            .expect("root binding")
            .path,
        root.canonicalize().expect("canonical root")
    );
    service.shutdown().await.expect("shutdown");
}

#[test]
fn reopening_rejects_a_replaced_physical_root() {
    run_large_stack("root-identity-e2e", root_identity_case);
}

#[test]
fn author_configuration_is_user_scoped_and_overridable() {
    run_large_stack("author-config-e2e", author_configuration_case);
}

async fn author_configuration_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("state");
    fs::create_dir_all(&data).expect("state directory");
    fs::write(
        data.join("author.json"),
        br#"{"version":1,"author":"Configured Author <configured@example.test>"}"#,
    )
    .expect("author configuration");
    let control = ControlPlane::open(data).await.expect("control");
    assert_eq!(
        control
            .author_for_argv(
                &["commit".to_owned(), "-m".to_owned(), "x".to_owned()],
                "host"
            )
            .expect("configured author"),
        "Configured Author <configured@example.test>"
    );
    assert_eq!(
        control
            .author_for_argv(
                &[
                    "commit".to_owned(),
                    "--author=Explicit <explicit@example.test>".to_owned(),
                ],
                "host",
            )
            .expect("explicit author"),
        "host"
    );
}

async fn root_identity_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("state");
    let root = temporary.path().join("root");
    fs::create_dir(&root).expect("root");
    fs::create_dir_all(&data).expect("state directory");
    let local = LocalFs::local(LocalOptions::new(data.join("filesystem")))
        .await
        .expect("local filesystem");
    let store = LocalCoreStateStore::open_owned(data.join("core-state")).expect("state owner");
    let shared_roots = SharedRootRegistry::default();
    let mut control = ControlPlane::open_with(
        data.clone(),
        data.clone(),
        local.clone(),
        store.clone(),
        shared_roots.clone(),
    )
    .await
    .expect("control");
    control
        .session_start(json!({"session_id":"session","cwd":root}))
        .await
        .expect("session");
    control.shutdown().await.expect("shutdown");
    let displaced = temporary.path().join("displaced-root");
    fs::rename(&root, &displaced).expect("displace original root");
    fs::create_dir(&root).expect("replacement root");
    let Err(error) = ControlPlane::open_with(data.clone(), data, local, store, shared_roots).await
    else {
        panic!("replaced root must be rejected");
    };
    assert!(error.contains("changed identity"), "{error}");
}

#[test]
fn copilot_hooks_route_a_child_without_environment_state() {
    run_large_stack("copilot-hook-e2e", copilot_hook_case);
}

#[test]
fn cursor_root_hook_is_capability_honest_and_uses_workspace_roots() {
    run_large_stack("cursor-hook-e2e", cursor_hook_case);
}

#[test]
fn host_capability_profiles_never_conflate_routing_with_confinement() {
    for host in ["codex", "claude-code"] {
        let profile = host_adapter_profile(host);
        assert_eq!(
            profile.child_workspaces,
            ChildWorkspaceSupport::RecursiveToolRouting
        );
        assert!(profile.stable_child_identity);
        let guidance = guidance_for(host);
        assert!(guidance.contains("routing"));
        assert!(guidance.contains("not process-level confinement"));
        assert!(!guidance.contains("subagents isolated"));
    }
    let copilot = host_adapter_profile("copilot");
    assert_eq!(
        copilot.child_workspaces,
        ChildWorkspaceSupport::RootLifecycleOnly
    );
    assert!(!copilot.stable_child_identity);
    for host in ["cursor", "opencode", "unknown"] {
        assert_eq!(
            host_adapter_profile(host).child_workspaces,
            ChildWorkspaceSupport::CliOnly
        );
        assert!(guidance_for(host).contains("CLI-only"));
    }
}

async fn cursor_hook_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    fs::create_dir(&root).expect("root");
    let hook_directory = temporary.path().join("cursor-config");
    fs::create_dir(&hook_directory).expect("hook directory");
    let mut service = ServiceControl::open(temporary.path().join("state"))
        .await
        .expect("service");
    let output = service
        .dispatch_native_hook(
            "cursor",
            "sessionStart",
            json!({
                "session_id":"cursor-session",
                "workspace_roots":[root.display().to_string()]
            }),
            &hook_directory,
        )
        .await
        .expect("session start");
    let guidance = output["additional_context"]
        .as_str()
        .expect("Cursor guidance");
    assert!(guidance.contains("CLI-only"));
    assert!(!guidance.contains("native subagents isolated"));
    assert_eq!(
        service.sessions["cursor-session"]
            .state
            .roots
            .values()
            .next()
            .expect("root binding")
            .path,
        root.canonicalize().expect("canonical root")
    );
    service.shutdown().await.expect("shutdown");
}

async fn copilot_hook_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    fs::create_dir(&root).expect("root");
    let mut service = ServiceControl::open(temporary.path().join("state"))
        .await
        .expect("service");
    service
        .dispatch_native_hook(
            "copilot",
            "sessionStart",
            json!({"sessionId":"session","cwd":root.display().to_string()}),
            &root,
        )
        .await
        .expect("session start");
    service
        .dispatch_native_hook(
            "copilot",
            "preToolUse",
            json!({
                "sessionId":"session",
                "cwd":root.display().to_string(),
                "toolName":"task",
                "toolArgs":{"description":"child"}
            }),
            &root,
        )
        .await
        .expect("spawn handshake");
    let child = service
        .dispatch_native_hook(
            "copilot",
            "subagentStart",
            json!({
                "sessionId":"session",
                "cwd":root.display().to_string(),
                "agentName":"child",
                "agentType":"custom"
            }),
            &root,
        )
        .await
        .expect("child start");
    assert!(child.to_string().contains("acyclic git"));
    let child_path = route_path(&service.sessions["session"].state.routes["child"]);
    assert!(
        service
            .dispatch_native_hook(
                "claude-code",
                "PreToolUse",
                json!({
                    "session_id":"session",
                    "cwd":child_path.display().to_string(),
                    "tool_name":"Bash",
                    "tool_input":{"cmd":"pwd"}
                }),
                &child_path,
            )
            .await
            .is_err()
    );
    assert!(service.sessions["session"].state.leases.is_empty());
    let routed = service
        .dispatch_native_hook(
            "copilot",
            "preToolUse",
            json!({
                "sessionId":"session",
                "cwd":child_path.display().to_string(),
                "toolUseId":"copilot-bash-1",
                "toolName":"Bash",
                "toolArgs":{"cmd":"pwd","workdir":root.display().to_string()}
            }),
            &child_path,
        )
        .await
        .expect("child tool");
    assert_eq!(
        routed["modifiedArgs"]["workdir"],
        child_path.display().to_string()
    );
    service.shutdown().await.expect("shutdown");
}

#[test]
fn claude_hooks_preserve_tool_identity_rewrite_cwd_and_fail_closed() {
    run_large_stack("claude-hook-e2e", claude_hook_case);
}

#[test]
fn codex_hooks_route_child_tools_by_documented_turn_identity() {
    run_large_stack("codex-hook-turn-identity", codex_hook_turn_identity_case);
}

#[test]
fn codex_mcp_hooks_reach_the_service_exactly_as_command_hooks() {
    run_large_stack("codex-mcp-equivalence", codex_mcp_equivalence_case);
}

/// One Codex hook event exactly as Codex serializes it for a command hook
/// (every field its schema requires, and `agent_id`/`agent_type` only in
/// a spawned subagent's thread), with the thread Codex names in an MCP
/// hook call's metadata.
struct CodexEvent {
    name: &'static str,
    thread: &'static str,
    fields: Value,
}

fn codex_session_events(root: &Path) -> Vec<CodexEvent> {
    let root = root.display().to_string();
    let common = |event: &str, turn: &str| {
        json!({
            "session_id": "root-thread",
            "turn_id": turn,
            "transcript_path": null,
            "cwd": root,
            "hook_event_name": event,
            "model": "gpt-5.6-sol",
            "permission_mode": "bypassPermissions",
        })
    };
    let with = |mut base: Value, extra: Value| {
        let object = base.as_object_mut().expect("event object");
        object.extend(extra.as_object().expect("extra object").clone());
        base
    };
    let child = |event: &'static str, turn: &str, extra: Value| CodexEvent {
        name: event,
        thread: "child-thread",
        fields: with(
            common(event, turn),
            with(
                json!({"agent_id":"child-thread","agent_type":"explorer"}),
                extra,
            ),
        ),
    };
    let bash = |id: &str, command: &str| json!({"tool_name":"Bash","tool_use_id":id,"tool_input":{"command":command}});
    let spawn = json!({
        "tool_name":"collaborationspawn_agent","tool_use_id":"spawn-child",
        "tool_input":{"message":"inspect","agent_type":"explorer"}
    });
    vec![
        CodexEvent {
            name: "UserPromptSubmit",
            thread: "root-thread",
            fields: with(
                common("UserPromptSubmit", "root-turn"),
                json!({"prompt":"Run it."}),
            ),
        },
        CodexEvent {
            name: "PreToolUse",
            thread: "root-thread",
            fields: with(
                common("PreToolUse", "root-turn"),
                bash("root-bash", "git status"),
            ),
        },
        CodexEvent {
            name: "PostToolUse",
            thread: "root-thread",
            fields: with(
                common("PostToolUse", "root-turn"),
                with(
                    bash("root-bash", "git status"),
                    json!({"tool_response":"clean"}),
                ),
            ),
        },
        CodexEvent {
            name: "PreToolUse",
            thread: "root-thread",
            fields: with(common("PreToolUse", "root-turn"), spawn.clone()),
        },
        CodexEvent {
            name: "SubagentStart",
            thread: "child-thread",
            fields: with(
                common("SubagentStart", "child-turn"),
                json!({"agent_id":"child-thread","agent_type":"explorer"}),
            ),
        },
        // A later turn of the child, bound to it by its thread alone.
        child("PreToolUse", "child-later-turn", bash("child-pwd", "pwd")),
        child(
            "PostToolUse",
            "child-later-turn",
            with(bash("child-pwd", "pwd"), json!({"tool_response":"/"})),
        ),
        // An internal Codex subagent has a thread of its own but no agent
        // identity, and its unbound turn is refused on both paths.
        CodexEvent {
            name: "PreToolUse",
            thread: "review-thread",
            fields: with(common("PreToolUse", "review-turn"), bash("review-ls", "ls")),
        },
        CodexEvent {
            name: "SubagentStop",
            thread: "child-thread",
            fields: with(
                common("SubagentStop", "child-later-turn"),
                json!({
                    "agent_id":"child-thread","agent_type":"explorer",
                    "agent_transcript_path":null,"stop_hook_active":false,
                    "last_assistant_message":"done"
                }),
            ),
        },
        CodexEvent {
            name: "PostToolUse",
            thread: "root-thread",
            fields: with(
                common("PostToolUse", "root-turn"),
                with(spawn, json!({"tool_response":{"agent_id":"child-thread"}})),
            ),
        },
    ]
}

/// Codex's `mcp_tool` argument expansion: a string that is exactly one
/// `${path}` placeholder becomes that event field's JSON value, and a
/// missing field fails the hook.
fn codex_expand(template: &Value, event: &Value) -> Result<Value, String> {
    match template {
        Value::Object(object) => object
            .iter()
            .map(|(key, value)| Ok((key.clone(), codex_expand(value, event)?)))
            .collect::<Result<serde_json::Map<_, _>, String>>()
            .map(Value::Object),
        Value::String(text) => {
            let Some(path) = text
                .strip_prefix("${")
                .and_then(|path| path.strip_suffix('}'))
            else {
                assert!(!text.contains("${"), "placeholders fill whole strings");
                return Ok(template.clone());
            };
            assert!(
                !path.contains(['{', '}', '$']),
                "one placeholder per string"
            );
            path.split('.')
                .try_fold(event, |value, field| value.get(field))
                .cloned()
                .ok_or_else(|| format!("hook input placeholder `{text}` was not found"))
        }
        _ => Ok(template.clone()),
    }
}

/// Runs one Codex session through a fresh service, delivering its events
/// as Codex's MCP hook calls when `mcp` is set and as command hooks
/// otherwise, and returns every answer with this run's identities made
/// neutral.
async fn codex_session_answers(mcp: bool) -> Vec<Result<Value, String>> {
    let manifest: Value =
        serde_json::from_str(include_str!("../hooks/hooks.json")).expect("hook manifest");
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    fs::create_dir(&root).expect("root");
    let service = ConcurrentServiceControl::open(temporary.path().join("state"))
        .await
        .expect("service");
    service
        .dispatch_request(super::native_hook_request(
            "codex",
            "SessionStart",
            root.clone(),
            json!({
                "session_id":"root-thread","transcript_path":null,"cwd":root,
                "hook_event_name":"SessionStart","model":"gpt-5.6-sol",
                "permission_mode":"bypassPermissions","source":"startup"
            }),
        ))
        .await
        .expect("session start");
    let mut answers = Vec::new();
    for event in codex_session_events(&root) {
        let request = if mcp {
            let template = manifest
                .pointer(&format!("/hooks/{}/0/hooks/0/input", event.name))
                .expect("MCP hook template");
            let arguments = codex_expand(template, &event.fields).expect("template fields");
            let (name, cwd, input) =
                codex_hook_mcp::hook_request(Some(&arguments), Some(event.thread.to_owned()))
                    .expect("hook call");
            super::native_hook_request("codex", name, cwd, input)
        } else {
            let cwd = PathBuf::from(event.fields["cwd"].as_str().expect("cwd"));
            super::native_hook_request("codex", event.name, cwd, event.fields)
        };
        answers.push(service.dispatch_request(request).await);
    }
    service.shutdown().await.expect("service shutdown");
    // Each run has its own directory and freshly generated workspace,
    // root and generation identities (long base64url or hex words).
    let neutral = |text: String| {
        let temporary = temporary.path().display().to_string();
        let escaped = serde_json::to_string(&temporary).expect("escaped path");
        let text = text
            .replace(escaped.trim_matches('"'), "<run>")
            .replace(&temporary, "<run>")
            .replace(&temporary.replace('\\', "/"), "<run>");
        let mut neutral = String::with_capacity(text.len());
        let mut word = String::new();
        for character in text.chars().chain(std::iter::once(' ')) {
            if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
                word.push(character);
                continue;
            }
            neutral.push_str(if word.len() >= 20 { "<id>" } else { &word });
            word.clear();
            neutral.push(character);
        }
        neutral.pop();
        neutral
    };
    answers
        .into_iter()
        .map(|answer| match answer {
            Ok(value) => {
                Ok(serde_json::from_str(&neutral(value.to_string())).expect("neutral answer"))
            }
            Err(error) => Err(neutral(error)),
        })
        .collect()
}

async fn codex_mcp_equivalence_case() {
    let command = codex_session_answers(false).await;
    let mcp = codex_session_answers(true).await;
    for (index, answer) in command.iter().enumerate() {
        // Only the internal subagent's unbound turn is refused.
        assert_eq!(answer.is_ok(), index != 7, "event {index}: {answer:?}");
    }
    assert_eq!(command, mcp);
}

fn run_large_stack<F>(name: &str, make: impl FnOnce() -> F + Send + 'static)
where
    F: std::future::Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name(name.to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(make());
        })
        .expect("test thread")
        .join()
        .expect("large-stack test thread");
}

async fn claude_hook_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    fs::create_dir(&root).expect("root");
    let mut service = ServiceControl::open(temporary.path().join("state"))
        .await
        .expect("service");
    service
        .dispatch_native_hook(
            "claude-code",
            "SessionStart",
            json!({"session_id":"session","cwd":root.display().to_string()}),
            &root,
        )
        .await
        .expect("session start");
    service
        .dispatch_native_hook(
            "claude-code",
            "UserPromptSubmit",
            json!({"session_id":"session","cwd":root.display().to_string()}),
            &root,
        )
        .await
        .expect("root prompt");
    service
        .dispatch_native_hook(
            "claude-code",
            "PreToolUse",
            json!({
                "session_id":"session",
                "cwd":root.display().to_string(),
                "tool_name":"Agent",
                "tool_use_id":"spawn-1",
                "tool_input":{"description":"child"}
            }),
            &root,
        )
        .await
        .expect("spawn handshake");
    service
        .dispatch_native_hook(
            "claude-code",
            "SubagentStart",
            json!({
                "session_id":"session",
                "cwd":root.display().to_string(),
                "agent_id":"child",
                "agent_type":"general-purpose"
            }),
            &root,
        )
        .await
        .expect("child start");
    let child_path = route_path(&service.sessions["session"].state.routes["child"]);
    let routed = service
        .dispatch_native_hook(
            "claude-code",
            "PreToolUse",
            json!({
                "session_id":"session",
                "cwd":root.display().to_string(),
                "agent_id":"child",
                "agent_type":"general-purpose",
                "tool_name":"Bash",
                "tool_use_id":"tool-1",
                "tool_input":{"command":"pwd"}
            }),
            &root,
        )
        .await
        .expect("child tool");
    let child_shell_path = child_path.display().to_string().replace('\\', "/");
    assert!(
        routed["hookSpecificOutput"]["updatedInput"]["command"]
            .as_str()
            .is_some_and(|command| command.contains(&child_shell_path))
    );
    assert!(
        service.sessions["session"]
            .state
            .leases
            .contains_key("tool-1")
    );
    assert!(
        service
            .dispatch_native_hook(
                "claude-code",
                "PreToolUse",
                json!({
                    "session_id":"session",
                    "cwd":child_path.display().to_string(),
                    "tool_name":"UnknownWriter",
                    "tool_use_id":"tool-2",
                    "tool_input":{}
                }),
                &child_path,
            )
            .await
            .is_err()
    );
    assert!(
        service
            .dispatch_native_hook(
                "claude-code",
                "PreToolUse",
                json!({
                    "session_id":"session",
                    "cwd":root.display().to_string(),
                    "agent_id":"unknown",
                    "agent_type":"general-purpose",
                    "tool_name":"Bash",
                    "tool_use_id":"tool-forged",
                    "tool_input":{"command":"pwd"}
                }),
                &root,
            )
            .await
            .is_err()
    );
    service
        .dispatch_native_hook(
            "claude-code",
            "PostToolUseFailure",
            json!({
                "session_id":"session",
                "cwd":root.display().to_string(),
                "agent_id":"child",
                "agent_type":"general-purpose",
                "tool_name":"Bash",
                "tool_use_id":"tool-1",
                "tool_input":{"command":"pwd"}
            }),
            &root,
        )
        .await
        .expect("failed tool close");
    assert!(service.sessions["session"].state.leases.is_empty());
    service
        .dispatch_native_hook(
            "claude-code",
            "SubagentStop",
            json!({
                "session_id":"session",
                "cwd":child_path.display().to_string(),
                "agent_id":"child"
            }),
            &child_path,
        )
        .await
        .expect("child stop");
    service
        .dispatch_native_hook(
            "claude-code",
            "SubagentStop",
            json!({
                "session_id":"session",
                "cwd":child_path.display().to_string(),
                "agent_id":"child"
            }),
            &child_path,
        )
        .await
        .expect("duplicate child stop");
    assert!(
        service.sessions["session"].state.routes["child"]
            .lifecycle
            .is_frozen()
    );
    service
        .dispatch_native_hook(
            "claude-code",
            "SubagentStart",
            json!({
                "session_id":"session",
                "cwd":root.display().to_string(),
                "agent_id":"child",
                "agent_type":"general-purpose"
            }),
            &root,
        )
        .await
        .expect("child resume");
    assert_eq!(
        service.sessions["session"].state.routes["child"].lifecycle,
        RouteLifecycle::Active
    );
    let context_id = service.sessions["session"].state.root_context_id;
    service
        .dispatch_native_hook(
            "claude-code",
            "SessionStart",
            json!({
                "session_id":"session",
                "cwd":root.display().to_string(),
                "source":"compact"
            }),
            &root,
        )
        .await
        .expect("compact session start");
    assert_eq!(
        service.sessions["session"].state.root_context_id,
        context_id
    );
    service
        .dispatch_native_hook(
            "claude-code",
            "SessionEnd",
            json!({"session_id":"session"}),
            &root,
        )
        .await
        .expect("session end");
    assert!(!service.sessions.contains_key("session"));
    service
        .dispatch_native_hook(
            "claude-code",
            "SessionStart",
            json!({"session_id":"session","cwd":root.display().to_string(),"source":"resume"}),
            &root,
        )
        .await
        .expect("session resume");
    assert_eq!(
        service.sessions["session"].state.root_context_id,
        context_id
    );
    service.shutdown().await.expect("shutdown");
}

async fn codex_hook_turn_identity_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    fs::create_dir(&root).expect("root");
    let mut service = ServiceControl::open(temporary.path().join("state"))
        .await
        .expect("service");
    service
        .dispatch_native_hook(
            "codex",
            "SessionStart",
            json!({"session_id":"session","cwd":root}),
            &root,
        )
        .await
        .expect("session start");
    service
        .dispatch_native_hook(
            "codex",
            "UserPromptSubmit",
            json!({"session_id":"session","turn_id":"root-turn","cwd":root}),
            &root,
        )
        .await
        .expect("root turn");
    service
        .dispatch_native_hook(
            "codex",
            "PreToolUse",
            json!({
                "session_id":"session","turn_id":"root-turn","cwd":root,
                "tool_name":"Bash","tool_use_id":"root-agents",
                "tool_input":{"command":"acyclic agents"}
            }),
            &root,
        )
        .await
        .expect("root CLI service access");
    service
        .dispatch_native_hook(
            "codex",
            "PreToolUse",
            json!({
                "session_id":"session","turn_id":"root-turn","cwd":root,
                "tool_name":"collaborationspawn_agent","tool_use_id":"spawn-child","tool_input":{}
            }),
            &root,
        )
        .await
        .expect("spawn handshake");
    service
        .dispatch_native_hook(
            "codex",
            "SubagentStart",
            json!({
                "session_id":"session","turn_id":"child-turn","cwd":root,
                "agent_id":"child","agent_type":"explorer"
            }),
            &root,
        )
        .await
        .expect("child start");
    let child = route_path(&service.sessions["session"].state.routes["child"]);
    let routed = service
        .dispatch_native_hook(
            "codex",
            "PreToolUse",
            json!({
                "session_id":"session","turn_id":"child-tool-turn","agent_id":"child","cwd":root,
                "tool_name":"Bash","tool_use_id":"child-command",
                "tool_input":{"command":"pwd"}
            }),
            &root,
        )
        .await
        .expect("child tool routed from physical cwd");
    assert!(
        routed["hookSpecificOutput"]["updatedInput"]["command"]
            .as_str()
            .is_some_and(|command| command.contains(&child.to_string_lossy().replace('\\', "/")))
    );
    assert_eq!(
        service.sessions["session"].state.turns["child-tool-turn"],
        "child"
    );
    assert!(
        service
            .dispatch_native_hook(
                "codex",
                "PreToolUse",
                json!({
                    "session_id":"session","turn_id":"forged-turn","agent_id":"unknown","cwd":root,
                    "tool_name":"Bash","tool_use_id":"forged-command",
                    "tool_input":{"command":"pwd"}
                }),
                &root,
            )
            .await
            .is_err()
    );
    service.shutdown().await.expect("shutdown");
}

#[test]
fn recursive_publication_is_direct_parent_only() {
    std::thread::Builder::new()
        .name("plugin-e2e".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(recursive_publication_case());
        })
        .expect("test thread")
        .join()
        .expect("plugin e2e thread");
}

#[test]
fn multi_root_contexts_fork_route_publish_and_resume_atomically() {
    std::thread::Builder::new()
        .name("plugin-multi-root-e2e".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(multi_root_publication_case());
        })
        .expect("test thread")
        .join()
        .expect("plugin multi-root e2e thread");
}

#[test]
fn root_registration_intents_recover_before_native_reopen() {
    run_large_stack("core-root-recovery", core_root_recovery_case);
}

#[test]
fn lifecycle_edges_recover_and_fail_closed() {
    std::thread::Builder::new()
        .name("plugin-lifecycle-e2e".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(lifecycle_hardening_case());
        })
        .expect("test thread")
        .join()
        .expect("plugin lifecycle e2e thread");
}

/// A response reaches a client that is slow to read it, for as long as a
/// client can wait; a stopping service stops waiting after its grace.
#[tokio::test]
async fn responses_reach_slow_readers_until_the_service_stops() {
    let (_running, shutdown) = watch::channel(false);
    let slow = async {
        tokio::time::sleep(CONTROL_RESPONSE_DRAIN_GRACE * 3 / 2).await;
        Ok(())
    };
    assert_eq!(deliver_control_response(slow, shutdown).await, Ok(()));

    let (stopping, shutdown) = watch::channel(false);
    let never = std::future::pending::<Result<(), String>>();
    let delivering = tokio::spawn(deliver_control_response(never, shutdown));
    stopping.send(true).expect("stop the service");
    let error = delivering
        .await
        .expect("delivery task")
        .expect_err("a stopping service stops delivering");
    assert!(error.contains("drain deadline"), "{error}");
}

/// A client accepts a pipe only from a server running as its own user.
#[cfg(windows)]
#[tokio::test]
async fn a_pipe_served_by_this_user_is_accepted() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let pipe = windows_control_pipe_path(temporary.path());
    let _server = create_current_user_pipe(&pipe, true).expect("pipe server");
    let client = tokio::net::windows::named_pipe::ClientOptions::new()
        .open(&pipe)
        .expect("pipe client");
    assert!(windows_pipe_server_is_this_user(&client).expect("server user"));
    // SAFETY: the pseudo-handle of this process needs no closing.
    #[allow(unsafe_code)]
    let own =
        windows_token_user(unsafe { windows_sys::Win32::System::Threading::GetCurrentProcess() })
            .expect("own user");
    assert!(own.len() >= 8, "a SID has a header and an authority");
}

#[cfg(any(unix, windows))]
#[test]
fn service_drain_waits_for_shutdown_completion_and_lock_release() {
    std::thread::Builder::new()
        .name("plugin-service-drain".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(async {
                    let temporary = tempfile::tempdir().expect("temporary directory");
                    let data = temporary.path().join("state");
                    let service_data = data.clone();
                    let service = tokio::spawn(async move { run_service(service_data).await });
                    for _ in 0..250 {
                        if data.join("service.identity").exists() {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                    let identity = ServiceMarker::read(&data)
                        .expect("service identity")
                        .instance_id;
                    let mismatch = drain_service(&data, Some("replacement-service")).await;
                    assert!(matches!(
                        mismatch,
                        Err(error) if error == "refusing to drain a replacement Acyclic service"
                    ));
                    assert!(acquire_service_lock(&data).expect("service lock").is_none());
                    // Two requesters that ask at once are both answered.
                    let (first, second) = tokio::join!(
                        async {
                            drop(
                                drain_service(&data, Some(&identity))
                                    .await
                                    .expect("identity-bound durable drain"),
                            );
                        },
                        async {
                            drop(
                                drain_service(&data, Some(&identity))
                                    .await
                                    .expect("concurrent durable drain"),
                            );
                        }
                    );
                    let ((), ()) = (first, second);
                    tokio::time::timeout(std::time::Duration::from_secs(5), service)
                        .await
                        .expect("service exit deadline")
                        .expect("service task")
                        .expect("clean service shutdown");
                    assert!(!data.join("service.identity").exists());
                    assert!(service_drain_completion_path(&data, &identity).exists());
                    assert!(acquire_service_lock(&data).expect("service lock").is_some());
                });
        })
        .expect("test thread")
        .join()
        .expect("service drain thread");
}

/// A service of another release speaks another control protocol, so it
/// rejects every request of this one; it is still identified and stopped
/// through the handoff contract.
#[cfg(any(unix, windows))]
#[test]
fn a_service_speaking_another_protocol_is_identified_and_stopped() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("state");
    let service_data = data.clone();
    let older = std::thread::Builder::new()
        .name("older-protocol-service".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            control_protocol::TEST_PROTOCOL_MAJOR
                .with(|major| major.set(Some(control_protocol::CONTROL_PROTOCOL_MAJOR - 1)));
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("older service runtime")
                .block_on(run_service_with_identity(
                    service_data,
                    Some("older-service-binary".to_owned()),
                    None,
                ))
        })
        .expect("older service thread");
    std::thread::Builder::new()
        .name("current-protocol-client".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("client runtime")
                .block_on(async {
                    let mut marker = None;
                    for _ in 0..500 {
                        marker = ServiceMarker::read(&data);
                        if marker.is_some() {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                    let marker = marker.expect("older service marker");
                    assert_eq!(marker.binary_identity, "older-service-binary");
                    let ping = ping_request().expect("ping");
                    let rejected = send_control_request_once(&data, &ping).await;
                    assert!(
                        matches!(rejected, Err(ControlRequestError::Response(ref error)) if error.contains("incompatible Acyclic control protocol")),
                        "{rejected:?}"
                    );
                    let identity = service_identity(&data).expect("current identity");
                    assert!(
                        !service_is_ready_for_identity(&data, &identity)
                            .await
                            .expect("stop the older service")
                    );
                    assert!(ServiceMarker::read(&data).is_none());
                    assert!(acquire_service_lock(&data).expect("service lock").is_some());
                    let completion: Value = serde_json::from_slice(
                        &fs::read(service_drain_completion_path(&data, &marker.instance_id))
                            .expect("drain record"),
                    )
                    .expect("drain record json");
                    assert_eq!(completion["identity"], marker.instance_id.as_str());
                    assert_eq!(completion["ok"], true);
                });
        })
        .expect("client thread")
        .join()
        .expect("current protocol client");
    older
        .join()
        .expect("older service thread")
        .expect("older service drained cleanly");
}

#[cfg(any(unix, windows))]
#[test]
fn failed_identity_publication_releases_endpoint_service_and_root() {
    std::thread::Builder::new()
        .name("plugin-service-marker-failure".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(async {
                    let temporary = tempfile::tempdir().expect("temporary directory");
                    let data = temporary.path().join("state");
                    fs::create_dir_all(data.join("service.identity"))
                        .expect("identity publication obstruction");
                    assert!(
                        run_service_with_identity(data.clone(), Some("service".to_owned()), None)
                            .await
                            .is_err()
                    );
                    #[cfg(unix)]
                    assert!(!data.join("service.sock").exists());
                    let fence = acquire_service_lock(&data)
                        .expect("service lifecycle lock")
                        .expect("failed startup released the service lifecycle");
                    drop(fence);
                    let reopened = ServiceControl::open(data)
                        .await
                        .expect("reopen root after failed service startup");
                    reopened.shutdown().await.expect("reopened shutdown");
                });
        })
        .expect("test thread")
        .join()
        .expect("service marker failure thread");
}

#[test]
fn service_lock_contention_uses_platform_error_semantics() {
    assert!(service_lock_is_contended(&io::Error::from(
        io::ErrorKind::WouldBlock
    )));
    #[cfg(windows)]
    assert!(service_lock_is_contended(&io::Error::from_raw_os_error(33)));
}

#[test]
fn service_identity_follows_artifact_bytes_not_launcher_path() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path();
    let launcher = data.join("launcher");
    let plugin_cache = data.join("plugin-cache");
    fs::write(&launcher, b"same signed artifact").expect("launcher artifact");
    fs::write(&plugin_cache, b"same signed artifact").expect("cached artifact");
    let identity = |path: &Path| service_identity_for(data, path).expect("identity");

    assert_eq!(identity(&launcher), identity(&plugin_cache));
    assert_eq!(
        identity(&launcher),
        executable_digests_for(&launcher)
            .expect("full launcher digests")
            .service_identity
    );

    fs::write(&plugin_cache, b"replacement artifact").expect("replacement artifact");
    assert_ne!(identity(&launcher), identity(&plugin_cache));
}

#[test]
fn cached_service_identity_is_keyed_by_the_file_fingerprint() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path();
    let executable = data.join("acyclic");
    fs::write(&executable, b"first artifact").expect("artifact");
    let identity = service_identity_for(data, &executable).expect("identity");
    let cache = data.join("executable-identity");
    assert!(
        !cache.exists(),
        "a just-written executable could change again within its timestamp tick"
    );
    std::thread::sleep(SETTLED_EXECUTABLE_AGE + std::time::Duration::from_millis(100));
    assert_eq!(
        service_identity_for(data, &executable).expect("settled identity"),
        identity
    );
    let entry = fs::read_to_string(&cache).expect("cached identity");
    assert!(entry.ends_with(&identity));

    // A hit returns the cached value without hashing the bytes again.
    let (fingerprint, _) = entry.split_once('\n').expect("cache entry");
    let marker = "0".repeat(64);
    fs::write(&cache, format!("{fingerprint}\n{marker}")).expect("marked cache");
    assert_eq!(
        service_identity_for(data, &executable).expect("cached identity"),
        marker
    );

    // Any rewrite changes the fingerprint, even to bytes of equal length.
    fs::write(&executable, b"other artifact").expect("rewritten artifact");
    let rewritten = service_identity_for(data, &executable).expect("rewritten identity");
    assert_ne!(rewritten, marker);
    assert_eq!(
        rewritten,
        executable_digests_for(&executable)
            .expect("rewritten digests")
            .service_identity
    );

    for torn in [b"".as_slice(), b"torn", entry.as_bytes().split_at(70).0] {
        fs::write(&cache, torn).expect("torn cache");
        assert_eq!(
            service_identity_for(data, &executable).expect("identity despite torn cache"),
            rewritten
        );
    }
}

#[test]
fn unreachable_service_cleanup_clears_only_its_stale_marker_under_lock() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(async {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let data = temporary.path().join("state");
            fs::create_dir_all(&data).expect("service data");
            fs::write(data.join("service.identity"), "dead-service").expect("stale identity");
            fs::write(data.join("durable-state"), "preserved").expect("durable sentinel");
            let fence = drain_service(&data, None)
                .await
                .expect("dead service cleanup fence");
            assert!(!data.join("service.identity").exists());
            assert_eq!(
                fs::read_to_string(data.join("durable-state")).expect("durable sentinel"),
                "preserved"
            );
            assert!(
                acquire_service_lock(&data)
                    .expect("contended service lock")
                    .is_none()
            );
            drop(fence);
            assert!(
                acquire_service_lock(&data)
                    .expect("released service lock")
                    .is_some()
            );
        });
}

#[test]
fn parent_lifecycle_fence_survives_removal_of_the_service_data_tree() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("state");
    let mut fence = acquire_service_lock(&data)
        .expect("acquire service lock")
        .expect("uncontended service lock");
    fence.prepare_purge();
    fs::remove_dir_all(&data).expect("remove service data while parent fence remains held");
    assert!(
        acquire_service_lock(&data)
            .expect("contended lifecycle check")
            .is_none(),
        "a replacement service must not create a new in-tree lock during purge"
    );
    drop(fence);
    assert!(
        acquire_service_lock(&data)
            .expect("reacquire after purge")
            .is_some()
    );
}

#[cfg(any(unix, windows))]
#[test]
fn service_handoff_durably_drains_a_mismatched_binary() {
    std::thread::Builder::new()
        .name("plugin-service-handoff".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(async {
                    let temporary = tempfile::tempdir().expect("temporary directory");
                    let data = temporary.path().join("state");
                    let service_data = data.clone();
                    let service = tokio::spawn(async move {
                        run_service_with_identity(
                            service_data,
                            Some("older-service-binary".to_owned()),
                            None,
                        )
                        .await
                    });
                    for _ in 0..250 {
                        if data.join("service.identity").exists() {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                    let instance_id = ServiceMarker::read(&data)
                        .expect("service instance identity")
                        .instance_id;
                    assert!(uuid::Uuid::parse_str(&instance_id).is_ok());
                    // Workspace state outlives the binary that wrote it.
                    let kept = ["core-state", "filesystem", "sessions", "w"]
                        .map(|name| data.join(name).join("kept"));
                    for path in &kept {
                        std::fs::create_dir_all(path.parent().expect("state directory"))
                            .expect("state directory");
                        std::fs::write(path, b"kept").expect("state file");
                    }
                    assert!(
                        !service_is_ready_for_identity(&data, "replacement-service-binary")
                            .await
                            .expect("durable handoff")
                    );
                    tokio::time::timeout(std::time::Duration::from_secs(5), service)
                        .await
                        .expect("service exit deadline")
                        .expect("service task")
                        .expect("clean service shutdown");
                    assert!(service_drain_completion_path(&data, &instance_id).exists());
                    assert!(acquire_service_lock(&data).expect("service lock").is_some());
                    assert!(kept.iter().all(|path| path.exists()));
                });
        })
        .expect("test thread")
        .join()
        .expect("service handoff thread");
}

#[cfg(any(unix, windows))]
#[test]
fn service_handoff_durably_drains_live_sessions_before_replacement() {
    std::thread::Builder::new()
        .name("plugin-live-service-handoff".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(async {
                    let temporary = tempfile::tempdir().expect("temporary directory");
                    let data = temporary.path().join("state");
                    let root = temporary.path().join("root");
                    fs::create_dir(&root).expect("root directory");
                    let service_data = data.clone();
                    let service = tokio::spawn(async move {
                        run_service_with_identity(
                            service_data,
                            Some("older-service-binary".to_owned()),
                            None,
                        )
                        .await
                    });
                    for _ in 0..250 {
                        if data.join("service.identity").exists() {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                    send_control_request(
                        &data,
                        &ControlRequest {
                            version: 1,
                            command: ControlCommand::Hook,
                            cwd: root.clone(),
                            argv: Vec::new(),
                            name: "codex:SessionStart".to_owned(),
                            arguments: json!({"session_id":"live","cwd":root.clone()}),
                        },
                    )
                    .await
                    .expect("start live session");

                    assert!(
                        !service_is_ready_for_identity(&data, "replacement-service-binary")
                            .await
                            .expect("drain live service")
                    );
                    tokio::time::timeout(std::time::Duration::from_secs(5), service)
                        .await
                        .expect("service exit deadline")
                        .expect("service task")
                        .expect("clean service shutdown");
                    let reopened = ServiceControl::open(data.clone())
                        .await
                        .expect("reopen drained service state");
                    assert!(
                        reopened.sessions.is_empty(),
                        "handoff-drained sessions must not reopen"
                    );
                });
        })
        .expect("test thread")
        .join()
        .expect("live service handoff thread");
}

#[test]
fn explicit_shutdown_processes_sessions_after_an_earlier_failure() {
    run_large_stack("complete-service-shutdown", || async {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("state");
        let mut service = ServiceControl::open(data.clone()).await.expect("service");

        {
            let broken = service
                .create_session("a-broken")
                .await
                .expect("broken session");
            broken.state.active = true;
            broken.state.root_session_id = "a-broken".to_owned();
            broken.state.leases.insert(
                "orphaned-lease".to_owned(),
                LeaseRecord {
                    agent_id: "missing-agent".to_owned(),
                    turn_id: String::new(),
                    tool_name: "Bash".to_owned(),
                    roots: BTreeMap::new(),
                    expires_at_millis: 0,
                },
            );
            broken.persist().expect("broken session state");
        }
        let broken_directory = service.session_directory("a-broken");
        {
            let healthy = service
                .create_session("z-healthy")
                .await
                .expect("healthy session");
            healthy.state.active = true;
            healthy.state.root_session_id = "z-healthy".to_owned();
            healthy.persist().expect("healthy session state");
        }
        let healthy_directory = service.session_directory("z-healthy");

        let error = service
            .shutdown_sessions(true)
            .await
            .expect_err("orphaned lease must fail shutdown");
        assert!(error.contains("a-broken"));
        assert!(service.sessions.is_empty());
        assert!(
            load_saved_state(&broken_directory)
                .expect("failed session remains recoverable")
                .active,
            "failed teardown must not publish an inactive session"
        );
        assert!(
            !load_saved_state(&healthy_directory)
                .expect("healthy session state")
                .active,
            "a later session must be durably inactive despite an earlier failure"
        );
    });
}

#[cfg(any(windows, all(unix, not(target_os = "linux"))))]
#[test]
fn endpoint_shutdown_cancels_inflight_requests_before_reopen() {
    std::thread::Builder::new()
        .name("plugin-endpoint-shutdown".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(endpoint_shutdown_case());
        })
        .expect("test thread")
        .join()
        .expect("plugin endpoint shutdown thread");
}

#[cfg(any(unix, windows))]
#[test]
fn concurrent_spawn_and_child_tool_hooks_both_complete() {
    run_large_stack("concurrent-hook-endpoint", concurrent_hook_endpoint_case);
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires a real Lean project path in ACYCLIC_E2E_LEAN_WORKSPACE"]
fn real_lean_compiler_executes_inside_the_child_mount() {
    run_large_stack("real-lean-child-mount", real_lean_child_mount_case);
}

#[cfg(any(target_os = "linux", windows))]
#[test]
#[ignore = "requires a live native mount and the installed Rust compiler"]
fn rustc_executes_inside_the_child_mount() {
    run_large_stack("rustc-child-mount", rustc_child_mount_case);
}

#[cfg(any(windows, all(unix, not(target_os = "linux"))))]
#[test]
fn client_disconnect_does_not_cancel_inflight_control_dispatch() {
    std::thread::Builder::new()
        .name("plugin-endpoint-disconnect".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(endpoint_disconnect_case());
        })
        .expect("test thread")
        .join()
        .expect("plugin endpoint disconnect thread");
}

#[cfg(target_os = "linux")]
#[test]
fn mailbox_shutdown_drains_inflight_dispatch() {
    run_large_stack(
        "plugin-mailbox-shutdown",
        mailbox_shutdown_drains_dispatch_case,
    );
}

#[cfg(target_os = "linux")]
#[test]
fn stalled_mailbox_respects_the_caller_deadline() {
    run_large_stack("plugin-mailbox-deadline", mailbox_deadline_case);
}

#[cfg(target_os = "linux")]
#[test]
fn malformed_mailbox_exchange_does_not_stop_the_endpoint() {
    run_large_stack("plugin-mailbox-malformed", mailbox_malformed_exchange_case);
}

#[cfg(any(windows, all(unix, not(target_os = "linux"))))]
#[test]
fn stalled_control_stream_respects_the_caller_deadline() {
    run_large_stack("plugin-stream-deadline", stream_deadline_case);
}

#[cfg(windows)]
#[test]
fn windows_control_endpoint_keeps_the_next_pipe_while_reaping_connections() {
    std::thread::Builder::new()
        .name("plugin-endpoint-sequential".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(sequential_endpoint_case());
        })
        .expect("test thread")
        .join()
        .expect("plugin sequential endpoint thread");
}

#[test]
fn authenticated_control_protocol_dispatches_git() {
    std::thread::Builder::new()
        .name("plugin-control-protocol".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(authenticated_control_protocol_case());
        })
        .expect("test thread")
        .join()
        .expect("plugin control protocol thread");
}

#[test]
fn root_git_uses_the_same_repository_and_materializer() {
    std::thread::Builder::new()
        .name("plugin-root-git".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(root_git_case());
        })
        .expect("test thread")
        .join()
        .expect("plugin root Git thread");
}

#[test]
#[ignore = "requires live native mounts"]
fn child_local_git_merge_control_stays_on_git_facade() {
    run_large_stack("child-local-git-routing", child_local_git_merge_control_case);
}

/// Writes `name` into `directory` under `root` from a separate process, as an
/// agent's tool does. `ProjFS` reports only other processes' I/O to its
/// provider, which runs in this test's process.
fn write_as_another_process(root: &Path, directory: &str, name: &str, contents: &str) {
    #[cfg(windows)]
    let status = std::process::Command::new("cmd.exe")
        .args([
            "/D",
            "/C",
            &format!(
                "(if not exist {directory} mkdir {directory}) && (echo {contents}> {directory}\\{name})"
            ),
        ])
        .current_dir(root)
        .status()
        .expect("spawn writer");
    #[cfg(not(windows))]
    let status = std::process::Command::new("sh")
        .args([
            "-c",
            &format!("mkdir -p {directory} && printf %s {contents} > {directory}/{name}"),
        ])
        .current_dir(root)
        .status()
        .expect("spawn writer");
    assert!(status.success(), "external write failed");
}

#[test]
fn unobserved_parent_directory_reports_a_typed_merge_conflict() {
    std::thread::Builder::new()
        .name("plugin-unobserved-directory-merge".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(async {
                    let temporary = tempfile::tempdir().expect("temporary directory");
                    let root = temporary.path().join("root");
                    fs::create_dir_all(root.join("sub")).expect("unobserved directory");
                    let mut control = ControlPlane::open(temporary.path().join("plugin-data"))
                        .await
                        .expect("control plane");
                    control
                        .session_start(
                            json!({"session_id":"session","cwd":root.display().to_string()}),
                        )
                        .await
                        .expect("root session");
                    control
                        .user_prompt(json!({"session_id":"session","turn_id":"root-turn"}))
                        .expect("root turn");
                    control
                        .pre_tool(json!({
                            "session_id":"session","turn_id":"root-turn",
                            "tool_use_id":"spawn-child","tool_name":"spawn_agent","tool_input":{}
                        }))
                        .await
                        .expect("spawn child");
                    control
                        .subagent_start(json!({
                            "session_id":"session","turn_id":"child-turn",
                            "agent_id":"child","agent_type":"explorer"
                        }))
                        .await
                        .expect("child start");
                    let child_root = route_path(&control.state.routes["child"]);
                    write_as_another_process(&child_root, "sub", "cache.tmp", "child");
                    fs::write(root.join("sub/cache.tmp"), b"parent").expect("parent file");
                    let result = control
                        .agent_merge(json!({"agent":"child","_caller_turn_id":"root-turn"}))
                        .await
                        .expect("typed conflict, not an invalid candidate");
                    // Both directories merge by path; only the file both
                    // sides added under one name conflicts.
                    assert_eq!(result["status"], "conflicted", "{result}");
                    assert!(
                        result["conflicts"].as_array().is_some_and(|conflicts| {
                            conflicts.len() == 1
                                && conflicts.iter().all(|conflict| {
                                    conflict["path"] == "/sub/cache.tmp"
                                        && conflict["kind"] == "Binding"
                                })
                        }),
                        "{result}"
                    );
                    assert_eq!(
                        fs::read(root.join("sub/cache.tmp")).expect("parent file remains"),
                        b"parent"
                    );
                });
        })
        .expect("test thread")
        .join()
        .expect("unobserved directory merge thread");
}

#[test]
fn publication_history_recovers_after_a_crash_boundary() {
    std::thread::Builder::new()
        .name("plugin-publication-history-recovery".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime")
                .block_on(publication_history_recovery_case());
        })
        .expect("test thread")
        .join()
        .expect("publication history recovery thread");
}

async fn publication_history_recovery_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    let root = temporary.path().join("root");
    fs::create_dir_all(&root).expect("root directory");
    fs::create_dir_all(root.join("sub")).expect("baseline nested directory");
    let mut control = ControlPlane::open(data.clone())
        .await
        .expect("control plane");
    control
        .session_start(json!({"session_id":"session","cwd":root.display().to_string()}))
        .await
        .expect("root session");
    control
        .root_git_tool(
            WorkspaceRootId::from_bytes(control.state.root_id),
            vec!["status".to_owned()],
        )
        .await
        .expect("observe baseline nested directory");
    control
        .user_prompt(json!({"session_id":"session","turn_id":"root-turn"}))
        .expect("root turn");
    control
        .pre_tool(json!({
            "session_id":"session","turn_id":"root-turn","tool_use_id":"spawn-child",
            "tool_name":"spawn_agent","tool_input":{}
        }))
        .await
        .expect("spawn child");
    control
        .subagent_start(json!({
            "session_id":"session","turn_id":"child-turn",
            "agent_id":"child","agent_type":"explorer"
        }))
        .await
        .expect("child start");
    let child = control
        .workspace(&control.state.routes["child"])
        .await
        .expect("child workspace");
    let mut transaction = child
        .begin_transaction(IdempotencyKey::new())
        .await
        .expect("child transaction");
    transaction
        .write_text("/published.txt", "published")
        .await
        .expect("child file");
    transaction
        .write_text("/.gitignore", "*.tmp\n")
        .await
        .expect("ignore policy");
    transaction
        .write_text("/scratch.tmp", "publish without tracking")
        .await
        .expect("ignored child file");
    transaction
        .create_dir_all("/sub")
        .await
        .expect("nested ignore directory");
    transaction
        .write_text("/sub/.gitignore", "*.tmp\n")
        .await
        .expect("nested ignore policy");
    transaction
        .write_text("/sub/cache.tmp", "nested child cache")
        .await
        .expect("nested ignored child file");
    transaction.commit().await.expect("child commit");
    fs::write(root.join("scratch.tmp"), b"competing parent cache").expect("parent cache update");
    fs::write(root.join("sub/cache.tmp"), b"competing nested parent cache")
        .expect("parent nested cache update");
    control.fail_before_publication_history = true;
    let failure = control
        .agent_merge(json!({"agent":"child","_caller_turn_id":"root-turn"}))
        .await
        .expect_err("history fault must interrupt an otherwise complete publication");
    assert_eq!(failure, "injected failure before compatibility history");
    assert_eq!(
        fs::read(root.join("published.txt")).expect("physical publication committed"),
        b"published"
    );
    assert_eq!(
        fs::read(root.join("scratch.tmp")).expect("ignored file reaches parent worktree"),
        b"publish without tracking"
    );
    assert_eq!(
        fs::read(root.join("sub/cache.tmp")).expect("nested ignored file reaches parent"),
        b"nested child cache"
    );
    assert_eq!(
        <LocalCoreStateStore as acyclic_fs::MultiRootPublicationStore>::list_operations(
            &control.store
        )
        .await
        .expect("retained publication")
        .len(),
        1
    );
    let root_id = WorkspaceRootId::from_bytes(control.state.root_id);
    let root_workspace = control
        .roots
        .get(&root_key(root_id))
        .expect("root workspace")
        .workspace()
        .clone();
    let mut later_transaction = root_workspace
        .begin_transaction(IdempotencyKey::new())
        .await
        .expect("later root transaction");
    later_transaction
        .write_text("/later.txt", "must not enter recovered publication history")
        .await
        .expect("later root update");
    later_transaction.commit().await.expect("later root commit");
    drop(transaction);
    drop(later_transaction);
    drop(child);
    drop(root_workspace);
    drop(control);

    let mut reopened = ControlPlane::open(data)
        .await
        .expect("restart recovers compatibility history");
    let child_route = &reopened.state.routes["child"];
    let child_workspace = reopened
        .workspace(child_route)
        .await
        .expect("recovered child workspace");
    let child_head = child_workspace.head().await.expect("recovered child head");
    assert_eq!(
        child_route.roots[&root_key(WorkspaceRootId::from_bytes(reopened.state.root_id))]
            .published_generation,
        *child_head.id().digest().as_bytes(),
        "recovery must persist child finalization before removing the publication journal"
    );
    assert_eq!(
        child_head
            .read("/later.txt", 1024)
            .await
            .expect("child rebased onto latest parent"),
        b"must not enter recovered publication history"[..]
    );
    let repository = GitCompatRepository::new(
        acyclic_fs::WorkspaceId::from_bytes(root_repository_workspace_id(&reopened)),
        reopened.store.clone(),
    );
    let root_workspace = reopened
        .roots
        .get(&root_key(root_id))
        .expect("reopened root workspace")
        .workspace()
        .clone();
    let output = repository
        .execute(
            GitCommand::Show { object: None },
            GitTreeRef::exact(
                root_workspace.id(),
                root_workspace.head().await.expect("root head").id(),
            ),
        )
        .await
        .expect("root compatibility head");
    let GitCommandOutput::Commits(commits) = output else {
        panic!("show must return the recovered publication commit");
    };
    let commit = commits.first().expect("publication commit");
    let published_workspace = reopened
        .distributed
        .workspace(commit.tree.workspace_id())
        .await
        .expect("published history workspace");
    let published_generation = published_workspace
        .generation(commit.tree.authored_generation())
        .await
        .expect("published history generation");
    assert_eq!(
        published_generation
            .read("/published.txt", 1024)
            .await
            .expect("published history file"),
        b"published"[..]
    );
    assert!(
        matches!(
            published_generation.read("/scratch.tmp", 1024).await,
            Err(WorkspaceError::NotFound)
        ),
        "newly ignored file must remain outside compatibility history"
    );
    assert!(
        matches!(
            published_generation.read("/sub/cache.tmp", 1024).await,
            Err(WorkspaceError::NotFound)
        ),
        "nested newly ignored file must remain outside compatibility history"
    );
    assert!(
        matches!(
            published_generation.read("/later.txt", 1024).await,
            Err(WorkspaceError::NotFound)
        ),
        "recovered compatibility history must remain pinned to the published generation"
    );
    assert!(
        <LocalCoreStateStore as acyclic_fs::MultiRootPublicationStore>::list_operations(
            &reopened.store
        )
        .await
        .expect("publication cleanup")
        .is_empty()
    );
    let diff: GitCommandOutput = serde_json::from_value(
        reopened
            .root_git_tool(root_id, vec!["diff".to_owned()])
            .await
            .expect("diff after ignored-file publication"),
    )
    .expect("typed diff result");
    let GitCommandOutput::Filesystem(GitFilesystemResult::Data { kind, value }) = diff else {
        panic!("root diff must return filesystem data");
    };
    assert_eq!(kind, "diff");
    assert_eq!(
        value["bindingChanges"], 1,
        "only the later uncommitted file, not the ignored scratch file, belongs in diff: {value}"
    );
    drop(child_workspace);
    drop(child_head);
    drop(root_workspace);
    drop(published_workspace);
    drop(published_generation);
    reopened.shutdown().await.expect("shutdown");
}

async fn root_git_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    fs::create_dir_all(&root).expect("root directory");
    fs::write(root.join("base.txt"), b"base").expect("baseline file");
    fs::write(
        root.join("change.patch"),
        b"diff --git a/created.txt b/created.txt\nnew file mode 100644\n--- /dev/null\n+++ b/created.txt\n@@ -0,0 +1 @@\n+created by root git\n",
    )
    .expect("patch file");
    let mut control = ControlPlane::open(temporary.path().join("plugin-data"))
        .await
        .expect("control plane");
    control
        .session_start(json!({"session_id":"session","cwd":root.display().to_string()}))
        .await
        .expect("root session");
    assert_eq!(
        fs::read(root.join("base.txt")).expect("base after attach"),
        b"base"
    );
    let status = control
        .root_git_tool(
            WorkspaceRootId::from_bytes(control.state.root_id),
            vec!["status".to_owned()],
        )
        .await
        .expect("root status");
    assert!(!status.is_null());
    control
        .root_git_tool(
            WorkspaceRootId::from_bytes(control.state.root_id),
            vec!["diff".to_owned()],
        )
        .await
        .expect("fresh lazy root diff");
    control
        .root_git_tool(
            WorkspaceRootId::from_bytes(control.state.root_id),
            vec!["grep".to_owned(), "base".to_owned(), "base.txt".to_owned()],
        )
        .await
        .expect("file-targeted lazy root grep");
    let repository_id = acyclic_fs::WorkspaceId::from_bytes(root_repository_workspace_id(&control));
    control
        .root_git_tool(
            WorkspaceRootId::from_bytes(control.state.root_id),
            vec!["commit".to_owned(), "-m".to_owned(), "initial".to_owned()],
        )
        .await
        .expect("root commit");
    fs::write(root.join("base.txt"), b"externally changed").expect("external root modification");
    let mut external_status = Value::Null;
    for _ in 0..80 {
        external_status = control
            .root_git_tool(
                WorkspaceRootId::from_bytes(control.state.root_id),
                vec!["status".to_owned()],
            )
            .await
            .expect("status after external root modification");
        if external_status.to_string().contains("\"dirty\":\"dirty\"") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        external_status.to_string().contains("\"dirty\":\"dirty\""),
        "external root modification must be visible to Git: {external_status}"
    );
    control
        .root_git_tool(
            WorkspaceRootId::from_bytes(control.state.root_id),
            vec!["archive".to_owned(), "HEAD".to_owned()],
        )
        .await
        .expect("archive lazy compatibility commit");
    control
        .root_git_tool(
            WorkspaceRootId::from_bytes(control.state.root_id),
            vec!["blame".to_owned(), "base.txt".to_owned()],
        )
        .await
        .expect("blame lazy compatibility commit");
    control
        .root_git_tool(
            WorkspaceRootId::from_bytes(control.state.root_id),
            vec!["switch".to_owned(), "-c".to_owned(), "feature".to_owned()],
        )
        .await
        .expect("root branch switch");
    let state =
        <LocalCoreStateStore as acyclic_fs::GitCompatStore>::load(&control.store, repository_id)
            .await
            .expect("load stable root repository")
            .expect("root repository state");
    assert_eq!(state.current_branch, "feature");
    assert!(state.branches.contains_key("main"));
    control
        .root_git_tool(
            WorkspaceRootId::from_bytes(control.state.root_id),
            vec!["apply".to_owned(), "change.patch".to_owned()],
        )
        .await
        .expect("root patch application");
    assert_eq!(
        fs::read_to_string(root.join("created.txt")).expect("materialized root file"),
        "created by root git\n"
    );
    control.shutdown().await.expect("control shutdown");
}

async fn child_local_git_merge_control_case() {
    async fn try_git(
        control: &mut ControlPlane,
        cwd: &Path,
        argv: &[&str],
    ) -> Result<Value, String> {
        dispatch_session_request(
            control,
            ControlRequest {
                version: 1,
                command: ControlCommand::Git,
                cwd: cwd.to_path_buf(),
                argv: argv.iter().map(|argument| (*argument).to_owned()).collect(),
                name: String::new(),
                arguments: Value::Null,
            },
        )
        .await
    }

    async fn git(control: &mut ControlPlane, cwd: &Path, argv: &[&str]) -> Value {
        try_git(control, cwd, argv)
            .await
            .unwrap_or_else(|error| panic!("acyclic git {} failed: {error}", argv.join(" ")))
    }

    async fn spawn_child(control: &mut ControlPlane, root: &Path, agent: &str) {
        control
            .pre_tool(json!({
                "session_id":"session",
                "turn_id":"root-turn",
                "tool_use_id":format!("spawn-{agent}"),
                "tool_name":"Agent",
                "tool_input":{}
            }))
            .await
            .expect("spawn handshake");
        control
            .subagent_start(json!({
                "session_id":"session",
                "cwd":root,
                "turn_id":format!("{agent}-turn"),
                "agent_id":agent,
                "agent_type":"sdk"
            }))
            .await
            .expect("child start");
    }

    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    fs::create_dir_all(&root).expect("root directory");
    fs::write(root.join("README.md"), b"base\n").expect("base file");
    assert!(
        std::process::Command::new("git")
            .args(["init", "-q"])
            .current_dir(&root)
            .status()
            .expect("git init")
            .success()
    );
    let mut control = ControlPlane::open(temporary.path().join("plugin-data"))
        .await
        .expect("control plane");
    control
        .session_start(json!({"session_id":"session","cwd":root.display().to_string()}))
        .await
        .expect("root session");
    control
        .user_prompt(json!({"session_id":"session","turn_id":"root-turn"}))
        .expect("root turn");
    let root_id = WorkspaceRootId::from_bytes(control.state.root_id);
    control
        .root_git_tool(
            root_id,
            vec!["commit".to_owned(), "-m".to_owned(), "base".to_owned()],
        )
        .await
        .expect("root base commit");

    spawn_child(&mut control, &root, "continued").await;
    let continued = route_path(&control.state.routes["continued"]);
    git(&mut control, &continued, &["switch", "-c", "feature"]).await;
    let continued = route_path(&control.state.routes["continued"]);
    fs::write(continued.join("README.md"), b"feature\n").expect("feature edit");
    git(&mut control, &continued, &["commit", "-m", "feature"]).await;
    assert_eq!(
        fs::read(continued.join("README.md")).expect("feature file"),
        b"feature\n"
    );
    git(&mut control, &continued, &["switch", "main"]).await;
    let continued = route_path(&control.state.routes["continued"]);
    assert_eq!(
        fs::read(continued.join("README.md")).expect("main base file"),
        b"base\n"
    );
    fs::write(continued.join("README.md"), b"main\n").expect("main edit");
    git(&mut control, &continued, &["commit", "-m", "main"]).await;
    assert_eq!(
        fs::read(continued.join("README.md")).expect("main file"),
        b"main\n"
    );
    let continued = route_path(&control.state.routes["continued"]);
    let merged = git(&mut control, &continued, &["merge", "feature"]).await;
    assert!(merged.get("Committed").is_some(), "{merged}");
    for option in ["--continue", "--abort"] {
        let error = try_git(&mut control, &continued, &["merge", option])
            .await
            .expect_err("child Git facade must reject a transition with no pending merge");
        assert!(
            error.contains("requires a pending merge"),
            "child-local merge control was routed to the publication coordinator: {error}"
        );
    }
    control.shutdown().await.expect("shutdown");
}

#[cfg(any(windows, all(unix, not(target_os = "linux"))))]
async fn endpoint_shutdown_case() {
    use tokio::io::AsyncWriteExt as _;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    let root = temporary.path().join("root");
    fs::create_dir_all(&root).expect("root directory");
    let mut control = ControlPlane::open(data.clone())
        .await
        .expect("control plane");
    control
        .session_start(json!({"session_id":"session","cwd":root.display().to_string()}))
        .await
        .expect("root session");
    let shared = Arc::new(AsyncMutex::new(control));
    let endpoint = start_control_endpoint(Arc::clone(&shared), &data)
        .await
        .expect("control endpoint");
    #[cfg(unix)]
    assert!(
        endpoint.socket_path.as_os_str().len() < 100,
        "the control socket must fit conservative Unix-domain path limits"
    );
    #[cfg(unix)]
    let mut client = tokio::net::UnixStream::connect(&endpoint.socket_path)
        .await
        .expect("connect control endpoint");
    #[cfg(windows)]
    let mut client = connect_test_pipe(&endpoint.pipe_path).await;
    endpoint.accepted.notified().await;
    client
        .write_all(b"{\"version\":1")
        .await
        .expect("write incomplete request");
    endpoint.shutdown().await.expect("endpoint shutdown");
    drop(client);

    let control = match Arc::try_unwrap(shared) {
        Ok(control) => control.into_inner(),
        Err(_) => panic!("endpoint retained an in-flight control request"),
    };
    control.shutdown().await.expect("control shutdown");
    let reopened = ControlPlane::open(data)
        .await
        .expect("reopen after in-flight request shutdown");
    reopened.shutdown().await.expect("reopened shutdown");
}

#[cfg(any(unix, windows))]
async fn concurrent_hook_endpoint_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    let root = temporary.path().join("root");
    fs::create_dir_all(&root).expect("root directory");
    let service = Arc::new(AsyncMutex::new(
        ServiceControl::open(data.clone()).await.expect("service"),
    ));
    let endpoint = start_control_endpoint(Arc::clone(&service), &data)
        .await
        .expect("control endpoint");
    let hook = |event: &str, cwd: &Path, arguments: Value| ControlRequest {
        version: 1,
        command: ControlCommand::Hook,
        cwd: cwd.to_path_buf(),
        argv: Vec::new(),
        name: format!("claude-code:{event}"),
        arguments,
    };
    send_control_request(
        &data,
        &hook(
            "SessionStart",
            &root,
            json!({"session_id":"session","cwd":root}),
        ),
    )
    .await
    .expect("session start");
    let first_spawn = send_control_request(
        &data,
        &hook(
            "PreToolUse",
            &root,
            json!({
                "session_id":"session","cwd":root,"tool_name":"Agent",
                "tool_use_id":"spawn-one","tool_input":{}
            }),
        ),
    )
    .await;
    if first_spawn.is_err() {
        endpoint.shutdown().await.expect("endpoint shutdown");
        let service = Arc::try_unwrap(service)
            .unwrap_or_else(|_| panic!("endpoint retained service"))
            .into_inner();
        assert!(service.sessions["session"].state.pending.is_empty());
        service.shutdown().await.expect("service shutdown");
        return;
    }
    send_control_request(
        &data,
        &hook(
            "SubagentStart",
            &root,
            json!({"session_id":"session","cwd":root,"agent_id":"child"}),
        ),
    )
    .await
    .expect("child start");
    let child = route_path(&service.lock().await.sessions["session"].state.routes["child"]);
    let spawn = hook(
        "PreToolUse",
        &root,
        json!({
            "session_id":"session","cwd":root,"tool_name":"Agent",
            "tool_use_id":"spawn-two","tool_input":{}
        }),
    );
    let child_tool = hook(
        "PreToolUse",
        &child,
        json!({
            "session_id":"session","cwd":child,"tool_name":"Bash",
            "tool_use_id":"child-tool","tool_input":{"command":"pwd"}
        }),
    );
    let requests = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            send_control_request(&data, &spawn),
            send_control_request(&data, &child_tool)
        )
    })
    .await
    .expect("concurrent hook deadline");
    requests.0.expect("second spawn response");
    requests.1.expect("child tool response");
    send_control_request(
        &data,
        &hook(
            "PostToolUse",
            &child,
            json!({
                "session_id":"session","cwd":child,"tool_name":"Bash",
                "tool_use_id":"child-tool","tool_input":{"command":"pwd"}
            }),
        ),
    )
    .await
    .expect("child tool close");
    endpoint.shutdown().await.expect("endpoint shutdown");
    let service = Arc::try_unwrap(service)
        .unwrap_or_else(|_| panic!("endpoint retained service"))
        .into_inner();
    service.shutdown().await.expect("service shutdown");
}

#[cfg(any(target_os = "linux", windows))]
async fn rustc_child_mount_case() {
    #[cfg(target_os = "linux")]
    let source = tempfile::tempdir_in("/dev/shm").expect("source workspace");
    #[cfg(windows)]
    let source = tempfile::tempdir().expect("source workspace");
    std::fs::write(
        source.path().join("acyclic-workflow.rs"),
        include_bytes!("../tests/fixtures/overlay_workflow.rs"),
    )
    .expect("Rust source");
    #[cfg(target_os = "linux")]
    let state = tempfile::tempdir_in("/dev/shm").expect("temporary state");
    #[cfg(windows)]
    let state = tempfile::tempdir().expect("temporary state");
    let mut service = ServiceControl::open(state.path().join("state"))
        .await
        .expect("service");
    service
        .dispatch_native_hook(
            "claude-code",
            "SessionStart",
            json!({"session_id":"rustc","cwd":source.path()}),
            source.path(),
        )
        .await
        .expect("session start");
    service
        .dispatch_native_hook(
            "claude-code",
            "PreToolUse",
            json!({
                "session_id":"rustc","cwd":source.path(),"tool_name":"Agent",
                "tool_use_id":"spawn","tool_input":{"description":"compile"}
            }),
            source.path(),
        )
        .await
        .expect("spawn handshake");
    service
        .dispatch_native_hook(
            "claude-code",
            "SubagentStart",
            json!({
                "session_id":"rustc","cwd":source.path(),"agent_id":"compiler",
                "agent_type":"general-purpose"
            }),
            source.path(),
        )
        .await
        .expect("child start");
    let child = route_path(&service.sessions["rustc"].state.routes["compiler"]);
    service
        .dispatch_native_hook(
            "claude-code",
            "PreToolUse",
            json!({
                "session_id":"rustc","cwd":child,"agent_id":"compiler",
                "tool_name":"Bash","tool_use_id":"rustc",
                "tool_input":{"command":format!("rustc --edition 2021 acyclic-workflow.rs -o {}", if cfg!(windows) { "main.exe" } else { "main" })}
            }),
            &child,
        )
        .await
        .expect("tool lease");
    let started = std::time::Instant::now();
    let mut compiler = std::process::Command::new("rustc")
        .current_dir(&child)
        .args([
            "--edition",
            "2021",
            "acyclic-workflow.rs",
            "-o",
            if cfg!(windows) { "main.exe" } else { "main" },
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("start rustc");
    let deadline = started + std::time::Duration::from_secs(10);
    let timed_out = loop {
        if compiler.try_wait().expect("poll rustc").is_some() {
            break false;
        }
        if std::time::Instant::now() >= deadline {
            compiler.kill().expect("kill timed out rustc");
            break true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    let output = compiler.wait_with_output().expect("collect rustc output");
    let executable = child.join(if cfg!(windows) { "main.exe" } else { "main" });
    #[cfg(unix)]
    let executable_mode = std::fs::metadata(&executable)
        .map(|metadata| std::os::unix::fs::MetadataExt::mode(&metadata));
    #[cfg(windows)]
    let executable_mode = std::fs::metadata(&executable).map(|metadata| metadata.len());
    service
        .dispatch_native_hook(
            "claude-code",
            "PostToolUse",
            json!({
                "session_id":"rustc","cwd":child,"agent_id":"compiler",
                "tool_name":"Bash","tool_use_id":"rustc"
            }),
            &child,
        )
        .await
        .expect("close tool lease");
    let mut workflow = std::process::Command::new(&executable)
        .current_dir(&child)
        .arg("workflow-output.txt")
        .env("ACYCLIC_WORKFLOW_TOKEN", "qualified")
        .spawn()
        .expect("start mounted workflow");
    let workflow_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let execution_after_sync = loop {
        if let Some(status) = workflow.try_wait().expect("poll mounted workflow") {
            break Some(status);
        }
        if std::time::Instant::now() >= workflow_deadline {
            workflow.kill().expect("kill timed-out mounted workflow");
            break None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    assert_eq!(
        std::fs::read(child.join("workflow-output.txt")).expect("workflow output"),
        b"isolated"
    );
    service
        .dispatch_native_hook(
            "claude-code",
            "SubagentStop",
            json!({"session_id":"rustc","cwd":child,"agent_id":"compiler"}),
            &child,
        )
        .await
        .expect("child stop");
    service
        .dispatch_native_hook(
            "claude-code",
            "SessionEnd",
            json!({"session_id":"rustc","cwd":source.path()}),
            source.path(),
        )
        .await
        .expect("session end");
    service.shutdown().await.expect("service shutdown");
    assert!(!timed_out, "rustc exceeded its 10 second deadline");
    assert!(
        output.status.success(),
        "rustc failed after {:?}\nstdout:\n{}\nstderr:\n{}",
        started.elapsed(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        execution_after_sync
            .as_ref()
            .is_some_and(std::process::ExitStatus::success),
        "compiled binary did not execute after tool synchronization: mode={executable_mode:?}, result={execution_after_sync:?}"
    );
    assert!(
        !source
            .path()
            .join(if cfg!(windows) { "main.exe" } else { "main" })
            .exists()
    );
}

#[cfg(target_os = "linux")]
async fn real_lean_child_mount_case() {
    let root = PathBuf::from(
        env::var_os("ACYCLIC_E2E_LEAN_WORKSPACE")
            .expect("set ACYCLIC_E2E_LEAN_WORKSPACE to a real Lean project"),
    )
    .canonicalize()
    .expect("Lean project path");
    assert!(root.join("lakefile.toml").exists() || root.join("lakefile.lean").exists());
    // Resolve the real project environment before mounting so this bounded gate measures
    // Lean compiler I/O, not Lake's whole dependency-graph freshness scan.
    let lean_environment = std::process::Command::new("lake")
        .args(["env", "printenv", "LEAN_PATH"])
        .current_dir(&root)
        .output()
        .expect("resolve Lean project environment");
    assert!(
        lean_environment.status.success(),
        "lake env failed: {}",
        String::from_utf8_lossy(&lean_environment.stderr)
    );
    let lean_path = String::from_utf8(lean_environment.stdout)
        .expect("UTF-8 LEAN_PATH")
        .trim()
        .to_owned();
    let temporary = tempfile::tempdir_in("/dev/shm").expect("temporary state");
    let mut service = ServiceControl::open(temporary.path().join("state"))
        .await
        .expect("service");
    service
        .dispatch_native_hook(
            "claude-code",
            "SessionStart",
            json!({"session_id":"lean","cwd":root}),
            &root,
        )
        .await
        .expect("session start");
    service
        .dispatch_native_hook(
            "claude-code",
            "PreToolUse",
            json!({
                "session_id":"lean","cwd":root,"tool_name":"Agent",
                "tool_use_id":"spawn","tool_input":{"description":"compile"}
            }),
            &root,
        )
        .await
        .expect("spawn handshake");
    service
        .dispatch_native_hook(
            "claude-code",
            "SubagentStart",
            json!({
                "session_id":"lean","cwd":root,"agent_id":"compiler",
                "agent_type":"general-purpose"
            }),
            &root,
        )
        .await
        .expect("child start");
    let child = route_path(&service.sessions["lean"].state.routes["compiler"]);
    service
        .dispatch_native_hook(
            "claude-code",
            "PreToolUse",
            json!({
                "session_id":"lean","cwd":child,"agent_id":"compiler",
                "tool_name":"Bash","tool_use_id":"lean-compile",
                "tool_input":{"command":"lean YcDemo/Lemmas.lean -o .acyclic-lean-qualification.olean"}
            }),
            &child,
        )
        .await
        .expect("tool lease");
    let started = std::time::Instant::now();
    let mut compilation = std::process::Command::new("lean")
        .args([
            "YcDemo/Lemmas.lean",
            "-o",
            ".acyclic-lean-qualification.olean",
        ])
        .current_dir(&child)
        .env("LEAN_PATH", lean_path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("start Lean compilation");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let timed_out = loop {
        if compilation
            .try_wait()
            .expect("poll Lean compilation")
            .is_some()
        {
            break false;
        }
        if std::time::Instant::now() >= deadline {
            compilation.kill().expect("kill timed out Lean compilation");
            break true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    let output = compilation
        .wait_with_output()
        .expect("collect Lean compilation output");
    let child_output_exists = child.join(".acyclic-lean-qualification.olean").is_file();
    service
        .dispatch_native_hook(
            "claude-code",
            "PostToolUse",
            json!({
                "session_id":"lean","cwd":child,"agent_id":"compiler",
                "tool_name":"Bash","tool_use_id":"lean-compile"
            }),
            &child,
        )
        .await
        .expect("close tool lease");
    service
        .dispatch_native_hook(
            "claude-code",
            "SubagentStop",
            json!({"session_id":"lean","cwd":child,"agent_id":"compiler"}),
            &child,
        )
        .await
        .expect("child stop");
    service
        .dispatch_native_hook(
            "claude-code",
            "SessionEnd",
            json!({"session_id":"lean","cwd":root}),
            &root,
        )
        .await
        .expect("session end");
    service.shutdown().await.expect("service shutdown");
    assert!(
        !timed_out,
        "Lean compilation exceeded the 30 second mount budget\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.status.success(),
        "Lean compilation failed after {:?}:\n{}",
        started.elapsed(),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(child_output_exists);
    assert!(!root.join(".acyclic-lean-qualification.olean").exists());
}

struct ControlledDispatcher {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    completed: Arc<tokio::sync::Notify>,
}

struct ReplayCountingDispatcher {
    executions: std::sync::atomic::AtomicUsize,
}

impl ConcurrentControlRequestDispatcher for ReplayCountingDispatcher {
    async fn dispatch_request(&self, _request: ControlRequest) -> Result<Value, String> {
        let execution = self
            .executions
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        Ok(json!({"execution": execution}))
    }
}

impl ControlRequestDispatcher for ControlledDispatcher {
    async fn dispatch_request(&mut self, _request: ControlRequest) -> Result<Value, String> {
        self.started.notify_one();
        self.release.notified().await;
        self.completed.notify_one();
        Ok(json!({}))
    }
}

#[cfg(any(windows, all(unix, not(target_os = "linux"))))]
async fn stream_deadline_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let completed = Arc::new(tokio::sync::Notify::new());
    let control = Arc::new(AsyncMutex::new(ControlledDispatcher {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        completed: Arc::clone(&completed),
    }));
    let endpoint = start_control_endpoint(Arc::clone(&control), &data)
        .await
        .expect("control endpoint");
    let request = ControlRequest {
        version: 1,
        command: ControlCommand::Ping,
        cwd: temporary.path().to_path_buf(),
        argv: Vec::new(),
        name: String::new(),
        arguments: Value::Null,
    };
    #[cfg(unix)]
    let client = tokio::net::UnixStream::connect(&endpoint.socket_path)
        .await
        .expect("connect control endpoint");
    #[cfg(windows)]
    let client = connect_test_pipe(&endpoint.pipe_path).await;
    endpoint.accepted.notified().await;
    let envelope = ControlEnvelope::new(request);
    let request_id = envelope.request_id.clone();
    let mut request = serde_json::to_vec(&envelope).expect("encode request");
    request.push(b'\n');
    let began = std::time::Instant::now();
    let exchange = tokio::spawn(async move {
        exchange_control_stream(
            client,
            &request,
            &request_id,
            std::time::Duration::from_millis(25),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), started.notified())
        .await
        .expect("stalled dispatcher never received the request");
    let error = exchange
        .await
        .expect("exchange task")
        .expect_err("stalled control response must time out");
    assert!(matches!(error, ControlRequestError::Indeterminate(_)));
    assert!(
        began.elapsed() < std::time::Duration::from_secs(1),
        "control stream ignored its request deadline"
    );
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(1), completed.notified())
        .await
        .expect("timed-out request was cancelled before completion");
    tokio::time::timeout(std::time::Duration::from_secs(1), endpoint.shutdown())
        .await
        .expect("endpoint shutdown deadline")
        .expect("endpoint shutdown");
    assert!(
        Arc::try_unwrap(control).is_ok(),
        "timed-out stream retained dispatch state"
    );
}

#[cfg(any(windows, all(unix, not(target_os = "linux"))))]
async fn endpoint_disconnect_case() {
    use tokio::io::AsyncWriteExt as _;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let completed = Arc::new(tokio::sync::Notify::new());
    let control = Arc::new(AsyncMutex::new(ControlledDispatcher {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        completed: Arc::clone(&completed),
    }));
    let endpoint = start_control_endpoint(Arc::clone(&control), &data)
        .await
        .expect("control endpoint");
    #[cfg(unix)]
    let mut client = tokio::net::UnixStream::connect(&endpoint.socket_path)
        .await
        .expect("connect control endpoint");
    #[cfg(windows)]
    let mut client = connect_test_pipe(&endpoint.pipe_path).await;
    endpoint.accepted.notified().await;
    let envelope = ControlEnvelope::new(ControlRequest {
        version: 1,
        command: ControlCommand::Ping,
        cwd: temporary.path().to_path_buf(),
        argv: Vec::new(),
        name: String::new(),
        arguments: Value::Null,
    });
    let request = serde_json::to_vec(&envelope).expect("encode request");
    client.write_all(&request).await.expect("write request");
    client.write_all(b"\n").await.expect("finish request");
    started.notified().await;
    drop(client);

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), control.lock())
            .await
            .is_err(),
        "disconnect cancelled the accepted request"
    );
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(1), completed.notified())
        .await
        .expect("disconnected request did not finish");
    endpoint.shutdown().await.expect("endpoint shutdown");
    assert!(
        Arc::try_unwrap(control).is_ok(),
        "endpoint retained disconnected dispatch state"
    );
}

#[tokio::test]
async fn durable_hook_response_is_replayed_without_reexecution_after_restart() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    fs::create_dir(&data).expect("plugin data directory");
    let dispatcher = Arc::new(ReplayCountingDispatcher {
        executions: std::sync::atomic::AtomicUsize::new(0),
    });
    let envelope = ControlEnvelope::new(ControlRequest {
        version: 1,
        command: ControlCommand::Hook,
        cwd: temporary.path().to_path_buf(),
        argv: Vec::new(),
        name: "codex:PreToolUse".to_owned(),
        arguments: json!({"session_id":"session","tool_use_id":"tool"}),
    });

    let first_ledger = Arc::new(ControlLedger::open(&data).expect("first ledger"));
    let first = dispatch_control_envelope(&dispatcher, &first_ledger, envelope.clone()).await;
    drop(first_ledger);
    let reopened = Arc::new(ControlLedger::open(&data).expect("reopened ledger"));
    let replay = dispatch_control_envelope(&dispatcher, &reopened, envelope).await;

    let executed: Value = serde_json::from_slice(&first).expect("first response");
    assert_eq!(executed["ok"], true, "first hook must execute: {executed}");
    assert_eq!(
        first, replay,
        "a retry must receive the exact bytes first sent"
    );
    assert_eq!(
        dispatcher
            .executions
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "a completed hook must never execute twice"
    );
}

#[tokio::test]
async fn unledgered_control_commands_still_require_exact_protocol_negotiation() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    fs::create_dir(&data).expect("plugin data directory");
    let dispatcher = Arc::new(ReplayCountingDispatcher {
        executions: std::sync::atomic::AtomicUsize::new(0),
    });
    let ledger = Arc::new(ControlLedger::open(&data).expect("ledger"));

    for command in [ControlCommand::Ping, ControlCommand::Agents] {
        let mut envelope = ControlEnvelope::new(ControlRequest {
            version: 1,
            command,
            cwd: temporary.path().to_path_buf(),
            argv: Vec::new(),
            name: String::new(),
            arguments: Value::Null,
        });
        envelope.protocol.major = envelope.protocol.major.saturating_add(1);
        let response: Value = serde_json::from_slice(
            &dispatch_control_envelope(&dispatcher, &ledger, envelope).await,
        )
        .expect("response");
        assert_eq!(response["ok"], false, "mismatched protocol was accepted");
    }
    assert_eq!(
        dispatcher
            .executions
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "invalid unledgered envelopes must not reach the dispatcher"
    );
}

#[tokio::test]
async fn no_request_runs_on_top_of_an_unflushed_transition() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    let root = temporary.path().join("root");
    fs::create_dir(&root).expect("root directory");
    let mut control = ControlPlane::open(data.clone())
        .await
        .expect("control plane");
    control
        .session_start(json!({"session_id":"session","cwd":root}))
        .await
        .expect("session start");
    let prompt = |turn: &str| ControlRequest {
        version: 1,
        command: ControlCommand::Hook,
        cwd: root.clone(),
        argv: Vec::new(),
        name: "codex:UserPromptSubmit".to_owned(),
        arguments: json!({"session_id":"session","cwd":root,"turn_id":turn}),
    };
    dispatch_session_request(&mut control, prompt("first"))
        .await
        .expect("first prompt");
    assert!(
        control.unflushed,
        "a remembered root turn is saved unflushed"
    );

    // Until the transition is durable, no later request may act on it.
    control.fail_next_flush = true;
    let refused = dispatch_session_request(&mut control, prompt("second"))
        .await
        .expect_err("a request must wait for the previous transition to be durable");
    assert!(refused.contains("flush"), "{refused}");
    assert!(!control.state.root_turns.contains("second"));
    assert!(control.unflushed);

    dispatch_session_request(&mut control, prompt("second"))
        .await
        .expect("second prompt once the first is durable");
    assert!(control.state.root_turns.contains("second"));
    control
        .shutdown()
        .await
        .expect("shutdown flushes the last transition");
    let reopened = load_saved_state(&data).expect("durable state");
    assert!(reopened.root_turns.contains("second"));
}

#[tokio::test]
async fn duplicate_spawn_hook_does_not_allocate_a_second_workspace_handshake() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    let root = temporary.path().join("root");
    fs::create_dir(&root).expect("root directory");
    let mut control = ControlPlane::open(data).await.expect("control plane");
    control
        .session_start(json!({"session_id":"session","cwd":root}))
        .await
        .expect("session start");
    control
        .user_prompt(json!({"session_id":"session","turn_id":"turn"}))
        .expect("root turn");
    let hook = json!({
        "session_id":"session",
        "turn_id":"turn",
        "tool_use_id":"spawn",
        "tool_name":"spawn_agent",
        "tool_input":{}
    });
    if control.pre_tool(hook.clone()).await.is_err() {
        assert!(control.state.pending.is_empty());
        assert!(control.pending_mounts.is_empty());
        control.shutdown().await.expect("shutdown after rejection");
        return;
    }
    let first = control
        .state
        .pending
        .front()
        .expect("pending spawn")
        .clone();
    control.pre_tool(hook).await.expect("duplicate spawn");
    assert_eq!(control.state.pending.len(), 1);
    assert_eq!(
        control
            .state
            .pending
            .front()
            .expect("pending spawn")
            .fork_key,
        first.fork_key
    );
    control.shutdown().await.expect("shutdown");
}

#[test]
fn spawn_preparation_is_atomic_and_restartable() {
    run_large_stack("spawn-preparation", spawn_preparation_case);
}

#[test]
fn direct_control_shutdown_waits_for_detached_root_owner() {
    run_large_stack("control-root-release", || async {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let control = ControlPlane::open(data.clone())
            .await
            .expect("control plane");
        let detached = control.fs.clone();
        let shutdown = tokio::spawn(control.shutdown());
        tokio::pin!(shutdown);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut shutdown)
                .await
                .is_err(),
            "shutdown acknowledged while a detached filesystem owner was still live"
        );
        drop(detached);
        tokio::time::timeout(std::time::Duration::from_secs(5), shutdown)
            .await
            .expect("root release deadline")
            .expect("shutdown task")
            .expect("shutdown");
        let reopened = ControlPlane::open(data)
            .await
            .expect("reopen after release");
        reopened.shutdown().await.expect("reopened shutdown");
    });
}

#[test]
fn shared_control_shutdown_leaves_root_release_to_service() {
    run_large_stack("shared-control-root-release", || async {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let data = temporary.path().join("plugin-data");
        let fs = LocalFs::local(LocalOptions::new(data.join("filesystem")))
            .await
            .expect("shared filesystem");
        let release = fs.local_root_release_barrier().expect("release barrier");
        let control = ControlPlane::open_with(
            data.clone(),
            data.clone(),
            fs.clone(),
            LocalCoreStateStore::open_owned(data.join("core-state")).expect("state owner"),
            SharedRootRegistry::default(),
        )
        .await
        .expect("shared control");
        tokio::time::timeout(std::time::Duration::from_secs(5), control.shutdown())
            .await
            .expect("session shutdown deadline")
            .expect("session shutdown");
        assert!(
            !release.is_released(),
            "session shutdown released the service-owned filesystem"
        );
        drop(fs);
        wait_for_root_release(Some(release))
            .await
            .expect("service root release");
    });
}

async fn spawn_preparation_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    let root = temporary.path().join("root");
    fs::create_dir(&root).expect("root directory");
    let mut control = ControlPlane::open(data.clone())
        .await
        .expect("control plane");
    control
        .session_start(json!({"session_id":"session","cwd":root}))
        .await
        .expect("session start");
    control
        .user_prompt(json!({"session_id":"session","turn_id":"turn"}))
        .expect("root turn");
    let incomplete_key = [9; 16];
    control.state.pending.push_back(PendingSpawn {
        parent_agent_id: control.state.root_agent_id.clone(),
        tool_use_id: "incomplete".to_owned(),
        active_root_id: None,
        expires_at_millis: now_millis() + 60_000,
        workspace_name: "incomplete".to_owned(),
        fork_key: incomplete_key,
        roots: BTreeMap::new(),
        mount_path: control
            .workspace_mount_root()
            .join(compact_id(&incomplete_key)),
        lifecycle: PendingSpawnLifecycle::Discarding,
    });
    assert!(
        control
            .pre_tool(json!({
                "session_id":"session",
                "turn_id":"turn",
                "tool_use_id":"incomplete",
                "tool_name":"spawn_agent",
                "tool_input":{}
            }))
            .await
            .is_err(),
        "an incomplete retry must remain denied"
    );
    control.state.pending.clear();
    let spawn = control
        .pre_tool(json!({
            "session_id":"session",
            "turn_id":"turn",
            "tool_use_id":"spawn",
            "tool_name":"spawn_agent",
            "tool_input":{}
        }))
        .await;
    if spawn.is_err() {
        assert!(
            control.state.pending.is_empty(),
            "a rejected spawn retained durable preparation state"
        );
        assert!(
            control.pending_mounts.is_empty(),
            "a rejected spawn retained a native mount"
        );
        control.shutdown().await.expect("shutdown after rejection");
        return;
    }
    let prepared = control
        .state
        .pending
        .front()
        .expect("prepared spawn")
        .clone();
    assert_eq!(prepared.lifecycle, PendingSpawnLifecycle::Prepared);
    assert!(control.pending_mounts.contains_key(&prepared.fork_key));
    control.shutdown().await.expect("first shutdown");

    let mut recovered = ControlPlane::open(data)
        .await
        .expect("recover control plane");
    assert_eq!(
        recovered
            .state
            .pending
            .front()
            .expect("recovered spawn")
            .lifecycle,
        PendingSpawnLifecycle::Prepared
    );
    assert!(recovered.pending_mounts.contains_key(&prepared.fork_key));
    let started = recovered
        .subagent_start(json!({
            "session_id":"session",
            "turn_id":"child-turn",
            "agent_id":"child",
            "agent_type":"explorer"
        }))
        .await
        .expect("bind prepared spawn");
    let active_path = prepared
        .mount_path
        .join(route_name(WorkspaceRootId::from_bytes(
            recovered.state.root_id,
        )));
    assert!(
        started
            .pointer("/hookSpecificOutput/additionalContext")
            .and_then(Value::as_str)
            .is_some_and(|context| context.contains(active_path.to_string_lossy().as_ref()))
    );
    assert!(recovered.state.pending.is_empty());
    assert!(recovered.mounts.contains_key("child"));
    recovered.shutdown().await.expect("recovered shutdown");
}

#[cfg(target_os = "linux")]
async fn mailbox_shutdown_drains_dispatch_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let completed = Arc::new(tokio::sync::Notify::new());
    let control = Arc::new(AsyncMutex::new(ControlledDispatcher {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
        completed: Arc::clone(&completed),
    }));
    let endpoint = start_control_endpoint(Arc::clone(&control), &data)
        .await
        .expect("control endpoint");
    let request = ControlRequest {
        version: 1,
        command: ControlCommand::Ping,
        cwd: temporary.path().to_path_buf(),
        argv: Vec::new(),
        name: String::new(),
        arguments: Value::Null,
    };
    let client_data = data.clone();
    let client =
        tokio::spawn(async move { send_control_request_once(&client_data, &request).await });
    started.notified().await;

    let shutdown = tokio::spawn(endpoint.shutdown());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), control.lock())
            .await
            .is_err(),
        "mailbox shutdown cancelled the accepted request"
    );
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(1), completed.notified())
        .await
        .expect("mailbox request did not finish during shutdown");
    tokio::time::timeout(std::time::Duration::from_secs(1), shutdown)
        .await
        .expect("mailbox shutdown deadline")
        .expect("mailbox shutdown task")
        .expect("mailbox shutdown");
    let _ = client.await;
    assert!(
        Arc::try_unwrap(control).is_ok(),
        "mailbox retained cancelled dispatch state"
    );
}

#[cfg(target_os = "linux")]
async fn mailbox_deadline_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    prepare_linux_control_mailbox(&data).expect("mailbox");
    let envelope = ControlEnvelope::new(ControlRequest {
        version: 1,
        command: ControlCommand::Hook,
        cwd: temporary.path().to_path_buf(),
        argv: Vec::new(),
        name: "claude-code:PreToolUse".to_owned(),
        arguments: Value::Null,
    });
    let request_id = envelope.request_id.clone();
    let request = serde_json::to_vec(&envelope).expect("request");
    let started = std::time::Instant::now();
    let error = send_linux_mailbox_request(
        &data,
        &request,
        &request_id,
        std::time::Duration::from_millis(25),
    )
    .await
    .expect_err("stalled mailbox must time out");
    assert!(matches!(error, ControlRequestError::Indeterminate(_)));
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "mailbox ignored its caller deadline"
    );
    assert!(
        fs::read_dir(linux_control_mailbox_path(&data))
            .expect("mailbox")
            .next()
            .is_none(),
        "a client that gives up must remove its exchange"
    );
}

#[cfg(target_os = "linux")]
async fn mailbox_malformed_exchange_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    let control = Arc::new(ReplayCountingDispatcher {
        executions: std::sync::atomic::AtomicUsize::new(0),
    });
    let endpoint = start_control_endpoint(Arc::clone(&control), &data)
        .await
        .expect("control endpoint");
    let malformed = endpoint.mailbox_path.join("000-malformed");
    fs::create_dir(&malformed).expect("malformed exchange");
    fs::write(malformed.join("request"), b"{}").expect("malformed request");
    fs::create_dir(malformed.join("processing")).expect("conflicting processing directory");

    let response = send_control_request_once(
        &data,
        &ControlRequest {
            version: 1,
            command: ControlCommand::Ping,
            cwd: temporary.path().to_path_buf(),
            argv: Vec::new(),
            name: String::new(),
            arguments: Value::Null,
        },
    )
    .await
    .expect("valid request after malformed exchange");
    assert_eq!(response["execution"], 1);
    assert_eq!(
        control.executions.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    endpoint.shutdown().await.expect("endpoint shutdown");
}

#[cfg(windows)]
async fn sequential_endpoint_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    let control = Arc::new(ReplayCountingDispatcher {
        executions: std::sync::atomic::AtomicUsize::new(0),
    });
    let endpoint = start_control_endpoint(Arc::clone(&control), &data)
        .await
        .expect("control endpoint");
    for sequence in 0..64 {
        let response = send_control_request(
            &data,
            &ControlRequest {
                version: 1,
                command: ControlCommand::Ping,
                cwd: temporary.path().to_path_buf(),
                argv: Vec::new(),
                name: String::new(),
                arguments: Value::Null,
            },
        )
        .await
        .expect("sequential request");
        assert_eq!(
            response["execution"],
            sequence + 1,
            "every accepted pipe must return its own complete response"
        );
    }
    endpoint.shutdown().await.expect("endpoint shutdown");
    assert!(
        Arc::try_unwrap(control).is_ok(),
        "endpoint retained a completed request"
    );
}

#[cfg(windows)]
async fn connect_test_pipe(pipe_path: &str) -> tokio::net::windows::named_pipe::NamedPipeClient {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match tokio::net::windows::named_pipe::ClientOptions::new().open(pipe_path) {
            Ok(client) => return client,
            Err(error) if std::time::Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Err(error) => panic!("connect control pipe: {error}"),
        }
    }
}

async fn lifecycle_hardening_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    let root = temporary.path().join("root");
    fs::create_dir_all(&root).expect("root directory");
    let mut control = ControlPlane::open(data.clone())
        .await
        .expect("control plane");
    control
        .session_start(json!({"session_id":"session","cwd":root.display().to_string()}))
        .await
        .expect("root session");
    control
        .session_start(json!({"session_id":"session","cwd":root.display().to_string()}))
        .await
        .expect("duplicate session start is idempotent");
    control
        .user_prompt(json!({"session_id":"session","turn_id":"root-turn"}))
        .expect("root turn");
    control
        .user_prompt(json!({"session_id":"session","turn_id":"root-turn"}))
        .expect("duplicate root turn is idempotent");
    assert!(control
        .pre_tool(json!({"session_id":"other","turn_id":"root-turn","tool_use_id":"foreign","tool_name":"Read","tool_input":{}}))
        .await
        .is_err());
    assert!(control
        .pre_tool(json!({"session_id":"session","turn_id":"unknown-turn","tool_use_id":"unknown","tool_name":"Read","tool_input":{}}))
        .await
        .is_err());

    control
        .pre_tool(json!({"session_id":"session","turn_id":"root-turn","tool_use_id":"spawn-expiring","tool_name":"spawn_agent","tool_input":{}}))
        .await
        .expect("first serialized spawn");
    control
        .pre_tool(json!({"session_id":"session","turn_id":"root-turn","tool_use_id":"spawn-expiring","tool_name":"spawn_agent","tool_input":{}}))
        .await
        .expect("duplicate spawn hook is idempotent");
    assert_eq!(control.state.pending.len(), 1);
    assert!(control
        .pre_tool(json!({"session_id":"session","turn_id":"root-turn","tool_use_id":"spawn-overlap","tool_name":"spawn_agent","tool_input":{}}))
        .await
        .is_err());
    control
        .state
        .pending
        .front_mut()
        .expect("pending spawn")
        .expires_at_millis = 0;
    assert!(control
        .subagent_start(json!({"session_id":"session","turn_id":"expired-turn","agent_id":"expired","agent_type":"explorer"}))
        .await
        .is_err());
    control
        .pre_tool(json!({"session_id":"session","turn_id":"root-turn","tool_use_id":"spawn-child","tool_name":"spawn_agent","tool_input":{}}))
        .await
        .expect("spawn after expiry");
    control
        .subagent_start(json!({"session_id":"session","turn_id":"child-turn","agent_id":"child","agent_type":"explorer"}))
        .await
        .expect("child start");
    control
        .subagent_start(json!({"session_id":"session","turn_id":"child-turn","agent_id":"child","agent_type":"explorer"}))
        .await
        .expect("duplicate child start is idempotent");
    assert!(control
        .subagent_start(json!({"session_id":"session","turn_id":"root-turn","agent_id":"collision","agent_type":"explorer"}))
        .await
        .is_err());
    assert!(
        control
            .user_prompt(json!({"session_id":"session","turn_id":"child-turn"}))
            .is_err()
    );
    assert!(control
        .subagent_start(json!({"session_id":"other","turn_id":"child-turn","agent_id":"child","agent_type":"explorer"}))
        .await
        .is_err());
    let (caller, _, _) = native_tool_identity(
        &mut control,
        "claude-code",
        &json!({"agent_id":"child"}),
        &root,
    )
    .expect("the authenticated child identity permits cwd redirection from its physical root");
    assert_eq!(caller, "child");
    assert!(control
        .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"unknown-tool","tool_name":"mystery_mutator","tool_input":{}}))
        .await
        .is_err());
    let leases_before_remote = control.state.leases.len();
    control
        .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"remote-message","tool_name":"mcp__codex_app__send_message_to_thread","tool_input":{"threadId":"thread","prompt":"status"}}))
        .await
        .expect("known non-filesystem tool bypasses workspace routing");
    control
        .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"remote-message","tool_name":"mcp__codex_app__send_message_to_thread"}))
        .await
        .expect("known non-filesystem post hook is a no-op");
    assert_eq!(control.state.leases.len(), leases_before_remote);
    let shell = control
        .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"system-status","tool_name":"exec_command","tool_input":{"cmd":"git status","workdir":root.display().to_string()}}))
        .await
        .expect("shell command is redirected to the child mount");
    assert_eq!(
        shell["hookSpecificOutput"]["updatedInput"]["workdir"],
        route_path(&control.state.routes["child"])
            .display()
            .to_string()
    );
    control
        .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"system-status","tool_name":"exec_command"}))
        .await
        .expect("close redirected shell lease");

    let child_path = route_path(&control.state.routes["child"]);
    for tool_use_id in ["read-one", "read-two"] {
        control
            .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":tool_use_id,"tool_name":"Read","tool_input":{"path":child_path.join("missing.txt").display().to_string()}}))
            .await
            .expect("overlapping pre-tool hook");
    }
    assert!(control
        .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"read-one","tool_name":"Write"}))
        .await
        .is_err());
    control
        .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"read-two","tool_name":"Read"}))
        .await
        .expect("close second overlapping tool");
    control
        .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"read-one","tool_name":"Read"}))
        .await
        .expect("close first overlapping tool");
    control
        .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"read-one","tool_name":"Read"}))
        .await
        .expect("duplicate post-tool is idempotent");
    assert!(control
        .post_tool(json!({"session_id":"session","turn_id":"unknown-turn","tool_use_id":"unknown-post","tool_name":"Read"}))
        .await
        .is_err());

    let child = control
        .workspace(&control.state.routes["child"])
        .await
        .expect("child workspace");
    let mut transaction = child
        .begin_transaction(IdempotencyKey::new())
        .await
        .expect("first child transaction");
    transaction
        .write_text("/first.txt", "first")
        .await
        .expect("first file");
    transaction.commit().await.expect("commit first file");
    drop(transaction);
    control.mounts["child"]
        .advance_to_head()
        .await
        .expect("advance child mount");

    let first_merge = control
        .agent_merge(json!({"agent":"child","_caller_turn_id":"root-turn"}))
        .await
        .expect("first merge");
    assert!(matches!(
        first_merge["status"].as_str(),
        Some("applied" | "already-applied")
    ));
    assert_eq!(
        fs::read(root.join("first.txt")).expect("published first file"),
        b"first"
    );
    assert!(control.mounts.contains_key("child"));

    let route = &control.state.routes["child"];
    let published_generation = route
        .roots
        .get(&root_key(WorkspaceRootId::from_bytes(route.root_id)))
        .expect("active route root")
        .published_generation;
    drop(child);
    let child = control.workspace(route).await.expect("incremental child");
    let mut transaction = child
        .begin_transaction(IdempotencyKey::new())
        .await
        .expect("incremental transaction");
    transaction
        .write_text("/second.txt", "second")
        .await
        .expect("incremental file");
    transaction.commit().await.expect("incremental commit");
    drop(transaction);
    control.mounts["child"]
        .advance_to_head()
        .await
        .expect("advance incremental mount");
    assert_ne!(
        *child
            .head()
            .await
            .expect("incremental child head")
            .id()
            .digest()
            .as_bytes(),
        published_generation
    );
    let incremental = control
        .agent_merge(json!({"agent":"child","_caller_turn_id":"root-turn"}))
        .await
        .expect("incremental merge");
    assert!(
        matches!(
            incremental["status"].as_str(),
            Some("applied" | "already-applied")
        ),
        "unexpected incremental merge outcome: {incremental}"
    );
    assert_eq!(
        fs::read(root.join("second.txt")).expect("published second file"),
        b"second"
    );

    control.fail_next_unmount.insert("child".to_owned());
    assert!(
        control
            .subagent_stop(
                json!({"session_id":"session","turn_id":"child-turn","agent_id":"child"})
            )
            .await
            .is_err()
    );
    assert!(control.mounts.contains_key("child"));
    assert_eq!(
        control.state.routes["child"].lifecycle,
        RouteLifecycle::StopRequested
    );
    drop(child);
    drop(control);
    let mut control = ControlPlane::open(data.clone())
        .await
        .expect("reopen after failed stop teardown");
    assert!(!control.mounts.contains_key("child"));
    assert!(control.state.routes["child"].lifecycle.is_frozen());
    let root_agent = control.state.root_agent_id.clone();
    control
        .subagent_start(json!({
            "session_id":"session",
            "turn_id":"child-turn",
            "agent_id":"child",
            "agent_type":"explorer",
            "_authenticated_parent_agent_id":root_agent,
        }))
        .await
        .expect("resume child after durable stop recovery");
    assert!(control.mounts.contains_key("child"));
    assert_eq!(
        control.state.routes["child"].lifecycle,
        RouteLifecycle::Active
    );

    control
        .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"active-read","tool_name":"Read","tool_input":{"path":child_path.join("first.txt").display().to_string()}}))
        .await
        .expect("active tool before stop");
    assert!(
        control
            .subagent_stop(
                json!({"session_id":"session","turn_id":"child-turn","agent_id":"child"})
            )
            .await
            .is_err()
    );
    assert!(
        control
            .agent_discard(json!({"agent":"child","_caller_turn_id":"root-turn"}))
            .await
            .is_err()
    );
    control
        .state
        .leases
        .get_mut("active-read")
        .expect("active adapter lease")
        .expires_at_millis = 0;
    control.persist().expect("persist expired adapter lease");
    control
        .pre_tool(json!({
            "session_id":"session",
            "turn_id":"child-turn",
            "tool_use_id":"recovered-read",
            "tool_name":"Read",
            "tool_input":{"path":child_path.join("first.txt").display().to_string()}
        }))
        .await
        .expect("next tool fences and recovers the expired writer first");
    assert!(!control.state.leases.contains_key("active-read"));
    assert!(control.state.leases.contains_key("recovered-read"));
    control
        .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"recovered-read","tool_name":"Read"}))
        .await
        .expect("close recovered tool");
    control
        .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"pending-descendant","tool_name":"spawn_agent","tool_input":{}}))
        .await
        .expect("pending descendant before discard");
    control
        .subagent_start(json!({
            "session_id":"session",
            "turn_id":"grandchild-turn",
            "agent_id":"grandchild",
            "agent_type":"explorer",
            "_authenticated_parent_agent_id":"child",
        }))
        .await
        .expect("start live grandchild");
    let stopped = control
        .subagent_stop(json!({"session_id":"session","turn_id":"child-turn","agent_id":"child"}))
        .await
        .expect("request parent stop while grandchild is live");
    assert_eq!(stopped["suppressOutput"], true);
    assert_eq!(stopped["acyclicSummary"]["state"], "stopping");
    assert_eq!(stopped["acyclicSummary"]["agent"], "agents/child");
    assert_eq!(stopped["acyclicSummary"]["tests"]["status"], "not-reported");
    assert!(
        stopped["acyclicSummary"]["actions"]["merge"]
            .as_str()
            .is_some_and(|command| command.contains("agents/child"))
    );
    assert_eq!(
        control.state.routes["child"].lifecycle,
        RouteLifecycle::StopRequested
    );
    assert!(control.mounts.contains_key("child"));
    assert!(control
        .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"after-stop-request","tool_name":"Read","tool_input":{"path":child_path.join("first.txt").display().to_string()}}))
        .await
        .is_err());
    control
        .subagent_stop(json!({
            "session_id":"session",
            "turn_id":"grandchild-turn",
            "agent_id":"grandchild",
        }))
        .await
        .expect("grandchild stop freezes it and its waiting parent");
    let root_agent = control.state.root_agent_id.clone();
    let status = control
        .agents_status(&root_agent)
        .await
        .expect("recursive agent status");
    assert_eq!(status["schemaVersion"], 1);
    assert_eq!(status["agents"][0]["ref"], "agents/child");
    assert_eq!(status["agents"][0]["state"], "frozen");
    assert!(status["agents"][0]["roots"].is_array());
    assert!(control.state.routes["child"].lifecycle.is_frozen());
    assert!(!control.mounts.contains_key("child"));
    drop(control);

    let mut control = ControlPlane::open(data.clone())
        .await
        .expect("reopen stopped child");
    assert!(!control.mounts.contains_key("child"));
    assert!(control
        .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"after-stop","tool_name":"Read","tool_input":{"path":child_path.join("first.txt").display().to_string()}}))
        .await
        .is_err());
    assert!(control.state.pending.is_empty());
    control.fail_after_context_discard = true;
    assert!(
        control
            .agent_discard(json!({"agent":"child","_caller_turn_id":"root-turn"}))
            .await
            .is_err()
    );
    assert!(control.state.pending_discards.contains_key("child"));
    drop(control);
    let control = ControlPlane::open(data)
        .await
        .expect("recover interrupted durable discard");
    assert!(control.state.pending.is_empty());
    assert!(control.state.pending_discards.is_empty());
    assert!(!control.state.routes.contains_key("child"));
    control.shutdown().await.expect("graceful shutdown");
}

async fn authenticated_control_protocol_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    let root = temporary.path().join("root");
    fs::create_dir(&root).expect("root directory");
    let mut control = ControlPlane::open(data.clone())
        .await
        .expect("control plane");
    control
        .session_start(json!({"session_id":"session","cwd":root.display().to_string()}))
        .await
        .expect("root session");
    control
        .user_prompt(json!({"session_id":"session","turn_id":"root-turn"}))
        .expect("root turn");
    control
        .pre_tool(json!({"session_id":"session","turn_id":"root-turn","tool_use_id":"spawn","tool_name":"spawn_agent","tool_input":{}}))
        .await
        .expect("spawn handshake");
    control
        .subagent_start(json!({"session_id":"session","turn_id":"child-turn","agent_id":"child","agent_type":"explorer"}))
        .await
        .expect("child start");
    control
        .pre_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"git-switch","tool_name":"exec_command","tool_input":{"cmd":"acyclic git switch -c feature","workdir":root.display().to_string()}}))
        .await
        .expect("open authenticated command");
    let child_context_id = WorkspaceContextId::from_bytes(control.state.routes["child"].context_id);
    let child_root_id = WorkspaceRootId::from_bytes(control.state.routes["child"].root_id);
    let original_workspace = control
        .distributed
        .contexts()
        .resolve(child_context_id)
        .await
        .expect("child context")
        .roots[&child_root_id]
        .workspace_id;
    let child_cwd = route_path(&control.state.routes["child"]);
    let shared = Arc::new(AsyncMutex::new(control));
    let ledger = Arc::new(ControlLedger::open(&data).expect("control ledger"));
    for argv in [vec![
        "status".to_owned(),
        "&&".to_owned(),
        "push".to_owned(),
    ]] {
        let rejected = dispatch_control_request(
            &shared,
            ControlRequest {
                version: 1,
                command: ControlCommand::Git,
                cwd: child_cwd.clone(),
                argv,
                name: String::new(),
                arguments: Value::Null,
            },
        )
        .await;
        assert!(rejected.is_err(), "strict Git argv must fail closed");
    }
    let (mut client, server) = tokio::io::duplex(64 * 1024);
    let (_connection_shutdown, receiver) = watch::channel(false);
    let handler = tokio::spawn(handle_control_connection(
        server,
        Arc::clone(&shared),
        Arc::clone(&ledger),
        receiver,
    ));
    let envelope = ControlEnvelope::new(ControlRequest {
        version: 1,
        command: ControlCommand::Git,
        cwd: child_cwd.clone(),
        argv: vec!["status".to_owned()],
        name: String::new(),
        arguments: Value::Null,
    });
    let request = serde_json::to_vec(&envelope).expect("control request");
    client.write_all(&request).await.expect("write request");
    client.write_all(b"\n").await.expect("write newline");
    client.flush().await.expect("flush request");
    let mut response = String::new();
    BufReader::new(&mut client)
        .read_line(&mut response)
        .await
        .expect("read response");
    let response: Value = serde_json::from_str(&response).expect("response JSON");
    assert_eq!(response["version"], 2);
    assert_eq!(
        response["requestId"],
        Value::String(envelope.request_id.as_str().to_owned())
    );
    assert_eq!(response["ok"], true, "{response}");
    assert!(response["result"].is_object());
    handler
        .await
        .expect("handler task")
        .expect("handler result");

    let (mut client, server) = tokio::io::duplex(1);
    let (connection_shutdown, receiver) = watch::channel(false);
    let handler = tokio::spawn(handle_control_connection(
        server,
        Arc::clone(&shared),
        Arc::clone(&ledger),
        receiver,
    ));
    let switch_envelope = ControlEnvelope::new(ControlRequest {
        version: 1,
        command: ControlCommand::Git,
        cwd: child_cwd.clone(),
        argv: vec!["switch".to_owned(), "-c".to_owned(), "feature".to_owned()],
        name: String::new(),
        arguments: Value::Null,
    });
    let request = serde_json::to_vec(&switch_envelope).expect("control request");
    client.write_all(&request).await.expect("write request");
    client.write_all(b"\n").await.expect("write newline");
    client.flush().await.expect("flush request");
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let control = shared.lock().await;
            let current = control
                .distributed
                .contexts()
                .resolve(child_context_id)
                .await
                .expect("switched child context")
                .roots[&child_root_id]
                .workspace_id;
            if current != original_workspace {
                break;
            }
            drop(control);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the accepted Git switch must finish before response cancellation");
    connection_shutdown
        .send(true)
        .expect("signal connection shutdown");
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), handler)
        .await
        .expect("blocked response shutdown")
        .expect("handler task")
        .expect_err("an unread bounded response must reach its drain deadline");
    assert!(
        error.contains("response exceeded its drain deadline"),
        "{error}"
    );
    drop(client);
    drop(ledger);
    let ledger = Arc::new(ControlLedger::open(&data).expect("reopen control ledger"));

    let (mut retry_client, retry_server) = tokio::io::duplex(64 * 1024);
    let (_retry_shutdown, retry_receiver) = watch::channel(false);
    let retry_handler = tokio::spawn(handle_control_connection(
        retry_server,
        Arc::clone(&shared),
        Arc::clone(&ledger),
        retry_receiver,
    ));
    let retry_request = serde_json::to_vec(&switch_envelope).expect("retry request");
    retry_client
        .write_all(&retry_request)
        .await
        .expect("write retry request");
    retry_client
        .write_all(b"\n")
        .await
        .expect("write retry newline");
    retry_client.flush().await.expect("flush retry request");
    let mut retry_response = String::new();
    BufReader::new(&mut retry_client)
        .read_line(&mut retry_response)
        .await
        .expect("read cached retry response");
    let retry_response: Value =
        serde_json::from_str(&retry_response).expect("cached response JSON");
    assert_eq!(retry_response["ok"], true, "{retry_response}");
    assert_eq!(
        retry_response["requestId"],
        Value::String(switch_envelope.request_id.as_str().to_owned())
    );
    retry_handler
        .await
        .expect("retry handler task")
        .expect("retry handler result");

    let mut control = shared.lock().await;
    control
        .agent_changes(json!({"agent":"child","_caller_turn_id":"root-turn"}))
        .await
        .expect("inspect selected compatibility branch");
    control
        .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"git-switch","tool_name":"exec_command"}))
        .await
        .expect("close authenticated command");
    control
        .subagent_stop(json!({"session_id":"session","turn_id":"child-turn","agent_id":"child"}))
        .await
        .expect("stop child");
    let stale = ControlRequest {
        version: 1,
        command: ControlCommand::Git,
        cwd: child_cwd,
        argv: vec!["status".to_owned()],
        name: String::new(),
        arguments: Value::Null,
    };
    drop(control);
    assert!(dispatch_control_request(&shared, stale).await.is_err());
    let control = match Arc::try_unwrap(shared) {
        Ok(control) => control.into_inner(),
        Err(_) => panic!("response cancellation retained the control plane"),
    };
    control.shutdown().await.expect("shutdown");
    let reopened = ControlPlane::open(data)
        .await
        .expect("reopen after blocked response shutdown");
    reopened.shutdown().await.expect("reopened shutdown");
}

async fn recursive_publication_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    let root = temporary.path().join("root");
    fs::create_dir_all(&root).expect("root directory");
    fs::write(root.join("base.txt"), b"base").expect("unobserved root fixture");
    let mut control = ControlPlane::open(data.clone())
        .await
        .expect("control plane");
    control
        .session_start(json!({"session_id":"session","cwd":root.display().to_string()}))
        .await
        .expect("root session");
    control
        .user_prompt(json!({"session_id":"session","turn_id":"root-turn"}))
        .expect("root turn");
    control
        .user_prompt(json!({"session_id":"session","turn_id":"root-turn-2"}))
        .expect("second root turn");
    assert_eq!(
        control
            .resolve_turn("root-turn-2")
            .expect("resolve second root turn"),
        control.state.root_agent_id
    );
    control
        .pre_tool(json!({
            "session_id":"session",
            "turn_id":"root-turn","tool_use_id":"spawn-child",
            "tool_name":"spawn_agent","tool_input":{}
        }))
        .await
        .expect("child spawn");
    control
        .subagent_start(json!({
            "session_id":"session","turn_id":"child-turn",
            "agent_id":"child","agent_type":"explorer"
        }))
        .await
        .expect("child start");
    let child_path = route_path(&control.state.routes["child"]);
    control
        .pre_tool(json!({
            "session_id":"session",
            "turn_id":"child-turn","tool_use_id":"git-commit",
            "tool_name":"exec_command",
            "tool_input":{"cmd":"git commit -m initial","workdir":root.display().to_string()}
        }))
        .await
        .expect("bare Git is allowed and remains unrelated to Acyclic");
    assert_eq!(
        fs::read(root.join("base.txt")).expect("base before command close"),
        b"base"
    );
    control
        .post_tool(json!({
            "session_id":"session","turn_id":"child-turn",
            "tool_use_id":"git-commit","tool_name":"exec_command"
        }))
        .await
        .expect("close bare Git lease");
    assert_eq!(
        fs::read(root.join("base.txt")).expect("base after command lease"),
        b"base"
    );
    let read_hook = json!({
        "session_id":"session",
        "turn_id":"child-turn","tool_use_id":"read-retry",
        "tool_name":"Read","tool_input":{"path":child_path.join("base.txt").display().to_string()}
    });
    let first = control
        .pre_tool(read_hook.clone())
        .await
        .expect("first read hook");
    let retry = control
        .pre_tool(read_hook)
        .await
        .expect("retried read hook");
    assert_eq!(first, retry);
    let child_workspace = control
        .workspace(&control.state.routes["child"])
        .await
        .expect("child workspace");
    let snapshot = control
        .distributed
        .operations()
        .snapshot(child_workspace.id())
        .await
        .expect("operation window");
    assert!(matches!(
        snapshot.phase,
        acyclic_fs::OperationWindowPhase::Active { ref leases, .. } if leases.len() == 1
    ));
    control
        .post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"read-retry","tool_name":"Read"}))
        .await
        .expect("close read hook");
    assert_eq!(
        fs::read(root.join("base.txt")).expect("base after read lease"),
        b"base"
    );
    control
        .pre_tool(json!({
            "session_id":"session",
            "turn_id":"child-turn","tool_use_id":"spawn-grandchild",
            "tool_name":"spawn_agent","tool_input":{}
        }))
        .await
        .expect("grandchild spawn");
    assert_eq!(
        fs::read(root.join("base.txt")).expect("base before grandchild start"),
        b"base"
    );
    control
        .subagent_start(json!({
            "session_id":"session","turn_id":"grandchild-turn",
            "agent_id":"grandchild","agent_type":"explorer"
        }))
        .await
        .expect("grandchild start");
    assert_eq!(
        fs::read(root.join("base.txt")).expect("base after grandchild fork"),
        b"base"
    );
    // A descendant can finish against the parent's last stable generation
    // while the parent still has an active tool. The parent's later close
    // supplies the next generation, rather than blocking the descendant.
    control
        .pre_tool(json!({
            "session_id":"session","turn_id":"child-turn",
            "tool_use_id":"parent-active-read","tool_name":"Read",
            "tool_input":{"path":child_path.join("base.txt").display().to_string()}
        }))
        .await
        .expect("parent starts overlapping read");
    assert_eq!(
        fs::read(child_path.join("base.txt")).expect("parent reads through its mount"),
        b"base"
    );
    let grandchild_path = route_path(&control.state.routes["grandchild"]);
    control
        .pre_tool(json!({
            "session_id":"session","turn_id":"grandchild-turn",
            "tool_use_id":"descendant-read","tool_name":"Read",
            "tool_input":{"path":grandchild_path.join("base.txt").display().to_string()}
        }))
        .await
        .expect("descendant starts while parent is active");
    control
        .post_tool(json!({
            "session_id":"session","turn_id":"grandchild-turn",
            "tool_use_id":"descendant-read","tool_name":"Read"
        }))
        .await
        .expect("descendant closes against stable parent generation");
    let writer = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "tests::projected_mount_writer_child",
            "--ignored",
        ])
        .env(
            "ACYCLIC_TEST_PROJECTED_WRITE_PATH",
            child_path.join("base.txt"),
        )
        .output()
        .expect("external writer process");
    assert!(
        writer.status.success(),
        "external writer failed: {}",
        String::from_utf8_lossy(&writer.stderr)
    );
    control
        .post_tool(json!({
            "session_id":"session","turn_id":"child-turn",
            "tool_use_id":"parent-active-read","tool_name":"Read"
        }))
        .await
        .expect("parent later closes");
    assert_eq!(
        control
            .workspace(&control.state.routes["child"])
            .await
            .expect("parent workspace")
            .read("/base.txt", 32)
            .await
            .expect("parent captured late write")
            .as_ref(),
        b"late parent"
    );
    let grandchild = control
        .workspace(&control.state.routes["grandchild"])
        .await
        .expect("grandchild workspace");
    let observed = grandchild
        .read("/base.txt", 32)
        .await
        .expect("descendant immediately observes completed parent");
    assert_eq!(observed.as_ref(), b"late parent");
    let mut transaction = grandchild
        .begin_transaction(IdempotencyKey::new())
        .await
        .expect("grandchild transaction");
    transaction
        .write_text("/nested.txt", "nested")
        .await
        .expect("nested file");
    transaction.commit().await.expect("commit nested file");
    drop(transaction);
    assert_eq!(
        fs::read(root.join("base.txt")).expect("base after grandchild write"),
        b"base"
    );

    assert!(
        control
            .agent_merge(json!({
                "agent":"grandchild","_caller_turn_id":"root-turn"
            }))
            .await
            .is_err()
    );
    assert_eq!(
        fs::read(root.join("base.txt")).expect("base after unauthorized merge"),
        b"base"
    );
    control
        .agent_merge(json!({
            "agent":"grandchild","_caller_turn_id":"child-turn"
        }))
        .await
        .expect("merge into child");
    assert_eq!(
        fs::read(root.join("base.txt")).expect("base before root merge"),
        b"base"
    );
    control
        .agent_merge(json!({
            "agent":"child","_caller_turn_id":"root-turn"
        }))
        .await
        .expect("merge into root");
    assert_eq!(
        fs::read(root.join("nested.txt")).expect("nested root file"),
        b"nested"
    );
    assert_eq!(
        fs::read(root.join("base.txt")).expect("external child write reaches root"),
        b"late parent"
    );

    control.fail_next_unmount.insert("grandchild".to_owned());
    assert!(
        control
            .agent_discard(json!({"agent":"child","_caller_turn_id":"root-turn"}))
            .await
            .is_err()
    );
    assert!(control.mounts.contains_key("child"));
    assert!(control.mounts.contains_key("grandchild"));
    assert!(control.state.pending_discards.contains_key("child"));
    control.fail_after_discard_delete = true;
    assert!(
        control
            .agent_discard(json!({"agent":"child","_caller_turn_id":"root-turn"}))
            .await
            .is_err()
    );
    assert!(!control.mounts.contains_key("grandchild"));
    assert!(control.state.pending_discards.contains_key("child"));
    drop(child_workspace);
    drop(grandchild);
    drop(control);

    let control = ControlPlane::open(data)
        .await
        .expect("recover recursive discard after durable delete");
    assert!(control.state.pending_discards.is_empty());
    assert!(control.state.routes.is_empty());
    assert!(control.state.turns.is_empty());
    assert!(control.mounts.is_empty());
    control
        .shutdown()
        .await
        .expect("shutdown after discard recovery");
}

async fn multi_root_publication_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    let first = temporary.path().join("first");
    let second = temporary.path().join("second");
    fs::create_dir_all(&first).expect("first root");
    fs::create_dir_all(&second).expect("second root");
    fs::write(first.join("base.txt"), b"first").expect("first fixture");
    fs::write(second.join("base.txt"), b"second").expect("second fixture");

    let mut control = ControlPlane::open(data.clone())
        .await
        .expect("control plane");
    control
        .session_start(json!({"session_id":"session","cwd":first}))
        .await
        .expect("first root attach");
    control
        .session_start(json!({"session_id":"session","cwd":second}))
        .await
        .expect("second root adopt");
    assert_eq!(control.state.roots.len(), 2);
    let second_root = control
        .state
        .roots
        .values()
        .find(|root| root.root_id != control.state.root_id)
        .map(|root| WorkspaceRootId::from_bytes(root.root_id))
        .expect("second root binding");
    control
        .user_prompt(json!({"session_id":"session","turn_id":"root-turn"}))
        .expect("root turn");
    control
        .pre_tool(json!({
            "session_id":"session",
            "turn_id":"root-turn",
            "tool_use_id":"spawn-child",
            "tool_name":"spawn_agent",
            "tool_input":{},
            "_caller_root_id":hex::encode(second_root.into_bytes())
        }))
        .await
        .expect("child spawn handshake");
    control
        .subagent_start(json!({
            "session_id":"session",
            "turn_id":"child-turn",
            "agent_id":"child"
        }))
        .await
        .expect("child start");
    let route = control.state.routes["child"].clone();
    assert_eq!(route.roots.len(), 2);
    assert_eq!(route.root_id, second_root.into_bytes());
    let root_keys = route.roots.keys().cloned().collect::<Vec<_>>();
    let first_key = root_keys.first().expect("first ordered root").clone();
    let last_key = root_keys.last().expect("last ordered root").clone();
    let child_context_id = WorkspaceContextId::from_bytes(route.context_id);
    let last_root_id = WorkspaceRootId::from_bytes(route.roots[&last_key].root_id);
    let original_root = control
        .distributed
        .contexts()
        .resolve(child_context_id)
        .await
        .expect("child context")
        .roots[&last_root_id]
        .clone();
    control
        .distributed
        .contexts()
        .set_workspace(
            child_context_id,
            last_root_id,
            acyclic_fs::WorkspaceId::from_bytes([u8::MAX; 16]),
            "missing-workspace".to_owned(),
            original_root.parent_workspace_id,
        )
        .await
        .expect("inject missing core workspace");
    assert!(
        control
            .pre_tool(json!({
                "session_id":"session",
                "turn_id":"child-turn",
                "tool_use_id":"partial-lease",
                "tool_name":"Read",
                "tool_input":{"path":route_path(&route).join("base.txt")}
            }))
            .await
            .is_err()
    );
    assert!(!control.state.leases.contains_key("partial-lease"));
    let first_route = &route.roots[&first_key];
    let first_workspace = control
        .workspace_root(&route, WorkspaceRootId::from_bytes(first_route.root_id))
        .await
        .expect("first child root");
    assert!(matches!(
        control
            .distributed
            .operations()
            .snapshot(first_workspace.id())
            .await
            .expect("rolled-back window")
            .phase,
        acyclic_fs::OperationWindowPhase::Idle
    ));
    control
        .distributed
        .contexts()
        .set_workspace(
            child_context_id,
            last_root_id,
            original_root.workspace_id,
            original_root.workspace_name,
            original_root.parent_workspace_id,
        )
        .await
        .expect("restore core workspace");
    control
        .pre_tool(json!({
            "session_id":"session",
            "turn_id":"child-turn",
            "tool_use_id":"active-write",
            "tool_name":"Write",
            "tool_input":{"path":route_path(&route).join("in-flight.txt")}
        }))
        .await
        .expect("active child write lease");
    for command in ["changes", "merge", "discard"] {
        let input = json!({
            "agent":"child",
            "_caller_agent_id":control.state.root_agent_id
        });
        let result = match command {
            "changes" => control.agent_changes(input).await,
            "merge" => control.agent_merge(input).await,
            "discard" => control.agent_discard(input).await,
            _ => unreachable!(),
        };
        assert!(
            result.is_err(),
            "untrusted MCP input must not supply caller identity for {command}"
        );
    }
    assert!(
        control
            .agent_changes(json!({"agent":"child","_caller_turn_id":"root-turn"}))
            .await
            .is_err(),
        "inspection must not flush a mount while a tool lease is active"
    );
    assert!(
        control
            .agent_merge(json!({"agent":"child","_caller_turn_id":"root-turn"}))
            .await
            .is_err(),
        "publication must not flush a mount while a tool lease is active"
    );
    control
        .post_tool(json!({
            "session_id":"session",
            "turn_id":"child-turn",
            "tool_use_id":"active-write",
            "tool_name":"Write"
        }))
        .await
        .expect("close child write lease");
    for route_root in route.roots.values() {
        let workspace = control
            .workspace_root(&route, WorkspaceRootId::from_bytes(route_root.root_id))
            .await
            .expect("child root workspace");
        let mut transaction = workspace
            .begin_transaction(IdempotencyKey::new())
            .await
            .expect("root transaction");
        transaction
            .write_text("/child.txt", &route_root.mount_name())
            .await
            .expect("root write");
        transaction.commit().await.expect("root commit");
    }
    let changes = control
        .agent_changes(json!({
            "agent":"child",
            "path":"child.txt",
            "_caller_turn_id":"root-turn"
        }))
        .await
        .expect("multi-root filtered changes");
    assert_eq!(changes["roots"].as_array().map(Vec::len), Some(2));
    assert_eq!(changes["fileChanges"], 0);
    assert_eq!(changes["bindingChanges"], 2);
    assert!(changes["roots"].as_array().is_some_and(|roots| {
        roots
            .iter()
            .all(|root| root["paths"] == json!(["/child.txt"]))
    }));
    control.mounts["child"]
        .advance_to_head()
        .await
        .expect("advance child mounts");
    let result = control
        .agent_merge(json!({"agent":"child","_caller_turn_id":"root-turn"}))
        .await
        .expect("multi-root publication");
    assert_eq!(result["status"], "applied");
    assert!(first.join("child.txt").is_file());
    assert!(second.join("child.txt").is_file());
    drop(first_workspace);
    control.shutdown().await.expect("shutdown");

    let resumed = ControlPlane::open(data)
        .await
        .expect("resume control plane");
    assert_eq!(resumed.state.roots.len(), 2);
    assert_eq!(resumed.state.routes["child"].roots.len(), 2);
    let (_, routed, root_id) = resumed
        .route_root_from_cwd(&route_path(&resumed.state.routes["child"]))
        .expect("route resumed child cwd");
    assert_eq!(routed.expect("child route").agent_id, "child");
    assert_eq!(root_id, second_root);
    resumed.shutdown().await.expect("resumed shutdown");
}

async fn core_root_recovery_case() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path().join("plugin-data");
    let first = temporary.path().join("first");
    let second = temporary.path().join("second");
    fs::create_dir_all(&first).expect("first root");
    fs::create_dir_all(&second).expect("second root");

    let initial_data = temporary.path().join("initial-plugin-data");
    let mut initial = ControlPlane::open(initial_data.clone())
        .await
        .expect("initial control plane");
    initial.fail_after_root_intent = true;
    let error = initial
        .session_start(json!({"session_id":"initial","cwd":first}))
        .await
        .expect_err("injected crash after durable initial root intent");
    assert_eq!(error, "injected failure after root registration intent");
    assert!(initial.state.pending_root_registration);
    let expected_context = initial.state.root_context_id;
    let expected_workspace = initial
        .state
        .roots
        .values()
        .next()
        .expect("root intent")
        .repository_workspace_id;
    drop(initial);
    // Only the root's attach reached the core log, unflushed: a power loss
    // leaves the flushed intent without the binding.
    let core_log = initial_data
        .join("core-state")
        .join("core-state-log-v1")
        .join(format!("{}.json", "0".repeat(32)));
    assert!(
        fs::metadata(&core_log).expect("core log").len() > 0,
        "the deferred attach reached the core log"
    );
    fs::write(&core_log, b"").expect("lose the unflushed attach");
    let resumed_initial = ControlPlane::open(initial_data)
        .await
        .expect("finish pending initial root registration");
    assert!(!resumed_initial.state.pending_root_registration);
    assert_eq!(resumed_initial.state.root_context_id, expected_context);
    assert_eq!(
        resumed_initial
            .state
            .roots
            .values()
            .next()
            .expect("recovered root")
            .repository_workspace_id,
        expected_workspace
    );
    resumed_initial.shutdown().await.expect("initial shutdown");

    // A power loss after the root registered loses the unflushed final
    // save; recovery repeats the registration against the durable intent.
    let registered_data = temporary.path().join("registered-plugin-data");
    let mut registered = ControlPlane::open(registered_data.clone())
        .await
        .expect("registered control plane");
    registered
        .session_start(json!({"session_id":"registered","cwd":first}))
        .await
        .expect("registered root");
    assert!(!registered.state.pending_root_registration);
    let expected_context = registered.state.root_context_id;
    drop(registered);
    fs::remove_file(registered_data.join(ADAPTER_STATE_SLOTS[2]))
        .expect("lose the unflushed final save");
    let reregistered = ControlPlane::open(registered_data)
        .await
        .expect("repeat the root registration");
    assert!(!reregistered.state.pending_root_registration);
    assert_eq!(reregistered.state.root_context_id, expected_context);
    reregistered
        .shutdown()
        .await
        .expect("reregistered shutdown");

    let mut control = ControlPlane::open(data.clone())
        .await
        .expect("control plane");
    control
        .session_start(json!({"session_id":"session","cwd":first}))
        .await
        .expect("first root attach");
    control.fail_after_root_intent = true;
    let error = control
        .session_start(json!({"session_id":"session","cwd":second}))
        .await
        .expect_err("injected crash after durable root intent");
    assert_eq!(error, "injected failure after root adoption intent");
    assert_eq!(control.state.pending_root_adoptions.len(), 1);
    let second_root = control
        .state
        .roots
        .values()
        .find(|root| root.root_id != control.state.root_id)
        .map(|root| WorkspaceRootId::from_bytes(root.root_id))
        .expect("second root binding");
    let second_key = root_key(second_root);
    let expected_workspace = control
        .state
        .roots
        .get(&second_key)
        .expect("durable root intent")
        .repository_workspace_id;
    drop(control);

    let resumed = ControlPlane::open(data)
        .await
        .expect("finish pending core root adoption");
    assert_eq!(resumed.state.roots.len(), 2);
    assert!(resumed.state.pending_root_adoptions.is_empty());
    assert_eq!(
        resumed.state.roots[&second_key].repository_workspace_id,
        expected_workspace
    );
    resumed.shutdown().await.expect("shutdown");

    let fenced_data = temporary.path().join("fenced-plugin-data");
    let mut fenced = ControlPlane::open(fenced_data.clone())
        .await
        .expect("fenced control plane");
    fenced
        .session_start(json!({"session_id":"fenced","cwd":first}))
        .await
        .expect("fenced first root");
    fenced.fail_after_root_intent = true;
    fenced
        .session_start(json!({"session_id":"fenced","cwd":second}))
        .await
        .expect_err("fenced adoption fault");
    let pending = fenced
        .state
        .pending_root_adoptions
        .first()
        .cloned()
        .expect("pending fenced root");
    fenced
        .state
        .roots
        .get_mut(&pending)
        .expect("pending fenced binding")
        .native_root_identity = [u8::MAX; 16];
    fenced.persist().expect("persist replacement identity");
    drop(fenced);
    let error = ControlPlane::open(fenced_data)
        .await
        .err()
        .expect("replacement root must be fenced");
    assert!(error.contains("identity changed"), "{error}");
}

#[test]
fn patch_paths_are_rewritten_inside_the_child() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let child = temporary.path().join("workspace");
    fs::create_dir(&child).expect("child directory");
    let input = json!({"command": "*** Begin Patch\n*** Add File: src/new.rs\n*** Move to: src/moved.rs\n*** End Patch"});
    let rewritten =
        rewrite_tool_input("apply_patch", input, temporary.path(), &child).expect("rewrite patch");
    let command = rewritten["command"].as_str().expect("command");
    assert!(command.contains(&child.join("src/new.rs").display().to_string()));
    assert!(command.contains(&child.join("src/moved.rs").display().to_string()));
}

#[test]
fn patch_paths_cannot_escape_the_child() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    let child = temporary.path().join("workspace");
    fs::create_dir(&root).expect("root directory");
    fs::create_dir(&child).expect("child directory");
    let outside = temporary.path().join("outside.txt").display().to_string();
    for directive in [
        "Add File",
        "Update File",
        "Delete File",
        "Move to",
        "Copy to",
    ] {
        for path in ["../parent.txt", &outside] {
            let input = json!({
                "command": format!("*** Begin Patch\n*** {directive}: {path}\n*** End Patch")
            });
            assert!(rewrite_tool_input("apply_patch", input, &root, &child).is_err());
        }
    }
}

#[test]
fn remote_tool_hooks_are_process_local_noops_for_every_native_host() {
    for host in ["codex", "claude-code", "copilot", "cursor"] {
        for event in ["PreToolUse", "PostToolUse", "PostToolUseFailure"] {
            for tool in [
                "mcp__codex_app__list_threads",
                "mcp__codex_app__send_message_to_thread",
            ] {
                assert!(native_hook_is_process_local_noop(
                    host,
                    event,
                    &json!({"tool_name":tool})
                ));
            }
        }
    }
    assert!(!native_hook_is_process_local_noop(
        "codex",
        "PreToolUse",
        &json!({"tool_name":"Bash"})
    ));
    assert!(!native_hook_is_process_local_noop(
        "claude-code",
        "SessionStart",
        &json!({"tool_name":"mcp__codex_app__list_threads"})
    ));
    for tool in [
        "mcp__codex_app__create_thread",
        "mcp__codex_app__create_worktree",
        "mcp__codex_app__fork_thread",
        "mcp__codex_app__handoff_thread",
        "mcp__codex_app__open_in_codex",
        "mcp__codex_app__uninstall_plugin",
        "mcp__codex_app__future_unknown_tool",
    ] {
        assert!(
            !is_known_non_filesystem_tool(tool),
            "filesystem-capable or unknown app tool must fail closed: {tool}"
        );
    }
}

#[test]
fn structured_paths_must_remain_inside_the_child() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let child = temporary.path().join("workspace");
    fs::create_dir(&child).expect("child directory");
    let valid = json!({"path": child.join("src/lib.rs").display().to_string()});
    validate_tool_paths("Read", &valid, &child).expect("child path");
    let rewritten = rewrite_tool_input(
        "Write",
        json!({"file_path":"relative.txt","content":"child"}),
        &temporary.path().join("root"),
        &child,
    )
    .expect("relative child path rewrite");
    assert_eq!(
        rewritten["file_path"],
        child.join("relative.txt").display().to_string()
    );
    assert!(validate_tool_paths("Read", &json!({"path": "../parent"}), &child).is_err());
    let outside = temporary.path().join("outside/file.rs");
    assert!(
        validate_tool_paths(
            "Read",
            &json!({"path": outside.display().to_string()}),
            &child
        )
        .is_err()
    );
}

#[test]
fn persisted_mount_paths_are_derived_not_authoritative() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root_id = WorkspaceRootId::from_bytes([1; 16]);
    let mount_path = workspace_mount_root(temporary.path()).join(compact_id(&[2; 16]));
    assert_eq!(compact_id(&[2; 16]).len(), 22);
    assert_eq!(route_name(root_id).len(), 23);
    assert_eq!(
        mount_path
            .strip_prefix(temporary.path())
            .expect("relative mount"),
        Path::new("w").join(compact_id(&[2; 16]))
    );
    let mut different_context = [2; 16];
    different_context[15] = 3;
    assert_ne!(compact_id(&[2; 16]), compact_id(&different_context));
    let mut different_root = [1; 16];
    different_root[15] = 2;
    assert_ne!(
        route_name(root_id),
        route_name(WorkspaceRootId::from_bytes(different_root))
    );
    let staging = root_materialization_directory(
        &temporary.path().join("physical-root"),
        OperationId::from_bytes([4; 16]),
    )
    .expect("materialization directory");
    let staging_segments = staging
        .strip_prefix(temporary.path())
        .expect("relative staging")
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(staging_segments.len(), 3);
    assert_eq!(staging_segments[0], ".acyclic-m");
    assert_eq!(staging_segments[1].len(), 22);
    assert_eq!(staging_segments[2].len(), 22);
    let mut route = Route {
        agent_id: "child".to_owned(),
        turn_id: "turn".to_owned(),
        context_id: [2; 16],
        root_id: root_id.into_bytes(),
        parent_agent_id: "root".to_owned(),
        roots: BTreeMap::from([(
            root_key(root_id),
            RouteRoot {
                root_id: root_id.into_bytes(),
                repository_workspace_id: [3; 16],
                published_generation: [0; 32],
            },
        )]),
        mount_path: mount_path.clone(),
        lifecycle: RouteLifecycle::Active,
    };
    assert_eq!(route_path(&route), mount_path.join(route_name(root_id)));
    validate_persisted_route_paths(temporary.path(), &route).expect("derived route paths");
    route.mount_path = temporary.path().join("outside");
    assert!(validate_persisted_route_paths(temporary.path(), &route).is_err());
}

#[test]
fn shell_expansion_cannot_escape_the_child() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let child = temporary.path().join("workspace");
    fs::create_dir(&child).expect("child directory");
    assert!(validate_shell_paths("type $env:TEMP\\secret", &child).is_err());
    assert!(validate_shell_paths("cat `pwd`/secret", &child).is_err());
    assert!(validate_shell_paths("cat ~/secret", &child).is_err());
    assert!(validate_shell_paths("cat<../outside", &child).is_err());
    assert!(validate_shell_paths("echo value>../outside", &child).is_err());
    assert!(validate_shell_paths("echo value 2>../outside", &child).is_err());
}

#[test]
fn only_acyclic_commands_require_standalone_shell_syntax() {
    assert_eq!(
        is_acyclic_cli_invocation(
            "Bash",
            &json!({"command":"printf 'accepted\\n' > accepted.txt\ncat contract.txt"})
        ),
        Ok(false)
    );
    assert_eq!(
        is_acyclic_cli_invocation("Bash", &json!({"command":"acyclic agents"})),
        Ok(true)
    );
    assert_eq!(
        is_acyclic_cli_invocation("PowerShell", &json!({"command":"acyclic agents"})),
        Ok(true)
    );
    assert!(
        is_acyclic_cli_invocation("Bash", &json!({"command":"acyclic agents && whoami"})).is_err()
    );
}

#[test]
fn commandless_hosts_receive_exactly_one_shell_free_tool() {
    assert_eq!(public_tools(false), json!([]));
    let tools = public_tools(true);
    assert_eq!(tools.as_array().map(Vec::len), Some(1));
    assert_eq!(tools[0]["name"], "acyclic");
    assert_eq!(tools[0]["inputSchema"]["required"], json!(["argv"]));
}

#[test]
fn hard_coded_parent_root_is_rejected_but_tool_workdir_is_redirected() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let root = temporary.path().join("root");
    let child = temporary.path().join("workspace");
    fs::create_dir(&root).expect("root directory");
    fs::create_dir(&child).expect("child directory");
    let root_text = root.display().to_string();
    assert!(
        rewrite_tool_input(
            "exec_command",
            json!({"cmd": format!("type {root_text}\\secret"), "workdir": root_text}),
            &root,
            &child,
        )
        .is_err()
    );
    let rewritten = rewrite_tool_input(
        "exec_command",
        json!({"cmd": "type relative.txt", "workdir": root.display().to_string()}),
        &root,
        &child,
    )
    .expect("redirect workdir");
    assert_eq!(rewritten["workdir"], child.display().to_string());
    let rewritten = rewrite_tool_input(
        "mcp__shell__exec_command",
        json!({"cmd": "pwd"}),
        &root,
        &child,
    )
    .expect("redirect namespaced exec workdir");
    assert_eq!(rewritten["workdir"], child.display().to_string());
    let rewritten = rewrite_tool_input(
        "PowerShell",
        json!({"command": "Get-Location"}),
        &root,
        &child,
    )
    .expect("redirect PowerShell cwd");
    assert!(
        rewritten["command"]
            .as_str()
            .is_some_and(|command| command.starts_with("Set-Location -LiteralPath"))
    );
}

#[test]
fn control_responses_are_bounded() {
    let payload = "x".repeat(MAXIMUM_CONTROL_MESSAGE_BYTES);
    let encoded = encode_control_response(&json!({"payload": payload}), None);
    assert!(encoded.len() < MAXIMUM_CONTROL_MESSAGE_BYTES);
    let response: Value = serde_json::from_slice(&encoded).expect("response JSON");
    assert_eq!(response["ok"], false);
    assert!(
        response["error"]
            .as_str()
            .is_some_and(|error| error.contains("4 MiB"))
    );

    let request_id = control_protocol::RequestId::fresh();
    let encoded = control_response_for(&request_id, Ok(json!({"payload": payload})));
    assert!(encoded.len() < MAXIMUM_CONTROL_MESSAGE_BYTES);
    let response: Value = serde_json::from_slice(&encoded).expect("v2 response JSON");
    assert_eq!(response["version"], 2);
    assert_eq!(response["requestId"], request_id.as_str());
    assert_eq!(response["ok"], false);
}

#[test]
fn malformed_v2_requests_keep_their_correlation_identity() {
    let request_id = control_protocol::RequestId::fresh();
    let request = serde_json::to_vec(&json!({
        "requestId": request_id.as_str(),
        "unexpected": true
    }))
    .expect("invalid envelope fixture");
    let error = serde_json::from_slice::<ControlEnvelope<ControlRequest>>(&request)
        .expect_err("fixture must not be a valid envelope");
    let response: Value =
        serde_json::from_slice(&invalid_control_request_response(&request, &error))
            .expect("correlated response");
    assert_eq!(response["version"], 2);
    assert_eq!(response["requestId"], request_id.as_str());
    assert_eq!(response["ok"], false);

    let uncorrelated = br#"{"unexpected":true}"#;
    let error = serde_json::from_slice::<ControlEnvelope<ControlRequest>>(uncorrelated)
        .expect_err("fixture must not be a valid envelope");
    let encoded = invalid_control_request_response(uncorrelated, &error);
    let response: Value = serde_json::from_slice(&encoded).expect("uncorrelated response");
    assert_eq!(response["version"], 2);
    assert!(response.get("requestId").is_none());
    assert!(matches!(
        decode_control_response(&encoded, &request_id),
        Err(ControlRequestError::Indeterminate(_))
    ));
}

#[test]
fn configuration_writes_are_idempotent_and_keep_the_original_backup() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let path = temporary.path().join("host/config.json");
    fs::create_dir_all(path.parent().expect("configuration parent")).expect("create parent");
    fs::write(&path, br#"{"existing":true}"#).expect("original configuration");

    let installed = json!({"existing":true,"mcpServers":{"acyclic":{}}});
    write_json_with_backup(&path, &installed).expect("first write");
    write_json_with_backup(&path, &installed).expect("idempotent retry");
    let backup = path.with_extension("json.acyclic-backup");
    assert_eq!(
        fs::read(&backup).expect("original backup"),
        br#"{"existing":true}"#
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&fs::read(&path).expect("installed config"))
            .expect("valid installed JSON"),
        installed
    );
    assert!(!path.with_extension("acyclic-next").exists());
}

#[test]
fn mcp_configuration_requires_exact_durable_ownership() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let path = temporary.path().join("host/config.json");
    let executable = temporary.path().join("acyclic");
    fs::create_dir_all(path.parent().expect("configuration parent")).expect("create parent");
    fs::write(
        &path,
        serde_json::to_vec(&json!({
            "mcpServers": {"foreign": {"command":"foreign"}}
        }))
        .expect("foreign JSON"),
    )
    .expect("original configuration");

    merge_mcp_config(&path, McpConfigShape::Standard, &executable).expect("install");
    merge_mcp_config(&path, McpConfigShape::Standard, &executable).expect("idempotent install");
    let installed = fs::read(&path).expect("installed configuration");
    let mut modified: Value = serde_json::from_slice(&installed).expect("installed JSON");
    modified["mcpServers"]["acyclic"]["args"] = json!(["foreign"]);
    write_json_with_backup(&path, &modified).expect("user edit");
    assert!(remove_mcp_config(&path, McpConfigShape::Standard).is_err());
    assert_eq!(
        serde_json::from_slice::<Value>(&fs::read(&path).expect("preserved user edit"))
            .expect("preserved JSON"),
        modified
    );
    fs::write(&path, installed).expect("restore exact installed value");
    remove_mcp_config(&path, McpConfigShape::Standard).expect("owned uninstall");
    let restored: Value = serde_json::from_slice(&fs::read(&path).expect("restored configuration"))
        .expect("restored JSON");
    assert!(restored["mcpServers"].get("acyclic").is_none());
    assert_eq!(restored["mcpServers"]["foreign"]["command"], "foreign");

    fs::write(
        &path,
        serde_json::to_vec(&json!({
            "mcpServers": {"acyclic": {"command":"not-ours"}}
        }))
        .expect("foreign Acyclic JSON"),
    )
    .expect("foreign Acyclic configuration");
    assert!(merge_mcp_config(&path, McpConfigShape::Standard, &executable).is_err());
}

#[test]
fn copilot_hooks_restore_prior_content_and_preserve_user_edits() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let path = temporary.path().join(".copilot/hooks/acyclic.json");
    let executable = temporary.path().join("acyclic");
    fs::create_dir_all(path.parent().expect("hook parent")).expect("hook parent");
    let prior = json!({"version":1,"hooks":{"user":[{"exec":"user"}]}});
    fs::write(&path, serde_json::to_vec(&prior).expect("prior JSON")).expect("prior hooks");

    install_copilot_hooks_at(&executable, &path).expect("install hooks");
    install_copilot_hooks_at(&executable, &path).expect("idempotent install");
    let installed = fs::read(&path).expect("installed hooks");
    let mut edited: Value = serde_json::from_slice(&installed).expect("installed JSON");
    edited["hooks"]["sessionStart"][0]["timeout"] = json!(1);
    fs::write(&path, serde_json::to_vec(&edited).expect("edited JSON")).expect("edit hooks");
    assert!(remove_copilot_hooks_at(&path).is_err());
    assert_eq!(
        serde_json::from_slice::<Value>(&fs::read(&path).expect("preserved edit"))
            .expect("preserved JSON"),
        edited
    );

    fs::write(&path, installed).expect("restore installed hooks");
    remove_copilot_hooks_at(&path).expect("owned uninstall");
    assert_eq!(
        serde_json::from_slice::<Value>(&fs::read(&path).expect("restored prior"))
            .expect("restored JSON"),
        prior
    );

    fs::write(&path, b"not JSON").expect("malformed prior hooks");
    assert!(install_copilot_hooks_at(&executable, &path).is_err());
    assert_eq!(
        fs::read(&path).expect("preserved malformed hooks"),
        b"not JSON"
    );
}

#[test]
fn shared_hook_documents_restore_foreign_entries_and_preserve_edits() {
    for section in ["claude-hooks", "cursor-hooks"] {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join("hooks.json");
        let prior = json!({
            "hooks": {
                "foreign": [{"command": format!("foreign __hook {section}")}]
            }
        });
        fs::write(&path, serde_json::to_vec(&prior).expect("prior JSON")).expect("prior hooks");
        install_owned_json(&path, section, "shared hooks", |prior| {
            let mut installed = prior.cloned().expect("prior hook document");
            installed["acyclicOwned"] = json!({"command":"acyclic"});
            Ok(installed)
        })
        .expect("install owned hooks");
        let installed = fs::read(&path).expect("installed hooks");
        let installed_value: Value = serde_json::from_slice(&installed).expect("installed JSON");
        assert_eq!(installed_value["hooks"], prior["hooks"]);

        let mut edited = installed_value;
        edited["acyclicOwned"]["command"] = json!("user-edited");
        fs::write(&path, serde_json::to_vec(&edited).expect("edited JSON")).expect("edit hooks");
        assert!(remove_owned_json(&path, section, "shared hooks").is_err());
        assert_eq!(
            serde_json::from_slice::<Value>(&fs::read(&path).expect("preserved edit"))
                .expect("preserved JSON"),
            edited
        );

        fs::write(&path, installed).expect("restore installed hooks");
        remove_owned_json(&path, section, "shared hooks").expect("owned uninstall");
        assert_eq!(
            serde_json::from_slice::<Value>(&fs::read(&path).expect("restored prior"))
                .expect("restored JSON"),
            prior
        );
    }
}

#[test]
fn claude_hook_reinstall_replaces_stale_acyclic_commands_without_owning_foreign_hooks() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let path = temporary.path().join("settings.json");
    let stale = |event: &str, matcher: bool| {
        let mut group = serde_json::Map::new();
        if matcher {
            group.insert("matcher".to_owned(), json!(".*"));
        }
        group.insert(
            "hooks".to_owned(),
            json!([{
                "type": "command",
                "command": "/deleted/plugin/bin/acyclic",
                "args": ["__hook", "claude-code", event],
                "timeout": claude_hook_timeout(event),
                "statusMessage": "Acyclic is routing the workspace"
            }]),
        );
        Value::Object(group)
    };
    let foreign = json!({
        "hooks": [{"type":"command", "command":"foreign", "timeout":5}]
    });
    let similar_but_foreign = json!({
        "hooks": [{
            "type":"command",
            "command":"acyclic",
            "args":["__hook", "claude-code", "SessionStart"],
            "timeout":120,
            "statusMessage":"different"
        }]
    });
    let prior = json!({
        "theme": "dark",
        "hooks": {
            "SessionStart": [stale("SessionStart", false), foreign, similar_but_foreign],
            "PreToolUse": [stale("PreToolUse", true)],
            "SessionEnd": [stale("SessionEnd", false)]
        }
    });
    fs::write(&path, serde_json::to_vec(&prior).expect("prior JSON")).expect("prior hooks");

    install_owned_json_normalized(
        &path,
        "claude-hooks",
        "Claude Code hooks",
        normalize_claude_hook_document,
        |prior| {
            let mut installed = prior.cloned().expect("normalized prior");
            installed["currentAcyclic"] = json!(true);
            Ok(installed)
        },
    )
    .expect("replace stale hooks");

    let installed: Value =
        serde_json::from_slice(&fs::read(&path).expect("installed hooks")).expect("installed JSON");
    assert_eq!(installed["theme"], "dark");
    assert_eq!(
        installed["hooks"]["SessionStart"],
        json!([foreign, similar_but_foreign])
    );
    assert!(installed["hooks"].get("PreToolUse").is_none());
    assert!(installed["hooks"].get("SessionEnd").is_none());
    assert_eq!(claude_hook_timeout("SessionEnd"), 3);
    assert_eq!(claude_hook_timeout("PreToolUse"), 120);

    remove_owned_json(&path, "claude-hooks", "Claude Code hooks").expect("uninstall current hooks");
    let restored: Value =
        serde_json::from_slice(&fs::read(&path).expect("restored hooks")).expect("restored JSON");
    assert_eq!(restored["theme"], "dark");
    assert_eq!(
        restored["hooks"]["SessionStart"],
        json!([foreign, similar_but_foreign])
    );
    assert!(restored["hooks"].get("PreToolUse").is_none());
    assert!(restored.get("currentAcyclic").is_none());
}

#[test]
fn codex_install_preserves_old_or_disabled_plugin_state() {
    assert!(
        validate_existing_codex_plugin(&json!({
            "version": "0.0.0",
            "enabled": true
        }))
        .is_err()
    );
    assert!(
        validate_existing_codex_plugin(&json!({
            "version": env!("CARGO_PKG_VERSION"),
            "enabled": false
        }))
        .is_err()
    );
    assert!(
        validate_existing_codex_plugin(&json!({
            "version": env!("CARGO_PKG_VERSION"),
            "enabled": true
        }))
        .is_ok()
    );
}

#[test]
fn opencode_guidance_markers_preserve_unrelated_instructions() {
    let original = "# Existing\n\nKeep this.\n";
    let installed =
        format!("{original}\n{OPENCODE_GUIDANCE_START}\nold\n{OPENCODE_GUIDANCE_END}\n");
    assert_eq!(
        remove_marked_block(&installed, OPENCODE_GUIDANCE_START, OPENCODE_GUIDANCE_END),
        original.trim()
    );
    assert_eq!(
        remove_marked_block(original, OPENCODE_GUIDANCE_START, OPENCODE_GUIDANCE_END),
        original
    );
}

#[test]
fn adapter_state_loading_is_byte_and_structure_bounded() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let oversized = temporary.path().join("oversized");
    let file = fs::File::create(&oversized).expect("oversized state");
    file.set_len(MAXIMUM_ADAPTER_STATE_BYTES + ADAPTER_STATE_HEADER_BYTES as u64 + 1)
        .expect("extend oversized state");
    assert!(
        read_state_slot(&oversized)
            .err()
            .expect("oversized state must fail")
            .contains("byte bound")
    );

    let mut state = AdapterState {
        version: ADAPTER_STATE_VERSION,
        ..AdapterState::default()
    };
    for index in 0..=MAXIMUM_ADAPTER_TURNS {
        state.turns.insert(index.to_string(), "root".to_owned());
    }
    assert!(
        validate_state_bounds(&state)
            .expect_err("oversized turn map must fail")
            .contains("too many turns")
    );
    state.turns.clear();
    for index in 0..=MAXIMUM_ADAPTER_ROOT_TURNS {
        state.root_turns.insert(index.to_string());
    }
    assert!(
        validate_state_bounds(&state)
            .expect_err("oversized root turn set must fail")
            .contains("too many root turns")
    );
    state.root_turns.clear();
    state.roots.insert(
        "wrong-key".to_owned(),
        RootBinding {
            root_id: [1; 16],
            path: temporary.path().to_path_buf(),
            repository_workspace_id: [2; 16],
            source_identity: [3; 16],
            source_epoch: 0,
            native_root_identity: [4; 16],
        },
    );
    assert!(
        validate_state_bounds(&state)
            .expect_err("mismatched root map key must fail")
            .contains("typed root identity")
    );
    state.roots.clear();
    state.root_session_id =
        "x".repeat(usize::try_from(MAXIMUM_ADAPTER_STATE_BYTES).expect("state bound fits usize"));
    assert!(
        save_state(temporary.path(), &state, Survives::PowerLoss)
            .expect_err("oversized state persistence must fail")
            .contains("byte bound")
    );
    assert!(
        ADAPTER_STATE_SLOTS
            .iter()
            .all(|slot| !temporary.path().join(slot).exists())
    );
}

#[test]
fn adapter_state_requires_the_current_explicit_schema() {
    let incomplete = serde_json::from_value::<AdapterState>(json!({
        "version": 1,
        "root_session_id": "session",
        "root_agent_id": "agent",
        "root_path": "root",
        "root_workspace_name": "workspace",
        "root_context_id": vec![0; 16],
        "root_id": vec![0; 16],
        "routes": {},
        "turns": {},
        "leases": {},
        "pending": []
    }));
    assert!(
        incomplete.is_err(),
        "state without the current schema must be refused"
    );
    let unsupported = AdapterState {
        version: ADAPTER_STATE_VERSION + 1,
        active: true,
        ..AdapterState::default()
    };
    assert!(validate_state_version(&unsupported).is_err());
}

#[test]
fn adapter_state_loads_the_last_completed_save_despite_any_torn_write() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path();
    let named = |name: &str| AdapterState {
        version: ADAPTER_STATE_VERSION,
        root_session_id: name.to_owned(),
        ..AdapterState::default()
    };
    let loaded = || load_saved_state(data).map(|state| state.root_session_id);
    assert_eq!(loaded(), Ok(String::new()));
    for name in ["first", "second", "third", "fourth"] {
        save_state(data, &named(name), Survives::PowerLoss).expect("adapter state save");
        assert_eq!(loaded().as_deref(), Ok(name));
    }
    let target = data.join(ADAPTER_STATE_SLOTS[0]);
    let written = fs::read(&target).expect("newest slot");
    let mut flipped = written.clone();
    *flipped.last_mut().expect("payload byte") ^= 1;
    let mut stale_tail = written.clone();
    stale_tail.extend_from_slice(b"}}");
    let prefixes = [
        0,
        1,
        ADAPTER_STATE_HEADER_BYTES - 1,
        ADAPTER_STATE_HEADER_BYTES,
        written.len() - 1,
    ]
    .map(|length| written[..length].to_vec());
    for torn in prefixes.into_iter().chain([flipped, stale_tail]) {
        fs::write(&target, torn).expect("torn slot");
        assert_eq!(loaded().as_deref(), Ok("third"));
        save_state(data, &named("fifth"), Survives::PowerLoss).expect("save over the torn slot");
        assert_eq!(loaded().as_deref(), Ok("fifth"));
    }
    for slot in ADAPTER_STATE_SLOTS {
        fs::write(data.join(slot), b"torn").expect("torn slot");
    }
    assert!(
        loaded().is_err(),
        "state without a completed save must fail closed"
    );
}

#[test]
fn unflushed_adapter_saves_never_touch_the_newest_flushed_save() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path();
    let named = |name: &str| AdapterState {
        version: ADAPTER_STATE_VERSION,
        root_session_id: name.to_owned(),
        ..AdapterState::default()
    };
    let loaded = || load_saved_state(data).map(|state| state.root_session_id);
    let flushed_slots = || {
        ADAPTER_STATE_SLOTS[..2]
            .iter()
            .map(|slot| fs::read(data.join(slot)).ok())
            .collect::<Vec<_>>()
    };
    let unflushed = data.join(ADAPTER_STATE_SLOTS[2]);

    // Losing every unflushed save before the first flushed one leaves the
    // state before any save.
    save_state(data, &named("volatile"), Survives::ServiceCrash).expect("unflushed save");
    assert_eq!(loaded().as_deref(), Ok("volatile"));
    fs::write(&unflushed, b"lost").expect("lose unflushed save");
    assert_eq!(loaded(), Ok(String::new()));

    save_state(data, &named("first"), Survives::PowerLoss).expect("flushed save");
    let durable = flushed_slots();
    for name in ["second", "third", "fourth"] {
        save_state(data, &named(name), Survives::ServiceCrash).expect("unflushed save");
        assert_eq!(loaded().as_deref(), Ok(name));
        assert_eq!(flushed_slots(), durable);
    }
    fs::write(&unflushed, b"lost").expect("lose unflushed save");
    assert_eq!(loaded().as_deref(), Ok("first"));

    save_state(data, &named("fifth"), Survives::ServiceCrash).expect("unflushed save");
    save_state(data, &named("sixth"), Survives::PowerLoss).expect("flushed save");
    assert_eq!(
        loaded().as_deref(),
        Ok("sixth"),
        "a later flushed save wins"
    );
    save_state(data, &named("seventh"), Survives::ServiceCrash).expect("unflushed save");
    let intact = fs::read(&unflushed).expect("unflushed slot");
    fs::write(&unflushed, &intact[..intact.len() - 1]).expect("tear unflushed save");
    assert_eq!(loaded().as_deref(), Ok("sixth"));
}

#[test]
fn the_unflushed_slot_entry_is_flushed_until_known_durable() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path();
    let state = AdapterState {
        version: ADAPTER_STATE_VERSION,
        root_session_id: "session".to_owned(),
        ..AdapterState::default()
    };
    let (_, mut slots) = load_state(data).expect("fresh state");
    slots
        .save(data, &state, Survives::ServiceCrash)
        .expect("unflushed save");
    assert!(!slots.durable_entry[2], "a created entry is not durable");
    slots.flush_unflushed(data).expect("first flush");
    assert!(slots.durable_entry[2], "the first flush syncs the entry");
    // Another process cannot tell whether the entry was synced, so it
    // syncs it with its first flush, which loading performs here.
    assert!(!StateSlots::read(data).expect("slots").0.durable_entry[2]);
    let (_, reloaded) = load_state(data).expect("reloaded state");
    assert!(reloaded.durable_entry[2]);
}

#[test]
fn a_session_saves_from_what_it_knows_of_its_slots() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let data = temporary.path();
    let named = |name: &str| AdapterState {
        version: ADAPTER_STATE_VERSION,
        root_session_id: name.to_owned(),
        ..AdapterState::default()
    };
    let loaded = || load_saved_state(data).map(|state| state.root_session_id);
    let on_disk = || StateSlots::read(data).expect("slots").0;
    let (_, mut slots) = load_state(data).expect("fresh state");
    for (index, name) in ["a", "b", "c", "d", "e", "f", "g", "h"]
        .into_iter()
        .enumerate()
    {
        let survives = if index % 3 == 1 {
            Survives::ServiceCrash
        } else {
            Survives::PowerLoss
        };
        slots
            .save(data, &named(name), survives)
            .expect("adapter state save");
        assert_eq!(loaded().as_deref(), Ok(name));
        let read = on_disk();
        assert_eq!(read.flushed, slots.flushed);
        assert_eq!(read.durable_entry, slots.durable_entry);
        assert_eq!(read.last_generation, slots.last_generation);
    }

    // A write that fails stays the target of the next flushed save, so the
    // newest flushed save is never overwritten, and it consumes its
    // generation.
    let newest = data.join(ADAPTER_STATE_SLOTS[slots.newest_flushed()]);
    let target = data.join(ADAPTER_STATE_SLOTS[1 - slots.newest_flushed()]);
    let durable = fs::read(&newest).expect("newest flushed save");
    fs::remove_file(&target).expect("remove target slot");
    fs::create_dir(&target).expect("block target slot");
    let before = slots.last_generation;
    assert!(
        slots
            .save(data, &named("failed"), Survives::PowerLoss)
            .is_err()
    );
    assert_eq!(slots.last_generation, before + 1);
    fs::remove_dir(&target).expect("unblock target slot");
    slots
        .save(data, &named("retried"), Survives::PowerLoss)
        .expect("retried save");
    assert_eq!(fs::read(&newest).expect("newest flushed save"), durable);
    assert_eq!(loaded().as_deref(), Ok("retried"));
    assert_eq!(on_disk().last_generation, before + 2);
}

#[test]
fn doctor_human_output_requires_the_canonical_check_shape() {
    assert!(
        print_doctor_response(&json!({
            "ok": true,
            "checks": [{"name":"service","status":"pass","detail":"ready"}]
        }))
        .is_ok()
    );
    assert!(print_doctor_response(&json!({"ok": false})).is_err());
}

#[test]
fn rpc_line_reader_is_bounded_and_handles_stream_boundaries() {
    let mut input = io::Cursor::new(b"first\r\nsecond\nlast".to_vec());
    assert_eq!(
        read_bounded_rpc_line(&mut input).expect("first frame"),
        Some(b"first".to_vec())
    );
    assert_eq!(
        read_bounded_rpc_line(&mut input).expect("second frame"),
        Some(b"second".to_vec())
    );
    assert_eq!(
        read_bounded_rpc_line(&mut input).expect("final frame"),
        Some(b"last".to_vec())
    );
    assert_eq!(read_bounded_rpc_line(&mut input).expect("eof"), None);

    let mut oversized = io::Cursor::new(vec![b'x'; MAXIMUM_CONTROL_MESSAGE_BYTES + 1]);
    assert!(
        read_bounded_rpc_line(&mut oversized)
            .expect_err("oversized frame must fail closed")
            .contains("maximum frame size")
    );
}

#[test]
fn doctor_requires_an_exact_binary_bound_platform_receipt() {
    let kind = match env::consts::OS {
        "linux" => "linux-fuse",
        "macos" => "macos-nfs",
        "windows" => "windows-projfs",
        _ => return,
    };
    let provider_process_io_observable = !cfg!(windows);
    let canonical = json!({
        "schema":"acyclic-native-mount-qualification-v2",
        "os":env::consts::OS,
        "arch":env::consts::ARCH,
        "coverage":[
            "create-read-write","atomic-save","rename-delete","rename-before-hydration",
            "nested-paths","large-directory-paging",
            "concurrent-handles","watchers","crash-detach-recovery",
            "mount-restoration","hard-links","symbolic-links-reparse-points",
            "metadata","case-behavior","escape-attempts",
            "root-checkout-untouched","git-administration-untouched"
        ],
        "capability":{
            "kind":kind,"available":true,"writable":true,
            "provider_process_io_observable":provider_process_io_observable,
            "session_isolation":"SharedProcess","unavailable_reason":null
        },
        "required_kind":kind,
        "release_version":env!("CARGO_PKG_VERSION"),
        "executable_blake3":"exact-digest",
        "passed":true,
        "cases":[
            {"name":"real-mount-mutation-matrix","status":"passed","elapsed_ms":1,"reason":null},
            {"name":"crash-detach-recovery","status":"passed","elapsed_ms":1,"reason":null},
            {"name":"checkout-and-git-untouched","status":"passed","elapsed_ms":1,"reason":null}
        ]
    });
    assert!(valid_platform_receipt(&canonical, "exact-digest"));
    assert!(!valid_platform_receipt(&canonical, "other-digest"));
    let mut expanded = canonical.clone();
    expanded["coverage"]
        .as_array_mut()
        .expect("coverage")
        .push(json!("future-coverage"));
    expanded["cases"]
        .as_array_mut()
        .expect("cases")
        .push(json!({
            "name":"future-case","status":"passed","elapsed_ms":1,"reason":null
        }));
    assert!(valid_platform_receipt(&expanded, "exact-digest"));
    let mut duplicate_case = expanded.clone();
    duplicate_case["cases"]
        .as_array_mut()
        .expect("cases")
        .push(json!({
            "name":"future-case","status":"passed","elapsed_ms":1,"reason":null
        }));
    assert!(!valid_platform_receipt(&duplicate_case, "exact-digest"));
    expanded["cases"][3]["status"] = json!("failed");
    assert!(!valid_platform_receipt(&expanded, "exact-digest"));
    let mut missing_coverage = canonical.clone();
    missing_coverage["coverage"]
        .as_array_mut()
        .expect("coverage")
        .pop();
    assert!(!valid_platform_receipt(&missing_coverage, "exact-digest"));
    let mut incomplete = canonical.clone();
    incomplete["cases"].as_array_mut().expect("cases").pop();
    assert!(!valid_platform_receipt(&incomplete, "exact-digest"));
    let mut extended = canonical;
    extended["untrusted"] = json!(true);
    assert!(!valid_platform_receipt(&extended, "exact-digest"));
}

#[test]
fn host_lifecycle_arguments_are_exact_before_mutation() {
    let strings = |values: &[&str]| {
        values
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>()
    };

    let install = strings(&["codex", "--project"]);
    assert_eq!(parse_install_arguments(&install), Ok(("codex", true)));
    let detected = strings(&["--detected"]);
    assert_eq!(
        parse_install_arguments(&detected),
        Ok(("--detected", false))
    );
    for invalid in [
        strings(&[]),
        strings(&["--project", "codex"]),
        strings(&["--detected", "--project"]),
        strings(&["codex", "unexpected"]),
        strings(&["codex", "--project", "--project"]),
    ] {
        assert!(parse_install_arguments(&invalid).is_err());
    }

    let uninstall = strings(&["codex", "--purge"]);
    assert_eq!(parse_uninstall_arguments(&uninstall), Ok(("codex", true)));
    for invalid in [
        strings(&[]),
        strings(&["--purge", "codex"]),
        strings(&["codex", "unexpected"]),
        strings(&["codex", "--purge", "--purge"]),
    ] {
        assert!(parse_uninstall_arguments(&invalid).is_err());
    }

    assert_eq!(parse_read_only_arguments(&strings(&[])), Ok(false));
    assert_eq!(parse_read_only_arguments(&strings(&["--json"])), Ok(true));
    for invalid in [
        strings(&["unexpected"]),
        strings(&["--json", "unexpected"]),
        strings(&["--json", "--json"]),
    ] {
        assert!(parse_read_only_arguments(&invalid).is_err());
    }
}

#[test]
fn native_hook_identities_are_nonempty_bounded_and_control_free() {
    assert_eq!(
        hook_id(
            &json!({"session_id":"session-1"}),
            "session_id",
            "sessionId"
        )
        .expect("valid session ID"),
        "session-1"
    );
    for invalid in [String::new(), "line\nbreak".to_owned(), "x".repeat(513)] {
        assert!(validate_hook_id("session_id", invalid).is_err());
    }
}

#[cfg(unix)]
#[test]
fn structured_paths_reject_symlink_escapes() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let child = temporary.path().join("workspace");
    let outside = temporary.path().join("outside");
    fs::create_dir(&child).expect("child directory");
    fs::create_dir(&outside).expect("outside directory");
    symlink(&outside, child.join("escape")).expect("escape symlink");
    assert!(
        validate_tool_paths(
            "Read",
            &json!({"path": child.join("escape/file").display().to_string()}),
            &child
        )
        .is_err()
    );
    let patch = json!({
        "command": "*** Begin Patch\n*** Add File: escape/file\n*** End Patch"
    });
    assert!(rewrite_tool_input("apply_patch", patch, temporary.path(), &child).is_err());
}
