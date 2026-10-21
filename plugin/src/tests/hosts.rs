use super::*;

async fn host_state(service: &ConcurrentServiceControl) -> AdapterState {
    let handle = service.session("session").await.unwrap();
    let resume = handle.pause().await;
    let state = handle
        .with_snapshot(|snapshot| match snapshot {
            Some(SessionSnapshot::HostManaged(state)) => *state.clone(),
            _ => panic!("host-managed session snapshot"),
        })
        .unwrap();
    resume.send(()).unwrap();
    state
}

#[test]
fn native_hosts_route_children_and_reject_forged_identity() {
    run_large_stack("host-routing", || async {
        for (host, spawn_tool) in [
            ("codex", "collaborationspawn_agent"),
            ("claude-code", "Agent"),
        ] {
            let fixture = Fixture::new();
            let (_lock, service) = test_concurrent_service(fixture.data.clone()).await.unwrap();
            for (event, extra) in [
                ("SessionStart", json!({})),
                ("UserPromptSubmit", json!({"turn_id":"root-turn"})),
                (
                    "PreToolUse",
                    json!({"turn_id":"root-turn", "tool_name":spawn_tool,
                    "tool_use_id":"spawn", "tool_input":{}}),
                ),
                (
                    "SubagentStart",
                    json!({"turn_id":"child-turn", "agent_id":"child", "agent_type":"explorer"}),
                ),
            ] {
                let request = native_hook_request(host, event, "session", &fixture.root, extra);
                service.dispatch_request(request, &mut None).await.unwrap();
            }
            let child = host_state(&service).await.routes["child"]
                .active_path()
                .unwrap();
            let input = json!({"turn_id":"child-tool-turn", "agent_id":"child", "agent_type":"explorer",
                "tool_name":"Bash", "tool_use_id":"tool", "tool_input":{"command":"pwd"}});
            let request =
                native_hook_request(host, "PreToolUse", "session", &fixture.root, input.clone());
            let (routed, _) = service.dispatch_request(request, &mut None).await.unwrap();
            assert!(
                routed["hookSpecificOutput"]["updatedInput"]["command"]
                    .as_str()
                    .unwrap()
                    .contains(&child.to_string_lossy().replace('\\', "/"))
            );
            if host == "codex" {
                assert_eq!(host_state(&service).await.turns["child-tool-turn"], "child");
            }
            for denied in [
                json!({"turn_id":"forged", "agent_id":"unknown", "agent_type":"explorer",
                    "tool_name":"Bash", "tool_use_id":"forged", "tool_input":{"command":"pwd"}}),
                json!({"turn_id":"child-tool-turn", "agent_id":"child", "agent_type":"explorer", "tool_name":"UnknownWriter",
                    "tool_use_id":"unsupported", "tool_input":{}}),
            ] {
                let request =
                    native_hook_request(host, "PreToolUse", "session", &fixture.root, denied);
                assert!(service.dispatch_request(request, &mut None).await.is_err());
            }
            let request =
                native_hook_request(host, "PostToolUseFailure", "session", &fixture.root, input);
            service.dispatch_request(request, &mut None).await.unwrap();
            let context = host_state(&service).await.root_context_id;
            for (event, cwd, extra) in [
                (
                    "SubagentStop",
                    &child,
                    json!({"turn_id":"child-turn","agent_id":"child"}),
                ),
                (
                    "SubagentStop",
                    &child,
                    json!({"turn_id":"child-turn","agent_id":"child"}),
                ),
                (
                    "SubagentStart",
                    &fixture.root,
                    json!({"turn_id":"child-turn","agent_id":"child", "agent_type":"explorer"}),
                ),
                ("SessionStart", &fixture.root, json!({"source":"compact"})),
                ("SessionEnd", &fixture.root, json!({})),
                ("SessionStart", &fixture.root, json!({"source":"resume"})),
            ] {
                let request = native_hook_request(host, event, "session", cwd, extra);
                service.dispatch_request(request, &mut None).await.unwrap();
            }
            assert_eq!(host_state(&service).await.root_context_id, context);
            let _ = service.shutdown().await.unwrap();
        }
    });
}

#[test]
fn concurrent_spawns_from_one_fork_point_start_in_any_order() {
    run_large_stack("concurrent-spawns", || async {
        let fixture = Fixture::new();
        let (_lock, service) = test_concurrent_service(fixture.data.clone()).await.unwrap();
        let hook = |event: &str, extra: Value| {
            native_hook_request("claude-code", event, "session", &fixture.root, extra)
        };
        let spawn = |id: &str| {
            hook(
                "PreToolUse",
                json!({"turn_id":"root-turn", "tool_name":"Agent", "tool_use_id":id, "tool_input":{}}),
            )
        };
        for request in [
            hook("SessionStart", json!({})),
            hook("UserPromptSubmit", json!({"turn_id":"root-turn"})),
            spawn("left"),
            spawn("right"),
        ] {
            service.dispatch_request(request, &mut None).await.unwrap();
        }
        assert_eq!(host_state(&service).await.pending.len(), 2);
        // The host starts the children in either order, naming neither call.
        for child in ["second", "first"] {
            let start = hook(
                "SubagentStart",
                json!({"turn_id":format!("{child}-turn"), "agent_id":child, "agent_type":"general-purpose"}),
            );
            service.dispatch_request(start, &mut None).await.unwrap();
        }
        let state = host_state(&service).await;
        assert!(state.pending.is_empty());
        assert_ne!(
            state.routes["first"].mount_path,
            state.routes["second"].mount_path
        );
        let _ = service.shutdown().await.unwrap();
    });
}

#[test]
fn only_spawns_of_one_fork_point_are_interchangeable() {
    let prepared = |fork_key: u8| PendingSpawn {
        task_binding: PluginTaskBinding::HostManaged,
        agent_id: None,
        parent_agent_id: "root".to_owned(),
        tool_use_id: format!("spawn-{fork_key}"),
        active_root_id: Some([1; 16]),
        expires_at_millis: u64::MAX,
        workspace_name: format!("agent-{fork_key}"),
        fork_key: [fork_key; 16],
        roots: BTreeMap::new(),
        fork_generations: BTreeMap::from([("root".to_owned(), [7; 32])]),
        mount_path: PathBuf::from(format!("mount-{fork_key}")),
        lifecycle: PendingSpawnLifecycle::Prepared,
    };
    let spawn = prepared(1);
    assert!(spawn.interchangeable_with(&prepared(2)));
    let variants: [fn(&mut PendingSpawn); 6] = [
        |other| other.fork_generations = BTreeMap::from([("root".to_owned(), [8; 32])]),
        |other| other.fork_generations.clear(),
        |other| other.parent_agent_id = "child".to_owned(),
        |other| other.active_root_id = Some([2; 16]),
        |other| other.agent_id = Some("alias".to_owned()),
        |other| other.lifecycle = PendingSpawnLifecycle::Mounting,
    ];
    for change in variants {
        let mut other = prepared(2);
        change(&mut other);
        assert!(!spawn.interchangeable_with(&other));
        assert!(!other.interchangeable_with(&spawn));
    }
}
