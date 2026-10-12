use super::*;

async fn host_state(service: &ConcurrentServiceControl) -> AdapterState {
    let handle = service.session("session").await.unwrap();
    let resume = handle.pause().await;
    let state = handle.with_snapshot(|snapshot| match snapshot {
        Some(SessionSnapshot::HostManaged(state)) => state.clone(),
        _ => panic!("host-managed session snapshot"),
    }).unwrap();
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
                service
                    .dispatch_request(request, &mut None)
                    .await
                    .unwrap();
            }
            let child = host_state(&service).await.routes["child"]
                .active_path()
                .unwrap();
            let input = json!({"turn_id":"child-tool-turn", "agent_id":"child", "agent_type":"explorer",
                "tool_name":"Bash", "tool_use_id":"tool", "tool_input":{"command":"pwd"}});
            let request =
                native_hook_request(host, "PreToolUse", "session", &fixture.root, input.clone());
            let (routed, _) = service
                .dispatch_request(request, &mut None)
                .await
                .unwrap();
            assert!(
                routed["hookSpecificOutput"]["updatedInput"]["command"]
                    .as_str()
                    .unwrap()
                    .contains(&child.to_string_lossy().replace('\\', "/"))
            );
            if host == "codex" {
                assert_eq!(
                    host_state(&service).await.turns["child-tool-turn"],
                    "child"
                );
            }
            for denied in [
                json!({"turn_id":"forged", "agent_id":"unknown", "agent_type":"explorer",
                    "tool_name":"Bash", "tool_use_id":"forged", "tool_input":{"command":"pwd"}}),
                json!({"turn_id":"child-tool-turn", "agent_id":"child", "agent_type":"explorer", "tool_name":"UnknownWriter",
                    "tool_use_id":"unsupported", "tool_input":{}}),
            ] {
                let request =
                    native_hook_request(host, "PreToolUse", "session", &fixture.root, denied);
                assert!(
                    service
                        .dispatch_request(request, &mut None)
                        .await
                        .is_err()
                );
            }
            let request =
                native_hook_request(host, "PostToolUseFailure", "session", &fixture.root, input);
            service
                .dispatch_request(request, &mut None)
                .await
                .unwrap();
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
                service
                    .dispatch_request(request, &mut None)
                    .await
                    .unwrap();
            }
            assert_eq!(host_state(&service).await.root_context_id, context);
            service.shutdown().await.unwrap();
        }
    });
}
