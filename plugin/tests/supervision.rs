//! Shipping headless entry point over a scripted compatible model endpoint.
use super::*;
use acyclic_harness::supervision::{Action, Kind, Policy};

#[tokio::test]
async fn remote_headless_run_and_json_supervisor_share_file_controls() {
    use acyclic_harness::{
        client_files::{ClientFileChange, FileRoot},
        local::{Connection, agent_conversation::Conversation},
        supervision::{Event, Lifecycle},
    };
    use acyclic_native_runtime::{ProcessPipeReader, ProcessTree};
    use tokio::io::{AsyncBufReadExt as _, BufReader};

    let fixture = fixture();
    let model = model(fixture._directory.path()).await;
    let plugins = task_plugins(&fixture);
    let mut server = graphcoder_command(&fixture);
    server
        .arg("--settings-root")
        .arg(&fixture.settings)
        .arg("--host-state-directory")
        .arg(fixture._directory.path().join("served-state"))
        .arg("--workspace-mounting")
        .arg("physical")
        .arg("--catalog")
        .arg(&model.catalog)
        .arg("--home-file")
        .arg(&plugins)
        .args(["--model", "graphcoder-qual/tiny", "serve", "127.0.0.1:0"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut server = ProcessTree::spawn(&mut server).unwrap();
    let mut ready = BufReader::new(ProcessPipeReader::take_stdout(&mut server).unwrap()).lines();
    let endpoint: Value = serde_json::from_str(&ready.next_line().await.unwrap().unwrap()).unwrap();
    let url = endpoint["endpoint"].as_str().unwrap();
    let credential = endpoint["credential"].as_str().unwrap();
    let options =
        acyclic_harness_client::ConnectionOptions::from_env(Some(url), Some(credential)).unwrap();
    let owner = Connection::connect(&options).await.unwrap();
    let existing =
        acyclic_harness::local::sessions::create(&owner, "existing-supervised-root", None)
            .await
            .unwrap();
    let mut supervisor = graphcoder_command(&fixture);
    supervisor
        .env("ACYCLIC_API_KEY", credential)
        .args(["--endpoint", url, "supervise", "--json"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut supervisor = ProcessTree::spawn(&mut supervisor).unwrap();
    let mut events =
        BufReader::new(ProcessPipeReader::take_stdout(&mut supervisor).unwrap()).lines();

    let goal = fixture._directory.path().join("remote-goal.md");
    let policy_file = fixture._directory.path().join("remote-policy.json");
    std::fs::write(&goal, "Finish this task\n").unwrap();
    let mut policy = Policy {
        no_progress_turns: Some(1),
        ..Default::default()
    };
    policy.actions.insert(Kind::NoProgress, Action::Pause);
    std::fs::write(&policy_file, serde_json::to_vec(&policy).unwrap()).unwrap();
    let mut drifted = graphcoder_command(&fixture);
    drifted
        .env("ACYCLIC_API_KEY", credential)
        .args([
            "--endpoint",
            url,
            "--model",
            "graphcoder-qual/drifted",
            "run",
            "--goal",
        ])
        .arg(&goal)
        .arg("--policy")
        .arg(&policy_file);
    let rejected = capture(drifted, "").await;
    assert!(
        !rejected.status.success(),
        "headless run bypassed the bound execution model"
    );
    assert_eq!(
        model.generated(),
        0,
        "mismatched launch dispatched paid work"
    );
    let mut run = graphcoder_command(&fixture);
    run.env("ACYCLIC_API_KEY", credential)
        .args(["--endpoint", url, "run", "--goal"])
        .arg(&goal)
        .arg("--policy")
        .arg(&policy_file);
    let output = capture(run, "").await;
    assert_eq!(
        output.status.code(),
        Some(22),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut started = false;
    let mut signalled = false;
    loop {
        let event: Event =
            serde_json::from_str(&events.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(event.version, 1);
        match event.event {
            Lifecycle::Started { .. } => started = true,
            Lifecycle::Signal { signal, action } => {
                assert_eq!(signal.kind, Kind::NoProgress);
                assert_eq!(action, Action::Pause);
                signalled = true;
            }
            Lifecycle::Paused {
                signal: Some(Kind::NoProgress),
            } => break,
            _ => {}
        }
    }
    assert!(started && signalled);
    let conversation = Conversation::open(owner.clone());
    conversation
        .publish_files(
            FileRoot::Private {},
            acyclic_harness::OperationId::new(),
            vec![ClientFileChange {
                path: "control/stop".into(),
                expected: None,
                text: Some("stop the paused run".into()),
            }],
        )
        .await
        .unwrap();
    loop {
        let event: Event =
            serde_json::from_str(&events.next_line().await.unwrap().unwrap()).unwrap();
        if matches!(event.event, Lifecycle::Stopped { signal: None }) {
            break;
        }
    }
    for selected in [Some(existing), None] {
        let selected = match selected {
            Some(selected) => selected,
            None => acyclic_harness::local::sessions::create(&owner, "new-supervised-root", None)
                .await
                .unwrap(),
        };
        let agent = selected.root().to_string();
        let mut run = graphcoder_command(&fixture);
        run.env("ACYCLIC_API_KEY", credential)
            .args(["--endpoint", url, "--agent", &agent, "run", "--goal"])
            .arg(&goal)
            .arg("--policy")
            .arg(&policy_file);
        let output = capture(run, "").await;
        assert_eq!(
            output.status.code(),
            Some(22),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut started = false;
        let mut signalled = false;
        loop {
            let event: Event =
                serde_json::from_str(&events.next_line().await.unwrap().unwrap()).unwrap();
            if &event.agent != selected.root() {
                continue;
            }
            match event.event {
                Lifecycle::Started { .. } => started = true,
                Lifecycle::Signal { signal, .. } if signal.kind == Kind::NoProgress => {
                    signalled = true
                }
                Lifecycle::Paused {
                    signal: Some(Kind::NoProgress),
                } => break,
                _ => {}
            }
        }
        assert!(
            started && signalled,
            "independent session was missing from the feed"
        );
    }
    drop(events);
    drop(ready);
    drop(conversation);
    tokio::task::spawn_blocking(move || {
        supervisor.terminate(None).unwrap();
        server.terminate(None).unwrap();
    })
    .await
    .unwrap();
    assert_eq!(model.generated(), 3);
    model.close().await;
}

#[tokio::test]
async fn local_supervisor_attaches_without_model_setup_and_refuses_to_create_an_owner() {
    use acyclic_harness::{
        client_files::{ClientFileChange, FileRoot},
        local::{agent_conversation::Conversation, service::Directory},
        supervision::{Event, Lifecycle},
    };
    use acyclic_native_runtime::{ProcessPipeReader, ProcessTree};
    use tokio::io::{AsyncBufReadExt as _, BufReader};

    let fixture = fixture();
    let state = fixture._directory.path().join("observer-service");
    let mut absent = cli_command(&fixture, acyclic_test_support::cargo_binary!("graphcoder"));
    absent
        .arg("--host-state-directory")
        .arg(&state)
        .args(["supervise", "--json"]);
    let rejected = capture(absent, "").await;
    assert!(!rejected.status.success());
    assert!(
        String::from_utf8_lossy(&rejected.stderr).contains("requires an existing session service")
    );

    let model = model_responses(
        fixture._directory.path(),
        vec![
            ANSWER.into(),
            tool_response(
                "write",
                json!({
                    "path":"volume:///home/GOAL.md",
                    "content":"---\nstatus: done\n---\nCompleted the steered objective\n"
                }),
                0,
            ),
            ANSWER.replace("fixture answer", "Completed the steered objective"),
        ],
        "responses",
    )
    .await;
    let goal = fixture._directory.path().join("observer-goal.md");
    let policy = fixture._directory.path().join("observer-policy.json");
    let plugins = task_plugins(&fixture);
    std::fs::write(&goal, "Finish this task\n").unwrap();
    let mut selected = Policy {
        no_progress_turns: Some(1),
        ..Default::default()
    };
    selected.actions.insert(Kind::NoProgress, Action::Pause);
    std::fs::write(&policy, serde_json::to_vec(&selected).unwrap()).unwrap();
    let run = graphcoder(
        &fixture,
        &model.catalog,
        vec![
            "--home-file",
            &plugins,
            "--host-state-directory",
            state.to_str().unwrap(),
            "run",
            "--goal",
            goal.to_str().unwrap(),
            "--policy",
            policy.to_str().unwrap(),
        ],
        "",
    )
    .await;
    assert_eq!(
        run.status.code(),
        Some(22),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert_eq!(model.generated(), 1);

    let mut command = cli_command(&fixture, acyclic_test_support::cargo_binary!("graphcoder"));
    command
        .arg("--host-state-directory")
        .arg(&state)
        .args(["supervise", "--json"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut supervisor = ProcessTree::spawn(&mut command).unwrap();
    let mut events =
        BufReader::new(ProcessPipeReader::take_stdout(&mut supervisor).unwrap()).lines();
    let mut started = false;
    let paused = loop {
        let event: Event =
            serde_json::from_str(&events.next_line().await.unwrap().unwrap()).unwrap();
        match event.event {
            Lifecycle::Started { .. } => started = true,
            Lifecycle::Paused {
                signal: Some(Kind::NoProgress),
            } => break event.agent,
            _ => {}
        }
    };
    assert!(started);
    let owner = Directory::for_workspace(&fixture.workspace, Some(&state))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let conversation = Conversation::open(owner.select(paused.clone()).await.unwrap());
    conversation
        .publish_files(
            FileRoot::Private {},
            acyclic_harness::OperationId::new(),
            vec![
                ClientFileChange {
                    path: "control/pause".into(),
                    expected: None,
                    text: Some(String::new()),
                },
                ClientFileChange {
                    path: "control/steer.md".into(),
                    expected: None,
                    text: Some("accepted steering".into()),
                },
            ],
        )
        .await
        .unwrap();
    loop {
        let event: Event =
            serde_json::from_str(&events.next_line().await.unwrap().unwrap()).unwrap();
        if event.agent == paused && matches!(event.event, Lifecycle::Paused { signal: None }) {
            break;
        }
    }
    assert_eq!(
        model.generated(),
        1,
        "paused steering dispatched model work"
    );
    let files = conversation
        .files(
            FileRoot::Private {},
            vec!["control/pause".into(), "control/steer.md".into()],
            None,
        )
        .await
        .unwrap();
    conversation
        .publish_files(
            FileRoot::Private {},
            acyclic_harness::OperationId::new(),
            vec![
                ClientFileChange {
                    path: "control/pause".into(),
                    expected: files.files["control/pause"]
                        .as_ref()
                        .unwrap()
                        .original()
                        .cloned(),
                    text: None,
                },
                ClientFileChange {
                    path: "control/steer.md".into(),
                    expected: files.files["control/steer.md"]
                        .as_ref()
                        .unwrap()
                        .original()
                        .cloned(),
                    text: Some("replaced steering".into()),
                },
            ],
        )
        .await
        .unwrap();
    let (mut resumed, mut rejected) = (false, false);
    loop {
        let event: Event =
            serde_json::from_str(&events.next_line().await.unwrap().unwrap()).unwrap();
        if event.agent != paused {
            continue;
        }
        match event.event {
            Lifecycle::Resumed {} => resumed = true,
            Lifecycle::ControlRejected { path } => {
                assert_eq!(path, "control/steer.md");
                rejected = true;
            }
            Lifecycle::TurnDone { .. } => break,
            Lifecycle::Paused { .. } | Lifecycle::Stopped { .. } => {
                panic!("resumed goal was interrupted")
            }
            _ => {}
        }
    }
    assert!(resumed && rejected);
    let goal = conversation
        .files(FileRoot::Home {}, vec!["GOAL.md".into()], None)
        .await
        .unwrap();
    assert!(
        goal.files["GOAL.md"]
            .as_ref()
            .unwrap()
            .text
            .contains("status: done")
    );
    {
        let requests = model.requests.lock().unwrap();
        assert_eq!(requests.len(), 3, "steering or recovery redispatched work");
        let first = requests[0]["input"].as_array().unwrap();
        let steered = requests[1]["input"].as_array().unwrap();
        assert!(
            steered.starts_with(first),
            "steering replaced the original admitted prompt prefix"
        );
        let input = serde_json::to_string(steered).unwrap();
        assert!(input.contains("accepted steering"));
        assert!(!input.contains("replaced steering"));
    }
    conversation
        .publish_files(
            FileRoot::Private {},
            acyclic_harness::OperationId::new(),
            vec![ClientFileChange {
                path: "control/stop".into(),
                expected: None,
                text: Some(String::new()),
            }],
        )
        .await
        .unwrap();
    loop {
        let event: Event =
            serde_json::from_str(&events.next_line().await.unwrap().unwrap()).unwrap();
        if event.agent == paused && matches!(event.event, Lifecycle::Stopped { signal: None }) {
            break;
        }
    }
    drop(events);
    tokio::task::spawn_blocking(move || supervisor.terminate(None).unwrap())
        .await
        .unwrap();
    assert_eq!(
        model.generated(),
        3,
        "observer or stop control dispatched model work"
    );
    model.close().await;
}

async fn run_goal(fixture: &Fixture, model: &Model, policy: &Policy) -> Output {
    let goal = fixture._directory.path().join("goal.md");
    let configuration = fixture._directory.path().join("supervision.json");
    std::fs::write(&goal, "Finish this task\n").unwrap();
    std::fs::write(&configuration, serde_json::to_vec(policy).unwrap()).unwrap();
    let plugins = task_plugins(fixture);
    graphcoder(
        fixture,
        &model.catalog,
        vec![
            "--home-file",
            plugins.as_str(),
            "run",
            "--output-format",
            "jsonl",
            "--goal",
            goal.to_str().unwrap(),
            "--policy",
            configuration.to_str().unwrap(),
        ],
        "",
    )
    .await
}

fn tool_response(name: &str, arguments: Value, call: usize) -> String {
    let event = json!({"type":"response.output_item.done","output_index":0,"item":{
        "type":"function_call","call_id":format!("scripted-call-{call}"),"name":name,
        "arguments":serde_json::to_string(&arguments).unwrap()
    }});
    format!(
        "data: {event}\n\ndata: {}\n\n",
        json!({"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":64,"output_tokens":2}}})
    )
}

async fn terminal_script(
    responses: Vec<String>,
    policy: Policy,
    signal: Kind,
    status: i32,
    calls: usize,
) {
    let fixture = fixture();
    let model = model_responses(fixture._directory.path(), responses, "responses").await;
    let output = run_goal(&fixture, &model, &policy).await;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        output.status.code(),
        Some(status),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(result["version"], 1);
    assert_eq!(result["result"]["kind"], "terminal");
    assert_eq!(result["result"]["signal"], signal.name());
    assert_eq!(
        model.generated(),
        calls,
        "terminal action redispatched work"
    );
    model.close().await;
}

#[tokio::test]
async fn headless_spend_stops_at_the_configured_reservation() {
    let mut policy = Policy::default();
    policy.reservation.tokens = Some(1);
    terminal_script(vec![ANSWER.into()], policy, Kind::Spend, 25, 1).await;
}

#[tokio::test]
async fn headless_configured_morph_chat_preserves_usage_and_process_credentials() {
    const KEY: &str = "fixture-morph-chat-process-key";
    for (credential, measured) in [(false, true), (true, true), (true, false)] {
        let fixture = fixture();
        let authentication = Arc::new(Mutex::new(Vec::new()));
        let observed = authentication.clone();
        let model = model_with(fixture._directory.path(), "chat", move |headers, _, _| {
            observed.lock().unwrap().push(
                headers.get("authorization").is_some_and(|header| {
                    header.as_bytes() == b"Bearer fixture-morph-chat-process-key"
                }),
            );
            let usage = if measured {
                format!("data: {}\n\n", json!({
                    "choices":[], "usage":{"prompt_tokens":64,"completion_tokens":2,"total_tokens":66}
                }))
            } else { String::new() };
            format!(
                "data: {}\n\ndata: {}\n\n{usage}data: [DONE]\n\n",
                json!({"choices":[{"delta":{"content":"Work remains."},"finish_reason":null}]}),
                json!({"choices":[{"delta":{},"finish_reason":"stop"}]}),
            )
        }).await;
        let mut catalog: Value =
            serde_json::from_slice(&std::fs::read(&model.catalog).unwrap()).unwrap();
        catalog["morph"]["authentication"] = json!("api_key");
        std::fs::write(&model.catalog, serde_json::to_vec(&catalog).unwrap()).unwrap();
        let plugins = task_plugins(&fixture);
        let goal = fixture._directory.path().join("morph-goal.md");
        let policy_file = fixture._directory.path().join("morph-policy.json");
        std::fs::write(&goal, "Finish the task with the configured provider.\n").unwrap();
        let mut policy = Policy {
            no_progress_turns: Some(1),
            ..Default::default()
        };
        policy.reservation.tokens = Some(10_000);
        policy.actions.insert(Kind::NoProgress, Action::Stop);
        policy.actions.insert(Kind::Spend, Action::Stop);
        std::fs::write(&policy_file, serde_json::to_vec(&policy).unwrap()).unwrap();
        let mut command = graphcoder_command(&fixture);
        command
            .env_remove("MORPH_API_KEY")
            .arg("--settings-root")
            .arg(&fixture.settings)
            .arg("--catalog")
            .arg(&model.catalog)
            .arg("--home-file")
            .arg(&plugins)
            .args(["--model", "morph/morph-kimik31", "run", "--goal"])
            .arg(&goal)
            .arg("--policy")
            .arg(&policy_file);
        if credential {
            command.env("MORPH_API_KEY", KEY);
        }
        let output = capture(command, "").await;
        if !credential {
            assert!(!output.status.success());
            assert_eq!(
                model.generated(),
                0,
                "missing Morph credential dispatched paid work"
            );
            assert!(
                String::from_utf8_lossy(&output.stderr)
                    .contains("model provider API key is unavailable"),
                "missing Morph credential was not refused before dispatch"
            );
        } else {
            let expected = if measured {
                Kind::NoProgress
            } else {
                Kind::Spend
            };
            assert_eq!(
                output.status.code(),
                Some(if measured { 22 } else { 25 }),
                "configured Morph chat lost original usage supervision: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let result: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["result"]["kind"], "terminal");
            assert_eq!(
                result["result"]["signal"],
                expected.name(),
                "missing Chat usage became a measured zero or complete usage was lost"
            );
            assert_eq!(authentication.lock().unwrap().as_slice(), &[true]);
            let requests = model.requests.lock().unwrap();
            assert_eq!(
                requests.len(),
                1,
                "Morph terminal signal redispatched paid work"
            );
            assert_eq!(requests[0]["model"], "morph-kimik31");
            assert!(requests[0]["messages"].is_array());
            assert!(
                requests[0].get("input").is_none(),
                "configured Chat used the Responses wire shape"
            );
            assert_eq!(requests[0]["stream_options"]["include_usage"], true);
        }
        assert_no_fixture_credentials(&fixture, &output, &[KEY]);
        model.close().await;
    }
}

#[tokio::test]
async fn headless_repeated_model_output_stops_across_goal_turns() {
    terminal_script(
        vec![ANSWER.into()],
        Policy::default(),
        Kind::LoopOutput,
        24,
        2,
    )
    .await;
}

#[tokio::test]
async fn headless_identical_tool_errors_obey_the_selected_signal_action() {
    let arguments = json!({
        "path":"volume:///home/GOAL.md", "old_text":"absent original text", "new_text":"replacement"
    });
    let mut policy = Policy::default();
    policy.actions.insert(Kind::RepeatFailure, Action::Stop);
    policy.thresholds.loop_output = None;
    let responses = (0..2)
        .map(|call| tool_response("edit", arguments.clone(), call))
        .collect();
    terminal_script(responses, policy, Kind::RepeatFailure, 20, 2).await;
}

#[tokio::test]
async fn headless_repeated_selected_checks_pause_before_another_execution() {
    let mut policy = Policy::default();
    policy.validation_commands.insert("echo checked".into());
    policy.thresholds.loop_output = None;
    let responses = (0..3)
        .map(|call| tool_response("shell", json!({"command":"echo checked"}), call))
        .collect();
    terminal_script(responses, policy, Kind::ValidationLoop, 21, 3).await;
}

#[cfg(unix)]
#[tokio::test]
async fn headless_quiet_tool_stops_from_its_original_actor_alarm() {
    let mut policy = Policy {
        quiet_ms: Some(1),
        ..Default::default()
    };
    policy.actions.insert(Kind::Stuck, Action::Stop);
    let response = tool_response("shell", json!({"command":"mkfifo hold; cat hold"}), 0);
    terminal_script(vec![response], policy, Kind::Stuck, 23, 1).await;
}

#[tokio::test]
async fn headless_no_progress_uses_the_same_terminal_signal_status_for_pause_and_stop() {
    for action in [Action::Pause, Action::Stop] {
        let fixture = fixture();
        let model = model(fixture._directory.path()).await;
        let mut policy = Policy {
            no_progress_turns: Some(1),
            ..Default::default()
        };
        policy.actions.insert(Kind::NoProgress, action);
        let output = run_goal(&fixture, &model, &policy).await;
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert_eq!(
            output.status.code(),
            Some(22),
            "{stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result: Value = serde_json::from_str(stdout.trim()).unwrap();
        assert_eq!(result["version"], 1);
        assert_eq!(result["result"]["kind"], "terminal");
        assert_eq!(result["result"]["signal"], "no_progress");
        assert_eq!(
            result["result"]["action"],
            serde_json::to_value(action).unwrap()
        );
        assert_eq!(
            model.generated(),
            1,
            "terminal policy admitted another model call"
        );
        model.close().await;
    }
}

#[tokio::test]
async fn headless_original_goal_change_is_healthy_under_no_progress_supervision() {
    let completed = "---\nstatus: done\n---\nCompleted the objective\n";
    for (name, arguments) in [
        (
            "edit",
            json!({"path":"volume:///home/GOAL.md","old_text":"Finish this task\n","new_text":completed}),
        ),
        (
            "write",
            json!({"path":"volume:///home/GOAL.md","content":completed}),
        ),
    ] {
        let fixture = fixture();
        let first = tool_response(name, arguments, 0);
        let model = model_responses(
            fixture._directory.path(),
            vec![first, ANSWER.into()],
            "responses",
        )
        .await;
        let mut policy = Policy {
            no_progress_turns: Some(1),
            ..Default::default()
        };
        policy.actions.insert(Kind::NoProgress, Action::Stop);
        policy.reservation.tokens = Some(10_000);
        let output = run_goal(&fixture, &model, &policy).await;
        let stdout = succeeded(&output);
        let result: Value = serde_json::from_str(stdout.trim()).unwrap();
        assert_eq!(result["version"], 1);
        assert_eq!(result["result"]["kind"], "goal");
        assert_eq!(result["result"]["goal"]["status"], "done");
        assert_eq!(
            model.generated(),
            2,
            "goal completion did not settle its original {name} publication"
        );
        model.close().await;
    }
}

#[tokio::test]
async fn headless_native_edits_count_progress_but_unchanged_rewrites_do_not() {
    async fn scenario(changed: bool, earlier_edit: bool) {
        let fixture = fixture();
        std::fs::write(fixture.workspace.join("progress.txt"), b"before").unwrap();
        let content = if changed { "after!" } else { "before" };
        #[cfg(unix)]
        let command = format!("printf '{content}' > progress.txt; cat progress.txt");
        #[cfg(windows)]
        let command = format!(
            "Set-Content -NoNewline -Path progress.txt -Value '{content}'; Get-Content -Raw progress.txt"
        );
        let mut responses = Vec::new();
        if earlier_edit {
            // A subsequent clean receipt must not inherit this edit's DAG
            // parent and repeatedly claim the same content change.
            responses.extend([
                tool_response("shell", json!({"command":command}), 0),
                ANSWER.into(),
            ]);
        }
        #[cfg(unix)]
        let clean_command = "cat progress.txt";
        #[cfg(windows)]
        let clean_command = "Get-Content -Raw progress.txt";
        responses.extend([
            tool_response("shell", json!({"command":if earlier_edit { clean_command } else { &command }}), usize::from(earlier_edit)),
            ANSWER.into(),
            tool_response("write", json!({"path":"volume:///home/GOAL.md", "content":"---\nstatus: done\n---\nCompleted the objective\n"}), 1 + usize::from(earlier_edit)),
            ANSWER.into(),
        ]);
        let model = model_responses(fixture._directory.path(), responses, "responses").await;
        let mut policy = Policy {
            no_progress_turns: Some(1),
            ..Default::default()
        };
        policy.actions.insert(Kind::NoProgress, Action::Stop);
        policy.reservation.tokens = Some(10_000);
        let output = run_goal(&fixture, &model, &policy).await;
        let requests = model.requests.lock().unwrap().clone();
        model.close().await;
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert_eq!(
            output.status.code(),
            Some(if changed && !earlier_edit { 0 } else { 22 }),
            "native changed={changed} earlier_edit={earlier_edit}: {stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result: Value = serde_json::from_str(stdout.trim()).unwrap();
        assert_eq!(
            requests.len(),
            if changed { 4 } else { 2 },
            "native capture progress dispatched the wrong number of paid requests"
        );
        for request in [1, 3].into_iter().take(if earlier_edit { 2 } else { 1 }) {
            let native = requests[request]["input"]
                .as_array()
                .unwrap()
                .iter()
                .rev()
                .find(|item| item["type"] == "function_call_output")
                .expect("original native result was not delivered");
            let native: Value = serde_json::from_str(native["output"].as_str().unwrap()).unwrap();
            assert_eq!(native["success"], true, "native command failed: {native}");
            assert_eq!(native["exit_code"], 0, "native command failed: {native}");
        }
        if changed && !earlier_edit {
            assert_eq!(result["result"]["kind"], "goal");
            assert_eq!(result["result"]["goal"]["status"], "done");
        } else {
            assert_eq!(result["result"]["kind"], "terminal");
            assert_eq!(result["result"]["signal"], "no_progress");
        }
    }
    tokio::join!(
        scenario(true, false),
        scenario(false, false),
        scenario(true, true)
    );
}

#[tokio::test]
async fn healthy_headless_run_keeps_below_configured_supervision_thresholds() {
    let fixture = fixture();
    let model = model_responses(
        fixture._directory.path(),
        vec![
            tool_response("edit", json!({
                "path":"volume:///home/GOAL.md", "old_text":"absent original text", "new_text":"replacement"
            }), 0),
            tool_response("shell", json!({"command":"echo checked"}), 1),
            tool_response("shell", json!({"command":"echo checked"}), 2),
            tool_response("write", json!({
                "path":"volume:///home/GOAL.md", "content":"---\nstatus: done\n---\nCompleted the objective\n"
            }), 3),
            ANSWER.into(),
        ],
        "responses",
    ).await;
    let mut policy = Policy {
        no_progress_turns: Some(1),
        quiet_ms: Some(60_000),
        ..Default::default()
    };
    policy.validation_commands.insert("echo checked".into());
    policy.reservation.tokens = Some(10_000);
    for kind in [
        Kind::RepeatFailure,
        Kind::ValidationLoop,
        Kind::NoProgress,
        Kind::Stuck,
        Kind::LoopOutput,
        Kind::Spend,
    ] {
        policy.actions.insert(kind, Action::Stop);
    }
    let output = run_goal(&fixture, &model, &policy).await;
    let result: Value = serde_json::from_str(succeeded(&output).trim()).unwrap();
    assert_eq!(result["result"]["goal"]["status"], "done");
    assert_eq!(
        model.generated(),
        5,
        "a healthy run was interrupted or redispatched"
    );
    model.close().await;
}

#[tokio::test]
async fn headless_role_models_and_mixed_catalog_use_only_process_credentials() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    const ROOT_KEY: &str = "fixture-root-process-credential";
    const WORKER_KEY: &str = "fixture-worker-process-credential";
    let fixture = fixture();
    let plugins = task_plugins(&fixture);
    let root_selection = acyclic_harness::templates::root()
        .unwrap()
        .model
        .unwrap_or_else(|| "graphcoder-qual/tiny".into());
    let (root_provider, root_model) = root_selection.split_once('/').unwrap();
    let root_environment = format!(
        "{}_API_KEY",
        root_provider
            .chars()
            .map(|character| if character.is_ascii_alphanumeric() {
                character.to_ascii_uppercase()
            } else {
                '_'
            })
            .collect::<String>()
    );
    let role = "---\nversion: 1\ncontext: fork\nmay_spawn: []\nmodel: alternate/drifted\n---\nComplete the assigned audit.\n";
    let script = vec![
        tool_response(
            "write",
            json!({"path":"volume:///home/templates/worker/AGENT.md","content":role}),
            0,
        ),
        tool_response(
            "spawn",
            json!({"name":"audit","task":"Audit the supplied brief and report.","role":"worker"}),
            1,
        ),
        tool_response("wait", json!({"agents":["audit"]}), 2),
        tool_response(
            "write",
            json!({"path":"volume:///home/GOAL.md","content":"---\nstatus: done\n---\nCompleted the audit.\n"}),
            3,
        ),
        ANSWER.into(),
    ];
    let roots = Arc::new(AtomicUsize::new(0));
    let authentication = Arc::new(Mutex::new(Vec::new()));
    let observed_authentication = authentication.clone();
    let scripted_root = root_model.to_owned();
    let model = model_with(
        fixture._directory.path(),
        "responses",
        move |headers, body, _| {
            let worker = body["model"] == "drifted";
            let expected = format!("Bearer {}", if worker { WORKER_KEY } else { ROOT_KEY });
            let authenticated = headers
                .get("authorization")
                .is_some_and(|header| header.as_bytes() == expected.as_bytes());
            observed_authentication.lock().unwrap().push((
                body["model"].as_str().unwrap_or("").to_owned(),
                authenticated,
            ));
            if worker || body["model"] != scripted_root {
                ANSWER.into()
            } else {
                let index = roots.fetch_add(1, Ordering::SeqCst);
                script[index.min(script.len() - 1)].clone()
            }
        },
    )
    .await;
    let mut catalog: Value =
        serde_json::from_slice(&std::fs::read(&model.catalog).unwrap()).unwrap();
    catalog[root_provider] = catalog["graphcoder-qual"].clone();
    catalog[root_provider]["authentication"] = json!("api_key");
    catalog[root_provider]["models"][root_model] =
        catalog["graphcoder-qual"]["models"]["tiny"].clone();
    catalog["alternate"] = catalog["graphcoder-qual"].clone();
    catalog["alternate"]["authentication"] = json!("api_key");
    std::fs::write(&model.catalog, serde_json::to_vec(&catalog).unwrap()).unwrap();
    std::fs::write(
        fixture.settings.join(".codex/config.toml"),
        "model = 'tiny'\nmodel_provider = 'graphcoder-qual'\n",
    )
    .unwrap();
    let goal = fixture._directory.path().join("goal.md");
    let policy = fixture._directory.path().join("supervision.json");
    std::fs::write(&goal, "Complete the audit.\n").unwrap();
    std::fs::write(&policy, serde_json::to_vec(&Policy::default()).unwrap()).unwrap();
    let mut command = graphcoder_command(&fixture);
    command
        .env(&root_environment, ROOT_KEY)
        .env("ALTERNATE_API_KEY", WORKER_KEY)
        .arg("--settings-root")
        .arg(&fixture.settings)
        .arg("--catalog")
        .arg(&model.catalog)
        .arg("--home-file")
        .arg(&plugins)
        .arg("run")
        .arg("--goal")
        .arg(&goal)
        .arg("--policy")
        .arg(&policy);
    let output = capture(command, "").await;
    {
        let authentication = authentication.lock().unwrap();
        assert!(
            authentication
                .iter()
                .all(|(model, accepted)| (model == root_model || model == "drifted") && *accepted),
            "configured provider model or process credential differed: {authentication:?}"
        );
    }
    let stdout = succeeded(&output);
    let result: Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(result["result"]["goal"]["status"], "done");
    let requests = model.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["model"] == root_model)
            .count(),
        5
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["model"] == "drifted")
            .count(),
        1
    );
    drop(requests);
    assert_no_fixture_credentials(&fixture, &output, &[ROOT_KEY, WORKER_KEY]);
    model.close().await;
}

fn assert_no_fixture_credentials(fixture: &Fixture, output: &Output, keys: &[&str]) {
    for key in keys {
        assert!(
            !output
                .stdout
                .windows(key.len())
                .any(|bytes| bytes == key.as_bytes())
        );
        assert!(
            !output
                .stderr
                .windows(key.len())
                .any(|bytes| bytes == key.as_bytes())
        );
    }
    let mut directories = vec![fixture._directory.path().to_owned()];
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                directories.push(entry.path());
            } else if kind.is_file() {
                let bytes = std::fs::read(entry.path()).unwrap();
                for key in keys {
                    assert!(
                        !bytes
                            .windows(key.len())
                            .any(|bytes| bytes == key.as_bytes()),
                        "process credential entered a persisted artifact"
                    );
                }
            }
        }
    }
}
