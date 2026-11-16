use super::*;
use proptest::prelude::*;
use serde_json::json;

mod failures;
mod output_credit;

#[tokio::test]
async fn early_stdin_closure_preserves_a_deny_with_a_large_tool_payload() -> Result<()> {
    let mut hooks = Hooks::default();
    #[cfg(windows)]
    let (command, shell) = (
        r#"echo {"hookSpecificOutput":{"permissionDecision":"deny","permissionDecisionReason":"guard denied"}}"#,
        "cmd",
    );
    #[cfg(not(windows))]
    let (command, shell) = (
        r#"exec 0<&-; printf '%s' '{"hookSpecificOutput":{"permissionDecision":"deny","permissionDecisionReason":"guard denied"}}'"#,
        "sh",
    );
    hooks.add_rules([HookRule {
        on: HookEvent::PreToolUse,
        matcher: String::new(),
        hooks: vec![HookSpec {
            run: HookRun::Shell {
                command: command.into(),
                shell: Some(shell.into()),
            },
            timeout_ms: Some(5_000),
            failure: HookFailure::Report,
        }],
    }])?;
    let root = std::env::current_dir().map_err(|error| Error::Invalid(error.to_string()))?;
    let output = hooks
        .run(
            HookEvent::PreToolUse,
            &root,
            &root,
            &json!({"tool_name":"shell", "tool_input":{"payload":"x".repeat(1_048_576)}}),
            &registry()?,
        )
        .await?;
    assert_eq!(output.blocked.as_deref(), Some("guard denied"));
    assert!(output.diagnostics.is_empty(), "{output:?}");
    Ok(())
}

fn command(hook: &HookSpec) -> &str {
    match &hook.run {
        HookRun::Executable { executable, .. } => executable,
        HookRun::Shell { command, .. } => command,
        HookRun::Plugin { instance, .. } => instance,
    }
}

#[tokio::test]
async fn exec_form_placeholders_launch_guards_without_retokenizing_paths_or_arguments() -> Result<()>
{
    if let Some(argument) = std::env::args()
        .find_map(|argument| argument.strip_prefix("hook-exec-child:").map(str::to_owned))
    {
        let root = crate::native_environment::variable("CLAUDE_PROJECT_DIR")
            .and_then(|value| value.into_string().ok())
            .expect("project root environment");
        if argument != format!("{root}/payload with ' $ ` spaces") {
            eprintln!("guard argument was changed");
            std::process::exit(3);
        }
        eprintln!("expanded guard denied");
        std::process::exit(2);
    }
    let executable = std::env::current_exe().map_err(|error| Error::Invalid(error.to_string()))?;
    let fixture = tempfile::Builder::new()
        .prefix("hook ' $ ` space ")
        .tempdir()
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let root = fixture.path();
    let guard = root.join("guard.exe");
    // A link keeps the guard this executable without copying it; a hard link would
    // need the temporary directory on the executable's volume.
    #[cfg(unix)]
    std::os::unix::fs::symlink(&executable, &guard)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    #[cfg(windows)]
    std::fs::copy(&executable, &guard).map_err(|error| Error::Invalid(error.to_string()))?;
    let cwd = root.join("tool-cwd");
    std::fs::create_dir(&cwd).map_err(|error| Error::Invalid(error.to_string()))?;
    for (command, argument) in [
        (
            "${CLAUDE_PROJECT_DIR}/guard.exe".to_owned(),
            format!(
                "hook-exec-child:{}/payload with ' $ ` spaces",
                root.display()
            ),
        ),
        (
            guard.to_string_lossy().into_owned(),
            "hook-exec-child:${CLAUDE_PROJECT_DIR}/payload with ' $ ` spaces".into(),
        ),
    ] {
        let mut hooks = Hooks::default();
        hooks.add_json(json!({"hooks":{"PreToolUse":[{"hooks":[{
            "type":"command", "command":command, "shell":"bash", "args":[
                "--exact", "hooks::tests::exec_form_placeholders_launch_guards_without_retokenizing_paths_or_arguments",
                "--nocapture", "--skip", argument
            ]
        }]}]}}).to_string().as_bytes())?;
        let output = hooks
            .run(
                HookEvent::PreToolUse,
                root,
                &cwd,
                &json!({"tool_name":"shell"}),
                &registry()?,
            )
            .await?;
        assert_eq!(
            output.blocked.as_deref(),
            Some("expanded guard denied\n"),
            "{output:?}"
        );
        assert!(output.diagnostics.is_empty(), "{output:?}");
    }
    Ok(())
}

#[test]
fn exec_form_ignores_the_shell_setting_including_empty_arguments() -> Result<()> {
    for shell in ["bash", "powershell", "not-a-shell"] {
        for args in [vec![], vec!["one argument with spaces"]] {
            let mut hooks = Hooks::default();
            hooks.add_json(
                json!({"hooks":{"Stop":[{"hooks":[{
                    "type":"command", "command":"guard", "args":args, "shell":shell
                }]}]}})
                .to_string()
                .as_bytes(),
            )?;
            let registry = registry()?;
            let input = json!({});
            let rule = hooks
                .commands(HookEvent::Stop, &input, &registry)?
                .next()
                .expect("hook");
            assert_eq!(
                rule.run,
                HookRun::Executable {
                    executable: "guard".into(),
                    argv: args.into_iter().map(str::to_owned).collect()
                }
            );
        }
    }
    Ok(())
}

#[test]
fn bracket_prefixed_session_notes_remain_context_under_both_failure_policies() {
    for policy in [HookFailure::Report, HookFailure::Block] {
        for text in [
            "[Project notes]\nRun tests before editing",
            "[unfinished note",
            "[",
        ] {
            let mut output = HookOutput::default();
            output.apply(
                HookEvent::SessionStart,
                policy,
                Ok((Some(0), text.as_bytes().to_vec(), vec![])),
            );
            assert_eq!(output.additional_context, [text]);
            assert!(output.diagnostics.is_empty(), "{output:?}");
            assert!(output.blocked.is_none(), "{output:?}");
        }
        let mut guard = HookOutput::default();
        guard.apply(
            HookEvent::PreToolUse,
            policy,
            Ok((Some(0), br#"{"decision":"block""#.to_vec(), vec![])),
        );
        assert_eq!(guard.diagnostics.len(), 1);
        assert_eq!(guard.blocks(), policy == HookFailure::Block);
    }
}

#[test]
fn claude_json_and_codex_toml_are_additive_and_match_standard_events() -> Result<()> {
    let mut hooks = Hooks::default();
    hooks.add_json(br#"{"hooks":{
        "SessionStart":[{"matcher":"startup|resume","hooks":[{"type":"command","command":"start"}]}],
        "PreToolUse":[{"matcher":"^(write|edit)$","hooks":[{"type":"command","command":"check","timeout":30}]}],
        "Stop":[{"matcher":"ignored[","hooks":[{"type":"command","command":"stop"}]}]
    }}"#)?;
    hooks.add_toml(
        r#"
        [[hooks.PreToolUse]]
        matcher = "write"
        [[hooks.PreToolUse.hooks]]
        type = "command"
        command = "format"
    "#,
    )?;
    let registry = registry()?;
    let input = json!({"tool_name":"write","source":"resume"});
    assert_eq!(
        hooks
            .commands(HookEvent::SessionStart, &input, &registry)?
            .count(),
        1
    );
    assert_eq!(
        hooks
            .commands(HookEvent::PreToolUse, &input, &registry)?
            .map(|hook| command(hook))
            .collect::<Vec<_>>(),
        ["check", "format"]
    );
    assert_eq!(
        hooks.commands(HookEvent::Stop, &input, &registry)?.count(),
        1
    );
    assert_eq!(
        hooks
            .commands(
                HookEvent::PreToolUse,
                &json!({"tool_name":"read"}),
                &registry
            )?
            .count(),
        0
    );
    assert_eq!(
        hooks
            .commands(HookEvent::PostToolUse, &input, &registry)?
            .count(),
        0
    );
    Ok(())
}

#[tokio::test]
async fn cancelling_the_hook_future_terminates_its_running_child() -> Result<()> {
    use super::native::run_hook;
    let process_error = Error::process_io;
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(process_error)?;
    let address = listener.local_addr().map_err(process_error)?;
    let hook = HookSpec {
        run: HookRun::Executable {
            executable: std::env::current_exe().map_err(process_error)?.to_string_lossy().into_owned(),
            argv: vec!["--exact".into(), "hooks::tests::native_hooks_default_user_guards_and_project_trust_use_one_shared_script".into(),
                "--nocapture".into(), "--skip".into(), format!("hook-child:{address}")],
        },
        timeout_ms: None,
        failure: HookFailure::Report,
    };
    let root = std::env::current_dir().map_err(process_error)?;
    let mut future = Box::pin(run_hook(
        Arc::new(hook),
        (root.clone(), root, vec![]),
        Arc::new(|| Ok(())),
        Some(65_536),
        None,
        None,
    ));
    let (mut stream, _) = tokio::select! {
        result = listener.accept() => result.map_err(process_error)?,
        result = &mut future => return Err(Error::Invalid(format!("hook exited before handshake: {result:?}"))),
    };
    assert_eq!(stream.read_u8().await.map_err(process_error)?, 1);
    drop(future);
    let mut bytes = Vec::new();
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        stream.read_to_end(&mut bytes),
    )
    .await
    .map_err(|_| Error::Invalid("cancelled hook process stayed alive".into()))?;
    match result {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
        Err(error) => return Err(process_error(error)),
    }
    assert!(bytes.is_empty());
    Ok(())
}


#[test]
fn invalid_hook_replacement_is_atomic() -> Result<()> {
    let mut hooks = Hooks::default();
    for document in [
        json!({"hooks":{"PreToolUse":[{"matcher":"[","hooks":[]}]}}),
        json!({"hooks":{"Stop":[{"hooks":[{"type":"command","command":""}]}]}}),
        json!({"hooks":{"Stop":[{"hooks":[{"type":"command","command":"test","timeout":0}]}]}}),
        json!({"hooks":{"Stop":[{"hooks":[{"type":"prompt","command":"test"}]}]}}),
        json!({"hooks":{"Stop":[{"hooks":[{"type":"command","command":"test","async":true}]}]}}),
        json!({"hooks":{"Stop":[{"hooks":[{"type":"command","command":"test","shell":"invalid"}]}]}}),
        json!({"hooks":{"Stop":[{"hooks":[{"type":"command","command":"test","args":42,"shell":"sh"}]}]}}),
        json!({"hooks":{"Stop":[{"hooks":[{"type":"command","command":"test","args":["\u{0}"]}]}]}}),
        json!({"hooks":{"Stop":[{"hooks":[{"type":"command","command":"test","onFailure":"ignore"}]}]}}),
        json!({"hooks":{"Stop":[{"hooks":[{"type":"command","command":"test","timeout":u64::MAX}]}]}}),
    ] {
        assert!(hooks.add_json(document.to_string().as_bytes()).is_err());
        assert!(hooks.bindings.is_empty());
    }
    Ok(())
}

#[test]
fn imports_normalize_to_the_same_typed_rules_and_validate_atomically() -> Result<()> {
    let mut imported = Hooks::default();
    imported.add_json(
        json!({"hooks":{"PreToolUse":[{"matcher":crate::tools::shell::PLATFORM.name,"hooks":[
        {"type":"command","command":"guard","args":["--check"],"shell":"bash","timeout":2,"onFailure":"block"},
        {"type":"command","command":"format","shell":"sh"}
    ]}]}}).to_string().as_bytes(),
    )?;
    let mut toml = Hooks::default();
    toml.add_toml(&format!(
        r#"
        [[hooks.PreToolUse]]
        matcher = "{matcher}"
        [[hooks.PreToolUse.hooks]]
        type = "command"
        command = "guard"
        args = ["--check"]
        shell = "bash"
        timeout = 2
        onFailure = "block"
        [[hooks.PreToolUse.hooks]]
        type = "command"
        command = "format"
        shell = "sh"
    "#,
        matcher = crate::tools::shell::PLATFORM.name
    ))?;
    let rule = HookRule {
        on: HookEvent::PreToolUse,
        matcher: crate::tools::shell::PLATFORM.name.into(),
        hooks: vec![
            HookSpec {
                run: HookRun::Executable {
                    executable: "guard".into(),
                    argv: vec!["--check".into()],
                },
                timeout_ms: Some(2_000),
                failure: HookFailure::Block,
            },
            HookSpec {
                run: HookRun::Shell {
                    command: "format".into(),
                    shell: Some("sh".into()),
                },
                timeout_ms: None,
                failure: HookFailure::Report,
            },
        ],
    };
    let restored: HookRule = serde_json::from_value(
        serde_json::to_value(&rule).map_err(|error| Error::Invalid(error.to_string()))?,
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(restored, rule);
    let mut typed = Hooks::default();
    typed.add_rules([restored])?;
    let input = json!({"tool_name":crate::tools::shell::PLATFORM.name});
    let registry = registry()?;
    for hooks in [&imported, &toml, &typed] {
        assert_eq!(
            hooks
                .commands(HookEvent::PreToolUse, &input, &registry)?
                .map(Arc::as_ref)
                .collect::<Vec<_>>(),
            rule.hooks.iter().collect::<Vec<_>>()
        );
    }
    let mut invalid = rule.clone();
    invalid.hooks[0].timeout_ms = Some(0);
    assert!(typed.add_rules([rule, invalid]).is_err());
    assert_eq!(
        typed
            .commands(HookEvent::PreToolUse, &input, &registry)?
            .count(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn failure_policy_controls_blocking_and_always_finishes_other_hooks() -> Result<()> {
    use std::cell::Cell;
    let registry = registry()?;
    for event in [
        HookEvent::SessionStart,
        HookEvent::PreToolUse,
        HookEvent::PostToolUse,
        HookEvent::Stop,
    ] {
        for policy in [HookFailure::Report, HookFailure::Block] {
            for case in [
                "spawn", "timeout", "exit", "exit2", "signal", "output", "success",
            ] {
                let mut hooks = Hooks::default();
                hooks.add_json(
                    &serde_json::to_vec(&json!({"hooks":{event.name():[{"hooks":[
                        {"type":"command","command":"guard","onFailure":match policy { HookFailure::Report => "continue", HookFailure::Block => "block" }},
                        {"type":"command","command":"remaining","onFailure":match policy { HookFailure::Report => "continue", HookFailure::Block => "block" }}
                    ]}]}}))
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                )?;
                let finished = Cell::new(0);
                let output = hooks
                    .run_commands(
                        event,
                        &json!({"tool_name":"shell"}),
                        &registry,
                        65_536,
                        |hook, _, _| {
                            finished.set(finished.get() + 1);
                            let result =
                                if command(hook) == "remaining" {
                                    Ok((
                                    Some(0),
                                    br#"{"hookSpecificOutput":{"additionalContext":"remaining"}}"#
                                        .to_vec(),
                                    vec![],
                                ))
                                } else {
                                    match case {
                                    "spawn" => Err(Error::Process(crate::ProcessError::NotFound {
                                        exit_code: None,
                                        message: "guard missing".into(),
                                    })),
                                    "timeout" => Err(Error::Process(crate::ProcessError::Timeout {
                                        exit_code: None,
                                        message: "guard timed out".into(),
                                    })),
                                    "exit" => Ok((Some(3), vec![], b"guard failed".to_vec())),
                                    "exit2" => Ok((Some(2), vec![], b"guard blocked".to_vec())),
                                    "signal" => Ok((None, vec![], b"guard terminated".to_vec())),
                                    "output" => Ok((
                                        Some(0),
                                        br#"{"hookSpecificOutput":{"hookEventName":"invalid"}}"#
                                            .to_vec(),
                                        vec![],
                                    )),
                                    _ => Ok((Some(0), vec![], vec![])),
                                }
                                };
                            std::future::ready(result)
                        },
                    )
                    .await?;
                assert_eq!(finished.get(), 2);
                assert_eq!(
                    output.additional_context.last().map(String::as_str),
                    Some("remaining")
                );
                let conventional_block =
                    case == "exit2" && matches!(event, HookEvent::PreToolUse | HookEvent::Stop);
                let policy_block =
                    policy == HookFailure::Block && event != HookEvent::Stop && case != "success";
                assert_eq!(
                    output.blocks(),
                    conventional_block || policy_block,
                    "{event:?} {policy:?} {case}: {output:?}"
                );
                let diagnostic = case != "success"
                    && !(case == "exit2"
                        && (policy == HookFailure::Report || event == HookEvent::Stop)
                        && event != HookEvent::SessionStart);
                assert_eq!(
                    output.diagnostics.len(),
                    usize::from(diagnostic),
                    "{event:?} {policy:?} {case}: {output:?}"
                );
            }
        }
    }
    Ok(())
}

fn registry() -> Result<ToolRegistry> {
    let mut registry = crate::tools::defaults()?;
    let mut notebook = crate::tool::files::read()?;
    notebook.definition.name = "NotebookEdit".into();
    notebook.hook_names = &[];
    registry.register((notebook, None), &mut None, None)?;
    Ok(registry)
}

#[test]
fn tool_matchers_use_declared_names_and_exact_identifiers() -> Result<()> {
    let registry = registry()?;
    let mut hooks = Hooks::default();
    hooks.add_json(
        json!({"hooks":{"PreToolUse":[
            {"matcher":"Bash","hooks":[{"type":"command","command":"guard"}]},
            {"matcher":"PowerShell","hooks":[{"type":"command","command":"windows-guard"}]},
            {"matcher":"shell","hooks":[{"type":"command","command":"shell-guard"}]},
            {"matcher":"Edit","hooks":[{"type":"command","command":"edit"}]},
            {"matcher":"Edit|Write","hooks":[{"type":"command","command":"changes"}]},
            {"matcher":"fs","hooks":[{"type":"command","command":"fs"}]}
        ]}})
        .to_string()
        .as_bytes(),
    )?;
    for (name, commands) in [
        ("shell", vec!["shell-guard"]),
        ("edit", vec!["edit", "changes"]),
        ("write", vec!["changes"]),
        ("fs", vec!["fs"]),
        // Regex alternation keeps regex semantics; plain Edit is anchored.
        ("NotebookEdit", vec!["changes"]),
        ("read", vec![]),
    ] {
        assert_eq!(
            hooks
                .commands(HookEvent::PreToolUse, &json!({"tool_name":name}), &registry)?
                .map(|hook| command(hook))
                .collect::<Vec<_>>(),
            commands
        );
    }
    Ok(())
}

#[tokio::test]
async fn shell_hooks_reject_host_tool_aliases() -> Result<()> {
    let registry = crate::tools::defaults()?;
    for matcher in ["Bash", "PowerShell"] {
        let mut hooks = Hooks::default();
        hooks.add_json(
            json!({"hooks":{"PreToolUse":[{
                "matcher":matcher,"hooks":[{"type":"command","command":"guard"}]
            }]}})
            .to_string()
            .as_bytes(),
        )?;
        let mut calls = 0;
        hooks
            .run_commands(
                HookEvent::PreToolUse,
                &json!({"tool_name":"shell"}),
                &registry,
                65_536,
                |_, _, _| {
                    calls += 1;
                    std::future::ready(Ok((Some(0), vec![], vec![])))
                },
            )
            .await?;
        assert_eq!(calls, 0, "{matcher} must not match shell");
    }
    Ok(())
}

#[tokio::test]
async fn tool_catalog_routes_approval_hooks() -> Result<()> {
    // Every catalog entry, including ask_question, which the standard
    // toolset leaves to the question plugin's selection.
    let names: Vec<_> = crate::tools::catalog()
        .map(|spec| spec.name.to_owned())
        .collect();
    let registry = crate::tools::registry(&names, |_| true, Vec::new())?;
    for spec in crate::tools::catalog() {
        let mut hooks = Hooks::default();
        let matcher = spec.hook_names.first().copied().unwrap_or(spec.name);
        hooks.add_json(
            json!({"hooks":{"PreToolUse":[{
                "matcher":matcher,"hooks":[{"type":"command","command":"approve"}]
            }]}})
            .to_string()
            .as_bytes(),
        )?;
        let mut calls = 0;
        let output = hooks
            .run_commands(
                HookEvent::PreToolUse,
                &json!({"tool_name":spec.name}),
                &registry,
                65_536,
                |_, _, _| {
                    calls += 1;
                    std::future::ready(Ok((
                        Some(0),
                        br#"{"hookSpecificOutput":{
                    "permissionDecision":"ask","permissionDecisionReason":"review tool"
                }}"#
                        .to_vec(),
                        vec![],
                    )))
                },
            )
            .await?;
        assert_eq!(calls, 1, "{}", spec.name);
        assert_eq!(
            output
                .approval
                .as_ref()
                .map(|approval| approval.prompt.as_str()),
            Some("review tool")
        );
    }
    Ok(())
}

#[test]
fn non_blocking_failures_return_diagnostics() -> Result<()> {
    let timeout = Error::process_io(std::io::ErrorKind::TimedOut.into());
    assert_eq!(timeout.code(), crate::wire::ErrorCode::Invalid);
    assert!(matches!(
        timeout,
        Error::Process(crate::ProcessError::Timeout { .. })
    ));
    for event in [
        HookEvent::SessionStart,
        HookEvent::PreToolUse,
        HookEvent::PostToolUse,
        HookEvent::Stop,
    ] {
        for code in [Some(1), Some(3), None] {
            let mut output = HookOutput::default();
            output.apply(
                event,
                HookFailure::Report,
                Ok((code, b"ignored".to_vec(), b"command failed".to_vec())),
            );
            assert!(output.blocked.is_none());
            assert_eq!(output.diagnostics.len(), 1);
            assert!(matches!(
                output.diagnostics[0].cause,
                crate::ProcessError::Exit { .. }
            ));
            assert_eq!(output.diagnostics[0].cause.exit_code(), code);
            assert!(
                output.diagnostics[0]
                    .cause
                    .message()
                    .contains("command failed")
            );
        }
    }
    let mut output = HookOutput::default();
    output.apply(
        HookEvent::SessionStart,
        HookFailure::Report,
        Ok((Some(2), vec![], b"start failed".to_vec())),
    );
    assert!(
        output.diagnostics[0]
            .cause
            .message()
            .contains("start failed")
    );
    Ok(())
}

#[test]
fn output_applies_context_tool_decisions_and_stop_blocking() -> Result<()> {
    let mut output = HookOutput::default();
    output.observe(HookEvent::SessionStart, Some(0), b"workspace ready\n", b"")?;
    assert_eq!(output.additional_context, ["workspace ready"]);
    output.observe(
        HookEvent::PreToolUse,
        Some(0),
        br#"{"hookSpecificOutput":{
        "hookEventName":"PreToolUse","updatedInput":{"path":"src/lib.rs"},
        "additionalContext":"Inspect tests first"
    }}"#,
        b"",
    )?;
    assert_eq!(output.updated_input, Some(json!({"path":"src/lib.rs"})));
    assert_eq!(
        output.additional_context,
        ["workspace ready", "Inspect tests first"]
    );
    output.observe(
        HookEvent::PreToolUse,
        Some(0),
        br#"{"hookSpecificOutput":{
        "permissionDecision":"deny","permissionDecisionReason":"protected path"
    }}"#,
        b"",
    )?;
    assert_eq!(output.blocked.as_deref(), Some("protected path"));
    output.observe(
        HookEvent::PreToolUse,
        Some(0),
        br#"{"hookSpecificOutput":{
        "permissionDecision":"ask","permissionDecisionReason":"risky command"}}"#,
        b"",
    )?;
    assert_eq!(
        output
            .approval
            .as_ref()
            .map(|approval| approval.prompt.as_str()),
        Some("risky command")
    );
    assert_eq!(output.blocked.as_deref(), Some("protected path"));
    let mut output = HookOutput::default();
    output.observe(HookEvent::Stop, Some(2), b"", b"tests failed")?;
    assert_eq!(output.blocked.as_deref(), Some("tests failed"));
    let mut post = HookOutput::default();
    post.observe(HookEvent::PostToolUse, Some(2), b"", b"format failed")?;
    assert_eq!(post.additional_context, ["format failed"]);
    assert!(post.blocked.is_none());
    assert!(
        output
            .observe(
                HookEvent::PreToolUse,
                Some(0),
                br#"{"hookSpecificOutput":{"hookEventName":"Stop"}}"#,
                b""
            )
            .is_err()
    );
    assert!(
        output
            .observe(
                HookEvent::PreToolUse,
                Some(0),
                br#"{"hookSpecificOutput":{"updatedInput":[]}}"#,
                b""
            )
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn invalid_structured_output_obeys_failure_policy_without_partial_decisions() -> Result<()> {
    let invalid: &[&[u8]] = &[
        b"{\"decision\":\"block\"",
        b"{\"hookSpecificOutput\":{\"permissionDecision\":\"deny\"}",
        b"{\"decision\":\"block\"} trailing",
        b"[",
        br#"{"hookSpecificOutput":{"permissionDecision":false}}"#,
        br#"{"hookSpecificOutput":{"permissionDecision":null}}"#,
        br#"{"hookSpecificOutput":{"permissionDecision":"unknown"}}"#,
        br#"{"continue":"false"}"#,
        br#"{"continue":null}"#,
        br#"{"stopReason":false}"#,
        br#"{"decision":false}"#,
        br#"{"decision":"unknown"}"#,
        br#"{"reason":{}}"#,
        br#"{"hookSpecificOutput":null}"#,
        br#"{"hookSpecificOutput":[]}"#,
        br#"{"hookSpecificOutput":false}"#,
        br#"{"hookSpecificOutput":{"hookEventName":false}}"#,
        br#"{"hookSpecificOutput":{"permissionDecisionReason":[]}}"#,
        br#"{"hookSpecificOutput":{"additionalContext":false}}"#,
        br#"{"hookSpecificOutput":{"updatedInput":null}}"#,
        br#"{"decision":"block","hookSpecificOutput":{"updatedInput":[],"additionalContext":"discard"}}"#,
        br#"{"continue":false,"hookSpecificOutput":{"permissionDecision":false}}"#,
        b"[]", b"null", b"true", b"42", br#""block""#, b"\xff",
    ];
    let registry = registry()?;
    for event in [
        HookEvent::SessionStart,
        HookEvent::PreToolUse,
        HookEvent::PostToolUse,
        HookEvent::Stop,
    ] {
        for policy in [HookFailure::Report, HookFailure::Block] {
            let mut hooks = Hooks::default();
            hooks.add_json(
                &serde_json::to_vec(&json!({"hooks":{event.name():[{"hooks":[
                    {"type":"command","command":"guard","onFailure":match policy { HookFailure::Report => "continue", HookFailure::Block => "block" }},
                    {"type":"command","command":"remaining"}
                ]}]}}))
                .map_err(|error| Error::Invalid(error.to_string()))?,
            )?;
            for bytes in invalid {
                // SessionStart accepts bracket-prefixed plain text; guarded events still reject it.
                let session_note = event == HookEvent::SessionStart && *bytes == b"[";
                let finished = std::cell::Cell::new(0);
                let output = hooks
                    .run_commands(
                        event,
                        &json!({"tool_name":"shell"}),
                        &registry,
                        65_536,
                        |hook, _, _| {
                            finished.set(finished.get() + 1);
                            let stdout = if command(hook) == "guard" {
                                bytes.to_vec()
                            } else {
                                br#"{"hookSpecificOutput":{"additionalContext":"remaining"}}"#
                                    .to_vec()
                            };
                            std::future::ready(Ok((Some(0), stdout, vec![])))
                        },
                    )
                    .await?;
                assert_eq!(finished.get(), 2);
                assert_eq!(
                    output.diagnostics.len(),
                    usize::from(!session_note),
                    "{event:?} {policy:?} {bytes:?}: {output:?}"
                );
                assert_eq!(
                    output.blocks(),
                    policy == HookFailure::Block && event != HookEvent::Stop && !session_note,
                    "{event:?} {policy:?} {bytes:?}: {output:?}"
                );
                assert_eq!(
                    output.additional_context,
                    if session_note {
                        vec!["[", "remaining"]
                    } else {
                        vec!["remaining"]
                    }
                );
                assert!(output.updated_input.is_none());
                assert!(output.approval.is_none());
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn matching_hooks_run_concurrently_and_merge_in_config_order() -> Result<()> {
    use futures::{FutureExt as _, channel::oneshot};
    use std::cell::Cell;
    let mut hooks = Hooks::default();
    hooks.add_json(
        br#"{"hooks":{"PreToolUse":[{"hooks":[
        {"type":"command","command":"first"},
        {"type":"command","command":"second"}
    ]}]}}"#,
    )?;
    let registry = registry()?;
    let input = json!({"tool_name":"shell"});
    let started = Cell::new(0);
    let (first_tx, first_rx) = oneshot::channel();
    let (second_tx, second_rx) = oneshot::channel();
    let mut receivers = [first_rx, second_rx].into_iter();
    let future = hooks.run_commands(
        HookEvent::PreToolUse,
        &input,
        &registry,
        65_536,
        |_, _, _| {
            let receiver = receivers.next();
            let started = &started;
            async move {
                started.set(started.get() + 1);
                Ok((
                    Some(0),
                    receiver
                        .ok_or_else(|| Error::Invalid("missing hook receiver".into()))?
                        .await
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                    vec![],
                ))
            }
        },
    );
    let mut future = core::pin::pin!(future);
    assert!(future.as_mut().now_or_never().is_none());
    assert_eq!(
        started.get(),
        2,
        "both commands start before either completes"
    );
    let response = |name| {
        serde_json::to_vec(&json!({"decision":"block","reason":name,
        "hookSpecificOutput":{"additionalContext":name,"updatedInput":{"winner":name}}}))
        .map_err(|error| Error::Invalid(error.to_string()))
    };
    second_tx
        .send(response("second")?)
        .map_err(|_| Error::Invalid("second hook cancelled".into()))?;
    first_tx
        .send(response("first")?)
        .map_err(|_| Error::Invalid("first hook cancelled".into()))?;
    let output = future.await?;
    assert_eq!(output.blocked.as_deref(), Some("first"));
    assert_eq!(output.additional_context, ["first", "second"]);
    // A denial wins over every rewrite, so none applies.
    assert_eq!(output.updated_input, None);
    Ok(())
}

#[tokio::test]
async fn process_failures_and_invalid_outputs_are_diagnostics_and_do_not_stop_other_hooks()
-> Result<()> {
    let mut hooks = Hooks::default();
    hooks.add_json(
        br#"{"hooks":{"SessionStart":[{"hooks":[
        {"type":"command","command":"timeout"},
        {"type":"command","command":"spawn"},
        {"type":"command","command":"invalid"},
        {"type":"command","command":"success"}
    ]}]}}"#,
    )?;
    let output = hooks
        .run_commands(
            HookEvent::SessionStart,
            &json!({}),
            &registry()?,
            65_536,
            |hook, _, _| {
                let result = match command(hook) {
                    "timeout" => Err(Error::Process(crate::ProcessError::Timeout {
                        exit_code: None,
                        message: "deadline elapsed".into(),
                    })),
                    "spawn" => Err(Error::Process(crate::ProcessError::NotFound {
                        exit_code: None,
                        message: "shell missing".into(),
                    })),
                    "invalid" => Ok((
                        Some(0),
                        br#"{"hookSpecificOutput":{"hookEventName":"Stop"}}"#.to_vec(),
                        vec![],
                    )),
                    _ => Ok((Some(0), b"ready".to_vec(), vec![])),
                };
                std::future::ready(result)
            },
        )
        .await?;
    assert!(output.blocked.is_none());
    assert_eq!(output.additional_context, ["ready"]);
    assert_eq!(output.diagnostics.len(), 3);
    assert!(matches!(
        output.diagnostics[0].cause,
        crate::ProcessError::Timeout { .. }
    ));
    assert!(
        output.diagnostics[1]
            .cause
            .message()
            .contains("shell missing")
    );
    assert!(
        output.diagnostics[2]
            .cause
            .message()
            .contains("another event")
    );
    Ok(())
}

#[test]
fn aggregate_failure_preserves_an_already_decoded_denial() -> Result<()> {
    futures::executor::block_on(async {
        let mut hooks = Hooks::default();
        hooks.add_rules([HookRule {
            on: HookEvent::SessionStart,
            matcher: String::new(),
            hooks: vec![
                HookSpec {
                    run: HookRun::Executable {
                        executable: "deny".into(),
                        argv: vec![],
                    },
                    timeout_ms: Some(1000),
                    failure: HookFailure::Report,
                },
                HookSpec {
                    run: HookRun::Executable {
                        executable: "expand".into(),
                        argv: vec![],
                    },
                    timeout_ms: Some(1000),
                    failure: HookFailure::Report,
                },
            ],
        }])?;
        let output = hooks
            .run_commands(
                HookEvent::SessionStart,
                &json!({}),
                &ToolRegistry::default(),
                512,
                |hook, quota, _| {
                    let stdout = match &hook.run {
                        HookRun::Executable { executable, .. } if executable == "deny" => {
                            br#"{"continue":false,"stopReason":"deny"}"#.to_vec()
                        }
                        _ => vec![0; 100],
                    };
                    assert!(
                        quota.is_some_and(|quota| stdout.len() <= quota),
                        "fixture exceeds capture quota"
                    );
                    std::future::ready(Ok((Some(0), stdout, vec![])))
                },
            )
            .await?;
        assert!(
            output.blocks(),
            "bounded failure discarded an explicit guard decision"
        );
        assert!(matches!(
            output
                .diagnostics
                .first()
                .map(|diagnostic| &diagnostic.cause),
            Some(crate::ProcessError::OutputLimit { .. })
        ));
        // The former failure-policy-only fallback loses the decoded denial.
        assert!(!HookOutput::output_limit(false).blocks());
        Ok(())
    })
}

#[test]
fn runner_folds_deny_after_encoded_overflow_in_config_order() -> Result<()> {
    futures::executor::block_on(async {
        let mut hooks = Hooks::default();
        hooks.add_rules([HookRule {
            on: HookEvent::SessionStart,
            matcher: String::new(),
            hooks: ["expand", "deny"]
                .into_iter()
                .map(|command| HookSpec {
                    run: HookRun::Executable {
                        executable: command.into(),
                        argv: vec![],
                    },
                    timeout_ms: Some(1000),
                    failure: HookFailure::Report,
                })
                .collect(),
        }])?;
        let mut calls = 0;
        let output = hooks.run_commands(HookEvent::SessionStart, &json!({}), &ToolRegistry::default(), 4096,
            |hook, quota, _| {
                calls += 1;
                let stdout = if matches!(&hook.run, HookRun::Executable { executable, .. } if executable == "expand") {
                    vec![0;1000]
                } else { br#"{"continue":false,"stopReason":"deny"}"#.to_vec() };
                assert!(quota.is_some_and(|quota| stdout.len() <= quota));
                std::future::ready(Ok((Some(0),stdout,vec![])))
            }).await?;
        assert_eq!(calls, 2);
        assert!(output.blocks(), "encoded overflow bypassed a later guard");
        assert_eq!(output.blocked.as_deref(), Some("Hook denied"));
        assert!(matches!(
            output
                .diagnostics
                .first()
                .map(|diagnostic| &diagnostic.cause),
            Some(crate::ProcessError::OutputLimit { .. })
        ));
        assert!(
            serde_json::to_vec(&output)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .len()
                <= 4096
        );
        Ok(())
    })
}

proptest! {
    // Property: simultaneously live capture quotas fit one aggregate byte
    // budget for every command count, without a synchronization primitive.
    #[test]
    fn capture_partitions_bound_aggregate_storage(maximum in 256u64..=65_536, count in 2usize..512) {
        let quota = capture_quota(maximum, count);
        prop_assert!((quota as u64) * count as u64 <= maximum);
        // The former independent per-command limit violates the same detector.
        let broken = maximum.min(65_536);
        prop_assert!(broken * count as u64 > maximum);
    }
}
