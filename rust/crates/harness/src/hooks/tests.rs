use super::*;
use crate::native_settings::{NativeSettings, SettingsConfig};
use serde_json::json;

mod failures;

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
            timeout_ms: 5_000,
            failure: HookFailure::Report,
        }],
    }])?;
    let root = std::env::current_dir().map_err(|error| Error::Invalid(error.to_string()))?;
    let output = hooks
        .run(
            HookEvent::PreToolUse,
            &root,
            &root,
            json!({"tool_name":"bash", "tool_input":{"payload":"x".repeat(1_048_576)}}),
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
    }
}

#[tokio::test]
async fn exec_form_placeholders_launch_guards_without_retokenizing_paths_or_arguments() -> Result<()>
{
    if let Some(argument) = std::env::args()
        .find_map(|argument| argument.strip_prefix("hook-exec-child:").map(str::to_owned))
    {
        let root = std::env::var("CLAUDE_PROJECT_DIR").expect("project root environment");
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
        .tempdir_in(executable.parent().expect("binary parent"))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let root = fixture.path();
    let guard = root.join("guard.exe");
    std::fs::hard_link(&executable, &guard).map_err(|error| Error::Invalid(error.to_string()))?;
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
                json!({"tool_name":"bash"}),
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
        timeout_ms: 60_000,
        failure: HookFailure::Report,
    };
    let root = std::env::current_dir().map_err(process_error)?;
    let mut future = Box::pin(run_hook(
        Arc::new(hook),
        Arc::new((root.clone(), root, vec![])),
        Arc::new(|| Ok(())),
        65_536,
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

#[tokio::test]
async fn relocated_codex_configuration_and_duplicate_paths_are_loaded_once() -> Result<()> {
    let process_error = Error::process_io;
    let root = tempfile::tempdir().map_err(process_error)?;
    let home = root.path();
    let relocated = home.join("custom-codex");
    std::fs::create_dir_all(home.join(".claude")).map_err(process_error)?;
    std::fs::create_dir(&relocated).map_err(process_error)?;
    let document = br#"{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"guard"}]}]}}"#;
    std::fs::write(home.join(".claude/settings.json"), document).map_err(process_error)?;
    std::fs::write(relocated.join("hooks.json"), document).map_err(process_error)?;
    let hooks = NativeSettings::load(
        SettingsConfig::new(home, home)
            .codex_home(&relocated)
            .trust_project_hooks(true),
    )
    .await?
    .hooks;
    let registry = registry()?;
    assert_eq!(
        hooks
            .commands(HookEvent::Stop, &json!({}), &registry)?
            .count(),
        2
    );
    #[cfg(unix)]
    {
        let alias = home.join("alias");
        std::os::unix::fs::symlink(home, &alias).map_err(process_error)?;
        let hooks = NativeSettings::load(
            SettingsConfig::new(&alias, home)
                .codex_home(&relocated)
                .trust_project_hooks(true),
        )
        .await?
        .hooks;
        assert_eq!(
            hooks
                .commands(HookEvent::Stop, &json!({}), &registry)?
                .count(),
            2
        );
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_settings_keep_the_format_of_the_configured_path() -> Result<()> {
    let process_error = Error::process_io;
    let root = tempfile::tempdir().map_err(process_error)?;
    let home = root.path().join("home");
    let codex = home.join(".codex");
    let claude = home.join(".claude");
    let workspace = root.path().join("workspace");
    for directory in [&codex, &claude, &workspace] {
        std::fs::create_dir_all(directory).map_err(process_error)?;
    }
    let toml = root.path().join("codex-dotfile");
    std::fs::write(
        &toml,
        "[[hooks.Stop]]\n[[hooks.Stop.hooks]]\ntype = 'command'\ncommand = 'toml-guard'\n",
    )
    .map_err(process_error)?;
    let json = root.path().join("claude-dotfile.toml");
    std::fs::write(
        &json,
        br#"{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"json-guard"}]}]}}"#,
    )
    .map_err(process_error)?;
    std::os::unix::fs::symlink(&toml, codex.join("config.toml")).map_err(process_error)?;
    std::os::unix::fs::symlink(&json, claude.join("settings.json")).map_err(process_error)?;
    // The trusted project alias resolves to the same JSON file and still runs once.
    std::fs::create_dir(workspace.join(".claude")).map_err(process_error)?;
    std::os::unix::fs::symlink(&json, workspace.join(".claude/settings.json"))
        .map_err(process_error)?;
    let hooks = NativeSettings::load(
        SettingsConfig::new(&workspace, &home)
            .codex_home(&codex)
            .trust_project_hooks(true),
    )
    .await?
    .hooks;
    assert_eq!(
        hooks
            .commands(HookEvent::Stop, &json!({}), &registry()?)?
            .map(|hook| command(hook))
            .collect::<Vec<_>>(),
        ["json-guard", "toml-guard"]
    );
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
        br#"{"hooks":{"PreToolUse":[{"matcher":"Bash","hooks":[
        {"type":"command","command":"guard","args":["--check"],"shell":"bash","timeout":2,"onFailure":"block"},
        {"type":"command","command":"format","shell":"sh"}
    ]}]}}"#,
    )?;
    let mut toml = Hooks::default();
    toml.add_toml(
        r#"
        [[hooks.PreToolUse]]
        matcher = "Bash"
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
    )?;
    let rule = HookRule {
        on: HookEvent::PreToolUse,
        matcher: "Bash".into(),
        hooks: vec![
            HookSpec {
                run: HookRun::Executable {
                    executable: "guard".into(),
                    argv: vec!["--check".into()],
                },
                timeout_ms: 2_000,
                failure: HookFailure::Block,
            },
            HookSpec {
                run: HookRun::Shell {
                    command: "format".into(),
                    shell: Some("sh".into()),
                },
                timeout_ms: 60_000,
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
    let input = json!({"tool_name":"bash"});
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
    invalid.hooks[0].timeout_ms = 0;
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
                        &json!({"tool_name":"bash"}),
                        &registry,
                        65_536,
                        |hook, _| {
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
        br#"{"hooks":{"PreToolUse":[
        {"matcher":"Bash","hooks":[{"type":"command","command":"guard"}]},
        {"matcher":"Edit","hooks":[{"type":"command","command":"edit"}]},
        {"matcher":"Edit|Write","hooks":[{"type":"command","command":"changes"}]},
        {"matcher":"fs","hooks":[{"type":"command","command":"fs"}]}
    ]}}"#,
    )?;
    for (name, commands) in [
        ("bash", vec!["guard"]),
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
async fn tool_catalog_routes_approval_hooks() -> Result<()> {
    let registry = crate::tools::defaults()?;
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
                |_, _| {
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
        assert_eq!(output.approval.as_deref(), Some("review tool"));
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
    assert_eq!(output.approval.as_deref(), Some("risky command"));
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
                        &json!({"tool_name":"bash"}),
                        &registry,
                        65_536,
                        |hook, _| {
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
    let input = json!({"tool_name":"bash"});
    let started = Cell::new(0);
    let (first_tx, first_rx) = oneshot::channel();
    let (second_tx, second_rx) = oneshot::channel();
    let mut receivers = [first_rx, second_rx].into_iter();
    let future = hooks.run_commands(HookEvent::PreToolUse, &input, &registry, 65_536, |_, _| {
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
    });
    futures::pin_mut!(future);
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
    assert_eq!(output.updated_input, Some(json!({"winner":"second"})));
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
            |hook, _| {
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

#[tokio::test]
async fn native_hooks_default_user_guards_and_project_trust_use_one_shared_script() -> Result<()> {
    let process_error = Error::process_io;
    if let Some(address) =
        std::env::args().find_map(|arg| arg.strip_prefix("hook-child:").map(str::to_owned))
    {
        use std::io::{Read, Write};
        let mut stream = std::net::TcpStream::connect(address).map_err(process_error)?;
        stream.write_all(&[1]).map_err(process_error)?;
        let mut byte = [0];
        stream.read_exact(&mut byte).map_err(process_error)?;
        return Err(Error::Invalid(
            "hook fixture was released instead of cancelled".into(),
        ));
    }
    let fixture = tempfile::tempdir().map_err(process_error)?;
    let home = fixture.path().join("home");
    let workspace = fixture.path().join("project");
    let cwd = workspace.join("nested");
    std::fs::create_dir_all(&cwd).map_err(process_error)?;
    #[cfg(windows)]
    let (script_name, script, shell) = (
        "fixture.cmd",
        r#"@echo off
setlocal
set /p payload=
if not defined payload exit /b 3
if not "%CD%"=="%CLAUDE_PROJECT_DIR%\nested" exit /b 4
> "%CLAUDE_PROJECT_DIR%\hook-ran" echo %payload%
exit /b 2
"#,
        "cmd",
    );
    #[cfg(not(windows))]
    let (script_name, script, shell) = (
        "fixture.sh",
        r#"IFS= read -r input
test -n "$input" || exit 3
test "$(pwd -P)" = "$(cd "$CLAUDE_PROJECT_DIR/nested" && pwd -P)" || exit 4
printf '%s' "$input" > "$CLAUDE_PROJECT_DIR/hook-ran"
exit 2
"#,
        "sh",
    );
    let script_path = fixture.path().join(script_name);
    std::fs::write(&script_path, script).map_err(process_error)?;
    #[cfg(windows)]
    let command = format!("call \"{}\"", script_path.display());
    #[cfg(not(windows))]
    let command = format!("sh \"{}\"", script_path.display());
    let hook = json!({"type":"command","command":command,"shell":shell});
    let write = |root: &std::path::Path, path: &str, value: &Value| -> Result<()> {
        let path = root.join(path);
        std::fs::create_dir_all(
            path.parent()
                .ok_or_else(|| Error::Invalid("fixture parent missing".into()))?,
        )
        .map_err(process_error)?;
        std::fs::write(
            path,
            serde_json::to_vec(value).map_err(|error| Error::Invalid(error.to_string()))?,
        )
        .map_err(process_error)
    };
    write(
        &home,
        ".claude/settings.json",
        &json!({"hooks":{"PreToolUse":[{"matcher":"Bash","hooks":[hook]}]}}),
    )?;
    let project = json!({"hooks":{"Stop":[{"hooks":[hook]}]}});
    for path in [
        ".claude/settings.json",
        ".claude/settings.local.json",
        ".codex/hooks.json",
    ] {
        write(&workspace, path, &project)?;
    }
    std::fs::write(workspace.join(".codex/config.toml"), format!(
        "[[hooks.Stop]]\n[[hooks.Stop.hooks]]\ntype = 'command'\ncommand = {command:?}\nshell = '{shell}'\n"
    )).map_err(process_error)?;
    let registry = registry()?;
    let hooks = NativeSettings::load(
        SettingsConfig::new(&workspace, &home).codex_home(home.join(".codex")),
    )
    .await?
    .hooks;
    let input = json!({"tool_name":"bash","session_id":"test"});
    assert_eq!(
        hooks
            .run(HookEvent::Stop, &workspace, &cwd, input.clone(), &registry)
            .await?,
        HookOutput::default()
    );
    assert!(
        !workspace.join("hook-ran").exists(),
        "untrusted project commands never execute"
    );
    let output = hooks
        .run(
            HookEvent::PreToolUse,
            &workspace,
            &cwd,
            input.clone(),
            &registry,
        )
        .await?;
    assert!(
        output.blocks(),
        "user Bash guard fires on registry tool bash: {output:?}"
    );
    assert!(output.diagnostics.is_empty(), "{output:?}");
    assert!(
        workspace.join("hook-ran").exists(),
        "script receives the project root and per-call cwd"
    );
    let captured: Value =
        serde_json::from_slice(&std::fs::read(workspace.join("hook-ran")).map_err(process_error)?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        captured,
        json!({"tool_name":"bash", "session_id":"test", "hook_event_name":"PreToolUse", "cwd":cwd.to_string_lossy()})
    );
    let failure = hooks
        .run(
            HookEvent::PreToolUse,
            &workspace,
            &workspace.join("missing"),
            input,
            &registry,
        )
        .await?;
    assert!(failure.blocked.is_none());
    assert_eq!(failure.diagnostics.len(), 1);
    let trusted = NativeSettings::load(
        SettingsConfig::new(&workspace, &home)
            .codex_home(home.join(".codex"))
            .trust_project_hooks(true),
    )
    .await?
    .hooks;
    assert_eq!(
        trusted
            .commands(HookEvent::Stop, &json!({}), &registry)?
            .count(),
        4
    );
    let trusted_output = trusted
        .run_commands(HookEvent::Stop, &json!({}), &registry, 65_536, |_, _| {
            std::future::ready(Ok((Some(2), vec![], b"project guard".to_vec())))
        })
        .await?;
    assert_eq!(trusted_output.blocked.as_deref(), Some("project guard"));
    std::fs::remove_file(home.join(".claude/settings.json")).map_err(process_error)?;
    std::fs::create_dir(home.join(".claude/settings.json")).map_err(process_error)?;
    assert!(matches!(
        NativeSettings::load(
            SettingsConfig::new(&workspace, &home).codex_home(home.join(".codex"))
        )
        .await,
        Err(Error::Process(_))
    ));
    Ok(())
}
