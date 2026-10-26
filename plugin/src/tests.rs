use super::*;

#[test]
fn unknown_internal_commands_fail_before_workspace_lookup() -> Result<()> {
    let error = internal(&["__unknown".into()])
        .err()
        .ok_or("unknown internal command admitted")?;
    assert!(
        error
            .to_string()
            .contains("step=admit internal command: unknown __unknown")
    );
    assert!(error.to_string().contains("recovery: run acyclic --help"));
    Ok(())
}

#[test]
fn release_certification_uses_the_shared_policy_for_foreign_artifacts() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let executable = directory.path().join("release artifact");
    let receipt = directory.path().join("receipt.json");
    std::fs::write(&executable, b"internal artifact input")?;
    // Internal verifier inputs, not evidence of a native mount qualification.
    let mut document = json!({
        "schema":"acyclic-native-mount-qualification-v2", "os":"macos", "arch":"aarch64",
        "coverage":acyclic_fs::mount_qualification::COVERAGE,
        "required_kind":"macos-nfs", "release_version":"internal-test",
        "executable_blake3":blake3::hash(b"internal artifact input").to_hex().to_string(),
        "passed":true, "capability":{"kind":"macos-nfs", "available":true, "writable":true,
            "provider_process_io_observable":true,"session_isolation":"SharedProcess","unavailable_reason":null},
        "cases":(["real-mount-mutation-matrix","crash-detach-recovery","checkout-and-git-untouched"]
            .map(|name|json!({"name":name,"status":"passed","reason":null})))
    });
    let args = [
        "__verify-release-certification".to_owned(),
        receipt.to_str().ok_or("receipt path")?.to_owned(),
        executable.to_str().ok_or("artifact path")?.to_owned(),
        "macos".into(),
        "aarch64".into(),
        "internal-test".into(),
    ];
    std::fs::write(&receipt, serde_json::to_vec(&document)?)?;
    assert!(internal(&args)?);
    *document.get_mut("coverage").ok_or("coverage")? = json!([]);
    std::fs::write(&receipt, serde_json::to_vec(&document)?)?;
    let error = match internal(&args) {
        Ok(_) => return Err("incomplete foreign release receipt accepted".into()),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("step=verify native mount release certification"));
    let (_, recovery) = error.split_once("recovery argv: ").ok_or("recovery argv")?;
    let recovery: Vec<String> = serde_json::from_str(recovery)?;
    assert_eq!(recovery.get(1..).ok_or("recovery arguments")?, args);
    Ok(())
}

#[test]
fn native_hook_context_encodes_only_valid_task_ids() -> Result<()> {
    let identity = "harness/agents/18aa58bb16c0440c9367fc0230a95dd5";
    let task = parse_task(identity)?;
    let input = json!({"cwd":"fixture"});
    let encoded = hook_input(input.clone(), Some(std::ffi::OsStr::new(identity)))?;
    let encoded_task = encoded.get("task").cloned().ok_or("hook omitted task")?;
    assert_eq!(serde_json::from_value::<AgentPath>(encoded_task)?, task);
    assert_eq!(hook_input(input.clone(), None)?, input);
    assert!(hook_input(input, Some(std::ffi::OsStr::new("invalid"))).is_err());
    assert!(hook_input(Value::Null, Some(std::ffi::OsStr::new(identity))).is_err());
    // Negative control: serializing the OS environment value directly is the
    // old hook failure, on both Unix and Windows. It cannot decode as a task.
    let legacy = json!(std::ffi::OsString::from(identity));
    assert!(serde_json::from_value::<AgentPath>(legacy).is_err());
    Ok(())
}

#[test]
fn relative_parent_paths_keep_their_targets_when_child_cwd_changes() -> Result<()> {
    let fixture = tempfile::tempdir()?;
    let workspace = fixture.path().join("workspace");
    let view = workspace.join(".volumes/agents/child");
    let shared = fixture.path().join("shared");
    std::fs::create_dir_all(&view)?;
    std::fs::create_dir(&shared)?;
    std::fs::write(shared.join("policy.json"), "{}")?;
    for (host, supplied) in [
        (
            "codex",
            vec![
                "--sandbox",
                "workspace-write",
                "--ask-for-approval",
                "never",
                "--add-dir",
                "../shared",
            ],
        ),
        (
            "claude-code",
            vec![
                "--settings",
                "../shared/policy.json",
                "--permission-mode",
                "default",
                "--plugin-dir",
                "../shared",
                "--add-dir",
                "../shared",
            ],
        ),
        (
            "pi",
            vec![
                "--session-dir",
                "../shared",
                "--extension",
                "../shared/policy.json",
            ],
        ),
    ] {
        let supplied = supplied.into_iter().map(str::to_owned).collect::<Vec<_>>();
        let flags = codex::parent_flags(host, &supplied, &workspace)?;
        let child = codex::child(host, &flags, &view, None)?;
        let arguments = child
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        for (option, expected) in [
            ("--add-dir", shared.clone()),
            ("--plugin-dir", shared.clone()),
            ("--settings", shared.join("policy.json")),
            ("--session-dir", shared.clone()),
            ("--extension", shared.join("policy.json")),
        ] {
            if supplied.iter().any(|arg| arg == option) {
                let actual = arguments
                    .windows(2)
                    .find_map(|pair| match pair {
                        [name, value] if name == option => Some(PathBuf::from(value)),
                        _ => None,
                    })
                    .ok_or("native path option absent")?;
                assert!(
                    actual.is_absolute(),
                    "native path must not change with child cwd"
                );
                assert_eq!(view.join(actual).canonicalize()?, expected.canonicalize()?);
            }
        }
    }
    let inline = [
        "--settings",
        " {\"sandbox\":{\"enabled\":true}}",
        "--permission-mode",
        "default",
    ]
    .map(str::to_owned);
    let flags = codex::parent_flags("claude-code", &inline, &workspace)?;
    assert_eq!(
        flags.first_chunk::<4>().ok_or("inline settings absent")?,
        &inline
    );
    Ok(())
}

#[test]
fn pi_package_sources_are_not_shadowed_by_local_filenames() -> Result<()> {
    let fixture = tempfile::tempdir()?;
    let workspace = fixture.path();
    for source in [
        "npm:fixture-extension",
        "builtin:fixture",
        "git:example.com/fixture/extension",
        "github:fixture/extension",
        "http://example.com/fixture/extension",
        "https://example.com/fixture/extension",
        "ssh://example.com/fixture/extension",
    ] {
        #[cfg(unix)]
        {
            let collision = workspace.join(source);
            std::fs::create_dir_all(collision.parent().ok_or("fixture parent absent")?)?;
            std::fs::write(
                collision,
                "a local filename must not replace a package source",
            )?;
        }
        let supplied = ["--extension".into(), source.into()];
        assert_eq!(
            codex::parent_flags("pi", &supplied, workspace)?,
            supplied,
            "native package source must not become a local extension"
        );
    }
    Ok(())
}

#[test]
fn native_launch_preserves_policy_cwd_and_resume_without_bypass() -> Result<()> {
    let supplied = [
        "--sandbox",
        "workspace-write",
        "--ask-for-approval",
        "never",
    ]
    .map(str::to_owned);
    assert!(
        codex::validate(
            "codex",
            supplied.first_chunk::<2>().ok_or("policy pair absent")?
        )
        .is_err()
    );
    let flags = codex::parent_flags("codex", &supplied, Path::new("workspace"))?;
    let (policy, roots) = flags
        .split_at_checked(supplied.len())
        .ok_or("inherited policy absent")?;
    assert_eq!(policy, supplied);
    assert_eq!(roots, ["--add-dir", "workspace"]);
    let command = codex::child("codex", &flags, Path::new("view"), Some("resume-id"))?;
    assert!(
        codex::child("codex", &flags, Path::new("view"), Some("--unsafe")).is_err(),
        "resume references cannot change native options"
    );
    assert_eq!(command.get_current_dir(), Some(Path::new("view")));
    assert_eq!(
        command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>(),
        [
            "--sandbox",
            "workspace-write",
            "--ask-for-approval",
            "never",
            "--add-dir",
            "workspace",
            "exec",
            "--json",
            "--cd",
            "view",
            "resume",
            "resume-id",
            "-"
        ]
    );
    Ok(())
}

#[test]
fn claude_and_pi_children_preserve_native_options_and_resume_references() -> Result<()> {
    for (host, flags, resume_option) in [
        (
            "claude-code",
            vec!["--settings", "policy.json", "--permission-mode", "default"],
            "--resume",
        ),
        (
            "pi",
            vec!["--thinking", "high", "--session-dir", "native-sessions"],
            "--session",
        ),
    ] {
        let supplied = flags.into_iter().map(str::to_owned).collect::<Vec<_>>();
        let flags = codex::parent_flags(host, &supplied, Path::new("workspace"))?;
        if host == "pi" {
            assert_eq!(
                flags,
                [
                    "--thinking".to_owned(),
                    "high".to_owned(),
                    "--session-dir".to_owned(),
                    Path::new("workspace")
                        .join("native-sessions")
                        .to_string_lossy()
                        .into_owned()
                ]
            );
        } else {
            assert_eq!(
                flags.get(supplied.len()..).ok_or("workspace root absent")?,
                ["--add-dir", "workspace"]
            );
        }
        let command = codex::child(
            host,
            &flags,
            Path::new("admitted-view"),
            Some("native-reference"),
        )?;
        assert!(
            codex::child(host, &flags, Path::new("admitted-view"), Some("--unsafe")).is_err(),
            "resume references cannot change native options"
        );
        assert_eq!(command.get_current_dir(), Some(Path::new("admitted-view")));
        let arguments = command
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        for pair in flags.as_chunks::<2>().0 {
            assert!(arguments.windows(2).any(|window| window == pair));
        }
        assert!(
            arguments
                .windows(2)
                .any(|pair| pair == [resume_option, "native-reference"])
        );
    }
    assert!(
        codex::validate(
            "claude-code",
            &["--permission-mode".into(), "default".into()]
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn discard_routes_references_through_shared_fs_instead_of_cancelling_a_task() -> Result<()> {
    let task: acyclic_harness::agent::AgentPath =
        serde_json::from_value(json!("harness/agents/00000000000000000000000000000001"))?;
    let args = ["discard", "agents/child's-view"].map(String::from);
    let HostAction::Fs {
        task: selected,
        command,
    } = action(task.clone(), &args)?
    else {
        return Err("discard did not use the shared filesystem facade".into());
    };
    assert_eq!(selected, task);
    assert_eq!(command, "'discard' 'agents/child'\"'\"'s-view'");
    let fs = ["fs", "discard", "agents/child's-view"].map(String::from);
    assert!(
        matches!(action(task.clone(), &fs)?, HostAction::Fs { command: explicit, .. } if explicit == command)
    );
    assert!(action(task, &["cancel".into(), "agents/child".into()]).is_err());
    Ok(())
}

#[test]
fn malformed_and_oversized_inputs_fail_before_any_workspace_request() -> Result<()> {
    assert!(read_json(&b"not-json"[..]).is_err());
    assert!(read_json(vec![b' '; 65_537].as_slice()).is_err());
    assert_eq!(
        read_json(&b"{\"cwd\":\"fixture\"}"[..])?.get("cwd"),
        Some(&json!("fixture"))
    );
    Ok(())
}

#[test]
fn failed_lifecycle_coordination_stops_without_changing_native_permissions() {
    let denied = hook_failure("SessionStart", "owner unavailable");
    assert!(denied.get("hookSpecificOutput").is_none());
    assert_eq!(
        hook_failure("Stop", "owner unavailable").get("continue"),
        Some(&json!(false))
    );
    assert_eq!(
        hook("codex", "SessionStart", json!({})).get("continue"),
        Some(&json!(false))
    );
}

#[test]
fn claude_installation_preserves_foreign_hooks_permissions_and_unrelated_settings() -> Result<()> {
    let foreign = json!({"hooks":[{"type":"command","command":"foreign guard"}],"matcher":".*"});
    let original = json!({"permissions":{"deny":["Read(secret)"]},"theme":"dark","hooks":{"PreToolUse":[foreign]}});
    let mut document = original.clone();
    let executable = std::env::temp_dir().join("a path").join(if cfg!(windows) {
        "acyclic.exe"
    } else {
        "acyclic"
    });
    for _ in 0..2 {
        install::update_claude(&mut document, &executable.to_string_lossy(), true)?;
    }
    assert_eq!(
        document
            .pointer("/hooks/PreToolUse")
            .and_then(Value::as_array)
            .ok_or("fixture hook missing")?
            .len(),
        1
    );
    assert_eq!(document.get("permissions"), original.get("permissions"));
    assert_eq!(document.get("theme"), Some(&json!("dark")));
    install::update_claude(&mut document, &executable.to_string_lossy(), false)?;
    assert_eq!(document, original);
    let mut invalid = json!({"hooks":{"Stop":"foreign"}});
    assert!(install::update_claude(&mut invalid, &executable.to_string_lossy(), true).is_err());
    Ok(())
}

#[test]
fn claude_installer_uses_shared_private_publication() -> Result<()> {
    installer_publication("claude-code", "settings.json")
}

#[test]
fn pi_installer_uses_shared_private_publication() -> Result<()> {
    installer_publication("pi", "acyclic.ts")
}

fn installer_publication(host: &str, name: &str) -> Result<()> {
    let fixture = tempfile::tempdir()?;
    let path = fixture.path().join(name);
    install::write_config(host, &path, b"original", None)?;
    // End inspection custody before the next publisher takes the destination.
    assert!(acyclic_native_runtime::private_file::is_private(
        &acyclic_native_runtime::private_file::open(&path, false)?
    )?);
    install::write_config(host, &path, b"replacement", Some(b"original"))?;
    assert_eq!(std::fs::read(&path)?, b"replacement");
    assert!(acyclic_native_runtime::private_file::open(&path, false).is_ok());
    let error = install::write_config(host, &path, b"stale", Some(b"original"))
        .err()
        .ok_or("changed host settings must not be overwritten")?;
    assert!(error.to_string().contains("source changed"));
    let error = error.to_string();
    let (_, recovery) = error
        .split_once("recovery argv: ")
        .ok_or("installer refusal omitted its exact recovery arguments")?;
    assert_eq!(
        serde_json::from_str::<Value>(recovery)?,
        json!(["acyclic", "install", host, "--settings", path])
    );
    assert_eq!(std::fs::read(&path)?, b"replacement");
    assert_eq!(std::fs::read_dir(fixture.path())?.count(), 1);
    Ok(())
}
