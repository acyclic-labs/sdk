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
