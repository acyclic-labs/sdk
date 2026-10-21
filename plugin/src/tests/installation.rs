use super::*;

#[test]
fn pi_install_preserves_foreign_settings_and_refuses_modified_ownership() {
    let fixture = Fixture::new();
    let settings = fixture.data.join("settings.json");
    let assets = fixture.data.join("assets");
    let executable = fixture.data.join("acyclic path with spaces.exe");
    let prior = json!({"theme":"dark", "extensions":["foreign.ts"]});
    fs::write(&settings, serde_json::to_vec(&prior).unwrap()).unwrap();
    install_pi_at(&settings, &assets, &executable).unwrap();
    install_pi_at(&settings, &assets, &executable).unwrap();
    let installed = read_optional_json(&settings, "Pi settings")
        .unwrap()
        .unwrap();
    assert_eq!(installed["theme"], prior["theme"]);
    assert_eq!(installed["extensions"][0], "foreign.ts");
    assert_eq!(installed["extensions"].as_array().unwrap().len(), 2);
    let asset = Path::new(installed["extensions"][1].as_str().unwrap());
    let source = fs::read_to_string(asset).unwrap();
    assert!(source.contains(&serde_json::to_string(&executable).unwrap()));
    assert!(!source.contains("__ACYCLIC_EXECUTABLE__"));
    let mut edited = installed.clone();
    edited["theme"] = json!("light");
    fs::write(&settings, serde_json::to_vec(&edited).unwrap()).unwrap();
    assert!(install_pi_at(&settings, &assets, &executable).is_err());
    assert!(remove_owned_json(&settings, "pi-extension", "Pi settings").is_err());
    assert_eq!(
        read_optional_json(&settings, "Pi settings").unwrap(),
        Some(edited)
    );
    fs::write(&settings, serde_json::to_vec(&installed).unwrap()).unwrap();
    remove_owned_json(&settings, "pi-extension", "Pi settings").unwrap();
    assert_eq!(
        read_optional_json(&settings, "Pi settings").unwrap(),
        Some(prior)
    );
    fs::write(asset, "corrupt").unwrap();
    assert!(install_pi_at(&settings, &assets, &executable).is_err());
}

#[test]
fn mcp_install_restores_foreign_configuration_and_preserves_user_edits() {
    let fixture = Fixture::new();
    let path = fixture.data.join("config.json");
    let executable = fixture.data.join("acyclic");
    let prior = json!({"mcpServers":{"foreign":{"command":"foreign"}}});
    fs::write(&path, serde_json::to_vec(&prior).unwrap()).unwrap();
    merge_mcp_config(&path, McpConfigShape::Standard, &executable).unwrap();
    merge_mcp_config(&path, McpConfigShape::Standard, &executable).unwrap();
    let installed = fs::read(&path).unwrap();
    let mut edited: Value = serde_json::from_slice(&installed).unwrap();
    edited["mcpServers"]["acyclic"]["args"] = json!(["foreign"]);
    write_json_with_backup(&path, &edited).unwrap();
    assert!(remove_mcp_config(&path, McpConfigShape::Standard).is_err());
    assert_eq!(read_optional_json(&path, "MCP").unwrap(), Some(edited));
    fs::write(&path, installed).unwrap();
    remove_mcp_config(&path, McpConfigShape::Standard).unwrap();
    assert_eq!(read_optional_json(&path, "MCP").unwrap(), Some(prior));
    fs::write(
        &path,
        serde_json::to_vec(&json!({"mcpServers":{"acyclic":{"command":"not-ours"}}})).unwrap(),
    )
    .unwrap();
    assert!(merge_mcp_config(&path, McpConfigShape::Standard, &executable).is_err());
}

#[test]
fn shared_hooks_restore_foreign_entries_and_preserve_edits() {
    let fixture = Fixture::new();
    for section in ["claude-hooks", "cursor-hooks"] {
        let path = fixture.data.join(format!("{section}.json"));
        let prior = json!({"hooks":{"foreign":[{"command":format!("foreign __hook {section}")}]}});
        fs::write(&path, serde_json::to_vec(&prior).unwrap()).unwrap();
        install_owned_json(&path, section, "hooks", |prior| {
            let mut installed = prior.cloned().unwrap();
            installed["acyclicOwned"] = json!({"command":"acyclic"});
            Ok(installed)
        })
        .unwrap();
        let installed = fs::read(&path).unwrap();
        let mut edited: Value = serde_json::from_slice(&installed).unwrap();
        assert_eq!(edited["hooks"], prior["hooks"]);
        edited["acyclicOwned"]["command"] = json!("user-edited");
        fs::write(&path, serde_json::to_vec(&edited).unwrap()).unwrap();
        assert!(remove_owned_json(&path, section, "hooks").is_err());
        assert_eq!(read_optional_json(&path, "hooks").unwrap(), Some(edited));
        fs::write(&path, installed).unwrap();
        remove_owned_json(&path, section, "hooks").unwrap();
        assert_eq!(read_optional_json(&path, "hooks").unwrap(), Some(prior));
    }
}

#[test]
fn claude_user_settings_follow_the_configuration_directory_claude_reads() {
    let home = Path::new("home");
    assert_eq!(
        claude_user_settings(None, home),
        home.join(".claude").join("settings.json")
    );
    let configured = Path::new("configured");
    assert_eq!(
        claude_user_settings(Some(configured), home),
        configured.join("settings.json")
    );
}

#[test]
fn host_commands_resolve_package_manager_launchers_through_pathext() {
    let fixture = Fixture::new();
    let first = fixture.data.join("first");
    let second = fixture.data.join("second");
    fs::create_dir_all(&first).unwrap();
    fs::create_dir_all(&second).unwrap();
    let executable = |path: PathBuf| {
        fs::write(&path, "").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        }
    };
    executable(second.join("codex.cmd"));
    executable(second.join("codex"));
    // On Unix a file without execute permission earlier on PATH does not
    // hide the command, exactly as execvp skips it.
    #[cfg(unix)]
    fs::write(first.join("codex"), "").unwrap();
    let directories = || [first.clone(), second.clone()];
    assert_eq!(
        executable_in(
            directories(),
            "codex",
            Some(std::ffi::OsStr::new(".COM;.EXE;.BAT;.CMD"))
        ),
        Some(second.join("codex.cmd"))
    );
    assert_eq!(
        executable_in(directories(), "codex", None),
        Some(second.join("codex"))
    );
    executable(first.join("codex.exe"));
    assert_eq!(
        executable_in(
            directories(),
            "codex",
            Some(std::ffi::OsStr::new(".EXE;.CMD"))
        ),
        Some(first.join("codex.exe"))
    );
    assert_eq!(
        executable_in(directories(), "claude", Some(std::ffi::OsStr::new(".EXE"))),
        None
    );
}

#[test]
fn codex_installations_run_from_the_marketplace_or_codexs_plugin_copy() {
    let fixture = Fixture::new();
    let marketplace = fixture.data.join("package").join("plugin");
    let codex_home = fixture.data.join("codex");
    let copy = codex_plugin_cache_root(&codex_home);
    let foreign = fixture.data.join("other").join("plugin");
    for root in [&marketplace, &copy, &foreign] {
        fs::create_dir_all(root).unwrap();
    }
    let ownership = CodexPluginOwnership {
        version: 1,
        marketplace_root: marketplace.clone(),
        added_marketplace: true,
        plugin_was_installed: false,
        config_path: None,
        prior_default_permissions: None,
    };
    assert!(runs_owned_codex_plugin(
        &ownership,
        &marketplace,
        &codex_home
    ));
    assert!(runs_owned_codex_plugin(&ownership, &copy, &codex_home));
    assert!(!runs_owned_codex_plugin(&ownership, &foreign, &codex_home));
}

#[test]
fn doctor_judges_claude_code_hooks_by_the_executable_each_event_runs() {
    let fixture = Fixture::new();
    let settings = fixture.data.join("claude").join("settings.json");
    let status = |path: &Path| {
        claude_code_installation_check_at(path)["status"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    assert_eq!(status(&settings), "warn");
    let executable = fixture.data.join(if cfg!(windows) {
        "acyclic.exe"
    } else {
        "acyclic"
    });
    fs::write(&executable, "").unwrap();
    install_claude_hooks_at(&executable, &settings).unwrap();
    assert_eq!(status(&settings), "pass");
    fs::remove_file(&executable).unwrap();
    assert_eq!(status(&settings), "fail");
    fs::write(&executable, "").unwrap();
    let mut document = read_optional_json(&settings, "settings").unwrap().unwrap();
    document["hooks"]
        .as_object_mut()
        .unwrap()
        .remove("SubagentStop");
    fs::write(&settings, serde_json::to_vec(&document).unwrap()).unwrap();
    let check = claude_code_installation_check_at(&settings);
    assert_eq!(check["status"], "fail");
    assert!(check["detail"].as_str().unwrap().contains("SubagentStop"));
}

#[test]
fn doctor_reports_a_service_that_cannot_start_as_a_failed_check() {
    let (service, state) = service_checks(Err("native mount registry is unavailable"));
    assert_eq!(service["status"], "fail");
    assert_eq!(service["detail"], "native mount registry is unavailable");
    assert_eq!(state["status"], "warn");
    let (service, state) = service_checks(Ok(&ServiceSummary {
        identity: "identity".to_owned(),
        sessions: 1,
        routes: 0,
        leases: 0,
        pending_recovery: false,
    }));
    assert_eq!(service["status"], "pass");
    assert_eq!(state["status"], "pass");
}

#[test]
fn executable_digests_are_reused_only_for_the_same_settled_file() {
    let executable = current_executable().unwrap();
    let first = executable_digests_for(&executable).unwrap();
    let again = executable_digests_for(&executable).unwrap();
    assert_eq!(
        (&first.sha256, &first.blake3),
        (&again.sha256, &again.blake3)
    );
    let directory = tempfile::tempdir().unwrap();
    let other = directory.path().join("other-executable");
    fs::write(&other, b"another artifact").unwrap();
    let digests = executable_digests_for(&other).unwrap();
    assert_eq!(
        digests.blake3,
        blake3::hash(b"another artifact").to_hex().to_string()
    );
    assert_eq!(
        executable_digests_for(&executable).unwrap().blake3,
        first.blake3
    );
}

#[test]
fn doctor_fails_a_damaged_codex_ownership_record_instead_of_calling_it_absent() {
    let directory = tempfile::tempdir().unwrap();
    let record = directory.path().join("codex-plugin.json");
    let absent = codex_installation_check(&read_codex_ownership(&record), false, &record);
    assert_eq!(absent["status"], "warn");
    fs::write(&record, b"{ damaged").unwrap();
    let damaged = codex_installation_check(&read_codex_ownership(&record), false, &record);
    assert_eq!(damaged["status"], "fail");
    assert!(
        damaged["detail"]
            .as_str()
            .unwrap()
            .contains(&record.display().to_string())
    );
}
