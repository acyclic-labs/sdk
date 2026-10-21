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
    assert!(runs_owned_codex_plugin(&ownership, &marketplace, &codex_home));
    assert!(runs_owned_codex_plugin(&ownership, &copy, &codex_home));
    assert!(!runs_owned_codex_plugin(&ownership, &foreign, &codex_home));
}
