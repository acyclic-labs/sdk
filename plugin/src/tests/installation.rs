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
