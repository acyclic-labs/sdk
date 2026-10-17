use super::*;

#[tokio::test]
async fn project_disabled_servers_stay_untyped_across_claude_scopes() -> Result<()> {
    let fixture = Fixture::new()?;
    let canonical = std::fs::canonicalize(&fixture.workspace).map_err(Error::process_io)?;
    write(
        &fixture.home.join(".claude.json"),
        json!({
            "mcpServers": {
                "user": {"command": 42},
                "local": {"command": "overridden"},
                "shared": {"command": 42},
                "enabled": {"command": "enabled"},
                "inline-disabled": {"enabled": false, "command": 42}
            },
            "projects": {canonical.to_string_lossy().as_ref(): {
                "disabledMcpServers": ["user", "local", "project", "shared", "absent", "user"],
                "mcpServers": {"local": {"command": false}}
            }}
        })
        .to_string(),
    )?;
    write(
        &fixture.workspace.join(".mcp.json"),
        json!({"mcpServers": {
            "project": {"command": []},
            "local": {"command": "project must not replace local"}
        }})
        .to_string(),
    )?;
    write(
        &fixture.codex.join("config.toml"),
        "[mcp_servers.shared]\ncommand='codex'\n",
    )?;
    for trust in [false, true] {
        let snapshot = NativeSettings::load(fixture.config().trust_project_mcp(trust)).await?;
        assert_eq!(
            snapshot.mcp.server_names().collect::<Vec<_>>(),
            ["enabled", "shared"]
        );
        assert_eq!(executable(&snapshot.mcp, "shared"), Some("codex"));
    }
    Ok(())
}

#[tokio::test]
async fn project_disable_changes_do_not_remove_or_merge_declarations() -> Result<()> {
    let fixture = Fixture::new()?;
    let canonical = std::fs::canonicalize(&fixture.workspace).map_err(Error::process_io)?;
    let mut document = json!({
        "mcpServers": {"shared": {"command": "user", "args": ["discard"]}},
        "projects": {canonical.to_string_lossy().as_ref(): {
            "mcpServers": {"shared": {"command": "local"}}
        }}
    });
    write(
        &fixture.codex.join("config.toml"),
        "[mcp_servers.shared]\ncommand='codex'\n",
    )?;
    write(
        &fixture.workspace.join(".mcp.json"),
        r#"{"mcpServers":{"shared":{"command":"project"}}}"#,
    )?;
    for disabled in [json!([]), json!(["shared"]), json!([])] {
        let enabled = disabled.as_array().unwrap().is_empty();
        document["projects"][canonical.to_string_lossy().as_ref()]["disabledMcpServers"] = disabled;
        write(&fixture.home.join(".claude.json"), document.to_string())?;
        let snapshot = NativeSettings::load(fixture.config().trust_project_mcp(true)).await?;
        if enabled {
            assert_eq!(executable(&snapshot.mcp, "shared"), Some("local"));
            let (_, server) = snapshot.mcp.servers().next().unwrap();
            assert!(
                matches!(&server.transport, McpTransport::Stdio { args, .. } if args.is_empty())
            );
        } else {
            assert_eq!(snapshot.mcp.server_names().count(), 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn only_the_selected_user_project_can_disable_claude_servers() -> Result<()> {
    let fixture = Fixture::new()?;
    let canonical = std::fs::canonicalize(&fixture.workspace).map_err(Error::process_io)?;
    write(
        &fixture.home.join(".claude.json"),
        json!({
            "mcpServers": {"selected": {"command": "user"}},
            "projects": {
                canonical.to_string_lossy().as_ref(): {"disabledMcpServers": []},
                "unrelated": {"disabledMcpServers": 42, "mcpServers": false}
            }
        })
        .to_string(),
    )?;
    write(
        &fixture.workspace.join(".mcp.json"),
        json!({
            "disabledMcpServers": ["selected"],
            "projects": {canonical.to_string_lossy().as_ref(): {"disabledMcpServers": ["selected"]}}
        })
        .to_string(),
    )?;
    let snapshot = NativeSettings::load(fixture.config().trust_project_mcp(true)).await?;
    assert_eq!(executable(&snapshot.mcp, "selected"), Some("user"));
    Ok(())
}

#[tokio::test]
async fn selected_project_disable_list_requires_an_array_of_names() -> Result<()> {
    let fixture = Fixture::new()?;
    let canonical = std::fs::canonicalize(&fixture.workspace).map_err(Error::process_io)?;
    for disabled in [
        json!(false),
        json!("server"),
        json!({}),
        json!(["server", 42]),
        Value::Null,
    ] {
        write(
            &fixture.home.join(".claude.json"),
            json!({"projects": {canonical.to_string_lossy().as_ref(): {
                "disabledMcpServers": disabled
            }}})
            .to_string(),
        )?;
        let error = NativeSettings::load(fixture.config()).await.err().unwrap();
        assert!(matches!(error, Error::Invalid(message) if message.contains("disabledMcpServers")));
        let mut config = fixture.config();
        config.load_mcp = false;
        assert_eq!(
            NativeSettings::load(config)
                .await?
                .mcp
                .server_names()
                .count(),
            0
        );
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn project_disable_lists_accept_configured_and_canonical_workspace_names() -> Result<()> {
    let fixture = Fixture::new()?;
    let alias = fixture.home.join("workspace-alias");
    std::os::unix::fs::symlink(&fixture.workspace, &alias).map_err(Error::process_io)?;
    let canonical = std::fs::canonicalize(&fixture.workspace).map_err(Error::process_io)?;
    for workspace_key in [&alias, &canonical] {
        write(
            &fixture.home.join(".claude.json"),
            json!({
                "mcpServers": {"disabled": {"command": "unused"}, "enabled": {"command": "enabled"}},
                "projects": {workspace_key.to_string_lossy().as_ref(): {"disabledMcpServers": ["disabled"]}}
            })
            .to_string(),
        )?;
        let config = SettingsConfig::new(&alias, &fixture.home).codex_home(&fixture.codex);
        let snapshot = NativeSettings::load(config).await?;
        assert_eq!(snapshot.mcp.server_names().collect::<Vec<_>>(), ["enabled"]);
    }
    Ok(())
}
