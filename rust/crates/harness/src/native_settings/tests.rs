use super::*;
use crate::mcp::McpTransport;
use serde_json::json;
#[cfg(feature = "hooks")]
#[path = "tests/selection.rs"]
mod selection;

#[cfg(all(
    feature = "filesystem-local",
    feature = "test-support",
    any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http")
))]
mod isolation {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        // Property: workspace spelling never changes the isolated settings roots;
        // the owned temporary home survives construction and is removed on drop.
        #[test]
        fn local_test_config_cannot_resolve_user_settings(workspace in ".{0,64}") {
            let config = crate::test_support::local_harness_config(
                PathBuf::from(workspace),
                (
                    crate::model::Model::new("test".into(), "settings".into(), "1".into(), Value::Null).unwrap(),
                    std::sync::Arc::new(crate::test_support::ScriptedModelProvider::new([])),
                ),
            ).unwrap();
            let settings = config.settings.expect("test configuration must select settings");
            let temporary = settings._test_home.as_ref().expect("test settings must own their temporary home");
            prop_assert_eq!(&settings.home, temporary.path());
            prop_assert_eq!(&settings.codex_home, &temporary.path().join(".codex"));
            prop_assert!(temporary.path().is_dir());
            let home = settings.home.clone();
            drop(settings);
            prop_assert!(!home.exists(), "temporary settings must be cleaned up");
        }
    }
}
struct Fixture {
    _root: tempfile::TempDir,
    home: PathBuf,
    workspace: PathBuf,
    codex: PathBuf,
}

#[cfg(all(
    feature = "filesystem-local",
    feature = "hooks",
    any(feature = "mcp-stdio", feature = "mcp-http")
))]
#[test]
fn mcp_startup_queries_only_its_selected_settings_roles() -> Result<()> {
    use proptest::prelude::*;
    // Reuse the fixture and worker runtime: only the selected file data changes
    // in each shrinking case, rather than paying directory setup 256 times.
    let fixture = Fixture::new()?;
    let config = std::sync::Arc::new(fixture.config().trust_project_hooks(true));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(Error::process_io)?;
    proptest!(|(invalid in prop_oneof![
        any::<i64>().prop_map(|value| value.to_string()), Just("false".into()),
        Just("'invalid'".into()), Just("[]".into()),
    ])| {
        runtime.block_on(async {
            let user = fixture.home.join(".claude/settings.json");
            let project = fixture.workspace.join(".claude/settings.json");
            write(&user, "{}")?;
            write(&project, "{}")?;
            write(&fixture.codex.join("config.toml"), format!(
                "hooks = {invalid}\n[mcp_servers.selected]\ncommand = 'selected'\n"
            ))?;
            let selected = NativeSettings::load_mcp(std::sync::Arc::clone(&config)).await?;
            prop_assert_eq!(selected.server_names().collect::<Vec<_>>(), ["selected"]);
            // The hook query must reject the same malformed selected table;
            // this is also the negative control for accidental eager import.
            prop_assert!(NativeSettings::load_hooks(std::sync::Arc::clone(&config)).await.is_err());
            write(&user, "{invalid JSON")?;
            write(&project, "{invalid JSON")?;
            let selected = NativeSettings::load_mcp(std::sync::Arc::clone(&config)).await?;
            prop_assert_eq!(selected.server_names().collect::<Vec<_>>(), ["selected"]);
            Ok(())
        })?;
    });
    Ok(())
}

#[cfg(windows)]
#[tokio::test]
async fn project_keys_accept_nonverbatim_case_and_separator_aliases() -> Result<()> {
    let fixture = Fixture::new()?;
    let canonical = std::fs::canonicalize(&fixture.workspace).map_err(Error::process_io)?;
    let key = canonical
        .to_str()
        .unwrap()
        .strip_prefix(r"\\?\")
        .unwrap()
        .replace('\\', "/")
        .to_lowercase();
    write(
        &fixture.home.join(".claude.json"),
        json!({"projects":{
            key.as_str():{"mcpServers":{"selected":{"command":"selected"}}},
            "unrelated":{"mcpServers":42}
        }})
        .to_string(),
    )?;
    let mut config = fixture.config();
    config.workspace = fixture.workspace.join(".");
    let snapshot = NativeSettings::load(config).await?;
    assert_eq!(
        snapshot.mcp.server_names().collect::<Vec<_>>(),
        ["selected"]
    );
    Ok(())
}

impl Fixture {
    fn new() -> Result<Self> {
        let root = tempfile::tempdir().map_err(Error::process_io)?;
        let home = root.path().join("home");
        let workspace = root.path().join("project");
        let codex = root.path().join("codex");
        for path in [&home, &workspace, &codex] {
            std::fs::create_dir(path).map_err(Error::process_io)?;
        }
        Ok(Self {
            _root: root,
            home,
            workspace,
            codex,
        })
    }

    fn config(&self) -> SettingsConfig {
        SettingsConfig::new(&self.workspace, &self.home).codex_home(&self.codex)
    }
}

fn write(path: &Path, bytes: impl AsRef<[u8]>) -> Result<()> {
    std::fs::create_dir_all(
        path.parent()
            .ok_or_else(|| Error::Invalid("missing parent".into()))?,
    )
    .map_err(Error::process_io)?;
    std::fs::write(path, bytes).map_err(Error::process_io)
}

fn executable<'a>(config: &'a McpConfig, name: &str) -> Option<&'a str> {
    config.servers().find_map(|(key, server)| {
        if key != name {
            return None;
        }
        match &server.transport {
            McpTransport::Stdio { command, .. } => Some(command.as_str()),
            McpTransport::Http { .. } => None,
        }
    })
}

#[tokio::test]
async fn user_project_and_local_scopes_have_whole_entry_precedence() -> Result<()> {
    let fixture = Fixture::new()?;
    let canonical = std::fs::canonicalize(&fixture.workspace).map_err(Error::process_io)?;
    let user = json!({"mcpServers": {
        "user":{"command":"user"}, "shared":{"command":"claude", "args":["discard"]},
        "local":{"command":"user"}, "disabled":{"command":"user"}},
        "projects": {canonical.to_string_lossy().as_ref(): {"mcpServers":{"local":{"command":"local"}}}}});
    write(
        &fixture.home.join(".claude.json"),
        serde_json::to_vec(&user).map_err(|error| Error::Invalid(error.to_string()))?,
    )?;
    write(
        &fixture.codex.join("config.toml"),
        "[mcp_servers.shared]\ncommand='codex'\n[mcp_servers.disabled]\nenabled=false\n",
    )?;
    write(
        &fixture.workspace.join(".mcp.json"),
        br#"{"mcpServers":{"project":{"command":"project"},"local":{"command":"project"}}}"#,
    )?;
    write(
        &fixture.workspace.join(".codex/config.toml"),
        "[mcp_servers.shared]\ncommand='project-codex'\n",
    )?;
    let defaults = NativeSettings::load(fixture.config()).await?;
    assert_eq!(
        defaults.mcp.server_names().collect::<Vec<_>>(),
        ["local", "shared", "user"]
    );
    assert_eq!(executable(&defaults.mcp, "shared"), Some("codex"));
    assert_eq!(executable(&defaults.mcp, "local"), Some("local"));
    let trusted = NativeSettings::load(fixture.config().trust_project_mcp(true)).await?;
    assert_eq!(
        trusted.mcp.server_names().collect::<Vec<_>>(),
        ["local", "project", "shared", "user"]
    );
    assert_eq!(executable(&trusted.mcp, "shared"), Some("project-codex"));
    assert_eq!(executable(&trusted.mcp, "local"), Some("local"));
    let (_, shared) = trusted
        .mcp
        .servers()
        .find(|(name, _)| *name == "shared")
        .ok_or_else(|| Error::Invalid("missing shared server".into()))?;
    assert!(matches!(&shared.transport, McpTransport::Stdio { args, .. } if args.is_empty()));
    Ok(())
}

#[tokio::test]
async fn untrusted_project_files_are_not_read() -> Result<()> {
    let fixture = Fixture::new()?;
    for name in [
        ".mcp.json",
        ".codex/config.toml",
        ".codex/hooks.json",
        ".claude/settings.json",
        ".claude/settings.local.json",
    ] {
        write(&fixture.workspace.join(name), b"invalid configuration")?;
    }
    let snapshot = NativeSettings::load(fixture.config()).await?;
    assert_eq!(snapshot.mcp.server_names().count(), 0);
    assert!(
        NativeSettings::load(fixture.config().trust_project_mcp(true))
            .await
            .is_err()
    );
    #[cfg(feature = "hooks")]
    assert!(
        NativeSettings::load(fixture.config().trust_project_hooks(true))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn local_project_selection_accepts_the_configured_workspace_spelling() -> Result<()> {
    let fixture = Fixture::new()?;
    let configured = fixture.workspace.join("..").join("project");
    let document = json!({"projects": {configured.to_string_lossy().as_ref(): {
        "mcpServers":{"local":{"command":"local"}}
    }}});
    write(
        &fixture.home.join(".claude.json"),
        serde_json::to_vec(&document).map_err(|error| Error::Invalid(error.to_string()))?,
    )?;
    let snapshot = NativeSettings::load(
        SettingsConfig::new(&configured, &fixture.home).codex_home(&fixture.codex),
    )
    .await?;
    assert_eq!(executable(&snapshot.mcp, "local"), Some("local"));
    Ok(())
}

#[cfg(feature = "hooks")]
#[tokio::test]
async fn project_hook_and_server_trust_are_independent() -> Result<()> {
    let fixture = Fixture::new()?;
    let path = fixture.workspace.join(".codex/config.toml");
    write(
        &path,
        "[mcp_servers.project]\ncommand='server'\n[[hooks.PreToolUse]]\nmatcher='['\n[[hooks.PreToolUse.hooks]]\ntype='command'\ncommand='guard'\n",
    )?;
    let snapshot = NativeSettings::load(fixture.config().trust_project_mcp(true)).await?;
    assert_eq!(snapshot.mcp.server_names().collect::<Vec<_>>(), ["project"]);
    assert!(
        NativeSettings::load(fixture.config().trust_project_hooks(true))
            .await
            .is_err()
    );
    write(
        &path,
        "[mcp_servers.project]\ncommand=42\n[[hooks.Stop]]\n[[hooks.Stop.hooks]]\ntype='command'\ncommand='guard'\n",
    )?;
    let snapshot = NativeSettings::load(fixture.config().trust_project_hooks(true)).await?;
    assert_eq!(snapshot.mcp.server_names().count(), 0);
    assert!(
        NativeSettings::load(fixture.config().trust_project_mcp(true))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn settings_reads_have_positive_finite_byte_bounds() -> Result<()> {
    let fixture = Fixture::new()?;
    write(&fixture.codex.join("config.toml"), b"[mcp_servers]\n")?;
    assert!(
        NativeSettings::load(fixture.config().maximum_file_bytes(14))
            .await
            .is_ok()
    );
    for maximum in [0, 13, u64::MAX] {
        assert!(
            NativeSettings::load(fixture.config().maximum_file_bytes(maximum))
                .await
                .is_err()
        );
    }
    write(&fixture.codex.join("config.toml"), [0xff])?;
    assert!(NativeSettings::load(fixture.config()).await.is_err());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn canonical_deduplication_combines_hook_and_mcp_roles() -> Result<()> {
    let fixture = Fixture::new()?;
    let source = fixture.codex.join("hooks.json");
    write(&source, br#"{"mcpServers":{"shared":{"command":"server"}},"hooks":{"Stop":[{"hooks":[{"type":"command","command":"guard"}]}]}}"#)?;
    std::os::unix::fs::symlink(&source, fixture.workspace.join(".mcp.json"))
        .map_err(Error::process_io)?;
    let snapshot = NativeSettings::load(fixture.config().trust_project_mcp(true)).await?;
    assert_eq!(snapshot.mcp.server_names().collect::<Vec<_>>(), ["shared"]);
    Ok(())
}

#[test]
fn codex_home_uses_the_configured_environment_path() {
    let home = Path::new("user");
    assert_eq!(codex_directory(home, None), home.join(".codex"));
    assert_eq!(
        codex_directory(home, Some(std::ffi::OsStr::new(""))),
        home.join(".codex")
    );
    assert_eq!(
        codex_directory(home, Some(std::ffi::OsStr::new("relocated"))),
        PathBuf::from("relocated")
    );
}

#[cfg(unix)]
#[tokio::test]
async fn canonical_aliases_keep_the_highest_declared_scope() -> Result<()> {
    let fixture = Fixture::new()?;
    let source = fixture.home.join(".claude.json");
    write(
        &source,
        br#"{"mcpServers":{"shared":{"command":"claude"}}}"#,
    )?;
    write(
        &fixture.codex.join("config.toml"),
        "[mcp_servers.shared]\ncommand='codex'\n",
    )?;
    std::os::unix::fs::symlink(&source, fixture.workspace.join(".mcp.json"))
        .map_err(Error::process_io)?;
    let user = NativeSettings::load(fixture.config()).await?;
    assert_eq!(executable(&user.mcp, "shared"), Some("codex"));
    let project = NativeSettings::load(fixture.config().trust_project_mcp(true)).await?;
    assert_eq!(executable(&project.mcp, "shared"), Some("claude"));
    Ok(())
}

#[tokio::test]
async fn only_winning_server_entries_are_validated() -> Result<()> {
    let fixture = Fixture::new()?;
    write(
        &fixture.home.join(".claude.json"),
        br#"{"mcpServers":{"shared":{"command":42},"disabled":{"command":42}}}"#,
    )?;
    write(
        &fixture.codex.join("config.toml"),
        "[mcp_servers.shared]\ncommand='winner'\n[mcp_servers.disabled]\nenabled=false\ncommand=42\n",
    )?;
    let snapshot = NativeSettings::load(fixture.config()).await?;
    assert_eq!(snapshot.mcp.server_names().collect::<Vec<_>>(), ["shared"]);
    assert_eq!(executable(&snapshot.mcp, "shared"), Some("winner"));
    write(
        &fixture.codex.join("config.toml"),
        "[mcp_servers.shared]\ncommand=42\n[mcp_servers.disabled]\nenabled=false\n",
    )?;
    assert!(NativeSettings::load(fixture.config()).await.is_err());
    write(
        &fixture.workspace.join(".mcp.json"),
        br#"{"mcpServers":{"shared":{"command":"project-winner"}}}"#,
    )?;
    let snapshot = NativeSettings::load(fixture.config().trust_project_mcp(true)).await?;
    assert_eq!(executable(&snapshot.mcp, "shared"), Some("project-winner"));
    Ok(())
}

#[tokio::test]
async fn unrelated_workspace_entries_are_not_validated() -> Result<()> {
    let fixture = Fixture::new()?;
    let configured = fixture.workspace.to_string_lossy();
    let document = json!({"projects": {
        "unrelated-project": {"mcpServers":{"broken":{"command":42}}},
        "another-project": 42,
        configured.as_ref(): {"mcpServers":{"selected":{"command":"winner"}}}
    }});
    write(
        &fixture.home.join(".claude.json"),
        serde_json::to_vec(&document).map_err(|error| Error::Invalid(error.to_string()))?,
    )?;
    let snapshot = NativeSettings::load(fixture.config()).await?;
    assert_eq!(
        snapshot.mcp.server_names().collect::<Vec<_>>(),
        ["selected"]
    );
    assert_eq!(executable(&snapshot.mcp, "selected"), Some("winner"));
    let invalid =
        json!({"projects": {configured.as_ref(): {"mcpServers":{"selected":{"command":42}}}}});
    write(
        &fixture.home.join(".claude.json"),
        serde_json::to_vec(&invalid).map_err(|error| Error::Invalid(error.to_string()))?,
    )?;
    assert!(NativeSettings::load(fixture.config()).await.is_err());
    Ok(())
}

#[cfg(feature = "hooks")]
#[tokio::test]
async fn untrusted_hook_sections_do_not_block_trusted_project_mcp() -> Result<()> {
    let fixture = Fixture::new()?;
    write(
        &fixture.workspace.join(".codex/config.toml"),
        "hooks=42\n[mcp_servers.selected]\ncommand='winner'\n",
    )?;
    let snapshot = NativeSettings::load(fixture.config().trust_project_mcp(true)).await?;
    assert_eq!(executable(&snapshot.mcp, "selected"), Some("winner"));
    assert!(
        NativeSettings::load(
            fixture
                .config()
                .trust_project_mcp(true)
                .trust_project_hooks(true)
        )
        .await
        .is_err()
    );
    Ok(())
}

#[cfg(feature = "hooks")]
async fn stop_hooks(
    snapshot: NativeSettings,
    fixture: &Fixture,
) -> Result<crate::hooks::HookOutput> {
    snapshot
        .hooks
        .run(
            crate::hooks::HookEvent::Stop,
            &fixture.workspace,
            &fixture.workspace,
            &json!({}),
            &crate::tool::ToolRegistry::default(),
        )
        .await
}

#[cfg(feature = "hooks")]
#[tokio::test]
async fn disabling_hooks_uses_trusted_settings_precedence_before_registration() -> Result<()> {
    let fixture = Fixture::new()?;
    let guard = json!({"Stop":[{"hooks":[{"type":"command",
        "command":"nonexistent-settings-regression-guard", "args":[], "onFailure":"block"}]}]});
    let user = fixture.home.join(".claude/settings.json");
    let project = fixture.workspace.join(".claude/settings.json");
    let local = fixture.workspace.join(".claude/settings.local.json");
    write(
        &user,
        json!({"disableAllHooks":true, "hooks":guard}).to_string(),
    )?;
    write(
        &project,
        json!({"disableAllHooks":false, "hooks":guard}).to_string(),
    )?;
    for trust_mcp in [false, true] {
        let snapshot = NativeSettings::load(fixture.config().trust_project_mcp(trust_mcp)).await?;
        let output = stop_hooks(snapshot, &fixture).await?;
        assert!(
            output.diagnostics.is_empty(),
            "untrusted project override ran: {output:?}"
        );
        assert!(output.blocked.is_none());
    }
    let snapshot = NativeSettings::load(fixture.config().trust_project_hooks(true)).await?;
    let output = stop_hooks(snapshot, &fixture).await?;
    assert_eq!(
        output.diagnostics.len(),
        2,
        "trusted false must restore both guards"
    );
    assert!(
        output.blocked.is_none(),
        "Stop hook failures only report diagnostics"
    );
    write(&local, br#"{"disableAllHooks":true,"hooks":42}"#)?;
    let snapshot = NativeSettings::load(fixture.config().trust_project_hooks(true)).await?;
    let output = stop_hooks(snapshot, &fixture).await?;
    assert!(
        output.diagnostics.is_empty(),
        "disabled hooks must stay untyped: {output:?}"
    );
    assert!(output.blocked.is_none());
    write(&user, br#"{"disableAllHooks":"invalid","hooks":42}"#)?;
    assert!(
        NativeSettings::load(fixture.config().trust_project_hooks(true))
            .await
            .is_ok(),
        "overridden disable flag must remain untyped"
    );
    assert!(
        NativeSettings::load(fixture.config()).await.is_err(),
        "selected disable flag must be a boolean"
    );
    Ok(())
}

#[cfg(all(unix, feature = "hooks"))]
#[tokio::test]
async fn canonical_aliases_preserve_the_first_hook_bearing_source_order() -> Result<()> {
    let fixture = Fixture::new()?;
    let hook = |name: &str| {
        json!({"PreToolUse":[{"hooks":[{"type":"command", "command":format!(
        "printf '%s' '{{\"hookSpecificOutput\":{{\"updatedInput\":{{\"selected\":\"{name}\"}},\"additionalContext\":\"{name}\"}}}}'"
    ), "shell":"sh"}]}]})
    };
    let source = fixture.home.join(".claude.json");
    write(&source, json!({"hooks":hook("project")}).to_string())?;
    write(
        &fixture.home.join(".claude/settings.json"),
        json!({"hooks":hook("user")}).to_string(),
    )?;
    std::fs::create_dir_all(fixture.workspace.join(".claude")).map_err(Error::process_io)?;
    std::os::unix::fs::symlink(
        &source,
        fixture.workspace.join(".claude/settings.local.json"),
    )
    .map_err(Error::process_io)?;
    let snapshot = NativeSettings::load(fixture.config().trust_project_hooks(true)).await?;
    let output = snapshot
        .hooks
        .run(
            crate::hooks::HookEvent::PreToolUse,
            &fixture.workspace,
            &fixture.workspace,
            &json!({"tool_name":"custom"}),
            &crate::tool::ToolRegistry::default(),
        )
        .await?;
    assert!(output.diagnostics.is_empty(), "{output:?}");
    assert_eq!(output.additional_context, ["user", "project"]);
    assert_eq!(output.updated_input, Some(json!({"selected":"project"})));
    Ok(())
}
