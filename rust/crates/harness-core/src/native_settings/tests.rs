use super::*;
use crate::mcp::McpTransport;
use serde_json::json;

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
            let settings = SettingsConfig::for_test(PathBuf::from(workspace)).unwrap();
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
        "printf '%s' '{}'", json!({"hookSpecificOutput":{"additionalContext":name}})
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
    assert!(output.blocked.is_none(), "{output:?}");
    assert_eq!(output.additional_context, ["user", "project"]);
    assert!(output.updated_input.is_none());
    Ok(())
}
