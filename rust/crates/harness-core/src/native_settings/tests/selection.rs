use super::*;
use proptest::prelude::*;

#[test]
fn inactive_mcp_sources_stay_unread_or_untyped_while_hooks_remain_selected() {
    let fixture = Fixture::new().unwrap();
    write(
        &fixture.home.join(".claude/settings.json"),
        br#"{"hooks":{"PreToolUse":[{"hooks":[{"type":"command","command":"guard"}]}]}}"#,
    )
    .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let strategy = (0usize..3, ".{0,64}");
    // Property: every invalid MCP-only document/section is ignored when that
    // subsystem is inactive, regardless of user/project scope. Hook selection
    // survives. Re-enabling MCP is a negative control that must reject it.
    proptest::test_runner::TestRunner::default()
        .run(&strategy, |(source, invalid)| {
            write(&fixture.home.join(".claude.json"), b"{}").unwrap();
            write(&fixture.codex.join("config.toml"), b"").unwrap();
            write(&fixture.workspace.join(".mcp.json"), b"{}").unwrap();
            match source {
                0 => write(
                    &fixture.home.join(".claude.json"),
                    serde_json::to_vec(&invalid).unwrap(),
                ),
                1 => write(&fixture.codex.join("config.toml"), b"mcp_servers = 42"),
                _ => write(
                    &fixture.workspace.join(".mcp.json"),
                    json!({"mcpServers":invalid}).to_string(),
                ),
            }
            .unwrap();
            let config = |load_mcp| {
                let mut config = fixture.config().trust_project_mcp(true);
                config.load_mcp = load_mcp;
                config
            };
            let snapshot = runtime
                .block_on(NativeSettings::load(config(false)))
                .unwrap();
            prop_assert!(!snapshot.hooks.is_empty());
            prop_assert_eq!(snapshot.mcp.server_names().count(), 0);
            prop_assert!(
                runtime
                    .block_on(NativeSettings::load(config(true)))
                    .is_err()
            );
            Ok(())
        })
        .unwrap();
}
